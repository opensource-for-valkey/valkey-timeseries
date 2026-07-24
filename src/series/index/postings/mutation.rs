//! Write path: adding and removing series from the inverted index.
//!
//! Every mutation here maintains three pieces of state together — the `label=value` posting lists,
//! `all_postings`, and the id directory — so that a partially indexed series is never observable.

use super::{IndexKey, Postings, PostingsBitmap};
use crate::common::logging::log_warning;
use crate::labels::{InternedLabel, SeriesLabel};
use crate::series::{SeriesRef, TimeSeries};
use blart::AsBytes;
use blart::map::Entry as ARTEntry;

use super::KeyType;

/// One series buffered during a load window, consumed by [`Postings::bulk_index`]
pub(crate) struct BulkIndexEntry {
    pub id: SeriesRef,
    pub key: KeyType,
    /// Precomputed `label=value` index keys for every label of the series.
    pub label_keys: Vec<IndexKey>,
}

impl Postings {
    pub(super) fn remove_posting_for_label_value(
        &mut self,
        label: &str,
        value: &str,
        ts_id: SeriesRef,
    ) -> bool {
        let key = IndexKey::for_label_value(label, value);
        if let Some(bmp) = self.label_index.get_mut(&key) {
            let removed = bmp.remove_checked(ts_id);
            if removed && bmp.is_empty() {
                self.label_index.remove(&key);
            }
            return removed;
        }
        false
    }

    pub(in crate::series::index) fn add_posting_for_label_value(
        &mut self,
        ts_id: SeriesRef,
        label: &str,
        value: &str,
    ) -> bool {
        self.all_postings.add(ts_id);
        self.add_posting_for_label_value_internal(ts_id, label, value)
    }

    fn add_posting_for_label_value_internal(
        &mut self,
        ts_id: SeriesRef,
        label: &str,
        value: &str,
    ) -> bool {
        let key = IndexKey::for_label_value(label, value);
        match self.label_index.entry(key) {
            ARTEntry::Occupied(mut entry) => {
                entry.get_mut().add(ts_id);
                false
            }
            ARTEntry::Vacant(entry) => {
                let mut bitmap = PostingsBitmap::new();
                bitmap.add(ts_id);
                entry.insert(bitmap);
                true
            }
        }
    }

    /// Indexes a batch of loaded series in one pass. Semantically equivalent to calling
    /// [`Postings::index_timeseries`] once per entry, but does the work set-at-a-time:
    /// `all_postings` gets one sorted `add_many`, and the `(label=value, id)` pairs are sorted
    /// so each posting list is touched once (`add_many` per run) with cache-friendly
    /// sorted-order ART inserts.
    ///
    /// Safe against a non-empty index: existing posting lists are extended (bitmap adds are
    /// idempotent) and `id_to_key` inserts overwrite.
    pub(crate) fn bulk_index(&mut self, entries: Vec<BulkIndexEntry>) {
        if entries.is_empty() {
            return;
        }

        // (Re)indexing asserts the series are alive — revoke pending stale markings, same as
        // `index_timeseries`.
        if !self.stale_ids.is_empty() {
            for entry in &entries {
                self.stale_ids.remove(entry.id);
            }
        }

        let mut ids: Vec<SeriesRef> = entries.iter().map(|e| e.id).collect();
        ids.sort_unstable();
        self.all_postings.add_many(&ids);

        let total_labels: usize = entries.iter().map(|e| e.label_keys.len()).sum();
        let mut pairs: Vec<(IndexKey, SeriesRef)> = Vec::with_capacity(total_labels);
        for entry in entries {
            for label_key in entry.label_keys {
                pairs.push((label_key, entry.id));
            }
            self.id_to_key.insert(entry.id, entry.key);
        }
        pairs.sort_unstable_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()).then(a.1.cmp(&b.1)));

        // Walk the sorted pairs, flushing one add_many per run of equal label keys.
        let mut run_ids: Vec<SeriesRef> = Vec::new();
        let mut current_key: Option<IndexKey> = None;
        for (key, id) in pairs {
            match &current_key {
                Some(cur) if *cur == key => run_ids.push(id),
                _ => {
                    if let Some(cur) = current_key.take() {
                        self.add_many_for_key(cur, &run_ids);
                    }
                    run_ids.clear();
                    run_ids.push(id);
                    current_key = Some(key);
                }
            }
        }
        if let Some(cur) = current_key.take() {
            self.add_many_for_key(cur, &run_ids);
        }
    }

    fn add_many_for_key(&mut self, key: IndexKey, ids: &[SeriesRef]) {
        match self.label_index.entry(key) {
            ARTEntry::Occupied(mut entry) => entry.get_mut().add_many(ids),
            ARTEntry::Vacant(entry) => {
                let mut bitmap = PostingsBitmap::new();
                bitmap.add_many(ids);
                entry.insert(bitmap);
            }
        }
    }

    pub fn index_timeseries(&mut self, ts: &TimeSeries, key: &[u8]) {
        debug_assert!(ts.id != 0);
        let id = ts.id;

        // (Re)indexing asserts the series is alive: revoke any pending stale marking, or a later
        // GC drain would strip the id from the very label bitmaps being filled here (e.g. the
        // post-load repair scan re-indexing an id the reconciliation sweep just marked stale).
        if !self.stale_ids.is_empty() {
            self.stale_ids.remove(id);
        }

        for InternedLabel { name, value } in ts.labels.iter() {
            self.add_posting_for_label_value_internal(id, name, value);
        }

        self.all_postings.add(id);
        self.set_timeseries_key(id, key);
    }

    pub fn remove_timeseries(&mut self, series: &TimeSeries) -> bool {
        let id = series.id;
        if self.id_to_key.remove(&id).is_none() {
            log_warning(format!(
                "Tried to remove non-existing series id {id} from index"
            ));
        };
        let removed = self.all_postings.remove_checked(id);
        for label in series.labels.iter() {
            self.remove_posting_for_label_value(label.name(), label.value(), id);
        }
        removed
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::{assert_postings_eq, make_bulk_entry, make_series};
    use super::*;
    use crate::labels::{Label, MetricName};
    use crate::series::index::postings::Postings;

    type SeriesSpec = (SeriesRef, String, Vec<(String, String)>);

    #[test]
    fn test_bulk_index_matches_per_key_indexing() {
        // 40 series over overlapping labels: shared job/env values, unique instance values.
        let specs: Vec<SeriesSpec> = (1..=40u64)
            .map(|i| {
                let key = format!("ts:{i}");
                let labels = vec![
                    ("job".to_string(), format!("job{}", i % 3)),
                    (
                        "env".to_string(),
                        if i % 2 == 0 { "prod" } else { "dev" }.to_string(),
                    ),
                    ("instance".to_string(), format!("host-{i}")),
                ];
                (i, key, labels)
            })
            .collect();

        let mut per_key = Postings::default();
        for (id, key, labels) in &specs {
            let labels: Vec<(&str, &str)> = labels
                .iter()
                .map(|(n, v)| (n.as_str(), v.as_str()))
                .collect();
            let series = make_series(*id, &labels);
            per_key.index_timeseries(&series, key.as_bytes());
        }

        let mut bulk = Postings::default();
        let entries: Vec<BulkIndexEntry> = specs
            .iter()
            .map(|(id, key, labels)| {
                let labels: Vec<(&str, &str)> = labels
                    .iter()
                    .map(|(n, v)| (n.as_str(), v.as_str()))
                    .collect();
                make_bulk_entry(*id, key, &labels)
            })
            .collect();
        bulk.bulk_index(entries);

        assert_postings_eq(&per_key, &bulk);
    }

    #[test]
    fn test_bulk_index_merges_into_non_empty_index() {
        // Pre-populate per-key (as in a degraded window: some keys per-key, some bulk).
        let mut postings = Postings::default();
        let series = make_series(1, &[("job", "web"), ("host", "a")]);
        postings.index_timeseries(&series, b"key1");

        postings.bulk_index(vec![
            make_bulk_entry(2, "key2", &[("job", "web"), ("host", "b")]),
            make_bulk_entry(3, "key3", &[("job", "batch"), ("host", "a")]),
        ]);

        let web = postings.postings_for_label_value("job", "web");
        assert!(web.contains(1) && web.contains(2) && !web.contains(3));
        let host_a = postings.postings_for_label_value("host", "a");
        assert!(host_a.contains(1) && host_a.contains(3));
        assert_eq!(postings.count(), 3);
        assert!(postings.all_postings.contains(2));
    }

    #[test]
    fn test_bulk_index_revokes_stale_marks() {
        let mut postings = Postings::default();
        let series = make_series(7, &[("job", "web")]);
        postings.index_timeseries(&series, b"key7");
        postings.mark_id_as_stale(7);
        assert!(postings.has_stale_ids());

        // Re-loading the id asserts it is alive again (same semantics as index_timeseries).
        postings.bulk_index(vec![make_bulk_entry(7, "key7", &[("job", "web")])]);

        assert!(!postings.stale_ids.contains(7));
        assert!(postings.all_postings.contains(7));
        assert!(postings.postings_for_label_value("job", "web").contains(7));
    }

    #[test]
    fn test_memory_postings_add_and_remove() {
        let mut postings = Postings::default();

        // Add postings
        postings.add_posting_for_label_value(1, "label1", "value1");
        postings.add_posting_for_label_value(1, "label2", "value2");
        postings.add_posting_for_label_value(2, "label1", "value1");

        // Check postings
        assert_eq!(
            postings
                .postings_for_label_value("label1", "value1")
                .cardinality(),
            2
        );
        assert_eq!(
            postings
                .postings_for_label_value("label2", "value2")
                .cardinality(),
            1
        );

        // Remove posting
        postings.remove_posting_for_label_value("label1", "value1", 1);
        assert_eq!(
            postings
                .postings_for_label_value("label1", "value1")
                .cardinality(),
            1
        );

        // Remove non-existent posting (should not panic)
        postings.remove_posting_for_label_value("label3", "value3", 3);
    }

    #[test]
    fn test_memory_postings_remove_timeseries() {
        let mut postings = Postings::default();
        let mut series = TimeSeries::new();
        series.id = 1;
        series.labels = MetricName::new(&[
            Label::new("label1", "value1"),
            Label::new("label2", "value2"),
        ]);

        postings.add_posting_for_label_value(1, "label1", "value1");
        postings.add_posting_for_label_value(1, "label2", "value2");

        postings.remove_timeseries(&series);

        assert!(
            postings
                .postings_for_label_value("label1", "value1")
                .is_empty()
        );
        assert!(
            postings
                .postings_for_label_value("label2", "value2")
                .is_empty()
        );
        assert!(postings.all_postings.is_empty());
    }
}
