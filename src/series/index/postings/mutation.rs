//! Write path: adding and removing series from the inverted index.
//!
//! Every mutation here maintains three pieces of state together — the `label=value` posting lists,
//! `all_postings`, and the id directory — so that a partially indexed series is never observable.

use super::{IndexKey, Postings, PostingsBitmap};
use crate::common::logging::log_warning;
use crate::labels::{InternedLabel, SeriesLabel};
use crate::series::index::asm::importing_slot;
use crate::series::{SeriesRef, TimeSeries};
use blart::map::Entry as ARTEntry;

/// A label holding a NUL is left out of the label index (`IndexKey::has_interior_nul`). No write
/// accepts one and a foreign load refuses it, but an own RDB or AOF from an older build may still
/// carry one: the series keeps the label and stays readable by key and by its other labels, and
/// only a filter on that label misses it (a filter holding a NUL is a parse error anyway).
/// Indexing it instead would either reach blart's `unwrap_unchecked` in `entry`/`insert`
/// (undefined behavior) or, once inserted first, make a later valid key clash and be dropped.
///
/// With such keys never stored, no valid key can clash, so `try_entry` failing is unreachable;
/// it is still handled rather than unwrapped, logged once and without the label's contents.
fn skip_prefix_clash() {
    static LOGGED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    if !LOGGED.swap(true, std::sync::atomic::Ordering::Relaxed) {
        log_warning("label index: skipped a key that clashes with another as a prefix");
    }
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

    #[cfg(test)]
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
        if key.has_interior_nul() {
            return false;
        }
        match self.label_index.try_entry(key) {
            Ok(ARTEntry::Occupied(mut entry)) => {
                entry.get_mut().add(ts_id);
                false
            }
            Ok(ARTEntry::Vacant(entry)) => {
                let mut bitmap = PostingsBitmap::new();
                bitmap.add(ts_id);
                entry.insert(bitmap);
                true
            }
            Err(_) => {
                skip_prefix_clash();
                false
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
            self.stale_ids.revoke(id);
        }
        // Before the directory entry changes: it says which slot the id is hidden under.
        self.forget_hidden(id);

        for InternedLabel { name, value } in ts.labels.iter() {
            self.add_posting_for_label_value_internal(id, name, value);
        }

        // Every path that indexes a series comes through here: this is what keeps an import in
        // flight out of reads, whichever path brought the key.
        match importing_slot(key) {
            Some(slot) => self.hidden_ids.hide(id, slot, &mut self.all_postings),
            None => self.all_postings.add(id),
        }
        self.set_timeseries_key(id, key);
    }

    /// Remove `series` only if the index maps its id to `key`.
    ///
    /// For callers that know which key is going away (the type's `unlink` callback, a
    /// RENAME/MOVE source). Removing by id alone trusted the id to be unique to one key, and
    /// a series restored from a `DUMP` of a live key used to carry the original's id — so
    /// deleting the copy stripped the original from the index. An id that is absent or owned
    /// by another key is left alone, quietly: `unlink` has usually retired it already when a
    /// RENAME/MOVE handler gets here.
    pub fn remove_timeseries_for_key(&mut self, series: &TimeSeries, key: &[u8]) -> bool {
        if self
            .id_to_key
            .get(&series.id)
            .is_none_or(|owner| owner.as_ref() != key)
        {
            return false;
        }
        self.remove_timeseries(series)
    }

    pub fn remove_timeseries(&mut self, series: &TimeSeries) -> bool {
        let id = series.id;
        let was_hidden = self.forget_hidden(id);
        if self.id_to_key.remove(&id).is_none() {
            log_warning(format!(
                "Tried to remove non-existing series id {id} from index"
            ));
        };
        let removed = self.all_postings.remove_checked(id) || was_hidden;
        for label in series.labels.iter() {
            self.remove_posting_for_label_value(label.name(), label.value(), id);
        }
        removed
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::make_series;
    use super::*;
    use crate::labels::{Label, MetricName};
    use crate::series::index::postings::Postings;

    /// A label holding a NUL is never indexed. `b` and `b\0c` give the keys `a=b\0` and
    /// `a=b\0c\0`, the first a prefix of the second, which blart's radix tree cannot hold. No
    /// write accepts such a label, but an own load keeps one an older build stored; indexing it
    /// would either reach undefined behavior or, once inserted first, block a later valid `a=b`.
    #[test]
    fn labels_with_nul_are_not_indexed_and_never_block_valid_ones() {
        for (legacy_name, legacy_value) in [("a", "b\0c"), ("a\0b", "v"), ("a", "\0")] {
            for legacy_first in [true, false] {
                let what = format!("{legacy_name:?}={legacy_value:?} legacy_first={legacy_first}");
                let mut postings = Postings::default();
                let add_legacy = |p: &mut Postings| {
                    assert!(
                        !p.add_posting_for_label_value(1, legacy_name, legacy_value),
                        "{what}"
                    );
                };
                if legacy_first {
                    add_legacy(&mut postings);
                }
                assert!(postings.add_posting_for_label_value(2, "a", "b"), "{what}");
                if !legacy_first {
                    add_legacy(&mut postings);
                }
                let key = IndexKey::for_label_value("a", "b");
                let valid = postings
                    .label_index
                    .get(&key)
                    .expect("valid posting indexed");
                assert!(valid.contains(2) && !valid.contains(1), "{what}");
                assert_eq!(postings.label_index.len(), 1, "{what}");
            }
        }
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
    #[test]
    fn test_remove_timeseries_for_key_leaves_another_keys_id_alone() {
        // A `RESTORE`d copy used to carry the original's id: unlinking the copy must not strip
        // the original, which the index has under a different key.
        let mut postings = Postings::default();
        let original = make_series(7, &[("name", "x")]);
        postings.index_timeseries(&original, b"a");

        assert!(!postings.remove_timeseries_for_key(&original, b"b"));
        assert_eq!(
            postings.get_key_by_id(7).map(|k| k.as_ref()),
            Some(&b"a"[..])
        );
        assert!(postings.postings_for_label_value("name", "x").contains(7));

        assert!(postings.remove_timeseries_for_key(&original, b"a"));
        assert!(postings.get_key_by_id(7).is_none());
        assert!(postings.postings_for_label_value("name", "x").is_empty());
        assert!(postings.all_postings.is_empty());
    }

    #[test]
    fn test_remove_timeseries_for_key_of_unindexed_id_is_a_noop() {
        let mut postings = Postings::default();
        let series = make_series(9, &[("name", "y")]);
        assert!(!postings.remove_timeseries_for_key(&series, b"k"));
        assert!(postings.all_postings.is_empty());
    }
}
