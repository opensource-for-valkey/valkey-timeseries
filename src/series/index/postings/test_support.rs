//! Shared fixtures for the `postings` submodule tests.

use super::{BulkIndexEntry, IndexKey, Postings};
use crate::labels::{Label, MetricName};
use crate::series::{SeriesRef, TimeSeries};

pub(super) fn make_series(id: SeriesRef, labels: &[(&str, &str)]) -> TimeSeries {
    let mut series = TimeSeries::new();
    series.id = id;
    let labels: Vec<Label> = labels
        .iter()
        .map(|(name, value)| Label::new(*name, *value))
        .collect();
    series.labels = MetricName::new(&labels);
    series
}

pub(super) fn make_bulk_entry(id: SeriesRef, key: &str, labels: &[(&str, &str)]) -> BulkIndexEntry {
    BulkIndexEntry {
        id,
        key: key.as_bytes().to_vec().into_boxed_slice(),
        label_keys: labels
            .iter()
            .map(|(name, value)| IndexKey::for_label_value(name, value))
            .collect(),
    }
}

/// Asserts the two indexes are structurally identical (same label keys, same posting-list
/// contents, same id map, same all_postings).
pub(super) fn assert_postings_eq(a: &Postings, b: &Postings) {
    assert_eq!(a.id_to_key, b.id_to_key);
    assert_eq!(a.all_postings, b.all_postings, "all_postings differ");
    let a_entries: Vec<_> = a.label_index.iter().collect();
    let b_entries: Vec<_> = b.label_index.iter().collect();
    assert_eq!(a_entries.len(), b_entries.len(), "label key counts differ");
    for ((ka, va), (kb, vb)) in a_entries.iter().zip(b_entries.iter()) {
        assert_eq!(ka.as_str(), kb.as_str());
        assert_eq!(va, vb, "posting list for {} differs", ka.as_str());
    }
}
