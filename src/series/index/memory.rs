//! Heap accounting for the label index.
//!
//! The index is module-global state, not a value hanging off any key, so it is invisible to
//! `MEMORY USAGE` and to `TS.INFO memoryUsage` — both of which only ever see one series. On a
//! high-cardinality keyspace the term dictionary and its posting bitmaps are frequently the
//! larger half of the module's footprint, so capacity planning from the per-key numbers alone
//! understates the module by an unbounded factor. [`index_memory_usage`] is what the module's
//! `INFO` section reports.
//!
//! Everything here is measured, not estimated, wherever the underlying structure can be asked:
//! the roaring bitmaps report their own container bytes, and byte buffers report their lengths.
//! The two structures that cannot be asked — the adaptive radix tree holding the term dictionary
//! and the `BTreeMap` holding the forward map — contribute their entries' bytes without their
//! internal node overhead, so the totals are a floor rather than an exact figure. `INFO` labels
//! them accordingly.

use super::index_key::IndexKey;
use super::postings::{KeyType, Postings, PostingsBitmap};
use super::{TIMESERIES_INDEX, TimeSeriesIndex};
use crate::series::SeriesRef;
use std::mem::size_of;

/// A breakdown of the label index's heap footprint, in bytes.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct IndexMemory {
    /// Databases holding an index.
    pub db_count: usize,
    /// Terms in the dictionary: one per `label` and per `label=value` pair.
    pub term_count: usize,
    /// Series in the forward map.
    pub series_count: usize,
    /// The term dictionary's keys.
    pub terms_bytes: usize,
    /// Roaring containers backing the per-term posting lists.
    pub postings_bytes: usize,
    /// The `SeriesRef -> key` forward map.
    pub id_to_key_bytes: usize,
    /// `all_postings` and the stale-id tombstone set.
    pub bookkeeping_bytes: usize,
}

impl IndexMemory {
    pub fn total_bytes(&self) -> usize {
        self.terms_bytes + self.postings_bytes + self.id_to_key_bytes + self.bookkeeping_bytes
    }

    fn merge(&mut self, other: Self) {
        self.db_count += other.db_count;
        self.term_count += other.term_count;
        self.series_count += other.series_count;
        self.terms_bytes += other.terms_bytes;
        self.postings_bytes += other.postings_bytes;
        self.id_to_key_bytes += other.id_to_key_bytes;
        self.bookkeeping_bytes += other.bookkeeping_bytes;
    }
}

/// Bytes the roaring containers of `bitmap` occupy.
///
/// `statistics()` walks the containers rather than the values, so this is proportional to
/// `cardinality / 65536`, not to the cardinality itself.
pub(super) fn bitmap_heap_size(bitmap: &PostingsBitmap) -> usize {
    let stats = bitmap.statistics();
    (stats.n_bytes_array_containers
        + stats.n_bytes_run_containers
        + stats.n_bytes_bitset_containers) as usize
}

impl Postings {
    /// This index body's heap footprint. Callers hold the postings lock.
    pub(crate) fn memory_usage(&self) -> IndexMemory {
        let mut memory = IndexMemory {
            db_count: 1,
            series_count: self.id_to_key.len(),
            ..Default::default()
        };

        for (key, ids) in self.label_index.iter() {
            memory.term_count += 1;
            // `+ 1`: `IndexKey::len` excludes the NUL sentinel the radix tree needs.
            memory.terms_bytes += size_of::<IndexKey>() + key.len() + 1;
            memory.postings_bytes += bitmap_heap_size(ids);
        }

        for key in self.id_to_key.values() {
            memory.id_to_key_bytes += size_of::<SeriesRef>() + size_of::<KeyType>() + key.len();
        }

        memory.bookkeeping_bytes =
            bitmap_heap_size(&self.all_postings) + self.stale_ids.heap_size();

        memory
    }
}

impl TimeSeriesIndex {
    /// This database's index footprint, taken under the postings read lock.
    pub fn memory_usage(&self) -> IndexMemory {
        let mut state = ();
        self.with_postings(&mut state, |postings, _| postings.memory_usage())
    }
}

/// The footprint of every database's index, summed.
pub fn index_memory_usage() -> IndexMemory {
    let mut total = IndexMemory::default();
    for index in TIMESERIES_INDEX.pin().values() {
        total.merge(index.memory_usage());
    }
    total
}
