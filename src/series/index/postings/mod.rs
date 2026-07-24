//! The core in-memory inverted index for time series data.
//!
//! This module owns the index state ([`Postings`]); each submodule implements one concern
//! against it:
//!
//! * [`mutation`] — indexing and un-indexing series, one at a time or in bulk.
//! * [`directory`] — the `SeriesRef -> key` forward map.
//! * [`terms`] — term-dictionary lookups: label key/prefix scans down to posting bitmaps.
//! * [`predicate`] — translating a single `LabelFilter` into a bitmap.
//! * [`planner`] — boolean set algebra assembling filters and selectors into one bitmap.
//! * [`stale`] — tombstoned ids: marking, masking them out of reads, incremental draining.
//! * [`maintenance`] — incremental bitmap compaction.
//! * [`serialization`] — the wire format for one index body, container-agnostic.
//!
//! Consumers above this layer go through `TimeSeriesIndex` (locking) or `querier`/`label_querier`
//! (ACL, date ranges, ranking). Only the entry points those need are exported past this module;
//! the lookup and planning primitives are deliberately confined to `postings`.

mod directory;
mod maintenance;
mod mutation;
mod planner;
mod predicate;
pub(in crate::series::index) mod serialization;
mod stale;
mod terms;

#[cfg(test)]
mod test_support;

pub(crate) use mutation::BulkIndexEntry;
pub(in crate::series::index) use stale::StaleSet;

use super::index_key::IndexKey;
use crate::series::SeriesRef;
use blart::TreeMap;
use croaring::Bitmap64;
use std::collections::BTreeMap;
use std::sync::LazyLock;

pub(super) static EMPTY_BITMAP: LazyLock<PostingsBitmap> = LazyLock::new(PostingsBitmap::new);

pub type PostingsBitmap = Bitmap64;
// label
// label=value
pub type PostingsIndex = TreeMap<IndexKey, PostingsBitmap>;

/// Type for the key of the index.
pub type KeyType = Box<[u8]>;

/// `Postings` is the core in-memory inverted index for time series data. It is designed for efficient
/// querying and retrieving of time series based on their labels.
#[derive(Clone)]
pub struct Postings {
    /// Map from label name and (label name, label value) to a set of timeseries ids.
    pub(super) label_index: PostingsIndex,
    /// Map from timeseries id to the key of the timeseries.
    pub(in crate::series) id_to_key: BTreeMap<SeriesRef, KeyType>,
    /// Set of timeseries ids of series that should be removed from the index. This really only
    /// happens when the index is inconsistent (value does not exist in the db but exists in the index)
    /// Keep track and cleanup from the index during a gc pass.
    pub(super) stale_ids: StaleSet,
    /// Set of all timeseries ids in the index. This is used to optimize queries that are subtractive.
    ///
    /// Invariant: this set never contains stale ids — [`Postings::mark_ids_as_stale`] removes them
    /// here as it marks them. Read paths may therefore use it without masking.
    pub(crate) all_postings: PostingsBitmap,
}

impl Default for Postings {
    fn default() -> Self {
        Postings {
            label_index: PostingsIndex::new(),
            id_to_key: BTreeMap::new(),
            stale_ids: StaleSet::default(),
            all_postings: PostingsBitmap::default(),
        }
    }
}

impl Postings {
    #[allow(dead_code)]
    pub(super) fn clear(&mut self) {
        self.label_index.clear();
        self.id_to_key.clear();
        self.stale_ids.clear();
        self.all_postings.clear();
    }

    /// `swap` the inner value with some other value
    /// this is specifically to handle the `swapdb` event callback
    pub fn swap(&mut self, other: &mut Self) {
        std::mem::swap(&mut self.label_index, &mut other.label_index);
        std::mem::swap(&mut self.id_to_key, &mut other.id_to_key);
        std::mem::swap(&mut self.stale_ids, &mut other.stale_ids);
        std::mem::swap(&mut self.all_postings, &mut other.all_postings);
    }
}
