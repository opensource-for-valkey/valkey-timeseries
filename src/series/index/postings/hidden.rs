//! Series hidden from reads because their slot is still being imported.
//!
//! An atomic slot migration streams series into this node before the node owns their slot, and
//! the import can still abort. Those series are indexed as they arrive, on every path that indexes
//! anything, but stay invisible until the import completes: [`Postings::index_timeseries`] asks
//! `asm::importing_slot` and records the id here instead of in `all_postings`. Every read then
//! treats the id as absent, while the directory and the label bitmaps carry it as usual, so the
//! `unlink` callback, `TS.ALTER` and `RENAME` maintain a hidden series like any other.
//!
//! Completion is one set operation per slot, under the write lock on the main thread: the ids move
//! to `all_postings` and the slot's series become visible together with the ownership change.
//!
//! Invariants: an id is either in `all_postings` or here, never both; every id here has a
//! directory entry, and sits under the slot of that entry's key.
//!
//! Ids are grouped by slot, not by import job: a slot belongs to at most one import at a time, and
//! the bitmaps live and die with the [`Postings`] that holds them, which a `FLUSHDB` drops whole.

use super::{Postings, PostingsBitmap};
use crate::fanout::calculate_hash_slot;
use crate::series::SeriesRef;
use range_set_blaze::RangeSetBlaze;
use std::borrow::Cow;
use std::collections::BTreeMap;

/// The ids hidden from reads, with the union kept beside the per-slot bitmaps so that masking a
/// read is one bitmap operation, and free when no import is in flight.
#[derive(Clone, Debug, Default, PartialEq)]
pub(in crate::series::index) struct HiddenSet {
    ids: PostingsBitmap,
    by_slot: BTreeMap<u16, PostingsBitmap>,
}

impl HiddenSet {
    #[inline]
    pub(super) fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    #[inline]
    pub(super) fn cardinality(&self) -> u64 {
        self.ids.cardinality()
    }

    #[inline]
    pub(super) fn contains(&self, id: SeriesRef) -> bool {
        self.ids.contains(id)
    }

    /// How many of `postings`' ids are hidden.
    #[inline]
    pub(super) fn count_in(&self, postings: &PostingsBitmap) -> u64 {
        self.ids.and_cardinality(postings)
    }

    /// Drops every hidden id from `postings`. No-op when nothing is hidden.
    #[inline]
    pub(super) fn mask(&self, postings: &mut PostingsBitmap) {
        if !self.ids.is_empty() {
            postings.andnot_inplace(&self.ids);
        }
    }

    /// [`HiddenSet::mask`] for a `Cow`: a borrowed input stays borrowed when nothing is hidden.
    #[inline]
    pub(super) fn mask_cow<'a>(
        &self,
        postings: Cow<'a, PostingsBitmap>,
    ) -> Cow<'a, PostingsBitmap> {
        if self.ids.is_empty() {
            return postings;
        }
        match postings {
            Cow::Borrowed(bmp) => Cow::Owned(bmp.andnot(&self.ids)),
            Cow::Owned(mut bmp) => {
                bmp.andnot_inplace(&self.ids);
                Cow::Owned(bmp)
            }
        }
    }

    /// `all` together with the hidden ids: every id in the directory.
    pub(super) fn union_with<'a>(&self, all: &'a PostingsBitmap) -> Cow<'a, PostingsBitmap> {
        if self.ids.is_empty() {
            Cow::Borrowed(all)
        } else {
            Cow::Owned(all.or(&self.ids))
        }
    }

    /// Bytes the bitmaps' roaring containers occupy.
    pub(in crate::series::index) fn heap_size(&self) -> usize {
        crate::series::index::memory::bitmap_heap_size(&self.ids)
            + self
                .by_slot
                .values()
                .map(crate::series::index::memory::bitmap_heap_size)
                .sum::<usize>()
    }

    /// Hides `id`, evicting it from `visible` (`all_postings`), which is passed in so that no
    /// caller can leave an id in both sets.
    pub(super) fn hide(&mut self, id: SeriesRef, slot: u16, visible: &mut PostingsBitmap) {
        visible.remove(id);
        self.ids.add(id);
        self.by_slot.entry(slot).or_default().add(id);
    }

    fn reveal(&mut self, id: SeriesRef, slot: u16) {
        self.ids.remove(id);
        if let Some(ids) = self.by_slot.get_mut(&slot) {
            ids.remove(id);
            if ids.is_empty() {
                self.by_slot.remove(&slot);
            }
        }
    }

    /// [`HiddenSet::reveal`] for an id whose slot cannot be known, so every bucket is searched.
    fn reveal_from_any_slot(&mut self, id: SeriesRef) {
        self.ids.remove(id);
        self.by_slot.retain(|_, ids| {
            ids.remove(id);
            !ids.is_empty()
        });
    }

    /// Removes and returns every id hidden under one of `slots`.
    fn take_slots(&mut self, slots: &RangeSetBlaze<u16>) -> PostingsBitmap {
        let mut taken = PostingsBitmap::new();
        self.by_slot.retain(|slot, ids| {
            if slots.contains(*slot) {
                taken |= &*ids;
                false
            } else {
                true
            }
        });
        self.ids.andnot_inplace(&taken);
        taken
    }

    #[inline]
    pub(super) fn clear(&mut self) {
        self.ids.clear();
        self.by_slot.clear();
    }
}

impl Postings {
    /// Hides the series already indexed under a key in `slots`. For an import this node learns
    /// about only after indexing its keys: one restored from a snapshot fires no start event.
    /// Returns how many series were newly hidden.
    pub(in crate::series::index) fn hide_indexed_keys_in_slots(
        &mut self,
        slots: &RangeSetBlaze<u16>,
    ) -> u64 {
        let mut hidden = 0;
        for (&id, key) in &self.id_to_key {
            let slot = calculate_hash_slot(key);
            if slots.contains(slot) && !self.hidden_ids.contains(id) {
                self.hidden_ids.hide(id, slot, &mut self.all_postings);
                hidden += 1;
            }
        }
        hidden
    }

    /// The import of `slots` ended: their hidden series become visible. Returns how many. Called
    /// on completion and on abort, where only series whose key survived are left.
    pub(in crate::series::index) fn publish_hidden_in_slots(
        &mut self,
        slots: &RangeSetBlaze<u16>,
    ) -> u64 {
        if self.hidden_ids.is_empty() {
            return 0;
        }
        let ids = self.hidden_ids.take_slots(slots);
        self.all_postings |= &ids;
        ids.cardinality()
    }

    /// How many of `postings`' ids a read may see: neither tombstoned nor hidden. For the readers
    /// that walk the term dictionary directly rather than through `Terms`.
    pub(in crate::series::index) fn visible_cardinality(&self, postings: &PostingsBitmap) -> u64 {
        if self.stale_ids.is_empty() && self.hidden_ids.is_empty() {
            return postings.cardinality();
        }
        postings
            .cardinality()
            .saturating_sub(self.stale_ids.stale_count_in(postings))
            .saturating_sub(self.hidden_ids.count_in(postings))
    }

    /// Stops hiding `id`. Returns whether it was hidden. Callers run this before removing the
    /// directory entry, which names the slot; without one the id is still cleared, by searching.
    pub(super) fn forget_hidden(&mut self, id: SeriesRef) -> bool {
        if !self.hidden_ids.contains(id) {
            return false;
        }
        match self.id_to_key.get(&id) {
            Some(key) => self.hidden_ids.reveal(id, calculate_hash_slot(key)),
            None => self.hidden_ids.reveal_from_any_slot(id),
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::super::serialization::{deserialize_standalone, serialize};
    use super::super::test_support::{assert_postings_eq, make_series};
    use super::*;
    use crate::labels::Label;
    use crate::series::TimeSeries;

    const IMPORTED: u64 = 1;
    const LOCAL: u64 = 2;
    const IMPORTED_KEY: &[u8] = b"{imported}a";
    const LOCAL_KEY: &[u8] = b"{local}b";

    fn imported_series() -> TimeSeries {
        make_series(
            IMPORTED,
            &[("job", "imported"), ("env", "prod"), ("only_imported", "x")],
        )
    }

    fn fixture() -> Postings {
        let mut postings = Postings::default();
        postings.index_timeseries(&imported_series(), IMPORTED_KEY);
        postings.index_timeseries(
            &make_series(LOCAL, &[("job", "local"), ("env", "prod")]),
            LOCAL_KEY,
        );
        postings
    }

    fn importing() -> RangeSetBlaze<u16> {
        RangeSetBlaze::from_iter([calculate_hash_slot(IMPORTED_KEY)])
    }

    fn hidden_fixture() -> Postings {
        let mut postings = fixture();
        assert_eq!(postings.hide_indexed_keys_in_slots(&importing()), 1);
        postings
    }

    fn ids(bitmap: &PostingsBitmap) -> Vec<u64> {
        bitmap.iter().collect()
    }

    #[test]
    fn hidden_series_match_no_label_lookup() {
        let postings = hidden_fixture();
        let terms = postings.terms();
        assert!(terms.postings_for_label_value("job", "imported").is_empty());
        assert_eq!(ids(&terms.postings_for_label_value("env", "prod")), [LOCAL]);
        assert_eq!(ids(&terms.postings_for_all_label_values("env")), [LOCAL]);
        assert_eq!(ids(&terms.postings_by_prefix("env", "pr")), [LOCAL]);
        assert_eq!(
            ids(&terms.postings_by_labels(&[Label::new("env", "prod")])),
            [LOCAL]
        );
    }

    #[test]
    fn subtractive_reads_do_not_see_hidden_series() {
        let postings = hidden_fixture();
        assert_eq!(ids(&postings.all_postings), [LOCAL]);
        assert_eq!(
            ids(&postings.terms().postings_without_label("only_imported")),
            [LOCAL]
        );
    }

    #[test]
    fn hidden_series_do_not_count() {
        let postings = hidden_fixture();
        assert_eq!(postings.count(), 1);
        let prod = postings.label_index.get(&b"env=prod\0"[..]).unwrap();
        assert_eq!(prod.cardinality(), 2, "the bitmap itself still carries it");
        assert_eq!(postings.visible_cardinality(prod), 1);
    }

    #[test]
    fn a_label_only_hidden_series_carry_is_not_listed() {
        let postings = hidden_fixture();
        assert!(!postings.get_label_names().contains("only_imported"));
        assert!(postings.get_label_names().contains("env"));
        assert!(postings.get_label_values("only_imported").is_empty());
        assert_eq!(postings.get_label_values("job"), ["local"]);
    }

    #[test]
    fn tombstoned_series_are_masked_with_nothing_hidden() {
        let mut postings = fixture();
        postings.mark_id_as_stale(IMPORTED);
        assert!(postings.hidden_ids.is_empty());
        assert!(!postings.get_label_names().contains("only_imported"));
        assert_eq!(postings.get_label_values("job"), ["local"]);
        let prod = postings.label_index.get(&b"env=prod\0"[..]).unwrap();
        assert_eq!(prod.cardinality(), 2, "the bitmap itself still carries it");
        assert_eq!(postings.visible_cardinality(prod), 1);
    }

    #[test]
    fn nothing_masked_counts_every_id() {
        let postings = fixture();
        let prod = postings.label_index.get(&b"env=prod\0"[..]).unwrap();
        assert_eq!(postings.visible_cardinality(prod), 2);
        assert!(postings.get_label_names().contains("only_imported"));
    }

    #[test]
    fn a_hidden_series_is_not_a_duplicate_of_a_new_one() {
        let postings = hidden_fixture();
        let unique = [Label::new("only_imported", "x")];
        assert!(postings.postings_by_labels(&unique).is_empty());
        let shared = [Label::new("job", "imported"), Label::new("env", "prod")];
        assert!(postings.postings_by_labels(&shared).is_empty());
    }

    #[test]
    fn the_directory_keeps_a_hidden_series() {
        // The id allocator and the key-checked removal both rely on this.
        let postings = hidden_fixture();
        assert!(postings.has_id(IMPORTED));
        assert_eq!(
            postings.get_key_by_id(IMPORTED).unwrap().as_ref(),
            IMPORTED_KEY
        );
    }

    #[test]
    fn publishing_leaves_the_index_as_if_it_had_never_been_hidden() {
        let mut postings = hidden_fixture();
        assert_eq!(postings.publish_hidden_in_slots(&importing()), 1);
        assert_postings_eq(&postings, &fixture());
        assert_eq!(postings.hidden_ids, HiddenSet::default());
        assert_eq!(postings.count(), 2);
    }

    #[test]
    fn publishing_releases_only_the_slots_it_names() {
        let mut postings = hidden_fixture();
        let other = RangeSetBlaze::from_iter([calculate_hash_slot(LOCAL_KEY)]);
        assert_eq!(postings.publish_hidden_in_slots(&other), 0);
        assert!(
            postings
                .terms()
                .postings_for_label_value("job", "imported")
                .is_empty()
        );
    }

    #[test]
    fn deleting_a_hidden_series_removes_it_for_good() {
        let mut postings = hidden_fixture();
        assert!(
            postings.remove_timeseries_for_key(&imported_series(), IMPORTED_KEY),
            "the unlink callback warns about a series it finds was not indexed"
        );
        assert!(!postings.has_id(IMPORTED));
        assert_eq!(postings.hidden_ids, HiddenSet::default());
        assert_eq!(postings.publish_hidden_in_slots(&importing()), 0);
        assert_eq!(ids(&postings.all_postings), [LOCAL]);
        assert!(!postings.get_label_names().contains("only_imported"));
    }

    #[test]
    fn forgetting_a_hidden_id_with_no_directory_entry_still_clears_it() {
        // Breaks the invariant on purpose: with no key there is no slot to look under.
        let mut postings = hidden_fixture();
        postings.id_to_key.remove(&IMPORTED);

        assert!(postings.forget_hidden(IMPORTED));

        assert_eq!(postings.hidden_ids, HiddenSet::default());
        assert!(!postings.forget_hidden(IMPORTED), "already gone");
    }

    #[test]
    fn forgetting_without_a_directory_entry_keeps_other_hidden_series() {
        let mut postings = hidden_fixture();
        let other = make_series(3, &[("job", "other")]);
        postings.index_timeseries(&other, b"{imported}b");
        postings.hide_indexed_keys_in_slots(&importing());
        postings.id_to_key.remove(&IMPORTED);

        assert!(postings.forget_hidden(IMPORTED));

        assert!(!postings.hidden_ids.contains(IMPORTED));
        assert!(postings.hidden_ids.contains(3));
        assert_eq!(postings.publish_hidden_in_slots(&importing()), 1);
        assert_eq!(postings.hidden_ids, HiddenSet::default());
    }

    #[test]
    fn a_stale_marked_hidden_series_stays_out_after_publishing() {
        let mut postings = hidden_fixture();
        postings.mark_id_as_stale(IMPORTED);
        assert_eq!(postings.hidden_ids, HiddenSet::default());
        assert_eq!(postings.publish_hidden_in_slots(&importing()), 0);
        assert!(!postings.all_postings.contains(IMPORTED));
    }

    #[test]
    fn a_snapshot_persists_hidden_series_as_ordinary_ones() {
        // The keys are in the snapshot too, so index and keyspace agree on load; the load-end
        // reconciliation hides them again when the import is still in progress.
        let postings = hidden_fixture();
        let mut buf = Vec::new();
        serialize(&mut buf, &postings).unwrap();
        let loaded =
            deserialize_standalone(&mut buf.as_slice()).expect("the body is self-consistent");
        assert_postings_eq(&loaded, &fixture());
        assert_eq!(loaded.hidden_ids, HiddenSet::default());
    }

    #[test]
    fn swapping_moves_hidden_series_with_their_index() {
        let mut first = hidden_fixture();
        let mut second = Postings::default();
        first.swap(&mut second);
        assert!(first.hidden_ids.is_empty());
        assert!(second.hidden_ids.contains(IMPORTED));
    }
}
