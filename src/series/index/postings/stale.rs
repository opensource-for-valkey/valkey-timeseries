//! Tombstoned series ids.
//!
//! When the index is found to disagree with the keyspace (an id resolves to no series), or when a
//! migrated slot's series must go, we cannot do a proper label-by-label removal — the series data
//! needed to find its posting lists is gone. Instead the id is recorded here, masked out of every
//! read, and physically drained from the posting lists later by a background pass.
//!
//! [`StaleSet`] exists so that masking cannot be forgotten. The raw bitmap is private to this
//! module; every consumer must go through [`StaleSet::mask`] or [`StaleSet::mask_cow`], both of
//! which are free when nothing is marked. Reads sourced from `all_postings` need no masking at
//! all — [`Postings::mark_ids_as_stale`] evicts from that set eagerly.

use super::{IndexKey, Postings, PostingsBitmap};
use crate::series::SeriesRef;
use std::borrow::Cow;
use std::ops::Bound;

/// The set of series ids that are marked for removal but still present in the label bitmaps.
#[derive(Clone, Debug, Default, PartialEq)]
pub(in crate::series::index) struct StaleSet {
    ids: PostingsBitmap,
    /// The ids a drain pass (see [`Postings::remove_stale_ids`]) may retire when it finishes:
    /// those marked when the pass started and not marked again since. `None` between passes.
    ///
    /// A pass walks the label index in batches, releasing the lock between them, so ids marked
    /// mid-pass are masked only in the keys it has yet to visit. Clearing the whole set at the
    /// end (as the drain used to) unmasked those ids in the keys it had already passed, and
    /// TS.CARD / TS.LABELSTATS / label values over-counted them until something marked them
    /// again. Only the snapshot is retired; later marks wait for the next pass.
    pass: Option<PostingsBitmap>,
}

impl StaleSet {
    #[inline]
    pub(in crate::series::index) fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    #[inline]
    pub(in crate::series::index) fn cardinality(&self) -> u64 {
        self.ids.cardinality()
    }

    /// Bytes the tombstone bitmap's roaring containers occupy.
    #[inline]
    pub(in crate::series::index) fn heap_size(&self) -> usize {
        crate::series::index::memory::bitmap_heap_size(&self.ids)
            + self
                .pass
                .as_ref()
                .map_or(0, crate::series::index::memory::bitmap_heap_size)
    }

    /// How many of `postings`' ids are stale. Cheaper than masking when only the count is wanted.
    #[inline]
    pub(in crate::series::index) fn stale_count_in(&self, postings: &PostingsBitmap) -> u64 {
        self.ids.and_cardinality(postings)
    }

    /// Drops every stale id from `postings`. No-op when nothing is marked.
    #[inline]
    pub(super) fn mask(&self, postings: &mut PostingsBitmap) {
        if !self.ids.is_empty() {
            postings.andnot_inplace(&self.ids);
        }
    }

    /// [`StaleSet::mask`] for a `Cow`: a borrowed input stays borrowed when nothing is marked,
    /// and an owned one is filtered in place rather than copied.
    #[inline]
    pub(in crate::series::index) fn mask_cow<'a>(
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

    #[cfg(test)]
    pub(in crate::series::index) fn contains(&self, id: SeriesRef) -> bool {
        self.ids.contains(id)
    }

    #[inline]
    fn mark_many(&mut self, ids: &[SeriesRef]) {
        self.ids.add_many(ids);
        // Marked (again) during a pass: its postings may sit in keys the pass already visited,
        // so this pass must not retire it.
        if let Some(pass) = self.pass.as_mut() {
            pass.remove_many(ids);
        }
    }

    /// Starts a drain pass: snapshot the ids it may retire.
    fn begin_pass(&mut self) {
        self.pass = Some(self.ids.clone());
    }

    /// Ends a drain pass that has visited every label key: retire the snapshot.
    fn finish_pass(&mut self) {
        if let Some(pass) = self.pass.take() {
            self.ids.andnot_inplace(&pass);
        }
    }

    /// Un-marks an id, asserting it is alive again.
    #[inline]
    pub(super) fn revoke(&mut self, id: SeriesRef) {
        self.ids.remove(id);
    }

    #[inline]
    pub(super) fn clear(&mut self) {
        self.ids.clear();
        self.pass = None;
    }
}

impl Postings {
    pub(crate) fn mark_id_as_stale(&mut self, id: SeriesRef) {
        self.mark_ids_as_stale(&[id]);
    }

    /// Marks ids as stale by adding its ID to the stale IDs set.
    /// ## Context
    /// This is used in the case of possible index sync issues. When the index is queried and an id is returned
    /// with no corresponding series, we have no access to the series data to do a proper cleanup. Also, at the end
    /// of cluster migration, the source node needs to remove all series ids corresponding to the migrated slots.
    ///
    /// We remove the key from the index and mark the ID as stale, which will be cleaned up later in a background task.
    /// The stale IDs are stored in a bitmap for efficient removal and are checked to ensure that no stale IDs are
    /// returned in queries until they are removed.
    pub(crate) fn mark_ids_as_stale(&mut self, ids: &[SeriesRef]) {
        for id in ids {
            let _ = self.id_to_key.remove(id);
        }
        self.stale_ids.mark_many(ids);
        self.all_postings.remove_many(ids);
    }

    #[cfg(test)]
    pub(super) fn has_stale_ids(&self) -> bool {
        !self.stale_ids.is_empty()
    }

    /// Removes stale series IDs from a subset of the index structures.
    ///
    /// This method processes at most `count` keys starting from `start_prefix`,
    /// removing stale IDs from their bitmaps and cleaning up empty entries.
    ///
    /// ## Arguments
    /// * `start_prefix` - The key to start processing from (inclusive)
    /// * `count` - Maximum number of keys to process in this batch
    ///
    /// ## Returns
    /// * `Option<IndexKey>` - The next key to continue processing from, or None if processing is complete
    ///
    pub(crate) fn remove_stale_ids(
        &mut self,
        start_prefix: Option<IndexKey>,
        count: usize,
    ) -> Option<IndexKey> {
        // Skip if there are no stale IDs to process
        if self.stale_ids.is_empty() {
            return None;
        }
        // A pass starts at the first key; a resumed batch keeps the pass's snapshot.
        if start_prefix.is_none() || self.stale_ids.pass.is_none() {
            self.stale_ids.begin_pass();
        }

        let mut keys_processed = 0;
        let mut keys_to_remove = Vec::new();
        let mut next_key = None;

        // Resume from the cursor key using an ordered range (NOT a prefix scan): a full key used
        // as a prefix only matches itself and its extensions, which would abandon the remaining
        // keys and prematurely clear `stale_ids`.
        let range = match start_prefix {
            Some(k) => (Bound::Included(k), Bound::Unbounded),
            None => (Bound::Unbounded, Bound::Unbounded),
        };

        let stale = &self.stale_ids;
        for (key, bitmap) in self.label_index.range_mut::<IndexKey, _>(range) {
            if keys_processed == count {
                // Save the first *unprocessed* key as the next starting point.
                next_key = Some(key.clone());
                break;
            }

            // Remove stale IDs from the bitmap
            let should_remove = if !bitmap.is_empty() {
                stale.mask(bitmap);
                bitmap.is_empty()
            } else {
                true
            };

            if should_remove {
                keys_to_remove.push(key.clone());
            }

            keys_processed += 1;
        }

        // Process empty keys
        for key in keys_to_remove {
            self.label_index.remove(&key);
        }

        // If we processed some keys but didn't reach the end, we can return the next key to continue from.
        // If we processed keys and there are no more keys to process, it means we've reached the end of the index,
        // so we can clear the stale IDs as they have been fully processed.
        if keys_processed > 0 && next_key.is_none() {
            self.stale_ids.finish_pass();
        }

        next_key
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_remove_stale_ids_across_multiple_batches() {
        // Regression test: draining stale ids must visit *every* label key, not just those sharing
        // a prefix with the resume cursor. Previously the resume used the cursor as a prefix, which
        // abandoned later keys and cleared `stale_ids` while their bitmaps were still dirty.
        let mut postings = Postings::default();
        let n: u64 = 250;
        for i in 0..n {
            postings.add_posting_for_label_value(i as SeriesRef, "host", &format!("h{i:04}"));
            postings.add_posting_for_label_value(i as SeriesRef, "job", "web");
        }

        // Mark the even ids stale.
        let stale: Vec<SeriesRef> = (0..n).step_by(2).map(|i| i as SeriesRef).collect();
        postings.mark_ids_as_stale(&stale);
        assert!(postings.has_stale_ids());

        // Drain in small batches, like the background maintenance task.
        let mut cursor = None;
        while let Some(next) = postings.remove_stale_ids(cursor.take(), 16) {
            cursor = Some(next);
        }

        assert!(
            !postings.has_stale_ids(),
            "stale ids should be fully drained"
        );

        // The shared `job=web` bitmap sorts after all the `host=*` keys, so with the old
        // prefix-based resume it was never cleaned. Inspect the raw bitmap directly (queries mask
        // stale ids regardless of whether the bitmap was physically cleaned).
        let key = IndexKey::for_label_value("job", "web");
        let bmp = postings
            .label_index
            .get::<IndexKey>(&key)
            .expect("job=web posting list should exist");
        for id in &stale {
            assert!(
                !bmp.contains(*id),
                "stale id {id} should have been removed from the bitmap"
            );
        }
        // Odd (non-stale) ids remain.
        assert!(bmp.contains(1));
        assert!(bmp.contains(3));
    }

    #[test]
    fn mask_cow_leaves_borrowed_untouched_when_nothing_is_marked() {
        let mut postings = Postings::default();
        postings.add_posting_for_label_value(1, "job", "web");
        postings.add_posting_for_label_value(2, "job", "web");

        let raw = postings
            .label_index
            .get::<IndexKey>(&IndexKey::for_label_value("job", "web"))
            .expect("posting list should exist");

        let out = postings.stale_ids.mask_cow(Cow::Borrowed(raw));
        assert!(matches!(out, Cow::Borrowed(_)), "no marks: stays borrowed");

        postings.mark_id_as_stale(1);
        let raw = postings
            .label_index
            .get::<IndexKey>(&IndexKey::for_label_value("job", "web"))
            .expect("posting list should exist");
        let out = postings.stale_ids.mask_cow(Cow::Borrowed(raw));
        assert!(matches!(out, Cow::Owned(_)), "marks present: must copy");
        assert!(!out.contains(1));
        assert!(out.contains(2));
    }

    #[test]
    fn test_ids_marked_mid_pass_survive_the_pass() {
        // An id marked while a pass is between batches was masked only in the keys still to
        // come; clearing every stale id at the end unmasked it in the keys already visited.
        let mut postings = Postings::default();
        for i in 0..40u64 {
            postings.add_posting_for_label_value(i as SeriesRef, "host", &format!("h{i:04}"));
            postings.add_posting_for_label_value(i as SeriesRef, "job", "web");
        }
        postings.mark_ids_as_stale(&[0]);

        // First batch covers the early `host=*` keys only.
        let cursor = postings
            .remove_stale_ids(None, 4)
            .expect("more keys to visit");
        // Id 1 goes stale now: its `host=h0001` key has already been visited.
        postings.mark_ids_as_stale(&[1]);

        let mut cursor = Some(cursor);
        while let Some(next) = postings.remove_stale_ids(cursor.take(), 4) {
            cursor = Some(next);
        }

        assert!(
            !postings.stale_ids.contains(0),
            "id 0 was drained everywhere"
        );
        assert!(
            postings.stale_ids.contains(1),
            "id 1 must stay marked until a pass has visited every key after its marking"
        );

        // The next full pass retires it.
        let mut cursor = None;
        while let Some(next) = postings.remove_stale_ids(cursor.take(), 4) {
            cursor = Some(next);
        }
        assert!(!postings.has_stale_ids());
        let host1 = IndexKey::for_label_value("host", "h0001");
        assert!(postings.label_index.get::<IndexKey>(&host1).is_none());
    }

    #[test]
    fn test_id_remarked_mid_pass_survives_the_pass() {
        let mut postings = Postings::default();
        for i in 0..40u64 {
            postings.add_posting_for_label_value(i as SeriesRef, "host", &format!("h{i:04}"));
        }
        postings.mark_ids_as_stale(&[2]);
        let cursor = postings
            .remove_stale_ids(None, 4)
            .expect("more keys to visit");
        // Re-indexed, then stale again, all within the pass.
        postings.stale_ids.revoke(2);
        postings.mark_ids_as_stale(&[2]);
        let mut cursor = Some(cursor);
        while let Some(next) = postings.remove_stale_ids(cursor.take(), 4) {
            cursor = Some(next);
        }
        assert!(postings.stale_ids.contains(2));
    }
}
