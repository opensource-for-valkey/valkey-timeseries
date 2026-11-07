//! Incremental compaction of the posting lists.
//!
//! Runs from a background task in cursor-bounded batches so a large index never blocks the server
//! for long: each pass run-optimizes and shrinks the bitmaps it visits and drops any that have
//! become empty, returning the key to resume from.

use super::{IndexKey, Postings, PostingsBitmap};
use std::ops::Bound;
use std::time::{Duration, Instant};

impl Postings {
    /// Incrementally optimizes posting bitmaps for better memory usage and performance.
    ///
    /// This method processes at most `count` keys starting from `start_prefix`,
    /// performing the following optimizations on each bitmap:
    /// 1. Remove the bitmap if it is empty
    /// 2. Call run_optimize() to optimize the bitmap's internal structure
    /// 3. Call shrink_to_fit() to reduce memory overhead
    ///
    /// ### Arguments
    /// * `start_prefix` - The key to start processing from (inclusive)
    /// * `count` - Maximum number of keys to process in this batch
    ///
    /// ### Returns
    /// * `Option<IndexKey>` - The next key to continue processing from, or None if processing is complete
    ///
    pub(crate) fn optimize_postings(
        &mut self,
        start_prefix: Option<IndexKey>,
        count: usize,
    ) -> Option<IndexKey> {
        self.optimize_postings_within(start_prefix, count, None)
    }

    /// [`Postings::optimize_postings`] that also stops once `budget` has elapsed, whatever the
    /// lists cost: that varies with a list's containers, not its presence, and a label's values
    /// sort next to each other, so a low-cardinality label's expensive lists come in a row. Empty
    /// lists count against it too. At least one list is always read, so a pass advances even when
    /// the budget is spent at once; a hold can therefore overrun by the one list in progress.
    pub(crate) fn optimize_postings_within(
        &mut self,
        start_prefix: Option<IndexKey>,
        count: usize,
        budget: Option<Duration>,
    ) -> Option<IndexKey> {
        let deadline = budget.map(|budget| Instant::now() + budget);
        let mut next_key = None;

        if start_prefix.is_none() {
            optimize_bitmap(&mut self.all_postings);
        }

        let mut keys_to_delete = Vec::new();
        let mut keys_processed: usize = 0;
        // Resume from the cursor key using an ordered range rather than a prefix scan (a full key
        // as a prefix only matches itself and its extensions).
        let range = match start_prefix {
            Some(k) => (Bound::Included(k), Bound::Unbounded),
            None => (Bound::Unbounded, Bound::Unbounded),
        };

        // Collect keys to process
        for (keys_read, (key, bitmap)) in
            self.label_index.range_mut::<IndexKey, _>(range).enumerate()
        {
            if keys_read > 0 && deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                next_key = Some(key.clone());
                break;
            }

            if bitmap.is_empty() {
                keys_to_delete.push(key.clone());
                continue;
            }
            if keys_processed == count {
                // Save the key we stopped at as the next starting point
                next_key = Some(key.clone());
                break;
            }

            optimize_bitmap(bitmap);

            keys_processed += 1;
        }

        // Remove empty bitmaps collected earlier
        for key in keys_to_delete {
            self.label_index.remove(&key);
        }

        next_key
    }
}

/// Optimizes a bitmap in place for better memory usage and performance.
/// This applies run_optimize() and shrink_to_fit() operations to the bitmap
/// if it exists in the index.
fn optimize_bitmap(bitmap: &mut PostingsBitmap) {
    // Optimize the bitmap's internal structure
    bitmap.run_optimize();

    // Shrink to fit to reduce memory overhead
    bitmap.shrink_to_fit();
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `count` lists holding one id each, named `k=000`, `k=001`, and so on.
    fn postings_with_lists(count: u64) -> Postings {
        let mut postings = Postings::default();
        for id in 0..count {
            postings.add_posting_for_label_value(id + 1, "k", &format!("{id:03}"));
        }
        postings
    }

    fn clear_list(postings: &mut Postings, value: &str) {
        let key = IndexKey::for_label_value("k", value);
        postings.label_index.get_mut(&key).unwrap().clear();
    }

    #[test]
    fn a_spent_budget_ends_the_hold_after_one_list() {
        let mut postings = postings_with_lists(3);
        let next = postings.optimize_postings_within(None, 100, Some(Duration::ZERO));
        assert_eq!(next, Some(IndexKey::for_label_value("k", "001")));
    }

    #[test]
    fn a_spent_budget_ends_a_run_of_empty_lists_too() {
        let mut postings = postings_with_lists(4);
        for value in ["000", "001", "002"] {
            clear_list(&mut postings, value);
        }
        let next = postings.optimize_postings_within(None, 100, Some(Duration::ZERO));
        assert_eq!(next, Some(IndexKey::for_label_value("k", "001")));
        assert_eq!(postings.label_index.len(), 3, "the one it read is removed");
    }

    #[test]
    fn a_spent_budget_still_makes_progress_to_the_end() {
        let mut postings = postings_with_lists(5);
        clear_list(&mut postings, "002");
        let (mut cursor, mut holds) = (None, 0);
        loop {
            holds += 1;
            cursor = postings.optimize_postings_within(cursor, 100, Some(Duration::ZERO));
            if cursor.is_none() {
                break;
            }
            assert!(holds < 10, "the pass is not advancing");
        }
        assert_eq!(holds, 5);
        assert_eq!(postings.label_index.len(), 4);
    }

    #[test]
    fn the_list_cap_still_applies_with_budget_to_spare() {
        let mut postings = postings_with_lists(3);
        let next = postings.optimize_postings_within(None, 2, Some(Duration::from_secs(3600)));
        assert_eq!(next, Some(IndexKey::for_label_value("k", "002")));
    }
}
