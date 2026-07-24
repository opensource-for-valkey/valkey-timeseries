//! Incremental compaction of the posting lists.
//!
//! Runs from a background task in cursor-bounded batches so a large index never blocks the server
//! for long: each pass run-optimizes and shrinks the bitmaps it visits and drops any that have
//! become empty, returning the key to resume from.

use super::{IndexKey, Postings, PostingsBitmap};
use std::ops::Bound;

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
        for (key, bitmap) in self.label_index.range_mut::<IndexKey, _>(range) {
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
