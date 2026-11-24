//! Heap accounting for the label index.
//!
//! The index is module-global state, not a value hanging off any key, so it is invisible to
//! `MEMORY USAGE` and to `TS.INFO memoryUsage` — both of which only ever see one series. On a
//! high-cardinality keyspace the term dictionary and its posting bitmaps are frequently the
//! larger half of the module's footprint, so capacity planning from the per-key numbers alone
//! understates the module by an unbounded factor. [`index_memory_usage`] is what the module's
//! `INFO` section reports.
//!
//! Byte buffers report their lengths. The roaring bitmaps report their container payload, and
//! [`bitmap_heap_size`] adds the structure around it from CRoaring's layout: per container it
//! costs more than the payload on a sparse posting list. The two structures that cannot be
//! asked — the adaptive radix tree holding the term dictionary and the `BTreeMap` holding the
//! forward map — contribute their entries' bytes without their internal node overhead, so the
//! totals are a floor rather than an exact figure. `INFO` labels them accordingly.

use super::index_key::IndexKey;
use super::postings::{KeyType, Postings, PostingsBitmap};
use super::{TIMESERIES_INDEX, TimeSeriesIndex};
use crate::series::SeriesRef;
use std::mem::size_of;
use std::ops::Bound;
use std::time::{Duration, Instant};

/// A breakdown of the label index's heap footprint, in bytes.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct IndexMemory {
    /// Databases holding an index.
    pub db_count: usize,
    /// Terms in the dictionary: one per `label=value` pair.
    pub term_count: usize,
    /// Series in the forward map.
    pub series_count: usize,
    /// The term dictionary's keys.
    pub terms_bytes: usize,
    /// The per-term posting bitmaps: container payload plus the bitmap structure around it.
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

    pub(crate) fn merge(&mut self, other: Self) {
        self.db_count += other.db_count;
        self.term_count += other.term_count;
        self.series_count += other.series_count;
        self.terms_bytes += other.terms_bytes;
        self.postings_bytes += other.postings_bytes;
        self.id_to_key_bytes += other.id_to_key_bytes;
        self.bookkeeping_bytes += other.bookkeeping_bytes;
    }
}

/// Bytes `bitmap` occupies: its containers' payload plus the structure CRoaring keeps around
/// them (see [`roaring64_structure_bytes`]).
///
/// `statistics()` walks the containers rather than the values, so this is proportional to
/// `cardinality / 65536`, not to the cardinality itself.
pub(super) fn bitmap_heap_size(bitmap: &PostingsBitmap) -> usize {
    let stats = bitmap.statistics();
    let payload = stats.n_bytes_array_containers
        + stats.n_bytes_run_containers
        + stats.n_bytes_bitset_containers;
    payload as usize + roaring64_structure_bytes(stats.n_containers as usize)
}

// CRoaring's 64-bit bitmap (croaring-sys 5.5) keeps the high 48 bits of each value in an
// adaptive radix tree whose leaves name a container. Every allocation goes through croaring's
// allocator hook (`roaring_alloc.rs`), which prefixes it with its size.
const ROARING_ALLOC_HEADER: usize = 8;
/// `roaring64_bitmap_t`, allocated even when empty.
const ROARING_BITMAP_BYTES: usize = 224 + ROARING_ALLOC_HEADER;
/// The container struct, plus the header of its separately allocated payload.
const ROARING_CONTAINER_BYTES: usize = 16 + 2 * ROARING_ALLOC_HEADER;
/// One ART leaf per container, in a pool (as are its `containers[]` slot and typecode).
const ROARING_LEAF_BYTES: usize = 16;
/// ART inner nodes: (most children, bytes). A pool is allocated for each type the tree has
/// grown through, and an outgrown node's pool is kept.
const ROARING_NODES: [(usize, usize); 4] = [(4, 48), (16, 152), (48, 656), (256, 2056)];

/// Capacity CRoaring grows a pool to for `n` entries: 2, doubling below 1024, then by a quarter.
fn roaring_pool_capacity(n: usize) -> usize {
    let mut capacity = 2;
    while capacity < n {
        capacity = if capacity < 1024 {
            capacity * 2
        } else {
            capacity * 5 / 4
        };
    }
    capacity
}

/// Heap bytes of a 64-bit roaring bitmap's structure, excluding container payload, for one
/// holding `containers` containers.
///
/// Counted from CRoaring's growth rules. Against an allocator that counts bytes it is exact
/// for array containers in trees of up to 256 containers whose ids share their high bytes (one
/// inner level); a bitset container's aligned payload costs 24 bytes more. Not counted: the
/// allocator's size classes (about a fifth more on a sparse index), slots freed by removals and
/// not yet reused, array capacity beyond the cardinality (`shrink_to_fit` in the optimizer trims
/// it), and the extra levels that ids of several epochs add.
///
/// Pools are counted at the capacity growth leaves them. The optimizer's `shrink_to_fit` trims
/// them to size, so a list it has just visited reads high (by up to ~40 % on the sparse shapes
/// measured). A list removals have emptied reads low until the optimizer reaches it, by as much
/// as the removals freed in the estimate but not yet in the heap: no bound holds there.
/// `statistics()` cannot tell these states apart. `INFO ts_index_roaring_bytes` is CRoaring's
/// exact heap, process-wide.
///
/// The structure is what separates a dense posting list from a scattered one of the same
/// cardinality: 40 series spread over 14 containers cost ~1.6 KiB, packed into one ~0.4 KiB.
fn roaring64_structure_bytes(containers: usize) -> usize {
    if containers == 0 {
        return ROARING_BITMAP_BYTES;
    }
    let pool = |entries: usize, bytes: usize| entries * bytes + ROARING_ALLOC_HEADER;
    let slots = roaring_pool_capacity(containers);
    let mut total = ROARING_BITMAP_BYTES
        + pool(slots, 8)
        + pool(slots, 1)
        + pool(slots, ROARING_LEAF_BYTES)
        + containers * ROARING_CONTAINER_BYTES;
    if containers > 1 {
        // One inner node holds the leaves, past 256 containers one node256 per 256 (the node
        // above those is not counted). Growing through the smaller types left their pools.
        for &(children, bytes) in &ROARING_NODES {
            let nodes = if children == 256 {
                containers.div_ceil(256)
            } else {
                1
            };
            total += pool(roaring_pool_capacity(nodes), bytes);
            if containers <= children {
                break;
            }
        }
    }
    total
}

/// The longest one slice of a walk holds the postings read lock. The walk releases the lock
/// between slices, so an index writer queued behind it waits at most about this long rather
/// than for a walk of the whole term dictionary.
const WALK_SLICE: Duration = Duration::from_micros(250);

/// Entries visited between clock reads. Also the fewest a phase visits per slice, so every
/// slice makes progress however slow the clock says it was.
const CLOCK_CHECK_INTERVAL: usize = 64;

/// Where a sliced walk resumes once the lock is taken again. Each cursor names the last entry
/// already counted, so the walk resumes strictly after it: the entry may have been removed
/// while the lock was released, and the ordered range still lands on its successor.
#[derive(Debug, Clone, PartialEq, Eq)]
enum WalkCursor {
    Terms(Option<IndexKey>),
    Series(Option<SeriesRef>),
    Bookkeeping,
    Done,
}

impl Default for WalkCursor {
    fn default() -> Self {
        WalkCursor::Terms(None)
    }
}

fn after<K>(cursor: Option<K>) -> (Bound<K>, Bound<K>) {
    (
        cursor.map_or(Bound::Unbounded, Bound::Excluded),
        Bound::Unbounded,
    )
}

impl Postings {
    /// Adds what it can of this index body's footprint to `memory` before `deadline`, starting
    /// at `cursor`, and returns where to resume. Callers hold the postings read lock for the
    /// call and release it between calls.
    fn memory_usage_slice(
        &self,
        mut cursor: WalkCursor,
        memory: &mut IndexMemory,
        deadline: Instant,
    ) -> WalkCursor {
        let out_of_time = |visited: usize| {
            visited.is_multiple_of(CLOCK_CHECK_INTERVAL) && Instant::now() >= deadline
        };

        loop {
            cursor = match cursor {
                WalkCursor::Terms(last) => {
                    let mut visited = 0;
                    for (key, ids) in self.label_index.range::<IndexKey, _>(after(last)) {
                        memory.term_count += 1;
                        // `+ 1`: `IndexKey::len` excludes the NUL sentinel the radix tree needs.
                        memory.terms_bytes += size_of::<IndexKey>() + key.len() + 1;
                        memory.postings_bytes += bitmap_heap_size(ids);
                        visited += 1;
                        if out_of_time(visited) {
                            return WalkCursor::Terms(Some(key.clone()));
                        }
                    }
                    WalkCursor::Series(None)
                }
                WalkCursor::Series(last) => {
                    let mut visited = 0;
                    for (&id, key) in self.id_to_key.range(after(last)) {
                        memory.series_count += 1;
                        memory.id_to_key_bytes +=
                            size_of::<SeriesRef>() + size_of::<KeyType>() + key.len();
                        visited += 1;
                        if out_of_time(visited) {
                            return WalkCursor::Series(Some(id));
                        }
                    }
                    WalkCursor::Bookkeeping
                }
                // Proportional to the id range / 65536, not to the series count: one step.
                WalkCursor::Bookkeeping => {
                    memory.bookkeeping_bytes = bitmap_heap_size(&self.all_postings)
                        + self.stale_ids.heap_size()
                        + self.hidden_ids.heap_size();
                    WalkCursor::Done
                }
                WalkCursor::Done => return WalkCursor::Done,
            };
        }
    }
}

impl TimeSeriesIndex {
    /// This database's index footprint.
    ///
    /// The walk takes the postings read lock in slices of at most [`WALK_SLICE`], releasing it
    /// in between so index writers wait at most one slice for it. Most writers run on the main
    /// thread holding the GIL; the tombstone drain (`tasks::stale_ids`) and the posting-list
    /// optimizer (`tasks::optimize_indices`) take the write lock on their own threads without
    /// it. No yield is needed between slices: `std`'s current `RwLock` implementations (futex and
    /// queue) refuse a new read lock while a writer is queued, so re-taking it straight away
    /// parks behind the writer. `std` documents its policy as unspecified, so this rests on the
    /// implementation, not a guarantee.
    ///
    /// Called with the GIL held (`INFO`), slicing still matters only for those off-GIL writers:
    /// a main-thread writer cannot be queued on the lock while the walk holds the GIL.
    ///
    /// Off the GIL, the figures are not a snapshot: series indexed or removed while the lock is
    /// released are counted if they fall after the cursor and missed if they fall before it,
    /// so the total is within the churn of the walk's duration. That is the right trade for
    /// capacity figures, whose consumers care about the magnitude.
    pub fn memory_usage(&self) -> IndexMemory {
        let mut memory = IndexMemory {
            db_count: 1,
            ..Default::default()
        };
        let mut cursor = WalkCursor::default();
        while cursor != WalkCursor::Done {
            let deadline = Instant::now() + WALK_SLICE;
            cursor = self.with_postings(&mut memory, |postings, memory| {
                postings.memory_usage_slice(cursor, memory, deadline)
            });
        }
        memory
    }
}

/// The footprint of `db`'s index; all zeroes, `db_count` included, if it has none. Looks the
/// index up rather than going through `get_db_index`, which would create an empty one.
pub fn db_index_memory_usage(db: i32) -> IndexMemory {
    TIMESERIES_INDEX
        .pin()
        .get(&db)
        .map(TimeSeriesIndex::memory_usage)
        .unwrap_or_default()
}

/// The footprint of every database's index, summed.
pub fn index_memory_usage() -> IndexMemory {
    let mut total = IndexMemory::default();
    for index in TIMESERIES_INDEX.pin().values() {
        total.merge(index.memory_usage());
    }
    total
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::labels::{Label, MetricName};
    use crate::series::TimeSeries;

    /// `containers` array containers of `per_container` values each, in consecutive 64 Ki blocks
    /// of one epoch (the shape a single process's ids take).
    fn spread_bitmap(containers: u64, per_container: u64) -> PostingsBitmap {
        const EPOCH: u64 = 0xA5_B4C3 << 40;
        let mut bitmap = PostingsBitmap::new();
        for block in 0..containers {
            for i in 0..per_container {
                bitmap.add(EPOCH + block * 65536 + i * 7);
            }
        }
        bitmap
    }

    #[test]
    fn bitmap_heap_size_matches_counted_allocations() {
        // Bytes a counting global allocator saw CRoaring allocate (croaring 2.9 / croaring-sys
        // 5.5, exact-capacity arrays as after `shrink_to_fit`), by containers and values each.
        // The steps at 5 and 17 containers are the ART growing into node16 and node48.
        let measured = [
            (0, 0, 232),
            (1, 1, 340),
            (1, 40, 418),
            (2, 1, 478),
            (4, 3, 612),
            (5, 1, 1042),
            (8, 9, 1272),
            (16, 40, 2864),
            (17, 1, 3370),
            (64, 3, 10144),
        ];
        for (containers, per_container, bytes) in measured {
            let bitmap = spread_bitmap(containers, per_container);
            assert_eq!(
                bitmap_heap_size(&bitmap),
                bytes,
                "{containers} containers of {per_container}"
            );
        }
    }

    #[test]
    fn a_scattered_posting_list_costs_more_than_a_packed_one() {
        // The same 40 ids, packed into one container or spread over 14: the payload is equal,
        // so only the structure tells them apart.
        let packed = spread_bitmap(1, 40);
        let mut scattered = PostingsBitmap::new();
        for i in 0..40u64 {
            scattered.add((0xA5_B4C3 << 40) + (i % 14) * 65536 + i);
        }
        assert_eq!(packed.cardinality(), scattered.cardinality());
        assert_eq!(scattered.statistics().n_containers, 14);
        assert!(
            bitmap_heap_size(&scattered) > 3 * bitmap_heap_size(&packed),
            "{} vs {}",
            bitmap_heap_size(&scattered),
            bitmap_heap_size(&packed)
        );
    }

    /// An index body of `n` series, each with a shared and a unique label: `n + 1` terms.
    fn postings(n: u64) -> Postings {
        let mut postings = Postings::default();
        for id in 1..=n {
            let mut series = TimeSeries::new();
            series.id = id;
            let unique = format!("s{id}");
            series.labels =
                MetricName::new(&[Label::new("env", "prod"), Label::new("uniq", &unique)]);
            postings.index_timeseries(&series, format!("key:{id}").as_bytes());
        }
        postings
    }

    /// Walks `postings` to completion, returning the footprint and how many slices it took.
    fn walk(postings: &Postings, slice: Duration) -> (IndexMemory, usize) {
        let mut memory = IndexMemory::default();
        let mut cursor = WalkCursor::default();
        let mut slices = 0;
        while cursor != WalkCursor::Done {
            cursor = postings.memory_usage_slice(cursor, &mut memory, Instant::now() + slice);
            slices += 1;
        }
        (memory, slices)
    }

    #[test]
    fn sliced_walk_matches_a_single_pass() {
        let postings = postings(500);
        let (whole, one) = walk(&postings, Duration::from_secs(3600));
        // A deadline that has already passed stops every phase at its first clock check.
        let (sliced, many) = walk(&postings, Duration::ZERO);

        assert_eq!(one, 1);
        assert!(many > 10, "took {many} slices");
        assert_eq!(sliced, whole);
        assert_eq!(whole.series_count, 500);
        assert_eq!(whole.term_count, 501);
        assert!(whole.postings_bytes > 0 && whole.bookkeeping_bytes > 0);
    }

    #[test]
    fn resumes_strictly_after_the_cursor_present_or_not() {
        let postings = postings(200);
        // Ids 1..=200 are present. A cursor counts strictly after itself whether its entry is
        // still there (50) or has since been removed (1000, and the absent term below).
        for (last, expected) in [(50, 150), (1000, 0)] {
            let mut memory = IndexMemory::default();
            let cursor = postings.memory_usage_slice(
                WalkCursor::Series(Some(last)),
                &mut memory,
                Instant::now() + Duration::from_secs(3600),
            );
            assert_eq!(cursor, WalkCursor::Done);
            assert_eq!(memory.series_count, expected, "after {last}");
        }

        let mut memory = IndexMemory::default();
        // A term no series has, sorting between `env=prod` and the `uniq` terms.
        let absent = IndexKey::for_label_value("lost", "x");
        postings.memory_usage_slice(
            WalkCursor::Terms(Some(absent)),
            &mut memory,
            Instant::now() + Duration::from_secs(3600),
        );
        assert_eq!(memory.term_count, 200, "the 200 `uniq` values");
    }

    #[test]
    fn db_without_an_index_reports_zero_and_creates_none() {
        // A database number no other test touches: the index map is process-global.
        const DB: i32 = 0x1d_e3;
        assert_eq!(db_index_memory_usage(DB), IndexMemory::default());
        assert!(TIMESERIES_INDEX.pin().get(&DB).is_none());
    }
}
