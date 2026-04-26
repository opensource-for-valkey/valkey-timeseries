//! A string interner that deallocates unused values.
//!
//! Original code https://github.com/ryzhyk/arc-interner
//! Copyright (c) 2021-2024 Leonid Ryzhyk
//! License: MIT
//!
//!
//! Interning reduces the memory footprint of an application by storing
//! a unique copy of each distinct value.  It speeds up equality
//! comparison and hashing operations, as only pointers rather than actual
//! values need to be compared.  On the flip side, object creation is
//! slower, as it involves lookup in the interned string pool.
//!
//! This library makes the following design choices:
//!
//! - Interned strings are reference counted.  When the last reference to
//!   an interned object is dropped, the string is deallocated.  This
//!   prevents unbounded growth of the interned object pool in applications
//!   where the set of interned values changes dynamically at the cost of
//!   some CPU and memory overhead (due to storing and maintaining an
//!   atomic counter).
//! - Multithreading.  A single pool of interned strings is shared by all
//!   threads in the program.  The pool is protected by a RwLock, allowing
//!   concurrent reads but exclusive writes.
//! - Safe: this library is built on the `Arc` type from the Rust
//!   standard library and does not contain any unsafe code.
//!
//! # Example
//! ```rust
//! use crate::common::string_interner::InternedString;
//! let x = InternedString::new("hello");
//! let y: InternedString = "world".into();
//! assert_ne!(x, y);
//! assert_eq!(x, InternedString::new("hello"));
//! assert_eq!(&*x, "hello"); // dereference an InternedString like a pointer
//! ```

use crate::common::sync::{read_lock, write_lock};
use ahash::RandomState;
use get_size2::GetSize;
use min_max_heap::MinMaxHeap;
use std::borrow::Borrow;
use std::collections::{BTreeMap, HashSet};
use std::convert::Infallible;
use std::fmt;
use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::str::FromStr;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, LazyLock, RwLock};

type StringContainer = RwLock<HashSet<Arc<[u8]>, RandomState>>;

static STRING_POOL: LazyLock<StringContainer> =
    LazyLock::new(|| RwLock::new(HashSet::with_hasher(RandomState::new())));

/// Total memory used by all interned strings.
static STRING_MEMORY_USED: AtomicUsize = AtomicUsize::new(0);

#[derive(Default)]
pub struct BucketStats {
    pub count: usize,
    pub bytes: usize,
    pub allocated: usize,
}

impl BucketStats {
    pub fn get_avg_size(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.bytes as f64 / self.count as f64
        }
    }

    pub fn get_avg_allocated(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.allocated as f64 / self.count as f64
        }
    }

    pub fn get_utilization(&self) -> f64 {
        if self.count == 0 {
            return 0.0;
        }

        let avg_size = self.get_avg_size();
        let avg_allocated = self.get_avg_allocated();

        if avg_size == 0.0 || avg_allocated == 0.0 {
            0.0
        } else {
            avg_size / avg_allocated
        }
    }
}

impl Display for BucketStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let avg_size = self.get_avg_size();
        let avg_allocated = self.get_avg_allocated();

        let utilization = self.get_utilization();

        write!(
            f,
            "Count: {} Bytes: {} AvgSize: {:.2} Allocated: {} AvgAllocated: {:.2} Utilization: {}%",
            self.count,
            self.bytes,
            avg_size,
            self.allocated,
            avg_allocated,
            (100.0 * utilization) as i64
        )
    }
}

/// A snapshot of an interned string's metrics, used for top-K reporting.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TopKEntry {
    /// The interned string value.
    pub value: InternedString,
    /// Number of external references (excludes the pool's own reference).
    pub ref_count: usize,
    /// Length of the string in bytes.
    pub bytes: usize,
    /// Total allocated memory for this string (Arc overhead + data).
    pub allocated: usize,
}

/// Wrapper for max-heap ordering by `bytes` (used to maintain top-K by size).
#[derive(Eq, PartialEq)]
struct TopKBySize(TopKEntry);

impl Ord for TopKBySize {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.bytes.cmp(&other.0.bytes)
    }
}

impl PartialOrd for TopKBySize {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Wrapper for max-heap ordering by `ref_count` (used to maintain top-K by refs).
#[derive(Eq, PartialEq)]
struct TopKByRef(TopKEntry);

impl Ord for TopKByRef {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.ref_count.cmp(&other.0.ref_count)
    }
}

impl PartialOrd for TopKByRef {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Default)]
pub struct Stats {
    pub by_ref_stats: BTreeMap<usize, BucketStats>,
    pub by_size_stats: BTreeMap<usize, BucketStats>,
    pub total_stats: BucketStats,
    /// Top K interned strings by allocated size, sorted largest first.
    pub top_k_by_size: Vec<TopKEntry>,
    /// Top K interned strings by external reference count, sorted highest first.
    pub top_k_by_ref: Vec<TopKEntry>,
    /// Bytes saved by interning: sum of `allocated * (ref_count - 1)` over all pool entries.
    pub memory_saved_bytes: usize,
    /// Percentage of memory saved relative to the hypothetical uninterned cost.
    /// `memory_saved_bytes / (memory_saved_bytes + total_stats.allocated) * 100`
    pub memory_saved_pct: f64,
}

/// A pointer to an interned, reference-counted, and immutable string object.
///
/// The interned string will be held in memory only until its
/// reference count reaches zero.
///
/// # Example
/// ```rust
/// use crate::common::string_interner::InternedString;
///
/// let x = InternedString::new("hello");
/// let y: InternedString = "world".into();
/// assert_ne!(x, y);
/// assert_eq!(x, InternedString::new("hello"));
/// assert_eq!(&*x, "hello"); // dereference an InternedString like a pointer
/// ```
#[derive(Debug, GetSize)]
pub struct InternedString {
    /// The actual string data. We use `Arc<[u8]>` instead of `Arc<String>` to save
    /// some memory (no need to store the capacity of the string since we're immutable).
    arc: Arc<[u8]>,
}

impl InternedString {
    /// Intern a string value.  If this value has not previously been
    /// interned, then `new` will allocate a spot for the value on the
    /// heap.  Otherwise, it will return a pointer to the object
    /// previously allocated.
    ///
    /// Note that `InternedString::new` is a bit slower than direct allocation, since it needs to check
    /// a lock. However, the performance should be acceptable for our use cases,
    /// especially under low contention.
    pub fn new(val: &str) -> Self {
        let v = Arc::from(val.as_bytes());
        Self::from_arc(v)
    }

    fn from_arc(val: Arc<[u8]>) -> InternedString {
        // First, try to get an existing entry with a read lock
        {
            let pool = read_lock(&STRING_POOL);
            if let Some(existing) = pool.get(&val) {
                return InternedString {
                    arc: existing.clone(),
                };
            }
        }

        // If not found, acquire write lock and insert
        let mut pool = write_lock(&STRING_POOL);

        // Double-check after acquiring write lock (another thread may have inserted)
        if let Some(existing) = pool.get(&val) {
            return InternedString {
                arc: existing.clone(),
            };
        }

        // Insert new value
        let size = val.get_size();
        pool.insert(val.clone());
        STRING_MEMORY_USED.fetch_add(size, std::sync::atomic::Ordering::SeqCst);

        InternedString { arc: val }
    }

    pub fn len(&self) -> usize {
        self.arc.len()
    }

    pub fn is_empty(&self) -> bool {
        self.arc.is_empty()
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.arc.as_ref()
    }

    /// Return the number of references to this value.
    pub fn ref_count(&self) -> usize {
        // The hashset holds one reference; we return the number of
        // references held by actual clients.
        Arc::strong_count(&self.arc) - 1
    }

    /// Return true if this is the only reference to this value.
    pub fn is_unique(&self) -> bool {
        self.ref_count() == 1
    }

    /// Return the number of unique interned strings.
    pub fn interned_count() -> usize {
        read_lock(&STRING_POOL).len()
    }

    /// Return the total memory used by all interned strings.
    pub fn memory_used() -> usize {
        STRING_MEMORY_USED.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Collect statistics about the interned string pool, including the top `k`
    /// strings by allocated size and by external reference count.
    ///
    /// Passing `k = 0` skips the top-K collection entirely (both `top_k_by_size` and
    /// `top_k_by_ref` will be empty).
    pub fn get_stats_with_top_k(k: usize) -> Stats {
        let pool = read_lock(&STRING_POOL);
        let mut stats = Stats::default();

        // MinMaxHeap allows us to efficiently track top-K and extract the max values
        let mut size_heap: MinMaxHeap<TopKBySize> = MinMaxHeap::new();
        let mut ref_heap: MinMaxHeap<TopKByRef> = MinMaxHeap::new();

        for arc in pool.iter() {
            let ref_count = Arc::strong_count(arc) - 1; // exclude the pool's reference
            let allocated = arc.get_size();
            let bytes = arc.as_ref().len();

            let by_ref_stats = stats.by_ref_stats.entry(ref_count).or_default();
            by_ref_stats.count += 1;
            by_ref_stats.bytes += bytes;
            by_ref_stats.allocated += allocated;

            let by_size_stats = stats.by_size_stats.entry(bytes).or_default();
            by_size_stats.count += 1;
            by_size_stats.bytes += bytes;
            by_size_stats.allocated += allocated;

            stats.total_stats.count += 1;
            stats.total_stats.bytes += bytes;
            stats.total_stats.allocated += allocated;

            // Each duplicate reference that shares this allocation is a saved copy.
            // ref_count includes the pool's own ref, so duplicates = ref_count - 1.
            // (strings with ref_count == 1 have no duplicates → zero saving)
            if ref_count > 1 {
                stats.memory_saved_bytes += allocated * (ref_count - 1);
            }

            if k > 0 {
                let value = InternedString {
                    arc: Arc::clone(arc),
                };
                let entry = TopKEntry {
                    value,
                    ref_count,
                    bytes,
                    allocated,
                };

                size_heap.push(TopKBySize(entry.clone()));
                if size_heap.len() > k {
                    size_heap.pop_min(); // Remove the smallest to maintain top-K
                }

                ref_heap.push(TopKByRef(entry));
                if ref_heap.len() > k {
                    ref_heap.pop_min(); // Remove the smallest to maintain top-K
                }
            }
        }

        if k > 0 {
            // Extract the largest values from the heaps
            let mut top_by_size: Vec<TopKEntry> = Vec::with_capacity(size_heap.len());
            while let Some(item) = size_heap.pop_max() {
                top_by_size.push(item.0);
            }
            stats.top_k_by_size = top_by_size;

            let mut top_by_ref: Vec<TopKEntry> = Vec::with_capacity(ref_heap.len());
            while let Some(item) = ref_heap.pop_max() {
                top_by_ref.push(item.0);
            }

            stats.top_k_by_ref = top_by_ref;
        }

        // Hypothetical uninterned cost = what we hold now + what we saved
        let total_uninterned = stats.total_stats.allocated + stats.memory_saved_bytes;
        stats.memory_saved_pct = if total_uninterned == 0 {
            0.0
        } else {
            stats.memory_saved_bytes as f64 / total_uninterned as f64 * 100.0
        };

        stats
    }

    pub fn get_stats() -> Stats {
        Self::get_stats_with_top_k(0)
    }
}

impl Clone for InternedString {
    fn clone(&self) -> Self {
        InternedString {
            arc: self.arc.clone(),
        }
    }
}

impl Borrow<str> for InternedString {
    fn borrow(&self) -> &str {
        self.deref()
    }
}

impl Hash for InternedString {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_bytes().hash(state);
    }
}

impl AsRef<[u8]> for InternedString {
    fn as_ref(&self) -> &[u8] {
        self.arc.as_ref()
    }
}

impl AsRef<str> for InternedString {
    fn as_ref(&self) -> &str {
        self.deref()
    }
}

impl Deref for InternedString {
    type Target = str;
    fn deref(&self) -> &str {
        // SAFETY: we only intern valid UTF-8 strings
        debug_assert!(
            std::str::from_utf8(self.arc.as_ref()).is_ok(),
            "InternedString: interned bytes are not valid UTF-8"
        );
        unsafe { std::str::from_utf8_unchecked(self.arc.as_ref()) }
    }
}

impl Display for InternedString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let v: &str = self.deref();
        write!(f, "{v}")
    }
}

impl Drop for InternedString {
    fn drop(&mut self) {
        // Fast path: if there are definitely other external refs, do nothing.
        // Counts: 1 ref is held by the pool (if still interned) and 1 by `self`.
        if Arc::strong_count(&self.arc) > 2 {
            return;
        }

        let mut pool = write_lock(&STRING_POOL);

        // Only remove/account if the pool currently contains this arc AND `self` is
        // the last external reference at the moment we hold the write lock.
        if Arc::strong_count(&self.arc) == 2 && pool.remove(&self.arc) {
            let size = self.arc.get_size();
            STRING_MEMORY_USED.fetch_sub(size, std::sync::atomic::Ordering::SeqCst);
        }
    }
}

impl FromStr for InternedString {
    type Err = Infallible;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::new(s))
    }
}

impl From<String> for InternedString {
    fn from(t: String) -> Self {
        Self::new(t.as_str())
    }
}

impl From<&[u8]> for InternedString {
    fn from(s: &[u8]) -> Self {
        Self::from_arc(Arc::from(s))
    }
}

impl From<&str> for InternedString {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl Default for InternedString {
    fn default() -> InternedString {
        InternedString::new(Default::default())
    }
}

/// Efficiently compares two interned values by comparing their pointers.
impl PartialEq for InternedString {
    fn eq(&self, other: &InternedString) -> bool {
        Arc::ptr_eq(&self.arc, &other.arc)
    }
}

impl Eq for InternedString {}

impl PartialOrd for InternedString {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
    fn lt(&self, other: &Self) -> bool {
        self.as_bytes().lt(other.as_bytes())
    }
    fn le(&self, other: &Self) -> bool {
        self.as_bytes().le(other.as_bytes())
    }
    fn gt(&self, other: &Self) -> bool {
        self.as_bytes().gt(other.as_bytes())
    }
    fn ge(&self, other: &Self) -> bool {
        self.as_bytes().ge(other.as_bytes())
    }
}

impl Ord for InternedString {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_bytes().cmp(other.as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::{InternedString, STRING_MEMORY_USED, STRING_POOL};
    use ahash::{HashSet, HashSetExt};
    use serial_test::serial;
    use std::collections::HashMap;
    use std::ops::Deref;
    use std::thread;

    // Tests are run serially to avoid interference via the global string pool and memory tracker.

    // Test basic functionality.
    #[test]
    #[serial]
    fn basic() {
        assert_eq!(InternedString::new("foo"), InternedString::new("foo"));
        assert_ne!(InternedString::new("foo"), InternedString::new("bar"));
        // The above refs should be deallocated by now.
        assert_eq!(InternedString::interned_count(), 0);

        let _interned1 = InternedString::new("foo");
        {
            let interned2 = InternedString::new("foo");
            let interned3 = InternedString::new("bar");

            assert_eq!(interned2.ref_count(), 2);
            assert_eq!(interned3.ref_count(), 1);
            // We now have two unique interned strings: "foo" and "bar".
            assert_eq!(InternedString::interned_count(), 2);
        }

        // "bar" is now gone.
        assert_eq!(InternedString::interned_count(), 1);
    }

    // Ordering should be based on values, not pointers.
    // Also tests `Display` implementation.
    #[test]
    #[serial]
    fn sorting() {
        let mut interned_vals = [
            InternedString::new("4"),
            InternedString::new("2"),
            InternedString::new("5"),
            InternedString::new("0"),
            InternedString::new("1"),
            InternedString::new("3"),
        ];
        interned_vals.sort();
        let sorted: Vec<String> = interned_vals.iter().map(|v| format!("{v}")).collect();
        assert_eq!(&sorted.join(","), "0,1,2,3,4,5");
    }

    #[test]
    #[serial]
    fn sequential() {
        for _i in 0..10_000 {
            let mut interned = Vec::with_capacity(100);
            for j in 0..100 {
                let val = format!("foo{j}");
                let interned_string = InternedString::new(&val);
                interned.push(interned_string);
            }
        }

        assert_eq!(InternedString::interned_count(), 0);
    }

    // Quickly create and destroy a small number of interned objects from
    // multiple threads.
    #[test]
    #[serial]
    fn multithreading1() {
        let mut thread_handles = vec![];
        for _i in 0..10 {
            let t = thread::spawn({
                move || {
                    for _i in 0..100_000 {
                        let interned1 = InternedString::new("foo");
                        let _interned2 = InternedString::new("bar");
                        let mut m = HashMap::new();
                        // force some hashing
                        m.insert(interned1, ());
                    }
                }
            });
            thread_handles.push(t);
        }
        for h in thread_handles.into_iter() {
            h.join().unwrap()
        }

        assert_eq!(InternedString::interned_count(), 0);
    }

    // Helper to reset memory tracking before each test
    fn reset_memory_tracking() {
        STRING_MEMORY_USED.store(0, std::sync::atomic::Ordering::SeqCst);
        crate::common::sync::write_lock(&STRING_POOL).clear();
    }

    #[test]
    #[serial]
    fn test_new_creates_interned_string() {
        reset_memory_tracking();

        let s1 = InternedString::new("hello");
        let s2 = InternedString::new("hello");

        // Both should refer to the same interned value
        assert_eq!(s1, s2);
        assert_eq!(s1.len(), 5);
        assert!(!s1.is_empty());
    }

    #[test]
    #[serial]
    fn test_different_interned_strings_are_not_equal() {
        reset_memory_tracking();

        let s1 = InternedString::new("hello");
        let s2 = InternedString::new("world");

        assert_ne!(s1, s2);
    }

    #[test]
    #[serial]
    fn test_clone_interned_string_increases_refcount() {
        reset_memory_tracking();

        let s1 = InternedString::new("test");
        let initial_refcount = s1.ref_count();

        let s2 = s1.clone();

        assert_eq!(s1.ref_count(), initial_refcount + 1);
        assert_eq!(s2.ref_count(), initial_refcount + 1);
        assert_eq!(s1, s2);
    }

    #[test]
    #[serial]
    fn test_drop_decreases_interned_string_refcount() {
        reset_memory_tracking();

        let s1 = InternedString::new("test");
        let s2 = s1.clone();
        let initial_refcount = s1.ref_count();

        drop(s1);

        assert_eq!(s2.ref_count(), initial_refcount - 1);
    }

    #[test]
    #[serial]
    fn test_memory_tracking_on_first_creation() {
        reset_memory_tracking();

        assert_eq!(InternedString::memory_used(), 0);

        let s1 = InternedString::new("test_memory");
        let memory_after_creation = InternedString::memory_used();

        assert!(memory_after_creation > 0);

        // Creating the same string again should not increase memory
        let s2 = InternedString::new("test_memory");
        assert_eq!(InternedString::memory_used(), memory_after_creation);

        // But refcount should increase
        assert_eq!(s1.ref_count(), 2);
        assert_eq!(s2.ref_count(), 2);
    }

    #[test]
    #[serial]
    fn test_memory_tracking_on_drop() {
        reset_memory_tracking();

        let s1 = InternedString::new("test_drop_memory");
        let s2 = s1.clone();
        let memory_with_two_refs = InternedString::memory_used();

        // Dropping one reference should not decrease memory yet
        drop(s1);
        assert_eq!(InternedString::memory_used(), memory_with_two_refs);

        // Dropping the last reference should decrease memory
        drop(s2);
        assert_eq!(InternedString::memory_used(), 0);
    }

    #[test]
    #[serial]
    fn test_memory_tracking_with_different_strings() {
        reset_memory_tracking();

        let _s1 = InternedString::new("short");
        let memory_after_first = InternedString::memory_used();

        let _s2 = InternedString::new("this_is_a_much_longer_string_for_testing");
        let memory_after_second = InternedString::memory_used();

        // Memory should increase more for the longer string
        assert!(memory_after_second > memory_after_first);

        // The difference should be at least the difference in string lengths
        let length_diff = "this_is_a_much_longer_string_for_testing".len() - "short".len();
        assert!((memory_after_second - memory_after_first) >= length_diff);
    }

    #[test]
    #[serial]
    fn test_interned_string_deref_trait() {
        reset_memory_tracking();

        let s = InternedString::new("test_string");

        // Test various string methods through Deref
        assert_eq!(s.len(), 11);
        assert!(s.contains("test"));
        assert!(s.starts_with("test"));
        assert!(s.ends_with("string"));
        assert_eq!(s.to_uppercase(), "TEST_STRING");
    }

    #[test]
    #[serial]
    fn test_partial_ord_and_ord() {
        reset_memory_tracking();

        let s1 = InternedString::new("apple");
        let s2 = InternedString::new("banana");
        let s3 = InternedString::new("cherry");

        assert!(s1 < s2);
        assert!(s2 < s3);
        assert!(s1 < s3);

        let mut vec = [s3.clone(), s1.clone(), s2.clone()];
        vec.sort();

        assert_eq!(vec[0], s1);
        assert_eq!(vec[1], s2);
        assert_eq!(vec[2], s3);
    }

    #[test]
    #[serial]
    fn test_hash_consistency() {
        reset_memory_tracking();

        let s1 = InternedString::new("hash_test");
        let s2 = InternedString::new("hash_test");
        let s3 = InternedString::new("different");

        let mut set = HashSet::new();
        set.insert(s1.clone());

        // s2 should be found in the set because it's equal to s1
        assert!(set.contains(&s2));

        // s3 should not be found
        assert!(!set.contains(&s3));

        // Adding s2 should not increase the set size
        set.insert(s2);
        assert_eq!(set.len(), 1);

        // Adding s3 should increase the set size
        set.insert(s3);
        assert_eq!(set.len(), 2);
    }

    #[test]
    #[serial]
    fn test_empty_string() {
        reset_memory_tracking();

        let empty1 = InternedString::new("");
        let empty2 = InternedString::new("");

        assert_eq!(empty1, empty2);
        assert_eq!(empty1.deref(), "");
        assert_eq!(empty1.len(), 0);
        assert!(empty1.is_empty());
    }

    #[test]
    #[serial]
    fn test_unicode_strings() {
        reset_memory_tracking();

        let unicode1 = InternedString::new("🦀 Rust");
        let unicode2 = InternedString::new("🦀 Rust");
        let different = InternedString::new("🐍 Python");

        assert_eq!(unicode1, unicode2);
        assert_ne!(unicode1, different);
        assert_eq!(unicode1.deref(), "🦀 Rust");
    }

    #[test]
    #[serial]
    fn test_very_long_strings() {
        reset_memory_tracking();

        let long_string = "a".repeat(10000);
        let s1 = InternedString::new(&long_string);
        let s2 = InternedString::new(&long_string);

        assert_eq!(s1, s2);
        assert_eq!(s1.len(), 10000);
        assert_eq!(s1.ref_count(), 2);
    }

    // ── Basic top-K by size ──────────────────────────────────────────────────

    #[test]
    #[serial]
    fn test_top_k_by_size_returns_k_entries() {
        reset_memory_tracking();

        let _s1 = InternedString::new("a");
        let _s2 = InternedString::new("bbb");
        let _s3 = InternedString::new("ccccc");
        let _s4 = InternedString::new("ddddddd");
        let _s5 = InternedString::new("eeeeeeeee");

        let stats = InternedString::get_stats_with_top_k(3);
        assert_eq!(stats.top_k_by_size.len(), 3);
    }

    #[test]
    #[serial]
    fn test_top_k_by_size_sorted_descending() {
        reset_memory_tracking();

        let _s1 = InternedString::new("z");
        let _s2 = InternedString::new("yy");
        let _s3 = InternedString::new("xxx");
        let _s4 = InternedString::new("wwww");
        let _s5 = InternedString::new("vvvvv");

        let stats = InternedString::get_stats_with_top_k(4);
        let sizes: Vec<usize> = stats.top_k_by_size.iter().map(|e| e.bytes).collect();
        for window in sizes.windows(2) {
            assert!(
                window[0] >= window[1],
                "top_k_by_size not sorted descending: {:?}",
                sizes
            );
        }
    }

    #[test]
    #[serial]
    fn test_top_k_by_size_contains_largest() {
        reset_memory_tracking();

        const LARGE_STRING: &str = "this_is_the_largest_string_in_pool";

        let _s1 = InternedString::new("tiny");
        let _s2 = InternedString::new("medium_string");
        let _s3 = InternedString::new(LARGE_STRING);

        let stats = InternedString::get_stats_with_top_k(1);
        assert_eq!(stats.top_k_by_size.len(), 1);
        assert_eq!(&*stats.top_k_by_size[0].value, LARGE_STRING);
    }

    #[test]
    #[serial]
    fn test_top_k_by_size_excludes_smaller_strings() {
        reset_memory_tracking();

        let _s1 = InternedString::new("a");
        let _s2 = InternedString::new("bb");
        let _s3 = InternedString::new("ccc");
        let _s4 = InternedString::new("dddd");
        let _s5 = InternedString::new("eeeee");

        let stats = InternedString::get_stats_with_top_k(2);
        // Top 2 by size must be "eeeee" (5) and "dddd" (4)
        let values: Vec<String> = stats
            .top_k_by_size
            .iter()
            .map(|e| e.value.to_string())
            .collect();
        assert!(values.contains(&"eeeee".to_string()));
        assert!(values.contains(&"dddd".to_string()));
        assert!(!values.contains(&"a".to_string()));
    }

    // ── Basic top-K by ref ───────────────────────────────────────────────────

    #[test]
    #[serial]
    fn test_top_k_by_ref_returns_k_entries() {
        reset_memory_tracking();

        let _s1 = InternedString::new("one");
        let s2 = InternedString::new("two");
        let _c1 = s2.clone();
        let s3 = InternedString::new("three");
        let _c2 = s3.clone();
        let _c3 = s3.clone();

        let stats = InternedString::get_stats_with_top_k(2);
        assert_eq!(stats.top_k_by_ref.len(), 2);
    }

    #[test]
    #[serial]
    fn test_top_k_by_ref_sorted_descending() {
        reset_memory_tracking();

        let _s1 = InternedString::new("alpha");
        let s2 = InternedString::new("beta");
        let _b1 = s2.clone();
        let s3 = InternedString::new("gamma");
        let _g1 = s3.clone();
        let _g2 = s3.clone();
        let _g3 = s3.clone();

        let stats = InternedString::get_stats_with_top_k(3);
        let ref_counts: Vec<usize> = stats.top_k_by_ref.iter().map(|e| e.ref_count).collect();
        for window in ref_counts.windows(2) {
            assert!(
                window[0] >= window[1],
                "top_k_by_ref not sorted descending: {:?}",
                ref_counts
            );
        }
    }

    #[test]
    #[serial]
    fn test_top_k_by_ref_most_referenced_is_first() {
        reset_memory_tracking();

        let _s_low = InternedString::new("low_refs");
        let s_high = InternedString::new("high_refs");
        let _c1 = s_high.clone();
        let _c2 = s_high.clone();
        let _c3 = s_high.clone();
        let _c4 = s_high.clone();

        let stats = InternedString::get_stats_with_top_k(2);
        assert_eq!(
            stats.top_k_by_ref[0].value,
            InternedString::new("high_refs")
        );
        assert!(stats.top_k_by_ref[0].ref_count > stats.top_k_by_ref[1].ref_count);
    }

    #[test]
    #[serial]
    fn test_top_k_by_ref_excludes_low_ref_strings() {
        reset_memory_tracking();

        let _s1 = InternedString::new("single");
        let s2 = InternedString::new("double");
        let _c2 = s2.clone();
        let s3 = InternedString::new("triple");
        let _c3a = s3.clone();
        let _c3b = s3.clone();
        let s4 = InternedString::new("quad");
        let _c4a = s4.clone();
        let _c4b = s4.clone();
        let _c4c = s4.clone();

        let stats = InternedString::get_stats_with_top_k(2);
        let values: Vec<String> = stats
            .top_k_by_ref
            .iter()
            .map(|e| e.value.to_string())
            .collect();
        assert!(values.contains(&"quad".to_string()));
        assert!(values.contains(&"triple".to_string()));
        assert!(!values.contains(&"single".to_string()));
    }

    #[test]
    #[serial]
    fn test_top_k_entry_value_field() {
        reset_memory_tracking();

        let _s = InternedString::new("check_value");

        let stats = InternedString::get_stats_with_top_k(1);
        assert_eq!(
            stats.top_k_by_size[0].value,
            InternedString::new("check_value")
        );
    }

    #[test]
    #[serial]
    fn test_top_k_entry_bytes_field() {
        reset_memory_tracking();

        let _s = InternedString::new("hello");

        let stats = InternedString::get_stats_with_top_k(1);
        assert_eq!(stats.top_k_by_size[0].bytes, "hello".len());
    }

    #[test]
    #[serial]
    fn test_top_k_entry_ref_count_field() {
        reset_memory_tracking();

        let s = InternedString::new("ref_check");
        let _c1 = s.clone();
        let _c2 = s.clone();
        // 3 external refs total

        let stats = InternedString::get_stats_with_top_k(1);
        assert_eq!(stats.top_k_by_ref[0].ref_count, 3);
    }

    #[test]
    #[serial]
    fn test_top_k_entry_allocated_gte_bytes() {
        reset_memory_tracking();

        let _s = InternedString::new("allocation_check");

        let stats = InternedString::get_stats_with_top_k(1);
        let entry = &stats.top_k_by_size[0];
        assert!(
            entry.allocated >= entry.bytes,
            "allocated ({}) must be >= bytes ({})",
            entry.allocated,
            entry.bytes
        );
    }

    // ── Edge cases ───────────────────────────────────────────────────────────

    #[test]
    #[serial]
    fn test_top_k_zero_returns_empty_vecs() {
        reset_memory_tracking();

        let _s = InternedString::new("ignored");

        let stats = InternedString::get_stats_with_top_k(0);
        assert!(stats.top_k_by_size.is_empty());
        assert!(stats.top_k_by_ref.is_empty());
    }

    #[test]
    #[serial]
    fn test_top_k_zero_still_populates_aggregate_stats() {
        reset_memory_tracking();

        let _s1 = InternedString::new("aaa");
        let _s2 = InternedString::new("bbbb");

        let stats = InternedString::get_stats_with_top_k(0);
        assert_eq!(stats.total_stats.count, 2);
        assert!(stats.total_stats.bytes > 0);
        assert!(!stats.by_size_stats.is_empty());
        assert!(!stats.by_ref_stats.is_empty());
    }

    #[test]
    #[serial]
    fn test_top_k_empty_pool() {
        reset_memory_tracking();

        let stats = InternedString::get_stats_with_top_k(5);
        assert!(stats.top_k_by_size.is_empty());
        assert!(stats.top_k_by_ref.is_empty());
        assert_eq!(stats.total_stats.count, 0);
    }

    #[test]
    #[serial]
    fn test_top_k_larger_than_pool_returns_all() {
        reset_memory_tracking();

        let _s1 = InternedString::new("x");
        let _s2 = InternedString::new("yy");
        let _s3 = InternedString::new("zzz");

        // Request more than pool size
        let stats = InternedString::get_stats_with_top_k(100);
        assert_eq!(stats.top_k_by_size.len(), 3);
        assert_eq!(stats.top_k_by_ref.len(), 3);
    }

    #[test]
    #[serial]
    fn test_top_k_equal_to_pool_size_returns_all() {
        reset_memory_tracking();

        let _s1 = InternedString::new("p");
        let _s2 = InternedString::new("qq");
        let _s3 = InternedString::new("rrr");

        let stats = InternedString::get_stats_with_top_k(3);
        assert_eq!(stats.top_k_by_size.len(), 3);
        assert_eq!(stats.top_k_by_ref.len(), 3);
    }

    #[test]
    #[serial]
    fn test_top_k_single_string_in_pool() {
        reset_memory_tracking();

        let s = InternedString::new("only_one");
        let _c = s.clone();

        let stats = InternedString::get_stats_with_top_k(5);
        assert_eq!(stats.top_k_by_size.len(), 1);
        assert_eq!(stats.top_k_by_ref.len(), 1);
        assert_eq!(
            stats.top_k_by_size[0].value,
            InternedString::new("only_one")
        );
        assert_eq!(stats.top_k_by_ref[0].ref_count, 2);
    }

    #[test]
    #[serial]
    fn test_top_k_k_equals_one() {
        reset_memory_tracking();

        let _s1 = InternedString::new("short");
        let _s2 = InternedString::new("much_longer_string");

        let stats = InternedString::get_stats_with_top_k(1);
        assert_eq!(stats.top_k_by_size.len(), 1);
        assert_eq!(
            stats.top_k_by_size[0].value,
            InternedString::new("much_longer_string")
        );
    }

    #[test]
    #[serial]
    fn test_get_stats_equivalent_to_top_k_zero() {
        reset_memory_tracking();

        let _s = InternedString::new("convenience");

        let stats = InternedString::get_stats();
        assert!(stats.top_k_by_size.is_empty());
        assert!(stats.top_k_by_ref.is_empty());
        assert_eq!(stats.total_stats.count, 1);
    }

    #[test]
    #[serial]
    fn test_top_k_entries_ref_count_excludes_pool_ref() {
        reset_memory_tracking();

        // One external reference only
        let _s = InternedString::new("solo");

        let stats = InternedString::get_stats_with_top_k(1);
        // Pool holds 1 ref, `_s` holds 1 ref → strong_count = 2 → external = 1
        assert_eq!(stats.top_k_by_ref[0].ref_count, 1);
    }

    #[test]
    #[serial]
    fn test_top_k_by_size_ties_all_included_when_k_gte_pool_size() {
        reset_memory_tracking();

        // Four strings of the same length
        let _s1 = InternedString::new("aa");
        let _s2 = InternedString::new("bb");
        let _s3 = InternedString::new("cc");
        let _s4 = InternedString::new("dd");

        let stats = InternedString::get_stats_with_top_k(4);
        assert_eq!(stats.top_k_by_size.len(), 4);
        // All must have bytes == 2
        for entry in &stats.top_k_by_size {
            assert_eq!(entry.bytes, 2);
        }
    }

    #[test]
    #[serial]
    fn test_top_k_by_ref_ties_all_included_when_k_gte_pool_size() {
        reset_memory_tracking();

        // Three strings each with 2 external refs
        let s1 = InternedString::new("tie_a");
        let _c1 = s1.clone();
        let s2 = InternedString::new("tie_b");
        let _c2 = s2.clone();
        let s3 = InternedString::new("tie_c");
        let _c3 = s3.clone();

        let stats = InternedString::get_stats_with_top_k(3);
        assert_eq!(stats.top_k_by_ref.len(), 3);
        for entry in &stats.top_k_by_ref {
            assert_eq!(entry.ref_count, 2);
        }
    }
}
