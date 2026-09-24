//! A string interner that deallocates unused values.
//!
//! Derived from https://github.com/ryzhyk/arc-interner
//! Copyright (c) 2021-2024 Leonid Ryzhyk
//! License: MIT
//!
//! Interning reduces the memory footprint of an application by storing
//! a unique copy of each distinct value.  It speeds up equality
//! comparison and hashing operations, as only pointers rather than actual
//! values need to be compared.  On the flip side, object creation is
//! slower, as it involves lookup in the interned string pool.
//!
//! Design choices:
//!
//! - Interned strings are reference counted.  When the last reference to
//!   an interned object is dropped, the string is deallocated.  This
//!   prevents unbounded growth of the interned object pool in applications
//!   where the set of interned values changes dynamically at the cost of
//!   some CPU and memory overhead (due to storing and maintaining an
//!   atomic counter).
//! - Multithreading.  A single pool of interned strings is shared by all
//!   threads in the program.  The pool is a lock-free [`papaya::HashMap`];
//!   interning, cloning and dropping never take a lock. The pool's own
//!   reference to a string is released through papaya's deferred
//!   reclamation, so a thread that found an entry can always finish
//!   reading it (see [`retire_if_dead`]).
//! - Thin. An [`InternedString`] is one pointer (8 bytes) to a header that
//!   holds the reference count, the byte length and the position of the
//!   `=` in a `name=value` label. The original held an `Arc<[u8]>`, a fat
//!   pointer whose 16-byte slot per label dominated the memory of a
//!   `MetricName` once the pool had deduplicated the strings themselves
//!   (see `tools/interning_report.sh`). Keeping the separator in the
//!   header makes [`InternedString::name`] and [`InternedString::value`]
//!   O(1) slices, so a label needs no side structure to be split.
//!
//! # Example
//! ```ignore
//! use valkey_timeseries::common::string_interner::InternedString;
//! let x = InternedString::new("hello");
//! let y: InternedString = "world".into();
//! assert_ne!(x, y);
//! assert_eq!(x, InternedString::new("hello"));
//! assert_eq!(&*x, "hello"); // dereference an InternedString like a pointer
//! let l = InternedString::new_pair("env", "prod");
//! assert_eq!((l.name(), l.value()), ("env", "prod"));
//! ```

use ahash::RandomState;
use get_size2::GetSize;
use min_max_heap::MinMaxHeap;
use papaya::{Guard, HashMap};
use smallvec::SmallVec;
use std::alloc::{Layout, alloc, dealloc, handle_alloc_error};
use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::fmt;
use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::ptr::NonNull;
use std::str::FromStr;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicUsize, Ordering, fence};

/// The pool: every live interned string, keyed by content. Used as a set;
/// `papaya::HashSet` lacks the conditional insert/remove this needs.
type StringPool = HashMap<PoolEntry, (), RandomState>;

/// A pool of interned strings and the memory it accounts for.
pub(crate) struct Interner {
    pool: StringPool,
    /// Total memory used by the interned strings in `pool`.
    memory_used: AtomicUsize,
}

impl Interner {
    fn new() -> Self {
        Self {
            pool: HashMap::builder().hasher(RandomState::new()).build(),
            memory_used: AtomicUsize::new(0),
        }
    }
}

static GLOBAL_INTERNER: LazyLock<Interner> = LazyLock::new(Interner::new);

/// The interner every string goes through: the process-wide one.
#[cfg(not(test))]
#[inline(always)]
fn interner() -> &'static Interner {
    &GLOBAL_INTERNER
}

/// The interner every string goes through: the process-wide one, unless the calling thread
/// has installed a private one ([`test_pool`]).
#[cfg(test)]
#[inline]
fn interner() -> &'static Interner {
    test_pool::installed().unwrap_or(&GLOBAL_INTERNER)
}

/// Per-test string pools.
///
/// The pool is process-wide, and ~1400 other tests intern concurrently with any one test, so
/// a test that inspects pool state — counts, memory, statistics — sees theirs too, and failed
/// intermittently (`#[serial]` only excludes other `#[serial]` tests). A test that inspects the
/// pool installs its own with [`isolated`] instead, which also needs no reset.
///
/// The override is per thread: strings interned or dropped on a thread the test spawns go
/// through that thread's pool, so such a test installs the same pool there with
/// [`TestPool::enter`]. Guards must outlive the strings interned under them — declare the guard
/// first, so it drops last.
#[cfg(test)]
pub(crate) mod test_pool {
    use super::Interner;
    use std::cell::Cell;

    thread_local! {
        static INSTALLED: Cell<Option<&'static Interner>> = const { Cell::new(None) };
    }

    pub(super) fn installed() -> Option<&'static Interner> {
        INSTALLED.with(Cell::get)
    }

    /// A handle to an isolated pool, for installing it on other threads.
    #[derive(Clone, Copy)]
    pub(crate) struct TestPool(&'static Interner);

    impl TestPool {
        /// Installs this pool on the calling thread until the guard drops.
        pub(crate) fn enter(self) -> TestPoolGuard {
            let previous = INSTALLED.with(|cell| cell.replace(Some(self.0)));
            TestPoolGuard {
                pool: self,
                previous,
            }
        }
    }

    /// Restores the thread's previous pool when dropped.
    pub(crate) struct TestPoolGuard {
        pool: TestPool,
        previous: Option<&'static Interner>,
    }

    impl TestPoolGuard {
        pub(crate) fn pool(&self) -> TestPool {
            self.pool
        }
    }

    impl Drop for TestPoolGuard {
        fn drop(&mut self) {
            INSTALLED.with(|cell| cell.set(self.previous));
        }
    }

    /// Installs a fresh, empty pool on the calling thread. Leaked: tests are short-lived, and a
    /// string that outlives its test must never find its pool freed.
    pub(crate) fn isolated() -> TestPoolGuard {
        TestPool(Box::leak(Box::new(Interner::new()))).enter()
    }
}

/// Marker in [`Header::name_len`] for a string with no `=`.
const NO_SEPARATOR: u32 = u32::MAX;

/// Largest supported strong-reference count. Keeping the count below the
/// signed pointer range matches the limit used by shared-pointer types such
/// as `Arc` and leaves no path for a count increment to wrap.
const MAX_REFCOUNT: usize = isize::MAX as usize;

/// Bytes between the start of an allocation and its payload.
const HEADER_SIZE: usize = size_of::<Header>();

/// What precedes the bytes of every interned string.
///
/// Sixteen bytes, like the `Arc<[u8]>` control block it replaces: the never-used
/// weak count became `len` (which the fat pointer used to carry in every holder)
/// and `name_len`. `repr(C)` fixes the payload offset at [`HEADER_SIZE`].
///
/// # Reference protocol
///
/// `strong` counts the pool's reference (while the entry is in the map) plus
/// one per holder. Without a lock, the count and the map membership have to
/// be reconciled by convention:
///
/// - **A pool entry whose count is 1 is dead.** Only the pool refers to it, and
///   nothing may bring it back: [`Node::try_acquire`] refuses to increment from
///   1, so a count of 1 is stable, which is what makes "remove it if its count
///   is 1" a sound test inside `remove_if`.
/// - **Whoever removes an entry releases the pool's reference** — the last
///   holder in [`InternedString::drop`], or an interner that stumbled on a dead
///   entry — and does so through [`Guard::defer_retire`], so the release runs
///   only once every thread that could have found the entry has unpinned. A
///   thread that reached a node through the map can therefore keep reading it
///   for as long as it stays pinned.
/// - A holder pins the map **before** giving up its own reference in the slow
///   path, so the node it is about to remove cannot be reclaimed under it.
#[repr(C)]
struct Header {
    /// References: one for the pool while the string is interned, one per holder.
    strong: AtomicUsize,
    /// Payload length in bytes.
    len: u32,
    /// Byte offset of the first `=`, or [`NO_SEPARATOR`]. Determined by the
    /// content alone, so every constructor agrees on it and deduplication
    /// cannot change what `name()` returns.
    name_len: u32,
}

impl Header {
    fn layout(len: usize) -> Layout {
        Layout::from_size_align(HEADER_SIZE + len, align_of::<Header>())
            .expect("interned string layout")
    }
}

/// Bytes the pool allocated for a string of `len` bytes: the header followed by the payload.
///
/// Every site that accounts for pool memory goes through here so the counters agree.
fn allocated_size(len: usize) -> usize {
    HEADER_SIZE + len
}

#[inline]
fn increment_refcount(current: usize) -> Option<usize> {
    current.checked_add(1).filter(|&next| next <= MAX_REFCOUNT)
}

/// One allocation: header immediately followed by `len` payload bytes.
///
/// The raw pointer shared by [`InternedString`] (a holder) and [`PoolEntry`]
/// (the pool's own reference). Each of those owns one count; this type owns
/// nothing and does no counting.
#[derive(Clone, Copy)]
struct Node(NonNull<Header>);

impl Node {
    /// Allocate a node holding `strong` references, all owned by the caller.
    fn allocate(bytes: &[u8], strong: usize) -> Node {
        let len = bytes.len();
        assert!(
            len < NO_SEPARATOR as usize,
            "interned string of {len} bytes exceeds u32::MAX"
        );
        let name_len = bytes
            .iter()
            .position(|&b| b == b'=')
            .map_or(NO_SEPARATOR, |i| i as u32);
        let layout = Header::layout(len);
        // SAFETY: the layout is non-zero-sized (it holds at least the header).
        let ptr = unsafe { alloc(layout) } as *mut Header;
        let Some(ptr) = NonNull::new(ptr) else {
            handle_alloc_error(layout)
        };
        // SAFETY: `ptr` is a fresh, properly aligned allocation of `layout`
        // bytes; the header is written in full before anything reads it and
        // the payload region starts `HEADER_SIZE` bytes in and holds `len` bytes.
        unsafe {
            ptr.write(Header {
                strong: AtomicUsize::new(strong),
                len: len as u32,
                name_len,
            });
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                ptr.as_ptr().cast::<u8>().add(HEADER_SIZE),
                len,
            );
        }
        Node(ptr)
    }

    #[inline]
    fn header(&self) -> &Header {
        // SAFETY: the node is alive for as long as any reference it counts
        // exists, and every `Node` handed out is held by such a reference.
        unsafe { self.0.as_ref() }
    }

    #[inline]
    fn len(&self) -> usize {
        self.header().len as usize
    }

    #[inline]
    fn bytes(&self) -> &[u8] {
        // SAFETY: the payload starts `HEADER_SIZE` bytes after the header and
        // is `len` bytes long, written in full by `allocate`.
        unsafe {
            std::slice::from_raw_parts(self.0.as_ptr().cast::<u8>().add(HEADER_SIZE), self.len())
        }
    }

    #[inline]
    fn strong(&self) -> &AtomicUsize {
        &self.header().strong
    }

    /// Take one reference.
    #[inline]
    fn retain(&self) {
        // Relaxed is enough for an increment: the holder cloning from already
        // owns a reference, so the count cannot reach zero underneath us
        // (same reasoning as `Arc::clone`).
        let strong = self.strong();
        let mut current = strong.load(Ordering::Relaxed);
        loop {
            let Some(next) = increment_refcount(current) else {
                // Continuing would make a safe `clone` wrap the count and
                // eventually allow a live allocation to be freed.
                std::process::abort();
            };
            match strong.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => return,
                Err(seen) => current = seen,
            }
        }
    }

    /// Take one reference to a node found through the pool, unless the entry
    /// is dead (count 1: only the pool's reference is left and a remover is on
    /// its way). See the protocol on [`Header`].
    #[inline]
    fn try_acquire(&self) -> bool {
        let strong = self.strong();
        let mut current = strong.load(Ordering::Acquire);
        loop {
            if current < 2 {
                return false;
            }
            let Some(next) = increment_refcount(current) else {
                // See `retain`: refcount overflow would invalidate the
                // ownership protocol, so it cannot be allowed to continue.
                std::process::abort();
            };
            match strong.compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return true,
                Err(seen) => current = seen,
            }
        }
    }

    /// Give back one reference, freeing the allocation with the last one.
    #[inline]
    fn release(self) {
        if self.strong().fetch_sub(1, Ordering::Release) == 1 {
            // Every other release happened-before this fence; no reader remains.
            fence(Ordering::Acquire);
            self.dealloc();
        }
    }

    /// Free the allocation. The caller guarantees no reference to it remains.
    #[inline]
    fn dealloc(self) {
        let layout = Header::layout(self.len());
        // SAFETY: by contract no reference to this node exists any more;
        // `layout` is the one it was allocated with.
        unsafe { dealloc(self.0.as_ptr().cast::<u8>(), layout) }
    }
}

/// Retire `node`'s pool entry if it is dead — still in the map, and with the
/// pool's reference as its only one. Returns whether this call did the removal.
///
/// The entry owns that reference, so Papaya releases it by dropping the key
/// only after the entry is unreachable from every table involved in an
/// incremental resize.
///
/// Both the last holder and an interner that finds a dead entry call this;
/// `remove_if` makes exactly one of them succeed, and the pointer check keeps
/// a lookalike entry (same bytes, different allocation) untouched.
fn retire_if_dead(node: Node, guard: &impl Guard) -> bool {
    let interner = interner();
    let removed = interner.pool.remove_if(
        node.bytes(),
        |entry, _| entry.0.0 == node.0 && node.strong().load(Ordering::Acquire) == 1,
        guard,
    );
    if !matches!(removed, Ok(Some(_))) {
        return false;
    }
    interner
        .memory_used
        .fetch_sub(allocated_size(node.len()), Ordering::SeqCst);
    true
}

// SAFETY: a node is immutable after construction except for its atomic count,
// so sharing pointers to it across threads is sound, exactly as for `Arc`.
unsafe impl Send for Node {}
unsafe impl Sync for Node {}

/// A pool key. Hashes and compares by content, and borrows as `[u8]`, so the
/// map can be probed with the bytes of a candidate string before anything is
/// allocated for it.
///
/// It owns the pool's reference to the node. Papaya may keep this key reachable
/// from an older table while incrementally resizing, so releasing the reference
/// in [`Drop`] ensures the node outlives every such lookup.
struct PoolEntry(Node);

impl PoolEntry {
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        self.0.bytes()
    }
}

impl Hash for PoolEntry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Must match `<[u8] as Hash>::hash` for `Borrow<[u8]>` lookups to land.
        self.as_bytes().hash(state);
    }
}

impl PartialEq for PoolEntry {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl Eq for PoolEntry {}

impl Borrow<[u8]> for PoolEntry {
    fn borrow(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl Drop for PoolEntry {
    fn drop(&mut self) {
        self.0.release();
    }
}

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
    /// Total allocated memory for this string (header + data).
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
    /// Share of the *pool's* hypothetical uninterned cost that interning avoids:
    /// `memory_saved_bytes / (memory_saved_bytes + total_stats.allocated) * 100`.
    ///
    /// This is a pool-efficiency figure, not a label-memory one, and it runs high (99 %+ on a
    /// fleet-shaped label set) because both sides of the ratio count only heap allocations. A
    /// holder pays for a [`InternedString`] slot as well, and pays for it either way, so the
    /// slot cancels out of the numerator but belongs in the denominator of any claim about
    /// what interning saves overall. [`Self::storage_saved_pct`] is that claim; prefer it when
    /// the question is "how much memory do labels cost", and use this one when the question is
    /// "how well is the pool deduplicating".
    pub memory_saved_pct: f64,
    /// Live holders across the whole pool: `Σ ref_count`, so one per outstanding
    /// [`InternedString`] value. Equivalently, the number of label occurrences when every
    /// holder is a label in a series.
    pub holder_count: usize,
    /// What those holders spend on slots: `holder_count * size_of::<InternedString>()`.
    /// Unaffected by interning — an uninterned layout with one allocation per holder pays the
    /// same slot, only pointing somewhere else.
    pub holder_slot_bytes: usize,
    /// What interned strings actually cost right now: `total_stats.allocated +
    /// holder_slot_bytes`. On a fleet-shaped label set the slots dominate, which is the point
    /// of reporting them.
    pub total_storage_bytes: usize,
    /// Share of total string storage that interning avoids, counting holder slots on both
    /// sides: `memory_saved_bytes / (total_storage_bytes + memory_saved_bytes) * 100`.
    ///
    /// The honest whole-storage figure, and always at or below [`Self::memory_saved_pct`].
    /// Still a slight over-statement of what a *series* saves: the pool cannot see the
    /// per-series container that holds the slots (`MetricName`'s `Arc<[InternedString]>`
    /// header), and it counts transient holders — a clone on the stack — alongside stored
    /// ones.
    pub storage_saved_pct: f64,
}

/// A pointer to an interned, reference-counted, and immutable string object.
///
/// One machine word. The interned string will be held in memory only until
/// its reference count reaches zero.
///
/// Labels are interned as `name=value`; [`Self::name`] and [`Self::value`]
/// return the two halves without scanning, from the separator position the
/// header records at intern time.
///
/// # Example
/// ```rust
/// use valkey_timeseries::common::string_interner::InternedString;
///
/// let x = InternedString::new("hello");
/// let y: InternedString = "world".into();
/// assert_ne!(x, y);
/// assert_eq!(x, InternedString::new("hello"));
/// assert_eq!(&*x, "hello"); // dereference an InternedString like a pointer
/// ```
pub struct InternedString(Node);

/// Buffer for assembling a `name=value` candidate before the pool is probed;
/// sized so ordinary labels never touch the heap for it.
type PairBuf = SmallVec<[u8; 128]>;

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
        Self::intern(val.as_bytes())
    }

    /// Intern the label `name=value` without building the string first: the
    /// pool is probed with the bytes on the stack, and only a miss allocates.
    pub fn new_pair(name: &str, value: &str) -> Self {
        let mut buf = PairBuf::with_capacity(name.len() + 1 + value.len());
        buf.extend_from_slice(name.as_bytes());
        buf.push(b'=');
        buf.extend_from_slice(value.as_bytes());
        Self::intern(&buf)
    }

    fn intern(bytes: &[u8]) -> InternedString {
        let interner = interner();
        let guard = interner.pool.guard();
        loop {
            if let Some((entry, _)) = interner.pool.get_key_value(bytes, &guard) {
                let node = entry.0;
                if node.try_acquire() {
                    return InternedString(node);
                }
                // Dead entry: its last holder is retiring it. Help, then look again.
                retire_if_dead(node, &guard);
                continue;
            }

            // Absent: insert a node carrying the pool's reference and ours.
            let node = Node::allocate(bytes, 2);
            match interner
                .pool
                .try_insert_with(PoolEntry(node), || (), &guard)
            {
                Ok(_) => {
                    interner
                        .memory_used
                        .fetch_add(allocated_size(bytes.len()), Ordering::SeqCst);
                    return InternedString(node);
                }
                Err(_) => {
                    // Another thread inserted the same bytes first. The
                    // rejected temporary `PoolEntry` has dropped and released
                    // its pool reference, leaving this prospective holder
                    // reference for us to release.
                    node.release();
                }
            }
        }
    }

    /// A new holder of `node`, which the caller already holds a reference to.
    #[inline]
    fn retained(node: Node) -> InternedString {
        node.retain();
        InternedString(node)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.bytes()
    }

    #[inline]
    pub fn as_str(&self) -> &str {
        // SAFETY: we only intern valid UTF-8 strings
        debug_assert!(
            std::str::from_utf8(self.as_bytes()).is_ok(),
            "InternedString: interned bytes are not valid UTF-8"
        );
        unsafe { std::str::from_utf8_unchecked(self.as_bytes()) }
    }

    /// True when the string contains a `=`, i.e. it splits as a label.
    #[inline]
    pub fn is_pair(&self) -> bool {
        self.0.header().name_len != NO_SEPARATOR
    }

    /// The part before the first `=`; the whole string when there is none.
    #[inline]
    pub fn name(&self) -> &str {
        let s = self.as_str();
        match self.0.header().name_len {
            NO_SEPARATOR => s,
            n => &s[..n as usize],
        }
    }

    /// The part after the first `=`; empty when there is none.
    #[inline]
    pub fn value(&self) -> &str {
        let s = self.as_str();
        match self.0.header().name_len {
            NO_SEPARATOR => "",
            n => &s[n as usize + 1..],
        }
    }

    /// `(name, value)` for a string containing `=`, else `None`.
    #[inline]
    pub fn split_pair(&self) -> Option<(&str, &str)> {
        self.is_pair().then(|| (self.name(), self.value()))
    }

    /// Return the number of references to this value.
    pub fn ref_count(&self) -> usize {
        // The pool holds one reference; we return the number of
        // references held by actual clients.
        self.0.strong().load(Ordering::Acquire) - 1
    }

    /// Return true if this is the only reference to this value.
    pub fn is_unique(&self) -> bool {
        self.ref_count() == 1
    }

    /// Bytes the pool allocated for this value: the header (reference count, length,
    /// separator) followed by the payload. See [`allocated_size`].
    pub fn allocated_size(&self) -> usize {
        allocated_size(self.len())
    }

    /// This holder's share of [`Self::allocated_size`].
    ///
    /// A single pool allocation backs every series carrying the same `label=value` pair, so
    /// charging all of it to each holder would multiply-count it: summing `MEMORY USAGE` over a
    /// keyspace would report far more memory than the module holds. Splitting it evenly keeps
    /// that sum equal to the pool's real footprint, which is the number capacity planning wants.
    pub fn amortized_size(&self) -> usize {
        self.allocated_size().div_ceil(self.ref_count().max(1))
    }

    /// Return the number of unique interned strings.
    pub fn interned_count() -> usize {
        interner().pool.len()
    }

    /// Return the total memory used by all interned strings.
    pub fn memory_used() -> usize {
        interner().memory_used.load(Ordering::Relaxed)
    }

    /// Collect statistics about the interned string pool, including the top `k`
    /// strings by allocated size and by external reference count.
    ///
    /// Passing `k = 0` skips the top-K collection entirely (both `top_k_by_size` and
    /// `top_k_by_ref` will be empty).
    pub fn get_stats_with_top_k(k: usize) -> Stats {
        let interner = interner();
        let guard = interner.pool.guard();
        let mut stats = Stats::default();

        // MinMaxHeap allows us to efficiently track top-K and extract the max values
        let mut size_heap: MinMaxHeap<TopKBySize> = MinMaxHeap::new();
        let mut ref_heap: MinMaxHeap<TopKByRef> = MinMaxHeap::new();

        for (entry, _) in interner.pool.iter(&guard) {
            let node = entry.0;
            let strong = node.strong().load(Ordering::Acquire);
            if strong < 2 {
                // Dead: being retired by its last holder. Not a live string.
                continue;
            }
            let ref_count = strong - 1; // exclude the pool's reference
            let bytes = node.len();
            let allocated = allocated_size(bytes);

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

            // One slot per holder, interned or not: it is the denominator's share of the cost
            // that dedup never touches.
            stats.holder_count += ref_count;

            // Each duplicate reference that shares this allocation is a saved copy.
            // ref_count includes the pool's own ref, so duplicates = ref_count - 1.
            // (strings with ref_count == 1 have no duplicates → zero saving)
            if ref_count > 1 {
                stats.memory_saved_bytes += allocated * (ref_count - 1);
            }

            if k > 0 {
                // The entry may have died since the load above; then it is
                // not a live string and is skipped like the ones caught earlier.
                if !node.try_acquire() {
                    continue;
                }
                let entry = TopKEntry {
                    value: InternedString(node),
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

        // The same saving against everything the strings cost, slots included. The slot is in
        // both layouts, so it enters the denominator only, which is exactly why this figure
        // sits below `memory_saved_pct` instead of alongside it.
        stats.holder_slot_bytes = stats.holder_count * size_of::<InternedString>();
        stats.total_storage_bytes = stats.total_stats.allocated + stats.holder_slot_bytes;
        let storage_uninterned = stats.total_storage_bytes + stats.memory_saved_bytes;
        stats.storage_saved_pct = if storage_uninterned == 0 {
            0.0
        } else {
            stats.memory_saved_bytes as f64 / storage_uninterned as f64 * 100.0
        };

        stats
    }

    pub fn get_stats() -> Stats {
        Self::get_stats_with_top_k(0)
    }
}

impl Clone for InternedString {
    fn clone(&self) -> Self {
        Self::retained(self.0)
    }
}

impl Drop for InternedString {
    fn drop(&mut self) {
        let node = self.0;
        let strong = node.strong();
        let mut current = strong.load(Ordering::Acquire);
        loop {
            match current {
                // Another holder remains besides the pool: a plain decrement.
                3.. => match strong.compare_exchange_weak(
                    current,
                    current - 1,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return,
                    Err(seen) => current = seen,
                },
                // The pool and us: we are the last holder. Pin first, so that
                // the node outlives our use of it below however the retirement
                // races (see `Header`), then give up our reference — the entry
                // is dead from that moment — and retire it.
                2 => {
                    let guard = interner().pool.guard();
                    match strong.compare_exchange(2, 1, Ordering::AcqRel, Ordering::Acquire) {
                        Ok(_) => {
                            retire_if_dead(node, &guard);
                            return;
                        }
                        Err(seen) => current = seen,
                    }
                }
                // Only us: the pool's reference is already gone (the entry was
                // retired without us, which the protocol rules out, but a
                // decrement is the safe answer either way).
                1 => {
                    node.release();
                    return;
                }
                0 => unreachable!("dropping an InternedString with no reference"),
            }
        }
    }
}

impl fmt::Debug for InternedString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.as_str(), f)
    }
}

impl GetSize for InternedString {
    /// A holder's share of the pool allocation; see [`Self::amortized_size`].
    fn get_heap_size(&self) -> usize {
        self.amortized_size()
    }
}

impl Borrow<str> for InternedString {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

/// Hashes by content, not pointer: fingerprints derived from labels must be
/// stable across processes.
impl Hash for InternedString {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_bytes().hash(state);
    }
}

impl AsRef<[u8]> for InternedString {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl AsRef<str> for InternedString {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Deref for InternedString {
    type Target = str;
    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl Display for InternedString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
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
    /// Panics if `s` is not valid UTF-8.
    ///
    /// Every `InternedString` exposes its bytes as `str`; validate before
    /// storing them so `as_str` can safely use `from_utf8_unchecked`.
    fn from(s: &[u8]) -> Self {
        assert!(
            std::str::from_utf8(s).is_ok(),
            "InternedString bytes must be valid UTF-8"
        );
        Self::intern(s)
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
        self.0.0 == other.0.0
    }
}

impl Eq for InternedString {}

impl PartialOrd for InternedString {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for InternedString {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_bytes().cmp(other.as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::{InternedString, MAX_REFCOUNT, Node, PoolEntry, increment_refcount, test_pool};
    use ahash::{HashSet, HashSetExt};
    use std::collections::HashMap;
    use std::ops::Deref;
    use std::sync::atomic::Ordering;
    use std::thread;

    // Tests that inspect pool state run against a pool of their own (`test_pool::isolated`).

    #[test]
    fn refcount_increment_stops_at_the_limit() {
        assert_eq!(increment_refcount(MAX_REFCOUNT - 1), Some(MAX_REFCOUNT));
        assert_eq!(increment_refcount(MAX_REFCOUNT), None);
        assert_eq!(increment_refcount(usize::MAX), None);
    }

    #[test]
    fn pool_entry_owns_one_reference() {
        let node = Node::allocate(b"pool-entry", 2);
        drop(PoolEntry(node));
        assert_eq!(node.strong().load(Ordering::Acquire), 1);
        node.release();
    }

    // Test basic functionality.
    #[test]
    fn basic() {
        let _pool = test_pool::isolated();
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
    fn sorting() {
        let _pool = test_pool::isolated();
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
    fn sequential() {
        let _pool = test_pool::isolated();
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
    fn multithreading1() {
        let _pool = test_pool::isolated();
        let pool = _pool.pool();
        let mut thread_handles = vec![];
        for _i in 0..10 {
            let t = thread::spawn({
                move || {
                    let _pool = pool.enter();
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

    #[test]
    fn test_new_creates_interned_string() {
        let _pool = test_pool::isolated();

        let s1 = InternedString::new("hello");
        let s2 = InternedString::new("hello");

        // Both should refer to the same interned value
        assert_eq!(s1, s2);
        assert_eq!(s1.len(), 5);
        assert!(!s1.is_empty());
    }

    #[test]
    fn test_different_interned_strings_are_not_equal() {
        let _pool = test_pool::isolated();

        let s1 = InternedString::new("hello");
        let s2 = InternedString::new("world");

        assert_ne!(s1, s2);
    }

    #[test]
    fn test_clone_interned_string_increases_refcount() {
        let _pool = test_pool::isolated();

        let s1 = InternedString::new("test");
        let initial_refcount = s1.ref_count();

        let s2 = s1.clone();

        assert_eq!(s1.ref_count(), initial_refcount + 1);
        assert_eq!(s2.ref_count(), initial_refcount + 1);
        assert_eq!(s1, s2);
    }

    #[test]
    fn test_drop_decreases_interned_string_refcount() {
        let _pool = test_pool::isolated();

        let s1 = InternedString::new("test");
        let s2 = s1.clone();
        let initial_refcount = s1.ref_count();

        drop(s1);

        assert_eq!(s2.ref_count(), initial_refcount - 1);
    }

    #[test]
    fn test_memory_tracking_on_first_creation() {
        let _pool = test_pool::isolated();

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
    fn test_memory_tracking_on_drop() {
        let _pool = test_pool::isolated();

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
    fn test_memory_tracking_with_different_strings() {
        let _pool = test_pool::isolated();

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
    fn test_interned_string_deref_trait() {
        let _pool = test_pool::isolated();

        let s = InternedString::new("test_string");

        // Test various string methods through Deref
        assert_eq!(s.len(), 11);
        assert!(s.contains("test"));
        assert!(s.starts_with("test"));
        assert!(s.ends_with("string"));
        assert_eq!(s.to_uppercase(), "TEST_STRING");
    }

    #[test]
    fn test_partial_ord_and_ord() {
        let _pool = test_pool::isolated();

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
    fn test_hash_consistency() {
        let _pool = test_pool::isolated();

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
    fn test_empty_string() {
        let _pool = test_pool::isolated();

        let empty1 = InternedString::new("");
        let empty2 = InternedString::new("");

        assert_eq!(empty1, empty2);
        assert_eq!(empty1.deref(), "");
        assert_eq!(empty1.len(), 0);
        assert!(empty1.is_empty());
    }

    #[test]
    fn test_unicode_strings() {
        let _pool = test_pool::isolated();

        let unicode1 = InternedString::new("🦀 Rust");
        let unicode2 = InternedString::new("🦀 Rust");
        let different = InternedString::new("🐍 Python");

        assert_eq!(unicode1, unicode2);
        assert_ne!(unicode1, different);
        assert_eq!(unicode1.deref(), "🦀 Rust");
    }

    #[test]
    #[should_panic(expected = "InternedString bytes must be valid UTF-8")]
    fn byte_slice_constructor_rejects_invalid_utf8() {
        let _ = InternedString::from(&[0xff][..]);
    }

    #[test]
    fn test_very_long_strings() {
        let _pool = test_pool::isolated();

        let long_string = "a".repeat(10000);
        let s1 = InternedString::new(&long_string);
        let s2 = InternedString::new(&long_string);

        assert_eq!(s1, s2);
        assert_eq!(s1.len(), 10000);
        assert_eq!(s1.ref_count(), 2);
    }

    // ── Basic top-K by size ──────────────────────────────────────────────────

    #[test]
    fn test_top_k_by_size_returns_k_entries() {
        let _pool = test_pool::isolated();

        let _s1 = InternedString::new("a");
        let _s2 = InternedString::new("bbb");
        let _s3 = InternedString::new("ccccc");
        let _s4 = InternedString::new("ddddddd");
        let _s5 = InternedString::new("eeeeeeeee");

        let stats = InternedString::get_stats_with_top_k(3);
        assert_eq!(stats.top_k_by_size.len(), 3);
    }

    #[test]
    fn test_top_k_by_size_sorted_descending() {
        let _pool = test_pool::isolated();

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
    fn test_top_k_by_size_contains_largest() {
        let _pool = test_pool::isolated();

        const LARGE_STRING: &str = "this_is_the_largest_string_in_pool";

        let _s1 = InternedString::new("tiny");
        let _s2 = InternedString::new("medium_string");
        let _s3 = InternedString::new(LARGE_STRING);

        let stats = InternedString::get_stats_with_top_k(1);
        assert_eq!(stats.top_k_by_size.len(), 1);
        assert_eq!(&*stats.top_k_by_size[0].value, LARGE_STRING);
    }

    #[test]
    fn test_top_k_by_size_excludes_smaller_strings() {
        let _pool = test_pool::isolated();

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
    fn test_top_k_by_ref_returns_k_entries() {
        let _pool = test_pool::isolated();

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
    fn test_top_k_by_ref_sorted_descending() {
        let _pool = test_pool::isolated();

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
    fn test_top_k_by_ref_most_referenced_is_first() {
        let _pool = test_pool::isolated();

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
    fn test_top_k_by_ref_excludes_low_ref_strings() {
        let _pool = test_pool::isolated();

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
    fn test_top_k_entry_value_field() {
        let _pool = test_pool::isolated();

        let _s = InternedString::new("check_value");

        let stats = InternedString::get_stats_with_top_k(1);
        assert_eq!(
            stats.top_k_by_size[0].value,
            InternedString::new("check_value")
        );
    }

    #[test]
    fn test_top_k_entry_bytes_field() {
        let _pool = test_pool::isolated();

        let _s = InternedString::new("hello");

        let stats = InternedString::get_stats_with_top_k(1);
        assert_eq!(stats.top_k_by_size[0].bytes, "hello".len());
    }

    #[test]
    fn test_top_k_entry_ref_count_field() {
        let _pool = test_pool::isolated();

        let s = InternedString::new("ref_check");
        let _c1 = s.clone();
        let _c2 = s.clone();
        // 3 external refs total

        let stats = InternedString::get_stats_with_top_k(1);
        assert_eq!(stats.top_k_by_ref[0].ref_count, 3);
    }

    #[test]
    fn test_top_k_entry_allocated_gte_bytes() {
        let _pool = test_pool::isolated();

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
    fn test_top_k_zero_returns_empty_vecs() {
        let _pool = test_pool::isolated();

        let _s = InternedString::new("ignored");

        let stats = InternedString::get_stats_with_top_k(0);
        assert!(stats.top_k_by_size.is_empty());
        assert!(stats.top_k_by_ref.is_empty());
    }

    #[test]
    fn test_top_k_zero_still_populates_aggregate_stats() {
        let _pool = test_pool::isolated();

        let _s1 = InternedString::new("aaa");
        let _s2 = InternedString::new("bbbb");

        let stats = InternedString::get_stats_with_top_k(0);
        assert_eq!(stats.total_stats.count, 2);
        assert!(stats.total_stats.bytes > 0);
        assert!(!stats.by_size_stats.is_empty());
        assert!(!stats.by_ref_stats.is_empty());
    }

    #[test]
    fn test_top_k_empty_pool() {
        let _pool = test_pool::isolated();

        let stats = InternedString::get_stats_with_top_k(5);
        assert!(stats.top_k_by_size.is_empty());
        assert!(stats.top_k_by_ref.is_empty());
        assert_eq!(stats.total_stats.count, 0);
    }

    #[test]
    fn test_top_k_larger_than_pool_returns_all() {
        let _pool = test_pool::isolated();

        let _s1 = InternedString::new("x");
        let _s2 = InternedString::new("yy");
        let _s3 = InternedString::new("zzz");

        // Request more than pool size
        let stats = InternedString::get_stats_with_top_k(100);
        assert_eq!(stats.top_k_by_size.len(), 3);
        assert_eq!(stats.top_k_by_ref.len(), 3);
    }

    #[test]
    fn test_top_k_equal_to_pool_size_returns_all() {
        let _pool = test_pool::isolated();

        let _s1 = InternedString::new("p");
        let _s2 = InternedString::new("qq");
        let _s3 = InternedString::new("rrr");

        let stats = InternedString::get_stats_with_top_k(3);
        assert_eq!(stats.top_k_by_size.len(), 3);
        assert_eq!(stats.top_k_by_ref.len(), 3);
    }

    #[test]
    fn test_top_k_single_string_in_pool() {
        let _pool = test_pool::isolated();

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
    fn test_top_k_k_equals_one() {
        let _pool = test_pool::isolated();

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
    fn test_get_stats_equivalent_to_top_k_zero() {
        let _pool = test_pool::isolated();

        let _s = InternedString::new("convenience");

        let stats = InternedString::get_stats();
        assert!(stats.top_k_by_size.is_empty());
        assert!(stats.top_k_by_ref.is_empty());
        assert_eq!(stats.total_stats.count, 1);
    }

    /// `memory_saved_pct` weighs the pool against one allocation per holder and stops there, so
    /// it climbs towards 100% as sharing improves however much the holders themselves cost.
    /// `storage_saved_pct` restates the same saving over total storage, where the slot each
    /// holder keeps either way sits in the denominator alone. The two are checked against hand
    /// arithmetic here because the gap between them is the whole point of reporting both.
    #[test]
    fn storage_saving_counts_the_slot_every_holder_keeps() {
        let _pool = test_pool::isolated();

        const HOLDERS: usize = 8;
        // 48 bytes of payload, so every figure below is exact in binary.
        let payload = "storage-saving-fixture".to_string() + &"x".repeat(26);
        assert_eq!(payload.len(), 48);

        let first = InternedString::new(&payload);
        let _clones: Vec<InternedString> = (1..HOLDERS).map(|_| first.clone()).collect();

        let stats = InternedString::get_stats();
        assert_eq!(stats.total_stats.count, 1, "fixture is the only live entry");

        let allocated = super::HEADER_SIZE + payload.len();
        assert_eq!(stats.total_stats.allocated, allocated);

        // One holder per outstanding `InternedString`, each paying for its own slot.
        assert_eq!(stats.holder_count, HOLDERS);
        assert_eq!(
            stats.holder_slot_bytes,
            HOLDERS * size_of::<InternedString>()
        );
        assert_eq!(
            stats.total_storage_bytes,
            allocated + stats.holder_slot_bytes
        );

        // Seven allocations avoided, out of the eight an uninterned layout would make.
        let saved = allocated * (HOLDERS - 1);
        assert_eq!(stats.memory_saved_bytes, saved);
        assert_eq!(
            stats.memory_saved_pct,
            saved as f64 / (saved + allocated) as f64 * 100.0
        );
        assert_eq!(
            stats.storage_saved_pct,
            saved as f64 / (saved + stats.total_storage_bytes) as f64 * 100.0
        );

        // 87.5% against the pool alone, 77.8% against everything the strings cost.
        assert!(
            stats.storage_saved_pct < stats.memory_saved_pct,
            "slots belong in the denominator: {} vs {}",
            stats.storage_saved_pct,
            stats.memory_saved_pct
        );
    }

    /// The slot is the whole difference between the two percentages, so a pool whose strings
    /// are shared by exactly one holder each saves nothing under either measure.
    #[test]
    fn unshared_strings_save_nothing_under_either_measure() {
        let _pool = test_pool::isolated();

        let _a = InternedString::new("unshared-alpha");
        let _b = InternedString::new("unshared-beta");

        let stats = InternedString::get_stats();
        assert_eq!(stats.holder_count, 2);
        assert_eq!(stats.memory_saved_bytes, 0);
        assert_eq!(stats.memory_saved_pct, 0.0);
        assert_eq!(stats.storage_saved_pct, 0.0);
        assert_eq!(
            stats.total_storage_bytes,
            stats.total_stats.allocated + 2 * size_of::<InternedString>()
        );
    }

    #[test]
    fn test_top_k_entries_ref_count_excludes_pool_ref() {
        let _pool = test_pool::isolated();

        // One external reference only
        let _s = InternedString::new("solo");

        let stats = InternedString::get_stats_with_top_k(1);
        // Pool holds 1 ref, `_s` holds 1 ref → strong_count = 2 → external = 1
        assert_eq!(stats.top_k_by_ref[0].ref_count, 1);
    }

    #[test]
    fn test_top_k_by_size_ties_all_included_when_k_gte_pool_size() {
        let _pool = test_pool::isolated();

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
    fn test_top_k_by_ref_ties_all_included_when_k_gte_pool_size() {
        let _pool = test_pool::isolated();

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

    // ── thin representation ────────────────────────────────────────────────

    #[test]
    fn interned_string_is_one_pointer() {
        let _pool = test_pool::isolated();
        assert_eq!(size_of::<InternedString>(), size_of::<usize>());
        assert_eq!(size_of::<Option<InternedString>>(), size_of::<usize>());
        // The header replaces the `Arc<[u8]>` control block byte for byte.
        assert_eq!(super::HEADER_SIZE, 2 * size_of::<usize>());
        assert_eq!(InternedString::new("abc").allocated_size(), 16 + 3);
    }

    #[test]
    fn name_and_value_split_at_first_separator() {
        let _pool = test_pool::isolated();
        let l = InternedString::new_pair("env", "prod");
        assert!(l.is_pair());
        assert_eq!(l.name(), "env");
        assert_eq!(l.value(), "prod");
        assert_eq!(l.split_pair(), Some(("env", "prod")));
        assert_eq!(&*l, "env=prod");

        // A value may itself contain `=`; only the first one splits.
        let eq = InternedString::new_pair("q", "a=b=c");
        assert_eq!(eq.split_pair(), Some(("q", "a=b=c")));

        let bare = InternedString::new("no_separator");
        assert!(!bare.is_pair());
        assert_eq!(bare.name(), "no_separator");
        assert_eq!(bare.value(), "");
        assert_eq!(bare.split_pair(), None);

        let empty = InternedString::default();
        assert_eq!(empty.len(), 0);
        assert_eq!(empty.name(), "");
        assert_eq!(empty.split_pair(), None);

        // Empty halves are still pairs.
        let empty_value = InternedString::new_pair("k", "");
        assert_eq!(empty_value.split_pair(), Some(("k", "")));
        let empty_name = InternedString::new("=v");
        assert_eq!(empty_name.split_pair(), Some(("", "v")));
    }

    #[test]
    fn constructors_deduplicate_to_one_allocation() {
        let _pool = test_pool::isolated();
        let a = InternedString::new_pair("host", "h1");
        let b = InternedString::new("host=h1");
        let c: InternedString = String::from("host=h1").into();
        let d = InternedString::from(b"host=h1".as_slice());
        assert_eq!(a, b);
        assert_eq!(b, c);
        assert_eq!(c, d);
        assert_eq!(a.ref_count(), 4);
        assert_eq!(InternedString::interned_count(), 1);
        assert_eq!(InternedString::memory_used(), 16 + "host=h1".len());
        // The split is a property of the content, whichever constructor ran first.
        assert_eq!(b.split_pair(), Some(("host", "h1")));
    }

    #[test]
    fn long_pair_beyond_stack_buffer() {
        let _pool = test_pool::isolated();
        let value = "v".repeat(1000);
        let l = InternedString::new_pair("id", &value);
        assert_eq!(l.name(), "id");
        assert_eq!(l.value(), value);
        assert_eq!(l.allocated_size(), 16 + 3 + 1000);
    }

    /// Two last holders dropping at once must not both take the fast path and
    /// leave a holder-less entry in the pool.
    #[test]
    fn racing_last_holders_retire_the_entry() {
        let _pool = test_pool::isolated();
        for round in 0..200 {
            let s = format!("race{round}");
            let a = InternedString::new(&s);
            let b = a.clone();
            let c = a.clone();
            let handles: Vec<_> = [a, b, c]
                .into_iter()
                .map(|h| {
                    let pool = _pool.pool();
                    thread::spawn(move || {
                        let _pool = pool.enter();
                        drop(h)
                    })
                })
                .collect();
            for h in handles {
                h.join().unwrap();
            }
            assert_eq!(InternedString::interned_count(), 0, "round {round}");
        }
        assert_eq!(InternedString::memory_used(), 0);
    }

    /// Interning a string while its last holder is dropping it must yield a
    /// live value either way.
    #[test]
    fn intern_races_drop_of_last_holder() {
        let _pool = test_pool::isolated();
        for round in 0..200 {
            let s = format!("churn{round}");
            let holder = InternedString::new(&s);
            let pool = _pool.pool();
            let dropper = thread::spawn(move || {
                let _pool = pool.enter();
                drop(holder)
            });
            let re = InternedString::new(&s);
            dropper.join().unwrap();
            assert_eq!(&*re, s);
            assert_eq!(re.ref_count(), 1);
            assert_eq!(InternedString::interned_count(), 1);
            drop(re);
            assert_eq!(InternedString::interned_count(), 0);
        }
    }

    /// Many threads interning, cloning and dropping the same few strings: two
    /// live values with equal content must always be the same allocation, and
    /// nothing may be left behind.
    #[test]
    fn concurrent_churn_keeps_one_allocation_per_content() {
        let _pool = test_pool::isolated();
        const NAMES: [&str; 4] = ["churn=a", "churn=b", "churn=c", "churn=d"];
        let pool = _pool.pool();
        let handles: Vec<_> = (0..8)
            .map(|t| {
                thread::spawn(move || {
                    let _pool = pool.enter();
                    for i in 0..20_000usize {
                        let s = NAMES[(i + t) % NAMES.len()];
                        let a = InternedString::new(s);
                        let b = InternedString::new_pair("churn", &s[6..]);
                        assert_eq!(a, b, "{s}: equal content, distinct allocations");
                        assert_eq!(a.value(), &s[6..]);
                        if i % 3 == 0 {
                            drop(a.clone());
                        }
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(InternedString::interned_count(), 0);
        assert_eq!(InternedString::memory_used(), 0);
    }

    #[test]
    fn stats_top_k_holds_no_extra_references_afterwards() {
        let _pool = test_pool::isolated();
        let a = InternedString::new("solo");
        let stats = InternedString::get_stats_with_top_k(1);
        assert_eq!(stats.top_k_by_ref[0].ref_count, 1);
        drop(stats);
        assert_eq!(a.ref_count(), 1);
    }
}
