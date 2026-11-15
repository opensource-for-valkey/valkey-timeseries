//! A bounded, process-wide cache of compiled label regexes.
//!
//! Compiling a label regex under the module's limits costs 1.5–30 µs for everyday patterns and
//! milliseconds for large ones (`.{1,1000}` 3–7 ms, `(?:/[^/]{1,255}){1,8}` about 10 ms), and a
//! clone of the compiled regex costs 0.2–0.5 µs. Every `=~`/`!~` matcher in a `FILTER` argument
//! was compiled on every command, and again on each shard when a cluster fan-out decoded the
//! filters.
//!
//! The cache hands out clones, never the stored value. A compiled regex clone shares the program
//! behind an `Arc` and gets its own, empty match cache pool, so callers on different threads do
//! not contend on a pool, and the stored copy — which never runs a match — keeps no match cache.
//! Every caller of this cache only calls `is_match`.
//!
//! Patterns come from client input, so the cache is bounded in entries, in pattern length and in
//! bytes, and failed compiles are not cached. The byte bound matters: a compiled program may be
//! up to 4 MiB, and short patterns reach that (`.{1,1000}` is about 1 MiB, `(?:/[^/]{1,255}){1,8}`
//! 2.2 MiB), so a count alone would let a few hundred distinct patterns pin gigabytes. Everyday
//! patterns such as `prod-.*` or `api-[0-9]+` take 3–7 KB, so they fill every slot long before
//! the byte budget. A value heavier than an eighth of the budget is never stored: it is built
//! again on every use rather than flushing the cache.
//!
//! The cache is generic over what it keeps: [`RegexCache`] holds compiled regexes, and
//! `regex_utils` keeps whole decomposed label matchers in one too.

use crate::labels::regex::CompiledRegex;
use ahash::{AHashMap, AHashSet, RandomState};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

/// The number of compiled regexes retained.
pub(crate) const REGEX_CACHE_CAPACITY: usize = 256;

/// Longer patterns are compiled without being cached, so the keys of a full
/// cache stay within `REGEX_CACHE_CAPACITY` × this many bytes (1 MiB).
pub(crate) const REGEX_CACHE_MAX_PATTERN_LEN: usize = 4096;

/// The bytes the compiled-regex cache may hold, keys included. Its largest entry is an eighth,
/// 4 MiB, the most a pattern may compile to.
pub(crate) const REGEX_CACHE_MAX_BYTES: usize = 32 << 20;

/// The bytes each decomposed-matcher cache in `regex_utils` may hold. A matcher's regex is
/// usually also in the compiled-regex cache, and counted in both.
pub(crate) const MATCHER_CACHE_MAX_BYTES: usize = 16 << 20;

/// The heap a cached value keeps alive, as the cache charges it. An estimate is fine as long as
/// it does not undercount the large parts.
pub(crate) trait HeapSize {
    fn heap_size(&self) -> usize;
}

/// A bounded map from a pattern to the value built from it.
///
/// When an insert would go over the entry count or the byte budget, the least recently used
/// entries make room, at least a tenth of the capacity at a time, so a burst of new patterns
/// does not pay an eviction scan each. Values are handed out as clones, made outside the lock.
pub(crate) struct PatternCache<T> {
    capacity: usize,
    max_pattern_len: usize,
    max_bytes: usize,
    hasher: RandomState,
    inner: Mutex<Inner<T>>,
}

/// A bounded map from a fully anchored pattern to its compiled regex.
pub(crate) type RegexCache = PatternCache<CompiledRegex>;

struct Inner<T> {
    entries: AHashMap<Box<str>, Entry<T>>,
    /// The sum of the entries' sizes.
    bytes: usize,
    /// Advances on every hit and insertion: the recency stamp.
    clock: u64,
    /// With admission on second sighting, the patterns built once and not stored.
    seen: Option<Seen>,
}

/// Hashes of patterns built but not yet admitted, in two generations of `generation` hashes:
/// when the current one fills it becomes the previous one, and the old previous is dropped. A
/// pattern is admitted when it is built again while its hash is still in either, that is, before
/// about `2 × generation` other distinct patterns have been built. One that recurs less often
/// would have been evicted before its next use anyway.
struct Seen {
    current: AHashSet<u64>,
    previous: AHashSet<u64>,
    generation: usize,
}

impl Seen {
    /// Whether `hash` was seen before; it is recorded either way.
    fn check_and_mark(&mut self, hash: u64) -> bool {
        if self.current.contains(&hash) || self.previous.contains(&hash) {
            return true;
        }
        if self.current.len() >= self.generation {
            self.previous = std::mem::take(&mut self.current);
        }
        self.current.insert(hash);
        false
    }
}

struct Entry<T> {
    value: Arc<T>,
    /// What the entry is charged: the value's [`HeapSize`] and the key.
    size: usize,
    last_used: u64,
}

impl<T: Clone + HeapSize> PatternCache<T> {
    pub(crate) fn new(capacity: usize, max_pattern_len: usize, max_bytes: usize) -> Self {
        Self::with_seen(capacity, max_pattern_len, max_bytes, None)
    }

    /// A cache that stores a value only the second time its pattern is built (see [`Seen`]),
    /// so a stream of patterns that never recur costs no insertions or evictions. For values
    /// cheap enough to build that cache upkeep is a visible share of a miss.
    pub(crate) fn admitting_on_second_sighting(
        capacity: usize,
        max_pattern_len: usize,
        max_bytes: usize,
    ) -> Self {
        let seen = Seen {
            current: AHashSet::default(),
            previous: AHashSet::default(),
            generation: capacity,
        };
        Self::with_seen(capacity, max_pattern_len, max_bytes, Some(seen))
    }

    fn with_seen(
        capacity: usize,
        max_pattern_len: usize,
        max_bytes: usize,
        seen: Option<Seen>,
    ) -> Self {
        Self {
            capacity,
            max_pattern_len,
            max_bytes,
            hasher: RandomState::new(),
            inner: Mutex::new(Inner {
                entries: AHashMap::default(),
                bytes: 0,
                clock: 0,
                seen,
            }),
        }
    }

    /// The most one entry may be charged: an eighth of the budget, so a single heavy pattern
    /// cannot flush the cache.
    fn max_entry_bytes(&self) -> usize {
        self.max_bytes / 8
    }

    /// The value for `pattern`, building it with `compile` if it is not
    /// cached. `compile` runs without the cache lock held, so concurrent
    /// misses on one pattern may each compile it; the first to finish is kept.
    /// An error from `compile` is returned as it is and not cached.
    pub(crate) fn get_or_compile<E>(
        &self,
        pattern: &str,
        compile: impl FnOnce(&str) -> Result<T, E>,
    ) -> Result<T, E> {
        if self.capacity == 0 || pattern.len() > self.max_pattern_len {
            return compile(pattern);
        }
        if let Some(value) = self.lookup(pattern) {
            // Cloned outside the lock: a regex clone allocates its match cache pool.
            return Ok(T::clone(&value));
        }
        let value = compile(pattern)?;
        // Weighed outside the lock too.
        let size = value.heap_size() + pattern.len();
        if size <= self.max_entry_bytes() {
            let evicted = self.insert(pattern, &value, size);
            // Freed outside the lock as well.
            drop(evicted);
        }
        Ok(value)
    }

    fn lock(&self) -> MutexGuard<'_, Inner<T>> {
        // Only map operations run under the lock, so a panic cannot leave an
        // entry half-written; the worst a poisoned map holds is a stale stamp.
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn lookup(&self, pattern: &str) -> Option<Arc<T>> {
        let mut inner = self.lock();
        inner.clock += 1;
        let now = inner.clock;
        let entry = inner.entries.get_mut(pattern)?;
        entry.last_used = now;
        Some(Arc::clone(&entry.value))
    }

    /// Stores a copy of `value`, charged `size` bytes, under `pattern` unless another thread
    /// stored one first. Returns the evicted values, for the caller to drop.
    fn insert(&self, pattern: &str, value: &T, size: usize) -> Vec<Arc<T>> {
        let hash = self.hasher.hash_one(pattern);
        let mut inner = self.lock();
        inner.clock += 1;
        let now = inner.clock;
        if let Some(entry) = inner.entries.get_mut(pattern) {
            entry.last_used = now;
            return Vec::new();
        }
        if let Some(seen) = &mut inner.seen
            && !seen.check_and_mark(hash)
        {
            return Vec::new();
        }
        let evicted = make_room(&mut inner, self.capacity, self.max_bytes, size);
        inner.bytes += size;
        inner.entries.insert(
            pattern.into(),
            Entry {
                value: Arc::new(value.clone()),
                size,
                last_used: now,
            },
        );
        evicted
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.lock().entries.len()
    }

    #[cfg(test)]
    fn bytes(&self) -> usize {
        self.lock().bytes
    }
}

/// Removes the least recently used entries until one more, charged `incoming` bytes, fits both
/// the entry count and the byte budget, and returns their values. When anything must go, at
/// least a tenth of the capacity goes.
fn make_room<T>(
    inner: &mut Inner<T>,
    capacity: usize,
    max_bytes: usize,
    incoming: usize,
) -> Vec<Arc<T>> {
    let fits = |count: usize, bytes: usize| count < capacity && bytes + incoming <= max_bytes;
    if fits(inner.entries.len(), inner.bytes) {
        return Vec::new();
    }
    let oldest: Vec<Box<str>> = {
        let mut by_age: Vec<(u64, &Box<str>, usize)> = inner
            .entries
            .iter()
            .map(|(pattern, entry)| (entry.last_used, pattern, entry.size))
            .collect();
        by_age.sort_unstable_by_key(|&(last_used, _, _)| last_used);
        let min_count = capacity.div_ceil(10);
        let (mut count, mut bytes, mut take) = (inner.entries.len(), inner.bytes, 0);
        for &(_, _, size) in &by_age {
            if take >= min_count && fits(count, bytes) {
                break;
            }
            count -= 1;
            bytes -= size;
            take += 1;
        }
        by_age[..take]
            .iter()
            .map(|(_, pattern, _)| (*pattern).clone())
            .collect()
    };
    let mut evicted = Vec::with_capacity(oldest.len());
    for pattern in &oldest {
        if let Some(entry) = inner.entries.remove(pattern) {
            inner.bytes -= entry.size;
            evicted.push(entry.value);
        }
    }
    evicted
}

#[cfg(test)]
mod tests {
    use super::*;
    use regex::Regex;
    use std::sync::Barrier;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// The tests below are about counts and recency, so each regex weighs one byte.
    impl HeapSize for Regex {
        fn heap_size(&self) -> usize {
            1
        }
    }

    type TestCache = PatternCache<Regex>;

    /// Plenty of bytes: only the entry count binds.
    const NO_BYTE_LIMIT: usize = 1 << 30;

    fn counting_compile(count: &AtomicUsize) -> impl FnOnce(&str) -> Result<Regex, regex::Error> {
        move |pattern| {
            count.fetch_add(1, Ordering::Relaxed);
            Regex::new(pattern)
        }
    }

    #[test]
    fn a_hit_reuses_the_compiled_regex() {
        let cache = TestCache::new(8, 64, NO_BYTE_LIMIT);
        let compiles = AtomicUsize::new(0);
        let first = cache
            .get_or_compile("^(?:prod-.*)$", counting_compile(&compiles))
            .unwrap();
        let second = cache
            .get_or_compile("^(?:prod-.*)$", counting_compile(&compiles))
            .unwrap();

        assert_eq!(compiles.load(Ordering::Relaxed), 1);
        assert_eq!(first.as_str(), second.as_str());
        assert!(second.is_match("prod-eu"));
        assert!(!second.is_match("staging"));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn a_failed_compile_is_not_cached() {
        let cache = TestCache::new(8, 64, NO_BYTE_LIMIT);
        let compiles = AtomicUsize::new(0);
        for _ in 0..2 {
            assert!(
                cache
                    .get_or_compile("^(?:a(b)$", counting_compile(&compiles))
                    .is_err()
            );
        }
        assert_eq!(compiles.load(Ordering::Relaxed), 2);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn long_patterns_and_a_zero_capacity_bypass_the_cache() {
        let compiles = AtomicUsize::new(0);
        let cache = TestCache::new(8, 4, NO_BYTE_LIMIT);
        for _ in 0..2 {
            cache
                .get_or_compile("abcde", counting_compile(&compiles))
                .unwrap();
        }
        assert_eq!(cache.len(), 0);

        let disabled = TestCache::new(0, 64, NO_BYTE_LIMIT);
        for _ in 0..2 {
            disabled
                .get_or_compile("a", counting_compile(&compiles))
                .unwrap();
        }
        assert_eq!(disabled.len(), 0);
        assert_eq!(compiles.load(Ordering::Relaxed), 4);
    }

    #[test]
    fn a_full_cache_evicts_the_least_recently_used() {
        let cache = TestCache::new(10, 64, NO_BYTE_LIMIT);
        let compiles = AtomicUsize::new(0);
        for i in 0..10 {
            cache
                .get_or_compile(&format!("p{i}"), counting_compile(&compiles))
                .unwrap();
        }
        // Touch the oldest, so p1 becomes the least recently used.
        cache
            .get_or_compile("p0", counting_compile(&compiles))
            .unwrap();
        cache
            .get_or_compile("p10", counting_compile(&compiles))
            .unwrap();
        assert_eq!(compiles.load(Ordering::Relaxed), 11);
        assert_eq!(cache.len(), 10);

        cache
            .get_or_compile("p0", counting_compile(&compiles))
            .unwrap();
        assert_eq!(compiles.load(Ordering::Relaxed), 11, "p0 was kept");
        cache
            .get_or_compile("p1", counting_compile(&compiles))
            .unwrap();
        assert_eq!(compiles.load(Ordering::Relaxed), 12, "p1 was evicted");
    }

    #[test]
    fn many_patterns_never_exceed_the_capacity() {
        let cache = TestCache::new(16, 64, NO_BYTE_LIMIT);
        for i in 0..200 {
            cache.get_or_compile(&format!("p{i}"), Regex::new).unwrap();
            assert!(cache.len() <= 16);
        }
    }

    #[test]
    fn concurrent_misses_keep_one_entry() {
        const THREADS: usize = 8;
        let cache = TestCache::new(8, 64, NO_BYTE_LIMIT);
        let barrier = Barrier::new(THREADS);
        std::thread::scope(|scope| {
            for _ in 0..THREADS {
                scope.spawn(|| {
                    barrier.wait();
                    let regex = cache.get_or_compile("^(?:web-[0-9a-f]+)$", Regex::new);
                    assert!(regex.unwrap().is_match("web-00ff"));
                });
            }
        });
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn second_sighting_admission_stores_only_recurring_patterns() {
        let cache = TestCache::admitting_on_second_sighting(4, 64, NO_BYTE_LIMIT);
        let compiles = AtomicUsize::new(0);
        let get = |p: &str| {
            cache
                .get_or_compile(p, counting_compile(&compiles))
                .unwrap()
        };

        // Built twice before it is stored, then a hit.
        get("a");
        assert_eq!(cache.len(), 0);
        get("a");
        assert_eq!(cache.len(), 1);
        get("a");
        assert_eq!(compiles.load(Ordering::Relaxed), 2);

        // Patterns that never recur are never stored, however many there are.
        for i in 0..100 {
            get(&format!("u{i}"));
        }
        assert_eq!(cache.len(), 1);

        // A pattern last seen more than two generations (8 patterns) ago starts over.
        get("b");
        for i in 0..8 {
            get(&format!("v{i}"));
        }
        get("b");
        assert_eq!(cache.len(), 1, "b was forgotten, not admitted");
    }

    /// A value of a chosen weight.
    #[derive(Clone)]
    struct Weighted(usize);

    impl HeapSize for Weighted {
        fn heap_size(&self) -> usize {
            self.0
        }
    }

    fn weighted(size: usize) -> impl FnOnce(&str) -> Result<Weighted, ()> {
        move |_| Ok(Weighted(size))
    }

    #[test]
    fn the_byte_budget_evicts_the_least_recently_used() {
        // 800 bytes, so at most 100 per entry; a one-byte key adds one byte.
        let cache = PatternCache::<Weighted>::new(100, 64, 800);
        for p in ["a", "b", "c", "d", "e", "f", "g"] {
            cache.get_or_compile(p, weighted(99)).unwrap();
        }
        assert_eq!((cache.len(), cache.bytes()), (7, 700));
        // Touch `a`, so `b` is the oldest; the eighth entry fits exactly.
        cache.get_or_compile("a", weighted(99)).unwrap();
        cache.get_or_compile("h", weighted(99)).unwrap();
        assert_eq!((cache.len(), cache.bytes()), (8, 800));
        // A ninth does not: `b` makes room, the batch being a tenth of the capacity (10) at
        // least, here everything but the newest entries until it fits.
        cache.get_or_compile("i", weighted(99)).unwrap();
        assert!(cache.bytes() <= 800, "{}", cache.bytes());
        assert!(cache.lock().entries.contains_key("i"));
        assert!(!cache.lock().entries.contains_key("b"));
        let sum: usize = cache.lock().entries.values().map(|e| e.size).sum();
        assert_eq!(sum, cache.bytes(), "the running total matches the entries");
    }

    #[test]
    fn a_value_over_an_eighth_of_the_budget_is_not_stored() {
        let cache = PatternCache::<Weighted>::new(100, 64, 800);
        cache.get_or_compile("small", weighted(10)).unwrap();
        let compiles = AtomicUsize::new(0);
        for _ in 0..2 {
            cache
                .get_or_compile("heavy", |_| {
                    compiles.fetch_add(1, Ordering::Relaxed);
                    Ok::<_, ()>(Weighted(100))
                })
                .unwrap();
        }
        assert_eq!(compiles.load(Ordering::Relaxed), 2, "built on every use");
        assert_eq!(
            (cache.len(), cache.bytes()),
            (1, 15),
            "and the cache kept what it had"
        );
    }

    #[test]
    fn heavy_patterns_never_exceed_the_budget() {
        let cache = PatternCache::<Weighted>::new(256, 64, 4096);
        for i in 0..1000 {
            cache
                .get_or_compile(&format!("p{i}"), weighted(1 + i % 500))
                .unwrap();
            assert!(cache.bytes() <= 4096, "{}", cache.bytes());
        }
    }

    /// The compiled-regex cache charges what the engine reports: a pattern that compiles to
    /// about 1 MiB is charged about that, and one over 4 MiB (an eighth of the budget) would
    /// not be stored.
    #[test]
    fn compiled_regexes_are_charged_their_program_size() {
        let small = crate::labels::regex::compile_matcher("prod-.*").unwrap();
        let large = crate::labels::regex::compile_matcher(".{1,1000}").unwrap();
        assert!(small.heap_size() < 64 << 10, "{}", small.heap_size());
        assert!(large.heap_size() > 512 << 10, "{}", large.heap_size());
        assert!(large.heap_size() <= REGEX_CACHE_MAX_BYTES / 8);
    }
}
