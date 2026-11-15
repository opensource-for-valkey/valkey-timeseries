//! A bounded, process-wide cache of compiled label regexes.
//!
//! Compiling a label regex under the module's limits costs 1.5–30 µs for everyday patterns and
//! up to ~220 µs for large ones (`benches/parser.rs`), and a clone of the compiled regex costs
//! 0.2–0.5 µs. Every `=~`/`!~` matcher in a `FILTER` argument was compiled on every command, and
//! again on each shard when a cluster fan-out decoded the filters.
//!
//! The cache hands out clones, never the stored regex. A `regex::Regex` clone
//! shares the compiled program behind an `Arc` and gets its own, empty match
//! cache pool, so callers on different threads do not contend on a pool, and
//! the stored copy — which never runs a match — keeps no match cache. The one
//! thing a clone does share is the program's `GroupInfo` `Arc`, which
//! `captures` clones on every call, so a caller that needs `captures` from
//! many threads should compile its own. Every caller of this cache only calls
//! `is_match`.
//!
//! Patterns come from client input, so the cache is bounded in both entries
//! and pattern length, and failed compiles are not cached.
//!
//! The cache is generic over what it keeps: [`RegexCache`] holds compiled
//! regexes, and `regex_utils` keeps whole decomposed label matchers in one too.

use ahash::{AHashMap, AHashSet, RandomState};
use regex::Regex;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

/// The number of compiled regexes retained. Patterns compile under the
/// module's limits (a 64 KiB program), and the largest admitted programs
/// measured 44–62 KB of heap, so a full cache holds at most about 17 MiB;
/// everyday patterns such as `prod-.*` or `5..` take 6–8 KB each.
pub(crate) const REGEX_CACHE_CAPACITY: usize = 256;

/// Longer patterns are compiled without being cached, so the keys of a full
/// cache stay within `REGEX_CACHE_CAPACITY` × this many bytes (1 MiB).
pub(crate) const REGEX_CACHE_MAX_PATTERN_LEN: usize = 4096;

/// A bounded map from a pattern to the value built from it.
///
/// When full, the least recently used tenth of the entries makes room for the
/// next one, so a burst of new patterns does not pay an eviction scan each.
/// Values are handed out as clones, made outside the lock.
pub(crate) struct PatternCache<T> {
    capacity: usize,
    max_pattern_len: usize,
    hasher: RandomState,
    inner: Mutex<Inner<T>>,
}

/// A bounded map from a fully anchored pattern to its compiled regex.
pub(crate) type RegexCache = PatternCache<Regex>;

struct Inner<T> {
    entries: AHashMap<Box<str>, Entry<T>>,
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
    regex: Arc<T>,
    last_used: u64,
}

impl<T: Clone> PatternCache<T> {
    pub(crate) fn new(capacity: usize, max_pattern_len: usize) -> Self {
        Self::with_seen(capacity, max_pattern_len, None)
    }

    /// A cache that stores a value only the second time its pattern is built (see [`Seen`]),
    /// so a stream of patterns that never recur costs no insertions or evictions. For values
    /// cheap enough to build that cache upkeep is a visible share of a miss.
    pub(crate) fn admitting_on_second_sighting(capacity: usize, max_pattern_len: usize) -> Self {
        let seen = Seen {
            current: AHashSet::default(),
            previous: AHashSet::default(),
            generation: capacity,
        };
        Self::with_seen(capacity, max_pattern_len, Some(seen))
    }

    fn with_seen(capacity: usize, max_pattern_len: usize, seen: Option<Seen>) -> Self {
        Self {
            capacity,
            max_pattern_len,
            hasher: RandomState::new(),
            inner: Mutex::new(Inner {
                entries: AHashMap::default(),
                clock: 0,
                seen,
            }),
        }
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
        if let Some(regex) = self.lookup(pattern) {
            // Cloned outside the lock: a regex clone allocates its match cache pool.
            return Ok(T::clone(&regex));
        }
        let regex = compile(pattern)?;
        let evicted = self.insert(pattern, &regex);
        // Freed outside the lock as well.
        drop(evicted);
        Ok(regex)
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
        Some(Arc::clone(&entry.regex))
    }

    /// Stores a copy of `regex` under `pattern` unless another thread stored
    /// one first. Returns the evicted values, for the caller to drop.
    fn insert(&self, pattern: &str, regex: &T) -> Vec<Arc<T>> {
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
        let evicted = if inner.entries.len() >= self.capacity {
            evict_least_recent(&mut inner.entries, self.capacity.div_ceil(10))
        } else {
            Vec::new()
        };
        inner.entries.insert(
            pattern.into(),
            Entry {
                regex: Arc::new(regex.clone()),
                last_used: now,
            },
        );
        evicted
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.lock().entries.len()
    }
}

/// Removes the `count` least recently used entries and returns their values.
fn evict_least_recent<T>(entries: &mut AHashMap<Box<str>, Entry<T>>, count: usize) -> Vec<Arc<T>> {
    let count = count.min(entries.len());
    if count == 0 {
        return Vec::new();
    }
    let mut by_age: Vec<(u64, &Box<str>)> = entries
        .iter()
        .map(|(pattern, entry)| (entry.last_used, pattern))
        .collect();
    if count < by_age.len() {
        by_age.select_nth_unstable_by_key(count - 1, |(last_used, _)| *last_used);
    }
    let oldest: Vec<Box<str>> = by_age[..count]
        .iter()
        .map(|(_, pattern)| (*pattern).clone())
        .collect();
    oldest
        .iter()
        .filter_map(|pattern| entries.remove(pattern))
        .map(|entry| entry.regex)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn counting_compile(count: &AtomicUsize) -> impl FnOnce(&str) -> Result<Regex, regex::Error> {
        move |pattern| {
            count.fetch_add(1, Ordering::Relaxed);
            Regex::new(pattern)
        }
    }

    #[test]
    fn a_hit_reuses_the_compiled_regex() {
        let cache = RegexCache::new(8, 64);
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
        let cache = RegexCache::new(8, 64);
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
        let cache = RegexCache::new(8, 4);
        for _ in 0..2 {
            cache
                .get_or_compile("abcde", counting_compile(&compiles))
                .unwrap();
        }
        assert_eq!(cache.len(), 0);

        let disabled = RegexCache::new(0, 64);
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
        let cache = RegexCache::new(10, 64);
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
        let cache = RegexCache::new(16, 64);
        for i in 0..200 {
            cache.get_or_compile(&format!("p{i}"), Regex::new).unwrap();
            assert!(cache.len() <= 16);
        }
    }

    #[test]
    fn concurrent_misses_keep_one_entry() {
        const THREADS: usize = 8;
        let cache = RegexCache::new(8, 64);
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
        let cache = PatternCache::<Regex>::admitting_on_second_sighting(4, 64);
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
}
