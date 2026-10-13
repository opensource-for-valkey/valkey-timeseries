//! A bounded, process-wide cache of compiled label regexes.
//!
//! Compiling a label regex under the module's limits costs 25–220 µs, and a
//! clone of the compiled regex costs 0.2–0.5 µs (measured 2026-10-12, release,
//! Apple M2). A PromQL query with regex matchers compiled each of them twice
//! per request before this cache: once in the pre-check
//! (`validate_query_regexes`) and again when each selector read converted its
//! matchers. TS.* `FILTER` parsing and shard-side decoding of fanned-out
//! filters compiled them on every command too.
//!
//! The cache hands out clones, never the stored regex. A `regex::Regex` clone
//! shares the compiled program behind an `Arc` and gets its own, empty match
//! cache pool, so callers on different threads do not contend on a pool, and
//! the stored copy — which never runs a match — keeps no match cache. The one
//! thing a clone does share is the program's `GroupInfo` `Arc`, which
//! `captures` clones on every call; that is why `label_replace`, which calls
//! `captures` from parallel step workers, keeps its own per-thread compile
//! cache instead of using this one. Every caller of this cache only calls
//! `is_match`.
//!
//! Patterns come from client input, so the cache is bounded in both entries
//! and pattern length, and failed compiles are not cached.

use ahash::AHashMap;
use regex::Regex;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

/// The number of compiled regexes retained. Patterns compile under the
/// module's limits (a 64 KiB program), and the largest admitted programs
/// measured 44–62 KB of heap, so a full cache holds at most about 17 MiB;
/// everyday patterns such as `prod-.*` or `5..` take 6–8 KB each.
pub(crate) const REGEX_CACHE_CAPACITY: usize = 256;

/// Longer patterns are compiled without being cached, so the keys of a full
/// cache stay within `REGEX_CACHE_CAPACITY` × this many bytes (1 MiB). It is
/// the default `ts-promql-max-query-len`, which no single PromQL matcher can
/// exceed by default.
pub(crate) const REGEX_CACHE_MAX_PATTERN_LEN: usize = 4096;

/// A bounded map from a fully anchored pattern to its compiled regex.
///
/// When full, the least recently used tenth of the entries makes room for the
/// next one, so a burst of new patterns does not pay an eviction scan each.
pub(crate) struct RegexCache {
    capacity: usize,
    max_pattern_len: usize,
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    entries: AHashMap<Box<str>, Entry>,
    /// Advances on every hit and insertion: the recency stamp.
    clock: u64,
}

struct Entry {
    regex: Arc<Regex>,
    last_used: u64,
}

impl RegexCache {
    pub(crate) fn new(capacity: usize, max_pattern_len: usize) -> Self {
        Self {
            capacity,
            max_pattern_len,
            inner: Mutex::new(Inner::default()),
        }
    }

    /// The compiled regex for `pattern`, compiling it with `compile` if it is
    /// not cached. `compile` runs without the cache lock held, so concurrent
    /// misses on one pattern may each compile it; the first to finish is kept.
    /// An error from `compile` is returned as it is and not cached.
    pub(crate) fn get_or_compile(
        &self,
        pattern: &str,
        compile: impl FnOnce(&str) -> Result<Regex, regex::Error>,
    ) -> Result<Regex, regex::Error> {
        if self.capacity == 0 || pattern.len() > self.max_pattern_len {
            return compile(pattern);
        }
        if let Some(regex) = self.lookup(pattern) {
            // Cloned outside the lock: a clone allocates its match cache pool.
            return Ok(Regex::clone(&regex));
        }
        let regex = compile(pattern)?;
        let evicted = self.insert(pattern, &regex);
        // Freed outside the lock as well.
        drop(evicted);
        Ok(regex)
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        // Only map operations run under the lock, so a panic cannot leave an
        // entry half-written; the worst a poisoned map holds is a stale stamp.
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn lookup(&self, pattern: &str) -> Option<Arc<Regex>> {
        let mut inner = self.lock();
        inner.clock += 1;
        let now = inner.clock;
        let entry = inner.entries.get_mut(pattern)?;
        entry.last_used = now;
        Some(Arc::clone(&entry.regex))
    }

    /// Stores a copy of `regex` under `pattern` unless another thread stored
    /// one first. Returns the evicted regexes, for the caller to drop.
    fn insert(&self, pattern: &str, regex: &Regex) -> Vec<Arc<Regex>> {
        let mut inner = self.lock();
        inner.clock += 1;
        let now = inner.clock;
        if let Some(entry) = inner.entries.get_mut(pattern) {
            entry.last_used = now;
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

/// Removes the `count` least recently used entries and returns their regexes.
fn evict_least_recent(entries: &mut AHashMap<Box<str>, Entry>, count: usize) -> Vec<Arc<Regex>> {
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
}
