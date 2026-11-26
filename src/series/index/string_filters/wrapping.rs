// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Ported from Prometheus `web/api/v1/search_filters.go` (caseFoldingFilter, memoizingFilter).

//! Filters that wrap one inner filter and change how values reach it.

use crate::series::index::{StringFilter, to_lowercase_cow};
use std::collections::HashMap;
use std::sync::{Mutex, TryLockError};

/// Lowercases the value once before delegating, so a chain of case-insensitive filters does not
/// each repeat the case fold. The inner filter must have been built from a lowercased query.
pub struct CaseFoldingFilter<F = Box<dyn StringFilter>> {
    inner: F,
}

impl<F: StringFilter> CaseFoldingFilter<F> {
    pub fn new(inner: F) -> Self {
        Self { inner }
    }
}

impl<F: StringFilter> StringFilter for CaseFoldingFilter<F> {
    fn accept(&self, value: &str) -> (bool, f64) {
        self.inner.accept(&to_lowercase_cow(value))
    }
}

/// The most distinct values a [`MemoizingFilter`] caches. Guards against many short values.
pub const MEMOIZING_FILTER_MAX_ENTRIES: usize = 100_000;

/// The most value bytes a [`MemoizingFilter`] caches, counted as the sum of the cached values'
/// lengths: a coarse but cheap stand-in for the map's memory. Guards against long values (URLs,
/// user agents), where the entry cap alone would allow tens of megabytes.
pub const MEMOIZING_FILTER_MAX_BYTES: usize = 10 << 20;

#[derive(Clone, Copy)]
struct MemoEntry {
    accepted: bool,
    score: f64,
}

#[derive(Default)]
struct MemoCache {
    entries: HashMap<Box<str>, MemoEntry, ahash::RandomState>,
    /// Sum of the cached keys' lengths.
    bytes: usize,
}

/// Caches the inner filter's `(accepted, score)` for each distinct value, so a value that
/// reaches the filter more than once in a search is scored once. It pays only where values
/// repeat: the label search builders leave it out, because a search sees each distinct value
/// about once and every lookup would miss.
///
/// Once the cache holds [`MEMOIZING_FILTER_MAX_ENTRIES`] values or
/// [`MEMOIZING_FILTER_MAX_BYTES`] bytes, misses go to the inner filter without being cached.
///
/// A search drives its filter from one thread, so the cache lock is never contended there.
/// A caller that finds it held, by another thread or by a re-entrant call, scores the value with
/// the inner filter instead of waiting.
pub struct MemoizingFilter<F = Box<dyn StringFilter>> {
    inner: F,
    cache: Mutex<MemoCache>,
}

impl<F: StringFilter> MemoizingFilter<F> {
    pub fn new(inner: F) -> Self {
        Self {
            inner,
            cache: Mutex::default(),
        }
    }

    #[cfg(test)]
    fn cached(&self) -> (usize, usize) {
        let cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        (cache.entries.len(), cache.bytes)
    }
}

impl<F: StringFilter> StringFilter for MemoizingFilter<F> {
    fn accept(&self, value: &str) -> (bool, f64) {
        let mut cache = match self.cache.try_lock() {
            Ok(cache) => cache,
            // An entry is inserted only after the inner filter returns, so a panic in it
            // leaves the cache consistent.
            Err(TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            Err(TryLockError::WouldBlock) => return self.inner.accept(value),
        };
        if let Some(entry) = cache.entries.get(value) {
            return (entry.accepted, entry.score);
        }
        let (accepted, score) = self.inner.accept(value);
        if cache.entries.len() < MEMOIZING_FILTER_MAX_ENTRIES
            && cache.bytes + value.len() <= MEMOIZING_FILTER_MAX_BYTES
        {
            cache
                .entries
                .insert(value.into(), MemoEntry { accepted, score });
            cache.bytes += value.len();
        }
        (accepted, score)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Accepts values containing `query`, scoring 1.0, and counts its calls.
    struct Counting {
        query: &'static str,
        calls: AtomicUsize,
    }

    impl Counting {
        fn new(query: &'static str) -> Self {
            Self {
                query,
                calls: AtomicUsize::new(0),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::Relaxed)
        }
    }

    impl StringFilter for Counting {
        fn accept(&self, value: &str) -> (bool, f64) {
            self.calls.fetch_add(1, Ordering::Relaxed);
            if value.contains(self.query) {
                (true, 1.0)
            } else {
                (false, 0.0)
            }
        }
    }

    /// Records the last value it saw.
    #[derive(Default)]
    struct Recording(Mutex<String>);

    impl StringFilter for Recording {
        fn accept(&self, value: &str) -> (bool, f64) {
            *self.0.lock().unwrap() = value.to_owned();
            (true, 1.0)
        }
    }

    #[test]
    fn case_folding_lowercases_before_delegating() {
        let filter = CaseFoldingFilter::new(Counting::new("cpu"));
        assert_eq!(filter.accept("Node_CPU_seconds"), (true, 1.0));
        assert_eq!(filter.accept("cpu"), (true, 1.0));
        assert_eq!(filter.accept("MEMORY"), (false, 0.0));
    }

    #[test]
    fn case_folding_folds_non_ascii() {
        let filter = CaseFoldingFilter::new(Recording::default());
        filter.accept("ÄRGER");
        assert_eq!(*filter.inner.0.lock().unwrap(), "ärger");
    }

    #[test]
    fn case_folding_does_not_lowercase_the_query() {
        // The inner filter's query must already be lowercase.
        let filter = CaseFoldingFilter::new(Counting::new("CPU"));
        assert_eq!(filter.accept("CPU"), (false, 0.0));
    }

    #[test]
    fn memoizing_scores_each_distinct_value_once() {
        let filter = MemoizingFilter::new(Counting::new("cpu"));
        for _ in 0..3 {
            assert_eq!(filter.accept("node_cpu"), (true, 1.0));
            assert_eq!(filter.accept("memory"), (false, 0.0));
        }
        assert_eq!(filter.inner.calls(), 2);
        assert_eq!(filter.cached(), (2, "node_cpu".len() + "memory".len()));
    }

    #[test]
    fn memoizing_stops_caching_at_the_entry_cap() {
        let filter = MemoizingFilter::new(Counting::new("x"));
        for i in 0..MEMOIZING_FILTER_MAX_ENTRIES {
            filter.accept(&i.to_string());
        }
        assert_eq!(filter.cached().0, MEMOIZING_FILTER_MAX_ENTRIES);

        let calls = filter.inner.calls();
        assert_eq!(filter.accept("x past the cap"), (true, 1.0));
        assert_eq!(filter.accept("x past the cap"), (true, 1.0));
        assert_eq!(filter.inner.calls(), calls + 2);
        assert_eq!(filter.cached().0, MEMOIZING_FILTER_MAX_ENTRIES);

        // Values cached before the cap still hit.
        filter.accept("0");
        assert_eq!(filter.inner.calls(), calls + 2);
    }

    #[test]
    fn memoizing_stops_caching_at_the_byte_cap() {
        let filter = MemoizingFilter::new(Counting::new("x"));
        let long = "x".repeat(MEMOIZING_FILTER_MAX_BYTES - 1);
        filter.accept(&long);
        assert_eq!(filter.cached(), (1, MEMOIZING_FILTER_MAX_BYTES - 1));

        // One more byte fits exactly; anything after it does not.
        filter.accept("a");
        assert_eq!(filter.cached(), (2, MEMOIZING_FILTER_MAX_BYTES));
        filter.accept("b");
        filter.accept("b");
        assert_eq!(filter.cached(), (2, MEMOIZING_FILTER_MAX_BYTES));
        assert_eq!(filter.inner.calls(), 4);
    }

    #[test]
    fn memoizing_falls_through_while_the_cache_is_held() {
        let filter = MemoizingFilter::new(Counting::new("cpu"));
        filter.accept("cpu");
        let _held = filter.cache.lock().unwrap();
        assert_eq!(filter.accept("cpu"), (true, 1.0));
        assert_eq!(filter.accept("disk"), (false, 0.0));
        assert_eq!(filter.inner.calls(), 3);
    }

    #[test]
    fn memoizing_recovers_a_poisoned_cache() {
        struct PanicsOn(&'static str);
        impl StringFilter for PanicsOn {
            fn accept(&self, value: &str) -> (bool, f64) {
                assert_ne!(value, self.0);
                (true, 0.5)
            }
        }

        let filter = MemoizingFilter::new(PanicsOn("boom"));
        assert_eq!(filter.accept("ok"), (true, 0.5));
        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            filter.accept("boom");
        }));
        assert!(panicked.is_err());
        assert!(filter.cache.is_poisoned());

        assert_eq!(filter.accept("ok"), (true, 0.5));
        assert_eq!(filter.accept("fine"), (true, 0.5));
        assert_eq!(filter.cached().0, 2);
    }

    #[test]
    fn wrappers_compose_over_boxed_filters() {
        let inner: Box<dyn StringFilter> = Box::new(Counting::new("cpu"));
        let filter = MemoizingFilter::new(CaseFoldingFilter::new(inner));
        assert_eq!(filter.accept("NODE_CPU"), (true, 1.0));
        assert_eq!(filter.accept("NODE_CPU"), (true, 1.0));
        assert_eq!(filter.cached().0, 1);
    }
}
