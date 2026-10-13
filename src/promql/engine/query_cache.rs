//! A bounded, process-wide cache of parsed `TS.QUERY` / `TS.QUERYRANGE` expressions.
//!
//! Preparing a query — the regex pre-check, the parse and the depth check — is repeated for
//! every request, and dashboards send the same query text over and over. With the compiled
//! label-regex cache in place (`labels::regex_cache`), what remains is mostly promql-parser
//! compiling each regex matcher itself: 60–96 µs per regex-bearing request end to end,
//! 16–18 % of a light instant query. This cache keeps the prepared expression,
//! keyed by the exact query text, and hands each request its own clone.
//!
//! Only the parsed expression is kept, before any optional rewrite: static optimization
//! (at most ~2 µs) still runs per request, and so does everything that depends on time,
//! data, the database or the caller. A cached expression holds no observation of any of
//! those, so nothing — writes, FLUSHDB, ACL or label changes — invalidates it.
//!
//! Each entry is charged an estimate of the memory it keeps, regex programs included, and
//! the cache is bounded by both `ts-promql-query-cache-max-entries` and
//! `ts-promql-query-cache-max-bytes`. A change to either trims the cache at once
//! ([`apply_limits`]). Admission and trimming read the limits only while holding the cache
//! lock, so a preparation that started under older limits cannot overfill the cache: an
//! admission that takes the lock before a trim is trimmed by it, and one that takes it after
//! sees the new limits. Failed preparations are not cached.
//!
//! A query is kept from the second time its text is seen, not the first. Sizing an entry
//! compiles each regex matcher's program once more, and a workload of one-off texts (ad hoc
//! queries, or a tool that writes a changing value into the text) paid that on every request
//! and was slower with the cache than without it. A first sighting now records only a
//! hash of the text ([`Seen`]), so a one-off query costs a hash lookup, and a flood of them
//! cannot displace the queries that repeat.

use crate::config::{query_cache_max_bytes, query_cache_max_entries};
use ahash::{AHashMap, AHashSet, RandomState};
use promql_parser::label::{MatchOp, Matcher, Matchers};
use promql_parser::parser::{
    BinModifier, Expr, LabelModifier, VectorMatchCardinality, VectorSelector,
};
use regex_automata::meta;
use std::mem::size_of;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError};

static QUERY_CACHE: LazyLock<QueryCache> = LazyLock::new(QueryCache::default);

/// The prepared expression for `query`: a clone of the cached one, or `prepare`'s result,
/// which is cached if it succeeds and fits.
pub(crate) fn prepare_cached<E>(
    query: &str,
    prepare: impl FnOnce(&str) -> Result<Expr, E>,
) -> Result<Expr, E> {
    QUERY_CACHE.get_or_prepare(query, current_limits, prepare)
}

/// Brings the cache within the current limits, emptying it if either is 0. Called after
/// either limit is changed. What it evicts is freed off the calling thread.
pub(crate) fn apply_limits() {
    let evicted = QUERY_CACHE.trim(current_limits);
    if !evicted.is_empty() {
        crate::common::threads::spawn_background("ts-promql-cache-trim", move || drop(evicted));
    }
}

/// The cache's counters and gauges, for `INFO ts_promql`. Read without the cache lock.
pub(crate) fn stats() -> QueryCacheStats {
    QUERY_CACHE.stats()
}

fn current_limits() -> Limits {
    Limits {
        max_entries: query_cache_max_entries(),
        max_bytes: query_cache_max_bytes(),
    }
}

#[derive(Clone, Copy, Debug)]
struct Limits {
    max_entries: usize,
    max_bytes: usize,
}

impl Limits {
    fn disabled(&self) -> bool {
        self.max_entries == 0 || self.max_bytes == 0
    }

    /// The largest single entry admitted: one template may not displace most of the cache.
    fn max_entry_bytes(&self) -> usize {
        self.max_bytes / 16
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct QueryCacheStats {
    /// Cached expressions, and the memory charged for them.
    pub entries: usize,
    pub bytes: usize,
    /// Lookups while the cache was enabled, since startup.
    pub hits: u64,
    pub misses: u64,
    /// Entries removed to stay within the limits, including on a change of limits.
    pub evictions: u64,
}

#[derive(Default)]
struct QueryCache {
    inner: Mutex<Inner>,
    /// Hashes query texts for [`Seen`].
    hasher: RandomState,
    // Published after each change under the lock, so `stats` never takes it.
    entries: AtomicUsize,
    bytes: AtomicUsize,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
}

#[derive(Default)]
struct Inner {
    entries: AHashMap<Box<str>, Entry>,
    /// The sum of the entries' charges.
    bytes: usize,
    /// Advances on every hit and insertion: the recency stamp.
    clock: u64,
    /// Texts missed once and not cached yet.
    seen: Seen,
}

/// How many first sightings a generation of [`Seen`] holds.
const SEEN_GENERATION: usize = 4096;

/// The hashes of query texts seen once, waiting for a second sighting. Two generations of at
/// most [`SEEN_GENERATION`] hashes each: when the current one fills, it becomes the previous
/// one and the old previous one is forgotten. A text is thus remembered for at least
/// `SEEN_GENERATION` other first sightings — far more than a dashboard sends between two
/// refreshes of one panel — in at most about 150 KB. A hash collision only caches a query a
/// request early.
#[derive(Default)]
struct Seen {
    current: AHashSet<u64>,
    previous: AHashSet<u64>,
}

impl Seen {
    /// Whether `hash` was seen recently. A second sighting consumes it; a first records it.
    fn second_sighting(&mut self, hash: u64) -> bool {
        if self.current.remove(&hash) || self.previous.remove(&hash) {
            return true;
        }
        if self.current.len() >= SEEN_GENERATION {
            std::mem::swap(&mut self.current, &mut self.previous);
            self.current.clear();
        }
        self.current.insert(hash);
        false
    }

    fn clear(&mut self) {
        self.current.clear();
        self.previous.clear();
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.current.len() + self.previous.len()
    }
}

enum Lookup {
    Hit(Arc<Expr>),
    /// `admit`: whether the text was seen recently, so a successful preparation is cached.
    Miss {
        admit: bool,
    },
}

struct Entry {
    expr: Arc<Expr>,
    charge: usize,
    last_used: u64,
}

impl QueryCache {
    fn get_or_prepare<E>(
        &self,
        query: &str,
        limits: impl Fn() -> Limits,
        prepare: impl FnOnce(&str) -> Result<Expr, E>,
    ) -> Result<Expr, E> {
        if limits().disabled() {
            return prepare(query);
        }
        let admit = match self.lookup(query) {
            Lookup::Hit(expr) => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                // Cloned outside the lock; the request owns, and may rewrite, its copy.
                return Ok(Expr::clone(&expr));
            }
            Lookup::Miss { admit } => admit,
        };
        self.misses.fetch_add(1, Ordering::Relaxed);
        let expr = prepare(query)?;
        // A first sighting is neither sized nor kept: see the module docs.
        if admit {
            // Sized outside the lock: it compiles each regex matcher's program once more.
            if let Some(charge) = template_charge(query, &expr) {
                let evicted = self.admit(query, Arc::new(expr.clone()), charge, limits);
                // Freed outside the lock as well.
                drop(evicted);
            }
        }
        Ok(expr)
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        // Every change under the lock leaves the map and `bytes` consistent before anything
        // that could panic, so a poisoned cache is still a valid one.
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn lookup(&self, query: &str) -> Lookup {
        let hash = self.hasher.hash_one(query);
        let mut inner = self.lock();
        inner.clock += 1;
        let now = inner.clock;
        if let Some(entry) = inner.entries.get_mut(query) {
            entry.last_used = now;
            return Lookup::Hit(Arc::clone(&entry.expr));
        }
        Lookup::Miss {
            admit: inner.seen.second_sighting(hash),
        }
    }

    /// Caches `expr` under `query` if it fits the current limits, evicting the least recently
    /// used entries to make room. Returns the evicted expressions, for the caller to drop.
    ///
    /// `limits` is read under the lock, as [`trim`](Self::trim) reads it: read before, a
    /// `CONFIG SET` could lower a limit and finish its trim in between, and this would then
    /// insert under the old limits and leave the cache above the new ones.
    fn admit(
        &self,
        query: &str,
        expr: Arc<Expr>,
        charge: usize,
        limits: impl Fn() -> Limits,
    ) -> Vec<Arc<Expr>> {
        let mut inner = self.lock();
        let limits = limits();
        if limits.disabled() || charge > limits.max_entry_bytes() {
            return Vec::new();
        }
        inner.clock += 1;
        let now = inner.clock;
        if let Some(entry) = inner.entries.get_mut(query) {
            // Another worker prepared the same query first.
            entry.last_used = now;
            return Vec::new();
        }
        let mut evicted = Vec::new();
        if inner.entries.len() >= limits.max_entries
            || inner.bytes.saturating_add(charge) > limits.max_bytes
        {
            // Down to 90 % of each limit, so a cache at capacity does not pay a scan per miss.
            let max_entries = limits.max_entries - (limits.max_entries / 10).max(1);
            let max_bytes = (limits.max_bytes - limits.max_bytes / 10).saturating_sub(charge);
            evicted = evict_least_recent(&mut inner, max_entries, max_bytes);
        }
        inner.bytes += charge;
        inner.entries.insert(
            query.into(),
            Entry {
                expr,
                charge,
                last_used: now,
            },
        );
        self.publish(&inner, evicted.len());
        evicted
    }

    /// Evicts the least recently used entries until the cache is within `limits`, read under
    /// the lock (see [`admit`](Self::admit)); all of them if the cache is now disabled.
    fn trim(&self, limits: impl Fn() -> Limits) -> Vec<Arc<Expr>> {
        let mut inner = self.lock();
        let limits = limits();
        let (max_entries, max_bytes) = if limits.disabled() {
            inner.seen.clear();
            (0, 0)
        } else {
            (limits.max_entries, limits.max_bytes)
        };
        let evicted = evict_least_recent(&mut inner, max_entries, max_bytes);
        self.publish(&inner, evicted.len());
        evicted
    }

    fn publish(&self, inner: &Inner, evicted: usize) {
        self.entries.store(inner.entries.len(), Ordering::Relaxed);
        self.bytes.store(inner.bytes, Ordering::Relaxed);
        self.evictions.fetch_add(evicted as u64, Ordering::Relaxed);
    }

    fn stats(&self) -> QueryCacheStats {
        QueryCacheStats {
            entries: self.entries.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
        }
    }
}

/// Removes the least recently used entries until at most `max_entries` remain holding at
/// most `max_bytes`, and returns their expressions.
fn evict_least_recent(inner: &mut Inner, max_entries: usize, max_bytes: usize) -> Vec<Arc<Expr>> {
    if inner.entries.len() <= max_entries && inner.bytes <= max_bytes {
        return Vec::new();
    }
    let mut by_age: Vec<(u64, &Box<str>)> = inner
        .entries
        .iter()
        .map(|(query, entry)| (entry.last_used, query))
        .collect();
    by_age.sort_unstable_by_key(|(last_used, _)| *last_used);
    let oldest: Vec<Box<str>> = by_age.into_iter().map(|(_, query)| query.clone()).collect();

    let mut evicted = Vec::new();
    for query in oldest {
        if inner.entries.len() <= max_entries && inner.bytes <= max_bytes {
            break;
        }
        if let Some(entry) = inner.entries.remove(&query) {
            inner.bytes -= entry.charge;
            evicted.push(entry.expr);
        }
    }
    evicted
}

/// What one regex program is charged beyond its own [`meta::Regex::memory_usage`] and its
/// pattern text: the `regex::Regex` wrapper and its (never used) match-cache pool. Measured
/// at 5.9 KB for every pattern tried (regex 1.13.1, 2026-10-12): with the pattern length
/// added, the charge was within 2 % above the regex's real heap, from `5..` (14.7 KB) to a
/// 300-value alternation (63 KB).
const REGEX_OVERHEAD_BYTES: usize = 6 * 1024;

/// The memory a cached `expr` keeps: the key, the entry, and the expression's heap, regex
/// programs included. `None` if part of it cannot be sized, and then it is not cached.
fn template_charge(query: &str, expr: &Expr) -> Option<usize> {
    // The map slot (key, entry, control byte) and the `Arc`'s allocation.
    let fixed = size_of::<Box<str>>() + size_of::<Entry>() + 1 + 2 * size_of::<usize>();
    let mut charge = Charge(
        fixed
            .checked_add(query.len())?
            .checked_add(size_of::<Expr>())?,
    );
    charge.expr(expr)?;
    Some(charge.0)
}

/// An accumulating, overflow-checked byte count. Each method returns `None` on overflow or
/// on something it cannot size.
struct Charge(usize);

impl Charge {
    fn add(&mut self, bytes: usize) -> Option<()> {
        self.0 = self.0.checked_add(bytes)?;
        Some(())
    }

    fn vec<T>(&mut self, capacity: usize) -> Option<()> {
        self.add(capacity.checked_mul(size_of::<T>())?)
    }

    fn string(&mut self, value: &str, capacity: usize) -> Option<()> {
        debug_assert!(capacity >= value.len());
        self.add(capacity)
    }

    fn boxed(&mut self, expr: &Expr) -> Option<()> {
        self.add(size_of::<Expr>())?;
        self.expr(expr)
    }

    /// The heap `expr` owns, not counting its own size.
    fn expr(&mut self, expr: &Expr) -> Option<()> {
        match expr {
            Expr::Aggregate(agg) => {
                self.boxed(&agg.expr)?;
                if let Some(param) = &agg.param {
                    self.boxed(param)?;
                }
                if let Some(modifier) = &agg.modifier {
                    self.label_modifier(modifier)?;
                }
                Some(())
            }
            Expr::Unary(unary) => self.boxed(&unary.expr),
            Expr::Binary(binary) => {
                self.boxed(&binary.lhs)?;
                self.boxed(&binary.rhs)?;
                if let Some(modifier) = &binary.modifier {
                    self.bin_modifier(modifier)?;
                }
                Some(())
            }
            Expr::Paren(paren) => self.boxed(&paren.expr),
            Expr::Subquery(subquery) => self.boxed(&subquery.expr),
            Expr::NumberLiteral(_) => Some(()),
            Expr::StringLiteral(literal) => self.string(&literal.val, literal.val.capacity()),
            Expr::VectorSelector(selector) => self.vector_selector(selector),
            Expr::MatrixSelector(matrix) => self.vector_selector(&matrix.vs),
            Expr::Call(call) => {
                self.vec::<promql_parser::parser::value::ValueType>(
                    call.func.arg_types.capacity(),
                )?;
                self.vec::<Box<Expr>>(call.args.args.capacity())?;
                call.args.args.iter().try_for_each(|arg| self.boxed(arg))
            }
            // Never produced by the parser, and opaque.
            Expr::Extension(_) => None,
        }
    }

    fn vector_selector(&mut self, selector: &VectorSelector) -> Option<()> {
        if let Some(name) = &selector.name {
            self.string(name, name.capacity())?;
        }
        self.matchers(&selector.matchers)
    }

    fn matchers(&mut self, matchers: &Matchers) -> Option<()> {
        self.matcher_list(&matchers.matchers)?;
        self.vec::<Vec<Matcher>>(matchers.or_matchers.capacity())?;
        matchers
            .or_matchers
            .iter()
            .try_for_each(|list| self.matcher_list(list))
    }

    fn matcher_list(&mut self, list: &Vec<Matcher>) -> Option<()> {
        self.vec::<Matcher>(list.capacity())?;
        list.iter().try_for_each(|matcher| {
            self.string(&matcher.name, matcher.name.capacity())?;
            self.string(&matcher.value, matcher.value.capacity())?;
            match &matcher.op {
                MatchOp::Re(regex) | MatchOp::NotRe(regex) => {
                    self.add(regex_charge(regex.as_str())?)
                }
                MatchOp::Equal | MatchOp::NotEqual => Some(()),
            }
        })
    }

    fn label_modifier(&mut self, modifier: &LabelModifier) -> Option<()> {
        let (LabelModifier::Include(labels) | LabelModifier::Exclude(labels)) = modifier;
        self.labels(&labels.labels)
    }

    fn bin_modifier(&mut self, modifier: &BinModifier) -> Option<()> {
        match &modifier.card {
            VectorMatchCardinality::ManyToOne(labels)
            | VectorMatchCardinality::OneToMany(labels) => self.labels(&labels.labels)?,
            VectorMatchCardinality::OneToOne | VectorMatchCardinality::ManyToMany => {}
        }
        if let Some(matching) = &modifier.matching {
            self.label_modifier(matching)?;
        }
        Some(())
    }

    fn labels(&mut self, labels: &Vec<String>) -> Option<()> {
        self.vec::<String>(labels.capacity())?;
        labels
            .iter()
            .try_for_each(|label| self.string(label, label.capacity()))
    }
}

/// The memory a regex promql-parser compiled from `pattern` keeps. `regex::Regex` does not
/// expose its size, so this compiles the same pattern with the same configuration
/// `regex::Regex::new` uses (regex 1.13's `builders.rs`) and asks the engine — once per
/// pattern while [`PATTERN_CHARGES`] remembers it. `None` if that compile fails, which the
/// parser's own successful compile of the same text rules out.
fn regex_charge(pattern: &str) -> Option<usize> {
    if let Some(charge) = pattern_charges().get(pattern) {
        return Some(charge);
    }
    let charge = measure_regex(pattern)?;
    pattern_charges().insert(pattern, charge);
    Some(charge)
}

fn measure_regex(pattern: &str) -> Option<usize> {
    let program = meta::Builder::new()
        .configure(
            meta::Config::new()
                .nfa_size_limit(Some(10 * (1 << 20)))
                .hybrid_cache_capacity(2 * (1 << 20))
                .utf8_empty(true),
        )
        .syntax(regex_automata::util::syntax::Config::new().utf8(true))
        .build(pattern)
        .ok()?
        .memory_usage();
    program
        .checked_add(pattern.len())?
        .checked_add(REGEX_OVERHEAD_BYTES)
}

/// The charges [`regex_charge`] measured, by pattern. Query texts that differ but share
/// matchers — the same panel with a different value written into it, or texts that are
/// admitted, evicted and admitted again — then pay one measuring compile per pattern rather
/// than one per admission: with a cache too small for its working set, that compile kept
/// regex queries 3–4 % slower than with no cache at all.
static PATTERN_CHARGES: LazyLock<Mutex<PatternCharges>> = LazyLock::new(Mutex::default);

fn pattern_charges() -> MutexGuard<'static, PatternCharges> {
    // Only map operations run under the lock.
    PATTERN_CHARGES
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

/// How many patterns a generation of [`PatternCharges`] holds.
const PATTERN_GENERATION: usize = 1024;
/// Longer patterns are measured every time, so the memo's keys stay within
/// 2 × [`PATTERN_GENERATION`] × this many bytes (512 KiB).
const MAX_MEMOIZED_PATTERN_LEN: usize = 256;

/// A bounded memo of pattern charges, aged like [`Seen`]: two generations of at most
/// [`PATTERN_GENERATION`] patterns, the older forgotten when the newer fills. A charge is a
/// function of the pattern alone, so a remembered one is always exact.
#[derive(Default)]
struct PatternCharges {
    current: AHashMap<Box<str>, usize>,
    previous: AHashMap<Box<str>, usize>,
}

impl PatternCharges {
    fn get(&mut self, pattern: &str) -> Option<usize> {
        if let Some(&charge) = self.current.get(pattern) {
            return Some(charge);
        }
        // Still in use: carry it into the current generation.
        let (key, charge) = self.previous.remove_entry(pattern)?;
        self.insert_owned(key, charge);
        Some(charge)
    }

    fn insert(&mut self, pattern: &str, charge: usize) {
        if pattern.len() <= MAX_MEMOIZED_PATTERN_LEN {
            self.insert_owned(pattern.into(), charge);
        }
    }

    fn insert_owned(&mut self, pattern: Box<str>, charge: usize) {
        if self.current.len() >= PATTERN_GENERATION {
            std::mem::swap(&mut self.current, &mut self.previous);
            self.current.clear();
        }
        self.current.insert(pattern, charge);
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.current.len() + self.previous.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use promql_parser::parser::parse;
    use std::sync::atomic::AtomicUsize;
    use std::sync::{Barrier, mpsc};

    const ROOMY: Limits = Limits {
        max_entries: 64,
        max_bytes: 64 << 20,
    };

    fn counting_parse(count: &AtomicUsize) -> impl FnOnce(&str) -> Result<Expr, String> {
        move |query| {
            count.fetch_add(1, Ordering::Relaxed);
            parse(query)
        }
    }

    /// Sends `query` twice, which caches it: the first sighting is only recorded.
    fn warm(cache: &QueryCache, query: &str, limits: Limits) -> Expr {
        cache.get_or_prepare(query, || limits, parse).unwrap();
        cache.get_or_prepare(query, || limits, parse).unwrap()
    }

    fn charge_of(query: &str) -> usize {
        template_charge(query, &parse(query).unwrap()).unwrap()
    }

    #[test]
    fn a_query_is_kept_from_its_second_sighting() {
        let cache = QueryCache::default();
        let parses = AtomicUsize::new(0);
        let query = r#"sum by (pod) (rate(cpu{namespace=~"prod-.*"}[5m]))"#;
        let mut results = Vec::new();
        for expected_entries in [0, 1, 1] {
            results.push(
                cache
                    .get_or_prepare(query, || ROOMY, counting_parse(&parses))
                    .unwrap(),
            );
            assert_eq!(cache.stats().entries, expected_entries);
        }

        assert_eq!(parses.load(Ordering::Relaxed), 2, "the third is a hit");
        assert!(results.iter().all(|expr| *expr == parse(query).unwrap()));
        let stats = cache.stats();
        assert_eq!((stats.hits, stats.misses), (1, 2));
        assert_eq!(stats.bytes, charge_of(query));
    }

    #[test]
    fn one_off_queries_neither_fill_the_cache_nor_displace_it() {
        let cache = QueryCache::default();
        let limits = Limits {
            max_entries: 10,
            ..ROOMY
        };
        warm(&cache, "kept", limits);
        for i in 0..1000 {
            cache
                .get_or_prepare(&format!("once{i}"), || limits, parse)
                .unwrap();
        }
        let stats = cache.stats();
        assert_eq!((stats.entries, stats.evictions), (1, 0));
        let parses = AtomicUsize::new(0);
        cache
            .get_or_prepare("kept", || limits, counting_parse(&parses))
            .unwrap();
        assert_eq!(parses.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn first_sightings_are_bounded_and_age_out() {
        let mut seen = Seen::default();
        assert!(!seen.second_sighting(0));
        for hash in 1..=(3 * SEEN_GENERATION as u64) {
            assert!(!seen.second_sighting(hash));
            assert!(seen.len() <= 2 * SEEN_GENERATION);
        }
        assert!(!seen.second_sighting(0), "forgotten after two generations");
        let recent = 3 * SEEN_GENERATION as u64;
        assert!(seen.second_sighting(recent));
        assert!(
            !seen.second_sighting(recent),
            "a second sighting consumes it"
        );
    }

    #[test]
    fn a_request_rewriting_its_copy_leaves_the_cached_one_alone() {
        let cache = QueryCache::default();
        let query = r#"up{job="api"}"#;
        let mut first = warm(&cache, query, ROOMY);
        let Expr::VectorSelector(selector) = &mut first else {
            panic!("a selector");
        };
        // What the derived-filter pass does to a range query's tree.
        selector
            .matchers
            .matchers
            .push(Matcher::new(MatchOp::Equal, "host", "a"));

        let second = cache.get_or_prepare(query, || ROOMY, parse).unwrap();
        assert_eq!(cache.stats().hits, 1);
        assert_ne!(first, second);
        assert_eq!(second, parse(query).unwrap());
    }

    #[test]
    fn a_failed_preparation_is_not_cached() {
        let cache = QueryCache::default();
        let parses = AtomicUsize::new(0);
        for _ in 0..3 {
            assert!(
                cache
                    .get_or_prepare("sum(", || ROOMY, counting_parse(&parses))
                    .is_err()
            );
        }
        assert_eq!(parses.load(Ordering::Relaxed), 3);
        let stats = cache.stats();
        assert_eq!((stats.entries, stats.misses), (0, 3));
    }

    #[test]
    fn a_zero_limit_disables_the_cache() {
        let parses = AtomicUsize::new(0);
        for limits in [
            Limits {
                max_entries: 0,
                ..ROOMY
            },
            Limits {
                max_bytes: 0,
                ..ROOMY
            },
        ] {
            let cache = QueryCache::default();
            for _ in 0..3 {
                cache
                    .get_or_prepare("up", || limits, counting_parse(&parses))
                    .unwrap();
            }
            assert_eq!(cache.stats(), QueryCacheStats::default());
        }
        assert_eq!(parses.load(Ordering::Relaxed), 6);
    }

    #[test]
    fn the_entry_limit_evicts_the_least_recently_used() {
        let cache = QueryCache::default();
        let limits = Limits {
            max_entries: 10,
            ..ROOMY
        };
        for i in 0..10 {
            warm(&cache, &format!("m{i}"), limits);
        }
        // Touch the oldest, so m1 becomes the least recently used.
        let parses = AtomicUsize::new(0);
        cache
            .get_or_prepare("m0", || limits, counting_parse(&parses))
            .unwrap();
        assert_eq!(parses.load(Ordering::Relaxed), 0);
        warm(&cache, "m10", limits);
        assert_eq!(cache.stats().entries, 10);
        assert_eq!(cache.stats().evictions, 1);

        cache
            .get_or_prepare("m0", || limits, counting_parse(&parses))
            .unwrap();
        assert_eq!(parses.load(Ordering::Relaxed), 0, "m0 was kept");
        cache
            .get_or_prepare("m1", || limits, counting_parse(&parses))
            .unwrap();
        assert_eq!(parses.load(Ordering::Relaxed), 1, "m1 was evicted");
    }

    #[test]
    fn the_byte_limit_bounds_the_charged_total() {
        let cache = QueryCache::default();
        let per_entry = charge_of("m100");
        // Room for about twenty such entries, so each is under a sixteenth of the budget.
        let limits = Limits {
            max_entries: 1000,
            max_bytes: per_entry * 20 + per_entry / 2,
        };
        assert!(per_entry <= limits.max_entry_bytes());
        for i in 100..200 {
            warm(&cache, &format!("m{i}"), limits);
            let stats = cache.stats();
            assert!(stats.bytes <= limits.max_bytes, "{stats:?}");
        }
        let stats = cache.stats();
        assert!((16..=20).contains(&stats.entries), "{stats:?}");
        assert_eq!(stats.evictions as usize + stats.entries, 100);
    }

    #[test]
    fn an_oversized_template_is_evaluated_without_evicting_anything() {
        let cache = QueryCache::default();
        let small = charge_of("up");
        let limits = Limits {
            max_entries: 64,
            max_bytes: small * 16,
        };
        warm(&cache, "up", limits);
        let big = r#"up{a=~"x.*", b=~"y.*", c=~"z.*"}"#;
        assert!(charge_of(big) > limits.max_entry_bytes());

        assert_eq!(warm(&cache, big, limits), parse(big).unwrap());
        let stats = cache.stats();
        assert_eq!((stats.entries, stats.evictions), (1, 0));
    }

    #[test]
    fn trimming_to_new_limits_evicts_at_once() {
        let cache = QueryCache::default();
        for i in 0..20 {
            warm(&cache, &format!("m{i}"), ROOMY);
        }
        let evicted = cache.trim(|| Limits {
            max_entries: 5,
            ..ROOMY
        });
        assert_eq!(evicted.len(), 15);
        assert_eq!(cache.stats().entries, 5);
        // The five most recent survive.
        let parses = AtomicUsize::new(0);
        cache
            .get_or_prepare("m19", || ROOMY, counting_parse(&parses))
            .unwrap();
        assert_eq!(parses.load(Ordering::Relaxed), 0);

        let evicted = cache.trim(|| Limits {
            max_bytes: 0,
            ..ROOMY
        });
        assert_eq!(evicted.len(), 5);
        let stats = cache.stats();
        assert_eq!((stats.entries, stats.bytes, stats.evictions), (0, 0, 20));
        assert_eq!(
            cache.lock().seen.len(),
            0,
            "disabling forgets first sightings too"
        );
    }

    #[test]
    fn a_disable_during_preparation_prevents_admission() {
        let cache = QueryCache::default();
        cache.get_or_prepare("up", || ROOMY, parse).unwrap();
        let enabled = std::sync::atomic::AtomicBool::new(true);
        let limits = || {
            if enabled.load(Ordering::Relaxed) {
                ROOMY
            } else {
                Limits {
                    max_entries: 0,
                    ..ROOMY
                }
            }
        };
        cache
            .get_or_prepare("up", limits, |query| {
                enabled.store(false, Ordering::Relaxed);
                parse(query)
            })
            .unwrap();
        assert_eq!(cache.stats().entries, 0);
    }

    /// A limit lowered while a query is being admitted still binds. The worker's admission
    /// reads the limits (the old ones), and a `CONFIG SET` then lowers the entry limit and
    /// trims. Admission reads the limits under the lock the trim takes, so the trim waits for
    /// it and evicts what it inserted. Were they read before the lock, the trim could finish
    /// first and the insertion leave the cache above the new limit.
    #[test]
    fn a_limit_lowered_during_admission_still_binds() {
        let cache = QueryCache::default();
        let max_entries = AtomicUsize::new(ROOMY.max_entries);
        let current = || Limits {
            max_entries: max_entries.load(Ordering::SeqCst),
            ..ROOMY
        };
        for i in 0..4 {
            warm(&cache, &format!("m{i}"), ROOMY);
        }
        cache.get_or_prepare("late", current, parse).unwrap();

        let reads = AtomicUsize::new(0);
        let (trimmed, trim_done) = mpsc::channel();
        std::thread::scope(|scope| {
            // Its first read is the enabled check before the lookup; the second decides
            // admission, and that is where the `CONFIG SET` lands.
            let limits = || {
                let read = current();
                if reads.fetch_add(1, Ordering::SeqCst) == 1 {
                    let trimmed = trimmed.clone();
                    let (cache, max_entries) = (&cache, &max_entries);
                    scope.spawn(move || {
                        max_entries.store(2, Ordering::SeqCst);
                        drop(cache.trim(|| Limits {
                            max_entries: max_entries.load(Ordering::SeqCst),
                            ..ROOMY
                        }));
                        let _ = trimmed.send(());
                    });
                    // Long enough for an unblocked trim to finish; a blocked one waits for
                    // the lock, and the outcome is the same either way.
                    let _ = trim_done.recv_timeout(std::time::Duration::from_millis(100));
                }
                read
            };
            cache.get_or_prepare("late", limits, parse).unwrap();
        });

        let stats = cache.stats();
        assert!(stats.entries <= 2, "{stats:?}");
        assert_eq!(cache.lock().entries.len(), stats.entries);
    }

    #[test]
    fn concurrent_misses_keep_one_charged_entry() {
        const THREADS: usize = 8;
        let cache = QueryCache::default();
        let barrier = Barrier::new(THREADS);
        let query = r#"rate(http_requests_total{code=~"5.."}[5m])"#;
        std::thread::scope(|scope| {
            for _ in 0..THREADS {
                scope.spawn(|| {
                    barrier.wait();
                    for _ in 0..2 {
                        assert!(cache.get_or_prepare(query, || ROOMY, parse).is_ok());
                    }
                });
            }
        });
        let stats = cache.stats();
        assert_eq!(stats.entries, 1);
        assert_eq!(stats.bytes, charge_of(query));
        assert_eq!(stats.hits + stats.misses, 2 * THREADS as u64);
    }

    #[test]
    fn regex_matchers_are_charged_for_their_programs() {
        let plain = charge_of(r#"up{job="api"}"#);
        let one = charge_of(r#"up{job=~"api"}"#);
        let two = charge_of(r#"up{job=~"api", host=~"web-[0-9a-f]+"}"#);
        assert!(one > plain + REGEX_OVERHEAD_BYTES, "{plain} {one}");
        assert!(two > one + REGEX_OVERHEAD_BYTES, "{one} {two}");
        // A Unicode class is a much larger program than a literal.
        let word = charge_of(r#"up{job=~"\\w+"}"#);
        assert!(word > one + 32 * 1024, "{one} {word}");
        // Matchers under `or` count too.
        let or = charge_of(r#"up{job=~"a.*" or host=~"b.*"}"#);
        assert!(or > plain + 2 * REGEX_OVERHEAD_BYTES, "{plain} {or}");
    }

    #[test]
    fn a_remembered_pattern_charge_matches_a_measured_one() {
        let pattern = "^(?:memo-[0-9a-f]+-test)$";
        let measured = measure_regex(pattern).unwrap();
        assert_eq!(regex_charge(pattern), Some(measured));
        assert_eq!(pattern_charges().get(pattern), Some(measured));
        assert_eq!(regex_charge(pattern), Some(measured));
    }

    #[test]
    fn pattern_charges_are_bounded_and_age_out() {
        let mut charges = PatternCharges::default();
        charges.insert("kept", 7);
        charges.insert(&"x".repeat(MAX_MEMOIZED_PATTERN_LEN + 1), 1);
        assert_eq!(charges.len(), 1, "a long pattern is not remembered");
        for i in 0..(3 * PATTERN_GENERATION) {
            charges.insert(&format!("p{i}"), i);
            assert!(charges.len() <= 2 * PATTERN_GENERATION);
            // A pattern in use is carried forward, so it is never forgotten.
            assert_eq!(charges.get("kept"), Some(7));
        }
        assert_eq!(charges.get("p0"), None, "forgotten after two generations");
        let recent = 3 * PATTERN_GENERATION - 1;
        assert_eq!(charges.get(&format!("p{recent}")), Some(recent));
    }

    #[test]
    fn deeper_trees_are_charged_more() {
        let a = charge_of("sum(rate(x[5m]))");
        let b = charge_of("sum by (job, instance) (rate(x[5m])) / on(job) group_left(team) sum(y)");
        assert!(b > a, "{a} {b}");
    }
}
