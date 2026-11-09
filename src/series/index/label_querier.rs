//! High-level label query APIs layered on top of index postings and selector queriers.
//!
//! This module provides label-oriented discovery features (value listing, fuzzy matching,
//! similarity ranking, cardinality-aware sorting, and bounded result shaping) that are
//! consumed by label commands.
//!
//! Compared with `querier.rs`, which focuses on selector-to-series resolution with ACL and
//! series materialization, this module is value-centric: it evaluates and ranks label values
//! using index primitives while reusing selector hints when available.
//!
//! A scoped query (selectors, a date range, or both) reads the matching series through the
//! batched reader, from the caller's plan when it made one ([`for_each_planned_series`]): only the label names or values are copied out under the
//! module lock, a batch at a time, and fuzzy scoring and cardinality lookups run once the
//! lock is released. On a fan-out share the lock is given back between batches; a caller on
//! the main thread already holds it and reads in one batch.

use super::postings::Postings;
use super::{
    DEFAULT_SERIES_BATCH_SIZE, FuzzyFilter, IndexKey, NoOpSearchMatcher, PostingsBitmap,
    SeriesCursor, SeriesSelection, TimeSeriesIndex, ensure_generation, for_each_planned_series,
    get_db_index, read_planned,
};
use crate::common::acl::check_metadata_permissions;
use crate::common::constants::METRIC_NAME_LABEL;
use crate::common::threads::GilSource;
use crate::error::{TsdbError, TsdbResult};
use crate::labels::filters::SeriesSelector;
use crate::series::index::key_buffer::KeyBuffer;
use crate::series::request_types::MetaDateRangeFilter;
use ahash::{AHashMap, AHashSet};
use std::cmp::Ordering;
use std::collections::{Bound, VecDeque};
use std::fmt::Display;
use std::ops::ControlFlow;
use valkey_module::{ValkeyError, ValkeyResult};

/// A starting capacity for result buffers and maps; not a limit.
const INITIAL_RESULT_CAPACITY: usize = 100;

// todo: get from config
pub const SEARCH_RESULT_LIMIT_MAX: usize = 1000;

/// SortBy is a closed set of label search result sort keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum SortBy {
    #[default]
    Value,
    Score,
    Cardinality,
}

impl Display for SortBy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SortBy::Value => write!(f, "value"),
            SortBy::Score => write!(f, "score"),
            SortBy::Cardinality => write!(f, "cardinality"),
        }
    }
}

impl TryFrom<&str> for SortBy {
    type Error = ValkeyError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.to_ascii_lowercase().as_str() {
            "value" => Ok(SortBy::Value),
            "score" => Ok(SortBy::Score),
            "cardinality" => Ok(SortBy::Cardinality),
            _ => Err(ValkeyError::Str(
                "TSDB: SORTBY must be value, score, or cardinality",
            )),
        }
    }
}

/// Ordering is a closed set of label search result orderings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SearchResultOrdering {
    /// Orders results ascending by Value.
    #[default]
    ValueAsc,
    /// Orders results descending by Value.
    ValueDesc,
    /// Orders results descending by Score, breaking ties ascending by Value.
    ScoreDesc,
    /// Orders results ascending by Cardinality, breaking ties ascending by Value.
    CardinalityAsc,
    /// Orders results descending by Cardinality, breaking ties ascending by Value.
    CardinalityDesc,
}

/// SelectHints specifies hints passed for data selections.
#[derive(Debug, Clone, Default)]
pub struct SelectHints {
    /// Series selectors to apply to series selection.
    pub selectors: Vec<SeriesSelector>,
    /// Optional date range filter to apply to series selection.
    pub date_range: Option<MetaDateRangeFilter>,
    /// `selectors` and `date_range` planned already, when the caller planned them to size the
    /// read ([`plan_if_cheap`](super::plan_if_cheap)). The read uses it rather than planning
    /// again.
    planned: Option<SeriesCursor>,
}

impl SelectHints {
    pub fn new(selectors: Vec<SeriesSelector>, date_range: Option<MetaDateRangeFilter>) -> Self {
        Self {
            selectors,
            date_range,
            planned: None,
        }
    }

    /// Read `plan`, made from these hints' [`selection`](Self::selection) and date range,
    /// instead of planning them again. Nothing can check that it was made from the same
    /// selectors, but its date range must be the same.
    pub fn with_plan(mut self, plan: Option<SeriesCursor>) -> Self {
        debug_assert!(
            plan.as_ref()
                .is_none_or(|plan| plan.date_range() == self.date_range),
            "a plan made from another date range"
        );
        self.planned = plan;
        self
    }

    pub fn is_empty(&self) -> bool {
        self.selectors.is_empty() && self.date_range.is_none()
    }

    /// The series these hints select before the date range is applied: those the selectors
    /// match, or, with a date range alone, every series.
    pub fn selection(&self) -> SeriesSelection<'_> {
        if self.selectors.is_empty() {
            SeriesSelection::All
        } else {
            SeriesSelection::Matching(&self.selectors)
        }
    }

    /// Run `read` over the series these hints select in database `db`: the plan given with
    /// [`Self::with_plan`] first, and a new one when there is none or a `SWAPDB` moved the
    /// database under it ([`read_planned`]).
    fn read_planned<T>(
        mut self,
        db: i32,
        read: impl FnMut(SeriesCursor) -> ValkeyResult<T>,
    ) -> ValkeyResult<T> {
        let planned = self.planned.take();
        let replan = || SeriesCursor::plan(db, self.selection(), self.date_range);
        read_planned(planned, replan, read)
    }
}

/// The result limit of a label search that sets none, or sets `0`.
pub const SEARCH_RESULT_DEFAULT_LIMIT: usize = 100;

/// SearchHints configures search operations with filtering and scoring.
pub struct SearchHints<'a> {
    /// Filter determines which values to include and their relevance scores.
    pub filter: Option<Box<dyn FuzzyFilter + 'a>>,

    /// Maximum number of results to return: `0` means [`SEARCH_RESULT_DEFAULT_LIMIT`], and
    /// anything above [`SEARCH_RESULT_LIMIT_MAX`] is capped to it ([`normalize_limit`]).
    pub limit: usize,

    /// Selects the ordering of results.
    pub order_by: SearchResultOrdering,

    pub include_meta: bool,
}

impl<'a> Default for SearchHints<'a> {
    fn default() -> Self {
        Self {
            filter: None,
            limit: SEARCH_RESULT_DEFAULT_LIMIT,
            order_by: SearchResultOrdering::default(),
            include_meta: false,
        }
    }
}

/// SearchResult represents a single search result with its relevance score.
#[derive(Debug, Clone, Default)]
pub struct LabelSearchResult {
    /// The label name or label value.
    pub value: String,

    /// Relevance score, with 1.0 being a perfect match.
    pub score: f64,

    /// How many series carry the label name or value, or `0` where it was not computed: a
    /// scoped search computes it only for `INCLUDE_METADATA` or a cardinality order. The
    /// series counted differ by path:
    ///
    /// * unscoped names and values: every visible series in the database;
    /// * scoped values: the selected series that carry the value;
    /// * scoped names: every visible series in the database that carries the name, however few
    ///   were selected — the selection decides which names are returned, not what they count.
    pub cardinality: usize,
}

impl PartialEq for LabelSearchResult {
    fn eq(&self, other: &Self) -> bool {
        // As the ranking compares scores, so that equality is reflexive even for NaN.
        self.value == other.value
            && self.score.total_cmp(&other.score).is_eq()
            && self.cardinality == other.cardinality
    }
}

impl Eq for LabelSearchResult {}

/// `LabelQueryResult` is the paginated result of a label query. It pairs the matched
/// [`LabelSearchResult`] entries with a `has_more` flag that signals whether the underlying
/// data set contained more entries than the requested `limit`.
#[derive(Debug, Default)]
pub struct LabelQueryResult {
    /// The matched label search results, sorted and limited according to the query hints.
    pub results: Vec<LabelSearchResult>,
    /// `has_more` is `true` when the result set was truncated at `limit`, indicating
    /// that additional entries exist beyond what was returned.
    pub has_more: bool,
}

impl LabelQueryResult {
    pub fn new(results: Vec<LabelSearchResult>, has_more: bool) -> Self {
        Self { results, has_more }
    }

    /// Merges another query result into this one.
    ///
    /// Duplicate values are coalesced by summing cardinality and keeping the best score.
    /// The merged set is then sorted and limited according to `order_by` and `limit`, which is
    /// normalized as a search's is ([`normalize_limit`]).
    pub fn merge_from(
        &mut self,
        other: LabelQueryResult,
        order_by: SearchResultOrdering,
        limit: usize,
    ) {
        let limit = normalize_limit(limit);
        let mut merged: AHashMap<String, LabelSearchResult> =
            AHashMap::with_capacity(self.results.len() + other.results.len());

        for item in self.results.drain(..).chain(other.results) {
            match merged.get_mut(&item.value) {
                Some(existing) => {
                    existing.cardinality += item.cardinality;
                    existing.score = existing.score.max(item.score);
                }
                None => {
                    let key = item.value.clone();
                    merged.insert(key, item);
                }
            }
        }

        let merged = select_best(merged.into_values().collect(), order_by, limit);
        self.has_more = self.has_more || other.has_more || merged.has_more;
        self.results = merged.results;
    }
}

impl IntoIterator for LabelQueryResult {
    type Item = LabelSearchResult;
    type IntoIter = std::vec::IntoIter<LabelSearchResult>;

    fn into_iter(self) -> Self::IntoIter {
        self.results.into_iter()
    }
}

/// LabelQuerier defines the interface for querying label names and values with flexible filtering and ordering.
///
/// Each method takes the module lock from `src` as it needs it, so it must not be called
/// while holding the lock through a [`GilSource`] that releases it. Pass the
/// [`Context`](valkey_module::Context) on the main thread, and the request's `FanoutContext` on a fan-out share.
pub trait LabelQuerier {
    /// Returns a list of label names matching the given hints.
    fn get_label_names<S: GilSource + ?Sized>(
        &self,
        src: &S,
        select_hints: Option<SelectHints>,
        search_hints: Option<&SearchHints>,
    ) -> TsdbResult<LabelQueryResult>;

    /// Returns a list of label values for the given label name, matching the given hints.
    fn get_label_values<S: GilSource + ?Sized>(
        &self,
        src: &S,
        name: &str,
        select_hints: Option<SelectHints>,
        search_hints: Option<&SearchHints>,
    ) -> TsdbResult<LabelQueryResult>;

    /// Returns a list of metric names (`__name__` values) matching the given hints.
    fn get_metric_names<S: GilSource + ?Sized>(
        &self,
        src: &S,
        select_hints: Option<SelectHints>,
        search_hints: Option<&SearchHints>,
    ) -> TsdbResult<LabelQueryResult>;
}

/// The fields a result is ranked by, borrowed, so that a candidate can be compared with the
/// results kept so far before its value is copied.
#[derive(Clone, Copy, Debug)]
struct RankKey<'a> {
    value: &'a str,
    score: f64,
    cardinality: usize,
}

impl LabelSearchResult {
    fn rank_key(&self) -> RankKey<'_> {
        RankKey {
            value: &self.value,
            score: self.score,
            cardinality: self.cardinality,
        }
    }
}

impl SearchResultOrdering {
    /// The one ranking rule every label search uses: `Less` means `a` comes first.
    ///
    /// A total order: score and cardinality ties fall back to the value, ascending, and scores
    /// compare by [`f64::total_cmp`], so even a NaN from a [`FuzzyFilter`] has one place and
    /// cannot make a sort panic.
    fn compare(self, a: RankKey<'_>, b: RankKey<'_>) -> Ordering {
        let by_value = || a.value.cmp(b.value);
        match self {
            SearchResultOrdering::ValueAsc => by_value(),
            SearchResultOrdering::ValueDesc => b.value.cmp(a.value),
            SearchResultOrdering::ScoreDesc => b.score.total_cmp(&a.score).then_with(by_value),
            SearchResultOrdering::CardinalityAsc => {
                a.cardinality.cmp(&b.cardinality).then_with(by_value)
            }
            SearchResultOrdering::CardinalityDesc => {
                b.cardinality.cmp(&a.cardinality).then_with(by_value)
            }
        }
    }
}

/// [`SearchResultOrdering::compare`] over whole results.
fn compare_search_results(
    order: SearchResultOrdering,
) -> impl Fn(&LabelSearchResult, &LabelSearchResult) -> Ordering + Copy {
    move |a, b| order.compare(a.rank_key(), b.rank_key())
}

/// A requested result limit as every collector applies it: `0` means the default, and the
/// limit is capped at [`SEARCH_RESULT_LIMIT_MAX`]. Each entry point normalizes once; the
/// helpers below it take a limit in `1..=SEARCH_RESULT_LIMIT_MAX`.
pub(crate) fn normalize_limit(limit: usize) -> usize {
    if limit == 0 {
        SEARCH_RESULT_DEFAULT_LIMIT
    } else {
        limit
    }
    .min(SEARCH_RESULT_LIMIT_MAX)
}

/// `apply_search_hints` keeps the best `hints.limit` of `values` under the hints' ordering,
/// sorted, returning a [`LabelQueryResult`] whose `has_more` says whether any were cut.
pub(crate) fn apply_search_hints(
    values: Vec<LabelSearchResult>,
    hints: &SearchHints,
) -> LabelQueryResult {
    select_best(values, hints.order_by, normalize_limit(hints.limit))
}

/// The best `limit` of `values` under `order`, sorted, in place: a selection then a sort of
/// what is kept, rather than a sort of every candidate, which may be every distinct value of a
/// label.
fn select_best(
    mut values: Vec<LabelSearchResult>,
    order: SearchResultOrdering,
    limit: usize,
) -> LabelQueryResult {
    debug_assert!((1..=SEARCH_RESULT_LIMIT_MAX).contains(&limit));
    let cmp = compare_search_results(order);
    let has_more = values.len() > limit;
    if has_more {
        values.select_nth_unstable_by(limit - 1, cmp);
        values.truncate(limit);
    }
    values.sort_unstable_by(cmp);
    LabelQueryResult::new(values, has_more)
}

/// Runs `f` with the hints' filter, or without `SEARCH` one that accepts everything with a
/// score of 1 and never looks at the candidate.
fn with_search_filter<R>(hints: &SearchHints, f: impl FnOnce(&dyn FuzzyFilter) -> R) -> R {
    match &hints.filter {
        Some(filter) => f(filter.as_ref()),
        None => f(&NoOpSearchMatcher),
    }
}

fn can_use_unscoped_scan(select_hints: Option<&SelectHints>) -> bool {
    select_hints.map(|h| h.is_empty()).unwrap_or(true)
}

/// The most series a read that opens them may cover on the main thread. Applies to a
/// scoped label search, `TS.QUERYLABELS`, `TS.QUERYINDEX`, and range-filtered `TS.CARD`. Equal
/// to one batch of the batched reader today, but a constant of its own so that retuning the
/// batch size does not silently move this policy.
const INLINE_SERIES_READ_LIMIT: usize = 512;

/// Whether a read that opens every series it matches should leave the main thread.
///
/// With `plan`, made by [`plan_if_cheap`](super::plan_if_cheap), the match count is exact.
/// Without one the selectors were too costly to plan here, so the read is judged by how many
/// series the database indexes: a selective query against a large database is then also sent
/// to a worker, which costs it a lane round trip, not correctness.
pub(crate) fn series_read_exceeds_inline_budget(
    index: &TimeSeriesIndex,
    plan: Option<&SeriesCursor>,
) -> bool {
    let series = match plan {
        Some(plan) => plan.remaining(),
        None => index.count(),
    };
    series > INLINE_SERIES_READ_LIMIT
}

/// A cheap, conservative estimate of whether a standalone search should leave the main
/// thread. Never plans selectors or scores strings here: that could cost as much as the
/// search itself. Scoped searches use database-wide counts, so a selective query against
/// a large database may also be sent to a worker.
///
/// `label_name = None` searches names; `Some` searches that label's values. Name scans
/// sum cardinalities across every value of each name, so even LIMIT 1 can read many entries.
/// Value scans can stop after limit + 1 live entries only without SEARCH and in value order.
///
/// A scoped search may pass `plan`, its selectors planned already because that was cheap
/// ([`plan_if_cheap`](super::plan_if_cheap)): it is then sized by its exact match count.
pub(crate) fn label_search_exceeds_inline_budget(
    index: &TimeSeriesIndex,
    label_name: Option<&str>,
    scoped: bool,
    plan: Option<&SeriesCursor>,
    hints: &SearchHints,
    entries_budget: usize,
) -> bool {
    // An empty label name matches no index entry, so the search is trivial.
    if label_name == Some("") {
        return false;
    }
    let stop_after = (hints.filter.is_none()
        && matches!(
            hints.order_by,
            SearchResultOrdering::ValueAsc | SearchResultOrdering::ValueDesc
        ))
    .then(|| normalize_limit(hints.limit) + 1);

    if scoped {
        // A name search reporting cardinalities walks every value of every name it finds,
        // however few series matched.
        let reads_name_cardinality = label_name.is_none()
            && (hints.include_meta
                || matches!(
                    hints.order_by,
                    SearchResultOrdering::CardinalityAsc | SearchResultOrdering::CardinalityDesc
                ));
        let postings = index.get_postings();
        return match plan {
            // Planned already: the match count is exact, and planning is paid for.
            // Nothing matched: nothing is read and no cardinality is looked up.
            Some(plan) if plan.remaining() == 0 => false,
            Some(plan) => {
                plan.remaining() > INLINE_SERIES_READ_LIMIT
                    || (reads_name_cardinality && postings.label_index.len() > entries_budget)
            }
            // Also bound the term dictionary: regex planning and name-cardinality lookups can
            // scan it, even for few series.
            None => {
                postings.count() > INLINE_SERIES_READ_LIMIT
                    || postings.label_index.len() > entries_budget
            }
        };
    }
    let postings = index.get_postings();
    let Some(label_name) = label_name else {
        return postings.label_index.len() > entries_budget;
    };

    let prefix = KeyBuffer::for_prefix(label_name);
    let mut iter = postings.label_index.prefix(prefix.as_bytes());
    let exceeds = |iter: &mut dyn Iterator<Item = (&IndexKey, &PostingsBitmap)>| {
        let mut live_entries = 0;
        // Bounded probe: at most budget + 1 entries, regardless of label cardinality.
        for (scanned, (_, map)) in iter.take(entries_budget.saturating_add(1)).enumerate() {
            if scanned == entries_budget {
                return true;
            }
            // As the scan does: an entry no read can see is not a result, so it cannot end this.
            if postings.visible_cardinality(map) > 0 {
                live_entries += 1;
                if stop_after == Some(live_entries) {
                    return false;
                }
            }
        }
        false
    };
    if hints.order_by == SearchResultOrdering::ValueDesc {
        exceeds(&mut iter.rev())
    } else {
        exceeds(&mut iter)
    }
}

/// `collect_limited` collects up to `limit + 1` items from `iter`, then truncates to `limit`
/// and sets `has_more` if the extra item was present. `limit` is normalized already.
fn collect_limited(
    iter: impl Iterator<Item = LabelSearchResult>,
    limit: usize,
) -> LabelQueryResult {
    debug_assert!((1..=SEARCH_RESULT_LIMIT_MAX).contains(&limit));

    let mut items: Vec<LabelSearchResult> = iter.take(limit + 1).collect();
    let has_more = items.len() > limit;
    if has_more {
        items.truncate(limit);
    }
    LabelQueryResult::new(items, has_more)
}

/// The best `limit` of the results offered to it under one ordering, and whether more than
/// `limit` were offered.
///
/// It holds at most `2 * limit` results. When full it keeps the best `limit`, by selection
/// rather than a sort, and from then on turns away, without copying it, any candidate that
/// does not beat the worst of those. The one sort is over at most `2 * limit` results, in
/// [`Self::finish`].
struct TopK {
    order: SearchResultOrdering,
    limit: usize,
    items: Vec<LabelSearchResult>,
    /// Whether `items[limit - 1]` is the worst of `limit` results already kept, and so the
    /// bar a candidate must beat. Results pushed after it do not move it.
    bounded: bool,
    offered: usize,
}

impl TopK {
    fn new(order: SearchResultOrdering, limit: usize) -> Self {
        debug_assert!(
            (1..=SEARCH_RESULT_LIMIT_MAX).contains(&limit),
            "a label search limit is normalized first"
        );
        let limit = limit.max(1);
        Self {
            order,
            limit,
            items: Vec::with_capacity(limit.saturating_mul(2).min(INITIAL_RESULT_CAPACITY)),
            bounded: false,
            offered: 0,
        }
    }

    fn admits(&self, key: RankKey<'_>) -> bool {
        !self.bounded
            || self
                .order
                .compare(key, self.items[self.limit - 1].rank_key())
                .is_lt()
    }

    /// Offer the result `key` ranks, made by `make` only if it is kept.
    fn offer_with(&mut self, key: RankKey<'_>, make: impl FnOnce() -> LabelSearchResult) {
        self.offered += 1;
        if self.admits(key) {
            self.push(make());
        }
    }

    fn offer(&mut self, item: LabelSearchResult) {
        self.offered += 1;
        if self.admits(item.rank_key()) {
            self.push(item);
        }
    }

    fn push(&mut self, item: LabelSearchResult) {
        self.items.push(item);
        if self.items.len() >= self.limit.saturating_mul(2) {
            self.keep_best();
        }
    }

    /// The worst of the best `limit` results offered so far. At least `limit` must have been
    /// offered.
    fn worst_kept(&mut self) -> &LabelSearchResult {
        if !self.bounded {
            self.keep_best();
        }
        &self.items[self.limit - 1]
    }

    /// Keep only the best `limit` results, the worst of them at `limit - 1`.
    fn keep_best(&mut self) {
        let cmp = compare_search_results(self.order);
        self.items.select_nth_unstable_by(self.limit - 1, cmp);
        self.items.truncate(self.limit);
        self.bounded = true;
    }

    fn finish(mut self) -> LabelQueryResult {
        self.items
            .sort_unstable_by(compare_search_results(self.order));
        self.items.truncate(self.limit);
        LabelQueryResult::new(self.items, self.offered > self.limit)
    }
}

/// The best `limit` of `iter` under `order`, sorted: see [`TopK`].
fn collect_top_k(
    iter: impl Iterator<Item = LabelSearchResult>,
    order: SearchResultOrdering,
    limit: usize,
) -> LabelQueryResult {
    let mut top = TopK::new(order, limit);
    iter.for_each(|item| top.offer(item));
    top.finish()
}

/// Label-index entries a scan reads under one hold of the postings read lock.
///
/// A name-scan entry is a key comparison and a bitmap cardinality; a value-scan entry may also
/// be scored against `SEARCH` terms. Either way a hold this size is microseconds to tens of
/// microseconds: short enough that an index writer waiting on the lock is not starved by a scan
/// over a large index, long enough that re-taking the lock is noise.
const LABEL_SCAN_ENTRIES_PER_HOLD: usize = 1024;

/// `name=`: no key of `name`'s values sorts below it.
fn first_label_key(name: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(name.len() + 1);
    key.extend_from_slice(name.as_bytes());
    key.push(b'=');
    key
}

/// `name=\xFF`: sorts above every `name=value` key, since index keys are UTF-8 and no value
/// holds the byte 0xFF, and below every key of a later name.
fn past_label_keys(name: &str) -> Vec<u8> {
    let mut key = first_label_key(name);
    key.push(0xFF);
    key
}

/// Overwrite `buf` with the bound that skips every value of `name` and nothing else: `name=\xFF`
/// above them ([`past_label_keys`]) or `name=` below them ([`first_label_key`]). A bound such as
/// `name?` would also skip a name like `name>x`, which sorts between `name=…` and `name?`, and
/// one such as `name` would skip `name2`, which sorts between `name` and `name=`.
fn set_label_bound(buf: &mut Vec<u8>, name: &str, above: bool) {
    buf.clear();
    buf.extend_from_slice(name.as_bytes());
    buf.push(b'=');
    if above {
        buf.push(0xFF);
    }
}

/// What a name scan does after reading part of its range under one hold.
enum NameStep<'p> {
    /// The range ran out.
    Done,
    /// The callback asked to stop.
    Stopped,
    /// Read on past this rejected name's values; give the lock back first if `release`.
    Skip { name: &'p str, release: bool },
    /// Give the lock back and resume beyond this key.
    Release(&'p [u8]),
}

/// The state a name scan carries from one hold to the next.
struct NameScan<'f, F> {
    filter: &'f dyn FuzzyFilter,
    f: F,
    entries_per_hold: usize,
    read_this_hold: usize,
    /// The name of the entries being read. Owned, so that a name whose entries straddle a
    /// release goes on accumulating after the lock is taken again.
    curr_name: String,
    /// If `curr_name` was accepted: its score and the cardinality summed so far.
    pending: Option<(f64, usize)>,
}

impl<F> NameScan<'_, F>
where
    F: FnMut(&str, f64, usize) -> ControlFlow<()>,
{
    /// Hand the completed name, if it was accepted, to the callback.
    fn emit(&mut self) -> ControlFlow<()> {
        match self.pending.take() {
            Some((score, cardinality)) => (self.f)(&self.curr_name, score, cardinality),
            None => ControlFlow::Continue(()),
        }
    }

    fn read<'p>(
        &mut self,
        postings: &Postings,
        entries: impl Iterator<Item = (&'p IndexKey, &'p PostingsBitmap)>,
    ) -> NameStep<'p> {
        for (key, map) in entries {
            self.read_this_hold += 1;
            let hold_full = self.read_this_hold >= self.entries_per_hold;
            // An entry only hidden or tombstoned series carry does not exist yet, and a
            // malformed one is passed over: neither is a name, but both count toward the hold.
            let visible = postings.visible_cardinality(map) as usize;
            let name = if visible == 0 {
                None
            } else {
                key.split().map(|(name, _)| name)
            };
            if let Some(name) = name {
                if self.curr_name != name {
                    if self.emit().is_break() {
                        return NameStep::Stopped;
                    }
                    self.curr_name.clear();
                    self.curr_name.push_str(name);
                    let (accepted, score) = self.filter.accept(name);
                    if !accepted {
                        return NameStep::Skip {
                            name,
                            release: hold_full,
                        };
                    }
                    self.pending = Some((score, visible));
                } else if let Some((_, cardinality)) = self.pending.as_mut() {
                    *cardinality += visible;
                }
            }
            if hold_full {
                return NameStep::Release(key.as_ref());
            }
        }
        NameStep::Done
    }
}

/// Hand each label name in the label index between the keys `after` and `before` (both
/// exclusive, `None` for no bound) that `filter` accepts to `f`, with its score and the visible
/// series count summed over its values, until `f` breaks.
///
/// Names come in index-key order, descending if `descending`. That is the byte order of the
/// `name=value` keys, which is not quite the order of the names: `job2=…` sorts before `job=…`,
/// because `2` sorts before `=`. See [`rank_label_names`] for what that costs a value-ordered
/// search.
///
/// It takes the postings read lock itself and gives it back every `entries_per_hold` entries,
/// so it must not be called with that lock held. Giving it back costs the scan its
/// point-in-time view: an entry added behind the resume point after it was passed is not
/// seen. A name whose entries straddle a release is still reported once, with one count summed
/// across the holds. `f` gets each name as it is completed; the `&str` is valid only for that
/// call.
fn scan_label_names_in_holds<F>(
    index: &TimeSeriesIndex,
    filter: &dyn FuzzyFilter,
    descending: bool,
    after: Option<&[u8]>,
    before: Option<&[u8]>,
    entries_per_hold: usize,
    f: F,
) where
    F: FnMut(&str, f64, usize) -> ControlFlow<()>,
{
    let mut scan = NameScan {
        filter,
        f,
        entries_per_hold: entries_per_hold.max(1),
        read_this_hold: 0,
        curr_name: String::new(),
        pending: None,
    };
    let (fixed, start) = if descending {
        (after, before)
    } else {
        (before, after)
    };
    let fixed: Bound<&[u8]> = fixed.map_or(Bound::Unbounded, Bound::Excluded);
    // The moving end of the range: each restart, whether to skip a rejected name or to take
    // the lock again, reads on beyond it — above it ascending, below it descending. One
    // buffer serves every restart.
    let mut resume: Vec<u8> = start.map(<[u8]>::to_vec).unwrap_or_default();
    let mut resuming = start.is_some();

    loop {
        let postings = index.get_postings();
        scan.read_this_hold = 0;
        loop {
            let moving: Bound<&[u8]> = if resuming {
                Bound::Excluded(resume.as_slice())
            } else {
                Bound::Unbounded
            };
            let step = if descending {
                let entries = postings.label_index.range::<[u8], _>((fixed, moving));
                scan.read(&postings, entries.rev())
            } else {
                let entries = postings.label_index.range::<[u8], _>((moving, fixed));
                scan.read(&postings, entries)
            };
            resuming = true;
            match step {
                NameStep::Done => {
                    // The last name read is still pending.
                    let _ = scan.emit();
                    return;
                }
                NameStep::Stopped => return,
                NameStep::Skip { name, release } => {
                    set_label_bound(&mut resume, name, !descending);
                    if release {
                        break;
                    }
                }
                NameStep::Release(key) => {
                    resume.clear();
                    resume.extend_from_slice(key);
                    break;
                }
            }
        }
    }
}

/// The values of one label, in index order, read at most `entries_per_hold` entries per hold
/// of the postings read lock.
///
/// Under each hold `check` turns an entry and its visible series count, which is never zero, into a
/// result or rejects it, and the results are
/// buffered, owned, for after the lock is given back: nothing borrowed from the postings
/// outlives a hold. A hold also ends once `results_per_hold` results are buffered, so a
/// value-order read with `LIMIT n`, which needs only `n + 1`, scores no more entries than a
/// single unbroken scan would.
///
/// It is not a point-in-time view: a value added behind the resume point after it was passed
/// is not seen. Callers run it under [`TimeSeriesIndex::read_without_swap`].
struct LabelValueScan<'a, F> {
    index: &'a TimeSeriesIndex,
    /// See [`first_label_key`].
    first: Vec<u8>,
    /// See [`past_label_keys`].
    past: Vec<u8>,
    descending: bool,
    /// The last key read: the next hold continues beyond it. `None` before the first hold.
    resume: Option<Vec<u8>>,
    entries_per_hold: usize,
    results_per_hold: usize,
    buffered: VecDeque<LabelSearchResult>,
    done: bool,
    check: F,
}

impl<'a, F> LabelValueScan<'a, F>
where
    F: FnMut(&IndexKey, usize) -> Option<LabelSearchResult>,
{
    fn new(
        index: &'a TimeSeriesIndex,
        label_name: &str,
        descending: bool,
        entries_per_hold: usize,
        results_per_hold: usize,
        check: F,
    ) -> Self {
        Self {
            index,
            first: first_label_key(label_name),
            past: past_label_keys(label_name),
            descending,
            resume: None,
            entries_per_hold: entries_per_hold.max(1),
            results_per_hold: results_per_hold.max(1),
            buffered: VecDeque::new(),
            done: false,
            check,
        }
    }

    /// One hold: read on from the resume point into `buffered`.
    fn read_hold(&mut self) {
        let postings = self.index.get_postings();
        let lower: Bound<&[u8]> = match (self.descending, self.resume.as_deref()) {
            (false, Some(after)) => Bound::Excluded(after),
            _ => Bound::Included(self.first.as_slice()),
        };
        let upper: Bound<&[u8]> = match (self.descending, self.resume.as_deref()) {
            (true, Some(below)) => Bound::Excluded(below),
            _ => Bound::Excluded(self.past.as_slice()),
        };
        let range = postings.label_index.range::<[u8], _>((lower, upper));
        let limits = (self.entries_per_hold, self.results_per_hold);
        let stopped_at = if self.descending {
            buffer_label_values(
                &postings,
                range.rev(),
                &mut self.check,
                &mut self.buffered,
                limits,
            )
        } else {
            buffer_label_values(
                &postings,
                range,
                &mut self.check,
                &mut self.buffered,
                limits,
            )
        };
        match stopped_at {
            Some(key) => {
                let resume = self.resume.get_or_insert_with(Vec::new);
                resume.clear();
                resume.extend_from_slice(key);
            }
            // The range ran out: the scan is done.
            None => self.done = true,
        }
    }
}

/// One hold of a [`LabelValueScan`], in either direction: buffer what `check` makes of each
/// entry until `entries_per_hold` entries are read or `results_per_hold` results buffered, and
/// return the key it stopped at, or `None` if the range ran out.
fn buffer_label_values<'p, F>(
    postings: &Postings,
    entries: impl Iterator<Item = (&'p IndexKey, &'p PostingsBitmap)>,
    check: &mut F,
    buffered: &mut VecDeque<LabelSearchResult>,
    (entries_per_hold, results_per_hold): (usize, usize),
) -> Option<&'p [u8]>
where
    F: FnMut(&IndexKey, usize) -> Option<LabelSearchResult>,
{
    for (read, (key, map)) in entries.enumerate() {
        let visible = postings.visible_cardinality(map) as usize;
        if visible > 0
            && let Some(result) = check(key, visible)
        {
            buffered.push_back(result);
        }
        if read + 1 == entries_per_hold || buffered.len() >= results_per_hold {
            return Some(key.as_ref());
        }
    }
    None
}

impl<F> Iterator for LabelValueScan<'_, F>
where
    F: FnMut(&IndexKey, usize) -> Option<LabelSearchResult>,
{
    type Item = LabelSearchResult;

    fn next(&mut self) -> Option<LabelSearchResult> {
        while self.buffered.is_empty() && !self.done {
            self.read_hold();
        }
        self.buffered.pop_front()
    }
}

/// The sum of the visible cardinalities of `name`'s values, read at most `entries_per_hold` entries
/// per hold of the postings read lock: a high-cardinality name has a million entries.
fn label_cardinality_in_holds(
    index: &TimeSeriesIndex,
    name: &str,
    entries_per_hold: usize,
) -> usize {
    let entries_per_hold = entries_per_hold.max(1);
    let first = first_label_key(name);
    let past = past_label_keys(name);
    // The last key read, once a hold has ended early: one buffer for every hold.
    let mut resume: Vec<u8> = Vec::new();
    let mut resuming = false;
    let mut total = 0u64;
    loop {
        let postings = index.get_postings();
        let lower: Bound<&[u8]> = if resuming {
            Bound::Excluded(resume.as_slice())
        } else {
            Bound::Included(first.as_slice())
        };
        let mut stopped_at: Option<&[u8]> = None;
        for (read, (key, map)) in postings
            .label_index
            .range::<[u8], _>((lower, Bound::Excluded(past.as_slice())))
            .enumerate()
        {
            total += postings.visible_cardinality(map);
            if read + 1 == entries_per_hold {
                stopped_at = Some(key.as_ref());
                break;
            }
        }
        match stopped_at {
            Some(key) => {
                resume.clear();
                resume.extend_from_slice(key);
                resuming = true;
            }
            None => return total as usize,
        }
    }
}

/// The best `limit` label names under `order` that `filter` accepts, each with the visible
/// series count summed over its values, and whether there are more. See [`rank_label_names_in_holds`].
fn rank_label_names(
    index: &TimeSeriesIndex,
    filter: &dyn FuzzyFilter,
    order: SearchResultOrdering,
    limit: usize,
) -> ValkeyResult<LabelQueryResult> {
    index.read_without_swap(|| {
        Ok(rank_label_names_in_holds(
            index,
            filter,
            order,
            limit,
            LABEL_SCAN_ENTRIES_PER_HOLD,
        ))
    })
}

/// [`rank_label_names`] reading at most `entries_per_hold` entries per lock hold.
///
/// A score or cardinality order must see every name. A value order stops the scan, as a
/// stream would, once it has `limit + 1` names, but the scan's order is that of the index
/// keys, not of the names ([`scan_label_names_in_holds`]), so it then reads the few names that
/// could still belong in the answer. Take `w`, the worst name kept when the scan stops:
///
/// * ascending, a name `p` not yet read can sort before `w` only if `p` is a proper prefix of
///   `w` followed in it by a byte below `=` (`job` and `job2`): its keys come after `w`'s.
///   Each is looked up directly.
/// * descending, a name not yet read can sort after `w` only if it extends `w` by a byte below
///   `=`; all of those sit in one key range, just below `w`'s own keys, which is read.
///
/// For any other pair of names the first differing byte lies inside both names and decides
/// the key order and the name order alike.
fn rank_label_names_in_holds(
    index: &TimeSeriesIndex,
    filter: &dyn FuzzyFilter,
    order: SearchResultOrdering,
    limit: usize,
    entries_per_hold: usize,
) -> LabelQueryResult {
    let mut top = TopK::new(order, limit);
    let offer = |top: &mut TopK, name: &str, score: f64, cardinality: usize| {
        let key = RankKey {
            value: name,
            score,
            cardinality,
        };
        // The name is valid only for the call, so only one that is kept is copied.
        top.offer_with(key, || LabelSearchResult {
            value: name.to_owned(),
            score,
            cardinality,
        });
    };

    let descending = match order {
        SearchResultOrdering::ValueAsc => false,
        SearchResultOrdering::ValueDesc => true,
        SearchResultOrdering::ScoreDesc
        | SearchResultOrdering::CardinalityAsc
        | SearchResultOrdering::CardinalityDesc => {
            scan_label_names_in_holds(
                index,
                filter,
                false,
                None,
                None,
                entries_per_hold,
                |name, score, cardinality| {
                    offer(&mut top, name, score, cardinality);
                    ControlFlow::Continue(())
                },
            );
            return top.finish();
        }
    };

    // The last name the scan read, if it stopped early: the names at and before it in the
    // scan's order have all been read.
    let mut last: Option<String> = None;
    scan_label_names_in_holds(
        index,
        filter,
        descending,
        None,
        None,
        entries_per_hold,
        |name, score, cardinality| {
            offer(&mut top, name, score, cardinality);
            if top.offered > top.limit {
                last = Some(name.to_owned());
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        },
    );
    let Some(last) = last else {
        // The scan read every name.
        return top.finish();
    };
    let worst = top.worst_kept().value.clone();
    let read_to = first_label_key(&last);

    if descending {
        // Extensions of `worst` by a byte below `=` lie between the key `worst` and
        // `worst=`; only those below `last`'s keys are unread.
        let below = first_label_key(&worst).min(read_to);
        if worst.as_bytes() < below.as_slice() {
            scan_label_names_in_holds(
                index,
                filter,
                false,
                Some(worst.as_bytes()),
                Some(&below),
                entries_per_hold,
                |name, score, cardinality| {
                    offer(&mut top, name, score, cardinality);
                    ControlFlow::Continue(())
                },
            );
        }
    } else {
        let mut prefix_key = Vec::new();
        for (end, byte) in worst.bytes().enumerate().skip(1) {
            if byte >= b'=' {
                continue;
            }
            // ASCII, so `end` is a character boundary.
            let prefix = &worst[..end];
            set_label_bound(&mut prefix_key, prefix, false);
            if prefix_key <= read_to {
                continue; // Read already.
            }
            let (accepted, score) = filter.accept(prefix);
            if !accepted {
                continue;
            }
            match label_cardinality_in_holds(index, prefix, entries_per_hold) {
                0 => {}
                cardinality => offer(&mut top, prefix, score, cardinality),
            }
        }
    }
    top.finish()
}

/// `collect_unscoped_label_names` answers a label name search from the label index alone.
///
/// Takes the index rather than locked postings so that each scan owns its postings read lock
/// and gives it back periodically, so writers are not starved (see
/// [`scan_label_names_in_holds`]). No lock is held here: the scans take it themselves, and a
/// read lock taken again behind a waiting writer can deadlock.
fn collect_unscoped_label_names(
    index: &TimeSeriesIndex,
    hints: &SearchHints,
) -> ValkeyResult<LabelQueryResult> {
    let limit = normalize_limit(hints.limit);
    with_search_filter(hints, |filter| {
        rank_label_names(index, filter, hints.order_by, limit)
    })
}

/// `collect_unscoped_label_values` answers a value search from the label index alone.
///
/// Like the name scans, it takes the postings read lock itself and gives it back every
/// [`LABEL_SCAN_ENTRIES_PER_HOLD`] entries (see [`LabelValueScan`]), so it must not be called
/// with the postings lock held. On the analysis lane a single hold over every value of a large
/// label would block the main thread's next index write for the whole scan.
fn collect_unscoped_label_values(
    index: &TimeSeriesIndex,
    label_name: &str,
    hints: &SearchHints,
) -> ValkeyResult<LabelQueryResult> {
    collect_unscoped_label_values_in_holds(index, label_name, hints, LABEL_SCAN_ENTRIES_PER_HOLD)
}

/// [`collect_unscoped_label_values`] reading at most `entries_per_hold` entries per lock hold.
fn collect_unscoped_label_values_in_holds(
    index: &TimeSeriesIndex,
    label_name: &str,
    hints: &SearchHints,
    entries_per_hold: usize,
) -> ValkeyResult<LabelQueryResult> {
    let limit = normalize_limit(hints.limit);

    index.read_without_swap(|| {
        Ok(with_search_filter(hints, |filter| {
            let check = |entry: &IndexKey, cardinality: usize| {
                let (_, value) = entry.split()?;
                let (accepted, score) = filter.accept(value);
                accepted.then(|| LabelSearchResult {
                    value: value.to_owned(),
                    score,
                    cardinality,
                })
            };

            // A value-order read stops at `limit + 1` results, so no hold buffers more.
            match hints.order_by {
                SearchResultOrdering::ValueAsc => collect_limited(
                    LabelValueScan::new(
                        index,
                        label_name,
                        false,
                        entries_per_hold,
                        limit + 1,
                        check,
                    ),
                    limit,
                ),
                SearchResultOrdering::ValueDesc => collect_limited(
                    LabelValueScan::new(
                        index,
                        label_name,
                        true,
                        entries_per_hold,
                        limit + 1,
                        check,
                    ),
                    limit,
                ),
                SearchResultOrdering::ScoreDesc
                | SearchResultOrdering::CardinalityAsc
                | SearchResultOrdering::CardinalityDesc => collect_top_k(
                    LabelValueScan::new(
                        index,
                        label_name,
                        false,
                        entries_per_hold,
                        usize::MAX,
                        check,
                    ),
                    hints.order_by,
                    limit,
                ),
            }
        }))
    })
}

fn collect_label_names<S: GilSource + ?Sized>(
    src: &S,
    select_hints: Option<SelectHints>,
    hints: &SearchHints,
) -> TsdbResult<LabelQueryResult> {
    if can_use_unscoped_scan(select_hints.as_ref()) {
        // The index alone answers this: no series is opened, so no module lock is taken.
        let index = get_db_index(src.db());
        return Ok(collect_unscoped_label_names(&index, hints)?);
    }

    // At this point we know we have non-empty selectors or a date range.
    let db = src.db();
    let result = select_hints
        .unwrap_or_default()
        .read_planned(db, |cursor| {
            let generation = cursor.generation();

            // Under the lock, only the distinct names are copied out: a hash lookup per label.
            // The fuzzy filter and the cardinality lookups wait until the read is over.
            //
            // Still unbounded: every matched series is scanned, `names` holds every distinct
            // name in the match, and on the main thread (`&Context`) the whole scan is one lock
            // hold. The batched reader bounds only a fan-out share's lock holds, not the scan or
            // the set.
            let mut names: AHashSet<String> = AHashSet::with_capacity(INITIAL_RESULT_CAPACITY);
            for_each_planned_series(src, cursor, DEFAULT_SERIES_BATCH_SIZE, |_ctx, ts, _key| {
                for label in ts.labels.iter() {
                    // Not `names.insert(label.name.to_string())`: that allocates for every
                    // label, seen or not.
                    if !names.contains(label.name) {
                        names.insert(label.name.to_string());
                    }
                }
                Ok(ControlFlow::Continue(()))
            })?;

            let need_cardinality = matches!(
                hints.order_by,
                SearchResultOrdering::CardinalityAsc | SearchResultOrdering::CardinalityDesc
            ) || hints.include_meta;

            // A name's cardinality is its database-wide one, not the selection's. Each is read in
            // bounded holds of the postings lock, taken only for names the filter accepts: one
            // hold across every name would keep the lock for the sum of their values, a million
            // entries for a name such as `instance`, while index writers wait. Never taken while
            // the batched read runs either: it takes the postings lock itself, for writing when it
            // has a stale id to record, and `RwLock` is not reentrant.
            let index = need_cardinality.then(|| get_db_index(db));

            let result = with_search_filter(hints, |filter| {
                let mut result = Vec::with_capacity(names.len());
                for name in names {
                    let (accepted, score) = filter.accept(&name);
                    if !accepted {
                        continue;
                    }
                    let cardinality = match index.as_ref() {
                        Some(index) => match label_cardinality_in_holds(
                            index,
                            &name,
                            LABEL_SCAN_ENTRIES_PER_HOLD,
                        ) {
                            0 => continue,
                            cardinality => cardinality,
                        },
                        None => 0,
                    };
                    result.push(LabelSearchResult {
                        value: name,
                        score,
                        cardinality,
                    });
                }
                apply_search_hints(result, hints)
            });
            // The cardinalities are read after the scan, in holds of their own: they belong to
            // the scanned database only if no `SWAPDB` came in between.
            if index.is_some() {
                ensure_generation(db, generation)?;
            }
            Ok(result)
        })?;
    Ok(result)
}

fn collect_label_values<S: GilSource + ?Sized>(
    src: &S,
    label_name: &str,
    select_hints: Option<SelectHints>,
    search_hints: &SearchHints,
) -> TsdbResult<LabelQueryResult> {
    if label_name.is_empty() {
        return Ok(LabelQueryResult::default());
    }

    if can_use_unscoped_scan(select_hints.as_ref()) {
        // The index alone answers this: no series is opened, so no module lock is taken. No
        // postings lock either: the scan takes it a hold at a time.
        let index = get_db_index(src.db());
        return Ok(collect_unscoped_label_values(
            &index,
            label_name,
            search_hints,
        )?);
    }

    // At this point we know we have non-empty selectors or a date range.
    //
    // Under the lock, only each distinct value and how many matched series carry it are
    // copied out. The filter then runs once per distinct value, after the read.
    //
    // Still unbounded, as in `collect_label_names`: `counts` holds every distinct value in
    // the match, and a main-thread scan is one lock hold.
    let counts = select_hints
        .unwrap_or_default()
        .read_planned(src.db(), |cursor| {
            let mut counts: AHashMap<String, usize> =
                AHashMap::with_capacity(INITIAL_RESULT_CAPACITY);
            for_each_planned_series(src, cursor, DEFAULT_SERIES_BATCH_SIZE, |_ctx, ts, _key| {
                if let Some(label) = ts.get_label(label_name) {
                    match counts.get_mut(label.value) {
                        Some(count) => *count += 1,
                        None => {
                            counts.insert(label.value.to_string(), 1);
                        }
                    }
                }
                Ok(ControlFlow::Continue(()))
            })?;
            Ok(counts)
        })?;

    let need_cardinality = matches!(
        search_hints.order_by,
        SearchResultOrdering::CardinalityAsc | SearchResultOrdering::CardinalityDesc
    ) || search_hints.include_meta;

    Ok(with_search_filter(search_hints, |filter| {
        let mut result = Vec::with_capacity(counts.len());
        for (value, count) in counts {
            let (accepted, score) = filter.accept(&value);
            if accepted {
                result.push(LabelSearchResult {
                    value,
                    score,
                    cardinality: if need_cardinality { count } else { 0 },
                });
            }
        }
        apply_search_hints(result, search_hints)
    }))
}

/// The coarse gate on label discovery, checked under one short lock hold: on a fan-out
/// share the caller's ACL identity is only installed while the lock is held.
pub(crate) fn check_label_query_permissions<S: GilSource + ?Sized>(src: &S) -> TsdbResult<()> {
    let ctx = src.lock()?;
    check_metadata_permissions(&ctx).map_err(|_| {
        TsdbError::PermissionsError("insufficient permissions to query labels".to_string())
    })
}

#[derive(Default)]
pub struct DefaultLabelQuerier;

impl LabelQuerier for DefaultLabelQuerier {
    fn get_label_names<S: GilSource + ?Sized>(
        &self,
        src: &S,
        select_hints: Option<SelectHints>,
        search_hints: Option<&SearchHints>,
    ) -> TsdbResult<LabelQueryResult> {
        check_label_query_permissions(src)?;
        let default_hints = SearchHints::default();
        let hints = search_hints.unwrap_or(&default_hints);
        collect_label_names(src, select_hints, hints)
    }

    fn get_label_values<S: GilSource + ?Sized>(
        &self,
        src: &S,
        name: &str,
        select_hints: Option<SelectHints>,
        search_hints: Option<&SearchHints>,
    ) -> TsdbResult<LabelQueryResult> {
        check_label_query_permissions(src)?;
        let default_hints = SearchHints::default();
        let hints = search_hints.unwrap_or(&default_hints);
        collect_label_values(src, name, select_hints, hints)
    }

    fn get_metric_names<S: GilSource + ?Sized>(
        &self,
        src: &S,
        select_hints: Option<SelectHints>,
        search_hints: Option<&SearchHints>,
    ) -> TsdbResult<LabelQueryResult> {
        check_label_query_permissions(src)?;
        let default_hints = SearchHints::default();
        let hints = search_hints.unwrap_or(&default_hints);
        collect_label_values(src, METRIC_NAME_LABEL, select_hints, hints)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        FuzzyFilter, LabelQueryResult, LabelSearchResult, NoOpSearchMatcher, SearchHints,
        SearchResultOrdering, TimeSeriesIndex, apply_search_hints, collect_top_k,
        collect_unscoped_label_names, collect_unscoped_label_values,
        collect_unscoped_label_values_in_holds, compare_search_results, label_cardinality_in_holds,
        label_search_exceeds_inline_budget, rank_label_names_in_holds, scan_label_names_in_holds,
    };
    use crate::labels::{Label, MetricName};
    use crate::series::TimeSeries;
    use crate::series::index::postings::Postings;
    use itertools::Itertools;
    use std::ops::ControlFlow;

    /// One series on each side of an import: `imported` will be hidden, `local` stays visible.
    fn index_with_hidden_series() -> TimeSeriesIndex {
        let series = |id, labels: &[(&str, &str)]| {
            let mut series = TimeSeries::new();
            series.id = id;
            let labels: Vec<Label> = labels.iter().map(|(n, v)| Label::new(*n, *v)).collect();
            series.labels = MetricName::new(&labels);
            series
        };
        let imported = b"{imported}1".as_slice();
        let mut postings = Postings::default();
        postings.index_timeseries(
            &series(
                1,
                &[("env", "imported"), ("job", "api"), ("only_imported", "x")],
            ),
            imported,
        );
        postings.index_timeseries(&series(2, &[("env", "local"), ("job", "api")]), b"{local}1");
        postings.hide_indexed_keys_in_slots(&range_set_blaze::RangeSetBlaze::from_iter([
            crate::fanout::calculate_hash_slot(imported),
        ]));
        index_of(postings)
    }

    /// An index holding `postings`, for the collectors that take the index and lock it
    /// themselves.
    fn index_of(postings: Postings) -> TimeSeriesIndex {
        TimeSeriesIndex::from_postings(postings)
    }

    struct StaticScoreFilter;

    impl FuzzyFilter for StaticScoreFilter {
        fn accept(&self, value: &str) -> (bool, f64) {
            match value {
                "us-east" => (true, 0.9),
                "us-west" => (true, 0.9),
                "eu-west" => (true, 0.4),
                _ => (false, 0.0),
            }
        }
    }

    struct StaticNameScoreFilter;

    impl FuzzyFilter for StaticNameScoreFilter {
        fn accept(&self, value: &str) -> (bool, f64) {
            match value {
                "alpha" => (true, 0.9),
                "beta" => (true, 0.9),
                "gamma" => (true, 0.4),
                _ => (false, 0.0),
            }
        }
    }

    fn build_label_values_postings() -> Postings {
        let mut postings = Postings::default();
        postings.add_posting_for_label_value(1, "region", "us-east");
        postings.add_posting_for_label_value(2, "region", "us-west");
        postings.add_posting_for_label_value(3, "region", "eu-west");
        // Distractor label to verify prefix scoping.
        postings.add_posting_for_label_value(4, "service", "api");
        postings
    }

    #[test]
    fn label_search_budget_scopes_value_probes_to_the_requested_label() {
        let index = index_of(build_label_values_postings());
        let hints = SearchHints::default();
        assert!(!label_search_exceeds_inline_budget(
            &index,
            Some("service"),
            false,
            None,
            &hints,
            2,
        ));
        assert!(!label_search_exceeds_inline_budget(
            &index,
            Some("missing"),
            false,
            None,
            &hints,
            2,
        ));
        assert!(!label_search_exceeds_inline_budget(
            &index,
            Some(""),
            false,
            None,
            &hints,
            2,
        ));
        assert!(label_search_exceeds_inline_budget(
            &index,
            Some("region"),
            false,
            None,
            &hints,
            2,
        ));
        assert!(!label_search_exceeds_inline_budget(
            &index,
            Some("region"),
            false,
            None,
            &hints,
            3,
        ));
    }

    #[test]
    fn label_search_budget_only_uses_limits_for_unfiltered_value_order() {
        let index = index_of(build_label_values_postings());
        for order_by in [
            SearchResultOrdering::ValueAsc,
            SearchResultOrdering::ValueDesc,
        ] {
            let mut hints = SearchHints {
                limit: 1,
                order_by,
                ..Default::default()
            };
            // One result and one lookahead fit the budget.
            assert!(!label_search_exceeds_inline_budget(
                &index,
                Some("region"),
                false,
                None,
                &hints,
                2,
            ));
            // A name's cardinality may span all its values, even with LIMIT 1.
            assert!(label_search_exceeds_inline_budget(
                &index, None, false, None, &hints, 2,
            ));
            hints.filter = Some(Box::new(StaticScoreFilter));
            assert!(label_search_exceeds_inline_budget(
                &index,
                Some("region"),
                false,
                None,
                &hints,
                2,
            ));
        }
        for order_by in [
            SearchResultOrdering::ScoreDesc,
            SearchResultOrdering::CardinalityAsc,
            SearchResultOrdering::CardinalityDesc,
        ] {
            let hints = SearchHints {
                limit: 1,
                order_by,
                ..Default::default()
            };
            assert!(label_search_exceeds_inline_budget(
                &index,
                Some("region"),
                false,
                None,
                &hints,
                2,
            ));
        }
    }

    #[test]
    fn label_search_budget_counts_empty_entries_before_short_circuiting() {
        let mut postings = build_label_values_postings();
        // The middle entry is empty, so either direction must scan three entries
        // to find the result and the lookahead.
        postings
            .label_index
            .get_mut(b"region=us-east\0".as_slice())
            .unwrap()
            .clear();
        let index = index_of(postings);
        for order_by in [
            SearchResultOrdering::ValueAsc,
            SearchResultOrdering::ValueDesc,
        ] {
            let hints = SearchHints {
                limit: 1,
                order_by,
                ..Default::default()
            };
            assert!(label_search_exceeds_inline_budget(
                &index,
                Some("region"),
                false,
                None,
                &hints,
                2,
            ));
        }
    }

    /// `region` values that no read can see, more of them than the budget used below.
    fn postings_with_invisible_values(mask: impl FnOnce(&mut Postings, &[u64])) -> Postings {
        let ids: Vec<u64> = (1..=6).collect();
        let mut postings = Postings::default();
        for &id in &ids {
            postings.add_posting_for_label_value(id, "region", &format!("r{id}"));
            postings
                .id_to_key
                .insert(id, format!("{{same}}{id}").into_bytes().into());
        }
        mask(&mut postings, &ids);
        postings
    }

    #[test]
    fn label_search_budget_does_not_count_invisible_entries_as_results() {
        // The scan skips these, so stopping the probe at them would approve an inline search that
        // must read past every one of them.
        let tombstoned = postings_with_invisible_values(|postings, ids| {
            postings.mark_ids_as_stale(ids);
        });
        let hidden = postings_with_invisible_values(|postings, _| {
            postings.hide_indexed_keys_in_slots(&range_set_blaze::RangeSetBlaze::from_iter([
                crate::fanout::calculate_hash_slot(b"{same}"),
            ]));
        });
        for postings in [tombstoned, hidden] {
            let index = index_of(postings);
            for order_by in [
                SearchResultOrdering::ValueAsc,
                SearchResultOrdering::ValueDesc,
            ] {
                let hints = SearchHints {
                    limit: 1,
                    order_by,
                    ..Default::default()
                };
                assert!(
                    label_search_exceeds_inline_budget(
                        &index,
                        Some("region"),
                        false,
                        None,
                        &hints,
                        3,
                    ),
                    "{order_by:?}"
                );
            }
        }
    }

    #[test]
    fn label_search_budget_does_not_apply_result_limits_to_scoped_reads() {
        let index = index_of(build_label_values_postings());
        let hints = SearchHints {
            limit: 1,
            ..Default::default()
        };
        assert!(label_search_exceeds_inline_budget(
            &index,
            Some("service"),
            true,
            None,
            &hints,
            2,
        ));
        assert!(!label_search_exceeds_inline_budget(
            &index, None, true, None, &hints, 4,
        ));

        // A database can contain many series with the same label value.
        let mut postings = index.get_postings_mut();
        postings
            .id_to_key
            .extend((0..=super::DEFAULT_SERIES_BATCH_SIZE.get()).map(|id| {
                (
                    id as u64,
                    format!("series:{id}").into_bytes().into_boxed_slice(),
                )
            }));
        drop(postings);
        assert!(label_search_exceeds_inline_budget(
            &index,
            Some("service"),
            true,
            None,
            &hints,
            4,
        ));
    }

    fn values(result: LabelQueryResult) -> Vec<String> {
        result.into_iter().map(|r| r.value).collect()
    }

    #[test]
    fn collect_unscoped_label_values_orders_ascending() {
        let postings = build_label_values_postings();
        let hints = SearchHints {
            filter: None,
            limit: 10,
            order_by: SearchResultOrdering::ValueAsc,
            include_meta: false,
        };

        let result = collect_unscoped_label_values(&index_of(postings), "region", &hints).unwrap();

        assert!(!result.has_more);
        assert_eq!(values(result), vec!["eu-west", "us-east", "us-west"]);
    }

    #[test]
    fn collect_unscoped_label_values_orders_descending() {
        let postings = build_label_values_postings();
        let hints = SearchHints {
            filter: None,
            limit: 10,
            order_by: SearchResultOrdering::ValueDesc,
            include_meta: false,
        };

        let result = collect_unscoped_label_values(&index_of(postings), "region", &hints).unwrap();

        assert!(!result.has_more);
        assert_eq!(values(result), vec!["us-west", "us-east", "eu-west"]);
    }

    fn build_label_names_postings() -> Postings {
        let mut postings = Postings::default();
        postings.add_posting_for_label_value(1, "alpha", "alpha");
        postings.add_posting_for_label_value(2, "beta", "beta");
        postings.add_posting_for_label_value(3, "gamma", "gamma");
        postings.add_posting_for_label_value(4, "service", "api");
        postings
    }

    fn build_label_names_metric_index_postings() -> Postings {
        let mut postings = Postings::default();
        postings.add_posting_for_label_value(1, "alpha", "a");
        postings.add_posting_for_label_value(2, "beta", "b");
        postings.add_posting_for_label_value(3, "gamma", "g");
        postings.add_posting_for_label_value(4, "service", "api");
        postings
    }

    #[test]
    fn collect_unscoped_label_names_orders_ascending() {
        let postings = build_label_names_metric_index_postings();
        let hints = SearchHints {
            filter: None,
            limit: 10,
            order_by: SearchResultOrdering::ValueAsc,
            include_meta: false,
        };

        let result = collect_unscoped_label_names(&index_of(postings), &hints).unwrap();

        assert!(!result.has_more);
        let values: Vec<String> = result.into_iter().map(|r| r.value).collect();
        assert_eq!(values, vec!["alpha", "beta", "gamma", "service"]);
    }

    #[test]
    fn collect_unscoped_label_names_orders_descending() {
        let postings = build_label_names_metric_index_postings();
        let hints = SearchHints {
            filter: None,
            limit: 10,
            order_by: SearchResultOrdering::ValueDesc,
            include_meta: false,
        };

        let result = collect_unscoped_label_names(&index_of(postings), &hints).unwrap();

        assert!(!result.has_more);
        let values: Vec<String> = result.into_iter().map(|r| r.value).collect();
        assert_eq!(values, vec!["service", "gamma", "beta", "alpha"]);
    }

    #[test]
    fn collect_unscoped_label_values_sets_has_more_when_truncated() {
        let postings = build_label_values_postings();
        // limit of 2 when there are 3 region values → has_more must be true
        let hints = SearchHints {
            filter: None,
            limit: 2,
            order_by: SearchResultOrdering::ValueAsc,
            include_meta: false,
        };

        let result = collect_unscoped_label_values(&index_of(postings), "region", &hints).unwrap();

        assert!(result.has_more);
        assert_eq!(result.results.len(), 2);
    }

    fn build_label_names_cardinality_postings() -> Postings {
        let mut postings = Postings::default();
        // label 'b' -> cardinality 1
        postings.add_posting_for_label_value(1, "beta", "b");
        // label 'a' -> cardinality 2
        postings.add_posting_for_label_value(2, "alpha", "a");
        postings.add_posting_for_label_value(3, "alpha", "a");
        // label 'c' -> cardinality 3
        postings.add_posting_for_label_value(4, "gamma", "c");
        postings.add_posting_for_label_value(5, "gamma", "c");
        postings.add_posting_for_label_value(6, "gamma", "c");
        postings
    }

    fn build_label_values_cardinality_postings() -> Postings {
        let mut postings = Postings::default();
        // us-east -> cardinality 1
        postings.add_posting_for_label_value(1, "region", "us-east");
        // us-west -> cardinality 2
        postings.add_posting_for_label_value(2, "region", "us-west");
        postings.add_posting_for_label_value(3, "region", "us-west");
        // eu-west -> cardinality 3
        postings.add_posting_for_label_value(4, "region", "eu-west");
        postings.add_posting_for_label_value(5, "region", "eu-west");
        postings.add_posting_for_label_value(6, "region", "eu-west");
        postings
    }

    #[test]
    fn collect_unscoped_label_names_cardinality_orders_ascending() {
        let postings = build_label_names_cardinality_postings();
        let hints = SearchHints {
            filter: None,
            limit: 10,
            order_by: SearchResultOrdering::CardinalityAsc,
            include_meta: false,
        };

        let result = collect_unscoped_label_names(&index_of(postings), &hints).unwrap();

        assert!(!result.has_more);
        let values: Vec<String> = result.into_iter().map(|r| r.value).collect();
        // cardinalities: beta=1, alpha=2, gamma=3 -> ascending by cardinality
        assert_eq!(values, vec!["beta", "alpha", "gamma"]);
    }

    #[test]
    fn collect_unscoped_label_names_cardinality_sets_has_more_when_truncated() {
        let postings = build_label_names_cardinality_postings();
        // limit of 2 when there are 3 label names -> has_more must be true
        let hints = SearchHints {
            filter: None,
            limit: 2,
            order_by: SearchResultOrdering::CardinalityAsc,
            include_meta: false,
        };

        let result = collect_unscoped_label_names(&index_of(postings), &hints).unwrap();

        assert!(result.has_more);
        assert_eq!(result.results.len(), 2);
        let values: Vec<String> = result.into_iter().map(|r| r.value).collect();
        assert_eq!(values, vec!["beta", "alpha"]);
    }

    #[test]
    fn collect_unscoped_label_names_score_orders_descending_with_ties() {
        let postings = build_label_names_postings();
        let hints = SearchHints {
            filter: Some(Box::new(StaticNameScoreFilter)),
            limit: 2,
            order_by: SearchResultOrdering::ScoreDesc,
            include_meta: false,
        };

        let result = collect_unscoped_label_names(&index_of(postings), &hints).unwrap();

        assert!(result.has_more);
        assert_eq!(values(result), vec!["alpha", "beta"]);
    }

    #[test]
    fn collect_unscoped_label_names_score_truncation_keeps_best_tie() {
        let postings = build_label_names_postings();
        let hints = SearchHints {
            filter: Some(Box::new(StaticNameScoreFilter)),
            limit: 1,
            order_by: SearchResultOrdering::ScoreDesc,
            include_meta: false,
        };

        let result = collect_unscoped_label_names(&index_of(postings), &hints).unwrap();

        assert!(result.has_more);
        assert_eq!(values(result), vec!["alpha"]);
    }

    #[test]
    fn collect_unscoped_label_names_cardinality_desc_replaces_worst_candidate() {
        let postings = build_label_names_cardinality_postings();
        let hints = SearchHints {
            filter: None,
            limit: 1,
            order_by: SearchResultOrdering::CardinalityDesc,
            include_meta: false,
        };

        let result = collect_unscoped_label_names(&index_of(postings), &hints).unwrap();

        assert!(result.has_more);
        assert_eq!(values(result), vec!["gamma"]);
    }

    #[test]
    fn collect_unscoped_label_values_score_orders_descending_with_ties() {
        let postings = build_label_values_postings();
        let index = index_of(postings);
        for limit in [1, 2] {
            let hints = SearchHints {
                filter: Some(Box::new(StaticScoreFilter)),
                limit,
                order_by: SearchResultOrdering::ScoreDesc,
                include_meta: false,
            };
            let result = collect_unscoped_label_values(&index, "region", &hints).unwrap();
            assert!(result.has_more);
            assert_eq!(values(result), ["us-east", "us-west"][..limit]);
        }
    }

    #[test]
    fn collect_unscoped_label_values_cardinality_orders_descending_with_limit() {
        let postings = build_label_values_cardinality_postings();
        let index = index_of(postings);
        for limit in [1, 2] {
            let hints = SearchHints {
                filter: None,
                limit,
                order_by: SearchResultOrdering::CardinalityDesc,
                include_meta: false,
            };
            let result = collect_unscoped_label_values(&index, "region", &hints).unwrap();
            assert!(result.has_more);
            assert_eq!(values(result), ["eu-west", "us-west"][..limit]);
        }
    }

    #[test]
    fn label_query_result_merge_from_coalesces_duplicates() {
        let mut left = LabelQueryResult::new(
            vec![
                LabelSearchResult {
                    value: "a".into(),
                    score: 0.5,
                    cardinality: 2,
                },
                LabelSearchResult {
                    value: "c".into(),
                    score: 0.1,
                    cardinality: 1,
                },
            ],
            false,
        );

        let right = LabelQueryResult::new(
            vec![
                LabelSearchResult {
                    value: "a".into(),
                    score: 0.9,
                    cardinality: 3,
                },
                LabelSearchResult {
                    value: "b".into(),
                    score: 0.7,
                    cardinality: 4,
                },
            ],
            true,
        );

        left.merge_from(right, SearchResultOrdering::ValueAsc, 10);

        assert!(left.has_more);
        assert_eq!(left.results.len(), 3);
        assert_eq!(left.results[0].value, "a");
        assert_eq!(left.results[0].score, 0.9);
        assert_eq!(left.results[0].cardinality, 5);
    }

    #[test]
    fn label_query_result_merge_from_sets_has_more_when_truncated() {
        let mut left = LabelQueryResult::new(
            vec![LabelSearchResult {
                value: "a".into(),
                score: 0.1,
                cardinality: 1,
            }],
            false,
        );

        let right = LabelQueryResult::new(
            vec![
                LabelSearchResult {
                    value: "b".into(),
                    score: 0.2,
                    cardinality: 1,
                },
                LabelSearchResult {
                    value: "c".into(),
                    score: 0.3,
                    cardinality: 1,
                },
            ],
            false,
        );

        left.merge_from(right, SearchResultOrdering::ValueAsc, 2);

        assert!(left.has_more);
        assert_eq!(values(left), vec!["a", "b"]);
    }

    #[test]
    fn collect_top_k_returns_sorted_results() {
        let items = vec![
            LabelSearchResult {
                value: "c".to_string(),
                score: 0.5,
                cardinality: 3,
            },
            LabelSearchResult {
                value: "a".to_string(),
                score: 0.9,
                cardinality: 1,
            },
            LabelSearchResult {
                value: "b".to_string(),
                score: 0.7,
                cardinality: 2,
            },
        ];

        // Test ValueAsc
        let result = collect_top_k(
            items.clone().into_iter(),
            SearchResultOrdering::ValueAsc,
            10,
        );
        assert_eq!(values(result), vec!["a", "b", "c"]);

        // Test ValueDesc
        let result = collect_top_k(
            items.clone().into_iter(),
            SearchResultOrdering::ValueDesc,
            10,
        );
        assert_eq!(values(result), vec!["c", "b", "a"]);

        // Test ScoreDesc
        let result = collect_top_k(
            items.clone().into_iter(),
            SearchResultOrdering::ScoreDesc,
            10,
        );
        assert_eq!(values(result), vec!["a", "b", "c"]);

        // Test CardinalityAsc
        let result = collect_top_k(
            items.clone().into_iter(),
            SearchResultOrdering::CardinalityAsc,
            10,
        );
        assert_eq!(values(result), vec!["a", "b", "c"]);

        // Test CardinalityDesc
        let result = collect_top_k(
            items.clone().into_iter(),
            SearchResultOrdering::CardinalityDesc,
            10,
        );
        assert_eq!(values(result), vec!["c", "b", "a"]);
    }

    /// The best `limit` of `input` by a full sort: what a bounded selection must agree with.
    fn sorted_prefix(
        input: &[LabelSearchResult],
        order: SearchResultOrdering,
        limit: usize,
    ) -> (Vec<LabelSearchResult>, bool) {
        let mut all = input.to_vec();
        all.sort_by(compare_search_results(order));
        let has_more = all.len() > limit;
        all.truncate(limit);
        (all, has_more)
    }

    #[test]
    fn limited_heap_selection_matches_full_sort() {
        // Both ties and distinct ranks; more than limit + 1 candidates must be
        // offered to exercise selection rather than just the final sort.
        let items = [("a", 0.9, 1), ("b", 0.9, 3), ("c", 0.5, 3), ("d", 0.7, 2)].map(
            |(value, score, cardinality)| LabelSearchResult {
                value: value.to_owned(),
                score,
                cardinality,
            },
        );
        let orders = [
            SearchResultOrdering::ValueAsc,
            SearchResultOrdering::ValueDesc,
            SearchResultOrdering::ScoreDesc,
            SearchResultOrdering::CardinalityAsc,
            SearchResultOrdering::CardinalityDesc,
        ];
        for permutation in items.into_iter().permutations(4) {
            for count in 0..=permutation.len() {
                let input = &permutation[..count];
                for order in orders {
                    for limit in 1..=5 {
                        let hints = SearchHints {
                            limit,
                            order_by: order,
                            ..Default::default()
                        };
                        let (expected, expected_more) = sorted_prefix(input, order, limit);
                        for actual in [
                            collect_top_k(input.iter().cloned(), order, limit),
                            apply_search_hints(input.to_vec(), &hints),
                        ] {
                            assert_eq!(
                                actual.results, expected,
                                "input {input:?}, order {order:?}, limit {limit}"
                            );
                            assert_eq!(
                                actual.has_more, expected_more,
                                "input {input:?}, order {order:?}, limit {limit}"
                            );
                        }
                    }
                }
            }
        }
    }

    /// A filter may score NaN: ranking must still be a total order, so sorting cannot panic
    /// and a bounded selection keeps what a full sort would.
    #[test]
    fn ranking_tolerates_nan_scores() {
        let items: Vec<LabelSearchResult> = (0..64)
            .map(|i| LabelSearchResult {
                value: format!("v{:02}", (i * 37) % 64),
                score: match i % 4 {
                    0 => f64::NAN,
                    1 => -f64::NAN,
                    _ => (i % 7) as f64 / 7.0,
                },
                cardinality: i % 5,
            })
            .collect();
        for order in all_orderings() {
            let cmp = compare_search_results(order);
            let (sorted, _) = sorted_prefix(&items, order, items.len());
            assert!(sorted.windows(2).all(|w| cmp(&w[0], &w[1]).is_lt()));
            for limit in [1, 3, 10, 63, 64, 100] {
                let (expected, expected_more) = sorted_prefix(&items, order, limit);
                let actual = collect_top_k(items.iter().cloned(), order, limit);
                assert_eq!(actual.results, expected, "{order:?}, limit {limit}");
                assert_eq!(actual.has_more, expected_more, "{order:?}, limit {limit}");
            }
        }
        let nan = LabelSearchResult {
            value: "x".into(),
            score: f64::NAN,
            cardinality: 1,
        };
        assert_eq!(nan, nan.clone());
    }

    /// Accepts every name but `beta`, scoring each by its length.
    struct RejectBeta;

    impl FuzzyFilter for RejectBeta {
        fn accept(&self, value: &str) -> (bool, f64) {
            (value != "beta", value.len() as f64)
        }
    }

    /// Several values per name, so a lock release can fall between two entries of one name.
    fn build_multi_value_label_postings() -> Postings {
        let mut postings = Postings::default();
        postings.add_posting_for_label_value(1, "alpha", "a1");
        postings.add_posting_for_label_value(2, "alpha", "a1");
        postings.add_posting_for_label_value(3, "alpha", "a2");
        postings.add_posting_for_label_value(4, "beta", "b1");
        postings.add_posting_for_label_value(5, "beta", "b2");
        postings.add_posting_for_label_value(6, "beta", "b3");
        postings.add_posting_for_label_value(7, "gamma", "g1");
        postings.add_posting_for_label_value(8, "gamma", "g1");
        postings.add_posting_for_label_value(9, "service", "api");
        postings
    }

    /// Every name the scan reports, in its order, with `RejectBeta`.
    fn scan_names(
        index: &TimeSeriesIndex,
        descending: bool,
        entries_per_hold: usize,
    ) -> Vec<(String, f64, usize)> {
        let mut names = Vec::new();
        scan_label_names_in_holds(
            index,
            &RejectBeta,
            descending,
            None,
            None,
            entries_per_hold,
            |name, score, cardinality| {
                names.push((name.to_owned(), score, cardinality));
                ControlFlow::Continue(())
            },
        );
        names
    }

    /// The best `limit` names in value order, with `RejectBeta`, and whether there are more.
    fn rank_names(
        index: &TimeSeriesIndex,
        descending: bool,
        limit: usize,
        entries_per_hold: usize,
    ) -> (Vec<String>, bool) {
        let order = if descending {
            SearchResultOrdering::ValueDesc
        } else {
            SearchResultOrdering::ValueAsc
        };
        let result = rank_label_names_in_holds(index, &RejectBeta, order, limit, entries_per_hold);
        let has_more = result.has_more;
        (values(result), has_more)
    }

    #[test]
    fn name_scan_sums_cardinality_and_skips_rejected_names() {
        let index = index_of(build_multi_value_label_postings());
        let ascending = vec![
            ("alpha".to_owned(), 5.0, 3),
            ("gamma".to_owned(), 5.0, 2),
            ("service".to_owned(), 7.0, 1),
        ];
        let mut descending = ascending.clone();
        descending.reverse();

        assert_eq!(scan_names(&index, false, usize::MAX), ascending);
        assert_eq!(scan_names(&index, true, usize::MAX), descending);
    }

    /// `beta` is rejected; `beta2` shares its prefix and sorts between `beta` and `beta=…`.
    fn build_prefix_sharing_label_postings() -> Postings {
        let mut postings = build_multi_value_label_postings();
        postings.add_posting_for_label_value(10, "beta2", "x");
        postings.add_posting_for_label_value(11, "beta2", "y");
        postings
    }

    /// Giving the lock back after every entry, or every few, resumes where the scan stopped:
    /// a name split across holds keeps its summed cardinality, and limits cut at the same name.
    #[test]
    fn name_scan_is_unchanged_by_where_it_gives_the_lock_back() {
        for postings in [
            build_multi_value_label_postings(),
            build_prefix_sharing_label_postings(),
        ] {
            let index = index_of(postings);
            for descending in [false, true] {
                let one_hold = scan_names(&index, descending, usize::MAX);
                for entries_per_hold in [1, 2, 3, 4] {
                    assert_eq!(
                        scan_names(&index, descending, entries_per_hold),
                        one_hold,
                        "descending {descending}, {entries_per_hold} entries per hold"
                    );
                }
                for limit in [1, 2, 3] {
                    let one_hold = rank_names(&index, descending, limit, usize::MAX);
                    for entries_per_hold in [1, 2, 3, 4] {
                        assert_eq!(
                            rank_names(&index, descending, limit, entries_per_hold),
                            one_hold,
                            "descending {descending}, limit {limit}, {entries_per_hold} per hold"
                        );
                    }
                }
            }
        }
    }

    /// Skipping a rejected name must not skip a longer name that starts with it.
    #[test]
    fn name_scans_keep_a_name_that_extends_a_rejected_one() {
        let index = index_of(build_prefix_sharing_label_postings());
        let expected_asc = vec![
            ("alpha".to_owned(), 5.0, 3),
            ("beta2".to_owned(), 5.0, 2),
            ("gamma".to_owned(), 5.0, 2),
            ("service".to_owned(), 7.0, 1),
        ];
        let mut expected_desc = expected_asc.clone();
        expected_desc.reverse();

        for entries_per_hold in [1, 2, usize::MAX] {
            assert_eq!(scan_names(&index, false, entries_per_hold), expected_asc);
            assert_eq!(scan_names(&index, true, entries_per_hold), expected_desc);
        }
    }

    /// Three names pass the filter; a limit of two must say there is more, in both directions,
    /// however the scan's holds fall. The last name was once dropped at the end of the range.
    #[test]
    fn a_name_ranking_cut_at_its_last_name_reports_more() {
        let index = index_of(build_multi_value_label_postings());
        for entries_per_hold in [1, 2, 3, usize::MAX] {
            let (names, has_more) = rank_names(&index, false, 2, entries_per_hold);
            assert_eq!(names, ["alpha", "gamma"], "{entries_per_hold} per hold");
            assert!(has_more, "ascending, {entries_per_hold} per hold");

            let (names, has_more) = rank_names(&index, true, 2, entries_per_hold);
            assert_eq!(names, ["service", "gamma"], "{entries_per_hold} per hold");
            assert!(has_more, "descending, {entries_per_hold} per hold");

            let (_, has_more) = rank_names(&index, false, 3, entries_per_hold);
            assert!(!has_more, "ascending, exactly the limit");
            let (_, has_more) = rank_names(&index, true, 3, entries_per_hold);
            assert!(!has_more, "descending, exactly the limit");
        }
    }

    /// `job2=…` sorts before `job=…` in the index, but `job` before `job2` by name: a value
    /// order must follow the names, however small the limit.
    #[test]
    fn value_ordered_names_follow_the_names_not_the_index_keys() {
        let mut postings = Postings::default();
        postings.add_posting_for_label_value(1, "job", "api");
        postings.add_posting_for_label_value(1, "job2", "worker");
        postings.add_posting_for_label_value(1, "scope", "selected");
        let index = index_of(postings);
        let names = |order_by, limit| {
            let hints = SearchHints {
                limit,
                order_by,
                ..Default::default()
            };
            let result = collect_unscoped_label_names(&index, &hints).unwrap();
            let has_more = result.has_more;
            (values(result), has_more)
        };

        let asc = SearchResultOrdering::ValueAsc;
        let desc = SearchResultOrdering::ValueDesc;
        assert_eq!(
            names(asc, 10),
            (vec!["job".into(), "job2".into(), "scope".into()], false)
        );
        assert_eq!(names(asc, 1), (vec!["job".into()], true));
        assert_eq!(names(asc, 2), (vec!["job".into(), "job2".into()], true));
        assert_eq!(
            names(desc, 10),
            (vec!["scope".into(), "job2".into(), "job".into()], false)
        );
        assert_eq!(names(desc, 1), (vec!["scope".into()], true));
        assert_eq!(names(desc, 2), (vec!["scope".into(), "job2".into()], true));
    }

    /// Rejects names containing `b`; scores by the first byte.
    struct RejectB;

    impl FuzzyFilter for RejectB {
        fn accept(&self, value: &str) -> (bool, f64) {
            (!value.contains('b'), value.as_bytes()[0] as f64 / 256.0)
        }
    }

    /// Name ranking agrees with sorting every accepted name, over name sets dense in names that
    /// extend one another by bytes either side of `=`, for every ordering, limit and hold size.
    #[test]
    fn name_ranking_matches_a_full_sort() {
        // Bytes below `=` ('-', '.', '2') and above it ('a', 'b', 'z').
        let alphabet = ["-", ".", "2", "a", "b", "z"];
        let mut all_names: Vec<String> = Vec::new();
        for a in alphabet {
            all_names.push(a.to_owned());
            for b in alphabet {
                all_names.push(format!("{a}{b}"));
                for c in ["2", "a"] {
                    all_names.push(format!("{a}{b}{c}"));
                }
            }
        }
        // A deterministic pseudo-random choice of name sets.
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for round in 0..60 {
            let mut postings = Postings::default();
            let mut id = 1;
            let mut expected_all: Vec<(String, usize)> = Vec::new();
            for name in &all_names {
                if next() % 3 != 0 {
                    continue;
                }
                let values = 1 + (next() % 3) as usize;
                for v in 0..values {
                    postings.add_posting_for_label_value(id, name, &format!("v{v}"));
                    id += 1;
                }
                expected_all.push((name.clone(), values));
            }
            let index = index_of(postings);
            for filtered in [false, true] {
                let filter: &dyn FuzzyFilter = if filtered {
                    &RejectB
                } else {
                    &NoOpSearchMatcher
                };
                let accepted: Vec<LabelSearchResult> = expected_all
                    .iter()
                    .filter_map(|(name, cardinality)| {
                        let (ok, score) = filter.accept(name);
                        ok.then(|| LabelSearchResult {
                            value: name.clone(),
                            score,
                            cardinality: *cardinality,
                        })
                    })
                    .collect();
                for order in all_orderings() {
                    for limit in [1, 2, 3, 5, 8, 13, 40] {
                        let (expected, expected_more) = sorted_prefix(&accepted, order, limit);
                        for entries_per_hold in [1, 3, usize::MAX] {
                            let actual = rank_label_names_in_holds(
                                &index,
                                filter,
                                order,
                                limit,
                                entries_per_hold,
                            );
                            let context = format!(
                                "round {round}, filtered {filtered}, {order:?}, limit {limit}, {entries_per_hold} per hold"
                            );
                            assert_eq!(actual.results, expected, "{context}");
                            assert_eq!(actual.has_more, expected_more, "{context}");
                        }
                    }
                }
            }
        }
    }

    /// Ranking reads every name, not the first `SEARCH_RESULT_LIMIT_MAX` in byte order.
    #[test]
    fn top_k_name_ranking_considers_every_name() {
        let mut postings = Postings::default();
        for i in 0..1500u64 {
            postings.add_posting_for_label_value(i + 1, &format!("label{i:04}"), "v");
        }
        // The highest-cardinality name sorts last.
        for id in 2000..2010u64 {
            postings.add_posting_for_label_value(id, "zone", &format!("z{id}"));
        }
        let index = index_of(postings);
        let hints = SearchHints {
            limit: 1,
            order_by: SearchResultOrdering::CardinalityDesc,
            ..Default::default()
        };

        let result = collect_unscoped_label_names(&index, &hints).unwrap();

        assert!(result.has_more);
        assert_eq!(result.results.len(), 1);
        assert_eq!(result.results[0].value, "zone");
        assert_eq!(result.results[0].cardinality, 10);
    }

    struct RejectJob;

    impl FuzzyFilter for RejectJob {
        fn accept(&self, value: &str) -> (bool, f64) {
            (value != "job", 1.0)
        }
    }

    /// Skipping a rejected name skips its values and nothing else: `job>x` sorts between
    /// `job=…` and `job?`, and a name may begin with a byte below `!`.
    #[test]
    fn skipping_a_rejected_name_keeps_names_that_sort_just_after_it() {
        let mut postings = Postings::default();
        postings.add_posting_for_label_value(1, " lead", "v");
        postings.add_posting_for_label_value(2, "job", "a");
        postings.add_posting_for_label_value(3, "job", "b");
        postings.add_posting_for_label_value(4, "job>x", "v");
        postings.add_posting_for_label_value(5, "jobs", "v");
        let index = index_of(postings);

        for entries_per_hold in [1, 2, usize::MAX] {
            for descending in [false, true] {
                let mut names = Vec::new();
                scan_label_names_in_holds(
                    &index,
                    &RejectJob,
                    descending,
                    None,
                    None,
                    entries_per_hold,
                    |n, _, _| {
                        names.push(n.to_owned());
                        ControlFlow::Continue(())
                    },
                );
                let mut expected = vec![" lead", "job>x", "jobs"];
                if descending {
                    expected.reverse();
                }
                assert_eq!(names, expected, "{entries_per_hold} per hold");
            }
        }
    }

    #[test]
    fn swapping_two_indexes_changes_both_generations() {
        let a = TimeSeriesIndex::new();
        let b = TimeSeriesIndex::new();
        assert_ne!(a.generation(), b.generation());
        let (before_a, before_b) = (a.generation(), b.generation());

        a.swap(&b);

        assert_ne!(a.generation(), before_a);
        assert_ne!(b.generation(), before_b);
        assert_ne!(a.generation(), b.generation());
    }

    /// A scan that straddles a `SWAPDB` is run again, against what the index then holds.
    #[test]
    fn a_scan_straddling_a_swap_is_run_again() {
        let index = index_of(build_multi_value_label_postings());
        let other = index_of(build_label_values_postings());
        let mut attempts = 0;

        let names = index
            .read_without_swap(|| {
                attempts += 1;
                let mut names = Vec::new();
                scan_label_names_in_holds(&index, &RejectBeta, false, None, None, 1, |n, _, _| {
                    names.push(n.to_owned());
                    ControlFlow::Continue(())
                });
                if attempts == 1 {
                    index.swap(&other);
                }
                Ok(names)
            })
            .unwrap();

        assert_eq!(attempts, 2);
        assert_eq!(names, ["region", "service"]);
    }

    fn all_orderings() -> [SearchResultOrdering; 5] {
        [
            SearchResultOrdering::ValueAsc,
            SearchResultOrdering::ValueDesc,
            SearchResultOrdering::ScoreDesc,
            SearchResultOrdering::CardinalityAsc,
            SearchResultOrdering::CardinalityDesc,
        ]
    }

    /// The value scan's answer does not depend on where it gives the lock back, for every
    /// ordering and limit, with and without a filter, and never strays into another label.
    #[test]
    fn value_scan_is_unchanged_by_where_it_gives_the_lock_back() {
        let mut postings = build_label_values_postings();
        postings.add_posting_for_label_value(5, "region", "ap-south");
        postings.add_posting_for_label_value(6, "region", "us-east");
        postings.add_posting_for_label_value(7, "regions", "zz");
        postings.add_posting_for_label_value(8, "regio", "aa");
        let index = index_of(postings);

        for order_by in all_orderings() {
            for limit in [1, 2, 3, 4, 10] {
                for filtered in [false, true] {
                    let hints = || SearchHints {
                        filter: filtered
                            .then(|| Box::new(StaticScoreFilter) as Box<dyn FuzzyFilter>),
                        limit,
                        order_by,
                        include_meta: true,
                    };
                    let one_hold = collect_unscoped_label_values_in_holds(
                        &index,
                        "region",
                        &hints(),
                        usize::MAX,
                    )
                    .unwrap();
                    assert!(
                        one_hold
                            .results
                            .iter()
                            .all(|r| !r.value.starts_with('z') && r.value != "aa")
                    );
                    for entries_per_hold in [1, 2, 3] {
                        let held = collect_unscoped_label_values_in_holds(
                            &index,
                            "region",
                            &hints(),
                            entries_per_hold,
                        )
                        .unwrap();
                        let context = format!(
                            "{order_by:?}, limit {limit}, filtered {filtered}, {entries_per_hold} per hold"
                        );
                        assert_eq!(held.results, one_hold.results, "{context}");
                        assert_eq!(held.has_more, one_hold.has_more, "{context}");
                    }
                }
            }
        }
    }

    #[test]
    fn label_names_leave_out_what_only_hidden_series_carry() {
        let index = index_with_hidden_series();
        for order_by in all_orderings() {
            let hints = SearchHints {
                order_by,
                ..Default::default()
            };
            let result = collect_unscoped_label_names(&index, &hints).unwrap();
            let names: Vec<(&str, usize)> = result
                .results
                .iter()
                .map(|r| (r.value.as_str(), r.cardinality))
                .sorted()
                .collect();
            assert_eq!(names, [("env", 1), ("job", 1)], "{order_by:?}");
        }
    }

    #[test]
    fn label_values_leave_out_what_only_hidden_series_carry() {
        let index = index_with_hidden_series();
        for order_by in all_orderings() {
            let hints = SearchHints {
                order_by,
                ..Default::default()
            };
            let env = collect_unscoped_label_values(&index, "env", &hints).unwrap();
            assert_eq!(env.results.len(), 1, "{order_by:?}");
            assert_eq!(env.results[0].value, "local");
            let job = collect_unscoped_label_values(&index, "job", &hints).unwrap();
            assert_eq!(job.results[0].cardinality, 1, "{order_by:?}");
            assert!(
                collect_unscoped_label_values(&index, "only_imported", &hints)
                    .unwrap()
                    .results
                    .is_empty()
            );
        }
    }

    #[test]
    fn label_cardinality_of_a_filtered_search_leaves_out_hidden_series() {
        // The filtered path reports this figure, and drops a name when it is zero.
        let index = index_with_hidden_series();
        for entries_per_hold in [1, 2, usize::MAX] {
            for (name, expected) in [("job", 1), ("env", 1), ("only_imported", 0)] {
                assert_eq!(
                    label_cardinality_in_holds(&index, name, entries_per_hold),
                    expected,
                    "{name}, {entries_per_hold} per hold"
                );
            }
        }
    }

    #[test]
    fn value_scan_of_a_missing_label_is_empty() {
        let index = index_of(build_label_values_postings());
        for order_by in all_orderings() {
            let hints = SearchHints {
                order_by,
                ..Default::default()
            };
            let result =
                collect_unscoped_label_values_in_holds(&index, "missing", &hints, 1).unwrap();
            assert!(result.results.is_empty());
            assert!(!result.has_more);
        }
    }

    #[test]
    fn label_cardinality_is_unchanged_by_where_it_gives_the_lock_back() {
        let index = index_of(build_multi_value_label_postings());
        for (name, expected) in [("alpha", 3), ("beta", 3), ("gamma", 2), ("missing", 0)] {
            for entries_per_hold in [1, 2, 3, usize::MAX] {
                assert_eq!(
                    label_cardinality_in_holds(&index, name, entries_per_hold),
                    expected,
                    "{name}, {entries_per_hold} per hold"
                );
            }
        }
    }

    /// A date range without selectors selects every series, through its own plan or one the
    /// caller made; it was once planned as "no selectors", which matches nothing.
    #[test]
    fn date_range_hints_alone_select_every_series() {
        use crate::series::DateRange;
        use crate::series::index::{clear_timeseries_index, get_db_index};
        use crate::series::request_types::MetaDateRangeFilter;

        const DB: i32 = 7311;
        clear_timeseries_index(DB);
        let index = get_db_index(DB);
        for id in 1..=3 {
            let mut series = TimeSeries::new();
            series.id = id;
            series.labels = MetricName::new(&[Label::new("job", "api")]);
            index.index_timeseries(&series, format!("series:{id}").as_bytes());
        }
        let range = Some(MetaDateRangeFilter::Includes(DateRange {
            start: 0,
            end: 100,
        }));
        let hints = || super::SelectHints::new(vec![], range);
        assert!(!hints().is_empty());
        assert!(matches!(hints().selection(), super::SeriesSelection::All));

        let planned = hints().read_planned(DB, |cursor| Ok(cursor.remaining()));
        assert_eq!(planned.unwrap(), 3);

        let plan = super::SeriesCursor::plan(DB, hints().selection(), range).unwrap();
        let reused = hints()
            .with_plan(Some(plan))
            .read_planned(DB, |cursor| Ok(cursor.remaining()));
        assert_eq!(reused.unwrap(), 3);
        clear_timeseries_index(DB);
    }

    /// More series than may be read inline, with ids `1..=count`.
    fn index_with_series(count: u64) -> TimeSeriesIndex {
        let mut postings = Postings::default();
        for id in 1..=count {
            postings.add_posting_for_label_value(id, "service", "api");
            postings.add_posting_for_label_value(id, "instance", &format!("host{id}"));
            postings
                .id_to_key
                .insert(id, format!("series:{id}").into_bytes().into());
        }
        index_of(postings)
    }

    fn plan_of(ids: impl IntoIterator<Item = u64>) -> super::SeriesCursor {
        super::SeriesCursor::from_ids(0, crate::series::index::PostingsBitmap::from_iter(ids))
    }

    /// With a plan, a read is sized by what it matches, not by the database.
    #[test]
    fn a_planned_read_is_sized_by_its_matches() {
        let index = index_with_series(600);
        assert!(super::series_read_exceeds_inline_budget(&index, None));
        assert!(!super::series_read_exceeds_inline_budget(
            &index,
            Some(&plan_of([7]))
        ));
        assert!(!super::series_read_exceeds_inline_budget(
            &index,
            Some(&plan_of(1..=512))
        ));
        assert!(super::series_read_exceeds_inline_budget(
            &index,
            Some(&plan_of(1..=513))
        ));
    }

    /// A planned scoped search stays inline when it matches few series, unless it is a name
    /// search that reports cardinalities: those walk the term dictionary however few match.
    #[test]
    fn a_planned_scoped_search_is_sized_by_its_matches() {
        // 1,101 index entries: over the 1,024-entry budget used below.
        let index = index_with_series(1100);
        let narrow = plan_of([7]);
        let hints = SearchHints::default();
        let budget = 1024;

        // Without a plan: the database decides.
        assert!(label_search_exceeds_inline_budget(
            &index,
            Some("instance"),
            true,
            None,
            &hints,
            budget
        ));
        // A narrow plan: values and plain names stay inline.
        assert!(!label_search_exceeds_inline_budget(
            &index,
            Some("instance"),
            true,
            Some(&narrow),
            &hints,
            budget
        ));
        assert!(!label_search_exceeds_inline_budget(
            &index,
            None,
            true,
            Some(&narrow),
            &hints,
            budget
        ));
        // Names with cardinality read every value of every name.
        for hints in [
            SearchHints {
                include_meta: true,
                ..Default::default()
            },
            SearchHints {
                order_by: SearchResultOrdering::CardinalityDesc,
                ..Default::default()
            },
        ] {
            assert!(label_search_exceeds_inline_budget(
                &index,
                None,
                true,
                Some(&narrow),
                &hints,
                budget
            ));
        }
        // A broad plan still leaves.
        assert!(label_search_exceeds_inline_budget(
            &index,
            Some("instance"),
            true,
            Some(&plan_of(1..=513)),
            &hints,
            budget
        ));
    }

    /// A plan that matched nothing reads nothing, so even a name search reporting
    /// cardinalities, which would otherwise walk the term dictionary, stays inline.
    #[test]
    fn an_empty_planned_scoped_search_stays_inline() {
        let index = index_with_series(1100);
        let empty = plan_of([]);
        for hints in [
            SearchHints::default(),
            SearchHints {
                include_meta: true,
                ..Default::default()
            },
            SearchHints {
                order_by: SearchResultOrdering::CardinalityDesc,
                ..Default::default()
            },
        ] {
            for label_name in [None, Some("instance")] {
                assert!(!label_search_exceeds_inline_budget(
                    &index,
                    label_name,
                    true,
                    Some(&empty),
                    &hints,
                    1024
                ));
            }
        }
    }
}
