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
//! A scoped query (selectors or a date range) reads the matching series through the batched
//! reader ([`for_each_series`]): only the label names or values are copied out under the
//! module lock, a batch at a time, and fuzzy scoring and cardinality lookups run once the
//! lock is released. On a fan-out share the lock is given back between batches; a caller on
//! the main thread already holds it and reads in one batch.

use super::{
    DEFAULT_SERIES_BATCH_SIZE, FuzzyFilter, IndexKey, PostingsBitmap, SeriesCursor,
    SeriesSelection, SimilarityFilter, TimeSeriesIndex, ensure_generation, for_each_planned_series,
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
use itertools::Itertools;
use min_max_heap::MinMaxHeap;
use std::cmp::Ordering;
use std::collections::{Bound, VecDeque};
use std::fmt::Display;
use std::ops::ControlFlow;
use valkey_module::{ValkeyError, ValkeyResult};

const DEFAULT_LIMIT: usize = 100;

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

impl SearchResultOrdering {
    pub fn is_ascending(&self) -> bool {
        matches!(
            self,
            SearchResultOrdering::ValueAsc | SearchResultOrdering::CardinalityAsc
        )
    }

    pub fn is_descending(&self) -> bool {
        matches!(
            self,
            SearchResultOrdering::ValueDesc
                | SearchResultOrdering::ScoreDesc
                | SearchResultOrdering::CardinalityDesc
        )
    }

    pub fn sort_field(&self) -> SortBy {
        match self {
            SearchResultOrdering::ValueAsc | SearchResultOrdering::ValueDesc => SortBy::Value,
            SearchResultOrdering::ScoreDesc => SortBy::Score,
            SearchResultOrdering::CardinalityAsc | SearchResultOrdering::CardinalityDesc => {
                SortBy::Cardinality
            }
        }
    }
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

    /// Read `plan`, made from these hints' selectors and date range, instead of planning them
    /// again.
    pub fn with_plan(mut self, plan: Option<SeriesCursor>) -> Self {
        self.planned = plan;
        self
    }

    pub fn is_empty(&self) -> bool {
        self.selectors.is_empty() && self.date_range.is_none()
    }

    /// Run `read` over the series these hints select in database `db`: the plan given with
    /// [`Self::with_plan`] first, and a new one when there is none or a `SWAPDB` moved the
    /// database under it ([`read_planned`]).
    fn read_planned<T>(
        self,
        db: i32,
        read: impl FnMut(SeriesCursor) -> ValkeyResult<T>,
    ) -> ValkeyResult<T> {
        let replan = || {
            SeriesCursor::plan(
                db,
                SeriesSelection::Matching(&self.selectors),
                self.date_range,
            )
        };
        read_planned(self.planned, replan, read)
    }
}

pub const SEARCH_RESULT_DEFAULT_LIMIT: usize = 100;

/// SearchHints configures search operations with filtering and scoring.
pub struct SearchHints<'a> {
    /// Filter determines which values to include and their relevance scores.
    pub filter: Option<Box<dyn FuzzyFilter + 'a>>,

    /// Maximum number of results to return.
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

    /// Optional cardinality of the label name or label value, if known. This can be used for relevance ranking or filtering.
    pub cardinality: usize,
}

impl LabelSearchResult {
    pub fn new(value: String, score: f64) -> Self {
        Self {
            value,
            score,
            cardinality: 1,
        }
    }
}

impl PartialEq for LabelSearchResult {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
            && self.score == other.score
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
    /// The merged set is then sorted and limited according to `order_by` and `limit`.
    pub fn merge_from(
        &mut self,
        other: LabelQueryResult,
        order_by: SearchResultOrdering,
        limit: usize,
    ) {
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

        let merged_has_more = merged.len() > limit;

        let results = if merged_has_more {
            let comparator = compare_search_results(order_by);
            let mut items: Vec<_> = merged.into_values().collect();
            items.sort_by(comparator);
            items.truncate(limit);
            items
        } else {
            collect_limited_with_heap(merged.into_values(), order_by, limit).results
        };

        self.has_more = self.has_more || other.has_more || merged_has_more;
        self.results = results;
    }
}

impl std::ops::Deref for LabelQueryResult {
    type Target = Vec<LabelSearchResult>;

    fn deref(&self) -> &Self::Target {
        &self.results
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

type ComparisonFn = fn(&LabelSearchResult, &LabelSearchResult) -> Ordering;

/// `compare_search_results` returns the total-order comparison function for the
/// given Ordering. For `OrderByValueAsc` and `OrderByValueDesc` the order is on
/// value alone. For `OrderByScoreDesc` the order is (score desc, value asc),
/// which is a total order and defines the position at which a duplicate value
/// is first emitted by the streaming merge.
fn compare_search_results(order: SearchResultOrdering) -> ComparisonFn {
    fn value_asc(a: &LabelSearchResult, b: &LabelSearchResult) -> Ordering {
        a.value.cmp(&b.value)
    }

    fn value_desc(a: &LabelSearchResult, b: &LabelSearchResult) -> Ordering {
        b.value.cmp(&a.value)
    }

    fn score_desc(a: &LabelSearchResult, b: &LabelSearchResult) -> Ordering {
        match b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal) {
            Ordering::Equal => value_asc(a, b),
            other => other,
        }
    }

    fn cardinality_asc(a: &LabelSearchResult, b: &LabelSearchResult) -> Ordering {
        match a.cardinality.cmp(&b.cardinality) {
            Ordering::Equal => value_asc(a, b),
            other => other,
        }
    }

    fn cardinality_desc(a: &LabelSearchResult, b: &LabelSearchResult) -> Ordering {
        match b.cardinality.cmp(&a.cardinality) {
            Ordering::Equal => value_asc(a, b),
            other => other,
        }
    }

    match order {
        SearchResultOrdering::ValueDesc => value_desc,
        SearchResultOrdering::ScoreDesc => score_desc,
        SearchResultOrdering::CardinalityAsc => cardinality_asc,
        SearchResultOrdering::CardinalityDesc => cardinality_desc,
        SearchResultOrdering::ValueAsc => value_asc,
    }
}

fn normalize_limit(limit: usize) -> usize {
    if limit == 0 {
        SEARCH_RESULT_DEFAULT_LIMIT
    } else {
        limit
    }
    .min(SEARCH_RESULT_LIMIT_MAX)
}

/// `apply_search_hints` sorts and limits a slice of values according to hints,
/// returning a [`LabelQueryResult`] that includes a `has_more` flag indicating
/// whether any entries were truncated at the limit.
pub(crate) fn apply_search_hints(
    mut values: Vec<LabelSearchResult>,
    hints: &SearchHints,
) -> LabelQueryResult {
    let limit = normalize_limit(hints.limit);

    let comparator = compare_search_results(hints.order_by);
    values.sort_by(comparator);

    let has_more = limit > 0 && values.len() > limit;
    if has_more {
        values.truncate(limit);
    }
    LabelQueryResult::new(values, has_more)
}

fn with_search_filter<R>(hints: &SearchHints, f: impl FnOnce(&dyn FuzzyFilter) -> R) -> R {
    if let Some(filter) = &hints.filter {
        return f(filter.as_ref());
    }
    let default_filter: Box<dyn FuzzyFilter> = Box::new(SimilarityFilter::default());
    f(default_filter.as_ref())
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
            if !map.is_empty() {
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
/// and sets `has_more` if the extra item was present. When `limit` is 0, all items are
/// collected and `has_more` is always `false`.
fn collect_limited(
    iter: impl Iterator<Item = LabelSearchResult>,
    limit: usize,
) -> LabelQueryResult {
    let limit = if limit == 0 { DEFAULT_LIMIT } else { limit };

    let mut items: Vec<LabelSearchResult> = iter.take(limit + 1).collect();
    let has_more = items.len() > limit;
    if has_more {
        items.truncate(limit);
    }
    LabelQueryResult::new(items, has_more)
}

/// `collect_limited_with_heap` keeps only the best `limit` results in memory for non-value
/// orderings where we cannot stream directly from the index order.
fn collect_limited_with_heap(
    iter: impl Iterator<Item = LabelSearchResult>,
    order: SearchResultOrdering,
    limit: usize,
) -> LabelQueryResult {
    let cmp = compare_search_results(order);

    // The comparator already includes the requested direction: the best results
    // compare less, even for descending scores or cardinalities.
    let mut items = iter
        .k_smallest_relaxed_by(limit + 1, &cmp)
        .collect::<Vec<_>>();

    items.sort_by(cmp);

    let has_more = items.len() > limit;
    if has_more {
        items.truncate(limit);
    }

    LabelQueryResult::new(items, has_more)
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

fn set_resume_key(slot: &mut Option<Vec<u8>>, key: &[u8]) {
    let buf = slot.get_or_insert_with(Vec::new);
    buf.clear();
    buf.extend_from_slice(key);
}

/// Get label names in ascending order, applying a fuzzy filter if provided.
fn iterate_label_names_asc<F>(
    index: &TimeSeriesIndex,
    filter: &dyn FuzzyFilter,
    limit: Option<usize>,
    f: F,
) -> bool
where
    F: FnMut(&str, f64, usize),
{
    iterate_label_names_asc_in_holds(index, filter, limit, LABEL_SCAN_ENTRIES_PER_HOLD, f)
}

/// [`iterate_label_names_asc`] reading at most `entries_per_hold` entries per lock hold.
///
/// `limit` is the number of accepted names after which the scan stops and reports `has_more`;
/// `None` reads every name, for a caller ranking them.
fn iterate_label_names_asc_in_holds<F>(
    index: &TimeSeriesIndex,
    filter: &dyn FuzzyFilter,
    limit: Option<usize>,
    entries_per_hold: usize,
    mut f: F,
) -> bool
where
    F: FnMut(&str, f64, usize),
{
    let entries_per_hold = entries_per_hold.max(1);
    let limit = limit.unwrap_or(usize::MAX);
    // The scan resumes after this key: each restart, whether to skip a rejected name or to
    // re-take the lock, is a fresh range from here. `None` means from the bottom of the index.
    let mut resume_after: Option<Vec<u8>> = None;
    // The name of the entries being read, and, if it was accepted, its score and the
    // cardinality summed so far. Owned, so a name whose entries straddle a release keeps
    // accumulating after the lock is taken again.
    let mut curr_name = String::new();
    let mut pending: Option<(f64, usize)> = None;
    let mut has_more = false;
    let mut count: usize = 0;

    'hold: loop {
        // Note: releasing the lock costs the scan its point-in-time view: an entry added before
        // the resume point after it was passed is not seen, and a label name whose entries
        // straddle a release is counted from both holds. `f` gets each name as it is
        // completed; the `&str` is valid only for that call.
        let postings = index.get_postings();
        let mut read_this_hold = 0usize;

        'range: loop {
            let start_bound: Bound<&[u8]> = match &resume_after {
                Some(key) => Bound::Excluded(key.as_slice()),
                None => Bound::Unbounded,
            };

            for (key, map) in postings
                .label_index
                .range::<[u8], _>((start_bound, Bound::<&[u8]>::Unbounded))
            {
                read_this_hold += 1;
                let Some((key_name, _)) = key.split() else {
                    // Defensive: a malformed entry is passed over, never re-visited.
                    if read_this_hold >= entries_per_hold {
                        // Malformed entries still count toward the lock budget.
                        set_resume_key(&mut resume_after, key.as_ref());
                        continue 'hold;
                    }
                    continue;
                };

                if curr_name != key_name {
                    if let Some((score, cardinality)) = pending.take() {
                        if count == limit {
                            has_more = true;
                            break 'hold;
                        }
                        f(&curr_name, score, cardinality);
                        count += 1;
                    }
                    curr_name.clear();
                    curr_name.push_str(key_name);

                    let (accepted, score) = filter.accept(key_name);
                    if !accepted {
                        // Jump past every value of this name, and nothing else: see
                        // `past_label_keys`. A bound such as "name?" would also skip a name
                        // like "name>x", which sorts between "name=…" and "name?".
                        resume_after = Some(past_label_keys(key_name));
                        if read_this_hold >= entries_per_hold {
                            continue 'hold;
                        }
                        continue 'range;
                    }
                    pending = Some((score, map.cardinality() as usize));
                } else if let Some((_, cardinality)) = pending.as_mut() {
                    *cardinality += map.cardinality() as usize;
                }

                if read_this_hold >= entries_per_hold {
                    // Give the lock back and resume after this entry.
                    set_resume_key(&mut resume_after, key.as_ref());
                    continue 'hold;
                }
            }
            // The range ran out: the scan is done.
            break 'hold;
        }
    }

    // The last name the scan read is still pending: reported if there is room for it, and
    // otherwise proof that there is more than the limit.
    if let Some((score, cardinality)) = pending {
        if count < limit {
            f(&curr_name, score, cardinality);
        } else {
            has_more = true;
        }
    }

    has_more
}

/// Get label names in ascending order, applying a fuzzy filter if provided. Takes the postings
/// read lock itself and gives it back periodically; see [`iterate_label_names_asc`].
fn get_label_names_asc(
    index: &TimeSeriesIndex,
    filter: &dyn FuzzyFilter,
    limit: usize,
) -> ValkeyResult<LabelQueryResult> {
    index.read_without_swap(|| {
        let mut results: Vec<LabelSearchResult> = Vec::with_capacity(limit.min(DEFAULT_LIMIT));
        let has_more = iterate_label_names_asc(
            index,
            filter,
            Some(limit),
            |key_name, score, cardinality| {
                results.push(LabelSearchResult {
                    value: key_name.to_owned(),
                    score,
                    cardinality,
                });
            },
        );
        Ok(LabelQueryResult::new(results, has_more))
    })
}

/// Get label names in descending order, applying a fuzzy filter if provided. Efficiently skips label names
/// that don't match the filter by using the index ordering to jump past them.
///
/// Like [`iterate_label_names_asc`], it takes the postings read lock itself and gives it back
/// every [`LABEL_SCAN_ENTRIES_PER_HOLD`] entries, so it must not be called with the postings
/// lock held, and it is not a point-in-time view.
fn get_label_names_desc(
    index: &TimeSeriesIndex,
    filter: &dyn FuzzyFilter,
    limit: usize,
) -> ValkeyResult<LabelQueryResult> {
    index.read_without_swap(|| {
        Ok(get_label_names_desc_in_holds(
            index,
            filter,
            limit,
            LABEL_SCAN_ENTRIES_PER_HOLD,
        ))
    })
}

/// [`get_label_names_desc`] reading at most `entries_per_hold` entries per lock hold.
fn get_label_names_desc_in_holds(
    index: &TimeSeriesIndex,
    filter: &dyn FuzzyFilter,
    limit: usize,
    entries_per_hold: usize,
) -> LabelQueryResult {
    let entries_per_hold = entries_per_hold.max(1);
    let mut results: Vec<LabelSearchResult> = Vec::with_capacity(limit.min(DEFAULT_LIMIT));
    let mut current_result: Option<LabelSearchResult> = None;
    let mut has_more = false;
    // The scan resumes below this key: each restart, whether to skip a rejected name or to
    // re-take the lock, is a fresh range under it. `None` means from the top of the index.
    let mut resume_below: Option<Vec<u8>> = None;
    // The name of the entries being read. Owned, so a name whose entries straddle a release
    // keeps accumulating into `current_result` after the lock is taken again.
    let mut curr_name = String::new();

    'hold: loop {
        let postings = index.get_postings();
        let mut read_this_hold = 0usize;

        'range: loop {
            let end_bound: Bound<&[u8]> = match &resume_below {
                Some(buf) => Bound::Excluded(buf.as_slice()),
                None => Bound::Unbounded,
            };

            for (key, map) in postings
                .label_index
                .range::<[u8], _>((Bound::<&[u8]>::Unbounded, end_bound))
                .rev()
            {
                read_this_hold += 1;
                let Some((key_name, _)) = key.split() else {
                    if read_this_hold >= entries_per_hold {
                        // Malformed entries still count toward the lock budget.
                        set_resume_key(&mut resume_below, key.as_ref());
                        continue 'hold;
                    }
                    continue;
                };

                if curr_name != key_name {
                    if let Some(stored) = current_result.take() {
                        if results.len() == limit {
                            has_more = true;
                            break 'hold;
                        }
                        results.push(stored);
                    }
                    curr_name.clear();
                    curr_name.push_str(key_name);

                    let (accepted, score) = filter.accept(key_name);
                    if !accepted {
                        // Jump past every value of this name: each is "name=value", so all of
                        // them are at or above "name=". The bound must be "name=", not "name":
                        // a longer name such as "name2" sorts between the two ("2" < "=") and
                        // would be skipped with it.
                        resume_below = Some(first_label_key(key_name));
                        if read_this_hold >= entries_per_hold {
                            continue 'hold;
                        }
                        continue 'range;
                    }

                    current_result = Some(LabelSearchResult {
                        value: key_name.to_owned(),
                        score,
                        cardinality: map.cardinality() as usize,
                    });
                } else if let Some(active) = current_result.as_mut() {
                    active.cardinality += map.cardinality() as usize;
                }

                if read_this_hold >= entries_per_hold {
                    // Give the lock back and resume below this entry.
                    set_resume_key(&mut resume_below, key.as_ref());
                    continue 'hold;
                }
            }
            // The range ran out: the scan is done.
            break 'hold;
        }
    }

    // As in the ascending scan: the last name read is either reported or proof of more.
    if let Some(stored) = current_result {
        if results.len() < limit {
            results.push(stored);
        } else {
            has_more = true;
        }
    }

    LabelQueryResult::new(results, has_more)
}

/// The values of one label, in index order, read at most `entries_per_hold` entries per hold
/// of the postings read lock.
///
/// Under each hold `check` turns an entry into a result or rejects it, and the results are
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
    F: FnMut(&IndexKey, &PostingsBitmap) -> Option<LabelSearchResult>,
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
        let entries: Box<dyn Iterator<Item = (&IndexKey, &PostingsBitmap)>> = if self.descending {
            Box::new(range.rev())
        } else {
            Box::new(range)
        };

        let mut read = 0usize;
        let mut stopped_at: Option<Vec<u8>> = None;
        for (key, map) in entries {
            read += 1;
            if let Some(result) = (self.check)(key, map) {
                self.buffered.push_back(result);
            }
            if read == self.entries_per_hold || self.buffered.len() >= self.results_per_hold {
                stopped_at = Some(key.as_ref().to_vec());
                break;
            }
        }
        match stopped_at {
            Some(key) => self.resume = Some(key),
            // The range ran out: the scan is done.
            None => self.done = true,
        }
    }
}

impl<F> Iterator for LabelValueScan<'_, F>
where
    F: FnMut(&IndexKey, &PostingsBitmap) -> Option<LabelSearchResult>,
{
    type Item = LabelSearchResult;

    fn next(&mut self) -> Option<LabelSearchResult> {
        while self.buffered.is_empty() && !self.done {
            self.read_hold();
        }
        self.buffered.pop_front()
    }
}

/// The sum of the cardinalities of `name`'s values, read at most `entries_per_hold` entries
/// per hold of the postings read lock: a high-cardinality name has a million entries.
fn label_cardinality_in_holds(
    index: &TimeSeriesIndex,
    name: &str,
    entries_per_hold: usize,
) -> usize {
    let entries_per_hold = entries_per_hold.max(1);
    let first = first_label_key(name);
    let past = past_label_keys(name);
    let mut resume: Option<Vec<u8>> = None;
    let mut total = 0u64;
    loop {
        let postings = index.get_postings();
        let lower: Bound<&[u8]> = match resume.as_deref() {
            Some(after) => Bound::Excluded(after),
            None => Bound::Included(first.as_slice()),
        };
        let mut read = 0usize;
        let mut stopped_at: Option<Vec<u8>> = None;
        for (key, map) in postings
            .label_index
            .range::<[u8], _>((lower, Bound::Excluded(past.as_slice())))
        {
            total += map.cardinality();
            read += 1;
            if read == entries_per_hold {
                stopped_at = Some(key.as_ref().to_vec());
                break;
            }
        }
        match stopped_at {
            Some(key) => resume = Some(key),
            None => return total as usize,
        }
    }
}

/// `HeapResultHolder` is a helper struct for use in the binary heap below.
///
/// It stores the full [`SearchResultOrdering`] (not just [`SortBy`]) because the
/// correct value tie-break direction depends on which drain direction will be used:
///
/// * `drain_asc()` (ascending orderings) yields min → max, so items that should appear
///   first in the output must compare **less** — value-asc tie-break is correct.
/// * `drain_desc()` (descending orderings) yields max → min, so items that should appear
///   first must compare **greater** — value-desc tie-break in `Ord` produces value-asc
///   output after the drain.
///
/// This means the `Ord` implementation can encode the complete desired output order
/// directly, and no post-drain sort is required.
///
/// The value is owned: the name scan feeding the heap gives the postings lock back as it goes,
/// so nothing borrowed from the postings outlives a hold. Only names that enter the heap are
/// copied.
#[derive(Debug, Clone)]
struct HeapResultHolder {
    value: String,
    score: f64,
    cardinality: usize,
    order: SearchResultOrdering,
}

impl HeapResultHolder {
    fn new(value: &str, score: f64, cardinality: usize, order: SearchResultOrdering) -> Self {
        Self {
            value: value.to_owned(),
            score,
            cardinality,
            order,
        }
    }

    fn compare_parts(
        order: SearchResultOrdering,
        a_value: &str,
        a_score: f64,
        a_cardinality: usize,
        b_value: &str,
        b_score: f64,
        b_cardinality: usize,
    ) -> Ordering {
        match order {
            SearchResultOrdering::ValueAsc => a_value.cmp(b_value),
            SearchResultOrdering::ValueDesc => b_value.cmp(a_value),
            SearchResultOrdering::ScoreDesc => {
                match b_score.partial_cmp(&a_score).unwrap_or(Ordering::Equal) {
                    Ordering::Equal => a_value.cmp(b_value),
                    other => other,
                }
            }
            SearchResultOrdering::CardinalityAsc => match a_cardinality.cmp(&b_cardinality) {
                Ordering::Equal => a_value.cmp(b_value),
                other => other,
            },
            SearchResultOrdering::CardinalityDesc => match b_cardinality.cmp(&a_cardinality) {
                Ordering::Equal => a_value.cmp(b_value),
                other => other,
            },
        }
    }

    fn compare_candidate(
        &self,
        candidate_value: &str,
        candidate_score: f64,
        candidate_cardinality: usize,
    ) -> Ordering {
        Self::compare_parts(
            self.order,
            candidate_value,
            candidate_score,
            candidate_cardinality,
            &self.value,
            self.score,
            self.cardinality,
        )
    }
}

impl PartialOrd<Self> for HeapResultHolder {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapResultHolder {
    fn cmp(&self, other: &Self) -> Ordering {
        Self::compare_parts(
            self.order,
            &self.value,
            self.score,
            self.cardinality,
            &other.value,
            other.score,
            other.cardinality,
        )
    }
}

impl PartialEq for HeapResultHolder {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
            && self.score == other.score
            && self.cardinality == other.cardinality
            && self.order == other.order
    }
}

impl Eq for HeapResultHolder {}

fn get_label_names_top_k(
    index: &TimeSeriesIndex,
    filter: &dyn FuzzyFilter,
    order: SearchResultOrdering,
    limit: usize,
) -> ValkeyResult<LabelQueryResult> {
    let limit = if limit == 0 { DEFAULT_LIMIT } else { limit };
    index.read_without_swap(|| Ok(get_label_names_top_k_once(index, filter, order, limit)))
}

/// One attempt of [`get_label_names_top_k`].
fn get_label_names_top_k_once(
    index: &TimeSeriesIndex,
    filter: &dyn FuzzyFilter,
    order: SearchResultOrdering,
    limit: usize,
) -> LabelQueryResult {
    let mut heap: MinMaxHeap<HeapResultHolder> = MinMaxHeap::with_capacity(limit + 2);
    let mut has_more = false;
    // Unbounded: the heap bounds the result, and every name is a candidate for it.
    let _ = iterate_label_names_asc(index, filter, None, |key_name, score, cardinality| {
        if heap.len() == limit {
            let Some(worst) = heap.peek_max() else {
                return;
            };

            has_more = true;

            let candidate_cmp = worst.compare_candidate(key_name, score, cardinality);
            if candidate_cmp != Ordering::Less {
                return;
            }

            heap.pop_max();
        }

        heap.push(HeapResultHolder::new(key_name, score, cardinality, order));
    });

    let results: Vec<LabelSearchResult> = heap
        .drain_asc()
        .map(|h| LabelSearchResult {
            value: h.value,
            score: h.score,
            cardinality: h.cardinality,
        })
        .collect();

    LabelQueryResult::new(results, has_more)
}

/// `collect_unscoped_label_names` attempts to satisfy a label name search by scanning the label index directly
///
/// Takes the index rather than locked postings so that each scan owns its postings read lock
/// and gives it back periodically, so writers are not starved (see
/// [`iterate_label_names_asc`]). No lock is held here: the scans take it themselves, and a read
/// lock taken again behind a waiting writer can deadlock.
fn collect_unscoped_label_names(
    index: &TimeSeriesIndex,
    hints: &SearchHints,
) -> ValkeyResult<LabelQueryResult> {
    let limit = normalize_limit(hints.limit);

    with_search_filter(hints, |filter| match hints.order_by {
        SearchResultOrdering::ValueAsc => get_label_names_asc(index, filter, limit),
        SearchResultOrdering::ValueDesc => get_label_names_desc(index, filter, limit),
        SearchResultOrdering::ScoreDesc
        | SearchResultOrdering::CardinalityAsc
        | SearchResultOrdering::CardinalityDesc => {
            get_label_names_top_k(index, filter, hints.order_by, limit)
        }
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
            let check = |entry: &IndexKey, map: &PostingsBitmap| {
                if map.is_empty() {
                    return None;
                }
                let (_, value) = entry.split()?;
                let (accepted, score) = filter.accept(value);
                let cardinality = map.cardinality() as usize;
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
                | SearchResultOrdering::CardinalityDesc => collect_limited_with_heap(
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
            let mut names: AHashSet<String> = AHashSet::with_capacity(DEFAULT_LIMIT);
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
            let mut counts: AHashMap<String, usize> = AHashMap::with_capacity(DEFAULT_LIMIT);
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
        FuzzyFilter, LabelQueryResult, LabelSearchResult, SearchHints, SearchResultOrdering,
        SimilarityFilter, TimeSeriesIndex, apply_search_hints, collect_limited_with_heap,
        collect_unscoped_label_names, collect_unscoped_label_values,
        collect_unscoped_label_values_in_holds, get_label_names_desc_in_holds,
        iterate_label_names_asc_in_holds, label_cardinality_in_holds,
        label_search_exceeds_inline_budget,
    };
    use crate::series::index::FuzzyAlgorithm;
    use crate::series::index::postings::Postings;
    use itertools::Itertools;

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
    fn similarity_filter_defaults_to_case_sensitive() {
        let filter = SimilarityFilter::new("node", FuzzyAlgorithm::JaroWinkler, 1.0);
        let (accepted, score) = filter.accept("node");
        assert!(accepted);
        assert_eq!(1.0, score);
    }

    #[test]
    fn similarity_filter_case_sensitive_rejects_case_mismatch() {
        let filter = SimilarityFilter::new_with_case_sensitivity(
            "NoDe",
            FuzzyAlgorithm::JaroWinkler,
            1.0,
            true,
        );

        let (accepted, score) = filter.accept("node");
        assert!(!accepted);
        assert!(score < 1.0);
    }

    #[test]
    fn similarity_filter_case_insensitive_accepts_case_mismatch() {
        let filter = SimilarityFilter::new_with_case_sensitivity(
            "NoDe",
            FuzzyAlgorithm::JaroWinkler,
            1.0,
            false,
        );

        let (accepted, score) = filter.accept("node");
        assert!(accepted);
        assert_eq!(1.0, score);
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
    fn jaro_winkler_transposition_matches() {
        // 'metirc' is a transposition of 'metric'
        let filter = SimilarityFilter::new_with_case_sensitivity(
            "metirc",
            FuzzyAlgorithm::JaroWinkler,
            0.5,
            false,
        );
        let (accepted, score) = filter.accept("metric");
        assert!((0.0..=1.0).contains(&score));
        assert!(
            accepted,
            "Expected 'metric' to match 'metirc' with JaroWinkler (score={})",
            score
        );
    }

    #[test]
    fn subsequence_basic_match() {
        let filter = SimilarityFilter::new_with_case_sensitivity(
            "nde",
            FuzzyAlgorithm::Subsequence,
            0.3,
            false,
        );
        let (accepted, score) = filter.accept("node");
        assert!((0.0..=1.0).contains(&score));
        assert!(
            accepted,
            "Expected 'node' to match subsequence 'nde' (score={})",
            score
        );
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
    fn collect_limited_with_heap_returns_sorted_results() {
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
        let result = collect_limited_with_heap(
            items.clone().into_iter(),
            SearchResultOrdering::ValueAsc,
            10,
        );
        assert_eq!(values(result), vec!["a", "b", "c"]);

        // Test ValueDesc
        let result = collect_limited_with_heap(
            items.clone().into_iter(),
            SearchResultOrdering::ValueDesc,
            10,
        );
        assert_eq!(values(result), vec!["c", "b", "a"]);

        // Test ScoreDesc
        let result = collect_limited_with_heap(
            items.clone().into_iter(),
            SearchResultOrdering::ScoreDesc,
            10,
        );
        assert_eq!(values(result), vec!["a", "b", "c"]);

        // Test CardinalityAsc
        let result = collect_limited_with_heap(
            items.clone().into_iter(),
            SearchResultOrdering::CardinalityAsc,
            10,
        );
        assert_eq!(values(result), vec!["a", "b", "c"]);

        // Test CardinalityDesc
        let result = collect_limited_with_heap(
            items.clone().into_iter(),
            SearchResultOrdering::CardinalityDesc,
            10,
        );
        assert_eq!(values(result), vec!["c", "b", "a"]);
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
                        let expected = apply_search_hints(input.to_vec(), &hints);
                        let actual = collect_limited_with_heap(input.iter().cloned(), order, limit);
                        assert_eq!(
                            actual.results, expected.results,
                            "input {input:?}, order {order:?}, limit {limit}"
                        );
                        assert_eq!(
                            actual.has_more, expected.has_more,
                            "input {input:?}, order {order:?}, limit {limit}"
                        );
                    }
                }
            }
        }
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

    fn scan_names(
        index: &TimeSeriesIndex,
        limit: Option<usize>,
        entries_per_hold: usize,
    ) -> (Vec<(String, f64, usize)>, bool) {
        let mut names = Vec::new();
        let has_more = iterate_label_names_asc_in_holds(
            index,
            &RejectBeta,
            limit,
            entries_per_hold,
            |name, score, cardinality| names.push((name.to_owned(), score, cardinality)),
        );
        (names, has_more)
    }

    #[test]
    fn name_scan_sums_cardinality_and_skips_rejected_names() {
        let index = index_of(build_multi_value_label_postings());

        let (names, has_more) = scan_names(&index, None, usize::MAX);

        assert!(!has_more);
        assert_eq!(
            names,
            vec![
                ("alpha".to_owned(), 5.0, 3),
                ("gamma".to_owned(), 5.0, 2),
                ("service".to_owned(), 7.0, 1),
            ]
        );
    }

    /// Giving the lock back after every entry, or every few, resumes where the scan stopped:
    /// a name split across holds keeps its summed cardinality, and limits cut at the same name.
    #[test]
    fn name_scan_is_unchanged_by_where_it_gives_the_lock_back() {
        let index = index_of(build_multi_value_label_postings());

        for limit in [None, Some(1), Some(2), Some(3)] {
            let one_hold = scan_names(&index, limit, usize::MAX);
            for entries_per_hold in [1, 2, 3, 4] {
                assert_eq!(
                    scan_names(&index, limit, entries_per_hold),
                    one_hold,
                    "limit {limit:?}, {entries_per_hold} entries per hold"
                );
            }
        }
    }

    /// `beta` is rejected; `beta2` shares its prefix and sorts between `beta` and `beta=…`.
    fn build_prefix_sharing_label_postings() -> Postings {
        let mut postings = build_multi_value_label_postings();
        postings.add_posting_for_label_value(10, "beta2", "x");
        postings.add_posting_for_label_value(11, "beta2", "y");
        postings
    }

    fn scan_names_desc(
        index: &TimeSeriesIndex,
        limit: Option<usize>,
        entries_per_hold: usize,
    ) -> (Vec<(String, f64, usize)>, bool) {
        let result = get_label_names_desc_in_holds(
            index,
            &RejectBeta,
            limit.unwrap_or(usize::MAX),
            entries_per_hold,
        );
        let has_more = result.has_more;
        let names = result
            .into_iter()
            .map(|r| (r.value, r.score, r.cardinality))
            .collect();
        (names, has_more)
    }

    #[test]
    fn descending_name_scan_sums_cardinality_and_skips_rejected_names() {
        let index = index_of(build_multi_value_label_postings());

        let (names, has_more) = scan_names_desc(&index, None, usize::MAX);

        assert!(!has_more);
        assert_eq!(
            names,
            vec![
                ("service".to_owned(), 7.0, 1),
                ("gamma".to_owned(), 5.0, 2),
                ("alpha".to_owned(), 5.0, 3),
            ]
        );
    }

    #[test]
    fn descending_name_scan_is_unchanged_by_where_it_gives_the_lock_back() {
        for postings in [
            build_multi_value_label_postings(),
            build_prefix_sharing_label_postings(),
        ] {
            let index = index_of(postings);
            for limit in [None, Some(1), Some(2), Some(3)] {
                let one_hold = scan_names_desc(&index, limit, usize::MAX);
                for entries_per_hold in [1, 2, 3, 4] {
                    assert_eq!(
                        scan_names_desc(&index, limit, entries_per_hold),
                        one_hold,
                        "limit {limit:?}, {entries_per_hold} entries per hold"
                    );
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
            assert_eq!(
                scan_names(&index, None, entries_per_hold),
                (expected_asc.clone(), false)
            );
            assert_eq!(
                scan_names_desc(&index, None, entries_per_hold),
                (expected_desc.clone(), false)
            );
        }
    }

    /// Three names pass the filter; a limit of two must say there is more, in both directions,
    /// however the scan's holds fall. The last name was once dropped at the end of the range.
    #[test]
    fn a_name_scan_cut_at_its_last_name_reports_more() {
        let index = index_of(build_multi_value_label_postings());
        for entries_per_hold in [1, 2, 3, usize::MAX] {
            let (names, has_more) = scan_names(&index, Some(2), entries_per_hold);
            let names: Vec<_> = names.into_iter().map(|(name, ..)| name).collect();
            assert_eq!(names, ["alpha", "gamma"], "{entries_per_hold} per hold");
            assert!(has_more, "ascending, {entries_per_hold} per hold");

            let (names, has_more) = scan_names_desc(&index, Some(2), entries_per_hold);
            let names: Vec<_> = names.into_iter().map(|(name, ..)| name).collect();
            assert_eq!(names, ["service", "gamma"], "{entries_per_hold} per hold");
            assert!(has_more, "descending, {entries_per_hold} per hold");

            let (_, has_more) = scan_names(&index, Some(3), entries_per_hold);
            assert!(!has_more, "ascending, exactly the limit");
            let (_, has_more) = scan_names_desc(&index, Some(3), entries_per_hold);
            assert!(!has_more, "descending, exactly the limit");
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
            let mut ascending = Vec::new();
            iterate_label_names_asc_in_holds(
                &index,
                &RejectJob,
                None,
                entries_per_hold,
                |n, _, _| ascending.push(n.to_owned()),
            );
            assert_eq!(
                ascending,
                [" lead", "job>x", "jobs"],
                "{entries_per_hold} per hold"
            );

            let descending: Vec<_> =
                get_label_names_desc_in_holds(&index, &RejectJob, usize::MAX, entries_per_hold)
                    .into_iter()
                    .map(|r| r.value)
                    .collect();
            assert_eq!(
                descending,
                ["jobs", "job>x", " lead"],
                "{entries_per_hold} per hold"
            );
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
                iterate_label_names_asc_in_holds(&index, &RejectBeta, None, 1, |n, _, _| {
                    names.push(n.to_owned())
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
}
