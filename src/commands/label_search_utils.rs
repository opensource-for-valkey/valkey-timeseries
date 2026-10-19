use crate::commands::analysis_runner::run_read_on_analysis_lane;
use crate::commands::command_parser::{
    parse_filter_by_range_options, parse_hash_tags, validate_selector_list,
};
use crate::commands::fanout_codec::LabelSearchType;
use crate::commands::ts_label_search_fanout_command::LabelSearchFanoutCommand;
use crate::commands::utils::get_multi_command_targets;
use crate::common::SortDir;
use crate::common::constants::METRIC_NAME_LABEL;
use crate::common::context::{get_current_db, is_blocking_denied};
use crate::common::replies::{
    reply_with_array, reply_with_bool, reply_with_bulk_string, reply_with_integer, reply_with_map,
};
use crate::common::threads::GilSource;
use crate::error_consts;
use crate::fanout::{FanoutClientCommand, FanoutTarget, is_clustered};
use crate::parser::series_selector::parse_series_selector;
use crate::series::index::{
    DefaultLabelQuerier, FuzzyAlgorithm, FuzzyFilter, LabelNameSearchFilter, LabelQuerier,
    LabelQueryResult, SEARCH_RESULT_DEFAULT_LIMIT, SEARCH_RESULT_LIMIT_MAX, SearchHints,
    SearchResultOrdering, SelectHints, SeriesCursor, SeriesSelection, SortBy,
    check_label_query_permissions, get_db_index, label_search_exceeds_inline_budget, plan_if_cheap,
};
use crate::series::request_types::MatchFilterOptions;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

#[derive(Debug, Clone)]
pub(crate) struct LabelNameSearchArgs {
    pub(crate) search_type: LabelSearchType,
    pub(crate) label: Option<String>,
    pub(crate) search_terms: Vec<String>,
    pub(crate) fuzzy_threshold: f64,
    pub(crate) fuzzy_algorithm: FuzzyAlgorithm,
    pub(crate) ignore_case: bool,
    pub(crate) include_metadata: bool,
    pub(crate) sort_order: SearchResultOrdering,
    pub(crate) series_filter: MatchFilterOptions,
    /// Optional hash tags that scope only the cluster fan-out.
    pub(crate) tags: Vec<String>,
}

impl Default for LabelNameSearchArgs {
    fn default() -> Self {
        Self {
            search_type: LabelSearchType::Name,
            label: None,
            search_terms: Vec::new(),
            fuzzy_threshold: 0.0,
            fuzzy_algorithm: FuzzyAlgorithm::JaroWinkler,
            ignore_case: false,
            include_metadata: false,
            sort_order: SearchResultOrdering::ValueAsc,
            series_filter: Default::default(),
            tags: Vec::new(),
        }
    }
}

impl LabelNameSearchArgs {
    pub fn build_search_hints(&self) -> ValkeyResult<SearchHints<'static>> {
        self.validate()?;

        let limit = self.get_limit();

        let filter = if self.search_terms.is_empty() {
            None
        } else {
            Some(Box::new(LabelNameSearchFilter::new(
                self.search_terms.clone(),
                self.fuzzy_algorithm,
                self.fuzzy_threshold,
                !self.ignore_case,
            )) as Box<dyn FuzzyFilter>)
        };

        Ok(SearchHints {
            filter,
            limit,
            order_by: self.sort_order,
            include_meta: self.include_metadata,
        })
    }

    pub fn get_limit(&self) -> usize {
        self.series_filter
            .limit
            .unwrap_or(SEARCH_RESULT_DEFAULT_LIMIT)
            .min(SEARCH_RESULT_LIMIT_MAX)
    }

    pub fn validate(&self) -> ValkeyResult<()> {
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LabelNameSearchToken {
    Search,
    FuzzyThreshold,
    FuzzyAlgorithm,
    IgnoreCase,
    IncludeMetadata,
    SortBy,
    Limit,
    Filter,
    FilterByRange,
    HashTag,
}

fn parse_label_name_search_token(value: &[u8]) -> Option<LabelNameSearchToken> {
    hashify::map_ignore_case!(
        value,
        LabelNameSearchToken,
        "search" => LabelNameSearchToken::Search,
        "fuzzy_threshold" => LabelNameSearchToken::FuzzyThreshold,
        "fuzzy_algo" => LabelNameSearchToken::FuzzyAlgorithm,
        "fuzzy_algorithm" => LabelNameSearchToken::FuzzyAlgorithm,
        "ignore_case" => LabelNameSearchToken::IgnoreCase,
        "include_metadata" => LabelNameSearchToken::IncludeMetadata,
        "sortby" => LabelNameSearchToken::SortBy,
        "limit" => LabelNameSearchToken::Limit,
        "filter" => LabelNameSearchToken::Filter,
        "filter_by_range" => LabelNameSearchToken::FilterByRange,
        "hashtag" => LabelNameSearchToken::HashTag,
    )
    .copied()
}

fn resolve_label_search_ordering(
    sort_by: Option<SortBy>,
    sort_dir: SortDir,
) -> ValkeyResult<SearchResultOrdering> {
    match sort_by {
        None => Ok(match sort_dir {
            SortDir::Asc => SearchResultOrdering::ValueAsc,
            SortDir::Desc => SearchResultOrdering::ValueDesc,
        }),
        Some(SortBy::Value) => Ok(match sort_dir {
            SortDir::Asc => SearchResultOrdering::ValueAsc,
            SortDir::Desc => SearchResultOrdering::ValueDesc,
        }),
        Some(SortBy::Score) => match sort_dir {
            SortDir::Desc => Ok(SearchResultOrdering::ScoreDesc),
            SortDir::Asc => Err(ValkeyError::Str("TSDB: SORTBY score ASC is not supported")),
        },
        Some(SortBy::Cardinality) => Ok(match sort_dir {
            SortDir::Asc => SearchResultOrdering::CardinalityAsc,
            SortDir::Desc => SearchResultOrdering::CardinalityDesc,
        }),
    }
}

fn is_search_token(value: &[u8]) -> bool {
    parse_label_name_search_token(value).is_some()
}

pub(super) fn parse_label_name_search_args(
    args: Vec<ValkeyString>,
    search_type: LabelSearchType,
) -> ValkeyResult<LabelNameSearchArgs> {
    let mut args = args.into_iter().skip(1).peekable();
    let mut parsed = LabelNameSearchArgs::default();
    let mut sorting_specified = false;
    let mut algo_specified = false;

    if search_type == LabelSearchType::Value {
        // For label value search, we require the label name to be specified as the first argument
        if args.peek().is_none() {
            return Err(ValkeyError::Str(
                "TSDB: Label name is required for label value search",
            ));
        }
        parsed.label = Some(args.next_string()?);
    }

    parsed.search_type = search_type;

    while let Some(arg) = args.next() {
        let Some(token) = parse_label_name_search_token(arg.as_slice()) else {
            return Err(ValkeyError::String(format!(
                "TSDB: unrecognized argument '{}'",
                arg.to_string_lossy()
            )));
        };
        match token {
            LabelNameSearchToken::Search => {
                let mut saw_term = false;
                while let Some(next) = args.peek() {
                    if is_search_token(next.as_slice()) {
                        break;
                    }
                    let next_arg = args.next_arg()?;
                    parsed.search_terms.push(next_arg.to_string_lossy());
                    saw_term = true;
                }
                if !saw_term {
                    return Err(ValkeyError::Str("TSDB: missing SEARCH value"));
                }
            }
            LabelNameSearchToken::FuzzyThreshold => {
                let value = args.next_str()?;
                let threshold = value.parse::<f64>().map_err(|_| {
                    ValkeyError::Str("TSDB: FUZZY_THRESHOLD must be a number in [0.0..1.0]")
                })?;
                if !(0.0..=1.0).contains(&threshold) {
                    return Err(ValkeyError::Str(
                        "TSDB: FUZZY_THRESHOLD must be in [0.0..1.0]",
                    ));
                }
                parsed.fuzzy_threshold = threshold;
            }
            LabelNameSearchToken::FuzzyAlgorithm => {
                let value = args.next_str()?;
                parsed.fuzzy_algorithm =
                    FuzzyAlgorithm::try_from(value).map_err(ValkeyError::String)?;
                algo_specified = true;
            }
            LabelNameSearchToken::IgnoreCase => {
                parsed.ignore_case = true;
            }
            LabelNameSearchToken::SortBy => {
                let value = args.next_str()?;
                let sort_by = SortBy::try_from(value).map_err(|_| {
                    ValkeyError::Str("TSDB: SORTBY must be value, score, or cardinality")
                })?;
                if let Some(dir) = args.peek()
                    && let Some(sort_dir) = SortDir::try_from(dir.as_slice()).ok()
                {
                    args.next();
                    parsed.sort_order = resolve_label_search_ordering(Some(sort_by), sort_dir)?;
                } else {
                    let order = if sort_by == SortBy::Score {
                        SearchResultOrdering::ScoreDesc
                    } else {
                        resolve_label_search_ordering(Some(sort_by), SortDir::Asc)?
                    };
                    parsed.sort_order = order;
                }
                sorting_specified = true;
            }
            LabelNameSearchToken::IncludeMetadata => {
                parsed.include_metadata = true;
            }
            LabelNameSearchToken::Limit => {
                let limit = args
                    .next_u64()
                    .map_err(|_| ValkeyError::Str(error_consts::INVALID_LIMIT_VALUE))?
                    as usize;
                if limit == 0 || limit > SEARCH_RESULT_LIMIT_MAX {
                    return Err(ValkeyError::Str(error_consts::INVALID_LIMIT_VALUE));
                }
                parsed.series_filter.limit = Some(limit);
            }
            LabelNameSearchToken::Filter => {
                let filter_count = parsed.series_filter.matchers.len();
                while let Some(next) = args.peek() {
                    if is_search_token(next.as_slice()) {
                        break;
                    }
                    let arg = args.next_str()?;
                    let selector = parse_series_selector(arg)
                        .map_err(|_| ValkeyError::Str(error_consts::MISSING_FILTER))?;
                    parsed.series_filter.matchers.push(selector);
                }
                if filter_count == parsed.series_filter.matchers.len() {
                    return Err(ValkeyError::Str(error_consts::MISSING_FILTER));
                }
                validate_selector_list(&parsed.series_filter.matchers)?;
            }
            LabelNameSearchToken::FilterByRange => {
                let filter = parse_filter_by_range_options(&mut args)?;
                parsed.series_filter.date_range = Some(filter);
            }
            LabelNameSearchToken::HashTag => {
                parsed.tags = parse_hash_tags(&mut args)?;
            }
        }
    }

    if !sorting_specified {
        // if the user specified a search term but didn't specify sorting, we default to sorting by relevance score
        if !parsed.search_terms.is_empty() {
            parsed.sort_order = SearchResultOrdering::ScoreDesc;
        } else {
            // otherwise, we default to sorting by value ascending
            parsed.sort_order = SearchResultOrdering::ValueAsc;
        }
    }

    if !algo_specified {
        if !parsed.search_terms.is_empty() {
            // if the user specified search terms but didn't specify an algorithm, we default to JaroWinkler
            parsed.fuzzy_algorithm = FuzzyAlgorithm::JaroWinkler;
        } else {
            // if no search terms, the algorithm doesn't matter since it won't be used, so we can default to NoOp
            parsed.fuzzy_algorithm = FuzzyAlgorithm::NoOp;
        }
    }

    // if we have a date range, we require a matcher to prevent expensive unbounded queries
    if parsed.series_filter.date_range.is_some() && parsed.series_filter.matchers.is_empty() {
        return Err(ValkeyError::Str(
            "TSDB: at least one FILTER is required when filtering by date range",
        ));
    }

    // when LIMIT is omitted, they should return the full result set. Keep the
    // bounded default only for metric-name discovery.
    if parsed.series_filter.limit.is_none()
        && matches!(parsed.search_type, LabelSearchType::MetricName)
    {
        parsed.series_filter.limit = Some(SEARCH_RESULT_DEFAULT_LIMIT);
    }

    Ok(parsed)
}

pub(crate) fn label_search_targets(context: &Context, tags: &[String]) -> FanoutTarget {
    get_multi_command_targets(context, tags)
}

/// Run a label search against this node's index.
///
/// `src` supplies the module lock, which the search takes as it needs it: the [`Context`]
/// on the main thread, or the request's `FanoutContext` on a fan-out share, which gives the
/// lock back between batches of matched series. Must not be called while holding the lock
/// through a source that releases it.
pub(crate) fn process_label_search_request<S: GilSource + ?Sized>(
    src: &S,
    parsed: &LabelNameSearchArgs,
) -> ValkeyResult<LabelQueryResult> {
    let raw_hints = parsed.build_search_hints()?;
    process_label_search_with_hints(src, parsed, &raw_hints, None)
}

/// `plan`, when given, is `parsed.series_filter` planned already ([`plan_series_filter`]).
fn process_label_search_with_hints<S: GilSource + ?Sized>(
    src: &S,
    parsed: &LabelNameSearchArgs,
    raw_hints: &SearchHints,
    plan: Option<SeriesCursor>,
) -> ValkeyResult<LabelQueryResult> {
    let querier = DefaultLabelQuerier;
    let select_hints =
        if parsed.series_filter.matchers.is_empty() && parsed.series_filter.date_range.is_none() {
            None
        } else {
            Some(
                SelectHints::new(
                    parsed.series_filter.matchers.clone(),
                    parsed.series_filter.date_range,
                )
                .with_plan(plan),
            )
        };
    match parsed.search_type {
        LabelSearchType::Unspecified => Err(ValkeyError::Str(
            "TSDB: LabelSearchType::Unspecified on the wire — protocol error",
        )),
        LabelSearchType::Name => Ok(querier.get_label_names(src, select_hints, Some(raw_hints))?),
        LabelSearchType::Value => {
            let label = parsed.label.as_ref().ok_or({
                ValkeyError::Str("TSDB: label name must be provided for label value search")
            })?;
            Ok(querier.get_label_values(src, label.as_str(), select_hints, Some(raw_hints))?)
        }
        LabelSearchType::MetricName => {
            Ok(querier.get_metric_names(src, select_hints, Some(raw_hints))?)
        }
    }
}

pub(super) fn reply_with_label_search_result(
    ctx: &Context,
    query_result: LabelQueryResult,
    include_meta: bool,
) -> ValkeyResult {
    reply_with_map(ctx, 2);
    reply_with_bulk_string(ctx, "results");
    reply_with_array(ctx, query_result.results.len());
    if include_meta {
        for result in query_result.results.iter() {
            reply_with_array(ctx, 3);
            reply_with_bulk_string(ctx, &result.value);
            reply_with_bulk_string(ctx, &result.score.to_string());
            reply_with_integer(ctx, result.cardinality as i64);
        }
    } else {
        for result in query_result.results.iter() {
            reply_with_bulk_string(ctx, &result.value);
        }
    }
    reply_with_bulk_string(ctx, "has_more");
    reply_with_bool(ctx, query_result.has_more);

    Ok(ValkeyValue::NoReply)
}

pub(super) fn run_label_search(
    ctx: &Context,
    args: Vec<ValkeyString>,
    cmd_type: LabelSearchType,
) -> ValkeyResult {
    let parsed = parse_label_name_search_args(args, cmd_type)?;

    if is_clustered(ctx) {
        let operation = LabelSearchFanoutCommand::new(parsed)?;
        return operation.exec(ctx);
    }

    // Checked before anything is queued: a caller who may not query labels must not take a
    // place on the analysis lane only to be refused there, nor see a full lane's error in
    // place of the permission error. The worker checks again under its own identity.
    check_label_query_permissions(ctx)?;

    let hints = parsed.build_search_hints()?;
    let plan = plan_series_filter(ctx, &parsed)?;
    if !is_blocking_denied(ctx) && should_run_in_background(ctx, &parsed, &hints, plan.as_ref()) {
        let include_metadata = parsed.include_metadata;
        return run_read_on_analysis_lane(
            ctx,
            "label search",
            move |src| process_label_search_with_hints(src, &parsed, &hints, plan),
            move |reply_ctx, result| {
                let _ = reply_with_label_search_result(reply_ctx, result, include_metadata);
            },
        );
    }

    let query_result = process_label_search_with_hints(ctx, &parsed, &hints, plan)?;
    reply_with_label_search_result(ctx, query_result, parsed.include_metadata)
}

/// Starting budgets, not a latency guarantee: strings, bitmaps and selector complexity
/// vary. See `docs/plans/label-search-precheck-benchmark.md` for the measurements.
///
/// A name scan or plain value scan costs about 10–20 ns an entry, so this budget holds the
/// main thread for roughly 75–150 µs at most. Below it, handing the search to the analysis
/// lane (about 20 µs) would cost the caller more than the scan itself.
const INLINE_LABEL_SEARCH_ENTRIES: usize = 8192;
/// The budget for a value scan with `SEARCH`, which scores every entry it reads.
const INLINE_LABEL_SEARCH_WITH_SEARCH_ENTRIES: usize = 256;
/// The least a value scan with `SEARCH` may read inline, however many terms it has: terms
/// are ORed, and each value is scored once against all of them, so cost grows with the
/// term count far less than linearly.
const INLINE_LABEL_SEARCH_WITH_SEARCH_MIN_ENTRIES: usize =
    INLINE_LABEL_SEARCH_WITH_SEARCH_ENTRIES / 4;

/// How many label-index entries a search may read on the main thread.
///
/// Only a value scan pays more per entry for `SEARCH`: it scores every `name=value` entry.
/// A name scan scores each distinct name once and only sums cardinalities per entry, so its
/// per-entry cost does not depend on `SEARCH`.
fn inline_entries_budget(is_name_scan: bool, search_terms: usize) -> usize {
    if is_name_scan || search_terms == 0 {
        return INLINE_LABEL_SEARCH_ENTRIES;
    }
    (INLINE_LABEL_SEARCH_WITH_SEARCH_ENTRIES / search_terms)
        .max(INLINE_LABEL_SEARCH_WITH_SEARCH_MIN_ENTRIES)
}

/// A scoped search's series filter, planned on the main thread when that is cheap
/// ([`plan_if_cheap`]): the plan both sizes the search by its exact match count and is the
/// read's own plan, so it is made once whichever thread reads it. `None` for an unscoped
/// search, which reads the label index rather than series.
fn plan_series_filter(
    ctx: &Context,
    parsed: &LabelNameSearchArgs,
) -> ValkeyResult<Option<SeriesCursor>> {
    let filter = &parsed.series_filter;
    if filter.matchers.is_empty() && filter.date_range.is_none() {
        return Ok(None);
    }
    plan_if_cheap(
        get_current_db(ctx),
        SeriesSelection::Matching(&filter.matchers),
        filter.date_range,
    )
}

fn should_run_in_background(
    ctx: &Context,
    parsed: &LabelNameSearchArgs,
    hints: &SearchHints,
    plan: Option<&SeriesCursor>,
) -> bool {
    let label_name = match parsed.search_type {
        LabelSearchType::Name => None,
        LabelSearchType::Value => parsed.label.as_deref(),
        LabelSearchType::MetricName => Some(METRIC_NAME_LABEL),
        LabelSearchType::Unspecified => return false,
    };
    // `hints.filter` is the one source of "has SEARCH": the probe's stop-after rule reads it
    // too, so the two cannot disagree.
    let search_terms = if hints.filter.is_some() {
        parsed.search_terms.len().max(1)
    } else {
        0
    };
    let entries_budget = inline_entries_budget(label_name.is_none(), search_terms);
    let index = get_db_index(get_current_db(ctx));
    label_search_exceeds_inline_budget(
        &index,
        label_name,
        !parsed.series_filter.matchers.is_empty() || parsed.series_filter.date_range.is_some(),
        plan,
        hints,
        entries_budget,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_scans_keep_the_full_budget_with_search() {
        assert_eq!(inline_entries_budget(true, 0), INLINE_LABEL_SEARCH_ENTRIES);
        assert_eq!(inline_entries_budget(true, 1), INLINE_LABEL_SEARCH_ENTRIES);
        assert_eq!(inline_entries_budget(true, 8), INLINE_LABEL_SEARCH_ENTRIES);
    }

    #[test]
    fn value_scans_with_search_get_a_divided_budget_with_a_floor() {
        assert_eq!(inline_entries_budget(false, 0), INLINE_LABEL_SEARCH_ENTRIES);
        assert_eq!(inline_entries_budget(false, 1), 256);
        assert_eq!(inline_entries_budget(false, 2), 128);
        assert_eq!(inline_entries_budget(false, 4), 64);
        assert_eq!(inline_entries_budget(false, 100), 64);
    }
}
