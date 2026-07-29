use crate::commands::command_parser::{parse_filter_by_range_options, validate_selector_list};
use crate::commands::fanout_codec::LabelSearchType;
use crate::commands::ts_label_search_fanout_command::LabelSearchFanoutCommand;
use crate::common::SortDir;
use crate::common::replies::{
    reply_with_array, reply_with_bool, reply_with_bulk_string, reply_with_integer, reply_with_map,
};
use crate::error_consts;
use crate::fanout::{FanoutClientCommand, is_clustered};
use crate::parser::series_selector::parse_series_selector;
use crate::series::index::{
    DefaultLabelQuerier, FuzzyAlgorithm, FuzzyFilter, LabelNameSearchFilter, LabelQuerier,
    LabelQueryResult, SEARCH_RESULT_DEFAULT_LIMIT, SEARCH_RESULT_LIMIT_MAX, SearchHints,
    SearchResultOrdering, SelectHints, SortBy,
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
}

fn parse_label_name_search_token(value: &[u8]) -> Option<LabelNameSearchToken> {
    hashify::tiny_map_ignore_case! {
        value,
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
    }
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

pub(crate) fn process_label_search_request(
    ctx: &Context,
    parsed: &LabelNameSearchArgs,
) -> ValkeyResult<LabelQueryResult> {
    let querier = DefaultLabelQuerier;
    let raw_hints = parsed.build_search_hints()?;
    let select_hints =
        if parsed.series_filter.matchers.is_empty() && parsed.series_filter.date_range.is_none() {
            None
        } else {
            Some(SelectHints::new(
                parsed.series_filter.matchers.clone(),
                parsed.series_filter.date_range,
            ))
        };
    match parsed.search_type {
        LabelSearchType::Unspecified => Err(ValkeyError::Str(
            "TSDB: LabelSearchType::Unspecified on the wire — protocol error",
        )),
        LabelSearchType::Name => {
            Ok(querier.get_label_names(ctx, select_hints, Some(&raw_hints))?)
        }
        LabelSearchType::Value => {
            let label = parsed.label.as_ref().ok_or({
                ValkeyError::Str("TSDB: label name must be provided for label value search")
            })?;
            Ok(querier.get_label_values(ctx, label.as_str(), select_hints, Some(&raw_hints))?)
        }
        LabelSearchType::MetricName => {
            Ok(querier.get_metric_names(ctx, select_hints, Some(&raw_hints))?)
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

    let query_result = process_label_search_request(ctx, &parsed)?;
    reply_with_label_search_result(ctx, query_result, parsed.include_metadata)
}
