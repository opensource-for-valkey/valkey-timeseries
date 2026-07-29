use super::fanout_codec::generated::{
    LabelResultsSortOrder, LabelSearchRequest, LabelSearchResponse,
};
use crate::commands::fanout_codec::filters::{deserialize_matchers_list, serialize_matchers_list};
use crate::commands::fanout_codec::{
    FuzzySearchAlgorithm, LabelSearchResult as FanoutLabelSearchResult, LabelSearchType,
};
use crate::commands::label_search_utils::{
    LabelNameSearchArgs, process_label_search_request, reply_with_label_search_result,
};
use crate::fanout::{FanoutClientCommand, FanoutCommandResult, FanoutContext, NodeInfo};
use crate::series::index::{
    FuzzyAlgorithm, LabelSearchResult as IndexLabelSearchResult, SEARCH_RESULT_LIMIT_MAX,
    SearchHints, SearchResultOrdering, apply_search_hints,
};
use crate::series::request_types::MatchFilterOptions;
use ahash::AHashMap;
use valkey_module::{Context, Status, ValkeyError, ValkeyResult};

#[derive(Default)]
pub struct LabelSearchFanoutCommand {
    args: LabelNameSearchArgs,
    limit: usize,
    result_map: AHashMap<String, IndexLabelSearchResult>,
    has_more: bool,
}

impl LabelSearchFanoutCommand {
    pub fn new(args: LabelNameSearchArgs) -> ValkeyResult<Self> {
        let limit = args.get_limit();
        Ok(Self {
            args,
            limit,
            has_more: false,
            result_map: AHashMap::with_capacity(limit),
        })
    }
}

impl FanoutClientCommand for LabelSearchFanoutCommand {
    type Request = LabelSearchRequest;
    type Response = LabelSearchResponse;

    fn name() -> &'static str {
        "cmd:label-search"
    }

    fn get_local_response(
        ctx: &Context,
        req: LabelSearchRequest,
    ) -> ValkeyResult<LabelSearchResponse> {
        if req.fuzz_threshold > 1.0 {
            return Err(ValkeyError::Str(
                "TSDB: FUZZY_THRESHOLD must be in [0.0..1.0]",
            ));
        }

        let search_type = LabelSearchType::try_from(req.request_type)
            .map_err(|_| ValkeyError::Str("TSDB: invalid label search request type"))?;

        let matchers = deserialize_matchers_list(Some(req.filters))?;

        let algorithm: FuzzySearchAlgorithm = req.fuzz_algorithm.try_into().map_err(|_| {
            ValkeyError::Str(
                "TSDB: invalid FUZZY_ALGORITHM value; expected jarowinkler, subsequence, or noop",
            )
        })?;

        let fuzz_algorithm = match algorithm {
            FuzzySearchAlgorithm::JaroWinkler => FuzzyAlgorithm::JaroWinkler,
            FuzzySearchAlgorithm::Subsequence => FuzzyAlgorithm::Subsequence,
            FuzzySearchAlgorithm::Noop => FuzzyAlgorithm::NoOp,
            // `try_from` above rejects only *unknown* discriminants; zero is a
            // defined variant, and proto3 omits a zero-valued field, so an
            // unset `fuzz_algorithm` lands here.
            FuzzySearchAlgorithm::Unspecified => {
                return Err(ValkeyError::Str(
                    "TSDB: invalid FUZZY_ALGORITHM value; expected jarowinkler, subsequence, or noop",
                ));
            }
        };

        let label = if req.label.is_empty() {
            None
        } else {
            Some(req.label)
        };

        let order: LabelResultsSortOrder = req
            .sort_order
            .try_into()
            .map_err(|_| ValkeyError::Str("TSDB: invalid sort order for label search results"))?;

        let sort_order = match order {
            LabelResultsSortOrder::ValueAsc => SearchResultOrdering::ValueAsc,
            LabelResultsSortOrder::ValueDesc => SearchResultOrdering::ValueDesc,
            LabelResultsSortOrder::ScoreDesc => SearchResultOrdering::ScoreDesc,
            LabelResultsSortOrder::CardinalityAsc => SearchResultOrdering::CardinalityAsc,
            LabelResultsSortOrder::CardinalityDesc => SearchResultOrdering::CardinalityDesc,
            LabelResultsSortOrder::Unspecified => {
                return Err(ValkeyError::Str(
                    "TSDB: invalid sort order for label search results",
                ));
            }
        };

        let parsed = LabelNameSearchArgs {
            search_type,
            label,
            search_terms: req.search_terms,
            fuzzy_threshold: req.fuzz_threshold,
            fuzzy_algorithm: fuzz_algorithm,
            ignore_case: req.ignore_case,
            // include_score was removed as a user option; shards always return plain values.
            include_metadata: req.include_metadata,
            sort_order,
            series_filter: MatchFilterOptions {
                matchers,
                date_range: req.range.map(Into::into),
                limit: Some(req.limit as usize),
            },
        };

        process_label_search_request(ctx, &parsed).map(|results| LabelSearchResponse {
            has_more: results.has_more,
            results: results
                .into_iter()
                .map(|r| FanoutLabelSearchResult {
                    value: r.value,
                    score: r.score,
                    cardinality: r.cardinality as u64,
                })
                .collect(),
        })
    }

    fn generate_request(&self) -> LabelSearchRequest {
        let filters = serialize_matchers_list(self.args.series_filter.matchers.as_ref())
            .expect("serialize matchers list");

        let label = self
            .args
            .label
            .as_ref()
            .map_or(String::new(), |x| x.clone());

        let sort_order = match self.args.sort_order {
            SearchResultOrdering::ValueAsc => LabelResultsSortOrder::ValueAsc,
            SearchResultOrdering::ValueDesc => LabelResultsSortOrder::ValueDesc,
            SearchResultOrdering::ScoreDesc => LabelResultsSortOrder::ScoreDesc,
            SearchResultOrdering::CardinalityAsc => LabelResultsSortOrder::CardinalityAsc,
            SearchResultOrdering::CardinalityDesc => LabelResultsSortOrder::CardinalityDesc,
        };

        let fuzz_algorithm = match self.args.fuzzy_algorithm {
            FuzzyAlgorithm::JaroWinkler => FuzzySearchAlgorithm::JaroWinkler,
            FuzzyAlgorithm::Subsequence => FuzzySearchAlgorithm::Subsequence,
            FuzzyAlgorithm::NoOp => FuzzySearchAlgorithm::Noop,
        }
        .into();

        LabelSearchRequest {
            request_type: self.args.search_type.into(),
            label,
            search_terms: self.args.search_terms.clone(),
            fuzz_threshold: self.args.fuzzy_threshold,
            fuzz_algorithm,
            ignore_case: self.args.ignore_case,
            range: self.args.series_filter.date_range.map(Into::into),
            filters,
            include_metadata: self.args.include_metadata,
            sort_order: sort_order.into(),
            // Send the maximum shard limit so each shard returns all local matches.
            // The coordinator applies the user's actual limit after merging shard results,
            // ensuring that values which appear across multiple shards are merged and
            // cardinality is summed correctly before truncation.
            limit: SEARCH_RESULT_LIMIT_MAX as u32,
        }
    }

    fn on_response(&mut self, resp: Self::Response, _target: &NodeInfo) -> FanoutCommandResult {
        for r in resp.results.iter() {
            if let Some(current) = self.result_map.get_mut(&r.value) {
                current.cardinality += r.cardinality as usize;
                continue;
            }
            self.result_map.insert(
                r.value.clone(),
                IndexLabelSearchResult {
                    value: r.value.clone(),
                    score: r.score,
                    cardinality: r.cardinality as usize,
                },
            );
        }

        self.has_more |= resp.has_more;

        Ok(())
    }

    fn reply(&mut self, ctx: &FanoutContext) -> Status {
        let map = std::mem::take(&mut self.result_map);
        let values = map.into_values().collect::<Vec<_>>();

        let hints = SearchHints {
            filter: None,
            limit: self.limit,
            order_by: self.args.sort_order,
            include_meta: self.args.include_metadata,
        };

        let query_result = apply_search_hints(values, &hints);
        // A shard may have reported has_more, or the coordinator merge itself may have
        // truncated additional entries — either condition means results were cut off.
        self.has_more |= query_result.has_more;

        match reply_with_label_search_result(ctx, query_result, hints.include_meta) {
            Ok(_) => Status::Ok,
            Err(_e) => Status::Err,
        }
    }
}
