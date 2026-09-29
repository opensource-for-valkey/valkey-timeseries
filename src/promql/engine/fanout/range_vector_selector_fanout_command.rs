use crate::common::Timestamp;
use crate::fanout::{
    FanoutCommand, FanoutCommandResult, FanoutContext, NodeInfo, get_cluster_command_timeout,
};
use crate::labels::filters::SeriesSelector;
use crate::promql::engine::fanout::query_utils::handle_range_query;
use crate::promql::generated::{
    RangeQuery, RangeQueryResponse, SeriesSelector as ProtoSeriesSelector,
};
use promql_parser::label::Matchers;
use std::time::Duration;
use valkey_module::ValkeyResult;

pub struct RangeVectorSelectorFanoutCommand {
    matchers: Matchers,
    start_time: i64,
    end_time: i64,
    max_series: u64,
    max_points_per_series: u64,
    timeout: Duration,
    /// One entry per shard, each with its own symbol table. Label resolution
    /// and the duplicate-series check wait for the requester's pool (see
    /// `check_unique_series`): this callback runs on the main thread.
    responses: Vec<RangeQueryResponse>,
}

impl Default for RangeVectorSelectorFanoutCommand {
    fn default() -> Self {
        let matchers = Matchers::empty();
        Self {
            matchers,
            start_time: 0,
            end_time: 0,
            max_series: 0,
            max_points_per_series: 0,
            timeout: get_cluster_command_timeout(),
            responses: Vec::new(),
        }
    }
}
impl RangeVectorSelectorFanoutCommand {
    pub fn new(
        matchers: Matchers,
        start_time: Timestamp,
        end_time: Timestamp,
        max_series: u64,
        max_points_per_series: u64,
        timeout: Duration,
    ) -> Self {
        Self {
            matchers,
            start_time,
            end_time,
            max_series,
            max_points_per_series,
            timeout,
            responses: Vec::new(),
        }
    }

    /// Consume the accumulated selector results: every shard's response.
    pub fn get_responses(self) -> Vec<RangeQueryResponse> {
        self.responses
    }
}

impl FanoutCommand for RangeVectorSelectorFanoutCommand {
    type Request = RangeQuery;
    type Response = RangeQueryResponse;

    fn name() -> &'static str {
        "query-range"
    }

    fn get_local_response(
        ctx: &FanoutContext,
        req: RangeQuery,
    ) -> ValkeyResult<RangeQueryResponse> {
        let Some(selector) = req.selector else {
            // todo: return error
            ctx.log_warning("Received range query with no selector, returning empty response");
            return Ok(RangeQueryResponse::default());
        };
        let series_selector: SeriesSelector = (&selector).try_into()?;
        let ctx = ctx.lock()?;
        handle_range_query(
            &ctx,
            series_selector,
            req.start_time,
            req.end_time,
            req.max_series,
            req.max_points_per_series,
        )
    }

    fn get_timeout(&self) -> Duration {
        self.timeout
    }

    fn generate_request(&self) -> RangeQuery {
        let selector = ProtoSeriesSelector::from(&self.matchers);
        RangeQuery {
            selector: Some(selector),
            start_time: self.start_time,
            end_time: self.end_time,
            max_series: self.max_series,
            max_points_per_series: self.max_points_per_series,
        }
    }

    fn on_response(&mut self, resp: Self::Response, _target: &NodeInfo) -> FanoutCommandResult {
        self.responses.push(resp);
        Ok(())
    }
}
