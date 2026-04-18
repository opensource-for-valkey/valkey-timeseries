use crate::common::Timestamp;
use crate::fanout::{get_cluster_command_timeout, FanoutCommand, FanoutCommandResult, FanoutError, NodeInfo};
use crate::labels::filters::SeriesSelector;
use crate::promql::engine::fanout::query_utils::handle_range_query;
use crate::promql::generated::{
    series_selector::Matchers as ProtoMatchers, RangeQuery, RangeQueryResponse, RangeSample,
    SeriesSelector as ProtoSeriesSelector,
};
use crate::promql::hashers::{FingerprintHashSet, HasFingerprint};
use ahash::HashSetExt;
use promql_parser::label::Matchers;
use std::time::Duration;
use valkey_module::{Context, ValkeyResult};

pub struct QueryRangeFanoutCommand {
    matchers: Matchers,
    start_time: i64,
    end_time: i64,
    timeout: Duration,
    series: Vec<RangeSample>,
    seen: FingerprintHashSet,
}

impl Default for QueryRangeFanoutCommand {
    fn default() -> Self {
        let matchers = Matchers {
            matchers: vec![],
            or_matchers: vec![],
        };
        Self {
            matchers,
            start_time: 0,
            end_time: 0,
            timeout: get_cluster_command_timeout(),
            series: Vec::with_capacity(16),
            seen: FingerprintHashSet::with_capacity(16),
        }
    }
}
impl QueryRangeFanoutCommand {
    pub fn new(
        matchers: Matchers,
        start_time: Timestamp,
        end_time: Timestamp,
        timeout: Duration,
    ) -> Self {
        Self {
            matchers,
            start_time,
            end_time,
            timeout,
            series: Vec::with_capacity(16),
            seen: Default::default(),
        }
    }
}

impl FanoutCommand for QueryRangeFanoutCommand {
    type Request = RangeQuery;
    type Response = RangeQueryResponse;

    fn name() -> &'static str {
        "query-range"
    }

    fn get_local_response(ctx: &Context, req: RangeQuery) -> ValkeyResult<RangeQueryResponse> {
        let Some(selector) = req.selector else {
            // todo: return error
            ctx.log_warning("Received range query with no selector, returning empty response");
            return Ok(RangeQueryResponse { series: vec![] });
        };
        let series_selector: SeriesSelector = selector.try_into()?;
        handle_range_query(ctx, series_selector, req.start_time, req.end_time)
    }

    fn get_timeout(&self) -> Duration {
        self.timeout
    }

    fn generate_request(&self) -> RangeQuery {
        let matchers: ProtoMatchers =
            ProtoMatchers::try_from(&self.matchers).expect("invalid matchers");
        let selector = ProtoSeriesSelector {
            matchers: Some(matchers),
        };
        RangeQuery {
            selector: Some(selector),
            start_time: self.start_time,
            end_time: self.end_time,
        }
    }

    fn on_response(&mut self, resp: Self::Response, _target: &NodeInfo) -> FanoutCommandResult {
        // dedupe samples by labels - if multiple responses contain the same labels, we have an issue
        // Using prometheus semantics, series should have unique label-value pairs..

        for series in resp.series.iter() {
            let fingerprint = series.labels.fingerprint();
            if !self.seen.insert(fingerprint) {
                let msg = format!("Duplicate series found with labels {:?}", series.labels);
                let err = FanoutError::custom(msg);
                return Err(err);
            }
        }
        self.series.extend(resp.series);
        Ok(())
    }

    fn get_response(self) -> Self::Response {
        RangeQueryResponse { series: self.series }
    }
}
