use crate::common::Timestamp;
use crate::fanout::{FanoutCommand, NodeInfo};
use crate::labels::filters::SeriesSelector;
use crate::promql::engine::fanout::query_utils::handle_range_query;
use crate::promql::generated::{
    RangeQuery, RangeQueryResponse, RangeSample, SeriesSelector as ProtoSeriesSelector,
};
use promql_parser::parser::VectorSelector;
use valkey_module::{Context, ValkeyResult};

#[derive(Default, Clone)]
pub struct VectorSelectorFanoutCommand {
    selector: VectorSelector,
    start_time: i64,
    end_time: i64,
    results: Vec<RangeSample>,
}

impl VectorSelectorFanoutCommand {
    pub fn new(selector: VectorSelector, start_time: Timestamp, end_time: Timestamp) -> Self {
        Self {
            selector,
            start_time,
            end_time,
            results: vec![],
        }
    }
}

impl FanoutCommand for VectorSelectorFanoutCommand {
    type Request = RangeQuery;
    type Response = RangeQueryResponse;

    fn name() -> &'static str {
        "range-query"
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

    fn generate_request(&self) -> RangeQuery {
        let selector: ProtoSeriesSelector = (&self.selector).into();
        RangeQuery {
            selector: Some(selector),
            start_time: self.start_time,
            end_time: self.end_time,
        }
    }

    fn on_response(&mut self, resp: Self::Response, _target: &NodeInfo) {
        let mut resp = resp;
        // dedupe samples by labels - if multiple responses contain the same labels, we have an issue
        // Using prometheus semantics, series should have unique label-value pairs..
        self.results.append(&mut resp.series);
    }

    fn get_response(self) -> Self::Response {
        RangeQueryResponse {
            series: self.results,
        }
    }
}
