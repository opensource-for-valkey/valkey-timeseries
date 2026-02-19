use crate::common::Timestamp;
use crate::fanout::{FanoutCommand, NodeInfo};
use crate::labels::filters::SeriesSelector;
use crate::promql::engine::fanout::query_utils::handle_instant_query;
use crate::promql::generated::{
    InstantQuery, InstantQueryResponse, InstantSample, SeriesSelector as ProtoSeriesSelector,
    series_selector::Matchers as ProtoMatchers,
};
use promql_parser::label::Matchers;
use valkey_module::{Context, ValkeyResult};

pub struct QueryFanoutCommand {
    matchers: Matchers,
    timestamp: i64,
    results: Vec<InstantSample>,
}

impl Default for QueryFanoutCommand {
    fn default() -> Self {
        let matchers = Matchers {
            matchers: vec![],
            or_matchers: vec![],
        };
        Self {
            matchers,
            timestamp: 0,
            results: vec![],
        }
    }
}
impl QueryFanoutCommand {
    pub fn new(matchers: Matchers, timestamp: Timestamp) -> Self {
        Self {
            matchers,
            timestamp,
            results: vec![],
        }
    }
}

impl FanoutCommand for QueryFanoutCommand {
    type Request = InstantQuery;
    type Response = InstantQueryResponse;

    fn name() -> &'static str {
        "instant-query"
    }

    fn get_local_response(ctx: &Context, req: InstantQuery) -> ValkeyResult<InstantQueryResponse> {
        let Some(selector) = req.selector else {
            ctx.log_warning("Received instant query with no selector, returning empty response");
            return Ok(InstantQueryResponse { samples: vec![] });
        };
        let series_selector: SeriesSelector = selector.try_into()?;
        let timestamp = req.start_time;
        handle_instant_query(ctx, series_selector, timestamp)
    }

    fn generate_request(&self) -> InstantQuery {
        let matchers: ProtoMatchers =
            ProtoMatchers::try_from(&self.matchers).expect("invalid matchers");
        let selector = ProtoSeriesSelector {
            matchers: Some(matchers),
        };
        InstantQuery {
            selector: Some(selector),
            start_time: self.timestamp,
        }
    }

    fn on_response(&mut self, resp: Self::Response, _target: &NodeInfo) {
        let mut resp = resp;
        // todo: dedupe samples by labels - if multiple responses contain the same labels, we have an issue
        // Using prometheus semantics, series should have unique label-value pairs..
        self.results.append(&mut resp.samples);
    }

    fn get_response(self) -> Self::Response {
        InstantQueryResponse {
            samples: self.results,
        }
    }
}
