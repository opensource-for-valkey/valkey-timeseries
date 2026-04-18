use crate::common::Timestamp;
use crate::fanout::{FanoutCommand, NodeInfo, FanoutCommandResult};
use crate::labels::filters::SeriesSelector;
use crate::promql::engine::config::PROMQL_CONFIG;
use crate::promql::engine::fanout::query_utils::handle_instant_query;
use crate::promql::generated::{
    InstantQuery, InstantQueryResponse, InstantSample, SeriesSelector as ProtoSeriesSelector,
    series_selector::Matchers as ProtoMatchers,
};
use crate::promql::hashers::{FingerprintHashSet, HasFingerprint};
use promql_parser::label::Matchers;
use std::time::Duration;
use valkey_module::{Context, ValkeyResult};

pub struct QueryFanoutCommand {
    matchers: Matchers,
    timestamp: i64,
    lookback_delta: u64,
    results: Vec<InstantSample>,
    timeout: Duration,
    seen: FingerprintHashSet,
}

impl Default for QueryFanoutCommand {
    fn default() -> Self {
        let matchers = Matchers {
            matchers: vec![],
            or_matchers: vec![],
        };
        let lookback_delta = {
            let guard = PROMQL_CONFIG.read().unwrap();
            guard.lookback_delta.as_millis() as u64
        };
        Self {
            matchers,
            timestamp: 0,
            results: vec![],
            timeout: crate::fanout::get_cluster_command_timeout(),
            seen: FingerprintHashSet::default(),
            lookback_delta,
        }
    }
}
impl QueryFanoutCommand {
    pub fn new(
        matchers: Matchers,
        timestamp: Timestamp,
        lookback_delta: u64,
        timeout: Duration,
    ) -> Self {
        Self {
            matchers,
            timestamp,
            lookback_delta,
            results: vec![],
            timeout,
            seen: FingerprintHashSet::default(),
        }
    }
}

impl FanoutCommand for QueryFanoutCommand {
    type Request = InstantQuery;
    type Response = InstantQueryResponse;

    fn name() -> &'static str {
        "query"
    }

    fn get_local_response(ctx: &Context, req: InstantQuery) -> ValkeyResult<InstantQueryResponse> {
        let Some(selector) = req.selector else {
            ctx.log_warning("Received instant query with no selector, returning empty response");
            return Ok(InstantQueryResponse { samples: vec![] });
        };
        let series_selector: SeriesSelector = selector.try_into()?;
        handle_instant_query(ctx, series_selector, req.timestamp, req.lookback_delta)
    }

    fn get_timeout(&self) -> Duration {
        self.timeout
    }

    fn generate_request(&self) -> InstantQuery {
        let matchers: ProtoMatchers =
            ProtoMatchers::try_from(&self.matchers).expect("invalid matchers");
        let selector = ProtoSeriesSelector {
            matchers: Some(matchers),
        };
        InstantQuery {
            selector: Some(selector),
            timestamp: self.timestamp,
            lookback_delta: self.lookback_delta,
        }
    }

    fn on_response(&mut self, mut resp: Self::Response, _target: &NodeInfo) -> FanoutCommandResult {
        for s in resp.samples.iter() {
            let fingerprint = s.labels.fingerprint();
            if !self.seen.insert(fingerprint) {
                // error. we have a duplicate
                // Using prometheus semantics, series should have unique label-value pairs..
                return Err(format!(
                    "TSDB: received duplicate sample with labels {:?} in instant query response",
                    s.labels
                ).into());
            }
        }
        self.results.append(&mut resp.samples);
        Ok(())
    }

    fn get_response(self) -> Self::Response {
        InstantQueryResponse {
            samples: self.results,
        }
    }
}
