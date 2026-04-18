use crate::common::Timestamp;
use crate::fanout::{FanoutCommand, NodeInfo, get_cluster_command_timeout};
use crate::labels::filters::SeriesSelector;
use crate::promql::engine::fanout::query_utils::handle_range_query;
use crate::promql::generated::{
    RangeQuery, RangeQueryResponse, RangeSample, SeriesSelector as ProtoSeriesSelector,
    series_selector::Matchers as ProtoMatchers,
};
use crate::promql::hashers::{FingerprintHashMap, HasFingerprint};
use orx_parallel::{IterIntoParIter, ParIter};
use promql_parser::label::Matchers;
use std::time::Duration;
use valkey_module::{Context, ValkeyResult};

pub struct QueryRangeFanoutCommand {
    matchers: Matchers,
    start_time: i64,
    end_time: i64,
    timeout: Duration,
    merged: FingerprintHashMap<(RangeSample, usize)>,
    need_sort_dedup: bool,
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
            merged: Default::default(),
            need_sort_dedup: false,
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
            merged: Default::default(),
            need_sort_dedup: false,
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

    fn on_response(&mut self, resp: Self::Response, _target: &NodeInfo) {
        // dedupe samples by labels - if multiple responses contain the same labels, we have an issue
        // Using prometheus semantics, series should have unique label-value pairs..
        for series in resp.series {
            let fingerprint = series.labels.fingerprint();
            let entry = self.merged.entry(fingerprint).or_insert_with(|| {
                let len = series.samples.len();
                let rs = RangeSample {
                    labels: series.labels,
                    samples: Vec::with_capacity(len),
                };
                (rs, 1)
            });
            entry.0.samples.extend(series.samples);
            entry.1 += 1;
            if entry.1 > 1 {
                self.need_sort_dedup = true;
            }
        }
    }

    fn get_response(self) -> Self::Response {
        let series = if self.need_sort_dedup {
            self.merged
                .into_iter()
                .iter_into_par()
                .map(|(_, (mut r, cnt))| {
                    if cnt > 1 {
                        r.samples.sort_unstable_by_key(|s| s.timestamp);
                        r.samples.dedup_by_key(|s| s.timestamp);
                    }
                    r
                })
                .collect()
        } else {
            self.merged.into_iter().map(|(_, (r, _))| r).collect()
        };
        RangeQueryResponse { series }
    }
}
