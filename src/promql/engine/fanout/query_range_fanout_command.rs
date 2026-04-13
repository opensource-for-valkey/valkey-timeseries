use crate::common::{Sample, Timestamp};
use crate::fanout::{FanoutCommand, NodeInfo, get_cluster_command_timeout};
use crate::labels::filters::SeriesSelector;
use crate::promql::generated::{
    Label as ProtoLabel, RangeQuery, RangeQueryResponse, RangeSample, Sample as ProtoSample,
    SeriesSelector as ProtoSeriesSelector, series_selector::Matchers as ProtoMatchers,
};
use crate::series::index::series_by_selectors;
use orx_parallel::{IterIntoParIter, ParIter};
use promql_parser::label::Matchers;
use std::ops::Deref;
use std::time::Duration;
use valkey_module::{Context, ValkeyResult};

pub struct QueryRangeFanoutCommand {
    matchers: Matchers,
    start_time: i64,
    end_time: i64,
    results: Vec<RangeSample>,
    timeout: Duration,
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
            results: vec![],
            timeout: get_cluster_command_timeout(),
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
            results: vec![],
            timeout,
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
        let mut resp = resp;
        // todo: dedupe samples by labels - if multiple responses contain the same labels, we have an issue
        // Using prometheus semantics, series should have unique label-value pairs..
        self.results.append(&mut resp.series);
    }

    fn get_response(self) -> Self::Response {
        RangeQueryResponse {
            series: self.results,
        }
    }
}

fn handle_range_query(
    ctx: &Context,
    selector: SeriesSelector,
    start_time: i64,
    end_time: i64,
) -> ValkeyResult<RangeQueryResponse> {
    let series = series_by_selectors(ctx, &[selector], None)?;
    let ranges = series
        .iter()
        .map(|(s, _)| s.deref())
        .iter_into_par()
        .filter_map(|s| {
            let samples = s.get_range(start_time, end_time);
            if samples.is_empty() {
                return None;
            }
            let res_samples: Vec<ProtoSample> = samples.into_iter().map(Sample::into).collect();
            let mut labels: Vec<ProtoLabel> = Vec::with_capacity(s.labels.len());
            for label in s.labels.iter() {
                labels.push(ProtoLabel {
                    name: label.name.to_string(),
                    value: label.value.to_string(),
                });
            }
            let range = RangeSample {
                labels,
                samples: res_samples,
            };
            Some(range)
        })
        .collect::<Vec<_>>();

    Ok(RangeQueryResponse { series: ranges })
}
