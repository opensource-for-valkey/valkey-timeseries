//! Shard-side PromQL rollup push-down.
//!
//! Without this, evaluating `sum_over_time(http_requests_total[5m])` in a
//! cluster ships every sample in the window to the coordinator and reduces
//! there. This command pushes the window *and* the reduction to each shard, so
//! what crosses the wire is one float per series per step rather than the raw
//! window — which, whenever the range exceeds the step, the neighbouring steps
//! would each have shipped again.
//!
//! It follows the same three rules as [`super::AggregationFanoutCommand`]:
//!
//! * The coordinator decides whether to push down; shards obey the request.
//! * Responses are self-describing: a shard that cannot apply the rollup says so
//!   (`applied == false`) and returns its raw windows, which the coordinator
//!   reduces itself. Mixed-version clusters degrade instead of returning wrong
//!   answers.
//! * The coordinator's combination of the shards' output equals the single-node
//!   result over the concatenated input.
//!
//! The third rule is cheaper here than for aggregation. A PromQL series lives
//! entirely on one shard, so no two shards ever contribute to the same output
//! series and there is no merge algebra at all: the coordinator concatenates.
//! What it *must* preserve is the sparse shape — a `(series, step)` pair whose
//! window held no samples is absent from the result, and absence is not the same
//! as a NaN value.

use crate::common::Timestamp;
use crate::fanout::{
    ErrorKind, FanoutCommand, FanoutCommandResult, FanoutError, NodeInfo,
    get_cluster_command_timeout,
};
use crate::labels::filters::SeriesSelector;
use crate::promql::engine::fanout::query_utils::local_rollup_windows;
use crate::promql::engine::proto_labels_to_labels;
use crate::promql::engine::query_reader::RollupRequest;
use crate::promql::functions::RollupKind;
use crate::promql::generated::{
    RangeSample as ProtoRangeSample, RollupKind as ProtoRollupKind, RollupQuery,
    RollupQueryResponse, RollupSeries, SeriesSelector as ProtoSeriesSelector,
    series_selector::Matchers as ProtoMatchers,
};
use crate::promql::model::RangeSample;
use promql_parser::label::Matchers;
use std::time::Duration;
use valkey_module::{Context, ValkeyResult};

impl From<RollupKind> for ProtoRollupKind {
    fn from(kind: RollupKind) -> Self {
        match kind {
            RollupKind::SumOverTime => ProtoRollupKind::RollupSumOverTime,
            RollupKind::CountOverTime => ProtoRollupKind::RollupCountOverTime,
            RollupKind::LastOverTime => ProtoRollupKind::RollupLastOverTime,
            RollupKind::AvgOverTime => ProtoRollupKind::RollupAvgOverTime,
            RollupKind::MinOverTime => ProtoRollupKind::RollupMinOverTime,
            RollupKind::MaxOverTime => ProtoRollupKind::RollupMaxOverTime,
            RollupKind::StddevOverTime => ProtoRollupKind::RollupStddevOverTime,
            RollupKind::StdvarOverTime => ProtoRollupKind::RollupStdvarOverTime,
            RollupKind::MadOverTime => ProtoRollupKind::RollupMadOverTime,
            RollupKind::PresentOverTime => ProtoRollupKind::RollupPresentOverTime,
            RollupKind::FirstOverTime => ProtoRollupKind::RollupFirstOverTime,
            RollupKind::QuantileOverTime => ProtoRollupKind::RollupQuantileOverTime,
            RollupKind::TsOfFirstOverTime => ProtoRollupKind::RollupTsOfFirstOverTime,
            RollupKind::TsOfLastOverTime => ProtoRollupKind::RollupTsOfLastOverTime,
            RollupKind::TsOfMinOverTime => ProtoRollupKind::RollupTsOfMinOverTime,
            RollupKind::TsOfMaxOverTime => ProtoRollupKind::RollupTsOfMaxOverTime,
            RollupKind::Rate => ProtoRollupKind::RollupRate,
            RollupKind::Increase => ProtoRollupKind::RollupIncrease,
            RollupKind::Delta => ProtoRollupKind::RollupDelta,
            RollupKind::IRate => ProtoRollupKind::RollupIrate,
            RollupKind::IDelta => ProtoRollupKind::RollupIdelta,
            RollupKind::Deriv => ProtoRollupKind::RollupDeriv,
            RollupKind::Resets => ProtoRollupKind::RollupResets,
            RollupKind::Changes => ProtoRollupKind::RollupChanges,
        }
    }
}

impl From<ProtoRollupKind> for RollupKind {
    fn from(kind: ProtoRollupKind) -> Self {
        match kind {
            ProtoRollupKind::RollupSumOverTime => RollupKind::SumOverTime,
            ProtoRollupKind::RollupCountOverTime => RollupKind::CountOverTime,
            ProtoRollupKind::RollupLastOverTime => RollupKind::LastOverTime,
            ProtoRollupKind::RollupAvgOverTime => RollupKind::AvgOverTime,
            ProtoRollupKind::RollupMinOverTime => RollupKind::MinOverTime,
            ProtoRollupKind::RollupMaxOverTime => RollupKind::MaxOverTime,
            ProtoRollupKind::RollupStddevOverTime => RollupKind::StddevOverTime,
            ProtoRollupKind::RollupStdvarOverTime => RollupKind::StdvarOverTime,
            ProtoRollupKind::RollupMadOverTime => RollupKind::MadOverTime,
            ProtoRollupKind::RollupPresentOverTime => RollupKind::PresentOverTime,
            ProtoRollupKind::RollupFirstOverTime => RollupKind::FirstOverTime,
            ProtoRollupKind::RollupQuantileOverTime => RollupKind::QuantileOverTime,
            ProtoRollupKind::RollupTsOfFirstOverTime => RollupKind::TsOfFirstOverTime,
            ProtoRollupKind::RollupTsOfLastOverTime => RollupKind::TsOfLastOverTime,
            ProtoRollupKind::RollupTsOfMinOverTime => RollupKind::TsOfMinOverTime,
            ProtoRollupKind::RollupTsOfMaxOverTime => RollupKind::TsOfMaxOverTime,
            ProtoRollupKind::RollupRate => RollupKind::Rate,
            ProtoRollupKind::RollupIncrease => RollupKind::Increase,
            ProtoRollupKind::RollupDelta => RollupKind::Delta,
            ProtoRollupKind::RollupIrate => RollupKind::IRate,
            ProtoRollupKind::RollupIdelta => RollupKind::IDelta,
            ProtoRollupKind::RollupDeriv => RollupKind::Deriv,
            ProtoRollupKind::RollupResets => RollupKind::Resets,
            ProtoRollupKind::RollupChanges => RollupKind::Changes,
        }
    }
}

pub(in crate::promql) struct RollupFanoutCommand {
    matchers: Matchers,
    rollup: RollupRequest,
    max_series: u64,
    max_points_per_series: u64,
    timeout: Duration,
    /// Rolled-up series from shards that applied the rollup. Series are
    /// shard-local, so these accumulate by concatenation.
    rolled: Vec<RangeSample>,
    /// Raw windows from a shard that did not apply the rollup
    /// (`applied == false`), reduced by [`Self::into_result`].
    raw: Vec<RangeSample>,
    /// Latched when a peer rejected the operation outright — an older node with
    /// no rollup push-down at all. The caller retries without push-down rather
    /// than failing the query.
    unsupported_peer: bool,
}

impl Default for RollupFanoutCommand {
    fn default() -> Self {
        // An arbitrary but valid request: this instance only ever stands in for
        // a moved-out command (see `FanoutStateInner`), never accumulates.
        Self {
            matchers: Matchers {
                matchers: vec![],
                or_matchers: vec![],
            },
            rollup: RollupRequest {
                kind: RollupKind::SumOverTime,
                range_ms: 0,
                lookback_delta_ms: 0,
                step_ms: 0,
                query_start: 0,
                query_end: 0,
                range_end_ms: 0,
                param: None,
            },
            max_series: 0,
            max_points_per_series: 0,
            timeout: get_cluster_command_timeout(),
            rolled: Vec::new(),
            raw: Vec::new(),
            unsupported_peer: false,
        }
    }
}

impl RollupFanoutCommand {
    pub fn new(
        matchers: Matchers,
        rollup: RollupRequest,
        max_series: u64,
        max_points_per_series: u64,
        timeout: Duration,
    ) -> Self {
        Self {
            matchers,
            rollup,
            max_series,
            max_points_per_series,
            timeout,
            rolled: Vec::new(),
            raw: Vec::new(),
            unsupported_peer: false,
        }
    }

    /// True when at least one peer could not handle the operation: the caller
    /// should re-run the query without push-down.
    pub fn peer_unsupported(&self) -> bool {
        self.unsupported_peer
    }

    /// The shards' output as one result: rolled-up series as they arrived, plus
    /// any raw windows reduced here.
    pub fn into_result(mut self) -> Vec<RangeSample> {
        let raw = std::mem::take(&mut self.raw);
        let mut rolled = std::mem::take(&mut self.rolled);

        // A shard that shipped raw windows is compensated by running the
        // shard-side reduction here, over the same window ends.
        if !raw.is_empty() {
            let window_ends = self.rollup.window_ends();
            rolled.extend(reduce_windows(&self.rollup, &window_ends, raw));
        }

        rolled
    }
}

/// Reduce raw windows with the requested rollup — the shard-side computation,
/// run on the coordinator for a peer that could not run it itself.
fn reduce_windows(
    rollup: &RollupRequest,
    window_ends: &[Timestamp],
    series: Vec<RangeSample>,
) -> Vec<RangeSample> {
    series
        .into_iter()
        .filter_map(|s| {
            let points = rollup.kind.eval_windows(
                &s.samples,
                rollup.range_ms,
                rollup.lookback_delta_ms,
                rollup.step_ms,
                window_ends.iter().copied(),
                rollup.param,
            );
            // Every window was empty: the series contributes nothing at all,
            // which is not the same as contributing NaN.
            (!points.is_empty()).then_some(RangeSample {
                labels: s.labels,
                samples: points,
            })
        })
        .collect()
}

impl FanoutCommand for RollupFanoutCommand {
    type Request = RollupQuery;
    type Response = RollupQueryResponse;

    fn name() -> &'static str {
        "query-rollup"
    }

    fn get_local_response(ctx: &Context, req: RollupQuery) -> ValkeyResult<RollupQueryResponse> {
        let Some(selector) = req.selector.clone() else {
            ctx.log_warning("Received rollup query with no selector, returning empty response");
            return Ok(RollupQueryResponse::default());
        };
        let series_selector: SeriesSelector = selector.try_into()?;

        let rollup = decode_request(&req);
        let window_ends = match &rollup {
            Some(rollup) => rollup.window_ends(),
            // Unknown kind: still ship the windows the coordinator asked about,
            // derived the same way it would have.
            None => window_ends_of(
                req.step_ms,
                req.query_start,
                req.query_end,
                req.range_end_ms,
            ),
        };

        let windows = local_rollup_windows(
            ctx,
            series_selector,
            &window_ends,
            req.range_ms,
            req.max_series,
            req.max_points_per_series,
        )?;

        // An unrecognized rollup means the coordinator is newer than this node.
        // Ship the raw windows and let it reduce them: correct, at the cost of
        // the transfer the push-down would have saved.
        let Some(rollup) = rollup else {
            ctx.log_warning(&format!(
                "Unsupported rollup kind {} in pushed-down query; returning the raw windows",
                req.kind
            ));
            return Ok(raw_response(windows));
        };

        Ok(RollupQueryResponse {
            series: reduce_windows(&rollup, &window_ends, windows)
                .into_iter()
                .map(|s| RollupSeries {
                    labels: metric_name_to_proto_labels_from(&s.labels),
                    points: s.samples.into_iter().map(Into::into).collect(),
                })
                .collect(),
            raw: Vec::new(),
            applied: true,
        })
    }

    fn get_timeout(&self) -> Duration {
        self.timeout
    }

    fn generate_request(&self) -> RollupQuery {
        let matchers: ProtoMatchers =
            ProtoMatchers::try_from(&self.matchers).expect("invalid matchers");

        RollupQuery {
            selector: Some(ProtoSeriesSelector {
                matchers: Some(matchers),
            }),
            query_start: self.rollup.query_start,
            query_end: self.rollup.query_end,
            step_ms: self.rollup.step_ms,
            range_ms: self.rollup.range_ms,
            lookback_delta_ms: self.rollup.lookback_delta_ms as u64,
            range_end_ms: self.rollup.range_end_ms,
            kind: ProtoRollupKind::from(self.rollup.kind) as i32,
            scalar_param: self.rollup.param,
            max_series: self.max_series,
            max_points_per_series: self.max_points_per_series,
        }
    }

    fn on_response(&mut self, resp: Self::Response, target: &NodeInfo) -> FanoutCommandResult {
        if !resp.applied {
            // Self-describing response from a peer that did not reduce.
            self.raw
                .extend(resp.raw.into_iter().map(proto_range_sample_to_native));
            return Ok(());
        }

        // Corrupt-peer defense: an applied response carries rolled-up series
        // only. Reducing stray raw windows here as well would double-count the
        // series they belong to.
        if !resp.raw.is_empty() {
            return Err(FanoutError::custom(format!(
                "TSDB: peer {} returned {} raw series alongside an applied {:?} rollup",
                target.socket_address,
                resp.raw.len(),
                self.rollup.kind,
            )));
        }

        self.rolled
            .extend(resp.series.into_iter().map(|s| RangeSample {
                labels: proto_labels_to_labels(s.labels),
                samples: s.points.into_iter().map(Into::into).collect(),
            }));
        Ok(())
    }

    fn on_error(&mut self, error: FanoutError, target: &NodeInfo) {
        // A peer that does not know this operation at all (rolling upgrade)
        // rejects the envelope rather than answering it. Latch that so the
        // caller can fall back to selecting the raw matrix.
        if matches!(
            error.kind,
            ErrorKind::InvalidMessage
                | ErrorKind::UnknownMessageType
                | ErrorKind::UnsupportedFeatures
        ) {
            self.unsupported_peer = true;
        }
        crate::common::logging::log_warning(format!(
            "Fanout operation {}, failed for target {}: {error}",
            Self::name(),
            target.socket_address,
        ))
    }
}

/// The window ends a request describes, derived identically on both sides so a
/// shard reduces exactly the windows the coordinator asked about.
fn window_ends_of(step_ms: i64, query_start: i64, query_end: i64, range_end_ms: i64) -> Vec<i64> {
    if step_ms <= 0 {
        return vec![range_end_ms];
    }
    crate::promql::time::step_times(query_start, query_end, step_ms).collect()
}

/// Decode the rollup this node is asked to apply, rejecting a value it does not
/// know (proto3 decodes an unknown enum to its raw `i32`, which must not be
/// silently taken for the zero variant).
fn decode_request(req: &RollupQuery) -> Option<RollupRequest> {
    let kind = RollupKind::from(ProtoRollupKind::try_from(req.kind).ok()?);
    Some(RollupRequest {
        kind,
        range_ms: req.range_ms,
        lookback_delta_ms: req.lookback_delta_ms as i64,
        step_ms: req.step_ms,
        query_start: req.query_start,
        query_end: req.query_end,
        range_end_ms: req.range_end_ms,
        param: req.scalar_param,
    })
}

/// A response carrying the unreduced windows, for a shard that could not apply
/// the requested rollup.
fn raw_response(series: Vec<RangeSample>) -> RollupQueryResponse {
    RollupQueryResponse {
        series: Vec::new(),
        raw: series
            .into_iter()
            .map(|s| ProtoRangeSample {
                labels: metric_name_to_proto_labels_from(&s.labels),
                samples: s.samples.into_iter().map(Into::into).collect(),
                key: String::new(),
            })
            .collect(),
        applied: false,
    }
}

fn proto_range_sample_to_native(s: ProtoRangeSample) -> RangeSample {
    RangeSample {
        labels: proto_labels_to_labels(s.labels),
        samples: s.samples.into_iter().map(Into::into).collect(),
    }
}

/// Proto labels for a materialized [`crate::labels::Labels`], matching the
/// encoding [`metric_name_to_proto_labels`] produces for the index's own
/// representation.
fn metric_name_to_proto_labels_from(
    labels: &crate::labels::Labels,
) -> Vec<crate::promql::generated::Label> {
    labels
        .iter()
        .map(|l| crate::promql::generated::Label {
            name: l.name.to_string(),
            value: l.value.to_string(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Sample;
    use crate::labels::{Label, Labels};

    const RANGE_MS: i64 = 60_000;
    const EVAL_TS: Timestamp = 300_000;

    fn labels(name: &str, instance: &str) -> Labels {
        Labels::new(vec![
            Label::new("__name__", name),
            Label::new("instance", instance),
        ])
    }

    fn series(instance: &str, points: &[(i64, f64)]) -> RangeSample {
        RangeSample {
            labels: labels("m", instance),
            samples: points
                .iter()
                .map(|&(timestamp, value)| Sample { timestamp, value })
                .collect(),
        }
    }

    fn request(kind: RollupKind) -> RollupRequest {
        RollupRequest {
            kind,
            param: param_for(kind),
            range_ms: RANGE_MS,
            lookback_delta_ms: 300_000,
            step_ms: 0,
            query_start: EVAL_TS,
            query_end: EVAL_TS,
            range_end_ms: EVAL_TS,
        }
    }

    /// A whole-grid request: eleven windows at a 30s step, each 60s wide, so
    /// consecutive windows overlap — the shape the push-down exists for.
    fn grid_request(kind: RollupKind) -> RollupRequest {
        RollupRequest {
            step_ms: 30_000,
            query_start: 0,
            query_end: EVAL_TS,
            range_end_ms: EVAL_TS,
            ..request(kind)
        }
    }

    /// Both request shapes, so every contract below is checked for a single
    /// evaluation *and* for a step grid.
    fn request_shapes(kind: RollupKind) -> [(&'static str, RollupRequest); 2] {
        [("instant", request(kind)), ("grid", grid_request(kind))]
    }

    /// The rollup's parameter, where it takes one.
    fn param_for(kind: RollupKind) -> Option<f64> {
        (kind == RollupKind::QuantileOverTime).then_some(0.9)
    }

    fn command(rollup: RollupRequest) -> RollupFanoutCommand {
        RollupFanoutCommand::new(
            Matchers {
                matchers: vec![],
                or_matchers: vec![],
            },
            rollup,
            0,
            0,
            Duration::from_secs(1),
        )
    }

    fn node(port: u16) -> NodeInfo {
        NodeInfo::for_test(port)
    }

    /// One shard's response, produced the way `get_local_response` produces it
    /// once the windows have been read.
    fn shard_response(rollup: &RollupRequest, windows: Vec<RangeSample>) -> RollupQueryResponse {
        let ends = rollup.window_ends();
        RollupQueryResponse {
            series: reduce_windows(rollup, &ends, windows)
                .into_iter()
                .map(|s| RollupSeries {
                    labels: metric_name_to_proto_labels_from(&s.labels),
                    points: s.samples.into_iter().map(Into::into).collect(),
                })
                .collect(),
            raw: Vec::new(),
            applied: true,
        }
    }

    /// Results as sorted `(labels, points)` so they compare irrespective of the
    /// order shards answered in.
    fn rendered(series: Vec<RangeSample>) -> Vec<(String, Vec<(i64, String)>)> {
        let mut out: Vec<_> = series
            .into_iter()
            .map(|s| {
                let points = s
                    .samples
                    .iter()
                    .map(|p| (p.timestamp, format!("{:?}", p.value)))
                    .collect();
                (s.labels.to_string(), points)
            })
            .collect();
        out.sort();
        out
    }

    fn test_shards() -> Vec<Vec<RangeSample>> {
        vec![
            vec![
                series("0", &[(250_000, 1.0), (260_000, 2.0), (300_000, 3.0)]),
                series("1", &[(280_000, 7.0)]),
            ],
            vec![series("2", &[(245_000, 5.0), (299_000, 9.0)])],
            // Every sample is outside the window (240s, 300]: this series must
            // not appear in the result at all.
            vec![series("3", &[(100_000, 4.0)])],
        ]
    }

    /// The push-down contract: reducing at the shards and concatenating equals
    /// reducing the concatenated input on one node.
    #[test]
    fn test_fanout_matches_single_node() {
        let shards = test_shards();
        let all: Vec<RangeSample> = shards.iter().flatten().cloned().collect();

        for kind in RollupKind::all() {
            for (shape, rollup) in request_shapes(kind) {
                let mut cmd = command(rollup.clone());
                for (index, shard) in shards.iter().enumerate() {
                    cmd.on_response(
                        shard_response(&rollup, shard.clone()),
                        &node(7000 + index as u16),
                    )
                    .expect("shard response accepted");
                }

                let ends = rollup.window_ends();
                assert_eq!(
                    rendered(reduce_windows(&rollup, &ends, all.clone())),
                    rendered(cmd.into_result()),
                    "{kind:?} ({shape})"
                );
            }
        }
    }

    /// A peer that did not reduce returns its raw windows; the coordinator must
    /// reduce them and still match the single-node result.
    #[test]
    fn test_unreduced_peer_is_compensated() {
        let shards = test_shards();
        let all: Vec<RangeSample> = shards.iter().flatten().cloned().collect();

        for kind in RollupKind::all() {
            for (shape, rollup) in request_shapes(kind) {
                let mut cmd = command(rollup.clone());
                // Shard 0 speaks the protocol; shards 1 and 2 do not.
                cmd.on_response(shard_response(&rollup, shards[0].clone()), &node(7000))
                    .unwrap();
                for (index, shard) in shards.iter().enumerate().skip(1) {
                    let raw = raw_response(shard.clone());
                    assert!(!raw.applied);
                    cmd.on_response(raw, &node(7000 + index as u16)).unwrap();
                }

                let ends = rollup.window_ends();
                assert_eq!(
                    rendered(reduce_windows(&rollup, &ends, all.clone())),
                    rendered(cmd.into_result()),
                    "{kind:?} ({shape})"
                );
            }
        }
    }

    /// Over a grid, a series contributes only the steps whose window held
    /// samples — the sparse shape the transport has to carry. A shard that
    /// reduced and one that did not must produce the same set of steps.
    #[test]
    fn test_grid_transport_is_sparse() {
        let rollup = grid_request(RollupKind::CountOverTime);
        // A gap between t=30s and t=270s: the windows in between are empty.
        let gappy = vec![series("0", &[(30_000, 1.0), (270_000, 2.0)])];

        let applied = shard_response(&rollup, gappy.clone());
        assert_eq!(applied.series.len(), 1);
        let steps: Vec<i64> = applied.series[0]
            .points
            .iter()
            .map(|p| p.timestamp)
            .collect();
        // Windows are `(end - 60s, end]`, so t=30s is reported at steps 30s and
        // 60s, and t=270s at steps 270s and 300s. Nothing in between.
        assert_eq!(
            steps,
            vec![30_000, 60_000, 270_000, 300_000],
            "only the steps whose window held samples are transported"
        );

        // The fallback path must land on exactly the same steps.
        let mut cmd = command(rollup.clone());
        cmd.on_response(raw_response(gappy), &node(7000)).unwrap();
        let fallback = cmd.into_result();
        assert_eq!(fallback.len(), 1);
        assert_eq!(
            fallback[0]
                .samples
                .iter()
                .map(|p| p.timestamp)
                .collect::<Vec<_>>(),
            steps,
        );
    }

    /// A series whose window is empty produces no output at all — the absence
    /// has to survive both the applied and the fallback path.
    #[test]
    fn test_empty_windows_produce_no_series() {
        let rollup = request(RollupKind::CountOverTime);
        let outside = vec![series("3", &[(100_000, 4.0)])];

        let applied = shard_response(&rollup, outside.clone());
        assert!(
            applied.series.is_empty(),
            "a series with no samples in the window must not be reported"
        );

        let mut cmd = command(rollup.clone());
        cmd.on_response(raw_response(outside), &node(7000)).unwrap();
        assert!(cmd.into_result().is_empty(), "nor via the fallback path");
    }

    /// A NaN rolled-up value is a result and must be reported, unlike an empty
    /// window. Confusing the two is the failure this protocol has to avoid.
    #[test]
    fn test_nan_value_is_reported() {
        let rollup = request(RollupKind::SumOverTime);
        let nan_series = vec![series("0", &[(250_000, f64::NAN)])];

        let applied = shard_response(&rollup, nan_series);
        assert_eq!(applied.series.len(), 1, "the NaN result is a result");
        assert_eq!(applied.series[0].points.len(), 1);
        assert!(applied.series[0].points[0].value.is_nan());
    }

    /// The request carries the resolved window, grid and rollup, and survives a
    /// proto round trip.
    #[test]
    fn test_request_round_trip() {
        use crate::fanout::serialization::{Deserialized, Serialized};

        let rollup = RollupRequest {
            kind: RollupKind::CountOverTime,
            range_ms: RANGE_MS,
            lookback_delta_ms: 300_000,
            step_ms: 0,
            query_start: EVAL_TS,
            query_end: EVAL_TS,
            // `@`/`offset` already resolved: the shard is told the window end.
            range_end_ms: EVAL_TS - 3_600_000,
            param: Some(0.9),
        };
        let cmd = RollupFanoutCommand::new(
            Matchers {
                matchers: vec![],
                or_matchers: vec![],
            },
            rollup,
            1_000,
            11_000,
            Duration::from_secs(1),
        );

        let request = cmd.generate_request();
        let mut buf = Vec::new();
        request.serialize(&mut buf);
        let decoded = RollupQuery::deserialize(&buf).unwrap();

        assert_eq!(decoded, request);
        assert_eq!(decoded.kind, ProtoRollupKind::RollupCountOverTime as i32);
        assert_eq!(decoded.range_ms, RANGE_MS);
        assert_eq!(decoded.range_end_ms, EVAL_TS - 3_600_000);
        assert_eq!(decoded.step_ms, 0);
        assert_eq!(decoded.scalar_param, Some(0.9));
        assert_eq!(decoded.max_series, 1_000);
        assert_eq!(decoded.max_points_per_series, 11_000);
        assert!(decoded.selector.is_some());

        // What the shard makes of it must be what the coordinator asked for.
        let round_tripped = decode_request(&decoded).expect("known kind");
        assert_eq!(round_tripped.kind, RollupKind::CountOverTime);
        assert_eq!(round_tripped.window_ends(), vec![EVAL_TS - 3_600_000]);
    }

    /// A rollup this node does not know is never mistaken for the zero variant.
    #[test]
    fn test_decode_rejects_unknown_kind() {
        let mut req = RollupQuery {
            kind: ProtoRollupKind::RollupSumOverTime as i32,
            ..RollupQuery::default()
        };
        assert!(decode_request(&req).is_some());

        // Beyond the enum: a newer coordinator's rollup.
        req.kind = 99;
        assert!(decode_request(&req).is_none());
        req.kind = -1;
        assert!(decode_request(&req).is_none());
    }

    /// A response that carries both payload shapes is rejected instead of being
    /// counted twice.
    #[test]
    fn test_mismatched_payload_is_rejected() {
        let mut cmd = command(request(RollupKind::SumOverTime));
        let stray = RollupQueryResponse {
            series: vec![RollupSeries::default()],
            raw: vec![ProtoRangeSample::default()],
            applied: true,
        };
        assert!(cmd.on_response(stray, &node(7000)).is_err());
    }

    /// A peer that rejects the operation outright latches the fallback flag.
    #[test]
    fn test_unsupported_peer_latches_fallback() {
        let mut cmd = command(request(RollupKind::SumOverTime));
        assert!(!cmd.peer_unsupported());
        cmd.on_error(FanoutError::invalid_message(), &node(7000));
        assert!(cmd.peer_unsupported());

        // An ordinary failure is not a version problem.
        let mut cmd = command(request(RollupKind::SumOverTime));
        cmd.on_error(FanoutError::timeout(), &node(7000));
        assert!(!cmd.peer_unsupported());
    }

    /// Both sides derive the window ends from the same fields, so a shard
    /// reduces exactly the windows the coordinator will ask about.
    #[test]
    fn test_window_ends_agree_across_the_wire() {
        // Instant: one window, at the resolved end.
        let instant = request(RollupKind::SumOverTime);
        assert_eq!(instant.window_ends(), vec![EVAL_TS]);
        assert_eq!(
            window_ends_of(
                0,
                instant.query_start,
                instant.query_end,
                instant.range_end_ms
            ),
            instant.window_ends(),
        );

        // Grid: every step, which is what the range-query phase will send.
        let grid = RollupRequest {
            step_ms: 30_000,
            query_start: 60_000,
            query_end: 180_000,
            ..request(RollupKind::SumOverTime)
        };
        assert_eq!(
            grid.window_ends(),
            vec![60_000, 90_000, 120_000, 150_000, 180_000]
        );
        assert_eq!(
            window_ends_of(
                grid.step_ms,
                grid.query_start,
                grid.query_end,
                grid.range_end_ms
            ),
            grid.window_ends(),
        );
    }
}
