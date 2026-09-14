//! Shard-side PromQL grid push-down: stepped selection, rollups, and their
//! fusion with an outer aggregation.
//!
//! A range query reads every one of its selectors over the whole step grid at
//! once. Without this, that read ships every raw sample in the span to the
//! coordinator, which then buckets or reduces it to one point per step — for a
//! 1 s series at a 15 s step, fifteen samples cross the wire for every point
//! that survives. This command pushes the grid *and* the per-step stage to each
//! shard, so what crosses the wire is one point per series per step: the last
//! sample at each step for a bare selector (`avg(cpu)`, `cpu / cpu offset 5m`),
//! or the window's reduction for `sum_over_time(http_requests_total[5m])` — and
//! one partial per group per step when the selector sits directly under a
//! reducing aggregation.
//!
//! It follows the same rules as [`super::AggregationFanoutCommand`]:
//!
//! * The coordinator decides whether to push down; shards obey the request.
//! * Responses are self-describing: a series a shard chose to ship raw (because
//!   the span was smaller than its grid output) arrives in `raw`, and the
//!   coordinator runs the same per-series stage over it.
//! * The coordinator's combination of the shards' output equals the single-node
//!   result over the concatenated input.
//!
//! The third rule is cheaper here than for aggregation. A PromQL series lives
//! entirely on one shard, so no two shards ever contribute to the same output
//! series and there is no merge algebra at all: the coordinator concatenates.
//! What it *must* preserve is the sparse shape — a `(series, step)` pair with
//! no eligible sample is absent from the result, and absence is not the same
//! as a NaN value.

use crate::common::Sample;
use crate::fanout::{
    FanoutCommand, FanoutCommandResult, FanoutError, NodeInfo, get_cluster_command_timeout,
    log_fanout_failure,
};
use crate::labels::filters::SeriesSelector;
use crate::promql::EvalLabels;
use crate::promql::engine::fanout::query_utils::local_grid_windows;
use crate::promql::engine::fanout::type_conversions::{
    proto_labels_to_eval_labels, range_sample_to_proto,
};
use crate::promql::engine::query_reader::{
    AggregationParam, GridAggregation, GridOutcome, GridRequest, GridRollup,
    GridSeries as GridStage, SteppedPoint, SteppedSeries, grid_window_ends,
};
use crate::promql::exec::aggregations::{AggregationKind, PushdownStrategy};
use crate::promql::exec::partial_aggregation::{SteppedPartialGroups, SteppedSelection};
use crate::promql::functions::RollupKind;
use crate::promql::generated::{
    AggregationKind as ProtoAggregationKind, GridAggregation as ProtoGridAggregation,
    GridGroupPartial, GridQuery, GridQueryResponse, GridRollup as ProtoGridRollup, GridSeries,
    RollupKind as ProtoRollupKind, SeriesSelector as ProtoSeriesSelector,
};
use crate::promql::model::RangeSample;
use promql_parser::label::Matchers;
use promql_parser::parser::LabelModifier;
use std::time::Duration;
use valkey_module::{Context, ValkeyError, ValkeyResult};

impl From<RollupKind> for ProtoRollupKind {
    fn from(kind: RollupKind) -> Self {
        match kind {
            RollupKind::SumOverTime => ProtoRollupKind::SumOverTime,
            RollupKind::CountOverTime => ProtoRollupKind::CountOverTime,
            RollupKind::LastOverTime => ProtoRollupKind::LastOverTime,
            RollupKind::AvgOverTime => ProtoRollupKind::AvgOverTime,
            RollupKind::MinOverTime => ProtoRollupKind::MinOverTime,
            RollupKind::MaxOverTime => ProtoRollupKind::MaxOverTime,
            RollupKind::StddevOverTime => ProtoRollupKind::StddevOverTime,
            RollupKind::StdvarOverTime => ProtoRollupKind::StdvarOverTime,
            RollupKind::MadOverTime => ProtoRollupKind::MadOverTime,
            RollupKind::PresentOverTime => ProtoRollupKind::PresentOverTime,
            RollupKind::FirstOverTime => ProtoRollupKind::FirstOverTime,
            RollupKind::QuantileOverTime => ProtoRollupKind::QuantileOverTime,
            RollupKind::TsOfFirstOverTime => ProtoRollupKind::TsOfFirstOverTime,
            RollupKind::TsOfLastOverTime => ProtoRollupKind::TsOfLastOverTime,
            RollupKind::TsOfMinOverTime => ProtoRollupKind::TsOfMinOverTime,
            RollupKind::TsOfMaxOverTime => ProtoRollupKind::TsOfMaxOverTime,
            RollupKind::Rate => ProtoRollupKind::Rate,
            RollupKind::Increase => ProtoRollupKind::Increase,
            RollupKind::Delta => ProtoRollupKind::Delta,
            RollupKind::IRate => ProtoRollupKind::Irate,
            RollupKind::IDelta => ProtoRollupKind::Idelta,
            RollupKind::Deriv => ProtoRollupKind::Deriv,
            RollupKind::Resets => ProtoRollupKind::Resets,
            RollupKind::Changes => ProtoRollupKind::Changes,
        }
    }
}

/// `ROLLUP_KIND_UNSPECIFIED` — the value a peer produces when it omits the
/// field — is refused like any other value this node does not know: a rollup
/// message with no function in it is a corrupt request, not a stepped one.
impl TryFrom<ProtoRollupKind> for RollupKind {
    type Error = ValkeyError;

    fn try_from(kind: ProtoRollupKind) -> Result<Self, Self::Error> {
        Ok(match kind {
            ProtoRollupKind::SumOverTime => RollupKind::SumOverTime,
            ProtoRollupKind::CountOverTime => RollupKind::CountOverTime,
            ProtoRollupKind::LastOverTime => RollupKind::LastOverTime,
            ProtoRollupKind::AvgOverTime => RollupKind::AvgOverTime,
            ProtoRollupKind::MinOverTime => RollupKind::MinOverTime,
            ProtoRollupKind::MaxOverTime => RollupKind::MaxOverTime,
            ProtoRollupKind::StddevOverTime => RollupKind::StddevOverTime,
            ProtoRollupKind::StdvarOverTime => RollupKind::StdvarOverTime,
            ProtoRollupKind::MadOverTime => RollupKind::MadOverTime,
            ProtoRollupKind::PresentOverTime => RollupKind::PresentOverTime,
            ProtoRollupKind::FirstOverTime => RollupKind::FirstOverTime,
            ProtoRollupKind::QuantileOverTime => RollupKind::QuantileOverTime,
            ProtoRollupKind::TsOfFirstOverTime => RollupKind::TsOfFirstOverTime,
            ProtoRollupKind::TsOfLastOverTime => RollupKind::TsOfLastOverTime,
            ProtoRollupKind::TsOfMinOverTime => RollupKind::TsOfMinOverTime,
            ProtoRollupKind::TsOfMaxOverTime => RollupKind::TsOfMaxOverTime,
            ProtoRollupKind::Rate => RollupKind::Rate,
            ProtoRollupKind::Increase => RollupKind::Increase,
            ProtoRollupKind::Delta => RollupKind::Delta,
            ProtoRollupKind::Irate => RollupKind::IRate,
            ProtoRollupKind::Idelta => RollupKind::IDelta,
            ProtoRollupKind::Deriv => RollupKind::Deriv,
            ProtoRollupKind::Resets => RollupKind::Resets,
            ProtoRollupKind::Changes => RollupKind::Changes,
            ProtoRollupKind::Unspecified => {
                return Err(ValkeyError::Str(
                    "TSDB: grid push-down request carries a rollup with no function",
                ));
            }
        })
    }
}

pub(in crate::promql) struct GridFanoutCommand {
    matchers: Matchers,
    request: GridRequest,
    /// The request's window ends, which the columnar responses index into.
    window_ends: Vec<i64>,
    max_series: u64,
    max_points_per_series: u64,
    timeout: Duration,
    /// Stepped series from the shards, for an unfused request without a
    /// rollup. Series are shard-local, so these accumulate by concatenation.
    stepped: Vec<SteppedSeries>,
    /// Rolled-up series from the shards, for an unfused rollup request.
    rolled: Vec<RangeSample<EvalLabels>>,
    /// Raw spans a shard chose to ship instead, run through the per-series
    /// stage by [`Self::into_result`].
    raw: Vec<RangeSample<EvalLabels>>,
    /// Per-`(group, step)` states from the shards, for a request fused with a
    /// reduction. `None` otherwise.
    partials: Option<SteppedPartialGroups>,
    /// Per-step candidates from the shards, for a request fused with a
    /// selecting or counting operator. `None` otherwise.
    selection: Option<SteppedSelection>,
}

impl Default for GridFanoutCommand {
    fn default() -> Self {
        // An arbitrary but valid request: this instance only ever stands in for
        // a moved-out command (see `FanoutStateInner`), never accumulates.
        Self::new(
            Matchers::empty(),
            GridRequest {
                step_ms: 0,
                query_start: 0,
                query_end: 0,
                range_end_ms: 0,
                lookback_delta_ms: 0,
                rollup: None,
                aggregation: None,
                sample_timestamps: false,
            },
            0,
            0,
            get_cluster_command_timeout(),
        )
    }
}

impl GridFanoutCommand {
    pub fn new(
        matchers: Matchers,
        request: GridRequest,
        max_series: u64,
        max_points_per_series: u64,
        timeout: Duration,
    ) -> Self {
        let (partials, selection) = match request.aggregation.as_ref() {
            Some(agg) if agg.strategy() == PushdownStrategy::Reduce => {
                (Some(SteppedPartialGroups::new(agg.kind)), None)
            }
            Some(agg) => (None, Some(SteppedSelection::new(agg.clone()))),
            None => (None, None),
        };
        let window_ends = request.window_ends();
        Self {
            matchers,
            request,
            window_ends,
            max_series,
            max_points_per_series,
            timeout,
            stepped: Vec::new(),
            rolled: Vec::new(),
            raw: Vec::new(),
            partials,
            selection,
        }
    }

    /// The shards' output as one result.
    ///
    /// Whatever arrived raw is run through the per-series stage here, over the
    /// same window ends a shard used, and — for a fused request — folded into
    /// the partials or candidates the shards sent. What comes out is the same
    /// either way.
    pub fn into_result(mut self) -> Result<GridOutcome, FanoutError> {
        let raw = std::mem::take(&mut self.raw);
        let mut stepped = std::mem::take(&mut self.stepped);
        let mut rolled = std::mem::take(&mut self.rolled);

        if !raw.is_empty() {
            match self.request.per_series(&self.window_ends, raw) {
                GridStage::Stepped(series) => stepped.extend(series),
                GridStage::Rolled(series) => rolled.extend(series),
            }
        }

        if let Some(mut selection) = self.selection.take() {
            // Fused with a selecting or counting operator: the shards'
            // outputs are candidates; whatever arrived raw or un-selected is
            // run through the operator here and joins them.
            let unselected = if self.request.rollup.is_some() {
                GridStage::Rolled(rolled)
            } else {
                GridStage::Stepped(stepped)
            }
            .into_step_values();
            if !unselected.is_empty() {
                selection
                    .apply(unselected)
                    .map_err(|e| FanoutError::custom(e.to_string()))?;
            }
            return selection
                .finalize()
                .map(GridOutcome::Reduced)
                .map_err(|e| FanoutError::custom(e.to_string()));
        }

        Ok(match self.partials.take() {
            // Fused: fold in whatever arrived un-grouped, then finalize every
            // (group, step).
            Some(mut partials) => {
                let modifier = self
                    .request
                    .aggregation
                    .as_ref()
                    .and_then(|agg| agg.modifier.as_ref());
                let ungrouped = if self.request.rollup.is_some() {
                    GridStage::Rolled(rolled)
                } else {
                    GridStage::Stepped(stepped)
                }
                .into_step_values();
                if !ungrouped.is_empty() {
                    partials.accumulate(modifier, ungrouped);
                }
                GridOutcome::Reduced(partials.finalize())
            }
            None if self.request.rollup.is_some() => GridOutcome::Rolled(rolled),
            None => GridOutcome::Stepped(stepped),
        })
    }
}

impl FanoutCommand for GridFanoutCommand {
    type Request = GridQuery;
    type Response = GridQueryResponse;

    fn name() -> &'static str {
        "query-grid"
    }

    fn get_local_response(ctx: &Context, req: GridQuery) -> ValkeyResult<GridQueryResponse> {
        let Some(selector) = req.selector.clone() else {
            ctx.log_warning("Received grid query with no selector, returning empty response");
            return Ok(GridQueryResponse::default());
        };
        let series_selector: SeriesSelector = (&selector).try_into()?;
        let request = decode_request(&req)?;

        let window_ends = grid_window_ends(
            req.step_ms,
            req.query_start,
            req.query_end,
            req.range_end_ms,
        );

        let windows = local_grid_windows(
            ctx,
            series_selector,
            &window_ends,
            request.backward_ms(),
            req.max_series,
            req.max_points_per_series,
        )?;

        shard_response(&request, &window_ends, windows)
    }

    fn get_timeout(&self) -> Duration {
        self.timeout
    }

    fn generate_request(&self) -> GridQuery {
        GridQuery {
            selector: Some(ProtoSeriesSelector::from(&self.matchers)),
            query_start: self.request.query_start,
            query_end: self.request.query_end,
            step_ms: self.request.step_ms,
            range_end_ms: self.request.range_end_ms,
            lookback_delta_ms: self.request.lookback_delta_ms as u64,
            max_series: self.max_series,
            max_points_per_series: self.max_points_per_series,
            rollup: self.request.rollup.as_ref().map(|rollup| ProtoGridRollup {
                kind: ProtoRollupKind::from(rollup.kind) as i32,
                range_ms: rollup.range_ms,
                scalar_param: rollup.param,
            }),
            aggregation: self.request.aggregation.as_ref().map(|agg| {
                let (scalar_param, label_param) = match &agg.param {
                    Some(AggregationParam::Scalar(value)) => (Some(*value), None),
                    Some(AggregationParam::Label(label)) => (None, Some(label.clone())),
                    None => (None, None),
                };
                ProtoGridAggregation {
                    kind: ProtoAggregationKind::from(agg.kind) as i32,
                    grouping: agg.modifier.as_ref().map(Into::into),
                    scalar_param,
                    label_param,
                }
            }),
            sample_timestamps: self.request.sample_timestamps,
        }
    }

    fn on_response(&mut self, resp: Self::Response, target: &NodeInfo) -> FanoutCommandResult {
        // Corrupt-peer defenses. A request fused with a reduction is answered
        // in partials; every other request in series — a fused selection's
        // series are the shard's per-step picks or counts. A response carrying
        // the wrong list would be folded in twice, or grouped when the query
        // asked for series.
        match self.partials.as_mut() {
            Some(partials) => {
                if !resp.series.is_empty() {
                    return Err(FanoutError::custom(format!(
                        "TSDB: peer {} returned {} per-series values for a fused grid query",
                        target.socket_address,
                        resp.series.len(),
                    )));
                }
                for partial in resp.partials {
                    partials.merge(
                        partial.step_ts,
                        proto_labels_to_eval_labels(partial.labels),
                        partial.state.unwrap_or_default().into(),
                    );
                }
            }
            None => {
                if !resp.partials.is_empty() {
                    return Err(FanoutError::custom(format!(
                        "TSDB: peer {} grouped a grid query that was not requested grouped",
                        target.socket_address,
                    )));
                }
                let mut candidates = Vec::new();
                for series in resp.series {
                    let points: Vec<(i64, i64, f64)> = decode_columns(&self.window_ends, &series)
                        .map_err(|why| {
                            FanoutError::custom(format!(
                                "TSDB: peer {} returned a malformed grid series: {why}",
                                target.socket_address,
                            ))
                        })?
                        .collect();
                    let labels = proto_labels_to_eval_labels(series.labels);
                    if self.selection.is_some() {
                        candidates.push(RangeSample {
                            labels,
                            samples: points
                                .into_iter()
                                .map(|(step_ts, _, value)| Sample::new(step_ts, value))
                                .collect(),
                        });
                    } else if self.request.rollup.is_some() {
                        self.rolled.push(RangeSample {
                            labels,
                            samples: points
                                .into_iter()
                                .map(|(step_ts, _, value)| Sample::new(step_ts, value))
                                .collect(),
                        });
                    } else {
                        self.stepped.push(SteppedSeries {
                            labels,
                            points: points
                                .into_iter()
                                .map(|(step_ts, sample_ts, value)| SteppedPoint {
                                    step_ts,
                                    sample: Sample::new(sample_ts, value),
                                })
                                .collect(),
                        });
                    }
                }
                if let Some(selection) = self.selection.as_mut() {
                    selection.merge(candidates);
                }
            }
        }

        for raw in resp.raw {
            let series = RangeSample::try_from(raw).map_err(|why| {
                FanoutError::custom(format!(
                    "TSDB: peer {} returned an undecodable raw series: {why}",
                    target.socket_address,
                ))
            })?;
            self.raw.push(series);
        }
        Ok(())
    }

    fn on_error(&mut self, error: FanoutError, target: &NodeInfo) {
        log_fanout_failure(Self::name(), target, &error);
    }
}

/// Decode the request this node is asked to evaluate. A rollup or aggregation
/// it does not know is a corrupt request (every node runs the same build) and
/// is refused: proto3 decodes an unknown enum to its raw `i32`, which must not
/// be silently taken for the zero variant.
fn decode_request(req: &GridQuery) -> ValkeyResult<GridRequest> {
    let rollup = req
        .rollup
        .as_ref()
        .map(|rollup| {
            let kind = ProtoRollupKind::try_from(rollup.kind)
                .map_err(|_| {
                    ValkeyError::String(format!(
                        "TSDB: unknown rollup kind {} in grid push-down request",
                        rollup.kind
                    ))
                })
                .and_then(RollupKind::try_from)?;
            Ok::<_, ValkeyError>(GridRollup {
                kind,
                range_ms: rollup.range_ms,
                param: rollup.scalar_param,
            })
        })
        .transpose()?;
    let aggregation = req
        .aggregation
        .as_ref()
        .map(|agg| {
            let kind = ProtoAggregationKind::try_from(agg.kind)
                .ok()
                .and_then(|kind| AggregationKind::try_from(kind).ok())
                .filter(|kind| kind.pushdown_strategy().is_some())
                .ok_or_else(|| {
                    ValkeyError::String(format!(
                        "TSDB: aggregation kind {} cannot be fused onto a grid push-down request",
                        agg.kind
                    ))
                })?;
            let param = match (agg.scalar_param, agg.label_param.clone()) {
                (Some(value), _) => Some(AggregationParam::Scalar(value)),
                (None, Some(label)) => Some(AggregationParam::Label(label)),
                (None, None) => None,
            };
            Ok::<_, ValkeyError>(GridAggregation {
                kind,
                modifier: agg.grouping.clone().map(LabelModifier::from),
                param,
            })
        })
        .transpose()?;
    Ok(GridRequest {
        step_ms: req.step_ms,
        query_start: req.query_start,
        query_end: req.query_end,
        range_end_ms: req.range_end_ms,
        lookback_delta_ms: req.lookback_delta_ms as i64,
        rollup,
        aggregation,
        sample_timestamps: req.sample_timestamps,
    })
}

/// A series' points as the wire's columns: a presence bitmap over
/// `window_ends`, the values of the present windows in order, and — when the
/// request asked for them — each pick's lag behind its window end.
///
/// `points` must arrive in window order, as the per-series stage produces
/// them; a point whose step is not a window end is dropped (it cannot be
/// addressed), which never happens for a stage run over these ends.
fn encode_columns(
    window_ends: &[i64],
    points: impl Iterator<Item = (i64, i64, f64)>,
    with_lag: bool,
) -> (Vec<u8>, Vec<f64>, Vec<i64>) {
    let mut presence = vec![0u8; window_ends.len().div_ceil(8)];
    let mut values = Vec::new();
    let mut lags = Vec::new();
    let mut cursor = 0usize;
    for (step_ts, sample_ts, value) in points {
        while cursor < window_ends.len() && window_ends[cursor] < step_ts {
            cursor += 1;
        }
        if cursor >= window_ends.len() || window_ends[cursor] != step_ts {
            debug_assert!(false, "grid point {step_ts} is not a window end");
            continue;
        }
        presence[cursor / 8] |= 1 << (cursor % 8);
        values.push(value);
        if with_lag {
            lags.push(step_ts - sample_ts);
        }
        cursor += 1;
    }
    (presence, values, lags)
}

/// The inverse of [`encode_columns`]: `(step_ts, sample_ts, value)` per
/// present window. A bitmap that reaches past the grid, or a value count that
/// disagrees with the bitmap, is a corrupt response rather than a short one.
fn decode_columns<'a>(
    window_ends: &'a [i64],
    series: &'a GridSeries,
) -> Result<impl Iterator<Item = (i64, i64, f64)> + 'a, String> {
    let present: Vec<usize> = series
        .presence
        .iter()
        .enumerate()
        .flat_map(|(byte, bits)| {
            (0..8)
                .filter(move |bit| bits & (1 << bit) != 0)
                .map(move |bit| byte * 8 + bit)
        })
        .collect();
    if present.last().is_some_and(|&i| i >= window_ends.len()) {
        return Err(format!(
            "presence bitmap addresses window {} of {}",
            present.last().unwrap(),
            window_ends.len()
        ));
    }
    if present.len() != series.values.len() {
        return Err(format!(
            "{} present windows but {} values",
            present.len(),
            series.values.len()
        ));
    }
    if !series.sample_lag.is_empty() && series.sample_lag.len() != series.values.len() {
        return Err(format!(
            "{} values but {} sample lags",
            series.values.len(),
            series.sample_lag.len()
        ));
    }
    Ok(present.into_iter().enumerate().map(move |(k, i)| {
        let step_ts = window_ends[i];
        let lag = series.sample_lag.get(k).copied().unwrap_or(0);
        (step_ts, step_ts - lag, series.values[k])
    }))
}

/// Whether a series is smaller as its raw span than as one point per window
/// end — `step` finer than the sample cadence — in which case shipping the
/// span and letting the coordinator run the per-series stage is the cheaper
/// transfer.
fn ships_raw(window_ends: &[i64], series: &RangeSample<EvalLabels>) -> bool {
    window_ends.len() > series.samples.len()
}

/// One shard's response: the per-series stage over every series whose grid
/// output is the smaller form, the rest shipped raw, and — for a fused request
/// — the staged series folded into per-`(group, step)` partials.
fn shard_response(
    request: &GridRequest,
    window_ends: &[i64],
    windows: Vec<RangeSample<EvalLabels>>,
) -> ValkeyResult<GridQueryResponse> {
    let (raw, staged): (Vec<_>, Vec<_>) = windows
        .into_iter()
        .partition(|series| ships_raw(window_ends, series));
    let staged = request.per_series(window_ends, staged);

    let raw = raw
        .into_iter()
        .map(range_sample_to_proto)
        .collect::<ValkeyResult<Vec<_>>>()?;

    if let Some(aggregation) = request.aggregation.as_ref()
        && aggregation.strategy() != PushdownStrategy::Reduce
    {
        // A selecting or counting operator: run it per step over this
        // shard's series and ship its output as candidates — k series per
        // step, or one count per value — rather than every series.
        let mut selection = SteppedSelection::new(aggregation.clone());
        return selection
            .apply(staged.into_step_values())
            .and_then(|()| selection.finalize())
            .map_err(|e| ValkeyError::String(e.to_string()))
            .map(|selected| GridQueryResponse {
                series: columnar_series(window_ends, selected),
                partials: Vec::new(),
                raw,
            });
    }

    if let Some(aggregation) = request.aggregation.as_ref() {
        let mut groups = SteppedPartialGroups::new(aggregation.kind);
        groups.accumulate(aggregation.modifier.as_ref(), staged.into_step_values());
        return Ok(GridQueryResponse {
            series: Vec::new(),
            partials: groups
                .into_partials()
                .map(|(step_ts, labels, state)| GridGroupPartial {
                    labels: labels.iter().map(Into::into).collect(),
                    step_ts,
                    state: Some(state.into()),
                })
                .collect(),
            raw,
        });
    }

    let series = match staged {
        GridStage::Stepped(series) => series
            .into_iter()
            .map(|s| {
                let (presence, values, sample_lag) = encode_columns(
                    window_ends,
                    s.points
                        .iter()
                        .map(|p| (p.step_ts, p.sample.timestamp, p.sample.value)),
                    request.sample_timestamps,
                );
                GridSeries {
                    labels: (&s.labels).into(),
                    presence,
                    values,
                    sample_lag,
                }
            })
            .collect(),
        GridStage::Rolled(series) => columnar_series(window_ends, series),
    };

    Ok(GridQueryResponse {
        series,
        partials: Vec::new(),
        raw,
    })
}

/// Per-entry `(step, value)` points as columnar series: rollup output, or a
/// fused selection's per-step picks and counts. No lag column — a value here
/// belongs to its window.
fn columnar_series(window_ends: &[i64], series: Vec<RangeSample<EvalLabels>>) -> Vec<GridSeries> {
    series
        .into_iter()
        .map(|s| {
            let (presence, values, sample_lag) = encode_columns(
                window_ends,
                s.samples
                    .iter()
                    .map(|p| (p.timestamp, p.timestamp, p.value)),
                false,
            );
            GridSeries {
                labels: (&s.labels).into(),
                presence,
                values,
                sample_lag,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Timestamp;
    use crate::labels::{Label, Labels};
    use crate::promql::exec::pipeline::for_each_step_sample;

    const RANGE_MS: i64 = 60_000;
    const LOOKBACK_MS: i64 = 300_000;
    const EVAL_TS: Timestamp = 300_000;

    fn labels(name: &str, instance: &str) -> Labels {
        Labels::new(vec![
            Label::new("__name__", name),
            Label::new("instance", instance),
        ])
    }

    fn series(instance: &str, points: &[(i64, f64)]) -> RangeSample<EvalLabels> {
        RangeSample {
            labels: labels("m", instance).into(),
            samples: points
                .iter()
                .map(|&(timestamp, value)| Sample { timestamp, value })
                .collect(),
        }
    }

    /// A single-evaluation stepped request at `EVAL_TS`, asking for the
    /// picks' own timestamps so the fan-out can be compared point for point
    /// with the single-node stage.
    fn stepped_request() -> GridRequest {
        GridRequest {
            step_ms: 0,
            query_start: EVAL_TS,
            query_end: EVAL_TS,
            range_end_ms: EVAL_TS,
            lookback_delta_ms: LOOKBACK_MS,
            rollup: None,
            aggregation: None,
            sample_timestamps: true,
        }
    }

    /// The same request over a grid: eleven steps at 30s.
    fn stepped_grid_request() -> GridRequest {
        GridRequest {
            step_ms: 30_000,
            query_start: 0,
            query_end: EVAL_TS,
            ..stepped_request()
        }
    }

    fn rollup_request(kind: RollupKind) -> GridRequest {
        GridRequest {
            rollup: Some(GridRollup {
                kind,
                range_ms: RANGE_MS,
                param: param_for(kind),
            }),
            ..stepped_request()
        }
    }

    /// A whole-grid rollup: eleven windows at a 30s step, each 60s wide, so
    /// consecutive windows overlap — the shape the push-down exists for.
    fn rollup_grid_request(kind: RollupKind) -> GridRequest {
        GridRequest {
            step_ms: 30_000,
            query_start: 0,
            query_end: EVAL_TS,
            ..rollup_request(kind)
        }
    }

    fn fused(base: GridRequest, agg: AggregationKind, by: &[&str]) -> GridRequest {
        fused_with(base, agg, by, None)
    }

    fn fused_with(
        base: GridRequest,
        agg: AggregationKind,
        by: &[&str],
        param: Option<AggregationParam>,
    ) -> GridRequest {
        GridRequest {
            aggregation: Some(GridAggregation {
                kind: agg,
                modifier: (!by.is_empty()).then(|| {
                    LabelModifier::Include(promql_parser::label::Labels::new(by.to_vec()))
                }),
                param,
            }),
            ..base
        }
    }

    /// The rollup's parameter, where it takes one.
    fn param_for(kind: RollupKind) -> Option<f64> {
        (kind == RollupKind::QuantileOverTime).then_some(0.9)
    }

    fn command(request: GridRequest) -> GridFanoutCommand {
        GridFanoutCommand::new(Matchers::empty(), request, 0, 0, Duration::from_secs(1))
    }

    fn node(port: u16) -> NodeInfo {
        NodeInfo::for_test(port)
    }

    /// One shard's response, produced the way `get_local_response` produces it
    /// once the windows have been read.
    fn response(request: &GridRequest, windows: Vec<RangeSample<EvalLabels>>) -> GridQueryResponse {
        shard_response(request, &request.window_ends(), windows).unwrap()
    }

    /// A response carrying every series raw, whatever its size.
    fn raw_response(windows: Vec<RangeSample<EvalLabels>>) -> GridQueryResponse {
        GridQueryResponse {
            series: Vec::new(),
            partials: Vec::new(),
            raw: windows
                .into_iter()
                .map(|s| range_sample_to_proto(s).unwrap())
                .collect(),
        }
    }

    /// One rendered point: `(step, sample timestamp, value)`.
    type Point = (i64, i64, String);

    /// An outcome as sorted `(labels, points)`, so results compare irrespective
    /// of the order shards answered in. Stepped points carry the sample's own
    /// timestamp beside the step; the others carry zero there.
    fn rendered(outcome: GridOutcome) -> Vec<(String, Vec<Point>)> {
        let mut out: Vec<_> = match outcome {
            GridOutcome::Stepped(series) => series
                .into_iter()
                .map(|s| {
                    let points = s
                        .points
                        .iter()
                        .map(|p| {
                            (
                                p.step_ts,
                                p.sample.timestamp,
                                format!("{:?}", p.sample.value),
                            )
                        })
                        .collect();
                    (s.labels.to_string(), points)
                })
                .collect(),
            GridOutcome::Rolled(series)
            | GridOutcome::Reduced(series)
            | GridOutcome::Raw(series) => series
                .into_iter()
                .map(|s| {
                    let points = s
                        .samples
                        .iter()
                        .map(|p| (p.timestamp, 0, format!("{:?}", p.value)))
                        .collect();
                    (s.labels.to_string(), points)
                })
                .collect(),
        };
        out.sort();
        out
    }

    fn test_shards() -> Vec<Vec<RangeSample<EvalLabels>>> {
        vec![
            vec![
                series("0", &[(250_000, 1.0), (260_000, 2.0), (300_000, 3.0)]),
                series("1", &[(280_000, 7.0)]),
            ],
            vec![series("2", &[(245_000, 5.0), (299_000, 9.0)])],
            // Every sample is outside the window (240s, 300]: this series must
            // not appear in a rollup result at all.
            vec![series("3", &[(100_000, 4.0)])],
        ]
    }

    /// Dense data, so every series has more samples than the grid has steps
    /// and the shards answer in the staged form rather than raw.
    fn dense_shards() -> Vec<Vec<RangeSample<EvalLabels>>> {
        let dense = |instance: &str, base: f64| {
            series(
                instance,
                &(0..=30)
                    .map(|i| (i * 10_000, base + i as f64))
                    .collect::<Vec<_>>(),
            )
        };
        vec![
            vec![dense("0", 0.0), dense("1", 100.0)],
            vec![dense("2", 1000.0)],
            // Ends early: absent from the later steps.
            vec![series(
                "3",
                &(0..=5).map(|i| (i * 10_000, i as f64)).collect::<Vec<_>>(),
            )],
        ]
    }

    /// Every request shape a range query issues, so each contract below is
    /// checked for all of them.
    fn request_shapes() -> Vec<(String, GridRequest)> {
        let mut shapes = vec![
            ("stepped/instant".to_string(), stepped_request()),
            ("stepped/grid".to_string(), stepped_grid_request()),
            (
                "stepped/fused sum by".to_string(),
                fused(stepped_grid_request(), AggregationKind::Sum, &["__name__"]),
            ),
            (
                "stepped/fused avg".to_string(),
                fused(stepped_grid_request(), AggregationKind::Avg, &[]),
            ),
            // The selecting and counting operators: k is larger than any one
            // shard's share, so the coordinator's re-selection has to choose
            // across shards.
            (
                "stepped/fused topk 1".to_string(),
                fused_with(
                    stepped_grid_request(),
                    AggregationKind::Topk,
                    &[],
                    Some(AggregationParam::Scalar(1.0)),
                ),
            ),
            (
                "stepped/fused topk 3".to_string(),
                fused_with(
                    stepped_grid_request(),
                    AggregationKind::Topk,
                    &[],
                    Some(AggregationParam::Scalar(3.0)),
                ),
            ),
            (
                "stepped/fused bottomk by".to_string(),
                fused_with(
                    stepped_grid_request(),
                    AggregationKind::Bottomk,
                    &["__name__"],
                    Some(AggregationParam::Scalar(2.0)),
                ),
            ),
            (
                "stepped/fused limitk".to_string(),
                fused_with(
                    stepped_grid_request(),
                    AggregationKind::Limitk,
                    &[],
                    Some(AggregationParam::Scalar(2.0)),
                ),
            ),
            (
                "stepped/fused limit_ratio".to_string(),
                fused_with(
                    stepped_grid_request(),
                    AggregationKind::LimitRatio,
                    &[],
                    Some(AggregationParam::Scalar(0.5)),
                ),
            ),
            (
                "stepped/fused count_values".to_string(),
                fused_with(
                    stepped_grid_request(),
                    AggregationKind::CountValues,
                    &[],
                    Some(AggregationParam::Label("v".into())),
                ),
            ),
            (
                "rate/fused topk".to_string(),
                fused_with(
                    rollup_grid_request(RollupKind::Rate),
                    AggregationKind::Topk,
                    &[],
                    Some(AggregationParam::Scalar(2.0)),
                ),
            ),
            (
                "sum_over_time/fused count_values by".to_string(),
                fused_with(
                    rollup_grid_request(RollupKind::SumOverTime),
                    AggregationKind::CountValues,
                    &["__name__"],
                    Some(AggregationParam::Label("v".into())),
                ),
            ),
        ];
        for kind in RollupKind::all() {
            shapes.push((format!("{kind:?}/instant"), rollup_request(kind)));
            shapes.push((format!("{kind:?}/grid"), rollup_grid_request(kind)));
            shapes.push((
                format!("{kind:?}/fused"),
                fused(
                    rollup_grid_request(kind),
                    AggregationKind::Sum,
                    &["__name__"],
                ),
            ));
        }
        shapes
    }

    /// Feed every shard's response to a command and take the result.
    fn fan_out(request: &GridRequest, shards: &[Vec<RangeSample<EvalLabels>>]) -> GridOutcome {
        let mut cmd = command(request.clone());
        for (index, shard) in shards.iter().enumerate() {
            cmd.on_response(response(request, shard.clone()), &node(7000 + index as u16))
                .expect("shard response accepted");
        }
        cmd.into_result().unwrap()
    }

    /// The push-down contract: evaluating at the shards and combining equals
    /// evaluating the concatenated input on one node — for every shape, and
    /// whether the shards answered staged or raw.
    #[test]
    fn test_fanout_matches_single_node() {
        for shards in [test_shards(), dense_shards()] {
            let all: Vec<RangeSample<EvalLabels>> = shards.iter().flatten().cloned().collect();
            for (shape, request) in request_shapes() {
                let want = rendered(request.evaluate(all.clone()).unwrap());

                assert_eq!(want, rendered(fan_out(&request, &shards)), "{shape}");

                // Shards that shipped everything raw are compensated here.
                let mut cmd = command(request.clone());
                for (index, shard) in shards.iter().enumerate() {
                    cmd.on_response(raw_response(shard.clone()), &node(7000 + index as u16))
                        .unwrap();
                }
                assert_eq!(
                    want,
                    rendered(cmd.into_result().unwrap()),
                    "{shape} (all raw)"
                );

                // And a mix: one staged shard, the rest raw.
                let mut cmd = command(request.clone());
                cmd.on_response(response(&request, shards[0].clone()), &node(7000))
                    .unwrap();
                for (index, shard) in shards.iter().enumerate().skip(1) {
                    cmd.on_response(raw_response(shard.clone()), &node(7000 + index as u16))
                        .unwrap();
                }
                assert_eq!(
                    want,
                    rendered(cmd.into_result().unwrap()),
                    "{shape} (mixed)"
                );
            }
        }
    }

    /// The stepped stage is `for_each_step_sample`'s rule, run on the shard:
    /// the last sample at or before each step and inside the lookback, with
    /// the sample's own timestamp carried through.
    #[test]
    fn test_stepped_selection_matches_the_step_loop_rule() {
        let request = GridRequest {
            lookback_delta_ms: 25_000,
            ..stepped_grid_request()
        };
        let input = series(
            "0",
            &[(5_000, 1.0), (35_000, 2.0), (36_000, 3.0), (200_000, 4.0)],
        );
        let ends = request.window_ends();

        let mut want = Vec::new();
        for_each_step_sample(
            &input.samples,
            ends.iter().copied(),
            25_000,
            |step, latest| {
                if let Some(s) = latest {
                    want.push((step, s.timestamp, format!("{:?}", s.value)));
                }
            },
        );
        // Steps 0s (no sample yet), 30s (the 5s sample is exactly one lookback
        // back, and the lower bound is exclusive), 60s (3.0 at 36s), 90s …
        // 180s (36s is beyond the lookback), 210s (4.0 at 200s), then nothing
        // again.
        assert_eq!(
            want,
            vec![
                (60_000, 36_000, "3.0".to_string()),
                (210_000, 200_000, "4.0".to_string()),
            ]
        );

        let got = rendered(request.evaluate(vec![input]).unwrap());
        assert_eq!(got, vec![("m{instance=\"0\"}".to_string(), want)]);
    }

    /// `@` collapses the grid onto one window end; `offset` shifts it. The
    /// coordinator resolves both into the geometry, and the shard sees only
    /// window ends — so an `@` request is a single-evaluation request at the
    /// pinned end, and an offset one is the grid shifted uniformly.
    #[test]
    fn test_modifiers_are_geometry() {
        // `m @ 120` over any grid: one window end.
        let pinned = GridRequest {
            step_ms: 30_000,
            query_start: 120_000,
            query_end: 120_000,
            range_end_ms: 120_000,
            ..stepped_request()
        };
        assert_eq!(pinned.window_ends(), vec![120_000]);
        let out = request_points(&pinned, series("0", &[(100_000, 1.0), (130_000, 2.0)]));
        assert_eq!(out, vec![(120_000, 100_000, "1.0".to_string())]);

        // `m offset 1m` over 120s..240s at 60s: ends at 60s, 120s, 180s.
        let shifted = GridRequest {
            step_ms: 60_000,
            query_start: 60_000,
            query_end: 180_000,
            range_end_ms: 180_000,
            ..stepped_request()
        };
        assert_eq!(shifted.window_ends(), vec![60_000, 120_000, 180_000]);
        let out = request_points(&shifted, series("0", &[(100_000, 1.0), (130_000, 2.0)]));
        assert_eq!(
            out,
            vec![
                (120_000, 100_000, "1.0".to_string()),
                (180_000, 130_000, "2.0".to_string()),
            ]
        );
    }

    fn request_points(request: &GridRequest, input: RangeSample<EvalLabels>) -> Vec<Point> {
        rendered(request.evaluate(vec![input]).unwrap())
            .pop()
            .map(|(_, points)| points)
            .unwrap_or_default()
    }

    /// The size rule: a series with fewer samples in the span than the grid
    /// has steps is shipped raw, one with more is staged — per series, in one
    /// response — and the coordinator lands on the single-node answer.
    #[test]
    fn test_size_rule_ships_the_smaller_form() {
        let request = stepped_grid_request(); // 11 window ends
        let sparse = series("sparse", &[(0, 1.0), (150_000, 2.0)]);
        let dense = series(
            "dense",
            &(0..=30).map(|i| (i * 10_000, i as f64)).collect::<Vec<_>>(),
        );
        assert!(ships_raw(&request.window_ends(), &sparse));
        assert!(!ships_raw(&request.window_ends(), &dense));

        let resp = response(&request, vec![sparse.clone(), dense.clone()]);
        assert_eq!(resp.raw.len(), 1, "the sparse series travels raw");
        assert_eq!(resp.series.len(), 1, "the dense one travels stepped");
        assert!(resp.partials.is_empty());

        let mut cmd = command(request.clone());
        cmd.on_response(resp, &node(7000)).unwrap();
        assert_eq!(
            rendered(cmd.into_result().unwrap()),
            rendered(
                request
                    .evaluate(vec![sparse.clone(), dense.clone()])
                    .unwrap()
            ),
        );

        // Fused: the raw series is folded into the partials on the coordinator.
        let fused = fused(request, AggregationKind::Sum, &["__name__"]);
        let resp = response(&fused, vec![sparse.clone(), dense.clone()]);
        assert_eq!(resp.raw.len(), 1);
        assert!(resp.series.is_empty());
        assert!(!resp.partials.is_empty());
        let mut cmd = command(fused.clone());
        cmd.on_response(resp, &node(7000)).unwrap();
        assert_eq!(
            rendered(cmd.into_result().unwrap()),
            rendered(fused.evaluate(vec![sparse, dense]).unwrap()),
        );
    }

    /// Over a grid, a series contributes only the steps whose window held
    /// samples — the sparse shape the transport has to carry. A shard that
    /// staged and one that shipped raw must produce the same set of steps.
    #[test]
    fn test_grid_transport_is_sparse() {
        let request = rollup_grid_request(RollupKind::CountOverTime);
        // A gap between t=30s and t=270s: the windows in between are empty.
        // Twelve samples so the series stages rather than ships raw.
        let mut points: Vec<(i64, f64)> = (0..=30_000).step_by(5_000).map(|t| (t, 1.0)).collect();
        points.extend((270_000..=300_000).step_by(6_000).map(|t| (t, 2.0)));
        let gappy = vec![series("0", &points)];

        let staged = response(&request, gappy.clone());
        assert_eq!(staged.series.len(), 1);
        let steps: Vec<i64> = decode_columns(&request.window_ends(), &staged.series[0])
            .unwrap()
            .map(|(step_ts, _, _)| step_ts)
            .collect();
        // Windows are `(end - 60s, end]`, so the early samples are reported at
        // steps 0s..=60s (the 90s window starts just past t=30s) and the late
        // ones at 270s and 300s. Nothing between.
        assert_eq!(steps, vec![0, 30_000, 60_000, 270_000, 300_000]);

        let mut cmd = command(request.clone());
        cmd.on_response(raw_response(gappy), &node(7000)).unwrap();
        let GridOutcome::Rolled(fallback) = cmd.into_result().unwrap() else {
            panic!("a rollup request answers Rolled");
        };
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

    /// A series whose window is empty produces no output at all, and a NaN
    /// rolled-up value is a result and must be reported. Confusing the two is
    /// the failure this protocol has to avoid.
    #[test]
    fn test_absence_is_not_nan() {
        let request = rollup_request(RollupKind::CountOverTime);
        let outside = vec![series("3", &[(100_000, 4.0)])];
        let mut cmd = command(request.clone());
        cmd.on_response(raw_response(outside), &node(7000)).unwrap();
        assert!(
            rendered(cmd.into_result().unwrap()).is_empty(),
            "a series with no samples in the window must not be reported"
        );

        let request = rollup_request(RollupKind::SumOverTime);
        let nan_series = vec![series("0", &[(250_000, f64::NAN)])];
        let mut cmd = command(request.clone());
        cmd.on_response(raw_response(nan_series), &node(7000))
            .unwrap();
        let GridOutcome::Rolled(rolled) = cmd.into_result().unwrap() else {
            panic!("a rollup request answers Rolled");
        };
        assert_eq!(rolled.len(), 1, "the NaN result is a result");
        assert!(rolled[0].samples[0].value.is_nan());
    }

    /// The request carries the resolved grid, rollup and aggregation, and
    /// survives a proto round trip into exactly what the coordinator asked.
    /// The columnar form addresses a point by its window index: a bitmap of
    /// the windows that produced a value, and the values in that order. What
    /// comes out of the decoder is what went into the encoder, over gaps and
    /// across byte boundaries of the bitmap.
    #[test]
    fn test_columns_round_trip() {
        let window_ends: Vec<i64> = (0..=20).map(|i| i * 30_000).collect();
        // Present at windows 0, 7, 8 (a byte boundary), 15 and 20 only.
        let points: Vec<(i64, i64, f64)> = [0usize, 7, 8, 15, 20]
            .iter()
            .map(|&i| {
                (
                    window_ends[i],
                    window_ends[i] - 1_234 * i as i64,
                    i as f64 * 0.5,
                )
            })
            .collect();

        let (presence, values, sample_lag) =
            encode_columns(&window_ends, points.iter().copied(), true);
        assert_eq!(presence, vec![0b1000_0001, 0b1000_0001, 0b0001_0000]);
        assert_eq!(values.len(), 5);
        assert_eq!(
            sample_lag,
            vec![0, 1_234 * 7, 1_234 * 8, 1_234 * 15, 1_234 * 20]
        );
        let series = GridSeries {
            labels: Vec::new(),
            presence,
            values,
            sample_lag,
        };
        let decoded: Vec<(i64, i64, f64)> =
            decode_columns(&window_ends, &series).unwrap().collect();
        assert_eq!(decoded, points);

        // Without the lag column a pick is stamped with its window end.
        let (presence, values, sample_lag) =
            encode_columns(&window_ends, points.iter().copied(), false);
        assert!(sample_lag.is_empty());
        let series = GridSeries {
            labels: Vec::new(),
            presence,
            values,
            sample_lag,
        };
        let decoded: Vec<(i64, i64, f64)> =
            decode_columns(&window_ends, &series).unwrap().collect();
        let stamped: Vec<(i64, i64, f64)> = points.iter().map(|&(s, _, v)| (s, s, v)).collect();
        assert_eq!(decoded, stamped);

        // Nothing present: an empty bitmap, no values.
        let (presence, values, _) = encode_columns(&window_ends, std::iter::empty(), true);
        assert_eq!(presence, vec![0, 0, 0]);
        assert!(values.is_empty());
    }

    /// A stepped request that does not ask for sample timestamps ships no
    /// lag column and stamps each pick with its step, which is what every
    /// consumer but `timestamp()` sees anyway.
    #[test]
    fn test_sample_timestamps_travel_only_on_request() {
        let request = GridRequest {
            sample_timestamps: false,
            ..stepped_grid_request()
        };
        let dense = || {
            series(
                "0",
                &[
                    (5_000, 1.0),
                    (65_000, 2.0),
                    (125_000, 3.0),
                    (185_000, 4.0),
                    (245_000, 5.0),
                    (305_000, 6.0),
                    (365_000, 7.0),
                    (425_000, 8.0),
                    (485_000, 9.0),
                    (545_000, 10.0),
                    (595_000, 11.0),
                    (599_000, 12.0),
                ],
            )
        };
        let resp = response(&request, vec![dense()]);
        assert_eq!(resp.series.len(), 1);
        assert!(resp.series[0].sample_lag.is_empty());

        let mut cmd = command(request.clone());
        cmd.on_response(resp, &node(7000)).unwrap();
        let GridOutcome::Stepped(stepped) = cmd.into_result().unwrap() else {
            panic!("a stepped request answers Stepped");
        };
        assert!(
            stepped[0]
                .points
                .iter()
                .all(|p| p.sample.timestamp == p.step_ts)
        );

        // The values are the ones the single-node stage picks; only the
        // timestamps differ.
        let GridStage::Stepped(local) = request.per_series(&request.window_ends(), vec![dense()])
        else {
            panic!()
        };
        let values = |s: &[SteppedSeries]| -> Vec<(i64, f64)> {
            s[0].points
                .iter()
                .map(|p| (p.step_ts, p.sample.value))
                .collect()
        };
        assert_eq!(values(&stepped), values(&local));

        // Asked for, the lags travel and the picks' own timestamps come back.
        let asking = GridRequest {
            sample_timestamps: true,
            ..request
        };
        let mut cmd = command(asking.clone());
        cmd.on_response(response(&asking, vec![dense()]), &node(7000))
            .unwrap();
        let GridOutcome::Stepped(stepped) = cmd.into_result().unwrap() else {
            panic!()
        };
        assert!(
            stepped[0]
                .points
                .iter()
                .any(|p| p.sample.timestamp != p.step_ts)
        );
    }

    /// Columns that disagree with each other or with the grid are a corrupt
    /// response, not a short one.
    #[test]
    fn test_malformed_columns_are_rejected() {
        let request = stepped_grid_request();
        let windows = request.window_ends().len();
        let malformed = [
            // A bit past the last window.
            GridSeries {
                presence: {
                    let mut p = vec![0u8; windows.div_ceil(8)];
                    p[windows / 8] |= 1 << (windows % 8);
                    p
                },
                values: vec![1.0],
                ..Default::default()
            },
            // Two windows present, one value.
            GridSeries {
                presence: vec![0b11],
                values: vec![1.0],
                ..Default::default()
            },
            // A lag column shorter than the values.
            GridSeries {
                presence: vec![0b11],
                values: vec![1.0, 2.0],
                sample_lag: vec![0],
                ..Default::default()
            },
        ];
        for series in malformed {
            let mut cmd = command(request.clone());
            let err = cmd
                .on_response(
                    GridQueryResponse {
                        series: vec![series],
                        ..Default::default()
                    },
                    &node(7000),
                )
                .unwrap_err();
            assert!(err.to_string().contains("malformed grid series"), "{err}");
        }
    }

    #[test]
    fn test_request_round_trip() {
        use crate::fanout::serialization::{Deserialized, Serialized};

        let request = GridRequest {
            // `@`/`offset` already resolved: the shard is told the window end.
            range_end_ms: EVAL_TS - 3_600_000,
            query_start: EVAL_TS - 3_600_000,
            query_end: EVAL_TS - 3_600_000,
            ..fused(
                rollup_request(RollupKind::QuantileOverTime),
                AggregationKind::Sum,
                &["job", "env"],
            )
        };
        let cmd = GridFanoutCommand::new(
            Matchers::empty(),
            request,
            1_000,
            11_000,
            Duration::from_secs(1),
        );

        let wire = cmd.generate_request();
        let mut buf = Vec::new();
        wire.serialize(&mut buf);
        let decoded = GridQuery::deserialize(&buf).unwrap();
        assert_eq!(decoded, wire);
        assert_eq!(decoded.max_series, 1_000);
        assert_eq!(decoded.max_points_per_series, 11_000);
        assert!(decoded.selector.is_some());

        let round_tripped = decode_request(&decoded).expect("well-formed request");
        assert_eq!(round_tripped.window_ends(), vec![EVAL_TS - 3_600_000]);
        assert_eq!(round_tripped.lookback_delta_ms, LOOKBACK_MS);
        let rollup = round_tripped.rollup.expect("a rollup");
        assert_eq!(rollup.kind, RollupKind::QuantileOverTime);
        assert_eq!(rollup.range_ms, RANGE_MS);
        assert_eq!(rollup.param, Some(0.9));
        let aggregation = round_tripped.aggregation.expect("fused");
        assert_eq!(aggregation.kind, AggregationKind::Sum);
        assert_eq!(
            aggregation.modifier,
            Some(LabelModifier::Include(promql_parser::label::Labels::new(
                vec!["job", "env"]
            )))
        );

        // A stepped request carries neither part.
        let wire = command(stepped_grid_request()).generate_request();
        assert!(wire.rollup.is_none() && wire.aggregation.is_none());
        let stepped = decode_request(&wire).unwrap();
        assert!(stepped.rollup.is_none() && stepped.aggregation.is_none());
        assert_eq!(stepped.window_ends().len(), 11);
    }

    /// A rollup this node does not know, or an aggregation it cannot fuse, is a
    /// corrupt request — every node runs the same build — and is refused, never
    /// mistaken for the zero variant or quietly degraded.
    #[test]
    fn test_decode_rejects_malformed_requests() {
        let mut req = command(rollup_request(RollupKind::SumOverTime)).generate_request();
        assert!(decode_request(&req).is_ok());

        for kind in [0, 99, -1] {
            req.rollup.as_mut().unwrap().kind = kind;
            assert!(decode_request(&req).is_err(), "rollup kind {kind}");
        }

        let mut req = command(fused(
            stepped_grid_request(),
            AggregationKind::Sum,
            &["job"],
        ))
        .generate_request();
        assert!(decode_request(&req).is_ok());
        req.aggregation.as_mut().unwrap().kind = ProtoAggregationKind::Unspecified as i32;
        assert!(
            decode_request(&req).is_err(),
            "an unspecified operator cannot be fused"
        );
        req.aggregation.as_mut().unwrap().kind = 99;
        assert!(decode_request(&req).is_err());

        // A selecting operator decodes with its parameter.
        let req = command(fused_with(
            stepped_grid_request(),
            AggregationKind::Topk,
            &["job"],
            Some(AggregationParam::Scalar(3.0)),
        ))
        .generate_request();
        let decoded = decode_request(&req).unwrap();
        assert!(matches!(
            decoded.aggregation.as_ref().and_then(|a| a.param.as_ref()),
            Some(AggregationParam::Scalar(k)) if *k == 3.0
        ));
        let req = command(fused_with(
            stepped_grid_request(),
            AggregationKind::CountValues,
            &[],
            Some(AggregationParam::Label("v".into())),
        ))
        .generate_request();
        let decoded = decode_request(&req).unwrap();
        assert!(matches!(
            decoded.aggregation.as_ref().and_then(|a| a.param.as_ref()),
            Some(AggregationParam::Label(l)) if l == "v"
        ));
    }

    /// A response whose lists contradict the request is rejected instead of
    /// being counted twice: partials for an unfused request, or per-series
    /// values for a fused one.
    #[test]
    fn test_mismatched_payload_is_rejected() {
        let mut cmd = command(stepped_grid_request());
        let stray = GridQueryResponse {
            series: Vec::new(),
            partials: vec![GridGroupPartial::default()],
            raw: Vec::new(),
        };
        assert!(cmd.on_response(stray, &node(7000)).is_err());

        let mut cmd = command(fused(
            rollup_grid_request(RollupKind::SumOverTime),
            AggregationKind::Sum,
            &["__name__"],
        ));
        let stray = GridQueryResponse {
            series: vec![GridSeries::default()],
            partials: vec![GridGroupPartial::default()],
            raw: Vec::new(),
        };
        assert!(cmd.on_response(stray, &node(7000)).is_err());

        // Raw alongside either list is legitimate: that is the size rule.
        let mut cmd = command(stepped_grid_request());
        let mixed = GridQueryResponse {
            series: vec![GridSeries::default()],
            partials: Vec::new(),
            raw: vec![range_sample_to_proto(series("0", &[(EVAL_TS, 1.0)])).unwrap()],
        };
        assert!(cmd.on_response(mixed, &node(7000)).is_ok());

        // A raw series whose chunk does not decode is a corrupt response.
        let mut cmd = command(stepped_grid_request());
        let garbage = GridQueryResponse {
            raw: vec![crate::promql::generated::RangeSample {
                data: Some(crate::promql::generated::SampleData {
                    version: 1,
                    compression: 2,
                    data: vec![0xFF, 0x00, 0x13],
                }),
                ..Default::default()
            }],
            ..Default::default()
        };
        let err = cmd.on_response(garbage, &node(7000)).unwrap_err();
        assert!(err.to_string().contains("undecodable raw series"), "{err}");
    }

    /// The geometry a request describes, which both sides derive through the
    /// one [`grid_window_ends`] — so what is pinned here is the shape itself
    /// rather than the two sides agreeing about it.
    #[test]
    fn test_window_ends_and_fetch_bounds() {
        // Instant: one window, at the resolved end; reaching back one full
        // range for a rollup, one lookback for a stepped selection, exclusive
        // of the lower bound.
        let instant = rollup_request(RollupKind::SumOverTime);
        assert_eq!(instant.window_ends(), vec![EVAL_TS]);
        assert_eq!(
            instant.fetch_bounds(),
            Some((EVAL_TS - RANGE_MS + 1, EVAL_TS))
        );
        let stepped = stepped_request();
        assert_eq!(
            stepped.fetch_bounds(),
            Some((EVAL_TS - LOOKBACK_MS + 1, EVAL_TS))
        );

        // Grid: every step, and one span covering every window rather than one
        // fetch per step.
        let grid = GridRequest {
            step_ms: 30_000,
            query_start: 60_000,
            query_end: 180_000,
            ..rollup_request(RollupKind::SumOverTime)
        };
        assert_eq!(
            grid.window_ends(),
            vec![60_000, 90_000, 120_000, 150_000, 180_000]
        );
        assert_eq!(grid.fetch_bounds(), Some((60_000 - RANGE_MS + 1, 180_000)));

        // A grid with no steps in it describes nothing to read.
        let empty = GridRequest {
            step_ms: 30_000,
            query_start: 180_000,
            query_end: 60_000,
            ..stepped_request()
        };
        assert!(empty.window_ends().is_empty());
        assert_eq!(empty.fetch_bounds(), None);
    }
}
