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

use crate::commands::fanout_codec::symbol_table::{EvalLabelResolver, SymbolTableBuilder};
use crate::common::Sample;
use crate::fanout::{
    FanoutCommand, FanoutCommandResult, FanoutContext, FanoutError, NodeInfo,
    get_cluster_command_timeout, log_fanout_failure,
};
use crate::labels::filters::SeriesSelector;
use crate::promql::EvalLabels;
use crate::promql::engine::fanout::query_utils::local_grid_windows;
use crate::promql::engine::fanout::type_conversions::{
    aggregation_param_from_wire, aggregation_param_to_wire, decode_peer_partial,
    decode_pushdown_kind, range_sample_from_proto, range_sample_to_proto, wrong_payload,
};
use crate::promql::engine::query_reader::{
    GridAggregation, GridOutcome, GridRequest, GridRollup, GridSeries, SteppedPoint, SteppedSeries,
    grid_window_ends,
};
use crate::promql::exec::aggregations::PushdownStrategy;
use crate::promql::exec::partial_aggregation::{SteppedPartialGroups, SteppedSelection};
use crate::promql::functions::RollupKind;
use crate::promql::generated::{
    AggregationKind as ProtoAggregationKind, GridAggregation as ProtoGridAggregation,
    GridGroupPartial, GridQuery, GridQueryResponse, GridRollup as ProtoGridRollup,
    GridSeries as ProtoGridSeries, RollupKind as ProtoRollupKind,
    SeriesSelector as ProtoSeriesSelector,
};
use crate::promql::model::RangeSample;
use crate::promql::time::{MAX_GRID_STEPS, grid_step_count};
use promql_parser::label::Matchers;
use promql_parser::parser::LabelModifier;
use std::time::Duration;
use valkey_module::{ValkeyError, ValkeyResult};

mod columns;
#[cfg(test)]
mod tests;
mod transport;

use columns::{columnar_series, decode_columns, encode_columns};
use transport::transport_plan;

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
    /// A peer refused the operation as unknown: it runs an older build that
    /// has no grid push-down (a rolling upgrade). The caller then evaluates
    /// without it, over the selector operations every build has.
    unsupported_peer: bool,
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
            unsupported_peer: false,
        }
    }

    /// Whether a peer rejected the grid operation as unknown (see
    /// `unsupported_peer`). Checked when the command fails, before its error is
    /// reported: the failure is then a reason to evaluate locally, not an error.
    pub fn peer_unsupported(&self) -> bool {
        self.unsupported_peer
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
                GridSeries::Stepped(series) => stepped.extend(series),
                GridSeries::Rolled(series) => rolled.extend(series),
            }
        }

        if let Some(mut selection) = self.selection.take() {
            // Fused with a selecting or counting operator: the shards'
            // outputs are candidates; whatever arrived raw or un-selected is
            // run through the operator here and joins them.
            let unselected = if self.request.rollup.is_some() {
                GridSeries::Rolled(rolled)
            } else {
                GridSeries::Stepped(stepped)
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
                    GridSeries::Rolled(rolled)
                } else {
                    GridSeries::Stepped(stepped)
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

    fn get_local_response(ctx: &FanoutContext, req: GridQuery) -> ValkeyResult<GridQueryResponse> {
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

        // Locks per batch of matched series; decodes with the lock released.
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
                let (scalar_param, label_param) = aggregation_param_to_wire(agg.param.as_ref());
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

    fn on_response(&mut self, mut resp: Self::Response, target: &NodeInfo) -> FanoutCommandResult {
        // Every element's labels are refs into the response's symbol table,
        // resolved to interned labels: each distinct pair once per response.
        let table = resp.labels.take().unwrap_or_default();
        let mut resolver = EvalLabelResolver::new(&table);
        let malformed = |why: ValkeyError| {
            FanoutError::custom(format!(
                "TSDB: peer {} returned malformed labels: {why}",
                target.socket_address,
            ))
        };

        // Corrupt-peer defenses. A request fused with a reduction is answered
        // in partials; every other request in series — a fused selection's
        // series are the shard's per-step picks or counts. A response carrying
        // the wrong list would be folded in twice, or grouped when the query
        // asked for series.
        match self.partials.as_mut() {
            Some(partials) => {
                if !resp.series.is_empty() {
                    return Err(wrong_payload(
                        target,
                        resp.series.len(),
                        "series",
                        "a grid query fused with a reduction",
                        "partial states",
                    ));
                }
                for mut partial in resp.partials {
                    let labels = resolver.resolve(&mut partial).map_err(malformed)?;
                    let state = decode_peer_partial(
                        partial.state,
                        target,
                        "a grid query fused with a reduction",
                    )?;
                    partials.merge(partial.step_ts, labels, state);
                }
            }
            None => {
                if !resp.partials.is_empty() {
                    return Err(wrong_payload(
                        target,
                        resp.partials.len(),
                        "partial states",
                        "a grid query not fused with a reduction",
                        "series",
                    ));
                }
                let mut candidates = Vec::new();
                for mut series in resp.series {
                    let labels = resolver.resolve(&mut series).map_err(malformed)?;
                    let points: Vec<(i64, i64, f64)> = decode_columns(&self.window_ends, &series)
                        .map_err(|why| {
                            FanoutError::custom(format!(
                                "TSDB: peer {} returned a malformed grid series: {why}",
                                target.socket_address,
                            ))
                        })?
                        .collect();
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
            let series = range_sample_from_proto(raw, &mut resolver).map_err(|why| {
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
        // A peer that does not know this operation (rolling upgrade) rejects
        // the envelope rather than answering it. Latched, as the aggregation
        // push-down does, so the caller can fall back to local evaluation.
        self.unsupported_peer |= error.is_unsupported_operation();
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
            let (kind, _) = decode_pushdown_kind(agg.kind).ok_or_else(|| {
                ValkeyError::String(format!(
                    "TSDB: aggregation kind {} cannot be fused onto a grid push-down request",
                    agg.kind
                ))
            })?;
            let param = aggregation_param_from_wire(agg.scalar_param, agg.label_param.clone());
            Ok::<_, ValkeyError>(GridAggregation {
                kind,
                modifier: agg.grouping.clone().map(LabelModifier::from),
                param,
            })
        })
        .transpose()?;
    // The shard walks this grid and materializes its window ends before any
    // series or point limit applies, so the peer's geometry is bounded here.
    let steps = grid_step_count(req.query_start, req.query_end, req.step_ms);
    if steps > MAX_GRID_STEPS {
        return Err(ValkeyError::String(format!(
            "TSDB: grid push-down request has {steps} steps; cannot exceed {MAX_GRID_STEPS}"
        )));
    }
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

/// One shard's response: the per-series stage over every series that
/// [`transport_plan`] keeps, the rest shipped raw, and — for a fused request
/// — the staged series folded into per-`(group, step)` partials.
fn shard_response(
    request: &GridRequest,
    window_ends: &[i64],
    windows: Vec<RangeSample<EvalLabels>>,
) -> ValkeyResult<GridQueryResponse> {
    let plan = transport_plan(request, window_ends, &windows);
    let mut raw = Vec::new();
    let mut staged = Vec::with_capacity(windows.len());
    for (series, ships_raw) in windows.into_iter().zip(plan) {
        if ships_raw {
            raw.push(series);
        } else {
            staged.push(series);
        }
    }
    let staged = request.per_series(window_ends, staged);

    // One symbol table for every labelled element of the response.
    let mut symbols = SymbolTableBuilder::default();
    let raw = raw
        .into_iter()
        .map(|series| range_sample_to_proto(series, &mut symbols))
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
                series: columnar_series(window_ends, selected, &mut symbols),
                partials: Vec::new(),
                raw,
                labels: Some(symbols.finish()),
            });
    }

    if let Some(aggregation) = request.aggregation.as_ref() {
        let mut groups = SteppedPartialGroups::new(aggregation.kind);
        groups.accumulate(aggregation.modifier.as_ref(), staged.into_step_values());
        let partials = groups
            .into_partials()
            .map(|(step_ts, labels, state)| {
                let (label_name_refs, label_value_refs) = symbols.intern_eval(&labels);
                GridGroupPartial {
                    labels: Vec::new(),
                    step_ts,
                    state: Some(state.into()),
                    label_name_refs,
                    label_value_refs,
                }
            })
            .collect();
        return Ok(GridQueryResponse {
            series: Vec::new(),
            partials,
            raw,
            labels: Some(symbols.finish()),
        });
    }

    let series = match staged {
        GridSeries::Stepped(series) => series
            .into_iter()
            .map(|s| {
                let (presence, values, sample_lag) = encode_columns(
                    window_ends,
                    s.points
                        .iter()
                        .map(|p| (p.step_ts, p.sample.timestamp, p.sample.value)),
                    request.sample_timestamps,
                );
                let (label_name_refs, label_value_refs) = symbols.intern_eval(&s.labels);
                ProtoGridSeries {
                    labels: Vec::new(),
                    presence,
                    values,
                    sample_lag,
                    label_name_refs,
                    label_value_refs,
                }
            })
            .collect(),
        GridSeries::Rolled(series) => columnar_series(window_ends, series, &mut symbols),
    };

    Ok(GridQueryResponse {
        series,
        partials: Vec::new(),
        raw,
        labels: Some(symbols.finish()),
    })
}
