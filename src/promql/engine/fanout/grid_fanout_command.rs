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
use crate::promql::deadline::{Deadline, SERIES_WORK};
use crate::promql::engine::fanout::query_utils::{local_grid_windows, shard_read};
use crate::promql::engine::fanout::shard_error;
use crate::promql::engine::fanout::type_conversions::{
    aggregation_param_from_wire, aggregation_param_to_wire, decode_peer_partial,
    decode_pushdown_kind, range_sample_from_proto, range_sample_to_proto, wrong_payload,
};
use crate::promql::engine::query_reader::{
    GridAggregation, GridOutcome, GridRequest, GridRollup, GridSeries, GridSink, SteppedPoint,
    SteppedSeries, grid_window_ends,
};
use crate::promql::engine::sample_budget::SampleBudget;
use crate::promql::functions::RollupKind;
use crate::promql::generated::{
    AggregationKind as ProtoAggregationKind, GridAggregation as ProtoGridAggregation,
    GridGroupPartial, GridQuery, GridQueryResponse, GridRollup as ProtoGridRollup,
    GridSeries as ProtoGridSeries, RollupKind as ProtoRollupKind,
    SeriesSelector as ProtoSeriesSelector,
};
use crate::promql::hashers::FingerprintHashSet;
use crate::promql::model::RangeSample;
use crate::promql::time::{MAX_GRID_STEPS, grid_step_count};
use crate::promql::{EvalLabels, QueryError, QueryResult};
use promql_parser::label::Matchers;
use promql_parser::parser::LabelModifier;
use std::time::Duration;
use valkey_module::{ValkeyError, ValkeyResult};

mod columns;
#[cfg(test)]
mod tests;
mod transport;

use columns::{columnar_series, decode_columns, encode_columns};
use transport::ships_raw;

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
    /// What the shards staged, in the request's shape: per-series entries, a
    /// reduction's per-`(group, step)` partials, or a selection's per-step
    /// candidates. Series are shard-local, so entries accumulate by
    /// concatenation; partials and candidates merge.
    sink: GridSink,
    /// Raw spans a shard chose to ship instead, run through the per-series
    /// stage by [`Self::into_result`].
    raw: Vec<RangeSample<EvalLabels>>,
    /// `ts-promql-max-samples-per-query`, charged for what this command holds
    /// as it grows: every shard bounds its own output by the same limit, so
    /// without it the shards together could leave the coordinator holding
    /// that many times it before the query is refused.
    budget: SampleBudget,
    /// A peer refused the operation as unknown: it runs an older build that
    /// has no grid push-down (a rolling upgrade). The caller then evaluates
    /// without it, over the selector operations every build has.
    unsupported_peer: bool,
    /// The query's: checked as the shards' output is merged, and as what
    /// arrived raw is folded and finalized ([`Self::into_result`]).
    deadline: Deadline,
    /// Why merging stopped, when `deadline` passed during it: every later
    /// response is dropped unread, and [`Self::into_result`] reports this, in
    /// its own words, rather than a fan-out failure.
    stopped: Option<QueryError>,
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
            0,
            get_cluster_command_timeout(),
            Deadline::default(),
        )
    }
}

impl GridFanoutCommand {
    pub fn new(
        matchers: Matchers,
        request: GridRequest,
        max_series: u64,
        max_points_per_series: u64,
        max_samples: usize,
        timeout: Duration,
        deadline: Deadline,
    ) -> Self {
        let window_ends = request.window_ends();
        let sink = GridSink::for_request(&request);
        Self {
            matchers,
            request,
            window_ends,
            max_series,
            max_points_per_series,
            timeout,
            sink,
            raw: Vec::new(),
            budget: SampleBudget::new(max_samples),
            unsupported_peer: false,
            deadline,
            stopped: None,
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
    ///
    /// Shards ship a series raw exactly when it holds fewer samples than there
    /// are steps ([`ships_raw`]), so the raw spans are the ones whose output
    /// can dwarf them: one sample in a long window answers at every step. They
    /// go through the stage a chunk at a time
    /// ([`GridRequest::per_series_chunked`]), `deadline` checked before each.
    /// What the sink holds — an unfused request's points, a fused one's
    /// `(group, step)` states or candidates, which outlive the chunks folded
    /// into them — is counted against `budget` as it grows, as the shards'
    /// output was when it arrived ([`FanoutCommand::on_response`]): the
    /// evaluator charges the result to the query's budget once it has it, so a
    /// request over the limit on its own is refused at the chunk that crosses
    /// it rather than after the whole grid is built. The raw spans were charged
    /// as they arrived and each chunk's are dropped once it has run, so a chunk
    /// is charged only for what it holds past them.
    pub fn into_result(self) -> QueryResult<GridOutcome> {
        let Self {
            request,
            window_ends,
            raw,
            mut sink,
            budget,
            deadline,
            stopped,
            ..
        } = self;
        if let Some(err) = stopped {
            return Err(err);
        }
        let deadline = &deadline;
        request.per_series_chunked(&window_ends, raw, deadline, |chunk, read| {
            let added = sink.take(&request, chunk, deadline)?;
            budget.charge(added.saturating_sub(read))
        })?;
        sink.finish(deadline).map_err(QueryError::from)
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

        // Locks per batch of matched series; each batch is decoded and run
        // through the per-series stage with the lock released, before the next
        // batch is read.
        // Held through `finish`: the grid keeps what it made of the windows until then.
        let read = shard_read(ctx)?;
        let mut grid = ShardGrid::new(&request, &window_ends, &read.budget, read.deadline);
        local_grid_windows(
            ctx,
            series_selector,
            &window_ends,
            request.backward_ms(),
            req.max_series,
            req.max_points_per_series,
            &read,
            |windows| grid.add(windows),
        )?;
        grid.finish()
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
            time_budget_ms: super::time_budget_to_wire(self.timeout),
        }
    }

    fn time_budget(req: &GridQuery) -> Option<Duration> {
        super::time_budget_from_wire(req.time_budget_ms)
    }

    fn on_response(&mut self, mut resp: Self::Response, target: &NodeInfo) -> FanoutCommandResult {
        if self.stopped.is_some() {
            return Ok(());
        }
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
        let added = if let GridSink::Partials(partials) = &mut self.sink {
            if !resp.series.is_empty() {
                return Err(wrong_payload(
                    target,
                    resp.series.len(),
                    "series",
                    "a grid query fused with a reduction",
                    "partial states",
                ));
            }
            let mut added = 0;
            for mut partial in resp.partials {
                let labels = resolver.resolve(&mut partial).map_err(malformed)?;
                let state = decode_peer_partial(
                    partial.state,
                    target,
                    "a grid query fused with a reduction",
                )?;
                added += partials.merge(partial.step_ts, labels, state);
                if let Err(err) = self.deadline.checkpoint(SERIES_WORK) {
                    self.stopped = Some(err);
                    return Ok(());
                }
            }
            added
        } else {
            if !resp.partials.is_empty() {
                return Err(wrong_payload(
                    target,
                    resp.partials.len(),
                    "partial states",
                    "a grid query not fused with a reduction",
                    "series",
                ));
            }
            // Decoded into the stage's own output shape whatever the shard
            // ran over it: a selection's candidates are its per-step picks,
            // which the sink reads from either form alike.
            let mut output = self.request.empty_output();
            for mut series in resp.series {
                let labels = resolver.resolve(&mut series).map_err(malformed)?;
                let points = decode_columns(&self.window_ends, &series).map_err(|why| {
                    FanoutError::custom(format!(
                        "TSDB: peer {} returned a malformed grid series: {why}",
                        target.socket_address,
                    ))
                })?;
                match &mut output {
                    GridSeries::Rolled(rolled) => rolled.push(RangeSample {
                        labels,
                        samples: points
                            .map(|(step_ts, _, value)| Sample::new(step_ts, value))
                            .collect(),
                    }),
                    GridSeries::Stepped(stepped) => stepped.push(SteppedSeries {
                        labels,
                        points: points
                            .map(|(step_ts, sample_ts, value)| SteppedPoint {
                                step_ts,
                                sample: Sample::new(sample_ts, value),
                            })
                            .collect(),
                    }),
                }
            }
            match self.sink.merge(&self.request, output, &self.deadline) {
                Ok(added) => added,
                Err(err) => {
                    self.stopped = Some(err.into());
                    return Ok(());
                }
            }
        };

        let mut raw_samples = 0;
        let raw = resp
            .raw
            .into_iter()
            .map(|raw| {
                let series = range_sample_from_proto(raw, &mut resolver).map_err(|why| {
                    FanoutError::custom(format!(
                        "TSDB: peer {} returned an undecodable raw series: {why}",
                        target.socket_address,
                    ))
                })?;
                raw_samples += series.samples.len();
                Ok(series)
            })
            .collect::<Result<Vec<_>, FanoutError>>()?;

        // Charged as each response arrives rather than once every shard has
        // answered: past the limit, the rest are dropped unread.
        if let Err(err) = self.budget.charge(added + raw_samples) {
            self.stopped = Some(err);
            return Ok(());
        }
        self.raw.extend(raw);
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

/// One shard's response over all of its windows at once: the per-series stage
/// over every series that [`transport::transport_plan`] keeps, the rest shipped
/// raw, and — for a fused request — the staged series folded into
/// per-`(group, step)` partials. The shard itself builds it a batch at a time
/// with [`ShardGrid`]; this is that, in one batch.
#[cfg(test)]
fn shard_response(
    request: &GridRequest,
    window_ends: &[i64],
    windows: Vec<RangeSample<EvalLabels>>,
) -> ValkeyResult<GridQueryResponse> {
    let budget = SampleBudget::default();
    let mut grid = ShardGrid::new(request, window_ends, &budget, Deadline::default());
    grid.add(windows)?;
    grid.finish()
}

/// A shard's grid response, built one batch of windows at a time so that the
/// shard never holds more than a batch of decoded samples.
///
/// Each series goes through the per-series stage as its batch arrives, which
/// shrinks it to one point per window end, unless [`ships_raw`] finds its span
/// the smaller form — and such a span is, by that test, smaller than its stage
/// output would be. The raw spans wait for [`ShardGrid::finish`], because a
/// fused reduction decides per *group* whether they travel raw or join their
/// group's partials, and a group is only complete once every batch is in.
///
/// A promoted span is sparse, so its stage output can dwarf it: one sample in
/// a long window answers at every step, and promotion is decided on wire
/// bytes, which a group of many sparse members wins however much they expand.
/// That output is never kept: under a fused reduction every staged series is
/// folded into its group's partials as it is staged ([`GridSink::Partials`]),
/// promoted spans a chunk at a time ([`GridRequest::per_series_chunked`]). What
/// outlives a chunk is the partials, which are the response itself, so nothing
/// here needs charging beyond the raw samples the read charged.
///
/// Promoted spans are folded after the group's other members rather than in
/// match order. Under a fused reduction that can move a sum in its last bits,
/// as merging the shards' partials in arrival order already does.
pub(super) struct ShardGrid<'a> {
    request: &'a GridRequest,
    window_ends: &'a [i64],
    raw: Vec<RangeSample<EvalLabels>>,
    /// What the shard keeps of the series it stages, in the request's shape.
    /// Per-series output lands here only from series [`ships_raw`] keeps, so
    /// it is never larger than their spans; a fused aggregation folds it as
    /// it arrives and keeps only its groups or picks.
    staged: GridSink,
    staged_groups: FingerprintHashSet,
    /// The read's budget, charged for what `staged` holds beyond the spans
    /// folded into it.
    budget: &'a SampleBudget,
    /// When the requester stops waiting ([`super::shard_deadline`]): checked by a fused
    /// selection as it runs, and between the chunks of promoted spans.
    deadline: Deadline,
}

impl<'a> ShardGrid<'a> {
    pub(super) fn new(
        request: &'a GridRequest,
        window_ends: &'a [i64],
        budget: &'a SampleBudget,
        deadline: Deadline,
    ) -> Self {
        Self {
            request,
            window_ends,
            raw: Vec::new(),
            staged: GridSink::for_request(request),
            staged_groups: FingerprintHashSet::default(),
            budget,
            deadline,
        }
    }

    /// Take one batch of non-empty windows.
    ///
    /// A series is staged only when its span holds at least as many samples as
    /// there are steps ([`ships_raw`]), so its stage output is never the
    /// larger, and a sparse series that would expand waits for
    /// [`Self::finish`], which ships it raw or folds it. What `staged` holds is
    /// still charged past the spans staged into it ([`Self::stage`]): a fused
    /// selection's candidates need not be fewer than its input.
    ///
    /// Fails only for a fused selection: it refuses its parameter, or the
    /// requester gives up while it runs.
    pub(super) fn add(&mut self, windows: Vec<RangeSample<EvalLabels>>) -> ValkeyResult<()> {
        let (raw, staged): (Vec<_>, Vec<_>) = windows
            .into_iter()
            .partition(|series| ships_raw(self.window_ends, series));
        self.raw.extend(raw);
        self.staged_groups.extend(transport::staged_groups(
            self.request,
            staged.iter().map(|series| (series, &false)),
        ));
        if !staged.is_empty() {
            let read = staged.iter().map(|series| series.samples.len()).sum();
            let output = self.request.per_series(self.window_ends, staged);
            Self::stage(
                &mut self.staged,
                self.request,
                output,
                read,
                self.budget,
                &self.deadline,
            )
            .map_err(shard_error)?;
        }
        Ok(())
    }

    /// Take one batch of stage output, charging `budget` for what the sink now
    /// holds beyond the `read` raw samples the batch was built from, which the
    /// read already charged.
    fn stage(
        staged: &mut GridSink,
        request: &GridRequest,
        output: GridSeries,
        read: usize,
        budget: &SampleBudget,
        deadline: &Deadline,
    ) -> QueryResult<()> {
        let added = staged.take(request, output, deadline)?;
        budget.charge(added.saturating_sub(read))
    }

    /// The response, once every batch is in: the raw spans that stay raw, and
    /// the promoted ones folded into their groups' partials.
    ///
    /// The promoted spans are staged a chunk at a time, the requester's
    /// deadline checked before each, as between the read's batches, so a
    /// large promotion neither holds its whole expansion nor runs on for
    /// nobody.
    pub(super) fn finish(mut self) -> ValkeyResult<GridQueryResponse> {
        let request = self.request;
        let window_ends = self.window_ends;
        let candidates = std::mem::take(&mut self.raw);
        let promoted = {
            let candidates: Vec<&RangeSample<EvalLabels>> = candidates.iter().collect();
            transport::promoted_raw(request, window_ends, &candidates, &self.staged_groups)
        };
        let mut raw = Vec::with_capacity(candidates.len());
        let mut folded = Vec::new();
        for (series, promote) in candidates.into_iter().zip(promoted) {
            if promote {
                folded.push(series);
            } else {
                raw.push(series);
            }
        }
        let (staged, budget, deadline) = (&mut self.staged, self.budget, &self.deadline);
        request
            .per_series_chunked(window_ends, folded, deadline, |output, read| {
                Self::stage(staged, request, output, read, budget, deadline)
            })
            .map_err(shard_error)?;
        respond(request, window_ends, raw, self.staged, &self.deadline)
    }
}

/// The wire response from a shard's raw spans and what it staged. A fused
/// selection re-selects over its candidates here, `deadline` checked as it does.
fn respond(
    request: &GridRequest,
    window_ends: &[i64],
    raw: Vec<RangeSample<EvalLabels>>,
    staged: GridSink,
    deadline: &Deadline,
) -> ValkeyResult<GridQueryResponse> {
    // One symbol table for every labelled element of the response.
    let mut symbols = SymbolTableBuilder::default();
    let raw = raw
        .into_iter()
        .map(|series| range_sample_to_proto(series, &mut symbols))
        .collect::<ValkeyResult<Vec<_>>>()?;

    let staged = match staged {
        GridSink::Series(series) => series,
        // A fused reduction: the partials were folded as the series were staged.
        GridSink::Partials(groups) => {
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
        // A fused selecting or counting operator: its picks over everything
        // staged here — k series per step, or one count per value — ship as
        // candidates rather than every series.
        GridSink::Selection(selection) => {
            let selected = selection.finalize(deadline).map_err(shard_error)?;
            return Ok(GridQueryResponse {
                series: columnar_series(window_ends, selected, &mut symbols),
                partials: Vec::new(),
                raw,
                labels: Some(symbols.finish()),
            });
        }
    };

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
