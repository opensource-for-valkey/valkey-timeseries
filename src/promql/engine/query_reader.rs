use crate::common::threads::IntoParRayon;
use crate::common::{Sample, Timestamp};
use crate::promql::EvalLabels;
use crate::promql::deadline::{Deadline, SERIES_WORK};
use crate::promql::engine::label_profile::{LabelProfile, ProfileWindow};
use crate::promql::exec::aggregations::{AggregationKind, PushdownStrategy};
use crate::promql::exec::partial_aggregation::{SteppedPartialGroups, SteppedSelection};
use crate::promql::exec::pipeline::for_each_step_sample;
use crate::promql::functions::RollupKind;
use crate::promql::{
    EvalResult, ExprResult, PromqlResult, QueryError, QueryOptions,
    model::{InstantSample, RangeSample},
};
use orx_parallel::Par;
use promql_parser::parser::{LabelModifier, VectorSelector};
use std::sync::Arc;

/// The parameter of an aggregation operator, already evaluated to a literal:
/// `topk(5, …)`'s K, `count_values("le", …)`'s destination label.
#[derive(Debug, Clone, PartialEq)]
pub enum AggregationParam {
    Scalar(f64),
    Label(String),
}

impl AggregationParam {
    /// As an evaluator value, for handing to
    /// [`crate::promql::exec::aggregations::apply_aggregation`].
    pub(in crate::promql) fn to_expr_result(&self) -> ExprResult {
        match self {
            AggregationParam::Scalar(value) => ExprResult::Scalar(*value),
            AggregationParam::Label(label) => ExprResult::String(label.clone()),
        }
    }
}

/// An aggregation to evaluate over an instant vector: the operator, its
/// `by (…)` / `without (…)` modifier, and its parameter.
///
/// Paired with the instant-vector parameters (selector + timestamp) of
/// [`QueryReader::query_aggregation`], this is the whole of a
/// `sum by (job) (metric)` — which is what makes it something a data source can
/// evaluate on its own, close to the data.
#[derive(Debug, Clone)]
pub struct AggregationRequest {
    pub kind: AggregationKind,
    pub modifier: Option<LabelModifier>,
    pub param: Option<AggregationParam>,
    /// Timestamp the aggregated output is stamped with: the query's evaluation
    /// timestamp, which differs from the timestamp the input is selected at when
    /// the selector carries an `@` or `offset` modifier.
    pub eval_timestamp: i64,
}

/// What a data source made of an [`AggregationRequest`]. Every variant tells the
/// caller what it still has to do.
pub enum AggregationOutcome {
    /// The source evaluated the aggregation: this is the final result vector.
    Aggregated(Vec<InstantSample<EvalLabels>>),
    /// The source returned the raw instant vector instead of aggregating it
    /// (nothing to push down to, e.g. a single node): the caller aggregates.
    Raw(Vec<InstantSample<EvalLabels>>),
    /// The source cannot evaluate pushed-down aggregations: the caller should
    /// select the instant vector itself and aggregate that.
    Unsupported,
}

/// The range-vector function half of a [`GridRequest`]: `rate(m[5m])`'s `rate`
/// and `[5m]`. Its presence is what makes a grid request a rollup rather than a
/// stepped instant selection.
#[derive(Debug, Clone, PartialEq)]
pub struct GridRollup {
    pub kind: RollupKind,
    /// Window width: the `[5m]`. Each window is `(end - range_ms, end]`.
    pub range_ms: i64,
    /// Numeric function parameter, e.g. `quantile_over_time`'s phi.
    pub param: Option<f64>,
}

/// An outer aggregation fused onto a grid request: the `sum by (job)` of
/// `sum by (job) (rate(m[5m]))` or of `sum by (job) (m)`.
///
/// Only the reducing operators appear here. The selecting ones (`topk` and
/// friends) need the individual samples to choose among, so fusing them would
/// not reduce what crosses the wire — see
/// [`crate::promql::exec::aggregations::PushdownStrategy`].
#[derive(Debug, Clone)]
pub struct GridAggregation {
    pub kind: AggregationKind,
    pub modifier: Option<LabelModifier>,
    /// The operator's parameter where it takes one: `k` for the selecting
    /// operators, the destination label for `count_values`.
    pub param: Option<AggregationParam>,
}

impl GridAggregation {
    /// How the source folds this operator over a grid; see
    /// [`AggregationKind::pushdown_strategy`]. Never `None` for an
    /// aggregation that reached a request — the caller checks first.
    pub(in crate::promql) fn strategy(&self) -> PushdownStrategy {
        self.kind
            .pushdown_strategy()
            .expect("a fused aggregation has a push-down strategy")
    }
}

/// One read of a selector over a step grid, plus everything needed to
/// reproduce that grid exactly.
///
/// Three shapes share this one request, told apart by the two optional parts:
///
/// * neither — *stepped instant selection*: at every window end, the last
///   sample at or before it and inside the lookback window, which is what a
///   range query's step loop picks for a bare vector selector;
/// * [`Self::rollup`] — the function reduced over each window;
/// * [`Self::aggregation`] — either of the above, folded per `(group, step)`.
///
/// Every time-dependent field is *resolved*: `@` and `offset` are applied by
/// the coordinator before the request is built, so a data source evaluates the
/// window ends it is handed and never re-derives a modifier. Paired with the
/// selector passed alongside it, this is the whole of a
/// `sum_over_time(m[5m] offset 1h)` — or of a plain `m` over the grid — which
/// is what makes it something a source can evaluate close to the data.
#[derive(Debug, Clone)]
pub struct GridRequest {
    /// Step grid of the enclosing query. `step_ms == 0` is a single evaluation
    /// at [`Self::range_end_ms`].
    pub step_ms: i64,
    pub query_start: i64,
    pub query_end: i64,
    /// Window end for the single-evaluation case, `@`/`offset` resolved.
    pub range_end_ms: i64,
    /// The staleness window: a stepped selection picks the last sample in
    /// `(end - lookback, end]`, and the rollups that consult it see the same
    /// value.
    pub lookback_delta_ms: i64,
    pub rollup: Option<GridRollup>,
    /// When set, the source groups its per-series values as well as computing
    /// them, and returns one value per group per step instead of one per series
    /// per step.
    pub aggregation: Option<GridAggregation>,
    /// Whether a stepped selection must report each pick's own timestamp.
    /// Only `timestamp()` observes it — every other consumer stamps a value
    /// with its step — so a source may leave it out (answering the step
    /// itself) unless this is set.
    pub sample_timestamps: bool,
}

/// One step's pick of a stepped instant selection: the step it answers for and
/// the sample chosen for it, whose own timestamp is what `timestamp()` reports.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SteppedPoint {
    pub step_ts: Timestamp,
    pub sample: Sample,
}

/// One series' stepped instant selection: sparse, one point per step that had
/// an eligible sample.
#[derive(Debug, Clone)]
pub struct SteppedSeries {
    pub labels: EvalLabels,
    pub points: Vec<SteppedPoint>,
}

impl SteppedSeries {
    /// The points as `(step, value)` samples — the form the per-`(group, step)`
    /// fold takes, where the sample's own timestamp no longer matters.
    pub(in crate::promql) fn into_step_values(self) -> RangeSample<EvalLabels> {
        RangeSample {
            labels: self.labels,
            samples: self
                .points
                .into_iter()
                .map(|p| Sample::new(p.step_ts, p.sample.value))
                .collect(),
        }
    }
}

/// The per-series stage of a [`GridRequest`] applied to raw spans: what a
/// request produces before any fused aggregation folds it.
pub enum GridSeries {
    Stepped(Vec<SteppedSeries>),
    Rolled(Vec<RangeSample<EvalLabels>>),
}

impl GridSeries {
    /// Append another batch's output of the same request's per-series stage.
    pub(in crate::promql) fn append(&mut self, other: GridSeries) {
        match (self, other) {
            (GridSeries::Stepped(series), GridSeries::Stepped(more)) => series.extend(more),
            (GridSeries::Rolled(series), GridSeries::Rolled(more)) => series.extend(more),
            _ => unreachable!("BUG: one request's per-series stage yields one kind of output"),
        }
    }

    /// How many points the entries hold.
    pub(in crate::promql) fn points(&self) -> usize {
        match self {
            GridSeries::Stepped(series) => series.iter().map(|s| s.points.len()).sum(),
            GridSeries::Rolled(series) => series.iter().map(|s| s.samples.len()).sum(),
        }
    }

    /// Every entry as sparse `(step, value)` samples, whichever stage made it.
    pub(in crate::promql) fn into_step_values(self) -> Vec<RangeSample<EvalLabels>> {
        match self {
            GridSeries::Stepped(series) => series
                .into_iter()
                .map(SteppedSeries::into_step_values)
                .collect(),
            GridSeries::Rolled(series) => series,
        }
    }
}

impl From<GridSeries> for GridOutcome {
    fn from(series: GridSeries) -> Self {
        match series {
            GridSeries::Stepped(series) => GridOutcome::Stepped(series),
            GridSeries::Rolled(series) => GridOutcome::Rolled(series),
        }
    }
}

impl GridRequest {
    /// The window ends to evaluate at, in ascending order.
    pub(in crate::promql) fn window_ends(&self) -> Vec<i64> {
        self.window_ends_iter().collect()
    }

    /// [`Self::window_ends`] without collecting them, for a caller that only
    /// walks or compares them once.
    pub(in crate::promql) fn window_ends_iter(&self) -> impl Iterator<Item = Timestamp> + use<> {
        grid_window_ends_iter(
            self.step_ms,
            self.query_start,
            self.query_end,
            self.range_end_ms,
        )
    }

    /// How far back of a window end the raw samples it needs reach: the window
    /// width for a rollup, the lookback for a stepped selection.
    pub(in crate::promql) fn backward_ms(&self) -> i64 {
        match &self.rollup {
            Some(rollup) => rollup.range_ms,
            None => self.lookback_delta_ms,
        }
    }

    /// The span of raw samples this request's windows cover, or `None` when it
    /// describes no windows at all.
    pub(in crate::promql) fn fetch_bounds(&self) -> Option<(Timestamp, Timestamp)> {
        // Only the first and last ends matter: walk them, never collect them.
        let mut ends = self.window_ends_iter();
        let first = ends.next()?;
        let last = ends.last().unwrap_or(first);
        Some(fetch_span(first, last, self.backward_ms()))
    }

    /// Apply the per-series stage — stepped selection, or the rollup — to raw
    /// spans, over window ends the caller already has in hand.
    ///
    /// A series that produced nothing at any window contributes no entry at
    /// all, which is not the same as contributing NaN — preserving that
    /// distinction is what the sparse transport exists for.
    pub(in crate::promql) fn per_series(
        &self,
        window_ends: &[Timestamp],
        series: Vec<RangeSample<EvalLabels>>,
    ) -> GridSeries {
        match &self.rollup {
            Some(rollup) => GridSeries::Rolled(self.reduce_windows(rollup, window_ends, series)),
            None => GridSeries::Stepped(self.step_series(window_ends, series)),
        }
    }

    /// Stepped instant selection: at each window end, the last sample at or
    /// before it and after `end - lookback` — the rule the range query's step
    /// loop applies, run once per series over every step.
    fn step_series(
        &self,
        window_ends: &[Timestamp],
        series: Vec<RangeSample<EvalLabels>>,
    ) -> Vec<SteppedSeries> {
        let lookback_delta_ms = self.lookback_delta_ms;
        series
            .into_par_rayon()
            .filter_map(|s| {
                let mut points = Vec::new();
                for_each_step_sample(
                    &s.samples,
                    window_ends.iter().copied(),
                    lookback_delta_ms,
                    |step_ts, latest| {
                        if let Some(&sample) = latest {
                            points.push(SteppedPoint { step_ts, sample });
                        }
                    },
                );
                (!points.is_empty()).then_some(SteppedSeries {
                    labels: s.labels,
                    points,
                })
            })
            .collect()
    }

    /// Reduce raw windows with `rollup`, over the given window ends.
    fn reduce_windows(
        &self,
        rollup: &GridRollup,
        window_ends: &[Timestamp],
        series: Vec<RangeSample<EvalLabels>>,
    ) -> Vec<RangeSample<EvalLabels>> {
        // Series are independent, and on a single node this is the entire
        // rollup — every series over every step — so it fans out. Sequential,
        // a 1000-series `rate(m[5m])` at 240 steps ran on one thread while the
        // pool idled.
        series
            .into_par_rayon()
            .filter_map(|s| {
                let points = rollup.kind.eval_windows(
                    &s.samples,
                    rollup.range_ms,
                    self.lookback_delta_ms,
                    self.step_ms,
                    window_ends.iter().copied(),
                    rollup.param,
                );
                (!points.is_empty()).then_some(RangeSample {
                    labels: s.labels,
                    samples: points,
                })
            })
            .collect()
    }

    /// Everything this request asks for, over raw spans: the per-series stage,
    /// then the fused aggregation when there is one. Never answers
    /// [`GridOutcome::Raw`].
    ///
    /// This is the compensation path for a source that did less than was asked
    /// — a single node, which has nothing to push down to, or the series a
    /// shard chose to ship raw. It runs the same kernels a shard would, so the
    /// answer does not depend on who did the work.
    ///
    /// The stage runs a chunk at a time ([`Self::per_series_chunked`]), each
    /// chunk's output into a [`GridSink`]. What the sink keeps of a chunk is
    /// passed to `charge` as the chunk is taken, for what it holds beyond the
    /// raw samples the chunk was built from: one sample in a long window
    /// answers at every step, so what is kept can dwarf the span that was
    /// charged for it, and a query over its budget must stop at the chunk that
    /// crossed it rather than after building the whole grid. That holds for a
    /// fused request too: its per-series output is dropped once folded, but
    /// the groups' `(group, step)` states or a selection's candidates it is
    /// folded into stay until the end, and grow with every chunk.
    pub(in crate::promql) fn evaluate(
        &self,
        series: Vec<RangeSample<EvalLabels>>,
        deadline: &Deadline,
        charge: &mut dyn FnMut(usize) -> PromqlResult<()>,
    ) -> PromqlResult<GridOutcome> {
        let window_ends = self.window_ends();
        let mut sink = GridSink::for_request(self);
        self.per_series_chunked(&window_ends, series, deadline, |output, read| {
            let added = sink.take(self, output, deadline)?;
            charge(added.saturating_sub(read))
        })?;
        sink.finish(deadline).map_err(QueryError::from)
    }

    /// Run the per-series stage over `series` a chunk at a time, `deadline`
    /// checked before each, handing `sink` each chunk's output along with the
    /// raw samples it was built from.
    ///
    /// A chunk is sized by the larger of what its series read and what they
    /// can produce ([`output_bound`]), since a sparse series under a wide
    /// window produces a point per step from a sample or two. So no chunk
    /// produces much more than [`EVALUATE_CHUNK_WORK`] points, however sparse
    /// its input: what one series alone can produce is bounded by the step
    /// cap. A chunk is still evaluated in parallel, and the stage's loops over
    /// one series' windows are left alone.
    pub(in crate::promql) fn per_series_chunked(
        &self,
        window_ends: &[Timestamp],
        series: Vec<RangeSample<EvalLabels>>,
        deadline: &Deadline,
        mut sink: impl FnMut(GridSeries, usize) -> PromqlResult<()>,
    ) -> PromqlResult<()> {
        let backward_ms = self.backward_ms();
        let mut pending = series.into_iter();
        loop {
            // Never `take_while`: it consumes, and drops, the series that ends a chunk.
            let mut chunk = Vec::new();
            let (mut work, mut read) = (0usize, 0usize);
            while work < EVALUATE_CHUNK_WORK {
                let Some(s) = pending.next() else {
                    break;
                };
                let bound = output_bound(window_ends, backward_ms, &s.samples);
                work += s.samples.len().max(bound) + SERIES_WORK;
                read += s.samples.len();
                chunk.push(s);
            }
            if chunk.is_empty() {
                return Ok(());
            }
            deadline.check()?;
            sink(self.per_series(window_ends, chunk), read)?;
        }
    }

    /// The per-series stage's output over no series, of the kind this request
    /// produces.
    pub(in crate::promql) fn empty_output(&self) -> GridSeries {
        match self.rollup {
            Some(_) => GridSeries::Rolled(Vec::new()),
            None => GridSeries::Stepped(Vec::new()),
        }
    }
}

/// Where a [`GridRequest`]'s per-series output goes once the stage has made
/// it: kept, as the entries an unfused request answers with, or folded into
/// the fused aggregation the request carries and dropped. It is the one fold
/// for the three places stage output arises — a shard staging its own batches
/// (`ShardGrid`), a coordinator finishing what its shards sent
/// (`GridFanoutCommand::into_result`), and a node evaluating a raw span
/// ([`GridRequest::evaluate`]) — so each is written once and the answer does
/// not depend on who did the work.
pub(in crate::promql) enum GridSink {
    /// Per-series output, kept whole: an unfused request's answer.
    Series(GridSeries),
    /// A fused reduction's per-`(group, step)` partials. Output is folded in
    /// as it arrives, so nothing larger than the groups outlives a batch.
    Partials(SteppedPartialGroups),
    /// A fused selecting or counting operator's per-step candidates. Output
    /// is run through the operator as it arrives and only its picks (or
    /// counts) are kept; [`Self::finish`] re-selects over their union, as the
    /// coordinator does over its shards' candidates. The top-k of a union lies
    /// within the union of the parts' top-k's, and counts add, so the answer
    /// is the one a single pass over everything would give.
    Selection(SteppedSelection),
}

impl GridSink {
    /// The sink `request`'s output belongs in.
    pub(in crate::promql) fn for_request(request: &GridRequest) -> Self {
        match request.aggregation.as_ref() {
            None => GridSink::Series(request.empty_output()),
            Some(aggregation) => match aggregation.strategy() {
                PushdownStrategy::Reduce => {
                    GridSink::Partials(SteppedPartialGroups::new(aggregation.kind))
                }
                PushdownStrategy::Select | PushdownStrategy::CountValues => {
                    GridSink::Selection(SteppedSelection::new(aggregation.clone()))
                }
            },
        }
    }

    /// Take one batch of the stage's output, run here over raw spans, returning
    /// how much more the sink holds for it — points kept, `(group, step)`
    /// states, or selection candidates: what a caller bounding its memory
    /// charges. Fails when `deadline` passes while it folds, or a selection's
    /// operator refuses its parameter.
    ///
    /// Only a selection differs from [`Self::merge`]: the operator has yet to
    /// run over this output, so it runs here.
    pub(in crate::promql) fn take(
        &mut self,
        request: &GridRequest,
        output: GridSeries,
        deadline: &Deadline,
    ) -> EvalResult<usize> {
        match self {
            GridSink::Selection(selection) => selection.apply(output.into_step_values(), deadline),
            GridSink::Series(_) | GridSink::Partials(_) => self.merge(request, output, deadline),
        }
    }

    /// Take a batch another party already staged — a shard's per-series
    /// entries, or the candidates its own run of the operator picked — so the
    /// operator is not run over it again here, returning how much more the sink
    /// holds for it. Fails when `deadline` passes while it folds.
    pub(in crate::promql) fn merge(
        &mut self,
        request: &GridRequest,
        output: GridSeries,
        deadline: &Deadline,
    ) -> EvalResult<usize> {
        Ok(match self {
            GridSink::Series(series) => {
                let added = output.points();
                series.append(output);
                added
            }
            GridSink::Partials(partials) => Self::fold(partials, request, output, deadline)?,
            GridSink::Selection(selection) => {
                selection.merge(output.into_step_values(), deadline)?
            }
        })
    }

    fn fold(
        partials: &mut SteppedPartialGroups,
        request: &GridRequest,
        output: GridSeries,
        deadline: &Deadline,
    ) -> EvalResult<usize> {
        let modifier = request
            .aggregation
            .as_ref()
            .and_then(|aggregation| aggregation.modifier.as_ref());
        partials.accumulate(modifier, output.into_step_values(), deadline)
    }

    /// Everything taken, as the request's outcome. A fused sink walks every
    /// state or candidate it accumulated to finalize them, `deadline` checked
    /// as it goes.
    pub(in crate::promql) fn finish(self, deadline: &Deadline) -> EvalResult<GridOutcome> {
        Ok(match self {
            GridSink::Series(series) => series.into(),
            GridSink::Partials(partials) => GridOutcome::Reduced(partials.finalize(deadline)?),
            GridSink::Selection(selection) => GridOutcome::Reduced(selection.finalize(deadline)?),
        })
    }
}

/// The most points the per-series stage can produce from `samples` over
/// `window_ends`: a window `(end - backward_ms, end]` answers only when it
/// holds a sample — a rollup reduces nothing to nothing, and a stepped pick
/// needs a sample inside the lookback — so only the ends in
/// `[first, last + backward_ms)` can. Two binary searches, so a chunk can be
/// sized by it before any of its windows is evaluated.
pub(in crate::promql) fn output_bound(
    window_ends: &[Timestamp],
    backward_ms: i64,
    samples: &[Sample],
) -> usize {
    let (Some(first), Some(last)) = (samples.first(), samples.last()) else {
        return 0;
    };
    let reach = last.timestamp.saturating_add(backward_ms);
    let lo = window_ends.partition_point(|&end| end < first.timestamp);
    let hi = window_ends.partition_point(|&end| end < reach);
    hi.saturating_sub(lo)
}

/// Work, in samples or output points (a series counting [`SERIES_WORK`]), that
/// [`GridRequest::per_series_chunked`] runs between two deadline checks: a few
/// milliseconds of the per-series stage, and about 16 MiB of its output.
const EVALUATE_CHUNK_WORK: usize = 1 << 20;

/// The window ends a grid request describes, in ascending order.
///
/// Derived from resolved geometry alone — `@` and `offset` are applied before a
/// request is built — so the coordinator and a shard reading the same request
/// land on exactly the same windows. `step_ms <= 0` is a single evaluation at
/// `range_end_ms`.
pub(in crate::promql) fn grid_window_ends(
    step_ms: i64,
    query_start: Timestamp,
    query_end: Timestamp,
    range_end_ms: Timestamp,
) -> Vec<Timestamp> {
    grid_window_ends_iter(step_ms, query_start, query_end, range_end_ms).collect()
}

/// [`grid_window_ends`], uncollected: the one definition of a request's
/// windows, so a caller that only walks them need not allocate them.
pub(in crate::promql) fn grid_window_ends_iter(
    step_ms: i64,
    query_start: Timestamp,
    query_end: Timestamp,
    range_end_ms: Timestamp,
) -> impl Iterator<Item = Timestamp> + use<> {
    let single = (step_ms <= 0).then_some(range_end_ms);
    let stepped =
        (step_ms > 0).then(|| crate::promql::time::step_times(query_start, query_end, step_ms));
    single.into_iter().chain(stepped.into_iter().flatten())
}

/// The inclusive `[start, end]` span of raw samples `window_ends` covers, for
/// storage's `get_range`, reaching `backward_ms` behind the first end.
///
/// Windows are half-open — `(end - backward, end]` — and `get_range` takes an
/// inclusive lower bound, so the span starts one millisecond past the first
/// window's lower bound. `None` when there are no windows to cover.
pub(in crate::promql) fn grid_fetch_bounds(
    window_ends: &[Timestamp],
    backward_ms: i64,
) -> Option<(Timestamp, Timestamp)> {
    let (&first, &last) = (window_ends.first()?, window_ends.last()?);
    Some(fetch_span(first, last, backward_ms))
}

fn fetch_span(first: Timestamp, last: Timestamp, backward_ms: i64) -> (Timestamp, Timestamp) {
    (first.saturating_sub(backward_ms).saturating_add(1), last)
}

/// What a data source made of a [`GridRequest`]. Every variant tells the
/// caller what it still has to do.
pub enum GridOutcome {
    /// The source ran a stepped selection (a request with neither rollup nor
    /// aggregation): one entry per series, sparse over the steps.
    Stepped(Vec<SteppedSeries>),
    /// The source reduced the windows of an unfused rollup request: one entry
    /// per series, holding sparse `(window end, value)` pairs. A window that
    /// held no samples is absent, not NaN.
    Rolled(Vec<RangeSample<EvalLabels>>),
    /// The source did everything a fused request asked — the per-series stage
    /// and the grouping — so the entries are groups rather than series, each
    /// holding sparse `(step, value)` pairs.
    Reduced(Vec<RangeSample<EvalLabels>>),
    /// The source returned the raw spans instead (nothing to push down to,
    /// e.g. a single node): the caller runs [`GridRequest::evaluate`] over
    /// them.
    Raw(Vec<RangeSample<EvalLabels>>),
    /// A peer runs an older build without grid push-down (a rolling upgrade),
    /// so nothing was computed. The caller evaluates the expression without
    /// the grid: a preload is skipped, and a push-down is declined, so the
    /// per-step selector reads every build supports do the work.
    Unsupported,
}

impl GridOutcome {
    /// `None` for [`GridOutcome::Unsupported`], so a caller can fall back in one line.
    pub(in crate::promql) fn supported(self) -> Option<GridOutcome> {
        match self {
            GridOutcome::Unsupported => None,
            outcome => Some(outcome),
        }
    }
}

pub trait QueryReader: Send + Sync {
    /// Query instant samples at `timestamp`.
    /// `deadline` is an optional absolute Instant by which the operation should complete.
    fn query(
        &self,
        selector: &VectorSelector,
        timestamp: i64,
        options: QueryOptions,
    ) -> PromqlResult<Vec<InstantSample<EvalLabels>>>;

    /// Query range samples between `start_ms` and `end_ms` with an optional `deadline`.
    fn query_range(
        &self,
        selector: &VectorSelector,
        start_ms: i64,
        end_ms: i64,
        options: QueryOptions,
    ) -> PromqlResult<Vec<RangeSample<EvalLabels>>>;

    /// Evaluate `aggregation` over the instant vector `selector` selects at
    /// `timestamp`, at the source.
    ///
    /// A source that spans several nodes can push the whole aggregation to the
    /// nodes that hold the data and return only the reduced result, which is
    /// dramatically less data than the input vector. Sources that cannot do this
    /// say so and the caller aggregates itself, so implementing this is purely
    /// an optimization: the default does nothing.
    fn query_aggregation(
        &self,
        _selector: &VectorSelector,
        _timestamp: i64,
        _aggregation: &AggregationRequest,
        _options: QueryOptions,
    ) -> PromqlResult<AggregationOutcome> {
        Ok(AggregationOutcome::Unsupported)
    }

    /// Evaluate `request` over the grid for the series `selector` selects, at
    /// the source.
    ///
    /// Because a series lives entirely on one node, a source that spans several
    /// can have each compute its own series' output — one point per series per
    /// step, or one partial per group per step — instead of shipping the raw
    /// span for the coordinator to bucket or reduce. A source with nothing to
    /// push down to answers [`GridOutcome::Raw`] from its range read, which is
    /// what this default does; the caller then runs the same kernels itself.
    fn query_grid(
        &self,
        selector: &VectorSelector,
        request: &GridRequest,
        options: QueryOptions,
    ) -> PromqlResult<GridOutcome> {
        let Some((start_ms, end_ms)) = request.fetch_bounds() else {
            return Ok(GridOutcome::Raw(Vec::new()));
        };
        self.query_range(selector, start_ms, end_ms, options)
            .map(GridOutcome::Raw)
    }

    /// The labels of the series `selector` matches, from the index rather
    /// than from a read — see [`LabelProfile`]. With a `window`, only the
    /// series with a retained sample in it count ([`ProfileWindow::admits`]).
    /// `None` when the source cannot say (no index, or more series than it
    /// will profile), in which case the caller treats the selector as it
    /// would without a profile. Purely an optimization: the default declines.
    fn label_profile(
        &self,
        _selector: &VectorSelector,
        _window: Option<ProfileWindow>,
        _options: QueryOptions,
    ) -> PromqlResult<Option<LabelProfile>> {
        Ok(None)
    }
}

impl QueryReader for Arc<dyn QueryReader> {
    fn query(
        &self,
        selector: &VectorSelector,
        timestamp: i64,
        options: QueryOptions,
    ) -> PromqlResult<Vec<InstantSample<EvalLabels>>> {
        self.as_ref().query(selector, timestamp, options)
    }

    fn query_range(
        &self,
        selector: &VectorSelector,
        start_ms: i64,
        end_ms: i64,
        options: QueryOptions,
    ) -> PromqlResult<Vec<RangeSample<EvalLabels>>> {
        self.as_ref()
            .query_range(selector, start_ms, end_ms, options)
    }

    fn query_aggregation(
        &self,
        selector: &VectorSelector,
        timestamp: i64,
        aggregation: &AggregationRequest,
        options: QueryOptions,
    ) -> PromqlResult<AggregationOutcome> {
        self.as_ref()
            .query_aggregation(selector, timestamp, aggregation, options)
    }

    fn query_grid(
        &self,
        selector: &VectorSelector,
        request: &GridRequest,
        options: QueryOptions,
    ) -> PromqlResult<GridOutcome> {
        self.as_ref().query_grid(selector, request, options)
    }

    fn label_profile(
        &self,
        selector: &VectorSelector,
        window: Option<ProfileWindow>,
        options: QueryOptions,
    ) -> PromqlResult<Option<LabelProfile>> {
        self.as_ref().label_profile(selector, window, options)
    }
}

#[cfg(test)]
pub(crate) mod test_utils {
    use crate::common::Sample;
    use crate::labels::Labels;
    pub(crate) use crate::promql::engine::memory_series_querier::MemorySeriesQuerier;

    /// Builder for creating MockQueryReader instances from test data.
    /// Convenience wrapper for single-bucket scenarios.
    pub(crate) struct MockQueryReaderBuilder {
        inner: MockMultiBucketQueryReaderBuilder,
    }

    impl MockQueryReaderBuilder {
        pub(crate) fn new() -> Self {
            Self {
                inner: MockMultiBucketQueryReaderBuilder::new(),
            }
        }

        /// Add a sample with labels. If a series with the same labels already exists globally,
        /// the existing series ID is reused. Otherwise, a new series is created with a global ID.
        pub(crate) fn add_sample(&mut self, labels: &Labels, sample: Sample) -> &mut Self {
            self.inner.add_sample(labels, sample);
            self
        }

        pub(crate) fn build(self) -> MemorySeriesQuerier {
            self.inner.build()
        }
    }

    /// Builder for creating MockQueryReader instances from test data.
    /// Supports multi-bucket scenarios.
    pub(crate) struct MockMultiBucketQueryReaderBuilder {
        reader: MemorySeriesQuerier,
    }

    impl MockMultiBucketQueryReaderBuilder {
        pub(crate) fn new() -> Self {
            Self {
                reader: MemorySeriesQuerier::new(),
            }
        }

        /// Add a sample with labels to a specific bucket. If a series with the same labels already exists globally,
        /// the existing series ID is reused. Otherwise, a new series is created with a global ID.
        pub(crate) fn add_sample(&mut self, labels: &Labels, sample: Sample) -> &mut Self {
            self.reader.add_sample(labels, sample);
            self
        }

        pub(crate) fn build(self) -> MemorySeriesQuerier {
            self.reader
        }
    }
}

#[cfg(test)]
mod evaluate_chunk_tests {
    use super::*;
    use crate::common::Sample;
    use crate::common::time::current_time_millis;
    use crate::promql::QueryOptions;
    use crate::promql::engine::sample_budget::SampleBudget;

    fn stepped_request() -> GridRequest {
        GridRequest {
            step_ms: 1_000,
            query_start: 0,
            query_end: 9_000,
            range_end_ms: 9_000,
            lookback_delta_ms: 300_000,
            rollup: None,
            aggregation: None,
            sample_timestamps: false,
        }
    }

    /// Enough series for two chunks: each costs `SERIES_WORK` and its ten steps.
    fn many_series() -> Vec<RangeSample<EvalLabels>> {
        let count = EVALUATE_CHUNK_WORK / (SERIES_WORK + 10) + 10_000;
        (0..count)
            .map(|i| RangeSample {
                labels: EvalLabels::from_pairs(&[("i", &i.to_string())]),
                samples: vec![Sample::new(0, i as f64)],
            })
            .collect()
    }

    fn picks(outcome: GridOutcome) -> Vec<(String, usize)> {
        let GridOutcome::Stepped(series) = outcome else {
            panic!("a stepped request answers stepped");
        };
        series
            .into_iter()
            .map(|s| (s.labels.to_string(), s.points.len()))
            .collect()
    }

    /// The uncollected window ends, and the bounds walked from them, are exactly
    /// what the collected ends give — at a single evaluation, an empty grid, a
    /// one-step grid, and a grid that ends against `i64::MAX`.
    #[test]
    fn walked_window_ends_and_bounds_match_the_collected_ones() {
        let shapes = [
            (1_000, 0, 9_000, 9_000),
            (0, 0, 9_000, 4_000),
            (-1, 0, 9_000, 4_000),
            (1_000, 9_000, 0, 0),
            (1_000, 5_000, 5_000, 5_000),
            (10, i64::MAX - 25, i64::MAX, i64::MAX),
        ];
        for (step_ms, query_start, query_end, range_end_ms) in shapes {
            let request = GridRequest {
                step_ms,
                query_start,
                query_end,
                range_end_ms,
                ..stepped_request()
            };
            let collected = request.window_ends();
            let shape = format!("{step_ms} {query_start}..={query_end} @{range_end_ms}");
            assert_eq!(
                request.window_ends_iter().collect::<Vec<_>>(),
                collected,
                "{shape}"
            );
            assert_eq!(
                request.fetch_bounds(),
                grid_fetch_bounds(&collected, request.backward_ms()),
                "{shape}"
            );
        }
    }

    /// Chunking is invisible: every series comes out once, in order, exactly as from
    /// one call over all of them.
    #[test]
    fn a_chunked_evaluation_matches_one_over_every_series() {
        let request = stepped_request();
        let series = many_series();
        let whole = match request.per_series(&request.window_ends(), series.clone()) {
            GridSeries::Stepped(series) => GridOutcome::Stepped(series),
            GridSeries::Rolled(_) => unreachable!(),
        };
        let chunked = request
            .evaluate(series.clone(), &Deadline::default(), &mut |_| Ok(()))
            .unwrap();
        let (whole, chunked) = (picks(whole), picks(chunked));
        assert_eq!(
            chunked.len(),
            series.len(),
            "no series lost at a chunk boundary"
        );
        assert_eq!(chunked, whole);
    }

    #[test]
    fn a_passed_deadline_stops_the_evaluation_between_chunks() {
        let options = QueryOptions {
            deadline: Some(current_time_millis() - 1),
            ..QueryOptions::default()
        };
        let err = stepped_request()
            .evaluate(
                many_series(),
                &Deadline::from_options(&options),
                &mut |_| Ok(()),
            )
            .err()
            .expect("stopped at the first chunk");
        assert!(matches!(err, crate::promql::QueryError::Timeout), "{err}");
    }

    const STEPS: usize = 10_000;

    /// A rollup over windows wider than the whole range, so a series' one
    /// sample answers at every one of [`STEPS`] steps — reporting its value.
    fn expanding_request(aggregation: Option<GridAggregation>) -> GridRequest {
        let last = (STEPS as i64 - 1) * 1_000;
        GridRequest {
            step_ms: 1_000,
            query_start: 0,
            query_end: last,
            range_end_ms: last,
            lookback_delta_ms: 300_000,
            rollup: Some(GridRollup {
                kind: RollupKind::SumOverTime,
                range_ms: STEPS as i64 * 1_000,
                param: None,
            }),
            aggregation,
            sample_timestamps: false,
        }
    }

    /// One-sample series: one chunk by what they read, three by what they produce.
    fn sparse_series() -> Vec<RangeSample<EvalLabels>> {
        (0..3 * EVALUATE_CHUNK_WORK / STEPS)
            .map(|i| RangeSample {
                labels: EvalLabels::from_pairs(&[("i", &i.to_string())]),
                samples: vec![Sample::new(0, i as f64)],
            })
            .collect()
    }

    fn rendered(groups: Vec<RangeSample<EvalLabels>>) -> Vec<(String, Vec<(i64, u64)>)> {
        let mut out: Vec<_> = groups
            .into_iter()
            .map(|g| {
                let points = g.samples.iter().map(|p| (p.timestamp, p.value.to_bits()));
                (g.labels.to_string(), points.collect())
            })
            .collect();
        out.sort();
        out
    }

    #[test]
    fn the_output_bound_covers_every_window_that_can_answer() {
        let request = GridRequest {
            rollup: Some(GridRollup {
                kind: RollupKind::CountOverTime,
                range_ms: 3_000,
                param: None,
            }),
            ..stepped_request()
        };
        let window_ends = request.window_ends();
        // (sample timestamps, bound): the windows `(end - 3s, end]` over ends 0s..=9s.
        let cases: [(&[i64], usize); 7] = [
            (&[], 0),
            (&[5_000], 3),
            (&[2_500, 2_600], 3),
            (&[0, 9_000], 10),
            (&[-3_000], 0),
            (&[-2_999], 1),
            (&[20_000], 0),
        ];
        for (timestamps, want) in cases {
            let samples: Vec<Sample> = timestamps.iter().map(|&t| Sample::new(t, 1.0)).collect();
            let bound = output_bound(&window_ends, request.backward_ms(), &samples);
            assert_eq!(bound, want, "{timestamps:?}");
            let series = RangeSample {
                labels: EvalLabels::from_pairs(&[("i", "0")]),
                samples,
            };
            let produced = request.per_series(&window_ends, vec![series]).points();
            assert!(produced <= bound, "{timestamps:?}: {produced} > {bound}");
            if timestamps.len() == 1 {
                assert_eq!(produced, bound, "one sample answers every window it is in");
            }
        }
    }

    /// One-sample series are chunked by what they expand to, not what they
    /// read, and each chunk's expansion is charged as it is built.
    #[test]
    fn a_sparse_expansion_is_chunked_and_charged_by_its_output() {
        let series = sparse_series();
        let count = series.len();
        let mut charges = Vec::new();
        let outcome = expanding_request(None)
            .evaluate(series, &Deadline::default(), &mut |expanded| {
                charges.push(expanded);
                Ok(())
            })
            .unwrap();
        let GridOutcome::Rolled(rolled) = outcome else {
            panic!("a rollup request answers Rolled");
        };
        assert_eq!(rolled.len(), count);
        assert!(rolled.iter().all(|s| s.samples.len() == STEPS));
        assert!(charges.len() >= 3, "one charge per chunk: {charges:?}");
        assert!(
            charges.iter().all(|&n| n <= EVALUATE_CHUNK_WORK + STEPS),
            "no chunk produces much more than the chunk work: {charges:?}"
        );
        // Everything past the one raw sample each, which the caller charged.
        assert_eq!(charges.iter().sum::<usize>(), count * (STEPS - 1));
    }

    #[test]
    fn a_budget_stops_the_expansion_at_the_chunk_that_crosses_it() {
        let budget = SampleBudget::new(EVALUATE_CHUNK_WORK / 2);
        let err = expanding_request(None)
            .evaluate(sparse_series(), &Deadline::default(), &mut |expanded| {
                budget.charge(expanded)
            })
            .err()
            .expect("over budget");
        assert!(matches!(err, QueryError::TooManySamples { .. }), "{err}");
        assert!(
            budget.loaded() <= EVALUATE_CHUNK_WORK + STEPS,
            "stopped after the first chunk, not the whole grid: {}",
            budget.loaded()
        );
    }

    /// The aggregations a fused request can carry, one per strategy, each
    /// keeping a state or candidate per series per step over [`sparse_series`]:
    /// grouped by the label every series differs in, or counting its values,
    /// which differ too.
    fn per_series_aggregations() -> [GridAggregation; 3] {
        let by_i = || {
            Some(LabelModifier::Include(promql_parser::label::Labels::new(
                vec!["i"],
            )))
        };
        [
            GridAggregation {
                kind: AggregationKind::Sum,
                modifier: by_i(),
                param: None,
            },
            GridAggregation {
                kind: AggregationKind::Topk,
                modifier: by_i(),
                param: Some(AggregationParam::Scalar(1.0)),
            },
            GridAggregation {
                kind: AggregationKind::CountValues,
                modifier: None,
                param: Some(AggregationParam::Label("v".to_string())),
            },
        ]
    }

    /// What a fused request's groups or candidates hold past the raw samples
    /// they were folded from is charged as they grow, chunk by chunk, exactly
    /// as an unfused request's kept output is: the per-series output is
    /// dropped once folded, but what it was folded into stays.
    #[test]
    fn a_fused_accumulation_is_charged_as_it_grows() {
        for aggregation in per_series_aggregations() {
            let kind = aggregation.kind;
            let series = sparse_series();
            let count = series.len();
            let mut charges = Vec::new();
            let GridOutcome::Reduced(groups) = expanding_request(Some(aggregation))
                .evaluate(series, &Deadline::default(), &mut |expanded| {
                    charges.push(expanded);
                    Ok(())
                })
                .unwrap()
            else {
                panic!("a fused request answers Reduced");
            };
            assert_eq!(groups.len(), count, "{kind:?}");
            assert!(
                charges.len() >= 3,
                "{kind:?}: one charge per chunk: {charges:?}"
            );
            assert_eq!(
                charges.iter().sum::<usize>(),
                count * (STEPS - 1),
                "{kind:?}: everything held past the one raw sample each"
            );
        }
    }

    /// A budget stops a fused accumulation at the chunk that crosses it, not
    /// once every group is whole.
    #[test]
    fn a_budget_stops_a_fused_accumulation_at_the_chunk_that_crosses_it() {
        for aggregation in per_series_aggregations() {
            let kind = aggregation.kind;
            let budget = SampleBudget::new(EVALUATE_CHUNK_WORK / 2);
            let err = expanding_request(Some(aggregation))
                .evaluate(sparse_series(), &Deadline::default(), &mut |expanded| {
                    budget.charge(expanded)
                })
                .err()
                .unwrap_or_else(|| panic!("{kind:?}: over budget"));
            assert!(
                matches!(err, QueryError::TooManySamples { .. }),
                "{kind:?}: {err}"
            );
            assert!(
                budget.loaded() <= EVALUATE_CHUNK_WORK + STEPS,
                "{kind:?}: stopped after the first chunk, not the whole grid: {}",
                budget.loaded()
            );
        }
    }

    /// A fused request folds chunk by chunk, as a shard folds its own series:
    /// the answer is the one fold over every series' output would give.
    #[test]
    fn a_fused_evaluation_folds_chunks_into_the_whole_answer() {
        let aggregations = [
            (AggregationKind::Sum, None),
            (AggregationKind::Topk, Some(AggregationParam::Scalar(2.0))),
            (
                AggregationKind::CountValues,
                Some(AggregationParam::Label("v".to_string())),
            ),
        ];
        for (kind, param) in aggregations {
            let aggregation = GridAggregation {
                kind,
                modifier: None,
                param,
            };
            let request = expanding_request(Some(aggregation.clone()));
            let series = sparse_series();
            let GridOutcome::Reduced(chunked) = request
                .evaluate(series.clone(), &Deadline::default(), &mut |_| Ok(()))
                .unwrap()
            else {
                panic!("a fused request answers Reduced");
            };

            let whole = request
                .per_series(&request.window_ends(), series)
                .into_step_values();
            let want = match aggregation.strategy() {
                PushdownStrategy::Reduce => {
                    let mut partials = SteppedPartialGroups::new(kind);
                    partials
                        .accumulate(None, whole, &Deadline::default())
                        .unwrap();
                    partials.finalize(&Deadline::default()).unwrap()
                }
                _ => {
                    let mut selection = SteppedSelection::new(aggregation);
                    selection.apply(whole, &Deadline::default()).unwrap();
                    selection.finalize(&Deadline::default()).unwrap()
                }
            };
            assert_eq!(rendered(chunked), rendered(want), "{kind:?}");
        }
    }
}
