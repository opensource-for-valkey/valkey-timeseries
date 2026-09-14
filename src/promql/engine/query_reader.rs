use crate::common::threads::IntoParRayon;
use crate::common::{Sample, Timestamp};
use crate::promql::EvalLabels;
use crate::promql::engine::label_profile::LabelProfile;
use crate::promql::exec::aggregations::{AggregationKind, PushdownStrategy};
use crate::promql::exec::partial_aggregation::{SteppedPartialGroups, SteppedSelection};
use crate::promql::exec::pipeline::for_each_step_sample;
use crate::promql::functions::RollupKind;
use crate::promql::{
    ExprResult, PromqlResult, QueryError, QueryOptions,
    model::{InstantSample, RangeSample},
};
use crate::series::SeriesRef;
use orx_parallel::ParIter;
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

impl GridRequest {
    /// The window ends to evaluate at, in ascending order.
    pub(in crate::promql) fn window_ends(&self) -> Vec<i64> {
        grid_window_ends(
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
        grid_fetch_bounds(&self.window_ends(), self.backward_ms())
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

    /// Apply this request's fused aggregation to per-series `(step, value)`
    /// output, or pass it through when the request carries none.
    pub(in crate::promql) fn group(
        &self,
        series: Vec<RangeSample<EvalLabels>>,
    ) -> PromqlResult<Vec<RangeSample<EvalLabels>>> {
        let Some(aggregation) = self.aggregation.as_ref() else {
            return Ok(series);
        };
        match aggregation.strategy() {
            PushdownStrategy::Reduce => {
                let mut partials = SteppedPartialGroups::new(aggregation.kind);
                partials.accumulate(aggregation.modifier.as_ref(), series);
                Ok(partials.finalize())
            }
            PushdownStrategy::Select | PushdownStrategy::CountValues => {
                let mut selection = SteppedSelection::new(aggregation.clone());
                selection
                    .apply(series)
                    .and_then(|()| selection.finalize())
                    .map_err(|e| QueryError::Execution(e.to_string()))
            }
        }
    }

    /// Everything this request asks for, over raw spans: the per-series stage,
    /// then the fused aggregation when there is one. Never answers
    /// [`GridOutcome::Raw`].
    ///
    /// This is the compensation path for a source that did less than was asked
    /// — a single node, which has nothing to push down to, or the series a
    /// shard chose to ship raw. It runs the same kernels a shard would, so the
    /// answer does not depend on who did the work.
    pub(in crate::promql) fn evaluate(
        &self,
        series: Vec<RangeSample<EvalLabels>>,
    ) -> PromqlResult<GridOutcome> {
        let per_series = self.per_series(&self.window_ends(), series);
        if self.aggregation.is_some() {
            return self
                .group(per_series.into_step_values())
                .map(GridOutcome::Reduced);
        }
        Ok(match per_series {
            GridSeries::Stepped(series) => GridOutcome::Stepped(series),
            GridSeries::Rolled(series) => GridOutcome::Rolled(series),
        })
    }
}

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
    if step_ms <= 0 {
        return vec![range_end_ms];
    }
    crate::promql::time::step_times(query_start, query_end, step_ms).collect()
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
    Some(((first - backward_ms).saturating_add(1), last))
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
    /// than from a read — see [`LabelProfile`]. `None` when the source cannot
    /// say (no index, or more series than it will profile), in which case
    /// the caller treats the selector as it would without a profile. Purely
    /// an optimization: the default declines.
    fn label_profile(
        &self,
        _selector: &VectorSelector,
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
        options: QueryOptions,
    ) -> PromqlResult<Option<LabelProfile>> {
        self.as_ref().label_profile(selector, options)
    }
}

pub(crate) mod test_utils {
    use super::*;
    use crate::commands::parse_metric_name;
    use crate::common::Sample;
    use crate::labels::Labels;
    pub(crate) use crate::promql::engine::memory_series_querier::MemorySeriesQuerier;
    use crate::series::TimeSeries;
    use crate::series::index::TimeSeriesIndex;
    use std::collections::HashMap;

    /// Builder for creating MockQueryReader instances from test data.
    /// Convenience wrapper for single-bucket scenarios.
    pub(crate) struct MockQueryReaderBuilder {
        ts_index: TimeSeriesIndex,
        series: HashMap<SeriesRef, TimeSeries>,
        inner: MockMultiBucketQueryReaderBuilder,
    }

    impl MockQueryReaderBuilder {
        pub(crate) fn new() -> Self {
            Self {
                series: HashMap::new(),
                ts_index: TimeSeriesIndex::default(),
                inner: MockMultiBucketQueryReaderBuilder::new(),
            }
        }

        /// Add a sample with labels. If a series with the same labels already exists globally,
        /// the existing series ID is reused. Otherwise, a new series is created with a global ID.
        pub(crate) fn add_sample(&mut self, labels: &Labels, sample: Sample) -> &mut Self {
            self.inner.add_sample(labels, sample);
            self
        }

        pub(crate) fn add_samples(&mut self, labels: &Labels, samples: &[Sample]) -> &mut Self {
            for sample in samples {
                self.add_sample(labels, *sample);
            }
            self
        }

        pub(crate) fn add_metric_sample(&mut self, metric: &str, sample: Sample) -> &mut Self {
            let labels = parse_metric_name(metric)
                .unwrap_or_else(|_| panic!("Failed to parse metric name: {}", metric));
            let labels = Labels::new(labels);
            self.add_sample(&labels, sample)
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
