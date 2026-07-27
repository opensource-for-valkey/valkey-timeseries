use crate::promql::exec::aggregations::AggregationKind;
use crate::promql::functions::RollupKind;
use crate::promql::{
    ExprResult, PromqlResult, QueryOptions,
    model::{InstantSample, RangeSample},
};
use crate::series::SeriesRef;
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
    Aggregated(Vec<InstantSample>),
    /// The source returned the raw instant vector instead of aggregating it
    /// (nothing to push down to, e.g. a single node): the caller aggregates.
    Raw(Vec<InstantSample>),
    /// The source cannot evaluate pushed-down aggregations: the caller should
    /// select the instant vector itself and aggregate that.
    Unsupported,
}

/// A range-vector function to evaluate over the windows a matrix selector
/// describes, plus everything needed to reproduce those windows exactly.
///
/// Every time-dependent field is *resolved*: `@` and `offset` are applied by the
/// coordinator before the request is built, so a data source reduces the windows
/// it is handed and never re-derives a modifier. Paired with the selector passed
/// alongside it, this is the whole of a `sum_over_time(m[5m] offset 1h)` — which
/// is what makes it something a source can evaluate close to the data.
/// An outer aggregation fused onto a rollup: the `sum by (job)` of
/// `sum by (job) (rate(m[5m]))`.
///
/// Only the reducing operators appear here. The selecting ones (`topk` and
/// friends) need the individual rolled-up samples to choose among, so fusing
/// them would not reduce what crosses the wire — see
/// [`crate::promql::exec::aggregations::PushdownStrategy`].
#[derive(Debug, Clone)]
pub struct RollupAggregation {
    pub kind: AggregationKind,
    pub modifier: Option<LabelModifier>,
}

#[derive(Debug, Clone)]
pub struct RollupRequest {
    pub kind: RollupKind,
    /// When set, the source groups its rolled-up values as well as computing
    /// them, and returns one value per group per step instead of one per series
    /// per step.
    pub aggregation: Option<RollupAggregation>,
    /// Window width: the `[5m]`. Each window is `(end - range_ms, end]`.
    pub range_ms: i64,
    pub lookback_delta_ms: i64,
    /// Step grid of the enclosing query. `step_ms == 0` is a single evaluation
    /// at [`Self::range_end_ms`].
    pub step_ms: i64,
    pub query_start: i64,
    pub query_end: i64,
    /// Window end for the single-evaluation case, `@`/`offset` resolved.
    pub range_end_ms: i64,
    /// Numeric function parameter, e.g. `quantile_over_time`'s phi.
    pub param: Option<f64>,
}

impl RollupRequest {
    /// The window ends to reduce, in ascending order.
    pub(in crate::promql) fn window_ends(&self) -> Vec<i64> {
        if self.step_ms <= 0 {
            return vec![self.range_end_ms];
        }
        crate::promql::time::step_times(self.query_start, self.query_end, self.step_ms).collect()
    }
}

/// What a data source made of a [`RollupRequest`]. Every variant tells the
/// caller what it still has to do.
pub enum RollupOutcome {
    /// The source did everything the request asked: it reduced the windows and,
    /// when the request carried an aggregation, grouped the result — so the
    /// entries are groups rather than series. Each entry holds sparse
    /// `(window end, value)` pairs; a window that held no samples is absent, not
    /// NaN.
    Rolled(Vec<RangeSample>),
    /// The source reduced the windows but did *not* apply the request's
    /// aggregation: the entries are per-series values and the caller groups
    /// them.
    ///
    /// Distinct from [`Self::Rolled`] because the two carry the same type and
    /// mean different things. Without the distinction a source that skipped the
    /// grouping would be taken to have done it, and the query would answer with
    /// ungrouped series — a wrong answer rather than a slow one.
    Reduced(Vec<RangeSample>),
    /// The source returned the raw windows instead of reducing them (nothing to
    /// push down to, e.g. a single node): the caller reduces them, and groups
    /// them if the request asked for that.
    Raw(Vec<RangeSample>),
    /// The source cannot evaluate pushed-down rollups: the caller should select
    /// the matrix itself and reduce that.
    Unsupported,
}

pub trait QueryReader: Send + Sync {
    /// Query instant samples at `timestamp`.
    /// `deadline` is an optional absolute Instant by which the operation should complete.
    fn query(
        &self,
        selector: &VectorSelector,
        timestamp: i64,
        options: QueryOptions,
    ) -> PromqlResult<Vec<InstantSample>>;

    /// Query range samples between `start_ms` and `end_ms` with an optional `deadline`.
    fn query_range(
        &self,
        selector: &VectorSelector,
        start_ms: i64,
        end_ms: i64,
        options: QueryOptions,
    ) -> PromqlResult<Vec<RangeSample>>;

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

    /// Evaluate `rollup` over the windows `selector`'s series contribute, at the
    /// source.
    ///
    /// Because a series lives entirely on one node, a source that spans several
    /// can have each compute its own series' final values and return one float
    /// per series per step instead of the raw — and, when the range exceeds the
    /// step, heavily overlapping — windows. Sources that cannot do this say so
    /// and the caller reduces the matrix itself, so implementing this is purely
    /// an optimization: the default does nothing.
    fn query_rollup(
        &self,
        _selector: &VectorSelector,
        _rollup: &RollupRequest,
        _options: QueryOptions,
    ) -> PromqlResult<RollupOutcome> {
        Ok(RollupOutcome::Unsupported)
    }
}

impl QueryReader for Arc<dyn QueryReader> {
    fn query(
        &self,
        selector: &VectorSelector,
        timestamp: i64,
        options: QueryOptions,
    ) -> PromqlResult<Vec<InstantSample>> {
        self.as_ref().query(selector, timestamp, options)
    }

    fn query_range(
        &self,
        selector: &VectorSelector,
        start_ms: i64,
        end_ms: i64,
        options: QueryOptions,
    ) -> PromqlResult<Vec<RangeSample>> {
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

    fn query_rollup(
        &self,
        selector: &VectorSelector,
        rollup: &RollupRequest,
        options: QueryOptions,
    ) -> PromqlResult<RollupOutcome> {
        self.as_ref().query_rollup(selector, rollup, options)
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
