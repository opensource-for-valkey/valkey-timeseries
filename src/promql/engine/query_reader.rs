use crate::promql::exec::aggregations::AggregationKind;
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
