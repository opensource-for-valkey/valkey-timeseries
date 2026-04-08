use crate::common::Sample;
use crate::promql::hashers::SeriesFingerprint;
use crate::promql::{
    EvalResult, EvaluationError, PromqlResult, QueryOptions,
    model::{InstantSample, RangeSample},
};
use crate::series::SeriesRef;
use ahash::AHashMap;
use promql_parser::parser::VectorSelector;
use std::default::Default;
use std::sync::Arc;

pub(crate) trait SeriesQuerier: Send + Sync {
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
}

impl<T> SeriesQuerier for Box<T>
where
    T: SeriesQuerier,
{
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
}

impl SeriesQuerier for Box<dyn SeriesQuerier> {
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
}

impl SeriesQuerier for Arc<dyn SeriesQuerier> {
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
}

#[derive(Default)]
pub(crate) struct QueryReaderEvalCache {
    samples: AHashMap<SeriesFingerprint, Vec<Sample>>,
}

impl QueryReaderEvalCache {
    pub(crate) fn new() -> Self {
        Self {
            samples: Default::default(),
        }
    }

    pub(crate) fn cache_samples(&mut self, series_id: SeriesFingerprint, samples: Vec<Sample>) {
        self.samples.insert(series_id, samples);
    }

    pub(crate) fn get_samples(&self, series_id: &SeriesFingerprint) -> Option<&Vec<Sample>> {
        self.samples.get(series_id)
    }
}

pub struct CachedQueryReader<'querier, Q: SeriesQuerier> {
    inner: &'querier Q,
}

impl<'reader, R: SeriesQuerier> CachedQueryReader<'reader, R> {
    pub(crate) fn new(reader: &'reader R) -> Self {
        Self { inner: reader }
    }

    pub(crate) fn samples(
        &self,
        _series_id: SeriesFingerprint,
        _start_ms: i64,
        _end_ms: i64,
    ) -> EvalResult<Vec<Sample>> {
        // Caching by SeriesFingerprint is not supported with the current SeriesQuerier
        // abstraction (which operates on selectors). Keep the API but return an error
        // if called. The cached reader still forwards selector-based queries.
        Err(EvaluationError::InternalError(
            "CachedQueryReader.samples is not supported in this build".to_string(),
        ))
    }
}

impl<'reader, R: SeriesQuerier> SeriesQuerier for CachedQueryReader<'reader, R> {
    fn query(
        &self,
        selector: &VectorSelector,
        timestamp: i64,
        options: QueryOptions,
    ) -> PromqlResult<Vec<InstantSample>> {
        self.inner.query(selector, timestamp, options)
    }

    fn query_range(
        &self,
        selector: &VectorSelector,
        start_ms: i64,
        end_ms: i64,
        options: QueryOptions,
    ) -> PromqlResult<Vec<RangeSample>> {
        self.inner.query_range(selector, start_ms, end_ms, options)
    }
}

pub(crate) mod test_utils {
    use super::*;
    use crate::commands::parse_metric_name;
    use crate::common::Sample;
    use crate::promql::Labels;
    pub(crate) use crate::promql::engine::mock_series_querier::MockSeriesQuerier;
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

        pub(crate) fn build(self) -> MockSeriesQuerier {
            self.inner.build()
        }
    }

    /// Builder for creating MockQueryReader instances from test data.
    /// Supports multi-bucket scenarios.
    pub(crate) struct MockMultiBucketQueryReaderBuilder {
        reader: MockSeriesQuerier,
    }

    impl MockMultiBucketQueryReaderBuilder {
        pub(crate) fn new() -> Self {
            Self {
                reader: MockSeriesQuerier::new(),
            }
        }

        /// Add a sample with labels to a specific bucket. If a series with the same labels already exists globally,
        /// the existing series ID is reused. Otherwise, a new series is created with a global ID.
        pub(crate) fn add_sample(&mut self, labels: &Labels, sample: Sample) -> &mut Self {
            self.reader.add_sample(labels, sample);
            self
        }

        pub(crate) fn build(self) -> MockSeriesQuerier {
            self.reader
        }
    }
}
