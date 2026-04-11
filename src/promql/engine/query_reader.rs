use crate::common::Timestamp;
use crate::promql::hashers::{FingerprintHashMap, update_hasher_for_vector_selector};
use crate::promql::{
    EvalResult, PromqlResult, QueryOptions,
    model::{InstantSample, RangeSample},
};
use crate::series::SeriesRef;
use promql_parser::parser::VectorSelector;
use std::default::Default;
use std::hash::Hash;
use std::sync::{Arc, RwLock};
use twox_hash::xxhash3_128;

pub(crate) trait QueryReader: Send + Sync {
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
}

#[derive(Debug, Hash, Eq, PartialEq, Copy, Clone)]
pub(in crate::promql) struct SeriesSampleRangeKey(u128);

impl SeriesSampleRangeKey {
    fn new(vs: &VectorSelector, start: i64, end: i64) -> Self {
        let mut hasher = xxhash3_128::Hasher::new();
        update_hasher_for_vector_selector(vs, &mut hasher);
        hasher.write(&start.to_le_bytes());
        hasher.write(&end.to_le_bytes());
        Self(hasher.finish_128())
    }
}

#[derive(Default)]
pub(in crate::promql) struct QueryReaderEvalCache {
    samples: RwLock<FingerprintHashMap<Arc<Vec<InstantSample>>>>,
}

impl QueryReaderEvalCache {
    pub(crate) fn new() -> Self {
        Self {
            samples: Default::default(),
        }
    }

    pub fn cache_samples(
        &self,
        vs: &VectorSelector,
        start: Timestamp,
        end: Timestamp,
        samples: Vec<InstantSample>,
    ) -> bool {
        let key = SeriesSampleRangeKey::new(vs, start, end);
        self.cache_samples_raw(key, Arc::new(samples))
    }

    fn cache_samples_raw(
        &self,
        key: SeriesSampleRangeKey,
        samples: Arc<Vec<InstantSample>>,
    ) -> bool {
        let mut guard = self.samples.write().unwrap();
        if let Some(current) = guard.get(&key.0) {
            // merge current and new samples into a new Vec and replace
            let mut new_samples = (**current).clone();
            new_samples.extend((*samples).iter().cloned());
            guard.insert(key.0, Arc::new(new_samples));
            false
        } else {
            guard.insert(key.0, samples).is_none()
        }
    }

    pub(crate) fn get_samples(
        &self,
        vs: &VectorSelector,
        start: Timestamp,
        end: Timestamp,
    ) -> Option<Arc<Vec<InstantSample>>> {
        let key = SeriesSampleRangeKey::new(vs, start, end);
        self.get_samples_raw(key)
    }

    pub(crate) fn get_samples_raw(
        &self,
        key: SeriesSampleRangeKey,
    ) -> Option<Arc<Vec<InstantSample>>> {
        let guard = self.samples.read().unwrap();
        guard.get(&key.0).cloned()
    }
}

pub struct CachedQueryReader<'querier, Q: QueryReader> {
    inner: &'querier Q,
    cache: QueryReaderEvalCache,
}

impl<'reader, R: QueryReader> CachedQueryReader<'reader, R> {
    pub(crate) fn new(reader: &'reader R) -> Self {
        Self {
            inner: reader,
            cache: QueryReaderEvalCache::new(),
        }
    }

    pub(crate) fn samples(
        &self,
        selector: &VectorSelector,
        start_ms: i64,
        end_ms: i64,
    ) -> EvalResult<Arc<Vec<InstantSample>>> {
        let key = SeriesSampleRangeKey::new(selector, start_ms, end_ms);
        if let Some(samples) = self.cache.get_samples_raw(key) {
            return Ok(samples);
        }
        let samples = self
            .inner
            .query(selector, start_ms, QueryOptions::default())?;
        let arc = Arc::new(samples);
        // cache_samples_raw takes an Arc<Vec<Sample>>
        self.cache.cache_samples_raw(key, arc.clone());
        Ok(arc)
    }
}

impl<'reader, R: QueryReader> QueryReader for CachedQueryReader<'reader, R> {
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
