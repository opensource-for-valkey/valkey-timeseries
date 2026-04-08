use crate::common::Sample;
use crate::common::hash::IntMap;
use crate::labels::filters::SeriesSelector;
use crate::labels::{Label, MetricName};
use crate::promql::engine::SeriesQuerier;
use crate::promql::hashers::SeriesFingerprint;
use crate::promql::model::InstantSample;
use crate::promql::{Labels, PromqlResult, QueryError, QueryOptions, RangeSample};
use crate::series::index::Postings;
use crate::series::{SeriesRef, TimeSeries};
use ahash::AHashMap;
use orx_parallel::IterIntoParIter;
use orx_parallel::ParIter;
use orx_parallel::ParIterResult;
use promql_parser::parser::VectorSelector;
use std::sync::RwLock;
use valkey_module::ValkeyResult;

struct QuerierInner {
    series: IntMap<SeriesRef, TimeSeries>,
    postings: Postings,
    next_id: SeriesRef,
    fingerprint_to_id: AHashMap<SeriesFingerprint, SeriesRef>,
}

impl Default for QuerierInner {
    fn default() -> Self {
        QuerierInner {
            series: IntMap::default(),
            postings: Postings::default(),
            next_id: 1, // start IDs at 1 to avoid reserved 0
            fingerprint_to_id: AHashMap::default(),
        }
    }
}

/// A mock QueryReader for testing that holds data in memory.
/// Use `MockQueryReaderBuilder` to construct instances.
pub(crate) struct MockSeriesQuerier {
    inner: RwLock<QuerierInner>,
}

impl MockSeriesQuerier {
    pub(crate) fn new() -> Self {
        Self {
            inner: RwLock::new(QuerierInner::default()),
        }
    }

    /// Add a sample with labels. If a series with the same labels already exists globally,
    /// the existing series ID is reused. Otherwise, a new series is created with a global ID.
    pub(crate) fn add_sample(&self, labels: &Labels, sample: Sample) {
        let mut inner = self.inner.write().unwrap();
        let fingerprint = labels.get_fingerprint();

        let id = if let Some(&existing) = inner.fingerprint_to_id.get(&fingerprint) {
            existing
        } else {
            let id = inner.next_id;
            inner.next_id += 1;

            let ts = TimeSeries {
                id,
                labels: labels.as_ref().into(),
                ..Default::default()
            };

            inner.series.insert(id, ts);
            inner.fingerprint_to_id.insert(fingerprint, id);
            id
        };

        // To avoid simultaneous mutable borrows of different fields of
        // `inner`, take the TimeSeries out of the map, modify it, update the
        // postings, then put it back.
        if let Some(mut ts) = inner.series.remove(&id) {
            // update the timeseries
            ts.add(sample.timestamp, sample.value, None);

            // update postings using the owned `ts` reference
            let key = format!("ts:{id}");
            inner.postings.index_timeseries(&ts, key.as_bytes());

            // put the timeseries back into the map
            inner.series.insert(id, ts);
        }
    }

    fn select_series<F, R>(&self, selector: &VectorSelector, f: F) -> PromqlResult<Vec<R>>
    where
        F: Fn(&TimeSeries) -> ValkeyResult<Option<R>> + Send + Sync + Clone,
        R: Send + Sync,
    {
        let inner = self.inner.read().unwrap();

        let selector: SeriesSelector = SeriesSelector::from(selector);
        let series = inner
            .postings
            .postings_for_selectors(&[selector])
            .into_iter()
            .flat_map(|res| {
                res.iter()
                    .filter_map(|id| inner.series.get(&id))
                    .collect::<Vec<_>>()
            })
            .iter_into_par()
            .map(f)
            .into_fallible_result()
            .flat_map(|opt| opt)
            .collect();

        match series {
            Ok(series) => Ok(series),
            Err(e) => Err(QueryError::Execution(format!(
                "Error processing series: {:?}",
                e
            ))),
        }
    }

    pub fn clear(&self) {
        let mut inner = self.inner.write().unwrap();
        inner.series.clear();
        inner.next_id = 1;
        inner.postings.clear();
        inner.fingerprint_to_id.clear();
    }
}

impl SeriesQuerier for MockSeriesQuerier {
    fn query(
        &self,
        selector: &VectorSelector,
        timestamp: i64,
        _options: QueryOptions,
    ) -> PromqlResult<Vec<InstantSample>> {
        // Mock querier ignores the deadline for now (cooperative cancellation could be
        // implemented in tests by checking `_deadline`), and behaves like the
        // original implementation.
        self.select_series(selector, |ts| {
            let labels: Vec<Label> = metric_name_to_labels(&ts.labels);
            let Some(sample) = ts.get_sample(timestamp)? else {
                return Ok(None);
            };
            Ok(Some(InstantSample {
                labels: Labels::new(labels),
                value: sample.value,
                timestamp_ms: timestamp,
            }))
        })
    }

    fn query_range(
        &self,
        selector: &VectorSelector,
        start_ms: i64,
        end_ms: i64,
        _options: QueryOptions,
    ) -> PromqlResult<Vec<RangeSample>> {
        // As with `query`, this mock ignores the deadline and returns the full range.
        self.select_series(selector, |ts| {
            let labels: Labels = (&ts.labels).into();
            let samples = ts
                .get_range(start_ms, end_ms)
                .into_iter()
                .map(|s| Sample {
                    timestamp: s.timestamp,
                    value: s.value,
                })
                .collect();
            Ok(Some(RangeSample { labels, samples }))
        })
    }
}

fn metric_name_to_labels(metric_name: &MetricName) -> Vec<Label> {
    metric_name
        .iter()
        .map(|label| Label::new(label.name.to_string(), label.value.to_string()))
        .collect()
}
