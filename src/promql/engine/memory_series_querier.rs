use crate::common::Sample;
use crate::common::hash::IntMap;
use crate::common::threads::IterIntoParRayon;
use crate::labels::filters::SeriesSelector;
use crate::labels::{Labels, SeriesFingerprint};
use crate::promql::EvalLabels;
use crate::promql::engine::QueryReader;
use crate::promql::engine::label_profile::{
    LabelProfile, LabelProfileBuilder, ProfileStep, ProfileWalk, ProfileWindow, profiled_series_cap,
};
use crate::promql::engine::query_reader::{AggregationOutcome, AggregationRequest};
use crate::promql::model::InstantSample;
use crate::promql::time::duration_ms;
use crate::promql::{PromqlResult, QueryError, QueryOptions, RangeSample};
use crate::series::index::Postings;
use crate::series::{SeriesRef, TimeSeries};
use ahash::AHashMap;
use orx_parallel::Par;
use orx_parallel::ParResult;
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
pub struct MemorySeriesQuerier {
    inner: RwLock<QuerierInner>,
}

impl Default for MemorySeriesQuerier {
    fn default() -> Self {
        Self::new()
    }
}

impl MemorySeriesQuerier {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(QuerierInner::default()),
        }
    }

    /// Add a sample with labels. If a series with the same labels already exists globally,
    /// the existing series ID is reused. Otherwise, a new series is created with a global ID.
    pub fn add_sample(&self, labels: &Labels, sample: Sample) {
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
            // Last write wins, as when a promqltest `load` lists one series
            // twice: the later line's value is the one Prometheus keeps.
            ts.add(
                sample.timestamp,
                sample.value,
                Some(crate::series::DuplicatePolicy::KeepLast),
            );

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
            .iter_into_par_rayon()
            .map(&f)
            .into_fallible()
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
}

impl QueryReader for MemorySeriesQuerier {
    fn query(
        &self,
        selector: &VectorSelector,
        timestamp: i64,
        _options: QueryOptions,
    ) -> PromqlResult<Vec<InstantSample<EvalLabels>>> {
        // Mock querier ignores the deadline for now (cooperative cancellation could be
        // implemented in tests by checking `_deadline`), and behaves like the
        // original implementation.
        // Implement lookback semantics: return the latest sample with
        // timestamp <= `timestamp` and timestamp > (timestamp - lookback_delta).
        // `QueryOptions::lookback_delta` is provided in `_options`.
        let lookback_ms = duration_ms(_options.lookback_delta);
        let start_inclusive = timestamp.saturating_sub(lookback_ms).saturating_add(1);

        self.select_series(selector, |ts| {
            // Fetch samples in the inclusive range [start_inclusive, timestamp].
            let samples = ts.get_range(start_inclusive, timestamp);
            if samples.is_empty() {
                return Ok(None);
            }

            // `get_range` returns samples sorted ascending; take the last (latest)
            // sample that is <= timestamp and > (timestamp - lookback_delta).
            if let Some(s) = samples.last() {
                return Ok(Some(InstantSample {
                    labels: EvalLabels::interned(&ts.labels),
                    value: s.value,
                    timestamp_ms: s.timestamp,
                }));
            }

            Ok(None)
        })
    }

    fn query_range(
        &self,
        selector: &VectorSelector,
        start_ms: i64,
        end_ms: i64,
        _options: QueryOptions,
    ) -> PromqlResult<Vec<RangeSample<EvalLabels>>> {
        // As with `query`, this mock ignores the deadline and returns the full range.
        self.select_series(selector, |ts| {
            let labels = EvalLabels::interned(&ts.labels);
            let samples: Vec<Sample> = ts
                .get_range(start_ms, end_ms)
                .into_iter()
                .map(|s| Sample {
                    timestamp: s.timestamp,
                    value: s.value,
                })
                .collect();

            if samples.is_empty() {
                return Ok(None);
            }

            Ok(Some(RangeSample { labels, samples }))
        })
    }

    /// There is nothing to push down to in memory, but answering `Raw` (rather
    /// than leaving the default `Unsupported`) still routes the evaluator
    /// through the push-down path, so the whole PromQL test suite exercises it.
    fn query_aggregation(
        &self,
        selector: &VectorSelector,
        timestamp: i64,
        _aggregation: &AggregationRequest,
        options: QueryOptions,
    ) -> PromqlResult<AggregationOutcome> {
        self.query(selector, timestamp, options)
            .map(AggregationOutcome::Raw)
    }

    fn label_profile(
        &self,
        selector: &VectorSelector,
        window: Option<ProfileWindow>,
        options: QueryOptions,
    ) -> PromqlResult<Option<LabelProfile>> {
        let inner = self.inner.read().unwrap();
        let selector: SeriesSelector = SeriesSelector::from(selector);
        let ids = inner
            .postings
            .postings_for_selectors(&[selector])
            .map_err(|e| QueryError::Execution(e.to_string()))?;
        let mut walk = ProfileWalk::new(window, profiled_series_cap(&options));
        let mut builder = LabelProfileBuilder::new();
        for id in ids.iter() {
            let Some(series) = inner.series.get(&id) else {
                continue;
            };
            match walk.step(series) {
                ProfileStep::Skip => {}
                ProfileStep::Stop => return Ok(None),
                ProfileStep::Take => {
                    builder.add_series(series.labels.iter().map(|l| (l.name, l.value)))
                }
            }
        }
        Ok(Some(builder.finish()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::promql::engine::label_profile::MAX_PROFILED_KEYS_PER_SERIES;
    use promql_parser::parser::{Expr, parse};

    fn selector(query: &str) -> VectorSelector {
        match parse(query).unwrap() {
            Expr::VectorSelector(vs) => vs,
            other => panic!("not a selector: {other:?}"),
        }
    }

    /// `m{region="eu"}` stopped reporting at 10 ms; `m{region="us"}` reports from 1 s on.
    fn retired_and_live() -> MemorySeriesQuerier {
        let querier = MemorySeriesQuerier::new();
        let retired = Labels::from_pairs(&[("__name__", "m"), ("region", "eu")]);
        let live = Labels::from_pairs(&[("__name__", "m"), ("region", "us")]);
        for t in 0..10 {
            querier.add_sample(&retired, Sample::new(t, 1.0));
            querier.add_sample(&live, Sample::new(1_000 + t, 1.0));
        }
        querier
    }

    fn options(max_series: usize) -> QueryOptions {
        QueryOptions {
            max_series,
            ..QueryOptions::default()
        }
    }

    fn filters(profile: &LabelProfile) -> Vec<String> {
        profile
            .common_filters()
            .iter()
            .map(ToString::to_string)
            .collect()
    }

    #[test]
    fn a_window_profiles_only_the_series_with_a_sample_in_it() {
        let querier = retired_and_live();
        let m = selector("m");

        let index = querier
            .label_profile(&m, None, options(0))
            .unwrap()
            .unwrap();
        assert_eq!(index.series, 2);
        assert_eq!(filters(&index), vec![r#"region=~"eu|us""#]);

        let live = ProfileWindow::new(500, 2_000);
        let windowed = querier
            .label_profile(&m, Some(live), options(0))
            .unwrap()
            .unwrap();
        assert_eq!(windowed.series, 1);
        assert_eq!(filters(&windowed), vec![r#"region="us""#]);

        let none = ProfileWindow::new(100, 200);
        let empty = querier
            .label_profile(&m, Some(none), options(0))
            .unwrap()
            .unwrap();
        assert_eq!(empty.series, 0);
    }

    #[test]
    fn the_cap_counts_only_the_series_in_the_window() {
        let querier = retired_and_live();
        let m = selector("m");
        assert_eq!(querier.label_profile(&m, None, options(1)).unwrap(), None);
        let windowed = querier
            .label_profile(&m, Some(ProfileWindow::new(500, 2_000)), options(1))
            .unwrap()
            .unwrap();
        assert_eq!(windowed.series, 1);
    }

    /// `n` series of `m` retired at 10 ms, then one live from 1 s on.
    fn mostly_retired(n: usize) -> MemorySeriesQuerier {
        let querier = MemorySeriesQuerier::new();
        for i in 0..n {
            let retired = Labels::from_pairs(&[("__name__", "m"), ("gen", &i.to_string())]);
            querier.add_sample(&retired, Sample::new(10, 1.0));
        }
        let live = Labels::from_pairs(&[("__name__", "m"), ("gen", "live")]);
        querier.add_sample(&live, Sample::new(1_000, 1.0));
        querier
    }

    #[test]
    fn retired_series_count_against_the_keys_a_profile_opens() {
        let m = selector("m");
        let live = Some(ProfileWindow::new(500, 2_000));
        // A cap of one series opens at most four keys: three retired and the
        // live one are profiled, a fourth retired one gives the profile up.
        let fits = mostly_retired(MAX_PROFILED_KEYS_PER_SERIES - 1);
        let profile = fits.label_profile(&m, live, options(1)).unwrap().unwrap();
        assert_eq!(profile.series, 1);
        assert_eq!(filters(&profile), vec![r#"gen="live""#]);

        let past = mostly_retired(MAX_PROFILED_KEYS_PER_SERIES);
        assert_eq!(past.label_profile(&m, live, options(1)).unwrap(), None);
    }
}
