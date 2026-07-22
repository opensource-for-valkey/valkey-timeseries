use crate::common::{MultiSample, Sample};
use crate::iterators::{create_range_iterator, create_row_iterator};
use crate::series::request_types::RangeOptions;
use crate::series::{TimeSeries, get_latest_compaction_sample};
use valkey_module::Context;

/// Determines and retrieves the latest compaction sample if needed
/// (aggregation-agnostic; shared by the sample and row range iterators, and by the
/// multi-key path in `series::mrange` — see the note there).
pub(crate) fn get_range_latest_sample(
    ctx: Option<&Context>,
    series: &TimeSeries,
    options: &RangeOptions,
) -> Option<Sample> {
    let needs_latest = options.latest && series.is_compaction();
    if !needs_latest {
        return None;
    }

    let context = ctx.expect("Context is required when LATEST option is used");
    let (mut start_ts, mut end_ts) = options.date_range.get_timestamps(None);

    if !series.retention.is_zero() {
        let min_ts = series.get_min_timestamp();
        start_ts = start_ts.max(min_ts);
        end_ts = start_ts.max(end_ts);
    }

    // a partial compaction sample, if it exists, is beyond the last stored sample
    if end_ts > series.last_timestamp() {
        get_latest_compaction_sample(context, series)
            .filter(|sample| sample.timestamp >= start_ts && sample.timestamp <= end_ts)
            .filter(|sample| {
                // validate timestamp filter
                if let Some(ts_filter) = options.timestamp_filter.as_ref()
                    && !ts_filter.contains(&sample.timestamp)
                {
                    return false;
                }
                true
            })
    } else {
        None
    }
}

/// An iterator over a TimeSeries based on RangeOptions.
/// This iterator handles various filters (timestamp, value) and aggregations, as well as the
/// special "LATEST" option for compaction series.
pub struct TimeSeriesRangeIterator<'a> {
    inner: Box<dyn Iterator<Item = Sample> + 'a>,
    size_hint: (usize, Option<usize>),
}

impl<'a> TimeSeriesRangeIterator<'a> {
    /// Creates a new TimeSeriesRangeIterator based on the provided options.
    /// This is slightly complex due to the various combinations of filters and aggregations, and
    /// the desire to minimize boxing.
    /// ctx is made optional to allow for easier unit testing without a full Valkey context.
    pub fn new(
        ctx: Option<&'a Context>,
        series: &'a TimeSeries,
        options: &RangeOptions,
        is_reverse: bool,
    ) -> Self {
        let latest = get_range_latest_sample(ctx, series, options);
        let size_hint = Self::calculate_size_hint(options, latest.is_some());
        let inner = create_range_iterator(series, options, &None, latest, is_reverse);

        Self { inner, size_hint }
    }

    /// Calculates the size hint based on options and whether a latest sample exists.
    fn calculate_size_hint(options: &RangeOptions, has_latest: bool) -> (usize, Option<usize>) {
        if let Some(ts_filter) = options.timestamp_filter.as_ref() {
            let max_elems = ts_filter.len() + if has_latest { 1 } else { 0 };
            if let Some(count) = options.count {
                (0, Some(count.min(max_elems)))
            } else {
                (0, Some(max_elems))
            }
        } else {
            (0, options.count)
        }
    }
}

impl<'a> TimeSeriesRangeIterator<'a> {
    fn len_hint(&self) -> (usize, Option<usize>) {
        self.size_hint
    }
}

impl<'a> Iterator for TimeSeriesRangeIterator<'a> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

/// Row-yielding sibling of [`TimeSeriesRangeIterator`] for multi-aggregation
/// queries: each item is one bucket with a value per aggregator.
pub struct TimeSeriesRangeRowIterator<'a> {
    inner: Box<dyn Iterator<Item = MultiSample> + 'a>,
}

impl<'a> TimeSeriesRangeRowIterator<'a> {
    pub fn new(
        ctx: Option<&'a Context>,
        series: &'a TimeSeries,
        options: &RangeOptions,
        is_reverse: bool,
    ) -> Self {
        let latest = get_range_latest_sample(ctx, series, options);
        let inner = create_row_iterator(series, options, latest, is_reverse);
        Self { inner }
    }
}

impl<'a> Iterator for TimeSeriesRangeRowIterator<'a> {
    type Item = MultiSample;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

/// An iterator that yields the latest sample from a compaction series if it exists.
/// This is used specifically for the "LATEST" option in range queries. This simplifies the logic by
/// isolating the latest sample retrieval so that the base sample iterator does not need to handle
/// this special case. We simply `chain` this iterator with others as needed.
pub(crate) struct CompactionLatestSampleIterator<'a> {
    context: &'a Context,
    series: &'a TimeSeries,
    done: bool,
}

impl<'a> CompactionLatestSampleIterator<'a> {
    pub fn new(context: &'a Context, series: &'a TimeSeries) -> Self {
        Self {
            context,
            series,
            done: false,
        }
    }
}
impl<'a> Iterator for CompactionLatestSampleIterator<'a> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.done {
            self.done = true;
            return get_latest_compaction_sample(self.context, self.series);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregators::AggregationType;
    use crate::common::{Sample, Timestamp};
    use crate::series::request_types::AggregationOptions;
    use crate::series::{TimeSeries, TimestampRange, ValueFilter};

    // Helper function to create a test time series with sample data
    fn create_test_series() -> TimeSeries {
        let mut series = TimeSeries::default();

        // Add some test samples
        for i in 0..10 {
            let _ = series.add(i * 1000, i as f64, None);
        }

        series
    }

    fn date_range(start: Timestamp, end: Timestamp) -> TimestampRange {
        TimestampRange::from_timestamps(start, end).unwrap()
    }

    #[test]
    fn test_basic_iteration() {
        let series = create_test_series();
        let options = RangeOptions {
            date_range: date_range(0, 10000),
            count: None,
            aggregation: None,
            timestamp_filter: None,
            value_filter: None,
            latest: false,
        };

        let iter = TimeSeriesRangeIterator::new(None, &series, &options, false);
        let samples: Vec<Sample> = iter.collect();

        assert_eq!(samples.len(), 10);
        assert_eq!(samples[0].timestamp, 0);
        assert_eq!(samples[9].timestamp, 9000);
    }

    #[test]
    fn test_reverse_iteration() {
        let series = create_test_series();
        let options = RangeOptions {
            date_range: date_range(0, 10000),
            count: None,
            aggregation: None,
            timestamp_filter: None,
            value_filter: None,
            latest: false,
        };

        let iter = TimeSeriesRangeIterator::new(None, &series, &options, true);
        let samples: Vec<Sample> = iter.collect();

        assert_eq!(samples.len(), 10);
        assert_eq!(samples[0].timestamp, 9000);
        assert_eq!(samples[9].timestamp, 0);
    }

    #[test]
    fn test_iteration_with_count_limit() {
        let series = create_test_series();
        let options = RangeOptions {
            date_range: date_range(0, 10000),
            count: Some(5),
            aggregation: None,
            timestamp_filter: None,
            value_filter: None,
            latest: false,
        };

        let iter = TimeSeriesRangeIterator::new(None, &series, &options, false);
        let samples: Vec<Sample> = iter.collect();

        assert!(samples.len() <= 5);
    }

    #[test]
    fn test_iteration_with_timestamp_range() {
        let series = create_test_series();
        let options = RangeOptions {
            date_range: date_range(2000, 6000),
            count: None,
            aggregation: None,
            timestamp_filter: None,
            value_filter: None,
            latest: false,
        };

        let iter = TimeSeriesRangeIterator::new(None, &series, &options, false);
        let samples: Vec<Sample> = iter.collect();

        for sample in &samples {
            assert!(sample.timestamp >= 2000 && sample.timestamp <= 6000);
        }
    }

    #[test]
    fn test_iteration_with_value_filter() {
        let series = create_test_series();
        let options = RangeOptions {
            date_range: date_range(0, 10000),
            count: None,
            aggregation: None,
            timestamp_filter: None,
            value_filter: Some(ValueFilter { min: 3.0, max: 7.0 }),
            latest: false,
        };

        let iter = TimeSeriesRangeIterator::new(None, &series, &options, false);
        let samples: Vec<Sample> = iter.collect();

        for sample in &samples {
            assert!(sample.value >= 3.0 && sample.value <= 7.0);
        }
    }

    #[test]
    fn test_iteration_with_timestamp_filter() {
        let mut series = TimeSeries::default();
        series.add(1000, 10.1, None);
        series.add(2000, 20.2, None);
        series.add(3000, 30.3, None);
        series.add(4000, 40.4, None);
        series.add(5000, 50.5, None);

        let options = RangeOptions {
            date_range: date_range(0, 10000),
            count: None,
            aggregation: None,
            timestamp_filter: Some(vec![1000, 3000, 5000]),
            value_filter: None,
            latest: false,
        };

        let iter = TimeSeriesRangeIterator::new(None, &series, &options, false);
        let samples: Vec<Sample> = iter.collect();

        assert_eq!(samples.len(), 3);
        assert_eq!(samples[0].timestamp, 1000);
        assert_eq!(samples[1].timestamp, 3000);
        assert_eq!(samples[2].timestamp, 5000);

        // Test with a single timestamp in filter
        let options_single = RangeOptions {
            date_range: date_range(0, 10000),
            count: None,
            aggregation: None,
            timestamp_filter: Some(vec![2000]),
            value_filter: None,
            latest: false,
        };

        let iter_single = TimeSeriesRangeIterator::new(None, &series, &options_single, false);
        let samples_single: Vec<Sample> = iter_single.collect();
        assert_eq!(samples_single.len(), 1);
        assert_eq!(samples_single[0].timestamp, 2000);

        // test the reverse iteration with a timestamp filter
        let rev_iter = TimeSeriesRangeIterator::new(None, &series, &options, true);
        let rev_samples: Vec<Sample> = rev_iter.collect();

        assert_eq!(rev_samples.len(), 3);
        assert_eq!(rev_samples[0].timestamp, 5000);
        assert_eq!(rev_samples[1].timestamp, 3000);
        assert_eq!(rev_samples[2].timestamp, 1000);
    }

    #[test]
    fn test_iteration_with_aggregation() {
        let series = create_test_series();
        let options = RangeOptions {
            date_range: date_range(0, 10000),
            count: None,
            aggregation: Some(AggregationOptions {
                aggregations: smallvec::smallvec![AggregationType::Avg.into()],
                bucket_duration: 2000,
                timestamp_output: Default::default(),
                alignment: Default::default(),
                ..Default::default()
            }),
            timestamp_filter: None,
            value_filter: None,
            latest: false,
        };

        let iter = TimeSeriesRangeIterator::new(None, &series, &options, false);
        let samples: Vec<Sample> = iter.collect();

        // With bucket duration of 2000ms, we should have fewer samples than the original
        assert!(samples.len() < 10);
    }

    #[test]
    fn test_iteration_with_nan_values_and_different_aggregations() {
        let mut series = TimeSeries::default();
        series.add(0, 1.0, None);
        series.add(500, f64::NAN, None);
        series.add(1000, 3.0, None);
        series.add(2000, f64::NAN, None);
        series.add(3000, 7.0, None);

        let cases = [
            (AggregationType::Min, 1.0),
            (AggregationType::Max, 3.0),
            (AggregationType::Sum, 4.0),
            (AggregationType::Avg, 2.0),
            (AggregationType::First, 1.0),
            (AggregationType::Last, 3.0),
            (AggregationType::Range, 2.0),
            (AggregationType::CountAll, 3.0),
            (AggregationType::CountNan, 1.0),
            (AggregationType::Count, 2.0),
        ];

        for (aggregation, expected_first_bucket_value) in cases {
            let options = RangeOptions {
                date_range: date_range(0, 4000),
                count: None,
                aggregation: Some(AggregationOptions {
                    aggregations: smallvec::smallvec![aggregation.into()],
                    bucket_duration: 2000,
                    timestamp_output: Default::default(),
                    alignment: Default::default(),
                    ..Default::default()
                }),
                timestamp_filter: None,
                value_filter: None,
                latest: false,
            };

            let samples: Vec<Sample> =
                TimeSeriesRangeIterator::new(None, &series, &options, false).collect();

            assert_eq!(
                samples.len(),
                2,
                "failed for aggregation: {:?}",
                aggregation
            );
            assert_eq!(
                samples[0].timestamp, 0,
                "failed for aggregation: {:?}",
                aggregation
            );
            assert_eq!(
                samples[0].value, expected_first_bucket_value,
                "failed for aggregation: {:?}",
                aggregation
            );
        }
    }

    #[test]
    fn test_iteration_with_last_aggregation_and_nan_values() {
        let mut series = TimeSeries::default();
        series.add(0, 1.0, None);
        series.add(500, f64::NAN, None);
        series.add(1000, 3.0, None);
        series.add(2000, 5.0, None);
        series.add(2500, f64::NAN, None);
        series.add(3000, 7.0, None);
        series.add(3500, f64::NAN, None);

        let options = RangeOptions {
            date_range: date_range(0, 4000),
            count: None,
            aggregation: Some(AggregationOptions {
                aggregations: smallvec::smallvec![AggregationType::Last.into()],
                bucket_duration: 2000,
                timestamp_output: Default::default(),
                alignment: Default::default(),
                ..Default::default()
            }),
            timestamp_filter: None,
            value_filter: None,
            latest: false,
        };

        let samples: Vec<Sample> =
            TimeSeriesRangeIterator::new(None, &series, &options, false).collect();

        // Bucket [0, 2000): last non-NaN is 3.0 at t=1000
        // Bucket [2000, 4000): last non-NaN is 7.0 at t=3000 (3500 is NaN)
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].timestamp, 0);
        assert_eq!(samples[0].value, 3.0);
        assert_eq!(samples[1].timestamp, 2000);
        assert_eq!(samples[1].value, 7.0);
    }

    #[test]
    fn test_reverse_iteration_with_last_aggregation_and_nan_values() {
        let mut series = TimeSeries::default();
        series.add(0, 1.0, None);
        series.add(500, f64::NAN, None);
        series.add(1000, 3.0, None);
        series.add(2000, 5.0, None);
        series.add(2500, f64::NAN, None);
        series.add(3000, 7.0, None);
        series.add(3500, f64::NAN, None);

        let options = RangeOptions {
            date_range: date_range(0, 4000),
            count: None,
            aggregation: Some(AggregationOptions {
                aggregations: smallvec::smallvec![AggregationType::Last.into()],
                bucket_duration: 2000,
                timestamp_output: Default::default(),
                alignment: Default::default(),
                ..Default::default()
            }),
            timestamp_filter: None,
            value_filter: None,
            latest: false,
        };

        let samples: Vec<Sample> =
            TimeSeriesRangeIterator::new(None, &series, &options, true).collect();

        // Reverse order: bucket [2000, 4000) first, then [0, 2000)
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].timestamp, 2000);
        assert_eq!(samples[0].value, 7.0);
        assert_eq!(samples[1].timestamp, 0);
        assert_eq!(samples[1].value, 3.0);
    }

    #[test]
    fn test_iteration_with_last_aggregation_all_nans_in_bucket() {
        let mut series = TimeSeries::default();
        // Bucket [0, 2000): all NaN
        series.add(0, f64::NAN, None);
        series.add(500, f64::NAN, None);
        series.add(1000, f64::NAN, None);
        // Bucket [2000, 4000): has valid values
        series.add(2000, 5.0, None);
        series.add(2500, f64::NAN, None);
        series.add(3000, 7.0, None);

        let options = RangeOptions {
            date_range: date_range(0, 4000),
            count: None,
            aggregation: Some(AggregationOptions {
                aggregations: smallvec::smallvec![AggregationType::Last.into()],
                bucket_duration: 2000,
                timestamp_output: Default::default(),
                alignment: Default::default(),
                ..Default::default()
            }),
            timestamp_filter: None,
            value_filter: None,
            latest: false,
        };

        let samples: Vec<Sample> =
            TimeSeriesRangeIterator::new(None, &series, &options, false).collect();

        // Bucket [0, 2000) has only NaNs — result should be NaN
        // Bucket [2000, 4000) last non-NaN is 7.0
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].timestamp, 0);
        assert!(samples[0].value.is_nan());
        assert_eq!(samples[1].timestamp, 2000);
        assert_eq!(samples[1].value, 7.0);
    }

    #[test]
    fn test_iteration_with_last_aggregation_leading_nan() {
        let mut series = TimeSeries::default();
        // Bucket [0, 2000): NaN first, then valid
        series.add(0, f64::NAN, None);
        series.add(1000, 3.0, None);
        series.add(1500, f64::NAN, None);

        let options = RangeOptions {
            date_range: date_range(0, 2000),
            count: None,
            aggregation: Some(AggregationOptions {
                aggregations: smallvec::smallvec![AggregationType::Last.into()],
                bucket_duration: 2000,
                timestamp_output: Default::default(),
                alignment: Default::default(),
                ..Default::default()
            }),
            timestamp_filter: None,
            value_filter: None,
            latest: false,
        };

        let samples: Vec<Sample> =
            TimeSeriesRangeIterator::new(None, &series, &options, false).collect();

        // Last non-NaN in bucket is 3.0; trailing NaN at 1500 should not override
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].timestamp, 0);
        assert_eq!(samples[0].value, 3.0);
    }

    #[test]
    fn test_empty_series() {
        let series = TimeSeries::default();
        let options = RangeOptions {
            date_range: date_range(0, 10000),
            count: None,
            aggregation: None,
            timestamp_filter: None,
            value_filter: None,
            latest: false,
        };

        let iter = TimeSeriesRangeIterator::new(None, &series, &options, false);
        let samples: Vec<Sample> = iter.collect();

        assert_eq!(samples.len(), 0);
    }

    #[test]
    fn test_iteration_with_retention() {
        let mut series = TimeSeries::default();

        // Add samples with timestamps
        for i in 0..10 {
            let _ = series.add(i * 1000, i as f64, None);
        }

        let options = RangeOptions {
            date_range: date_range(0, 10000),
            count: None,
            aggregation: None,
            timestamp_filter: None,
            value_filter: None,
            latest: false,
        };

        let iter = TimeSeriesRangeIterator::new(None, &series, &options, false);
        let samples: Vec<Sample> = iter.collect();

        // Due to retention, older samples may be filtered
        if !samples.is_empty() {
            assert!(samples[0].timestamp >= series.get_min_timestamp());
        }
    }

    #[test]
    fn test_size_hint() {
        let series = create_test_series();
        let options = RangeOptions {
            date_range: date_range(0, 10000),
            count: Some(5),
            aggregation: None,
            timestamp_filter: None,
            value_filter: None,
            latest: false,
        };

        let iter = TimeSeriesRangeIterator::new(None, &series, &options, false);
        let hint = iter.len_hint();

        assert_eq!(hint.0, 0);
        assert_eq!(hint.1, Some(5));
    }

    #[test]
    fn test_size_hint_without_count() {
        let series = create_test_series();
        let options = RangeOptions {
            date_range: date_range(0, 10000),
            count: None,
            aggregation: None,
            timestamp_filter: None,
            value_filter: None,
            latest: false,
        };

        let iter = TimeSeriesRangeIterator::new(None, &series, &options, false);
        let hint = iter.len_hint();

        assert_eq!(hint.0, 0);
        assert_eq!(hint.1, None);
    }

    #[test]
    fn test_multiple_filters_combined() {
        let series = create_test_series();
        let options = RangeOptions {
            date_range: date_range(2000, 8000),
            count: Some(3),
            aggregation: None,
            timestamp_filter: None,
            value_filter: Some(ValueFilter { min: 2.0, max: 8.0 }),
            latest: false,
        };

        let iter = TimeSeriesRangeIterator::new(None, &series, &options, false);
        let samples: Vec<Sample> = iter.collect();

        assert!(samples.len() <= 3);
        for sample in &samples {
            assert!(sample.timestamp >= 2000 && sample.timestamp <= 8000);
            assert!(sample.value >= 2.0 && sample.value <= 8.0);
        }
    }

    #[test]
    fn test_iterator_is_lazy() {
        let series = create_test_series();
        let options = RangeOptions {
            date_range: date_range(0, 10000),
            count: None,
            aggregation: None,
            timestamp_filter: None,
            value_filter: None,
            latest: false,
        };

        let mut iter = TimeSeriesRangeIterator::new(None, &series, &options, false);

        // Taking only a few items shouldn't process the entire series
        let first = iter.next();
        assert!(first.is_some());
        assert_eq!(first.unwrap().timestamp, 0);

        let second = iter.next();
        assert!(second.is_some());
        assert_eq!(second.unwrap().timestamp, 1000);
    }

    #[test]
    fn test_boundary_timestamps() {
        let series = create_test_series();

        // Test the exact boundary match
        let options = RangeOptions {
            date_range: date_range(3000, 3000),
            count: None,
            aggregation: None,
            timestamp_filter: None,
            value_filter: None,
            latest: false,
        };

        let iter = TimeSeriesRangeIterator::new(None, &series, &options, false);
        let samples: Vec<Sample> = iter.collect();

        // Should get exactly one sample at timestamp 3000
        assert!(!samples.is_empty());
        assert!(samples.iter().any(|s| s.timestamp == 3000));
    }
}
