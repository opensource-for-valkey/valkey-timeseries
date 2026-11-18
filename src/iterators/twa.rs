//! The `twa` branch of the range and row pipelines. `utils.rs` dispatches here before it plans
//! any read of its own, since `twa` needs the series for its neighbors.

use super::TimestampFilterIterator;
use super::utils::{
    TailIter, create_filtered_sample_iterator, create_row_iterator, empty_fill_bounds,
    is_empty_fill_data,
};
use crate::aggregators::{ScanDirection, TwaContext, TwaNeighbors, TwaRowIterator};
use crate::common::{MultiSample, Sample, Timestamp};
use crate::series::request_types::RangeOptions;
use crate::series::{SeriesSampleIterator, TimeSeries};
use itertools::Either;
use smallvec::SmallVec;

/// Up to two real samples on each side of the window that pass the query's filters, found in
/// the live series. FILTER_BY_TS keeps its whole list: the timestamps outside the window are
/// where the neighbors come from.
fn twa_neighbors(series: &TimeSeries, options: &RangeOptions) -> TwaNeighbors {
    if series.is_empty() {
        return TwaNeighbors::default();
    }
    let (start, end) = options.get_timestamp_range();
    let floor = series.get_min_timestamp();
    let value_filter = options.value_filter;
    let nearest = |from: Timestamp, to: Timestamp, is_reverse: bool| -> SmallVec<[Sample; 2]> {
        let samples = match options.timestamp_filter.as_deref() {
            Some(timestamps) => Either::Left(TimestampFilterIterator::new(
                series, timestamps, from, to, is_reverse,
            )),
            None => Either::Right(SeriesSampleIterator::new(
                series,
                from.max(floor),
                to,
                is_reverse,
            )),
        };
        samples
            .filter(|sample| is_empty_fill_data(sample, value_filter))
            .take(2)
            .collect()
    };
    let mut before = start
        .checked_sub(1)
        .map(|to| nearest(Timestamp::MIN, to, true))
        .unwrap_or_default();
    before.reverse();
    let after = end
        .checked_add(1)
        .filter(|_| series.last_sample.is_some_and(|last| last.timestamp > end))
        .map(|from| nearest(from, Timestamp::MAX, false))
        .unwrap_or_default();
    TwaNeighbors { before, after }
}

/// [`twa_rows`] for a command that visits buckets in the order of its reply.
pub(super) fn twa_reply_rows<'a>(
    series: &'a TimeSeries,
    options: &RangeOptions,
    latest_sample: Option<Sample>,
    is_reverse: bool,
    tail_limit: Option<usize>,
) -> Box<dyn Iterator<Item = MultiSample> + 'a> {
    let scan = ScanDirection::from_reverse(is_reverse);
    twa_rows(series, options, latest_sample, scan, is_reverse, tail_limit)
}

/// Rows of a query with a `twa` column, in time order. `scan` is the order the command visits
/// buckets in, which decides what brackets a bucket; `reply_reverse` is the order of the reply,
/// which `last` needs to know. The caller reverses and applies COUNT.
fn twa_rows<'a>(
    series: &'a TimeSeries,
    options: &RangeOptions,
    latest_sample: Option<Sample>,
    scan: ScanDirection,
    reply_reverse: bool,
    tail_limit: Option<usize>,
) -> Box<dyn Iterator<Item = MultiSample> + 'a> {
    let aggregation = options
        .aggregation
        .as_ref()
        .expect("twa_rows requires aggregation options");
    let (start, end) = options.get_timestamp_range();
    // LATEST joins the stored samples inside the iterator, where it brackets buckets without
    // supporting a gap.
    let stored = create_filtered_sample_iterator(series, options, None, None);
    let latest = latest_sample.filter(|sample| {
        options
            .value_filter
            .is_none_or(|f| f.is_match(sample.value))
    });
    let context = TwaContext {
        start,
        end,
        explicit_end: options.date_range.has_explicit_end(),
        neighbors: twa_neighbors(series, options),
        direction: scan,
        latest,
        empty_fill: empty_fill_bounds(series, options, reply_reverse, tail_limit),
    };
    Box::new(TwaRowIterator::new(stored, aggregation, context))
}

/// The rows of one key of a pivoted query, always oldest first. A `twa` column picks its
/// bracketing samples in the command's direction, so `newest_first` is the order of the
/// command; `keep_newest` is the number of newest rows the caller can use.
pub(crate) fn create_pivot_row_iterator<'a>(
    series: &'a TimeSeries,
    options: &RangeOptions,
    latest_sample: Option<Sample>,
    newest_first: bool,
    keep_newest: Option<usize>,
) -> Box<dyn Iterator<Item = MultiSample> + 'a> {
    if !options.has_twa() {
        return create_row_iterator(series, options, latest_sample, false);
    }
    let tail_limit = keep_newest.filter(|_| newest_first);
    let rows = twa_rows(
        series,
        options,
        latest_sample,
        ScanDirection::from_reverse(newest_first),
        false,
        tail_limit,
    );
    match tail_limit {
        Some(count) => Box::new(TailIter::new(rows, count)),
        None => rows,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregators::AggregationType;
    use crate::iterators::create_range_iterator;
    use crate::series::ValueFilter;
    use crate::tests::aggregation;

    fn series_of(samples: &[(i64, f64)]) -> TimeSeries {
        let mut series = TimeSeries::default();
        for &(ts, value) in samples {
            series.add(ts, value, None);
        }
        series
    }

    fn series() -> TimeSeries {
        series_of(&[(0, 1.0), (10, 3.0), (20, 9.0)])
    }

    fn options(columns: &[AggregationType]) -> RangeOptions {
        let mut options = RangeOptions::with_range(5, 15).unwrap();
        options.aggregation = Some(aggregation(columns, 10, false));
        options
    }

    fn rows_of(
        series: &TimeSeries,
        options: &RangeOptions,
        is_reverse: bool,
    ) -> Vec<(Timestamp, Vec<f64>)> {
        create_row_iterator(series, options, None, is_reverse)
            .map(|row| (row.timestamp, row.values.to_vec()))
            .collect()
    }

    fn rows(options: &RangeOptions, is_reverse: bool) -> Vec<(Timestamp, Vec<f64>)> {
        rows_of(&series(), options, is_reverse)
    }

    #[test]
    fn neighbors_come_from_filter_timestamps_outside_the_window() {
        // The sample at 20 closes TWA's [10, 15] interval: (3 + 6) / 2. `avg` reads only the
        // sample at 10, which is the only filter timestamp inside the window.
        let mut options = options(&[AggregationType::Twa, AggregationType::Avg]);
        options.timestamp_filter = Some(vec![0, 10, 20]);
        for is_reverse in [false, true] {
            assert_eq!(rows(&options, is_reverse), vec![(10, vec![4.5, 3.0])]);
        }
        // With no timestamp outside the window there is no neighbor: the bucket is its sample.
        options.timestamp_filter = Some(vec![10]);
        assert_eq!(rows(&options, false), vec![(10, vec![3.0, 3.0])]);
    }

    #[test]
    fn neighbors_must_pass_the_value_filter() {
        let mut options = options(&[AggregationType::Twa]);
        assert_eq!(rows(&options, false), vec![(10, vec![4.5])]);
        options.value_filter = Some(ValueFilter::new(0.0, 5.0).unwrap());
        assert_eq!(rows(&options, false), vec![(10, vec![3.0])]);
    }

    #[test]
    fn neighbors_skip_nan() {
        let series = series_of(&[
            (0, 1.0),
            (4, f64::NAN),
            (10, 3.0),
            (16, f64::NAN),
            (20, 9.0),
        ]);
        let options = options(&[AggregationType::Twa]);
        assert_eq!(rows_of(&series, &options, false), vec![(10, vec![4.5])]);
        let neighbors = twa_neighbors(&series, &options);
        assert_eq!(neighbors.before.to_vec(), vec![Sample::new(0, 1.0)]);
        assert_eq!(neighbors.after.to_vec(), vec![Sample::new(20, 9.0)]);
    }

    #[test]
    fn a_window_at_the_ends_of_time_has_no_neighbors() {
        let series = series();
        let mut options = RangeOptions::with_range(Timestamp::MIN, Timestamp::MAX).unwrap();
        options.aggregation = Some(aggregation(&[AggregationType::Twa], 10, false));
        let neighbors = twa_neighbors(&series, &options);
        assert!(neighbors.before.is_empty() && neighbors.after.is_empty());
    }

    #[test]
    fn last_beside_twa_matches_last_alone_with_filter_timestamps_outside_the_window() {
        let series = series_of(&[(0, 7.0), (30, 3.0), (50, 9.0)]);
        let mut options = RangeOptions::with_range(15, 35).unwrap();
        options.timestamp_filter = Some(vec![0, 30, 50]);
        let column = |columns: &[AggregationType], is_reverse: bool| {
            let mut options = options.clone();
            options.aggregation = Some(aggregation(columns, 10, true));
            let mut last: Vec<(Timestamp, f64)> = rows_of(&series, &options, is_reverse)
                .into_iter()
                .map(|(timestamp, values)| (timestamp, values[0]))
                .collect();
            last.sort_by_key(|row| row.0);
            last
        };
        for is_reverse in [false, true] {
            let alone = column(&[AggregationType::Last], is_reverse);
            let beside = column(&[AggregationType::Last, AggregationType::Twa], is_reverse);
            assert_eq!(beside, alone, "reverse={is_reverse}");
            assert_eq!(
                beside,
                vec![(10, 7.0), (20, 7.0), (30, 3.0)],
                "reverse={is_reverse}"
            );
        }
    }

    #[test]
    fn single_twa_column_reaches_the_sample_pipeline() {
        let options = options(&[AggregationType::Twa]);
        let samples: Vec<Sample> = create_range_iterator(&series(), &options, None, true).collect();
        assert_eq!(samples, vec![Sample::new(10, 4.5)]);
    }

    /// A newest-first COUNT must not read a shortened window: the bucket's left neighbor, its
    /// carry and its closing sample all lie before the buckets COUNT keeps.
    #[test]
    fn reverse_count_returns_the_first_rows_of_the_uncounted_query() {
        let samples: Vec<(Timestamp, f64)> = (0..40)
            .map(|i| (i * 7, if i % 5 == 3 { f64::NAN } else { (i * i) as f64 }))
            .collect();
        let series = series_of(&samples);
        for columns in [
            vec![AggregationType::Twa],
            vec![AggregationType::Twa, AggregationType::Last],
        ] {
            let mut options = RangeOptions::with_range(0, 300).unwrap();
            options.aggregation = Some(aggregation(&columns, 15, false));
            let full = rows_of(&series, &options, true);
            assert!(full.len() > 10);
            for count in [1, 2, 3, 7, full.len(), full.len() + 3] {
                options.count = Some(count);
                let counted = rows_of(&series, &options, true);
                let expected: Vec<_> = full.iter().take(count).cloned().collect();
                assert_eq!(counted.len(), expected.len());
                for (got, want) in counted.iter().zip(&expected) {
                    assert_eq!(got.0, want.0);
                    assert_eq!(
                        got.1.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
                        want.1.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
                        "count={count}"
                    );
                }
            }
        }
    }
}
