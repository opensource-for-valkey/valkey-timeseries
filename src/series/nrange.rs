use crate::common::{MultiSample, Sample};
use crate::iterators::{
    PivotIter, create_range_iterator, create_row_iterator, get_range_latest_sample,
};
use crate::series::TimeSeries;
use crate::series::request_types::{NRangeOptions, RangeOptions};
use crate::series::utils::get_timeseries;
use smallvec::smallvec;
use valkey_module::{AclPermissions, Context, ValkeyResult};

/// One input key of a TS.NRANGE query: the series it names, plus its still-open compaction
/// bucket when `LATEST` asks for it. The latest sample is resolved up front because it needs
/// the `Context`, which the iterator pipeline below does not carry.
///
/// Keys are held positionally, not by name: the reply's column order is the command's key
/// order, and a key repeated in the request is a separate entry with its own column.
pub(crate) struct NRangeSeriesMeta<'a> {
    pub series: &'a TimeSeries,
    pub latest: Option<Sample>,
}

/// Run a TS.NRANGE / TS.NREVRANGE query: open every requested key, then pivot the per-key
/// ranges into one row per timestamp.
///
/// Every key must exist and be readable by the caller; a missing key is an error, as it is for
/// TS.RANGE, rather than a column of NaN (which would be indistinguishable from a key that
/// exists but has no samples).
pub(crate) fn process_nrange_query(
    ctx: &Context,
    options: &NRangeOptions,
) -> ValkeyResult<Vec<MultiSample>> {
    let guards = options
        .keys
        .iter()
        .map(|key| {
            // must_exist = true, so the Option is always Some when this returns Ok.
            get_timeseries(ctx, key, Some(AclPermissions::ACCESS), true)
                .map(|guard| guard.expect("get_timeseries(must_exist) returned no series"))
        })
        .collect::<ValkeyResult<Vec<_>>>()?;

    let metas: Vec<NRangeSeriesMeta> = guards
        .iter()
        .enumerate()
        .map(|(index, guard)| {
            let range = per_key_range_options(options, index);
            NRangeSeriesMeta {
                series: guard,
                latest: get_latest(ctx, guard, &range),
            }
        })
        .collect();

    Ok(process_nrange(&metas, options))
}

/// Pivot the per-key ranges of `metas` (in key order) into rows of
/// `[timestamp, value per column]`, then apply direction and COUNT.
pub(crate) fn process_nrange<'a>(
    metas: &[NRangeSeriesMeta<'a>],
    options: &NRangeOptions,
) -> Vec<MultiSample> {
    let sources: Vec<Box<dyn Iterator<Item = MultiSample> + 'a>> = metas
        .iter()
        .enumerate()
        .map(|(index, meta)| create_key_iterator(meta, options, index))
        .collect();

    let widths: Vec<usize> = (0..metas.len()).map(|i| options.column_count(i)).collect();

    let rows = PivotIter::new(sources, widths);
    collect_pivot_rows(rows, options.is_reverse, options.range.count)
}

/// The per-key slice of the request: the shared range parameters plus that key's own
/// aggregation clause.
///
/// COUNT is deliberately dropped. It limits the *pivoted* rows, so a per-key limit would
/// truncate each stream before the join and lose timestamps that other keys still cover; it is
/// re-applied to the merged rows in [`collect_pivot_rows`].
fn per_key_range_options(options: &NRangeOptions, index: usize) -> RangeOptions {
    RangeOptions {
        date_range: options.range.date_range,
        count: None,
        latest: options.range.latest,
        aggregation: options.aggregation_for(index).cloned(),
        timestamp_filter: options.range.timestamp_filter.clone(),
        value_filter: options.range.value_filter,
    }
}

/// The destination's still-open bucket, when `LATEST` asks for it. Mirrors TS.MRANGE: the
/// shared single-key helper decides, and a sample the value filter rejects is not reported.
fn get_latest(ctx: &Context, series: &TimeSeries, range: &RangeOptions) -> Option<Sample> {
    get_range_latest_sample(Some(ctx), series, range)
        .filter(|s| range.value_filter.is_none_or(|vf| vf.is_match(s.value)))
}

/// One key's contribution to the pivot: a row stream carrying that key's columns.
///
/// Always ascending — the merge yields its sources' order and [`collect_pivot_rows`] reverses
/// the *joined* rows, so reversing here would only scramble the join.
fn create_key_iterator<'a>(
    meta: &NRangeSeriesMeta<'a>,
    options: &NRangeOptions,
    index: usize,
) -> Box<dyn Iterator<Item = MultiSample> + 'a> {
    let range = per_key_range_options(options, index);

    if range.aggregation.is_some() {
        // One value per aggregator, in the order the key's list named them. This is the
        // multi-aggregation row pipeline even for a single aggregator, so the two cases stay
        // one code path.
        create_row_iterator(meta.series, &range, meta.latest, false)
    } else {
        let samples = create_range_iterator(meta.series, &range, &None, meta.latest, false);
        Box::new(samples.map(|sample| MultiSample {
            timestamp: sample.timestamp,
            values: smallvec![sample.value],
        }))
    }
}

/// Apply direction and COUNT to the joined rows.
///
/// COUNT limits rows in the *requested* order, so a reverse query must reverse before
/// truncating — taking from the (ascending) merge first would keep the oldest N and merely
/// reverse those, returning the wrong window rather than just the wrong order. This is the
/// same rule `collect_samples` applies to TS.MRANGE, including its one asymmetry: ascending
/// output matches the merge order, so COUNT can stop the merge early instead of materializing
/// every row.
fn collect_pivot_rows<I: Iterator<Item = MultiSample>>(
    iter: I,
    is_reverse: bool,
    count: Option<usize>,
) -> Vec<MultiSample> {
    if !is_reverse {
        return match count {
            Some(count) => iter.take(count).collect(),
            None => iter.collect(),
        };
    }

    let mut rows: Vec<MultiSample> = iter.collect();
    rows.reverse();
    if let Some(count) = count {
        rows.truncate(count);
    }
    rows
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregators::AggregationType;
    use crate::common::binop::ComparisonOperator;
    use crate::series::request_types::{
        AggregationOptions, AggregatorConfig, ValueComparisonFilter,
    };
    use smallvec::SmallVec;

    fn make_series(samples: &[(i64, f64)]) -> TimeSeries {
        let mut series = TimeSeries::default();
        for &(ts, value) in samples {
            let _ = series.add(ts, value, None);
        }
        series
    }

    fn meta(series: &TimeSeries) -> NRangeSeriesMeta<'_> {
        NRangeSeriesMeta {
            series,
            latest: None,
        }
    }

    fn options(start: i64, end: i64) -> NRangeOptions {
        NRangeOptions {
            range: RangeOptions::with_range(start, end).unwrap(),
            ..Default::default()
        }
    }

    /// An `AGGREGATION` clause for one key, over `bucket_duration` ms buckets.
    fn aggregation(
        bucket_duration: u64,
        aggregations: SmallVec<[AggregatorConfig; 2]>,
    ) -> AggregationOptions {
        AggregationOptions {
            aggregations,
            bucket_duration,
            timestamp_output: Default::default(),
            alignment: Default::default(),
            report_empty: false,
        }
    }

    fn configs(types: &[AggregationType]) -> SmallVec<[AggregatorConfig; 2]> {
        types.iter().map(|ty| (*ty).into()).collect()
    }

    /// `[(timestamp, values)]` with NaN rendered as `None`, so rows compare by value.
    fn rows_of(rows: &[MultiSample]) -> Vec<(i64, Vec<Option<f64>>)> {
        rows.iter()
            .map(|r| {
                (
                    r.timestamp,
                    r.values
                        .iter()
                        .map(|v| (!v.is_nan()).then_some(*v))
                        .collect(),
                )
            })
            .collect()
    }

    /// Raw mode: one row per distinct timestamp, one column per key in key order, NaN where a
    /// key has no sample. This is the worked example from the TS.NRANGE documentation.
    #[test]
    fn test_raw_pivot_by_timestamp() {
        let s1 = make_series(&[(1000, 10.0), (2000, 12.0)]);
        let s2 = make_series(&[(1000, 20.0), (3000, 25.0)]);

        let rows = process_nrange(&[meta(&s1), meta(&s2)], &options(0, 5000));

        assert_eq!(
            rows_of(&rows),
            vec![
                (1000, vec![Some(10.0), Some(20.0)]),
                (2000, vec![Some(12.0), None]),
                (3000, vec![None, Some(25.0)]),
            ]
        );
    }

    /// TS.NREVRANGE ordering: the same rows, highest timestamp first, and COUNT keeps the
    /// highest timestamps rather than the oldest ones reversed.
    #[test]
    fn test_reverse_and_count_apply_to_joined_rows() {
        let s1 = make_series(&[(1000, 10.0), (2000, 12.0)]);
        let s2 = make_series(&[(1000, 20.0), (3000, 25.0)]);

        let mut opts = options(0, 5000);
        opts.is_reverse = true;
        let rows = process_nrange(&[meta(&s1), meta(&s2)], &opts);
        assert_eq!(
            rows.iter().map(|r| r.timestamp).collect::<Vec<_>>(),
            vec![3000, 2000, 1000]
        );

        opts.range.count = Some(2);
        let rows = process_nrange(&[meta(&s1), meta(&s2)], &opts);
        assert_eq!(
            rows.iter().map(|r| r.timestamp).collect::<Vec<_>>(),
            vec![3000, 2000]
        );

        // Forward, the same COUNT keeps the lowest timestamps.
        opts.is_reverse = false;
        let rows = process_nrange(&[meta(&s1), meta(&s2)], &opts);
        assert_eq!(
            rows.iter().map(|r| r.timestamp).collect::<Vec<_>>(),
            vec![1000, 2000]
        );
    }

    /// Aggregation mode with a different aggregator per key, both over the same bucket
    /// duration (the documented `AGGREGATION avg sum 1000` example).
    #[test]
    fn test_per_key_aggregators() {
        let s3 = make_series(&[(1000, 10.0), (1100, 20.0), (2000, 30.0)]);
        let s4 = make_series(&[(1000, 5.0), (1100, 15.0), (2000, 25.0)]);

        let mut opts = options(0, 5000);
        opts.aggregations = vec![
            aggregation(1000, configs(&[AggregationType::Avg])),
            aggregation(1000, configs(&[AggregationType::Sum])),
        ];

        let rows = process_nrange(&[meta(&s3), meta(&s4)], &opts);

        assert_eq!(
            rows_of(&rows),
            vec![
                (1000, vec![Some(15.0), Some(20.0)]),
                (2000, vec![Some(30.0), Some(25.0)]),
            ]
        );
    }

    /// A key given a comma-separated list contributes one column per aggregator, and its
    /// columns stay together and in order (`avg,max` then `sum`).
    #[test]
    fn test_multiple_aggregators_for_one_key() {
        let s3 = make_series(&[(1000, 10.0), (1100, 20.0), (2000, 30.0)]);
        let s4 = make_series(&[(1000, 5.0), (1100, 15.0), (2000, 25.0)]);

        let mut opts = options(0, 5000);
        opts.aggregations = vec![
            aggregation(1000, configs(&[AggregationType::Avg, AggregationType::Max])),
            aggregation(1000, configs(&[AggregationType::Sum])),
        ];

        let rows = process_nrange(&[meta(&s3), meta(&s4)], &opts);

        assert_eq!(
            rows_of(&rows),
            vec![
                (1000, vec![Some(15.0), Some(20.0), Some(20.0)]),
                (2000, vec![Some(30.0), Some(30.0), Some(25.0)]),
            ]
        );
    }

    /// A key with no samples in a bucket reports NaN for every column it owns, even when
    /// another key produced the bucket.
    #[test]
    fn test_missing_buckets_blank_the_whole_key_block() {
        let dense = make_series(&[(1000, 1.0), (2000, 2.0), (3000, 3.0)]);
        let sparse = make_series(&[(2000, 9.0)]);

        let mut opts = options(0, 5000);
        opts.aggregations = vec![
            aggregation(1000, configs(&[AggregationType::Min, AggregationType::Max])),
            aggregation(1000, configs(&[AggregationType::Sum])),
        ];

        let rows = process_nrange(&[meta(&dense), meta(&sparse)], &opts);

        assert_eq!(
            rows_of(&rows),
            vec![
                (1000, vec![Some(1.0), Some(1.0), None]),
                (2000, vec![Some(2.0), Some(2.0), Some(9.0)]),
                (3000, vec![Some(3.0), Some(3.0), None]),
            ]
        );
    }

    /// The extended aggregators are available per key, inline condition included.
    #[test]
    fn test_extended_aggregators() {
        let s1 = make_series(&[(1000, 1.0), (1100, 7.0), (2000, 9.0)]);
        let s2 = make_series(&[(1000, 4.0), (1100, 6.0), (2000, 2.0)]);

        let above_five = Some(ValueComparisonFilter {
            operator: ComparisonOperator::GreaterThan,
            value: 5.0,
        });

        let mut opts = options(0, 5000);
        opts.aggregations = vec![
            aggregation(
                1000,
                smallvec![AggregatorConfig::new(AggregationType::CountIf, above_five).unwrap()],
            ),
            aggregation(1000, configs(&[AggregationType::Range])),
        ];

        let rows = process_nrange(&[meta(&s1), meta(&s2)], &opts);

        assert_eq!(
            rows_of(&rows),
            vec![
                // bucket 1000: s1 has one value > 5, s2 spans 4..6
                (1000, vec![Some(1.0), Some(2.0)]),
                (2000, vec![Some(1.0), Some(0.0)]),
            ]
        );
    }

    /// The same key listed twice is two independent columns, and a key whose samples all fall
    /// outside the range contributes only NaN.
    #[test]
    fn test_repeated_and_out_of_range_keys() {
        let s1 = make_series(&[(1000, 10.0)]);
        let out_of_range = make_series(&[(9_000, 1.0)]);

        let rows = process_nrange(
            &[meta(&s1), meta(&s1), meta(&out_of_range)],
            &options(0, 5000),
        );

        assert_eq!(
            rows_of(&rows),
            vec![(1000, vec![Some(10.0), Some(10.0), None])]
        );
    }

    /// Value and timestamp filters run per key, before the join, so a filtered-out sample
    /// leaves a NaN rather than removing the timestamp another key still reports.
    #[test]
    fn test_filters_apply_before_the_join() {
        let s1 = make_series(&[(1000, 10.0), (2000, 500.0)]);
        let s2 = make_series(&[(1000, 20.0), (2000, 30.0)]);

        let mut opts = options(0, 5000);
        opts.range.value_filter = Some(crate::series::ValueFilter::new(0.0, 100.0).unwrap());
        let rows = process_nrange(&[meta(&s1), meta(&s2)], &opts);
        assert_eq!(
            rows_of(&rows),
            vec![
                (1000, vec![Some(10.0), Some(20.0)]),
                (2000, vec![None, Some(30.0)]),
            ]
        );

        let mut opts = options(0, 5000);
        opts.range.timestamp_filter = Some(vec![2000]);
        let rows = process_nrange(&[meta(&s1), meta(&s2)], &opts);
        assert_eq!(rows_of(&rows), vec![(2000, vec![Some(500.0), Some(30.0)])]);
    }

    /// Nothing to report is an empty reply, not a row of NaN.
    #[test]
    fn test_no_samples_in_range() {
        let s1 = make_series(&[(9_000, 1.0)]);
        let s2 = make_series(&[(9_100, 2.0)]);

        let rows = process_nrange(&[meta(&s1), meta(&s2)], &options(0, 5000));
        assert!(rows.is_empty());
    }
}
