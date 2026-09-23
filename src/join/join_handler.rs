use crate::aggregators::aggregate;
use crate::common::Sample;
use crate::common::binop::BinopFunc;
use crate::common::threads::join;
use crate::join::{JoinOptions, JoinType, JoinValue, create_join_iter};
use crate::series::TimeSeries;
use joinkit::EitherOrBoth;
use valkey_module::{ValkeyError, ValkeyResult};

// naming is hard :-)
/// The result of a join operation, which can be either samples (if reduced) or raw join values
pub enum JoinResultType {
    Samples(Vec<Sample>),
    Values(Vec<JoinValue>),
}

pub fn process_join(
    left_series: &TimeSeries,
    right_series: &TimeSeries,
    options: &JoinOptions,
) -> ValkeyResult<JoinResultType> {
    let (left_samples, right_samples) = join(
        || fetch_samples(left_series, options),
        || fetch_samples(right_series, options),
    );
    join_internal(left_samples, right_samples, options)
}

pub(super) fn join_internal<L, R, IL, IR>(
    left: IL,
    right: IR,
    options: &JoinOptions,
) -> ValkeyResult<JoinResultType>
where
    L: Iterator<Item = Sample> + 'static,
    R: Iterator<Item = Sample> + 'static,
    IL: IntoIterator<IntoIter = L, Item = Sample>,
    IR: IntoIterator<IntoIter = R, Item = Sample>,
{
    // Disallow reducer for ANTI and SEMI joins (they return a single value per timestamp,
    // so REDUCE does not make sense)
    if options.reducer.is_some()
        && (options.join_type == JoinType::Semi || options.join_type == JoinType::Anti)
    {
        return Err(ValkeyError::Str(
            "TSDB: Reducer cannot be used with SEMI or ANTI joins",
        ));
    }

    let join_iter = create_join_iter(left, right, options.join_type);

    // COUNT limits the rows (or, with AGGREGATION, the buckets) the join returns. It is applied
    // to the output only: truncating each input to COUNT samples beforehand dropped matches
    // that lay past the first COUNT samples of either side (an INNER join of [10,20,30,40]
    // with [30,40] under COUNT 2 returned nothing).
    let count = options.count.unwrap_or(usize::MAX);
    if let Some(op) = options.reducer {
        let transform = op.get_handler();
        let iter = join_iter.map(|x| transform_join_value_to_sample(&x, transform));

        if let Some(aggr_options) = &options.aggregation {
            let (start, end) = options.date_range.get_timestamps(None);
            let aligned_timestamp = aggr_options.alignment.get_aligned_timestamp(start, end);
            let result = aggregate(aggr_options, aligned_timestamp, iter)
                .into_iter()
                .take(count)
                .collect();
            return Ok(JoinResultType::Samples(result));
        }

        return Ok(JoinResultType::Samples(iter.take(count).collect()));
    } else if options.join_type == JoinType::Semi || options.join_type == JoinType::Anti {
        // note that ANTI and SEMI joins return single values per timestamp, so we can use aggregation
        if let Some(aggr_options) = &options.aggregation {
            // For SEMI/ANTI joins, the presence of a right-hand value only affects whether the
            // left-hand sample is included; the actual right-hand value is irrelevant, so we
            // intentionally ignore it and propagate only the left value here.
            let sample_iter = join_iter.map(|x| transform_join_value_to_sample(&x, |l, _r| l));
            let (start, end) = options.date_range.get_timestamps(None);
            let aligned_timestamp = aggr_options.alignment.get_aligned_timestamp(start, end);
            let result = aggregate(aggr_options, aligned_timestamp, sample_iter)
                .into_iter()
                .take(count)
                .collect();
            return Ok(JoinResultType::Samples(result));
        }
    }

    Ok(JoinResultType::Values(join_iter.take(count).collect()))
}

pub(super) fn transform_join_value_to_sample(item: &JoinValue, f: BinopFunc) -> Sample {
    match item.0 {
        EitherOrBoth::Both(l, r) => Sample::new(l.timestamp, f(l.value, r.value)),
        EitherOrBoth::Left(l) => Sample::new(l.timestamp, f(l.value, f64::NAN)),
        EitherOrBoth::Right(r) => Sample::new(r.timestamp, f(f64::NAN, r.value)),
    }
}

fn fetch_samples(ts: &TimeSeries, options: &JoinOptions) -> Vec<Sample> {
    let (start, end) = options.date_range.get_series_range(ts, None, true);
    ts.get_range_filtered(
        start,
        end,
        options.timestamp_filter.as_deref(),
        options.value_filter,
    )
}
