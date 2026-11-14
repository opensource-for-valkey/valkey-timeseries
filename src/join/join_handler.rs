use crate::aggregators::AggregateIterator;
use crate::common::threads::join;
use crate::common::time::current_time_millis;
use crate::common::{Sample, Timestamp};
use crate::join::join_reducer::ReducerFn;
use crate::join::{JoinOptions, JoinValue, create_join_iter};
use crate::series::TimeSeries;
use itertools::EitherOrBoth;
use valkey_module::ValkeyResult;

/// The result of a join: scalar samples (reduced, or aggregated) or raw joined rows.
pub enum JoinResultType {
    Samples(Vec<Sample>),
    Values(Vec<JoinValue>),
}

pub fn process_join(
    left_series: &TimeSeries,
    right_series: &TimeSeries,
    options: &JoinOptions,
) -> ValkeyResult<JoinResultType> {
    options.validate()?;
    // One clock reading per command, so `*` and relative bounds resolve identically for both
    // inputs and for aggregation alignment.
    let now = current_time_millis();
    let (left_samples, right_samples) = join(
        || fetch_samples(left_series, options, now),
        || fetch_samples(right_series, options, now),
    );
    Ok(join_samples(left_samples, right_samples, options, now))
}

/// Joins two already-fetched, timestamp-sorted inputs. `options` must have been validated.
pub(super) fn join_samples(
    left: Vec<Sample>,
    right: Vec<Sample>,
    options: &JoinOptions,
    now: Timestamp,
) -> JoinResultType {
    let rows = create_join_iter(left, right, options.join_type);

    // COUNT limits the rows (or, with AGGREGATION, the buckets) the join returns. It is applied
    // to the output only: truncating each input to COUNT samples beforehand dropped matches
    // that lay past the first COUNT samples of either side (an INNER join of [10,20,30,40]
    // with [30,40] under COUNT 2 returned nothing).
    let count = options.count.unwrap_or(usize::MAX);

    // A scalar projection exists when there is a reducer, or when SEMI/ANTI rows (left samples
    // only) are aggregated. Without one, rows are returned as they are.
    let projection: ReducerFn = match (options.reducer, &options.aggregation) {
        (Some(reducer), _) => reducer.get_handler(),
        (None, Some(_)) => |left, _right| left,
        (None, None) => return JoinResultType::Values(rows.take(count).collect()),
    };
    let samples = rows.map(move |row| project(&row, projection));

    let Some(aggregation) = &options.aggregation else {
        return JoinResultType::Samples(samples.take(count).collect());
    };
    // `EMPTY` fills the gaps between the joined samples only: the join's input is a derived
    // stream with no data outside the window to extend a fill towards.
    let (start, end) = options.date_range.get_timestamps(Some(now));
    let aligned_timestamp = aggregation.alignment.get_aligned_timestamp(start, end);
    JoinResultType::Samples(
        AggregateIterator::new(samples, aggregation, aligned_timestamp)
            .take(count)
            .collect(),
    )
}

/// Reduces one row to a sample. A missing side is passed to the reducer as `NaN`; the row's
/// timestamp is the left sample's whenever there is one.
fn project(row: &JoinValue, f: ReducerFn) -> Sample {
    match *row {
        EitherOrBoth::Both(l, r) => Sample::new(l.timestamp, f(l.value, r.value)),
        EitherOrBoth::Left(l) => Sample::new(l.timestamp, f(l.value, f64::NAN)),
        EitherOrBoth::Right(r) => Sample::new(r.timestamp, f(f64::NAN, r.value)),
    }
}

fn fetch_samples(ts: &TimeSeries, options: &JoinOptions, now: Timestamp) -> Vec<Sample> {
    // `get_range_filtered` clamps the start to the retention floor itself.
    let (start, end) = options.date_range.get_series_range(ts, Some(now), false);
    ts.get_range_filtered(
        start,
        end,
        options.timestamp_filter.as_deref(),
        options.value_filter,
    )
}
