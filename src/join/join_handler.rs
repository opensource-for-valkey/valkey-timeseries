use crate::aggregators::AggregateIterator;
use crate::common::threads::{join, request_par_threads};
use crate::common::time::current_time_millis;
use crate::common::{Sample, Timestamp};
use crate::join::join_reducer::ReducerFn;
use crate::join::{JoinOptions, JoinValue, create_join_iter};
use crate::series::chunks::utils::filter_timestamp_slice;
use crate::series::types::ValueFilter;
use crate::series::{RangeSnapshot, TimeSeries};
use itertools::{Either, EitherOrBoth};
use valkey_module::{ValkeyError, ValkeyResult};

/// The result of a join: scalar samples (reduced, or aggregated) or raw joined rows.
pub enum JoinResultType {
    Samples(Vec<Sample>),
    Values(Vec<JoinValue>),
}

/// Joins two series on the calling thread: [`JoinInputs::read`], then [`JoinInputs::join`].
#[cfg(test)]
pub(super) fn process_join(
    left_series: &TimeSeries,
    right_series: &TimeSeries,
    options: &JoinOptions,
) -> ValkeyResult<JoinResultType> {
    Ok(JoinInputs::read(left_series, right_series, options)?.join(options))
}

/// Both inputs of a join. [`Self::read`] borrows the open series and copies nothing;
/// [`Self::into_owned`] copies the windows out, for a join that runs after the series are
/// released — on another thread.
pub struct JoinInputs<'a> {
    left: JoinInput<'a>,
    right: JoinInput<'a>,
    /// The request time `*` and relative bounds were resolved against, reused for aggregation
    /// alignment so one command sees one clock.
    now: Timestamp,
}

enum JoinInput<'a> {
    /// `FILTER_BY_TS`: the samples at the listed timestamps, already value-filtered. Point
    /// lookups, so proportional to the filter rather than to the window.
    Samples(Vec<Sample>),
    /// A window of a series that is still open, read in place.
    Live {
        series: &'a TimeSeries,
        start: Timestamp,
        end: Timestamp,
    },
    /// A copy of a window's chunks, decoded as the join consumes them.
    Snapshot(RangeSnapshot),
}

impl<'a> JoinInputs<'a> {
    /// Validates `options` and resolves both windows against one clock reading.
    pub fn read(
        left_series: &'a TimeSeries,
        right_series: &'a TimeSeries,
        options: &JoinOptions,
    ) -> ValkeyResult<Self> {
        options.validate()?;
        let now = current_time_millis();
        Ok(Self {
            left: JoinInput::read(left_series, options, now)?,
            right: JoinInput::read(right_series, options, now)?,
            now,
        })
    }

    /// Copies both windows out of their series. Cheap compared to the join: chunks are copied
    /// compressed, and decoded later.
    pub fn into_owned(self) -> JoinInputs<'static> {
        JoinInputs {
            left: self.left.into_owned(),
            right: self.right.into_owned(),
            now: self.now,
        }
    }

    /// An upper bound on the samples the join reads.
    pub fn estimated_samples(&self) -> usize {
        self.left.estimated_samples() + self.right.estimated_samples()
    }

    pub fn join(&self, options: &JoinOptions) -> JoinResultType {
        let filter = options.value_filter;
        // Under COUNT the join usually needs only a prefix of each input, so decode lazily, as
        // the merge consumes samples. Otherwise every sample is read anyway: decode both
        // inputs in parallel when there are enough of them to pay for the pool round trip
        // (break-even at about 4,096 samples, `benches/join.rs`, `join/decode`).
        if options.count.is_none() && request_par_threads(2, self.estimated_samples()) != 1 {
            let (left, right) = join(
                || self.left.samples(filter).collect::<Vec<_>>(),
                || self.right.samples(filter).collect::<Vec<_>>(),
            );
            join_samples(left, right, options, self.now)
        } else {
            join_samples(
                self.left.samples(filter),
                self.right.samples(filter),
                options,
                self.now,
            )
        }
    }
}

impl<'a> JoinInput<'a> {
    fn read(series: &'a TimeSeries, options: &JoinOptions, now: Timestamp) -> ValkeyResult<Self> {
        // Every source clamps the start to the retention floor itself.
        let (start, end) = options
            .date_range
            .get_series_range(series, Some(now), false);
        let Some(timestamps) = &options.timestamp_filter else {
            return Ok(JoinInput::Live { series, start, end });
        };
        let start = start.max(series.get_min_timestamp());
        let mut samples = series
            .samples_by_timestamps(&filter_timestamp_slice(timestamps, start, end))
            .map_err(|e| ValkeyError::String(format!("TSDB: {e}")))?;
        if let Some(filter) = options.value_filter {
            samples.retain(|s| filter.is_match(s.value));
        }
        Ok(JoinInput::Samples(samples))
    }

    fn into_owned(self) -> JoinInput<'static> {
        match self {
            JoinInput::Samples(samples) => JoinInput::Samples(samples),
            JoinInput::Live { series, start, end } => {
                JoinInput::Snapshot(series.snapshot_range(start, end))
            }
            JoinInput::Snapshot(snapshot) => JoinInput::Snapshot(snapshot),
        }
    }

    fn estimated_samples(&self) -> usize {
        match self {
            JoinInput::Samples(samples) => samples.len(),
            JoinInput::Live { series, start, end } => series.estimated_range_samples(*start, *end),
            JoinInput::Snapshot(snapshot) => snapshot.estimated_samples(),
        }
    }

    fn samples(&self, filter: Option<ValueFilter>) -> impl Iterator<Item = Sample> + '_ {
        let range = match self {
            // Already value-filtered by `read`.
            JoinInput::Samples(samples) => return Either::Left(samples.iter().copied()),
            JoinInput::Live { series, start, end } => series.range_iter(*start, *end),
            JoinInput::Snapshot(snapshot) => snapshot.sample_iter(false),
        };
        Either::Right(range.filter(move |s| filter.is_none_or(|f| f.is_match(s.value))))
    }
}

/// Joins two timestamp-sorted inputs. `options` must have been validated.
pub(super) fn join_samples<L, R>(
    left: impl IntoIterator<Item = Sample, IntoIter = L>,
    right: impl IntoIterator<Item = Sample, IntoIter = R>,
    options: &JoinOptions,
    now: Timestamp,
) -> JoinResultType
where
    L: Iterator<Item = Sample>,
    R: Iterator<Item = Sample>,
{
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
