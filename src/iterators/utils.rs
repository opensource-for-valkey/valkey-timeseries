use crate::aggregators::{
    AggregateIterator, AggregationType, EmptyFillBounds, MultiAggregateIterator, bucket_start_for,
};
use crate::common::hash::IntSet;
use crate::common::{MultiSample, Sample, Timestamp};
use crate::iterators::{ReduceIterator, TimestampFilterIterator};
use crate::series::request_types::{AggregationOptions, RangeGroupingOptions, RangeOptions};
use crate::series::{SeriesSampleIterator, TimeSeries, ValueFilter};
use smallvec::SmallVec;

macro_rules! apply_iter_limit {
    ($iter:expr, $limit:expr) => {
        if let Some(limit) = $limit {
            Box::new($iter.take(limit as usize)) as Box<dyn Iterator<Item = _> + '_>
        } else {
            Box::new($iter) as Box<dyn Iterator<Item = _> + '_>
        }
    };
}

pub fn create_aggregate_iterator<I>(
    iter: I,
    range: &RangeOptions,
    aggregation: &AggregationOptions,
    empty_fill: EmptyFillBounds,
) -> AggregateIterator<I>
where
    I: Iterator<Item = Sample>,
{
    let (start_ts, end_ts) = range.get_timestamp_range();
    let aligned_timestamp = aggregation
        .alignment
        .get_aligned_timestamp(start_ts, end_ts);

    AggregateIterator::with_empty_fill(iter, aggregation, aligned_timestamp, empty_fill)
}

/// Is this sample data as far as `EMPTY` is concerned? A stored NaN is a sample, but it is not a
/// reading: it neither extends the fill nor gives `last` a value to carry.
fn is_empty_fill_data(sample: &Sample, value_filter: Option<ValueFilter>) -> bool {
    !sample.value.is_nan() && value_filter.is_none_or(|filter| filter.is_match(sample.value))
}

/// Does any sample in `[from, to]` count as data for `EMPTY` under the query's
/// `FILTER_BY_TS`/`FILTER_BY_VALUE`?
///
/// Used to look *outside* the queried window, which is why it takes the series rather than the
/// (clipped) sample stream. `FILTER_BY_TS` looks up only the timestamps it names. Anything else
/// scans, stopping at its first hit, which is normally the first sample it reads.
fn has_passing_sample(
    series: &TimeSeries,
    options: &RangeOptions,
    from: Timestamp,
    to: Timestamp,
) -> bool {
    if from > to || series.is_empty() {
        return false;
    }

    // The earliest *visible* sample, not the retention floor: the floor is usually not a
    // sample timestamp, and reading it as one invents data before the first sample.
    let first = series.visible_first_timestamp();
    let last = series.last_timestamp();
    if to < first || from > last {
        return false;
    }

    let value_filter = options.value_filter;
    let is_data = |sample: Sample| is_empty_fill_data(&sample, value_filter);

    if let Some(timestamps) = options.timestamp_filter.as_deref() {
        return TimestampFilterIterator::new(series, timestamps, from, to, false).any(is_data);
    }
    // The last sample needs no chunk read, and is usually the hit when looking past the window.
    if series
        .last_sample
        .is_some_and(|sample| sample.timestamp >= from && sample.timestamp <= to && is_data(sample))
    {
        return true;
    }
    SeriesSampleIterator::new(series, from.max(first), to, false).any(is_data)
}

/// The last sample before `to` that counts as data for `EMPTY` under the query's filters,
/// searching backwards from it. `None` when there is none.
fn last_passing_sample_before(
    series: &TimeSeries,
    options: &RangeOptions,
    to: Timestamp,
) -> Option<Sample> {
    // The earliest *visible* sample, not the retention floor: the floor is usually not a
    // sample timestamp, and reading it as one invents data before the first sample.
    let first = series.visible_first_timestamp();
    if series.is_empty() || to < first {
        return None;
    }

    let value_filter = options.value_filter;
    let is_data = |sample: &Sample| is_empty_fill_data(sample, value_filter);

    if let Some(timestamps) = options.timestamp_filter.as_deref() {
        return TimestampFilterIterator::new(series, timestamps, first, to, true).find(is_data);
    }
    SeriesSampleIterator::new(series, first, to, true).find(is_data)
}

/// How far this query's `EMPTY` fill may run past the samples it sees — see
/// [`EmptyFillBounds`]. Cheap and `Default` (interior gaps only) whenever `EMPTY` was not
/// requested, so non-EMPTY queries never touch the series for this.
///
/// `carry_reverse` says the query's *reply* runs newest-first, which `last` depends on even though
/// buckets are always produced chronologically.
pub(crate) fn empty_fill_bounds(
    series: &TimeSeries,
    options: &RangeOptions,
    carry_reverse: bool,
) -> EmptyFillBounds {
    let Some(aggregation) = options.aggregation.as_ref().filter(|a| a.report_empty) else {
        return EmptyFillBounds::default();
    };

    let (start_ts, end_ts) = options.get_timestamp_range();
    let leads = has_passing_sample(series, options, Timestamp::MIN, start_ts.saturating_sub(1));
    let trails = has_passing_sample(series, options, end_ts.saturating_add(1), Timestamp::MAX);

    // Nothing is filled where the window and the data extent do not overlap at all. With no
    // stored sample inside the window that needs passing data on *both* sides — the window is
    // then a gap in the data rather than past its edge. The test is deliberately over stored
    // samples: `LATEST` can chain a compaction's still-open bucket into a window that lies
    // beyond everything the destination holds, and that sample is a bucket of its own, not a
    // reason to fill back towards data that ends before the window.
    let overlaps = leads && trails || has_passing_sample(series, options, start_ts, end_ts);

    let uses_last = aggregation
        .aggregations
        .iter()
        .any(|a| matches!(a.aggregation_type(), AggregationType::Last));

    // `last` fills a leading gap with the last reading before *the bucket's start*, and a later
    // gap with the last reading before the window. The two differ whenever the window opens
    // mid-bucket: a sample in between belongs to the first bucket rather than preceding it.
    let (carry_seed, carry_lookback) = if leads && uses_last {
        let aligned = aggregation
            .alignment
            .get_aligned_timestamp(start_ts, end_ts);
        let first_bucket = bucket_start_for(start_ts, aligned, aggregation.bucket_duration);
        let lookback = last_passing_sample_before(series, options, start_ts.saturating_sub(1));
        let seed = match lookback {
            Some(sample) if sample.timestamp < first_bucket => Some(sample),
            _ => last_passing_sample_before(series, options, first_bucket.saturating_sub(1)),
        };
        (
            seed.map_or(f64::NAN, |sample| sample.value),
            lookback.map_or(f64::NAN, |sample| sample.value),
        )
    } else {
        (f64::NAN, f64::NAN)
    };

    EmptyFillBounds {
        start: (overlaps && leads).then_some(start_ts),
        end: (overlaps && trails).then_some(end_ts),
        carry_seed,
        carry_lookback,
        carry_reverse,
    }
}

/// Create an optimized range iterator for the given series and options
pub fn create_range_iterator<'a>(
    series: &'a TimeSeries,
    options: &RangeOptions,
    grouping: &Option<RangeGroupingOptions>,
    latest_sample: Option<Sample>,
    is_reverse: bool,
) -> Box<dyn Iterator<Item = Sample> + 'a> {
    create_range_iterator_for_reply(
        series,
        options,
        grouping,
        latest_sample,
        is_reverse,
        is_reverse,
    )
}

/// [`create_range_iterator`] for a stream whose reply order is not its own: a shard streams
/// ascending for a coordinator that will reverse it, and `reply_reverse` tells EMPTY about that.
pub(crate) fn create_range_iterator_for_reply<'a>(
    series: &'a TimeSeries,
    options: &RangeOptions,
    grouping: &Option<RangeGroupingOptions>,
    latest_sample: Option<Sample>,
    is_reverse: bool,
    reply_reverse: bool,
) -> Box<dyn Iterator<Item = Sample> + 'a> {
    let has_aggregation = options.aggregation.is_some();
    let should_reverse_iter = !has_aggregation && is_reverse;
    let should_reverse_aggr = has_aggregation && is_reverse;
    // Derived from the series, before the stream is clipped to the query window.
    let empty_fill = empty_fill_bounds(series, options, reply_reverse);

    if let Some(ts_filter) = options.timestamp_filter.as_ref() {
        let (start_ts, end_ts) = options.get_timestamp_range();
        let base_iter =
            TimestampFilterIterator::new(series, ts_filter, start_ts, end_ts, should_reverse_iter);
        // Remove the timestamp filter from options to avoid double filtering
        let opts = RangeOptions {
            date_range: options.date_range,
            count: options.count,
            latest: false,
            aggregation: options.aggregation.clone(),
            value_filter: options.value_filter,
            timestamp_filter: None,
        };
        let output = SampleOutput {
            options: &opts,
            grouping,
            is_reverse: should_reverse_aggr,
            empty_fill,
        };
        output.chain_latest(base_iter, latest_sample, should_reverse_iter)
    } else {
        let base_iter =
            SeriesSampleIterator::from_range_options(series, options, should_reverse_iter);
        let output = SampleOutput {
            options,
            grouping,
            is_reverse: should_reverse_aggr,
            empty_fill,
        };
        output.chain_latest(base_iter, latest_sample, should_reverse_iter)
    }
}

/// What happens to a series' samples once they have been read.
struct SampleOutput<'o> {
    options: &'o RangeOptions,
    grouping: &'o Option<RangeGroupingOptions>,
    /// Reverse the *finished* output. Distinct from reading the series newest-first, which is
    /// what aggregation cannot do.
    is_reverse: bool,
    empty_fill: EmptyFillBounds,
}

impl SampleOutput<'_> {
    /// Add LATEST's open bucket to the stream, which comes after every stored sample in time:
    /// `read_newest_first` is the order `base` was read in, and has nothing to do with the order
    /// of the reply, since an aggregated stream is read ascending whichever way the reply runs.
    ///
    /// Generic over `base` so that the common case, no LATEST sample, adds no adapter at all.
    fn chain_latest<'a, I>(
        &self,
        base: I,
        latest: Option<Sample>,
        read_newest_first: bool,
    ) -> Box<dyn Iterator<Item = Sample> + 'a>
    where
        I: Iterator<Item = Sample> + 'a,
    {
        match latest {
            None => self.adapt(base),
            Some(sample) if read_newest_first => self.adapt(std::iter::once(sample).chain(base)),
            Some(sample) => self.adapt(base.chain(std::iter::once(sample))),
        }
    }

    fn adapt<'a, I>(&self, stream: I) -> Box<dyn Iterator<Item = Sample> + 'a>
    where
        I: Iterator<Item = Sample> + 'a,
    {
        create_sample_iterator_adapter(
            stream,
            self.options,
            self.grouping,
            self.is_reverse,
            self.empty_fill,
        )
    }
}

/// Pre-aggregation composition for one series: base reader (ts-filter variant
/// when FILTER_BY_TS is present) + LATEST chaining + value filter. The output
/// is always ascending — multi-aggregation consumes ascending input and
/// reverses rows post-aggregation.
fn create_filtered_sample_iterator<'a>(
    series: &'a TimeSeries,
    options: &RangeOptions,
    latest_sample: Option<Sample>,
) -> Box<dyn Iterator<Item = Sample> + 'a> {
    let base_iter: Box<dyn Iterator<Item = Sample> + 'a> =
        if let Some(ts_filter) = options.timestamp_filter.as_ref() {
            // The base iterator applies the timestamp filter; no re-filtering needed.
            let (start_ts, end_ts) = options.get_timestamp_range();
            Box::new(TimestampFilterIterator::new(
                series, ts_filter, start_ts, end_ts, false,
            ))
        } else {
            Box::new(SeriesSampleIterator::from_range_options(
                series, options, false,
            ))
        };

    let base_iter: Box<dyn Iterator<Item = Sample> + 'a> = if let Some(sample) = latest_sample {
        // A partial compaction sample is beyond the last stored sample, so it
        // is chained at the end of the ascending stream.
        Box::new(base_iter.chain(std::iter::once(sample)))
    } else {
        base_iter
    };

    if let Some(val_filter) = options.value_filter {
        Box::new(base_iter.filter(move |sample| val_filter.is_match(sample.value)))
    } else {
        base_iter
    }
}

/// Create the multi-aggregation row pipeline for one series:
/// filtered samples -> MultiAggregateIterator -> optional reverse -> take(COUNT).
/// Only valid when `options.aggregation` is present (typically multi).
pub fn create_row_iterator<'a>(
    series: &'a TimeSeries,
    options: &RangeOptions,
    latest_sample: Option<Sample>,
    is_reverse: bool,
) -> Box<dyn Iterator<Item = MultiSample> + 'a> {
    create_row_iterator_for_reply(series, options, latest_sample, is_reverse, is_reverse)
}

/// [`create_row_iterator`] with a separate reply order; see [`create_range_iterator_for_reply`].
pub(crate) fn create_row_iterator_for_reply<'a>(
    series: &'a TimeSeries,
    options: &RangeOptions,
    latest_sample: Option<Sample>,
    is_reverse: bool,
    reply_reverse: bool,
) -> Box<dyn Iterator<Item = MultiSample> + 'a> {
    let aggregation = options
        .aggregation
        .as_ref()
        .expect("create_row_iterator requires aggregation options");

    let filtered = create_filtered_sample_iterator(series, options, latest_sample);

    let (start_ts, end_ts) = options.get_timestamp_range();
    let aligned_timestamp = aggregation
        .alignment
        .get_aligned_timestamp(start_ts, end_ts);
    // first/last are chronological (earliest/latest sample) regardless of direction: this
    // stream is aggregated forward and reversed at the end, which already lands on the
    // chronological answer, so the aggregation is used as requested.
    let empty_fill = empty_fill_bounds(series, options, reply_reverse);
    let aggr_iter = MultiAggregateIterator::with_empty_fill(
        filtered,
        aggregation,
        aligned_timestamp,
        empty_fill,
    );

    finalize(aggr_iter, is_reverse, options.count)
}

/// Apply reversal and COUNT, then box. Buckets and samples alike: COUNT limits output items, and
/// the stream arrives in its final chronological form, `last`'s EMPTY carry included.
fn finalize<'a, I>(
    iter: I,
    is_reverse: bool,
    count: Option<usize>,
) -> Box<dyn Iterator<Item = I::Item> + 'a>
where
    I: Iterator + 'a,
    I::Item: 'a,
{
    if is_reverse {
        let rev = reverse_limited(iter, count);
        apply_iter_limit!(rev, count)
    } else {
        apply_iter_limit!(iter, count)
    }
}

/// Create a sample iterator adapter that applies filtering, aggregation, grouping, and limits
/// based on the provided options. The resulting iterator yields samples according to the specified
/// criteria.
/// Boxing is delayed to the last possible moment to allow for compiler optimizations.
pub fn create_sample_iterator_adapter<'a, T: Iterator<Item = Sample> + 'a>(
    base_iter: T,
    options: &RangeOptions,
    grouping: &Option<RangeGroupingOptions>,
    is_reverse: bool,
    empty_fill: EmptyFillBounds,
) -> Box<dyn Iterator<Item = Sample> + 'a> {
    // Apply Filters (Timestamp & Value)
    let ts_filter = options
        .timestamp_filter
        .as_ref()
        .map(|f| TimestampFilter::new(f));
    let val_filter = options.value_filter;

    let filtered = base_iter.filter(move |sample| {
        if let Some(ts) = &ts_filter
            && !ts.matches(sample.timestamp)
        {
            return false;
        }
        if let Some(val) = &val_filter
            && !val.is_match(sample.value)
        {
            return false;
        }
        true
    });

    let count = options.count;

    match (&options.aggregation, grouping) {
        (Some(agg), Some(grp)) => {
            // Only valid when `base_iter` is a single series: this buckets the whole stream
            // and then reduces one value per bucket. A multi-series group has to bucket each
            // series on its own before reducing across them (`get_grouped_samples`,
            // `reduce_aggregated_group`); bucketing their merged samples here gave the reducer
            // one value per bucket instead of one per series.
            let aggr_iter = create_aggregate_iterator(filtered, options, agg, empty_fill);
            let aggregator = grp.aggregation.create_aggregator();
            let reducer = ReduceIterator::new(aggr_iter, aggregator);
            finalize(reducer, is_reverse, count)
        }
        (None, Some(grp)) => {
            let aggregator = grp.aggregation.create_aggregator();
            let reducer = ReduceIterator::new(filtered, aggregator);
            finalize(reducer, is_reverse, count)
        }
        (Some(agg), None) => {
            let aggr_iter = create_aggregate_iterator(filtered, options, agg, empty_fill);
            finalize(aggr_iter, is_reverse, count)
        }
        (None, None) => finalize(filtered, is_reverse, count),
    }
}

/// Reverses `iter`, buffering only the items a COUNT limit can reach.
///
/// A reverse query is served by aggregating forward and reversing, so without a limit the
/// whole forward stream is buffered. With COUNT only its last `count` items can be returned,
/// and a ring buffer of that size ([`TailIter`]) is all that must be held — rather than every
/// bucket of the range (for `EMPTY` over a long range, every *empty* bucket too) — before the
/// first one is emitted. The forward pass itself still runs to the end.
fn reverse_limited<'a, I>(iter: I, count: Option<usize>) -> Box<dyn Iterator<Item = I::Item> + 'a>
where
    I: Iterator + 'a,
    I::Item: 'a,
{
    match count {
        Some(count) => Box::new(ReverseIter::new(TailIter::new(iter, count))),
        None => Box::new(ReverseIter::new(iter)),
    }
}

/// Buffers the inner iterator and yields its items in reverse order.
/// Used to reverse aggregation output (samples or multi-aggregation rows),
/// which buffers bucket count, not raw sample count.
pub(crate) struct ReverseIter<I: Iterator> {
    inner: I,
    buf: Vec<I::Item>,
    loaded: bool,
}

impl<I: Iterator> ReverseIter<I> {
    pub fn new(inner: I) -> Self {
        let buf = Vec::new();
        Self {
            inner,
            buf,
            loaded: false,
        }
    }

    fn load_items(&mut self) {
        // determine the length of the iterator if possible to pre-allocate the buffer
        let (lower, _) = self.inner.size_hint();
        if lower > 0 {
            self.buf.reserve(lower);
        }

        for item in self.inner.by_ref() {
            self.buf.push(item);
        }
    }
}

impl<I: Iterator> Iterator for ReverseIter<I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.loaded {
            self.loaded = true;
            self.load_items();
        }
        self.buf.pop()
    }
}

/// Yields the last `capacity` items of `inner` in their original order,
/// buffering at most `capacity` items (ring buffer). Used by COUNT push-down
/// to take the tail of an ascending shard stream for reverse queries without
/// disturbing the ascending pipeline.
pub struct TailIter<I: Iterator> {
    inner: I,
    capacity: usize,
    buf: std::collections::VecDeque<I::Item>,
    loaded: bool,
}

impl<I: Iterator> TailIter<I> {
    pub fn new(inner: I, capacity: usize) -> Self {
        Self {
            inner,
            capacity,
            buf: std::collections::VecDeque::with_capacity(capacity.min(64)),
            loaded: false,
        }
    }
}

impl<I: Iterator> Iterator for TailIter<I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.loaded {
            self.loaded = true;
            if self.capacity == 0 {
                return None;
            }
            for item in self.inner.by_ref() {
                if self.buf.len() == self.capacity {
                    self.buf.pop_front();
                }
                self.buf.push_back(item);
            }
        }
        self.buf.pop_front()
    }
}

const TIMESTAMP_FILTER_INLINE_THRESHOLD: usize = 16;

// this may be overkill, but we'll try optimizing memory for a
// very common case of a very small number of timestamps
pub enum TimestampFilter {
    Set(IntSet<Timestamp>),
    List(SmallVec<[Timestamp; TIMESTAMP_FILTER_INLINE_THRESHOLD]>),
}

impl TimestampFilter {
    pub fn new(timestamps: &[Timestamp]) -> Self {
        if timestamps.len() > TIMESTAMP_FILTER_INLINE_THRESHOLD {
            Self::Set(IntSet::from_iter(timestamps.iter().copied()))
        } else {
            Self::List(SmallVec::from_slice(timestamps))
        }
    }

    pub fn matches(&self, ts: Timestamp) -> bool {
        match self {
            TimestampFilter::Set(set) => set.contains(&ts),
            TimestampFilter::List(list) => list.contains(&ts),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::series::TimestampRange;

    #[test]
    fn test_tail_iter() {
        let tail = |n: usize, items: &[i32]| -> Vec<i32> {
            TailIter::new(items.iter().copied(), n).collect()
        };

        // shorter, equal and longer than capacity; order preserved
        assert_eq!(tail(5, &[1, 2, 3]), vec![1, 2, 3]);
        assert_eq!(tail(3, &[1, 2, 3]), vec![1, 2, 3]);
        assert_eq!(tail(2, &[1, 2, 3, 4, 5]), vec![4, 5]);
        assert_eq!(tail(1, &[1, 2, 3]), vec![3]);
        // degenerate cases
        assert_eq!(tail(0, &[1, 2, 3]), Vec::<i32>::new());
        assert_eq!(tail(3, &[]), Vec::<i32>::new());
    }

    fn series_with(samples: &[(i64, f64)]) -> TimeSeries {
        let mut series = TimeSeries::default();
        for &(ts, value) in samples {
            series.add(ts, value, None);
        }
        series
    }

    fn sum_empty_buckets(bucket_duration: u64) -> AggregationOptions {
        AggregationOptions {
            aggregations: smallvec::smallvec![AggregationType::Sum.into()],
            bucket_duration,
            report_empty: true,
            ..Default::default()
        }
    }

    /// FILTER_BY_TS selects within the query window: an entry outside it returns nothing.
    #[test]
    fn test_timestamp_filter_is_limited_to_the_window() {
        let series = series_with(&[(0, 1.0), (2, 2.0), (4, 3.0), (6, 4.0)]);
        let options = RangeOptions {
            date_range: TimestampRange::from_timestamps(2, 4).unwrap(),
            timestamp_filter: Some(vec![0, 2, 4, 6]),
            ..Default::default()
        };

        let timestamps = |is_reverse| -> Vec<i64> {
            create_range_iterator(&series, &options, &None, None, is_reverse)
                .map(|s| s.timestamp)
                .collect()
        };
        assert_eq!(timestamps(false), vec![2, 4]);
        assert_eq!(timestamps(true), vec![4, 2]);
    }

    /// The window limits what is returned, not what EMPTY can see: entries past each edge are
    /// what make the buckets out to that edge reportable.
    #[test]
    fn test_empty_fill_reaches_window_edges_from_outside_filter_entries() {
        let series = series_with(&[(10, 1.0), (55, 2.0), (90, 3.0)]);
        let options = RangeOptions {
            date_range: TimestampRange::from_timestamps(50, 70).unwrap(),
            timestamp_filter: Some(vec![10, 55, 90]),
            aggregation: Some(sum_empty_buckets(5)),
            ..Default::default()
        };
        let expected = vec![(50, 0.0), (55, 2.0), (60, 0.0), (65, 0.0), (70, 0.0)];

        let scalar: Vec<(i64, f64)> = create_range_iterator(&series, &options, &None, None, false)
            .map(|s| (s.timestamp, s.value))
            .collect();
        assert_eq!(scalar, expected);

        let rows: Vec<(i64, f64)> = create_row_iterator(&series, &options, None, false)
            .map(|r| (r.timestamp, r.values[0]))
            .collect();
        assert_eq!(rows, expected);
    }

    /// LATEST's open bucket belongs after every stored sample in time, so it goes last on an
    /// ascending stream and first on a stream read newest-first. Aggregation always reads
    /// ascending and reverses afterwards, so there it goes last whichever way the reply runs.
    #[test]
    fn test_latest_sample_position_in_the_reply() {
        let series = series_with(&[(0, 1.0), (1000, 2.0), (2000, 3.0)]);
        let latest = Sample::new(3000, 9.0);
        let filter = Some(vec![1000, 2000, 3000]);
        let aggregation = Some(AggregationOptions {
            aggregations: smallvec::smallvec![AggregationType::Sum.into()],
            bucket_duration: 1000,
            ..Default::default()
        });
        let options = |timestamp_filter: Option<Vec<i64>>,
                       aggregation: Option<AggregationOptions>| {
            RangeOptions {
                date_range: TimestampRange::from_timestamps(0, 5000).unwrap(),
                timestamp_filter,
                aggregation,
                ..Default::default()
            }
        };
        let replied = |options: &RangeOptions, is_reverse: bool| -> Vec<(i64, f64)> {
            create_range_iterator(&series, options, &None, Some(latest), is_reverse)
                .map(|s| (s.timestamp, s.value))
                .collect()
        };

        let raw = options(None, None);
        let ascending = vec![(0, 1.0), (1000, 2.0), (2000, 3.0), (3000, 9.0)];
        let descending: Vec<(i64, f64)> = ascending.iter().rev().copied().collect();
        assert_eq!(replied(&raw, false), ascending);
        assert_eq!(replied(&raw, true), descending);

        let aggregated = options(None, aggregation.clone());
        assert_eq!(replied(&aggregated, false), ascending);
        assert_eq!(replied(&aggregated, true), descending);

        let filtered_ascending = vec![(1000, 2.0), (2000, 3.0), (3000, 9.0)];
        let filtered_descending: Vec<(i64, f64)> =
            filtered_ascending.iter().rev().copied().collect();

        let filtered = options(filter.clone(), None);
        assert_eq!(replied(&filtered, false), filtered_ascending);
        assert_eq!(replied(&filtered, true), filtered_descending);

        let filtered_aggregated = options(filter, aggregation);
        assert_eq!(replied(&filtered_aggregated, false), filtered_ascending);
        assert_eq!(replied(&filtered_aggregated, true), filtered_descending);

        let rows = |is_reverse: bool| -> Vec<(i64, f64)> {
            create_row_iterator(&series, &aggregated, Some(latest), is_reverse)
                .map(|r| (r.timestamp, r.values[0]))
                .collect()
        };
        assert_eq!(rows(false), ascending);
        assert_eq!(rows(true), descending);
    }

    /// A LATEST sample inside an existing bucket has to reach the aggregator in time order, or it
    /// opens a bucket of its own ahead of the stored samples.
    #[test]
    fn test_latest_sample_joins_its_bucket_on_a_reverse_aggregation() {
        let series = series_with(&[(0, 1.0), (1000, 2.0)]);
        let options = RangeOptions {
            date_range: TimestampRange::from_timestamps(0, 5000).unwrap(),
            aggregation: Some(AggregationOptions {
                aggregations: smallvec::smallvec![AggregationType::Sum.into()],
                bucket_duration: 1000,
                ..Default::default()
            }),
            ..Default::default()
        };

        let replied: Vec<(i64, f64)> =
            create_range_iterator(&series, &options, &None, Some(Sample::new(1500, 5.0)), true)
                .map(|s| (s.timestamp, s.value))
                .collect();

        assert_eq!(replied, vec![(1000, 7.0), (0, 1.0)]);
    }

    fn last_empty_rows(samples: &[(i64, f64)], start: i64, end: i64) -> Vec<(i64, Option<f64>)> {
        last_empty_rows_in(samples, start, end, false)
    }

    fn last_empty_rows_in(
        samples: &[(i64, f64)],
        start: i64,
        end: i64,
        is_reverse: bool,
    ) -> Vec<(i64, Option<f64>)> {
        let series = series_with(samples);
        let options = RangeOptions {
            date_range: TimestampRange::from_timestamps(start, end).unwrap(),
            aggregation: Some(AggregationOptions {
                aggregations: smallvec::smallvec![AggregationType::Last.into()],
                bucket_duration: 10,
                report_empty: true,
                ..Default::default()
            }),
            ..Default::default()
        };
        create_range_iterator(&series, &options, &None, None, is_reverse)
            .map(|s| (s.timestamp, (!s.value.is_nan()).then_some(s.value)))
            .collect()
    }

    /// Expected rows are replies from the pinned reference server. A reverse query does not
    /// mirror the forward one here: `last` looks back per gap, and a bucket holding only NaN
    /// takes what the newest-first scan was holding.
    #[test]
    fn test_reverse_last_empty_fill_around_stored_nan() {
        let a = [(0, 5.0), (10, f64::NAN), (100, 7.0)];
        let b = [
            (0, 5.0),
            (10, f64::NAN),
            (20, 6.0),
            (30, f64::NAN),
            (100, 7.0),
        ];
        let d = [(12, 7.0), (40, f64::NAN), (70, 6.0)];
        let e = [
            (24, f64::NAN),
            (35, 4.0),
            (56, f64::NAN),
            (101, f64::NAN),
            (119, 6.0),
            (124, f64::NAN),
        ];
        let held = |value: f64, newest: i64, oldest: i64| -> Vec<(i64, Option<f64>)> {
            (oldest..=newest)
                .rev()
                .step_by(10)
                .map(|t| (t, Some(value)))
                .collect()
        };

        // Bucket 10 holds only NaN and takes the 5 the scan was holding; forward it reports NaN.
        let mut expected = vec![(100, Some(7.0))];
        expected.extend(held(5.0, 90, 10));
        assert_eq!(last_empty_rows_in(&a, 10, 200, true), expected);

        // The sample at 20 is outside the window, yet it is what the later gaps look back to, and
        // the NaN-only bucket 30 holds it. Leading bucket 20 keeps its own seed.
        let mut expected = vec![(100, Some(7.0))];
        expected.extend(held(6.0, 90, 30));
        expected.push((20, Some(5.0)));
        assert_eq!(last_empty_rows_in(&b, 25, 200, true), expected);

        // Leading gaps do not look back: the 7 at 12 reaches the interior gaps only.
        assert_eq!(
            last_empty_rows_in(&d, 15, 70, true),
            vec![
                (70, Some(6.0)),
                (60, Some(7.0)),
                (50, Some(7.0)),
                (40, Some(7.0)),
                (30, None),
                (20, None),
                (10, None),
            ]
        );

        // Nothing is held when the scan opens on a NaN-only bucket; bucket 100 holds the later 6.
        assert_eq!(
            last_empty_rows_in(&e, 91, 131, true),
            vec![
                (120, None),
                (110, Some(6.0)),
                (100, Some(6.0)),
                (90, Some(4.0)),
            ]
        );
    }

    /// Expected rows are replies from the pinned reference server.
    #[test]
    fn test_last_empty_fill_around_stored_nan() {
        let a = [(0, 5.0), (10, f64::NAN), (100, 7.0)];
        let b = [
            (0, 5.0),
            (10, f64::NAN),
            (20, 6.0),
            (30, f64::NAN),
            (100, 7.0),
        ];
        let c = [(0, f64::NAN), (100, 7.0)];
        let held = |value: f64, from: i64, to: i64| -> Vec<(i64, Option<f64>)> {
            (from..=to).step_by(10).map(|t| (t, Some(value))).collect()
        };
        let then_seven = |mut rows: Vec<(i64, Option<f64>)>| {
            rows.push((100, Some(7.0)));
            rows
        };

        // The seed looks back past a stored NaN.
        assert_eq!(last_empty_rows(&a, 50, 200), then_seven(held(5.0, 50, 90)));
        assert_eq!(last_empty_rows(&a, 20, 200), then_seven(held(5.0, 20, 90)));
        assert_eq!(last_empty_rows(&b, 40, 200), then_seven(held(6.0, 40, 90)));

        // A leading gap takes the seed from before its own bucket, and later buckets hold it.
        assert_eq!(last_empty_rows(&b, 25, 200), then_seven(held(5.0, 20, 90)));

        // A first bucket holding only NaN reports NaN, not the seed; the gap after it looks back.
        let mut expected = vec![(10, None)];
        expected.extend(then_seven(held(5.0, 20, 90)));
        assert_eq!(last_empty_rows(&a, 10, 200), expected);

        // Nothing precedes bucket 0, so it stays NaN; the sample at 0 predates the window but is
        // still what a gap after the first window sample looks back to.
        let mut expected = vec![(0, None), (10, None)];
        expected.extend(then_seven(held(5.0, 20, 90)));
        assert_eq!(last_empty_rows(&a, 5, 200), expected);

        // A stored NaN before the window is not data, so nothing is filled back towards it.
        assert_eq!(last_empty_rows(&c, 50, 200), vec![(100, Some(7.0))]);

        // A leading run holds its seed, though the sample at 12 is in reach of a later gap.
        let d = [(12, 7.0), (40, f64::NAN), (70, 6.0)];
        assert_eq!(
            last_empty_rows(&d, 15, 70),
            vec![
                (10, None),
                (20, None),
                (30, None),
                (40, None),
                (50, Some(7.0)),
                (60, Some(7.0)),
                (70, Some(6.0)),
            ]
        );
    }

    /// Expected rows are replies from the pinned reference server.
    #[test]
    fn test_stored_nan_does_not_extend_the_empty_fill() {
        let sum_rows = |samples: &[(i64, f64)]| -> Vec<i64> {
            let series = series_with(samples);
            let options = RangeOptions {
                date_range: TimestampRange::from_timestamps(50, 90).unwrap(),
                aggregation: Some(sum_empty_buckets(10)),
                ..Default::default()
            };
            create_range_iterator(&series, &options, &None, None, false)
                .map(|s| s.timestamp)
                .collect()
        };

        assert_eq!(sum_rows(&[(0, f64::NAN), (100, 7.0)]), Vec::<i64>::new());
        assert_eq!(sum_rows(&[(0, 7.0), (100, f64::NAN)]), Vec::<i64>::new());
        assert_eq!(sum_rows(&[(0, f64::NAN), (60, 1.0)]), vec![60]);
        assert_eq!(sum_rows(&[(60, 1.0), (100, f64::NAN)]), vec![60]);
        assert_eq!(sum_rows(&[(0, 1.0), (100, 2.0)]), vec![50, 60, 70, 80, 90]);
    }

    #[test]
    fn test_reverse_limited_matches_reverse_then_count() {
        // The bounded reversal must return exactly what reversing everything and then taking
        // COUNT would, while buffering at most COUNT items.
        let items: Vec<i32> = (0..50).collect();
        for count in [None, Some(0), Some(1), Some(7), Some(50), Some(80)] {
            let expected: Vec<i32> = {
                let rev = items.iter().rev().copied();
                match count {
                    Some(n) => rev.take(n).collect(),
                    None => rev.collect(),
                }
            };
            let got: Vec<i32> = {
                let rev = super::reverse_limited(items.iter().copied(), count);
                match count {
                    Some(n) => rev.take(n).collect(),
                    None => rev.collect(),
                }
            };
            assert_eq!(got, expected, "count={count:?}");
        }
    }
}
