use crate::aggregators::{
    AggregateIterator, AggregationType, EmptyFillBounds, MultiAggregateIterator, bucket_start_for,
};
use crate::common::hash::IntSet;
use crate::common::{MultiSample, Sample, Timestamp};
use crate::iterators::{ReduceIterator, TimestampFilterIterator};
use crate::series::request_types::{AggregationOptions, RangeGroupingOptions, RangeOptions};
use crate::series::{SeriesSampleIterator, TimeSeries};
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

/// Timestamp of the series' earliest *visible* sample: the earliest stored one, or the
/// retention floor when that is later. `get_min_timestamp` alone gives the floor, which for a
/// series whose data all postdates it is not a sample timestamp at all — reading it as one
/// invents data before the first sample.
fn visible_first_timestamp(series: &TimeSeries) -> Timestamp {
    series.first_timestamp.max(series.get_min_timestamp())
}

/// Does any sample in `[from, to]` pass the query's `FILTER_BY_TS`/`FILTER_BY_VALUE`?
///
/// Used to look *outside* the queried window, which is why it takes the series rather than the
/// (clipped) sample stream. Unfiltered queries answer from the series' own extent without
/// reading a chunk; only a filtered one has to scan, and then only until its first hit.
fn has_passing_sample(
    series: &TimeSeries,
    options: &RangeOptions,
    from: Timestamp,
    to: Timestamp,
) -> bool {
    if from > to || series.is_empty() {
        return false;
    }

    let first = visible_first_timestamp(series);
    let last = series.last_timestamp();
    if to < first || from > last {
        return false;
    }

    if options.value_filter.is_none() && options.timestamp_filter.is_none() {
        return true;
    }

    let ts_filter = options
        .timestamp_filter
        .as_deref()
        .map(TimestampFilter::new);
    let value_filter = options.value_filter;
    SeriesSampleIterator::new(series, from.max(first), to, false).any(|sample| {
        ts_filter
            .as_ref()
            .is_none_or(|filter| filter.matches(sample.timestamp))
            && value_filter.is_none_or(|filter| filter.is_match(sample.value))
    })
}

/// The last sample before `to` that passes the query's filters, scanning backwards from it.
/// `None` when there is none.
fn last_passing_sample_before(
    series: &TimeSeries,
    options: &RangeOptions,
    to: Timestamp,
) -> Option<Sample> {
    let first = visible_first_timestamp(series);
    if series.is_empty() || to < first {
        return None;
    }

    let ts_filter = options
        .timestamp_filter
        .as_deref()
        .map(TimestampFilter::new);
    let value_filter = options.value_filter;
    SeriesSampleIterator::new(series, first, to, true).find(|sample| {
        ts_filter
            .as_ref()
            .is_none_or(|filter| filter.matches(sample.timestamp))
            && value_filter.is_none_or(|filter| filter.is_match(sample.value))
    })
}

/// How far this query's `EMPTY` fill may run past the samples it sees — see
/// [`EmptyFillBounds`]. Cheap and `Default` (interior gaps only) whenever `EMPTY` was not
/// requested, so non-EMPTY queries never touch the series for this.
pub(crate) fn empty_fill_bounds(series: &TimeSeries, options: &RangeOptions) -> EmptyFillBounds {
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

    let start = (overlaps && leads).then_some(start_ts);

    // `last` fills a gap bucket with "the value of the last sample before *the bucket's
    // start*", so the seed is taken from before the first filled bucket, not from before the
    // window: the two differ whenever the window opens mid-bucket, and a sample in between
    // belongs to the bucket rather than preceding it.
    let carry_seed = if start.is_some() && last_carry_mask(aggregation).is_some() {
        let aligned = aggregation
            .alignment
            .get_aligned_timestamp(start_ts, end_ts);
        let first_bucket = bucket_start_for(start_ts, aligned, aggregation.bucket_duration);
        last_passing_sample_before(series, options, first_bucket.saturating_sub(1))
            .map_or(f64::NAN, |sample| sample.value)
    } else {
        f64::NAN
    };

    EmptyFillBounds {
        start,
        end: (overlaps && trails).then_some(end_ts),
        carry_seed,
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
    let has_aggregation = options.aggregation.is_some();
    let should_reverse_iter = !has_aggregation && is_reverse;
    let should_reverse_aggr = has_aggregation && is_reverse;
    // Derived from the series, before the stream is clipped to the query window.
    let empty_fill = empty_fill_bounds(series, options);

    // Helper to handle the "latest sample" chaining logic which depends on direction
    // and avoids boxing by using generics.
    #[allow(clippy::too_many_arguments)]
    fn chain_latest<'a, I>(
        base: I,
        latest: Option<Sample>,
        opts: &RangeOptions,
        grp: &Option<RangeGroupingOptions>,
        reverse_aggr: bool,
        should_reverse_iter: bool,
        empty_fill: EmptyFillBounds,
    ) -> Box<dyn Iterator<Item = Sample> + 'a>
    where
        I: Iterator<Item = Sample> + 'a,
    {
        if let Some(sample) = latest {
            let latest_iter = std::iter::once(sample);
            if should_reverse_iter && opts.aggregation.is_none() {
                create_sample_iterator_adapter(
                    latest_iter.chain(base),
                    opts,
                    grp,
                    reverse_aggr,
                    empty_fill,
                )
            } else {
                create_sample_iterator_adapter(
                    base.chain(latest_iter),
                    opts,
                    grp,
                    reverse_aggr,
                    empty_fill,
                )
            }
        } else {
            create_sample_iterator_adapter(base, opts, grp, reverse_aggr, empty_fill)
        }
    }

    if let Some(ts_filter) = options.timestamp_filter.as_ref() {
        let base_iter = TimestampFilterIterator::new(series, ts_filter, should_reverse_iter);
        // Remove the timestamp filter from options to avoid double filtering
        let opts = RangeOptions {
            date_range: options.date_range,
            count: options.count,
            latest: false,
            aggregation: options.aggregation.clone(),
            value_filter: options.value_filter,
            timestamp_filter: None,
        };
        chain_latest(
            base_iter,
            latest_sample,
            &opts,
            grouping,
            should_reverse_aggr,
            is_reverse,
            empty_fill,
        )
    } else {
        let base_iter =
            SeriesSampleIterator::from_range_options(series, options, should_reverse_iter);
        chain_latest(
            base_iter,
            latest_sample,
            options,
            grouping,
            should_reverse_aggr,
            is_reverse,
            empty_fill,
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
            Box::new(TimestampFilterIterator::new(series, ts_filter, false))
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
    let carry = last_carry_mask(aggregation);
    let empty_fill = empty_fill_bounds(series, options);
    let aggr_iter = MultiAggregateIterator::with_empty_fill(
        filtered,
        aggregation,
        aligned_timestamp,
        empty_fill,
    );

    finalize_row_iterator(
        aggr_iter,
        is_reverse,
        options.count,
        carry,
        empty_fill.carry_seed,
    )
}

/// Apply the `last` EMPTY carry, reversal and COUNT to a row stream (COUNT limits output
/// buckets, exactly like the sample path).
pub(crate) fn finalize_row_iterator<'a, I: Iterator<Item = MultiSample> + 'a>(
    iter: I,
    is_reverse: bool,
    count: Option<usize>,
    carry: Option<SmallVec<[bool; 2]>>,
    carry_seed: f64,
) -> Box<dyn Iterator<Item = MultiSample> + 'a> {
    match (is_reverse, carry) {
        (true, Some(mask)) => {
            let rev = ReverseIter::new(CarryLastEmpty::new(iter, mask, carry_seed));
            apply_iter_limit!(rev, count)
        }
        (true, None) => {
            let rev = ReverseIter::new(iter);
            apply_iter_limit!(rev, count)
        }
        (false, Some(mask)) => {
            let filled = CarryLastEmpty::new(iter, mask, carry_seed);
            apply_iter_limit!(filled, count)
        }
        (false, None) => apply_iter_limit!(iter, count),
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

    // Helper to apply the EMPTY carry, reversal and limits, then box.
    // This ensures we only box once at the very end of the chain.
    //
    // Order matters: the carry runs chronologically (before reversal, if any) but before
    // COUNT, so a truncated reply still carries from the buckets that precede it in time.
    fn finalize<'a, I: Iterator<Item = Sample> + 'a>(
        iter: I,
        is_reverse: bool,
        count: Option<usize>,
        carry: Option<SmallVec<[bool; 2]>>,
        carry_seed: f64,
    ) -> Box<dyn Iterator<Item = Sample> + 'a> {
        match (is_reverse, carry) {
            (true, Some(mask)) => {
                let rev = ReverseIter::new(CarryLastEmpty::new(iter, mask, carry_seed));
                apply_iter_limit!(rev, count)
            }
            (true, None) => {
                let rev = ReverseIter::new(iter);
                apply_iter_limit!(rev, count)
            }
            (false, Some(mask)) => {
                let filled = CarryLastEmpty::new(iter, mask, carry_seed);
                apply_iter_limit!(filled, count)
            }
            (false, None) => apply_iter_limit!(iter, count),
        }
    }

    match (&options.aggregation, grouping) {
        (Some(agg), Some(grp)) => {
            // No carry here: the group reducer combines across series, so a gap in one
            // series is not a gap in the reduced bucket.
            let aggr_iter = create_aggregate_iterator(filtered, options, agg, empty_fill);
            let aggregator = grp.aggregation.create_aggregator();
            let reducer = ReduceIterator::new(aggr_iter, aggregator);
            finalize(reducer, is_reverse, count, None, f64::NAN)
        }
        (None, Some(grp)) => {
            let aggregator = grp.aggregation.create_aggregator();
            let reducer = ReduceIterator::new(filtered, aggregator);
            finalize(reducer, is_reverse, count, None, f64::NAN)
        }
        (Some(agg), None) => {
            let carry = last_carry_mask(agg);
            let aggr_iter = create_aggregate_iterator(filtered, options, agg, empty_fill);
            finalize(aggr_iter, is_reverse, count, carry, empty_fill.carry_seed)
        }
        (None, None) => finalize(filtered, is_reverse, count, None, f64::NAN),
    }
}

/// One value slot of an emitted bucket, so the EMPTY carry can be applied to the sample
/// stream and the multi-aggregation row stream through the same adapter.
pub(crate) trait BucketValues {
    fn values_mut(&mut self) -> &mut [f64];
}

impl BucketValues for Sample {
    fn values_mut(&mut self) -> &mut [f64] {
        std::slice::from_mut(&mut self.value)
    }
}

impl BucketValues for MultiSample {
    fn values_mut(&mut self) -> &mut [f64] {
        &mut self.values
    }
}

/// Fills each `last` column's gap buckets with the chronologically preceding value.
///
/// RTS's EMPTY fill for `last` is "the value already reported for the preceding bucket in
/// time" (RedisTimeSeries 8.10; prior RTS pins defined the carry against output order
/// instead, so a reverse query carried from the chronologically *later* neighbor — see the
/// 8.10 reference bump for the reproducer). We always aggregate forward and reverse the
/// finished buckets, so running the carry *before* `ReverseIter` — while the stream is still
/// chronological — lands on the same answer for both directions with one rule; running it
/// after reversal is also why the aggregator itself cannot do the carry (see
/// `LastAggregator::empty_value`).
///
/// `carry[i]` marks the columns whose requested aggregator is `last`.
pub(crate) struct CarryLastEmpty<I: Iterator> {
    inner: I,
    carry: SmallVec<[bool; 2]>,
    prev: SmallVec<[f64; 2]>,
}

impl<I: Iterator> CarryLastEmpty<I> {
    /// `seed` is what a gap bucket inherits before any bucket has been seen: the last
    /// passing sample before the query window (see [`EmptyFillBounds::carry_seed`]), or NaN
    /// when the fill does not reach back past the first sample the query read.
    pub fn new(inner: I, carry: SmallVec<[bool; 2]>, seed: f64) -> Self {
        let prev = smallvec::smallvec![seed; carry.len()];
        Self { inner, carry, prev }
    }
}

impl<I: Iterator> Iterator for CarryLastEmpty<I>
where
    I::Item: BucketValues,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let mut item = self.inner.next()?;
        for (idx, value) in item.values_mut().iter_mut().enumerate() {
            if self.carry.get(idx).copied() != Some(true) {
                continue;
            }
            if value.is_nan() {
                *value = self.prev[idx];
            } else {
                self.prev[idx] = *value;
            }
        }
        Some(item)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

/// Which output columns need the `last` EMPTY carry, or `None` when none do (the common
/// case, so the adapter is skipped entirely).
pub(crate) fn last_carry_mask(options: &AggregationOptions) -> Option<SmallVec<[bool; 2]>> {
    if !options.report_empty {
        return None;
    }
    let mask: SmallVec<[bool; 2]> = options
        .aggregations
        .iter()
        .map(|a| matches!(a.aggregation_type(), AggregationType::Last))
        .collect();
    mask.iter().any(|c| *c).then_some(mask)
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
    use super::TailIter;

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
}
