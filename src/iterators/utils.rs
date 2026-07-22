use crate::aggregators::{AggregateIterator, AggregationType, MultiAggregateIterator};
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
) -> AggregateIterator<I>
where
    I: Iterator<Item = Sample>,
{
    let (start_ts, end_ts) = range.get_timestamp_range();
    let aligned_timestamp = aggregation
        .alignment
        .get_aligned_timestamp(start_ts, end_ts);

    AggregateIterator::new(iter, aggregation, aligned_timestamp)
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

    // Helper to handle the "latest sample" chaining logic which depends on direction
    // and avoids boxing by using generics.
    fn chain_latest<'a, I>(
        base: I,
        latest: Option<Sample>,
        opts: &RangeOptions,
        grp: &Option<RangeGroupingOptions>,
        reverse_aggr: bool,
        should_reverse_iter: bool,
    ) -> Box<dyn Iterator<Item = Sample> + 'a>
    where
        I: Iterator<Item = Sample> + 'a,
    {
        if let Some(sample) = latest {
            let latest_iter = std::iter::once(sample);
            if should_reverse_iter && opts.aggregation.is_none() {
                create_sample_iterator_adapter(latest_iter.chain(base), opts, grp, reverse_aggr)
            } else {
                create_sample_iterator_adapter(base.chain(latest_iter), opts, grp, reverse_aggr)
            }
        } else {
            create_sample_iterator_adapter(base, opts, grp, reverse_aggr)
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
    // Same scan-order swap the sample path applies: this stream is aggregated forward and
    // reversed at the end too, so `first`/`last` have to be exchanged for a reverse query.
    let carry = last_carry_mask(aggregation);
    let scan_aggregation = aggregation.for_scan_order(is_reverse);
    let aggr_iter = MultiAggregateIterator::new(filtered, &scan_aggregation, aligned_timestamp);

    finalize_row_iterator(aggr_iter, is_reverse, options.count, carry)
}

/// Apply reversal, the `last` EMPTY carry and COUNT to a row stream (COUNT limits output
/// buckets, exactly like the sample path).
pub(crate) fn finalize_row_iterator<'a, I: Iterator<Item = MultiSample> + 'a>(
    iter: I,
    is_reverse: bool,
    count: Option<usize>,
    carry: Option<SmallVec<[bool; 2]>>,
) -> Box<dyn Iterator<Item = MultiSample> + 'a> {
    match (is_reverse, carry) {
        (true, Some(mask)) => {
            let filled = CarryLastEmpty::new(ReverseIter::new(iter), mask);
            apply_iter_limit!(filled, count)
        }
        (true, None) => {
            let rev = ReverseIter::new(iter);
            apply_iter_limit!(rev, count)
        }
        (false, Some(mask)) => {
            let filled = CarryLastEmpty::new(iter, mask);
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

    // Helper to apply reversal, the EMPTY carry and limits, then box.
    // This ensures we only box once at the very end of the chain.
    //
    // Order matters: the carry runs on the reversed stream (output order) but before COUNT,
    // so a truncated reply still carries from the buckets that precede it.
    fn finalize<'a, I: Iterator<Item = Sample> + 'a>(
        iter: I,
        is_reverse: bool,
        count: Option<usize>,
        carry: Option<SmallVec<[bool; 2]>>,
    ) -> Box<dyn Iterator<Item = Sample> + 'a> {
        match (is_reverse, carry) {
            (true, Some(mask)) => {
                let filled = CarryLastEmpty::new(ReverseIter::new(iter), mask);
                apply_iter_limit!(filled, count)
            }
            (true, None) => {
                let rev = ReverseIter::new(iter);
                apply_iter_limit!(rev, count)
            }
            (false, Some(mask)) => {
                let filled = CarryLastEmpty::new(iter, mask);
                apply_iter_limit!(filled, count)
            }
            (false, None) => apply_iter_limit!(iter, count),
        }
    }

    match (&options.aggregation, grouping) {
        (Some(agg), Some(grp)) => {
            // No carry here: the group reducer combines across series, so a gap in one
            // series is not a gap in the reduced bucket.
            let scan_agg = agg.for_scan_order(is_reverse);
            let aggr_iter = create_aggregate_iterator(filtered, options, &scan_agg);
            let aggregator = grp.aggregation.create_aggregator();
            let reducer = ReduceIterator::new(aggr_iter, aggregator);
            finalize(reducer, is_reverse, count, None)
        }
        (None, Some(grp)) => {
            let aggregator = grp.aggregation.create_aggregator();
            let reducer = ReduceIterator::new(filtered, aggregator);
            finalize(reducer, is_reverse, count, None)
        }
        (Some(agg), None) => {
            let carry = last_carry_mask(agg);
            let scan_agg = agg.for_scan_order(is_reverse);
            let aggr_iter = create_aggregate_iterator(filtered, options, &scan_agg);
            finalize(aggr_iter, is_reverse, count, carry)
        }
        (None, None) => finalize(filtered, is_reverse, count, None),
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

/// Fills each `last` column's gap buckets with the previously *emitted* value.
///
/// RTS's EMPTY fill for `last` is "the value already reported for the preceding bucket",
/// which is a statement about output order, not about time: forward over `7 _ _ _ 9` it
/// reports `7,7,7,9`, and in reverse it reports `9,9,9,7`. Running in output order — after
/// `ReverseIter` — is what makes one rule cover both, and it is also why the aggregator
/// itself cannot do the carry (see `LastAggregator::empty_value`).
///
/// `carry[i]` marks the columns whose *requested* aggregator is `last`. It must be built
/// from the request rather than from the aggregators actually running, because a reverse
/// query swaps `first`/`last` to compensate for scanning forward (`for_scan_order`) — the
/// fill rule follows the name the caller asked for, while the swap only decides which
/// sample of a non-empty bucket wins.
pub(crate) struct CarryLastEmpty<I: Iterator> {
    inner: I,
    carry: SmallVec<[bool; 2]>,
    prev: SmallVec<[f64; 2]>,
}

impl<I: Iterator> CarryLastEmpty<I> {
    pub fn new(inner: I, carry: SmallVec<[bool; 2]>) -> Self {
        let prev = smallvec::smallvec![f64::NAN; carry.len()];
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
