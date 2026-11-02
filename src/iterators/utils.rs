use crate::aggregators::{
    AggregateIterator, AggregationType, EmptyFillBounds, MultiAggregateIterator, bucket_start_for,
};
use crate::common::hash::IntSet;
use crate::common::{MultiSample, Sample, Timestamp};
use crate::iterators::TimestampFilterIterator;
use crate::series::request_types::{AggregationOptions, RangeOptions};
use crate::series::{SeriesSampleIterator, TimeSeries, ValueFilter};
use itertools::Either;
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

fn create_aggregate_iterator<I>(
    iter: I,
    range: &RangeOptions,
    aggregation: &AggregationOptions,
    empty_fill: EmptyFillBounds,
    baseline: Option<Sample>,
) -> AggregateIterator<I>
where
    I: Iterator<Item = Sample>,
{
    let (start_ts, end_ts) = range.get_timestamp_range();
    let aligned_timestamp = aggregation
        .alignment
        .get_aligned_timestamp(start_ts, end_ts);

    AggregateIterator::with_empty_fill(iter, aggregation, aligned_timestamp, empty_fill)
        .with_baseline(baseline)
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

/// What a read starting at `read_floor` has to give the counter aggregators
/// ([`AggregationType::carries_baseline`]) to match one from the start of the window: the last
/// sample before the floor that the pipeline would have fed them, a non-NaN reading that passes
/// `FILTER_BY_VALUE`. `None` for an unfloored read, or one with no counter aggregator.
///
/// Floors are only planned without `FILTER_BY_TS` ([`ReverseTailRead::plan`]), so that filter
/// cannot apply here.
fn counter_baseline(
    series: &TimeSeries,
    options: &RangeOptions,
    read_floor: Option<Timestamp>,
) -> Option<Sample> {
    let floor = read_floor?;
    let aggregation = options.aggregation.as_ref()?;
    if !aggregation
        .aggregations
        .iter()
        .any(|config| config.aggregation.carries_baseline())
    {
        return None;
    }
    debug_assert!(options.timestamp_filter.is_none());
    // The same lower bound as `range_reader`: nothing below it reaches an unfloored read either.
    let (start, _) = options.get_timestamp_range();
    let lower = start.max(series.get_min_timestamp());
    if floor <= lower {
        return None;
    }
    let value_filter = options.value_filter;
    SeriesSampleIterator::new(series, lower, floor - 1, true)
        .find(|sample| is_empty_fill_data(sample, value_filter))
}

/// How far this query's `EMPTY` fill may run past the samples it sees — see
/// [`EmptyFillBounds`]. Cheap and `Default` (interior gaps only) whenever `EMPTY` was not
/// requested, so non-EMPTY queries never touch the series for this.
///
/// `carry_reverse` says the query's *reply* runs newest-first, which `last` depends on even though
/// buckets are always produced chronologically. `tail_limit` says only that many of the stream's
/// last rows will be used.
fn empty_fill_bounds(
    series: &TimeSeries,
    options: &RangeOptions,
    carry_reverse: bool,
    tail_limit: Option<usize>,
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
        tail_limit,
    }
}

/// Create an optimized range iterator for the given series and options
pub fn create_range_iterator<'a>(
    series: &'a TimeSeries,
    options: &RangeOptions,
    latest_sample: Option<Sample>,
    is_reverse: bool,
) -> Box<dyn Iterator<Item = Sample> + 'a> {
    create_range_iterator_for_reply(series, options, latest_sample, is_reverse, is_reverse)
}

/// Reverse COUNT over buckets, without aggregating the whole range.
///
/// Buckets are always built forward, so a newest-first reply has to read ahead of what it returns.
/// But a bucket depends on nothing before it except, for the counter aggregators, the one sample
/// it carries in ([`counter_baseline`]), so COUNT `n` only needs enough of the range to hold `n`
/// of them: this runs the ordinary pipeline over the newest `n` possible buckets, seeded with that
/// sample, and widens when filters or gaps leave it short. Anything else about the pipeline stays as it is, including
/// how each aggregator reads its samples in time order.
///
/// `EMPTY` is not handled here, since its buckets depend on the whole series, and `FILTER_BY_TS`
/// already reads only the timestamps it names.
struct ReverseTailRead {
    count: usize,
    newest_bucket: Timestamp,
    bucket_duration: u64,
    window_start: Timestamp,
    /// A value filter that emptied the newest buckets says nothing good about the ones before
    /// them, so a short read goes straight to the whole window rather than paying for more passes.
    filtered: bool,
}

impl ReverseTailRead {
    /// How much a short read widens by.
    const GROWTH: i128 = 4;

    fn plan(
        series: &TimeSeries,
        options: &RangeOptions,
        latest: Option<Sample>,
        count: usize,
    ) -> Option<Self> {
        if series.is_empty() || options.timestamp_filter.is_some() {
            return None;
        }
        let aggregation = options.aggregation.as_ref().filter(|a| !a.report_empty)?;

        let (start, end) = options.get_timestamp_range();
        let newest = latest
            .map_or(series.last_timestamp(), |sample| {
                sample.timestamp.max(series.last_timestamp())
            })
            .min(end);
        if newest < start {
            return None;
        }

        let aligned = aggregation.alignment.get_aligned_timestamp(start, end);
        Some(Self {
            count,
            newest_bucket: bucket_start_for(newest, aligned, aggregation.bucket_duration),
            bucket_duration: aggregation.bucket_duration,
            window_start: start,
            filtered: options.value_filter.is_some(),
        })
    }

    /// Where to start reading to cover the newest `buckets` buckets, or `None` once that is most
    /// of the window, when reading the rest as well costs little more than failing and re-reading
    /// it. Always a bucket start, so no bucket is read partially.
    fn read_floor(&self, buckets: i128) -> Option<Timestamp> {
        let span = (buckets - 1).saturating_mul(self.bucket_duration as i128);
        let floor = (self.newest_bucket as i128).saturating_sub(span);
        let window = self.newest_bucket as i128 - self.window_start as i128;
        (span <= window / 2).then_some(floor as Timestamp)
    }

    /// `pipeline` builds the query's whole reversed and limited output from a read floor.
    fn read<'a, T: 'a>(
        &self,
        mut pipeline: impl FnMut(Option<Timestamp>) -> Box<dyn Iterator<Item = T> + 'a>,
    ) -> Box<dyn Iterator<Item = T> + 'a> {
        let mut buckets = self.count as i128;
        while let Some(floor) = self.read_floor(buckets) {
            let rows: Vec<T> = pipeline(Some(floor)).collect();
            if rows.len() >= self.count {
                return Box::new(rows.into_iter());
            }
            if self.filtered {
                break;
            }
            buckets = buckets.saturating_mul(Self::GROWTH);
        }
        pipeline(None)
    }
}

/// [`create_range_iterator`] for a stream whose reply order is not its own: a shard streams
/// ascending for a coordinator that will reverse it, and `reply_reverse` tells EMPTY about that.
pub(crate) fn create_range_iterator_for_reply<'a>(
    series: &'a TimeSeries,
    options: &RangeOptions,
    latest_sample: Option<Sample>,
    is_reverse: bool,
    reply_reverse: bool,
) -> Box<dyn Iterator<Item = Sample> + 'a> {
    let tail_limit = options.count.filter(|_| is_reverse);
    let build = |read_floor| {
        range_pipeline(
            series,
            options,
            latest_sample,
            ReadPlan {
                is_reverse,
                reply_reverse,
                read_floor,
                tail_limit,
            },
        )
    };
    match tail_limit.and_then(|count| ReverseTailRead::plan(series, options, latest_sample, count))
    {
        Some(tail) => tail.read(build),
        None => build(None),
    }
}

/// A series' newest `count` items, oldest first, reading no more of it than that takes.
///
/// A shard's half of a newest-first COUNT: it streams ascending for the coordinator to reverse, and
/// only the tail can reach the reply. `reply_reverse` is the reply's real order, for EMPTY.
pub(crate) fn create_range_tail_iterator<'a>(
    series: &'a TimeSeries,
    options: &RangeOptions,
    latest_sample: Option<Sample>,
    count: usize,
    reply_reverse: bool,
) -> Box<dyn Iterator<Item = Sample> + 'a> {
    // Raw samples read newest-first without any buffering beyond a chunk, so the first `count`
    // that pass the filters are the answer.
    if options.aggregation.is_none() {
        let plan = ReadPlan {
            is_reverse: true,
            reply_reverse,
            read_floor: None,
            tail_limit: None,
        };
        let mut newest_first: Vec<Sample> = range_pipeline(series, options, latest_sample, plan)
            .take(count)
            .collect();
        newest_first.reverse();
        return Box::new(newest_first.into_iter());
    }

    let build = |read_floor| -> Box<dyn Iterator<Item = Sample> + 'a> {
        let plan = ReadPlan {
            is_reverse: false,
            reply_reverse,
            read_floor,
            tail_limit: Some(count),
        };
        Box::new(TailIter::new(
            range_pipeline(series, options, latest_sample, plan),
            count,
        ))
    };
    match ReverseTailRead::plan(series, options, latest_sample, count) {
        Some(tail) => tail.read(build),
        None => build(None),
    }
}

/// How one pass reads a series for a reply.
#[derive(Clone, Copy)]
struct ReadPlan {
    /// The stream this pass produces runs newest-first.
    is_reverse: bool,
    /// The reply runs newest-first, whichever way this stream does.
    reply_reverse: bool,
    /// Read nothing older than this.
    read_floor: Option<Timestamp>,
    /// Only this many of the stream's last rows will be used.
    tail_limit: Option<usize>,
}

fn range_pipeline<'a>(
    series: &'a TimeSeries,
    options: &RangeOptions,
    latest_sample: Option<Sample>,
    plan: ReadPlan,
) -> Box<dyn Iterator<Item = Sample> + 'a> {
    let ReadPlan {
        is_reverse,
        reply_reverse,
        read_floor,
        tail_limit,
    } = plan;
    let has_aggregation = options.aggregation.is_some();
    let should_reverse_iter = !has_aggregation && is_reverse;
    let should_reverse_aggr = has_aggregation && is_reverse;
    // Derived from the series, before the stream is clipped to the query window.
    let empty_fill = empty_fill_bounds(series, options, reply_reverse, tail_limit);

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
            is_reverse: should_reverse_aggr,
            empty_fill,
            baseline: None,
        };
        output.chain_latest(base_iter, latest_sample, should_reverse_iter)
    } else {
        let base_iter = range_reader(series, options, should_reverse_iter, read_floor);
        let output = SampleOutput {
            options,
            is_reverse: should_reverse_aggr,
            empty_fill,
            baseline: counter_baseline(series, options, read_floor),
        };
        output.chain_latest(base_iter, latest_sample, should_reverse_iter)
    }
}

/// A series' samples within the query's window, or from `read_floor` if that is later.
fn range_reader<'a>(
    series: &'a TimeSeries,
    options: &RangeOptions,
    is_reverse: bool,
    read_floor: Option<Timestamp>,
) -> SeriesSampleIterator<'a> {
    let (start, end) = options.get_timestamp_range();
    let start = start
        .max(series.get_min_timestamp())
        .max(read_floor.unwrap_or(Timestamp::MIN));
    SeriesSampleIterator::new(series, start, end, is_reverse)
}

/// What happens to a series' samples once they have been read.
struct SampleOutput<'o> {
    options: &'o RangeOptions,
    /// Reverse the *finished* output. Distinct from reading the series newest-first, which is
    /// what aggregation cannot do.
    is_reverse: bool,
    empty_fill: EmptyFillBounds,
    /// See [`counter_baseline`].
    baseline: Option<Sample>,
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
        sample_iterator_adapter(
            stream,
            self.options,
            self.is_reverse,
            self.empty_fill,
            self.baseline,
        )
    }
}

/// The pipeline `create_range_iterator` builds after its base reader, for a base
/// that is not a live series: `base` must already be in the direction the
/// query iterates (descending for a non-aggregated reverse query), `latest` and
/// `empty_fill` must have been derived from the series beforehand (see
/// [`range_empty_fill_bounds`]). Used by the deferred TS.MRANGE path, whose base is a
/// [`crate::series::RangeSnapshot`] decoded off the main thread. `FILTER_BY_TS` is not
/// supported here (its base reader needs the series), and the base covers the whole
/// window, so there is no read floor and no counter baseline.
pub(crate) fn create_range_iterator_from_base<'a, I>(
    base: I,
    options: &RangeOptions,
    latest_sample: Option<Sample>,
    is_reverse: bool,
    empty_fill: EmptyFillBounds,
) -> Box<dyn Iterator<Item = Sample> + 'a>
where
    I: Iterator<Item = Sample> + 'a,
{
    debug_assert!(options.timestamp_filter.is_none());
    let has_aggregation = options.aggregation.is_some();
    let should_reverse_iter = !has_aggregation && is_reverse;
    let output = SampleOutput {
        options,
        is_reverse: has_aggregation && is_reverse,
        empty_fill,
        baseline: None,
    };
    output.chain_latest(base, latest_sample, should_reverse_iter)
}

/// The [`EmptyFillBounds`] [`create_range_iterator_for_reply`] derives for `series`, for a
/// caller that builds the pipeline from a base it read separately
/// ([`create_range_iterator_from_base`]). `reply_reverse` is the reply's real order, which
/// differs from `is_reverse` on a shard that streams ascending for a coordinator that
/// reverses it.
pub(crate) fn range_empty_fill_bounds(
    series: &TimeSeries,
    options: &RangeOptions,
    is_reverse: bool,
    reply_reverse: bool,
) -> EmptyFillBounds {
    let tail_limit = options.count.filter(|_| is_reverse);
    empty_fill_bounds(series, options, reply_reverse, tail_limit)
}

/// Pre-aggregation composition for one series: base reader (ts-filter variant
/// when FILTER_BY_TS is present) + LATEST chaining + value filter. The output
/// is always ascending — multi-aggregation consumes ascending input and
/// reverses rows post-aggregation.
fn create_filtered_sample_iterator<'a>(
    series: &'a TimeSeries,
    options: &RangeOptions,
    latest_sample: Option<Sample>,
    read_floor: Option<Timestamp>,
) -> Box<dyn Iterator<Item = Sample> + 'a> {
    let base_iter: Box<dyn Iterator<Item = Sample> + 'a> =
        if let Some(ts_filter) = options.timestamp_filter.as_ref() {
            // The base iterator applies the timestamp filter; no re-filtering needed.
            let (start_ts, end_ts) = options.get_timestamp_range();
            Box::new(TimestampFilterIterator::new(
                series, ts_filter, start_ts, end_ts, false,
            ))
        } else {
            Box::new(range_reader(series, options, false, read_floor))
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
    let tail_limit = options.count.filter(|_| is_reverse);
    let build = |read_floor| {
        row_pipeline(
            series,
            options,
            latest_sample,
            ReadPlan {
                is_reverse,
                reply_reverse,
                read_floor,
                tail_limit,
            },
        )
    };
    match tail_limit.and_then(|count| ReverseTailRead::plan(series, options, latest_sample, count))
    {
        Some(tail) => tail.read(build),
        None => build(None),
    }
}

/// [`create_range_tail_iterator`] for multi-aggregation rows.
pub(crate) fn create_row_tail_iterator<'a>(
    series: &'a TimeSeries,
    options: &RangeOptions,
    latest_sample: Option<Sample>,
    count: usize,
    reply_reverse: bool,
) -> Box<dyn Iterator<Item = MultiSample> + 'a> {
    let build = |read_floor| -> Box<dyn Iterator<Item = MultiSample> + 'a> {
        let plan = ReadPlan {
            is_reverse: false,
            reply_reverse,
            read_floor,
            tail_limit: Some(count),
        };
        Box::new(TailIter::new(
            row_pipeline(series, options, latest_sample, plan),
            count,
        ))
    };
    match ReverseTailRead::plan(series, options, latest_sample, count) {
        Some(tail) => tail.read(build),
        None => build(None),
    }
}

fn row_pipeline<'a>(
    series: &'a TimeSeries,
    options: &RangeOptions,
    latest_sample: Option<Sample>,
    plan: ReadPlan,
) -> Box<dyn Iterator<Item = MultiSample> + 'a> {
    let ReadPlan {
        is_reverse,
        reply_reverse,
        read_floor,
        tail_limit,
    } = plan;
    let aggregation = options
        .aggregation
        .as_ref()
        .expect("create_row_iterator requires aggregation options");

    let filtered = create_filtered_sample_iterator(series, options, latest_sample, read_floor);

    let (start_ts, end_ts) = options.get_timestamp_range();
    let aligned_timestamp = aggregation
        .alignment
        .get_aligned_timestamp(start_ts, end_ts);
    // first/last are chronological (earliest/latest sample) regardless of direction: this
    // stream is aggregated forward and reversed at the end, which already lands on the
    // chronological answer, so the aggregation is used as requested.
    let empty_fill = empty_fill_bounds(series, options, reply_reverse, tail_limit);
    let aggr_iter = MultiAggregateIterator::with_empty_fill(
        filtered,
        aggregation,
        aligned_timestamp,
        empty_fill,
    )
    .with_baseline(counter_baseline(series, options, read_floor));

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

/// Create a sample iterator adapter that applies filtering, aggregation and limits based on the
/// provided options. The resulting iterator yields samples according to the specified criteria.
/// Boxing is delayed to the last possible moment to allow for compiler optimizations.
pub(crate) fn create_sample_iterator_adapter<'a, T: Iterator<Item = Sample> + 'a>(
    base_iter: T,
    options: &RangeOptions,
    is_reverse: bool,
    empty_fill: EmptyFillBounds,
) -> Box<dyn Iterator<Item = Sample> + 'a> {
    sample_iterator_adapter(base_iter, options, is_reverse, empty_fill, None)
}

/// [`create_sample_iterator_adapter`] for a stream read from a floor, with the counter
/// aggregators' [`counter_baseline`].
fn sample_iterator_adapter<'a, T: Iterator<Item = Sample> + 'a>(
    base_iter: T,
    options: &RangeOptions,
    is_reverse: bool,
    empty_fill: EmptyFillBounds,
    baseline: Option<Sample>,
) -> Box<dyn Iterator<Item = Sample> + 'a> {
    // Apply Filters (Timestamp & Value)
    let ts_filter = options
        .timestamp_filter
        .as_ref()
        .map(|f| TimestampFilter::new(f));
    let val_filter = options.value_filter;

    // An unfiltered query (the common one) gets the base iterator itself rather than a
    // `Filter` adapter whose closure tests two `None`s per sample.
    let filtered: Either<T, _> = if ts_filter.is_none() && val_filter.is_none() {
        Either::Left(base_iter)
    } else {
        Either::Right(base_iter.filter(move |sample| {
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
        }))
    };

    let count = options.count;

    match &options.aggregation {
        Some(agg) => {
            let aggr_iter = create_aggregate_iterator(filtered, options, agg, empty_fill, baseline);
            finalize(aggr_iter, is_reverse, count)
        }
        None => finalize(filtered, is_reverse, count),
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
pub(crate) struct TailIter<I: Iterator> {
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
enum TimestampFilter {
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
            create_range_iterator(&series, &options, None, is_reverse)
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

        let scalar: Vec<(i64, f64)> = create_range_iterator(&series, &options, None, false)
            .map(|s| (s.timestamp, s.value))
            .collect();
        assert_eq!(scalar, expected);

        let rows: Vec<(i64, f64)> = create_row_iterator(&series, &options, None, false)
            .map(|r| (r.timestamp, r.values[0]))
            .collect();
        assert_eq!(rows, expected);
    }

    fn tail_read(count: usize) -> ReverseTailRead {
        ReverseTailRead {
            count,
            newest_bucket: 100_000,
            bucket_duration: 100,
            window_start: 0,
            filtered: false,
        }
    }

    /// Records where each pass reads from, and answers with however many rows the case gives it.
    fn read_floors(
        tail: &ReverseTailRead,
        rows_at: impl Fn(Option<i64>) -> usize,
    ) -> Vec<Option<i64>> {
        let floors = std::cell::RefCell::new(Vec::new());
        tail.read(|floor| {
            floors.borrow_mut().push(floor);
            Box::new(std::iter::repeat_n(0, rows_at(floor)))
        })
        .for_each(drop);
        floors.into_inner()
    }

    /// The point of the plan: one pass over just the newest buckets when they hold enough.
    #[test]
    fn test_reverse_tail_read_stops_at_the_first_window_with_enough_rows() {
        assert_eq!(read_floors(&tail_read(1), |_| 1), vec![Some(100_000)]);
        assert_eq!(read_floors(&tail_read(5), |_| 5), vec![Some(99_600)]);
    }

    #[test]
    fn test_reverse_tail_read_widens_until_it_has_enough() {
        // COUNT 3 starts 3 buckets back, then widens to 12, where this series has enough.
        let rows_at = |floor: Option<i64>| match floor {
            Some(floor) if floor >= 99_000 => 1,
            _ => 3,
        };
        assert_eq!(
            read_floors(&tail_read(3), rows_at),
            vec![Some(99_800), Some(98_900)]
        );
    }

    /// The value filter's worst case, matching almost nothing, gets one cheap look at the newest
    /// buckets and then the same whole read it would have had anyway.
    #[test]
    fn test_reverse_tail_read_with_a_value_filter_does_not_keep_widening() {
        let tail = ReverseTailRead {
            filtered: true,
            ..tail_read(1)
        };
        let floors = read_floors(&tail, |floor| if floor.is_none() { 1 } else { 0 });
        assert_eq!(floors, vec![Some(100_000), None]);
    }

    /// The counter aggregators read from a floor too, carrying in the last reading before it that
    /// the full read would have fed them: not a NaN, not one the value filter drops, and nothing
    /// from before the window.
    #[test]
    fn test_reverse_tail_read_seeds_counters_from_before_the_floor() {
        let series = series_with(&[
            (50, 1.0),
            (150, 3.0),
            (250, 20.0),
            (260, f64::NAN),
            (350, 5.0),
        ]);
        let options = |aggregation: AggregationType, start, value_filter| RangeOptions {
            date_range: TimestampRange::from_timestamps(start, 10_000).unwrap(),
            value_filter,
            aggregation: Some(AggregationOptions {
                aggregations: smallvec::smallvec![aggregation.into()],
                bucket_duration: 100,
                ..Default::default()
            }),
            ..Default::default()
        };
        for aggregation in [
            AggregationType::Increase,
            AggregationType::Rate,
            AggregationType::IRate,
        ] {
            let plain = options(aggregation, 0, None);
            assert!(ReverseTailRead::plan(&series, &plain, None, 1).is_some());
            assert_eq!(
                counter_baseline(&series, &plain, Some(300)),
                Some(Sample::new(250, 20.0))
            );

            let filtered = options(aggregation, 0, Some(ValueFilter { min: 0.0, max: 9.0 }));
            assert_eq!(
                counter_baseline(&series, &filtered, Some(300)),
                Some(Sample::new(150, 3.0))
            );

            let late_window = options(aggregation, 100, Some(ValueFilter { min: 0.0, max: 2.0 }));
            assert_eq!(counter_baseline(&series, &late_window, Some(300)), None);
        }

        let sum = options(AggregationType::Sum, 0, None);
        assert_eq!(counter_baseline(&series, &sum, Some(300)), None);
    }

    /// Past half the window, another partial pass stops being worth it: the total a failed search
    /// reads stays below 1.7 times the window, however the windows land.
    #[test]
    fn test_reverse_tail_read_goes_to_the_whole_window_past_halfway() {
        let floors = read_floors(&tail_read(1), |floor| if floor.is_none() { 1 } else { 0 });
        let last_partial = floors.iter().rev().flatten().next().copied().unwrap();
        assert!(last_partial >= 50_000, "{floors:?}");
        assert_eq!(floors.last(), Some(&None), "{floors:?}");

        let read: i128 = floors
            .iter()
            .map(|floor| 100_000 - floor.unwrap_or(0) as i128 + 100)
            .sum();
        assert!(read * 10 < 100_000 * 17, "{floors:?} read {read}");
    }

    /// A window that reaches the start of the range is the ordinary, unbounded read, and it ends
    /// the search whether or not it has enough rows: there is nothing further back to read.
    #[test]
    fn test_reverse_tail_read_falls_back_to_the_whole_window() {
        let floors = read_floors(&tail_read(1), |floor| if floor.is_none() { 1 } else { 0 });
        assert_eq!(floors.last(), Some(&None));
        assert_eq!(floors.iter().filter(|floor| floor.is_none()).count(), 1);
        assert!(floors.len() <= 8, "{floors:?}");

        // COUNT itself spans the window: no point reading anything but all of it.
        assert_eq!(read_floors(&tail_read(5000), |_| 0), vec![None]);
    }

    /// What a shard keeps for a newest-first COUNT has to be what the reply would show: the same
    /// rows as a full read, whichever way it reads the series. A grouped query's reply order does
    /// not reach the shard's EMPTY fill, so it is checked against the forward read instead.
    #[test]
    fn test_shard_tail_matches_a_full_read() {
        let same = |a: &[(i64, f64)], b: &[(i64, f64)]| {
            a.len() == b.len()
                && a.iter()
                    .zip(b)
                    .all(|(x, y)| x.0 == y.0 && (x.1 == y.1 || (x.1.is_nan() && y.1.is_nan())))
        };
        let pair = |s: Sample| (s.timestamp, s.value);
        let aggregations = [
            None,
            Some(AggregationType::Sum),
            Some(AggregationType::Last),
            Some(AggregationType::First),
            Some(AggregationType::Rate),
        ];

        for samples in tail_read_fixtures() {
            let series = series_with(&samples);
            for aggregation in aggregations {
                for report_empty in [false, true] {
                    for value_filter in [None, Some(ValueFilter { min: 2.0, max: 9.0 })] {
                        for timestamp_filter in [None, Some(vec![35, 70, 350, 390, 4000, 9000])] {
                            for latest in [None, Some(Sample::new(11_500, 4.0))] {
                                let options = RangeOptions {
                                    date_range: TimestampRange::from_timestamps(0, 12_000).unwrap(),
                                    value_filter,
                                    timestamp_filter: timestamp_filter.clone(),
                                    aggregation: aggregation.map(|aggregator| AggregationOptions {
                                        aggregations: smallvec::smallvec![aggregator.into()],
                                        bucket_duration: 100,
                                        report_empty,
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                };
                                let mut newest_first: Vec<(i64, f64)> =
                                    create_range_iterator(&series, &options, latest, true)
                                        .map(pair)
                                        .collect();
                                newest_first.reverse();
                                let forward: Vec<(i64, f64)> =
                                    create_range_iterator(&series, &options, latest, false)
                                        .map(pair)
                                        .collect();
                                let last_of = |rows: &[(i64, f64)], n: usize| {
                                    rows[rows.len().saturating_sub(n)..].to_vec()
                                };

                                for count in [1usize, 3, 1000] {
                                    let tail: Vec<(i64, f64)> = create_range_tail_iterator(
                                        &series, &options, latest, count, true,
                                    )
                                    .map(pair)
                                    .collect();
                                    assert!(
                                        same(&tail, &last_of(&newest_first, count)),
                                        "{aggregation:?} empty {report_empty} {value_filter:?} \
                                         {timestamp_filter:?} {latest:?} count {count}"
                                    );

                                    let grouped: Vec<(i64, f64)> = create_range_tail_iterator(
                                        &series, &options, latest, count, false,
                                    )
                                    .map(pair)
                                    .collect();
                                    // Raw samples have no EMPTY fill, so direction cannot differ.
                                    assert!(
                                        same(&grouped, &last_of(&forward, count)),
                                        "grouped, {aggregation:?} count {count}"
                                    );

                                    if options.aggregation.is_some() {
                                        let rows: Vec<(i64, f64)> = create_row_tail_iterator(
                                            &series, &options, latest, count, true,
                                        )
                                        .map(|r| (r.timestamp, r.values[0]))
                                        .collect();
                                        assert!(
                                            same(&rows, &last_of(&newest_first, count)),
                                            "rows, {aggregation:?} count {count}"
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// A newest-first COUNT skips most of a long run of gaps. It finishes at once on a span that
    /// would otherwise take days, and what it returns is what a short span shows row for row.
    #[test]
    fn test_reverse_empty_count_does_not_walk_the_gap() {
        let span = 1_000_000_000_000i64;
        let series = series_with(&[(0, 5.0), (10, f64::NAN), (span, 7.0)]);
        let options = |aggregator: AggregationType, count: usize| RangeOptions {
            date_range: TimestampRange::from_timestamps(0, span).unwrap(),
            count: Some(count),
            aggregation: Some(AggregationOptions {
                aggregations: smallvec::smallvec![aggregator.into()],
                bucket_duration: 1,
                report_empty: true,
                ..Default::default()
            }),
            ..Default::default()
        };
        let replied = |options: &RangeOptions| -> Vec<(i64, f64)> {
            create_range_iterator(&series, options, None, true)
                .map(|s| (s.timestamp, s.value))
                .collect()
        };

        assert_eq!(
            replied(&options(AggregationType::Sum, 3)),
            vec![(span, 7.0), (span - 1, 0.0), (span - 2, 0.0)]
        );
        assert_eq!(
            replied(&options(AggregationType::Last, 3)),
            vec![(span, 7.0), (span - 1, 5.0), (span - 2, 5.0)]
        );

        let row: Vec<(i64, f64)> =
            create_row_iterator(&series, &options(AggregationType::Last, 2), None, true)
                .map(|r| (r.timestamp, r.values[0]))
                .collect();
        assert_eq!(row, vec![(span, 7.0), (span - 1, 5.0)]);
    }

    /// Every shape of series a tail read could get wrong: gaps wider than a bucket, stored NaN,
    /// counter resets for the rate family, a burst in one bucket, and nothing near the end.
    fn tail_read_fixtures() -> Vec<Vec<(i64, f64)>> {
        let regular: Vec<(i64, f64)> = (0..80).map(|i| (i * 7, (i % 17) as f64)).collect();
        let counter: Vec<(i64, f64)> = (0..80)
            .map(|i| {
                (
                    i * 11,
                    if i % 30 == 29 {
                        0.0
                    } else {
                        (i % 30) as f64 * 3.0
                    },
                )
            })
            .collect();
        let gappy: Vec<(i64, f64)> = (0..60)
            .map(|i| (i * 5 + (i / 10) * 1000, i as f64))
            .collect();
        let with_nan: Vec<(i64, f64)> = (0..60)
            .map(|i| (i * 13, if i % 3 == 0 { f64::NAN } else { i as f64 }))
            .collect();
        let nan_tail: Vec<(i64, f64)> = (0..50)
            .map(|i| (i * 13, if i > 30 { f64::NAN } else { i as f64 }))
            .collect();
        let sparse = vec![(3, 1.0), (4000, 2.0), (4001, f64::NAN), (9000, 3.0)];
        vec![regular, counter, gappy, with_nan, nan_tail, sparse]
    }

    /// Reverse COUNT reads only as far back as it needs. Whatever it reads, the reply has to be
    /// the full reversed result cut to COUNT, which also proves no aggregator carries state from
    /// one bucket into the next.
    #[test]
    fn test_reverse_count_matches_the_full_reversed_result() {
        use crate::aggregators::{BucketAlignment, BucketTimestamp};
        use crate::common::binop::ComparisonOperator;
        use crate::series::request_types::{AggregatorConfig, ValueComparisonFilter};

        // all, any and none take a condition; without one there is nothing to build.
        let config = |aggregator: AggregationType| {
            let condition = ValueComparisonFilter {
                operator: ComparisonOperator::GreaterThan,
                value: 5.0,
            };
            AggregatorConfig::new(aggregator, aggregator.is_filtered().then_some(condition))
                .unwrap()
        };

        let aggregators = [
            AggregationType::All,
            AggregationType::Any,
            AggregationType::Avg,
            AggregationType::Count,
            AggregationType::CountAll,
            AggregationType::CountNan,
            AggregationType::First,
            AggregationType::Increase,
            AggregationType::IRate,
            AggregationType::Last,
            AggregationType::Max,
            AggregationType::Min,
            AggregationType::None,
            AggregationType::Range,
            AggregationType::Rate,
            AggregationType::StdP,
            AggregationType::StdS,
            AggregationType::Sum,
            AggregationType::VarP,
            AggregationType::VarS,
        ];
        let alignments = [
            BucketAlignment::Default,
            BucketAlignment::Start,
            BucketAlignment::End,
            BucketAlignment::Timestamp(37),
        ];
        let windows = [(0, 12_000), (50, 400), (1000, 5000)];
        let value_filters = [None, Some(ValueFilter { min: 2.0, max: 9.0 })];
        let latest_samples = [None, Some(Sample::new(11_500, 4.0))];

        let same = |a: &[(i64, f64)], b: &[(i64, f64)]| {
            a.len() == b.len()
                && a.iter()
                    .zip(b)
                    .all(|(x, y)| x.0 == y.0 && (x.1 == y.1 || (x.1.is_nan() && y.1.is_nan())))
        };

        for samples in tail_read_fixtures() {
            let series = series_with(&samples);
            for aggregator in aggregators {
                for (bucket_duration, alignment) in [100u64, 1000, 30]
                    .into_iter()
                    .flat_map(|duration| alignments.map(|alignment| (duration, alignment)))
                {
                    for (start, end) in windows {
                        for value_filter in value_filters {
                            for (latest, report_empty) in latest_samples
                                .into_iter()
                                .flat_map(|latest| [false, true].map(|empty| (latest, empty)))
                            {
                                let mut options = RangeOptions {
                                    date_range: TimestampRange::from_timestamps(start, end)
                                        .unwrap(),
                                    value_filter,
                                    aggregation: Some(AggregationOptions {
                                        aggregations: smallvec::smallvec![config(aggregator)],
                                        bucket_duration,
                                        alignment,
                                        timestamp_output: BucketTimestamp::End,
                                        report_empty,
                                    }),
                                    ..Default::default()
                                };
                                let full: Vec<(i64, f64)> =
                                    create_range_iterator(&series, &options, latest, true)
                                        .map(|s| (s.timestamp, s.value))
                                        .collect();
                                for count in [1usize, 3, 1000] {
                                    options.count = Some(count);
                                    let limited: Vec<(i64, f64)> =
                                        create_range_iterator(&series, &options, latest, true)
                                            .map(|s| (s.timestamp, s.value))
                                            .collect();
                                    let expected = &full[..count.min(full.len())];
                                    assert!(
                                        same(&limited, expected),
                                        "{aggregator:?} bucket {bucket_duration} {alignment:?} \
                                         {start}..{end} filter {value_filter:?} latest {latest:?} \
                                         count {count}: {limited:?} != {expected:?}"
                                    );

                                    let rows: Vec<(i64, f64)> =
                                        create_row_iterator(&series, &options, latest, true)
                                            .map(|r| (r.timestamp, r.values[0]))
                                            .collect();
                                    assert!(same(&rows, expected), "row pipeline, {aggregator:?}");
                                }
                                options.count = None;
                            }
                        }
                    }
                }
            }
        }
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
            create_range_iterator(&series, options, Some(latest), is_reverse)
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
            create_range_iterator(&series, &options, Some(Sample::new(1500, 5.0)), true)
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
        create_range_iterator(&series, &options, None, is_reverse)
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
            create_range_iterator(&series, &options, None, false)
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
