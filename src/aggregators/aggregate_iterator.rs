use crate::aggregators::{AggregationHandler, AggregationType, Aggregator, BucketTimestamp};
use crate::common::{MultiSample, Sample, Timestamp};
use crate::series::request_types::AggregationOptions;
use smallvec::SmallVec;
use std::collections::VecDeque;

/// A pending run of empty (gap-fill) buckets, generated one at a time instead of being
/// materialized up front. `[start, bucket_duration wide) * ceil((end_exclusive - start) /
/// bucket_duration)` can span the entire query window when the series has a sparse or
/// adversarial gap (e.g. one sample at each end of an i64 timestamp range), so eagerly
/// allocating a `Vec`/`VecDeque` sized to that count -- or even just looping to push every
/// row -- does unbounded work up front regardless of how many rows the caller actually
/// consumes. `next_row` instead advances the cursor by one bucket per call, so a downstream
/// `.take(COUNT)` (see `iterators::utils`) bounds the real work to what is actually read.
///
/// Invariant relied on by `AggregationHelper`/`MultiAggregateIterator`: at most one run is
/// ever pending at a time -- each is fully drained (via `next_row`) before the next is
/// queued (via `queue`). See the call sites in this file for why that holds.
#[derive(Debug, Default, Clone, Copy)]
struct EmptyBucketRun {
    next_bucket: Timestamp,
    end_exclusive: Timestamp,
}

impl EmptyBucketRun {
    fn is_empty(&self) -> bool {
        self.next_bucket >= self.end_exclusive
    }

    /// Replace the run with `[start, end_exclusive)`. The caller must have drained any
    /// previously queued run first -- see the type-level invariant above.
    fn queue(&mut self, start: Timestamp, end_exclusive: Timestamp) {
        debug_assert!(
            self.is_empty(),
            "queuing an empty-bucket run over one that has not been drained"
        );
        self.next_bucket = start;
        self.end_exclusive = end_exclusive;
    }

    /// Jump ahead so that at most `keep` buckets remain, without generating the ones skipped.
    fn skip_to_last(&mut self, keep: usize, bucket_duration: u64) {
        let duration = bucket_duration as i128;
        let span = self.end_exclusive as i128 - self.next_bucket as i128;
        let remaining = (span + duration - 1) / duration;
        let surplus = remaining - keep as i128;
        if surplus > 0 {
            self.next_bucket = (self.next_bucket as i128 + surplus * duration) as Timestamp;
        }
    }
}

/// EMPTY's fill for `last`: unlike every other aggregator's it is not a constant, because a gap
/// means "no new reading, so the last reading still stands". It runs here rather than after the
/// stream is built because it treats a gap and a bucket holding only NaN differently, and both
/// look alike once emitted.
///
/// A stored NaN is not a reading. A leading run of gaps holds the last reading before its first
/// bucket ([`EmptyFillBounds::carry_seed`]). After that, RedisTimeSeries' answer depends on the
/// direction it scans in, though buckets are always produced chronologically here:
///
/// - **Forward:** a bucket holding only NaN reports the value held so far, or NaN if there is
///   none. A gap reports the same, except that with nothing held it looks back to the last reading
///   before the window ([`EmptyFillBounds::carry_lookback`]).
/// - **Reverse:** a gap reports the last reading before it in time, looking back past the window
///   if need be, and a leading run does not affect that. A bucket holding only NaN reports what the
///   scan held when it got there: the value of the next bucket in time, so it is left unresolved
///   for [`MultiAggregateIterator`] to fill once that bucket exists.
#[derive(Debug)]
struct LastCarry {
    /// Which output columns are `last`. They all see the same samples, so one held value serves.
    columns: SmallVec<[bool; 2]>,
    held: f64,
    seed: f64,
    lookback: f64,
    reverse: bool,
    /// A bucket holding samples has been reported, so a gap is no longer part of the leading run.
    past_leading_run: bool,
    /// The most recent bucket holding samples still needs a value from the next one.
    unresolved: bool,
}

impl LastCarry {
    fn new(options: &AggregationOptions) -> Option<Self> {
        if !options.report_empty {
            return None;
        }
        let columns: SmallVec<[bool; 2]> = options
            .aggregations
            .iter()
            .map(|a| matches!(a.aggregation_type(), AggregationType::Last))
            .collect();
        columns.iter().any(|&column| column).then_some(Self {
            columns,
            held: f64::NAN,
            seed: f64::NAN,
            lookback: f64::NAN,
            reverse: false,
            past_leading_run: false,
            unresolved: false,
        })
    }

    fn start_leading_run(&mut self, seed: f64) {
        self.seed = seed;
        if !self.reverse {
            self.held = seed;
        }
    }

    fn apply_to_bucket_with_samples(&mut self, values: &mut [f64]) {
        self.past_leading_run = true;
        let value = self.value_of(values);
        if value.is_nan() {
            if self.reverse {
                self.unresolved = true;
            } else {
                self.fill(values, self.held);
            }
        } else {
            self.held = value;
        }
    }

    fn apply_to_gap(&mut self, values: &mut [f64]) {
        let value = if !self.past_leading_run {
            if self.reverse { self.seed } else { self.held }
        } else if self.held.is_nan() {
            if !self.reverse {
                self.held = self.lookback;
            }
            self.lookback
        } else {
            self.held
        };
        self.fill(values, value);
    }

    fn value_of(&self, values: &[f64]) -> f64 {
        self.columns
            .iter()
            .position(|&is_last| is_last)
            .map_or(f64::NAN, |column| values[column])
    }

    fn fill(&self, values: &mut [f64], value: f64) {
        for (slot, _) in values
            .iter_mut()
            .zip(self.columns.iter())
            .filter(|(_, is_last)| **is_last)
        {
            *slot = value;
        }
    }
}

/// Helper class for minimizing monomorphization overhead for AggregationIterator.
/// Holds one aggregator per AGGREGATION list entry; all share the bucket bookkeeping,
/// so a query performs a single scan with N accumulator updates per sample.
#[derive(Debug)]
struct AggregationHelper {
    aggregators: SmallVec<[Aggregator; 2]>,
    /// Per-aggregator: did it accept at least one sample from the current bucket? Parallel
    /// to `aggregators`. Drives whether the bucket is emitted — see `complete_bucket`.
    accepted: SmallVec<[bool; 2]>,
    bucket_duration: u64,
    bucket_ts: BucketTimestamp,
    bucket_range_start: Timestamp,
    /// Last timestamp of the bucket, inclusive: the final representable bucket has no
    /// exclusive end, and a saturated one would push a sample at `i64::MAX` out of it.
    bucket_range_last: Timestamp,
    align_timestamp: Timestamp,
    /// Whether the current bucket received any sample at all, NaN or not. A bucket every
    /// aggregator ignored still counts as started, and the trailing one must not be dropped
    /// by `finalize_last_bucket_if_any`.
    saw_sample: bool,
    report_empty: bool,
    last_carry: Option<LastCarry>,
}

impl AggregationHelper {
    pub(crate) fn new(options: &AggregationOptions, align_timestamp: Timestamp) -> Self {
        let mut aggregators = options.create_aggregators();
        for aggregator in aggregators.iter_mut() {
            if let Aggregator::Rate(r) = aggregator {
                r.set_window_ms(options.bucket_duration);
            }
        }

        Self {
            accepted: smallvec::smallvec![false; aggregators.len()],
            aggregators,
            bucket_duration: options.bucket_duration,
            bucket_ts: options.timestamp_output,
            bucket_range_start: 0,
            bucket_range_last: 0,
            align_timestamp,
            saw_sample: false,
            report_empty: options.report_empty,
            last_carry: LastCarry::new(options),
        }
    }

    fn empty_row(&mut self, timestamp: Timestamp) -> MultiSample {
        let mut row = MultiSample {
            timestamp,
            values: self
                .aggregators
                .iter()
                .map(AggregationHandler::empty_value)
                .collect(),
        };
        if let Some(carry) = self.last_carry.as_mut() {
            carry.apply_to_gap(&mut row.values);
        }
        row
    }

    fn add_empty_bucket_internal(
        &self,
        run: &mut EmptyBucketRun,
        start_bucket: Timestamp,
        end_bucket_exclusive: Timestamp,
    ) {
        if end_bucket_exclusive <= start_bucket {
            return;
        }

        run.queue(start_bucket, end_bucket_exclusive);
    }

    fn add_empty_buckets_between_timestamps(
        &self,
        run: &mut EmptyBucketRun,
        first_ts: Timestamp,
        end_ts: Timestamp,
    ) {
        let start = self.calc_bucket_start(first_ts);
        let end = self.calc_bucket_start(end_ts);
        self.add_empty_bucket_internal(run, start, end);
    }

    fn output_timestamp(&self) -> Timestamp {
        self.render_timestamp(self.bucket_range_start)
    }

    fn complete_bucket(
        &mut self,
        last_ts: Option<Timestamp>,
        empty_buckets: &mut EmptyBucketRun,
    ) -> Option<MultiSample> {
        // Emission is a property of the aggregator, not of the bucket: RTS returns a bucket
        // iff *this* aggregation took something from it. So a bucket holding only NaNs is
        // returned for COUNTALL/COUNTNAN, which count NaNs, and omitted for the NaN-ignoring
        // ones; conversely COUNTNAN over a bucket of ordinary readings is omitted. Both
        // directions reference-checked. Under EMPTY the bucket is emitted either way, but an
        // aggregator that did take samples reports its real value rather than the fill — an
        // all-NaN bucket gives COUNTALL its count, not 0.
        //
        // The test is "accepted a sample" (`update`'s return), not "has a value": an
        // aggregator can accept input and still have nothing to report — IRATE over a
        // counter reset, SUMIF whose condition matched nothing — and those buckets are real,
        // reporting NaN and 0 respectively. Only a bucket an aggregator took nothing from is
        // absent for it.
        //
        // With several aggregations (an extension; RTS permits one) a row is all-or-nothing,
        // so it is emitted once any aggregator accepted a sample and the rest take their fill.
        let any_accepted = self.accepted.iter().any(|&accepted| accepted);

        // `finalize` resets each aggregator it reads; a bucket nobody accepted from is reset
        // here instead, so every path leaves the aggregators ready for the next bucket.
        let mut bucket = if any_accepted {
            Some(MultiSample::new(
                self.output_timestamp(),
                self.aggregators
                    .iter_mut()
                    .map(AggregationHandler::finalize)
                    .collect(),
            ))
        } else {
            for aggregator in self.aggregators.iter_mut() {
                AggregationHandler::reset(aggregator);
            }
            // Not `empty_row`: that applies the gap carry, and this bucket did see samples.
            self.report_empty.then(|| {
                MultiSample::new(
                    self.output_timestamp(),
                    self.aggregators
                        .iter()
                        .map(AggregationHandler::empty_value)
                        .collect(),
                )
            })
        };
        if let (Some(row), Some(carry)) = (bucket.as_mut(), self.last_carry.as_mut()) {
            carry.apply_to_bucket_with_samples(&mut row.values);
        }
        self.accepted.fill(false);

        if self.report_empty
            && let Some(last_ts) = last_ts
            && let Some(next_bucket) = self.next_bucket_start()
        {
            self.add_empty_buckets_between_timestamps(empty_buckets, next_bucket, last_ts);
        }

        self.saw_sample = false;
        bucket
    }

    /// Give each counter aggregator `seed` as the sample before its first bucket, without the
    /// bucket counting it: the state a stream that started earlier would have carried in.
    fn seed_baseline(&mut self, seed: Sample) {
        for aggregator in self.aggregators.iter_mut() {
            if aggregator.carries_baseline() {
                AggregationHandler::update(aggregator, seed.timestamp, seed.value);
                AggregationHandler::reset(aggregator);
            }
        }
    }

    fn update(&mut self, sample: Sample) {
        for (aggregator, accepted) in self.aggregators.iter_mut().zip(self.accepted.iter_mut()) {
            *accepted |= aggregator.update(sample.timestamp, sample.value);
        }
        self.saw_sample = true;
    }

    #[inline]
    fn should_finalize_bucket(&self, timestamp: Timestamp) -> bool {
        timestamp > self.bucket_range_last
    }

    /// Start of the bucket after the current one; `None` when the current one is the last.
    fn next_bucket_start(&self) -> Option<Timestamp> {
        self.bucket_range_last.checked_add(1)
    }

    fn update_bucket_timestamps(&mut self, timestamp: Timestamp) {
        self.bucket_range_start = self.calc_bucket_start(timestamp);
        self.bucket_range_last = self
            .bucket_range_start
            .saturating_add_unsigned(self.bucket_duration - 1);
    }

    /// True (unclamped) bucket start: may be negative when the alignment offset
    /// places the first bucket before 0. Bucket membership must use this value;
    /// only the *reported* timestamp is clamped (see [`Self::render_timestamp`]),
    /// matching RTS `CalcBucketStart`/`BucketStartNormalize`.
    fn calc_bucket_start(&self, ts: Timestamp) -> Timestamp {
        bucket_start_for(ts, self.align_timestamp, self.bucket_duration)
    }

    /// Reply timestamp for a bucket: clamp the start to 0 first, then apply the
    /// BUCKETTIMESTAMP adjustment (RTS normalizes before, not after: the mid of
    /// a `[-750, 250)` bucket reports 500, not 0).
    fn render_timestamp(&self, bucket_start: Timestamp) -> Timestamp {
        self.bucket_ts
            .calculate(bucket_start.max(0), self.bucket_duration)
    }
}

/// True (unclamped) start of the bucket holding `ts`, for a grid aligned on
/// `align_timestamp`. Negative when the alignment offset places the bucket before 0; only the
/// *reported* timestamp is clamped. Shared with the callers that have to reason about the
/// grid without an iterator in hand (see `iterators::empty_fill_bounds`).
///
/// Unsigned so that the whole accepted domain is safe: a signed `(diff % d + d) % d` overflows
/// once `d` nears `i64::MAX`, which is a valid duration. `bucket_duration` must be nonzero.
pub(crate) fn bucket_start_for(
    ts: Timestamp,
    align_timestamp: Timestamp,
    bucket_duration: u64,
) -> Timestamp {
    let distance = ts.abs_diff(align_timestamp) % bucket_duration;
    let offset = if ts >= align_timestamp || distance == 0 {
        distance
    } else {
        bucket_duration - distance
    };
    ts.saturating_sub_unsigned(offset)
}

pub fn aggregate(
    options: &AggregationOptions,
    aligned_timestamp: Timestamp,
    iter: impl Iterator<Item = Sample>,
) -> Vec<Sample> {
    let iterator = AggregateIterator::new(iter, options, aligned_timestamp);
    iterator.collect()
}

/// How far an `EMPTY` fill may run past the samples the query itself sees.
///
/// A bucket is reported iff it falls in the intersection of the query window with the extent
/// of the samples passing the query's filters — and that extent is taken over the *whole*
/// series, not the queried slice of it. So a bucket holding nothing is still reported when
/// passing data exists on both sides of it, while the window's own edge is only reached when
/// the series really has passing data beyond it. A stored NaN is not passing data for this,
/// whatever the aggregator. All of this is RedisTimeSeries 8.10 behavior, confirmed by
/// black-box probing of the reference.
///
/// The sample stream is clipped to the window and so cannot answer "is there anything out
/// there?"; these fields carry exactly that outside knowledge. `start` is
/// `Some(query_start)` when a passing sample exists before the window, `end` is
/// `Some(query_end)` when one exists after it. A `None` side means the fill stops at the
/// first (or last) sample the query actually saw, which is also the default — an iterator
/// built without bounds fills interior gaps only.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EmptyFillBounds {
    pub start: Option<Timestamp>,
    pub end: Option<Timestamp>,
    /// Value of the last passing, non-NaN sample before the first bucket, or NaN when there is
    /// none. A leading run of gap buckets holds it under `last`, inheriting a sample the query
    /// itself never reads. Carried here rather than looked up downstream because it comes from
    /// the same scan as `start`.
    pub carry_seed: f64,
    /// Value of the last passing, non-NaN sample before the window, or NaN when there is none.
    /// What a later gap inherits under `last` when every sample the query has read was NaN. It
    /// differs from `carry_seed` only when the window opens mid-bucket.
    pub carry_lookback: f64,
    /// `last` fills differently for a query that runs newest-first; see [`LastCarry`].
    pub carry_reverse: bool,
    /// Only the last this-many rows of the stream will be used, which is what a newest-first
    /// COUNT does with it. A run of gaps can then skip all but its own last rows: every gap in a
    /// run reports the same value, and `last`'s carry only ever looks at the run's first and
    /// last, so nothing the skipped ones would have done is lost.
    pub tail_limit: Option<usize>,
}

impl Default for EmptyFillBounds {
    fn default() -> Self {
        Self {
            start: None,
            end: None,
            carry_seed: f64::NAN,
            carry_lookback: f64::NAN,
            carry_reverse: false,
            tail_limit: None,
        }
    }
}

impl EmptyFillBounds {
    pub fn new(start: Option<Timestamp>, end: Option<Timestamp>) -> Self {
        Self {
            start,
            end,
            ..Default::default()
        }
    }
}

/// Bucketing iterator over N >= 1 aggregators; yields one row per bucket with
/// `values[i]` produced by `options.aggregations[i]`.
pub struct MultiAggregateIterator<T: Iterator<Item = Sample>> {
    inner: T,
    aggregator: AggregationHelper,
    empty_buckets: EmptyBucketRun,
    init: bool,
    empty_fill: EmptyFillBounds,
    /// Reverse `last` only: rows waiting on the next bucket's value, and how many at the front
    /// of the queue have it.
    pending: VecDeque<MultiSample>,
    ready: usize,
}

impl<T: Iterator<Item = Sample>> MultiAggregateIterator<T> {
    pub fn new(inner: T, options: &AggregationOptions, aligned_timestamp: Timestamp) -> Self {
        Self::with_empty_fill(
            inner,
            options,
            aligned_timestamp,
            EmptyFillBounds::default(),
        )
    }

    pub fn with_empty_fill(
        inner: T,
        options: &AggregationOptions,
        aligned_timestamp: Timestamp,
        empty_fill: EmptyFillBounds,
    ) -> Self {
        let mut aggregator = AggregationHelper::new(options, aligned_timestamp);
        if let Some(carry) = aggregator.last_carry.as_mut() {
            carry.lookback = empty_fill.carry_lookback;
            carry.reverse = empty_fill.carry_reverse;
        }
        Self {
            inner,
            aggregator,
            empty_buckets: EmptyBucketRun::default(),
            init: false,
            empty_fill,
            pending: VecDeque::new(),
            ready: 0,
        }
    }

    /// Start the counter aggregators from `baseline`, the last sample before this stream that
    /// a read from further back would have fed them. Must be earlier than every sample the
    /// stream yields.
    pub fn with_baseline(mut self, baseline: Option<Sample>) -> Self {
        if let Some(seed) = baseline {
            self.aggregator.seed_baseline(seed);
        }
        self
    }

    #[inline]
    fn update(&mut self, sample: Sample) {
        self.aggregator.update(sample);
    }

    #[inline]
    fn start_new_bucket(&mut self, sample: Sample) {
        self.aggregator.update_bucket_timestamps(sample.timestamp);
        self.update(sample);
    }

    #[inline]
    fn finalize_bucket(&mut self, ts: Option<Timestamp>) -> Option<MultiSample> {
        self.aggregator.complete_bucket(ts, &mut self.empty_buckets)
    }

    #[inline]
    fn pop_empty_bucket(&mut self) -> Option<MultiSample> {
        if self.empty_buckets.is_empty() {
            return None;
        }

        if let Some(keep) = self.empty_fill.tail_limit {
            self.empty_buckets
                .skip_to_last(keep, self.aggregator.bucket_duration);
        }

        let bucket_start = self.empty_buckets.next_bucket;
        self.empty_buckets.next_bucket =
            bucket_start.saturating_add_unsigned(self.aggregator.bucket_duration);
        let timestamp = self.aggregator.render_timestamp(bucket_start);
        Some(self.aggregator.empty_row(timestamp))
    }

    /// One past the last bucket of `ts`, for the exclusive end `add_empty_bucket_internal` takes.
    /// Saturating cannot lose a bucket: only a bucket starting at `i64::MAX` would need the end
    /// to be larger, and a fill bound implies passing data beyond `ts`, so `ts < i64::MAX`.
    fn bucket_end_exclusive(&self, ts: Timestamp) -> Timestamp {
        self.aggregator
            .calc_bucket_start(ts)
            .saturating_add_unsigned(self.aggregator.bucket_duration)
    }

    /// Buckets between the window start and the first sample the query saw. Only reached when
    /// passing data exists before the window, which is what makes those buckets reportable —
    /// otherwise they would end before the series' earliest passing sample.
    fn enqueue_leading_empty_buckets(&mut self, first_sample_ts: Timestamp) {
        if !self.aggregator.report_empty {
            return;
        }

        if let Some(query_start) = self.empty_fill.start {
            let requested_first_bucket = self.aggregator.calc_bucket_start(query_start);
            let first_sample_bucket = self.aggregator.calc_bucket_start(first_sample_ts);
            self.aggregator.add_empty_bucket_internal(
                &mut self.empty_buckets,
                requested_first_bucket,
                first_sample_bucket,
            );
            self.seed_leading_run();
        }
    }

    /// The run just queued precedes every sample the query will read, so it holds what came
    /// before it.
    fn seed_leading_run(&mut self) {
        if let Some(carry) = self.aggregator.last_carry.as_mut()
            && !self.empty_buckets.is_empty()
        {
            carry.start_leading_run(self.empty_fill.carry_seed);
        }
    }

    /// The window holds no sample of its own. It is still reported — as one run of empty
    /// buckets — when passing data exists on *both* sides, i.e. the window is a gap inside
    /// the data rather than past its edge.
    fn enqueue_full_empty_range_if_needed(&mut self) {
        if !self.aggregator.report_empty {
            return;
        }

        if let (Some(query_start), Some(query_end)) = (self.empty_fill.start, self.empty_fill.end) {
            let first_bucket = self.aggregator.calc_bucket_start(query_start);
            let end_exclusive = self.bucket_end_exclusive(query_end);
            self.aggregator.add_empty_bucket_internal(
                &mut self.empty_buckets,
                first_bucket,
                end_exclusive,
            );
            self.seed_leading_run();
        }
    }

    #[inline]
    fn ensure_initialized(&mut self) -> bool {
        if self.init {
            return true;
        }

        self.init = true;
        if let Some(sample) = self.inner.next() {
            self.enqueue_leading_empty_buckets(sample.timestamp);
            self.start_new_bucket(sample);
            true
        } else {
            self.enqueue_full_empty_range_if_needed();
            false
        }
    }

    fn process_bucket(&mut self) -> Option<MultiSample> {
        while let Some(sample) = self.inner.next() {
            if !self.aggregator.should_finalize_bucket(sample.timestamp) {
                self.update(sample);
                continue;
            }

            let bucket = self.finalize_bucket(Some(sample.timestamp));
            self.start_new_bucket(sample);

            if bucket.is_some() {
                return bucket;
            }
            // No aggregator accepted a sample from that bucket, so it was skipped; continue
            // processing the next samples in the newly started bucket.
        }

        None
    }

    /// Buckets between the last sample the query saw and the window end. Like the leading
    /// side, only reached when passing data exists beyond the window — otherwise those
    /// buckets would begin after the series' latest passing sample.
    fn finalize_last_bucket_if_any(&mut self) -> Option<MultiSample> {
        if !self.aggregator.saw_sample {
            return None;
        }

        let bucket = self.finalize_bucket(None);

        if self.aggregator.report_empty
            && let Some(query_end) = self.empty_fill.end
            && let Some(next_bucket) = self.aggregator.next_bucket_start()
        {
            let end_exclusive = self.bucket_end_exclusive(query_end);
            self.aggregator.add_empty_bucket_internal(
                &mut self.empty_buckets,
                next_bucket,
                end_exclusive,
            );
        }

        bucket
    }
}

impl<T: Iterator<Item = Sample>> MultiAggregateIterator<T> {
    /// Hold back buckets a reverse `last` cannot answer yet until the next bucket can.
    fn next_resolved_row(&mut self) -> Option<MultiSample> {
        while self.ready == 0 {
            let Some(row) = self.next_row() else {
                // No later bucket exists, so a scan starting here held nothing: they stay NaN.
                self.ready = self.pending.len();
                break;
            };
            let carry = self.aggregator.last_carry.as_mut()?;
            if std::mem::take(&mut carry.unresolved) {
                self.pending.push_back(row);
                // Every row queued here is unresolved (`ready` is 0), and one with `keep` newer
                // rows behind it cannot reach the stream's last `keep`, so a long NaN run holds
                // no more than that rather than all of itself.
                if let Some(keep) = self.empty_fill.tail_limit
                    && self.pending.len() > keep
                {
                    self.pending.pop_front();
                }
                continue;
            }
            let value = carry.value_of(&row.values);
            for waiting in self.pending.iter_mut() {
                carry.fill(&mut waiting.values, value);
            }
            self.pending.push_back(row);
            self.ready = self.pending.len();
        }

        let row = self.pending.pop_front()?;
        self.ready -= 1;
        Some(row)
    }

    fn next_row(&mut self) -> Option<MultiSample> {
        if let Some(row) = self.pop_empty_bucket() {
            return Some(row);
        }

        if !self.ensure_initialized() {
            return self.pop_empty_bucket();
        }

        // Initialization may have queued the buckets that precede the first sample; they
        // come before it in time, so they must come before it in the output too.
        if let Some(row) = self.pop_empty_bucket() {
            return Some(row);
        }

        if let Some(bucket) = self.process_bucket() {
            return Some(bucket);
        }

        self.finalize_last_bucket_if_any()
            .or_else(|| self.pop_empty_bucket())
    }
}

impl<T: Iterator<Item = Sample>> Iterator for MultiAggregateIterator<T> {
    type Item = MultiSample;

    fn next(&mut self) -> Option<Self::Item> {
        if self.empty_fill.carry_reverse && self.aggregator.last_carry.is_some() {
            self.next_resolved_row()
        } else {
            self.next_row()
        }
    }
}

/// Single-aggregator bucketing iterator: a thin wrapper over
/// [`MultiAggregateIterator`] that unwraps the one-column rows into samples.
pub struct AggregateIterator<T: Iterator<Item = Sample>> {
    inner: MultiAggregateIterator<T>,
}

impl<T: Iterator<Item = Sample>> AggregateIterator<T> {
    pub fn new(inner: T, options: &AggregationOptions, aligned_timestamp: Timestamp) -> Self {
        debug_assert!(
            !options.is_multi(),
            "AggregateIterator requires a single aggregator; use MultiAggregateIterator"
        );
        Self {
            inner: MultiAggregateIterator::new(inner, options, aligned_timestamp),
        }
    }

    pub fn with_empty_fill(
        inner: T,
        options: &AggregationOptions,
        aligned_timestamp: Timestamp,
        empty_fill: EmptyFillBounds,
    ) -> Self {
        debug_assert!(
            !options.is_multi(),
            "AggregateIterator requires a single aggregator; use MultiAggregateIterator"
        );
        Self {
            inner: MultiAggregateIterator::with_empty_fill(
                inner,
                options,
                aligned_timestamp,
                empty_fill,
            ),
        }
    }
}

impl<T: Iterator<Item = Sample>> AggregateIterator<T> {
    /// See [`MultiAggregateIterator::with_baseline`].
    pub fn with_baseline(self, baseline: Option<Sample>) -> Self {
        Self {
            inner: self.inner.with_baseline(baseline),
        }
    }
}

impl<T: Iterator<Item = Sample>> Iterator for AggregateIterator<T> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        let row = self.inner.next()?;
        debug_assert_eq!(row.values.len(), 1);
        Some(Sample::new(row.timestamp, row.values[0]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregators::{AggregationType, BucketAlignment, BucketTimestamp};
    use crate::common::Sample;

    fn create_test_samples() -> Vec<Sample> {
        vec![
            Sample::new(10, 1.0),
            Sample::new(15, 2.0),
            Sample::new(20, 3.0),
            Sample::new(30, 4.0),
            Sample::new(40, 5.0),
            Sample::new(50, 6.0),
            Sample::new(60, 7.0),
        ]
    }

    fn create_options(aggregator: AggregationType) -> AggregationOptions {
        AggregationOptions {
            aggregations: smallvec::smallvec![aggregator.into()],
            bucket_duration: 10,
            timestamp_output: BucketTimestamp::Start,
            alignment: BucketAlignment::Start,
            report_empty: false,
        }
    }

    #[test]
    fn test_sum_aggregation() {
        let samples = create_test_samples();
        let options = create_options(AggregationType::Sum);

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);

        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 6);
        assert_eq!(result[0].timestamp, 10);
        assert_eq!(result[0].value, 3.0); // 1.0 + 2.0
        assert_eq!(result[1].timestamp, 20);
        assert_eq!(result[1].value, 3.0);
        assert_eq!(result[2].timestamp, 30);
        assert_eq!(result[2].value, 4.0);
        assert_eq!(result[3].timestamp, 40);
        assert_eq!(result[3].value, 5.0);
        assert_eq!(result[4].timestamp, 50);
        assert_eq!(result[4].value, 6.0);
        assert_eq!(result[5].timestamp, 60);
        assert_eq!(result[5].value, 7.0);
    }

    fn sum_options(bucket_duration: u64, report_empty: bool) -> AggregationOptions {
        AggregationOptions {
            bucket_duration,
            report_empty,
            ..create_options(AggregationType::Sum)
        }
    }

    fn sum_rows(
        samples: &[(Timestamp, f64)],
        bucket_duration: u64,
        report_empty: bool,
    ) -> Vec<(Timestamp, f64)> {
        let options = sum_options(bucket_duration, report_empty);
        let samples = samples.iter().map(|&(ts, value)| Sample::new(ts, value));
        AggregateIterator::new(samples, &options, 0)
            .map(|sample| (sample.timestamp, sample.value))
            .collect()
    }

    #[test]
    fn test_empty_at_duration_one_reports_every_missing_bucket() {
        assert_eq!(
            sum_rows(&[(0, 1.0), (3, 2.0)], 1, true),
            [(0, 1.0), (1, 0.0), (2, 0.0), (3, 2.0)]
        );
        assert_eq!(
            sum_rows(&[(0, 1.0), (2, 2.0)], 1, true),
            [(0, 1.0), (1, 0.0), (2, 2.0)]
        );
    }

    #[test]
    fn test_empty_at_duration_one_multi_output() {
        let options = AggregationOptions {
            aggregations: smallvec::smallvec![
                AggregationType::Sum.into(),
                AggregationType::Max.into()
            ],
            ..sum_options(1, true)
        };
        let samples = [Sample::new(0, 1.0), Sample::new(2, 2.0)];
        let timestamps: Vec<Timestamp> =
            MultiAggregateIterator::new(samples.into_iter(), &options, 0)
                .map(|row| row.timestamp)
                .collect();
        assert_eq!(timestamps, [0, 1, 2]);
    }

    const TERMINAL_BUCKET: Timestamp = 9223372036854775800;

    #[test]
    fn test_final_representable_bucket_holds_i64_max() {
        let samples = [(Timestamp::MAX - 1, 1.0), (Timestamp::MAX, 2.0)];
        for report_empty in [false, true] {
            assert_eq!(
                sum_rows(&samples, 10, report_empty),
                [(TERMINAL_BUCKET, 3.0)]
            );
        }
    }

    #[test]
    fn test_final_bucket_at_duration_one_is_distinct_from_the_one_before() {
        let samples = [(Timestamp::MAX - 1, 1.0), (Timestamp::MAX, 2.0)];
        assert_eq!(
            sum_rows(&samples, 1, true),
            [(Timestamp::MAX - 1, 1.0), (Timestamp::MAX, 2.0)]
        );
    }

    #[test]
    fn test_empty_fills_up_to_the_final_bucket() {
        let samples = [(TERMINAL_BUCKET - 15, 1.0), (Timestamp::MAX, 2.0)];
        assert_eq!(
            sum_rows(&samples, 10, true),
            [
                (TERMINAL_BUCKET - 20, 1.0),
                (TERMINAL_BUCKET - 10, 0.0),
                (TERMINAL_BUCKET, 2.0)
            ]
        );
    }

    #[test]
    fn test_trailing_fill_past_the_final_bucket_stops() {
        let options = sum_options(10, true);
        let samples = [Sample::new(Timestamp::MAX, 2.0)];
        let bounds = EmptyFillBounds::new(None, Some(Timestamp::MAX));
        let rows: Vec<MultiSample> =
            MultiAggregateIterator::with_empty_fill(samples.into_iter(), &options, 0, bounds)
                .collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].timestamp, TERMINAL_BUCKET);
    }

    #[test]
    fn test_largest_accepted_duration_is_a_single_bucket() {
        let samples = [(Timestamp::MAX - 2, 1.0), (Timestamp::MAX - 1, 2.0)];
        assert_eq!(sum_rows(&samples, i64::MAX as u64, false), [(0, 3.0)]);
        assert_eq!(
            sum_rows(&[(1, 1.0), (2, 2.0)], i64::MAX as u64, false),
            [(0, 3.0)]
        );
    }

    #[test]
    fn test_bucket_start_matches_wide_euclidean_oracle() {
        let oracle = |ts: Timestamp, align: Timestamp, duration: u64| -> Timestamp {
            let (ts, align, duration) = (ts as i128, align as i128, duration as i128);
            (ts - (ts - align).rem_euclid(duration)) as Timestamp
        };
        let timestamps = [
            0,
            1,
            2,
            249,
            250,
            999,
            1000,
            1 << 61,
            Timestamp::MAX - 2,
            Timestamp::MAX - 1,
            Timestamp::MAX,
        ];
        let aligns = [0, 1, 250, 1 << 61, Timestamp::MAX - 1, Timestamp::MAX];
        let durations = [
            1,
            2,
            3,
            7,
            10,
            1000,
            1 << 61,
            (1 << 61) + 1,
            (i64::MAX as u64) - 1,
            i64::MAX as u64,
        ];
        for &ts in &timestamps {
            for &align in &aligns {
                for &duration in &durations {
                    assert_eq!(
                        bucket_start_for(ts, align, duration),
                        oracle(ts, align, duration),
                        "ts={ts} align={align} duration={duration}"
                    );
                }
            }
        }
    }

    // Reference-verified ALIGN semantics (RTS 8.6, compat finding #2): with
    // ALIGN 250 and bucketDuration 1000 the buckets are [-750,250), [250,1250),
    // [1250,2250), [2250,3250); membership uses the true (possibly negative)
    // bucket start, and only the reported timestamp clamps the start to 0.
    fn align_250_options(bucket_ts: BucketTimestamp, report_empty: bool) -> AggregationOptions {
        AggregationOptions {
            aggregations: smallvec::smallvec![AggregationType::Sum.into()],
            bucket_duration: 1000,
            timestamp_output: bucket_ts,
            alignment: BucketAlignment::Timestamp(250),
            report_empty,
        }
    }

    fn align_250_samples() -> Vec<Sample> {
        vec![
            Sample::new(0, 1.0),
            Sample::new(500, 2.0),
            Sample::new(1000, 3.0),
            Sample::new(1500, 4.0),
            Sample::new(2500, 10.5),
        ]
    }

    #[test]
    fn test_align_offset_bucket_membership_matches_reference() {
        let options = align_250_options(BucketTimestamp::Start, false);
        let result: Vec<Sample> =
            AggregateIterator::new(align_250_samples().into_iter(), &options, 250).collect();

        // ts 0 is alone in [-750,250) (reported clamped to 0); 500 and 1000
        // share [250,1250) — the old clamped-start logic wrongly grouped 0
        // with 500 and produced overlapping buckets.
        let expected = [(0, 1.0), (250, 5.0), (1250, 4.0), (2250, 10.5)];
        let actual: Vec<(Timestamp, f64)> = result.iter().map(|s| (s.timestamp, s.value)).collect();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_align_offset_bucket_timestamp_clamps_start_before_adjustment() {
        // RTS clamps the bucket start to 0 *before* applying BUCKETTIMESTAMP:
        // the mid of [-750,250) reports 0 + 500 = 500, not max(0, -250).
        let options = align_250_options(BucketTimestamp::Mid, false);
        let result: Vec<Sample> =
            AggregateIterator::new(align_250_samples().into_iter(), &options, 250).collect();
        let mids: Vec<Timestamp> = result.iter().map(|s| s.timestamp).collect();
        assert_eq!(mids, vec![500, 750, 1750, 2750]);

        let options = align_250_options(BucketTimestamp::End, false);
        let result: Vec<Sample> =
            AggregateIterator::new(align_250_samples().into_iter(), &options, 250).collect();
        let ends: Vec<Timestamp> = result.iter().map(|s| s.timestamp).collect();
        assert_eq!(ends, vec![1000, 1250, 2250, 3250]);
    }

    #[test]
    fn test_align_offset_empty_buckets_match_reference() {
        let options = align_250_options(BucketTimestamp::Start, true);
        let samples = vec![Sample::new(0, 1.0), Sample::new(2900, 5.0)];
        let result: Vec<Sample> = AggregateIterator::with_empty_fill(
            samples.into_iter(),
            &options,
            250,
            EmptyFillBounds::new(Some(0), Some(3000)),
        )
        .collect();

        let actual: Vec<(Timestamp, f64)> = result.iter().map(|s| (s.timestamp, s.value)).collect();
        assert_eq!(actual, [(0, 1.0), (250, 0.0), (1250, 0.0), (2250, 5.0)]);
    }

    /// The bounds only ever *extend* the fill past the samples in hand: with both sides set,
    /// the run reaches the window's own edges; with neither, it stops at the first and last
    /// sample. Same samples, same query — only the outside knowledge differs.
    #[test]
    fn test_empty_fill_bounds_extend_the_run() {
        let options = align_250_options(BucketTimestamp::Start, true);
        let samples = vec![Sample::new(1300, 1.0)];

        let timestamps = |bounds: EmptyFillBounds| -> Vec<Timestamp> {
            AggregateIterator::with_empty_fill(samples.clone().into_iter(), &options, 250, bounds)
                .map(|s| s.timestamp)
                .collect()
        };

        // Anchored to the single sample: one bucket, no fill in either direction.
        assert_eq!(timestamps(EmptyFillBounds::default()), vec![1250]);
        // Data known to exist before the window: fill back to the window start, whose bucket
        // begins at -750 under this alignment and so reports as 0.
        assert_eq!(
            timestamps(EmptyFillBounds::new(Some(0), None)),
            vec![0, 250, 1250]
        );
        // ...and after it: fill forward to the window end.
        assert_eq!(
            timestamps(EmptyFillBounds::new(None, Some(3000))),
            vec![1250, 2250]
        );
        assert_eq!(
            timestamps(EmptyFillBounds::new(Some(0), Some(3000))),
            vec![0, 250, 1250, 2250]
        );
    }

    /// A window holding no sample at all is reported only when data is known on *both* sides
    /// — the window is then a gap inside the data rather than past its edge.
    #[test]
    fn test_empty_fill_bounds_on_a_window_with_no_samples() {
        let options = align_250_options(BucketTimestamp::Start, true);

        let timestamps = |bounds: EmptyFillBounds| -> Vec<Timestamp> {
            AggregateIterator::with_empty_fill(std::iter::empty(), &options, 250, bounds)
                .map(|s| s.timestamp)
                .collect()
        };

        assert_eq!(
            timestamps(EmptyFillBounds::new(Some(1000), Some(3000))),
            vec![250, 1250, 2250]
        );
        assert!(timestamps(EmptyFillBounds::new(Some(1000), None)).is_empty());
        assert!(timestamps(EmptyFillBounds::new(None, Some(3000))).is_empty());
        assert!(timestamps(EmptyFillBounds::default()).is_empty());
    }

    #[test]
    fn test_sum_with_nans() {
        let samples = vec![
            Sample::new(10, 1.0),
            Sample::new(15, f64::NAN),
            Sample::new(20, 2.0),
        ];

        let options = create_options(AggregationType::Sum);

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);
        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].timestamp, 10);
        // NaN should be ignored, sum = 1.0
        assert_eq!(result[0].value, 1.0);
        assert_eq!(result[1].timestamp, 20);
        assert_eq!(result[1].value, 2.0);
    }

    #[test]
    fn test_sum_all_nans() {
        let samples = vec![
            Sample::new(10, f64::NAN),
            Sample::new(15, f64::NAN),
            Sample::new(20, f64::NAN),
        ];

        let options = create_options(AggregationType::Sum);

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);
        let result: Vec<Sample> = iterator.collect();

        // Every bucket holds only NaNs, so every bucket is empty and omitted. With EMPTY
        // they would come back as 0.0 -- see `test_sum_all_nans_report_empty`.
        assert!(result.is_empty());
    }

    #[test]
    fn test_sum_all_nans_report_empty() {
        let samples = vec![
            Sample::new(10, f64::NAN),
            Sample::new(15, f64::NAN),
            Sample::new(20, f64::NAN),
        ];

        let mut options = create_options(AggregationType::Sum);
        options.report_empty = true;

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);
        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].timestamp, 10);
        assert_eq!(result[0].value, 0.0);
        assert_eq!(result[1].timestamp, 20);
        assert_eq!(result[1].value, 0.0);
    }

    #[test]
    fn test_avg_aggregation() {
        let samples = create_test_samples();
        let options = create_options(AggregationType::Avg);

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);

        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 6);
        assert_eq!(result[0].timestamp, 10);
        assert_eq!(result[0].value, 1.5); // (1.0 + 2.0) / 2
    }

    #[test]
    fn test_max_aggregation() {
        let samples = create_test_samples();
        let options = create_options(AggregationType::Max);

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);

        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 6);
        assert_eq!(result[0].timestamp, 10);
        assert_eq!(result[0].value, 2.0); // max of 1.0 and 2.0
    }

    #[test]
    fn test_min_aggregation() {
        let samples = create_test_samples();
        let options = create_options(AggregationType::Min);

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);

        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 6);
        assert_eq!(result[0].timestamp, 10);
        assert_eq!(result[0].value, 1.0); // min of 1.0 and 2.0
    }

    #[test]
    fn test_count_aggregation() {
        let samples = create_test_samples();
        let options = create_options(AggregationType::Count);

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);

        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 6);
        assert_eq!(result[0].timestamp, 10);
        assert_eq!(result[0].value, 2.0); // count of values in the bucket
        assert_eq!(result[1].timestamp, 20);
        assert_eq!(result[1].value, 1.0);
    }

    #[test]
    fn test_count_with_nans() {
        let samples = vec![
            Sample::new(10, 1.0),
            Sample::new(15, f64::NAN),
            Sample::new(20, f64::NAN),
            Sample::new(25, 3.0),
            Sample::new(30, f64::NAN),
            Sample::new(35, f64::NAN),
        ];

        let options = create_options(AggregationType::Count);
        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);
        let result: Vec<Sample> = iterator.collect();

        // The trailing [30, 40) bucket holds only NaNs, so it is empty and omitted.
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].timestamp, 10);
        assert_eq!(result[0].value, 1.0); // only the non-NaN sample is counted
        assert_eq!(result[1].timestamp, 20);
        assert_eq!(result[1].value, 1.0); // NaN is ignored, valid sample still counts
    }

    #[test]
    fn test_count_all_nans() {
        let samples = vec![
            Sample::new(10, f64::NAN),
            Sample::new(15, f64::NAN),
            Sample::new(20, f64::NAN),
            Sample::new(25, f64::NAN),
        ];

        let options = create_options(AggregationType::Count);
        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);
        let result: Vec<Sample> = iterator.collect();

        // A bucket holding only NaNs is empty, so without EMPTY it is omitted rather than
        // reported as a zero count (reference-checked against RedisTimeSeries 8.6).
        assert!(result.is_empty());
    }

    /// F2: `EMPTY` gap fill must not eagerly materialize every bucket in the gap up front.
    /// A client-controlled gap this wide (`TS.ADD k 0 1`, `TS.ADD k i64::MAX 1`, then
    /// `AGGREGATION sum 1 EMPTY`) used to compute `count = gap / bucket_duration` and
    /// `VecDeque::reserve(count)` before generating a single row -- an allocation request
    /// far beyond what any caller (bounded by `.take(COUNT)` downstream) would ever consume.
    /// Taking a handful of rows here must return promptly with only the two real samples'
    /// worth of work plus the requested fill rows -- not billions of pre-reserved slots.
    #[test]
    fn test_empty_fill_over_huge_gap_is_lazy() {
        // ~9.2 billion buckets span this gap. Eagerly reserving/materializing that many
        // `MultiSample` rows up front is exactly the multi-gigabyte allocation / capacity
        // overflow F2 describes; a lazy cursor instead does only as much work as `.take`
        // below actually consumes.
        let bucket_duration = 1_000_000_000;
        let samples = vec![Sample::new(0, 1.0), Sample::new(i64::MAX, 2.0)];

        let options = AggregationOptions {
            aggregations: smallvec::smallvec![AggregationType::Sum.into()],
            bucket_duration,
            timestamp_output: BucketTimestamp::Start,
            alignment: BucketAlignment::Start,
            report_empty: true,
        };

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);
        let result: Vec<Sample> = iterator.take(5).collect();

        assert_eq!(result.len(), 5);
        assert_eq!(result[0], Sample::new(0, 1.0));
        for (i, sample) in result.iter().enumerate().skip(1) {
            assert_eq!(sample.timestamp, i as i64 * bucket_duration as i64);
            assert_eq!(sample.value, 0.0);
        }
    }

    #[test]
    fn test_empty_buckets_report_empty_true() {
        let samples = vec![
            Sample::new(10, 1.0),
            Sample::new(15, 2.0),
            // Gap at 20-30
            Sample::new(40, 5.0),
            Sample::new(50, 6.0),
        ];

        let mut options = create_options(AggregationType::Sum);
        options.report_empty = true;

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);

        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 5);
        assert_eq!(result[0].timestamp, 10);
        assert_eq!(result[0].value, 3.0);
        assert_eq!(result[1].timestamp, 20);
        assert_eq!(result[1].value, 0.0); // Empty bucket with value 0 for sum
        assert_eq!(result[2].timestamp, 30);
        assert_eq!(result[2].value, 0.0); // Empty bucket
        assert_eq!(result[3].timestamp, 40);
        assert_eq!(result[3].value, 5.0);
        assert_eq!(result[4].timestamp, 50);
        assert_eq!(result[4].value, 6.0);
    }

    #[test]
    fn test_empty_bucket_run_skips_to_its_last_buckets() {
        let run = |keep: usize| {
            let mut run = EmptyBucketRun {
                next_bucket: 0,
                end_exclusive: 100,
            };
            run.skip_to_last(keep, 10);
            run.next_bucket
        };

        assert_eq!(run(1), 90);
        assert_eq!(run(3), 70);
        assert_eq!(run(10), 0, "exactly what the run holds");
        assert_eq!(run(50), 0, "more than the run holds");
    }

    /// The buckets a run keeps start on the grid, whatever the grid's offset.
    #[test]
    fn test_empty_bucket_run_skip_stays_on_the_bucket_grid() {
        let mut run = EmptyBucketRun {
            next_bucket: 7,
            end_exclusive: 107,
        };
        run.skip_to_last(2, 10);
        assert_eq!(run.next_bucket, 87);
    }

    /// Timestamps at the far ends of the range cannot overflow the arithmetic.
    #[test]
    fn test_empty_bucket_run_skip_over_the_whole_timestamp_range() {
        let mut run = EmptyBucketRun {
            next_bucket: i64::MIN,
            end_exclusive: i64::MAX,
        };
        run.skip_to_last(1, 1);
        assert_eq!(run.next_bucket, i64::MAX - 1);
    }

    #[test]
    fn test_empty_buckets_last() {
        let samples = vec![
            Sample::new(10, 1.0),
            Sample::new(15, 99.0),
            // Gap at 20-30
            Sample::new(40, 5.0),
            Sample::new(50, 6.0),
        ];

        let mut options = create_options(AggregationType::Last);
        options.report_empty = true;

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);

        let result: Vec<Sample> = iterator.collect();

        let timestamps: Vec<_> = result.iter().map(|s| s.timestamp).collect();
        assert_eq!(timestamps, vec![10, 20, 30, 40, 50]);
        assert_eq!(result[0].value, 99.0);
        assert_eq!(result[1].value, 99.0, "a gap keeps the previous reading");
        assert_eq!(result[2].value, 99.0, "a gap keeps the previous reading");
        assert_eq!(result[3].value, 5.0);
        assert_eq!(result[4].value, 6.0);
    }

    /// Reverse `last` holds NaN-only buckets until a later bucket resolves them. Under a tail
    /// limit that queue must stay bounded by the limit, not by the length of the NaN run, and the
    /// rows that survive must match the unlimited stream's tail.
    #[test]
    fn test_reverse_last_pending_run_bounded_by_tail_limit() {
        let mut samples: Vec<Sample> = (0..1_000).map(|i| Sample::new(i * 10, f64::NAN)).collect();
        samples.push(Sample::new(10_000, 5.0));

        let mut options = create_options(AggregationType::Last);
        options.report_empty = true;
        let fill = |tail_limit| EmptyFillBounds {
            carry_reverse: true,
            tail_limit,
            ..Default::default()
        };

        let unlimited: Vec<MultiSample> = MultiAggregateIterator::with_empty_fill(
            samples.clone().into_iter(),
            &options,
            0,
            fill(None),
        )
        .collect();

        for keep in [1usize, 3] {
            let mut iter = MultiAggregateIterator::with_empty_fill(
                samples.clone().into_iter(),
                &options,
                0,
                fill(Some(keep)),
            );
            let first = iter.next().expect("a row");
            assert!(
                iter.pending.len() <= keep,
                "keep={keep}: {} rows still queued",
                iter.pending.len()
            );
            let limited: Vec<MultiSample> = std::iter::once(first).chain(iter).collect();

            let tail = |rows: &[MultiSample]| -> Vec<(Timestamp, f64)> {
                rows[rows.len() - keep..]
                    .iter()
                    .map(|row| (row.timestamp, row.values[0]))
                    .collect()
            };
            assert_eq!(tail(&limited), tail(&unlimited), "keep={keep}");
            assert_eq!(limited.last().map(|r| r.values[0]), Some(5.0));
            if keep > 1 {
                assert_eq!(limited[limited.len() - 2].values[0], 5.0, "NaN run filled");
            }
        }
    }

    #[test]
    fn test_bucket_timestamp_end() {
        let samples = create_test_samples();
        let mut options = create_options(AggregationType::Sum);
        options.timestamp_output = BucketTimestamp::End;

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);

        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 6);
        assert_eq!(result[0].timestamp, 20); // End of the first bucket
        assert_eq!(result[0].value, 3.0);
    }

    #[test]
    fn test_bucket_timestamp_mid() {
        let samples = create_test_samples();
        let mut options = create_options(AggregationType::Sum);
        options.timestamp_output = BucketTimestamp::Mid;

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);

        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 6);
        assert_eq!(result[0].timestamp, 15); // Mid of the first bucket
        assert_eq!(result[0].value, 3.0);
    }

    #[test]
    fn test_empty_input() {
        let samples: Vec<Sample> = vec![];
        let options = create_options(AggregationType::Sum);

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);

        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 0); // Last bucket with default value
        // assert!(result[0].value.is_nan() || result[0].value == 0.0);
    }

    #[test]
    fn test_range_aggregation_basic() {
        let samples = vec![
            Sample::new(10, 1.0),
            Sample::new(15, 5.0),
            Sample::new(20, 2.0),
            Sample::new(25, 8.0),
            Sample::new(30, 3.0),
            Sample::new(35, 7.0),
        ];

        let options = AggregationOptions {
            aggregations: smallvec::smallvec![AggregationType::Range.into()],
            bucket_duration: 10,
            timestamp_output: BucketTimestamp::Start,
            alignment: BucketAlignment::Start,
            report_empty: false,
        };

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);
        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 3);

        // The first bucket [10, 20): contains 1.0 and 5.0, range = 5.0 - 1.0 = 4.0
        assert_eq!(result[0].timestamp, 10);
        assert_eq!(result[0].value, 4.0);

        // The second bucket [20, 30): contains 2.0 and 8.0, range = 8.0 - 2.0 = 6.0
        assert_eq!(result[1].timestamp, 20);
        assert_eq!(result[1].value, 6.0);

        // The third bucket [30, 40): contains 3.0 and 7.0, range = 7.0 - 3.0 = 4.0
        assert_eq!(result[2].timestamp, 30);
        assert_eq!(result[2].value, 4.0);
    }

    /// Cornerstone multi-aggregation invariant: for every aggregation type and option
    /// combination, column i of the multi iterator over [a1, ..., aN] agrees with running
    /// the single-aggregator iterator with ai alone, at every bucket the single one emits.
    ///
    /// Row *counts* need not agree. Emission is per-aggregator — a bucket is returned iff
    /// that aggregation accepted a sample from it — so over NaN-bearing input a bucket can
    /// be real for COUNTALL and absent for AVG. A multi row is all-or-nothing, so the multi
    /// timestamps are the union of the single ones. Under EMPTY every bucket is emitted on
    /// both sides and the union is exact.
    #[test]
    fn test_multi_single_column_equivalence() {
        use crate::series::request_types::AggregatorConfig;
        use smallvec::SmallVec;

        let all_types = [
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
            AggregationType::Range,
            AggregationType::Rate,
            AggregationType::StdP,
            AggregationType::StdS,
            AggregationType::Sum,
            AggregationType::VarP,
            AggregationType::VarS,
        ];

        let inputs: Vec<Vec<Sample>> = vec![
            create_test_samples(),
            vec![
                Sample::new(10, 1.0),
                Sample::new(15, f64::NAN),
                Sample::new(20, 2.0),
                Sample::new(55, f64::NAN),
                Sample::new(90, 7.5),
            ],
            vec![],
        ];

        for report_empty in [false, true] {
            for timestamp_output in [
                BucketTimestamp::Start,
                BucketTimestamp::Mid,
                BucketTimestamp::End,
            ] {
                for input in &inputs {
                    // one multi clause containing every type at once
                    let aggregations: SmallVec<[AggregatorConfig; 2]> =
                        all_types.iter().map(|&ty| ty.into()).collect();
                    let multi_options = AggregationOptions {
                        aggregations,
                        bucket_duration: 10,
                        timestamp_output,
                        alignment: BucketAlignment::Start,
                        report_empty,
                    };
                    let rows: Vec<MultiSample> =
                        MultiAggregateIterator::new(input.clone().into_iter(), &multi_options, 0)
                            .collect();

                    for (column, &ty) in all_types.iter().enumerate() {
                        let single_options = AggregationOptions {
                            aggregations: smallvec::smallvec![ty.into()],
                            bucket_duration: 10,
                            timestamp_output,
                            alignment: BucketAlignment::Start,
                            report_empty,
                        };
                        let singles: Vec<Sample> =
                            AggregateIterator::new(input.clone().into_iter(), &single_options, 0)
                                .collect();

                        let context = format!(
                            "type={ty:?} empty={report_empty} ts_out={timestamp_output:?} input_len={}",
                            input.len()
                        );
                        if report_empty {
                            assert_eq!(rows.len(), singles.len(), "row count mismatch: {context}");
                        } else {
                            assert!(
                                rows.len() >= singles.len(),
                                "multi dropped a bucket the single iterator emitted: \
                                 multi={} single={} ({context})",
                                rows.len(),
                                singles.len(),
                            );
                        }

                        for single in singles.iter() {
                            let row = rows
                                .iter()
                                .find(|r| r.timestamp == single.timestamp)
                                .unwrap_or_else(|| {
                                    panic!(
                                        "multi is missing bucket {} emitted by the single \
                                         iterator ({context})",
                                        single.timestamp
                                    )
                                });
                            let multi_value = row.values[column];
                            assert!(
                                (multi_value.is_nan() && single.value.is_nan())
                                    || multi_value == single.value,
                                "value mismatch at ts {}: multi={multi_value} single={} ({context})",
                                row.timestamp,
                                single.value,
                            );
                        }
                    }
                }
            }
        }
    }

    /// Multi rows preserve the specified aggregator order.
    #[test]
    fn test_multi_column_order() {
        let samples = create_test_samples();
        let options = AggregationOptions {
            aggregations: [
                AggregationType::Sum,
                AggregationType::Min,
                AggregationType::Count,
            ]
            .into_iter()
            .map(Into::into)
            .collect(),
            bucket_duration: 10,
            timestamp_output: BucketTimestamp::Start,
            alignment: BucketAlignment::Start,
            report_empty: false,
        };

        let rows: Vec<MultiSample> =
            MultiAggregateIterator::new(samples.into_iter(), &options, 0).collect();

        // first bucket [10, 20): samples 1.0 and 2.0
        assert_eq!(rows[0].timestamp, 10);
        assert_eq!(rows[0].values.as_slice(), &[3.0, 1.0, 2.0]); // sum, min, count
    }

    /// EMPTY back-fill uses each aggregator's own empty value per column
    /// (sum -> 0, min -> NaN).
    #[test]
    fn test_multi_empty_backfill_per_column() {
        let samples = vec![
            Sample::new(10, 1.0),
            Sample::new(15, 2.0),
            // gap: buckets [20,30) and [30,40) are empty
            Sample::new(40, 5.0),
        ];
        let options = AggregationOptions {
            aggregations: [AggregationType::Sum, AggregationType::Min]
                .into_iter()
                .map(Into::into)
                .collect(),
            bucket_duration: 10,
            timestamp_output: BucketTimestamp::Start,
            alignment: BucketAlignment::Start,
            report_empty: true,
        };

        let rows: Vec<MultiSample> =
            MultiAggregateIterator::new(samples.into_iter(), &options, 0).collect();

        assert_eq!(rows.len(), 4);
        assert_eq!(rows[1].timestamp, 20);
        assert_eq!(rows[1].values[0], 0.0); // sum empty bucket
        assert!(rows[1].values[1].is_nan()); // min empty bucket
        assert_eq!(rows[3].values.as_slice(), &[5.0, 5.0]);
    }

    // #[test]
    // fn test_alignment_with_offset() {
    //     let samples = vec![
    //         Sample::new(12, 1.0),
    //         Sample::new(17, 2.0),
    //         Sample::new(22, 3.0),
    //         Sample::new(32, 4.0),
    //     ];
    //
    //     let options = create_options(Aggregator::Sum);
    //
    //     let iterator = AggregateIterator::new(
    //         samples.into_iter(),
    //         options,
    //         2, // Aligned timestamp is 2
    //     );
    //
    //     let result: Vec<Sample> = iterator.collect();
    //
    //     // With alignment at 2, buckets should be [2, 12), [12, 22), [22, 32), [32, 42)
    //     assert_eq!(result.len(), 4);
    //     assert_eq!(result[0].timestamp, 2);  // First bucket starts at alignment point
    //     assert_eq!(result[0].value, 0.0);    // No values in this bucket
    //     assert_eq!(result[1].timestamp, 12); // Second bucket
    //     assert_eq!(result[1].value, 3.0);    // Sum of 1.0 and 2.0
    //     assert_eq!(result[2].timestamp, 22);
    //     assert_eq!(result[2].value, 3.0);
    //     assert_eq!(result[3].timestamp, 32);
    //     assert_eq!(result[3].value, 4.0);
    // }
}
