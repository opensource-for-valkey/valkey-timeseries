use crate::aggregators::{AggregationHandler, Aggregator, BucketTimestamp};
use crate::common::{MultiSample, Sample, Timestamp};
use crate::series::request_types::AggregationOptions;
use smallvec::SmallVec;
use std::collections::VecDeque;

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
    bucket_range_end: Timestamp,
    align_timestamp: Timestamp,
    /// Whether the current bucket received any sample at all, NaN or not. A bucket every
    /// aggregator ignored still counts as started, and the trailing one must not be dropped
    /// by `finalize_last_bucket_if_any`.
    saw_sample: bool,
    report_empty: bool,
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
            bucket_range_end: 0,
            align_timestamp,
            saw_sample: false,
            report_empty: options.report_empty,
        }
    }

    fn empty_row(&self, timestamp: Timestamp) -> MultiSample {
        MultiSample {
            timestamp,
            values: self
                .aggregators
                .iter()
                .map(AggregationHandler::empty_bucket_value)
                .collect(),
        }
    }

    fn add_empty_bucket_internal(
        &self,
        samples: &mut VecDeque<MultiSample>,
        start_bucket: Timestamp,
        end_bucket_exclusive: Timestamp,
    ) {
        if end_bucket_exclusive <= start_bucket {
            return;
        }

        let count = ((end_bucket_exclusive - start_bucket) / self.bucket_duration as i64) as usize;
        samples.reserve(count);

        for bucket_start in
            (start_bucket..end_bucket_exclusive).step_by(self.bucket_duration as usize)
        {
            samples.push_back(self.empty_row(self.render_timestamp(bucket_start)));
        }
    }

    fn add_empty_buckets_between_timestamps(
        &self,
        samples: &mut VecDeque<MultiSample>,
        first_ts: Timestamp,
        end_ts: Timestamp,
    ) {
        let start = self.calc_bucket_start(first_ts);
        let end = self.calc_bucket_start(end_ts);
        self.add_empty_bucket_internal(samples, start, end);
    }

    fn output_timestamp(&self) -> Timestamp {
        self.render_timestamp(self.bucket_range_start)
    }

    fn complete_bucket(
        &mut self,
        last_ts: Option<Timestamp>,
        empty_buckets: &mut VecDeque<MultiSample>,
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

        let bucket = if any_accepted {
            Some(MultiSample::new(
                self.output_timestamp(),
                self.aggregators
                    .iter_mut()
                    .map(AggregationHandler::finalize)
                    .collect(),
            ))
        } else if self.report_empty {
            Some(MultiSample::new(
                self.output_timestamp(),
                self.aggregators
                    .iter()
                    .map(AggregationHandler::empty_value)
                    .collect(),
            ))
        } else {
            None
        };

        for aggregator in self.aggregators.iter_mut() {
            AggregationHandler::reset(aggregator);
        }
        self.accepted.fill(false);

        if self.report_empty
            && let Some(last_ts) = last_ts
            && last_ts >= self.bucket_range_end
        {
            let start = self.bucket_range_end + 1;
            self.add_empty_buckets_between_timestamps(empty_buckets, start, last_ts);
        }

        self.saw_sample = false;
        bucket
    }

    fn update(&mut self, sample: Sample) {
        for (aggregator, accepted) in self.aggregators.iter_mut().zip(self.accepted.iter_mut()) {
            *accepted |= aggregator.update(sample.timestamp, sample.value);
        }
        self.saw_sample = true;
    }

    #[inline]
    fn should_finalize_bucket(&self, timestamp: Timestamp) -> bool {
        timestamp >= self.bucket_range_end
    }

    fn update_bucket_timestamps(&mut self, timestamp: Timestamp) {
        self.bucket_range_start = self.calc_bucket_start(timestamp);
        self.bucket_range_end = self
            .bucket_range_start
            .saturating_add_unsigned(self.bucket_duration);
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
pub(crate) fn bucket_start_for(
    ts: Timestamp,
    align_timestamp: Timestamp,
    bucket_duration: u64,
) -> Timestamp {
    let diff = ts - align_timestamp;
    let delta = bucket_duration as i64;
    ts - ((diff % delta + delta) % delta)
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
/// the series really has passing data beyond it. Both directions are RedisTimeSeries 8.10
/// behavior, confirmed by black-box probing of the reference.
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
    /// Value of the last passing sample *before* the window, or NaN when there is none.
    /// `last`'s EMPTY fill is "the value of the last sample before the bucket's start", so a
    /// leading gap bucket inherits a sample the query itself never reads. Carried here
    /// rather than looked up downstream because it comes from the same scan as `start`.
    pub carry_seed: f64,
}

impl Default for EmptyFillBounds {
    fn default() -> Self {
        Self {
            start: None,
            end: None,
            carry_seed: f64::NAN,
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
    empty_buckets: VecDeque<MultiSample>,
    init: bool,
    empty_fill: EmptyFillBounds,
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
        Self {
            inner,
            aggregator: AggregationHelper::new(options, aligned_timestamp),
            empty_buckets: VecDeque::new(),
            init: false,
            empty_fill,
        }
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
        self.empty_buckets.pop_front()
    }

    /// One past the last bucket of `ts`, for the exclusive end `add_empty_bucket_internal` takes.
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
        {
            let end_exclusive = self.bucket_end_exclusive(query_end);
            self.aggregator.add_empty_bucket_internal(
                &mut self.empty_buckets,
                self.aggregator.bucket_range_end,
                end_exclusive,
            );
        }

        bucket
    }
}

impl<T: Iterator<Item = Sample>> Iterator for MultiAggregateIterator<T> {
    type Item = MultiSample;

    fn next(&mut self) -> Option<Self::Item> {
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
        use std::f64;

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

    // #[test] TODO
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

        assert_eq!(result.len(), 5);
        assert_eq!(result[0].timestamp, 10);
        assert_eq!(result[0].value, 99.0);
        assert_eq!(result[1].timestamp, 20);
        assert_eq!(result[1].value, 99.0); // Empty bucket with value 0 for sum
        assert_eq!(result[2].timestamp, 30);
        assert_eq!(result[2].value, 99.0); // Empty bucket
        assert_eq!(result[3].timestamp, 40);
        assert_eq!(result[3].value, 5.0);
        assert_eq!(result[4].timestamp, 50);
        assert_eq!(result[4].value, 6.0);
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
