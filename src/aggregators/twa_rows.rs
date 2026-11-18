//! Bucketed rows for AGGREGATION lists that contain `twa`.
//!
//! TWA brackets each bucket with samples from its neighbors, so it cannot be an ordinary
//! per-bucket accumulator. This driver walks the window's samples once, in time order, and
//! computes the `twa` column from [`TwaBucket`] and [`gap_twa`] while ordinary columns in the
//! same list accumulate as usual. Rows always come out in time order; reverse commands reverse
//! them afterwards like every other aggregation. A `last` column beside `twa` takes its EMPTY fill
//! from the same [`LastCarry`] that every other list uses.

use super::twa::value_at;
use crate::aggregators::{
    AggregationHandler, AggregationType, Aggregator, BucketTimestamp, EmptyFillBounds, LastCarry,
    PendingRows, TwaBucket, bucket_start_for, column_aggregator, gap_twa, is_real,
    render_bucket_timestamp, run_start_keeping_last,
};
use crate::common::{MultiSample, Sample, Timestamp};
use crate::series::request_types::AggregationOptions;
use smallvec::SmallVec;
use std::iter::Peekable;

/// The order in which a command visits buckets. TWA's value depends on it, because the
/// sample carried into a bucket comes from the bucket visited before it.
///
/// A bucket has a carry side, where the visit came from, and a closing side, where it goes
/// next. The carry side uses the nearest real sample. The closing side uses the raw sample, so a
/// NaN there stops interpolation. Forward, the carry is the earlier side. Reverse, it is the
/// later side. Because of that, RANGE and REVRANGE differ where a raw NaN closes a bucket.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ScanDirection {
    #[default]
    Forward,
    Reverse,
}

impl ScanDirection {
    pub fn from_reverse(is_reverse: bool) -> Self {
        if is_reverse {
            Self::Reverse
        } else {
            Self::Forward
        }
    }
}

/// Up to two real (non-NaN) samples on each side of the query window that pass its filters,
/// oldest first. The nearest is `before.last()` and `after.first()`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TwaNeighbors {
    pub before: SmallVec<[Sample; 2]>,
    pub after: SmallVec<[Sample; 2]>,
}

impl TwaNeighbors {
    /// Splits a timestamp-sorted `timeline` around the window `[start, end]` in one pass: the
    /// neighbors, and the samples inside the window. Stops reading once both sides are known.
    /// An inverted window holds no samples, and a sample between its edges can be a neighbor on
    /// both sides.
    #[cfg(test)]
    pub fn split(
        timeline: impl IntoIterator<Item = Sample>,
        start: Timestamp,
        end: Timestamp,
    ) -> (Self, Vec<Sample>) {
        let mut neighbors = Self::default();
        let mut window = Vec::new();
        for sample in timeline {
            let timestamp = sample.timestamp;
            if timestamp < start && is_real(&sample) {
                if neighbors.before.len() == 2 {
                    neighbors.before.remove(0);
                }
                neighbors.before.push(sample);
            }
            if (start..=end).contains(&timestamp) {
                window.push(sample);
            } else if timestamp > end && neighbors.after.len() < 2 && is_real(&sample) {
                neighbors.after.push(sample);
            }
            if neighbors.after.len() == 2 && timestamp >= start {
                break;
            }
        }
        (neighbors, window)
    }
}

/// What a TWA row stream needs besides its in-window samples.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TwaContext {
    pub start: Timestamp,
    pub end: Timestamp,
    /// Whether the query named its end timestamp rather than `+`.
    pub explicit_end: bool,
    pub neighbors: TwaNeighbors,
    pub direction: ScanDirection,
    /// The still-open compaction bucket that LATEST adds after the stored samples. It brackets
    /// buckets like any sample, but it is not stored data, so it never extends EMPTY rows or
    /// supports a gap.
    pub latest: Option<Sample>,
    /// What `last` needs from outside the window ([`EmptyFillBounds::carry_seed`] and the like),
    /// and the number of trailing rows the caller will use.
    pub empty_fill: EmptyFillBounds,
}

/// Opens a cursor over the stored real-or-NaN samples in the window, starting at the first one
/// at or after the given timestamp. The row iterator reads it only to find real samples, so the
/// main stream never has to look past its next sample.
pub type StoredReals<'a> = Box<dyn FnMut(Timestamp) -> Box<dyn Iterator<Item = Sample> + 'a> + 'a>;

/// The upcoming in-window samples. The main stream is read one sample ahead. Real samples
/// further on come from their own cursor, and at most two of them are kept.
struct Lookahead<'a, I: Iterator<Item = Sample>> {
    stored: Peekable<I>,
    latest: Option<Sample>,
    open_cursor: StoredReals<'a>,
    /// Real samples at or after the last threshold, oldest first.
    reals_ahead: SmallVec<[Sample; 2]>,
    /// Whether the cursor found no real sample after `reals_ahead`.
    exhausted: bool,
}

impl<'a, I: Iterator<Item = Sample>> Lookahead<'a, I> {
    fn new(stored: I, latest: Option<Sample>, open_cursor: StoredReals<'a>) -> Self {
        Self {
            stored: stored.peekable(),
            latest,
            open_cursor,
            reals_ahead: SmallVec::new(),
            exhausted: false,
        }
    }

    fn peek(&mut self) -> Option<Sample> {
        self.stored.peek().copied().or(self.latest)
    }

    fn next(&mut self) -> Option<Sample> {
        self.stored.next().or_else(|| self.latest.take())
    }

    /// The first `count` (at most two) stored real samples at or after `threshold`, which must
    /// not decrease between calls. NaNs between them are skipped without being kept.
    fn stored_reals_from(&mut self, threshold: Timestamp, count: usize) -> SmallVec<[Sample; 2]> {
        self.reals_ahead.retain(|real| real.timestamp >= threshold);
        if self.reals_ahead.len() < count && !self.exhausted {
            let from = self
                .reals_ahead
                .last()
                .and_then(|real| real.timestamp.checked_add(1))
                .map_or(threshold, |next| next.max(threshold));
            let missing = count - self.reals_ahead.len();
            let found: SmallVec<[Sample; 2]> = (self.open_cursor)(from)
                .filter(is_real)
                .take(missing)
                .collect();
            self.exhausted = found.len() < missing;
            self.reals_ahead.extend(found);
        }
        self.reals_ahead.iter().take(count).copied().collect()
    }
}

/// TWA over a window of one timestamp, `at`: the real sample there, else the line between the
/// real samples that bracket it, else NaN.
pub(super) fn twa_at(
    at: Timestamp,
    before: Option<Sample>,
    inside: Option<Sample>,
    after: Option<Sample>,
) -> f64 {
    match (before, inside, after) {
        (_, Some(sample), _) => sample.value,
        (Some(before), None, Some(after)) => value_at(before, after, at),
        _ => f64::NAN,
    }
}

/// The edge of the nearest earlier bucket that held samples, which brackets the next bucket.
#[derive(Clone, Copy, Debug)]
struct BucketEdge {
    key: Timestamp,
    last_raw: Sample,
    last_real: Option<Sample>,
}

enum Column {
    Twa,
    Ordinary(Aggregator),
}

/// Which bucket keys the stream still has to emit.
#[derive(Clone, Copy, Debug)]
enum Extent {
    NotStarted,
    /// Without EMPTY: only buckets that hold samples.
    Sampled,
    /// With EMPTY: every bucket from `next` on, up to [`TwaRowIterator::final_key`].
    /// `trailing_end` is the window's end when real data lies after the window.
    Every {
        next: Timestamp,
        trailing_end: Option<Timestamp>,
    },
    Done,
}

pub struct TwaRowIterator<'a, I: Iterator<Item = Sample>> {
    samples: Lookahead<'a, I>,
    columns: SmallVec<[Column; 2]>,
    width: u64,
    aligned: Timestamp,
    bucket_ts: BucketTimestamp,
    report_empty: bool,
    context: TwaContext,
    /// The last two real samples before the current bucket, oldest first.
    left_reals: SmallVec<[Sample; 2]>,
    earlier: Option<BucketEdge>,
    extent: Extent,
    last_carry: Option<LastCarry>,
    held: PendingRows,
}

impl<'a, I: Iterator<Item = Sample>> TwaRowIterator<'a, I> {
    /// `samples` are the window's filtered samples in time order. `reals` opens a second
    /// cursor over the same samples, at a given timestamp, to look for real ones ahead.
    pub fn new(
        samples: I,
        reals: StoredReals<'a>,
        options: &AggregationOptions,
        mut context: TwaContext,
    ) -> Self {
        let aligned = options
            .alignment
            .get_aligned_timestamp(context.start, context.end);
        let mut last_carry = LastCarry::new(options);
        if let Some(carry) = last_carry.as_mut() {
            carry.set_fill(&context.empty_fill);
        }
        let columns = options
            .aggregations
            .iter()
            .map(|config| match config.aggregation_type() {
                AggregationType::Twa => Column::Twa,
                _ => Column::Ordinary(column_aggregator(config, options)),
            })
            .collect();
        Self {
            samples: Lookahead::new(samples, context.latest, reals),
            columns,
            width: options.bucket_duration,
            aligned,
            bucket_ts: options.timestamp_output,
            report_empty: options.report_empty,
            left_reals: std::mem::take(&mut context.neighbors.before),
            context,
            earlier: None,
            extent: Extent::NotStarted,
            last_carry,
            held: PendingRows::default(),
        }
    }

    fn key(&self, timestamp: Timestamp) -> Timestamp {
        bucket_start_for(timestamp, self.aligned, self.width)
    }

    fn after_key(&self, key: Timestamp) -> Option<Timestamp> {
        key.checked_add_unsigned(self.width)
    }

    /// Decides the EMPTY row extent the way RedisTimeSeries does for TWA: real neighbors on
    /// a side let the rows reach the window's edge there, except where the aligned grid's
    /// first bucket starts below zero.
    fn start_extent(&mut self) -> Extent {
        if !self.report_empty {
            return Extent::Sampled;
        }
        let (start, end) = (self.context.start, self.context.end);
        let before = !self.left_reals.is_empty();
        let after = !self.context.neighbors.after.is_empty();
        if self.context.direction == ScanDirection::Reverse
            && self.context.explicit_end
            && self.key(end) < 0
        {
            return Extent::Done;
        }
        let Some(first) = self.samples.peek() else {
            return if before && after && self.key(start) >= 0 {
                Extent::Every {
                    next: self.key(start),
                    trailing_end: Some(end),
                }
            } else {
                Extent::Done
            };
        };
        let overlaps =
            before && after || !self.samples.stored_reals_from(Timestamp::MIN, 1).is_empty();
        let next = if overlaps && before && self.key(start) >= 0 {
            self.key(start)
        } else {
            self.key(first.timestamp)
        };
        Extent::Every {
            next,
            trailing_end: (overlaps && after).then_some(end),
        }
    }

    /// The real sample that brackets a bucket on its earlier side.
    fn left_bracket(&self) -> Option<Sample> {
        let Some(edge) = self.earlier else {
            return self.left_reals.last().copied();
        };
        match self.context.direction {
            ScanDirection::Forward => edge.last_real,
            ScanDirection::Reverse => Some(edge.last_raw).filter(is_real),
        }
    }

    /// The sample that brackets bucket `key` on its later side, once its samples are consumed.
    fn right_bracket(&mut self) -> Option<Sample> {
        let Some(next) = self.samples.peek() else {
            return self.context.neighbors.after.first().copied();
        };
        match self.context.direction {
            ScanDirection::Forward => Some(next),
            ScanDirection::Reverse => {
                if is_real(&next) {
                    return Some(next);
                }
                let next_key = self.key(next.timestamp);
                let stored = self.samples.stored_reals_from(next.timestamp, 1);
                let in_bucket = |sample: &Sample| self.key(sample.timestamp) == next_key;
                stored.first().copied().filter(in_bucket).or(self
                    .context
                    .latest
                    .filter(is_real)
                    .filter(in_bucket))
            }
        }
    }

    /// TWA of a bucket without real samples, from the nearest real samples around it. The
    /// first aligned bucket is measured from zero with its full width, as the reference does.
    fn gap_value(&mut self, key: Timestamp) -> f64 {
        let nominal = key.max(0);
        let start = self.context.start.max(nominal);
        let end = self
            .context
            .end
            .min(nominal.saturating_add_unsigned(self.width));
        let right: SmallVec<[Sample; 2]> = self
            .samples
            .stored_reals_from(end, 2)
            .into_iter()
            .chain(self.context.neighbors.after.iter().copied())
            .take(2)
            .collect();
        // An inverted window selects no samples, yet EMPTY still reports its bucket; the
        // reference averages that bucket's line from the window's end to its start.
        gap_twa(&self.left_reals, start.min(end), start.max(end), &right)
    }

    fn render(&self, key: Timestamp) -> Timestamp {
        render_bucket_timestamp(self.bucket_ts, key, self.width)
    }

    /// Consumes bucket `key`'s samples and returns its row, and whether any column accepted
    /// a sample from it.
    fn sampled_row(&mut self, key: Timestamp) -> (MultiSample, bool) {
        let start = self.context.start.max(key);
        let end = self
            .context
            .end
            .min(key.saturating_add_unsigned(self.width));
        let left = self.left_bracket();
        let mut bucket = TwaBucket::open(left, start, end);
        let mut accepted = false;
        let mut last_raw = None;
        let mut prev_real = None;
        let mut last_real = None;
        let bucket_end = self.after_key(key);
        // The first sample is always this bucket's: its timestamp chose `key`.
        while let Some(sample) = self
            .samples
            .peek()
            .filter(|s| last_raw.is_none() || bucket_end.is_none_or(|end| s.timestamp < end))
        {
            self.samples.next();
            bucket.push(sample);
            for column in self.columns.iter_mut() {
                if let Column::Ordinary(aggregator) = column {
                    accepted |= aggregator.update(sample.timestamp, sample.value);
                }
            }
            last_raw = Some(sample);
            if is_real(&sample) {
                prev_real = last_real.replace(sample);
            }
        }
        let has_real = last_real.is_some();
        if let Some(last) = last_real {
            let older = prev_real.or_else(|| self.left_reals.last().copied());
            self.left_reals.clear();
            self.left_reals.extend(older.into_iter().chain([last]));
        }
        accepted |= has_real;
        let twa = if has_real {
            let right = self.right_bracket();
            if start == end {
                twa_at(
                    start,
                    left.filter(is_real),
                    last_real,
                    right.filter(is_real),
                )
            } else {
                bucket.close(right).0
            }
        } else if accepted || self.report_empty {
            self.gap_value(key)
        } else {
            f64::NAN
        };
        self.earlier = last_raw.map(|last_raw| BucketEdge {
            key,
            last_raw,
            last_real,
        });

        let mut values: SmallVec<[f64; 4]> = self
            .columns
            .iter_mut()
            .map(|column| match column {
                Column::Twa => twa,
                Column::Ordinary(aggregator) if accepted => aggregator.finalize(),
                Column::Ordinary(aggregator) => {
                    let value = aggregator.empty_value();
                    aggregator.reset();
                    value
                }
            })
            .collect();
        if let Some(carry) = self.last_carry.as_mut() {
            carry.apply_to_bucket_with_samples(&mut values);
        }
        (MultiSample::new(self.render(key), values), accepted)
    }

    fn gap_row(&mut self, key: Timestamp) -> MultiSample {
        let twa = self.gap_value(key);
        let mut values: SmallVec<[f64; 4]> = self
            .columns
            .iter()
            .map(|column| match column {
                Column::Twa => twa,
                Column::Ordinary(aggregator) => aggregator.empty_value(),
            })
            .collect();
        if let Some(carry) = self.last_carry.as_mut() {
            carry.apply_to_gap(&mut values);
        }
        MultiSample::new(self.render(key), values)
    }

    /// A run of gap buckets that precedes every sample holds what came before the window. The
    /// ordinary pipeline decides that from the window and the first sample, whichever rows a
    /// `twa` list goes on to report, and so does this.
    fn start_carry(&mut self) {
        let fill = self.context.empty_fill;
        let Some(start) = fill.start else {
            return;
        };
        let requested = self.key(start);
        let leads = match self.samples.peek() {
            Some(first) => requested < self.key(first.timestamp),
            None => fill.end.is_some(),
        };
        if leads && let Some(carry) = self.last_carry.as_mut() {
            carry.start_leading_run(fill.carry_seed);
        }
    }

    /// Jumps over the head of a run of gap rows that ends before `end_exclusive`, so that no
    /// more than the caller's tail limit remain. Every gap row of a run holds what the first
    /// one does, so nothing the skipped ones would have done is lost.
    fn skip_gaps(&self, next: Timestamp, end_exclusive: i128) -> Timestamp {
        let Some(keep) = self.context.empty_fill.tail_limit else {
            return next;
        };
        let start = run_start_keeping_last(next, end_exclusive, keep.max(1), self.width);
        Timestamp::try_from(start).unwrap_or(next)
    }

    /// The last EMPTY bucket key, once every sample has been read. Rows run through the last
    /// sampled bucket, and on to the window's end when real data lies after it, except from
    /// a bucket below zero going forwards. A window without samples spans to its end.
    fn final_key(&self, trailing_end: Option<Timestamp>) -> Option<Timestamp> {
        let Some(last_seen) = self.earlier.map(|edge| edge.key) else {
            return trailing_end.map(|end| self.key(end));
        };
        let reaches_end = self.context.direction == ScanDirection::Reverse || last_seen >= 0;
        Some(match trailing_end {
            Some(end) if reaches_end => self.key(end),
            _ => last_seen,
        })
    }
}

impl<I: Iterator<Item = Sample>> TwaRowIterator<'_, I> {
    /// The next row in time order, before reverse `last` rows are resolved.
    fn next_row(&mut self) -> Option<MultiSample> {
        loop {
            match self.extent {
                Extent::Done => return None,
                Extent::NotStarted => {
                    self.start_carry();
                    self.extent = self.start_extent();
                }
                Extent::Sampled => {
                    let sample = self.samples.peek()?;
                    let (row, accepted) = self.sampled_row(self.key(sample.timestamp));
                    if accepted {
                        return Some(row);
                    }
                }
                Extent::Every {
                    mut next,
                    trailing_end,
                } => {
                    let sampled_key = self.samples.peek().map(|s| self.key(s.timestamp));
                    let last = self.final_key(trailing_end);
                    if sampled_key.is_none() && last.is_none_or(|last| next > last) {
                        self.extent = Extent::Done;
                        return None;
                    }
                    if sampled_key != Some(next) {
                        let end_exclusive = match (sampled_key, last) {
                            (Some(key), _) => i128::from(key),
                            (None, Some(last)) => i128::from(last) + i128::from(self.width),
                            (None, None) => i128::from(next),
                        };
                        next = self.skip_gaps(next, end_exclusive);
                    }
                    self.extent = match self.after_key(next) {
                        Some(following) => Extent::Every {
                            next: following,
                            trailing_end,
                        },
                        None => Extent::Done,
                    };
                    return Some(if sampled_key == Some(next) {
                        self.sampled_row(next).0
                    } else {
                        self.gap_row(next)
                    });
                }
            }
        }
    }

    /// Holds back the rows a reverse `last` cannot answer until the next bucket can.
    fn next_resolved_row(&mut self) -> Option<MultiSample> {
        while !self.held.has_ready() {
            let Some(row) = self.next_row() else {
                self.held.finish();
                break;
            };
            let carry = self.last_carry.as_mut()?;
            self.held
                .offer(row, carry, self.context.empty_fill.tail_limit);
        }
        self.held.pop()
    }
}

impl<I: Iterator<Item = Sample>> Iterator for TwaRowIterator<'_, I> {
    type Item = MultiSample;

    fn next(&mut self) -> Option<MultiSample> {
        if self.context.empty_fill.carry_reverse && self.last_carry.is_some() {
            self.next_resolved_row()
        } else {
            self.next_row()
        }
    }
}
