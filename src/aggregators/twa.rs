//! Time-weighted average (TWA) arithmetic, independent of storage and traversal.
//!
//! TWA joins real samples with straight lines and divides the area under those lines by the
//! time they cover. "Real" means not NaN, so infinities are real. Callers choose which samples
//! bracket a bucket; this module only does the arithmetic.

use crate::common::{Sample, Timestamp};
use get_size2::GetSize;

/// Whether the sample takes part in the average: NaN does not, infinities do.
pub(crate) fn is_real(sample: &Sample) -> bool {
    !sample.value.is_nan()
}

/// The value of the line from `a` to `b` at `timestamp`, with `a` before `b`. An endpoint
/// returns its own value exactly rather than an interpolation of it.
pub(super) fn value_at(a: Sample, b: Sample, timestamp: Timestamp) -> f64 {
    if timestamp == a.timestamp {
        return a.value;
    }
    if timestamp == b.timestamp {
        return b.value;
    }
    let elapsed = timestamp.abs_diff(a.timestamp) as f64;
    let span = b.timestamp.abs_diff(a.timestamp) as f64;
    a.value + (b.value - a.value) * elapsed / span
}

/// The running area and covered time of one time interval: a query bucket clipped to the window,
/// an aggregator's open bucket, or a temporary interval in [`gap_twa`]. The area lies under the straight
/// lines that join the samples, clipped to `[start, end]`.
///
/// The interval convention: buckets are `[start, end)` in timestamps, and an `Integral` works in
/// continuous time, where `[start, end]` has the same length. `end` is the next bucket's start
/// (`bucket_last + 1`), or the window's end when a query clips the bucket. The sample at `end`
/// only closes the last segment.
///
/// Samples may arrive in either time order. Callers skip the NaNs they want ignored; a NaN that
/// is added makes the area NaN once its segment covers time.
#[derive(Clone, Copy, Debug, PartialEq, GetSize)]
pub(super) struct Integral {
    start: Timestamp,
    end: Timestamp,
    previous: Option<Sample>,
    area: f64,
    /// Time inside `[start, end]` that the lines cover. The average divides by this, not by the
    /// interval width, so a bucket that data covers only partly averages that part.
    covered: u64,
}

impl Integral {
    fn new(start: Timestamp, end: Timestamp) -> Self {
        Self {
            start,
            end,
            previous: None,
            area: 0.0,
            covered: 0,
        }
    }

    fn extend_to(&mut self, sample: Sample) {
        let Some(previous) = self
            .previous
            .filter(|previous| previous.timestamp != sample.timestamp)
        else {
            self.previous = Some(sample);
            return;
        };

        let (a, b) = if previous.timestamp < sample.timestamp {
            (previous, sample)
        } else {
            (sample, previous)
        };
        let lo = self.start.max(a.timestamp);
        let hi = self.end.min(b.timestamp);
        if hi > lo {
            let width = hi.abs_diff(lo);
            self.area += (value_at(a, b, lo) + value_at(a, b, hi)) * (width as f64 / 2.0);
            self.covered = self.covered.saturating_add(width);
        } else if (self.start..=self.end).contains(&previous.timestamp) {
            // The line only touches the interval at `previous`, and samples go on away from it,
            // so `previous` stays the sample that a bucket without coverage reports.
            return;
        }
        self.previous = Some(sample);
    }

    pub(super) fn start(&self) -> Timestamp {
        self.start
    }

    pub(super) fn end(&self) -> Timestamp {
        self.end
    }

    pub(super) fn previous(&self) -> Option<Sample> {
        self.previous
    }

    pub(super) fn area(&self) -> f64 {
        self.area
    }

    pub(super) fn covered(&self) -> u64 {
        self.covered
    }

    /// `area / covered`. Without covered time: the sample inside the interval that the lines
    /// touch, or else NaN. An interval of one timestamp needs the line between the samples
    /// around it; see `twa_at` in the row driver.
    fn value(&self) -> f64 {
        if self.covered > 0 {
            self.area / self.covered as f64
        } else {
            self.previous
                .filter(|sample| (self.start..=self.end).contains(&sample.timestamp))
                .map_or(f64::NAN, |sample| sample.value)
        }
    }
}

/// One bucket's TWA, fed in traversal order: the incoming sample, the bucket's own samples, then
/// the closing sample.
///
/// NaNs inside the bucket are skipped. A NaN incoming or closing sample contributes nothing, so it
/// stops interpolation on that side. The bucket also remembers the sample it hands to the next
/// bucket: its last real sample, else its last NaN, else the incoming sample.
#[derive(Clone, Copy, Debug, PartialEq, GetSize)]
pub struct TwaBucket {
    integral: Integral,
    carry: Option<Sample>,
    has_real: bool,
}

impl TwaBucket {
    pub fn open(incoming: Option<Sample>, start: Timestamp, end: Timestamp) -> Self {
        let mut integral = Integral::new(start, end);
        if let Some(sample) = incoming.filter(is_real) {
            integral.extend_to(sample);
        }
        Self {
            integral,
            carry: incoming,
            has_real: false,
        }
    }

    pub fn push(&mut self, point: Sample) {
        if is_real(&point) {
            self.integral.extend_to(point);
            self.carry = Some(point);
            self.has_real = true;
        } else if !self.has_real {
            self.carry = Some(point);
        }
    }

    /// An aggregator between two buckets: no interval, only the sample the next bucket opens with.
    pub(super) fn awaiting(carry: Option<Sample>) -> Self {
        Self {
            integral: Integral::new(Timestamp::MIN, Timestamp::MAX),
            carry,
            has_real: false,
        }
    }

    pub(super) fn integral(&self) -> &Integral {
        &self.integral
    }

    pub(super) fn carry(&self) -> Option<Sample> {
        self.carry
    }

    /// Whether a real sample of the bucket's own was pushed.
    pub fn has_real(&self) -> bool {
        self.has_real
    }

    /// `LATEST` for an unfinished bucket. Without an average yet, the reference reports the last
    /// real sample the bucket has seen (possibly its incoming one), else 0.
    pub fn latest(&self) -> f64 {
        let integral = &self.integral;
        if integral.covered > 0 {
            integral.value()
        } else {
            integral.previous.map_or(0.0, |sample| sample.value)
        }
    }

    /// The bucket's average, and the sample carried into the next bucket.
    pub fn close(mut self, closing: Option<Sample>) -> (f64, Option<Sample>) {
        if let Some(sample) = closing.filter(is_real) {
            self.integral.extend_to(sample);
        }
        (self.integral.value(), self.carry)
    }
}

/// One bucket's TWA in a single call: `incoming` and `closing` are the samples just before and
/// just after the bucket in traversal order (`None` when there is none), `points` the bucket's
/// samples in that order, and `[start, end]` the interval averaged over. Returns the average and
/// the sample carried into the next bucket.
pub fn bucket_twa(
    incoming: Option<Sample>,
    points: impl IntoIterator<Item = Sample>,
    start: Timestamp,
    end: Timestamp,
    closing: Option<Sample>,
) -> (f64, Option<Sample>) {
    let mut bucket = TwaBucket::open(incoming, start, end);
    for point in points {
        bucket.push(point);
    }
    bucket.close(closing)
}

/// TWA of a bucket with no real samples of its own, from the nearest real samples around it.
///
/// `left` and `right` hold up to two real samples on each side, oldest first. With a sample on
/// both sides the line between the nearest two is averaged over `[start, end]`. With only one
/// side, two samples on it extend the nearest value for half their spacing (rounded down); the
/// bucket gets that value only if it starts (or, on the right, ends) inside the extension.
/// Anything less is NaN.
pub fn gap_twa(left: &[Sample], start: Timestamp, end: Timestamp, right: &[Sample]) -> f64 {
    let half_spacing = |p: &Sample, q: &Sample| i128::from(q.timestamp.abs_diff(p.timestamp) / 2);
    match (left, right) {
        ([.., nearest_left], [nearest_right, ..]) if start == end => {
            value_at(*nearest_left, *nearest_right, start)
        }
        ([.., nearest_left], [nearest_right, ..]) => {
            let mut integral = Integral::new(start, end);
            integral.extend_to(*nearest_left);
            integral.extend_to(*nearest_right);
            integral.value()
        }
        ([.., p, q], []) if i128::from(start) < i128::from(q.timestamp) + half_spacing(p, q) => {
            q.value
        }
        ([], [p, q, ..]) if i128::from(end) > i128::from(p.timestamp) - half_spacing(p, q) => {
            p.value
        }
        _ => f64::NAN,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(timestamp: Timestamp, value: f64) -> Sample {
        Sample::new(timestamp, value)
    }

    #[test]
    fn integral_continues_without_raw_history() {
        let mut integral = Integral::new(5, 15);
        integral.extend_to(p(0, 0.0));
        integral.extend_to(p(10, 10.0));
        assert_eq!(integral.value(), 7.5);
        integral.extend_to(p(20, 20.0));
        assert_eq!(integral.value(), 10.0);
    }

    #[test]
    fn bucket_skips_inside_nan_but_a_raw_integral_propagates_it() {
        let points = [p(2, 2.0), p(4, f64::NAN), p(8, 8.0)];
        assert_eq!(bucket_twa(None, points, 0, 10, None).0, 5.0);
        let mut raw = Integral::new(0, 10);
        for point in points {
            raw.extend_to(point);
        }
        assert!(raw.value().is_nan());
    }

    #[test]
    fn one_carry_is_not_enough_to_fill_one_sided_gaps() {
        assert_eq!(gap_twa(&[p(0, 1.0), p(19, 8.0)], 25, 50, &[]), 8.0);
        assert!(gap_twa(&[p(18, 1.0), p(19, 8.0)], 25, 50, &[]).is_nan());
    }

    #[test]
    fn all_four_incoming_and_closing_nan_combinations() {
        for (left_real, right_real, expected) in [
            (true, true, 5.0),
            (true, false, 2.5),
            (false, true, 7.5),
            (false, false, 5.0),
        ] {
            let left = p(0, if left_real { 0.0 } else { f64::NAN });
            let right = p(10, if right_real { 10.0 } else { f64::NAN });
            let (value, carry) = bucket_twa(Some(left), [p(5, 5.0)], 0, 10, Some(right));
            assert_eq!(
                value, expected,
                "left_real={left_real} right_real={right_real}"
            );
            assert_eq!(carry, Some(p(5, 5.0)));
        }
    }

    #[test]
    fn reverse_traversal_uses_the_same_integral() {
        let (left, right) = (p(0, 0.0), p(10, 10.0));
        for inside in [
            vec![p(2, 2.0), p(8, 8.0)],
            vec![p(2, 2.0), p(5, f64::NAN), p(8, 8.0)],
            vec![p(2, f64::NAN), p(5, 5.0), p(8, f64::NAN)],
        ] {
            let forward = bucket_twa(Some(left), inside.iter().copied(), 1, 9, Some(right));
            let reverse = bucket_twa(Some(right), inside.iter().rev().copied(), 1, 9, Some(left));
            assert_eq!(forward.0, 5.0);
            assert_eq!(reverse.0, 5.0);
            let real: Vec<Sample> = inside.into_iter().filter(|s| !s.value.is_nan()).collect();
            assert_eq!(forward.1, real.last().copied());
            assert_eq!(reverse.1, real.first().copied());
        }
    }

    #[test]
    fn affine_signal_has_analytic_average() {
        for (slope, intercept) in [(-3.0, 7.0), (0.0, 4.0), (2.0, -9.0)] {
            for (start, end) in [(0, 10), (2, 7)] {
                let points = [0, 4, 10].map(|t| p(t, slope * t as f64 + intercept));
                let expected = slope * ((start + end) as f64 / 2.0) + intercept;
                let (value, _) = bucket_twa(None, points, start, end, None);
                assert!(
                    (value - expected).abs() <= 1e-12 * expected.abs().max(1.0),
                    "slope={slope} [{start},{end}]: {value} != {expected}"
                );
            }
        }
    }

    #[test]
    fn only_covered_time_is_the_denominator() {
        // Data covers [2, 8], so this ramp averages 5, not area / 10 = 3.
        assert_eq!(bucket_twa(None, [p(2, 2.0), p(8, 8.0)], 0, 10, None).0, 5.0);
    }

    #[test]
    fn timestamp_distance_near_signed_limit_does_not_overflow() {
        let offset = Timestamp::MAX - 10;
        let points = [p(offset, 2.0), p(offset + 10, 8.0)];
        assert_eq!(
            bucket_twa(None, points, offset + 2, offset + 8, None).0,
            5.0
        );
        let span = [p(Timestamp::MIN, 1.0), p(Timestamp::MAX, 3.0)];
        assert_eq!(
            bucket_twa(None, span, Timestamp::MIN, Timestamp::MAX, None).0,
            2.0
        );
    }

    #[test]
    fn a_sample_on_the_boundary_is_reported_when_the_lines_only_touch_the_interval() {
        // A line that ends where the interval starts, or starts where it ends, covers no time.
        let before = bucket_twa(Some(p(0, 3.0)), [p(10, 9.0)], 10, 20, None);
        assert_eq!(before.0, 9.0);
        let after = bucket_twa(None, [p(20, 9.0)], 10, 20, Some(p(30, 1.0)));
        assert_eq!(after.0, 9.0);
        let reverse = bucket_twa(Some(p(30, 1.0)), [p(20, 9.0)], 10, 20, None);
        assert_eq!(reverse.0, 9.0);
        let nothing = bucket_twa(Some(p(0, 3.0)), [], 10, 20, None);
        assert!(nothing.0.is_nan());
        assert_eq!(nothing.1, Some(p(0, 3.0)));
    }

    #[test]
    fn gap_requires_either_two_brackets_or_two_samples_on_one_side() {
        assert!(gap_twa(&[], 10, 20, &[]).is_nan());
        assert!(gap_twa(&[p(0, 1.0)], 10, 20, &[]).is_nan());
        assert!(gap_twa(&[], 10, 20, &[p(30, 1.0)]).is_nan());
        assert_eq!(gap_twa(&[p(0, 0.0)], 10, 20, &[p(30, 30.0)]), 15.0);
    }

    #[test]
    fn half_spacing_support_has_strict_edges_and_integer_rounding() {
        let left = [p(0, 1.0), p(5, 8.0)];
        let right = [p(15, 8.0), p(20, 1.0)];
        assert_eq!(gap_twa(&left, 6, 10, &[]), 8.0);
        assert!(gap_twa(&left, 7, 10, &[]).is_nan());
        assert_eq!(gap_twa(&[], 10, 14, &right), 8.0);
        assert!(gap_twa(&[], 10, 13, &right).is_nan());
    }

    #[test]
    fn half_spacing_limit_past_the_signed_limit_still_holds_the_value() {
        let left = [p(Timestamp::MAX - 10, 1.0), p(Timestamp::MAX - 2, 8.0)];
        assert_eq!(gap_twa(&left, Timestamp::MAX, Timestamp::MAX, &[]), 8.0);
    }

    #[test]
    fn infinities_are_real_samples() {
        let (value, carry) = bucket_twa(None, [p(0, f64::INFINITY), p(10, 1.0)], 0, 10, None);
        assert_eq!(value, f64::INFINITY);
        assert_eq!(carry, Some(p(10, 1.0)));
    }

    #[test]
    fn latest_falls_back_to_the_incoming_value_then_zero() {
        let open = |incoming| {
            let mut bucket = TwaBucket::open(incoming, 10, 20);
            bucket.push(p(12, f64::NAN));
            bucket
        };
        assert_eq!(open(Some(p(2, 2.0))).latest(), 2.0);
        assert_eq!(open(None).latest(), 0.0);
        assert_eq!(open(Some(p(8, f64::NAN))).latest(), 0.0);
        let mut averaged = TwaBucket::open(Some(p(5, 5.0)), 10, 20);
        averaged.push(p(15, 15.0));
        assert_eq!(averaged.latest(), 12.5);
    }

    #[test]
    fn a_bucket_can_continue_without_its_raw_points() {
        let points = [p(2, 2.0), p(4, f64::NAN), p(8, 8.0)];
        let (incoming, closing) = (Some(p(-2, -2.0)), Some(p(12, 12.0)));
        let mut saved = TwaBucket::open(incoming, 0, 10);
        for point in &points[..2] {
            saved.push(*point);
        }
        let mut resumed = saved;
        resumed.push(points[2]);
        assert_eq!(
            resumed.close(closing),
            bucket_twa(incoming, points, 0, 10, closing)
        );
    }

    #[test]
    fn covered_time_cannot_overflow() {
        let integral = Integral {
            covered: u64::MAX,
            ..Integral::new(0, 100)
        };
        let mut extended = integral;
        extended.extend_to(p(10, 1.0));
        extended.extend_to(p(60, 2.0));
        assert_eq!(extended.covered, u64::MAX);
    }

    #[test]
    fn nan_only_bucket_carries_its_nan_and_has_no_real() {
        let mut bucket = TwaBucket::open(Some(p(0, 1.0)), 0, 10);
        bucket.push(p(3, f64::NAN));
        bucket.push(p(6, f64::NAN));
        assert!(!bucket.has_real());
        let (_, carry) = bucket.close(None);
        assert_eq!(carry, Some(p(6, f64::NAN)));
    }
}
