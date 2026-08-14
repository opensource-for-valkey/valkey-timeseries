use crate::common::Timestamp;
use crate::promql::{EvalResult, EvaluationError, QueryError};
use std::ops::{Bound, RangeBounds};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Convert a `RangeBounds<SystemTime>` into `(start: SystemTime, end: SystemTime)`.
///
/// `Excluded` bounds are adjusted by 1 ms — the smallest sample timestamp
/// granularity — so that `start..end` excludes the exact boundary timestamps.
pub(in crate::promql) fn range_bounds_to_system_time(
    range: impl RangeBounds<SystemTime>,
) -> (SystemTime, SystemTime) {
    let start = match range.start_bound() {
        Bound::Included(t) => *t,
        Bound::Excluded(t) => *t + Duration::from_millis(1),
        Bound::Unbounded => UNIX_EPOCH,
    };
    let end = match range.end_bound() {
        Bound::Included(t) => *t,
        Bound::Excluded(t) => t
            .checked_sub(Duration::from_millis(1))
            .unwrap_or(UNIX_EPOCH),
        Bound::Unbounded => UNIX_EPOCH + Duration::from_secs(i64::MAX as u64),
    };
    (start, end)
}

/// Convert a `RangeBounds<SystemTime>` into `(start_secs, end_secs)` as `i64`.
///
/// Returns an error if either bound resolves to a time before the Unix epoch.
/// Unbounded starts resolve to 0, unbounded ends resolve to `i64::MAX`.
pub(in crate::promql) fn range_bounds_to_secs(
    range: impl RangeBounds<SystemTime>,
) -> Result<(i64, i64), QueryError> {
    let (start, end) = range_bounds_to_system_time(range);
    let start_secs = start
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .map_err(|_| QueryError::InvalidQuery("start time is before Unix epoch".to_string()))?;
    let end_secs = end
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .map_err(|_| QueryError::InvalidQuery("end time is before Unix epoch".to_string()))?;
    Ok((start_secs, end_secs))
}

#[inline]
fn calc_points(start: Timestamp, end: Timestamp, step: &Duration) -> i64 {
    // Saturating: `end` can be the i64::MAX sentinel and `start` any timestamp, so a plain
    // `end - start` would overflow at the extremes — the exact input this guards against.
    end.saturating_sub(start)
        .saturating_div((step.as_millis() + 1) as i64)
}

/// The minimum number of points per timeseries for enabling time rounding.
/// This improves the cache hit ratio for frequently requested queries over
/// big time ranges.
const MIN_TIMESERIES_POINTS_FOR_TIME_ROUNDING: i64 = 50;

pub(in crate::promql) fn adjust_start_end(
    start: Timestamp,
    end: Timestamp,
    step: Duration,
) -> (Timestamp, Timestamp) {
    // if disableCache {
    //     // do not adjust start and end values when cache is disabled.
    //     // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/563
    //     return (start, end);
    // }
    let points = calc_points(start, end, &step);
    if points < MIN_TIMESERIES_POINTS_FOR_TIME_ROUNDING {
        // Too small a number of points for rounding.
        return (start, end);
    }

    // Round start and end to values divisible by step
    // to enable response caching (see EvalConfig.mayCache).
    let (start, end) = align_start_end(start, end, &step);

    // Make sure that the new number of points is the same as the initial number of points.
    let mut new_points = calc_points(start, end, &step);
    let mut _end = end;
    let _step = step.as_millis() as i64;
    while new_points > points {
        _end = end.saturating_sub(_step);
        new_points -= 1;
    }

    (start, _end)
}

pub(in crate::promql) fn align_start_end(
    start: Timestamp,
    end: Timestamp,
    step: &Duration,
) -> (Timestamp, Timestamp) {
    let step = step.as_millis() as i64;
    // Round start to the nearest smaller value divisible by step.
    let new_start = start - start % step;
    // Round end to the nearest bigger value divisible by step.
    let adjust = end % step;
    let mut new_end = end;
    if adjust > 0 {
        new_end += step - adjust
    }
    (new_start, new_end)
}

/// Checks the maximum number of points that may be returned per each time series.
///
/// The number mustn't exceed `max_points_per_timeseries`.
pub(crate) fn validate_max_points_per_timeseries(
    start: Timestamp,
    end: Timestamp,
    step: Duration,
    max_points_per_timeseries: usize,
) -> EvalResult<()> {
    let points = calc_points(start, end, &step);
    if (max_points_per_timeseries > 0) && points > max_points_per_timeseries as i64 {
        let msg = format!(
            "too many points for the given step={:?}, start={start} and end={end}: {points}; cannot exceed {}",
            step, max_points_per_timeseries
        );
        // A request-level rejection (the window is too wide for the step), not a server
        // fault: classify it so the surfaced error reads as a bad argument.
        Err(EvaluationError::ArgumentError(msg))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod max_points_tests {
    use super::*;
    use crate::common::constants::MAX_TIMESTAMP;

    #[test]
    fn zero_limit_is_unlimited() {
        // The default configuration (0). Even the pathological i64::MAX window must pass,
        // so an unconfigured server is never made to reject queries by this guard.
        assert!(
            validate_max_points_per_timeseries(0, MAX_TIMESTAMP, Duration::from_millis(1), 0)
                .is_ok()
        );
    }

    #[test]
    fn unbounded_end_sentinel_is_rejected_when_limited() {
        // `END +` resolves to i64::MAX; with a finite limit this is the abort case the guard
        // exists to stop. It must be rejected rather than allowed to size a preload buffer.
        let err =
            validate_max_points_per_timeseries(0, MAX_TIMESTAMP, Duration::from_millis(1), 100_000)
                .expect_err("an i64::MAX window must exceed a finite limit");
        assert!(matches!(err, EvaluationError::ArgumentError(_)));
    }

    #[test]
    fn within_limit_passes() {
        // 1000 steps of 1ms, limit 100000: comfortably under, must be accepted.
        assert!(
            validate_max_points_per_timeseries(0, 1000, Duration::from_millis(1), 100_000).is_ok()
        );
    }

    #[test]
    fn one_past_limit_is_rejected() {
        // step+1 divisor: a 1ms step counts points as span/2. A 2*(limit)+2 ms span yields
        // limit+1 points — the first value that must trip the ceiling.
        let limit = 10usize;
        let span = 2 * (limit as i64 + 1);
        assert!(
            validate_max_points_per_timeseries(0, span, Duration::from_millis(1), limit).is_err()
        );
    }

    #[test]
    fn extreme_span_does_not_overflow() {
        // start below zero with the max end must not panic/overflow in calc_points; it just
        // yields a huge point count that the limit rejects.
        let err = validate_max_points_per_timeseries(
            i64::MIN,
            MAX_TIMESTAMP,
            Duration::from_millis(1),
            100_000,
        )
        .expect_err("saturated span still exceeds the limit");
        assert!(matches!(err, EvaluationError::ArgumentError(_)));
    }
}
