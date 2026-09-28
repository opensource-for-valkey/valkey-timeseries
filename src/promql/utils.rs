use crate::common::Timestamp;
use crate::promql::time::{MAX_GRID_STEPS, grid_step_count};
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
    if end < start {
        return 0;
    }

    // A range grid includes both bounds: 0..=1000 at a 1ms step has 1,001
    // points. Saturate the conversion and arithmetic because an unbounded end
    // is represented by i64::MAX.
    let step_ms = i64::try_from(step.as_millis()).unwrap_or(i64::MAX).max(1);
    end.saturating_sub(start)
        .saturating_div(step_ms)
        .saturating_add(1)
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
/// The number mustn't exceed `max_points_per_timeseries`, or [`MAX_GRID_STEPS`]
/// when that is 0 (no configured limit) or larger.
pub(crate) fn validate_max_points_per_timeseries(
    start: Timestamp,
    end: Timestamp,
    step: Duration,
    max_points_per_timeseries: usize,
) -> EvalResult<()> {
    let points = calc_points(start, end, &step);
    let limit = match max_points_per_timeseries as u64 {
        0 => MAX_GRID_STEPS,
        configured => configured.min(MAX_GRID_STEPS),
    };
    if points as u64 > limit {
        let msg = format!(
            "too many points for the given step={:?}, start={start} and end={end}: {points}; cannot exceed {limit}",
            step
        );
        // A request-level rejection (the window is too wide for the step), not a server
        // fault: classify it so the surfaced error reads as a bad argument.
        Err(EvaluationError::ArgumentError(msg))
    } else {
        Ok(())
    }
}

/// Rejects a subquery whose own grid, `start..=end` at `step_ms`, has more
/// than [`MAX_GRID_STEPS`] steps. No configured limit covers subqueries, so
/// this is their only bound: `m[100y:1ms]` would otherwise be walked (and its
/// window ends collected) three trillion steps per series.
pub(crate) fn check_subquery_steps(start: Timestamp, end: Timestamp, step_ms: i64) -> EvalResult<()> {
    let steps = grid_step_count(start, end, step_ms);
    if steps > MAX_GRID_STEPS {
        return Err(EvaluationError::ArgumentError(format!(
            "subquery has too many steps: {steps} for a {}ms range at a {step_ms}ms step; \
             cannot exceed {MAX_GRID_STEPS}",
            i128::from(end) - i128::from(start)
        )));
    }
    Ok(())
}

#[cfg(test)]
mod max_points_tests {
    use super::*;
    use crate::common::constants::MAX_TIMESTAMP;

    #[test]
    fn zero_limit_falls_back_to_the_grid_ceiling() {
        // The default configuration (0) sets no limit of its own, but the
        // i64::MAX window (`END +` at a 1ms step) must still be rejected: it
        // used to be walked until the allocation aborted the server.
        let err = validate_max_points_per_timeseries(0, MAX_TIMESTAMP, Duration::from_millis(1), 0)
            .expect_err("an i64::MAX window must exceed the grid ceiling");
        assert!(matches!(err, EvaluationError::ArgumentError(_)));

        let at_ceiling = MAX_GRID_STEPS as i64 - 1;
        assert!(
            validate_max_points_per_timeseries(0, at_ceiling, Duration::from_millis(1), 0).is_ok()
        );
        assert!(
            validate_max_points_per_timeseries(0, at_ceiling + 1, Duration::from_millis(1), 0)
                .is_err()
        );
    }

    #[test]
    fn configured_limit_above_the_ceiling_is_capped() {
        let over = MAX_GRID_STEPS as i64;
        assert!(
            validate_max_points_per_timeseries(
                0,
                over,
                Duration::from_millis(1),
                2 * MAX_GRID_STEPS as usize
            )
            .is_err()
        );
    }

    #[test]
    fn subquery_steps_are_bounded() {
        // `m[100y:1ms]`: about 3.15e12 steps.
        let hundred_years_ms = 100 * 365 * 24 * 3_600_000_i64;
        let err = check_subquery_steps(-hundred_years_ms, 0, 1).expect_err("too many steps");
        assert!(matches!(err, EvaluationError::ArgumentError(_)));
        // A 30-day subquery at a 1-minute step is ordinary.
        assert!(check_subquery_steps(0, 30 * 24 * 3_600_000, 60_000).is_ok());
        // Extreme bounds must not overflow while counting.
        assert!(check_subquery_steps(i64::MIN, i64::MAX, 1).is_err());
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
        // 0..=999 at a 1ms step has exactly 1,000 points.
        assert!(
            validate_max_points_per_timeseries(0, 999, Duration::from_millis(1), 1_000).is_ok()
        );
    }

    #[test]
    fn one_past_limit_is_rejected() {
        // The inclusive end contributes one point, so 0..=10 has 11 points.
        let limit = 10usize;
        assert!(
            validate_max_points_per_timeseries(0, limit as i64, Duration::from_millis(1), limit)
                .is_err()
        );
    }

    #[test]
    fn inclusive_grid_count_rejects_ranges_that_exceed_the_limit() {
        // This used to count only 500 points by dividing by step + 1.
        assert!(
            validate_max_points_per_timeseries(0, 1_000, Duration::from_millis(1), 600).is_err()
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
