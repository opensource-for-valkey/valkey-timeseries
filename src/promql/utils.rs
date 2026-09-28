use crate::common::Timestamp;
use crate::promql::time::duration_ms;
use crate::promql::time::{MAX_GRID_STEPS, grid_step_count};
use crate::promql::{EvalResult, EvaluationError, QueryError};
use promql_parser::parser::Expr;
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
    let _step = duration_ms(step);
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
    let step = duration_ms(step);
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
pub(crate) fn check_subquery_steps(
    start: Timestamp,
    end: Timestamp,
    step_ms: i64,
) -> EvalResult<()> {
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

/// The deepest expression tree a query may have: the longest chain of nested
/// nodes, where every operator, function call, aggregation, subquery and
/// parenthesis counts as one.
///
/// The evaluator, the optimizer and the push-down passes all recurse over the
/// tree, on threads with 2 MiB stacks, and a stack overflow aborts the server.
/// Measured on 2026-09-28 (release build, 2 MiB stack), the cheapest overflow
/// was a chain of vector-to-vector operators (`a or a or ...`, `a + a + ...`,
/// `and on(...)`, `group_left`) at about 1,860 nodes: roughly 1.1 KiB of stack
/// per node. Aggregations, function calls and unary minus cost less. 500
/// leaves a margin of about 3.7 for frames on paths the measurement did not
/// reach. Real queries rarely nest past a few dozen, though a generated
/// `a or b or ...` chain counts one per operand.
pub const MAX_QUERY_DEPTH: usize = 500;

/// Rejects an expression nested deeper than [`MAX_QUERY_DEPTH`].
///
/// Walks the tree with an explicit stack rather than recursion: this runs on
/// the thread that parses the command, and a 16 KiB query can nest about
/// 16,000 unary minus signs.
pub(crate) fn check_query_depth(expr: &Expr) -> Result<(), String> {
    let mut pending: Vec<(&Expr, usize)> = vec![(expr, 1)];
    while let Some((expr, depth)) = pending.pop() {
        if depth > MAX_QUERY_DEPTH {
            return Err(format!(
                "TSDB: query is nested too deeply; the limit is {MAX_QUERY_DEPTH} levels"
            ));
        }
        let children = depth + 1;
        match expr {
            Expr::Aggregate(agg) => {
                pending.push((&agg.expr, children));
                if let Some(param) = &agg.param {
                    pending.push((param, children));
                }
            }
            Expr::Unary(unary) => pending.push((&unary.expr, children)),
            Expr::Binary(binary) => {
                pending.push((&binary.lhs, children));
                pending.push((&binary.rhs, children));
            }
            Expr::Paren(paren) => pending.push((&paren.expr, children)),
            Expr::Subquery(subquery) => pending.push((&subquery.expr, children)),
            Expr::Call(call) => pending.extend(call.args.args.iter().map(|arg| (&**arg, children))),
            Expr::Extension(ext) => {
                pending.extend(ext.expr.children().iter().map(|c| (c, children)))
            }
            Expr::NumberLiteral(_)
            | Expr::StringLiteral(_)
            | Expr::VectorSelector(_)
            | Expr::MatrixSelector(_) => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod depth_tests {
    use super::*;

    fn depth_ok(query: &str) -> Result<(), String> {
        check_query_depth(&promql_parser::parser::parse(query).expect("valid query"))
    }

    #[test]
    fn every_node_kind_counts_toward_the_depth() {
        let n = MAX_QUERY_DEPTH;
        // Each shape nests exactly `levels` nodes above the leaf.
        type Shape = (&'static str, fn(usize) -> String);
        let shapes: [Shape; 6] = [
            ("binary chain", |levels| {
                format!("up{}", "+up".repeat(levels))
            }),
            ("unary", |levels| format!("{}up", "-".repeat(levels))),
            ("parens", |levels| {
                format!("{}up{}", "(".repeat(levels), ")".repeat(levels))
            }),
            ("calls", |levels| {
                format!("{}up{}", "abs(".repeat(levels), ")".repeat(levels))
            }),
            ("aggregations", |levels| {
                format!("{}up{}", "sum(".repeat(levels), ")".repeat(levels))
            }),
            ("or chain", |levels| {
                format!("up{}", " or up".repeat(levels))
            }),
        ];
        for (name, build) in shapes {
            assert!(depth_ok(&build(n - 1)).is_ok(), "{name} at the limit");
            let err = depth_ok(&build(n)).expect_err(name);
            assert!(err.contains("nested too deeply"), "{name}: {err}");
        }
    }

    #[test]
    fn subqueries_and_aggregation_params_are_walked() {
        let deep = format!("{}up", "-".repeat(MAX_QUERY_DEPTH));
        assert!(depth_ok(&format!("max_over_time(({deep})[5m:1m])")).is_err());
        assert!(depth_ok(&format!("topk(scalar({deep}), up)")).is_err());
        assert!(depth_ok(&format!("clamp(up, 0, scalar({deep}))")).is_err());
    }

    #[test]
    fn the_walk_does_not_recurse() {
        // Far deeper than any stack-bound recursion would survive on a small
        // stack; the parser itself is not recursive.
        let query = format!("{}up", "-".repeat(16_000));
        let expr = promql_parser::parser::parse(&query).expect("valid query");
        std::thread::Builder::new()
            .stack_size(64 * 1024)
            .spawn(move || {
                assert!(check_query_depth(&expr).is_err());
                std::mem::forget(expr); // dropping it recurses; not what this tests
            })
            .unwrap()
            .join()
            .unwrap();
    }
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
