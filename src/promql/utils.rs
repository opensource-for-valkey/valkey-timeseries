use crate::common::Timestamp;
use crate::promql::time::{MAX_GRID_STEPS, duration_ms, grid_step_count};
use crate::promql::{EvalResult, EvaluationError};
use promql_parser::parser::{Expr, SubqueryExpr};
use std::time::Duration;
#[cfg(test)]
use std::{
    ops::{Bound, RangeBounds},
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(test)]
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

/// The step a subquery runs at, per the PromQL spec: its own `<resolution>`,
/// else the global evaluation interval — Prometheus' default of one minute.
///
/// Never the step of the query it sits in: that would make `m[5m:]` sample
/// every 15s inside a `step=15s` range query but every minute in an instant
/// query at the same timestamp, so `count_over_time(m[5m:])` would answer 20
/// in one and 5 in the other.
/// See: <https://prometheus.io/docs/prometheus/latest/querying/basics/#subquery>
/// and `DefaultGlobalConfig.EvaluationInterval` in prometheus/config/config.go.
pub(crate) fn subquery_step_ms(subquery: &SubqueryExpr) -> i64 {
    const DEFAULT_EVALUATION_INTERVAL_MS: i64 = 60_000;
    subquery
        .step
        .map_or(DEFAULT_EVALUATION_INTERVAL_MS, duration_ms)
}

/// The most subquery steps one query may evaluate, summed over every subquery
/// and multiplied through the ones that enclose it and the query's own steps.
///
/// [`check_subquery_steps`] bounds each subquery grid alone, but nesting
/// multiplies them: every step of an outer subquery evaluates the whole inner
/// one, so `max_over_time((...)[1m:10s])` nested 10 deep is up to 7^10 ≈ 280
/// million steps. Measured 2026-09-29 (debug build, `vector(1)` leaf) it ran for
/// 55 s. The subquery step loop now checks the deadline, but a query like that
/// still holds a query worker and the evaluation pool for the whole
/// `ts-promql-max-query-duration`; this rejects it before any work. A hundred
/// million leaves room for real queries: a 1,000-point range query over a 7-day
/// subquery at a 1-minute step is about 10 million.
pub(crate) const MAX_SUBQUERY_STEP_EVALUATIONS: u64 = 100 * MAX_GRID_STEPS;

/// Rejects a query whose subqueries would evaluate more than
/// [`MAX_SUBQUERY_STEP_EVALUATIONS`] steps in total, when the query itself is
/// evaluated at `outer_steps` points (1 for an instant query).
///
/// Counted from the query text alone, before anything is read: a subquery's
/// range and step are literals, and its grid has at most `range / step + 1`
/// steps wherever it is aligned.
pub(crate) fn check_subquery_cost(expr: &Expr, outer_steps: u64) -> EvalResult<()> {
    let mut total: u64 = 0;
    let mut pending: Vec<(&Expr, u64)> = vec![(expr, outer_steps.max(1))];
    while let Some((expr, evaluations)) = pending.pop() {
        let mut child_evaluations = evaluations;
        if let Expr::Subquery(subquery) = expr {
            let (range_ms, step_ms) = (duration_ms(subquery.range), subquery_step_ms(subquery));
            // A single grid over its own limit gets that error, which names the grid.
            check_subquery_steps(0, range_ms, step_ms)?;
            child_evaluations = evaluations.saturating_mul(grid_step_count(0, range_ms, step_ms));
            total = total.saturating_add(child_evaluations);
            if total > MAX_SUBQUERY_STEP_EVALUATIONS {
                return Err(EvaluationError::ArgumentError(format!(
                    "subqueries would evaluate more than {MAX_SUBQUERY_STEP_EVALUATIONS} steps; \
                     nested subqueries multiply their steps, so reduce the nesting, \
                     a subquery's range, or use a coarser subquery step"
                )));
            }
        }
        for_each_child(expr, |child| pending.push((child, child_evaluations)));
    }
    Ok(())
}

/// Calls `f` with each direct child of `expr`.
fn for_each_child<'a>(expr: &'a Expr, mut f: impl FnMut(&'a Expr)) {
    match expr {
        Expr::Aggregate(agg) => {
            f(&agg.expr);
            if let Some(param) = &agg.param {
                f(param);
            }
        }
        Expr::Unary(unary) => f(&unary.expr),
        Expr::Binary(binary) => {
            f(&binary.lhs);
            f(&binary.rhs);
        }
        Expr::Paren(paren) => f(&paren.expr),
        Expr::Subquery(subquery) => f(&subquery.expr),
        Expr::Call(call) => call.args.args.iter().for_each(|arg| f(arg)),
        Expr::Extension(ext) => ext.expr.children().iter().for_each(f),
        Expr::NumberLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::VectorSelector(_)
        | Expr::MatrixSelector(_) => {}
    }
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
        for_each_child(expr, |child| pending.push((child, depth + 1)));
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

#[cfg(test)]
mod subquery_cost_tests {
    use super::*;

    fn cost(query: &str, outer_steps: u64) -> EvalResult<()> {
        check_subquery_cost(
            &promql_parser::parser::parse(query).expect("valid query"),
            outer_steps,
        )
    }

    /// `m` wrapped in `depth` subqueries of seven steps each (`[1m:10s]`).
    fn nested(depth: u32) -> String {
        (0..depth).fold("m".to_string(), |expr, _| {
            format!("max_over_time(({expr})[1m:10s])")
        })
    }

    #[test]
    fn nesting_multiplies_the_steps() {
        // 7^9 ≈ 40 million is under the cap, 7^10 ≈ 282 million over it.
        assert!(7u64.pow(9) + 7u64.pow(8) < MAX_SUBQUERY_STEP_EVALUATIONS);
        cost(&nested(9), 1).unwrap();
        cost(&nested(10), 1).unwrap_err();
    }

    #[test]
    fn a_range_query_multiplies_by_its_own_steps() {
        // Every outer step evaluates the subquery tree in full: 3 × 7^9 ≈ 121 million.
        cost(&nested(9), 2).unwrap();
        cost(&nested(9), 3).unwrap_err();
    }

    #[test]
    fn sibling_subqueries_add_up() {
        // A 10-day subquery at 1 s is 864,001 steps: 115 of them fit, 116 do not.
        let one = "max_over_time(m[10d:1s])";
        let siblings = |n: usize| vec![one; n].join(" + ");
        cost(&siblings(115), 1).unwrap();
        let err = cost(&siblings(116), 1).unwrap_err();
        assert!(
            err.to_string().contains("subqueries would evaluate"),
            "{err}"
        );
    }

    #[test]
    fn one_oversized_grid_reports_its_own_limit() {
        let err = cost("max_over_time(m[30d:1s])", 1).unwrap_err();
        assert!(
            err.to_string().contains("subquery has too many steps"),
            "{err}"
        );
    }

    #[test]
    fn a_subquery_without_a_resolution_steps_every_minute() {
        // `[1y:]` is 525,601 one-minute steps: 190 outer steps fit, 191 do not.
        cost("max_over_time(m[1y:])", 190).unwrap();
        cost("max_over_time(m[1y:])", 191).unwrap_err();
    }

    #[test]
    fn subqueries_are_found_under_every_node_kind() {
        let deep = nested(10);
        for query in [
            format!("sum({deep})"),
            format!("-{deep}"),
            format!("1 + {deep}"),
            format!("({deep})"),
            format!("abs({deep})"),
            format!("topk(1, {deep})"),
        ] {
            cost(&query, 1).expect_err(&query);
        }
    }

    #[test]
    fn a_query_without_subqueries_costs_nothing() {
        cost("rate(m[5m]) + sum(m)", MAX_GRID_STEPS).unwrap();
    }
}
