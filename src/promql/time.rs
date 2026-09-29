use crate::common::time::system_time_to_millis;
use promql_parser::parser::{AtModifier, Offset};
use std::time::Duration;

/// Compute the effective evaluation-time range for a selector after applying
/// @ and offset modifiers, then return (earliest_ms, latest_ms) after
/// subtracting the backward window (lookback or matrix range).
///
/// `at_start_ms`/`at_end_ms` are the values that `@ start()` and `@ end()`
/// resolve to. `eval_start_ms`/`eval_end_ms` are the effective evaluation-time
/// range for selectors without `@`.
///
/// At the top level both pairs are identical (the query range). Inside
/// subqueries they diverge: `at_start_ms`/`at_end_ms` remain the outer query
/// bounds (since `evaluate_subquery` passes `query_start`/`query_end` through)
/// while `eval_start_ms`/`eval_end_ms` become the subquery step window.
pub(super) fn selector_bounds(
    at: Option<&AtModifier>,
    offset: Option<&Offset>,
    at_start_ms: i64,
    at_end_ms: i64,
    eval_start_ms: i64,
    eval_end_ms: i64,
    backward_window_ms: i64,
) -> (i64, i64) {
    // Step 1: Determine the evaluation time range.
    //
    // `@ <timestamp>` pins evaluation to a fixed instant (single point).
    //
    // `@ start()` / `@ end()`: in query_range(), each step creates an
    // instant_stmt with start=end=current_time, so both @ start() and
    // @ end() resolve to current_time, sweeping [range_start, range_end].
    // For preload purposes we must cover the full eval range for both.
    // Inside subqueries, evaluate_subquery passes the outer query bounds
    // through unchanged, so @ start()/@ end() resolve to constants —
    // but we still use (at_start_ms, at_end_ms) which correctly narrows
    // to a single point when those are equal (instant query or inner
    // subquery context).
    let (mut start, mut end) = if let Some(at_mod) = at {
        match at_mod {
            AtModifier::At(time) => {
                let t = system_time_to_millis(*time);
                (t, t)
            }
            // Both @ start() and @ end() sweep the full at-modifier range.
            // At the top level at_start == eval_start, and at_end == eval_end
            // (the query range). Inside subqueries at_start/at_end are the
            // outer query bounds passed through by evaluate_subquery.
            AtModifier::Start | AtModifier::End => (at_start_ms, at_end_ms),
        }
    } else {
        (eval_start_ms, eval_end_ms)
    };

    // Step 2: Apply offset
    if let Some(off) = offset {
        match off {
            Offset::Pos(d) => {
                let off_ms = duration_ms(d);
                start = start.saturating_sub(off_ms);
                end = end.saturating_sub(off_ms);
            }
            Offset::Neg(d) => {
                let off_ms = duration_ms(d);
                start = start.saturating_add(off_ms);
                end = end.saturating_add(off_ms);
            }
        }
    }

    // Step 3: Subtract backward window from start
    let earliest = start.saturating_sub(backward_window_ms);
    (earliest, end)
}

/// Single source of truth for offset / @ time-modifier arithmetic (in milliseconds).
/// Both evaluate_vector_selector (per-step) and preload_vector_selector call this.
pub(in crate::promql) fn apply_time_modifiers_ms(
    at: Option<&AtModifier>,
    offset: Option<&Offset>,
    query_start_ms: i64,
    query_end_ms: i64,
    evaluation_ts_ms: i64,
) -> i64 {
    let mut adjusted = if let Some(at_modifier) = at {
        match at_modifier {
            AtModifier::At(timestamp) => system_time_to_millis(*timestamp),
            AtModifier::Start => query_start_ms,
            AtModifier::End => query_end_ms,
        }
    } else {
        evaluation_ts_ms
    };

    if let Some(offset) = offset {
        adjusted = match offset {
            Offset::Pos(duration) => adjusted.saturating_sub(duration_ms(duration)),
            Offset::Neg(duration) => adjusted.saturating_add(duration_ms(duration)),
        };
    }

    adjusted
}

/// `d` in whole milliseconds, saturating at `i64::MAX`.
///
/// Timestamps are `i64` milliseconds, so every duration is added to or subtracted from one;
/// `as_millis() as i64` truncates a duration past `i64::MAX` ms (about 292 million years, which
/// promql-parser accepts) to a meaningless value instead. Pair it with saturating arithmetic.
pub(crate) fn duration_ms(d: impl std::borrow::Borrow<Duration>) -> i64 {
    i64::try_from(d.borrow().as_millis()).unwrap_or(i64::MAX)
}

/// The most steps one evaluation grid may have, whatever the configuration.
///
/// Applies to a range query's outer grid, to every subquery grid, and to a grid
/// a peer asks this node to evaluate. Every grid is walked step by step and its
/// window ends are materialized, so without a ceiling `END +` (`i64::MAX`) or
/// `m[100y:1ms]` would allocate until the process aborts.
/// `ts-promql-max-points-per-timeseries` can only lower the outer grid's limit.
/// One million leaves room for real queries: a 30-day subquery at a 1-minute
/// step is 43,200 steps.
pub const MAX_GRID_STEPS: u64 = 1_000_000;

/// How many timestamps [`step_times`] yields for `start..=end` at `step`,
/// computed without overflow. 0 when `step <= 0` or `end < start`.
pub fn grid_step_count(start: i64, end: i64, step: i64) -> u64 {
    if step <= 0 || end < start {
        return 0;
    }
    let steps = (i128::from(end) - i128::from(start)) / i128::from(step) + 1;
    u64::try_from(steps).unwrap_or(u64::MAX)
}

/// `start, start + step, ...` up to and including `end`.
///
/// Stops when the next step would overflow, so an `end` of `i64::MAX` still
/// ends. A `step <= 0` yields at most `start`: a zero step would otherwise
/// repeat it forever.
pub fn step_times(start: i64, end: i64, step: i64) -> impl Iterator<Item = i64> {
    let mut next = (start <= end).then_some(start);
    std::iter::from_fn(move || {
        let current = next?;
        next = if step > 0 {
            current.checked_add(step).filter(|&ts| ts <= end)
        } else {
            None
        };
        Some(current)
    })
}

#[cfg(test)]
mod tests {
    use crate::promql::time::{grid_step_count, step_times};

    #[test]
    fn step_times_ends_at_the_largest_timestamp() {
        // `END +` is i64::MAX. The last step used to saturate at i64::MAX and
        // repeat forever, since `current > end` could never become true.
        let tail: Vec<i64> = step_times(i64::MAX - 25, i64::MAX, 10).collect();
        assert_eq!(tail, vec![i64::MAX - 25, i64::MAX - 15, i64::MAX - 5]);
        let exact: Vec<i64> = step_times(i64::MAX - 10, i64::MAX, 10).collect();
        assert_eq!(exact, vec![i64::MAX - 10, i64::MAX]);
    }

    #[test]
    fn step_times_non_positive_step_yields_at_most_start() {
        assert_eq!(step_times(5, 10, 0).collect::<Vec<_>>(), vec![5]);
        assert_eq!(step_times(5, 10, -1).collect::<Vec<_>>(), vec![5]);
        assert_eq!(step_times(11, 10, 0).count(), 0);
    }

    #[test]
    fn step_times_matches_grid_step_count() {
        for (start, end, step) in [
            (0, 0, 1),
            (0, 999, 1),
            (0, 1000, 7),
            (-41, 59, 10),
            (3, 2, 1),
        ] {
            assert_eq!(
                step_times(start, end, step).count() as u64,
                grid_step_count(start, end, step),
                "{start}..={end} step {step}"
            );
        }
    }

    #[test]
    fn grid_step_count_does_not_overflow() {
        assert_eq!(grid_step_count(i64::MIN, i64::MAX, 1), u64::MAX);
        assert_eq!(grid_step_count(0, i64::MAX, 1), i64::MAX as u64 + 1);
        assert_eq!(grid_step_count(0, 10, 0), 0);
        assert_eq!(grid_step_count(10, 0, 1), 0);
    }

    // `selector_bounds` cases, in milliseconds. `at` is what `@ start()` /
    // `@ end()` resolve to, `eval` the evaluation range without `@`.
    mod selector_bounds_tests {
        use crate::promql::time::selector_bounds;
        use promql_parser::parser::{Expr, parse};

        const LOOKBACK: i64 = 300_000;

        fn bounds(query: &str, at: (i64, i64), eval: (i64, i64), window: i64) -> (i64, i64) {
            let expr = parse(query).unwrap();
            let vs = match &expr {
                Expr::VectorSelector(vs) => vs,
                Expr::MatrixSelector(ms) => &ms.vs,
                other => panic!("not a selector: {other:?}"),
            };
            selector_bounds(
                vs.at.as_ref(),
                vs.offset.as_ref(),
                at.0,
                at.1,
                eval.0,
                eval.1,
                window,
            )
        }

        fn instant(query: &str, t: i64, window: i64) -> (i64, i64) {
            bounds(query, (t, t), (t, t), window)
        }

        #[test]
        fn no_modifiers_subtracts_the_window() {
            assert_eq!(instant("m", 1_000_000, LOOKBACK), (700_000, 1_000_000));
        }

        #[test]
        fn offset_shifts_back_and_negative_offset_forward() {
            assert_eq!(
                instant("m offset 1h", 7_200_000, LOOKBACK),
                (3_300_000, 3_600_000)
            );
            assert_eq!(
                instant("m offset -5m", 1_000_000, LOOKBACK),
                (1_000_000, 1_300_000)
            );
        }

        #[test]
        fn absolute_at_pins_the_time_and_offset_applies_after() {
            assert_eq!(instant("m @ 500", 2_000_000, LOOKBACK), (200_000, 500_000));
            assert_eq!(
                instant("m @ 500 offset 5m", 2_000_000, LOOKBACK),
                (-100_000, 200_000)
            );
        }

        #[test]
        fn matrix_range_is_the_window() {
            assert_eq!(
                instant("m[5m] offset 1h", 7_200_000, 300_000),
                (3_300_000, 3_600_000)
            );
        }

        #[test]
        fn range_query_covers_every_step() {
            let range = (1_000_000, 5_000_000);
            assert_eq!(bounds("m", range, range, LOOKBACK), (700_000, 5_000_000));
            assert_eq!(
                bounds(
                    "m offset 1h",
                    (3_600_000, 7_200_000),
                    (3_600_000, 7_200_000),
                    LOOKBACK
                ),
                (-300_000, 3_600_000)
            );
            // Each step of a range query resolves `@ start()` / `@ end()` to its
            // own time, so together they sweep the whole range.
            assert_eq!(
                bounds("m @ end()", range, range, LOOKBACK),
                (700_000, 5_000_000)
            );
            assert_eq!(
                bounds("m @ start()", range, range, LOOKBACK),
                (700_000, 5_000_000)
            );
        }

        #[test]
        fn instant_query_at_end_is_a_single_point() {
            assert_eq!(
                instant("m @ end()", 2_000_000, LOOKBACK),
                (1_700_000, 2_000_000)
            );
        }

        #[test]
        fn inside_a_subquery_at_end_resolves_to_the_outer_query_bounds() {
            // `m @ end()[1h:5m] offset 30m` at 7200 s: the subquery window is
            // [1800 s, 5400 s], but `@ end()` still means the outer query end.
            assert_eq!(
                bounds(
                    "m @ end()",
                    (7_200_000, 7_200_000),
                    (1_800_000, 5_400_000),
                    LOOKBACK
                ),
                (6_900_000, 7_200_000)
            );
        }
    }
}
