use crate::common::time::system_time_to_millis;
use promql_parser::parser::{AtModifier, Expr, Offset};
use std::time::{Duration, SystemTime};

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
                let off_ms = d.as_millis() as i64;
                start = start.saturating_sub(off_ms);
                end = end.saturating_sub(off_ms);
            }
            Offset::Neg(d) => {
                let off_ms = d.as_millis() as i64;
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
            Offset::Pos(duration) => adjusted - (duration.as_millis() as i64),
            Offset::Neg(duration) => adjusted + (duration.as_millis() as i64),
        };
    }

    adjusted
}

/// Recursive inner function operating in milliseconds.
/// Appends per-selector `(earliest_ms, latest_ms)` ranges to `out`.
///
/// `at_start_ms`/`at_end_ms` are what `@ start()` / `@ end()` resolve to.
/// `eval_start_ms`/`eval_end_ms` are the effective evaluation-time range
/// for selectors without `@`.
fn preload_ranges_inner(
    expr: &Expr,
    at_start_ms: i64,
    at_end_ms: i64,
    eval_start_ms: i64,
    eval_end_ms: i64,
    lookback_ms: i64,
    out: &mut Vec<(i64, i64)>,
) {
    match expr {
        Expr::VectorSelector(vs) => {
            out.push(selector_bounds(
                vs.at.as_ref(),
                vs.offset.as_ref(),
                at_start_ms,
                at_end_ms,
                eval_start_ms,
                eval_end_ms,
                lookback_ms,
            ));
        }
        Expr::MatrixSelector(ms) => {
            let range_ms = ms.range.as_millis() as i64;
            out.push(selector_bounds(
                ms.vs.at.as_ref(),
                ms.vs.offset.as_ref(),
                at_start_ms,
                at_end_ms,
                eval_start_ms,
                eval_end_ms,
                range_ms,
            ));
        }
        Expr::Subquery(sq) => {
            // Compute the subquery's own adjusted eval-time window.
            let (sq_start, sq_end) = selector_bounds(
                sq.at.as_ref(),
                sq.offset.as_ref(),
                at_start_ms,
                at_end_ms,
                eval_start_ms,
                eval_end_ms,
                0,
            );
            let range_ms = sq.range.as_millis() as i64;
            let inner_eval_start = sq_start.saturating_sub(range_ms);
            // Recurse: evaluate_subquery passes the original query_start/query_end
            // through, so @ start()/@ end() inside the subquery resolve to the
            // outer query bounds. The eval-time range narrows to the subquery
            // step window.
            preload_ranges_inner(
                &sq.expr,
                at_start_ms,
                at_end_ms,
                inner_eval_start,
                sq_end,
                lookback_ms,
                out,
            );
        }
        Expr::Aggregate(agg) => {
            preload_ranges_inner(
                &agg.expr,
                at_start_ms,
                at_end_ms,
                eval_start_ms,
                eval_end_ms,
                lookback_ms,
                out,
            );
            if let Some(ref param) = agg.param {
                preload_ranges_inner(
                    param,
                    at_start_ms,
                    at_end_ms,
                    eval_start_ms,
                    eval_end_ms,
                    lookback_ms,
                    out,
                );
            }
        }
        Expr::Binary(b) => {
            preload_ranges_inner(
                &b.lhs,
                at_start_ms,
                at_end_ms,
                eval_start_ms,
                eval_end_ms,
                lookback_ms,
                out,
            );
            preload_ranges_inner(
                &b.rhs,
                at_start_ms,
                at_end_ms,
                eval_start_ms,
                eval_end_ms,
                lookback_ms,
                out,
            );
        }
        Expr::Paren(p) => preload_ranges_inner(
            &p.expr,
            at_start_ms,
            at_end_ms,
            eval_start_ms,
            eval_end_ms,
            lookback_ms,
            out,
        ),
        Expr::Call(call) => {
            for arg in &call.args.args {
                preload_ranges_inner(
                    arg,
                    at_start_ms,
                    at_end_ms,
                    eval_start_ms,
                    eval_end_ms,
                    lookback_ms,
                    out,
                );
            }
        }
        Expr::Unary(u) => preload_ranges_inner(
            &u.expr,
            at_start_ms,
            at_end_ms,
            eval_start_ms,
            eval_end_ms,
            lookback_ms,
            out,
        ),
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) | Expr::Extension(_) => {}
    }
}

/// Walk the AST and compute the disjoint time ranges needed for bucket preloading.
///
/// For each selector, compute the effective evaluation time by applying
/// @ and offset modifiers, then expand by lookback_delta (vector) or
/// range (matrix). Returns a sorted, non-overlapping list of
/// `(earliest_secs, latest_secs)` ranges covering all selectors.
///
/// Returns an empty Vec when the expression contains no selectors
/// (e.g. `1 + 2`), allowing the caller to fall back to the default window.
pub(in crate::promql) fn compute_preload_ranges(
    expr: &Expr,
    query_start: SystemTime,
    query_end: SystemTime,
    lookback_delta: Duration,
) -> Vec<(i64, i64)> {
    let start_ms = system_time_to_millis(query_start);
    let end_ms = system_time_to_millis(query_end);
    let lookback_ms = lookback_delta.as_millis() as i64;
    // At the top level, eval range == query range
    let mut ranges = Vec::new();
    preload_ranges_inner(
        expr,
        start_ms,
        end_ms,
        start_ms,
        end_ms,
        lookback_ms,
        &mut ranges,
    );
    // Convert ms to seconds (floor for start, ceil for end), then normalize
    let ranges_secs: Vec<(i64, i64)> = ranges
        .into_iter()
        .map(|(lo, hi)| {
            let start_secs = lo.div_euclid(1000);
            let end_secs = hi.div_euclid(1000) + i64::from(hi.rem_euclid(1000) != 0);
            (start_secs, end_secs)
        })
        .collect();
    normalize_ranges(ranges_secs)
}

/// Sort ranges by start and merge overlapping ones.
/// Adjacent-but-not-overlapping ranges are kept separate since they may map
/// to different buckets.
pub(crate) fn normalize_ranges(mut ranges: Vec<(i64, i64)>) -> Vec<(i64, i64)> {
    if ranges.is_empty() {
        return ranges;
    }
    ranges.sort_by_key(|&(start, _)| start);
    let mut merged = Vec::with_capacity(ranges.len());
    let (mut cur_start, mut cur_end) = ranges[0];
    for &(start, end) in &ranges[1..] {
        if start <= cur_end {
            // Overlapping — extend
            cur_end = cur_end.max(end);
        } else {
            merged.push((cur_start, cur_end));
            cur_start = start;
            cur_end = end;
        }
    }
    merged.push((cur_start, cur_end));
    merged
}

pub fn step_times(start: i64, end: i64, step: i64) -> impl Iterator<Item=i64> {
    let mut current = start;
    std::iter::from_fn(move || {
        if current > end {
            return None;
        }
        let out = current;
        current = current.saturating_add(step);
        Some(out)
    })
}

#[cfg(test)]
mod tests {
    use crate::promql::time::normalize_ranges;

    #[test]
    fn normalize_ranges_basic() {
        // Already sorted, overlapping
        assert_eq!(normalize_ranges(vec![(0, 5), (3, 10)]), vec![(0, 10)]);
        // Disjoint
        assert_eq!(
            normalize_ranges(vec![(0, 5), (10, 15)]),
            vec![(0, 5), (10, 15)]
        );
        // Unsorted
        assert_eq!(
            normalize_ranges(vec![(10, 15), (0, 5)]),
            vec![(0, 5), (10, 15)]
        );
        // Negative start is preserved (pre-epoch timestamps are valid)
        assert_eq!(normalize_ranges(vec![(-5, 10)]), vec![(-5, 10)]);
        // Empty
        assert_eq!(normalize_ranges(vec![]), vec![]);
    }

    // ── compute_preload_ranges tests ──────────────────────────────────

    mod preload_ranges_tests {
        use crate::promql::time::compute_preload_ranges;
        use std::time::{Duration, SystemTime, UNIX_EPOCH};

        fn t(secs: u64) -> SystemTime {
            UNIX_EPOCH + Duration::from_secs(secs)
        }

        #[test]
        fn scalar_only_returns_empty() {
            let expr = promql_parser::parser::parse("1 + 2").unwrap();
            let result = compute_preload_ranges(&expr, t(1000), t(1000), Duration::from_secs(300));
            assert_eq!(result, vec![]);
        }

        #[test]
        fn simple_selector_no_modifiers() {
            // query_time=1000s, lookback=300s
            // eval_time=1000s, data window: [1000-300, 1000] = [700, 1000]
            let expr = promql_parser::parser::parse("metric_name").unwrap();
            let result = compute_preload_ranges(&expr, t(1000), t(1000), Duration::from_secs(300));
            assert_eq!(result, vec![(700, 1000)]);
        }

        #[test]
        fn offset_shifts_start_back() {
            // query_time=7200s, lookback=300s, offset=1h=3600s
            // eval_time = 7200 - 3600 = 3600
            // data window: [3600 - 300, 3600] = [3300, 3600]
            let expr = promql_parser::parser::parse("metric_name offset 1h").unwrap();
            let result = compute_preload_ranges(&expr, t(7200), t(7200), Duration::from_secs(300));
            assert_eq!(result, vec![(3300, 3600)]);
        }

        #[test]
        fn at_modifier_absolute_time() {
            // query_time=2000s, lookback=300s, @500
            // eval_time = 500, data window: [500 - 300, 500] = [200, 500]
            let expr = promql_parser::parser::parse("metric_name @ 500").unwrap();
            let result = compute_preload_ranges(&expr, t(2000), t(2000), Duration::from_secs(300));
            assert_eq!(result, vec![(200, 500)]);
        }

        #[test]
        fn at_with_offset() {
            // query_time=2000s, lookback=300s, @500 offset 5m
            // eval_time = 500 - 300 = 200, data window: [200 - 300, 200] = [-100, 200]
            let expr = promql_parser::parser::parse("metric_name @ 500 offset 5m").unwrap();
            let result = compute_preload_ranges(&expr, t(2000), t(2000), Duration::from_secs(300));
            assert_eq!(result, vec![(-100, 200)]);
        }

        #[test]
        fn negative_offset_shifts_forward() {
            // query_time=1000s, lookback=300s, offset=-5m=-300s
            // eval_time = 1000 + 300 = 1300, data window: [1300 - 300, 1300] = [1000, 1300]
            let expr = promql_parser::parser::parse("metric_name offset -5m").unwrap();
            let result = compute_preload_ranges(&expr, t(1000), t(1000), Duration::from_secs(300));
            assert_eq!(result, vec![(1000, 1300)]);
        }

        #[test]
        fn matrix_selector_uses_range() {
            // query_time=7200s, lookback=300s (unused for matrix), offset=1h, range=5m=300s
            // eval_time = 7200 - 3600 = 3600
            // data window: [3600 - 300, 3600] = [3300, 3600]
            let expr = promql_parser::parser::parse("metric_name[5m] offset 1h").unwrap();
            let result = compute_preload_ranges(&expr, t(7200), t(7200), Duration::from_secs(300));
            assert_eq!(result, vec![(3300, 3600)]);
        }

        #[test]
        fn nested_binary_produces_disjoint_ranges() {
            // max(metric offset 1h) - max(metric offset 2h) at query_time=7200, lookback=300
            // Left:  eval=3600, window=[3300, 3600]
            // Right: eval=0,    window=[-300, 0]
            // Two disjoint ranges (not merged into one)
            let expr = promql_parser::parser::parse(
                "max(metric_name offset 1h) - max(metric_name offset 2h)",
            )
                .unwrap();
            let result = compute_preload_ranges(&expr, t(7200), t(7200), Duration::from_secs(300));
            assert_eq!(result, vec![(-300, 0), (3300, 3600)]);
        }

        #[test]
        fn subquery_accounts_for_range_and_offset() {
            // metric[1h:5m] offset 30m at query_time=7200s, lookback=300s
            // subquery eval_time = 7200 - 1800 = 5400
            // subquery range = 1h = 3600s, inner eval over [5400-3600, 5400] = [1800, 5400]
            // inner selector lookback: [1800 - 300, 5400] = [1500, 5400]
            let expr = promql_parser::parser::parse("metric_name[1h:5m] offset 30m").unwrap();
            let result = compute_preload_ranges(&expr, t(7200), t(7200), Duration::from_secs(300));
            assert_eq!(result, vec![(1500, 5400)]);
        }

        #[test]
        fn subquery_inner_at_end_uses_outer_query_bounds() {
            // metric @ end()[1h:5m] offset 30m
            // query_start=query_end=7200s, lookback=300s
            //
            // Subquery: eval_time = 7200 - 1800 = 5400, range = 3600s
            //   inner eval window: [5400 - 3600, 5400] = [1800, 5400]
            // Inner selector: @ end() resolves to outer query_end = 7200s (not sq_end!)
            //   eval_time = 7200, data window: [7200 - 300, 7200] = [6900, 7200]
            //
            // Without the fix, @ end() would resolve to sq_end = 5400, giving [5100, 5400].
            let expr =
                promql_parser::parser::parse("metric_name @ end()[1h:5m] offset 30m").unwrap();
            let result = compute_preload_ranges(&expr, t(7200), t(7200), Duration::from_secs(300));
            assert_eq!(result, vec![(6900, 7200)]);
        }

        #[test]
        fn subquery_inner_at_start_uses_outer_query_bounds() {
            // metric @ start()[1h:5m] offset 30m
            // query_start=3600s, query_end=7200s, lookback=300s
            //
            // Subquery: eval range = [3600, 7200], then offset 30m
            //   → [3600-1800, 7200-1800] = [1800, 5400], range=3600s
            //   → inner eval window: [1800-3600, 5400] = [-1800, 5400]
            // Inner selector: @ start() resolves to outer [query_start, query_end]
            //   = [3600, 7200] (@ start/end sweep the full at-modifier range)
            //   data window: [3600 - 300, 7200] = [3300, 7200]
            let expr =
                promql_parser::parser::parse("metric_name @ start()[1h:5m] offset 30m").unwrap();
            let result = compute_preload_ranges(&expr, t(3600), t(7200), Duration::from_secs(300));
            assert_eq!(result, vec![(3300, 7200)]);
        }

        #[test]
        fn range_query_simple_selector() {
            // range [1000, 2000] with lookback=300
            // data window: [1000 - 300, 2000] = [700, 2000]
            let expr = promql_parser::parser::parse("metric_name").unwrap();
            let result = compute_preload_ranges(&expr, t(1000), t(2000), Duration::from_secs(300));
            assert_eq!(result, vec![(700, 2000)]);
        }

        #[test]
        fn range_query_with_offset() {
            // range [3600, 7200] with lookback=300, offset=1h
            // eval range: [3600-3600, 7200-3600] = [0, 3600]
            // data window: [0 - 300, 3600] = [-300, 3600]
            let expr = promql_parser::parser::parse("metric_name offset 1h").unwrap();
            let result = compute_preload_ranges(&expr, t(3600), t(7200), Duration::from_secs(300));
            assert_eq!(result, vec![(-300, 3600)]);
        }

        #[test]
        fn range_query_at_end_covers_all_steps() {
            // range [1000, 5000] with lookback=300, metric @ end()
            // In query_range, each step sets start=end=current_time, so
            // @ end() resolves to current_time, sweeping [1000, 5000].
            // Preload must cover: [1000 - 300, 5000] = [700, 5000]
            // Without the fix, @ end() would collapse to (5000, 5000)
            // giving only [4700, 5000] and missing earlier steps.
            let expr = promql_parser::parser::parse("metric_name @ end()").unwrap();
            let result = compute_preload_ranges(&expr, t(1000), t(5000), Duration::from_secs(300));
            assert_eq!(result, vec![(700, 5000)]);
        }

        #[test]
        fn range_query_at_start_covers_all_steps() {
            // range [1000, 5000] with lookback=300, metric @ start()
            // Same as @ end(): sweeps [1000, 5000] across steps.
            // Preload must cover: [1000 - 300, 5000] = [700, 5000]
            let expr = promql_parser::parser::parse("metric_name @ start()").unwrap();
            let result = compute_preload_ranges(&expr, t(1000), t(5000), Duration::from_secs(300));
            assert_eq!(result, vec![(700, 5000)]);
        }

        #[test]
        fn instant_query_at_end_is_single_point() {
            // instant query at t=2000, lookback=300, metric @ end()
            // start=end=2000s, so @ end() → (2000, 2000)
            // data window: [2000 - 300, 2000] = [1700, 2000]
            let expr = promql_parser::parser::parse("metric_name @ end()").unwrap();
            let result = compute_preload_ranges(&expr, t(2000), t(2000), Duration::from_secs(300));
            assert_eq!(result, vec![(1700, 2000)]);
        }

        #[test]
        fn function_call_recurses_into_args() {
            // rate(metric[5m] offset 1h) at query_time=7200, lookback=300 (unused for matrix)
            // matrix: eval_time = 7200 - 3600 = 3600, range = 5m = 300s
            // data window: [3600 - 300, 3600] = [3300, 3600]
            let expr = promql_parser::parser::parse("rate(metric_name[5m] offset 1h)").unwrap();
            let result = compute_preload_ranges(&expr, t(7200), t(7200), Duration::from_secs(300));
            assert_eq!(result, vec![(3300, 3600)]);
        }

        #[test]
        fn ceil_conversion_exact_second_boundary() {
            // hi_ms = 2000000 (exact), end should be 2000 not 2001
            let expr = promql_parser::parser::parse("metric_name").unwrap();
            let result = compute_preload_ranges(&expr, t(2000), t(2000), Duration::from_secs(0));
            assert_eq!(result, vec![(2000, 2000)]);
        }

        #[test]
        fn ceil_conversion_non_exact_boundary() {
            // offset -500ms shifts eval_end to 2000500ms → ceil = 2001
            // eval_start = 2000500ms → floor = 2000
            let expr = promql_parser::parser::parse("metric_name offset -500ms").unwrap();
            let result = compute_preload_ranges(&expr, t(2000), t(2000), Duration::from_secs(0));
            assert_eq!(result, vec![(2000, 2001)]);
        }

        #[test]
        fn disjoint_ranges_not_merged() {
            // metric - metric offset 5d at query_time=1000000s, lookback=300
            // Left: eval=1000000, window=[999700, 1000000]
            // Right: eval=1000000-432000=568000, window=[567700, 568000]
            // These are far apart and should produce 2 disjoint ranges
            let expr = promql_parser::parser::parse("metric_name - metric_name offset 5d").unwrap();
            let result =
                compute_preload_ranges(&expr, t(1_000_000), t(1_000_000), Duration::from_secs(300));
            assert_eq!(result, vec![(567700, 568000), (999700, 1000000)]);
        }

        #[test]
        fn overlapping_ranges_are_merged() {
            // metric offset 1m - metric offset 2m at query_time=7200, lookback=300
            // Left:  eval=7140, window=[6840, 7140]
            // Right: eval=7080, window=[6780, 7080]
            // These overlap → merged into [6780, 7140]
            let expr =
                promql_parser::parser::parse("metric_name offset 1m - metric_name offset 2m")
                    .unwrap();
            let result = compute_preload_ranges(&expr, t(7200), t(7200), Duration::from_secs(300));
            assert_eq!(result, vec![(6780, 7140)]);
        }
    }
}
