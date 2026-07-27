use crate::common::{Sample, Timestamp};
use crate::promql::exec::types::{EvalSample, SeriesMap};
use promql_parser::parser::{Call, Expr, VectorSelector};

/// Append one evaluation step's instant-vector samples into a per-series map,
/// stamping each sample with the step timestamp.
///
/// Shared by the range-query step merge (`evaluate_range`) and the subquery
/// step merge (`evaluate_subquery`). Callers must feed steps in ascending
/// timestamp order so the per-series sample vectors stay chronologically sorted.
pub(in crate::promql) fn merge_step_into_series_map(
    series_map: &mut SeriesMap,
    step_ts: Timestamp,
    samples: Vec<EvalSample>,
) {
    for sample in samples {
        series_map
            .entry(sample.labels)
            .or_default()
            .push(Sample::new(step_ts, sample.value));
    }
}

/// Filter samples to (start_ms, end_ms] using binary search on sorted timestamps.
/// Samples are sorted by timestamp_ms (storage invariant), so we use partition_point
/// to find bounds in O(log n) instead of scanning the full vector.
pub fn filter_samples_binary_search(samples: &[Sample], start_ms: i64, end_ms: i64) -> Vec<Sample> {
    // Find the first index where timestamp_ms > start_ms
    let lo = samples.partition_point(|s| s.timestamp <= start_ms);
    // Find the last index where timestamp_ms > end_ms
    let hi = samples.partition_point(|s| s.timestamp <= end_ms);
    samples[lo..hi].to_vec()
}

pub(super) fn collect_vector_selectors(expr: &Expr) -> Vec<&VectorSelector> {
    let mut out = Vec::new();
    collect_vector_selectors_inner(expr, &mut out);
    out
}

fn collect_vector_selectors_inner<'a>(expr: &'a Expr, out: &mut Vec<&'a VectorSelector>) {
    match expr {
        Expr::VectorSelector(vs) => out.push(vs),
        Expr::Aggregate(agg) => {
            collect_vector_selectors_inner(&agg.expr, out);
            if let Some(ref param) = agg.param {
                collect_vector_selectors_inner(param, out);
            }
        }
        Expr::Binary(b) => {
            collect_vector_selectors_inner(&b.lhs, out);
            collect_vector_selectors_inner(&b.rhs, out);
        }
        Expr::Paren(p) => collect_vector_selectors_inner(&p.expr, out),
        Expr::Call(call) => {
            for arg in &call.args.args {
                collect_vector_selectors_inner(arg, out);
            }
        }
        Expr::Unary(u) => collect_vector_selectors_inner(&u.expr, out),
        // MatrixSelector: needs sample slices, not latest-value — not preloaded
        // Subquery: has own step loop with different step params — not preloaded
        Expr::MatrixSelector(_)
        | Expr::Subquery(_)
        | Expr::NumberLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::Extension(_) => {}
    }
}

/// Collect the function calls that are candidates for whole-grid rollup
/// push-down, in the order they appear.
///
/// Only the call shape is screened here — whether the function can be pushed
/// down, and whether its arguments allow it, is decided by the caller. What this
/// walk *does* decide is scope: a call inside a subquery is skipped, because a
/// subquery runs its own step grid and its inner calls are evaluated one instant
/// at a time against that grid, not the outer query's.
pub(in crate::promql) fn collect_rollup_calls(expr: &Expr) -> Vec<&Call> {
    let mut out = Vec::new();
    collect_rollup_calls_inner(expr, &mut out);
    out
}

fn collect_rollup_calls_inner<'a>(expr: &'a Expr, out: &mut Vec<&'a Call>) {
    match expr {
        Expr::Call(call) => {
            out.push(call);
            for arg in &call.args.args {
                collect_rollup_calls_inner(arg, out);
            }
        }
        Expr::Aggregate(agg) => {
            collect_rollup_calls_inner(&agg.expr, out);
            if let Some(ref param) = agg.param {
                collect_rollup_calls_inner(param, out);
            }
        }
        Expr::Binary(b) => {
            collect_rollup_calls_inner(&b.lhs, out);
            collect_rollup_calls_inner(&b.rhs, out);
        }
        Expr::Paren(p) => collect_rollup_calls_inner(&p.expr, out),
        Expr::Unary(u) => collect_rollup_calls_inner(&u.expr, out),
        // Subquery: own step grid, so its calls are not part of this one.
        Expr::Subquery(_)
        | Expr::MatrixSelector(_)
        | Expr::VectorSelector(_)
        | Expr::NumberLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::Extension(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn calls(query: &str) -> Vec<String> {
        let expr = promql_parser::parser::parse(query).unwrap();
        collect_rollup_calls(&expr)
            .into_iter()
            .map(|c| c.func.name.to_string())
            .collect()
    }

    #[test]
    fn should_collect_calls_outside_subqueries() {
        assert_eq!(calls("sum_over_time(m[5m])"), vec!["sum_over_time"]);
        assert_eq!(calls("sum(rate(m[5m]))"), vec!["rate"]);
        assert_eq!(
            calls("abs(sum_over_time(m[5m]))"),
            vec!["abs", "sum_over_time"]
        );
        assert_eq!(
            calls("rate(m[5m]) + count_over_time(n[1m])"),
            vec!["rate", "count_over_time"]
        );
        assert_eq!(calls("-sum_over_time(m[5m])"), vec!["sum_over_time"]);
        assert_eq!(calls("m"), Vec::<String>::new());
    }

    /// A subquery evaluates its inner expression against its own step grid, so
    /// the outer grid must not claim those calls.
    #[test]
    fn should_not_descend_into_subqueries() {
        assert_eq!(
            calls("max_over_time(sum_over_time(m[5m])[1h:1m])"),
            vec!["max_over_time"],
            "only the outer call belongs to the outer grid"
        );
        assert_eq!(
            calls("sum_over_time(rate(m[5m])[1h:1m])"),
            vec!["sum_over_time"]
        );
        assert_eq!(calls("(rate(m[5m]))[1h:1m]"), Vec::<String>::new());
    }
}
