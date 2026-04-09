use crate::common::Sample;
use promql_parser::parser::{Expr, VectorSelector};

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

#[cfg(test)]
mod tests {}
