use crate::common::Sample;
use crate::promql::Labels;
use crate::promql::hashers::FingerprintHashMap;
use crate::promql::types::EvalSamples;
use promql_parser::parser::{Expr, VectorSelector};

/// Computes time alignment and range boundaries for subquery evaluation.
///
/// Aligns the start time to step boundaries using floor division to ensure
/// consistent evaluation points. Extends the range backwards to include
/// lookback for the first step.
pub(super) fn compute_subquery_plan(
    subquery_start_ms: i64,
    subquery_end_ms: i64,
    step_ms: i64,
    lookback_delta_ms: i64,
) -> (i64, i64, i64, usize) {
    let div = subquery_start_ms.div_euclid(step_ms);
    let mut aligned_start_ms = div * step_ms;
    if aligned_start_ms <= subquery_start_ms {
        aligned_start_ms += step_ms;
    }

    let expected_steps = ((subquery_end_ms - aligned_start_ms) / step_ms) as usize + 1;
    let range_start_ms = aligned_start_ms - lookback_delta_ms;
    let range_end_ms = subquery_end_ms;

    (
        aligned_start_ms,
        range_start_ms,
        range_end_ms,
        expected_steps,
    )
}

/// Buckets samples into step-aligned time windows using a sliding window algorithm.
///
/// Uses O(samples + steps) complexity instead of O(samples × steps) by maintaining
/// a monotonic pointer through sorted samples. For each step, finds the most recent
/// sample within the lookback window. Uses > (not >=) for start boundary to match
/// Prometheus staleness semantics.
pub(super) fn bucket_series_samples(
    series_samples: FingerprintHashMap<(Labels, Vec<Sample>)>,
    aligned_start_ms: i64,
    subquery_end_ms: i64,
    step_ms: i64,
    lookback_delta_ms: i64,
    expected_steps: usize,
) -> Vec<EvalSamples> {
    let mut range_vector = Vec::with_capacity(series_samples.len());

    for (_fingerprint, (labels, samples)) in series_samples {
        let mut step_samples = Vec::with_capacity(expected_steps);
        let mut i = 0usize;
        let mut last_valid: Option<&Sample> = None;

        for current_step_ms in (aligned_start_ms..=subquery_end_ms).step_by(step_ms as usize) {
            let lookback_start_ms = current_step_ms - lookback_delta_ms;

            while i < samples.len() && samples[i].timestamp <= current_step_ms {
                last_valid = Some(&samples[i]);
                i += 1;
            }

            if let Some(sample) = last_valid
                && sample.timestamp > lookback_start_ms
            {
                step_samples.push(Sample::new(current_step_ms, sample.value));
            }
        }

        if !step_samples.is_empty() {
            range_vector.push(EvalSamples {
                values: step_samples,
                labels,
                drop_name: false,
            });
        }
    }

    range_vector
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

#[cfg(test)]
mod tests {}
