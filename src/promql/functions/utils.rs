use crate::common::Sample;
use crate::common::math::kahan_inc;
use crate::promql::functions::PromQLArg;
use crate::promql::{EvalResult, EvalSample, EvalSamples, EvaluationError, ExprResult, Labels};
use ahash::AHashSet;
use promql_parser::label::METRIC_NAME;
use promql_parser::parser::Expr;
use std::borrow::Cow;
use std::cmp::Ordering;

/// Average calculation matching Prometheus semantics.
///
/// Strategy:
/// 1. Use Kahan summation for numerical stability.
/// 2. If the intermediate sum overflows to ±Inf, switch to incremental mean
///    to avoid poisoning the entire result.
///
/// This mirrors Prometheus' hybrid strategy and prevents overflow-induced
/// divergence while maintaining IEEE-754 parity.
pub(in crate::promql) fn avg_kahan(values: &[Sample]) -> f64 {
    if values.len() == 1 {
        return values[0].value;
    }

    let mut sum = values[0].value;
    let mut c = 0.0;
    let mut mean = 0.0;
    let mut incremental = false;

    for (i, sample) in values.iter().enumerate().skip(1) {
        let count = (i + 1) as f64;

        if !incremental {
            let (new_sum, new_c) = kahan_inc(sample.value, sum, c);
            if !new_sum.is_infinite() {
                sum = new_sum;
                c = new_c;
                continue;
            }

            incremental = true;
            mean = sum / (count - 1.0);
            c /= count - 1.0;
        }

        let q = (count - 1.0) / count;
        (mean, c) = kahan_inc(sample.value / count, q * mean, q * c);
    }

    if incremental {
        mean + c
    } else {
        let count = values.len() as f64;
        sum / count + c / count
    }
}

/// Variance calculation using Welford's online algorithm (1962)
/// with compensated summation for improved numerical stability.
///
/// Algorithm:
///   For each value x:
///     count += 1
///     delta  = x - mean
///     mean  += delta / count
///     delta2 = x - mean
///     M2    += delta * delta2
///   variance = M2 / count   (population variance)
///
/// Enhancement:
///   Kahan compensated summation is applied to the incremental
///   updates of both the running mean and M2 accumulators,
///   reducing floating-point rounding error in long sequences.
///
/// Semantics:
///   - Computes population variance (divides by n)
///   - Matches Prometheus population variance semantics
///
/// NaN handling:
///   - Empty input returns NaN
///   - Single value returns 0.0
///   - NaN values propagate through the calculation
///
/// References:
///   - <https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm>
///   - Prometheus: `promql/functions.go::varianceOverTime`
pub(in crate::promql) fn variance_kahan(values: &[Sample]) -> f64 {
    if values.is_empty() {
        return f64::NAN;
    }

    let mut count = 0.0;
    let mut mean = 0.0;
    let mut c_mean = 0.0;
    let mut m2 = 0.0;
    let mut c_m2 = 0.0;

    for sample in values {
        count += 1.0;
        let delta = sample.value - (mean + c_mean);
        (mean, c_mean) = kahan_inc(delta / count, mean, c_mean);
        let new_delta = sample.value - (mean + c_mean);
        (m2, c_m2) = kahan_inc(delta * new_delta, m2, c_m2);
    }

    (m2 + c_m2) / count
}

#[inline]
pub(super) fn change_below_tolerance(v: f64, prev_value: f64) -> bool {
    let tolerance = 1e-12 * v.abs();
    (v - prev_value).abs() < tolerance
}

pub(super) fn exact_arity_error(
    function_name: &str,
    expected_args: usize,
    actual_args: usize,
) -> EvaluationError {
    EvaluationError::InternalError(format!(
        "{function_name} requires exactly {expected_args} argument(s), got {actual_args}"
    ))
}

pub(super) fn min_arity_error(
    function_name: &str,
    min_args: usize,
    actual_args: usize,
) -> EvaluationError {
    EvaluationError::InternalError(format!(
        "{function_name} requires at least {min_args} argument(s), got {actual_args}"
    ))
}

pub(super) fn max_arity_error(
    function_name: &str,
    max_args: usize,
    actual_args: usize,
) -> EvaluationError {
    EvaluationError::InternalError(format!(
        "{function_name} accepts at most {max_args} argument(s), got {actual_args}"
    ))
}

pub(super) fn expect_exact_arg_count(
    function_name: &str,
    expected: usize,
    actual: usize,
) -> EvalResult<()> {
    if expected != actual {
        return Err(exact_arity_error(function_name, expected, actual));
    }
    Ok(())
}

// Prometheus' current UTF-8 label-name validation only rejects empty names.
// Rust strings are already guaranteed to be valid UTF-8.
pub(super) fn is_valid_label_name(label: &str) -> bool {
    !label.is_empty()
}

pub(super) fn output_labelset_key(labels: &'_ Labels, drop_name: bool) -> Cow<'_, Labels> {
    let modified = labels
        .iter()
        .any(|label| drop_name && label.name.as_str() == METRIC_NAME);
    if !modified {
        return Cow::Borrowed(labels);
    }

    let key = labels
        .iter()
        .filter(|label| !drop_name || label.name.as_str() != METRIC_NAME)
        .cloned()
        .collect();

    Cow::Owned(Labels::new(key))
}

pub(super) fn extract_string_arg(
    expr: &Expr,
    function_name: &str,
    arg_index: usize,
) -> EvalResult<String> {
    match expr {
        Expr::StringLiteral(string) => Ok(string.val.clone()),
        Expr::Paren(paren) => extract_string_arg(&paren.expr, function_name, arg_index),
        _ => Err(EvaluationError::InternalError(format!(
            "expected string literal for argument {} to function '{}'",
            arg_index + 1,
            function_name
        ))),
    }
}

pub(super) fn expect_string(value: PromQLArg, func: &str, arg_name: &str) -> EvalResult<String> {
    let PromQLArg::String(s) = value else {
        return Err(EvaluationError::ArgumentError(format!(
            "{func} expects a string for {arg_name} argument, got {value:?}"
        )));
    };
    Ok(s)
}

pub(super) fn expect_instant_vector(value: PromQLArg, func: &str) -> EvalResult<Vec<EvalSample>> {
    match value {
        PromQLArg::InstantVector(v) => Ok(v),
        other => Err(EvaluationError::ArgumentError(format!(
            "{func} expects instant vector, got {other:?}"
        ))),
    }
}

pub(super) fn expect_range_vector(value: PromQLArg, func: &str) -> EvalResult<Vec<EvalSamples>> {
    match value {
        PromQLArg::RangeVector(v) => Ok(v),
        other => Err(EvaluationError::ArgumentError(format!(
            "{func} expects range vector, got {other:?}"
        ))),
    }
}

pub(super) fn expect_scalar(arg: PromQLArg, func: &str, param_name: &str) -> EvalResult<f64> {
    match arg {
        PromQLArg::Scalar(val) => return Ok(val),
        PromQLArg::InstantVector(s) => {
            let len = s.len();
            if len == 1 {
                return Ok(s[0].value);
            }
            let msg =
                format!("Expected a single value for {param_name} param of {func}, got {len}");
            return Err(EvaluationError::ArgumentError(msg));
        }
        _ => {}
    }

    let msg = format!(
        "expected a scalar for {param_name} param of {func}; got {:?}",
        arg.value_type()
    );
    Err(EvaluationError::ArgumentError(msg))
}

pub(super) fn ensure_unique_labelsets(samples: &[EvalSample]) -> EvalResult<()> {
    let mut seen_label_sets = AHashSet::with_capacity(samples.len());
    for sample in samples {
        let labelset_key = output_labelset_key(&sample.labels, sample.drop_name);
        if !seen_label_sets.insert(labelset_key) {
            return Err(EvaluationError::InternalError(
                "vector cannot contain metrics with the same labelset".to_string(),
            ));
        }
    }

    Ok(())
}

pub(super) fn map_scalar_or_vector(
    value: PromQLArg,
    map: impl Fn(f64) -> f64,
) -> EvalResult<ExprResult> {
    match value {
        PromQLArg::Scalar(v) => Ok(ExprResult::Scalar(map(v))),
        PromQLArg::InstantVector(mut vector) => {
            for sample in &mut vector {
                sample.value = map(sample.value);
            }
            Ok(ExprResult::InstantVector(vector))
        }
        other => Err(EvaluationError::ArgumentError(format!(
            "function expects scalar or instant vector, got {other:?}"
        ))),
    }
}

pub(super) fn series_len(val: &ExprResult) -> usize {
    match &val {
        ExprResult::RangeVector(rv) => rv.len(),
        ExprResult::InstantVector(iv) => iv.len(),
        _ => 1,
    }
}

#[inline]
pub fn remove_empty_series(tss: &mut Vec<EvalSamples>) {
    tss.retain(|ts| !ts.values.iter().all(|v| v.value.is_nan()));
}

pub(super) fn is_inf(x: f64, sign: i8) -> bool {
    match sign.cmp(&0_i8) {
        Ordering::Greater => x == f64::INFINITY,
        Ordering::Less => x == f64::NEG_INFINITY,
        Ordering::Equal => x.is_infinite(),
    }
}
