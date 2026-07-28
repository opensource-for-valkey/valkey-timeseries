use crate::common::Sample;
use crate::common::math::kahan_inc;
use crate::labels::{Label, Labels};
use crate::promql::exec::types::EvalLabels;
use crate::promql::functions::PromQLArg;
use crate::promql::{EvalResult, EvalSample, EvalSamples, EvaluationError, ExprResult};
use promql_parser::label::{METRIC_NAME, MatchOp};
use promql_parser::parser::Expr;
use std::borrow::Cow;
use std::cmp::Ordering;

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
///   Kahan compensated summation is applied to the M2 accumulator
///   to reduce floating-point rounding error in long sequences.
///   The mean update uses standard Welford (without Kahan) because
///   Kahan compensation on the running mean can introduce inconsistent
///   rounding between the mean update and delta2 computation, causing
///   catastrophic precision loss when values are extremely close.
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
    let mut m2 = 0.0;
    let mut c_m2 = 0.0;

    for sample in values {
        count += 1.0;
        let delta = sample.value - mean;
        mean += delta / count;
        let new_delta = sample.value - mean;
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

pub(super) fn expect_min_arg_count(
    function_name: &str,
    expected: usize,
    actual: usize,
) -> EvalResult<()> {
    if actual < expected {
        return Err(min_arity_error(function_name, expected, actual));
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

pub(super) fn map_scalar_or_vector(
    value: PromQLArg,
    map: impl Fn(f64) -> f64,
) -> EvalResult<ExprResult> {
    match value {
        PromQLArg::Scalar(v) => Ok(ExprResult::Scalar(map(v))),
        PromQLArg::InstantVector(mut vector) => {
            for sample in &mut vector {
                sample.value = map(sample.value);
                sample.drop_name = true;
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

/// The labels `absent(v)` / `absent_over_time(v[d])` carry when they fire.
///
/// Unique among the functions, absent's output labels come from the *query
/// text* rather than from data — there is no input series to take them from.
/// Prometheus copies the argument selector's equality matchers, so
/// `absent(up{job="api"})` answers `{job="api"} 1` and an alert on it can still
/// route by job. The rules, from Prometheus' `createLabelsForAbsentFunction`:
///
/// * Only a vector or matrix selector contributes labels. Any other expression
///   — an aggregation, a binary op, a nested call — yields `{}`, because there
///   is no single selector to speak for.
/// * `__name__` never appears in the output, whether it arrived as the metric
///   name or as an explicit `{__name__="x"}` matcher.
/// * The first `=` matcher for a label name sets it. A second matcher on that
///   same name — of any kind — deletes it instead, as does any non-`=` matcher.
///   So `{job="a",job="b",foo="c"}` gives `{foo="c"}`: a contradictory selector
///   cannot describe the series that is missing. This is backwards-compatible
///   behaviour Prometheus preserves deliberately, and it is arguably wrong for
///   the redundant `{job="a",job=~"a"}`, which also drops `job`.
///
/// Parentheses are deliberately *not* looked through: Prometheus' type switch
/// has no case for them, so `absent((up{job="api"}))` really does answer `{}`
/// upstream. Do not "fix" this without a matching upstream change.
pub(super) fn labels_for_absent(arg: Option<&Expr>) -> EvalLabels {
    let matchers = match arg {
        Some(Expr::VectorSelector(vs)) => &vs.matchers,
        Some(Expr::MatrixSelector(ms)) => &ms.vs.matchers,
        _ => return EvalLabels::empty(),
    };

    let mut labels: Vec<Label> = Vec::new();
    let mut seen: Vec<&str> = Vec::new();
    for matcher in &matchers.matchers {
        if matcher.name == METRIC_NAME {
            continue;
        }
        let first_mention = !seen.contains(&matcher.name.as_str());
        if first_mention {
            seen.push(&matcher.name);
        }
        if first_mention && matches!(matcher.op, MatchOp::Equal) {
            labels.push(Label::new(matcher.name.clone(), matcher.value.clone()));
        } else {
            labels.retain(|l| l.name != matcher.name);
        }
    }

    // `or_matchers` (`{a="1" or b="2"}`) is a parser extension with no
    // Prometheus equivalent. A label constrained by alternatives has no single
    // value to copy — the series could be missing under either branch — so a
    // name mentioned there is always a delete, never a source.
    for matcher in matchers.or_matchers.iter().flatten() {
        labels.retain(|l| l.name != matcher.name);
    }

    labels.sort_unstable_by(|a, b| a.name.cmp(&b.name));
    EvalLabels::owned(labels)
}

#[cfg(test)]
mod absent_label_tests {
    use super::labels_for_absent;
    use promql_parser::parser;

    /// `absent(<query>)` — the labels it answers with, rendered as the
    /// promqltest files write them.
    fn absent_labels(query: &str) -> String {
        let expr = parser::parse(query).expect("test query must parse");
        let parser::Expr::Call(call) = expr else {
            panic!("{query} is not a function call");
        };
        let labels = labels_for_absent(call.args.args.first().map(|a| &**a));
        let rendered = labels
            .as_ref()
            .iter()
            .map(|l| format!("{}=\"{}\"", l.name, l.value))
            .collect::<Vec<_>>()
            .join(", ");
        format!("{{{rendered}}}")
    }

    /// The cases from `promqltest/testdata/functions.test`, which cannot run
    /// there while the surrounding region is inside an `ignore` block.
    #[test]
    fn matches_prometheus_absent_labels() {
        // A bare selector has nothing but a name, and the name never survives.
        assert_eq!(absent_labels("absent(nonexistent)"), "{}");
        assert_eq!(
            absent_labels("absent_over_time(http_requests_total[5m])"),
            "{}"
        );

        // Equality matchers are copied; non-equality ones are not.
        assert_eq!(
            absent_labels(
                r#"absent(nonexistent{job="testjob", instance="testinstance", method=~".x"})"#
            ),
            r#"{instance="testinstance", job="testjob"}"#
        );
        assert_eq!(
            absent_labels(r#"absent_over_time(http_requests_total{handler="/foo"}[5m])"#),
            r#"{handler="/foo"}"#
        );
        assert_eq!(
            absent_labels(r#"absent_over_time(http_requests_total{handler!="/foo"}[5m])"#),
            "{}"
        );

        // A repeated name deletes the label, however many times it repeats and
        // whichever operators are involved.
        assert_eq!(
            absent_labels(r#"absent(nonexistent{job="testjob",job="testjob2",foo="bar"})"#),
            r#"{foo="bar"}"#
        );
        assert_eq!(
            absent_labels(
                r#"absent(nonexistent{job="testjob",job="testjob2",job="three",foo="bar"})"#
            ),
            r#"{foo="bar"}"#
        );
        assert_eq!(
            absent_labels(r#"absent(nonexistent{job="testjob",job=~"testjob2",foo="bar"})"#),
            r#"{foo="bar"}"#
        );
        assert_eq!(
            absent_labels(
                r#"absent_over_time(http_requests_total{handler="/foo", handler="/bar", handler="/foobar"}[5m])"#
            ),
            "{}"
        );
        assert_eq!(
            absent_labels(
                r#"absent_over_time(http_requests_total{handler="/foo", handler="/bar", instance="127.0.0.1"}[5m])"#
            ),
            r#"{instance="127.0.0.1"}"#
        );

        // A selector with no metric name still contributes its matchers.
        assert_eq!(
            absent_labels(r#"absent_over_time({instance="127.0.0.1"}[5m])"#),
            r#"{instance="127.0.0.1"}"#
        );
        assert_eq!(
            absent_labels(r#"absent_over_time({job="grok"}[20m])"#),
            r#"{job="grok"}"#
        );

        // Anything that is not a selector speaks for no single series.
        for query in [
            r#"absent(sum(nonexistent{job="testjob", instance="testinstance"}))"#,
            "absent(max(nonexistent))",
            "absent(nonexistent > 1)",
            "absent(a + b)",
            "absent(a and b)",
            "absent(rate(nonexistent[5m]))",
            "absent_over_time(rate(nonexistent[5m])[5m:])",
            r#"absent_over_time({instance="127.0.0.1"}[5m:5s])"#,
        ] {
            assert_eq!(absent_labels(query), "{}", "{query}");
        }
    }

    /// `__name__` is dropped wherever it came from — including the explicit
    /// matcher form, which the metric-name shorthand does not cover.
    #[test]
    fn metric_name_never_reaches_the_output() {
        assert_eq!(
            absent_labels(r#"absent({__name__="http_requests_total", job="api"})"#),
            r#"{job="api"}"#
        );
        assert_eq!(absent_labels(r#"absent({__name__=~"http_.*"})"#), "{}");
    }

    /// `or` matchers are a parser extension Prometheus has no counterpart for.
    /// A label constrained by alternatives has no single value that describes
    /// the missing series, so it is dropped rather than guessed at — including
    /// when every branch happens to agree on the name.
    #[test]
    fn or_matchers_contribute_nothing() {
        assert_eq!(absent_labels(r#"absent(up{job="a" or job="b"})"#), "{}");
        assert_eq!(absent_labels(r#"absent(up{job="a" or env="p"})"#), "{}");
    }

    /// Prometheus does not look through parentheses here, so neither do we.
    #[test]
    fn parentheses_suppress_the_labels() {
        assert_eq!(absent_labels(r#"absent((up{job="api"}))"#), "{}");
    }
}
