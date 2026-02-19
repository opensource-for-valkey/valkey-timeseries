use crate::common::Sample;
use crate::promql::functions::utils::{
    exact_arity_error, expect_exact_arg_count, expect_range_vector, expect_scalar,
};
use crate::promql::functions::{PromQLArg, PromQLFunction};
use crate::promql::{EvalResult, EvalSample, EvaluationError, ExprResult};
use orx_parallel::{IntoParIter, ParIter};

#[derive(Copy, Clone)]
pub(in crate::promql) struct DoubleExponentialSmoothingFunction;

impl PromQLFunction for DoubleExponentialSmoothingFunction {
    fn apply(&self, _arg: PromQLArg, _eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        Err(exact_arity_error("double_exponential_smoothing", 3, 1))
    }

    fn apply_args(&self, args: Vec<PromQLArg>, eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        eval_double_exponential_smoothing(args, eval_timestamp_ms)
    }
}

/// See https://en.wikipedia.org/wiki/Exponential_smoothing#Double_exponential_smoothing .
fn calculate_double_exponential_smoothing_value(
    samples: &[Sample],
    smoothing_factor: f64,
    trend_factor: f64,
) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }

    if samples.iter().any(|x| !x.value.is_finite()) {
        return Some(f64::NAN);
    }

    let mut level = samples[0].value;
    let mut trend = samples[1].value - level;

    for &Sample {
        timestamp: _,
        value,
    } in &samples[1..]
    {
        let previous_level = level;
        level = smoothing_factor * value + (1.0 - smoothing_factor) * (level + trend);
        trend = trend_factor * (level - previous_level) + (1.0 - trend_factor) * trend;
    }

    Some(level + trend)
}

const FUNCTION_NAME: &str = "double_exponential_smoothing_value";
fn eval_double_exponential_smoothing(
    args: Vec<PromQLArg>,
    eval_timestamp_ms: i64,
) -> EvalResult<ExprResult> {
    expect_exact_arg_count(FUNCTION_NAME, 3, args.len())?;
    let mut args = args.into_iter();
    let series = expect_range_vector(args.next().expect("checked arg count"), FUNCTION_NAME)?;
    let smoothing_factor =
        expect_scalar(args.next().expect("checked arg count"), FUNCTION_NAME, "sf")?;
    let trend_factor = expect_scalar(args.next().expect("checked arg count"), FUNCTION_NAME, "tf")?;

    if !(0.0..=1.0).contains(&smoothing_factor) {
        let msg = format!("invalid smoothing factor. Expected 0 < sf < 1, got {smoothing_factor}");
        return Err(EvaluationError::ArgumentError(msg));
    }

    if !(0.0..=1.0).contains(&trend_factor) {
        let msg = format!("invalid smoothing factor. Expected 0 < sf < 1, got {trend_factor}");
        return Err(EvaluationError::ArgumentError(msg));
    }

    // Can't do smoothing with less than 2 points
    if series.len() < 2 {
        return Ok(ExprResult::InstantVector(vec![]));
    }

    let out = series
        .into_par()
        .filter_map(|s| {
            let value = calculate_double_exponential_smoothing_value(
                &s.values,
                smoothing_factor,
                trend_factor,
            )?;
            Some(EvalSample {
                timestamp_ms: eval_timestamp_ms,
                labels: s.labels,
                value,
                drop_name: false,
            })
        })
        .collect::<Vec<_>>();

    Ok(ExprResult::InstantVector(out))
}
