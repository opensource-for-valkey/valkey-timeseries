use crate::promql::functions::types::{PromQLArg, PromQLFunction};
use crate::promql::{EvalResult, EvalSample, EvaluationError, ExprResult};
use std::default::Default;

/// Absent function: returns 1.0 if input is empty, empty vector otherwise
#[derive(Copy, Clone)]
pub(in crate::promql) struct AbsentFunction;

impl PromQLFunction for AbsentFunction {
    fn apply(&self, arg: PromQLArg, eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        let samples = arg.into_instant_vector()?;
        if samples.is_empty() {
            // Return a single sample with value 1.0 at the evaluation timestamp
            Ok(ExprResult::InstantVector(vec![EvalSample {
                timestamp_ms: eval_timestamp_ms,
                value: 1.0,
                labels: Default::default(),
                drop_name: false,
            }]))
        } else {
            // Return empty vector when input has samples
            Ok(ExprResult::InstantVector(vec![]))
        }
    }
}

impl Default for AbsentFunction {
    fn default() -> Self {
        AbsentFunction
    }
}

/// Scalar function: converts a single-element vector to scalar (returns as-is or empty)
#[derive(Copy, Clone)]
pub(in crate::promql) struct ScalarFunction;

impl PromQLFunction for ScalarFunction {
    fn apply(&self, arg: PromQLArg, _eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        match arg {
            PromQLArg::Scalar(f) => Ok(ExprResult::Scalar(f)),
            PromQLArg::InstantVector(samples) => {
                if samples.len() == 1 {
                    Ok(ExprResult::Scalar(samples[0].value))
                } else {
                    Ok(ExprResult::Scalar(f64::NAN))
                }
            }
            _ => Err(EvaluationError::InternalError(
                "scalar function expects a scalar or single-sample vector argument".to_string(),
            )),
        }
    }
}

impl Default for ScalarFunction {
    fn default() -> Self {
        ScalarFunction
    }
}

/// Vector function: converts a scalar value to a single-element instant vector with no labels.
#[derive(Copy, Clone)]
pub(in crate::promql) struct VectorFunction;

impl PromQLFunction for VectorFunction {
    fn apply(&self, arg: PromQLArg, eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        Ok(ExprResult::InstantVector(vec![EvalSample {
            timestamp_ms: eval_timestamp_ms,
            value: arg.into_scalar()?,
            labels: Default::default(),
            drop_name: false,
        }]))
    }
}

impl Default for VectorFunction {
    fn default() -> Self {
        VectorFunction
    }
}
