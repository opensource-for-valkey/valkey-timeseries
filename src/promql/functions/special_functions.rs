use crate::promql::functions::types::{PromQLArg, PromQLFunction};
use crate::promql::{EvalContext, EvalResult, EvalSample, EvaluationError, ExprResult};
use std::default::Default;

/// Absent function: returns 1.0 if input is empty, empty vector otherwise
#[derive(Copy, Clone)]
pub(in crate::promql) struct AbsentFunction;

impl PromQLFunction for AbsentFunction {
    fn apply(&self, arg: PromQLArg, ctx: &EvalContext) -> EvalResult<ExprResult> {
        let samples = arg.into_instant_vector()?;
        if samples.is_empty() {
            // Return a single sample with value 1.0 at the evaluation timestamp
            Ok(ExprResult::InstantVector(vec![EvalSample {
                timestamp_ms: ctx.evaluation_ts,
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

/// Scalar function: converts a single-element vector to scalar (returns as-is or empty)
#[derive(Copy, Clone)]
pub(in crate::promql) struct ScalarFunction;

impl PromQLFunction for ScalarFunction {
    fn apply(&self, arg: PromQLArg, _ctx: &EvalContext) -> EvalResult<ExprResult> {
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

/// Vector function: converts a scalar value to a single-element instant vector with no labels.
#[derive(Copy, Clone)]
pub(in crate::promql) struct VectorFunction;

impl PromQLFunction for VectorFunction {
    fn apply(&self, arg: PromQLArg, ctx: &EvalContext) -> EvalResult<ExprResult> {
        Ok(ExprResult::InstantVector(vec![EvalSample {
            timestamp_ms: ctx.evaluation_ts,
            value: arg.into_scalar()?,
            labels: Default::default(),
            drop_name: false,
        }]))
    }
}

/// Scalar function: returns the maximum of the input values
#[derive(Copy, Clone)]
pub(in crate::promql) struct StartFunction;

impl PromQLFunction for StartFunction {
    fn apply(&self, _arg: PromQLArg, ctx: &EvalContext) -> EvalResult<ExprResult> {
        Ok(ExprResult::Scalar(ctx.query_start as f64 / 1000.0))
    }

    fn apply_args(&self, _args: Vec<PromQLArg>, ctx: &EvalContext) -> EvalResult<ExprResult> {
        Ok(ExprResult::Scalar(ctx.query_start as f64 / 1000.0))
    }
}

/// End function: returns the end timestamp of the current query range evaluation as the number of seconds
/// since January 1, 1970 UTC. For instant queries, this is equal to the evaluation timestamp.
#[derive(Copy, Clone)]
pub(in crate::promql) struct EndFunction;

impl PromQLFunction for EndFunction {
    fn apply(&self, _arg: PromQLArg, ctx: &EvalContext) -> EvalResult<ExprResult> {
        Ok(ExprResult::Scalar(ctx.evaluation_ts as f64 / 1000.0))
    }

    fn apply_args(&self, mut _args: Vec<PromQLArg>, ctx: &EvalContext) -> EvalResult<ExprResult> {
        Ok(ExprResult::Scalar(ctx.query_end as f64 / 1000.0))
    }
}

/// Range function: returns the range duration in seconds for range selectors, or 0 for instant selectors
#[derive(Copy, Clone)]
pub(in crate::promql) struct RangeFunction;

impl RangeFunction {
    fn range_seconds(ctx: &EvalContext) -> f64 {
        let range = (ctx.query_end - ctx.query_start).max(0);
        if range > 0 {
            (range as f64) / 1000.0
        } else {
            0.0
        }
    }
}
impl PromQLFunction for RangeFunction {
    fn apply(&self, _arg: PromQLArg, ctx: &EvalContext) -> EvalResult<ExprResult> {
        let range_seconds = Self::range_seconds(ctx);
        Ok(ExprResult::Scalar(range_seconds))
    }

    fn apply_args(&self, _args: Vec<PromQLArg>, ctx: &EvalContext) -> EvalResult<ExprResult> {
        let range_seconds = Self::range_seconds(ctx);
        Ok(ExprResult::Scalar(range_seconds))
    }
}

/// step function: returns the step duration in seconds for range selectors, or 0 for instant selectors
#[derive(Copy, Clone)]
pub(in crate::promql) struct StepFunction;

impl StepFunction {
    fn step_seconds(ctx: &EvalContext) -> f64 {
        if ctx.step_ms > 0 {
            ctx.step_ms as f64 / 1000.0
        } else {
            0.0
        }
    }
}

impl PromQLFunction for StepFunction {
    fn apply(&self, _arg: PromQLArg, ctx: &EvalContext) -> EvalResult<ExprResult> {
        let step_seconds = Self::step_seconds(ctx);
        Ok(ExprResult::Scalar(step_seconds))
    }

    fn apply_args(&self, _args: Vec<PromQLArg>, ctx: &EvalContext) -> EvalResult<ExprResult> {
        let step_seconds = Self::step_seconds(ctx);
        Ok(ExprResult::Scalar(step_seconds))
    }
}
