use crate::common::Sample;
use crate::promql::functions::range_vector_functions::eval_range;
use crate::promql::functions::{PromQLArg, PromQLFunction};
use crate::promql::{EvalContext, EvalResult, ExprResult};

#[derive(Clone, Copy, Debug)]
pub(in crate::promql) struct IRateFunction;

impl PromQLFunction for IRateFunction {
    fn apply(&self, arg: PromQLArg, ctx: &EvalContext) -> EvalResult<ExprResult> {
        let samples = arg.into_range_vector()?;
        Ok(eval_range(samples, ctx.evaluation_ts, rollup_irate))
    }
}

/// `irate` over one window: the per-second rate between its last two samples.
///
/// Named rather than inline so the pushed-down path can reduce a window with the
/// very same function the local path runs.
pub(in crate::promql) fn rollup_irate(samples: &[Sample]) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    let a = &samples[samples.len() - 2];
    let b = &samples[samples.len() - 1];
    let dt = b.timestamp.saturating_sub(a.timestamp);
    if dt <= 0 {
        return None;
    }
    let mut dv = b.value - a.value;
    if dv < 0.0 {
        dv = b.value;
    }
    Some(dv / (dt as f64 / 1000f64))
}
