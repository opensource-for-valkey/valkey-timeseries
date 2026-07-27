use crate::common::Sample;
use crate::promql::functions::range_vector_functions::eval_range;
use crate::promql::functions::{PromQLArg, PromQLFunction};
use crate::promql::{EvalContext, EvalResult, ExprResult};

#[derive(Clone, Copy, Debug)]
pub(in crate::promql) struct IDeltaFunction;
impl PromQLFunction for IDeltaFunction {
    fn apply(&self, arg: PromQLArg, ctx: &EvalContext) -> EvalResult<ExprResult> {
        let samples = arg.into_range_vector()?;
        Ok(eval_range(samples, ctx.evaluation_ts, rollup_idelta))
    }
}

/// `idelta` over one window: the difference between its **last two** samples,
/// not across the whole window. `idelta` is the instant counterpart of `delta`
/// — everything before the final pair is only there to establish that a pair
/// exists.
///
/// Named rather than inline so the pushed-down path reduces a window with the
/// very same function the local path runs.
pub(in crate::promql) fn rollup_idelta(samples: &[Sample]) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    let previous = &samples[samples.len() - 2];
    let last = &samples[samples.len() - 1];
    if previous.timestamp == last.timestamp {
        return None;
    }
    Some(last.value - previous.value)
}
