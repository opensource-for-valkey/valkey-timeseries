use crate::promql::functions::range_vector_functions::eval_range;
use crate::promql::functions::{PromQLArg, PromQLFunction};
use crate::promql::{EvalResult, ExprResult};

#[derive(Clone, Copy, Debug)]
pub(in crate::promql) struct IDeltaFunction;
impl PromQLFunction for IDeltaFunction {
    fn apply(&self, arg: PromQLArg, eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        let samples = arg.into_range_vector()?;
        Ok(eval_range(samples, eval_timestamp_ms, |samples| {
            if samples.len() < 2 {
                return None;
            }
            let first = &samples[0];
            let last = &samples[samples.len() - 1];
            if first.timestamp == last.timestamp {
                return None;
            }
            Some(last.value - first.value)
        }))
    }
}
