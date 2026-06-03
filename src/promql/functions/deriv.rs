use crate::promql::common::math::sample_regression;
use crate::promql::functions::{PromQLArg, PromQLFunction};
use crate::promql::{EvalContext, EvalResult, EvalSample, ExprResult};
use orx_parallel::{IntoParIter, ParIter};

#[derive(Copy, Clone)]
pub(in crate::promql) struct DerivFunction;

impl PromQLFunction for DerivFunction {
    fn apply(&self, arg: PromQLArg, ctx: &EvalContext) -> EvalResult<ExprResult> {
        let series = arg.into_range_vector()?;

        let out = series
            .into_par()
            .filter_map(|series| {
                let (slope, _intercept) = sample_regression(&series.values)?;
                if slope.is_nan() {
                    return None;
                }

                Some(EvalSample {
                    labels: series.labels,
                    timestamp_ms: ctx.evaluation_ts,
                    value: slope,
                    drop_name: false,
                })
            })
            .collect();

        Ok(ExprResult::InstantVector(out))
    }
}
