use crate::promql::common::math::sample_regression;
use crate::promql::functions::{PromQLArg, PromQLFunction};
use crate::promql::{EvalResult, EvalSample, EvalSamples, ExprResult};
use orx_parallel::{IntoParIter, ParIter};

#[derive(Copy, Clone)]
pub(in crate::promql) struct DerivFunction;

impl PromQLFunction for DerivFunction {
    fn apply(&self, arg: PromQLArg, eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        let series = arg.into_range_vector()?;

        let out = series
            .into_par()
            .filter_map(|series| {
                let value = deriv_value(&series)?;

                Some(EvalSample {
                    labels: series.labels,
                    timestamp_ms: eval_timestamp_ms,
                    value,
                    drop_name: false,
                })
            })
            .collect();

        Ok(ExprResult::InstantVector(out))
    }
}

fn deriv_value(series: &EvalSamples) -> Option<f64> {
    let (slope, _intercept) = sample_regression(&series.values)?;
    if slope.is_nan() {
        return None;
    }
    Some(slope)
}
