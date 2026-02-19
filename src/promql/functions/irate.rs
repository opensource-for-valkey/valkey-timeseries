use crate::common::Sample;
use crate::promql::functions::{PromQLArg, PromQLFunction};
use crate::promql::{EvalResult, EvalSample, ExprResult};
use orx_parallel::IntoParIter;
use orx_parallel::ParIter;

#[derive(Copy, Clone)]
pub(in crate::promql) struct IRateFunction;

impl PromQLFunction for IRateFunction {
    fn apply(&self, arg: PromQLArg, eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        let series = arg.into_range_vector()?;

        let out = series
            .into_par()
            .filter_map(|series| {
                let value = irate_value(&series.values)?;

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

fn irate_value(samples: &[Sample]) -> Option<f64> {
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
    Some(dv / (dt as f64 / 1000_f64))
}
