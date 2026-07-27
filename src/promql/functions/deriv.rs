use crate::common::Sample;
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
                let value = rollup_deriv(&series.values)?;
                Some(EvalSample {
                    labels: series.labels,
                    timestamp_ms: ctx.evaluation_ts,
                    value,
                    drop_name: false,
                })
            })
            .collect();

        Ok(ExprResult::InstantVector(out))
    }
}

/// `deriv` over one window: the slope of the simple linear regression through
/// its samples, or nothing when there are too few samples or the slope is NaN.
///
/// Named rather than inline so the pushed-down path reduces a window with the
/// very same function the local path runs.
pub(in crate::promql) fn rollup_deriv(values: &[Sample]) -> Option<f64> {
    let (slope, _intercept) = sample_regression(values)?;
    (!slope.is_nan()).then_some(slope)
}
