use crate::common::Sample;
use crate::promql::common::math::sample_regression;
use crate::promql::functions::rollups::eval_series;
use crate::promql::functions::{PromQLArg, PromQLFunction};
use crate::promql::{EvalContext, EvalResult, ExprResult};

#[derive(Copy, Clone)]
pub(in crate::promql) struct DerivFunction;

impl PromQLFunction for DerivFunction {
    fn apply(&self, arg: PromQLArg, ctx: &EvalContext) -> EvalResult<ExprResult> {
        let series = arg.into_range_vector()?;
        Ok(ExprResult::InstantVector(eval_series(ctx, series, |s| {
            rollup_deriv(&s.values)
        })?))
    }
}

/// `deriv` over one window: the slope of the simple linear regression through
/// its samples, or nothing when there are too few samples. A NaN or infinite
/// sample in the window poisons the slope to NaN, which is returned as the
/// series' value rather than dropping the series, matching Prometheus.
///
/// Named rather than inline so the pushed-down path reduces a window with the
/// very same function the local path runs.
pub(in crate::promql) fn rollup_deriv(values: &[Sample]) -> Option<f64> {
    let (slope, _intercept) = sample_regression(values)?;
    Some(slope)
}
