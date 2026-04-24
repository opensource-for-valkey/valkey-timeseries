use crate::promql::common::math::sample_regression;
use crate::promql::functions::range_vector_functions::eval_range;
use crate::promql::functions::utils::{expect_scalar, min_arity_error};
use crate::promql::functions::{PromQLArg, PromQLFunction};
use crate::promql::{EvalResult, ExprResult};

/// `predict_linear(v range-vector, t scalar)`
///
/// predicts the value of time series t seconds from now, based on the range vector v, using simple linear regression.
#[derive(Copy, Clone)]
pub(in crate::promql) struct PredictLinearFunction;

impl PromQLFunction for PredictLinearFunction {
    fn apply(&self, _arg: PromQLArg, _eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        Err(min_arity_error("predict_linear", 2, 1))
    }

    fn apply_args(&self, args: Vec<PromQLArg>, eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        if args.len() != 2 {
            return Err(min_arity_error("predict_linear", 2, args.len()));
        }
        let mut arg_iter = args.into_iter();
        let range_arg = arg_iter.next().unwrap();
        let secs_arg = arg_iter.next().unwrap();

        let series = range_arg.into_range_vector()?;
        let seconds_ahead = expect_scalar(secs_arg, "predict_linear", "t")?;

        if series.len() < 2 {
            return Ok(ExprResult::InstantVector(vec![]));
        }

        let eval_time = eval_timestamp_ms as f64 / 1000_f64;
        let result = eval_range(series, eval_timestamp_ms, |samples| {
            let (slope, intercept) = sample_regression(samples)?;

            let origin = samples
                .first()
                .map(|sample| sample.timestamp)
                .unwrap_or(eval_timestamp_ms) as f64
                / 1_000f64;

            let x = eval_time + seconds_ahead - origin;
            let value = slope * x + intercept;

            Some(value)
        });

        Ok(result)
    }
}
