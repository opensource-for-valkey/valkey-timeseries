use super::go_compat::{cosh, sinh};
use crate::promql::common::math::{max_with_nan, min_with_nan};
use crate::promql::functions::types::{PromQLArg, PromQLFunction};
use crate::promql::functions::utils::{exact_arity_error, map_scalar_or_vector};
use crate::promql::{EvalResult, EvalSample, EvaluationError, ExprResult};

#[inline]
fn exec_unary_fn(arg: PromQLArg, f: fn(f64) -> f64) -> EvalResult<ExprResult> {
    map_scalar_or_vector(arg, f)
}

macro_rules! make_unary_function {
    ( $name: ident, $rf: expr ) => {
        #[derive(Copy, Clone, Default)]
        pub(crate) struct $name;

        impl $name {
            pub fn new() -> Self {
                Self
            }
        }

        impl PromQLFunction for $name {
            fn apply(&self, arg: PromQLArg, _eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
                exec_unary_fn(arg, $rf)
            }
        }
    };
}

make_unary_function!(AbsFunction, f64::abs);
make_unary_function!(AcosFunction, f64::acos);
make_unary_function!(AcoshFunction, f64::acosh);
make_unary_function!(AsinFunction, f64::asin);
make_unary_function!(AsinhFunction, f64::asinh);
make_unary_function!(AtanFunction, f64::atan);
make_unary_function!(AtanhFunction, f64::atanh);
make_unary_function!(CeilFunction, f64::ceil);
make_unary_function!(CosFunction, f64::cos);
make_unary_function!(CoshFunction, cosh);
make_unary_function!(DegFunction, f64::to_degrees);
make_unary_function!(ExpFunction, f64::exp);
make_unary_function!(FloorFunction, f64::floor);
make_unary_function!(LnFunction, f64::ln);
make_unary_function!(Log10Function, f64::log10);
make_unary_function!(Log2Function, f64::log2);
make_unary_function!(RadFunction, f64::to_radians);
make_unary_function!(SgnFunction, f64::signum);
make_unary_function!(SinFunction, f64::sin);
make_unary_function!(SinhFunction, sinh);
make_unary_function!(SqrtFunction, f64::sqrt);
make_unary_function!(TanFunction, f64::tan);
make_unary_function!(TanhFunction, f64::tanh);

/// Pi function: returns PI encoded as a single-sample vector.
#[derive(Copy, Clone)]
pub(in crate::promql) struct PiFunction;

impl PromQLFunction for PiFunction {
    fn apply(&self, _arg: PromQLArg, _eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        Err(exact_arity_error("pi", 0, 1))
    }

    fn apply_args(&self, args: Vec<PromQLArg>, eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        if !args.is_empty() {
            return Err(exact_arity_error("pi", 0, args.len()));
        }

        Ok(ExprResult::InstantVector(vec![EvalSample {
            timestamp_ms: eval_timestamp_ms,
            value: std::f64::consts::PI,
            labels: Default::default(),
            drop_name: false,
        }]))
    }
}

/// Round function with an optional scalar second argument (`to_nearest`).
#[derive(Copy, Clone)]
pub(in crate::promql) struct RoundFunction;

impl RoundFunction {
    fn round_to_nearest(value: f64, to_nearest: f64) -> f64 {
        if to_nearest == 0.0 {
            return value;
        }
        let inv = 1.0 / to_nearest;
        (value * inv + 0.5).floor() / inv
    }
}

impl PromQLFunction for RoundFunction {
    fn apply(&self, arg: PromQLArg, _eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        let mut samples = arg.into_instant_vector()?;
        for sample in &mut samples {
            sample.value = Self::round_to_nearest(sample.value, 1.0);
        }
        Ok(ExprResult::InstantVector(samples))
    }

    fn apply_args(&self, args: Vec<PromQLArg>, _eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        let mut args_iter = args.into_iter();
        let Some(first_arg) = args_iter.next() else {
            return Err(EvaluationError::InternalError(
                "round requires at least one argument".to_string(),
            ));
        };
        let mut samples = first_arg.into_instant_vector()?;
        let to_nearest = match args_iter.next() {
            None => 1.0,
            Some(second_arg) => {
                // Keep this defensive arity guard because function handlers can
                // still be called directly in tests/internal paths.
                if args_iter.next().is_some() {
                    return Err(EvaluationError::InternalError(
                        "round accepts at most two arguments".to_string(),
                    ));
                }
                second_arg.into_scalar()?.abs()
            }
        };

        for sample in &mut samples {
            sample.value = Self::round_to_nearest(sample.value, to_nearest);
        }
        Ok(ExprResult::InstantVector(samples))
    }
}

#[derive(Copy, Clone)]
pub(in crate::promql) struct ClampMaxFunction;

impl PromQLFunction for ClampMaxFunction {
    fn apply(&self, _arg: PromQLArg, _eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        Err(exact_arity_error("clamp_max", 2, 1))
    }

    fn apply_args(&self, args: Vec<PromQLArg>, _eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        if args.len() != 2 {
            return Err(exact_arity_error("clamp_max", 2, args.len()));
        }

        let mut args_iter = args.into_iter();
        let first_arg = args_iter.next().expect("validated args.len() == 2");
        let second_arg = args_iter.next().expect("validated args.len() == 2");

        let mut samples = first_arg.into_instant_vector()?;
        let max = second_arg.into_scalar()?;
        for sample in &mut samples {
            sample.value = min_with_nan(sample.value, max);
        }
        Ok(ExprResult::InstantVector(samples))
    }
}

#[derive(Copy, Clone)]
pub(in crate::promql) struct ClampMinFunction;

impl PromQLFunction for ClampMinFunction {
    fn apply(&self, _arg: PromQLArg, _eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        Err(exact_arity_error("clamp_min", 2, 1))
    }

    fn apply_args(&self, args: Vec<PromQLArg>, _eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        if args.len() != 2 {
            return Err(exact_arity_error("clamp_min", 2, args.len()));
        }

        let mut args_iter = args.into_iter();
        let first_arg = args_iter.next().expect("validated args.len() == 2");
        let second_arg = args_iter.next().expect("validated args.len() == 2");

        let mut samples = first_arg.into_instant_vector()?;
        let min = second_arg.into_scalar()?;
        for sample in &mut samples {
            sample.value = max_with_nan(sample.value, min);
        }
        Ok(ExprResult::InstantVector(samples))
    }
}

#[derive(Copy, Clone)]
pub(in crate::promql) struct ClampFunction;

impl PromQLFunction for ClampFunction {
    fn apply(&self, _arg: PromQLArg, _eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        Err(exact_arity_error("clamp", 3, 1))
    }

    fn apply_args(&self, args: Vec<PromQLArg>, _eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        if args.len() != 3 {
            return Err(exact_arity_error("clamp", 3, args.len()));
        }

        let mut args_iter = args.into_iter();
        let first_arg = args_iter.next().expect("validated args.len() == 3");
        let second_arg = args_iter.next().expect("validated args.len() == 3");
        let third_arg = args_iter.next().expect("validated args.len() == 3");

        let mut samples = first_arg.into_instant_vector()?;
        let min = second_arg.into_scalar()?;
        let max = third_arg.into_scalar()?;
        if min > max {
            return Ok(ExprResult::InstantVector(vec![]));
        }

        for sample in &mut samples {
            sample.value = max_with_nan(min_with_nan(sample.value, max), min);
        }
        Ok(ExprResult::InstantVector(samples))
    }
}
