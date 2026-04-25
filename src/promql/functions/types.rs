use crate::common::Timestamp;
use crate::promql::{
    EvalContext, EvalResult, EvalSample, EvalSamples, EvaluationError, ExprResult,
};
use promql_parser::parser::Expr;
use promql_parser::parser::value::ValueType;
use std::ops::Deref;

pub(crate) struct FunctionCallContext<'a> {
    pub eval_context: &'a EvalContext,
    pub raw_args: &'a [Box<Expr>],
}

impl<'a> Deref for FunctionCallContext<'a> {
    type Target = EvalContext;

    fn deref(&self) -> &Self::Target {
        self.eval_context
    }
}

#[derive(Debug, Clone)]
pub(crate) enum PromQLArg {
    String(String),
    InstantVector(Vec<EvalSample>),
    Scalar(f64),
    RangeVector(Vec<EvalSamples>),
}

impl PromQLArg {
    #[cfg(test)]
    pub fn empty_string() -> Self {
        Self::String(String::new())
    }

    pub fn into_instant_vector(self) -> EvalResult<Vec<EvalSample>> {
        match self {
            Self::InstantVector(s) => Ok(s),
            _ => Err(EvaluationError::InternalError(
                "expected instant vector".to_string(),
            )),
        }
    }

    pub fn into_scalar(self) -> EvalResult<f64> {
        match self {
            Self::Scalar(s) => Ok(s),
            _ => Err(EvaluationError::InternalError(
                "expected scalar".to_string(),
            )),
        }
    }

    pub fn into_range_vector(self) -> EvalResult<Vec<EvalSamples>> {
        match self {
            Self::RangeVector(samples) => Ok(samples),
            _ => Err(EvaluationError::InternalError(
                "expected range vector".to_string(),
            )),
        }
    }

    pub fn into_string(self) -> EvalResult<String> {
        match self {
            Self::String(s) => Ok(s),
            _ => Err(EvaluationError::InternalError(
                "expected string".to_string(),
            )),
        }
    }

    pub fn as_string(&self) -> EvalResult<&String> {
        match self {
            Self::String(s) => Ok(s),
            _ => Err(EvaluationError::InternalError(
                "expected string".to_string(),
            )),
        }
    }

    pub fn value_type(&self) -> ValueType {
        match self {
            PromQLArg::String(_) => ValueType::String,
            PromQLArg::Scalar(_) => ValueType::Scalar,
            PromQLArg::InstantVector(_) => ValueType::Vector,
            PromQLArg::RangeVector(_) => ValueType::Matrix,
        }
    }
}

impl From<ExprResult> for PromQLArg {
    fn from(value: ExprResult) -> Self {
        match value {
            ExprResult::String(x) => Self::String(x),
            ExprResult::Scalar(x) => Self::Scalar(x),
            ExprResult::InstantVector(x) => Self::InstantVector(x),
            ExprResult::RangeVector(x) => Self::RangeVector(x),
        }
    }
}

impl From<f64> for PromQLArg {
    fn from(value: f64) -> Self {
        Self::Scalar(value)
    }
}

impl From<usize> for PromQLArg {
    fn from(value: usize) -> Self {
        Self::Scalar(value as f64)
    }
}

impl From<String> for PromQLArg {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for PromQLArg {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}

/// Trait for PromQL functions that operate on instant vectors
pub(crate) trait PromQLFunction {
    /// Apply the function to the input samples.
    /// `eval_timestamp_ms` is the evaluation timestamp in milliseconds since UNIX epoch.
    /// Returns an `ExprResult` (typically `ExprResult::InstantVector`).
    fn apply(&self, arg: PromQLArg, eval_timestamp_ms: i64) -> EvalResult<ExprResult>;

    /// Apply the function to one or more evaluated arguments.
    ///
    /// Default behavior preserves current unary-function semantics.
    fn apply_args(
        &self,
        mut args: Vec<PromQLArg>,
        eval_timestamp_ms: i64,
    ) -> EvalResult<ExprResult> {
        if args.len() != 1 {
            return Err(EvaluationError::InternalError(format!(
                "function requires exactly one argument, got {}",
                args.len()
            )));
        }

        self.apply(args.swap_remove(0), eval_timestamp_ms)
    }

    /// Apply the function to evaluated arguments provided as a slice.
    ///
    /// This helper avoids allocating a temporary `Vec` in the common unary
    /// case by directly calling `apply` when there is exactly one argument.
    /// Callers that already have a `Vec` can use `apply_args` directly.
    fn apply_args_slice(
        &self,
        args: &[PromQLArg],
        eval_timestamp_ms: i64,
    ) -> EvalResult<ExprResult> {
        if args.len() != 1 {
            return Err(EvaluationError::InternalError(format!(
                "function requires exactly one argument, got {}",
                args.len()
            )));
        }

        // Clone the single argument and delegate to `apply`.
        self.apply(args[0].clone(), eval_timestamp_ms)
    }

    fn apply_call(
        &self,
        evaluated_args: Vec<PromQLArg>,
        ctx: &EvalContext,
    ) -> EvalResult<ExprResult> {
        self.apply_args(evaluated_args, ctx.evaluation_ts)
    }
}

/// Function that applies a unary operation to each sample
pub(super) struct UnaryFunction {
    pub(super) op: fn(f64) -> f64,
}

impl PromQLFunction for UnaryFunction {
    fn apply(&self, arg: PromQLArg, _eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        let mut samples = arg.into_instant_vector()?;
        for sample in &mut samples {
            sample.value = (self.op)(sample.value);
        }
        Ok(ExprResult::InstantVector(samples))
    }
}

impl Default for UnaryFunction {
    fn default() -> Self {
        fn identity(x: f64) -> f64 {
            x
        }
        UnaryFunction { op: identity }
    }
}

pub struct RangeFunctionOpts {
    pub step_ms: i64,
}

#[derive(Default, Clone, Debug)]
pub(super) struct RollupWindow<'a> {
    /// The value preceding values if it fits the staleness interval.
    pub(super) prev_value: f64,

    /// The timestamp for prev_value.
    pub(super) prev_timestamp: Timestamp,

    /// Values that fit the window ending at curr_timestamp.
    pub(crate) values: &'a [f64],

    /// Timestamps for values.
    pub(crate) timestamps: &'a [Timestamp],

    /// Real value preceding value
    /// Populated if the preceding value is within the staleness interval.
    pub(super) real_prev_value: f64,

    /// Real value that goes after values.
    pub(crate) real_next_value: f64,

    /// Current timestamp for rollup evaluation.
    pub(super) curr_timestamp: Timestamp,

    /// Index for the currently evaluated point relative to the time range for query evaluation.
    pub(super) idx: usize,

    /// Time window for rollup calculations.
    pub(super) window: i64,
}
