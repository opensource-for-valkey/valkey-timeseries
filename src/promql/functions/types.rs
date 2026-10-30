use crate::common::Timestamp;
use crate::promql::{
    EvalContext, EvalResult, EvalSample, EvalSamples, EvaluationError, ExprResult,
};
use promql_parser::parser::Expr;
use promql_parser::parser::value::ValueType;
use std::ops::Deref;

/// What a function sees at its call site: the evaluation context, plus the
/// *unevaluated* argument expressions.
///
/// Almost every function is a pure map from evaluated arguments to a result and
/// wants nothing to do with the AST. `absent`/`absent_over_time` are the
/// exception: their output *labels* are derived from the argument selector's
/// matchers rather than from any data (see
/// [`crate::promql::functions::utils::labels_for_absent`]), so the expression
/// has to reach them. This context is how.
pub(crate) struct FunctionCallContext<'a> {
    pub eval_context: &'a EvalContext,
    pub raw_args: &'a [Box<Expr>],
}

impl<'a> FunctionCallContext<'a> {
    pub fn new(eval_context: &'a EvalContext, raw_args: &'a [Box<Expr>]) -> Self {
        Self {
            eval_context,
            raw_args,
        }
    }

    /// The unevaluated expression for argument `idx`, if the call had one.
    pub fn raw_arg(&self, idx: usize) -> Option<&'a Expr> {
        self.raw_args.get(idx).map(|arg| &**arg)
    }
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

/// Trait for PromQL functions
pub(crate) trait PromQLFunction {
    /// Apply the function to the input samples.
    /// `ctx` provides the evaluation context (timestamp, query range, step, etc.).
    /// Returns an `ExprResult` (typically `ExprResult::InstantVector`).
    ///
    /// Only the one-argument functions implement this. The evaluator always
    /// enters through [`Self::apply_call`], after the parser has checked the
    /// arity and the evaluator the argument types, so a function taking any
    /// other number of arguments overrides [`Self::apply_args`] or
    /// [`Self::apply_call`] instead and never sees this default.
    fn apply(&self, _arg: PromQLArg, _ctx: &EvalContext) -> EvalResult<ExprResult> {
        Err(EvaluationError::InternalError(
            "function does not take exactly one argument".to_string(),
        ))
    }

    /// Apply the function to one or more evaluated arguments.
    ///
    /// Default behavior preserves current unary-function semantics.
    fn apply_args(&self, mut args: Vec<PromQLArg>, ctx: &EvalContext) -> EvalResult<ExprResult> {
        if args.len() != 1 {
            return Err(EvaluationError::InternalError(format!(
                "function requires exactly one argument, got {}",
                args.len()
            )));
        }

        self.apply(args.swap_remove(0), ctx)
    }

    /// Apply the function at a call site, with the unevaluated argument
    /// expressions available alongside the evaluated ones.
    ///
    /// Override this only when the AST is actually needed; `FunctionCallContext`
    /// derefs to `EvalContext`, so an implementation that ignores `raw_args`
    /// reads exactly as it would have with a bare context.
    fn apply_call(
        &self,
        evaluated_args: Vec<PromQLArg>,
        ctx: &FunctionCallContext,
    ) -> EvalResult<ExprResult> {
        self.apply_args(evaluated_args, ctx.eval_context)
    }
}

/// One window's view for a rollup function: the samples inside
/// `(t - range, t]`, de-interleaved into parallel arrays.
///
/// Nothing outside the window is offered. The local per-step path loads
/// exactly the window, while the pushed-down grid path slices it out of the
/// union of a grid's windows, so a kernel that could see its neighbours would
/// answer differently depending on whether push-down was available.
/// `every_kind_ignores_samples_outside_the_window` pins that the two paths
/// agree for every [`crate::promql::functions::RollupKind`].
#[derive(Default, Clone, Debug)]
pub(super) struct RollupWindow<'a> {
    /// Values that fit the window.
    pub(crate) values: &'a [f64],

    /// Timestamps for values.
    pub(crate) timestamps: &'a [Timestamp],
}
