//! Single-source-of-truth macro prototype for PromQL function mappings.
//! This module defines the `PromqlFunctionKind` enum and the `PromQLFunctionImpl` dispatch enum, both
//!
use crate::promql::functions::date_functions::{
    DayOfMonthFunction, DayOfWeekFunction, DayOfYearFunction, DaysInMonthFunction, HourFunction,
    MinuteFunction, MonthFunction, TimeFunction, TimestampFunction, YearFunction,
};
use crate::promql::functions::deriv::DerivFunction;
use crate::promql::functions::histogram::{HistogramFractionFunctions, HistogramQuantileFunction};
use crate::promql::functions::holt_winters::DoubleExponentialSmoothingFunction;
use crate::promql::functions::irate::IRateFunction;
use crate::promql::functions::labels::{LabelJoinFunction, LabelReplaceFunction};
use crate::promql::functions::math_functions::{
    AbsFunction, AcosFunction, AcoshFunction, AsinFunction, AsinhFunction, AtanFunction,
    AtanhFunction, CeilFunction, ClampFunction, ClampMaxFunction, ClampMinFunction, CosFunction,
    CoshFunction, DegFunction, ExpFunction, FloorFunction, LnFunction, Log2Function, Log10Function,
    PiFunction, RadFunction, RoundFunction, SgnFunction, SinFunction, SinhFunction, SqrtFunction,
    TanFunction, TanhFunction,
};
use crate::promql::functions::predict_linear::PredictLinearFunction;
use crate::promql::functions::range_vector_functions::{
    AbsentOverTimeFunction, AvgOverTimeFunction, ChangesFunction, CountOverTimeFunction,
    FirstOverTimeFunction, LastOverTimeFunction, MadOverTimeFunction, MaxOverTimeFunction,
    MinOverTimeFunction, PresentOverTimeFunction, QuantileOverTimeFunction, ResetsFunction,
    StddevOverTimeFunction, StdvarOverTimeFunction, SumOverTimeFunction, TSFirstOverTimeFunction,
    TSLastOverTimeFunction, TSOfMaxOverTimeFunction, TSOfMinOverTimeFunction,
};
use crate::promql::functions::rate::{DeltaFunction, RateFunction};
use crate::promql::functions::sort::{
    SortByLabelDescFunction, SortByLabelFunction, SortDescFunction, SortFunction,
};
use crate::promql::functions::special_functions::{AbsentFunction, ScalarFunction, VectorFunction};
use std::fmt::Display;
use strum_macros::EnumIter;
use valkey_module::ValkeyError;

macro_rules! impl_promql_function_kind {
    (
        $( ($Variant:ident, $name:expr, $Ty:ident) ),*
        $(,)?
    ) => {

        #[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, EnumIter)]
        pub enum PromqlFunctionKind {
            $( $Variant, )+
        }

        impl PromqlFunctionKind {
            #[allow(dead_code)]
            pub fn name(&self) -> &'static str {
                use PromqlFunctionKind::*;
                match self {
                    $( $Variant => $name, )*
                }
            }
        }

        impl Display for PromqlFunctionKind {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let s = self.name();
                write!(f, "{}", s)
            }
        }

        impl TryFrom<&str> for PromqlFunctionKind {
            type Error = ValkeyError;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                let v = hashify::tiny_map_ignore_case! {
                    value.as_bytes(),
                    $( $name => PromqlFunctionKind::$Variant, )*
                };

                match v {
                    Some(f) => Ok(f),
                    None => Err(ValkeyError::Str("PromQL: unknown function")),
                }
            }
        }
    };
}

macro_rules! impl_promql_function_impl {
    (
        $( ($Variant:ident, $name:expr, $Ty:ident) ),*
        $(,)?
    ) => {
        use $crate::promql::functions::types::PromQLArg;
        use $crate::promql::{EvalResult, ExprResult};
        use $crate::promql::functions::types::PromQLFunction;

        #[allow(clippy::large_enum_variant)]
        #[derive(Copy, Clone)]
        pub(in crate::promql) enum PromQLFunctionImpl {
            $( $Variant($Ty), )*
        }

        impl PromQLFunctionImpl {
            #[allow(dead_code)]
            pub fn name(&self) -> &'static str {
                match self {
                    $( Self::$Variant(_) => $name, )*
                }
            }

            #[inline]
            pub(crate) fn from_kind(kind: PromqlFunctionKind) -> Option<Self> {
                use PromqlFunctionKind::*;
                Some(match kind {
                    $( $Variant => Self::$Variant($Ty), )*
                })
            }

            pub(crate) fn kind(&self) -> PromqlFunctionKind {
                match self {
                    $( Self::$Variant(_) => PromqlFunctionKind::$Variant, )*
                }
            }

            pub(crate) fn from_name(name: &str) -> Option<Self> {
               let Ok(kind) = PromqlFunctionKind::try_from(name) else {
                   return None;
               };

               Self::from_kind(kind)
            }
        }

        impl Display for PromQLFunctionImpl {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.name())
            }
        }

        impl std::fmt::Debug for PromQLFunctionImpl {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                // todo: get metadata from promql_parser
                write!(f, "{}()", self.name())
            }
        }

        impl PromQLFunction for PromQLFunctionImpl {
            #[inline]
            fn apply(&self, arg: PromQLArg, eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
                match self {
                    $( PromQLFunctionImpl::$Variant(f) => f.apply(arg, eval_timestamp_ms), )*
                }
            }

            #[inline]
            fn apply_args(&self, args: Vec<PromQLArg>, eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
                match self {
                    $( PromQLFunctionImpl::$Variant(f) => f.apply_args(args, eval_timestamp_ms), )*
                }
            }

            #[inline]
            fn apply_call(&self, evaluated_args: Vec<PromQLArg>, ctx: &$crate::promql::functions::types::FunctionCallContext<'_>) -> EvalResult<ExprResult> {
                match self {
                    $( PromQLFunctionImpl::$Variant(f) => f.apply_call(evaluated_args, ctx), )*
                }
            }

            #[inline]
            fn is_experimental(&self) -> bool {
                match self {
                    $( PromQLFunctionImpl::$Variant(f) => f.is_experimental(), )*
                }
            }
        }
    };
}
// ─────────────────────────────────────────────────────────────────────────────
// Single canonical list.
//
// ADD NEW FUNCTIONS HERE.  Each entry:
//   (MyFunction, "my_function", MyFunction)
//
// Import Concrete type:
// use crate:promql::functions::new_function::MyFunction;

// The `promql_function_list!` macro below feeds the full list to whatever
// macro you pass as an argument, enabling reuse for multiple purposes (e.g.,
// also generating the dispatch arms or a registry table).

/// Feeds the canonical function list to `$target_macro` as a comma-separated
/// sequence of `(Variant, name, Type)` tuples.
///
/// ```ignore
/// // Generate from_kind + kind in one shot:
/// promql_function_list!(impl_promql_function_methods);
/// ```
#[macro_export]
macro_rules! promql_function_list {
    ($target_macro:path) => {
        $target_macro!(
            // ── Math / unary ─────────────────────────────────────────────
            (Abs, "abs", AbsFunction),
            (Acos, "acos", AcosFunction),
            (Acosh, "acosh", AcoshFunction),
            (Asin, "asin", AsinFunction),
            (Asinh, "asinh", AsinhFunction),
            (Atan, "atan", AtanFunction),
            (Atanh, "atanh", AtanhFunction),
            (Clamp, "clamp", ClampFunction),
            (ClampMax, "clamp_max", ClampMaxFunction),
            (ClampMin, "clamp_min", ClampMinFunction),
            (Ceil, "ceil", CeilFunction),
            (Cos, "cos", CosFunction),
            (Cosh, "cosh", CoshFunction),
            (Deg, "deg", DegFunction),
            (Exp, "exp", ExpFunction),
            (Floor, "floor", FloorFunction),
            (Ln, "ln", LnFunction),
            (Log10, "log10", Log10Function),
            (Log2, "log2", Log2Function),
            (Pi, "pi", PiFunction),
            (Rad, "rad", RadFunction),
            (Round, "round", RoundFunction),
            (Sgn, "sgn", SgnFunction),
            (Sin, "sin", SinFunction),
            (Sinh, "sinh", SinhFunction),
            (Sqrt, "sqrt", SqrtFunction),
            (Tan, "tan", TanFunction),
            (Tanh, "tanh", TanhFunction),
            // ── Date / time ──────────────────────────────────────────────
            (DayOfMonth, "day_of_month", DayOfMonthFunction),
            (DayOfWeek, "day_of_week", DayOfWeekFunction),
            (DayOfYear, "day_of_year", DayOfYearFunction),
            (DaysInMonth, "days_in_month", DaysInMonthFunction),
            (Hour, "hour", HourFunction),
            (Minute, "minute", MinuteFunction),
            (Month, "month", MonthFunction),
            (Year, "year", YearFunction),
            (Timestamp, "timestamp", TimestampFunction),
            (Time, "time", TimeFunction),
            // ── Special ──────────────────────────────────────────────────
            (Absent, "absent", AbsentFunction),
            (Scalar, "scalar", ScalarFunction),
            (Vector, "vector", VectorFunction),
            // ── Histogram ────────────────────────────────────────────────
            (
                HistogramQuantile,
                "histogram_quantile",
                HistogramQuantileFunction
            ),
            (
                HistogramFraction,
                "histogram_fraction",
                HistogramFractionFunctions
            ),
            // ── Range-vector ─────────────────────────────────────────────
            (AbsentOverTime, "absent_over_time", AbsentOverTimeFunction),
            (AvgOverTime, "avg_over_time", AvgOverTimeFunction),
            (Changes, "changes", ChangesFunction),
            (CountOverTime, "count_over_time", CountOverTimeFunction),
            (Delta, "delta", DeltaFunction),
            (Deriv, "deriv", DerivFunction),
            (
                DoubleExponentialSmoothing,
                "double_exponential_smoothing",
                DoubleExponentialSmoothingFunction
            ),
            (FirstOverTime, "first_over_time", FirstOverTimeFunction),
            (IRate, "irate", IRateFunction),
            (LastOverTime, "last_over_time", LastOverTimeFunction),
            (MadOverTime, "mad_over_time", MadOverTimeFunction),
            (MaxOverTime, "max_over_time", MaxOverTimeFunction),
            (MinOverTime, "min_over_time", MinOverTimeFunction),
            (PredictLinear, "predict_linear", PredictLinearFunction),
            (
                PresentOverTime,
                "present_over_time",
                PresentOverTimeFunction
            ),
            (
                QuantileOverTime,
                "quantile_over_time",
                QuantileOverTimeFunction
            ),
            (Rate, "rate", RateFunction),
            (Resets, "resets", ResetsFunction),
            (StddevOverTime, "stddev_over_time", StddevOverTimeFunction),
            (StdvarOverTime, "stdvar_over_time", StdvarOverTimeFunction),
            (SumOverTime, "sum_over_time", SumOverTimeFunction),
            (
                TsOfMaxOverTime,
                "ts_of_max_over_time",
                TSOfMaxOverTimeFunction
            ),
            (
                TsOfMinOverTime,
                "ts_of_min_over_time",
                TSOfMinOverTimeFunction
            ),
            (
                TLastOverTime,
                "ts_of_last_over_time",
                TSLastOverTimeFunction
            ),
            (
                TFirstOverTime,
                "ts_of_first_over_time",
                TSFirstOverTimeFunction
            ),
            // ── Label helpers ─────────────────────────────────────────────
            (LabelJoin, "label_join", LabelJoinFunction),
            (LabelReplace, "label_replace", LabelReplaceFunction),
            // ── Sort helpers ──────────────────────────────────────────────
            (Sort, "sort", SortFunction),
            (SortDesc, "sort_desc", SortDescFunction),
            (SortByLabel, "sort_by_label", SortByLabelFunction),
            (
                SortByLabelDesc,
                "sort_by_label_desc",
                SortByLabelDescFunction
            ),
        );
    };
}

// Expand the canonical list into this module. This will generate the
// `PromqlFunctionKind` enum and the helper impls for `PromQLFunctionImpl`.
promql_function_list!(impl_promql_function_kind);
promql_function_list!(impl_promql_function_impl);
