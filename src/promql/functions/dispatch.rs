//! Static-dispatch enum wrapping all concrete [`PromQLFunction`] implementations.
//!
//!
//! # Usage
//!
//! ```ignore
//! let f = PromQLFunctionImpl::from_name("abs")
//!             .expect("abs is implemented");
//! let result = f.apply(arg, eval_timestamp_ms)?;
//! ```
use crate::promql::functions::date_functions::{
    DayOfMonthFunction, DayOfWeekFunction, DayOfYearFunction, DaysInMonthFunction, HourFunction,
    MinuteFunction, MonthFunction, TimeFunction, TimestampFunction, YearFunction,
};
use crate::promql::functions::deriv::DerivFunction;
use crate::promql::functions::function_kind::PromqlFunctionKind;
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
    MinOverTimeFunction, PresentOverTimeFunction, QuantileOverTimeFunction, RateFunction,
    ResetsFunction, StddevOverTimeFunction, StdvarOverTimeFunction, SumOverTimeFunction,
    TSFirstOverTimeFunction, TSLastOverTimeFunction, TSOfMaxOverTimeFunction,
    TSOfMinOverTimeFunction,
};
use crate::promql::functions::rate::DeltaFunction;
use crate::promql::functions::sort::{
    SortByLabelDescFunction, SortByLabelFunction, SortDescFunction, SortFunction,
};
use crate::promql::functions::special_functions::{AbsentFunction, ScalarFunction, VectorFunction};
use crate::promql::functions::types::{FunctionCallContext, PromQLArg, PromQLFunction};
use crate::promql::{EvalResult, ExprResult};

/// A statically dispatched enum of all known PromQL function implementations.
///
/// Each variant wraps the concrete zero-sized (or near-zero-sized) struct for
/// the corresponding function.  The [`PromQLFunction`] impl below delegates
/// every method to the inner type, giving the compiler full visibility to
/// inline the call site.
///
/// Use [`PromQLFunctionImpl::from_kind`] to construct an instance from a
/// [`PromqlFunctionKind`].
#[allow(clippy::large_enum_variant)]
#[derive(Copy, Clone)]
pub(in crate::promql) enum PromQLFunctionImpl {
    // ── Math / unary ─────────────────────────────────────────────────────────
    Abs(AbsFunction),
    Acos(AcosFunction),
    Acosh(AcoshFunction),
    Asin(AsinFunction),
    Asinh(AsinhFunction),
    Atan(AtanFunction),
    Atanh(AtanhFunction),
    Clamp(ClampFunction),
    ClampMax(ClampMaxFunction),
    ClampMin(ClampMinFunction),
    Ceil(CeilFunction),
    Cos(CosFunction),
    Cosh(CoshFunction),
    Deg(DegFunction),
    Exp(ExpFunction),
    Floor(FloorFunction),
    Ln(LnFunction),
    Log10(Log10Function),
    Log2(Log2Function),
    Pi(PiFunction),
    Rad(RadFunction),
    Round(RoundFunction),
    Sgn(SgnFunction),
    Sin(SinFunction),
    Sinh(SinhFunction),
    Sqrt(SqrtFunction),
    Tan(TanFunction),
    Tanh(TanhFunction),

    // ── Date / time ──────────────────────────────────────────────────────────
    DayOfMonth(DayOfMonthFunction),
    DayOfWeek(DayOfWeekFunction),
    DayOfYear(DayOfYearFunction),
    DaysInMonth(DaysInMonthFunction),
    Hour(HourFunction),
    Minute(MinuteFunction),
    Month(MonthFunction),
    Year(YearFunction),
    Timestamp(TimestampFunction),
    Time(TimeFunction),

    // ── Special ──────────────────────────────────────────────────────────────
    Absent(AbsentFunction),
    Scalar(ScalarFunction),
    Vector(VectorFunction),

    // ── Histogram ────────────────────────────────────────────────────────────
    HistogramQuantile(HistogramQuantileFunction),
    HistogramFraction(HistogramFractionFunctions),

    // ── Range-vector ─────────────────────────────────────────────────────────
    AbsentOverTime(AbsentOverTimeFunction),
    AvgOverTime(AvgOverTimeFunction),
    Changes(ChangesFunction),
    CountOverTime(CountOverTimeFunction),
    Delta(DeltaFunction),
    Deriv(DerivFunction),
    DoubleExponentialSmoothing(DoubleExponentialSmoothingFunction),
    FirstOverTime(FirstOverTimeFunction),
    IRate(IRateFunction),
    LastOverTime(LastOverTimeFunction),
    MadOverTime(MadOverTimeFunction),
    MaxOverTime(MaxOverTimeFunction),
    MinOverTime(MinOverTimeFunction),
    PredictLinear(PredictLinearFunction),
    PresentOverTime(PresentOverTimeFunction),
    QuantileOverTime(QuantileOverTimeFunction),
    Rate(RateFunction),
    Resets(ResetsFunction),
    StddevOverTime(StddevOverTimeFunction),
    StdvarOverTime(StdvarOverTimeFunction),
    SumOverTime(SumOverTimeFunction),
    TFirstOverTime(TSFirstOverTimeFunction),
    TLastOverTime(TSLastOverTimeFunction),
    TsOfMaxOverTime(TSOfMaxOverTimeFunction),
    TsOfMinOverTime(TSOfMinOverTimeFunction),

    // ── Label helpers ────────────────────────────────────────────────────────
    LabelJoin(LabelJoinFunction),
    LabelReplace(LabelReplaceFunction),

    // ── Sort helpers ─────────────────────────────────────────────────────────
    Sort(SortFunction),
    SortDesc(SortDescFunction),
    SortByLabel(SortByLabelFunction),
    SortByLabelDesc(SortByLabelDescFunction),
}

impl std::fmt::Debug for PromQLFunctionImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Delegate to the `PromqlFunctionKind` mapping so the debug
        // representation stays in sync with the function kind.
        write!(f, "{:?}", self.kind())
    }
}

impl PromQLFunctionImpl {
    pub(crate) fn from_name(name: &str) -> Option<Self> {
        let Ok(kind) = PromqlFunctionKind::try_from(name) else {
            return None;
        };

        Self::from_kind(kind)
    }

    /// Returns the [`PromQLFunctionImpl`] for `kind`, or `None` for names
    /// that do not yet have a concrete implementation.
    pub(crate) fn from_kind(kind: PromqlFunctionKind) -> Option<Self> {
        use PromqlFunctionKind::*;
        Some(match kind {
            // Math
            Abs => Self::Abs(AbsFunction),
            Acos => Self::Acos(AcosFunction),
            Acosh => Self::Acosh(AcoshFunction),
            Asin => Self::Asin(AsinFunction),
            Asinh => Self::Asinh(AsinhFunction),
            Atan => Self::Atan(AtanFunction),
            Atanh => Self::Atanh(AtanhFunction),
            Clamp => Self::Clamp(ClampFunction),
            ClampMax => Self::ClampMax(ClampMaxFunction),
            ClampMin => Self::ClampMin(ClampMinFunction),
            Ceil => Self::Ceil(CeilFunction),
            Cos => Self::Cos(CosFunction),
            Cosh => Self::Cosh(CoshFunction),
            Deg => Self::Deg(DegFunction),
            Exp => Self::Exp(ExpFunction),
            Floor => Self::Floor(FloorFunction),
            Ln => Self::Ln(LnFunction),
            Log10 => Self::Log10(Log10Function),
            Log2 => Self::Log2(Log2Function),
            Pi => Self::Pi(PiFunction),
            Rad => Self::Rad(RadFunction),
            Round => Self::Round(RoundFunction),
            Sgn => Self::Sgn(SgnFunction),
            Sin => Self::Sin(SinFunction),
            Sinh => Self::Sinh(SinhFunction),
            Sqrt => Self::Sqrt(SqrtFunction),
            Tan => Self::Tan(TanFunction),
            Tanh => Self::Tanh(TanhFunction),

            // Date/time
            DayOfMonth => Self::DayOfMonth(DayOfMonthFunction),
            DayOfWeek => Self::DayOfWeek(DayOfWeekFunction),
            DayOfYear => Self::DayOfYear(DayOfYearFunction),
            DaysInMonth => Self::DaysInMonth(DaysInMonthFunction),
            Hour => Self::Hour(HourFunction),
            Minute => Self::Minute(MinuteFunction),
            Month => Self::Month(MonthFunction),
            Year => Self::Year(YearFunction),
            Timestamp => Self::Timestamp(TimestampFunction),
            Time => Self::Time(TimeFunction),

            // Special
            Absent => Self::Absent(AbsentFunction),
            Scalar => Self::Scalar(ScalarFunction),
            Vector => Self::Vector(VectorFunction),

            // Histogram
            HistogramQuantile => Self::HistogramQuantile(HistogramQuantileFunction),
            HistogramFraction => Self::HistogramFraction(HistogramFractionFunctions),

            // Range-vector
            AbsentOverTime => Self::AbsentOverTime(AbsentOverTimeFunction),
            AvgOverTime => Self::AvgOverTime(AvgOverTimeFunction),
            Changes => Self::Changes(ChangesFunction),
            CountOverTime => Self::CountOverTime(CountOverTimeFunction),
            Delta => Self::Delta(DeltaFunction),
            Deriv => Self::Deriv(DerivFunction),
            DoubleExponentialSmoothing => {
                Self::DoubleExponentialSmoothing(DoubleExponentialSmoothingFunction)
            }
            FirstOverTime => Self::FirstOverTime(FirstOverTimeFunction),
            IRate => Self::IRate(IRateFunction),
            LastOverTime => Self::LastOverTime(LastOverTimeFunction),
            MadOverTime => Self::MadOverTime(MadOverTimeFunction),
            MaxOverTime => Self::MaxOverTime(MaxOverTimeFunction),
            MinOverTime => Self::MinOverTime(MinOverTimeFunction),
            PredictLinear => Self::PredictLinear(PredictLinearFunction),
            PresentOverTime => Self::PresentOverTime(PresentOverTimeFunction),
            QuantileOverTime => Self::QuantileOverTime(QuantileOverTimeFunction),
            Rate => Self::Rate(RateFunction),
            Resets => Self::Resets(ResetsFunction),
            StddevOverTime => Self::StddevOverTime(StddevOverTimeFunction),
            StdvarOverTime => Self::StdvarOverTime(StdvarOverTimeFunction),
            SumOverTime => Self::SumOverTime(SumOverTimeFunction),
            TSOfMaxOverTime => Self::TsOfMaxOverTime(TSOfMaxOverTimeFunction),
            TSOfMinOverTime => Self::TsOfMinOverTime(TSOfMinOverTimeFunction),
            TLastOverTime => Self::TLastOverTime(TSLastOverTimeFunction),
            TFirstOverTime => Self::TFirstOverTime(TSFirstOverTimeFunction),

            // Label helpers
            LabelJoin => Self::LabelJoin(LabelJoinFunction),
            LabelReplace => Self::LabelReplace(LabelReplaceFunction),

            // Sort helpers
            Sort => Self::Sort(SortFunction),
            SortDesc => Self::SortDesc(SortDescFunction),
            SortByLabel => Self::SortByLabel(SortByLabelFunction),
            SortByLabelDesc => Self::SortByLabelDesc(SortByLabelDescFunction),

            // Not yet implemented — return None so callers can fall back gracefully
            IDelta | Increase | RateOverSum | RemoveResets => {
                return None;
            }
        })
    }

    /// Returns the corresponding [`PromqlFunctionKind`] for this implementation
    /// variant.
    pub(crate) fn kind(&self) -> PromqlFunctionKind {
        use PromQLFunctionImpl::*;
        match self {
            // Math
            Abs(_) => PromqlFunctionKind::Abs,
            Acos(_) => PromqlFunctionKind::Acos,
            Acosh(_) => PromqlFunctionKind::Acosh,
            Asin(_) => PromqlFunctionKind::Asin,
            Asinh(_) => PromqlFunctionKind::Asinh,
            Atan(_) => PromqlFunctionKind::Atan,
            Atanh(_) => PromqlFunctionKind::Atanh,
            Clamp(_) => PromqlFunctionKind::Clamp,
            ClampMax(_) => PromqlFunctionKind::ClampMax,
            ClampMin(_) => PromqlFunctionKind::ClampMin,
            Ceil(_) => PromqlFunctionKind::Ceil,
            Cos(_) => PromqlFunctionKind::Cos,
            Cosh(_) => PromqlFunctionKind::Cosh,
            Deg(_) => PromqlFunctionKind::Deg,
            Exp(_) => PromqlFunctionKind::Exp,
            Floor(_) => PromqlFunctionKind::Floor,
            Ln(_) => PromqlFunctionKind::Ln,
            Log10(_) => PromqlFunctionKind::Log10,
            Log2(_) => PromqlFunctionKind::Log2,
            Pi(_) => PromqlFunctionKind::Pi,
            Rad(_) => PromqlFunctionKind::Rad,
            Round(_) => PromqlFunctionKind::Round,
            Sgn(_) => PromqlFunctionKind::Sgn,
            Sin(_) => PromqlFunctionKind::Sin,
            Sinh(_) => PromqlFunctionKind::Sinh,
            Sqrt(_) => PromqlFunctionKind::Sqrt,
            Tan(_) => PromqlFunctionKind::Tan,
            Tanh(_) => PromqlFunctionKind::Tanh,

            // Date/time
            DayOfMonth(_) => PromqlFunctionKind::DayOfMonth,
            DayOfWeek(_) => PromqlFunctionKind::DayOfWeek,
            DayOfYear(_) => PromqlFunctionKind::DayOfYear,
            DaysInMonth(_) => PromqlFunctionKind::DaysInMonth,
            Hour(_) => PromqlFunctionKind::Hour,
            Minute(_) => PromqlFunctionKind::Minute,
            Month(_) => PromqlFunctionKind::Month,
            Year(_) => PromqlFunctionKind::Year,
            Timestamp(_) => PromqlFunctionKind::Timestamp,
            Time(_) => PromqlFunctionKind::Time,

            // Special
            Absent(_) => PromqlFunctionKind::Absent,
            Scalar(_) => PromqlFunctionKind::Scalar,
            Vector(_) => PromqlFunctionKind::Vector,

            // Histogram
            HistogramQuantile(_) => PromqlFunctionKind::HistogramQuantile,
            HistogramFraction(_) => PromqlFunctionKind::HistogramFraction,

            // Range-vector
            AbsentOverTime(_) => PromqlFunctionKind::AbsentOverTime,
            AvgOverTime(_) => PromqlFunctionKind::AvgOverTime,
            Changes(_) => PromqlFunctionKind::Changes,
            CountOverTime(_) => PromqlFunctionKind::CountOverTime,
            Delta(_) => PromqlFunctionKind::Delta,
            Deriv(_) => PromqlFunctionKind::Deriv,
            DoubleExponentialSmoothing(_) => PromqlFunctionKind::DoubleExponentialSmoothing,
            FirstOverTime(_) => PromqlFunctionKind::FirstOverTime,
            IRate(_) => PromqlFunctionKind::IRate,
            LastOverTime(_) => PromqlFunctionKind::LastOverTime,
            MadOverTime(_) => PromqlFunctionKind::MadOverTime,
            MaxOverTime(_) => PromqlFunctionKind::MaxOverTime,
            MinOverTime(_) => PromqlFunctionKind::MinOverTime,
            PredictLinear(_) => PromqlFunctionKind::PredictLinear,
            PresentOverTime(_) => PromqlFunctionKind::PresentOverTime,
            QuantileOverTime(_) => PromqlFunctionKind::QuantileOverTime,
            Rate(_) => PromqlFunctionKind::Rate,
            Resets(_) => PromqlFunctionKind::Resets,
            StddevOverTime(_) => PromqlFunctionKind::StddevOverTime,
            StdvarOverTime(_) => PromqlFunctionKind::StdvarOverTime,
            SumOverTime(_) => PromqlFunctionKind::SumOverTime,
            TFirstOverTime(_) => PromqlFunctionKind::TFirstOverTime,
            TLastOverTime(_) => PromqlFunctionKind::TLastOverTime,
            TsOfMaxOverTime(_) => PromqlFunctionKind::TSOfMaxOverTime,
            TsOfMinOverTime(_) => PromqlFunctionKind::TSOfMinOverTime,

            // Label helpers
            LabelJoin(_) => PromqlFunctionKind::LabelJoin,
            LabelReplace(_) => PromqlFunctionKind::LabelReplace,

            // Sort helpers
            Sort(_) => PromqlFunctionKind::Sort,
            SortDesc(_) => PromqlFunctionKind::SortDesc,
            SortByLabel(_) => PromqlFunctionKind::SortByLabel,
            SortByLabelDesc(_) => PromqlFunctionKind::SortByLabelDesc,
        }
    }
}

// ── Dispatch macro ────────────────────────────────────────────────────────────
//
// Generates the exhaustive match body for every `PromQLFunction` method,
// forwarding the call to the inner concrete type.  This keeps the four method
// impls below in sync automatically — add a new variant once here and everywhere
// picks it up.

macro_rules! dispatch {
    ($self:ident, $method:ident ( $($arg:expr),* )) => {
        match $self {
            Self::Abs(f)                        => f.$method($($arg),*),
            Self::Acos(f)                       => f.$method($($arg),*),
            Self::Acosh(f)                      => f.$method($($arg),*),
            Self::Asin(f)                       => f.$method($($arg),*),
            Self::Asinh(f)                      => f.$method($($arg),*),
            Self::Atan(f)                       => f.$method($($arg),*),
            Self::Atanh(f)                      => f.$method($($arg),*),
            Self::Clamp(f)                      => f.$method($($arg),*),
            Self::ClampMax(f)                   => f.$method($($arg),*),
            Self::ClampMin(f)                   => f.$method($($arg),*),
            Self::Ceil(f)                       => f.$method($($arg),*),
            Self::Cos(f)                        => f.$method($($arg),*),
            Self::Cosh(f)                       => f.$method($($arg),*),
            Self::Deg(f)                        => f.$method($($arg),*),
            Self::Delta(f)                      => f.$method($($arg),*),
            Self::Exp(f)                        => f.$method($($arg),*),
            Self::Floor(f)                      => f.$method($($arg),*),
            Self::Ln(f)                         => f.$method($($arg),*),
            Self::Log10(f)                      => f.$method($($arg),*),
            Self::Log2(f)                       => f.$method($($arg),*),
            Self::Pi(f)                         => f.$method($($arg),*),
            Self::Rad(f)                        => f.$method($($arg),*),
            Self::Round(f)                      => f.$method($($arg),*),
            Self::Sgn(f)                        => f.$method($($arg),*),
            Self::Sin(f)                        => f.$method($($arg),*),
            Self::Sinh(f)                       => f.$method($($arg),*),
            Self::Sqrt(f)                       => f.$method($($arg),*),
            Self::Tan(f)                        => f.$method($($arg),*),
            Self::Tanh(f)                       => f.$method($($arg),*),
            Self::DayOfMonth(f)                 => f.$method($($arg),*),
            Self::DayOfWeek(f)                  => f.$method($($arg),*),
            Self::DayOfYear(f)                  => f.$method($($arg),*),
            Self::DaysInMonth(f)                => f.$method($($arg),*),
            Self::Hour(f)                       => f.$method($($arg),*),
            Self::Minute(f)                     => f.$method($($arg),*),
            Self::Month(f)                      => f.$method($($arg),*),
            Self::Year(f)                       => f.$method($($arg),*),
            Self::Timestamp(f)                  => f.$method($($arg),*),
            Self::Time(f)                       => f.$method($($arg),*),
            Self::Absent(f)                     => f.$method($($arg),*),
            Self::Scalar(f)                     => f.$method($($arg),*),
            Self::Vector(f)                     => f.$method($($arg),*),
            Self::HistogramQuantile(f)          => f.$method($($arg),*),
            Self::HistogramFraction(f)          => f.$method($($arg),*),
            Self::AbsentOverTime(f)             => f.$method($($arg),*),
            Self::AvgOverTime(f)                => f.$method($($arg),*),
            Self::Changes(f)                    => f.$method($($arg),*),
            Self::CountOverTime(f)              => f.$method($($arg),*),
            Self::Deriv(f)                      => f.$method($($arg),*),
            Self::DoubleExponentialSmoothing(f) => f.$method($($arg),*),
            Self::FirstOverTime(f)              => f.$method($($arg),*),
            Self::IRate(f)                      => f.$method($($arg),*),
            Self::LastOverTime(f)               => f.$method($($arg),*),
            Self::MadOverTime(f)                => f.$method($($arg),*),
            Self::MaxOverTime(f)                => f.$method($($arg),*),
            Self::MinOverTime(f)                => f.$method($($arg),*),
            Self::PredictLinear(f)              => f.$method($($arg),*),
            Self::PresentOverTime(f)            => f.$method($($arg),*),
            Self::QuantileOverTime(f)           => f.$method($($arg),*),
            Self::Rate(f)                       => f.$method($($arg),*),
            Self::Resets(f)                     => f.$method($($arg),*),
            Self::StddevOverTime(f)             => f.$method($($arg),*),
            Self::StdvarOverTime(f)             => f.$method($($arg),*),
            Self::SumOverTime(f)                => f.$method($($arg),*),
            Self::TFirstOverTime(f)             => f.$method($($arg),*),
            Self::TLastOverTime(f)              => f.$method($($arg),*),
            Self::TsOfMaxOverTime(f)            => f.$method($($arg),*),
            Self::TsOfMinOverTime(f)            => f.$method($($arg),*),
            Self::LabelJoin(f)                  => f.$method($($arg),*),
            Self::LabelReplace(f)               => f.$method($($arg),*),
            Self::Sort(f)                       => f.$method($($arg),*),
            Self::SortDesc(f)                   => f.$method($($arg),*),
            Self::SortByLabel(f)                => f.$method($($arg),*),
            Self::SortByLabelDesc(f)            => f.$method($($arg),*),
        }
    };
}

impl PromQLFunction for PromQLFunctionImpl {
    #[inline]
    fn apply(&self, arg: PromQLArg, eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        dispatch!(self, apply(arg, eval_timestamp_ms))
    }

    #[inline]
    fn apply_args(&self, args: Vec<PromQLArg>, eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        dispatch!(self, apply_args(args, eval_timestamp_ms))
    }

    #[inline]
    fn apply_call(
        &self,
        evaluated_args: Vec<PromQLArg>,
        ctx: &FunctionCallContext<'_>,
    ) -> EvalResult<ExprResult> {
        dispatch!(self, apply_call(evaluated_args, ctx))
    }

    #[inline]
    fn is_experimental(&self) -> bool {
        dispatch!(self, is_experimental())
    }
}
