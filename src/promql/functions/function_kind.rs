use std::fmt::Display;
use std::time::Duration;
use valkey_module::ValkeyError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FunctionType {
    Aggregation,
    Transform,
    Rollup,
}

impl Display for FunctionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            FunctionType::Aggregation => "aggregation",
            FunctionType::Transform => "transform",
            FunctionType::Rollup => "rollup",
        };
        write!(f, "{}", s)
    }
}

/// Enum representing known PromQL function names used by the parser/registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PromqlFunctionKind {
    // Math / unary
    Abs,
    Acos,
    Acosh,
    Asin,
    Asinh,
    Atan,
    Atanh,
    Clamp,
    ClampMax,
    ClampMin,
    Ceil,
    Cos,
    Cosh,
    Deg,
    Exp,
    Floor,
    Ln,
    Log10,
    Log2,
    Pi,
    Rad,
    Round,
    Sgn,
    Sin,
    Sinh,
    Sqrt,
    Tan,
    Tanh,

    // Date/time
    DayOfMonth,
    DayOfWeek,
    DayOfYear,
    DaysInMonth,
    Hour,
    Minute,
    Month,
    Timestamp,
    Time,
    Year,

    // Special
    Absent,
    Scalar,
    Vector,

    // Histogram
    HistogramQuantile,
    HistogramFraction,

    // Range-vector functions
    AbsentOverTime,
    AvgOverTime,
    Changes,
    CountOverTime,
    DoubleExponentialSmoothing,
    FirstOverTime,
    Delta,
    Deriv,
    IDelta,
    IRate,
    Rate,
    RateOverSum,
    RemoveResets,
    Resets,
    Increase,
    SumOverTime,
    LastOverTime,
    MadOverTime,
    MinOverTime,
    MaxOverTime,
    PredictLinear,
    PresentOverTime,
    QuantileOverTime,
    StddevOverTime,
    StdvarOverTime,
    TSOfMinOverTime,
    TSOfMaxOverTime,
    TFirstOverTime,
    TLastOverTime,

    // Label helpers
    LabelJoin,
    LabelReplace,

    // Sort helpers
    Sort,
    SortDesc,
    SortByLabel,
    SortByLabelDesc,
}

impl PromqlFunctionKind {
    pub const fn should_remove_counter_resets(&self) -> bool {
        use PromqlFunctionKind::*;
        matches!(self, Increase | IRate | Rate)
    }

    // All rollup the functions that do not rely on the previous sample
    // before the lookbehind window (aka prev_value) do not need a silence interval.
    pub const fn need_silence_interval(&self) -> bool {
        use PromqlFunctionKind::*;
        !matches!(
            self,
            Changes | Delta | IDelta | Increase | IRate | Rate | Resets | Deriv
        )
    }

    /// We can extend the lookbehind window for these functions to make sure it contains enough
    /// points for returning non-empty results.
    ///
    /// This is needed for returning the expected non-empty graphs when zooming in the graph in Grafana,
    /// which is built with `func_name(metric)` query.
    pub const fn can_adjust_window(&self) -> bool {
        use PromqlFunctionKind::*;
        matches!(self, Deriv | IRate | Rate | RateOverSum | Timestamp)
    }

    /// Classifies the function by its type.
    /// Range-vector functions are classified as Rollup, all others as Transform.
    pub const fn function_type(&self) -> FunctionType {
        use PromqlFunctionKind::*;
        match self {
            // Range-vector functions -> Rollup
            AbsentOverTime
            | AvgOverTime
            | Changes
            | CountOverTime
            | Delta
            | DoubleExponentialSmoothing
            | FirstOverTime
            | Deriv
            | IDelta
            | IRate
            | Rate
            | RateOverSum
            | RemoveResets
            | Resets
            | Increase
            | SumOverTime
            | LastOverTime
            | MadOverTime
            | MinOverTime
            | MaxOverTime
            | PredictLinear
            | PresentOverTime
            | QuantileOverTime
            | StddevOverTime
            | StdvarOverTime
            | TFirstOverTime
            | TLastOverTime => FunctionType::Rollup,
            // All other functions are Transform
            _ => FunctionType::Transform,
        }
    }

    pub fn name(&self) -> &'static str {
        use PromqlFunctionKind::*;
        match self {
            Abs => "abs",
            Acos => "acos",
            Acosh => "acosh",
            Asin => "asin",
            Asinh => "asinh",
            Atan => "atan",
            Atanh => "atanh",
            Clamp => "clamp",
            ClampMax => "clamp_max",
            ClampMin => "clamp_min",
            Ceil => "ceil",
            Cos => "cos",
            Cosh => "cosh",
            Deg => "deg",
            Exp => "exp",
            Floor => "floor",
            Ln => "ln",
            Log10 => "log10",
            Log2 => "log2",
            Pi => "pi",
            Rad => "rad",
            Round => "round",
            Sgn => "sgn",
            Sin => "sin",
            Sinh => "sinh",
            Sqrt => "sqrt",
            Tan => "tan",
            Tanh => "tanh",

            DayOfMonth => "day_of_month",
            DayOfWeek => "day_of_week",
            DayOfYear => "day_of_year",
            DaysInMonth => "days_in_month",
            Hour => "hour",
            Minute => "minute",
            Month => "month",
            Year => "year",
            Timestamp => "timestamp",
            Time => "time",

            Absent => "absent",
            Scalar => "scalar",
            Vector => "vector",

            HistogramQuantile => "histogram_quantile",
            HistogramFraction => "histogram_fraction",

            AbsentOverTime => "absent_over_time",
            Changes => "changes",
            Delta => "delta",
            Deriv => "deriv",
            DoubleExponentialSmoothing => "double_exponential_smoothing",
            Rate => "rate",
            FirstOverTime => "first_over_time",
            LastOverTime => "last_over_time",
            IDelta => "idelta",
            Increase => "increase",
            IRate => "irate",
            RemoveResets => "remove_resets",
            Resets => "resets",
            SumOverTime => "sum_over_time",
            AvgOverTime => "avg_over_time",
            MadOverTime => "mad_over_time",
            MinOverTime => "min_over_time",
            MaxOverTime => "max_over_time",
            PresentOverTime => "present_over_time",
            QuantileOverTime => "quantile_over_time",
            RateOverSum => "rate_over_sum",
            CountOverTime => "count_over_time",
            StddevOverTime => "stddev_over_time",
            StdvarOverTime => "stdvar_over_time",
            TSOfMaxOverTime => "ts_of_max_over_time",
            TSOfMinOverTime => "ts_of_min_over_time",
            TFirstOverTime => "ts_of_first_over_time",
            TLastOverTime => "ts_of_last_over_time",

            LabelJoin => "label_join",
            LabelReplace => "label_replace",
            PredictLinear => "predict_linear",

            Sort => "sort",
            SortDesc => "sort_desc",
            SortByLabel => "sort_by_label",
            SortByLabelDesc => "sort_by_label_desc",
        }
    }

    pub fn keep_metric_name(&self) -> bool {
        use PromqlFunctionKind::*;
        matches!(
            self,
            HistogramQuantile | HistogramFraction | DoubleExponentialSmoothing | Scalar | Vector
        )
    }

    pub fn window(&self) -> Duration {
        Duration::ZERO
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
            // math
            "abs" => PromqlFunctionKind::Abs,
            "acos" => PromqlFunctionKind::Acos,
            "acosh" => PromqlFunctionKind::Acosh,
            "asin" => PromqlFunctionKind::Asin,
            "asinh" => PromqlFunctionKind::Asinh,
            "atan" => PromqlFunctionKind::Atan,
            "atanh" => PromqlFunctionKind::Atanh,
            "clamp" => PromqlFunctionKind::Clamp,
            "clamp_max" => PromqlFunctionKind::ClampMax,
            "clamp_min" => PromqlFunctionKind::ClampMin,
            "ceil" => PromqlFunctionKind::Ceil,
            "cos" => PromqlFunctionKind::Cos,
            "cosh" => PromqlFunctionKind::Cosh,
            "deg" => PromqlFunctionKind::Deg,
            "delta" => PromqlFunctionKind::Delta,
            "exp" => PromqlFunctionKind::Exp,
            "floor" => PromqlFunctionKind::Floor,
            "ln" => PromqlFunctionKind::Ln,
            "log10" => PromqlFunctionKind::Log10,
            "log2" => PromqlFunctionKind::Log2,
            "pi" => PromqlFunctionKind::Pi,
            "rad" => PromqlFunctionKind::Rad,
            "round" => PromqlFunctionKind::Round,
            "sgn" => PromqlFunctionKind::Sgn,
            "sin" => PromqlFunctionKind::Sin,
            "sinh" => PromqlFunctionKind::Sinh,
            "sqrt" => PromqlFunctionKind::Sqrt,
            "tan" => PromqlFunctionKind::Tan,
            "tanh" => PromqlFunctionKind::Tanh,

            // datetime
            "day_of_month" => PromqlFunctionKind::DayOfMonth,
            "day_of_week" => PromqlFunctionKind::DayOfWeek,
            "day_of_year" => PromqlFunctionKind::DayOfYear,
            "days_in_month" => PromqlFunctionKind::DaysInMonth,
            "hour" => PromqlFunctionKind::Hour,
            "minute" => PromqlFunctionKind::Minute,
            "month" => PromqlFunctionKind::Month,
            "timestamp" => PromqlFunctionKind::Timestamp,
            "time" => PromqlFunctionKind::Time,
            "year" => PromqlFunctionKind::Year,

            // special
            "absent" => PromqlFunctionKind::Absent,
            "scalar" => PromqlFunctionKind::Scalar,
            "vector" => PromqlFunctionKind::Vector,

            "histogram_quantile" => PromqlFunctionKind::HistogramQuantile,
            "histogram_fraction" => PromqlFunctionKind::HistogramFraction,

            // range-vector
            "changes" => PromqlFunctionKind::Changes,
            "delta" => PromqlFunctionKind::Delta,
            "deriv" => PromqlFunctionKind::Deriv,
            "holt_winters" => PromqlFunctionKind::DoubleExponentialSmoothing,
            "double_exponential_smoothing" => PromqlFunctionKind::DoubleExponentialSmoothing,
            "rate" => PromqlFunctionKind::Rate,
            "first_over_time" => PromqlFunctionKind::FirstOverTime,
            "last_over_time" => PromqlFunctionKind::LastOverTime,
            "increase" => PromqlFunctionKind::Increase,
            "idelta" => PromqlFunctionKind::IDelta,
            "irate" => PromqlFunctionKind::IRate,
            "remove_resets" => PromqlFunctionKind::RemoveResets,
            "resets" => PromqlFunctionKind::Resets,
            "sum_over_time" => PromqlFunctionKind::SumOverTime,
            "avg_over_time" => PromqlFunctionKind::AvgOverTime,
            "min_over_time" => PromqlFunctionKind::MinOverTime,
            "max_over_time" => PromqlFunctionKind::MaxOverTime,
            "predict_linear" => PromqlFunctionKind::PredictLinear,
            "present_over_time" => PromqlFunctionKind::PresentOverTime,
            "count_over_time" => PromqlFunctionKind::CountOverTime,
            "rate_over_sum" => PromqlFunctionKind::RateOverSum,
            "stddev_over_time" => PromqlFunctionKind::StddevOverTime,
            "stdvar_over_time" => PromqlFunctionKind::StdvarOverTime,
            "quantile_over_time" => PromqlFunctionKind::QuantileOverTime,
            "ts_of_first_over_time" => PromqlFunctionKind::TFirstOverTime,
            "ts_of_last_over_time" => PromqlFunctionKind::TLastOverTime,
            "ts_of_max_over_time" => PromqlFunctionKind::TSOfMaxOverTime,
            "ts_of_minover_time" => PromqlFunctionKind::TSOfMinOverTime,

            // labels
            "label_join" => PromqlFunctionKind::LabelJoin,
            "label_replace" => PromqlFunctionKind::LabelReplace,

            "sort" => PromqlFunctionKind::Sort,
            "sort_desc" => PromqlFunctionKind::SortDesc,
            "sort_by_label" => PromqlFunctionKind::SortByLabel,
            "sort_by_label_desc" => PromqlFunctionKind::SortByLabelDesc,
        };

        match v {
            Some(f) => Ok(f),
            None => Err(ValkeyError::Str("PromQL: unknown function")),
        }
    }
}
