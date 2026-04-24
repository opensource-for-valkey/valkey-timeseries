use crate::common::Sample;
use crate::promql::Labels;
use crate::promql::error::QueryError;
use crate::promql::hashers::{PreloadKey, SeriesFingerprint};
use ahash::RandomState;
use promql_parser::parser::value::ValueType;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone)]
pub enum EvaluationError {
    StorageError(String),
    InternalError(String),
    ArgumentError(String),
    UnsupportedFunction(String),
}

impl From<QueryError> for EvaluationError {
    fn from(err: QueryError) -> Self {
        EvaluationError::InternalError(err.to_string())
    }
}

impl Display for EvaluationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            EvaluationError::StorageError(err) => write!(f, "PromQL evaluation error: {err}"),
            EvaluationError::InternalError(err) => write!(f, "PromQL internal error: {err}"),
            EvaluationError::ArgumentError(err) => write!(f, "PromQL argument error: {err}"),
            EvaluationError::UnsupportedFunction(func_name) => {
                write!(f, "PromQL unknown function: {func_name}")
            }
        }
    }
}

impl std::error::Error for EvaluationError {}

pub(crate) type EvalResult<T> = Result<T, EvaluationError>;

/// Type alias for complex HashMap used in matrix selector evaluation.
/// Maps from a label key (sorted vector of label pairs) to samples vector
pub type SeriesMap = halfbrown::HashMap<Labels, Vec<Sample>, RandomState>;

pub(in crate::promql) type PreloadMap =
halfbrown::HashMap<PreloadKey, PreloadedInstantData, RandomState>;

/// Preloaded per-step evaluation data for a VectorSelector across a range query.
pub(in crate::promql) struct PreloadedInstantData {
    pub eval_start_ms: i64,
    pub step_ms: i64,
    pub series: Vec<PreloadedInstantSeries>,
}

pub(in crate::promql) struct PreloadedInstantSeries {
    pub(super) labels: Labels,
    /// Dense array indexed by outer step number. values[i] = Some(Sample) if a
    /// sample exists in the lookback window for that step, None otherwise.
    pub(super) values: Vec<Option<Sample>>,
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct EvalSample {
    pub(crate) timestamp_ms: i64,
    pub(crate) value: f64,
    pub(crate) labels: Labels,
    pub(crate) drop_name: bool,
}

impl EvalSample {
    pub fn label_value(&self, label: &str) -> Option<&str> {
        self.labels.get(label)
    }

    pub fn remove_metric_group(&mut self) {
        self.labels.remove("__name__");
    }

    pub fn add_tag(&mut self, label: &str, value: &str) {
        self.labels.set(label, value.to_string());
    }

    pub fn fingerprint(&self) -> SeriesFingerprint {
        self.labels.get_fingerprint()
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct EvalSamples {
    pub(crate) values: Vec<Sample>,
    pub(crate) labels: Labels,
    /// If true, the `__name__` label should be removed when materializing
    /// result labels. Mirrors `EvalSample.drop_name` behavior for instant
    /// vectors so range-vector operations can defer name-dropping.
    pub(crate) drop_name: bool,
    pub(crate) range_ms: i64,
    pub(crate) range_end_ms: i64,
}

impl EvalSamples {
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn label_value(&self, label: &str) -> Option<&str> {
        self.labels.get(label)
    }

    #[cfg(test)]
    pub fn first_sample(&self) -> Option<&Sample> {
        self.values.first()
    }

    #[cfg(test)]
    pub fn last_sample(&self) -> Option<&Sample> {
        self.values.last()
    }
}

#[derive(Debug)]
pub(crate) enum ExprResult {
    String(String),
    Scalar(f64),
    InstantVector(Vec<EvalSample>),
    RangeVector(Vec<EvalSamples>),
}

impl ExprResult {
    /// Extract the instant vector samples, returning None if this is a scalar or range vector result
    pub(crate) fn into_instant_vector(self) -> Option<Vec<EvalSample>> {
        match self {
            ExprResult::InstantVector(samples) => Some(samples),
            _ => None,
        }
    }

    /// Extract the range vector samples, returning None if this is not a range vector result
    pub(crate) fn into_range_vector(self) -> Option<Vec<EvalSamples>> {
        match self {
            ExprResult::RangeVector(samples) => Some(samples),
            _ => None,
        }
    }

    #[cfg(test)]
    /// Extract instant vector samples, panicking if this is not an instant vector result
    pub(crate) fn expect_instant_vector(self, msg: &str) -> Vec<EvalSample> {
        match self {
            ExprResult::InstantVector(samples) => samples,
            _ => panic!("{}", msg),
        }
    }

    pub fn value_type(&self) -> ValueType {
        match self {
            ExprResult::InstantVector(_) => ValueType::Vector,
            ExprResult::RangeVector(_) => ValueType::Matrix,
            ExprResult::Scalar(_) => ValueType::Scalar,
            ExprResult::String(_) => ValueType::String,
        }
    }
}

impl From<f64> for ExprResult {
    fn from(value: f64) -> Self {
        Self::Scalar(value)
    }
}

impl From<usize> for ExprResult {
    fn from(value: usize) -> Self {
        Self::Scalar(value as f64)
    }
}

impl From<String> for ExprResult {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for ExprResult {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}
