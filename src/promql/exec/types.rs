use crate::Label;
use crate::common::Sample;
use crate::common::constants::METRIC_NAME_LABEL;
use crate::labels::{HasFingerprint, Labels, SeriesFingerprint};
use crate::promql::binops::get_metric_signature;
use crate::promql::error::QueryError;
use crate::promql::hashers::PreloadKey;
use ahash::RandomState;
use enquote::enquote;
use promql_parser::parser::LabelModifier;
use promql_parser::parser::value::ValueType;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum EvaluationError {
    StorageError(String),
    InternalError(String),
    ArgumentError(String),
    DuplicateLabelSet,
    UnsupportedFunction(String),
    /// A `QueryError` surfaced by a `QueryReader` (or nested query evaluation),
    /// wrapped without loss so that converting back to `QueryError` preserves
    /// the original kind (e.g. `Timeout` stays `Timeout`).
    Query(QueryError),
}

impl From<QueryError> for EvaluationError {
    fn from(err: QueryError) -> Self {
        EvaluationError::Query(err)
    }
}

impl Display for EvaluationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            EvaluationError::StorageError(err) => write!(f, "PromQL evaluation error: {err}"),
            EvaluationError::InternalError(err) => write!(f, "PromQL internal error: {err}"),
            EvaluationError::ArgumentError(err) => write!(f, "PromQL argument error: {err}"),
            EvaluationError::DuplicateLabelSet => {
                write!(f, "vector cannot contain metrics with the same labelset")
            }
            EvaluationError::UnsupportedFunction(func_name) => {
                write!(f, "PromQL unknown function: {func_name}")
            }
            EvaluationError::Query(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for EvaluationError {}

pub(crate) type EvalResult<T> = Result<T, EvaluationError>;

/// Type alias for complex HashMap used in matrix selector evaluation.
/// Maps from a label key (sorted vector of label pairs) to samples vector
pub(crate) type SeriesMap = halfbrown::HashMap<EvalLabels, Vec<Sample>, RandomState>;

/// Cheap-to-clone label container for evaluator internals.
///
/// Storage provides labels as `Arc<[Label]>` (sorted). The `Shared` variant
/// wraps that Arc directly — cloning is an atomic refcount bump. Mutation
/// (remove/insert/retain) promotes to `Owned`, which copies the vec once.
#[derive(Debug, Clone)]
pub(crate) enum EvalLabels {
    /// Shared immutable labels from storage. Clone = O(1) refcount bump.
    Shared(Arc<[Label]>),
    /// Owned mutable sorted labels, materialized on the first mutation.
    Owned(Vec<Label>),
}

impl EvalLabels {
    pub fn owned(labels: Vec<Label>) -> Self {
        EvalLabels::Owned(labels)
    }

    pub fn shared(labels: Vec<Label>) -> Self {
        EvalLabels::Shared(Arc::from(labels))
    }

    /// Create an empty label set.
    pub(crate) fn empty() -> Self {
        EvalLabels::Owned(Vec::new())
    }

    /// Binary search on the sorted label slice.
    pub(crate) fn get(&self, key: &str) -> Option<&str> {
        let slice = self.as_slice();
        slice
            .binary_search_by(|l| l.name.as_str().cmp(key))
            .ok()
            .map(|i| slice[i].value.as_str())
    }

    /// Remove a label by name. Promotes Shared→Owned if needed.
    pub(crate) fn remove(&mut self, key: &str) {
        match self {
            EvalLabels::Shared(arc) => {
                // only promote to Owned if the label exists, otherwise do nothing
                if let Ok(i) = arc.binary_search_by(|l| l.name.as_str().cmp(key)) {
                    let mut vec = arc.to_vec();
                    vec.remove(i);
                    *self = EvalLabels::Owned(vec);
                }
            }
            EvalLabels::Owned(vec) => {
                if let Ok(i) = vec.binary_search_by(|l| l.name.as_str().cmp(key)) {
                    vec.remove(i);
                }
            }
        }
    }

    /// Returns the metric name (value of the `__name__` label).
    ///
    /// Returns `""` if no `__name__` label is present.
    pub fn metric_name(&self) -> &str {
        self.get(METRIC_NAME_LABEL).unwrap_or("")
    }

    pub(crate) fn drop_name(&mut self) {
        self.remove(METRIC_NAME_LABEL);
    }

    /// Insert or update a label. Maintains sort order. Promotes Shared→Owned.
    pub(crate) fn insert(&mut self, key: String, value: String) {
        self.make_owned();
        if let EvalLabels::Owned(vec) = self {
            match vec.binary_search_by(|l| l.name.as_str().cmp(key.as_str())) {
                Ok(i) => vec[i].value = value,
                Err(i) => vec.insert(i, Label { name: key, value }),
            }
        }
    }

    pub(crate) fn extend(&mut self, other: impl Iterator<Item = Label>) {
        self.make_owned();
        if let EvalLabels::Owned(vec) = self {
            vec.extend(other);
            vec.sort();
            vec.dedup_by(|a, b| a.name == b.name);
        }
    }

    /// Retain only labels matching the predicate. Promotes Shared→Owned.
    pub(crate) fn retain(&mut self, f: impl FnMut(&Label) -> bool) {
        self.make_owned();
        if let EvalLabels::Owned(vec) = self {
            vec.retain(f);
        }
    }

    /// Returns true if there are no labels.
    pub(crate) fn is_empty(&self) -> bool {
        self.as_slice().is_empty()
    }

    /// Returns true if the label set contains the given key (binary search).
    pub(crate) fn contains(&self, key: &str) -> bool {
        self.as_slice()
            .binary_search_by(|l| l.name.as_str().cmp(key))
            .is_ok()
    }

    /// Insert or update a label by `&str` key (convenience wrapper around
    /// `insert` that accepts `&str` instead of `String`).
    pub(crate) fn set(&mut self, key: &str, value: String) {
        self.insert(key.to_string(), value);
    }

    /// Compute grouping labels for aggregation and binary operations.
    ///
    /// Mirrors `Labels::compute_grouping_labels` / `Labels::into_grouping_labels`.
    /// Clones `self` (O(1) for `Shared`) and removes/retains labels per modifier.
    pub(crate) fn compute_grouping_labels(&self, modifier: Option<&LabelModifier>) -> EvalLabels {
        let mut this = self.clone();
        match modifier {
            None => EvalLabels::Owned(Vec::new()),
            Some(LabelModifier::Include(label_list)) => {
                this.retain(|k| label_list.labels.contains(&k.name));
                this
            }
            Some(LabelModifier::Exclude(label_list)) => {
                this.retain(|k| !label_list.labels.contains(&k.name));
                this
            }
        }
    }

    /// Iterate over labels (sorted order in both variants).
    pub(crate) fn iter(&self) -> impl Iterator<Item = &Label> {
        self.as_slice().iter()
    }

    /// Convert into `Labels` for the output boundary. Both variants are
    /// already sorted, so `Labels::new()` does no extra work.
    pub(crate) fn into_labels(self) -> Labels {
        match self {
            EvalLabels::Shared(arc) => Labels(arc.to_vec()),
            EvalLabels::Owned(vec) => Labels(vec),
        }
    }

    /// Construct from pairs (for tests and benchmarks). Sorts on construction.
    #[cfg(any(test, feature = "bench"))]
    pub(crate) fn from_pairs(pairs: &[(&str, &str)]) -> Self {
        let mut vec: Vec<Label> = pairs
            .iter()
            .map(|(k, v)| Label {
                name: k.to_string(),
                value: v.to_string(),
            })
            .collect();
        vec.sort();
        EvalLabels::Owned(vec)
    }

    fn as_slice(&self) -> &[Label] {
        match self {
            EvalLabels::Shared(arc) => arc,
            EvalLabels::Owned(vec) => vec,
        }
    }

    fn make_owned(&mut self) {
        if let EvalLabels::Shared(arc) = self {
            *self = EvalLabels::Owned(arc.to_vec());
        }
    }
}

impl PartialEq for EvalLabels {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for EvalLabels {}

impl PartialOrd for EvalLabels {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for EvalLabels {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_slice().cmp(other.as_slice())
    }
}

impl Hash for EvalLabels {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_slice().hash(state);
    }
}

impl HasFingerprint for EvalLabels {
    fn fingerprint(&self) -> SeriesFingerprint {
        let slice = self.as_slice();
        slice.fingerprint()
    }
}

impl AsRef<[Label]> for EvalLabels {
    fn as_ref(&self) -> &[Label] {
        self.as_slice()
    }
}

impl Display for EvalLabels {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{{", self.metric_name())?;

        let mut first = true;
        for label in self
            .iter()
            .filter(|l| !l.name.is_empty() && l.name != METRIC_NAME_LABEL)
        {
            if !first {
                write!(f, ",")?;
            }
            first = false;
            write!(f, "{}={}", label.name, enquote('"', &label.value))?;
        }

        write!(f, "}}")?;
        Ok(())
    }
}

impl Default for EvalLabels {
    fn default() -> Self {
        EvalLabels::Owned(Vec::new())
    }
}

impl From<Labels> for EvalLabels {
    fn from(labels: Labels) -> Self {
        let vec = labels.into_inner();
        EvalLabels::Shared(Arc::from(vec))
    }
}

impl From<Vec<Label>> for EvalLabels {
    fn from(vec: Vec<Label>) -> Self {
        EvalLabels::Owned(vec)
    }
}

pub(in crate::promql) type PreloadMap =
    halfbrown::HashMap<PreloadKey, PreloadedInstantData, RandomState>;

/// Preloaded per-step evaluation data for a VectorSelector across a range query.
pub(in crate::promql) struct PreloadedInstantData {
    pub eval_start_ms: i64,
    pub step_ms: i64,
    pub series: Vec<PreloadedInstantSeries>,
}

pub(in crate::promql) struct PreloadedInstantSeries {
    pub(super) labels: EvalLabels,
    /// Dense array indexed by outer step number. values[i] = Some(Sample) if a
    /// sample exists in the lookback window for that step, None otherwise.
    pub(super) values: Vec<Option<Sample>>,
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct EvalSample {
    pub(crate) timestamp_ms: i64,
    pub(crate) value: f64,
    pub(crate) labels: EvalLabels,
    pub(crate) drop_name: bool,
}

impl EvalSample {
    pub fn label_value(&self, label: &str) -> Option<&str> {
        self.labels.get(label)
    }

    pub fn remove_metric_group(&mut self) {
        self.labels.remove(METRIC_NAME_LABEL);
    }

    pub fn add_tag(&mut self, label: &str, value: &str) {
        self.labels.set(label, value.to_string());
    }

    pub fn fingerprint(&self) -> SeriesFingerprint {
        self.labels.fingerprint()
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct EvalSamples {
    pub(crate) values: Vec<Sample>,
    pub(crate) labels: EvalLabels,
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

    pub fn fingerprint(&self) -> SeriesFingerprint {
        let labels = self.labels.as_ref();
        get_metric_signature(labels, self.drop_name)
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

    pub fn is_empty(&self) -> bool {
        match self {
            ExprResult::InstantVector(samples) => samples.is_empty(),
            ExprResult::RangeVector(samples) => samples.is_empty(),
            ExprResult::String(s) => s.is_empty(),
            _ => false,
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
