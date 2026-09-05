use crate::Label;
use crate::common::Sample;
use crate::common::constants::METRIC_NAME_LABEL;
use crate::labels::{HasFingerprint, Labels, SeriesFingerprint, fingerprint_labels};
use crate::promql::binops::get_metric_signature;
use crate::promql::error::QueryError;
use crate::promql::exec::bitset::BitSet;
use crate::promql::hashers::{MatrixPreloadKey, PreloadKey, RollupPreloadKey};
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

    /// The fingerprint [`Self::compute_grouping_labels`] would produce, without
    /// building the label set.
    ///
    /// Grouping only needs the *labels* of a group once, when the group is
    /// first seen — but it needs a *key* for every sample. Materializing the
    /// labels to get that key is what made grouping expensive: `retain` goes
    /// through `make_owned`, which clones every label of the source set into
    /// owned `String`s before dropping the ones the modifier excludes, so a
    /// 1100-series `sum by (le)` allocated 1100 label sets per step to keep 11.
    ///
    /// Hashing the filtered view instead is allocation-free and yields the
    /// *same* value: [`HasFingerprint`] for `[Label]` hashes each `(name,
    /// value)` pair in order, and filtering preserves order, so this and
    /// `compute_grouping_labels(m).fingerprint()` agree by construction. The
    /// no-modifier case hashes nothing, matching the empty set that
    /// `compute_grouping_labels` returns for it.
    pub(crate) fn compute_grouping_key(
        &self,
        modifier: Option<&LabelModifier>,
    ) -> SeriesFingerprint {
        match modifier {
            None => fingerprint_labels(std::iter::empty()),
            Some(LabelModifier::Include(label_list)) => {
                fingerprint_labels(self.iter().filter(|l| label_list.labels.contains(&l.name)))
            }
            Some(LabelModifier::Exclude(label_list)) => {
                fingerprint_labels(self.iter().filter(|l| !label_list.labels.contains(&l.name)))
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

/// A per-step grid of values in which some steps hold nothing.
///
/// Replaces `Vec<Option<T>>` for the preload grids, which is wasteful at scale
/// in two separate ways. `Option<Sample>` costs 24 bytes to carry 16 bytes of
/// payload — neither `i64` nor `f64` has a spare niche, so the discriminant is
/// a whole word — and a series that only exists for part of the query range
/// still pays for every step outside it. This stores one packed value per step
/// with a one-bit-per-step presence bitmap, and drops the absent runs at either
/// end of the grid entirely.
///
/// Measured over a 2000-series, 1440-step range query: 69.2 MB → 46.5 MB when
/// every series spans the whole range (the packing alone), and 69.2 MB →
/// 12.1 MB when each series spans a quarter of it (packing plus trimming).
///
/// Absence lives in the bitmap and never in the value, so a stored NaN is a
/// *present* NaN — preserving the absent-vs-NaN distinction that the rollup
/// and selector paths depend on.
pub(in crate::promql) enum StepGrid<T> {
    /// Every step from `offset` onwards is present, so there is no bitmap to
    /// consult. This is the common shape — a series that reports without gaps
    /// over the part of the range it exists for — and it is the one on the hot
    /// read path, so it reads with a single bounds-checked load. Carrying a
    /// bitmap here measurably cost CPU: a step loop over 1100 series touches
    /// one cache line per series per step, and a second allocation made it two.
    Dense { offset: usize, values: Vec<T> },
    /// Some step inside the grid's span is absent, so presence has to be
    /// stored. One bit per step, which is still far cheaper than the word of
    /// `Option` discriminant per step that this replaces.
    Sparse {
        offset: usize,
        values: Vec<T>,
        /// One bit per entry of `values`; bit *i* set means `values[i]` is present.
        present: BitSet,
    },
}

impl<T: Copy> StepGrid<T> {
    /// The value at `step`, or `None` when that step holds nothing.
    ///
    /// A step before the grid's first stored value, past its last, or marked
    /// absent all answer `None`. `step` is derived by dividing timestamps, so
    /// an out-of-range evaluation timestamp can produce a nonsensically large
    /// index; that lands past the end and answers `None` rather than
    /// panicking, as the `Vec<Option<T>>` lookup it replaces did.
    pub(in crate::promql) fn get(&self, step: usize) -> Option<T> {
        match self {
            StepGrid::Dense { offset, values } => values.get(step.checked_sub(*offset)?).copied(),
            StepGrid::Sparse {
                offset,
                values,
                present,
            } => {
                let i = step.checked_sub(*offset)?;
                if i >= values.len() || i >= present.len() || !present.get(i) {
                    return None;
                }
                Some(values[i])
            }
        }
    }
}

/// Streaming builder for a [`StepGrid`], fed one step at a time in step order.
///
/// Streaming matters: materializing a `Vec<Option<T>>` and converting would
/// pay the very peak this type exists to avoid, so values are packed as they
/// arrive and leading absent steps are never stored at all.
pub(in crate::promql) struct StepGridBuilder<T> {
    offset: Option<usize>,
    next_step: usize,
    values: Vec<T>,
    present: BitSet,
    /// How many pushed steps were present. Trimming only ever drops absent
    /// steps, so comparing this against the trimmed length is an exact O(1)
    /// test for "no holes remain" — no need to rescan the bitmap.
    present_count: usize,
}

// Preloading can create a builder for every matching series. Keep the initial
// allocation small: most sparse series either never begin or have a short
// retained span, while dense grids grow amortized as values arrive.
const STEP_GRID_INITIAL_CAPACITY: usize = 64;

impl<T: Copy + Default> StepGridBuilder<T> {
    pub(in crate::promql) fn with_capacity(steps: usize) -> Self {
        let initial_capacity = steps.min(STEP_GRID_INITIAL_CAPACITY);
        Self {
            offset: None,
            next_step: 0,
            values: Vec::with_capacity(initial_capacity),
            present: BitSet::with_capacity(initial_capacity),
            present_count: 0,
        }
    }

    /// Append the next step's value.
    pub(in crate::promql) fn push(&mut self, value: Option<T>) {
        let step = self.next_step;
        self.next_step += 1;
        match value {
            Some(v) => {
                self.offset.get_or_insert(step);
                self.values.push(v);
                self.present.push(true);
                self.present_count += 1;
            }
            // Absent steps before the first present one are dropped rather
            // than stored; once the grid has started, an absent step has to
            // hold a slot so later steps keep their index.
            None if self.offset.is_some() => {
                self.values.push(T::default());
                self.present.push(false);
            }
            None => {}
        }
    }

    /// Finish the grid, dropping the trailing run of absent steps and shedding
    /// the bitmap entirely when nothing inside the span is absent.
    pub(in crate::promql) fn finish(mut self) -> StepGrid<T> {
        let last_present = (0..self.values.len()).rev().find(|&i| self.present.get(i));
        match last_present {
            Some(last) => {
                self.values.truncate(last + 1);
                self.present.truncate(last + 1);
            }
            // Nothing was ever present: keep an empty grid, not a run of holes.
            None => {
                self.values.clear();
                self.present.clear_all();
            }
        }
        self.values.shrink_to_fit();
        let offset = self.offset.unwrap_or(0);

        // Leading and trailing absences are gone, so the grid is dense unless
        // a step inside the remaining span is absent.
        if self.present_count == self.values.len() {
            return StepGrid::Dense {
                offset,
                values: self.values,
            };
        }
        self.present.shrink_to_fit();
        StepGrid::Sparse {
            offset,
            values: self.values,
            present: self.present,
        }
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
    /// Indexed by outer step number: a step is present when a sample exists in
    /// the lookback window for it, absent otherwise.
    pub(super) values: StepGrid<Sample>,
}

pub(in crate::promql) type RollupPreloadMap =
    halfbrown::HashMap<RollupPreloadKey, PreloadedRollupData, RandomState>;

/// A rollup whose whole step grid was evaluated in one go, rather than once per
/// step. Populated by `Evaluator::preload_rollups` before the step loop.
pub(in crate::promql) struct PreloadedRollupData {
    /// Start of the *step* grid — `query_start`, not the window end, which any
    /// `@`/`offset` on the selector will have shifted. Step index is derived
    /// from the step timestamp, so this is the right origin.
    pub eval_start_ms: i64,
    pub step_ms: i64,
    pub series: Vec<PreloadedRollupSeries>,
}

pub(in crate::promql) struct PreloadedRollupSeries {
    pub(super) labels: EvalLabels,
    /// Indexed by outer step number. A step is present when the window for it
    /// produced a value and absent when it held no samples — which is not the
    /// same as producing NaN, and is why absence lives in the grid's presence
    /// bitmap rather than in the `f64`.
    pub(super) values: StepGrid<f64>,
}

pub(in crate::promql) type MatrixPreloadMap =
    halfbrown::HashMap<MatrixPreloadKey, PreloadedMatrixData, RandomState>;

/// A matrix selector's raw samples over the whole span its outer-grid windows
/// cover, fetched in one request by `Evaluator::preload_matrices` and sliced
/// per step by `Evaluator::evaluate_matrix_selector`.
///
/// This is the fallback grid for rollups that could not be pushed down: it
/// holds *raw* samples rather than reduced values, so any range-vector
/// function — pushable or not — can evaluate its window from it.
pub(in crate::promql) struct PreloadedMatrixData {
    pub series: Vec<PreloadedMatrixSeries>,
}

pub(in crate::promql) struct PreloadedMatrixSeries {
    pub(super) labels: EvalLabels,
    /// Sorted ascending by timestamp (the storage invariant); spans every
    /// window of the step grid.
    pub(super) samples: Vec<Sample>,
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

#[cfg(test)]
mod grouping_key_tests {
    use super::*;
    use promql_parser::label::Labels as LabelList;

    fn labels(pairs: &[(&str, &str)]) -> EvalLabels {
        EvalLabels::shared(
            pairs
                .iter()
                .map(|(n, v)| Label {
                    name: n.to_string(),
                    value: v.to_string(),
                })
                .collect(),
        )
    }

    fn modifier(names: &[&str], include: bool) -> LabelModifier {
        let list = LabelList {
            labels: names.iter().map(|n| n.to_string()).collect(),
        };
        if include {
            LabelModifier::Include(list)
        } else {
            LabelModifier::Exclude(list)
        }
    }

    /// `compute_grouping_key` exists only because it is the fingerprint of the
    /// set `compute_grouping_labels` builds — computed without building it. If
    /// the two ever disagree, grouping silently splits or merges groups, so
    /// pin the equivalence across every modifier shape.
    #[test]
    fn grouping_key_matches_the_materialized_grouping_labels() {
        let sets = [
            labels(&[]),
            labels(&[("__name__", "http_requests")]),
            labels(&[("__name__", "http_requests"), ("job", "api"), ("le", "0.5")]),
            // A value that collides with a neighbouring name/value boundary if
            // the separator were dropped.
            labels(&[("a", "bc"), ("ab", "c")]),
        ];
        let modifiers = [
            None,
            Some(modifier(&[], true)),
            Some(modifier(&[], false)),
            Some(modifier(&["le"], true)),
            Some(modifier(&["le"], false)),
            Some(modifier(&["job", "le"], true)),
            Some(modifier(&["job", "le"], false)),
            Some(modifier(&["__name__"], true)),
            Some(modifier(&["absent"], true)),
        ];

        for set in &sets {
            for m in &modifiers {
                let m = m.as_ref();
                assert_eq!(
                    set.compute_grouping_key(m),
                    set.compute_grouping_labels(m).fingerprint(),
                    "labels {set} with modifier {m:?}"
                );
            }
        }
    }

    /// The point of the key: distinct groups must stay distinct.
    #[test]
    fn grouping_key_separates_groups_it_should() {
        let by_le = Some(modifier(&["le"], true));
        let m = by_le.as_ref();
        let a = labels(&[("__name__", "h"), ("l", "1"), ("le", "0.5")]);
        let b = labels(&[("__name__", "h"), ("l", "2"), ("le", "0.5")]);
        let c = labels(&[("__name__", "h"), ("l", "1"), ("le", "1.0")]);

        // Same `le` -> same group, whatever else differs.
        assert_eq!(a.compute_grouping_key(m), b.compute_grouping_key(m));
        // Different `le` -> different group.
        assert_ne!(a.compute_grouping_key(m), c.compute_grouping_key(m));
    }
}

#[cfg(test)]
mod step_grid_tests {
    use super::*;

    fn build<T: Copy + Default>(items: &[Option<T>]) -> StepGrid<T> {
        let mut b = StepGridBuilder::with_capacity(items.len());
        for &v in items {
            b.push(v);
        }
        b.finish()
    }

    /// The grid must answer exactly what the `Vec<Option<T>>` it replaces
    /// would, for every index — including the ones outside it.
    fn assert_matches<T: Copy + Default + PartialEq + std::fmt::Debug>(items: &[Option<T>]) {
        let grid = build(items);
        for (i, want) in items.iter().enumerate() {
            assert_eq!(grid.get(i), *want, "step {i} of {items:?}");
        }
        // Past the end, and the huge index a negative step timestamp produces
        // once cast to usize.
        assert_eq!(grid.get(items.len()), None, "one past the end of {items:?}");
        assert_eq!(grid.get(usize::MAX), None, "wrapped index on {items:?}");
    }

    #[test]
    fn grid_answers_exactly_what_a_vec_of_options_would() {
        assert_matches::<u32>(&[]);
        assert_matches(&[None::<u32>, None, None]);
        assert_matches(&[Some(1u32), Some(2), Some(3)]);
        // Leading absent run — dropped from storage, still absent on read.
        assert_matches(&[None, None, Some(7u32), Some(8)]);
        // Trailing absent run — likewise.
        assert_matches(&[Some(7u32), Some(8), None, None]);
        // Interior holes must keep later steps on their own index.
        assert_matches(&[Some(1u32), None, None, Some(4), None, Some(6)]);
        assert_matches(&[None, Some(1u32), None, Some(3), None]);
        // Longer than one bitmap word, with a hole either side of the boundary.
        let mut long: Vec<Option<u32>> = (0..200u32).map(Some).collect();
        long[63] = None;
        long[64] = None;
        long[128] = None;
        assert_matches(&long);
    }

    /// The distinction the rollup path depends on: a stored NaN is a *present*
    /// NaN, not an absent step.
    #[test]
    fn grid_separates_an_absent_step_from_a_present_nan() {
        let grid = build(&[Some(f64::NAN), None, Some(1.5)]);
        let got = grid.get(0).expect("step 0 is present");
        assert!(got.is_nan(), "a present NaN survives as a present NaN");
        assert_eq!(grid.get(1), None, "step 1 holds nothing");
        assert_eq!(grid.get(2), Some(1.5));
    }

    /// Trimming is the point: absent runs at either end cost no storage.
    #[test]
    fn grid_drops_absent_runs_at_both_ends() {
        let mut items = vec![None::<u64>; 500];
        items[400] = Some(1);
        items[402] = Some(2);
        let grid = build(&items);
        // Only steps 400..=402 are stored, not all 500.
        let StepGrid::Sparse { offset, values, .. } = &grid else {
            panic!("a grid with an interior hole is sparse");
        };
        assert_eq!(values.len(), 3, "stored slots");
        assert_eq!(*offset, 400);
        assert_matches(&items);

        // A grid that is absent throughout stores nothing at all.
        let empty = build(&vec![None::<u64>; 500]);
        let StepGrid::Dense { values, .. } = &empty else {
            panic!("an empty grid has no holes to record");
        };
        assert_eq!(values.len(), 0);

        // A grid with no interior holes sheds the bitmap entirely.
        let dense = build(&[None, Some(1u64), Some(2), Some(3), None]);
        let StepGrid::Dense { offset, values } = &dense else {
            panic!("a gapless grid is dense");
        };
        assert_eq!((*offset, values.len()), (1, 3));
        assert_matches(&[None, Some(1u64), Some(2), Some(3), None]);
    }

    #[test]
    fn grid_builder_bounds_its_initial_allocation() {
        let builder = StepGridBuilder::<u64>::with_capacity(1_000_000);

        assert!(
            builder.values.capacity() <= STEP_GRID_INITIAL_CAPACITY,
            "a sparse grid must not reserve every requested step"
        );
    }
}
