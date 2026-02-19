//! Core data types for Promql.
//!
//! This module defines the fundamental data structures used in the public API,
//! including labels for series identification, samples for data points, and
//! series for batched ingestion.
use crate::common::constants::{METRIC_NAME_LABEL, MILLIS_PER_MIN};
use crate::common::time::{current_time_millis, system_time_to_millis, valkey_cached_time_millis};
use crate::common::{Sample, Timestamp};
use crate::labels::{Label, MetricName};
use crate::promql::hashers::{HasFingerprint, SeriesFingerprint};
use crate::promql::{EvalSample, EvalSamples, ExprResult, QueryError, QueryResult};
use ahash::{AHashMap, AHashSet};
use enquote::enquote;
use promql_parser::parser::value::ValueType;
use promql_parser::parser::{EvalStmt, LabelModifier};
use serde::de::{MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::time::Duration;

/// StaleNaN is a signaling NaN used as a staleness marker in Prometheus.
///
/// This value indicates that a time series is no longer being scraped or updated.
/// It's a signaling NaN (MSB of mantissa is 0) chosen with leading zeros to allow
/// for future extensions. The value 2 (rather than 1) makes it easier to distinguish
/// from NormalNaN during debugging.
pub const STALE_NAN: u64 = 0x7ff0000000000002;

/// Check if a float value is the special StaleNaN marker.
///
/// # Example
///
/// ```
/// use timeseries::{is_stale_nan, STALE_NAN};
///
/// let stale = f64::from_bits(STALE_NAN);
/// assert!(is_stale_nan(stale));
/// assert!(!is_stale_nan(f64::NAN));
/// assert!(!is_stale_nan(42.0));
/// ```
pub fn is_stale_nan(v: f64) -> bool {
    v.to_bits() == STALE_NAN
}

/// An ordered set of labels identifying a series.
///
/// `Labels` wraps a sorted `Vec<Label>` and provides convenience accessors
/// for looking up label values and the metric name. This is the type returned
/// by read/query APIs to identify each result series.
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct Labels(Vec<Label>);

impl Labels {
    pub fn with_capacity(capacity: usize) -> Self {
        Labels(Vec::with_capacity(capacity))
    }

    pub fn push(&mut self, label: Label) {
        self.0.push(label);
    }

    /// Creates an empty `Labels`.
    pub fn empty() -> Self {
        Self(Vec::new())
    }

    /// Creates a new `Labels` from a vec of labels.
    pub fn new(labels: Vec<Label>) -> Self {
        let mut labels = labels;
        labels.sort_unstable();
        Self(labels)
    }

    /// Returns the number of labels.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if there are no labels.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the value of the label with the given name, if present.
    // TODO: labels are sorted, could use binary_search_by for O(log n)
    pub fn get(&self, name: &str) -> Option<&str> {
        self.0
            .iter()
            .find(|l| l.name == name)
            .map(|l| l.value.as_str())
    }

    pub fn set(&mut self, key: &str, value: String) {
        match self.0.binary_search_by(|l| l.name.as_str().cmp(key)) {
            Ok(i) => self.0[i].value = value,
            Err(i) => self.0.insert(
                i,
                Label {
                    name: key.to_string(),
                    value,
                },
            ),
        }
    }

    pub fn get_label_mut(&mut self, name: &str) -> Option<&mut Label> {
        if let Some(idx) = self.0.iter().position(|l| l.name == name) {
            self.0.get_mut(idx)
        } else {
            None
        }
    }

    pub fn insert(&mut self, key: &str, value: String) -> bool {
        match self.0.binary_search_by(|l| l.name.as_str().cmp(key)) {
            Ok(i) => {
                self.0[i].value = value;
                false
            }
            Err(i) => {
                self.0.insert(
                    i,
                    Label {
                        name: key.to_string(),
                        value,
                    },
                );
                true
            }
        }
    }

    pub fn remove(&mut self, key: &str) -> Option<Label> {
        self.0
            .iter()
            .position(|l| l.name == key)
            .map(|i| self.0.remove(i))
    }

    pub fn contains(&self, key: &str) -> bool {
        self.0
            .binary_search_by(|l| l.name.as_str().cmp(key))
            .is_ok()
    }

    /// Returns the metric name (value of the `__name__` label).
    ///
    /// Returns `""` if no `__name__` label is present.
    pub fn metric_name(&self) -> &str {
        self.get(METRIC_NAME_LABEL).unwrap_or("")
    }

    /// Iterates over the labels.
    pub fn iter(&self) -> impl Iterator<Item = &Label> {
        self.0.iter()
    }

    pub fn sort(&mut self) {
        self.0.sort();
    }

    pub fn signature(&self) -> u128 {
        self.0.fingerprint()
    }

    pub fn into_inner(self) -> Vec<Label> {
        self.0
    }

    pub fn retain<F>(&mut self, f: F)
    where
        F: Fn(&Label) -> bool,
    {
        self.0.retain(f)
    }

    pub fn retain_tags(&mut self, tags: &[String]) {
        if tags.is_empty() {
            self.0.clear();
            return;
        }
        if tags.len() >= 32 {
            // todo: make named constant
            let set: AHashSet<_> = AHashSet::from_iter(tags);
            self.0.retain(|tag| set.contains(&tag.name));
        } else {
            self.0.retain(|tag| tags.contains(&tag.name));
        }
    }

    pub fn remove_label(&mut self, name: &str) -> Option<Label> {
        self.0
            .iter()
            .position(|label| label.name == name)
            .map(|pos| self.0.remove(pos))
    }

    pub fn reset_metric_group(&mut self) {
        self.0.retain(|label| label.name != METRIC_NAME_LABEL);
    }

    pub(crate) fn into_grouping_labels(self, modifier: Option<&LabelModifier>) -> Self {
        let mut this = self;
        match modifier {
            None => Self(vec![]), // No grouping, return empty labels
            Some(LabelModifier::Include(label_list)) => {
                // Keep only specified labels
                this.retain(|k| label_list.labels.contains(&k.name));
                this
            }
            Some(LabelModifier::Exclude(label_list)) => {
                // Remove specified labels
                this.retain(|k| !label_list.labels.contains(&k.name));
                this
            }
        }
    }

    pub(crate) fn compute_grouping_labels(&self, modifier: Option<&LabelModifier>) -> Self {
        let this = self.clone();
        this.into_grouping_labels(modifier)
    }

    pub fn get_fingerprint(&self) -> SeriesFingerprint {
        self.0.fingerprint()
    }

    /// Construct from pairs (for tests). Sorts on construction.
    #[cfg(test)]
    pub(crate) fn from_pairs(pairs: &[(&str, &str)]) -> Self {
        let mut vec: Vec<Label> = pairs
            .iter()
            .map(|(k, v)| Label {
                name: k.to_string(),
                value: v.to_string(),
            })
            .collect();
        vec.sort();
        Labels::new(vec)
    }
}

impl Display for Labels {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{{", self.metric_name())?;

        let mut first = true;
        for label in self
            .0
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

impl Ord for Labels {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

impl PartialOrd for Labels {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl AsRef<[Label]> for Labels {
    fn as_ref(&self) -> &[Label] {
        &self.0
    }
}

impl Serialize for Labels {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for label in &self.0 {
            map.serialize_entry(&label.name, &label.value)?;
        }
        map.end()
    }
}

impl From<AHashMap<String, String>> for Labels {
    fn from(map: AHashMap<String, String>) -> Self {
        let mut labels: Vec<Label> = map
            .into_iter()
            .map(|(name, value)| Label { name, value })
            .collect();
        labels.sort();
        Self(labels)
    }
}

impl From<Labels> for AHashMap<String, String> {
    fn from(labels: Labels) -> Self {
        labels.0.into_iter().map(|l| (l.name, l.value)).collect()
    }
}

impl From<&MetricName> for Labels {
    fn from(metric_name: &MetricName) -> Self {
        let mut labels = Vec::with_capacity(metric_name.len());
        for label in metric_name.iter() {
            labels.push(Label::new(label.name.to_string(), label.value.to_string()));
        }
        Self::new(labels)
    }
}

impl From<&Labels> for MetricName {
    fn from(labels: &Labels) -> Self {
        MetricName::from(labels.as_ref())
    }
}

impl<'de> Deserialize<'de> for Labels {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct LabelsVisitor;

        impl<'de> Visitor<'de> for LabelsVisitor {
            type Value = Labels;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a map of label name to label value")
            }

            fn visit_map<M: MapAccess<'de>>(self, mut access: M) -> Result<Labels, M::Error> {
                let mut labels = Vec::with_capacity(access.size_hint().unwrap_or(0));
                while let Some((name, value)) = access.next_entry::<String, String>()? {
                    labels.push(Label { name, value });
                }
                labels.sort();
                Ok(Labels(labels))
            }
        }

        deserializer.deserialize_map(LabelsVisitor)
    }
}

impl From<Labels> for HashMap<String, String> {
    fn from(labels: Labels) -> Self {
        labels.0.into_iter().map(|l| (l.name, l.value)).collect()
    }
}

impl From<HashMap<String, String>> for Labels {
    fn from(map: HashMap<String, String>) -> Self {
        let mut labels: Vec<Label> = map
            .into_iter()
            .map(|(name, value)| Label { name, value })
            .collect();
        labels.sort();
        Self(labels)
    }
}

impl From<&HashMap<String, String>> for Labels {
    fn from(map: &HashMap<String, String>) -> Self {
        let mut labels: Vec<Label> = map
            .iter()
            .map(|(name, value)| Label {
                name: name.clone(),
                value: value.clone(),
            })
            .collect();
        labels.sort();
        Self(labels)
    }
}

impl From<Vec<Label>> for Labels {
    fn from(labels: Vec<Label>) -> Self {
        let mut sorted_labels = labels;
        sorted_labels.sort();
        Self(sorted_labels)
    }
}

/// The result of an instant PromQL query.
///
/// PromQL expressions evaluate to either a scalar (e.g. `1+1`), a
/// vector of time series samples (e.g. `http_requests_total`), or a
/// matrix of range samples (e.g. `http_requests_total[5m]`).
#[derive(Debug, Clone)]
pub enum QueryValue {
    Scalar { timestamp_ms: i64, value: f64 },
    Vector(Vec<InstantSample>),
    Matrix(Vec<RangeSample>),
    String(String),
}

impl QueryValue {
    /// Convert into the most general representation (`Vec<RangeSample>`).
    ///
    /// - `Scalar` becomes a single `RangeSample` with empty labels and one sample.
    /// - `Vector` becomes one `RangeSample` per instant sample (each with one point).
    /// - `Matrix` is returned as-is.
    pub fn into_matrix(self) -> QueryResult<Vec<RangeSample>> {
        let matrix = match self {
            QueryValue::Scalar {
                timestamp_ms,
                value,
            } => vec![RangeSample {
                labels: Labels::empty(),
                samples: vec![Sample::new(timestamp_ms, value)],
            }],
            QueryValue::Vector(samples) => samples
                .into_iter()
                .map(|s| RangeSample {
                    labels: s.labels,
                    samples: vec![Sample::new(s.timestamp_ms, s.value)],
                })
                .collect(),
            QueryValue::Matrix(range_samples) => range_samples,
            QueryValue::String(s) => {
                let msg = format!("cannot convert string value '{}' to matrix", s);
                return Err(QueryError::Execution(msg));
            }
        };
        Ok(matrix)
    }

    pub fn into_vector(self) -> QueryResult<Vec<InstantSample>> {
        let vector = match self {
            QueryValue::Scalar {
                timestamp_ms,
                value,
            } => vec![InstantSample {
                labels: Labels::empty(),
                timestamp_ms,
                value,
            }],
            QueryValue::Vector(samples) => samples,
            // todo: raise error if any range sample has more than 1 point, since we can't represent that in a vector
            QueryValue::Matrix(range_samples) => range_samples
                .into_iter()
                .flat_map(|rs| {
                    rs.samples.into_iter().map(move |s| InstantSample {
                        labels: rs.labels.clone(),
                        timestamp_ms: s.timestamp,
                        value: s.value,
                    })
                })
                .collect(),
            QueryValue::String(_) => {
                return Err(QueryError::Execution(
                    "cannot convert string to vector".into(),
                ));
            }
        };
        Ok(vector)
    }

    pub fn value_type(&self) -> ValueType {
        match self {
            QueryValue::Scalar { .. } => ValueType::Scalar,
            QueryValue::Vector { .. } => ValueType::Vector,
            QueryValue::Matrix { .. } => ValueType::Matrix,
            QueryValue::String(_) => ValueType::String,
        }
    }
}

impl From<ExprResult> for QueryValue {
    fn from(value: ExprResult) -> Self {
        match value {
            ExprResult::String(s) => QueryValue::String(s),
            ExprResult::RangeVector(samples) => {
                let _samples = samples.into_iter().map(|s| s.into()).collect();
                QueryValue::Matrix(_samples)
            }
            ExprResult::InstantVector(samples) => {
                let _samples = samples.into_iter().map(|s| s.into()).collect();
                QueryValue::Vector(_samples)
            }
            ExprResult::Scalar(value) => {
                // fake a value. caller should handle filling in
                let timestamp_ms = valkey_cached_time_millis();
                QueryValue::Scalar {
                    timestamp_ms,
                    value,
                }
            }
        }
    }
}

impl From<Vec<InstantSample>> for QueryValue {
    fn from(samples: Vec<InstantSample>) -> Self {
        QueryValue::Vector(samples)
    }
}

impl From<Sample> for QueryValue {
    fn from(sample: Sample) -> Self {
        QueryValue::Scalar {
            timestamp_ms: sample.timestamp,
            value: sample.value,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct EvalContext {
    pub query_start: Timestamp,
    pub query_end: Timestamp,
    pub evaluation_ts: Timestamp,
    pub step_ms: i64,
    pub lookback_delta_ms: i64,
    pub tracing_enabled: bool,
}

impl EvalContext {
    pub fn for_vector_selector(query_time: Timestamp, lookback_delta_ms: i64) -> Self {
        EvalContext {
            query_start: query_time,
            query_end: query_time,
            evaluation_ts: query_time,
            step_ms: 0,
            lookback_delta_ms,
            tracing_enabled: false,
        }
    }

    pub fn num_steps(&self) -> usize {
        if self.step_ms == 0 {
            return 0;
        }
        ((self.query_end - self.query_start) / self.step_ms) as usize + 1
    }

    pub fn get_timestamps(&self) -> Vec<Timestamp> {
        if self.step_ms == 0 {
            return vec![];
        }
        // todo: have an upper limit
        let capacity = self.num_steps();
        let mut timestamps = Vec::with_capacity(capacity);
        for timestamp in (self.query_start..=self.query_end).step_by(self.step_ms as usize) {
            timestamps.push(timestamp);
        }
        timestamps
    }

    pub fn align_start_end(&mut self) {
        let start = self.query_start;
        let end = self.query_end;
        // Round start to the nearest smaller value divisible by step.
        self.query_start = start - start % self.step_ms;
        // Round end to the nearest bigger value divisible by step.
        let adjust = end % self.step_ms;
        if adjust > 0 {
            self.query_end += self.step_ms - adjust
        }
    }
}

const DEFAULT_LOOKBACK_DELTA: i64 = (5 * MILLIS_PER_MIN) as i64;
impl Default for EvalContext {
    fn default() -> Self {
        let now = current_time_millis();
        let lookback = DEFAULT_LOOKBACK_DELTA;
        let query_end = now;
        let query_start = now - lookback;
        let evaluation_ts = query_end;

        Self {
            query_start,
            query_end,
            lookback_delta_ms: lookback,
            step_ms: 0,
            evaluation_ts,
            tracing_enabled: false,
        }
    }
}
impl From<&EvalStmt> for EvalContext {
    fn from(value: &EvalStmt) -> Self {
        // Convert SystemTime to Timestamp at the entry point
        let query_start = system_time_to_millis(value.start);
        let query_end = system_time_to_millis(value.end);
        let evaluation_ts = query_end; // using end follows the "as-of" convention
        let interval_ms = value.interval.as_millis() as i64;
        let lookback_delta_ms = value.lookback_delta.as_millis() as i64;
        Self {
            query_start,
            query_end,
            evaluation_ts,
            lookback_delta_ms,
            step_ms: interval_ms,
            tracing_enabled: false,
        }
    }
}

/// A single series value at a point in time.
///
/// Returned by instant (point-in-time) PromQL queries.
#[derive(Debug, Clone)]
pub struct InstantSample {
    /// The labels identifying this series.
    pub labels: Labels,
    /// Timestamp in milliseconds since Unix epoch.
    pub timestamp_ms: i64,
    /// The sample value.
    pub value: f64,
}

impl From<EvalSample> for InstantSample {
    fn from(sample: EvalSample) -> Self {
        Self {
            labels: sample.labels,
            value: sample.value,
            timestamp_ms: sample.timestamp_ms,
        }
    }
}

/// A series with values over a time range.
///
/// Returned by range PromQL queries.
#[derive(Debug, Clone)]
pub struct RangeSample {
    /// The labels identifying this series.
    pub labels: Labels,
    /// Samples ordered by timestamp.
    pub samples: Vec<Sample>,
}

impl From<EvalSamples> for RangeSample {
    fn from(samples: EvalSamples) -> Self {
        Self {
            labels: samples.labels,
            samples: samples.values,
        }
    }
}

impl RangeSample {
    pub fn fingerprint(&self) -> u128 {
        self.labels.signature()
    }
}

/// Options for PromQL query evaluation.
///
/// Provides tuning knobs that apply to both instant and range queries.
/// Use `Default::default()` for Prometheus-compatible defaults.
#[derive(Debug, Clone)]
pub struct QueryOptions {
    /// How far back to look for a sample when evaluating at a given timestamp.
    ///
    /// Defaults to 5 minutes (the Prometheus staleness delta).
    pub lookback_delta: Duration,
}

impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            lookback_delta: Duration::from_secs(5 * 60),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn should_create_label() {
        let label = Label::new("env", "prod");
        assert_eq!(label.name, "env");
        assert_eq!(label.value, "prod");
    }

    #[test]
    fn should_create_metric_name_label() {
        let label = Label::metric_name("http_requests");
        assert_eq!(label.name, "__name__");
        assert_eq!(label.value, "http_requests");
    }

    #[test]
    fn should_create_sample() {
        let sample = Sample::new(1700000000000, 42.5);
        assert_eq!(sample.timestamp, 1700000000000);
        assert_eq!(sample.value, 42.5);
    }

    #[test]
    fn should_create_sample_now() {
        let before = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let sample = Sample::now(100.0);
        let after = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        assert!(sample.timestamp >= before);
        assert!(sample.timestamp <= after);
        assert_eq!(sample.value, 100.0);
    }

    #[test]
    fn into_matrix_from_scalar() {
        let qv = QueryValue::Scalar {
            timestamp_ms: 5000,
            value: 42.0,
        };
        let result = qv.into_matrix().unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].labels, Labels::empty());
        assert_eq!(result[0].samples, vec![Sample::new(5000, 42.0)]);
    }

    #[test]
    fn into_matrix_from_vector() {
        let qv = QueryValue::Vector(vec![
            InstantSample {
                labels: Labels::new(vec![Label::metric_name("cpu")]),
                timestamp_ms: 1000,
                value: 1.0,
            },
            InstantSample {
                labels: Labels::new(vec![Label::metric_name("mem")]),
                timestamp_ms: 2000,
                value: 2.0,
            },
        ]);
        let result = qv.into_matrix().unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].labels.get("__name__").unwrap(), "cpu");
        assert_eq!(result[0].samples, vec![Sample::new(1000, 1.0)]);
        assert_eq!(result[1].labels.get("__name__").unwrap(), "mem");
        assert_eq!(result[1].samples, vec![Sample::new(2000, 2.0)]);
    }

    #[test]
    fn into_matrix_from_matrix_is_identity() {
        let range_samples = vec![
            RangeSample {
                labels: Labels::new(vec![Label::metric_name("cpu")]),
                samples: vec![Sample::new(1000, 1.0), Sample::new(2000, 2.0)],
            },
            RangeSample {
                labels: Labels::new(vec![Label::metric_name("mem")]),
                samples: vec![Sample::new(3000, 3.0)],
            },
        ];
        let qv = QueryValue::Matrix(range_samples.clone());
        let result = qv.into_matrix().unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].labels, range_samples[0].labels);
        assert_eq!(result[0].samples, range_samples[0].samples);
        assert_eq!(result[1].labels, range_samples[1].labels);
        assert_eq!(result[1].samples, range_samples[1].samples);
    }

    #[test]
    fn into_matrix_empty() {
        let qv = QueryValue::Matrix(vec![]);
        assert!(qv.into_matrix().unwrap().is_empty());

        let qv = QueryValue::Vector(vec![]);
        assert!(qv.into_matrix().unwrap().is_empty());
    }
}
