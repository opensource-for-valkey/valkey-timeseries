//! Core data types for Promql.
//!
//! This module defines the fundamental data structures used in the public API,
//! including labels for series identification, samples for data points, and
//! series for batched ingestion.
use crate::common::constants::MILLIS_PER_MIN;
use crate::common::time::{current_time_millis, system_time_to_millis, valkey_cached_time_millis};
use crate::common::{Sample, Timestamp};
use crate::labels::Labels;
use crate::promql::{EvalSample, EvalSamples, ExprResult, QueryError, QueryResult};
use promql_parser::parser::EvalStmt;
use promql_parser::parser::value::ValueType;

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
/// ```ignore
/// use valkey_timeseries::promql::model::{is_stale_nan, STALE_NAN};
///
/// let stale = f64::from_bits(STALE_NAN);
/// assert!(is_stale_nan(stale));
/// assert!(!is_stale_nan(f64::NAN));
/// assert!(!is_stale_nan(42.0));
/// ```
pub fn is_stale_nan(v: f64) -> bool {
    v.to_bits() == STALE_NAN
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
}

impl EvalContext {
    pub fn for_vector_selector(query_time: Timestamp, lookback_delta_ms: i64) -> Self {
        EvalContext {
            query_start: query_time,
            query_end: query_time,
            evaluation_ts: query_time,
            step_ms: 0,
            lookback_delta_ms,
        }
    }

    pub fn expected_steps(&self) -> usize {
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
        let capacity = self.expected_steps();
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

    pub fn is_range_selector(&self) -> bool {
        self.step_ms > 0 && self.query_end > self.query_start
    }

    pub fn is_instant_selector(&self) -> bool {
        self.step_ms == 0
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
            labels: sample.labels.into_labels(),
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
            labels: samples.labels.into_labels(),
            samples: samples.values,
        }
    }
}

impl RangeSample {
    pub fn fingerprint(&self) -> u128 {
        self.labels.signature()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::labels::Label;
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
