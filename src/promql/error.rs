//! Error types for OpenData TimeSeries operations.
//!
//! This module defines [`Error`], the primary error type for all time series
//! operations, along with a convenient [`Result`] type alias.

use crate::promql::EvaluationError;

/// Error type for PromQL query and discovery operations.
///
/// This is returned by the read/query methods on `TimeSeriesDb`.
#[derive(Debug, Clone, thiserror::Error)]
pub enum QueryError {
    /// The query string could not be parsed or is otherwise invalid.
    #[error("invalid query: {0}")]
    InvalidQuery(String),

    /// The query exceeded the configured timeout.
    #[error("query timed out")]
    Timeout,

    /// An error occurred during query execution.
    #[error("execution error: {0}")]
    Execution(String),

    /// The query would load more samples than `ts-promql-max-samples-per-query`
    /// allows. Its own variant so that a preload can tell it from a reader
    /// limit that legitimately degrades to per-step reads: this one is
    /// query-wide and already exceeded, so the query fails at once.
    #[error(
        "query processing would load too many samples into memory: {loaded} > {limit} (ts-promql-max-samples-per-query)"
    )]
    TooManySamples { loaded: usize, limit: usize },
}

impl From<EvaluationError> for QueryError {
    fn from(err: EvaluationError) -> Self {
        match err {
            // Unwrap reader/nested-query errors so the original kind survives
            // the evaluator round trip (e.g. Timeout stays Timeout instead of
            // becoming an Execution string).
            EvaluationError::Query(err) => err,
            other => QueryError::Execution(other.to_string()),
        }
    }
}

pub type PromqlResult<T> = Result<T, QueryError>;
pub type QueryResult<T> = Result<T, QueryError>;
