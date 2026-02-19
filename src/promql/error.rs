//! Error types for OpenData TimeSeries operations.
//!
//! This module defines [`Error`], the primary error type for all time series
//! operations, along with a convenient [`Result`] type alias.

use crate::promql::EvaluationError;

/// Error type for PromQL query and discovery operations.
///
/// This is returned by the read/query methods on `TimeSeriesDb`.
#[derive(Debug, thiserror::Error)]
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
}

impl From<EvaluationError> for QueryError {
    fn from(err: EvaluationError) -> Self {
        QueryError::Execution(err.to_string())
    }
}

pub type PromqlResult<T> = Result<T, QueryError>;
pub type QueryResult<T> = Result<T, QueryError>;
