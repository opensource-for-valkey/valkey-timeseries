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

    /// The client disconnected while its query waited or ran, so it was abandoned. The
    /// reply, if any, goes nowhere: this is for logs and counters.
    #[error("query cancelled: the client disconnected")]
    Cancelled,

    /// An error occurred during query execution.
    #[error("execution error: {0}")]
    Execution(String),

    /// The query would load more samples than `ts-promql-max-samples-per-query`
    /// allows. Its own variant so that a preload can tell it from a reader
    /// limit that legitimately degrades to per-step reads: this one is
    /// query-wide and already exceeded, so the query fails at once.
    #[error(
        "{prefix}: {loaded} > {limit} (ts-promql-max-samples-per-query)",
        prefix = crate::error_consts::PROMQL_TOO_MANY_SAMPLES_ERROR
    )]
    TooManySamples { loaded: usize, limit: usize },

    /// This node, or a shard the query reads from, refuses PromQL work for load: see
    /// [`crate::error_consts::PROMQL_OVERLOADED_ERROR`]. The text says which limit and why.
    #[error("{0}")]
    Overloaded(String),

    /// The reply, estimated before any of it is written, is larger than
    /// `ts-promql-max-reply-bytes`.
    #[error(
        "{prefix}: about {bytes} bytes > {limit} (ts-promql-max-reply-bytes)",
        prefix = crate::error_consts::PROMQL_REPLY_TOO_LARGE_ERROR
    )]
    ReplyTooLarge { bytes: usize, limit: usize },
}

impl QueryError {
    /// Whether this error ends the query rather than downgrading a best-effort read: a
    /// passed deadline, a client that has gone, a sample budget already spent, or a node
    /// refusing for load. Each only gets worse, so the reads a declined preload falls back
    /// to would be refused one by one.
    pub fn ends_query(&self) -> bool {
        matches!(
            self,
            QueryError::Timeout
                | QueryError::Cancelled
                | QueryError::TooManySamples { .. }
                | QueryError::Overloaded(_)
        )
    }
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
