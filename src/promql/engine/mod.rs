mod fanout;
pub mod memory_series_querier;
pub mod promql_config;
pub mod promql_engine;
mod query_limits;
pub mod query_reader;
mod query_stats;
mod selector_batch_executor;
mod querier;

use crate::common::time::current_time_millis;
use crate::common::Timestamp;
pub(crate) use fanout::*;
pub use promql_engine::{evaluate_instant, evaluate_range};
pub(in crate::promql) use query_limits::*;
pub use query_reader::*;
use std::time::Duration;

pub use promql_config::*;
pub use querier::*;

/// Options for PromQL query evaluation.
///
/// Provides tuning knobs that apply to both instant and range queries.
/// Use `Default::default()` for Prometheus-compatible defaults.
#[derive(Debug, Clone, Copy)]
pub struct QueryOptions {
    /// How far back to look for a sample when evaluating at a given timestamp.
    ///
    /// Defaults to 5 minutes (the Prometheus staleness delta).
    pub lookback_delta: Duration,
    /// Optional per-query timeout. If set, the engine will attempt to abort long-running
    /// evaluations and return a `QueryError::Timeout` when the deadline elapses.
    /// NOTE: this timeout is best-effort at the moment;
    pub timeout: Option<Duration>,
    /// Query deadline in epoch milliseconds
    pub deadline: Option<Timestamp>,
    /// The maximum number of series to return from instant or range queries. This option allows limiting memory usage.
    pub max_series: usize,
    /// The maximum number of data points to return per series for each series. This is to help guard against
    /// OOMs and accidental self-DOS, especially in cluster mode.
    pub max_points_per_series: Option<usize>,
    /// Enable tracing for the current request
    pub is_tracing: bool,
    /// Enable experimental functions for the current request
    pub enable_experimental_functions: bool,
    /// Whether to optimize the queries by simplify the query plan and pushing down filters to the data source.
    /// This can improve performance but may cause higher memory usage and slower response times for some queries.
    pub optimize_queries: bool,
    /// The db in which to execute the query
    pub db: i32,
}

impl Default for QueryOptions {
    fn default() -> Self {
        let config = PROMQL_CONFIG.read().unwrap();
        let timeout = config.max_query_duration;
        let deadline = current_time_millis().saturating_add(timeout.as_millis() as i64);
        let enable_experimental_functions = config.enable_experimental_functions;
        Self {
            lookback_delta: config.lookback_delta,
            timeout: Some(config.max_query_duration),
            deadline: Some(deadline),
            max_series: config.max_response_series,
            max_points_per_series: if config.max_points_per_timeseries > 0
                && config.max_points_per_timeseries != usize::MAX
            {
                Some(config.max_points_per_timeseries)
            } else {
                None
            },
            is_tracing: false,
            enable_experimental_functions,
            optimize_queries: config.optimize_queries,
            db: 0,
        }
    }
}