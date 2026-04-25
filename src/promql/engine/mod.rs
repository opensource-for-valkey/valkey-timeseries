pub mod config;
mod fanout;
pub mod mock_series_querier;
pub mod promql_engine;
pub mod query_reader;
mod query_stats;
mod query_worker;

use crate::common::Timestamp;
use crate::common::time::current_time_millis;
use crate::promql::engine::config::PROMQL_CONFIG;
use crate::promql::engine::mock_series_querier::MockSeriesQuerier;
use crate::promql::engine::query_worker::QueryWorker;
use crate::promql::model::{InstantSample, RangeSample};
use crate::promql::{PromqlResult, QueryError, QueryResult, QueryValue};
use cfg_if::cfg_if;
pub(crate) use fanout::*;
pub use promql_engine::{evaluate_instant, evaluate_range};
use promql_parser::label::{METRIC_NAME, MatchOp, Matcher, Matchers};
use promql_parser::parser::VectorSelector;
pub use query_reader::*;
use std::sync::LazyLock;
use std::time::Duration;
use valkey_module::Context;

pub static QUERY_WORKER: LazyLock<QueryWorker> = LazyLock::new(QueryWorker::new);

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
    /// Enable tracing for the current request
    pub is_tracing: bool,
    /// Enable experimental functions for the current request
    pub enable_experimental_functions: bool,
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
            is_tracing: false,
            enable_experimental_functions,
        }
    }
}

pub struct ValkeySeriesQuerier;

impl QueryReader for ValkeySeriesQuerier {
    fn query(
        &self,
        selector: &VectorSelector,
        timestamp: i64,
        options: QueryOptions,
    ) -> QueryResult<Vec<InstantSample>> {
        let matchers: Matchers = extract_matchers(selector);
        match QUERY_WORKER.query(matchers, timestamp, options) {
            Ok(QueryValue::Vector(samples)) => Ok(samples),
            Err(e) => Err(e),
            _ => Err(QueryError::Execution(
                "unexpected query result type".to_string(),
            )),
        }
    }

    fn query_range(
        &self,
        selector: &VectorSelector,
        start_ms: i64,
        end_ms: i64,
        options: QueryOptions,
    ) -> QueryResult<Vec<RangeSample>> {
        let matchers: Matchers = extract_matchers(selector);
        match QUERY_WORKER.query_range(matchers, start_ms, end_ms, options) {
            Ok(QueryValue::Matrix(samples)) => Ok(samples),
            Err(e) => Err(e),
            _ => Err(QueryError::Execution(
                "unexpected query result type".to_string(),
            )),
        }
    }
}

fn extract_matchers(selector: &VectorSelector) -> Matchers {
    let mut matchers = selector.matchers.clone();
    if let Some(ref name) = selector.name {
        matchers.matchers.push(Matcher {
            op: MatchOp::Equal,
            name: METRIC_NAME.to_string(),
            value: name.clone(),
        });
    }
    matchers
}

pub(crate) enum ConcreteSeriesQuerier {
    Actual(ValkeySeriesQuerier),
    Mock(MockSeriesQuerier),
}

impl ConcreteSeriesQuerier {
    pub fn create(_ctx: &Context) -> Self {
        cfg_if! {
            if #[cfg(test)] {
                ConcreteSeriesQuerier::Mock(MockSeriesQuerier::new())
            } else {
                ConcreteSeriesQuerier::Actual(ValkeySeriesQuerier)
            }
        }
    }

    pub fn as_series_querier(&self) -> &dyn QueryReader {
        match self {
            ConcreteSeriesQuerier::Actual(local) => local,
            ConcreteSeriesQuerier::Mock(mock) => mock,
        }
    }
}

impl QueryReader for ConcreteSeriesQuerier {
    fn query(
        &self,
        selector: &VectorSelector,
        timestamp: i64,
        options: QueryOptions,
    ) -> PromqlResult<Vec<InstantSample>> {
        match self {
            ConcreteSeriesQuerier::Actual(local) => local.query(selector, timestamp, options),
            ConcreteSeriesQuerier::Mock(mock) => mock.query(selector, timestamp, options),
        }
    }

    fn query_range(
        &self,
        selector: &VectorSelector,
        start_ms: i64,
        end_ms: i64,
        options: QueryOptions,
    ) -> PromqlResult<Vec<RangeSample>> {
        match self {
            ConcreteSeriesQuerier::Actual(local) => {
                local.query_range(selector, start_ms, end_ms, options)
            }
            ConcreteSeriesQuerier::Mock(mock) => {
                mock.query_range(selector, start_ms, end_ms, options)
            }
        }
    }
}
