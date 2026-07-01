use crate::promql::engine::QueryReader;
use crate::promql::engine::memory_series_querier::MemorySeriesQuerier;
use crate::promql::engine::selector_batch_executor::SelectorBatchExecutor;
use crate::promql::{
    InstantSample, PromqlResult, QueryError, QueryOptions, QueryResult, QueryValue, RangeSample,
};
use cfg_if::cfg_if;
use promql_parser::label::{METRIC_NAME, MatchOp, Matcher, Matchers};
use promql_parser::parser::VectorSelector;
use std::sync::LazyLock;
use valkey_module::Context;

pub static SERIES_SELECTOR: LazyLock<SelectorBatchExecutor> =
    LazyLock::new(SelectorBatchExecutor::new);

/// A concrete implementation of QueryReader that queries the Valkey keyspace.
pub struct ValkeySeriesQuerier;

impl QueryReader for ValkeySeriesQuerier {
    fn query(
        &self,
        selector: &VectorSelector,
        timestamp: i64,
        options: QueryOptions,
    ) -> QueryResult<Vec<InstantSample>> {
        let matchers: Matchers = extract_matchers(selector);
        match SERIES_SELECTOR.query(matchers, timestamp, options) {
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
        match SERIES_SELECTOR.query_range(matchers, start_ms, end_ms, options) {
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

/// A concrete implementation of QueryReader that can be either a ValkeySeriesQuerier or a MemorySeriesQuerier.
/// Use this enum to create a concrete series querier that can be used in the PromQL engine irrespective of
/// whether we are in a test or production environment.
pub(crate) enum ConcreteSeriesQuerier {
    Actual(ValkeySeriesQuerier),
    Mock(MemorySeriesQuerier),
}

impl ConcreteSeriesQuerier {
    pub fn create(_ctx: &Context) -> Self {
        cfg_if! {
            if #[cfg(test)] {
                ConcreteSeriesQuerier::Mock(MemorySeriesQuerier::new())
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
