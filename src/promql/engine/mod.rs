mod batch_worker;
pub mod config;
mod fanout;
mod mock_series_querier;
mod promql_engine;
mod querier;
mod query_stats;
mod query_worker;

use crate::promql::engine::mock_series_querier::MockSeriesQuerier;
use crate::promql::engine::query_worker::QueryWorker;
use crate::promql::model::{InstantSample, RangeSample};
use crate::promql::{PromqlResult, QueryError, QueryResult, QueryValue};
pub(crate) use batch_worker::*;
use cfg_if::cfg_if;
pub(crate) use fanout::*;
pub(crate) use promql_engine::*;
use promql_parser::label::{METRIC_NAME, MatchOp, Matcher, Matchers};
use promql_parser::parser::VectorSelector;
pub(crate) use querier::*;
use std::sync::LazyLock;
use valkey_module::Context;

pub static QUERY_WORKER: LazyLock<QueryWorker> = LazyLock::new(QueryWorker::new);

pub struct ValkeySeriesQuerier;

impl SeriesQuerier for ValkeySeriesQuerier {
    fn query(&self, selector: &VectorSelector, timestamp: i64) -> QueryResult<Vec<InstantSample>> {
        let matchers: Matchers = extract_matchers(selector);
        match QUERY_WORKER.query(matchers, timestamp) {
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
    ) -> QueryResult<Vec<RangeSample>> {
        let matchers: Matchers = extract_matchers(selector);
        match QUERY_WORKER.query_range(matchers, start_ms, end_ms) {
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

    pub fn as_series_querier(&self) -> &dyn SeriesQuerier {
        match self {
            ConcreteSeriesQuerier::Actual(local) => local,
            ConcreteSeriesQuerier::Mock(mock) => mock,
        }
    }
}

impl SeriesQuerier for ConcreteSeriesQuerier {
    fn query(&self, selector: &VectorSelector, timestamp: i64) -> PromqlResult<Vec<InstantSample>> {
        match self {
            ConcreteSeriesQuerier::Actual(local) => local.query(selector, timestamp),
            ConcreteSeriesQuerier::Mock(mock) => mock.query(selector, timestamp),
        }
    }

    fn query_range(
        &self,
        selector: &VectorSelector,
        start_ms: i64,
        end_ms: i64,
    ) -> PromqlResult<Vec<RangeSample>> {
        match self {
            ConcreteSeriesQuerier::Actual(local) => local.query_range(selector, start_ms, end_ms),
            ConcreteSeriesQuerier::Mock(mock) => mock.query_range(selector, start_ms, end_ms),
        }
    }
}

pub(crate) fn create_series_queries() {
    todo!()
}
