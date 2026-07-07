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
        let matchers: Matchers = normalize_selector(selector);
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
        let matchers: Matchers = normalize_selector(selector);
        match SERIES_SELECTOR.query_range(matchers, start_ms, end_ms, options) {
            Ok(QueryValue::Matrix(samples)) => Ok(samples),
            Err(e) => Err(e),
            _ => Err(QueryError::Execution(
                "unexpected query result type".to_string(),
            )),
        }
    }
}

/// Normalize a parsed [`VectorSelector`] into the [`Matchers`] representation consumed by
/// the selector executor, folding the optional metric name into an explicit `__name__`
/// equality matcher.
///
/// This is the single point at which the query path performs this transformation: the
/// resulting `Matchers` is the shared normalized form that both the local execution path
/// (`Matchers` -> `SeriesSelector`) and the fanout path (`Matchers` -> proto request) build
/// from, so the two stay in sync. `Matchers` is used as the intermediate — rather than the
/// crate-native `SeriesSelector` — because it preserves the original regex source strings
/// needed for lossless fanout serialization.
fn normalize_selector(selector: &VectorSelector) -> Matchers {
    let Some(ref name) = selector.name else {
        return selector.matchers.clone();
    };

    let name_matcher = Matcher {
        op: MatchOp::Equal,
        name: METRIC_NAME.to_string(),
        value: name.clone(),
    };

    let src = &selector.matchers;
    if src.or_matchers.is_empty() {
        // Build in a single pass with the name matcher up front to avoid the extra
        // reallocation a clone-then-push incurs.
        let mut matchers = Vec::with_capacity(src.matchers.len() + 1);
        matchers.push(name_matcher);
        matchers.extend(src.matchers.iter().cloned());
        Matchers {
            matchers,
            or_matchers: Vec::new(),
        }
    } else {
        // A vector selector produced by the parser never carries top-level OR groups, but
        // handle it defensively so the metric name is folded into every group rather than
        // silently dropped by the downstream `matchers`-first conversion.
        let or_matchers = src
            .or_matchers
            .iter()
            .map(|group| {
                let mut g = Vec::with_capacity(group.len() + 1);
                g.push(name_matcher.clone());
                g.extend(group.iter().cloned());
                g
            })
            .collect();
        Matchers {
            matchers: src.matchers.clone(),
            or_matchers,
        }
    }
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
