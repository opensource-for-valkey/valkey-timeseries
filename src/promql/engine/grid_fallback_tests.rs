//! A peer without grid push-down must not fail queries.
//!
//! During a rolling upgrade an older peer rejects the grid operation as unknown. The coordinator
//! then reports [`GridOutcome::Unsupported`], and every place the evaluator asks for a grid
//! falls back to the selector reads every build has. The results must be exactly those of a
//! source that answers the grid requests itself.
use crate::common::Sample;
use crate::labels::Labels;
use crate::promql::engine::label_profile::LabelProfile;
use crate::promql::engine::memory_series_querier::MemorySeriesQuerier;
use crate::promql::engine::query_reader::{
    AggregationOutcome, AggregationRequest, GridOutcome, GridRequest,
};
use crate::promql::engine::{QueryReader, evaluate_instant, evaluate_range};
use crate::promql::model::{InstantSample, QueryValue, RangeSample};
use crate::promql::{EvalLabels, PromqlResult, QueryOptions};
use promql_parser::parser::{EvalStmt, VectorSelector, parse};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, UNIX_EPOCH};

/// Answers every grid request the way a coordinator does when a peer lacks grid push-down.
struct NoGridPushdown {
    inner: Arc<dyn QueryReader>,
    grid_calls: AtomicUsize,
}

impl QueryReader for NoGridPushdown {
    fn query(
        &self,
        selector: &VectorSelector,
        timestamp: i64,
        options: QueryOptions,
    ) -> PromqlResult<Vec<InstantSample<EvalLabels>>> {
        self.inner.query(selector, timestamp, options)
    }

    fn query_range(
        &self,
        selector: &VectorSelector,
        start_ms: i64,
        end_ms: i64,
        options: QueryOptions,
    ) -> PromqlResult<Vec<RangeSample<EvalLabels>>> {
        self.inner.query_range(selector, start_ms, end_ms, options)
    }

    fn query_aggregation(
        &self,
        selector: &VectorSelector,
        timestamp: i64,
        aggregation: &AggregationRequest,
        options: QueryOptions,
    ) -> PromqlResult<AggregationOutcome> {
        self.inner
            .query_aggregation(selector, timestamp, aggregation, options)
    }

    fn query_grid(
        &self,
        _selector: &VectorSelector,
        _request: &GridRequest,
        _options: QueryOptions,
    ) -> PromqlResult<GridOutcome> {
        self.grid_calls.fetch_add(1, Ordering::Relaxed);
        Ok(GridOutcome::Unsupported)
    }

    fn label_profile(
        &self,
        selector: &VectorSelector,
        options: QueryOptions,
    ) -> PromqlResult<Option<LabelProfile>> {
        self.inner.label_profile(selector, options)
    }
}

fn data() -> Arc<dyn QueryReader> {
    let querier = MemorySeriesQuerier::new();
    for (job, instance, scale) in [("api", "a", 1.0), ("api", "b", 2.0), ("db", "a", 3.0)] {
        let labels = Labels::from_pairs(&[("__name__", "m"), ("job", job), ("instance", instance)]);
        for t in 0..=120 {
            querier.add_sample(&labels, Sample::new(t * 15_000, t as f64 * scale));
        }
    }
    Arc::new(querier)
}

fn options() -> QueryOptions {
    QueryOptions {
        timeout: None,
        deadline: None,
        ..QueryOptions::default()
    }
}

/// Every shape the evaluator asks a grid for: stepped selections, rollups, rollups fused with
/// an aggregation, aggregations over a bare selector, and the same inside a subquery.
const QUERIES: &[&str] = &[
    "m",
    "m offset 1m",
    "rate(m[5m])",
    "max_over_time(m[2m])",
    "sum(rate(m[5m]))",
    "sum by (job) (rate(m[5m]))",
    "count(increase(m[5m]))",
    "topk(1, rate(m[5m]))",
    "sum by (job) (m)",
    "max(m)",
    "avg_over_time(sum(m)[5m:1m])",
    "max_over_time(rate(m[2m])[10m:1m])",
];

fn range(reader: Arc<dyn QueryReader>, query: &str) -> String {
    let stmt = EvalStmt {
        expr: parse(query).unwrap(),
        start: UNIX_EPOCH + Duration::from_secs(600),
        end: UNIX_EPOCH + Duration::from_secs(1500),
        interval: Duration::from_secs(60),
        lookback_delta: options().lookback_delta,
    };
    let result = evaluate_range(reader, stmt, options()).unwrap_or_else(|e| panic!("{query}: {e}"));
    format!("{result:?}")
}

fn instant(reader: Arc<dyn QueryReader>, query: &str) -> String {
    let at = UNIX_EPOCH + Duration::from_secs(1500);
    let stmt = EvalStmt {
        expr: parse(query).unwrap(),
        start: at,
        end: at,
        interval: Duration::ZERO,
        lookback_delta: options().lookback_delta,
    };
    let result =
        evaluate_instant(reader, stmt, at, options()).unwrap_or_else(|e| panic!("{query}: {e}"));
    match result {
        // An instant vector's order is not part of its meaning.
        QueryValue::Vector(mut samples) => {
            samples.sort_by(|a, b| a.labels.cmp(&b.labels));
            format!("{samples:?}")
        }
        other => format!("{other:?}"),
    }
}

#[test]
fn queries_fall_back_when_a_peer_lacks_grid_pushdown() {
    let no_grid = Arc::new(NoGridPushdown {
        inner: data(),
        grid_calls: AtomicUsize::new(0),
    });
    for &query in QUERIES {
        let reader: Arc<dyn QueryReader> = no_grid.clone();
        assert_eq!(
            range(reader.clone(), query),
            range(data(), query),
            "range {query}"
        );
        assert_eq!(
            instant(reader, query),
            instant(data(), query),
            "instant {query}"
        );
    }
    assert!(
        no_grid.grid_calls.load(Ordering::Relaxed) > 0,
        "the queries must have asked for grids, or this tests nothing"
    );
}
