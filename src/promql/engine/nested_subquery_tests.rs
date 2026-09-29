//! Nested subqueries multiply their steps level by level, and once their data is preloaded the
//! evaluation is pure CPU: no reader call is left to notice a passed deadline. These pin that the
//! subquery step loop checks it, and that a tree too large to be worth starting is rejected
//! before any work.
use crate::common::Sample;
use crate::common::time::current_time_millis;
use crate::labels::Labels;
use crate::promql::QueryOptions;
use crate::promql::engine::memory_series_querier::MemorySeriesQuerier;
use crate::promql::engine::{QueryReader, evaluate_instant, evaluate_range};
use crate::promql::error::QueryError;
use promql_parser::parser::{EvalStmt, parse};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn reader() -> Arc<dyn QueryReader> {
    let querier = MemorySeriesQuerier::new();
    let labels = Labels::from_pairs(&[("__name__", "m"), ("job", "a")]);
    for t in 0..2_000 {
        querier.add_sample(&labels, Sample::new(t * 10_000, t as f64));
    }
    Arc::new(querier)
}

/// `leaf` wrapped in `depth` subqueries of up to seven steps each.
fn nested(leaf: &str, depth: usize) -> String {
    (0..depth).fold(leaf.to_string(), |expr, _| {
        format!("max_over_time(({expr})[1m:10s])")
    })
}

fn query_time() -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(10_000)
}

fn options(deadline_in_ms: Option<i64>) -> QueryOptions {
    QueryOptions {
        timeout: None,
        deadline: deadline_in_ms.map(|ms| current_time_millis() + ms),
        ..QueryOptions::default()
    }
}

fn instant(query: &str, opts: QueryOptions) -> Result<(), QueryError> {
    let stmt = EvalStmt {
        expr: parse(query).unwrap(),
        start: query_time(),
        end: query_time(),
        interval: Duration::ZERO,
        lookback_delta: Duration::from_secs(300),
    };
    evaluate_instant(reader(), stmt, query_time(), opts).map(|_| ())
}

fn range(query: &str, opts: QueryOptions) -> Result<(), QueryError> {
    let stmt = EvalStmt {
        expr: parse(query).unwrap(),
        start: query_time() - Duration::from_secs(60),
        end: query_time(),
        interval: Duration::from_secs(60),
        lookback_delta: Duration::from_secs(300),
    };
    evaluate_range(reader(), stmt, opts).map(|_| ())
}

/// Nine levels is about 40 million steps: under the up-front cap, and several seconds of work in a
/// debug build, so a 100 ms deadline is only met if evaluation checks it as it goes.
const DEEP: usize = 9;
const DEADLINE_MS: i64 = 100;
/// Generous for a loaded machine, and still far below the unchecked run.
const STOPS_WITHIN: Duration = Duration::from_secs(3);

fn assert_times_out(name: &str, run: impl FnOnce() -> Result<(), QueryError>) {
    let started = Instant::now();
    let result = run();
    let elapsed = started.elapsed();
    assert!(
        matches!(result, Err(QueryError::Timeout)),
        "{name}: expected a timeout, got {result:?}"
    );
    assert!(
        elapsed < STOPS_WITHIN,
        "{name}: the deadline was noticed only after {elapsed:?}"
    );
}

#[test]
fn nested_subqueries_stop_at_the_deadline() {
    // A leaf with no reader call at all, a selector, and a rollup the preload fetches once up
    // front: in none of them is a read left to notice the deadline during the step walk.
    for leaf in ["vector(1)", "m", "rate(m[1m])"] {
        let query = nested(leaf, DEEP);
        assert_times_out(&format!("instant {leaf}"), || {
            instant(&query, options(Some(DEADLINE_MS)))
        });
        assert_times_out(&format!("range {leaf}"), || {
            range(&query, options(Some(DEADLINE_MS)))
        });
    }
}

#[test]
fn nested_subqueries_evaluate_when_cheap() {
    let query = nested("m", 3);
    instant(&query, options(Some(60_000))).unwrap();
    range(&query, options(Some(60_000))).unwrap();
}

#[test]
fn a_subquery_tree_too_large_to_start_is_rejected_before_any_work() {
    // Ten levels is about 280 million steps, over the cap. No deadline, so only the cap stops it.
    let query = nested("vector(1)", 10);
    let started = Instant::now();
    for result in [instant(&query, options(None)), range(&query, options(None))] {
        let err = result.expect_err("over the subquery step cap");
        assert!(
            err.to_string()
                .contains("subqueries would evaluate more than"),
            "unexpected error: {err}"
        );
    }
    assert!(started.elapsed() < Duration::from_secs(1));
}
