//! A counting decorator over [`QueryReader`], for asserting how many reader
//! calls a query evaluation performs.
//!
//! Phase 0 of `docs/plans/promql-execution-optimization-plan.md`: the plan's
//! Tier 1 findings are all forms of reader-call amplification (per-step window
//! re-fetches, per-inner-step subquery requests), so its later phases are
//! verified by *counting reader calls*, not by timing. This wrapper is that
//! instrument: it delegates every `QueryReader` method to an inner reader and
//! counts invocations per method.
//!
//! Note the counters see only calls the *evaluator* issues. An inner reader
//! that implements one trait method in terms of another (as
//! [`super::memory_series_querier::MemorySeriesQuerier::query_rollup`] calls
//! its own `query_range`) does not inflate the counts, which is exactly what
//! the plan's assertions need.

use crate::promql::engine::QueryReader;
use crate::promql::engine::query_reader::{
    AggregationOutcome, AggregationRequest, RollupOutcome, RollupRequest,
};
use crate::promql::model::{InstantSample, RangeSample};
use crate::promql::{PromqlResult, QueryOptions};
use promql_parser::parser::VectorSelector;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A snapshot of how many times each [`QueryReader`] method was called.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReaderCallCounts {
    pub query: usize,
    pub query_range: usize,
    pub query_aggregation: usize,
    pub query_rollup: usize,
}

impl ReaderCallCounts {
    /// Total calls across every method.
    pub fn total(&self) -> usize {
        self.query + self.query_range + self.query_aggregation + self.query_rollup
    }
}

/// Counts calls to each [`QueryReader`] method while delegating to `inner`.
///
/// Wrap a reader, hand the wrapper (as `Arc<dyn QueryReader>`) to
/// `evaluate_instant` / `evaluate_range`, then read [`Self::counts`].
pub struct CountingQueryReader {
    inner: Arc<dyn QueryReader>,
    query_calls: AtomicUsize,
    query_range_calls: AtomicUsize,
    query_aggregation_calls: AtomicUsize,
    query_rollup_calls: AtomicUsize,
}

impl CountingQueryReader {
    pub fn new(inner: Arc<dyn QueryReader>) -> Self {
        Self {
            inner,
            query_calls: AtomicUsize::new(0),
            query_range_calls: AtomicUsize::new(0),
            query_aggregation_calls: AtomicUsize::new(0),
            query_rollup_calls: AtomicUsize::new(0),
        }
    }

    /// The calls observed since construction or the last [`Self::reset`].
    pub fn counts(&self) -> ReaderCallCounts {
        ReaderCallCounts {
            query: self.query_calls.load(Ordering::Relaxed),
            query_range: self.query_range_calls.load(Ordering::Relaxed),
            query_aggregation: self.query_aggregation_calls.load(Ordering::Relaxed),
            query_rollup: self.query_rollup_calls.load(Ordering::Relaxed),
        }
    }

    /// Zero all counters, e.g. between queries sharing one wrapper.
    pub fn reset(&self) {
        self.query_calls.store(0, Ordering::Relaxed);
        self.query_range_calls.store(0, Ordering::Relaxed);
        self.query_aggregation_calls.store(0, Ordering::Relaxed);
        self.query_rollup_calls.store(0, Ordering::Relaxed);
    }
}

impl QueryReader for CountingQueryReader {
    fn query(
        &self,
        selector: &VectorSelector,
        timestamp: i64,
        options: QueryOptions,
    ) -> PromqlResult<Vec<InstantSample>> {
        self.query_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.query(selector, timestamp, options)
    }

    fn query_range(
        &self,
        selector: &VectorSelector,
        start_ms: i64,
        end_ms: i64,
        options: QueryOptions,
    ) -> PromqlResult<Vec<RangeSample>> {
        self.query_range_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.query_range(selector, start_ms, end_ms, options)
    }

    fn query_aggregation(
        &self,
        selector: &VectorSelector,
        timestamp: i64,
        aggregation: &AggregationRequest,
        options: QueryOptions,
    ) -> PromqlResult<AggregationOutcome> {
        self.query_aggregation_calls.fetch_add(1, Ordering::Relaxed);
        self.inner
            .query_aggregation(selector, timestamp, aggregation, options)
    }

    fn query_rollup(
        &self,
        selector: &VectorSelector,
        rollup: &RollupRequest,
        options: QueryOptions,
    ) -> PromqlResult<RollupOutcome> {
        self.query_rollup_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.query_rollup(selector, rollup, options)
    }
}

/// Pin the reader-call counts of the query shapes named in
/// `docs/plans/promql-execution-optimization-plan.md`.
///
/// These tests assert *current* behavior, including the request amplification
/// the plan's Tier 1 targets. When a later phase lands, the affected
/// assertions are expected to change — each is annotated with the phase that
/// will change it, so a failure here is a prompt to update the pinned count
/// deliberately, never silently.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Sample;
    use crate::labels::Labels;
    use crate::promql::engine::memory_series_querier::MemorySeriesQuerier;
    use crate::promql::engine::{evaluate_instant, evaluate_range};
    use promql_parser::parser::EvalStmt;
    use std::sync::Barrier;
    use std::thread;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    const INTERVAL_MS: i64 = 10_000;
    const STEP: Duration = Duration::from_secs(60);

    /// Range-query window: [3_000_000 ms, 3_240_000 ms] at 60s step = 5 steps.
    const RANGE_START_MS: i64 = 3_000_000;
    const RANGE_END_MS: i64 = 3_240_000;
    const RANGE_STEPS: usize = 5;

    fn ms(ts: i64) -> SystemTime {
        UNIX_EPOCH + Duration::from_millis(ts as u64)
    }

    /// Three series each for metrics `a` and `b`, sampled every 10s over
    /// [0, 4_000_000 ms] — comfortably covering every test window plus the
    /// default 5m lookback.
    fn build_data() -> Arc<dyn QueryReader> {
        let querier = MemorySeriesQuerier::new();
        for metric in ["a", "b"] {
            for l in 0..3 {
                let labels = Labels::from_pairs(&[("__name__", metric), ("l", &l.to_string())]);
                for point in 0..=400 {
                    let ts = point * INTERVAL_MS;
                    querier.add_sample(&labels, Sample::new(ts, point as f64));
                }
            }
        }
        Arc::new(querier)
    }

    fn build_reader() -> (Arc<CountingQueryReader>, Arc<dyn QueryReader>) {
        let counting = Arc::new(CountingQueryReader::new(build_data()));
        let reader: Arc<dyn QueryReader> = counting.clone();
        (counting, reader)
    }

    fn options() -> QueryOptions {
        QueryOptions {
            timeout: None,
            deadline: None,
            ..QueryOptions::default()
        }
    }

    fn run_range(reader: Arc<dyn QueryReader>, query: &str) {
        let expr = promql_parser::parser::parse(query).expect("valid test query");
        let stmt = EvalStmt {
            expr,
            start: ms(RANGE_START_MS),
            end: ms(RANGE_END_MS),
            interval: STEP,
            lookback_delta: options().lookback_delta,
        };
        evaluate_range(reader, stmt, options()).expect("range query should evaluate");
    }

    fn run_instant(reader: Arc<dyn QueryReader>, query: &str, at_ms: i64) {
        let expr = promql_parser::parser::parse(query).expect("valid test query");
        let stmt = EvalStmt {
            expr,
            start: ms(at_ms),
            end: ms(at_ms),
            interval: Duration::ZERO,
            lookback_delta: options().lookback_delta,
        };
        evaluate_instant(reader, stmt, ms(at_ms), options())
            .expect("instant query should evaluate");
    }

    #[test]
    fn range_selector_is_preloaded_with_one_fetch() {
        let (counting, reader) = build_reader();
        run_range(reader, "a");
        assert_eq!(
            counting.counts(),
            ReaderCallCounts {
                query_range: 1, // preload_for_range fetches the whole span once
                ..Default::default()
            }
        );
    }

    #[test]
    fn range_unbounded_end_is_rejected_before_any_fetch() {
        // `END +` resolves to i64::MAX, making the step grid (end - start) / step
        // astronomically large. With a finite points limit the query must be rejected up
        // front — before the preload phase sizes a per-step buffer from that width and
        // before the reader is even queried. A zero fetch count proves the guard runs ahead
        // of the allocation, which is the whole point of wiring it in here.
        let (counting, reader) = build_reader();
        let opts = QueryOptions {
            timeout: None,
            deadline: None,
            max_points_per_series: Some(100_000),
            ..QueryOptions::default()
        };
        let stmt = EvalStmt {
            expr: promql_parser::parser::parse("a").expect("valid test query"),
            start: ms(RANGE_START_MS),
            end: ms(i64::MAX),
            interval: STEP,
            lookback_delta: opts.lookback_delta,
        };

        let err = evaluate_range(reader, stmt, opts)
            .expect_err("an unbounded-end range query must be rejected, not evaluated");
        assert!(
            err.to_string().contains("too many points"),
            "unexpected error: {err}"
        );
        assert_eq!(
            counting.counts(),
            ReaderCallCounts::default(),
            "the reader must not be queried once the point ceiling rejects the window"
        );
    }

    #[test]
    fn concurrent_oversized_ranges_are_rejected_before_any_fetch() {
        // Each caller gets an independent range guard. Starting several at once
        // must not allow any one of them to begin preloading an unbounded grid.
        let (counting, reader) = build_reader();
        let start = Arc::new(Barrier::new(4));
        let mut callers = Vec::new();

        for _ in 0..4 {
            let reader = reader.clone();
            let start = start.clone();
            callers.push(thread::spawn(move || {
                let opts = QueryOptions {
                    timeout: None,
                    deadline: None,
                    max_points_per_series: Some(100_000),
                    ..QueryOptions::default()
                };
                let stmt = EvalStmt {
                    expr: promql_parser::parser::parse("a").expect("valid test query"),
                    start: ms(RANGE_START_MS),
                    end: ms(i64::MAX),
                    interval: STEP,
                    lookback_delta: opts.lookback_delta,
                };
                start.wait();
                evaluate_range(reader, stmt, opts)
                    .expect_err("the point guard must reject each oversized range")
            }));
        }

        for caller in callers {
            assert!(
                caller
                    .join()
                    .unwrap()
                    .to_string()
                    .contains("too many points")
            );
        }
        assert_eq!(
            counting.counts(),
            ReaderCallCounts::default(),
            "no concurrent caller may reach a preload fetch"
        );
    }

    #[test]
    fn range_duplicate_selectors_are_deduplicated() {
        let (counting, reader) = build_reader();
        // Both operands share one PreloadKey, so one fetch serves both sides
        // at every step.
        run_range(reader, "a + a");
        assert_eq!(
            counting.counts(),
            ReaderCallCounts {
                query_range: 1,
                ..Default::default()
            }
        );
    }

    #[test]
    fn range_join_fetches_once_per_selector() {
        let (counting, reader) = build_reader();
        run_range(reader, "a - b");
        assert_eq!(
            counting.counts(),
            ReaderCallCounts {
                query_range: 2, // one preload per distinct selector
                ..Default::default()
            }
        );
    }

    #[test]
    fn range_grouped_aggregation_over_preloaded_selector_stays_local() {
        let (counting, reader) = build_reader();
        // The selector is preloaded, so per-step grouping is pure CPU
        // (plan finding 2.1) — no aggregation push-down requests.
        run_range(reader, "sum by (l) (a)");
        assert_eq!(
            counting.counts(),
            ReaderCallCounts {
                query_range: 1,
                ..Default::default()
            }
        );
    }

    #[test]
    fn range_pushable_rollup_is_one_grid_request() {
        let (counting, reader) = build_reader();
        // rate ∈ RollupKind: preload_rollups answers the whole step grid with
        // one query_rollup; the matrix selector is never fetched raw.
        run_range(reader, "rate(a[1m])");
        assert_eq!(
            counting.counts(),
            ReaderCallCounts {
                query_rollup: 1,
                ..Default::default()
            }
        );
    }

    #[test]
    fn range_non_pushable_rollup_is_preloaded_with_one_fetch() {
        let (counting, reader) = build_reader();
        // predict_linear ∉ RollupKind, so no rollup grid covers it — but
        // Phase 1's matrix preload fetches its whole span once and the step
        // loop slices windows locally. (Before Phase 1 this pinned
        // query_range == RANGE_STEPS, one window fetch per step.)
        run_range(reader, "predict_linear(a[1m], 60)");
        assert_eq!(
            counting.counts(),
            ReaderCallCounts {
                query_range: 1,
                ..Default::default()
            }
        );
    }

    #[test]
    fn range_fused_rollup_covers_its_matrix() {
        let (counting, reader) = build_reader();
        // The fused grid answers sum(rate(...)) wholesale, so the matrix
        // preload must not also fetch the raw span for the covered call.
        run_range(reader, "sum(rate(a[1m]))");
        assert_eq!(
            counting.counts(),
            ReaderCallCounts {
                query_rollup: 1,
                ..Default::default()
            }
        );
    }

    #[test]
    fn range_mixed_pushable_and_non_pushable_rollups() {
        let (counting, reader) = build_reader();
        // rate's grid comes from one query_rollup; predict_linear's raw span
        // from one query_range. Neither call touches the reader per step.
        run_range(reader, "rate(a[1m]) + predict_linear(a[1m], 60)");
        assert_eq!(
            counting.counts(),
            ReaderCallCounts {
                query_range: 1,
                query_rollup: 1,
                ..Default::default()
            }
        );
    }

    /// A reader that leaves the push-down methods at their `Unsupported`
    /// defaults — the production single-node shape when
    /// `ts-fanout-rollup-pushdown` is off (the default config).
    struct NoPushdownReader {
        inner: Arc<dyn QueryReader>,
    }

    impl QueryReader for NoPushdownReader {
        fn query(
            &self,
            selector: &promql_parser::parser::VectorSelector,
            timestamp: i64,
            options: QueryOptions,
        ) -> crate::promql::PromqlResult<Vec<crate::promql::model::InstantSample>> {
            self.inner.query(selector, timestamp, options)
        }

        fn query_range(
            &self,
            selector: &promql_parser::parser::VectorSelector,
            start_ms: i64,
            end_ms: i64,
            options: QueryOptions,
        ) -> crate::promql::PromqlResult<Vec<crate::promql::model::RangeSample>> {
            self.inner.query_range(selector, start_ms, end_ms, options)
        }
        // query_aggregation / query_rollup: trait defaults → Unsupported.
    }

    #[test]
    fn range_rollup_without_pushdown_support_uses_matrix_preload() {
        // The default-config cliff of plan finding 1.1: rollup push-down
        // answers Unsupported, so no rollup grid exists — the matrix preload
        // must cover the call with one raw-span fetch instead of the step
        // loop fetching one window per step (which is what this pinned
        // before Phase 1).
        let counting = Arc::new(CountingQueryReader::new(Arc::new(NoPushdownReader {
            inner: build_data(),
        })));
        let reader: Arc<dyn QueryReader> = counting.clone();
        run_range(reader, "rate(a[1m])");
        assert_eq!(
            counting.counts(),
            ReaderCallCounts {
                query_rollup: 1, // the attempt that answered Unsupported
                query_range: 1,  // the matrix span fetch that covers the grid
                ..Default::default()
            }
        );
    }

    /// A reader whose `query_range` rejects spans wider than `max_span_ms`,
    /// imitating a `max_points_per_series`-style limit that a whole-span
    /// preload exceeds but every per-step window respects.
    struct SpanLimitedReader {
        inner: Arc<dyn QueryReader>,
        max_span_ms: i64,
    }

    impl QueryReader for SpanLimitedReader {
        fn query(
            &self,
            selector: &promql_parser::parser::VectorSelector,
            timestamp: i64,
            options: QueryOptions,
        ) -> crate::promql::PromqlResult<Vec<crate::promql::model::InstantSample>> {
            self.inner.query(selector, timestamp, options)
        }

        fn query_range(
            &self,
            selector: &promql_parser::parser::VectorSelector,
            start_ms: i64,
            end_ms: i64,
            options: QueryOptions,
        ) -> crate::promql::PromqlResult<Vec<crate::promql::model::RangeSample>> {
            if end_ms - start_ms > self.max_span_ms {
                return Err(crate::promql::QueryError::Execution(
                    "span exceeds test limit".to_string(),
                ));
            }
            self.inner.query_range(selector, start_ms, end_ms, options)
        }
    }

    #[test]
    fn range_matrix_preload_over_limit_falls_back_to_per_step() {
        let inner = build_data();
        // Window is 60s but the whole 5-step span is ~300s: the preload
        // attempt fails the span limit and must fall back to the per-step
        // path — the query still succeeds, at one failed span attempt plus
        // one window fetch per step (§4 of the plan: a query that succeeds
        // per-step keeps succeeding).
        let limited = Arc::new(CountingQueryReader::new(Arc::new(SpanLimitedReader {
            inner,
            max_span_ms: 120_000,
        })));
        let reader: Arc<dyn QueryReader> = limited.clone();
        run_range(reader, "predict_linear(a[1m], 60)");
        assert_eq!(
            limited.counts(),
            ReaderCallCounts {
                query_range: 1 + RANGE_STEPS,
                ..Default::default()
            }
        );
    }

    #[test]
    fn instant_subquery_over_expr_is_one_grid_request() {
        let (counting, reader) = build_reader();
        // The subquery grid for [4m:1m] ending at t=3_600_000 has 4 aligned
        // inner steps. Before Phase 2 each one issued its own query_rollup for
        // the inner rate() (plan finding 1.2); subquery-scoped preloading now
        // covers the whole grid in one request. The outer max_over_time takes a
        // subquery argument, so it is never pushed down itself.
        run_instant(reader, "max_over_time(rate(a[1m])[4m:1m])", 3_600_000);
        assert_eq!(
            counting.counts(),
            ReaderCallCounts {
                query_rollup: 1,
                ..Default::default()
            }
        );
    }

    #[test]
    fn instant_subquery_over_expr_preloads_each_selector_once() {
        let (counting, reader) = build_reader();
        // The inner expression is not a bare selector, so it takes the general
        // per-step path. The subquery grid for [4m:1m] ending at t=3_600_000
        // has 4 aligned steps, which before Phase 2 meant 4 × 2 live `query`
        // calls — one per selector per inner step. Subquery-scoped preloading
        // makes it one span fetch per deduplicated selector, and the steps read
        // from those.
        run_instant(reader, "max_over_time((a + b)[4m:1m])", 3_600_000);
        assert_eq!(
            counting.counts(),
            ReaderCallCounts {
                query_range: 2,
                ..Default::default()
            }
        );
    }

    #[test]
    fn instant_nested_subquery_preloads_each_inner_grid() {
        let (counting, reader) = build_reader();
        // Nested subqueries nest sub-evaluators. The outer [4m:2m] grid has 2
        // aligned steps and nothing of its own to preload (its inner expression
        // is a subquery, which both collectors stop at). Each of those 2 steps
        // evaluates the inner [2m:1m] subquery, whose own sub-evaluator
        // preloads `a` once for its 2-step grid — so 2 fetches total, where
        // before Phase 2 there were 2 × 2 live `query` calls.
        run_instant(
            reader,
            "max_over_time(max_over_time((a)[2m:1m])[4m:2m])",
            3_600_000,
        );
        assert_eq!(
            counting.counts(),
            ReaderCallCounts {
                query_range: 2,
                ..Default::default()
            }
        );
    }

    #[test]
    fn range_subquery_over_expr_is_one_fetch_per_outer_step() {
        let (counting, reader) = build_reader();
        // The outer range query cannot preload across subquery boundaries — each
        // outer step's subquery covers a different window — so the request count
        // is one per outer step rather than the outer_steps × inner_steps
        // product that plan finding 1.2 describes (5 × 4 = 20 before Phase 2).
        run_range(reader, "max_over_time((a)[4m:1m])");
        assert_eq!(
            counting.counts(),
            ReaderCallCounts {
                query_range: RANGE_STEPS,
                ..Default::default()
            }
        );
    }

    #[test]
    fn instant_aggregation_is_one_pushdown_request() {
        let (counting, reader) = build_reader();
        run_instant(reader, "sum(a)", 3_600_000);
        assert_eq!(
            counting.counts(),
            ReaderCallCounts {
                query_aggregation: 1,
                ..Default::default()
            }
        );
    }

    #[test]
    fn instant_rollup_is_one_pushdown_request() {
        let (counting, reader) = build_reader();
        run_instant(reader, "rate(a[1m])", 3_600_000);
        assert_eq!(
            counting.counts(),
            ReaderCallCounts {
                query_rollup: 1,
                ..Default::default()
            }
        );
    }

    #[test]
    fn reset_zeroes_counters() {
        let (counting, reader) = build_reader();
        run_instant(reader, "a", 3_600_000);
        assert_eq!(counting.counts().total(), 1);
        counting.reset();
        assert_eq!(counting.counts(), ReaderCallCounts::default());
    }
}
