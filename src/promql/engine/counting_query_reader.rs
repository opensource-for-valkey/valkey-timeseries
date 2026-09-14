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
//! that implements one trait method in terms of another (as the default
//! [`QueryReader::query_grid`] calls the reader's own `query_range`) does not
//! inflate the counts, which is exactly what the plan's assertions need.

use crate::promql::EvalLabels;
use crate::promql::engine::QueryReader;
use crate::promql::engine::label_profile::LabelProfile;
use crate::promql::engine::query_reader::{
    AggregationOutcome, AggregationRequest, GridOutcome, GridRequest,
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
    pub query_grid: usize,
    pub label_profile: usize,
}

impl ReaderCallCounts {
    /// Total calls across every method.
    pub fn total(&self) -> usize {
        self.query
            + self.query_range
            + self.query_aggregation
            + self.query_grid
            + self.label_profile
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
    query_grid_calls: AtomicUsize,
    label_profile_calls: AtomicUsize,
}

impl CountingQueryReader {
    pub fn new(inner: Arc<dyn QueryReader>) -> Self {
        Self {
            inner,
            query_calls: AtomicUsize::new(0),
            query_range_calls: AtomicUsize::new(0),
            query_aggregation_calls: AtomicUsize::new(0),
            query_grid_calls: AtomicUsize::new(0),
            label_profile_calls: AtomicUsize::new(0),
        }
    }

    /// The calls observed since construction or the last [`Self::reset`].
    pub fn counts(&self) -> ReaderCallCounts {
        ReaderCallCounts {
            query: self.query_calls.load(Ordering::Relaxed),
            query_range: self.query_range_calls.load(Ordering::Relaxed),
            query_aggregation: self.query_aggregation_calls.load(Ordering::Relaxed),
            query_grid: self.query_grid_calls.load(Ordering::Relaxed),
            label_profile: self.label_profile_calls.load(Ordering::Relaxed),
        }
    }

    /// Zero all counters, e.g. between queries sharing one wrapper.
    pub fn reset(&self) {
        self.query_calls.store(0, Ordering::Relaxed);
        self.query_range_calls.store(0, Ordering::Relaxed);
        self.query_aggregation_calls.store(0, Ordering::Relaxed);
        self.query_grid_calls.store(0, Ordering::Relaxed);
        self.label_profile_calls.store(0, Ordering::Relaxed);
    }
}

impl QueryReader for CountingQueryReader {
    fn query(
        &self,
        selector: &VectorSelector,
        timestamp: i64,
        options: QueryOptions,
    ) -> PromqlResult<Vec<InstantSample<EvalLabels>>> {
        self.query_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.query(selector, timestamp, options)
    }

    fn query_range(
        &self,
        selector: &VectorSelector,
        start_ms: i64,
        end_ms: i64,
        options: QueryOptions,
    ) -> PromqlResult<Vec<RangeSample<EvalLabels>>> {
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

    fn query_grid(
        &self,
        selector: &VectorSelector,
        request: &GridRequest,
        options: QueryOptions,
    ) -> PromqlResult<GridOutcome> {
        self.query_grid_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.query_grid(selector, request, options)
    }

    fn label_profile(
        &self,
        selector: &VectorSelector,
        options: QueryOptions,
    ) -> PromqlResult<Option<LabelProfile>> {
        self.label_profile_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.label_profile(selector, options)
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

    /// A range query with an explicit sample budget, returning the result
    /// rather than expecting success.
    fn try_range(
        reader: Arc<dyn QueryReader>,
        query: &str,
        max_samples: usize,
    ) -> Result<(), crate::promql::QueryError> {
        let expr = promql_parser::parser::parse(query).expect("valid test query");
        let stmt = EvalStmt {
            expr,
            start: ms(RANGE_START_MS),
            end: ms(RANGE_END_MS),
            interval: STEP,
            lookback_delta: options().lookback_delta,
        };
        let options = QueryOptions {
            max_samples,
            ..options()
        };
        evaluate_range(reader, stmt, options).map(|_| ())
    }

    /// The samples one selector's preload loads for the range window: the
    /// window plus the 5m lookback, at 10s cadence, over three series.
    const SAMPLES_PER_SELECTOR: usize =
        3 * ((RANGE_END_MS - (RANGE_START_MS - 300_000)) / INTERVAL_MS) as usize;

    #[test]
    fn sample_budget_is_query_wide() {
        let (_, reader) = build_reader();
        // One selector fits; two do not: the budget sums every read the query makes,
        // so `a + b` is refused where `a` alone was accepted.
        let one = SAMPLES_PER_SELECTOR + 10;
        try_range(reader.clone(), "a", one).expect("a single selector fits the budget");
        let err = try_range(reader.clone(), "a + b", one).expect_err("two selectors exceed it");
        assert!(
            err.to_string().contains("too many samples"),
            "budget error should name the cause: {err}"
        );
        assert!(
            err.to_string().contains("ts-promql-max-samples-per-query"),
            "budget error should name the parameter: {err}"
        );
        try_range(reader.clone(), "a + b", 2 * one).expect("doubling the budget admits both");
        try_range(reader, "a + b", 0).expect("0 is unlimited");
    }

    #[test]
    fn sample_budget_covers_a_subquerys_reads() {
        let (_, reader) = build_reader();
        // The subquery's union grid is preloaded by a sub-evaluator that shares the
        // outer query's budget, so its read counts against the same total.
        let err = try_range(reader.clone(), "max_over_time(a[2m:1m])", 10)
            .expect_err("the subquery's preload is charged to the query");
        assert!(err.to_string().contains("too many samples"), "{err}");
        try_range(reader, "max_over_time(a[2m:1m])", 0).expect("unlimited");
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
    fn range_selector_is_preloaded_with_one_grid_request() {
        let (counting, reader) = build_reader();
        // preload_for_range asks the source for the stepped selection over
        // the whole grid, once — never the raw span, never one read per step.
        run_range(reader, "a");
        assert_eq!(
            counting.counts(),
            ReaderCallCounts {
                query_grid: 1,
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
        // at every step — and one label profile, asked before planning,
        // serves both operands of the binary operation.
        run_range(reader, "a + a");
        assert_eq!(
            counting.counts(),
            ReaderCallCounts {
                query_grid: 1,
                label_profile: 1,
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
                query_grid: 2,    // one grid request per distinct selector
                label_profile: 2, // and one profile each, before planning
                ..Default::default()
            }
        );
    }

    #[test]
    fn range_without_a_narrowable_operation_asks_for_no_profile() {
        let (counting, reader) = build_reader();
        // `or` keeps both sides and a lone selector has no operation:
        // nothing for the derived push-down to do, so the index is not
        // consulted.
        run_range(reader.clone(), "a or b");
        run_range(reader, "sum(a)");
        assert_eq!(counting.counts().label_profile, 0);
    }

    #[test]
    fn range_label_less_aggregations_are_still_profiled_for_the_short_circuit() {
        let (counting, reader) = build_reader();
        // `sum(a)` offers no labels, but an empty `a` would empty the
        // quotient, so both selectors are profiled.
        run_range(reader, "sum(a) / sum(b)");
        assert_eq!(counting.counts().label_profile, 2);
    }

    #[test]
    fn range_empty_operand_short_circuits_the_other_read() {
        let (counting, reader) = build_reader();
        // `a{l="nope"}` matches nothing, so `b` is never read: the one grid
        // request is the empty selector's, shared by both sides.
        run_range(reader, r#"a{l="nope"} - b"#);
        assert_eq!(
            counting.counts(),
            ReaderCallCounts {
                query_grid: 1,
                label_profile: 2,
                ..Default::default()
            }
        );
    }

    #[test]
    fn range_profiles_are_skipped_when_the_pushdown_is_off() {
        let (counting, reader) = build_reader();
        let opts = QueryOptions {
            derived_filter_pushdown: false,
            ..options()
        };
        evaluate_range(
            reader,
            EvalStmt {
                expr: promql_parser::parser::parse("a - b").unwrap(),
                start: ms(RANGE_START_MS),
                end: ms(RANGE_END_MS),
                interval: STEP,
                lookback_delta: opts.lookback_delta,
            },
            opts,
        )
        .unwrap();
        assert_eq!(
            counting.counts(),
            ReaderCallCounts {
                query_grid: 2,
                ..Default::default()
            }
        );
    }

    #[test]
    fn instant_join_derives_its_filters_at_evaluation_not_from_the_index() {
        let (counting, reader) = build_reader();
        run_instant(reader, "a - b", RANGE_END_MS);
        assert_eq!(counting.counts().label_profile, 0);
    }

    #[test]
    fn range_grouped_aggregation_over_selector_is_one_fused_grid_request() {
        let (counting, reader) = build_reader();
        // The aggregation sits directly over a bare selector, so the whole
        // thing is one fused grid request — groups × steps come back — and the
        // selector is not preloaded separately on top of it.
        run_range(reader, "sum by (l) (a)");
        assert_eq!(
            counting.counts(),
            ReaderCallCounts {
                query_grid: 1,
                ..Default::default()
            }
        );
    }

    #[test]
    fn range_selector_under_and_beside_an_aggregation_shares_a_raw_answer() {
        let (counting, reader) = build_reader();
        // The in-memory source answers the fused `avg(a)` request raw, which
        // hands over `a`'s whole span: that becomes the selector's stepped
        // preload, and the bare `a` on the right reads from it rather than
        // issuing a second request. (A source that folds the fused request
        // itself makes this two requests — pinned in the evaluator tests.)
        run_range(reader, "avg(a) / a");
        assert_eq!(
            counting.counts(),
            ReaderCallCounts {
                query_grid: 1,
                label_profile: 1, // one selector, one profile (short-circuit candidate)
                ..Default::default()
            }
        );
    }

    #[test]
    fn range_selecting_aggregation_over_selector_is_one_fused_grid_request() {
        let (counting, reader) = build_reader();
        // topk fuses like a reduction: one grid request carries the selector
        // and the operator, and the source's per-step picks come back.
        run_range(reader, "topk(2, a)");
        assert_eq!(
            counting.counts(),
            ReaderCallCounts {
                query_grid: 1,
                ..Default::default()
            }
        );
    }

    #[test]
    fn range_pushable_rollup_is_one_grid_request() {
        let (counting, reader) = build_reader();
        // rate ∈ RollupKind: preload_rollups answers the whole step grid with
        // one query_grid; the matrix selector is never fetched raw.
        run_range(reader, "rate(a[1m])");
        assert_eq!(
            counting.counts(),
            ReaderCallCounts {
                query_grid: 1,
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
                query_grid: 1,
                ..Default::default()
            }
        );
    }

    #[test]
    fn range_mixed_pushable_and_non_pushable_rollups() {
        let (counting, reader) = build_reader();
        // rate's grid comes from one query_grid; predict_linear's raw span
        // from one query_range. Neither call touches the reader per step.
        run_range(reader, "rate(a[1m]) + predict_linear(a[1m], 60)");
        assert_eq!(
            counting.counts(),
            ReaderCallCounts {
                query_range: 1,
                query_grid: 1,
                label_profile: 1,
                ..Default::default()
            }
        );
    }

    /// A reader that leaves the push-down methods at their defaults: an
    /// aggregation is `Unsupported`, and a grid request is answered raw from
    /// the reader's own `query_range`.
    struct NoPushdownReader {
        inner: Arc<dyn QueryReader>,
    }

    impl QueryReader for NoPushdownReader {
        fn query(
            &self,
            selector: &promql_parser::parser::VectorSelector,
            timestamp: i64,
            options: QueryOptions,
        ) -> crate::promql::PromqlResult<Vec<crate::promql::model::InstantSample<EvalLabels>>>
        {
            self.inner.query(selector, timestamp, options)
        }

        fn query_range(
            &self,
            selector: &promql_parser::parser::VectorSelector,
            start_ms: i64,
            end_ms: i64,
            options: QueryOptions,
        ) -> crate::promql::PromqlResult<Vec<crate::promql::model::RangeSample<EvalLabels>>>
        {
            self.inner.query_range(selector, start_ms, end_ms, options)
        }
        // query_aggregation / query_grid: trait defaults.
    }

    #[test]
    fn range_grid_without_pushdown_support_is_one_span_read() {
        // A reader with no grid evaluation of its own still answers the grid
        // request: the default reads the span once through `query_range` and
        // the evaluator runs the per-series stage. The counter sits *inside*
        // that reader, so what it sees is the one span read the default makes
        // — never one window per step, and no separate matrix preload.
        for query in ["rate(a[1m])", "a", "sum by (l) (a)"] {
            let (counting, inner) = build_reader();
            let reader: Arc<dyn QueryReader> = Arc::new(NoPushdownReader { inner });
            run_range(reader, query);
            assert_eq!(
                counting.counts(),
                ReaderCallCounts {
                    query_range: 1,
                    ..Default::default()
                },
                "{query}"
            );
        }
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
        ) -> crate::promql::PromqlResult<Vec<crate::promql::model::InstantSample<EvalLabels>>>
        {
            self.inner.query(selector, timestamp, options)
        }

        fn query_range(
            &self,
            selector: &promql_parser::parser::VectorSelector,
            start_ms: i64,
            end_ms: i64,
            options: QueryOptions,
        ) -> crate::promql::PromqlResult<Vec<crate::promql::model::RangeSample<EvalLabels>>>
        {
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
        // inner steps. Before Phase 2 each one issued its own query_grid for
        // the inner rate() (plan finding 1.2); subquery-scoped preloading now
        // covers the whole grid in one request. The outer max_over_time takes a
        // subquery argument, so it is never pushed down itself.
        run_instant(reader, "max_over_time(rate(a[1m])[4m:1m])", 3_600_000);
        assert_eq!(
            counting.counts(),
            ReaderCallCounts {
                query_grid: 1,
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
        // makes it one stepped grid request per deduplicated selector, and the
        // steps read from those.
        run_instant(reader, "max_over_time((a + b)[4m:1m])", 3_600_000);
        assert_eq!(
            counting.counts(),
            ReaderCallCounts {
                query_grid: 2,
                ..Default::default()
            }
        );
    }

    #[test]
    fn instant_nested_subquery_preloads_each_inner_grid() {
        let (counting, reader) = build_reader();
        // Nested subqueries nest sub-evaluators. The outer [4m:2m] grid has 2
        // aligned steps and nothing of its own to preload (its inner expression
        // is a subquery, which the selector collectors stop at). Preparing that
        // grid also prepares the inner [2m:1m] subquery once, for the union of
        // both outer steps' windows, so `a` is fetched once — where each outer
        // step used to fetch its own window (2), and before Phase 2 there were
        // 2 × 2 live `query` calls.
        run_instant(
            reader,
            "max_over_time(max_over_time((a)[2m:1m])[4m:2m])",
            3_600_000,
        );
        assert_eq!(
            counting.counts(),
            ReaderCallCounts {
                query_grid: 1,
                ..Default::default()
            }
        );
    }

    #[test]
    fn range_subquery_over_expr_is_one_fetch_for_all_outer_steps() {
        let (counting, reader) = build_reader();
        // Each outer step's subquery covers a different window, but every
        // window is a run of the same 1m lattice, so the outer preload prepares
        // the subquery once for their union and the steps read from that: one
        // fetch, where it used to be one per outer step (`RANGE_STEPS`) and,
        // before Phase 2, the outer_steps × inner_steps product that plan
        // finding 1.2 describes (5 × 4 = 20).
        run_range(reader, "max_over_time((a)[4m:1m])");
        assert_eq!(
            counting.counts(),
            ReaderCallCounts {
                query_grid: 1,
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
                query_grid: 1,
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
