use super::aggregations::{AggregationKind, PushdownStrategy, apply_aggregation, eval_aggregation};
use crate::common::Timestamp;
use crate::common::threads::join;
use crate::common::threads::{IntoParRayon, ParCollectionRayon};
use crate::common::time::{current_time_millis, system_time_to_millis};
use crate::promql::binops::{
    can_push_down_common_filters, ensure_unique_labelsets, eval_binary_expr, push_down_filters,
};
use crate::promql::engine::query_reader::{
    AggregationOutcome, AggregationParam, AggregationRequest, GridAggregation, GridOutcome,
    GridRequest, GridRollup,
};
use crate::promql::engine::sample_budget::SampleBudget;
use crate::promql::engine::{QueryOptions, QueryReader};
use crate::promql::exec::pipeline::{
    QueryPlan, compute_subquery_alignment, execute_selector_pipeline, for_each_step_sample,
};
use crate::promql::exec::planner::{PlannedQuery, PreloadGrid};
use crate::promql::exec::preloader::Preloader;
use crate::promql::exec::types::{
    EvalLabels, GridPreloadMap, MatrixPreloadMap, PreloadedGridData, PreloadedGridSeries,
    PreloadedMatrixData, PreloadedMatrixSeries, SampleWindow, StepGrid, StepGridBuilder,
    SubquerySeriesMap,
};
use crate::promql::exec::utils::{
    RollupCandidate, calls_function, collect_rollup_candidates,
    collect_stepped_aggregation_candidates, collect_subqueries, collect_vector_selectors,
    merge_step_into_subquery_map, strip_parens,
};
use crate::promql::functions::RollupKind;
use crate::promql::functions::{
    FunctionCallContext, PromQLArg, PromQLFunction, resolve_function, window_range,
};
use crate::promql::hashers::{AggregationKey, GridPreloadKey, MatrixPreloadKey, PreloadKey};
use crate::promql::model::EvalContext;
use crate::promql::model::RangeSample;
use crate::promql::time::{apply_time_modifiers_ms, selector_bounds, step_times};
use crate::promql::types::{PreloadedInstantData, PreloadedInstantSeries};
use crate::promql::{
    EvalResult, EvalSample, EvalSamples, EvaluationError, ExprResult, InstantSample, PreloadMap,
    QueryError,
};
use ahash::AHashSet;
use orx_parallel::ParIter;
use orx_parallel::ParIterResult;
use promql_parser::parser::token::T_LAND;
use promql_parser::parser::value::ValueType;
use promql_parser::parser::{
    AggregateExpr, BinaryExpr, Call, EvalStmt, Expr, MatrixSelector, SubqueryExpr, UnaryExpr,
    VectorSelector,
};
use std::sync::{Arc, RwLock};
use std::time::Duration;

/// How many preload requests may be in flight at once.
///
/// Each preload request is a blocking cluster fanout (or a whole-span read on
/// a single node), so unbounded parallelism would multiply concurrent fanouts
/// and peak memory. Mirrors the batch size of the selector executor.
const MAX_CONCURRENT_PRELOAD_REQUESTS: usize = 4;

/// Immutable preload data consumed by expression evaluation.
///
/// A [`crate::promql::exec::preloader::Preloader`] constructs this before the
/// range step loop. Evaluation only reads the three maps, keeping source I/O
/// and planning out of expression semantics.
///
/// The maps are behind `Arc` so that one prepared grid can back many
/// evaluators at once: every outer step of a range query evaluates a subquery
/// on an evaluator that shares the subquery's union preload (see
/// [`Evaluator::with_shared`]). Nothing writes to them after preparation.
#[derive(Default)]
pub(crate) struct PreparedQuery {
    preloaded_instant: Arc<RwLock<PreloadMap>>,
    preloaded_grids: Arc<RwLock<GridPreloadMap>>,
    preloaded_matrices: Arc<RwLock<MatrixPreloadMap>>,
    preloaded_subqueries: Arc<RwLock<SubqueryPreloadMap>>,
    /// The query's sample budget, shared by the preload phase, the step loop and
    /// every sub-evaluator, so it counts the whole query.
    budget: Arc<SampleBudget>,
}

impl PreparedQuery {
    /// Empty maps that account against an existing budget: what a sub-evaluator
    /// starts from when its union preload was declined.
    pub(crate) fn sharing(budget: Arc<SampleBudget>) -> Self {
        Self {
            budget,
            ..Default::default()
        }
    }
}

/// One subquery's evaluator, prepared for the union of every outer step's
/// inner grid. Keyed by the subquery node (the expression tree outlives the
/// evaluator) and the resolved step, which is all that distinguishes two
/// grids of the same subquery within one outer query.
type SubqueryPreloadMap = ahash::AHashMap<(usize, i64), Arc<PreparedQuery>>;

fn subquery_key(subquery: &SubqueryExpr, step_ms: i64) -> (usize, i64) {
    (subquery as *const SubqueryExpr as usize, step_ms)
}

/// The step a subquery runs at, per the PromQL spec: its own `<resolution>`,
/// else the global evaluation interval — Prometheus' default of one minute.
///
/// Never the step of the query it sits in: that would make `m[5m:]` sample
/// every 15s inside a `step=15s` range query but every minute in an instant
/// query at the same timestamp, so `count_over_time(m[5m:])` would answer 20
/// in one and 5 in the other.
/// See: <https://prometheus.io/docs/prometheus/latest/querying/basics/#subquery>
/// and `DefaultGlobalConfig.EvaluationInterval` in prometheus/config/config.go.
fn subquery_step_ms(subquery: &SubqueryExpr) -> i64 {
    const DEFAULT_EVALUATION_INTERVAL_MS: i64 = 60_000;
    subquery
        .step
        .map_or(DEFAULT_EVALUATION_INTERVAL_MS, |step| {
            step.as_millis() as i64
        })
}

pub(crate) struct Evaluator<'reader, R: QueryReader + ?Sized> {
    reader: &'reader R,
    /// Preloaded per-step instant vector data for range queries.
    /// Populated by preload_for_range() before the step loop.
    preloaded_instant: Arc<RwLock<PreloadMap>>,
    /// Rollups, and aggregations over bare selectors, whose whole step grid
    /// was evaluated at the source in one request. Populated by
    /// preload_rollups() and preload_stepped_aggregations() before the step
    /// loop.
    preloaded_grids: Arc<RwLock<GridPreloadMap>>,
    /// Raw spans for matrix selectors that no rollup grid covers, so the step
    /// loop slices windows locally instead of re-fetching them per step.
    /// Populated by preload_matrices() before the step loop.
    preloaded_matrices: Arc<RwLock<MatrixPreloadMap>>,
    /// Each subquery's inner grid, preloaded once for every outer step at once
    /// rather than once per outer step. Populated by preload_subqueries()
    /// before the step loop; a subquery absent here prepares its own grid when
    /// evaluated (an instant query, or a preload that hit a reader limit).
    preloaded_subqueries: Arc<RwLock<SubqueryPreloadMap>>,
    /// Samples loaded so far on behalf of the whole query; see
    /// [`crate::promql::engine::sample_budget`].
    budget: Arc<SampleBudget>,
    options: QueryOptions,
}

impl<'reader, R: QueryReader + ?Sized> Evaluator<'reader, R> {
    pub(crate) fn new(reader: &'reader R, options: QueryOptions) -> Self {
        Self::with_prepared(
            reader,
            options,
            PreparedQuery::sharing(Arc::new(SampleBudget::new(options.max_samples))),
        )
    }

    pub(crate) fn with_prepared(
        reader: &'reader R,
        options: QueryOptions,
        prepared: PreparedQuery,
    ) -> Self {
        Self {
            reader,
            preloaded_instant: prepared.preloaded_instant,
            preloaded_grids: prepared.preloaded_grids,
            preloaded_matrices: prepared.preloaded_matrices,
            preloaded_subqueries: prepared.preloaded_subqueries,
            budget: prepared.budget,
            options,
        }
    }

    /// An evaluator over a prepared grid that other evaluators use too — the
    /// per-outer-step evaluators of one subquery, all reading its union
    /// preload. Cheap: the maps are shared, not copied.
    pub(crate) fn with_shared(
        reader: &'reader R,
        options: QueryOptions,
        prepared: Arc<PreparedQuery>,
    ) -> Self {
        Self {
            reader,
            preloaded_instant: Arc::clone(&prepared.preloaded_instant),
            preloaded_grids: Arc::clone(&prepared.preloaded_grids),
            preloaded_matrices: Arc::clone(&prepared.preloaded_matrices),
            preloaded_subqueries: Arc::clone(&prepared.preloaded_subqueries),
            budget: Arc::clone(&prepared.budget),
            options,
        }
    }

    pub(crate) fn into_prepared(self) -> PreparedQuery {
        PreparedQuery {
            preloaded_instant: self.preloaded_instant,
            preloaded_grids: self.preloaded_grids,
            preloaded_matrices: self.preloaded_matrices,
            preloaded_subqueries: self.preloaded_subqueries,
            budget: self.budget,
        }
    }

    /// Count `samples` against the query's budget; fails the query once the
    /// total it has loaded passes `ts-promql-max-samples-per-query`.
    fn charge_samples(&self, samples: usize) -> EvalResult<()> {
        Ok(self.budget.charge(samples)?)
    }

    fn charge_range_samples(&self, series: &[RangeSample<EvalLabels>]) -> EvalResult<()> {
        self.charge_samples(series.iter().map(|s| s.samples.len()).sum())
    }

    /// Charge what a live (non-preloaded) selector read materialized.
    fn charge_result(&self, result: &ExprResult) -> EvalResult<()> {
        match result {
            ExprResult::InstantVector(samples) => self.charge_samples(samples.len()),
            ExprResult::RangeVector(series) => {
                self.charge_samples(series.iter().map(|s| s.values.len()).sum())
            }
            _ => Ok(()),
        }
    }

    /// Fail fast once the query deadline has passed, so a preload phase that
    /// issues several requests stops scheduling more of them.
    fn check_deadline(&self) -> EvalResult<()> {
        if let Some(deadline) = self.options.deadline
            && deadline > 0
            && current_time_millis() > deadline
        {
            return Err(EvaluationError::Query(QueryError::Timeout));
        }
        Ok(())
    }

    /// Preload VectorSelector data for all steps of a range query.
    /// Must be called before the step loop. Walks the AST, deduplicates selectors,
    /// and builds dense per-step sample arrays for O(1) per-step lookup.
    pub(in crate::promql) fn preload_for_range(
        &self,
        expr: &Expr,
        ctx: &EvalContext,
    ) -> EvalResult<()> {
        self.preload_grid(expr, &PreloadGrid::for_range(ctx))
    }

    /// Fill every preload map for one step grid.
    ///
    /// Shared by the outer range query and by subquery sub-evaluators — the
    /// grid, not the enclosing [`EvalContext`], says which steps to cover.
    pub(in crate::promql) fn preload_grid(
        &self,
        expr: &Expr,
        grid: &PreloadGrid,
    ) -> EvalResult<()> {
        // A stepped selection ships each pick's own timestamp only when
        // something can observe it: `timestamp()` is the one function that
        // reports a sample's time rather than its step's.
        let grid = &PreloadGrid {
            sample_timestamps: calls_function(expr, "timestamp"),
            ..*grid
        };

        // Aggregations directly over a bare selector go first: each is one
        // fused request, and a selector it covers must not also be preloaded
        // on its own — that would ship the same series twice, once stepped
        // and once folded.
        self.preload_stepped_aggregations(expr, grid)?;

        // Deduplicate by PreloadKey, then parallelize the loading. A selector
        // the fused pass already cached stepped (a source that answered raw)
        // is not loaded again.
        let mut seen: AHashSet<PreloadKey> = self
            .preloaded_instant
            .read()
            .unwrap()
            .keys()
            .cloned()
            .collect();
        let unique_selectors: Vec<_> = {
            let grids = self.preloaded_grids.read().unwrap();
            collect_vector_selectors(expr, &|aggregate| {
                stepped_aggregation_key(aggregate).is_some_and(|key| grids.contains_key(&key))
            })
            .into_iter()
            .filter(|&vs| seen.insert(PreloadKey::from_selector(vs)))
            .collect()
        };

        let _: Vec<()> = unique_selectors
            .par_rayon()
            .map(|&vs| self.preload_vector_selector(vs, grid))
            .into_fallible_result()
            .collect()?;

        self.preload_rollups(expr, grid)?;
        self.preload_matrices(expr, grid)?;
        self.preload_subqueries(expr, grid)?;

        Ok(())
    }

    /// Prepare each subquery once for the whole outer grid.
    ///
    /// Every outer step evaluates the subquery over its own window, and each
    /// window is a run of the same lattice — the multiples of the subquery
    /// step — so their union is one grid from the earliest window's start to
    /// the latest window's end. A sub-evaluator prepared for that union
    /// answers every outer step by index (`evaluate_vector_selector` and
    /// `preloaded_rollup_by_key` locate a step from the entry's own
    /// `eval_start_ms`/`step_ms`), which turns outer_steps fetches of mostly
    /// the same span into one.
    ///
    /// Done here, on the thread driving the range query, rather than lazily by
    /// the first outer step to need it: the outer steps run as pool jobs, and a
    /// pool job must not block others on a lock while it waits on the pool.
    fn preload_subqueries(&self, expr: &Expr, grid: &PreloadGrid) -> EvalResult<()> {
        if grid.step_ms <= 0 {
            return Ok(());
        }
        for subquery in collect_subqueries(expr) {
            self.check_deadline()?;
            let step_ms = subquery_step_ms(subquery);
            if step_ms <= 0 {
                continue;
            }
            let key = subquery_key(subquery, step_ms);
            if self.preloaded_subqueries.read().unwrap().contains_key(&key) {
                continue;
            }
            // The union of the windows: modifiers shift (or, with `@`,
            // collapse) every window end the same way, so the extremes of the
            // shifted outer bounds bound them all.
            let (Some(first), Some(last)) = (grid.steps().next(), grid.steps().last()) else {
                continue;
            };
            let resolve = |ts: Timestamp| {
                apply_time_modifiers_ms(
                    subquery.at.as_ref(),
                    subquery.offset.as_ref(),
                    grid.at_start_ms,
                    grid.at_end_ms,
                    ts,
                )
            };
            let (first_end, last_end) = (resolve(first), resolve(last));
            let (earliest_end, latest_end) = (first_end.min(last_end), first_end.max(last_end));
            let range_ms = subquery.range.as_millis() as i64;
            let (aligned_start_ms, _, _, _) =
                compute_subquery_alignment(earliest_end - range_ms, latest_end, step_ms, 0);
            let union = PreloadGrid {
                start_ms: aligned_start_ms,
                end_ms: latest_end,
                step_ms,
                at_start_ms: grid.at_start_ms,
                at_end_ms: grid.at_end_ms,
                lookback_delta_ms: grid.lookback_delta_ms,
                // The sub-evaluator's preload decides this from the inner
                // expression itself.
                sample_timestamps: false,
            };
            let plan = PlannedQuery::for_grid(&subquery.expr, union);
            match Preloader::sharing(self.reader, self.options, Arc::clone(&self.budget))
                .prepare(plan)
            {
                Ok(prepared) => {
                    self.preloaded_subqueries
                        .write()
                        .unwrap()
                        .insert(key, Arc::new(prepared));
                }
                Err(err) if matches!(err, EvaluationError::Query(QueryError::Timeout)) => {
                    return Err(err);
                }
                Err(err) => {
                    // Same rule as the per-step preload: a reader limit tripped
                    // by the union span downgrades to per-step evaluation.
                    tracing::debug!(
                        error = %err,
                        "subquery union preload failed; each outer step will prepare its own grid"
                    );
                }
            }
        }
        Ok(())
    }

    /// Ask the source to evaluate each pushable rollup over the *whole* step
    /// grid, once, before the step loop starts.
    ///
    /// This is where the round-trip collapse lives. Evaluating `rate(m[5m])` at
    /// a 15s step over six hours is 1440 steps; done per step that is 1440
    /// fan-outs, each shipping a five-minute window that its twenty neighbours
    /// also ship. Done here it is one fan-out and one float per series per step.
    ///
    /// A rollup that cannot be pushed down is simply not cached, and the step
    /// loop evaluates it locally as before.
    fn preload_rollups(&self, expr: &Expr, grid: &PreloadGrid) -> EvalResult<()> {
        if grid.step_ms <= 0 {
            return Ok(());
        }

        let mut seen = AHashSet::new();
        let mut requests = Vec::new();
        for candidate in collect_rollup_candidates(expr) {
            let Some((kind, matrix, param)) = self.pushable_rollup(candidate.call()) else {
                continue;
            };
            // An aggregation that cannot be fused leaves the rollup to be pushed
            // down on its own; the aggregation then runs here, per step.
            let aggregation = match candidate {
                RollupCandidate::Fused(aggregate, _) => fusable_aggregation(aggregate),
                RollupCandidate::Rollup(_) => None,
            };
            let key = GridPreloadKey::rollup(
                &matrix.vs,
                kind,
                matrix_range_ms(matrix),
                param,
                aggregation.as_ref().map(AggregationKey::of),
            );
            if !seen.insert(key.clone()) {
                continue;
            }
            requests.push((key, kind, matrix, param, aggregation));
        }

        // One request per distinct rollup, in parallel but capped: each is a
        // blocking round trip, so `rate(a[5m]) / rate(b[5m])` should pay one
        // fanout latency, not two in sequence — while a query with many
        // rollups must not open one fanout per rollup all at once. The
        // fallible collect stops scheduling after the first error.
        let _: Vec<()> = requests
            .into_par_rayon()
            .num_threads(MAX_CONCURRENT_PRELOAD_REQUESTS)
            .map(|(key, kind, matrix, param, aggregation)| {
                self.check_deadline()?;
                self.preload_rollup(key, kind, matrix, param, aggregation, grid)
            })
            .into_fallible_result()
            .collect()?;

        Ok(())
    }

    /// Fetch, once, the raw span every remaining matrix selector's windows
    /// cover.
    ///
    /// This is the fallback grid for calls that cannot be pushed down as a
    /// rollup — a function outside [`RollupKind`], a non-literal parameter,
    /// a modifier shape the grid request cannot describe. Without it every step
    /// re-fetches its own window, and neighbouring steps re-ship mostly the
    /// same samples (a `[5m]` window at a 15s step is fetched ~20 times over).
    /// With it the span is read in one request and each step slices its window
    /// locally, in `evaluate_matrix_selector`.
    ///
    /// A call already answered by a preloaded rollup grid is skipped: its
    /// matrix argument is never evaluated, so a span for it would be dead
    /// weight.
    ///
    /// Bounded by design: the span read is subject to the reader's own
    /// `max_series` / `max_points_per_series` validation, and a failed read
    /// leaves the map unpopulated so the step loop falls back to exactly the
    /// per-step path that ran before this optimization — a query that succeeds per-step keeps succeeding.
    fn preload_matrices(&self, expr: &Expr, grid: &PreloadGrid) -> EvalResult<()> {
        if grid.step_ms <= 0 {
            return Ok(());
        }

        let grids = self.preloaded_grids.read().unwrap();
        let mut seen = AHashSet::new();
        let mut targets: Vec<(MatrixPreloadKey, &MatrixSelector)> = Vec::new();
        for candidate in collect_rollup_candidates(expr) {
            let call = candidate.call();
            // A call the step loop will reject anyway must not trigger a
            // fetch for its arguments.
            if call.func.experimental && !self.options.enable_experimental_functions {
                continue;
            }
            // Mirror the key the step loop will look up: covered calls short-
            // circuit in evaluate_call / evaluate_fused_rollup before their
            // arguments are evaluated.
            if let Some((kind, matrix, param)) = self.pushable_rollup(call) {
                let aggregation = match candidate {
                    RollupCandidate::Fused(aggregate, _) => fusable_aggregation(aggregate),
                    RollupCandidate::Rollup(_) => None,
                };
                let key = GridPreloadKey::rollup(
                    &matrix.vs,
                    kind,
                    matrix_range_ms(matrix),
                    param,
                    aggregation.as_ref().map(AggregationKey::of),
                );
                if grids.contains_key(&key) {
                    continue;
                }
            }
            for arg in call.args.args.iter().map(|arg| strip_parens(arg)) {
                if let Expr::MatrixSelector(ms) = arg {
                    let key = MatrixPreloadKey::new(&ms.vs, matrix_range_ms(ms));
                    if seen.insert(key.clone()) {
                        targets.push((key, ms));
                    }
                }
            }
        }
        drop(grids);

        let _: Vec<()> = targets
            .into_par_rayon()
            .num_threads(MAX_CONCURRENT_PRELOAD_REQUESTS)
            .map(|(key, matrix)| -> EvalResult<()> {
                self.check_deadline()?;
                self.preload_matrix(key, matrix, grid)
            })
            .into_fallible_result()
            .collect()?;

        Ok(())
    }

    /// Read one matrix selector's whole span and cache it for per-step
    /// slicing.
    ///
    /// Errors are deliberately not propagated: the cache is an optimization,
    /// and the per-step path the step loop falls back to reproduces the
    /// unpreloaded behavior exactly, including its per-window limit checks. A
    /// span that exceeds the reader's limits therefore downgrades the query to
    /// the per-step path instead of failing it.
    fn preload_matrix(
        &self,
        key: MatrixPreloadKey,
        matrix: &MatrixSelector,
        grid: &PreloadGrid,
    ) -> EvalResult<()> {
        let window_ends = self.resolved_window_ends(&matrix.vs, grid);
        let (Some(&first), Some(&last)) = (window_ends.first(), window_ends.last()) else {
            return Ok(());
        };
        let range_ms = matrix_range_ms(matrix);
        // Windows are half-open — `(end - range, end]` — against an
        // inclusive-lower-bound reader, so start one past the earliest
        // window's lower bound. Same convention as `rollup_fetch_bounds` and
        // the per-step pipeline.
        let start_ms = (first - range_ms).saturating_add(1);

        match self
            .reader
            .query_range(&matrix.vs, start_ms, last, self.options)
        {
            Ok(series) => {
                // Over budget is a query failure, not a declined preload: the
                // budget is query-wide and already exceeded, so per-step reads
                // would only be refused one by one.
                self.charge_range_samples(&series)?;
                let series = series
                    .into_iter()
                    .map(|s| PreloadedMatrixSeries {
                        labels: s.labels,
                        samples: Arc::from(s.samples),
                    })
                    .collect();
                self.preloaded_matrices
                    .write()
                    .unwrap()
                    .insert(key, PreloadedMatrixData { series });
            }
            Err(err @ QueryError::TooManySamples { .. }) => return Err(err.into()),
            Err(err) => {
                tracing::debug!(
                    error = %err,
                    "matrix preload failed; falling back to per-step windows"
                );
            }
        }
        Ok(())
    }

    /// The window ends of `grid` for `vs` — one per step, in step order, with
    /// `@`/`offset` resolved here so a source (or the preloaded span's fetch
    /// bounds) can never resolve a modifier differently than the local path
    /// would.
    ///
    /// `@ start()`/`@ end()` resolve against the grid's *at* bounds, which for
    /// a subquery grid are the enclosing query's — matching what the per-step
    /// fallback in `evaluate_vector_selector` / `evaluate_matrix_selector`
    /// computes from `ctx.query_start`/`ctx.query_end`.
    fn resolved_window_ends(&self, vs: &VectorSelector, grid: &PreloadGrid) -> Vec<Timestamp> {
        grid.steps()
            .map(|step_ts| {
                apply_time_modifiers_ms(
                    vs.at.as_ref(),
                    vs.offset.as_ref(),
                    grid.at_start_ms,
                    grid.at_end_ms,
                    step_ts,
                )
            })
            .collect()
    }

    /// The rollup a call can be pushed down as, if any.
    ///
    /// Unlike the instant path's [`Self::rollup_arguments`], the scalar
    /// parameter must be a *literal*. One grid request carries one parameter, so
    /// a parameter that could differ per step — `quantile_over_time(scalar(q),
    /// m[5m])` — cannot be answered by a single request at all. That is a
    /// correctness bound, not an optimization: such a call stays local.
    fn pushable_rollup<'a>(
        &self,
        call: &'a Call,
    ) -> Option<(RollupKind, &'a MatrixSelector, Option<f64>)> {
        // The coordinator keeps authority over experimental functions: a shard
        // must never be asked to run one the request was not approved for. The
        // step loop rejects the query anyway, but preloading runs before it, so
        // without this the fan-out would go out first.
        if call.func.experimental && !self.options.enable_experimental_functions {
            return None;
        }

        let kind = RollupKind::from_function_name(call.func.name)?;

        let mut matrix = None;
        let mut param = None;
        for arg in call.args.args.iter().map(|arg| strip_parens(arg)) {
            match arg {
                Expr::MatrixSelector(ms) if matrix.is_none() => matrix = Some(ms),
                Expr::NumberLiteral(literal) if param.is_none() => param = Some(literal.val),
                _ => return None,
            }
        }

        Some((kind, matrix?, param))
    }

    #[allow(clippy::too_many_arguments)]
    fn preload_rollup(
        &self,
        key: GridPreloadKey,
        kind: RollupKind,
        matrix: &MatrixSelector,
        param: Option<f64>,
        aggregation: Option<GridAggregation>,
        grid: &PreloadGrid,
    ) -> EvalResult<()> {
        let rollup = GridRollup {
            kind,
            range_ms: matrix_range_ms(matrix),
            param,
        };
        let Some((request, window_ends)) =
            self.grid_request(&matrix.vs, grid, Some(rollup), aggregation)
        else {
            return Ok(());
        };

        let mut options = self.options;
        options.lookback_delta = Duration::from_millis(grid.lookback_delta_ms as u64);

        let rolled = self.finish_grid(
            &request,
            self.reader.query_grid(&matrix.vs, &request, options)?,
        )?;
        self.charge_range_samples(&rolled)?;

        let series = rolled
            .into_iter()
            .map(|s| PreloadedGridSeries {
                labels: s.labels,
                values: scatter_onto_grid(
                    &window_ends,
                    s.samples.iter().map(|p| (p.timestamp, p.value)),
                ),
            })
            .collect();

        self.cache_preloaded_grid(key, grid, series);
        Ok(())
    }

    /// The grid request for `vs` over `grid`, with `@`/`offset` resolved into
    /// window ends, or `None` when the request cannot describe them.
    ///
    /// The request describes its windows as a start/end/step progression;
    /// `@` collapses every step onto one window end, and `offset` shifts them
    /// uniformly. The progression the source will derive is verified to be
    /// exactly the set of ends resolved here rather than trusting that every
    /// modifier shape reduces to one — an unanticipated one stays local
    /// instead of silently answering for the wrong windows. Alongside the
    /// request come the *unreduced* ends, one per step, which is what
    /// [`scatter_onto_grid`] walks.
    fn grid_request(
        &self,
        vs: &VectorSelector,
        grid: &PreloadGrid,
        rollup: Option<GridRollup>,
        aggregation: Option<GridAggregation>,
    ) -> Option<(GridRequest, Vec<Timestamp>)> {
        let window_ends = self.resolved_window_ends(vs, grid);
        let (&first, &last) = (window_ends.first()?, window_ends.last()?);

        let request = GridRequest {
            step_ms: grid.step_ms,
            query_start: first,
            query_end: last,
            range_end_ms: last,
            lookback_delta_ms: grid.lookback_delta_ms,
            rollup,
            aggregation,
            sample_timestamps: grid.sample_timestamps,
        };

        let mut resolved = window_ends.clone();
        resolved.dedup();
        (request.window_ends() == resolved).then_some((request, window_ends))
    }

    /// A rollup or fused request's answer as per-entry `(step, value)` points,
    /// whoever did the work.
    ///
    /// A source that answered raw is compensated with the request's own
    /// kernels — the same ones a shard runs — after its span is charged to the
    /// budget. The stepped shape is not an answer to these requests at all.
    fn finish_grid(
        &self,
        request: &GridRequest,
        outcome: GridOutcome,
    ) -> EvalResult<Vec<RangeSample<EvalLabels>>> {
        let outcome = match outcome {
            GridOutcome::Raw(series) => {
                // The raw spans are what was loaded; charge them before the
                // evaluation turns them into one point per step.
                self.charge_range_samples(&series)?;
                request.evaluate(series)?
            }
            other => other,
        };
        match (
            outcome,
            request.rollup.is_some(),
            request.aggregation.is_some(),
        ) {
            (GridOutcome::Rolled(series), true, false) => Ok(series),
            (GridOutcome::Reduced(groups), _, true) => Ok(groups),
            _ => Err(EvaluationError::InternalError(
                "grid request answered with the wrong shape".to_string(),
            )),
        }
    }

    fn cache_preloaded_grid(
        &self,
        key: GridPreloadKey,
        grid: &PreloadGrid,
        series: Vec<PreloadedGridSeries>,
    ) {
        self.preloaded_grids.write().unwrap().insert(
            key,
            PreloadedGridData {
                eval_start_ms: grid.start_ms,
                step_ms: grid.step_ms,
                series,
            },
        );
    }

    /// Ask the source to fold each aggregation that sits directly over a bare
    /// vector selector — `avg(cpu)`, `sum by (region) (cpu)` — over the whole
    /// step grid, once, before the step loop starts.
    ///
    /// The shard steps the selector and folds the picks per `(group, step)`,
    /// so what crosses the wire is groups × steps rather than series × steps —
    /// and the selector itself is not preloaded separately (see
    /// [`Self::preload_grid`]).
    fn preload_stepped_aggregations(&self, expr: &Expr, grid: &PreloadGrid) -> EvalResult<()> {
        if grid.step_ms <= 0 {
            return Ok(());
        }

        let mut seen = AHashSet::new();
        let mut requests = Vec::new();
        for (aggregate, vs) in collect_stepped_aggregation_candidates(expr) {
            let Some(aggregation) = fusable_aggregation(aggregate) else {
                continue;
            };
            // A selecting operator's output keeps its picks' own timestamps,
            // which the fused form stamps with the step; only `timestamp()`
            // can tell, and when it is in the query the selection runs here.
            if grid.sample_timestamps && aggregation.strategy() == PushdownStrategy::Select {
                continue;
            }
            let key = GridPreloadKey::stepped(vs, AggregationKey::of(&aggregation));
            if !seen.insert(key.clone()) {
                continue;
            }
            requests.push((key, vs, aggregation));
        }

        let _: Vec<()> = requests
            .into_par_rayon()
            .num_threads(MAX_CONCURRENT_PRELOAD_REQUESTS)
            .map(|(key, vs, aggregation)| {
                self.check_deadline()?;
                self.preload_stepped_aggregation(key, vs, aggregation, grid)
            })
            .into_fallible_result()
            .collect()?;

        Ok(())
    }

    fn preload_stepped_aggregation(
        &self,
        key: GridPreloadKey,
        vs: &VectorSelector,
        aggregation: GridAggregation,
        grid: &PreloadGrid,
    ) -> EvalResult<()> {
        let Some((request, window_ends)) = self.grid_request(vs, grid, None, Some(aggregation))
        else {
            return Ok(());
        };

        let mut options = self.options;
        options.lookback_delta = Duration::from_millis(grid.lookback_delta_ms as u64);

        let groups = match self.reader.query_grid(vs, &request, options)? {
            // Nothing was pushed down: the span is here. Grouping it once per
            // step from a stepped grid — in parallel across steps, as the step
            // loop does — beats folding every point through one map on this
            // thread, so this becomes the selector's own stepped preload and
            // the aggregation runs per step over it, exactly as an unfused one.
            GridOutcome::Raw(series) => return self.cache_stepped_span(vs, grid, series),
            outcome => self.finish_grid(&request, outcome)?,
        };
        self.charge_range_samples(&groups)?;

        let series = groups
            .into_iter()
            .map(|s| PreloadedGridSeries {
                labels: s.labels,
                values: scatter_onto_grid(
                    &window_ends,
                    s.samples.iter().map(|p| (p.timestamp, p.value)),
                ),
            })
            .collect();

        self.cache_preloaded_grid(key, grid, series);
        Ok(())
    }

    /// This step's slice of a preloaded rollup, or `None` when the call was not
    /// preloaded and has to be evaluated here.
    fn preloaded_rollup(&self, call: &Call, ctx: &EvalContext) -> Option<ExprResult> {
        let (kind, matrix, param) = self.pushable_rollup(call)?;
        let key = GridPreloadKey::rollup(&matrix.vs, kind, matrix_range_ms(matrix), param, None);
        self.preloaded_grid_by_key(&key, ctx, false)
    }

    /// This step's slice of a preloaded grid, keyed explicitly so the fused
    /// forms — whose entries are groups rather than series — can share it.
    fn preloaded_grid_by_key(
        &self,
        key: &GridPreloadKey,
        ctx: &EvalContext,
        drop_name: bool,
    ) -> Option<ExprResult> {
        let guard = self.preloaded_grids.read().unwrap();
        let preloaded = guard.get(key)?;
        let step_idx = ((ctx.evaluation_ts - preloaded.eval_start_ms) / preloaded.step_ms) as usize;

        let samples = preloaded
            .series
            .iter()
            .filter_map(|series| {
                // A step whose window held no samples contributes nothing —
                // the series is absent at this step, not NaN here.
                let value = series.values.get(step_idx)?;
                Some(EvalSample {
                    timestamp_ms: ctx.evaluation_ts,
                    value,
                    labels: series.labels.clone(),
                    drop_name,
                })
            })
            .collect();

        Some(ExprResult::InstantVector(samples))
    }

    /// Convenience wrapper that builds an [`EvalContext`] from a full [`EvalStmt`]
    /// so callers outside the `exec` module don't need to construct it manually.
    pub(in crate::promql) fn preload_for_range_from_stmt(&self, stmt: &EvalStmt) -> EvalResult<()> {
        let ctx = EvalContext::from(stmt);
        self.preload_for_range(&stmt.expr, &ctx)
    }

    /// Preload one vector selector over the whole step grid: at every step,
    /// the last sample inside the lookback window.
    ///
    /// The source is asked for the *stepped* form — one point per series per
    /// step — rather than the raw span, so a cluster ships the grid and not
    /// every sample under it. A source that answers raw (a single node) has
    /// the same bucketing run here.
    fn preload_vector_selector(&self, vs: &VectorSelector, grid: &PreloadGrid) -> EvalResult<()> {
        let eval_start_ms = grid.start_ms;
        let step_ms = grid.step_ms;
        let lookback_delta_ms = grid.lookback_delta_ms;

        // One window end per step, `@`/`offset` resolved here so the source
        // is never handed a modifier. With `@` every step shares one end and
        // the source answers that one; the scatter below replicates it.
        let raw_series = match self.grid_request(vs, grid, None, None) {
            Some((request, window_ends)) => {
                let mut options = self.options;
                options.lookback_delta = Duration::from_millis(lookback_delta_ms as u64);
                match self.reader.query_grid(vs, &request, options)? {
                    GridOutcome::Stepped(series) => {
                        self.charge_samples(series.iter().map(|s| s.points.len()).sum())?;
                        let preloaded_series = series
                            .into_iter()
                            .map(|s| PreloadedInstantSeries {
                                labels: s.labels,
                                values: scatter_onto_grid(
                                    &window_ends,
                                    s.points.iter().map(|p| (p.step_ts, p.sample)),
                                ),
                            })
                            .collect();
                        self.cache_preloaded_series(vs, eval_start_ms, step_ms, preloaded_series);
                        return Ok(());
                    }
                    GridOutcome::Raw(series) => series,
                    GridOutcome::Rolled(_) | GridOutcome::Reduced(_) => {
                        return Err(EvaluationError::InternalError(
                            "stepped selection answered with the wrong shape".to_string(),
                        ));
                    }
                }
            }
            // A modifier shape the grid request cannot describe: read the
            // span the selector's bounds cover and bucket it here.
            None => {
                let (earliest_ms, latest_ms) = selector_bounds(
                    vs.at.as_ref(),
                    vs.offset.as_ref(),
                    grid.at_start_ms,
                    grid.at_end_ms,
                    eval_start_ms,
                    grid.end_ms,
                    lookback_delta_ms,
                );
                self.reader
                    .query_range(vs, earliest_ms, latest_ms, self.options)?
            }
        };
        self.cache_stepped_span(vs, grid, raw_series)
    }

    /// Bucket a selector's raw span to one sample per step — the last sample
    /// inside the lookback window — and cache it for the step loop.
    fn cache_stepped_span(
        &self,
        vs: &VectorSelector,
        grid: &PreloadGrid,
        raw_series: Vec<RangeSample<EvalLabels>>,
    ) -> EvalResult<()> {
        self.charge_range_samples(&raw_series)?;

        let eval_start_ms = grid.start_ms;
        let step_ms = grid.step_ms;
        let lookback_delta_ms = grid.lookback_delta_ms;
        let num_steps = grid.expected_steps();
        let at_modifier = vs.at.clone();
        let offset_mod = vs.offset.clone();
        let at_start_ms = grid.at_start_ms;
        let at_end_ms = grid.at_end_ms;

        // ── Per-series step-bucketing ─────────────────
        let preloaded_series: Vec<PreloadedInstantSeries> = raw_series
            .into_par_rayon()
            .map(|RangeSample { labels, samples }| {
                // Per-step instant stmt sets query_start = query_end = eval_ts for the evaluation
                // timestamp; however, when resolving `@ start()` / `@ end()` inside the
                // preloading phase we must use the enclosing query's bounds so that
                // `@ start()`/`@ end()` sweep the full query range across steps.
                // Pass `at_start_ms`/`at_end_ms` as the `query_start`/`query_end`
                // parameters so AtModifier::Start/End resolve exactly as the
                // per-step fallback path resolves them from the EvalContext.
                let steps = (0..num_steps).map(|step_idx| {
                    let eval_ts_i = eval_start_ms + (step_idx as i64) * step_ms;
                    apply_time_modifiers_ms(
                        at_modifier.as_ref(),
                        offset_mod.as_ref(),
                        at_start_ms,
                        at_end_ms,
                        eval_ts_i,
                    )
                });

                let mut values = StepGridBuilder::with_capacity(num_steps);
                for_each_step_sample(&samples, steps, lookback_delta_ms, |_, latest| {
                    values.push(latest.copied());
                });
                let values = values.finish();

                PreloadedInstantSeries { labels, values }
            })
            .collect();

        self.cache_preloaded_series(vs, eval_start_ms, step_ms, preloaded_series);

        Ok(())
    }

    fn cache_preloaded_series(
        &self,
        vs: &VectorSelector,
        eval_start_ms: Timestamp,
        step_ms: i64,
        preloaded_series: Vec<PreloadedInstantSeries>,
    ) {
        let key = PreloadKey::from_selector(vs);
        let data = PreloadedInstantData {
            eval_start_ms,
            step_ms,
            series: preloaded_series,
        };
        let mut cache = self.preloaded_instant.write().unwrap();
        cache.insert(key, data);
    }

    pub(crate) fn evaluate(&self, stmt: EvalStmt) -> EvalResult<ExprResult> {
        if stmt.start != stmt.end {
            return Err(EvaluationError::InternalError(format!(
                "evaluation must always be done at an instant.got start({:?}), end({:?})",
                stmt.start, stmt.end
            )));
        }

        let ctx = EvalContext {
            query_start: system_time_to_millis(stmt.start),
            query_end: system_time_to_millis(stmt.end),
            evaluation_ts: system_time_to_millis(stmt.end),
            lookback_delta_ms: stmt.lookback_delta.as_millis() as i64,
            step_ms: stmt.interval.as_millis() as i64,
        };

        self.evaluate_with_context(&stmt.expr, ctx)
    }

    pub(crate) fn evaluate_with_context(
        &self,
        expr: &Expr,
        ctx: EvalContext,
    ) -> EvalResult<ExprResult> {
        let mut result = self.evaluate_expr(expr, &ctx, true)?;

        // Deferred __name__ cleanup (mirrors Prometheus cleanupMetricLabels)
        Self::cleanup_metric_labels(&mut result)?;

        Ok(result)
    }

    /// Remove `__name__` label from the result if `drop_name` is true. Mirrors Prometheus's `cleanupMetricLabels` logic in engine.go.
    fn cleanup_metric_labels(v: &mut ExprResult) -> EvalResult<()> {
        match v {
            ExprResult::RangeVector(mat) => {
                for v in mat.iter_mut() {
                    v.drop_name_if_needed();
                }
            }
            ExprResult::InstantVector(vec) => {
                for v in vec.iter_mut() {
                    v.drop_name_if_needed();
                }

                ensure_unique_labelsets(vec)?;
            }
            _ => {}
        }

        Ok(())
    }

    // this call recurses to evaluate sub-expressions
    pub(super) fn evaluate_expr<'a>(
        &'a self,
        expr: &'a Expr,
        ctx: &'a EvalContext,
        preload_eligible: bool,
    ) -> EvalResult<ExprResult> {
        match expr {
            Expr::Aggregate(aggregate) => self.evaluate_aggregate(aggregate, ctx, preload_eligible),
            Expr::Unary(u) => self.evaluate_unary(u, ctx, preload_eligible),
            Expr::Binary(b) => self.evaluate_binary_expr(b, ctx, preload_eligible),
            Expr::Paren(p) => self.evaluate_expr(&p.expr, ctx, preload_eligible),
            Expr::Subquery(q) => self.evaluate_subquery(q, ctx),
            Expr::NumberLiteral(l) => Ok(ExprResult::Scalar(l.val)),
            Expr::StringLiteral(l) => Ok(ExprResult::String(l.val.clone())),
            Expr::VectorSelector(vector_selector) => {
                self.evaluate_vector_selector(vector_selector, ctx, preload_eligible)
            }
            Expr::MatrixSelector(matrix_selector) => {
                self.evaluate_matrix_selector(matrix_selector, ctx, preload_eligible)
            }
            Expr::Call(call) => self.evaluate_call(call, ctx, preload_eligible),
            Expr::Extension(_) => Err(EvaluationError::InternalError(
                "unsupported PromQL extension expression".to_string(),
            )),
        }
    }

    pub(super) fn evaluate_matrix_selector(
        &self,
        matrix_selector: &MatrixSelector,
        ctx: &EvalContext,
        preload_eligible: bool,
    ) -> EvalResult<ExprResult> {
        let vector_selector = &matrix_selector.vs;
        let range_ms = matrix_range_ms(matrix_selector);

        // Apply time modifiers to evaluation_ts
        let adjusted_eval_ts = apply_time_modifiers_ms(
            vector_selector.at.as_ref(),
            vector_selector.offset.as_ref(),
            ctx.query_start,
            ctx.query_end,
            ctx.evaluation_ts,
        );

        // Slice this step's window out of the preloaded span, if the
        // selector's whole grid was fetched up front (`preload_matrices`).
        // The slice is exactly what the live fetch below would return — the
        // same half-open `(end - range, end]` window, and a series whose
        // window is empty is absent rather than present-and-empty — so the
        // two paths cannot disagree.
        //
        // The span read is the one `self`'s map holds. A subquery's steps run
        // on a sub-evaluator whose span was fetched for the subquery's own
        // grid, so they never reach into the outer query's span — whose fetch
        // bounds their windows can fall outside of, where a truncated window
        // would be silently wrong rather than slow.
        if preload_eligible {
            let key = MatrixPreloadKey::new(vector_selector, range_ms);
            let guard = self.preloaded_matrices.read().unwrap();
            if let Some(preloaded) = guard.get(&key) {
                let series = preloaded
                    .series
                    .iter()
                    .filter_map(|s| {
                        let window = window_range(&s.samples, adjusted_eval_ts, range_ms)?;
                        Some(EvalSamples {
                            labels: s.labels.clone(),
                            drop_name: false,
                            range_ms,
                            values: SampleWindow::shared(&s.samples, window),
                            range_end_ms: adjusted_eval_ts,
                        })
                    })
                    .collect();
                return Ok(ExprResult::RangeVector(series));
            }
        }

        let plan = QueryPlan::for_matrix(adjusted_eval_ts, range_ms);

        let result = execute_selector_pipeline(self.reader, &plan, vector_selector, self.options)?;
        self.charge_result(&result)?;
        Ok(result)
    }

    pub(super) fn evaluate_subquery(
        &self,
        subquery: &SubqueryExpr,
        ctx: &EvalContext,
    ) -> EvalResult<ExprResult> {
        let adjusted_eval_ts = apply_time_modifiers_ms(
            subquery.at.as_ref(),
            subquery.offset.as_ref(),
            ctx.query_start,
            ctx.query_end,
            ctx.evaluation_ts,
        );

        // Calculate subquery time range: [adjusted_eval_ts - range, adjusted_eval_ts]
        let subquery_end_ms = adjusted_eval_ts;
        let range_ms = subquery.range.as_millis() as i64;
        let subquery_start_ms = subquery_end_ms - range_ms;

        let step_ms = subquery_step_ms(subquery);

        // Guard against invalid step
        if step_ms <= 0 {
            return Err(EvaluationError::InternalError(
                "subquery step must be > 0".to_string(),
            ));
        }

        // Fast path: if inner expression is a pure VectorSelector, evaluate over range once.
        //
        // Only for a selector with no time modifiers of its own.
        // `evaluate_subquery_vector_selector` derives its whole grid from the
        // subquery's start/end/step (`QueryPlan::for_subquery_vector_selector`)
        // and never sees `at`/`offset`, so a modifier on the inner selector
        // would be silently dropped. Those shapes take the general per-step
        // path below, which resolves modifiers through
        // `apply_time_modifiers_ms` — and which subquery-scoped preloading now
        // serves from one span fetch rather than one read per step, so the
        // detour is no longer expensive.
        if let Expr::VectorSelector(ref selector) = *subquery.expr
            && selector.at.is_none()
            && selector.offset.is_none()
        {
            return self.evaluate_subquery_vector_selector(
                selector,
                subquery_start_ms,
                subquery_end_ms,
                step_ms,
                ctx.lookback_delta_ms,
            );
        }

        // Align start time to the step interval to ensure consistent evaluation points
        // (see compute_subquery_alignment for the negative-timestamp rationale).
        let (aligned_start_ms, _, _, expected_steps) =
            compute_subquery_alignment(subquery_start_ms, subquery_end_ms, step_ms, 0);

        let mut steps = step_times(aligned_start_ms, subquery_end_ms, step_ms);
        const PARALLEL_SUBQUERY_STEP_THRESHOLD: usize = 4;
        const SUBQUERY_STEP_BATCH_SIZE: usize = 64;

        // Preload the subquery's *own* grid, in a sub-evaluator that owns the
        // maps.
        //
        // Without this each inner step reads live: an inner rollup issues a
        // `query_grid` per inner step and an inner selector a `query` per
        // inner step, so a range query over `max_over_time(rate(m[5m])[1h:1m])`
        // costs outer_steps × 60 requests — the worst asymptotic shape in the
        // engine. Preloading collapses the inner dimension to one request per
        // distinct selector/rollup.
        //
        // A sub-evaluator rather than a grid identity on the evaluator-global
        // maps: the subquery's grid is not the outer query's, so entries keyed
        // only by selector would answer the wrong grid. Scoping them to an
        // evaluator that drops with the subquery makes that structurally
        // impossible, and nests naturally for a subquery inside a subquery.
        // `collect_vector_selectors` / `collect_rollup_candidates` both stop at
        // `Expr::Subquery`, so this walk covers exactly the nodes evaluated at
        // this grid.
        //
        // For a range query the union of every outer step's grid was prepared
        // up front (`preload_subqueries`); this step only wraps it. Otherwise —
        // an instant query, or a union preload that was declined — prepare this
        // one step's grid here.
        let union = self
            .preloaded_subqueries
            .read()
            .unwrap()
            .get(&subquery_key(subquery, step_ms))
            .cloned();
        let sub = match union {
            Some(prepared) => Evaluator::with_shared(self.reader, self.options, prepared),
            None => {
                let grid =
                    PreloadGrid::for_subquery(aligned_start_ms, subquery_end_ms, step_ms, ctx);
                let sub_plan = PlannedQuery::for_grid(&subquery.expr, grid);
                let prepared =
                    match Preloader::sharing(self.reader, self.options, Arc::clone(&self.budget))
                        .prepare(sub_plan)
                    {
                        Ok(prepared) => prepared,
                        Err(err) => {
                            // A deadline means the query is over; more work cannot help.
                            if matches!(err, EvaluationError::Query(QueryError::Timeout)) {
                                return Err(err);
                            }
                            // Otherwise best-effort, on the same rule as the matrix preload: the per-step path below
                            // reproduces the unpreloaded behavior exactly, so a preload that trips a reader limit
                            // downgrades the subquery to per-step reads rather than failing a query that used to succeed.
                            tracing::debug!(
                                error = %err,
                                "subquery preload failed; falling back to per-step evaluation"
                            );
                            PreparedQuery::sharing(Arc::clone(&self.budget))
                        }
                    };
                Evaluator::with_prepared(self.reader, self.options, prepared)
            }
        };

        let mut series_map = SubquerySeriesMap::default();
        if expected_steps < PARALLEL_SUBQUERY_STEP_THRESHOLD {
            for current_time_ms in steps {
                let (current_time_ms, samples) =
                    sub.eval_subquery_step(subquery, ctx, current_time_ms)?;
                merge_step_into_subquery_map(&mut series_map, current_time_ms, samples);
            }
        } else {
            // Evaluate in bounded batches. Collecting every inner step before
            // merging makes a long subquery retain one vector per step in
            // addition to the final series map.
            loop {
                let batch: Vec<_> = steps.by_ref().take(SUBQUERY_STEP_BATCH_SIZE).collect();
                if batch.is_empty() {
                    break;
                }
                let step_results: Vec<(i64, Vec<EvalSample>)> = batch
                    .into_par_rayon()
                    .map(|eval_ts| sub.eval_subquery_step(subquery, ctx, eval_ts))
                    .into_fallible_result()
                    .collect()?;
                // Parallel collection preserves batch input order, and batches
                // are consumed chronologically, so series values stay sorted.
                for (current_time_ms, samples) in step_results {
                    merge_step_into_subquery_map(&mut series_map, current_time_ms, samples);
                }
            }
        }

        let vector = series_map
            .into_iter()
            .map(|(labels, (values, drop_name))| EvalSamples {
                values: values.into(),
                labels,
                range_ms,
                range_end_ms: subquery_end_ms,
                drop_name,
            })
            .collect();

        Ok(ExprResult::RangeVector(vector))
    }

    /// Fast path for VectorSelector subqueries using range-based evaluation.
    ///
    /// Instead of evaluating the selector once per step (O(steps × series × index_lookup)),
    /// this fetches all samples in the range once and buckets them into steps
    /// (O(series × samples_in_range + samples + steps)).
    fn evaluate_subquery_vector_selector(
        &self,
        vector_selector: &VectorSelector,
        subquery_start_ms: i64,
        subquery_end_ms: i64,
        step_ms: i64,
        lookback_delta_ms: i64,
    ) -> EvalResult<ExprResult> {
        let plan = QueryPlan::for_subquery_vector_selector(
            subquery_start_ms,
            subquery_end_ms,
            step_ms,
            lookback_delta_ms,
        );
        let result = execute_selector_pipeline(self.reader, &plan, vector_selector, self.options)?;
        self.charge_result(&result)?;
        Ok(result)
    }

    pub(super) fn evaluate_vector_selector(
        &self,
        vector_selector: &VectorSelector,
        ctx: &EvalContext,
        preload_eligible: bool,
    ) -> EvalResult<ExprResult> {
        // Fast path: use preloaded data if available. The step index comes from
        // the preloaded entry's own `eval_start_ms`/`step_ms`, so this serves
        // an outer range-query grid and a subquery sub-evaluator's grid alike;
        // `preload_eligible` says only whether `self`'s maps describe the grid
        // being stepped over.
        if preload_eligible {
            let preload_key = PreloadKey::from_selector(vector_selector);
            let guard = self.preloaded_instant.read().unwrap();
            if let Some(preloaded) = guard.get(&preload_key) {
                let evaluation_ts = ctx.evaluation_ts;
                // Step index from raw evaluation_ts (before modifiers) — matches outer step loop
                let step_idx =
                    ((evaluation_ts - preloaded.eval_start_ms) / preloaded.step_ms) as usize;

                let mut samples = Vec::new();
                for series in &preloaded.series {
                    if let Some(sample) = series.values.get(step_idx) {
                        samples.push(EvalSample {
                            timestamp_ms: sample.timestamp,
                            value: sample.value,
                            labels: series.labels.clone(),
                            drop_name: false,
                        });
                    }
                }
                return Ok(ExprResult::InstantVector(samples));
            }
        }

        // Apply time modifiers (offset and @)
        let adjusted_eval_ts = apply_time_modifiers_ms(
            vector_selector.at.as_ref(),
            vector_selector.offset.as_ref(),
            ctx.query_start,
            ctx.query_end,
            ctx.evaluation_ts,
        );

        // The pipeline's instant-vector path stamps `lookback_delta` from the plan
        // onto the options before calling QueryReader::query.
        let plan = QueryPlan::for_instant_vector(adjusted_eval_ts, ctx.lookback_delta_ms);

        let result = execute_selector_pipeline(self.reader, &plan, vector_selector, self.options)?;
        self.charge_result(&result)?;
        Ok(result)
    }

    /// Evaluate the subquery's inner expression at one of its steps.
    ///
    /// Must be called on the sub-evaluator whose maps were preloaded for this
    /// subquery's grid (see `evaluate_subquery`), never on the enclosing
    /// query's evaluator: the fast paths below read whichever maps `self`
    /// holds, and the outer query's describe a different grid.
    fn eval_subquery_step(
        &self,
        subquery: &SubqueryExpr,
        ctx: &EvalContext,
        current_time_ms: i64,
    ) -> EvalResult<(i64, Vec<EvalSample>)> {
        let new_ctx = EvalContext {
            query_start: ctx.query_start,
            query_end: ctx.query_end,
            evaluation_ts: current_time_ms,
            lookback_delta_ms: ctx.lookback_delta_ms,
            // Inner expression evaluation for a subquery step is an instant
            // evaluation at `current_time_ms`; keep `query_start/query_end`
            // unchanged so @start()/@end() still resolve to the outer query
            // bounds. `step_ms` stays 0 for the same reason: it describes the
            // evaluation, not the grid — the grid lives in the preloaded data,
            // which carries its own `eval_start_ms`/`step_ms`.
            step_ms: 0,
        };

        // Preload-eligible against *this* evaluator's maps, which cover the
        // subquery's grid.
        let result = self.evaluate_expr(&subquery.expr, &new_ctx, true)?;

        // PromQL requires subquery inner expression to evaluate to an instant vector. Enforce this invariant at runtime.
        let ExprResult::InstantVector(samples) = result else {
            return Err(EvaluationError::InternalError(
                "subquery inner expression must return instant vector".to_string(),
            ));
        };

        Ok((current_time_ms, samples))
    }

    fn evaluate_function_args(
        &self,
        ctx: &EvalContext,
        call: &Call,
        preload_eligible: bool,
    ) -> EvalResult<Vec<PromQLArg>> {
        let args = &call.args.args;
        let mut evaluated_args = Vec::with_capacity(args.len());
        for (idx, arg) in args.iter().enumerate() {
            let (_, expected_type) = get_function_arg(call, idx)?;

            // VectorSelector subqueries take the range-based fast path inside
            // evaluate_subquery, avoiding per-step evaluation.
            let arg_result = self.evaluate_expr(arg, ctx, preload_eligible)?;

            let actual_type = arg_result.value_type();
            if actual_type != expected_type {
                // maybe this is too strict?
                return Err(EvaluationError::ArgumentError(format!(
                    "argument {idx} for function {} expected type {}, got {}",
                    call.func.name, expected_type, actual_type
                )));
            }

            evaluated_args.push(arg_result.into());
        }

        Ok(evaluated_args)
    }

    pub(super) fn evaluate_call(
        &self,
        call: &Call,
        ctx: &EvalContext,
        preload_eligible: bool,
    ) -> EvalResult<ExprResult> {
        let Some(func) = resolve_function(call.func.name) else {
            return Err(EvaluationError::InternalError(format!(
                "Unknown instant/scalar function: {}",
                call.func.name
            )));
        };

        if call.func.experimental && !self.options.enable_experimental_functions {
            return Err(EvaluationError::InternalError(format!(
                "Experimental function {} is not enabled for this request",
                call.func.name
            )));
        }

        // Ask the data source to evaluate the whole rollup where the data lives
        // (see `QueryReader::query_grid`): across a cluster that turns each
        // series' window into one float per step, instead of shipping the window
        // — which neighbouring steps would each ship again.
        //
        // For a range query that already happened, for every step at once, in
        // `preload_rollups`; this step just reads its slice. Otherwise the
        // request is made here, for this one evaluation.
        //
        // The grid read here is whichever one `self`'s maps hold: the outer
        // range query's, or — inside a subquery sub-evaluator — the subquery's
        // own. A step whose grid was not preloaded falls through to a request
        // of its own.
        let pushed_down = match preload_eligible
            .then(|| self.preloaded_rollup(call, ctx))
            .flatten()
        {
            Some(result) => Some(result),
            None => self.evaluate_pushed_down_rollup(call, ctx)?,
        };

        let mut result = match pushed_down {
            Some(result) => result,
            None => {
                let evaluated_args = self.evaluate_function_args(ctx, call, preload_eligible)?;
                // The unevaluated arguments travel with the context: `absent` and
                // `absent_over_time` take their output labels from the argument
                // selector's matchers, which no evaluated value carries.
                let call_ctx = FunctionCallContext::new(ctx, &call.args.args);
                func.apply_call(evaluated_args, &call_ctx)?
            }
        };

        if let ExprResult::InstantVector(samples) = &mut result
            && drops_metric_name(call)
        {
            for sample in samples {
                sample.drop_name = true;
            }
        }

        if call.func.return_type == ValueType::Scalar {
            return match result {
                ExprResult::Scalar(_) => Ok(result),
                ExprResult::InstantVector(samples) if samples.len() == 1 => {
                    Ok(ExprResult::Scalar(samples[0].value))
                }
                ExprResult::InstantVector(samples) => Err(EvaluationError::InternalError(format!(
                    "scalar-returning function {} must return exactly one sample, got {}",
                    call.func.name,
                    samples.len()
                ))),
                _ => Err(EvaluationError::InternalError(format!(
                    "expected a scalar for function {}, got {}",
                    call.func.name,
                    result.value_type()
                ))),
            };
        }

        Ok(result)
    }

    /// Whether `preload_for_range` has already computed step grids for this
    /// query.
    ///
    /// Filter pushdown rewrites a selector's matchers, which changes its
    /// [`PreloadKey`] — so the rewritten subtree misses the grid preloaded for
    /// it and falls back to one live query per step. That trade is only worth
    /// making when there is no grid to lose. An instant query never calls
    /// `preload_for_range`, so both maps stay empty and pushdown costs nothing.
    fn has_preloaded_data(&self) -> bool {
        !self.preloaded_instant.read().unwrap().is_empty()
            || !self.preloaded_grids.read().unwrap().is_empty()
            || !self.preloaded_matrices.read().unwrap().is_empty()
    }

    fn evaluate_binary_expr(
        &self,
        expr: &BinaryExpr,
        ctx: &EvalContext,
        preload_eligible: bool,
    ) -> EvalResult<ExprResult> {
        let lhs = expr.lhs.as_ref();
        let rhs = expr.rhs.as_ref();

        if can_push_down_common_filters(expr) && !self.has_preloaded_data() {
            return self.eval_binop_with_pushdown(ctx, expr, lhs, rhs, preload_eligible);
        }

        let (left_result, right_result) = if should_parallelize_binary_expr(expr) {
            join(
                || self.evaluate_expr(lhs, ctx, preload_eligible),
                || self.evaluate_expr(rhs, ctx, preload_eligible),
            )
        } else {
            (
                self.evaluate_expr(lhs, ctx, preload_eligible),
                self.evaluate_expr(rhs, ctx, preload_eligible),
            )
        };

        eval_binary_expr(expr, left_result?, right_result?)
    }

    /// Evaluate a binary operation one side at a time, using the labels of the
    /// first result to narrow the selectors of the second.
    ///
    /// The caller has already established via `can_push_down_common_filters`
    /// that both operands are instant vectors whose labels can produce useful
    /// filters, and that the operator prunes non-matching series.
    fn eval_binop_with_pushdown(
        &self,
        ctx: &EvalContext,
        be: &BinaryExpr,
        expr_first: &Expr,
        expr_second: &Expr,
        preload_eligible: bool,
    ) -> EvalResult<ExprResult> {
        let op = be.op.id();

        let (eval_first_expr, eval_second_expr, is_swapped_for_eval) = if op == T_LAND {
            // For `AND` we can still evaluate RHS first (often smaller) to derive
            // narrower pushdown filters for LHS, while keeping semantic LHS/RHS
            // ownership explicit and stable inside this function.
            (expr_second, expr_first, true)
        } else {
            (expr_first, expr_second, false)
        };

        // Execute the binary operation in the following way:
        //
        // 1) execute the expr_first
        // 2) get common label filters for series returned at step 1
        // 3) push down the found common label filters to expr_second. This filters out unneeded series
        //    during expr_second execution instead of spending compute resources on extracting and
        //    processing these series before they are dropped later when matching time series, according to
        //    https://prometheus.io/docs/prometheus/latest/querying/operators/#vector-matching
        // 4) execute the expr_second with possible additional filters found at step 3
        //
        // Typical use-cases:
        // - Kubernetes-related: show pod creation time with the node name:
        //
        //     kube_pod_created{namespace="prod"} * on (uid) group_left(node) kube_pod_info
        //
        //   Without the optimization `kube_pod_info` would select and spend compute resources
        //   for more time series than needed. The selected time series would be dropped later
        //   when matching time series on the right and left sides of binary operand.
        //
        // - Generic alerting queries, which rely on `info` metrics.
        //   See https://grafana.com/blog/2021/08/04/how-to-use-promql-joins-for-more-effective-queries-of-prometheus-metrics-at-scale/
        //
        // - Queries, which get additional labels from `info` metrics.
        //   See https://www.robustperception.io/exposing-the-software-version-to-prometheus
        let first = self.evaluate_expr(eval_first_expr, ctx, preload_eligible)?;

        let sec_expr = push_down_filters(be, &first, eval_second_expr)?;
        let second = self.evaluate_expr(&sec_expr, ctx, preload_eligible)?;

        // For `and`, evaluation order is intentionally swapped for optimization,
        // but final binary-op argument order must remain semantic (LHS, RHS).
        if is_swapped_for_eval {
            eval_binary_expr(be, second, first)
        } else {
            eval_binary_expr(be, first, second)
        }
    }

    fn evaluate_unary(
        &self,
        expr: &UnaryExpr,
        ctx: &EvalContext,
        preload_eligible: bool,
    ) -> EvalResult<ExprResult> {
        if let Expr::NumberLiteral(num) = &*expr.expr {
            return Ok(ExprResult::Scalar(-num.val));
        }
        let res = self.evaluate_expr(&expr.expr, ctx, preload_eligible)?;
        match res {
            ExprResult::Scalar(scalar) => Ok(ExprResult::Scalar(-scalar)),
            // Negation changes what the series measures, so `__name__` goes,
            // exactly as it does for `-1 * x`. Prometheus does the same in
            // `evalUnaryExpr`.
            //
            // Recorded rather than applied: `drop_name` is materialized once,
            // at the end of evaluation, by [`Evaluator::cleanup_metric_labels`]
            // — the same deferral rollups use, and what lets
            // `label_replace(-m, "__name__", ...)` still see the name. Applying
            // it here would also split groups that should merge, because
            // `sum by (__name__) (...)` would see one operand's name already
            // gone and the other's still present.
            ExprResult::InstantVector(mut samples) => {
                samples.iter_mut().for_each(|s| {
                    s.value = -s.value;
                    s.drop_name = true;
                });
                Ok(ExprResult::InstantVector(samples))
            }
            ExprResult::RangeVector(mut samples) => {
                samples.iter_mut().for_each(|s| {
                    s.values
                        .to_mut()
                        .iter_mut()
                        .for_each(|sample| sample.value = -sample.value);
                    s.drop_name = true;
                });
                Ok(ExprResult::RangeVector(samples))
            }
            ExprResult::String(_) => Err(EvaluationError::InternalError(
                "cannot apply unary minus to a string".to_string(),
            )),
        }
    }

    fn evaluate_aggregate(
        &self,
        aggregate: &AggregateExpr,
        ctx: &EvalContext,
        preload_eligible: bool,
    ) -> EvalResult<ExprResult> {
        // A bare selector directly under a reducing aggregation, in a range
        // query, was folded per (group, step) at the source before the step
        // loop began: this step is a slice of that grid.
        if preload_eligible && let Some(result) = self.preloaded_stepped_aggregation(aggregate, ctx)
        {
            return Ok(result);
        }

        // A rollup directly under a decomposable aggregation is pushed down as
        // one fused request: the shard reduces each series' windows and then
        // accumulates them into per-group partials, so what crosses the wire is
        // one partial per group per step rather than one value per series.
        if let Some(result) = self.evaluate_fused_rollup(aggregate, ctx, preload_eligible)? {
            return Ok(result);
        }

        // Otherwise ask the data source to evaluate the whole aggregation where
        // the data lives (see `QueryReader::query_aggregation`): across a
        // cluster that turns the input vector into one value per group per
        // shard.
        if let Some(result) =
            self.evaluate_pushed_down_aggregate(aggregate, ctx, preload_eligible)?
        {
            return Ok(result);
        }

        // Evaluate the inner expression to get all samples
        let result = self.evaluate_expr(&aggregate.expr, ctx, preload_eligible)?;

        // Extract samples from the result
        let samples = match result {
            ExprResult::InstantVector(samples) => samples,
            ExprResult::RangeVector(_) => {
                return Err(EvaluationError::InternalError(
                    "Cannot aggregate range vectors directly - use functions like rate() first"
                        .to_string(),
                ));
            }
            _ => {
                return Err(EvaluationError::InternalError(format!(
                    "Cannot aggregate {} values",
                    result.value_type()
                )));
            }
        };

        // If there are no samples, return empty result
        if samples.is_empty() {
            return Ok(ExprResult::InstantVector(vec![]));
        }

        let param = if let Some(p) = &aggregate.param {
            Some(self.evaluate_expr(p, ctx, preload_eligible)?)
        } else {
            None
        };

        // Use the evaluation_ts time as the timestamp for the aggregated result
        let timestamp_ms = ctx.evaluation_ts;

        eval_aggregation(aggregate, samples, param, timestamp_ms)
    }

    /// Try to have the data source evaluate `aggregate` itself.
    ///
    /// Returns `None` when the aggregation stays here, which is the case unless
    /// all of the following hold:
    ///
    /// * the operator has a decomposable form (everything but `quantile`);
    /// * the operand is a bare vector selector — anything else has to be
    ///   evaluated before the aggregation can see it;
    /// * the selector was not preloaded, i.e. this is not a step of a range
    ///   query whose samples were already fetched in one go;
    /// * the operator parameter is a literal that can be shipped;
    /// * and the source says it can do it (only a cluster can).
    fn evaluate_pushed_down_aggregate(
        &self,
        aggregate: &AggregateExpr,
        ctx: &EvalContext,
        preload_eligible: bool,
    ) -> EvalResult<Option<ExprResult>> {
        let kind = AggregationKind::try_from(aggregate.op)?;
        if kind.pushdown_strategy().is_none() {
            return Ok(None);
        }

        let Expr::VectorSelector(selector) = strip_parens(&aggregate.expr) else {
            return Ok(None);
        };

        if preload_eligible && self.is_preloaded(selector) {
            return Ok(None);
        }

        let param = match &aggregate.param {
            None => None,
            Some(expr) => match self.evaluate_expr(expr, ctx, preload_eligible)? {
                ExprResult::Scalar(value) => Some(AggregationParam::Scalar(value)),
                ExprResult::String(label) => Some(AggregationParam::Label(label)),
                _ => return Ok(None),
            },
        };

        let request = AggregationRequest {
            kind,
            modifier: aggregate.modifier.clone(),
            param,
            // The output carries the query's evaluation timestamp even when the
            // input is selected at a shifted one.
            eval_timestamp: ctx.evaluation_ts,
        };

        // Selection timestamp and lookback, resolved exactly as
        // `evaluate_vector_selector` resolves them for the same selector.
        let adjusted_eval_ts = apply_time_modifiers_ms(
            selector.at.as_ref(),
            selector.offset.as_ref(),
            ctx.query_start,
            ctx.query_end,
            ctx.evaluation_ts,
        );
        let mut options = self.options;
        options.lookback_delta = Duration::from_millis(ctx.lookback_delta_ms as u64);

        let outcome =
            self.reader
                .query_aggregation(selector, adjusted_eval_ts, &request, options)?;

        let samples = match outcome {
            AggregationOutcome::Unsupported => return Ok(None),
            AggregationOutcome::Aggregated(samples) => to_eval_samples(samples),
            AggregationOutcome::Raw(samples) => {
                // The source selected but did not aggregate; finish the job.
                apply_aggregation(
                    kind,
                    request.modifier.as_ref(),
                    request.param.as_ref().map(AggregationParam::to_expr_result),
                    to_eval_samples(samples),
                    ctx.evaluation_ts,
                )?
            }
        };

        Ok(Some(ExprResult::InstantVector(samples)))
    }

    /// This step's slice of a preloaded `aggregate`-over-selector grid, or
    /// `None` when the aggregation was not preloaded that way and evaluates
    /// here. The output carries no pending `__name__` drop: the selector's
    /// samples never owed one.
    fn preloaded_stepped_aggregation(
        &self,
        aggregate: &AggregateExpr,
        ctx: &EvalContext,
    ) -> Option<ExprResult> {
        let key = stepped_aggregation_key(aggregate)?;
        self.preloaded_grid_by_key(&key, ctx, false)
    }

    /// Whether this selector's samples were already fetched by
    /// [`Self::preload_for_range`].
    fn is_preloaded(&self, selector: &VectorSelector) -> bool {
        let key = PreloadKey::from_selector(selector);
        self.preloaded_instant.read().unwrap().contains_key(&key)
    }

    /// Try to have the data source evaluate a rollup *and* the aggregation over
    /// it in one request.
    ///
    /// Returns `None` when the query stays on the ordinary paths, which is the
    /// case unless the operand is a pushable rollup call and the operator has a
    /// mergeable partial state. Fusing is what turns
    /// `sum by (job) (rate(m[5m]))` into one float per job per step: without it
    /// the shard would ship one float per *series*, and a job with a thousand
    /// pods would ship a thousand.
    ///
    /// Only the reducing operators fuse. `topk` needs the individual rolled-up
    /// samples to choose among, so pushing the selection down would not shrink
    /// the response — it stays on the unfused path, where the rollup alone is
    /// still pushed down.
    fn evaluate_fused_rollup(
        &self,
        aggregate: &AggregateExpr,
        ctx: &EvalContext,
        preload_eligible: bool,
    ) -> EvalResult<Option<ExprResult>> {
        let Expr::Call(call) = strip_parens(&aggregate.expr) else {
            return Ok(None);
        };
        let Some(aggregation) = fusable_aggregation(aggregate) else {
            return Ok(None);
        };
        let Some((kind, matrix, param)) = self.pushable_rollup(call) else {
            return Ok(None);
        };

        // The group inherits the pending `__name__` drop from the rollup that
        // produced its members — the same rule `evaluate_call` applies to an
        // unfused rollup, applied here because this result never passes through
        // it. See `drops_metric_name`.
        let drop_name = drops_metric_name(call);

        let key = GridPreloadKey::rollup(
            &matrix.vs,
            kind,
            matrix_range_ms(matrix),
            param,
            Some(AggregationKey::of(&aggregation)),
        );

        // A grid resolved before the step loop answers this step from its
        // slice — the outer range query's grid, or a subquery's own when this
        // is a sub-evaluator step. The preloaded entry carries its own
        // `eval_start_ms`/`step_ms`, so which grid it is need not be
        // re-derived from `ctx` here.
        if preload_eligible && let Some(slice) = self.preloaded_grid_by_key(&key, ctx, drop_name) {
            return Ok(Some(slice));
        }

        // A range-query step that no grid covers stays local, so the
        // pushed-down and local paths cannot disagree about window geometry.
        if ctx.step_ms > 0 {
            return Ok(None);
        }

        // A single evaluation — an instant query, or a subquery step the
        // preload did not cover: one request for this evaluation.
        let range_end_ms = apply_time_modifiers_ms(
            matrix.vs.at.as_ref(),
            matrix.vs.offset.as_ref(),
            ctx.query_start,
            ctx.query_end,
            ctx.evaluation_ts,
        );
        let request = GridRequest {
            step_ms: ctx.step_ms,
            query_start: ctx.query_start,
            query_end: ctx.query_end,
            range_end_ms,
            lookback_delta_ms: ctx.lookback_delta_ms,
            rollup: Some(GridRollup {
                kind,
                range_ms: matrix_range_ms(matrix),
                param,
            }),
            aggregation: Some(aggregation),
            // A rollup's value belongs to its window, not to any one sample.
            sample_timestamps: false,
        };

        let mut options = self.options;
        options.lookback_delta = Duration::from_millis(ctx.lookback_delta_ms as u64);

        let grouped = self.finish_grid(
            &request,
            self.reader.query_grid(&matrix.vs, &request, options)?,
        )?;

        let samples = grouped
            .into_iter()
            .filter_map(|group| {
                let point = group.samples.last()?;
                Some(EvalSample {
                    timestamp_ms: ctx.evaluation_ts,
                    value: point.value,
                    labels: group.labels,
                    drop_name,
                })
            })
            .collect();

        Ok(Some(ExprResult::InstantVector(samples)))
    }

    /// Try to have the data source evaluate `call`'s rollup itself.
    ///
    /// Returns `None` when the rollup stays here, which is the case unless all
    /// of the following hold:
    ///
    /// * the function can be evaluated from one series' window alone — see
    ///   [`RollupKind`];
    /// * this is a single evaluation (`step_ms == 0`). A range query's whole
    ///   step grid is pushed in a later phase; until then its steps stay local
    ///   so that both paths cannot disagree about the grid;
    /// * the argument is a bare matrix selector. A subquery brings its own step
    ///   grid, and anything else has to be evaluated before the rollup can see
    ///   it;
    /// * the function parameter, if any, is a literal scalar that can be
    ///   shipped;
    /// * and the source says it can do it (only a cluster can).
    fn evaluate_pushed_down_rollup(
        &self,
        call: &Call,
        ctx: &EvalContext,
    ) -> EvalResult<Option<ExprResult>> {
        if ctx.step_ms != 0 {
            return Ok(None);
        }

        let Some(kind) = RollupKind::from_function_name(call.func.name) else {
            return Ok(None);
        };

        let Some((matrix, param)) = self.rollup_arguments(call, ctx)? else {
            return Ok(None);
        };
        let aggregation = None;

        // Resolve `@`/`offset` here: the source is told the window, never the
        // modifier, so it cannot resolve one differently than the local path.
        let range_end_ms = apply_time_modifiers_ms(
            matrix.vs.at.as_ref(),
            matrix.vs.offset.as_ref(),
            ctx.query_start,
            ctx.query_end,
            ctx.evaluation_ts,
        );

        let request = GridRequest {
            step_ms: ctx.step_ms,
            query_start: ctx.query_start,
            query_end: ctx.query_end,
            range_end_ms,
            lookback_delta_ms: ctx.lookback_delta_ms,
            rollup: Some(GridRollup {
                kind,
                range_ms: matrix.range.as_millis() as i64,
                param,
            }),
            aggregation,
            // A rollup's value belongs to its window, not to any one sample.
            sample_timestamps: false,
        };

        let mut options = self.options;
        options.lookback_delta = Duration::from_millis(ctx.lookback_delta_ms as u64);

        let rolled = self.finish_grid(
            &request,
            self.reader.query_grid(&matrix.vs, &request, options)?,
        )?;

        // A single evaluation yields at most one point per series, stamped with
        // the query's evaluation timestamp rather than the window end — so a
        // shifted selector still reports at the instant the client asked for.
        let samples = rolled
            .into_iter()
            .filter_map(|s| {
                let point = s.samples.last()?;
                Some(EvalSample {
                    timestamp_ms: ctx.evaluation_ts,
                    value: point.value,
                    labels: s.labels,
                    drop_name: false,
                })
            })
            .collect();

        Ok(Some(ExprResult::InstantVector(samples)))
    }

    /// The matrix selector and optional scalar parameter of a pushable rollup
    /// call, or `None` when the call's shape rules push-down out.
    pub(super) fn rollup_arguments<'a>(
        &self,
        call: &'a Call,
        ctx: &EvalContext,
    ) -> EvalResult<Option<(&'a MatrixSelector, Option<f64>)>> {
        let args: Vec<&Expr> = call.args.args.iter().map(|arg| strip_parens(arg)).collect();

        // Find the matrix argument first and give up before evaluating anything
        // if there is none: a subquery argument, or an expression that has to be
        // evaluated before the rollup can see it, keeps the whole call local and
        // the ordinary path will evaluate the arguments anyway.
        let mut matrices = args
            .iter()
            .filter(|arg| matches!(arg, Expr::MatrixSelector(_)));
        let Some(Expr::MatrixSelector(matrix)) = matrices.next() else {
            return Ok(None);
        };
        if matrices.next().is_some() {
            return Ok(None);
        }

        // The remaining argument, if any, must be a scalar that can be shipped.
        // Position is not fixed: `quantile_over_time` takes phi first, while
        // `predict_linear` takes the matrix first. Only one such argument is
        // carried, which is what the request has room for.
        let mut param = None;
        for arg in args
            .iter()
            .filter(|arg| !matches!(arg, Expr::MatrixSelector(_)))
        {
            if param.is_some() {
                return Ok(None);
            }
            match self.evaluate_expr(arg, ctx, false)? {
                ExprResult::Scalar(value) => param = Some(value),
                _ => return Ok(None),
            }
        }

        Ok(Some((matrix, param)))
    }
}

fn matrix_range_ms(matrix: &MatrixSelector) -> i64 {
    matrix.range.as_millis() as i64
}

/// The aggregation of `aggregate` as something a shard can fold a grid into,
/// or `None` when it cannot be fused.
///
/// Two conditions, both about the operator rather than the data: it must have
/// a push-down form — a mergeable partial state for the reductions, a
/// re-selectable output for `topk`/`bottomk`/`limitk`/`limit_ratio`, addable
/// counts for `count_values`; `quantile` has none — and its parameter, where
/// it takes one, must be a literal the request can carry. `topk(scalar(x), y)`
/// is evaluated here.
fn fusable_aggregation(aggregate: &AggregateExpr) -> Option<GridAggregation> {
    let kind = AggregationKind::try_from(aggregate.op).ok()?;
    kind.pushdown_strategy()?;
    let param = match aggregate.param.as_deref() {
        None => None,
        Some(Expr::NumberLiteral(n)) => Some(AggregationParam::Scalar(n.val)),
        Some(Expr::StringLiteral(s)) => Some(AggregationParam::Label(s.val.clone())),
        Some(_) => return None,
    };
    Some(GridAggregation {
        kind,
        modifier: aggregate.modifier.clone(),
        param,
    })
}

/// The grid key under which `aggregate` — a fusable aggregation directly over
/// a bare vector selector — is preloaded, or `None` when it is not that shape.
fn stepped_aggregation_key(aggregate: &AggregateExpr) -> Option<GridPreloadKey> {
    let Expr::VectorSelector(vs) = strip_parens(&aggregate.expr) else {
        return None;
    };
    let aggregation = fusable_aggregation(aggregate)?;
    Some(GridPreloadKey::stepped(
        vs,
        AggregationKey::of(&aggregation),
    ))
}

/// Scatter one entry's sparse `(window end, value)` points onto the step
/// grid.
///
/// Both are ascending, so one merge walk places every point: with `@`, every
/// step shares one window end and the cursor stays on that point; otherwise
/// the mapping is one to one.
fn scatter_onto_grid<T: Copy + Default>(
    window_ends: &[Timestamp],
    points: impl Iterator<Item = (Timestamp, T)>,
) -> StepGrid<T> {
    let mut values = StepGridBuilder::with_capacity(window_ends.len());
    let mut points = points.peekable();
    for &end in window_ends {
        while points.peek().is_some_and(|(ts, _)| *ts < end) {
            points.next();
        }
        values.push(
            points
                .peek()
                .filter(|(ts, _)| *ts == end)
                .map(|(_, value)| *value),
        );
    }
    values.finish()
}

/// Range-vector functions that report a sample of the input series unchanged,
/// and so keep `__name__`. Every other range-vector function drops it; see
/// [`drops_metric_name`].
const NAME_PRESERVING_ROLLUPS: [&str; 2] = ["first_over_time", "last_over_time"];

/// Whether `call` strips `__name__` from its output.
///
/// This is the single rule for range-vector functions, and it is deliberately
/// stated once here rather than per function: a rollup reduces a series to
/// something that is no longer that metric, so the name goes. The exceptions in
/// [`NAME_PRESERVING_ROLLUPS`] hand back one of the input samples as-is, so
/// there is nothing to rename.
///
/// The drop is *recorded*, not applied — `drop_name` is materialized once, at
/// the end of evaluation, by [`Evaluator::cleanup_metric_labels`]. Everything in
/// between still sees the name, which is what lets
/// `label_replace(rate(m[5m]), "__name__", …, "__name__", "(.+)")` recover it.
///
/// Functions over instant vectors are not covered; each already marks its own
/// output (`abs` drops, `label_replace` does not), and this rule must not
/// override them.
///
/// The same rule governs pushed-down rollups: a shard returns the label set as
/// the function leaves it, and the drop is recorded once, here, on the
/// coordinator.
fn drops_metric_name(call: &Call) -> bool {
    !NAME_PRESERVING_ROLLUPS.contains(&call.func.name)
        && call.func.arg_types.contains(&ValueType::Matrix)
}

fn to_eval_samples(samples: Vec<InstantSample<EvalLabels>>) -> Vec<EvalSample> {
    samples
        .into_iter()
        .map(|s| EvalSample {
            timestamp_ms: s.timestamp_ms,
            value: s.value,
            labels: s.labels,
            drop_name: false,
        })
        .collect()
}

fn get_function_arg(call: &Call, idx: usize) -> EvalResult<(&Expr, ValueType)> {
    // Ensure the requested argument index exists in the provided call arguments.
    if idx >= call.args.args.len() {
        return Err(EvaluationError::InternalError(format!(
            "argument {idx} is out of bounds for call to function {}",
            call.func.name
        )));
    }

    // Determine the expected type for this argument according to the function
    // declaration. Use the explicit type if available; if the function is
    // variadic, use the last declared type for additional arguments. If
    // neither applies, return an error rather than indexing out of bounds.
    let expected_type = if idx < call.func.arg_types.len() {
        call.func.arg_types[idx]
    } else if call.func.variadic != 0 && !call.func.arg_types.is_empty() {
        // Safe: last() returns Some because we checked !is_empty()
        *call.func.arg_types.last().unwrap()
    } else {
        return Err(EvaluationError::InternalError(format!(
            "argument {idx} is out of bounds for function {}",
            call.func.name
        )));
    };

    let arg = &call.args.args[idx];
    Ok((arg, expected_type))
}

/// Whether evaluating `expr` reaches storage, and so is worth its own thread.
fn is_selector(expr: &Expr) -> bool {
    match expr {
        Expr::Unary(ue) => is_selector(&ue.expr),
        Expr::Paren(pe) => is_selector(&pe.expr),
        Expr::MatrixSelector(_) => true,
        Expr::VectorSelector(_) => true,
        Expr::Call(call) => call.args.args.iter().any(|arg| is_selector(arg)),
        Expr::Binary(be) => {
            let lhs = be.lhs.as_ref();
            let rhs = be.rhs.as_ref();
            is_selector(lhs) || is_selector(rhs)
        }
        // An aggregation or subquery reads whatever its inner expression reads.
        // Without these, `sum by (job) (a) / sum by (job) (b)` — and anything
        // over a subquery — is neither parallelized here nor eligible for
        // filter pushdown, and evaluates one side after the other for nothing.
        Expr::Aggregate(agg) => is_selector(&agg.expr),
        Expr::Subquery(sq) => is_selector(&sq.expr),
        _ => false,
    }
}

fn should_parallelize_binary_expr(be: &BinaryExpr) -> bool {
    is_selector(be.lhs.as_ref()) && is_selector(be.rhs.as_ref())
}
