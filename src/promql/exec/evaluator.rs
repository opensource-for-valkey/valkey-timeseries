use super::aggregations::{
    AggregationKind, PushdownStrategy, apply_aggregation, check_aggregation_param, eval_aggregation,
};
use crate::common::Timestamp;
use crate::common::logging::log_debug;
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
use crate::promql::time::duration_ms;
use crate::promql::time::{
    MAX_GRID_STEPS, apply_time_modifiers_ms, grid_step_count, selector_bounds, step_times,
};
use crate::promql::types::{PreloadedInstantData, PreloadedInstantSeries};
use crate::promql::utils::check_subquery_steps;
use crate::promql::{
    EvalResult, EvalSample, EvalSamples, EvaluationError, ExprResult, InstantSample, PreloadMap,
    QueryError,
};
use ahash::AHashSet;
use orx_parallel::Par;
use orx_parallel::ParResult;
use promql_parser::parser::token::T_LAND;
use promql_parser::parser::value::ValueType;
use promql_parser::parser::{
    AggregateExpr, BinaryExpr, Call, EvalStmt, Expr, MatrixSelector, SubqueryExpr, UnaryExpr,
    VectorSelector,
};
use promql_parser::util::{ExprVisitor, walk_expr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;

mod preload;
mod pushdown;
mod subquery;

use pushdown::{fusable_aggregation, stepped_aggregation_key};

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
    /// Set once any preload map gains an entry; see `has_preloaded_data`.
    any_preloaded: Arc<AtomicBool>,
    /// Filled at the end of the preload phase; see [`NodeKeys`].
    node_keys: Arc<OnceLock<NodeKeys>>,
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

/// The keys the step loop looks preloaded data up by, computed once per node of
/// the expression instead of once per step.
///
/// Building a key hashes a selector's matchers and, for an aggregation, clones
/// and sorts its grouping labels. A light range query (`sum(m)` over 1,000
/// steps) spent about a tenth of its CPU doing that three or four times a step.
///
/// Keyed by node address: the expression outlives the evaluation and is never
/// changed, so an address names one live node. A node filter push-down builds
/// during evaluation is a separate allocation, never a live node's address, so
/// it misses here and computes its key as before: a miss is slower, never
/// wrong. `None` records a node that has no such key (not a pushable rollup,
/// not a fusable aggregation), so that question is answered once too.
#[derive(Default)]
pub(crate) struct NodeKeys {
    selectors: ahash::AHashMap<usize, PreloadKey>,
    rollups: ahash::AHashMap<usize, Option<GridPreloadKey>>,
    stepped: ahash::AHashMap<usize, Option<GridPreloadKey>>,
    fused: ahash::AHashMap<usize, Option<GridPreloadKey>>,
}

/// A node's address, the key of [`NodeKeys`].
fn node_addr<T>(node: &T) -> usize {
    node as *const T as usize
}

/// Errors that end the query rather than downgrade a best-effort preload: a
/// passed deadline, or a sample budget already spent (it only grows, so every
/// later read would be refused too).
fn is_query_ending(err: &EvaluationError) -> bool {
    matches!(
        err,
        EvaluationError::Query(QueryError::Timeout | QueryError::TooManySamples { .. })
    )
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
    /// Whether any preload map has an entry: one atomic load for the per-step
    /// question `has_preloaded_data` answers, where reading the four maps took
    /// four lock round-trips for every binary operator at every step.
    any_preloaded: Arc<AtomicBool>,
    /// Per-node lookup keys; see [`NodeKeys`].
    node_keys: Arc<OnceLock<NodeKeys>>,
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
            any_preloaded: prepared.any_preloaded,
            node_keys: prepared.node_keys,
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
            any_preloaded: Arc::clone(&prepared.any_preloaded),
            node_keys: Arc::clone(&prepared.node_keys),
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
            any_preloaded: self.any_preloaded,
            node_keys: self.node_keys,
        }
    }

    /// Record that a preload map has an entry.
    fn note_preloaded(&self) {
        self.any_preloaded.store(true, Ordering::Release);
    }

    /// Compute [`NodeKeys`] for every node of `expr`. Called once the preload
    /// phase is done; a second call (a second grid on one evaluator) keeps the
    /// first table, whose misses still compute.
    fn fill_node_keys(&self, expr: &Expr) {
        self.node_keys.get_or_init(|| {
            struct Collect<'e, 'r, R: QueryReader + ?Sized> {
                evaluator: &'e Evaluator<'r, R>,
                keys: NodeKeys,
            }
            impl<R: QueryReader + ?Sized> ExprVisitor for Collect<'_, '_, R> {
                type Error = std::convert::Infallible;
                fn pre_visit(&mut self, expr: &Expr) -> Result<bool, Self::Error> {
                    match expr {
                        Expr::VectorSelector(vs) => {
                            self.keys
                                .selectors
                                .insert(node_addr(vs), PreloadKey::from_selector(vs));
                        }
                        Expr::Call(call) => {
                            let key = self.evaluator.rollup_key(call);
                            self.keys.rollups.insert(node_addr(call), key);
                        }
                        Expr::Aggregate(aggregate) => {
                            let addr = node_addr(aggregate);
                            self.keys
                                .stepped
                                .insert(addr, stepped_aggregation_key(aggregate));
                            let fused = self.evaluator.fused_rollup_key(aggregate);
                            self.keys.fused.insert(addr, fused);
                        }
                        _ => {}
                    }
                    Ok(true)
                }
            }
            let mut collect = Collect {
                evaluator: self,
                keys: NodeKeys::default(),
            };
            let Ok(_) = walk_expr(&mut collect, expr);
            collect.keys
        });
    }

    /// The memoized key for `node` in the table `pick` selects: `Some(key)`
    /// or `Some(None)` when [`NodeKeys`] answered, `None` when it has no entry
    /// (not filled yet, or a node built during evaluation).
    fn memoized<'k, T>(
        &'k self,
        node: &T,
        pick: impl FnOnce(&'k NodeKeys) -> &'k ahash::AHashMap<usize, Option<GridPreloadKey>>,
    ) -> Option<Option<&'k GridPreloadKey>> {
        let keys = self.node_keys.get()?;
        pick(keys).get(&node_addr(node)).map(Option::as_ref)
    }

    /// The [`PreloadKey`] of `vs`, from [`NodeKeys`] when it has one.
    fn selector_key(&self, vs: &VectorSelector) -> PreloadKey {
        self.node_keys
            .get()
            .and_then(|keys| keys.selectors.get(&node_addr(vs)).cloned())
            .unwrap_or_else(|| PreloadKey::from_selector(vs))
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
            lookback_delta_ms: duration_ms(stmt.lookback_delta),
            step_ms: duration_ms(stmt.interval),
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
            let preload_key = self.selector_key(vector_selector);
            let guard = self.preloaded_instant.read().unwrap();
            if let Some(preloaded) = guard.get(&preload_key) {
                let evaluation_ts = ctx.evaluation_ts;
                // Step index from raw evaluation_ts (before modifiers) — matches outer step loop
                let step_idx =
                    step_index(evaluation_ts, preloaded.eval_start_ms, preloaded.step_ms);

                let mut samples = Vec::with_capacity(preloaded.series.len());
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
    ///
    /// Subquery preloads count too: they are keyed by node address, so a
    /// pushed-down copy of a subquery would miss its prepared union.
    fn has_preloaded_data(&self) -> bool {
        self.any_preloaded.load(Ordering::Acquire)
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
        // The parameter first, as Prometheus does, and checked before any path
        // is chosen: `topk(NaN, m)`, an invalid `count_values` label or a
        // failing `scalar(...)` is an error whatever the input holds. The
        // preloaded and fused paths below would otherwise answer an empty
        // input without ever looking at it.
        let kind = AggregationKind::try_from(aggregate.op)?;
        let param = match &aggregate.param {
            Some(p) => Some(self.evaluate_expr(p, ctx, preload_eligible)?),
            None => None,
        };
        check_aggregation_param(kind, param.as_ref())?;

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
            self.evaluate_pushed_down_aggregate(aggregate, param.as_ref(), ctx, preload_eligible)?
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

        // No early return for an empty input: `apply_aggregation` checks the
        // parameter first, and the pushed-down path reaches the same checks on
        // each shard, so a single node and a cluster answer alike.
        // Use the evaluation_ts time as the timestamp for the aggregated result
        let timestamp_ms = ctx.evaluation_ts;

        eval_aggregation(aggregate, samples, param, timestamp_ms)
    }
}

fn matrix_range_ms(matrix: &MatrixSelector) -> i64 {
    duration_ms(matrix.range)
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

/// The index of the step at `ts` on a grid starting at `start` with `step`, without
/// overflow. A time before the grid (or a non-positive step) maps to `usize::MAX`, which no
/// series holds, as the plain cast of a negative difference did.
fn step_index(ts: Timestamp, start: Timestamp, step: i64) -> usize {
    if ts < start || step <= 0 {
        return usize::MAX;
    }
    let idx = (i128::from(ts) - i128::from(start)) / i128::from(step);
    usize::try_from(idx).unwrap_or(usize::MAX)
}

#[cfg(test)]
mod step_index_tests {
    use super::step_index;

    #[test]
    fn step_index_handles_the_whole_timestamp_range() {
        assert_eq!(step_index(10, 0, 5), 2);
        assert_eq!(step_index(0, 0, 5), 0);
        assert_eq!(step_index(-1, 0, 5), usize::MAX);
        assert_eq!(step_index(10, 0, 0), usize::MAX);
        // The plain difference overflowed i64 here.
        assert_eq!(step_index(i64::MAX, i64::MIN, 1), usize::MAX);
        assert_eq!(step_index(i64::MAX, i64::MIN, i64::MAX), 2);
    }
}
