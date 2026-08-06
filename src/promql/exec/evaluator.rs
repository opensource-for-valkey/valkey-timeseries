use super::aggregations::{AggregationKind, PushdownStrategy, apply_aggregation, eval_aggregation};
use crate::common::threads::join;
use crate::common::time::{current_time_millis, system_time_to_millis};
use crate::common::{Sample, Timestamp};
use crate::labels::Labels;
use crate::promql::binops::{
    can_push_down_common_filters, ensure_unique_labelsets, eval_binary_expr, push_down_filters,
};
use crate::promql::engine::query_reader::{
    AggregationOutcome, AggregationParam, AggregationRequest, RollupAggregation, RollupOutcome,
    RollupRequest,
};
use crate::promql::engine::{QueryOptions, QueryReader};
use crate::promql::exec::pipeline::{
    QueryPlan, compute_subquery_alignment, execute_selector_pipeline, for_each_step_sample,
};
use crate::promql::exec::types::{
    EvalLabels, MatrixPreloadMap, PreloadedMatrixData, PreloadedMatrixSeries, PreloadedRollupData,
    PreloadedRollupSeries, RollupPreloadMap, SeriesMap, StepGridBuilder,
};
use crate::promql::exec::utils::{
    RollupCandidate, collect_rollup_candidates, collect_vector_selectors,
    merge_step_into_series_map, strip_parens,
};
use crate::promql::functions::RollupKind;
use crate::promql::functions::{
    FunctionCallContext, PromQLArg, PromQLFunction, resolve_function, window_samples,
};
use crate::promql::hashers::{AggregationKey, MatrixPreloadKey, PreloadKey, RollupPreloadKey};
use crate::promql::model::EvalContext;
use crate::promql::time::{apply_time_modifiers_ms, selector_bounds, step_times};
use crate::promql::types::{PreloadedInstantData, PreloadedInstantSeries};
use crate::promql::{
    EvalResult, EvalSample, EvalSamples, EvaluationError, ExprResult, InstantSample, PreloadMap,
    QueryError,
};
use ahash::AHashSet;
use orx_parallel::ParallelizableCollection;
use orx_parallel::{IntoParIter, ParIterResult};
use orx_parallel::{IterIntoParIter, ParIter};
use promql_parser::parser::token::T_LAND;
use promql_parser::parser::value::ValueType;
use promql_parser::parser::{
    AggregateExpr, BinaryExpr, Call, EvalStmt, Expr, MatrixSelector, SubqueryExpr, UnaryExpr,
    VectorSelector,
};
use std::sync::RwLock;
use std::time::Duration;

/// How many preload requests may be in flight at once.
///
/// Each preload request is a blocking cluster fanout (or a whole-span read on
/// a single node), so unbounded parallelism would multiply concurrent fanouts
/// and peak memory. Mirrors the batch size of the selector executor.
const MAX_CONCURRENT_PRELOAD_REQUESTS: usize = 4;

/// The step grid a preload phase fills, plus the bounds `@ start()`/`@ end()`
/// resolve against.
///
/// [`EvalContext`] conflates the two: for a range query the grid *is*
/// `query_start..query_end`, and those same bounds are what `@ start()` and
/// `@ end()` mean. A subquery breaks that identity — it evaluates its inner
/// expression over its own aligned resolution, while `@ start()`/`@ end()`
/// inside it still refer to the **outer** query's bounds (PromQL resolves
/// them against the enclosing query, never the subquery). Preloading a
/// subquery's grid therefore needs both, kept apart.
#[derive(Clone, Copy, Debug)]
struct PreloadGrid {
    /// First step of the grid.
    start_ms: Timestamp,
    /// Last step of the grid; the grid is `start_ms..=end_ms` by `step_ms`.
    end_ms: Timestamp,
    step_ms: i64,
    /// What `@ start()` resolves to — the outer query's start, not the grid's.
    at_start_ms: Timestamp,
    /// What `@ end()` resolves to — the outer query's end, not the grid's.
    at_end_ms: Timestamp,
    lookback_delta_ms: i64,
}

impl PreloadGrid {
    /// The grid of an outer range query, where the two sets of bounds coincide.
    fn for_range(ctx: &EvalContext) -> Self {
        Self {
            start_ms: ctx.query_start,
            end_ms: ctx.query_end,
            step_ms: ctx.step_ms,
            at_start_ms: ctx.query_start,
            at_end_ms: ctx.query_end,
            lookback_delta_ms: ctx.lookback_delta_ms,
        }
    }

    /// The grid of a subquery: its own aligned resolution, but `@`-modifier
    /// bounds inherited from the enclosing query, exactly as
    /// `eval_subquery_step` leaves `query_start`/`query_end` untouched so the
    /// per-step fallback path resolves them the same way.
    fn for_subquery(
        aligned_start_ms: Timestamp,
        end_ms: Timestamp,
        step_ms: i64,
        ctx: &EvalContext,
    ) -> Self {
        Self {
            start_ms: aligned_start_ms,
            end_ms,
            step_ms,
            at_start_ms: ctx.query_start,
            at_end_ms: ctx.query_end,
            lookback_delta_ms: ctx.lookback_delta_ms,
        }
    }

    fn steps(&self) -> impl Iterator<Item = Timestamp> {
        step_times(self.start_ms, self.end_ms, self.step_ms)
    }

    fn expected_steps(&self) -> usize {
        if self.step_ms <= 0 {
            return 0;
        }
        ((self.end_ms - self.start_ms) / self.step_ms) as usize + 1
    }
}

pub(crate) struct Evaluator<'reader, R: QueryReader> {
    reader: &'reader R,
    /// Preloaded per-step instant vector data for range queries.
    /// Populated by preload_for_range() before the step loop.
    preloaded_instant: RwLock<PreloadMap>,
    /// Rollups whose whole step grid was evaluated at the source in one request.
    /// Populated by preload_rollups() before the step loop.
    preloaded_rollups: RwLock<RollupPreloadMap>,
    /// Raw spans for matrix selectors that no rollup grid covers, so the step
    /// loop slices windows locally instead of re-fetching them per step.
    /// Populated by preload_matrices() before the step loop.
    preloaded_matrices: RwLock<MatrixPreloadMap>,
    options: QueryOptions,
}

impl<'reader, R: QueryReader> Evaluator<'reader, R> {
    pub(crate) fn new(reader: &'reader R, options: QueryOptions) -> Self {
        Self {
            reader,
            preloaded_instant: RwLock::new(PreloadMap::default()),
            preloaded_rollups: RwLock::new(RollupPreloadMap::default()),
            preloaded_matrices: RwLock::new(MatrixPreloadMap::default()),
            options,
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
    fn preload_grid(&self, expr: &Expr, grid: &PreloadGrid) -> EvalResult<()> {
        // Deduplicate by PreloadKey, then parallelize the loading
        let mut seen = AHashSet::new();
        let unique_selectors: Vec<_> = collect_vector_selectors(expr)
            .into_iter()
            .filter(|&vs| seen.insert(PreloadKey::from_selector(vs)))
            .collect();

        let _: Vec<()> = unique_selectors
            .par()
            .map(|&vs| self.preload_vector_selector(vs, grid))
            .into_fallible_result()
            .collect()?;

        self.preload_rollups(expr, grid)?;
        self.preload_matrices(expr, grid)?;

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
            let key = RollupPreloadKey::new(
                &matrix.vs,
                kind,
                matrix_range_ms(matrix),
                param,
                aggregation
                    .as_ref()
                    .map(|agg| AggregationKey::new(agg.kind, agg.modifier.as_ref())),
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
            .into_par()
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
    /// This is the fallback grid for rollups that cannot be pushed down —
    /// `query_rollup` unsupported or disabled, a function outside
    /// [`RollupKind`], a non-literal parameter. Without it every step
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

        let rollups = self.preloaded_rollups.read().unwrap();
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
                let key = RollupPreloadKey::new(
                    &matrix.vs,
                    kind,
                    matrix_range_ms(matrix),
                    param,
                    aggregation
                        .as_ref()
                        .map(|agg| AggregationKey::new(agg.kind, agg.modifier.as_ref())),
                );
                if rollups.contains_key(&key) {
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
        drop(rollups);

        let _: Vec<()> = targets
            .into_par()
            .num_threads(MAX_CONCURRENT_PRELOAD_REQUESTS)
            .map(|(key, matrix)| -> EvalResult<()> {
                self.check_deadline()?;
                self.preload_matrix(key, matrix, grid);
                Ok(())
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
    fn preload_matrix(&self, key: MatrixPreloadKey, matrix: &MatrixSelector, grid: &PreloadGrid) {
        let window_ends = self.resolved_window_ends(&matrix.vs, grid);
        let (Some(&first), Some(&last)) = (window_ends.first(), window_ends.last()) else {
            return;
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
                let series = series
                    .into_iter()
                    .map(|s| PreloadedMatrixSeries {
                        labels: EvalLabels::from(s.labels),
                        samples: s.samples,
                    })
                    .collect();
                self.preloaded_matrices
                    .write()
                    .unwrap()
                    .insert(key, PreloadedMatrixData { series });
            }
            Err(err) => {
                tracing::debug!(
                    error = %err,
                    "matrix preload failed; falling back to per-step windows"
                );
            }
        }
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
        key: RollupPreloadKey,
        kind: RollupKind,
        matrix: &MatrixSelector,
        param: Option<f64>,
        aggregation: Option<RollupAggregation>,
        grid: &PreloadGrid,
    ) -> EvalResult<()> {
        // Resolve `@`/`offset` here, per step. The source is told window ends
        // and never a modifier, so it cannot resolve one differently than the
        // local path would.
        let window_ends = self.resolved_window_ends(&matrix.vs, grid);
        let (Some(&first), Some(&last)) = (window_ends.first(), window_ends.last()) else {
            return Ok(());
        };

        let request = RollupRequest {
            kind,
            aggregation,
            range_ms: matrix_range_ms(matrix),
            lookback_delta_ms: grid.lookback_delta_ms,
            step_ms: grid.step_ms,
            query_start: first,
            query_end: last,
            range_end_ms: last,
            param,
        };

        // The request describes its windows as a start/end/step progression;
        // `@` collapses every step onto one window end, and `offset` shifts them
        // uniformly. Verify the progression the source will derive is exactly
        // the set of ends resolved above rather than trusting that every
        // modifier shape reduces to one — an unanticipated one stays local
        // instead of silently answering for the wrong windows.
        let mut resolved = window_ends.clone();
        resolved.dedup();
        if request.window_ends() != resolved {
            return Ok(());
        }

        let mut options = self.options;
        options.lookback_delta = Duration::from_millis(grid.lookback_delta_ms as u64);

        let rolled = match self.reader.query_rollup(&matrix.vs, &request, options)? {
            RollupOutcome::Unsupported => return Ok(()),
            RollupOutcome::Rolled(series) => series,
            RollupOutcome::Reduced(series) => request.group(series),
            RollupOutcome::Raw(series) => request.reduce_and_group(series),
        };

        // Scatter each series' sparse `(window end, value)` pairs onto the step
        // grid. With `@`, every step shares one window end and therefore one
        // value; otherwise the mapping is one to one.
        let series = rolled
            .into_iter()
            .map(|s| {
                let points: ahash::AHashMap<Timestamp, f64> = s
                    .samples
                    .iter()
                    .map(|point| (point.timestamp, point.value))
                    .collect();
                let mut values = StepGridBuilder::with_capacity(window_ends.len());
                for end in &window_ends {
                    values.push(points.get(end).copied());
                }
                let values = values.finish();
                PreloadedRollupSeries {
                    labels: EvalLabels::from(s.labels),
                    values,
                }
            })
            .collect();

        self.preloaded_rollups.write().unwrap().insert(
            key,
            PreloadedRollupData {
                eval_start_ms: grid.start_ms,
                step_ms: grid.step_ms,
                series,
            },
        );

        Ok(())
    }

    /// This step's slice of a preloaded rollup, or `None` when the call was not
    /// preloaded and has to be evaluated here.
    fn preloaded_rollup(&self, call: &Call, ctx: &EvalContext) -> Option<ExprResult> {
        let (kind, matrix, param) = self.pushable_rollup(call)?;
        let key = RollupPreloadKey::new(&matrix.vs, kind, matrix_range_ms(matrix), param, None);
        self.preloaded_rollup_by_key(&key, ctx, false)
    }

    /// This step's slice of a preloaded rollup, keyed explicitly so the fused
    /// form — whose entries are groups rather than series — can share it.
    fn preloaded_rollup_by_key(
        &self,
        key: &RollupPreloadKey,
        ctx: &EvalContext,
        drop_name: bool,
    ) -> Option<ExprResult> {
        let guard = self.preloaded_rollups.read().unwrap();
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

    fn preload_vector_selector(&self, vs: &VectorSelector, grid: &PreloadGrid) -> EvalResult<()> {
        let eval_start_ms = grid.start_ms;
        let eval_end_ms = grid.end_ms;
        let step_ms = grid.step_ms;
        let lookback_delta_ms = grid.lookback_delta_ms;

        // Compute fetch range via selector_bounds. The `at_*` pair is what
        // `@ start()`/`@ end()` mean and the `eval_*` pair is the grid being
        // covered; they differ for a subquery grid, whose `@` modifiers still
        // refer to the enclosing query.
        let (earliest_ms, latest_ms) = selector_bounds(
            vs.at.as_ref(),
            vs.offset.as_ref(),
            grid.at_start_ms,
            grid.at_end_ms,
            eval_start_ms,
            eval_end_ms,
            lookback_delta_ms,
        );

        // Fetch all series + samples for the full time range
        let series_samples = self.fetch_series_samples(vs, earliest_ms, latest_ms)?;

        let num_steps = grid.expected_steps();

        // Clone the time-modifier options so they can be captured across parallel tasks.
        // AtModifier and Offset are small Copy-like enums; cloning is cheap.
        let at_modifier = vs.at.clone();
        let offset_mod = vs.offset.clone();
        let at_start_ms = grid.at_start_ms;
        let at_end_ms = grid.at_end_ms;

        // ── Per-series step-bucketing ─────────────────
        let preloaded_series: Vec<PreloadedInstantSeries> = series_samples
            .into_par()
            .map(|(labels, samples)| {
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

                PreloadedInstantSeries {
                    labels: labels.into(),
                    values,
                }
            })
            .collect();

        self.cache_preloaded_series(vs, eval_start_ms, step_ms, preloaded_series);

        Ok(())
    }

    /// Fetch raw per-series samples for the given selector and time window.
    /// Returns one `(Labels, Vec<Sample>)` entry per matching series, with
    /// samples sorted ascending by timestamp (as guaranteed by `query_range`).
    fn fetch_series_samples(
        &self,
        vs: &VectorSelector,
        earliest_ms: i64,
        latest_ms: i64,
    ) -> EvalResult<Vec<(Labels, Vec<Sample>)>> {
        let range_samples = self
            .reader
            .query_range(vs, earliest_ms, latest_ms, self.options)?;
        Ok(range_samples
            .into_iter()
            .map(|rs| (rs.labels, rs.samples))
            .collect())
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
                    if v.drop_name {
                        v.labels.drop_name();
                    }
                }
            }
            ExprResult::InstantVector(vec) => {
                for v in vec.iter_mut() {
                    if v.drop_name {
                        v.labels.drop_name();
                    }
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
                        let window = window_samples(&s.samples, adjusted_eval_ts, range_ms)?;
                        Some(EvalSamples {
                            labels: s.labels.clone(),
                            drop_name: false,
                            range_ms,
                            values: window.to_vec(),
                            range_end_ms: adjusted_eval_ts,
                        })
                    })
                    .collect();
                return Ok(ExprResult::RangeVector(series));
            }
        }

        let plan = QueryPlan::for_matrix(adjusted_eval_ts, range_ms);

        execute_selector_pipeline(self.reader, &plan, vector_selector, self.options)
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

        // Subquery step resolution fallback per PromQL spec:
        // "<resolution> is optional. Default is the global evaluation interval."
        // See: https://prometheus.io/docs/prometheus/latest/querying/basics/#subquery
        let step_ms = if let Some(s) = subquery.step {
            s.as_millis() as i64
        } else if ctx.step_ms > 0 {
            ctx.step_ms
        } else {
            // See: https://github.com/prometheus/prometheus/blob/main/config/config.go#L169
            // DefaultGlobalConfig.EvaluationInterval = 1 * time.Minute
            60_000
        };

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

        let steps = step_times(aligned_start_ms, subquery_end_ms, step_ms);
        const PARALLEL_SUBQUERY_STEP_THRESHOLD: usize = 4;

        // Preload the subquery's *own* grid, in a sub-evaluator that owns the
        // maps.
        //
        // Without this each inner step reads live: an inner rollup issues a
        // `query_rollup` per inner step and an inner selector a `query` per
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
        let sub = Evaluator::new(self.reader, self.options);
        let grid = PreloadGrid::for_subquery(aligned_start_ms, subquery_end_ms, step_ms, ctx);
        if let Err(err) = sub.preload_grid(&subquery.expr, &grid) {
            // A deadline means the query is over; more work cannot help.
            if matches!(err, EvaluationError::Query(QueryError::Timeout)) {
                return Err(err);
            }
            // Otherwise best-effort, on the same rule as the matrix preload: the per-step path below
            // reproduces the unpreloaded  behavior exactly, so a preload that trips a reader limit
            // downgrades the subquery to per-step reads rather than failing a
            // query that used to succeed.
            tracing::debug!(
                error = %err,
                "subquery preload failed; falling back to per-step evaluation"
            );
        }

        // Evaluate the inner expression at each step within the subquery range.
        // orx-parallel's `collect` preserves input order, so `step_results` is in
        // ascending step-timestamp order and per-series sample appends below
        // produce chronologically sorted vectors without an extra sort.
        let step_results: Vec<(i64, Vec<EvalSample>)> =
            if expected_steps < PARALLEL_SUBQUERY_STEP_THRESHOLD {
                let mut results = Vec::with_capacity(expected_steps);
                for current_time_ms in steps {
                    let res = sub.eval_subquery_step(subquery, ctx, current_time_ms)?;
                    results.push(res);
                }
                results
            } else {
                steps
                    .iter_into_par()
                    .map(|eval_ts| sub.eval_subquery_step(subquery, ctx, eval_ts))
                    .into_fallible_result()
                    .collect()?
            };

        let mut series_map = SeriesMap::default();
        for (current_time_ms, samples) in step_results {
            merge_step_into_series_map(&mut series_map, current_time_ms, samples);
        }

        let vector = series_map
            .into_iter()
            .map(|(labels, values)| EvalSamples {
                values,
                labels,
                range_ms,
                range_end_ms: subquery_end_ms,
                drop_name: false,
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
        execute_selector_pipeline(self.reader, &plan, vector_selector, self.options)
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

        execute_selector_pipeline(self.reader, &plan, vector_selector, self.options)
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
        // (see `QueryReader::query_rollup`): across a cluster that turns each
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
            || !self.preloaded_rollups.read().unwrap().is_empty()
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
            ExprResult::InstantVector(mut samples) => {
                samples.iter_mut().for_each(|s| s.value = -s.value);
                Ok(ExprResult::InstantVector(samples))
            }
            ExprResult::RangeVector(mut samples) => {
                samples.iter_mut().for_each(|s| {
                    s.values
                        .iter_mut()
                        .for_each(|sample| sample.value = -sample.value);
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

        let key = RollupPreloadKey::new(
            &matrix.vs,
            kind,
            matrix_range_ms(matrix),
            param,
            Some(AggregationKey::new(
                aggregation.kind,
                aggregation.modifier.as_ref(),
            )),
        );

        // A grid resolved before the step loop answers this step from its
        // slice — the outer range query's grid, or a subquery's own when this
        // is a sub-evaluator step. The preloaded entry carries its own
        // `eval_start_ms`/`step_ms`, so which grid it is need not be
        // re-derived from `ctx` here.
        if preload_eligible && let Some(slice) = self.preloaded_rollup_by_key(&key, ctx, drop_name)
        {
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
        let request = RollupRequest {
            kind,
            aggregation: Some(aggregation),
            range_ms: matrix_range_ms(matrix),
            lookback_delta_ms: ctx.lookback_delta_ms,
            step_ms: ctx.step_ms,
            query_start: ctx.query_start,
            query_end: ctx.query_end,
            range_end_ms,
            param,
        };

        let mut options = self.options;
        options.lookback_delta = Duration::from_millis(ctx.lookback_delta_ms as u64);

        let grouped = match self.reader.query_rollup(&matrix.vs, &request, options)? {
            RollupOutcome::Unsupported => return Ok(None),
            RollupOutcome::Rolled(groups) => groups,
            // Each of these did less than was asked; make up exactly the
            // difference, with the same kernels a shard would have used.
            RollupOutcome::Reduced(series) => request.group(series),
            RollupOutcome::Raw(series) => request.reduce_and_group(series),
        };

        let samples = grouped
            .into_iter()
            .filter_map(|group| {
                let point = group.samples.last()?;
                Some(EvalSample {
                    timestamp_ms: ctx.evaluation_ts,
                    value: point.value,
                    labels: EvalLabels::from(group.labels),
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

        let request = RollupRequest {
            kind,
            aggregation,
            range_ms: matrix.range.as_millis() as i64,
            lookback_delta_ms: ctx.lookback_delta_ms,
            step_ms: ctx.step_ms,
            query_start: ctx.query_start,
            query_end: ctx.query_end,
            range_end_ms,
            param,
        };

        let mut options = self.options;
        options.lookback_delta = Duration::from_millis(ctx.lookback_delta_ms as u64);

        let rolled = match self.reader.query_rollup(&matrix.vs, &request, options)? {
            RollupOutcome::Unsupported => return Ok(None),
            // No aggregation was requested, so `Reduced` and `Rolled` say the
            // same thing here.
            RollupOutcome::Rolled(series) | RollupOutcome::Reduced(series) => series,
            // The source read the windows but did not reduce them; finish the
            // job, with the same kernel a shard would have used.
            RollupOutcome::Raw(series) => request.reduce_and_group(series),
        };

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
                    labels: EvalLabels::from(s.labels),
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

/// The aggregation of `aggregate` as something a shard can fold a rollup into,
/// or `None` when it cannot be fused.
///
/// Two conditions, both about the operator rather than the data: it must have a
/// mergeable partial state (the reductions do; `topk` and `count_values` do
/// not), and it must take no parameter — every operator that takes one is in the
/// group that has no partial state anyway, so a parameter here means the shape
/// is not fusable.
fn fusable_aggregation(aggregate: &AggregateExpr) -> Option<RollupAggregation> {
    if aggregate.param.is_some() {
        return None;
    }
    let kind = AggregationKind::try_from(aggregate.op).ok()?;
    if kind.pushdown_strategy() != Some(PushdownStrategy::Reduce) {
        return None;
    }
    Some(RollupAggregation {
        kind,
        modifier: aggregate.modifier.clone(),
    })
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

fn to_eval_samples(samples: Vec<InstantSample>) -> Vec<EvalSample> {
    samples
        .into_iter()
        .map(|s| EvalSample {
            timestamp_ms: s.timestamp_ms,
            value: s.value,
            labels: EvalLabels::from(s.labels),
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
