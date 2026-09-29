//! Subqueries: per-step evaluation, the vector-selector fast path, and the keys their
//! union preload is cached under.

use super::*;

impl<'reader, R: QueryReader + ?Sized> Evaluator<'reader, R> {
    pub(in crate::promql::exec) fn evaluate_subquery(
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
        let range_ms = duration_ms(subquery.range);
        let subquery_start_ms = subquery_end_ms.saturating_sub(range_ms);

        let step_ms = subquery_step_ms(subquery);

        // Guard against invalid step
        if step_ms <= 0 {
            return Err(EvaluationError::InternalError(
                "subquery step must be > 0".to_string(),
            ));
        }
        // Before anything walks or sizes the grid; both paths below do.
        check_subquery_steps(subquery_start_ms, subquery_end_ms, step_ms)?;

        // The union of every outer step's grid, prepared up front for a range
        // query (`preload_subqueries`); see the per-step preload below.
        let union = self
            .preloaded_subqueries
            .read()
            .unwrap()
            .get(&subquery_key(subquery, step_ms))
            .cloned();

        // Fast path: if inner expression is a pure VectorSelector, evaluate over range once.
        //
        // Only without a prepared union: that one read already covers every
        // outer step's window, where this path would re-read a whole window
        // per outer step.
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
        if union.is_none()
            && let Expr::VectorSelector(ref selector) = *subquery.expr
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
        let sub = match union {
            Some(prepared) => Evaluator::with_shared(self.reader, self.options, prepared),
            None => {
                let grid =
                    PreloadGrid::for_subquery(aligned_start_ms, subquery_end_ms, step_ms, ctx);
                let sub_plan = PlannedQuery::for_grid(&subquery.expr, grid);
                let prepared = match Preloader::sharing(
                    self.reader,
                    self.options,
                    Arc::clone(&self.budget),
                )
                .prepare(sub_plan)
                {
                    Ok(prepared) => prepared,
                    Err(err) => {
                        // A deadline or a spent sample budget means the query is
                        // over: the budget only grows, so per-step reads would
                        // be refused one by one.
                        if is_query_ending(&err) {
                            return Err(err);
                        }
                        // Otherwise best-effort, on the same rule as the matrix preload: the per-step path below
                        // reproduces the unpreloaded behavior exactly, so a preload that trips a reader limit
                        // downgrades the subquery to per-step reads rather than failing a query that used to succeed.
                        log_debug(format!(
                            "subquery preload failed; falling back to per-step evaluation: {err}"
                        ));
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
                    .into_fallible()
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
    pub(super) fn evaluate_subquery_vector_selector(
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

    /// Evaluate the subquery's inner expression at one of its steps.
    ///
    /// Must be called on the sub-evaluator whose maps were preloaded for this
    /// subquery's grid (see `evaluate_subquery`), never on the enclosing
    /// query's evaluator: the fast paths below read whichever maps `self`
    /// holds, and the outer query's describe a different grid.
    pub(super) fn eval_subquery_step(
        &self,
        subquery: &SubqueryExpr,
        ctx: &EvalContext,
        current_time_ms: i64,
    ) -> EvalResult<(i64, Vec<EvalSample>)> {
        // Once preloaded, stepping a subquery is pure CPU: no reader call or
        // preload request is left to notice the deadline, and nested subqueries
        // multiply their steps level by level. Without this check a passed
        // deadline was only seen after the whole tree had been evaluated.
        self.check_deadline()?;

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
}

pub(super) fn subquery_key(subquery: &SubqueryExpr, step_ms: i64) -> (usize, i64) {
    (subquery as *const SubqueryExpr as usize, step_ms)
}
