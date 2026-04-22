use super::aggregations::eval_aggregation;
use crate::common::threads::join;
use crate::common::time::system_time_to_millis;
use crate::common::{Sample, Timestamp};
use crate::promql::binops::eval_binary_expr;
use crate::promql::engine::{CachedQueryReader, QueryOptions, QueryReader};
use crate::promql::exec::pipeline::{QueryPlan, execute_selector_pipeline};
use crate::promql::exec::utils::collect_vector_selectors;
use crate::promql::functions::PromQLFunction;
use crate::promql::functions::{PromQLArg, resolve_function};
use crate::promql::hashers::PreloadKey;
use crate::promql::model::EvalContext;
use crate::promql::time::{apply_time_modifiers_ms, selector_bounds};
use crate::promql::types::{PreloadedInstantData, PreloadedInstantSeries};
use crate::promql::{
    EvalResult, EvalSample, EvalSamples, EvaluationError, ExprResult, Labels, PreloadMap,
};
use ahash::{AHashSet, RandomState};
use orx_parallel::ParIter;
use orx_parallel::ParallelizableCollection;
use orx_parallel::{IntoParIter, ParIterResult};
use promql_parser::label::METRIC_NAME;
use promql_parser::parser::value::ValueType;
use promql_parser::parser::{
    AggregateExpr, AtModifier, BinaryExpr, Call, EvalStmt, Expr, MatrixSelector, Offset,
    SubqueryExpr, UnaryExpr, VectorSelector,
};
use std::sync::RwLock;
use std::time::Duration;

pub(crate) struct Evaluator<'reader, R: QueryReader> {
    reader: CachedQueryReader<'reader, R>,
    /// Preloaded per-step instant vector data for range queries.
    /// Populated by preload_for_range() before the step loop.
    preloaded_instant: RwLock<PreloadMap>,
    options: QueryOptions,
}

impl<'reader, R: QueryReader> Evaluator<'reader, R> {
    pub(crate) fn new(reader: &'reader R, options: QueryOptions) -> Self {
        Self {
            reader: CachedQueryReader::new(reader),
            preloaded_instant: RwLock::new(PreloadMap::default()),
            options,
        }
    }

    /// Preload VectorSelector data for all steps of a range query.
    /// Must be called before the step loop. Walks the AST, deduplicates selectors,
    /// and builds dense per-step sample arrays for O(1) per-step lookup.
    pub(in crate::promql) fn preload_for_range(
        &self,
        expr: &Expr,
        ctx: &EvalContext,
    ) -> EvalResult<()> {
        let selectors = collect_vector_selectors(expr);
        // Deduplicate by PreloadKey, then parallelize the loading
        let mut seen = AHashSet::new();
        let unique_selectors: Vec<_> = selectors
            .into_iter()
            .filter(|&vs| seen.insert(PreloadKey::from_selector(vs)))
            .collect();

        let _: Vec<()> = unique_selectors
            .par()
            .map(|&vs| self.preload_vector_selector(vs, ctx))
            .into_fallible_result()
            .collect()?;

        Ok(())
    }

    /// Convenience wrapper that builds an [`EvalContext`] from a full [`EvalStmt`]
    /// so callers outside the `exec` module don't need to construct it manually.
    pub(in crate::promql) fn preload_for_range_from_stmt(&self, stmt: &EvalStmt) -> EvalResult<()> {
        let ctx = EvalContext::from(stmt);
        self.preload_for_range(&stmt.expr, &ctx)
    }

    fn preload_vector_selector(&self, vs: &VectorSelector, ctx: &EvalContext) -> EvalResult<()> {
        let eval_start_ms = ctx.query_start;
        let eval_end_ms = ctx.query_end;
        let step_ms = ctx.step_ms;
        let lookback_delta_ms = ctx.lookback_delta_ms;

        // Compute fetch range via selector_bounds
        let (earliest_ms, latest_ms) = selector_bounds(
            vs.at.as_ref(),
            vs.offset.as_ref(),
            eval_start_ms,
            eval_end_ms,
            eval_start_ms,
            eval_end_ms,
            lookback_delta_ms,
        );

        // Fetch all series + samples for the full time range
        let series_samples = self.fetch_series_samples(vs, earliest_ms, latest_ms)?;

        let num_steps = ctx.expected_steps();

        // Clone the time-modifier options so they can be captured across parallel tasks.
        // AtModifier and Offset are small Copy-like enums; cloning is cheap.
        let at_modifier = vs.at.clone();
        let offset_mod = vs.offset.clone();

        // ── Per-series step-bucketing ─────────────────
        let preloaded_series: Vec<PreloadedInstantSeries> = series_samples
            .into_par()
            .map(|(labels, samples)| {
                let mut values = Vec::with_capacity(num_steps);
                let mut i = 0usize;
                let mut last_valid: Option<&Sample> = None;

                for step_idx in 0..num_steps {
                    let eval_ts_i = eval_start_ms + (step_idx as i64) * step_ms;

                    // Per-step instant stmt sets query_start = query_end = eval_ts
                    let adjusted_ts = apply_time_modifiers_ms(
                        at_modifier.as_ref(),
                        offset_mod.as_ref(),
                        eval_ts_i,
                        eval_ts_i,
                        eval_ts_i,
                    );
                    let lookback_start = adjusted_ts - lookback_delta_ms;

                    while i < samples.len() && samples[i].timestamp <= adjusted_ts {
                        last_valid = Some(&samples[i]);
                        i += 1;
                    }

                    if let Some(sample) = last_valid {
                        if sample.timestamp > lookback_start {
                            values.push(Some(Sample {
                                timestamp: sample.timestamp,
                                value: sample.value,
                            }));
                        } else {
                            values.push(None);
                        }
                    } else {
                        values.push(None);
                    }
                }

                PreloadedInstantSeries { labels, values }
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

        // Convert SystemTime to Timestamp at entry point
        let query_start = system_time_to_millis(stmt.start);
        let query_end = system_time_to_millis(stmt.end);
        let evaluation_ts = query_end; // using end follows the "as-of" convention
        let interval_ms = stmt.interval.as_millis() as i64;
        let lookback_delta_ms = stmt.lookback_delta.as_millis() as i64;

        let ctx = EvalContext {
            query_start,
            query_end,
            evaluation_ts,
            lookback_delta_ms,
            step_ms: interval_ms,
        };

        let mut result = self.evaluate_expr(&stmt.expr, &ctx, true)?;

        // Deferred __name__ cleanup (mirrors Prometheus cleanupMetricLabels)
        if let ExprResult::InstantVector(ref mut samples) = result {
            for sample in samples.iter_mut() {
                if sample.drop_name {
                    sample.labels.remove(METRIC_NAME);
                }
            }
        }

        Ok(result)
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
                self.evaluate_matrix_selector(matrix_selector, ctx)
            }
            Expr::Call(call) => self.evaluate_call(call, ctx, preload_eligible),
            Expr::Extension(_) => {
                todo!()
            }
        }
    }

    pub(super) fn evaluate_matrix_selector(
        &self,
        matrix_selector: &MatrixSelector,
        ctx: &EvalContext,
    ) -> EvalResult<ExprResult> {
        let vector_selector = &matrix_selector.vs;
        let range = matrix_selector.range;

        // Apply time modifiers to evaluation_ts
        let adjusted_eval_ts = self.apply_time_modifiers(
            vector_selector.at.as_ref(),
            vector_selector.offset.as_ref(),
            ctx.query_start,
            ctx.query_end,
            ctx.evaluation_ts,
        )?;

        let plan = QueryPlan::for_matrix(adjusted_eval_ts, range.as_millis() as i64);

        execute_selector_pipeline(&self.reader, &plan, vector_selector, self.options)
    }

    pub(super) fn evaluate_subquery(
        &self,
        subquery: &SubqueryExpr,
        ctx: &EvalContext,
    ) -> EvalResult<ExprResult> {
        let adjusted_eval_ts = self.apply_time_modifiers(
            subquery.at.as_ref(),
            subquery.offset.as_ref(),
            ctx.query_start,
            ctx.query_end,
            ctx.evaluation_ts,
        )?;

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

        // Fast path: if inner expression is a pure VectorSelector, evaluate over range once
        if let Expr::VectorSelector(ref selector) = *subquery.expr {
            return self.evaluate_subquery_vector_selector(
                selector,
                subquery_start_ms,
                subquery_end_ms,
                step_ms,
                ctx.lookback_delta_ms,
            );
        }

        // Align start time to the step interval to ensure consistent evaluation points.
        // Prometheus: newEv.startTimestamp = newEv.interval * ((ev.startTimestamp - offset - range) / newEv.interval)
        // Go's division truncates toward zero, but we need a floor division for negative timestamps.
        // Example: -41ms / 10ms
        //   Go (truncate): -41 / 10 = -4, then -4 * 10 = -40ms (wrong for negatives)
        //   Rust div_euclid (floor): -41 / 10 = -5, then -5 * 10 = -50ms (correct)
        // This ensures steps align consistently regardless of whether timestamps are negative.
        let div = subquery_start_ms.div_euclid(step_ms);
        let mut aligned_start_ms = div * step_ms;
        if aligned_start_ms <= subquery_start_ms {
            aligned_start_ms += step_ms;
        }

        // Evaluate the inner expression at each step within the subquery range
        let mut series_map: halfbrown::HashMap<Labels, Vec<Sample>, RandomState> =
            Default::default();

        // todo: possibly parallelize
        for current_time_ms in (aligned_start_ms..=subquery_end_ms).step_by(step_ms as usize) {
            let new_ctx = EvalContext {
                query_start: ctx.query_start,
                query_end: ctx.query_end,
                evaluation_ts: current_time_ms,
                lookback_delta_ms: ctx.lookback_delta_ms,
                step_ms,
            };

            // Disable preload fast path — subquery has its own step grid/lookback context.
            let result = self.evaluate_expr(&subquery.expr, &new_ctx, false)?;

            // PromQL requires subquery inner expression to evaluate to an instant vector.
            // Enforce this invariant at runtime.
            let ExprResult::InstantVector(samples) = result else {
                return Err(EvaluationError::InternalError(
                    "subquery inner expression must return instant vector".to_string(),
                ));
            };

            // DO NOT use the .entry() api here, as it would force an unnecessary copy in the
            // case that an entry already exists
            for sample in samples {
                let _sample = Sample {
                    timestamp: current_time_ms,
                    value: sample.value,
                };
                if let Some(values) = series_map.get_mut(&sample.labels) {
                    values.push(_sample);
                } else {
                    series_map.insert(sample.labels.clone(), vec![_sample]);
                }
            }
        }

        let mut range_vector = Vec::new();
        for (labels, values) in series_map {
            range_vector.push(EvalSamples {
                values,
                labels,
                range_ms,
                range_end_ms: subquery_end_ms,
                drop_name: false,
            });
        }

        Ok(ExprResult::RangeVector(range_vector))
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
        execute_selector_pipeline(&self.reader, &plan, vector_selector, self.options)
    }

    pub(super) fn evaluate_vector_selector(
        &self,
        vector_selector: &VectorSelector,
        ctx: &EvalContext,
        preload_eligible: bool,
    ) -> EvalResult<ExprResult> {
        // Fast path: use preloaded data if available (outer range-query context only).
        // Disabled inside subqueries which have their own step grid/lookback context.
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
                    if let Some(Some(sample)) = series.values.get(step_idx) {
                        samples.push(EvalSample {
                            timestamp_ms: sample.timestamp,
                            value: sample.value,
                            labels: series.labels.clone(), // at some point use EvalLabels to avoid full clone
                            drop_name: false,
                        });
                    }
                }
                return Ok(ExprResult::InstantVector(samples));
            }
        }

        // Apply time modifiers (offset and @)
        let adjusted_eval_ts = self.apply_time_modifiers(
            vector_selector.at.as_ref(),
            vector_selector.offset.as_ref(),
            ctx.query_start,
            ctx.query_end,
            ctx.evaluation_ts,
        )?;

        let mut options = self.options;
        // Ensure the query options carry the lookback delta from the evaluation context so
        // that QueryReader::query implementations (including mocks) can compute the
        // correct time window when applying lookback semantics.
        options.lookback_delta = Duration::from_millis(ctx.lookback_delta_ms as u64);

        let plan = QueryPlan::for_instant_vector(adjusted_eval_ts, ctx.lookback_delta_ms);

        execute_selector_pipeline(&self.reader, &plan, vector_selector, options)
    }

    /// Apply offset and @ modifiers to adjust the evaluation time.
    ///
    /// Implements PromQL time modifier semantics per the Prometheus specification:
    /// - `offset <duration>`: Shifts evaluation time backward (positive) or forward (negative)
    /// - `@ <timestamp>`: Sets absolute evaluation time
    /// - `@ start()`: Uses query start time
    /// - `@ end()`: Uses query end time
    ///
    /// When both modifiers are present, `offset` is applied relative to the `@`
    /// modifier time. Although PromQL defines the result as order-independent
    /// (e.g. `@ t offset d` == `offset d @ t`), we normalize the implementation
    /// by applying `@` first and then applying `offset`. This keeps the logic
    /// simple and matches Prometheus' semantics.
    ///
    /// See: <https://prometheus.io/docs/prometheus/latest/querying/basics/#offset-modifier>
    fn apply_time_modifiers(
        &self,
        at: Option<&AtModifier>,
        offset: Option<&Offset>,
        query_start: Timestamp,
        query_end: Timestamp,
        evaluation_ts: Timestamp,
    ) -> EvalResult<Timestamp> {
        let ms = apply_time_modifiers_ms(at, offset, query_start, query_end, evaluation_ts);
        Ok(ms)
    }

    fn evaluate_function_arg(
        &self,
        call: &Call,
        idx: usize,
        ctx: &EvalContext,
        preload_eligible: bool,
    ) -> EvalResult<PromQLArg> {
        let (arg, expected_type) = get_function_arg(call, idx)?;
        let arg_result: PromQLArg = self.evaluate_expr(arg, ctx, preload_eligible)?.into();

        let actual_type = arg_result.value_type();
        if actual_type != expected_type {
            // maybe this is too strict?
            return Err(EvaluationError::ArgumentError(format!(
                "argument {idx} for function {} expected type {}, got {}",
                call.func.name, expected_type, actual_type
            )));
        }

        Ok(arg_result)
    }

    fn evaluate_function_args(
        &self,
        call: &Call,
        ctx: &EvalContext,
        preload_eligible: bool,
    ) -> EvalResult<Vec<PromQLArg>> {
        let args = if should_parallelize_args_evaluation(call) {
            call.args
                .args
                .par()
                .map(|arg| match self.evaluate_expr(arg, ctx, preload_eligible) {
                    Ok(arg) => Ok(PromQLArg::from(arg)),
                    Err(err) => Err(err),
                })
                .into_fallible_result()
                .collect::<Vec<_>>()?
        } else {
            let mut evaluated_args = Vec::with_capacity(call.args.args.len());
            for idx in 0..call.args.args.len() {
                let arg: PromQLArg =
                    self.evaluate_function_arg(call, idx, ctx, preload_eligible)?;
                evaluated_args.push(arg);
            }
            evaluated_args
        };

        Ok(args)
    }

    pub(super) fn evaluate_call(
        &self,
        call: &Call,
        ctx: &EvalContext,
        preload_eligible: bool,
    ) -> EvalResult<ExprResult> {
        let evaluated_args = self.evaluate_function_args(call, ctx, preload_eligible)?;

        let Some(func) = resolve_function(call.func.name) else {
            return Err(EvaluationError::InternalError(format!(
                "Unknown instant/scalar function: {}",
                call.func.name
            )));
        };

        let result = func.apply_call(evaluated_args, &ctx)?;
        if call.func.return_type == ValueType::Scalar {
            return match result {
                ExprResult::Scalar(_) => Ok(result),
                ExprResult::InstantVector(samples) => {
                    if samples.len() != 1 {
                        return Err(EvaluationError::InternalError(format!(
                            "scalar-returning function {} must return exactly one sample, got {}",
                            call.func.name,
                            samples.len()
                        )));
                    }
                    let sample = &samples[0];
                    Ok(ExprResult::Scalar(sample.value))
                }
                _ => Err(EvaluationError::InternalError(format!(
                    "expected a scalar for function {}, got {}",
                    call.func.name,
                    result.value_type()
                ))),
            };
        }
        Ok(result)
    }

    fn evaluate_binary_expr(
        &self,
        expr: &BinaryExpr,
        ctx: &EvalContext,
        preload_eligible: bool,
    ) -> EvalResult<ExprResult> {
        let lhs = expr.lhs.as_ref();
        let rhs = expr.rhs.as_ref();

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
        _ => false,
    }
}

fn should_parallelize_binary_expr(be: &BinaryExpr) -> bool {
    is_selector(be.lhs.as_ref()) && is_selector(be.rhs.as_ref())
}

fn should_parallelize_args_evaluation(call: &Call) -> bool {
    call.args
        .args
        .iter()
        .filter(|&arg| is_selector(arg))
        .count()
        > 1
}
