//! Push-down evaluation: rollups and stepped aggregations a source answers over the whole
//! step grid, and the lookups that find those answers during the step loop.

use super::*;

impl<'reader, R: QueryReader + ?Sized> Evaluator<'reader, R> {
    /// The rollup a call can be pushed down as, if any.
    ///
    /// Unlike the instant path's [`Self::rollup_arguments`], the scalar
    /// parameter must be a *literal*. One grid request carries one parameter, so
    /// a parameter that could differ per step — `quantile_over_time(scalar(q),
    /// m[5m])` — cannot be answered by a single request at all. That is a
    /// correctness bound, not an optimization: such a call stays local.
    pub(super) fn pushable_rollup<'a>(
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

    /// This step's slice of a preloaded rollup, or `None` when the call was not
    /// preloaded and has to be evaluated here.
    pub(super) fn preloaded_rollup(&self, call: &Call, ctx: &EvalContext) -> Option<ExprResult> {
        match self.memoized(call, |keys| &keys.rollups) {
            Some(key) => self.preloaded_grid_by_key(key?, ctx, false),
            None => self.preloaded_grid_by_key(&self.rollup_key(call)?, ctx, false),
        }
    }

    /// The grid key an unfused pushable rollup `call` is preloaded under.
    pub(super) fn rollup_key(&self, call: &Call) -> Option<GridPreloadKey> {
        let (kind, matrix, param) = self.pushable_rollup(call)?;
        Some(GridPreloadKey::rollup(
            &matrix.vs,
            kind,
            matrix_range_ms(matrix),
            param,
            None,
        ))
    }

    /// The grid key a rollup fused with `aggregate` is preloaded under, when
    /// `aggregate` is a fusable aggregation directly over a pushable rollup.
    pub(super) fn fused_rollup_key(&self, aggregate: &AggregateExpr) -> Option<GridPreloadKey> {
        let Expr::Call(call) = strip_parens(&aggregate.expr) else {
            return None;
        };
        let aggregation = fusable_aggregation(aggregate)?;
        let (kind, matrix, param) = self.pushable_rollup(call)?;
        Some(GridPreloadKey::rollup(
            &matrix.vs,
            kind,
            matrix_range_ms(matrix),
            param,
            Some(AggregationKey::of(&aggregation)),
        ))
    }

    /// This step's slice of a preloaded grid, keyed explicitly so the fused
    /// forms — whose entries are groups rather than series — can share it.
    pub(super) fn preloaded_grid_by_key(
        &self,
        key: &GridPreloadKey,
        ctx: &EvalContext,
        drop_name: bool,
    ) -> Option<ExprResult> {
        let guard = self.preloaded_grids.read().unwrap();
        let preloaded = guard.get(key)?;
        let step_idx = step_index(
            ctx.evaluation_ts,
            preloaded.eval_start_ms,
            preloaded.step_ms,
        );

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
    pub(super) fn evaluate_pushed_down_aggregate(
        &self,
        aggregate: &AggregateExpr,
        param: Option<&ExprResult>,
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

        // Evaluated (and checked) once by the caller.
        let param = match param {
            None => None,
            Some(ExprResult::Scalar(value)) => Some(AggregationParam::Scalar(*value)),
            Some(ExprResult::String(label)) => Some(AggregationParam::Label(label.clone())),
            Some(_) => return Ok(None),
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
    pub(super) fn preloaded_stepped_aggregation(
        &self,
        aggregate: &AggregateExpr,
        ctx: &EvalContext,
    ) -> Option<ExprResult> {
        match self.memoized(aggregate, |keys| &keys.stepped) {
            Some(key) => self.preloaded_grid_by_key(key?, ctx, false),
            None => self.preloaded_grid_by_key(&stepped_aggregation_key(aggregate)?, ctx, false),
        }
    }

    /// Whether this selector's samples were already fetched by
    /// [`Self::preload_for_range`].
    pub(super) fn is_preloaded(&self, selector: &VectorSelector) -> bool {
        let key = self.selector_key(selector);
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
    pub(super) fn evaluate_fused_rollup(
        &self,
        aggregate: &AggregateExpr,
        ctx: &EvalContext,
        preload_eligible: bool,
    ) -> EvalResult<Option<ExprResult>> {
        let Expr::Call(call) = strip_parens(&aggregate.expr) else {
            return Ok(None);
        };
        // A range step answered from [`NodeKeys`] without rebuilding the key:
        // `Some(None)` means the aggregation does not fuse at all.
        if ctx.step_ms > 0
            && let Some(memo) = self.memoized(aggregate, |keys| &keys.fused)
        {
            let Some(key) = memo else {
                return Ok(None);
            };
            if preload_eligible {
                return Ok(self.preloaded_grid_by_key(key, ctx, drops_metric_name(call)));
            }
            return Ok(None);
        }
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

        // A peer without grid push-down: decline, and the aggregation is
        // evaluated here from an ordinary read.
        let Some(outcome) = self
            .reader
            .query_grid(&matrix.vs, &request, options)?
            .supported()
        else {
            return Ok(None);
        };
        let grouped = self.finish_grid(&request, outcome)?;

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
    pub(super) fn evaluate_pushed_down_rollup(
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
                range_ms: duration_ms(matrix.range),
                param,
            }),
            aggregation,
            // A rollup's value belongs to its window, not to any one sample.
            sample_timestamps: false,
        };

        let mut options = self.options;
        options.lookback_delta = Duration::from_millis(ctx.lookback_delta_ms as u64);

        // A peer without grid push-down: decline, and the rollup is evaluated
        // here from an ordinary read.
        let Some(outcome) = self
            .reader
            .query_grid(&matrix.vs, &request, options)?
            .supported()
        else {
            return Ok(None);
        };
        let rolled = self.finish_grid(&request, outcome)?;

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
    pub(in crate::promql::exec) fn rollup_arguments<'a>(
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

/// The aggregation of `aggregate` as something a shard can fold a grid into,
/// or `None` when it cannot be fused.
///
/// Two conditions, both about the operator rather than the data: it must have
/// a push-down form — a mergeable partial state for the reductions, a
/// re-selectable output for `topk`/`bottomk`/`limitk`/`limit_ratio`, addable
/// counts for `count_values`; `quantile` has none — and its parameter, where
/// it takes one, must be a literal the request can carry. `topk(scalar(x), y)`
/// is evaluated here.
pub(super) fn fusable_aggregation(aggregate: &AggregateExpr) -> Option<GridAggregation> {
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
pub(super) fn stepped_aggregation_key(aggregate: &AggregateExpr) -> Option<GridPreloadKey> {
    let Expr::VectorSelector(vs) = strip_parens(&aggregate.expr) else {
        return None;
    };
    let aggregation = fusable_aggregation(aggregate)?;
    Some(GridPreloadKey::stepped(
        vs,
        AggregationKey::of(&aggregation),
    ))
}

pub(super) fn to_eval_samples(samples: Vec<InstantSample<EvalLabels>>) -> Vec<EvalSample> {
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
