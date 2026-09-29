//! Preloading: everything a range query reads before its step loop, so that each step is a
//! lookup rather than a read. Covers selectors, matrices, subquery unions, and the rollups and
//! stepped aggregations a source answers over the whole grid.

use super::subquery::{subquery_key, subquery_step_ms};
use super::*;

impl<'reader, R: QueryReader + ?Sized> Evaluator<'reader, R> {
    #[cfg(test)]
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
            .into_fallible()
            .collect()?;

        self.preload_rollups(expr, grid)?;
        self.preload_matrices(expr, grid)?;
        self.preload_subqueries(expr, grid)?;

        self.fill_node_keys(expr);
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
    pub(super) fn preload_subqueries(&self, expr: &Expr, grid: &PreloadGrid) -> EvalResult<()> {
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
            let range_ms = duration_ms(subquery.range);
            let (aligned_start_ms, _, _, _) = compute_subquery_alignment(
                earliest_end.saturating_sub(range_ms),
                latest_end,
                step_ms,
                0,
            );
            // The union spans every outer step, so it can pass the ceiling
            // where each step's own grid does not. Those steps then prepare
            // their own grids, and each is checked there.
            if grid_step_count(aligned_start_ms, latest_end, step_ms) > MAX_GRID_STEPS {
                continue;
            }
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
                    self.note_preloaded();
                }
                Err(err) if is_query_ending(&err) => return Err(err),
                Err(err) => {
                    // Same rule as the per-step preload: a reader limit tripped
                    // by the union span downgrades to per-step evaluation.
                    log_debug(format!(
                        "subquery union preload failed; each outer step will prepare its own grid: {err}"
                    ));
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
    pub(super) fn preload_rollups(&self, expr: &Expr, grid: &PreloadGrid) -> EvalResult<()> {
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
            .into_fallible()
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
    pub(super) fn preload_matrices(&self, expr: &Expr, grid: &PreloadGrid) -> EvalResult<()> {
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
            .into_fallible()
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
    pub(super) fn preload_matrix(
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
        let start_ms = first.saturating_sub(range_ms).saturating_add(1);

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
                self.note_preloaded();
            }
            Err(err @ QueryError::TooManySamples { .. }) => return Err(err.into()),
            Err(err) => {
                log_debug(format!(
                    "matrix preload failed; falling back to per-step windows: {err}"
                ));
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
    pub(super) fn resolved_window_ends(
        &self,
        vs: &VectorSelector,
        grid: &PreloadGrid,
    ) -> Vec<Timestamp> {
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

    #[allow(clippy::too_many_arguments)]
    pub(super) fn preload_rollup(
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

        // A peer without grid push-down: leave the rollup unpreloaded, and
        // `preload_matrices` reads its span raw instead.
        let Some(outcome) = self
            .reader
            .query_grid(&matrix.vs, &request, options)?
            .supported()
        else {
            return Ok(());
        };
        let rolled = self.finish_grid(&request, outcome)?;

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
    pub(super) fn grid_request(
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
    pub(super) fn finish_grid(
        &self,
        request: &GridRequest,
        outcome: GridOutcome,
    ) -> EvalResult<Vec<RangeSample<EvalLabels>>> {
        // Charged here, whatever the answer's shape, so every caller pays the
        // same for the same request: a raw span for what was decoded (before
        // the evaluation turns it into one point per step), a pushed-down
        // answer for the points that arrived — its source charged the span
        // against its own budget.
        let outcome = match outcome {
            GridOutcome::Raw(series) => {
                self.charge_range_samples(&series)?;
                request.evaluate(series)?
            }
            GridOutcome::Rolled(series) => {
                self.charge_range_samples(&series)?;
                GridOutcome::Rolled(series)
            }
            GridOutcome::Reduced(groups) => {
                self.charge_range_samples(&groups)?;
                GridOutcome::Reduced(groups)
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

    pub(super) fn cache_preloaded_grid(
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
        self.note_preloaded();
    }

    /// Ask the source to fold each aggregation that sits directly over a bare
    /// vector selector — `avg(cpu)`, `sum by (region) (cpu)` — over the whole
    /// step grid, once, before the step loop starts.
    ///
    /// The shard steps the selector and folds the picks per `(group, step)`,
    /// so what crosses the wire is groups × steps rather than series × steps —
    /// and the selector itself is not preloaded separately (see
    /// [`Self::preload_grid`]).
    pub(super) fn preload_stepped_aggregations(
        &self,
        expr: &Expr,
        grid: &PreloadGrid,
    ) -> EvalResult<()> {
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
            .into_fallible()
            .collect()?;

        Ok(())
    }

    pub(super) fn preload_stepped_aggregation(
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
            // A peer without grid push-down: not cached, so the selector is
            // preloaded on its own and the aggregation runs per step.
            GridOutcome::Unsupported => return Ok(()),
            outcome => self.finish_grid(&request, outcome)?,
        };

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

    /// Preload one vector selector over the whole step grid: at every step,
    /// the last sample inside the lookback window.
    ///
    /// The source is asked for the *stepped* form — one point per series per
    /// step — rather than the raw span, so a cluster ships the grid and not
    /// every sample under it. A source that answers raw (a single node) has
    /// the same bucketing run here.
    pub(super) fn preload_vector_selector(
        &self,
        vs: &VectorSelector,
        grid: &PreloadGrid,
    ) -> EvalResult<()> {
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
                    // A peer without grid push-down: read the span raw, as
                    // for a shape the grid request cannot describe.
                    GridOutcome::Unsupported => self.read_selector_span(vs, grid)?,
                    GridOutcome::Rolled(_) | GridOutcome::Reduced(_) => {
                        return Err(EvaluationError::InternalError(
                            "stepped selection answered with the wrong shape".to_string(),
                        ));
                    }
                }
            }
            // A modifier shape the grid request cannot describe: read the
            // span the selector's bounds cover and bucket it here.
            None => self.read_selector_span(vs, grid)?,
        };
        self.cache_stepped_span(vs, grid, raw_series)
    }

    /// The raw samples `vs` needs over the whole of `grid`, from an ordinary
    /// range read.
    pub(super) fn read_selector_span(
        &self,
        vs: &VectorSelector,
        grid: &PreloadGrid,
    ) -> EvalResult<Vec<RangeSample<EvalLabels>>> {
        let (earliest_ms, latest_ms) = selector_bounds(
            vs.at.as_ref(),
            vs.offset.as_ref(),
            grid.at_start_ms,
            grid.at_end_ms,
            grid.start_ms,
            grid.end_ms,
            grid.lookback_delta_ms,
        );
        Ok(self
            .reader
            .query_range(vs, earliest_ms, latest_ms, self.options)?)
    }

    /// Bucket a selector's raw span to one sample per step — the last sample
    /// inside the lookback window — and cache it for the step loop.
    pub(super) fn cache_stepped_span(
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

    pub(super) fn cache_preloaded_series(
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
        drop(cache);
        self.note_preloaded();
    }
}
