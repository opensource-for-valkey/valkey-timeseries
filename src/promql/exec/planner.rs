use crate::common::Timestamp;
use crate::promql::exec::pipeline::compute_subquery_alignment;
use crate::promql::model::EvalContext;
use crate::promql::time::{apply_time_modifiers_ms, duration_ms, grid_step_count, step_times};
use promql_parser::parser::{Expr, SubqueryExpr};

/// Immutable description of the step grid preloading must cover.
///
/// The grid and the bounds used by `@ start()` / `@ end()` differ for a
/// subquery. Keeping both here makes that distinction explicit before any
/// source request is issued.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PreloadGrid {
    pub(crate) start_ms: Timestamp,
    pub(crate) end_ms: Timestamp,
    pub(crate) step_ms: i64,
    pub(crate) at_start_ms: Timestamp,
    pub(crate) at_end_ms: Timestamp,
    pub(crate) lookback_delta_ms: i64,
    /// Whether stepped selections over this grid must carry each pick's own
    /// timestamp (the expression calls `timestamp()`). Set by the preload
    /// from the expression; a grid built from a context alone says no.
    pub(crate) sample_timestamps: bool,
}

impl PreloadGrid {
    pub(crate) fn for_range(ctx: &EvalContext) -> Self {
        Self {
            start_ms: ctx.query_start,
            end_ms: ctx.query_end,
            step_ms: ctx.step_ms,
            at_start_ms: ctx.query_start,
            at_end_ms: ctx.query_end,
            lookback_delta_ms: ctx.lookback_delta_ms,
            sample_timestamps: false,
        }
    }

    pub(crate) fn for_subquery(
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
            sample_timestamps: false,
        }
    }

    pub(crate) fn steps(&self) -> impl Iterator<Item = Timestamp> {
        step_times(self.start_ms, self.end_ms, self.step_ms)
    }

    pub(crate) fn expected_steps(&self) -> usize {
        grid_step_count(self.start_ms, self.end_ms, self.step_ms) as usize
    }

    /// The last of [`Self::steps`], without walking them.
    pub(crate) fn last_step(&self) -> Option<Timestamp> {
        if self.step_ms <= 0 || self.end_ms < self.start_ms {
            return (self.start_ms <= self.end_ms).then_some(self.start_ms);
        }
        let span = i128::from(self.end_ms) - i128::from(self.start_ms);
        let last =
            i128::from(self.start_ms) + span / i128::from(self.step_ms) * i128::from(self.step_ms);
        // At most `end_ms`, so it fits.
        Some(last as Timestamp)
    }

    /// The grid that `subquery`, stepped at `step_ms`, evaluates over across
    /// every step of this one: the union of its per-step windows. Modifiers
    /// shift (or, with `@`, collapse) every window end the same way, so the
    /// extremes of the shifted outer bounds bound them all, and each window is
    /// a run of the same lattice — the multiples of `step_ms`. `None` for an
    /// empty grid or a subquery without a step.
    ///
    /// Not checked against [`crate::promql::time::MAX_GRID_STEPS`]: the union
    /// can pass it where each step's own grid does not, and whether that
    /// matters is the caller's call.
    pub(crate) fn subquery_union(&self, subquery: &SubqueryExpr, step_ms: i64) -> Option<Self> {
        if step_ms <= 0 {
            return None;
        }
        let first = self.steps().next()?;
        let last = self.last_step()?;
        let resolve = |ts: Timestamp| {
            apply_time_modifiers_ms(
                subquery.at.as_ref(),
                subquery.offset.as_ref(),
                self.at_start_ms,
                self.at_end_ms,
                ts,
            )
        };
        let (first_end, last_end) = (resolve(first), resolve(last));
        let (earliest_end, latest_end) = (first_end.min(last_end), first_end.max(last_end));
        let (aligned_start_ms, _, _, _) = compute_subquery_alignment(
            earliest_end.saturating_sub(duration_ms(subquery.range)),
            latest_end,
            step_ms,
            0,
        );
        Some(Self {
            start_ms: aligned_start_ms,
            end_ms: latest_end,
            step_ms,
            at_start_ms: self.at_start_ms,
            at_end_ms: self.at_end_ms,
            lookback_delta_ms: self.lookback_delta_ms,
            // Decided from the inner expression by whoever plans over it.
            sample_timestamps: false,
        })
    }
}

/// Immutable planning input for one expression and its preload grid.
pub(crate) struct PlannedQuery<'expr> {
    pub(crate) expr: &'expr Expr,
    pub(crate) grid: PreloadGrid,
    /// Other nodes with `expr`'s content — twin subqueries' inner expressions —
    /// that will be evaluated over the same preloaded data. Nothing is read for
    /// them; they only get their own per-node keys, so a twin's lookups do not
    /// miss.
    pub(crate) twins: Vec<&'expr Expr>,
}

impl<'expr> PlannedQuery<'expr> {
    pub(crate) fn for_range(expr: &'expr Expr, ctx: &EvalContext) -> Self {
        Self::for_grid(expr, PreloadGrid::for_range(ctx))
    }

    pub(crate) fn for_grid(expr: &'expr Expr, grid: PreloadGrid) -> Self {
        Self {
            expr,
            grid,
            twins: Vec::new(),
        }
    }

    pub(crate) fn with_twins(self, twins: Vec<&'expr Expr>) -> Self {
        Self { twins, ..self }
    }
}

#[cfg(test)]
mod tests {
    use super::PreloadGrid;
    use crate::promql::utils::subquery_step_ms;
    use promql_parser::parser::{Expr, parse};

    fn grid(start_ms: i64, end_ms: i64, step_ms: i64) -> PreloadGrid {
        PreloadGrid {
            start_ms,
            end_ms,
            step_ms,
            at_start_ms: start_ms,
            at_end_ms: end_ms,
            lookback_delta_ms: 300_000,
            sample_timestamps: false,
        }
    }

    #[test]
    fn last_step_is_the_last_of_steps() {
        for (start, end, step) in [
            (0, 0, 1),
            (0, 999, 7),
            (-41, 59, 10),
            (3, 2, 1),
            (5, 10, 0),
            (i64::MAX - 25, i64::MAX, 10),
            (i64::MIN, i64::MAX, i64::MAX),
        ] {
            let g = grid(start, end, step);
            assert_eq!(
                g.last_step(),
                g.steps().last(),
                "{start}..={end} step {step}"
            );
        }
    }

    fn union(query: &str, outer: PreloadGrid) -> Option<(i64, i64, i64)> {
        let Expr::Subquery(sq) = parse(query).unwrap() else {
            panic!("not a subquery: {query}");
        };
        outer
            .subquery_union(&sq, subquery_step_ms(&sq))
            .map(|g| (g.start_ms, g.end_ms, g.step_ms))
    }

    #[test]
    fn subquery_union_spans_every_outer_steps_window() {
        // Outer steps 600s..=1200s at 60s; each window is (end - 5m, end] on
        // the 30s lattice, so the union runs from the first multiple after
        // 300s to 1200s.
        let outer = grid(600_000, 1_200_000, 60_000);
        assert_eq!(
            union("m[5m:30s]", outer),
            Some((330_000, 1_200_000, 30_000))
        );
        assert_eq!(
            union("m[5m:30s] offset 1m", outer),
            Some((270_000, 1_140_000, 30_000))
        );
        // `@` collapses every window onto one.
        assert_eq!(
            union("m[5m:30s] @ 900", outer),
            Some((630_000, 900_000, 30_000))
        );
        assert_eq!(
            union("m[5m:30s] @ end()", outer),
            Some((930_000, 1_200_000, 30_000))
        );
    }

    #[test]
    fn subquery_union_is_none_for_an_empty_grid() {
        assert_eq!(union("m[5m:30s]", grid(10, 0, 60_000)), None);
    }
}
