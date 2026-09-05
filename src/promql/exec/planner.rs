use crate::common::Timestamp;
use crate::promql::model::EvalContext;
use crate::promql::time::step_times;
use promql_parser::parser::Expr;

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
        }
    }

    pub(crate) fn steps(&self) -> impl Iterator<Item = Timestamp> {
        step_times(self.start_ms, self.end_ms, self.step_ms)
    }

    pub(crate) fn expected_steps(&self) -> usize {
        if self.step_ms <= 0 {
            return 0;
        }
        ((self.end_ms - self.start_ms) / self.step_ms) as usize + 1
    }
}

/// Immutable planning input for one expression and its preload grid.
pub(crate) struct PlannedQuery<'expr> {
    pub(crate) expr: &'expr Expr,
    pub(crate) grid: PreloadGrid,
}

impl<'expr> PlannedQuery<'expr> {
    pub(crate) fn for_range(expr: &'expr Expr, ctx: &EvalContext) -> Self {
        Self {
            expr,
            grid: PreloadGrid::for_range(ctx),
        }
    }

    pub(crate) fn for_grid(expr: &'expr Expr, grid: PreloadGrid) -> Self {
        Self { expr, grid }
    }
}
