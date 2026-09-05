use crate::promql::EvalResult;
use crate::promql::engine::{QueryOptions, QueryReader};
use crate::promql::exec::evaluator::{Evaluator, PreparedQuery};
use crate::promql::exec::planner::PlannedQuery;

/// Executes an immutable preload plan and returns the data evaluation may read.
pub(crate) struct Preloader<'reader, R: QueryReader + ?Sized> {
    reader: &'reader R,
    options: QueryOptions,
}

impl<'reader, R: QueryReader + ?Sized> Preloader<'reader, R> {
    pub(crate) fn new(reader: &'reader R, options: QueryOptions) -> Self {
        Self { reader, options }
    }

    pub(crate) fn prepare(self, plan: PlannedQuery<'_>) -> EvalResult<PreparedQuery> {
        let evaluator = Evaluator::new(self.reader, self.options);
        evaluator.preload_grid(plan.expr, &plan.grid)?;
        Ok(evaluator.into_prepared())
    }
}
