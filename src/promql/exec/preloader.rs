use crate::promql::EvalResult;
use crate::promql::engine::sample_budget::SampleBudget;
use crate::promql::engine::{QueryOptions, QueryReader};
use crate::promql::exec::evaluator::{Evaluator, PreparedQuery};
use crate::promql::exec::planner::PlannedQuery;
use std::sync::Arc;

/// Executes an immutable preload plan and returns the data evaluation may read.
pub(crate) struct Preloader<'reader, R: QueryReader + ?Sized> {
    reader: &'reader R,
    options: QueryOptions,
    budget: Option<Arc<SampleBudget>>,
}

impl<'reader, R: QueryReader + ?Sized> Preloader<'reader, R> {
    pub(crate) fn new(reader: &'reader R, options: QueryOptions) -> Self {
        Self {
            reader,
            options,
            budget: None,
        }
    }

    pub(crate) fn prepare(self, plan: PlannedQuery<'_>) -> EvalResult<PreparedQuery> {
        let evaluator = match self.budget {
            Some(budget) => {
                Evaluator::with_prepared(self.reader, self.options, PreparedQuery::sharing(budget))
            }
            None => Evaluator::new(self.reader, self.options),
        };
        evaluator.preload_grid(plan.expr, &plan.grid)?;
        Ok(evaluator.into_prepared())
    }

    /// A preloader whose reads count against `budget` — a subquery's grid is
    /// part of the enclosing query, not a query of its own.
    pub(crate) fn sharing(
        reader: &'reader R,
        options: QueryOptions,
        budget: Arc<SampleBudget>,
    ) -> Self {
        Self {
            reader,
            options,
            budget: Some(budget),
        }
    }
}
