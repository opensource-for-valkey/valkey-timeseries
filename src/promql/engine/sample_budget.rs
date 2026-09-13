//! A query-wide bound on the samples a PromQL evaluation may load.
//!
//! `ts-promql-max-samples-per-query` is Prometheus' `--query.max-samples`:
//! the total number of samples materialized on behalf of one query, across
//! every selector, matrix and rollup read it makes. Per-series and per-count
//! limits (`ts-promql-max-points-per-timeseries`, `ts-promql-max-response-series`)
//! bound each dimension on its own; their product — what a range read of a
//! wide selector actually allocates — is what drove an 8 GB machine into swap.
//!
//! Enforced in two layers that share one limit: each selector read counts
//! exactly what it decodes and stops early, so no single materialization can
//! exceed the budget; and the evaluator sums every read it receives, so a query
//! spread over several selectors is refused once the total passes the budget.
//! Peak memory for one query is therefore bounded by about twice the budget.

use crate::promql::{QueryError, QueryResult};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, Default)]
pub struct SampleBudget {
    /// 0 means unlimited.
    limit: usize,
    loaded: AtomicUsize,
}

impl SampleBudget {
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            loaded: AtomicUsize::new(0),
        }
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn loaded(&self) -> usize {
        self.loaded.load(Ordering::Relaxed)
    }

    /// Account for `samples` more; fails once the total exceeds the limit.
    pub fn charge(&self, samples: usize) -> QueryResult<()> {
        let total = self
            .loaded
            .fetch_add(samples, Ordering::Relaxed)
            .saturating_add(samples);
        if self.limit > 0 && total > self.limit {
            return Err(too_many_samples(total, self.limit));
        }
        Ok(())
    }

    /// Whether the budget is already spent, for a producer that wants to stop
    /// before loading anything more.
    pub fn exhausted(&self) -> bool {
        self.limit > 0 && self.loaded() >= self.limit
    }
}

pub fn too_many_samples(loaded: usize, limit: usize) -> QueryError {
    QueryError::TooManySamples { loaded, limit }
}

/// Check a count that is already known, as for samples a shard shipped back.
pub fn validate_max_samples(loaded: usize, limit: usize) -> QueryResult<()> {
    if limit > 0 && loaded > limit {
        return Err(too_many_samples(loaded, limit));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unlimited_never_fails() {
        let b = SampleBudget::new(0);
        assert!(b.charge(usize::MAX / 2).is_ok());
        assert!(b.charge(usize::MAX / 2).is_ok());
        assert!(!b.exhausted());
    }

    #[test]
    fn fails_on_the_charge_that_crosses_the_limit() {
        let b = SampleBudget::new(10);
        assert!(b.charge(4).is_ok());
        assert!(b.charge(6).is_ok(), "exactly the limit is allowed");
        assert!(b.exhausted());
        let err = b.charge(1).unwrap_err().to_string();
        assert!(err.contains("11 > 10"), "{err}");
        assert!(err.contains("ts-promql-max-samples-per-query"), "{err}");
    }

    #[test]
    fn known_counts_are_checked_the_same_way() {
        assert!(validate_max_samples(10, 10).is_ok());
        assert!(validate_max_samples(11, 10).is_err());
        assert!(validate_max_samples(11, 0).is_ok());
    }
}
