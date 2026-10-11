//! A query-wide bound on the samples a PromQL evaluation may load.
//!
//! `ts-promql-max-samples-per-query` is Prometheus' `--query.max-samples`:
//! the total number of samples materialized on behalf of one query, across
//! every selector, matrix and rollup read it makes. Per-series and per-count
//! limits (`ts-promql-max-points-per-timeseries`, `ts-promql-max-response-series`)
//! bound each dimension on its own; this bounds their product, which is what a
//! range read of a wide selector actually allocates.
//!
//! **A charge, not a measure of what is held.** The count is cumulative: it
//! only grows while the budget lives, and nothing is given back when a read,
//! grid or step result is dropped. It bounds the work a query does over its
//! life, not its memory at any one moment, which is usually less (a per-step
//! read freed long ago still counts) and can briefly be more (a raw span and
//! the grid built from it coexist while the grid is built, but count once, as
//! the larger; a subquery's per-evaluation result is capped by a budget of its
//! own, not charged here).
//!
//! Enforced in two layers that share one limit: each selector read counts
//! exactly what it decodes and stops early, so no single materialization can
//! exceed the budget; and the evaluator sums every read it receives, so a query
//! spread over several selectors is refused once the total passes the budget.
//! A read is counted on its own budget before the query's, so a query can have
//! charged up to about twice the budget by the time it is refused.
//!
//! The step grids a range query builds from its reads are charged too, as they
//! are built: one sample inside a long lookback fills a slot at every step, so
//! a read of a few samples can become a grid of millions. A series costs the
//! larger of its raw span and its grid, not their sum, so ordinary data (whose
//! grid is the smaller) costs what it decoded. A shard's grid push-down charges
//! only its raw reads: what its per-series stage builds is either shipped no
//! larger than the span it came from or folded into its group's partials a
//! chunk at a time (`ShardGrid`), so the expansion that is kept happens, and is
//! charged, on the coordinator.

use crate::promql::engine::admission::{ADMISSION, Admission};
use crate::promql::{QueryError, QueryResult};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
pub struct SampleBudget {
    /// 0 means unlimited.
    limit: usize,
    /// Everything charged so far; never decreases.
    loaded: AtomicUsize,
    /// The process-wide limit this budget also counts toward, if it does
    /// ([`Self::counted`]); 0 for a budget that does not.
    in_flight_limit: usize,
    /// What this budget added to the process-wide count, given back when it is dropped.
    in_flight: AtomicUsize,
    /// Where the process-wide count is kept: this node's [`ADMISSION`], or a test's own.
    admission: &'static Admission,
}

impl Default for SampleBudget {
    fn default() -> Self {
        Self::new(0)
    }
}

impl SampleBudget {
    /// A budget for one read or one query, counted against nothing else.
    pub fn new(limit: usize) -> Self {
        Self::counted(limit, 0)
    }

    /// A budget that also counts toward the process-wide `ts-promql-max-samples-in-flight`
    /// (`in_flight_limit`; 0 counts nothing), until it is dropped. For a query's own budget
    /// and a shard read's: the ones that live as long as the work. Not for a read whose
    /// samples go on to a query budget that charges them again.
    pub fn counted(limit: usize, in_flight_limit: usize) -> Self {
        Self::counted_against(&ADMISSION, limit, in_flight_limit)
    }

    /// [`Self::counted`], against `admission` rather than this node's: for a test that
    /// drives the process-wide count, which on [`ADMISSION`] would refuse every other test's
    /// queries while it did.
    #[cfg(test)]
    pub(crate) fn counted_in(
        admission: &'static Admission,
        limit: usize,
        in_flight_limit: usize,
    ) -> Self {
        Self::counted_against(admission, limit, in_flight_limit)
    }

    fn counted_against(
        admission: &'static Admission,
        limit: usize,
        in_flight_limit: usize,
    ) -> Self {
        Self {
            limit,
            loaded: AtomicUsize::new(0),
            in_flight_limit,
            in_flight: AtomicUsize::new(0),
            admission,
        }
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    /// Everything charged so far, including samples since dropped.
    pub fn loaded(&self) -> usize {
        self.loaded.load(Ordering::Relaxed)
    }

    /// Account for `samples` more; fails once the total exceeds the limit, or, for a
    /// counted budget, once every counted budget together exceeds the process-wide one.
    ///
    /// Charging nothing only checks: a plain load, never a write. Parallel
    /// producers ask that per series (a grid no larger than its span, or
    /// "is the budget already spent?"), and a read-modify-write of zero would
    /// still take the shared counter's cache line exclusively on every one.
    pub fn charge(&self, samples: usize) -> QueryResult<()> {
        if samples == 0 {
            return self.check_ahead(0);
        }
        let total = self
            .loaded
            .fetch_add(samples, Ordering::Relaxed)
            .saturating_add(samples);
        if self.limit > 0 && total > self.limit {
            return Err(too_many_samples(total, self.limit));
        }
        if self.in_flight_limit > 0 {
            // Recorded before the check, so `Drop` gives back exactly what was added.
            self.in_flight.fetch_add(samples, Ordering::Relaxed);
            self.admission
                .charge_in_flight(samples, self.in_flight_limit)?;
        }
        Ok(())
    }

    /// Whether the budget is already spent, for a producer that wants to stop
    /// before loading anything more.
    pub fn exhausted(&self) -> bool {
        self.limit > 0 && self.loaded() >= self.limit
    }

    /// How many more samples the limit admits: what a producer charging in
    /// batches can keep before its next charge without asking again. Unbounded
    /// for an unlimited budget. Exact only while the producer is the one
    /// charging: another charger's samples are seen at the next charge.
    pub fn room(&self) -> usize {
        if self.limit == 0 {
            usize::MAX
        } else {
            self.limit.saturating_sub(self.loaded())
        }
    }

    /// Fail if `pending` more samples, not yet charged, would exceed the
    /// limit. For a read that can refuse itself before decoding: `pending`
    /// must be a lower bound on what it will charge, so this never fails a
    /// read that [`Self::charge`] would have admitted.
    pub fn check_ahead(&self, pending: usize) -> QueryResult<()> {
        validate_max_samples(self.loaded().saturating_add(pending), self.limit)
    }
}

impl Drop for SampleBudget {
    fn drop(&mut self) {
        let held = *self.in_flight.get_mut();
        if held > 0 {
            self.admission.release_in_flight(held);
        }
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
    fn check_ahead_counts_pending_on_top_of_what_is_charged() {
        let b = SampleBudget::new(10);
        assert!(b.check_ahead(10).is_ok(), "exactly the limit is allowed");
        assert!(b.check_ahead(11).is_err());
        b.charge(6).unwrap();
        assert!(b.check_ahead(4).is_ok());
        let err = b.check_ahead(5).unwrap_err().to_string();
        assert!(err.contains("11 > 10"), "{err}");
        assert_eq!(b.loaded(), 6, "a check charges nothing");
        assert!(SampleBudget::new(0).check_ahead(usize::MAX).is_ok());
    }

    /// Counted budgets share one process-wide total: two queries that each fit their own
    /// budget are refused together, and a finished one gives its samples back.
    #[test]
    fn counted_budgets_share_the_process_wide_limit() {
        // Its own count: 700 million samples on `ADMISSION` would refuse every concurrent
        // test's query, at the default limit of 200 million.
        static COUNTS: Admission = Admission::new();
        let limit = 1_000_000_000;
        let first = SampleBudget::counted_in(&COUNTS, 0, limit);
        first.charge(700_000_000).unwrap();
        assert_eq!(COUNTS.samples_in_flight(), 700_000_000);
        let second = SampleBudget::counted_in(&COUNTS, 0, limit);
        let err = second
            .charge(400_000_000)
            .expect_err("together past the limit");
        assert!(matches!(err, QueryError::Overloaded(_)), "{err}");
        assert!(
            err.to_string().contains("ts-promql-max-samples-in-flight"),
            "{err}"
        );
        assert!(err.ends_query());

        drop(second);
        drop(first);
        assert_eq!(
            COUNTS.samples_in_flight(),
            0,
            "dropped budgets give their samples back"
        );

        let uncounted = SampleBudget::new(0);
        uncounted.charge(5).unwrap();
        assert_eq!(
            uncounted.in_flight.load(Ordering::Relaxed),
            0,
            "a plain budget counts nothing"
        );
    }

    /// A zero charge is the same verdict as before, without adding to the count.
    #[test]
    fn charging_nothing_only_checks() {
        let budget = SampleBudget::new(10);
        budget.charge(10).unwrap();
        assert!(budget.charge(0).is_ok(), "at the limit is not past it");
        assert_eq!(budget.loaded(), 10);
        let _ = budget.charge(5);
        let err = budget.charge(0).expect_err("past the limit");
        assert!(
            matches!(
                err,
                QueryError::TooManySamples {
                    loaded: 15,
                    limit: 10
                }
            ),
            "{err}"
        );
        assert!(SampleBudget::new(0).charge(0).is_ok(), "unlimited");
    }

    #[test]
    fn room_is_what_the_limit_still_admits() {
        assert_eq!(SampleBudget::new(0).room(), usize::MAX, "unlimited");
        let budget = SampleBudget::new(10);
        assert_eq!(budget.room(), 10);
        budget.charge(4).unwrap();
        assert_eq!(budget.room(), 6);
        let _ = budget.charge(20);
        assert_eq!(budget.room(), 0, "past the limit leaves none");
    }

    #[test]
    fn known_counts_are_checked_the_same_way() {
        assert!(validate_max_samples(10, 10).is_ok());
        assert!(validate_max_samples(11, 10).is_err());
        assert!(validate_max_samples(11, 0).is_ok());
    }
}
