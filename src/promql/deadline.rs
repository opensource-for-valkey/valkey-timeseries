//! When a PromQL evaluation must stop, carried to the kernels that do its work.
//!
//! The evaluator checks its deadline at I/O boundaries and once per step, but a single kernel
//! (an aggregation over many series, a rollup over long windows) runs to completion between
//! two checks. [`Deadline`] travels in the [`EvalContext`](crate::promql::EvalContext) every
//! function receives, so a kernel can stop at its own series boundaries.
//!
//! Checks go between series, never inside a loop over one series' samples: those are the
//! loops the compiler vectorizes, and a call or a branch on shared state would stop it.
//! [`Deadline::checkpoint`] is cheap enough for a per-series loop: it adds to a per-thread
//! counter and runs the full [`Deadline::check`] only every [`CHECK_EVERY`] units of work.

use crate::common::Timestamp;
use crate::common::replies::CancelToken;
use crate::common::time::current_time_millis;
use crate::promql::engine::QueryOptions;
use crate::promql::engine::admission::{ADMISSION, Admission};
use crate::promql::{QueryError, QueryResult};
use std::cell::Cell;
use std::time::Instant;

/// Work between two full checks, in units of one sample. About 65 thousand samples or 4
/// thousand series ([`SERIES_WORK`]): a millisecond or so of kernel work, against a check
/// that costs tens of nanoseconds.
pub(crate) const CHECK_EVERY: usize = 1 << 16;

/// What one series costs a checkpoint before its samples: hashing its labels into a group,
/// or setting up its window, is worth about this many samples.
pub(crate) const SERIES_WORK: usize = 16;

thread_local! {
    /// Work since this thread's last full check, across every deadline it served.
    static WORK: Cell<usize> = const { Cell::new(0) };
}

/// When an evaluation must stop: its deadline, its client's disconnect, and the memory limit,
/// or — for a shard's share of a fan-out — when its requester stops waiting ([`Self::until`]).
/// The default stops nothing: the engine's tests run without one.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Deadline {
    /// Epoch milliseconds; 0 for none.
    at_ms: Timestamp,
    cancel: CancelToken,
    max_memory_percent: u32,
    /// When a fan-out's requester stops waiting, measured on this node's clock from the
    /// request's arrival; `None` for none.
    expires: Option<Instant>,
    /// Where the memory use `max_memory_percent` is checked against is sampled: this
    /// node's [`ADMISSION`], or a test's own ([`Self::sampled_by`]).
    admission: &'static Admission,
}

impl Default for Deadline {
    fn default() -> Self {
        Self {
            at_ms: 0,
            cancel: CancelToken::default(),
            max_memory_percent: 0,
            expires: None,
            admission: &ADMISSION,
        }
    }
}

impl Deadline {
    pub(crate) fn from_options(options: &QueryOptions) -> Self {
        Self {
            at_ms: options.deadline.unwrap_or(0).max(0),
            cancel: options.cancel,
            max_memory_percent: options.max_memory_percent,
            ..Self::default()
        }
    }

    /// For work done on another node's behalf: stop once `expires` (when the requester
    /// stops waiting) has passed, with [`QueryError::Timeout`]. `None` stops nothing.
    pub(crate) fn until(expires: Option<Instant>) -> Self {
        Self {
            expires,
            ..Self::default()
        }
    }

    /// This deadline, also stopping once this node's memory use is at or past
    /// `max_memory_percent` of `maxmemory` (0: no limit): for a shard's read, whose
    /// requester's own limit does not travel with the request.
    pub(crate) fn with_memory_limit(self, max_memory_percent: u32) -> Self {
        Self {
            max_memory_percent,
            ..self
        }
    }

    /// This deadline, checking memory use as `admission` samples it rather than this
    /// node's: for a test that fakes memory pressure, which on [`ADMISSION`] would refuse
    /// every other test's queries while it did.
    #[cfg(test)]
    pub(crate) fn sampled_by(self, admission: &'static Admission) -> Self {
        Self { admission, ..self }
    }

    fn is_none(&self) -> bool {
        self.at_ms == 0
            && self.cancel == CancelToken::default()
            && self.max_memory_percent == 0
            && self.expires.is_none()
    }

    /// Fail if the client has gone, memory is past its limit, or the deadline (or the
    /// requester's wait) has passed.
    pub(crate) fn check(&self) -> QueryResult<()> {
        if self.cancel.is_cancelled() {
            return Err(QueryError::Cancelled);
        }
        self.admission.check_memory(self.max_memory_percent)?;
        if self.at_ms > 0 && current_time_millis() > self.at_ms {
            return Err(QueryError::Timeout);
        }
        if self
            .expires
            .is_some_and(|expires| Instant::now() >= expires)
        {
            return Err(QueryError::Timeout);
        }
        Ok(())
    }

    /// Count `work` units done (a sample is one, a series [`SERIES_WORK`]), and run
    /// [`Self::check`] once every [`CHECK_EVERY`] of them on this thread. For a loop over
    /// series, never over one series' samples.
    #[inline]
    pub(crate) fn checkpoint(&self, work: usize) -> QueryResult<()> {
        if self.is_none() {
            return Ok(());
        }
        let due = WORK.with(|done| {
            let total = done.get() + work;
            let due = total >= CHECK_EVERY;
            done.set(if due { 0 } else { total });
            due
        });
        if due { self.check() } else { Ok(()) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::replies::TestClient;

    fn deadline_at(at_ms: Timestamp) -> Deadline {
        Deadline {
            at_ms,
            ..Default::default()
        }
    }

    #[test]
    fn no_deadline_never_stops() {
        let none = Deadline::default();
        assert!(none.check().is_ok());
        for _ in 0..4 {
            assert!(none.checkpoint(CHECK_EVERY).is_ok());
        }
    }

    #[test]
    fn a_passed_deadline_stops_at_the_next_due_checkpoint() {
        let passed = deadline_at(1);
        assert!(matches!(passed.check(), Err(QueryError::Timeout)));
        // Within one interval of work the checkpoint must have run the check.
        let stopped = (0..=CHECK_EVERY / SERIES_WORK)
            .map(|_| passed.checkpoint(SERIES_WORK))
            .any(|r| matches!(r, Err(QueryError::Timeout)));
        assert!(stopped);

        let later = deadline_at(current_time_millis() + 60_000);
        for _ in 0..=CHECK_EVERY / SERIES_WORK {
            assert!(later.checkpoint(SERIES_WORK).is_ok());
        }
    }

    #[test]
    fn a_requesters_expiry_stops_a_shards_kernels() {
        assert!(
            Deadline::until(None).check().is_ok(),
            "no expiry stops nothing"
        );
        let later = Deadline::until(Some(Instant::now() + std::time::Duration::from_secs(60)));
        assert!(later.check().is_ok());
        let passed = Deadline::until(Some(Instant::now()));
        assert!(matches!(passed.check(), Err(QueryError::Timeout)));
        let stopped = (0..=CHECK_EVERY / SERIES_WORK)
            .map(|_| passed.checkpoint(SERIES_WORK))
            .any(|r| matches!(r, Err(QueryError::Timeout)));
        assert!(
            stopped,
            "a checkpoint is not skipped for an expiry-only deadline"
        );
    }

    #[test]
    fn a_disconnected_client_stops_the_kernel_too() {
        let client = TestClient::new();
        let deadline = Deadline {
            cancel: client.token(),
            ..Default::default()
        };
        assert!(deadline.check().is_ok());
        client.disconnect();
        assert!(matches!(deadline.check(), Err(QueryError::Cancelled)));
    }

    /// A shard's deadline carries this node's memory limit, so its read and kernels stop
    /// when memory use rises past it while they run. Without the limit — what a shard's
    /// deadline used to be, its requester's expiry alone — the same pressure stops nothing.
    #[test]
    fn a_memory_limit_stops_a_shard_as_memory_rises() {
        // Its own memory sample: on `ADMISSION`, every concurrent test's query would be refused.
        static MEMORY: Admission = Admission::new();
        let shard = Deadline::until(None)
            .with_memory_limit(90)
            .sampled_by(&MEMORY);
        let expiry_only = Deadline::until(None).sampled_by(&MEMORY);
        assert!(shard.check().is_ok(), "admitted while memory is fine");

        MEMORY.store_memory_ratio(0.95);
        let err = shard.check().unwrap_err();
        assert!(matches!(err, QueryError::Overloaded(_)), "{err}");
        assert!(err.to_string().contains("95% of maxmemory"), "{err}");
        let stopped = (0..=CHECK_EVERY / SERIES_WORK)
            .map(|_| shard.checkpoint(SERIES_WORK))
            .any(|r| matches!(r, Err(QueryError::Overloaded(_))));
        assert!(stopped, "a checkpoint between series stops it too");

        assert!(expiry_only.check().is_ok());
        assert!(
            (0..=CHECK_EVERY / SERIES_WORK).all(|_| expiry_only.checkpoint(SERIES_WORK).is_ok())
        );
    }
}
