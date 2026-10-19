//! Worker threads for fan-out work that takes the module GIL.
//!
//! Each lane is a [`BoundedExecutor`]: a fixed set of blocking threads (the GIL is no job for
//! a pool worker: R1 in `common::threads`) behind a bounded queue, so a burst of fan-out
//! commands or peer requests cannot create a thread per request. The GIL serializes most of this work anyway,
//! so a handful of workers keeps the part that runs outside it (decoding, encoding) parallel.
//!
//! One lane carries both kinds of work: requests from peer coordinators and this node's own
//! share of a fan-out it coordinates, so the local share runs on the same kind of worker as
//! the remote ones.

use crate::common::threads::{BoundedExecutor, Capacity, ExecutorStats, Rejected, lane_workers};
use std::time::Duration;

/// Queued jobs per lane before submissions are rejected as busy. A client has at most one
/// blocked fan-out at a time, so this is only reached by a burst from that many clients (or
/// peers' coordinators) at once.
const QUEUE_CAPACITY: usize = 1024;

/// Runs requests received from peer coordinators.
pub(super) static PEER_REQUEST_EXECUTOR: BoundedExecutor = BoundedExecutor::new(
    "ts-fanout-request",
    lane_workers,
    Capacity::Fixed(QUEUE_CAPACITY),
);

/// Occupy every worker of [`PEER_REQUEST_EXECUTOR`] for `hold` (`TS._DEBUG HOLD_FANOUT_LANE`):
/// the lane as peers' long reads leave it, so a test can show what the fan-outs that share it —
/// their local shares and deferred decodes — do when it cannot get to them in time. Returns
/// how many workers were taken.
pub(crate) fn hold_request_lane(hold: Duration) -> Result<usize, Rejected> {
    let workers = lane_workers();
    for _ in 0..workers {
        PEER_REQUEST_EXECUTOR.try_spawn(move || std::thread::sleep(hold))?;
    }
    Ok(workers)
}

/// The load on the fan-out lane, for `TS._DEBUG METRICS`. Never starts the workers.
pub(crate) fn request_lane_stats() -> ExecutorStats {
    PEER_REQUEST_EXECUTOR.stats()
}
