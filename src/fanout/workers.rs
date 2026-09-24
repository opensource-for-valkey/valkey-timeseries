//! Worker threads for fan-out work that takes the module GIL.
//!
//! Each lane is a [`BoundedExecutor`]: a fixed set of threads off the rayon pool (see
//! `spawn_background` for why) behind a bounded queue, so a burst of fan-out commands or peer
//! requests cannot create a thread per request. The GIL serializes most of this work anyway,
//! so a handful of workers keeps the part that runs outside it (decoding, encoding) parallel.
//!
//! Two lanes, so that neither kind of work can starve the other: a node flooded with its own
//! clients' fan-outs still answers the requests its peers are waiting on, and the reverse.

use crate::common::threads::BoundedExecutor;
use crate::config::num_threads;
use std::sync::LazyLock;

/// Workers per lane.
const MIN_WORKERS: usize = 2;
const MAX_WORKERS: usize = 8;

/// Queued jobs per lane before submissions are rejected as busy. A client has at most one
/// blocked fan-out at a time, so this is only reached by a burst from that many clients (or
/// peers' coordinators) at once.
const QUEUE_CAPACITY: usize = 1024;

fn workers_per_lane() -> usize {
    // Read on first use, after config registration has resolved `ts-num-threads`.
    num_threads().clamp(MIN_WORKERS, MAX_WORKERS)
}

/// Runs this node's share of fan-outs it coordinates (`FanoutCommand::get_local_response`).
pub(super) static LOCAL_SHARE_EXECUTOR: LazyLock<BoundedExecutor> =
    LazyLock::new(|| BoundedExecutor::new("ts-fanout-local", workers_per_lane(), QUEUE_CAPACITY));

/// Runs requests received from peer coordinators.
pub(super) static PEER_REQUEST_EXECUTOR: LazyLock<BoundedExecutor> =
    LazyLock::new(|| BoundedExecutor::new("ts-fanout-request", workers_per_lane(), QUEUE_CAPACITY));
