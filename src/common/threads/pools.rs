//! The pinned pools PromQL runs on. Both are built on first use, so a node that never runs
//! PromQL never starts them. Both take `ts-num-threads` as given, where the shared pool caps it
//! at the core count: an evaluation worker can park waiting on the selector executor, and
//! threads beyond the cores let such waits overlap.

use super::{ThreadRole, set_thread_role};
use crate::config::num_threads;
use rayon_core::{ThreadPool, ThreadPoolBuilder};
use std::sync::LazyLock;

/// The pool PromQL evaluations fan out on (`run_evaluation` in the query workers).
///
/// An evaluation's parallel jobs block: they ask the selector executor for data and wait, and
/// the executor needs the GIL to answer. On the shared pool those parked jobs could occupy
/// every worker while a GIL holder — `TS.RANGE` over enough chunks, `TS.MRANGE`, the trim
/// cron, a shard's local fan-out handler — waits on that same pool for its own parallel
/// decode: the holder waits on the pool, the pool waits on the executor, the executor waits
/// on the holder, and the server freezes. Here the parked jobs can only exhaust this pool,
/// which no GIL holder ever waits on (R2), so the shared pool always drains. Its workers are
/// pinned, so the parallel work they start stays here (R4).
pub(crate) static EVAL_POOL: LazyLock<ThreadPool> =
    LazyLock::new(|| build_pinned_pool("ts-promql-eval", ThreadRole::BlockingPool));

/// The pool the PromQL selector executor materializes on. Private to the executor so that its
/// work never depends on a pool whose workers may all be parked waiting for exactly this work.
/// Its jobs never block (R1 without exception), so the processor may wait on it while holding
/// the GIL (R2).
pub(crate) static MATERIALIZE_POOL: LazyLock<ThreadPool> =
    LazyLock::new(|| build_pinned_pool("ts-promql-io", ThreadRole::IsolatedPool));

fn build_pinned_pool(name: &'static str, role: ThreadRole) -> ThreadPool {
    ThreadPoolBuilder::new()
        .num_threads(num_threads())
        .thread_name(move |index| format!("{name}-{index}"))
        .start_handler(move |_| set_thread_role(role))
        .build()
        .unwrap_or_else(|err| panic!("failed to build the {name} pool: {err}"))
}
