//! The threads that evaluate `TS.QUERY` / `TS.QUERYRANGE`.
//!
//! A fixed number of dedicated threads take queued evaluations in arrival
//! order. Each command used to spawn its own thread, so N concurrent queries
//! were N threads and N sets of intermediate results with no limit on the
//! concurrent work; now at most [`max_concurrent_queries`] evaluate at once
//! and the rest wait their turn, each still bounded by its own deadline
//! (`TIMEOUT`), which the evaluation checks before it starts.
//!
//! The wait itself is bounded by [`max_queued_queries`]. A query that arrives
//! to a full backlog is refused at once rather than admitted to sit out its
//! whole `TIMEOUT` holding a blocked client, its parsed statement and its
//! querier, only to be answered with a timeout anyway.
//!
//! Blocking threads, not a pool: an evaluation blocks — on the selector
//! executor's answer, on its own fan-outs — and a pool worker that blocks
//! steals other jobs while it waits (see `SelectorBatchExecutor`'s design
//! notes for what that did). A plain thread waits without stealing.
//!
//! An evaluation's own parallel work runs on the evaluation pool, never the
//! shared pool: see [`run_evaluation`].

use crate::common::Timestamp;
use crate::common::context::is_blocking_denied;
use crate::common::replies::{ReplyContext, ThreadSafeReplyContext, block_client};
use crate::common::threads::{
    BoundedExecutor, Capacity, ExecutorStats, Rejected, run_on_eval_pool,
};
use crate::common::time::current_time_millis;
use crate::config::{max_concurrent_queries, max_queued_queries};
use crate::error_consts;
use crate::promql::{QueryError, QueryResult};
use std::sync::atomic::{AtomicBool, Ordering};
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyValue};

/// Set by `TS._DEBUG PANIC_NEXT_EVALUATION` (debug mode only): the next evaluation panics.
/// Integration tests use it to check that a failed evaluation still answers its client.
static PANIC_NEXT_EVALUATION: AtomicBool = AtomicBool::new(false);

/// Makes the next PromQL evaluation panic on the evaluation pool. For tests; see
/// [`PANIC_NEXT_EVALUATION`].
pub(crate) fn panic_next_evaluation() {
    PANIC_NEXT_EVALUATION.store(true, Ordering::Relaxed);
}

static QUERY_WORKERS: BoundedExecutor = BoundedExecutor::new(
    "ts-promql-query",
    max_concurrent_queries,
    Capacity::Dynamic(max_queued_queries),
);

/// Run a PromQL evaluation on the evaluation pool.
///
/// Every parallel entry point the evaluation reaches — the `*_rayon` iterators,
/// `threads::join` — resolves to the pool of the pinned worker that calls it, so
/// installing the evaluation there keeps all of its nested work off the shared
/// pool without touching those call sites.
///
/// Wrap only the evaluation: the reply is written from the query worker.
#[track_caller]
pub(crate) fn run_evaluation<R: Send>(evaluate: impl FnOnce() -> R + Send) -> R {
    run_on_eval_pool(evaluate)
}

/// Evaluate a query for the client of `ctx` on a query worker, and answer it from there.
///
/// The client is blocked only once the backlog has room for the query, so a refusal is an
/// ordinary error reply. `evaluate` runs on the evaluation pool ([`run_evaluation`]); `reply`
/// writes its result afterwards. A query whose `deadline` passed while it waited for a worker
/// is answered with a timeout and never evaluated: the wait counts against its budget.
pub(crate) fn submit_evaluation<R, E, P>(
    ctx: &Context,
    deadline: Option<Timestamp>,
    evaluate: E,
    reply: P,
) -> ValkeyResult
where
    R: Send + 'static,
    E: FnOnce() -> QueryResult<R> + Send + 'static,
    P: FnOnce(&ReplyContext, R) + Send + 'static,
{
    // Checked before blocking: the server asserts on a blocked deny-blocking
    // client (a module `RM_Call` without the K flag) and aborts, and inside
    // MULTI or a script the client would get the server's own error while the
    // query still ran with nobody to answer.
    if is_blocking_denied(ctx) {
        return Err(ValkeyError::Str(error_consts::PROMQL_BLOCKING_NOT_ALLOWED));
    }
    let slot = QUERY_WORKERS
        .try_reserve()
        .map_err(|rejected| ValkeyError::String(format!("TSDB: {}", rejection(rejected))))?;
    let blocked_client = block_client(ctx);

    slot.spawn(move || {
        let thread_ctx = ThreadSafeReplyContext::with_blocked_client(blocked_client);
        if deadline.is_some_and(|d| current_time_millis() > d) {
            thread_ctx.reply(Err(ValkeyError::String(QueryError::Timeout.to_string())));
            return;
        }
        let evaluate = move || {
            if PANIC_NEXT_EVALUATION.swap(false, Ordering::Relaxed) {
                panic!("TS._DEBUG PANIC_NEXT_EVALUATION");
            }
            evaluate()
        };
        match run_evaluation(evaluate) {
            Ok(value) => reply(&thread_ctx.get_reply_context(), value),
            Err(err) => {
                thread_ctx.reply(Err(ValkeyError::String(err.to_string())));
            }
        }
    });

    // Answered later, from a query worker.
    Ok(ValkeyValue::NoReply)
}

fn rejection(rejected: Rejected) -> String {
    match rejected {
        Rejected::QueueFull { limit } => {
            format!("too many queued PromQL queries (ts-promql-max-queued-queries = {limit})")
        }
        Rejected::NotRunning => "query workers are not running".to_string(),
    }
}

/// The query workers' load, for `INFO`.
pub(crate) fn stats() -> ExecutorStats {
    QUERY_WORKERS.stats()
}

#[cfg(test)]
mod tests {
    use super::run_evaluation;
    use crate::common::threads::{IntoParRayon, join};
    use crate::config::num_threads;
    use orx_parallel::Par;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, mpsc};
    use std::time::Duration;

    /// The freeze the evaluation pool exists to prevent: evaluation jobs parked on
    /// a processor that needs the GIL (a mutex here), while the GIL holder waits on
    /// orx's shared pool for its own parallel work. With the evaluation on the shared
    /// pool, the parked jobs hold every shared worker and the holder's fan-out never
    /// runs.
    #[test]
    fn parked_evaluations_never_starve_a_gil_holder_on_the_shared_pool() {
        let gil = Arc::new(Mutex::new(()));
        let held = gil.lock().unwrap();

        let (task_tx, task_rx) = mpsc::channel::<mpsc::SyncSender<()>>();
        let processor_gil = Arc::clone(&gil);
        std::thread::spawn(move || {
            for responder in task_rx {
                let _gil = processor_gil.lock().unwrap_or_else(|e| e.into_inner());
                let _ = responder.send(());
            }
        });

        // Enough parking jobs to occupy every worker of either pool.
        let eval_workers = num_threads();
        let jobs = 2 * eval_workers.max(orx_parallel::Pool::global().current_num_threads());
        let parked = Arc::new(AtomicUsize::new(0));
        let off_eval_pool = Arc::new(AtomicUsize::new(0));
        let evaluation = {
            let (parked, off_eval_pool) = (Arc::clone(&parked), Arc::clone(&off_eval_pool));
            std::thread::spawn(move || {
                run_evaluation(|| {
                    (0..jobs).into_par_rayon().for_each(|_| {
                        let on_eval_pool = std::thread::current()
                            .name()
                            .is_some_and(|name| name.starts_with("ts-promql-eval-"));
                        if !on_eval_pool {
                            off_eval_pool.fetch_add(1, Ordering::Relaxed);
                        }
                        let (tx, rx) = mpsc::sync_channel(1);
                        task_tx.send(tx).unwrap();
                        parked.fetch_add(1, Ordering::AcqRel);
                        rx.recv().unwrap();
                    })
                })
            })
        };
        let parked_by = std::time::Instant::now() + Duration::from_secs(10);
        while parked.load(Ordering::Acquire) < eval_workers {
            assert!(
                std::time::Instant::now() < parked_by,
                "evaluation jobs never parked"
            );
            std::thread::yield_now();
        }

        // The GIL holder's own fan-out, on the shared pool: a parallel iterator, and a
        // `join` like TS.JOIN's and TS.LABELSTATS'.
        let (done_tx, done_rx) = mpsc::channel();
        std::thread::spawn(move || {
            let sum = || (0..10_000usize).into_par_rayon().sum::<usize>();
            let (a, b) = join(sum, sum);
            let _ = done_tx.send(a + b);
        });
        let sum = done_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("parked evaluation jobs starved the shared pool");
        assert_eq!(sum, 2 * (0..10_000usize).sum::<usize>());

        drop(held);
        evaluation.join().unwrap();
        assert_eq!(off_eval_pool.load(Ordering::Relaxed), 0);
    }
}
