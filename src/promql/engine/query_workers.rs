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
//! Dedicated threads, not the rayon pool: an evaluation blocks — on the
//! selector executor's answer, on its own fan-outs — and a pool worker that
//! blocks steals other jobs while it waits (see
//! `SelectorBatchExecutor`'s design notes for what that did). A plain thread
//! waits without stealing.
//!
//! An evaluation's own parallel work runs on [`EVAL_POOL`], never the global
//! rayon pool: see [`run_evaluation`].

use crate::common::logging::log_warning;
use crate::common::threads::panic_message;
use crate::config::{max_concurrent_queries, max_queued_queries, num_threads};
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, LazyLock, Mutex};

type Job = Box<dyn FnOnce() + Send + 'static>;

struct QueryWorkers {
    sender: mpsc::Sender<Job>,
}

/// Jobs accepted but not yet taken by a worker. Kept outside the pool so
/// reading it (`INFO`) does not build the pool.
static QUEUED: AtomicUsize = AtomicUsize::new(0);
/// Jobs a worker is currently running.
static RUNNING: AtomicUsize = AtomicUsize::new(0);
/// Submissions refused because the backlog was full, since startup.
static REJECTED: AtomicU64 = AtomicU64::new(0);

static QUERY_WORKERS: LazyLock<QueryWorkers> = LazyLock::new(|| {
    let (sender, receiver) = mpsc::channel::<Job>();
    let receiver = Arc::new(Mutex::new(receiver));
    for index in 0..max_concurrent_queries() {
        let receiver = Arc::clone(&receiver);
        let spawned = std::thread::Builder::new()
            .name(format!("ts-promql-query-{index}"))
            .spawn(move || {
                loop {
                    // Hold the queue lock only to take a job, never while running one.
                    let job = receiver.lock().unwrap_or_else(|e| e.into_inner()).recv();
                    match job {
                        Ok(job) => {
                            QUEUED.fetch_sub(1, Ordering::AcqRel);
                            RUNNING.fetch_add(1, Ordering::AcqRel);
                            run_job(index, job);
                            RUNNING.fetch_sub(1, Ordering::AcqRel);
                        }
                        Err(_) => return,
                    }
                }
            });
        if let Err(err) = spawned {
            log_warning(format!(
                "failed to spawn PromQL query worker {index}: {err}"
            ));
        }
    }
    QueryWorkers { sender }
});

/// Run one job, surviving its panic. A panic that unwound through the worker
/// would end its loop and shrink the pool by one for the life of the process;
/// after `max_concurrent_queries` such panics every query would be refused.
/// The client is still answered: the job's captured blocked client is dropped
/// during the unwind, which unblocks it.
fn run_job(worker: usize, job: Job) {
    if let Err(payload) = catch_unwind(AssertUnwindSafe(job)) {
        let reason = panic_message(payload.as_ref());
        log_warning(format!(
            "PromQL query worker {worker}: evaluation panicked: {reason}"
        ));
    }
}

/// The rayon pool PromQL evaluations fan out on, sized like the global pool
/// (`ts-num-threads`).
///
/// An evaluation's parallel jobs block: they ask the selector executor for data
/// and wait, and the executor needs the module GIL to answer. On the global pool
/// those parked jobs could occupy every worker while a GIL holder — `TS.RANGE`
/// over enough chunks, `TS.MRANGE`, the trim cron, a shard's local fan-out
/// handler — waits on that same pool for its own parallel decode: the holder
/// waits on the pool, the pool waits on the executor, the executor waits on the
/// holder, and the server freezes. Here the parked jobs can only exhaust this
/// pool, which no GIL holder ever waits on, so the global pool always drains.
static EVAL_POOL: LazyLock<rayon_core::ThreadPool> = LazyLock::new(|| {
    rayon_core::ThreadPoolBuilder::new()
        .num_threads(num_threads())
        .thread_name(|index| format!("ts-promql-eval-{index}"))
        .build()
        .expect("failed to build the PromQL evaluation pool")
});

/// Run a PromQL evaluation on [`EVAL_POOL`].
///
/// Every parallel entry point the evaluation reaches — the `*_rayon` iterators,
/// `threads::join`, `threads::spawn` — resolves to the pool of the worker that
/// calls it, so installing the evaluation here moves all of its nested work off
/// the global pool without touching those call sites.
///
/// Wrap only the evaluation, never the reply: replying takes the GIL, and a GIL
/// holder must not run on a pool that parks on the executor.
pub(crate) fn run_evaluation<R: Send>(evaluate: impl FnOnce() -> R + Send) -> R {
    EVAL_POOL.install(evaluate)
}

/// Why [`submit_query`] did not queue a job. In every case the caller still
/// owns the reply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SubmitError {
    /// `ts-promql-max-queued-queries` jobs are already waiting for a worker.
    QueueFull { limit: usize },
    /// No worker will ever run it: the workers are gone.
    WorkersGone,
}

impl fmt::Display for SubmitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::QueueFull { limit } => write!(
                f,
                "too many queued PromQL queries (ts-promql-max-queued-queries = {limit})"
            ),
            Self::WorkersGone => f.write_str("query workers are not running"),
        }
    }
}

/// Queue `job` for one of the query workers, unless the backlog is already at
/// `ts-promql-max-queued-queries`.
pub(crate) fn submit_query<F: FnOnce() + Send + 'static>(job: F) -> Result<(), SubmitError> {
    // Reserve the slot before checking, so two concurrent submitters cannot
    // both see room for one. `queued` is the count before this reservation.
    let queued = QUEUED.fetch_add(1, Ordering::AcqRel);
    let limit = max_queued_queries();
    if limit != 0 && queued >= limit {
        QUEUED.fetch_sub(1, Ordering::AcqRel);
        REJECTED.fetch_add(1, Ordering::Relaxed);
        return Err(SubmitError::QueueFull { limit });
    }
    if QUERY_WORKERS.sender.send(Box::new(job)).is_err() {
        QUEUED.fetch_sub(1, Ordering::AcqRel);
        return Err(SubmitError::WorkersGone);
    }
    Ok(())
}

/// A snapshot of the pool for `INFO`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct QueryWorkerStats {
    /// Queries waiting for a worker.
    pub queued: usize,
    /// Queries being evaluated.
    pub running: usize,
    /// Queries refused because the backlog was full, since startup.
    pub rejected: u64,
}

pub(crate) fn stats() -> QueryWorkerStats {
    QueryWorkerStats {
        queued: QUEUED.load(Ordering::Acquire),
        running: RUNNING.load(Ordering::Acquire),
        rejected: REJECTED.load(Ordering::Relaxed),
    }
}

#[cfg(test)]
mod tests {
    use super::{REJECTED, SubmitError, run_evaluation, stats, submit_query};
    use crate::common::threads::IntoParRayon;
    use crate::config::{
        DEFAULT_QUEUED_QUERIES, MAX_QUEUED_QUERIES_CELL, max_concurrent_queries, num_threads,
    };
    use orx_parallel::ParIter;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier, Mutex, mpsc};
    use std::time::Duration;

    /// The pool and its counters are process-wide, so tests that fill or
    /// block it must not overlap.
    static POOL: Mutex<()> = Mutex::new(());

    /// Sets `ts-promql-max-queued-queries` for one test and restores the
    /// default on the way out, however the test ends.
    struct QueueLimit;

    impl QueueLimit {
        fn set(limit: usize) -> Self {
            MAX_QUEUED_QUERIES_CELL.store(limit as i64, Ordering::Relaxed);
            Self
        }
    }

    impl Drop for QueueLimit {
        fn drop(&mut self) {
            MAX_QUEUED_QUERIES_CELL.store(DEFAULT_QUEUED_QUERIES, Ordering::Relaxed);
        }
    }

    #[test]
    fn a_panicking_job_does_not_take_its_worker_down() {
        let _pool = POOL.lock().unwrap_or_else(|e| e.into_inner());
        // Saturate the pool with panicking jobs so every worker sees one, then
        // check that the same number of ordinary jobs still get served.
        let workers = max_concurrent_queries();
        for _ in 0..workers {
            assert_eq!(submit_query(|| panic!("evaluation blew up")), Ok(()));
        }
        let (tx, rx) = mpsc::channel();
        for i in 0..workers {
            let tx = tx.clone();
            assert_eq!(submit_query(move || tx.send(i).unwrap()), Ok(()));
        }
        drop(tx);
        let mut served: Vec<usize> = rx.iter().collect();
        served.sort_unstable();
        assert_eq!(served, (0..workers).collect::<Vec<_>>());

        // And the pool is still alive for a later submission.
        let (tx, rx) = mpsc::channel();
        assert_eq!(submit_query(move || tx.send(()).unwrap()), Ok(()));
        rx.recv_timeout(Duration::from_secs(5))
            .expect("a worker should still be running");
    }

    #[test]
    fn a_full_backlog_refuses_on_arrival_and_drains_when_workers_free_up() {
        let _pool = POOL.lock().unwrap_or_else(|e| e.into_inner());
        const LIMIT: usize = 2;
        let rejected_before = REJECTED.load(Ordering::Relaxed);

        // Park every worker inside a job so nothing drains the backlog. The
        // limit goes on only once they are all parked: a job counts as queued
        // from submission until a worker takes it, so the parking jobs
        // themselves would otherwise trip a limit smaller than the pool.
        let workers = max_concurrent_queries();
        let entered = Arc::new(Barrier::new(workers + 1));
        let release = Arc::new(Barrier::new(workers + 1));
        for _ in 0..workers {
            let (entered, release) = (Arc::clone(&entered), Arc::clone(&release));
            assert_eq!(
                submit_query(move || {
                    entered.wait();
                    release.wait();
                }),
                Ok(())
            );
        }
        entered.wait();
        assert_eq!(stats().running, workers);
        assert_eq!(stats().queued, 0);
        let _limit = QueueLimit::set(LIMIT);

        // LIMIT more are admitted to wait; the next is refused at once.
        let (tx, rx) = mpsc::channel();
        for i in 0..LIMIT {
            let tx = tx.clone();
            assert_eq!(submit_query(move || tx.send(i).unwrap()), Ok(()));
        }
        assert_eq!(stats().queued, LIMIT);
        let overflow = tx.clone();
        assert_eq!(
            submit_query(move || overflow.send(usize::MAX).unwrap()),
            Err(SubmitError::QueueFull { limit: LIMIT })
        );
        // A refusal releases its reservation, so the backlog is unchanged...
        assert_eq!(stats().queued, LIMIT);
        assert_eq!(REJECTED.load(Ordering::Relaxed), rejected_before + 1);
        drop(tx);

        // ...and the admitted jobs all run once the workers are free again.
        release.wait();
        let mut served: Vec<usize> = rx.iter().collect();
        served.sort_unstable();
        assert_eq!(served, (0..LIMIT).collect::<Vec<_>>());
    }

    /// The freeze [`EVAL_POOL`](super::EVAL_POOL) exists to prevent: evaluation
    /// jobs parked on a processor that needs the GIL (a mutex here), while the
    /// GIL holder waits on the global pool for its own parallel work. With the
    /// evaluation on the global pool, the parked jobs hold every global worker
    /// and the holder's fan-out never runs.
    #[test]
    fn parked_evaluations_never_starve_a_gil_holder_on_the_global_pool() {
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
        let jobs = 2 * eval_workers.max(rayon_core::current_num_threads());
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

        // The GIL holder's own fan-out, on the global pool.
        let (done_tx, done_rx) = mpsc::channel();
        std::thread::spawn(move || {
            let _ = done_tx.send((0..10_000usize).into_par_rayon().sum::<usize>());
        });
        let sum = done_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("parked evaluation jobs starved the global pool");
        assert_eq!(sum, (0..10_000usize).sum::<usize>());

        drop(held);
        evaluation.join().unwrap();
        assert_eq!(off_eval_pool.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn zero_means_unbounded() {
        let _pool = POOL.lock().unwrap_or_else(|e| e.into_inner());
        let _limit = QueueLimit::set(0);
        let workers = max_concurrent_queries();
        let (tx, rx) = mpsc::channel();
        // Far more than any bound the pool size would imply.
        let burst = workers * 64;
        for i in 0..burst {
            let tx = tx.clone();
            assert_eq!(submit_query(move || tx.send(i).unwrap()), Ok(()));
        }
        drop(tx);
        assert_eq!(rx.iter().count(), burst);
    }
}
