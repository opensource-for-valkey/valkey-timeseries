//! The threads that evaluate `TS.QUERY` / `TS.QUERYRANGE`.
//!
//! A fixed number of dedicated threads take queued evaluations in arrival
//! order. Each command used to spawn its own thread, so N concurrent queries
//! were N threads and N sets of intermediate results with nothing to say no;
//! now at most [`max_concurrent_queries`] evaluate at once and the rest wait
//! their turn, each still bounded by its own deadline (`TIMEOUT`), which the
//! evaluation checks before it starts.
//!
//! Dedicated threads, not the rayon pool: an evaluation blocks — on the
//! selector executor's answer, on its own fan-outs — and a pool worker that
//! blocks steals other jobs while it waits (see
//! `SelectorBatchExecutor`'s design notes for what that did). A plain thread
//! waits without stealing.

use crate::common::logging::log_warning;
use crate::config::max_concurrent_queries;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::mpsc;
use std::sync::{Arc, LazyLock, Mutex};

type Job = Box<dyn FnOnce() + Send + 'static>;

struct QueryWorkers {
    sender: mpsc::Sender<Job>,
}

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
                        Ok(job) => run_job(index, job),
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
        let reason = payload
            .downcast_ref::<&str>()
            .map(|s| (*s).to_string())
            .or_else(|| payload.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "non-string panic payload".to_string());
        log_warning(format!(
            "PromQL query worker {worker}: evaluation panicked: {reason}"
        ));
    }
}

/// Queue `job` for one of the query workers. Returns `false` if no worker will
/// ever run it (the workers are gone), in which case the caller still owns
/// the reply.
pub(crate) fn submit_query<F: FnOnce() + Send + 'static>(job: F) -> bool {
    QUERY_WORKERS.sender.send(Box::new(job)).is_ok()
}

#[cfg(test)]
mod tests {
    use super::submit_query;
    use std::sync::mpsc;
    use std::time::Duration;

    #[test]
    fn a_panicking_job_does_not_take_its_worker_down() {
        // Saturate the pool with panicking jobs so every worker sees one, then
        // check that the same number of ordinary jobs still get served.
        let workers = crate::config::max_concurrent_queries();
        for _ in 0..workers {
            assert!(submit_query(|| panic!("evaluation blew up")));
        }
        let (tx, rx) = mpsc::channel();
        for i in 0..workers {
            let tx = tx.clone();
            assert!(submit_query(move || tx.send(i).unwrap()));
        }
        drop(tx);
        let mut served: Vec<usize> = rx.iter().collect();
        served.sort_unstable();
        assert_eq!(served, (0..workers).collect::<Vec<_>>());

        // And the pool is still alive for a later submission.
        let (tx, rx) = mpsc::channel();
        assert!(submit_query(move || tx.send(()).unwrap()));
        rx.recv_timeout(Duration::from_secs(5))
            .expect("a worker should still be running");
    }
}
