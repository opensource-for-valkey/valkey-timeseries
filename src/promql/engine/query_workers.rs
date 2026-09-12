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
                        Ok(job) => job(),
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

/// Queue `job` for one of the query workers. Returns `false` if no worker will
/// ever run it (the workers are gone), in which case the caller still owns
/// the reply.
pub(crate) fn submit_query<F: FnOnce() + Send + 'static>(job: F) -> bool {
    QUERY_WORKERS.sender.send(Box::new(job)).is_ok()
}
