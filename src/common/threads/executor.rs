use crate::common::logging::log_warning;
use crate::common::sync::lock;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, Mutex};
use valkey_module::{Context, MODULE_CONTEXT};

type Job = Box<dyn FnOnce() + Send + 'static>;

/// Returned by [`BoundedExecutor::try_spawn`] when the job was not accepted. The job has been
/// dropped: callers that must answer for it (a fan-out share, a peer request) keep what they
/// need to report the rejection before submitting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutorBusy;

/// A fixed set of plain OS threads draining a bounded queue.
///
/// For jobs that take the module GIL, which cannot run on the rayon pool (see
/// [`spawn_background`](super::spawn_background)). The workers are not rayon workers, so one
/// that waits on the pool while holding the GIL blocks without stealing, exactly like a detached
/// thread — but the thread count is capped, and a full queue rejects work instead of growing.
///
/// Jobs must not wait on other jobs of the same executor: with every worker waiting, nothing
/// is left to run what they wait for.
pub struct BoundedExecutor {
    name: &'static str,
    /// `None` when no worker thread could be started: every submission is then rejected, rather
    /// than queued with nothing to drain it.
    sender: Option<SyncSender<Job>>,
}

impl BoundedExecutor {
    pub fn new(name: &'static str, workers: usize, capacity: usize) -> Self {
        let (sender, receiver) = sync_channel::<Job>(capacity);
        let receiver = Arc::new(Mutex::new(receiver));

        let mut started = 0;
        for index in 0..workers.max(1) {
            let receiver = Arc::clone(&receiver);
            match std::thread::Builder::new()
                .name(format!("{name}-{index}"))
                .spawn(move || worker_loop(name, &receiver))
            {
                Ok(_) => started += 1,
                Err(err) => log_warning(format!("{name}: failed to start worker {index}: {err}")),
            }
        }

        Self {
            name,
            sender: (started > 0).then_some(sender),
        }
    }

    /// Queues `job` for a worker. Never blocks: a full queue rejects the job.
    pub fn try_spawn<F: FnOnce() + Send + 'static>(&self, job: F) -> Result<(), ExecutorBusy> {
        let Some(sender) = &self.sender else {
            return Err(ExecutorBusy);
        };
        match sender.try_send(Box::new(job)) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => Err(ExecutorBusy),
            Err(TrySendError::Disconnected(_)) => {
                log_warning(format!("{}: all workers have exited", self.name));
                Err(ExecutorBusy)
            }
        }
    }

    /// Queues `job` to run holding the module GIL, like
    /// [`spawn_with_context`](super::spawn_with_context) but on this executor. The lock is taken
    /// on the worker once the job is dequeued, never while it waits, and released when `job`
    /// returns — or unwinds, since the guard is dropped before the worker catches the panic.
    pub fn try_spawn_in_context<F>(&self, job: F) -> Result<(), ExecutorBusy>
    where
        F: FnOnce(&Context) + Send + 'static,
    {
        self.try_spawn(move || {
            let ctx = MODULE_CONTEXT.lock();
            job(&ctx);
        })
    }
}

fn worker_loop(name: &str, receiver: &Mutex<Receiver<Job>>) {
    loop {
        // Held across the blocking `recv`: idle workers queue on the mutex instead, and the
        // one that holds it takes the next job.
        let job = lock(receiver).recv();
        let Ok(job) = job else {
            return;
        };
        // A panicking job must not take its worker with it, or the executor shrinks for good.
        if catch_unwind(AssertUnwindSafe(job)).is_err() {
            log_warning(format!("{name}: job panicked"));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::channel;
    use std::time::Duration;

    #[test]
    fn runs_submitted_jobs() {
        let executor = BoundedExecutor::new("test-exec-run", 2, 8);
        let (tx, rx) = channel();
        for i in 0..8 {
            let tx = tx.clone();
            executor.try_spawn(move || tx.send(i).unwrap()).unwrap();
        }
        let mut seen: Vec<i32> = (0..8)
            .map(|_| rx.recv_timeout(Duration::from_secs(5)).unwrap())
            .collect();
        seen.sort_unstable();
        assert_eq!(seen, (0..8).collect::<Vec<_>>());
    }

    #[test]
    fn rejects_when_queue_is_full() {
        let executor = BoundedExecutor::new("test-exec-full", 1, 2);
        let (release_tx, release_rx) = channel::<()>();
        let (started_tx, started_rx) = channel::<()>();

        // Occupy the only worker, then fill the queue behind it.
        executor
            .try_spawn(move || {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            })
            .unwrap();
        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        executor.try_spawn(|| {}).unwrap();
        executor.try_spawn(|| {}).unwrap();

        assert_eq!(executor.try_spawn(|| {}), Err(ExecutorBusy));

        release_tx.send(()).unwrap();
    }

    #[test]
    fn survives_a_panicking_job() {
        let executor = BoundedExecutor::new("test-exec-panic", 1, 4);
        executor.try_spawn(|| panic!("boom")).unwrap();

        let (tx, rx) = channel();
        executor.try_spawn(move || tx.send(()).unwrap()).unwrap();
        rx.recv_timeout(Duration::from_secs(5)).unwrap();
    }
}
