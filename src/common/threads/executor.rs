use super::{ThreadRole, panic_message, set_thread_role};
use crate::common::logging::log_warning;
use crate::common::sync::lock;
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex, OnceLock};

type Job = Box<dyn FnOnce() + Send + 'static>;

/// How many jobs a [`BoundedExecutor`] lets wait for a worker. 0 means no limit.
#[derive(Clone, Copy)]
pub enum Capacity {
    Fixed(usize),
    /// Read on every submission, so a config change applies to the next one.
    Dynamic(fn() -> usize),
}

impl Capacity {
    fn limit(self) -> usize {
        match self {
            Self::Fixed(limit) => limit,
            Self::Dynamic(limit) => limit(),
        }
    }
}

/// Why a [`BoundedExecutor`] did not take a job. The job, if one was offered, has been dropped:
/// callers that must answer for it (a blocked client, a fan-out share, a peer request) reserve
/// with [`BoundedExecutor::try_reserve`] first, or keep what they need to report the refusal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rejected {
    /// `limit` jobs are already waiting for a worker.
    QueueFull { limit: usize },
    /// No worker could be started, so nothing would ever run the job.
    NotRunning,
}

impl fmt::Display for Rejected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::QueueFull { limit } => write!(f, "too many queued jobs (limit {limit})"),
            Self::NotRunning => f.write_str("worker threads are not running"),
        }
    }
}

/// A snapshot of a [`BoundedExecutor`]'s load, for `INFO`.
#[derive(Debug, Clone, Copy)]
pub struct ExecutorStats {
    /// Jobs accepted but not yet taken by a worker.
    pub queued: usize,
    /// Jobs a worker is running.
    pub running: usize,
    /// Jobs refused because the queue was full, since startup.
    pub rejected: u64,
}

/// A fixed set of blocking threads draining a bounded queue.
///
/// For per-request work that blocks or takes the GIL, which no pool worker may do (R1 in the
/// [module docs](super)). The workers are plain threads, so one that waits on a pool while
/// holding the GIL blocks without stealing; the thread count is fixed, and a full queue
/// rejects work instead of growing.
///
/// Declared as a `static`: the workers start on the first submission, so reading [`stats`]
/// (`INFO`) never starts them, and their count is read then, after the config has loaded.
///
/// That first submission comes from the main thread and pays for starting the workers:
/// measured (2026-09-29, macOS M-series) at about 40–120 µs for 8 threads, and 0.5–1 ms for 64,
/// once per process. Deliberately not done at module load: that would start every lane (about
/// 32 threads at the defaults, far more with a large `ts-promql-max-concurrent-queries`) on
/// every node, including nodes that never run PromQL or `TS.OUTLIERS`.
///
/// Jobs must not wait on other jobs of the same executor: with every worker waiting, nothing
/// is left to run what they wait for.
///
/// [`stats`]: Self::stats
pub struct BoundedExecutor {
    name: &'static str,
    workers: fn() -> usize,
    capacity: Capacity,
    /// `None` when no worker could be started: every submission is then rejected, rather than
    /// queued with nothing to drain it.
    queue: OnceLock<Option<Sender<Job>>>,
    queued: AtomicUsize,
    running: AtomicUsize,
    rejected: AtomicU64,
}

impl BoundedExecutor {
    pub const fn new(name: &'static str, workers: fn() -> usize, capacity: Capacity) -> Self {
        Self {
            name,
            workers,
            capacity,
            queue: OnceLock::new(),
            queued: AtomicUsize::new(0),
            running: AtomicUsize::new(0),
            rejected: AtomicU64::new(0),
        }
    }

    /// Claims a place in the queue, so the job that fills it cannot be refused. Reserve before
    /// committing to anything the job must answer for — blocking a client, say.
    pub fn try_reserve(&'static self) -> Result<Reservation, Rejected> {
        if self.sender().is_none() {
            return Err(Rejected::NotRunning);
        }
        // Reserve before checking, so two concurrent submitters cannot both see room for one.
        // `queued` is the count before this reservation.
        let queued = self.queued.fetch_add(1, Ordering::AcqRel);
        let limit = self.capacity.limit();
        if limit != 0 && queued >= limit {
            self.queued.fetch_sub(1, Ordering::AcqRel);
            self.rejected.fetch_add(1, Ordering::Relaxed);
            return Err(Rejected::QueueFull { limit });
        }
        Ok(Reservation {
            executor: self,
            spent: false,
        })
    }

    /// Queues `job` for a worker. Never blocks: a full queue rejects the job.
    pub fn try_spawn<F: FnOnce() + Send + 'static>(&'static self, job: F) -> Result<(), Rejected> {
        self.try_reserve()?.spawn(job);
        Ok(())
    }

    pub fn stats(&self) -> ExecutorStats {
        ExecutorStats {
            queued: self.queued.load(Ordering::Acquire),
            running: self.running.load(Ordering::Acquire),
            rejected: self.rejected.load(Ordering::Relaxed),
        }
    }

    fn sender(&'static self) -> Option<&'static Sender<Job>> {
        self.queue.get_or_init(|| self.start()).as_ref()
    }

    fn start(&'static self) -> Option<Sender<Job>> {
        let (sender, receiver) = channel::<Job>();
        let receiver = Arc::new(Mutex::new(receiver));
        let mut started = 0;
        for index in 0..(self.workers)().max(1) {
            let receiver = Arc::clone(&receiver);
            match std::thread::Builder::new()
                .name(format!("{}-{index}", self.name))
                .spawn(move || self.worker_loop(&receiver))
            {
                Ok(_) => started += 1,
                Err(err) => log_warning(format!(
                    "{}: failed to start worker {index}: {err}",
                    self.name
                )),
            }
        }
        (started > 0).then_some(sender)
    }

    fn worker_loop(&self, receiver: &Mutex<Receiver<Job>>) {
        set_thread_role(ThreadRole::Blocking);
        loop {
            // Held across the blocking `recv`: idle workers queue on the mutex instead, and
            // the one that holds it takes the next job.
            let Ok(job) = lock(receiver).recv() else {
                return;
            };
            self.queued.fetch_sub(1, Ordering::AcqRel);
            self.running.fetch_add(1, Ordering::AcqRel);
            // A panicking job must not take its worker with it, or the executor shrinks for
            // good. A job's captured resources (a blocked client) are released by the unwind.
            if let Err(payload) = catch_unwind(AssertUnwindSafe(job)) {
                log_warning(format!(
                    "{}: job panicked: {}",
                    self.name,
                    panic_message(payload.as_ref())
                ));
            }
            self.running.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

/// A place in a [`BoundedExecutor`]'s queue, from [`BoundedExecutor::try_reserve`]. Released
/// if dropped unspent.
pub struct Reservation {
    executor: &'static BoundedExecutor,
    spent: bool,
}

impl Reservation {
    /// Queues `job` in the reserved place.
    pub fn spawn<F: FnOnce() + Send + 'static>(mut self, job: F) {
        self.spent = true;
        let executor = self.executor;
        // A reservation exists only once the workers started, and they never exit while the
        // `static` sender lives, so the channel is always open.
        let sent = executor
            .sender()
            .is_some_and(|sender| sender.send(Box::new(job)).is_ok());
        if !sent {
            executor.queued.fetch_sub(1, Ordering::AcqRel);
            log_warning(format!("{}: workers are gone; job dropped", executor.name));
        }
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if !self.spent {
            self.executor.queued.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;
    use std::sync::mpsc::channel;
    use std::time::Duration;

    fn two() -> usize {
        2
    }

    fn one() -> usize {
        1
    }

    /// Parks every worker of `executor` in a job until the returned barrier is waited on.
    fn park_workers(executor: &'static BoundedExecutor, workers: usize) -> Arc<Barrier> {
        let entered = Arc::new(Barrier::new(workers + 1));
        let release = Arc::new(Barrier::new(workers + 1));
        for _ in 0..workers {
            let (entered, release) = (Arc::clone(&entered), Arc::clone(&release));
            executor
                .try_spawn(move || {
                    entered.wait();
                    release.wait();
                })
                .unwrap();
        }
        entered.wait();
        release
    }

    #[test]
    fn runs_submitted_jobs() {
        static EXECUTOR: BoundedExecutor =
            BoundedExecutor::new("test-exec-run", two, Capacity::Fixed(8));
        let (tx, rx) = channel();
        for i in 0..8 {
            let tx = tx.clone();
            EXECUTOR.try_spawn(move || tx.send(i).unwrap()).unwrap();
        }
        let mut seen: Vec<i32> = (0..8)
            .map(|_| rx.recv_timeout(Duration::from_secs(5)).unwrap())
            .collect();
        seen.sort_unstable();
        assert_eq!(seen, (0..8).collect::<Vec<_>>());
    }

    #[test]
    fn workers_are_blocking_threads() {
        static EXECUTOR: BoundedExecutor =
            BoundedExecutor::new("test-exec-role", one, Capacity::Fixed(1));
        let (tx, rx) = channel();
        EXECUTOR
            .try_spawn(move || tx.send(super::super::current_role()).unwrap())
            .unwrap();
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(5)).unwrap(),
            ThreadRole::Blocking
        );
    }

    #[test]
    fn a_full_queue_refuses_on_arrival_and_drains_when_workers_free_up() {
        static EXECUTOR: BoundedExecutor =
            BoundedExecutor::new("test-exec-full", two, Capacity::Fixed(2));
        let release = park_workers(&EXECUTOR, 2);
        assert_eq!(EXECUTOR.stats().running, 2);
        assert_eq!(EXECUTOR.stats().queued, 0);

        let (tx, rx) = channel();
        for i in 0..2 {
            let tx = tx.clone();
            EXECUTOR.try_spawn(move || tx.send(i).unwrap()).unwrap();
        }
        assert_eq!(EXECUTOR.stats().queued, 2);
        assert_eq!(
            EXECUTOR.try_spawn(|| {}),
            Err(Rejected::QueueFull { limit: 2 })
        );
        // A refusal releases its place, so the queue is unchanged.
        assert_eq!(EXECUTOR.stats().queued, 2);
        assert_eq!(EXECUTOR.stats().rejected, 1);
        drop(tx);

        release.wait();
        let mut served: Vec<i32> = rx.iter().collect();
        served.sort_unstable();
        assert_eq!(served, vec![0, 1]);
    }

    #[test]
    fn a_reservation_holds_its_place_until_spent_or_dropped() {
        static EXECUTOR: BoundedExecutor =
            BoundedExecutor::new("test-exec-reserve", one, Capacity::Fixed(1));
        let release = park_workers(&EXECUTOR, 1);

        let reservation = EXECUTOR.try_reserve().unwrap();
        assert_eq!(EXECUTOR.stats().queued, 1);
        assert!(matches!(
            EXECUTOR.try_reserve(),
            Err(Rejected::QueueFull { limit: 1 })
        ));
        drop(reservation);
        assert_eq!(EXECUTOR.stats().queued, 0);

        let (tx, rx) = channel();
        EXECUTOR
            .try_reserve()
            .unwrap()
            .spawn(move || tx.send(()).unwrap());
        release.wait();
        rx.recv_timeout(Duration::from_secs(5)).unwrap();
    }

    #[test]
    fn a_dynamic_capacity_is_read_on_every_submission_and_zero_is_unbounded() {
        static LIMIT: AtomicUsize = AtomicUsize::new(1);
        fn limit() -> usize {
            LIMIT.load(Ordering::Relaxed)
        }
        static EXECUTOR: BoundedExecutor =
            BoundedExecutor::new("test-exec-dynamic", one, Capacity::Dynamic(limit));
        let release = park_workers(&EXECUTOR, 1);

        EXECUTOR.try_spawn(|| {}).unwrap();
        assert!(EXECUTOR.try_spawn(|| {}).is_err());
        LIMIT.store(0, Ordering::Relaxed);
        let (tx, rx) = channel();
        let burst = 256;
        for i in 0..burst {
            let tx = tx.clone();
            EXECUTOR.try_spawn(move || tx.send(i).unwrap()).unwrap();
        }
        drop(tx);
        release.wait();
        assert_eq!(rx.iter().count(), burst);
    }

    #[test]
    fn survives_a_panicking_job() {
        static EXECUTOR: BoundedExecutor =
            BoundedExecutor::new("test-exec-panic", one, Capacity::Fixed(4));
        EXECUTOR.try_spawn(|| panic!("boom")).unwrap();

        let (tx, rx) = channel();
        EXECUTOR.try_spawn(move || tx.send(()).unwrap()).unwrap();
        rx.recv_timeout(Duration::from_secs(5)).unwrap();
    }
}
