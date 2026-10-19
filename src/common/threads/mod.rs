//! The module's threads and thread pools, and the rules that keep them from deadlocking.
//!
//! # Threads
//!
//! | Kind ([`ThreadRole`]) | What | May block | May take the GIL |
//! |---|---|---|---|
//! | `Main` | the server's main thread: commands, callbacks, the cron | no | holds it |
//! | `Blocking` | [`BoundedExecutor`] lanes (fan-out, `TS.OUTLIERS`), [`spawn_background`] tasks | yes | yes |
//! | `SharedPool` | orx-parallel's rayon-core pool, sized by `ts-num-threads` ([`init_thread_pool`]) | no | no |
//! | `BlockingPool` | a pinned pool whose jobs wait for a blocking thread's answer | on blocking threads | no |
//! | `IsolatedPool` | a pinned pool whose jobs never block | no | no |
//!
//! The two pinned kinds are for features that need a pool of their own; this module states
//! the rules for them so that a feature adds a pool, not a rule.
//!
//! One pool sits outside the table: rayon's global pool, which third-party crates enter on
//! their own (`krcf`, behind `TS.OUTLIERS METHOD rcf`, when its parallel heuristic fires). The
//! module never submits work to it, and the jobs it runs there neither block nor take the GIL,
//! so it obeys R1 without exception and any thread, a GIL holder included, may wait on it. It
//! is sized by rayon, not by `ts-num-threads`. Keep it that way: a crate that could block or
//! take the GIL inside its parallel iterators must not be called with the GIL held.
//!
//! # Rules
//!
//! A rayon worker that waits — on a `join`, a scope, a parallel iterator — runs other jobs of
//! its pool while it waits, and a pool whose workers are all parked runs nothing. Every
//! freeze this module has had came from one of those two facts meeting the GIL.
//!
//! - **R1.** A pool job never takes the GIL, and never blocks on anything but jobs of its own
//!   pool. A blocking pool is the one exception: its jobs wait on blocking threads. Its
//!   `join` and `*_rayon` work still steals while it waits, by design: that work stays on the
//!   pool (R4).
//!
//!   Waiting for a data lock is allowed, not an exception, as long as R5 holds for it. No
//!   shared-pool job reads an index's postings today; one that does may wait for a postings
//!   writer to finish: index updates, the stale-id sweep, `optimize_indices`. Those writers
//!   hold the postings lock for one bounded batch, and never wait on a pool or take the GIL
//!   while they hold it, so the wait always ends. `std`'s `RwLock` also queues a new reader
//!   behind a waiting writer, so a GIL holder that waits on such jobs can stall for one writer
//!   batch.
//! - **R2.** A GIL holder waits only on a pool that obeys R1 without exception: the shared pool
//!   or an isolated pool, never a blocking pool.
//! - **R3.** Only blocking threads and blocking-pool workers wait for another thread's answer,
//!   and never while holding the GIL: whoever answers may need it.
//! - **R4.** Parallel work started on a pinned worker stays on its pool. Enter parallel work
//!   through the `*_rayon` adapters and [`join`], which pick the pool by role — never the bare
//!   orx or rayon entry points, which would move a blocking pool's jobs onto the pool GIL
//!   holders wait on.
//! - **R5.** No lock guard — the GIL, an index's postings, a series — is held across a wait on
//!   a pool, except the GIL where R2 allows it. Nor is the postings write lock held while
//!   taking the GIL: the order is always GIL, then postings. That is what keeps R1's lock waits
//!   finite.
//! - **R6.** Work that reads or frees server state the GIL protects, and that no lock guard's
//!   type ties to the lock, runs on a thread holding the GIL: entering and dropping an ACL scope
//!   (`common::acl::ThreadAclScope`), whose user handle points into the server's ACL table.
//!
//! Work that takes the GIL or blocks therefore runs on a blocking thread: a [`BoundedExecutor`]
//! lane for per-request work (bounded queue, refused when full — reserve with
//! [`BoundedExecutor::try_reserve`] before blocking a client), [`spawn_background`] for one-shot
//! work, [`spawn_background_single`] for a periodic task.
//!
//! # Enforcement
//!
//! `clippy.toml` forbids the raw pool and GIL entry points outside this module. At runtime,
//! [`LockGil::lock_gil`] (and [`GilToken::take`], for other guards over the lock) checks R1 and
//! re-entry, [`check_may_wait_on_blocking_pool`] R2, [`check_may_block`] R3 and
//! [`check_holds_gil`] R6, in release builds too: a debug build panics, a release build logs the first violation of each rule
//! with a backtrace. R4 holds by construction; R5 is for review.

// The raw pool and GIL entry points that `clippy.toml` forbids elsewhere are wrapped here.
#![allow(clippy::disallowed_methods)]

mod executor;
mod gil;
mod orx_pool;
mod role;
mod single_flight;

pub use executor::{BoundedExecutor, Capacity, ExecutorStats, Rejected, Reservation};
pub use gil::{
    GilGuard, GilSource, GilToken, LockGil, YieldMode, check_holds_gil, check_may_block,
    check_may_wait_on_blocking_pool, holds_gil, yield_gil,
};
pub use orx_pool::{
    IntoParRayon, IterIntoParRayon, ModulePool, ParCollectionRayon, ParMutRayon, ParRayon,
    ParWithPool,
};
pub use role::{ThreadRole, current_role, on_pool_worker, set_thread_role};
pub use single_flight::{Flight, SingleFlight};
use std::env;
use valkey_module::logging::log_notice;

const MAX_NUM_THREADS_ENV_VARIABLE: &str = "ORX_NUM_THREADS";

/// Sizes and builds the shared pool: the rayon-core pool that runs the module's orx-parallel
/// computations.
///
/// With the `persistent-pool-rayon` feature, orx's default runner executes on a dedicated
/// rayon-core pool, and the `*_rayon` adapters and [`join`] send everything except pinned-pool
/// work there too (see [`ModulePool`]). orx builds the pool lazily on first use, sized from
/// `ORX_NUM_THREADS` capped at the core count, and never resizes it. So the variable must be
/// set before anything touches the pool, and we force the build here rather than leave it to
/// the first command — which would pay for spawning the workers, and would size the pool to
/// every core if it ran before this function.
///
/// Must run after the module config is loaded (`ts-num-threads`).
pub fn init_thread_pool() {
    let threads = crate::config::num_threads();
    // `num_threads()` has already resolved `ts-num-threads 0` to the CPU count.
    // SAFETY: `set_var` races with any concurrent read of the environment. This runs once,
    // during module load, before the pool it sizes has a thread. The process is not
    // single-threaded by then (the server's bio and I/O threads, and the allocator's, already
    // run), but none of those reads the environment after startup, so no read can overlap.
    unsafe {
        env::set_var(MAX_NUM_THREADS_ENV_VARIABLE, threads.to_string());
    }
    let actual = orx_parallel::Pool::global().current_num_threads();
    if actual != threads {
        log_notice(format!(
            "parallel query pool has {actual} threads (ts-num-threads={threads}, capped at the \
             available cores)"
        ));
    }
}

/// Workers for a [`BoundedExecutor`] lane: enough to keep the part of its jobs that runs
/// outside the GIL (decoding, encoding, analysis) parallel, few enough that a burst cannot
/// crowd the machine. Read on first use, after `ts-num-threads` has been resolved.
pub fn lane_workers() -> usize {
    crate::config::num_threads().clamp(2, 8)
}

/// The message a panic was raised with, for logging a panic that was caught.
pub(crate) fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|s| (*s).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "non-string panic payload".to_string())
}

/// Runs `job` on a thread of its own, as a blocking thread: for work that takes the GIL or
/// blocks, which no pool worker may do (R1).
///
/// One thread per call, so only for jobs with a bounded number of callers: one-shot work, or
/// a periodic task behind [`spawn_background_single`]. Per-request work goes to a
/// [`BoundedExecutor`] instead. A thread that cannot be started is logged and `job` dropped.
///
/// A panic in `job` is caught and logged to the server log, as a [`BoundedExecutor`] worker
/// logs one; left alone it would reach only stderr, which a daemonized server discards.
pub fn spawn_background<F: FnOnce() + Send + 'static>(name: &str, job: F) {
    use std::panic::{AssertUnwindSafe, catch_unwind};
    let thread_name = name.to_string();
    if let Err(err) = std::thread::Builder::new()
        .name(name.to_string())
        .spawn(move || {
            set_thread_role(ThreadRole::Blocking);
            if let Err(payload) = catch_unwind(AssertUnwindSafe(job)) {
                crate::common::logging::log_warning(format!(
                    "{thread_name}: background job panicked: {}",
                    panic_message(payload.as_ref())
                ));
            }
        })
    {
        log_notice(format!("failed to spawn background thread {name}: {err}"));
    }
}

/// [`spawn_background`] for a periodic task: does nothing while the task's previous run is
/// still in flight, so no thread is spent on a run that would be skipped.
pub fn spawn_background_single<F: FnOnce() + Send + 'static>(
    name: &str,
    flight: &'static SingleFlight,
    job: F,
) {
    let Some(run) = flight.try_start() else {
        return;
    };
    spawn_background(name, move || {
        let _run = run;
        job();
    });
}

/// Runs `oper_a` and `oper_b`, potentially in parallel, on the caller's pool if it is a pinned
/// worker (R4), on the shared pool otherwise.
pub fn join<A, B, RA, RB>(oper_a: A, oper_b: B) -> (RA, RB)
where
    A: Send + FnOnce() -> RA,
    B: Send + FnOnce() -> RB,
    RA: Send,
    RB: Send,
{
    if role::on_pinned_worker() {
        rayon_core::join(oper_a, oper_b)
    } else {
        orx_parallel::Pool::global().join(oper_a, oper_b)
    }
}

#[cfg(test)]
mod tests {
    use super::{SingleFlight, ThreadRole, current_role, join, set_thread_role};
    use super::{spawn_background, spawn_background_single};
    use orx_parallel::{Par, ParCollection, Parallelizable, Pool};
    use std::sync::mpsc;
    use std::time::Duration;

    /// Whether every item of a parallel computation ran on a worker of orx's rayon pool.
    fn all_on_orx_pool(items: &[usize]) -> bool {
        let pool = Pool::global();
        items
            .par()
            .map(|_| pool.current_thread_index().is_some())
            .collect::<Vec<_>>()
            .into_iter()
            .all(|on_pool| on_pool)
    }

    #[test]
    fn par_runs_on_the_orx_rayon_pool() {
        let items: Vec<usize> = (0..10_000).collect();
        assert!(Pool::global().current_thread_index().is_none());
        assert!(all_on_orx_pool(&items));
    }

    #[test]
    fn nested_par_stays_on_the_orx_rayon_pool() {
        let outer: Vec<usize> = (0..64).collect();
        let inner: Vec<usize> = (0..1_000).collect();
        let all = outer
            .par()
            .map(|_| all_on_orx_pool(&inner))
            .collect::<Vec<_>>();
        assert!(all.into_iter().all(|on_pool| on_pool));
    }

    #[test]
    fn join_from_an_unpinned_caller_runs_on_the_shared_pool() {
        let (a, b) = join(current_role, current_role);
        assert_eq!((a, b), (ThreadRole::SharedPool, ThreadRole::SharedPool));

        // A worker of some other, unpinned pool hands its halves over too.
        let foreign = rayon_core::ThreadPoolBuilder::new()
            .num_threads(1)
            .build()
            .unwrap();
        let (a, b) = foreign.install(|| join(current_role, current_role));
        assert_eq!((a, b), (ThreadRole::SharedPool, ThreadRole::SharedPool));
    }

    #[test]
    fn join_on_a_pinned_worker_stays_on_its_pool() {
        let pinned = rayon_core::ThreadPoolBuilder::new()
            .num_threads(2)
            .start_handler(|_| set_thread_role(ThreadRole::BlockingPool))
            .build()
            .unwrap();
        let (a, b) = pinned.install(|| join(current_role, current_role));
        assert_eq!((a, b), (ThreadRole::BlockingPool, ThreadRole::BlockingPool));
    }

    #[test]
    fn background_threads_are_blocking_and_survive_a_panic() {
        let (tx, rx) = mpsc::channel();
        spawn_background("test-bg-role", move || tx.send(current_role()).unwrap());
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(5)).unwrap(),
            ThreadRole::Blocking
        );
        // A panicking job is caught on its own thread; the test process carries on.
        let (tx, rx) = mpsc::channel::<()>();
        spawn_background("test-bg-panic", move || {
            let _tx = tx;
            panic!("boom");
        });
        // The sender is dropped by the unwind, so the receiver sees a disconnect.
        assert!(rx.recv_timeout(Duration::from_secs(5)).is_err());
    }

    #[test]
    fn a_periodic_task_skips_ticks_while_a_run_is_in_flight() {
        static TASK: SingleFlight = SingleFlight::new();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let (ran_tx, ran_rx) = mpsc::channel::<u32>();

        let first = ran_tx.clone();
        spawn_background_single("test-single-1", &TASK, move || {
            first.send(1).unwrap();
            release_rx.recv().unwrap();
        });
        assert_eq!(ran_rx.recv_timeout(Duration::from_secs(5)).unwrap(), 1);

        // Still in flight: the second tick starts nothing.
        let second = ran_tx.clone();
        spawn_background_single("test-single-2", &TASK, move || second.send(2).unwrap());
        assert!(ran_rx.recv_timeout(Duration::from_millis(100)).is_err());

        release_tx.send(()).unwrap();
        // Once the first run ends the flight is free again.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while TASK.try_start().is_none() {
            assert!(std::time::Instant::now() < deadline, "the run never ended");
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}
