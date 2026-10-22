//! `task`: maintenance task lifecycle and progress. Fixed task identities keep cardinality bounded.
//! A run starts in the task body, not when its thread is requested. Duration includes lock waits;
//! a caught panic or an explicit shutdown abort never counts as a completion.

use super::duration_histogram;
use metered::entry::{counter, gauge, metric};
use metered::{BucketHistogram, Registry};
use std::cell::Cell;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

pub struct TaskMetrics {
    pub name: &'static str,
    pub thread_name: &'static str,
    started: AtomicU64,
    completed: AtomicU64,
    pub skipped_inflight: AtomicU64,
    panics: AtomicU64,
    pub spawn_failures: AtomicU64,
    running: AtomicU64,
    last_completed: AtomicU64,
    duration: LazyLock<BucketHistogram>,
}

impl TaskMetrics {
    pub const fn new(name: &'static str, thread_name: &'static str) -> Self {
        Self {
            name,
            thread_name,
            started: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            skipped_inflight: AtomicU64::new(0),
            panics: AtomicU64::new(0),
            spawn_failures: AtomicU64::new(0),
            running: AtomicU64::new(0),
            last_completed: AtomicU64::new(0),
            duration: duration_histogram(),
        }
    }

    pub fn start(&self) -> TaskRun<'_> {
        self.started.fetch_add(1, Ordering::Relaxed);
        self.running.fetch_add(1, Ordering::Relaxed);
        TaskRun {
            metrics: self,
            start: Instant::now(),
            aborted: Cell::new(false),
        }
    }
}

/// Balances lifecycle accounting on normal return and unwinding. Completion means the body
/// returned without a panic or shutdown abort; recoverable per-item errors have separate counters.
pub struct TaskRun<'a> {
    metrics: &'a TaskMetrics,
    start: Instant,
    aborted: Cell<bool>,
}

impl TaskRun<'_> {
    /// Use at existing cancellation points, including inside batch loops, so a partial run
    /// cannot advance completion freshness. This does not add cancellation points to a task.
    pub fn check_shutdown(&self) -> bool {
        if crate::is_shutting_down() {
            self.aborted.set(true);
            true
        } else {
            false
        }
    }
}

fn now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

impl Drop for TaskRun<'_> {
    fn drop(&mut self) {
        let metrics = self.metrics;
        metrics.duration.observe_duration(self.start.elapsed());
        if std::thread::panicking() {
            metrics.panics.fetch_add(1, Ordering::Relaxed);
        } else if !self.aborted.get() {
            metrics.completed.fetch_add(1, Ordering::Relaxed);
            // Concurrent ASM drains can finish in either order.
            metrics
                .last_completed
                .fetch_max(now_seconds(), Ordering::Relaxed);
        }
        metrics.running.fetch_sub(1, Ordering::Relaxed);
    }
}

pub static TRIM: TaskMetrics = TaskMetrics::new("trim", "ts-series-trim");
pub static STALE_IDS: TaskMetrics = TaskMetrics::new("stale_ids", "ts-stale-ids");
pub static STALE_IDS_FULL_SWEEP: TaskMetrics =
    TaskMetrics::new("stale_ids_full_sweep", "ts-stale-ids-full-sweep");
pub static OPTIMIZE: TaskMetrics = TaskMetrics::new("optimize", "ts-optimize-indices");
pub static RECONCILE: TaskMetrics = TaskMetrics::new("reconcile", "ts-index-sweep");

static TASKS: &[&TaskMetrics] = &[
    &TRIM,
    &STALE_IDS,
    &STALE_IDS_FULL_SWEEP,
    &OPTIMIZE,
    &RECONCILE,
];

pub static TRIM_SERIES_PROCESSED: AtomicU64 = AtomicU64::new(0);
pub static TRIM_SAMPLES_DELETED: AtomicU64 = AtomicU64::new(0);
pub static TRIM_ERRORS: AtomicU64 = AtomicU64::new(0);
pub static STALE_IDS_RETIRED: AtomicU64 = AtomicU64::new(0);
pub static STALE_IDS_PASSES_COMPLETED: AtomicU64 = AtomicU64::new(0);
pub static OPTIMIZE_PASSES_COMPLETED: AtomicU64 = AtomicU64::new(0);
pub static OPTIMIZE_LAST_PASS_COMPLETED: AtomicU64 = AtomicU64::new(0);
/// Empty-db cleanup is not a [`TaskMetrics`] task: it is a short inline pass on the main thread,
/// so it reports only what it removed and whether it is running now.
pub static TRIM_UNUSED_DBS_REMOVED: AtomicU64 = AtomicU64::new(0);
pub static TRIM_UNUSED_DBS_RUNNING: AtomicU64 = AtomicU64::new(0);

/// Records a complete traversal of one database, rather than one bounded task invocation.
pub fn complete_pass(count: &AtomicU64, last_completed: &AtomicU64) {
    count.fetch_add(1, Ordering::Relaxed);
    last_completed.fetch_max(now_seconds(), Ordering::Relaxed);
}

/// Registers one task's lifecycle metrics, each named `<task>_<metric>` under the registry's
/// section prefix. Shared with sections that own tasks of their own (`asm`).
pub(super) fn register_task(registry: &mut Registry<'static>, task: &'static TaskMetrics) {
    for (name, source, help) in [
        ("started", &task.started, "Runs that entered the task body"),
        (
            "completed",
            &task.completed,
            "Runs that returned normally without a shutdown abort",
        ),
        (
            "skipped_inflight",
            &task.skipped_inflight,
            "Attempts skipped because a previous run holds the single-flight guard",
        ),
        ("panics", &task.panics, "Runs that unwound with a panic"),
        (
            "spawn_failures",
            &task.spawn_failures,
            "Task threads that could not be started; no run was counted",
        ),
    ] {
        registry.register(
            counter(format!("{}_{name}", task.name))
                .source(source)
                .help(help),
        );
    }
    registry
        .register(
            gauge(format!("{}_running", task.name))
                .source(&task.running)
                .help("Task bodies currently executing, including time waiting for locks"),
        )
        .register(
            gauge(format!("{}_last_completed_timestamp_seconds", task.name))
                .source(&task.last_completed)
                .unit("seconds")
                .help(
                    "Unix timestamp of the last normal completion, or zero before any completion",
                ),
        )
        .register(
            metric(format!("{}_duration_seconds", task.name))
                .source(LazyLock::force(&task.duration))
                .unit("seconds")
                .help("Task execution time including lock waits, panics and shutdown aborts"),
        );
}

pub(super) fn register(registry: &mut Registry<'static>) {
    for &task in TASKS {
        register_task(registry, task);
    }
    for (name, source, help) in [
        (
            "trim_series_processed",
            &TRIM_SERIES_PROCESSED,
            "Series attempted by background retention trimming",
        ),
        (
            "trim_samples_deleted",
            &TRIM_SAMPLES_DELETED,
            "Samples physically deleted by background retention trimming",
        ),
        (
            "trim_errors",
            &TRIM_ERRORS,
            "Series trim errors or failures to select a database",
        ),
        (
            "stale_ids_retired",
            &STALE_IDS_RETIRED,
            "Tombstoned ids retired after a complete postings pass, across all cleanup paths",
        ),
        (
            "stale_ids_passes_completed",
            &STALE_IDS_PASSES_COMPLETED,
            "Postings passes that retired a stale-id snapshot for one database, across all cleanup paths",
        ),
        (
            "optimize_passes_completed",
            &OPTIMIZE_PASSES_COMPLETED,
            "Periodic optimizer traversals completed for one existing database",
        ),
        (
            "trim_unused_dbs_removed",
            &TRIM_UNUSED_DBS_REMOVED,
            "Empty database indexes removed by cron",
        ),
    ] {
        registry.register(counter(name).source(source).help(help));
    }
    registry
        .register(
            gauge("optimize_last_pass_completed_timestamp_seconds")
                .source(&OPTIMIZE_LAST_PASS_COMPLETED)
                .unit("seconds")
                .help(
                    "Unix timestamp of the last completed database pass, or zero before any pass",
                ),
        )
        .register(
            gauge("trim_unused_dbs_running")
                .source(&TRIM_UNUSED_DBS_RUNNING)
                .help("Empty database index cleanups running now (inline on the main thread)"),
        );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::threads::{SingleFlight, spawn_task_single};
    use std::sync::mpsc;
    use std::time::Duration;

    fn wait_until(mut condition: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !condition() {
            assert!(Instant::now() < deadline, "task did not finish");
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn single_flight_skips_and_panics_are_counted_without_losing_the_flight() {
        static TASK: TaskMetrics = TaskMetrics::new("test_single", "test-task-metrics");
        static FLIGHT: SingleFlight = SingleFlight::new();
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        spawn_task_single(&TASK, &FLIGHT, move |_| {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        spawn_task_single(&TASK, &FLIGHT, |_| panic!("skipped job ran"));
        assert_eq!(TASK.started.load(Ordering::Relaxed), 1);
        assert_eq!(TASK.running.load(Ordering::Relaxed), 1);
        assert_eq!(TASK.skipped_inflight.load(Ordering::Relaxed), 1);
        release_tx.send(()).unwrap();
        wait_until(|| TASK.completed.load(Ordering::Relaxed) == 1);

        spawn_task_single(&TASK, &FLIGHT, |_| panic!("caught task panic"));
        wait_until(|| TASK.panics.load(Ordering::Relaxed) == 1);
        wait_until(|| TASK.running.load(Ordering::Relaxed) == 0);
        assert_eq!(TASK.started.load(Ordering::Relaxed), 2);
        assert_eq!(TASK.completed.load(Ordering::Relaxed), 1);
        assert_eq!(TASK.duration.snapshot().count, 2);
        assert!(FLIGHT.try_start().is_some());
    }

    #[test]
    fn an_abort_does_not_publish_a_completion_timestamp() {
        let task = TaskMetrics::new("test_abort", "test");
        let run = task.start();
        run.aborted.set(true);
        drop(run);
        assert_eq!(task.completed.load(Ordering::Relaxed), 0);
        assert_eq!(task.last_completed.load(Ordering::Relaxed), 0);
        assert_eq!(task.running.load(Ordering::Relaxed), 0);
        assert_eq!(task.duration.snapshot().count, 1);
    }

    #[test]
    fn concurrent_runs_and_unwinding_balance_lifecycle() {
        let task = TaskMetrics::new("test", "test");
        let first = task.start();
        let second = task.start();
        assert_eq!(task.running.load(Ordering::Relaxed), 2);
        drop(first);
        assert_eq!(task.running.load(Ordering::Relaxed), 1);
        assert!(task.last_completed.load(Ordering::Relaxed) > 0);
        let last = task.last_completed.load(Ordering::Relaxed);
        // Mark an abort locally so this test never changes the process-wide shutdown flag.
        second.aborted.set(true);
        drop(second);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _run = task.start();
            panic!("task failure");
        }));
        assert!(result.is_err());
        assert_eq!(task.running.load(Ordering::Relaxed), 0);
        assert_eq!(task.started.load(Ordering::Relaxed), 3);
        assert_eq!(task.completed.load(Ordering::Relaxed), 1);
        assert_eq!(task.panics.load(Ordering::Relaxed), 1);
        assert_eq!(task.duration.snapshot().count, 3);
        assert_eq!(task.last_completed.load(Ordering::Relaxed), last);
    }
}
