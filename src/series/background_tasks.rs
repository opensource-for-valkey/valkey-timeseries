use crate::common::logging::log_debug;
use crate::common::metrics::task;
use crate::is_shutting_down;
use crate::series::index::TIMESERIES_INDEX;
use crate::series::index::persistence::is_loading_active;
use crate::series::tasks::{
    optimize_indices_for_db, process_series_trim, remove_stale_series_ids_incremental,
};
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use valkey_module::Context;
use valkey_module_macros::cron_event_handler;

const STALE_ID_CLEANUP_INTERVAL: Duration = Duration::from_secs(20);
const RETENTION_CLEANUP_INTERVAL: Duration = Duration::from_secs(10);
const INDEX_OPTIMIZE_INTERVAL: Duration = Duration::from_secs(60);
const DB_CLEANUP_INTERVAL: Duration = Duration::from_secs(300);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskType {
    TrimSeries,
    RemoveStaleSeries,
    OptimizeIndices,
    TrimUnusedDbs,
}

/// A periodic task's monotonic deadline, in milliseconds since [`TASK_EPOCH`]. Starts due.
struct ScheduledTask {
    task: TaskType,
    interval_ms: u64,
    next_due_ms: AtomicU64,
}

impl ScheduledTask {
    const fn new(task: TaskType, interval: Duration) -> Self {
        Self {
            task,
            interval_ms: interval.as_millis() as u64,
            next_due_ms: AtomicU64::new(0),
        }
    }

    /// Returns whether the task is due at `now_ms`, advancing the deadline if so. Deadlines stay
    /// on the interval grid; after a stall the task runs once and re-anchors instead of bursting.
    fn poll(&self, now_ms: u64) -> bool {
        let due = self.next_due_ms.load(Ordering::Relaxed);
        if now_ms < due {
            return false;
        }
        let mut next = due + self.interval_ms;
        if next <= now_ms {
            next = now_ms + self.interval_ms;
        }
        self.next_due_ms.store(next, Ordering::Relaxed);
        true
    }
}

static TASK_EPOCH: LazyLock<Instant> = LazyLock::new(Instant::now);

static SCHEDULE: [ScheduledTask; 4] = [
    ScheduledTask::new(TaskType::TrimSeries, RETENTION_CLEANUP_INTERVAL),
    ScheduledTask::new(TaskType::RemoveStaleSeries, STALE_ID_CLEANUP_INTERVAL),
    ScheduledTask::new(TaskType::OptimizeIndices, INDEX_OPTIMIZE_INTERVAL),
    ScheduledTask::new(TaskType::TrimUnusedDbs, DB_CLEANUP_INTERVAL),
];

fn monotonic_ms() -> u64 {
    TASK_EPOCH.elapsed().as_millis() as u64
}

fn trim_unused_dbs() {
    task::TRIM_UNUSED_DBS_RUNNING.fetch_add(1, Ordering::Relaxed);
    let index = TIMESERIES_INDEX.pin();
    index.retain(|db, ts_index| {
        if ts_index.is_empty() {
            task::TRIM_UNUSED_DBS_REMOVED.fetch_add(1, Ordering::Relaxed);
            log_debug(format!("Removing unused db {db} from index"));
            false
        } else {
            true
        }
    });
    task::TRIM_UNUSED_DBS_RUNNING.fetch_sub(1, Ordering::Relaxed);
}

fn dispatch_background_task(task: TaskType) {
    match task {
        TaskType::TrimSeries => process_series_trim(),
        TaskType::RemoveStaleSeries => {
            remove_stale_series_ids_incremental();
        }
        TaskType::OptimizeIndices => optimize_indices_for_db(),
        TaskType::TrimUnusedDbs => trim_unused_dbs(),
    }
}

#[cron_event_handler]
fn __cron_event_handler(_ctx: &Context, _hz: u64) {
    if is_shutting_down() || is_loading_active() {
        return;
    }

    // Deadlines are in elapsed time; hz (static or dynamic) only sets how often they are polled.
    let now = monotonic_ms();
    for entry in &SCHEDULE {
        if entry.poll(now) {
            dispatch_background_task(entry.task);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const INTERVAL_MS: u64 = 10_000;

    fn simulate(interval_ms: u64, tick_times: impl IntoIterator<Item = u64>) -> Vec<u64> {
        let task = ScheduledTask::new(TaskType::TrimSeries, Duration::from_millis(interval_ms));
        tick_times
            .into_iter()
            .filter(|&now| task.poll(now))
            .collect()
    }

    fn grid(firings: usize) -> Vec<u64> {
        (0..firings as u64).map(|k| k * INTERVAL_MS).collect()
    }

    #[test]
    fn first_poll_is_due() {
        let task = ScheduledTask::new(TaskType::TrimSeries, Duration::from_millis(INTERVAL_MS));
        assert!(task.poll(0));
        assert!(!task.poll(1));
        assert!(!task.poll(9_999));
        assert!(task.poll(10_000));
    }

    #[test]
    fn fires_on_interval_regardless_of_cron_period() {
        // hz 500, 300 (the server truncates to 3 ms), 100, 10 and 1.
        for period in [2u64, 3, 10, 100, 1000] {
            let fired = simulate(INTERVAL_MS, (0..=60_000).step_by(period as usize));
            assert_eq!(fired.len(), 7, "period {period}");
            for (k, &at) in fired.iter().enumerate() {
                let target = k as u64 * INTERVAL_MS;
                assert!(
                    (target..target + period).contains(&at),
                    "period {period}: firing {k} at {at}"
                );
            }
        }
    }

    #[test]
    fn hz_change_mid_run_keeps_interval() {
        // hz 10 until 25 s, hz 500 until 50 s, hz 1 until 90 s.
        let ticks = (0..25_000)
            .step_by(100)
            .chain((25_000..50_000).step_by(2))
            .chain((50_000..=90_000).step_by(1000));
        assert_eq!(simulate(INTERVAL_MS, ticks), grid(10));
    }

    #[test]
    fn jittered_cron_has_no_cumulative_drift() {
        // dynamic-hz can change the period on every tick: seeded periods in [1, 1000] ms.
        let mut state = 0x2545_f491_4f6c_dd1du64;
        let mut now = 0;
        let mut ticks = vec![now];
        while now < 1_000_000 {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            now += 1 + (state >> 33) % 1000;
            ticks.push(now);
        }
        let fired = simulate(INTERVAL_MS, ticks);
        assert_eq!(fired.len(), 101);
        for (k, &at) in fired.iter().enumerate() {
            let target = k as u64 * INTERVAL_MS;
            assert!((target..target + 1000).contains(&at), "firing {k} at {at}");
        }
    }

    #[test]
    fn stall_fires_once_then_reanchors() {
        let ticks = (0..=5_000)
            .step_by(100)
            .chain([35_000])
            .chain((35_100..=50_000).step_by(100));
        assert_eq!(simulate(INTERVAL_MS, ticks), vec![0, 35_000, 45_000]);
    }
}
