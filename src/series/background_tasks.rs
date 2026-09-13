use crate::common::hash::BuildNoHashHasher;
use crate::common::logging::log_debug;
use crate::is_shutting_down;
use crate::series::index::TIMESERIES_INDEX;
use crate::series::index::persistence::is_loading_active;
use crate::series::tasks::{
    optimize_indices_for_db, process_series_trim, remove_stale_series_ids_incremental,
};
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use valkey_module::Context;
use valkey_module_macros::cron_event_handler;

const STALE_ID_CLEANUP_INTERVAL: Duration = Duration::from_secs(20);
const RETENTION_CLEANUP_INTERVAL: Duration = Duration::from_secs(10);
const INDEX_OPTIMIZE_INTERVAL: Duration = Duration::from_secs(60);
const DB_CLEANUP_INTERVAL: Duration = Duration::from_secs(300);

type DispatchMap = papaya::HashMap<u64, Vec<TaskType>, BuildNoHashHasher<u64>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskType {
    TrimSeries,
    RemoveStaleSeries,
    OptimizeIndices,
    TrimUnusedDbs,
}

static CRON_TICKS: AtomicU64 = AtomicU64::new(0);
static CRON_INTERVAL_MS: AtomicU64 = AtomicU64::new(100);
static DISPATCH_MAP: LazyLock<DispatchMap> = LazyLock::new(DispatchMap::default);

pub(crate) fn init_background_tasks(ctx: &Context) {
    let interval_ms = get_ticks_interval(ctx);
    CRON_INTERVAL_MS.store(interval_ms, Ordering::Relaxed);

    let cron_interval = Duration::from_millis(interval_ms);

    register_task(
        ctx,
        RETENTION_CLEANUP_INTERVAL,
        cron_interval,
        TaskType::TrimSeries,
    );
    register_task(
        ctx,
        STALE_ID_CLEANUP_INTERVAL,
        cron_interval,
        TaskType::RemoveStaleSeries,
    );
    register_task(
        ctx,
        INDEX_OPTIMIZE_INTERVAL,
        cron_interval,
        TaskType::OptimizeIndices,
    );
    register_task(
        ctx,
        DB_CLEANUP_INTERVAL,
        cron_interval,
        TaskType::TrimUnusedDbs,
    );
}

fn register_task(ctx: &Context, task_interval: Duration, cron_interval: Duration, task: TaskType) {
    if cron_interval.is_zero() {
        panic!("register_task: cron_interval is zero");
    }

    let floored = floor_duration(task_interval, cron_interval);
    if floored.is_zero() {
        ctx.log_warning(&format!(
            "register_task: interval is zero (task_interval={task_interval:?}, cron_interval={cron_interval:?})",
        ));
        return;
    }

    let interval_ticks = ticks_for_interval(floored, cron_interval);

    let map = DISPATCH_MAP.pin();
    map.update_or_insert_with(
        interval_ticks,
        |handlers| {
            if handlers.contains(&task) {
                handlers.clone()
            } else {
                let mut v = handlers.clone();
                v.push(task);
                v
            }
        },
        || vec![task],
    );
}

#[inline]
fn ticks_for_interval(task_interval: Duration, cron_interval: Duration) -> u64 {
    let task_ms = task_interval.as_millis() as u64;
    let cron_ms = cron_interval.as_millis() as u64;
    std::cmp::max(1, task_ms / cron_ms)
}

fn floor_duration(d: Duration, interval: Duration) -> Duration {
    if interval.is_zero() {
        return Duration::ZERO;
    }
    let nanos = d.as_nanos();
    let int_nanos = interval.as_nanos();
    let floored = (nanos / int_nanos) * int_nanos;

    if floored == 0 {
        return Duration::ZERO;
    }

    // Saturate instead of truncating if it does not fit u64 nanos.
    if floored > u64::MAX as u128 {
        Duration::from_nanos(u64::MAX)
    } else {
        Duration::from_nanos(floored as u64)
    }
}

fn trim_unused_dbs() {
    let index = TIMESERIES_INDEX.pin();
    index.retain(|db, ts_index| {
        if ts_index.is_empty() {
            log_debug(format!("Removing unused db {db} from index"));
            false
        } else {
            true
        }
    });
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

    let ticks = CRON_TICKS.fetch_add(1, Ordering::Relaxed);
    let map = DISPATCH_MAP.pin();

    for (&interval, handlers) in map.iter() {
        if ticks.is_multiple_of(interval) {
            for task in handlers {
                dispatch_background_task(*task);
            }
        }
    }
}

fn get_hz(ctx: &Context) -> u64 {
    let server_info = ctx.server_info("");
    server_info
        .field("hz")
        .and_then(|v| v.parse_integer().ok())
        .unwrap_or(10) as u64
}

fn get_ticks_interval(ctx: &Context) -> u64 {
    get_ticks_interval_from_hz(get_hz(ctx))
}

#[inline]
fn get_ticks_interval_from_hz(hz: u64) -> u64 {
    let hz = if hz == 0 { 10 } else { hz };
    1000 / hz
}
