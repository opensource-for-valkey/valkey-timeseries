use crate::common::context::{get_current_db, set_current_db};
use crate::common::logging::{log_debug, log_warning};
use crate::common::sync::lock;
use crate::common::threads::spawn_background;
use crate::is_shutting_down;
use crate::series::tasks::utils::{fetch_series_batch, find_next_db};
use orx_parallel::Par;
use orx_parallel::ParCollectionMut;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, Mutex};
use valkey_module::{Context, MODULE_CONTEXT, Status};

const MAX_TRIM_TURNS: usize = 5;
const SERIES_TRIM_BATCH_SIZE: usize = 25;

#[derive(Default)]
struct TrimContext {
    db: i32,
    cursor: u64,
}

// TODO: trim down if we don't have any items in a given db
static SERIES_TRIM_CURSORS: LazyLock<Mutex<TrimContext>> =
    LazyLock::new(|| Mutex::new(TrimContext::default()));

/// Set while a trim run is queued or running. A run waits on the module lock, so while the
/// main thread holds it for longer than the trim interval, the cron would otherwise start
/// another thread every tick.
static TRIM_IN_PROGRESS: AtomicBool = AtomicBool::new(false);

struct TrimRunGuard;

impl TrimRunGuard {
    fn acquire() -> Option<Self> {
        TRIM_IN_PROGRESS
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
            .then_some(TrimRunGuard)
    }
}

impl Drop for TrimRunGuard {
    fn drop(&mut self) {
        TRIM_IN_PROGRESS.store(false, Ordering::Release);
    }
}

pub fn process_series_trim() {
    if is_shutting_down() {
        return;
    }
    let Some(guard) = TrimRunGuard::acquire() else {
        return;
    };
    // Takes the module lock and fans out on the pool under it: must not be a pool job.
    // If the thread cannot be started, the job and the guard it owns are dropped.
    spawn_background("ts-series-trim", move || {
        let _guard = guard;
        process_trim_internal();
    });
}

fn process_trim_internal() {
    let mut processed = 0;
    let start_db = {
        let context = lock(&SERIES_TRIM_CURSORS);
        context.db
    };

    let mut db = start_db;
    let mut first_iteration = true;

    for _ in 0..MAX_TRIM_TURNS {
        if is_shutting_down() {
            break;
        }

        let cursor = {
            let context = lock(&SERIES_TRIM_CURSORS);
            context.cursor
        };

        let ctx = MODULE_CONTEXT.lock();
        let (subtotal, next_db) = trim_series(&ctx, db, cursor);
        processed += subtotal;

        if processed >= SERIES_TRIM_BATCH_SIZE {
            break;
        }

        db = next_db;
        if !first_iteration && db == start_db {
            break;
        }
        first_iteration = false;
    }
}

fn trim_series(ctx: &Context, db: i32, cursor: u64) -> (usize, i32) {
    let save_db = get_current_db(ctx);

    if set_current_db(ctx, db) == Status::Err {
        log_warning(format!("Failed to select db {db}"));
        return (0, db);
    }

    let mut batch = fetch_series_batch(ctx, cursor + 1, |series| {
        !series.retention.is_zero() && !series.is_empty()
    });

    set_current_db(ctx, save_db);

    if batch.is_empty() {
        let mut context = lock(&SERIES_TRIM_CURSORS);
        let db = find_next_db(context.db).unwrap_or(0);
        context.db = db;
        context.cursor = 0;
        return (0, db);
    }

    let last_processed = batch.last().map(|s| s.id).unwrap_or(0);
    let processed = batch.len();

    let total_deletes = batch
        .par_mut()
        .map(|series| match series.trim() {
            Ok(deletes) => deletes,
            Err(_) => {
                log_warning(format!(
                    "Failed to trim series {}",
                    series.prometheus_metric_name()
                ));
                0
            }
        })
        .sum();

    let mut context = lock(&SERIES_TRIM_CURSORS);
    context.cursor = last_processed;

    if processed > 0 {
        log_debug(format!(
            "Processed: {processed} Deleted Samples: {total_deletes} samples"
        ));
    }

    (processed, db)
}
