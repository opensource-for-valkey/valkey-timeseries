use crate::common::context::{get_current_db, set_current_db};
use crate::common::logging::{log_debug, log_warning};
use crate::common::metrics::task::{self as metrics, TaskRun};
use crate::common::sync::lock;
use crate::common::threads::{LockGil, ParMutRayon, SingleFlight, spawn_task_single};
use crate::is_shutting_down;
use crate::series::tasks::utils::{fetch_series_batch, find_next_db};
use orx_parallel::Par;
use std::sync::atomic::Ordering;
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

/// Held while a trim run is queued or running.
static TRIM_RUN: SingleFlight = SingleFlight::new();

/// Physically drops expired samples that the write path left behind (see
/// `TimeSeries::trim_lazily`).
///
/// Runs on every node, primaries and replicas alike, and is deliberately not replicated. The
/// retention floor is derived from the series' own last sample, not the clock, so a replica
/// that has applied the same stream computes the same floor, and the trim removes only
/// samples that reads already hide on both sides. Nodes may differ in what they *store*
/// between runs, never in what they return. A role check here would need a replicated
/// trim command to go with it; without one, replicas would hold on to every head-chunk
/// prefix the write path deferred.
pub fn process_series_trim() {
    if is_shutting_down() {
        return;
    }
    // Takes the GIL and fans out on the shared pool under it (R2): must not be a pool job.
    spawn_task_single(&metrics::RETENTION_TRIM, &TRIM_RUN, process_trim_internal);
}

fn process_trim_internal(run: &TaskRun<'_>) {
    let mut processed = 0;
    let start_db = {
        let context = lock(&SERIES_TRIM_CURSORS);
        context.db
    };

    let mut db = start_db;
    let mut first_iteration = true;

    for _ in 0..MAX_TRIM_TURNS {
        if run.check_shutdown() {
            break;
        }

        let cursor = {
            let context = lock(&SERIES_TRIM_CURSORS);
            context.cursor
        };

        let ctx = MODULE_CONTEXT.lock_gil();
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
        metrics::RETENTION_TRIM_ERRORS.fetch_add(1, Ordering::Relaxed);
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

    let total_deletes: usize = batch
        .par_mut_rayon()
        .map(|series| match series.trim() {
            Ok(deletes) => deletes,
            Err(_) => {
                metrics::RETENTION_TRIM_ERRORS.fetch_add(1, Ordering::Relaxed);
                log_warning(format!(
                    "Failed to trim series {}",
                    series.prometheus_metric_name()
                ));
                0
            }
        })
        .sum();

    metrics::RETENTION_TRIM_SERIES_PROCESSED.fetch_add(processed as u64, Ordering::Relaxed);
    metrics::RETENTION_TRIM_SAMPLES_DELETED.fetch_add(total_deletes as u64, Ordering::Relaxed);

    let mut context = lock(&SERIES_TRIM_CURSORS);
    context.cursor = last_processed;

    if processed > 0 {
        log_debug(format!(
            "Processed: {processed} Deleted Samples: {total_deletes} samples"
        ));
    }

    (processed, db)
}
