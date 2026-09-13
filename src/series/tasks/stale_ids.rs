use super::utils::find_next_db;
use crate::common::sync::lock;
use crate::is_shutting_down;
use crate::series::index::{IndexKey, TIMESERIES_INDEX};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, Mutex};

const STALE_ID_BATCH_SIZE: usize = 50;

#[derive(Default)]
struct StaleIdContext {
    db: i32,
    cursor: Option<IndexKey>,
}

static STALE_ID_CLEANUP_CONTEXT: LazyLock<Mutex<StaleIdContext>> =
    LazyLock::new(|| Mutex::new(StaleIdContext::default()));
static IN_STALE_ID_CLEANUP: AtomicBool = AtomicBool::new(false);

struct StaleIdCleanupGuard;

impl Drop for StaleIdCleanupGuard {
    fn drop(&mut self) {
        IN_STALE_ID_CLEANUP.store(false, Ordering::SeqCst);
    }
}

fn set_stale_id_cleanup_cursor(db: i32, cursor: Option<IndexKey>) {
    let mut context = lock(&STALE_ID_CLEANUP_CONTEXT);
    if cursor.is_none() {
        context.db = find_next_db(db).unwrap_or(0);
    } else {
        context.db = db;
    }
    context.cursor = cursor;
}

fn process_stale_id_batch(db: i32, cursor: Option<IndexKey>) -> Option<IndexKey> {
    let index_guard = TIMESERIES_INDEX.pin();
    let index = index_guard.get(&db)?;
    let mut state = 0;
    index.with_postings_mut(&mut state, move |postings, _| {
        postings.remove_stale_ids(cursor, STALE_ID_BATCH_SIZE)
    })
}

fn process_db_until_done(db: i32, start_cursor: Option<IndexKey>) -> Option<IndexKey> {
    let mut cursor = start_cursor;

    loop {
        if is_shutting_down() {
            return cursor;
        }

        let next_cursor = process_stale_id_batch(db, cursor);
        next_cursor.as_ref()?;
        // yield to avoid stalling index writes.
        std::thread::yield_now();

        cursor = next_cursor;
    }
}

fn acquire_run_lock() -> Option<StaleIdCleanupGuard> {
    let result =
        IN_STALE_ID_CLEANUP.compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed);

    if let Err(true) = result {
        // Another cleanup is already in progress
        return None;
    }

    Some(StaleIdCleanupGuard)
}

pub(in crate::series) fn remove_stale_series_internal() {
    if is_shutting_down() {
        return;
    }

    let Some(_cleanup_guard) = acquire_run_lock() else {
        return;
    };

    let (db, cursor) = {
        let mut context = lock(&STALE_ID_CLEANUP_CONTEXT);
        (context.db, context.cursor.take())
    };

    let new_cursor = process_db_until_done(db, cursor);
    set_stale_id_cleanup_cursor(db, new_cursor);
}

/// This function is intended to be called when we want to aggressively clean up all stale series,
/// for instance after slot migrations on the source shard. It should be used with caution since it can
/// be a long-running operation that can stall index writes.
/// ## Note
/// Must be called in a separate thread to avoid blocking the main thread
pub(in crate::series) fn remove_all_stale_series_internal() {
    let Some(_cleanup_guard) = acquire_run_lock() else {
        return;
    };

    let index = TIMESERIES_INDEX.pin();
    let mut dbs: Vec<i32> = index.keys().copied().collect();
    dbs.sort_unstable();
    drop(index);

    for db in dbs {
        if is_shutting_down() {
            break;
        }

        process_db_until_done(db, None);
    }

    let mut context = lock(&STALE_ID_CLEANUP_CONTEXT);
    context.db = 0;
    context.cursor = None;
}

pub(crate) fn remove_stale_series_ids_incremental() {
    std::thread::spawn(remove_stale_series_internal);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::series::TimeSeries;
    use crate::series::index::{get_db_index, next_timeseries_id};
    use serial_test::serial;

    fn reset_test_state() {
        TIMESERIES_INDEX.pin().retain(|_, _| false);

        let mut context = lock(&STALE_ID_CLEANUP_CONTEXT);
        context.db = 0;
        context.cursor = None;

        IN_STALE_ID_CLEANUP.store(false, Ordering::SeqCst);
    }

    fn make_series(id: u64, idx: usize) -> TimeSeries {
        let mut series = TimeSeries::new();
        series.id = id;
        series.labels = format!(r#"metric_1{{region="us-east-1",host="h{idx}"}}"#)
            .parse()
            .unwrap();
        series
    }

    fn populate_db_for_stale_batch(db: i32, entries: usize) {
        let index = get_db_index(db);

        for i in 0..entries {
            let id = next_timeseries_id();
            let key = format!("series:{db}:{i}");
            let series = make_series(id, i);
            index.index_timeseries(&series, key.as_bytes());
            index.mark_id_as_stale(id);
        }
    }

    fn ensure_db_exists(db: i32) {
        let index = get_db_index(db);
        let _ = index.count();
    }

    #[test]
    #[serial]
    fn stale_id_cursor_stays_on_same_db_when_cursor_exists() {
        reset_test_state();

        ensure_db_exists(2);
        ensure_db_exists(4);

        let cursor = Some(IndexKey::from("host=h1"));
        set_stale_id_cleanup_cursor(2, cursor.clone());

        let context = lock(&STALE_ID_CLEANUP_CONTEXT);
        assert_eq!(context.db, 2);
        assert_eq!(context.cursor, cursor);
    }

    #[test]
    #[serial]
    fn stale_id_cursor_advances_db_when_cursor_is_none() {
        reset_test_state();

        ensure_db_exists(2);
        ensure_db_exists(4);

        set_stale_id_cleanup_cursor(2, None);

        let context = lock(&STALE_ID_CLEANUP_CONTEXT);
        assert_eq!(context.db, 4);
        assert_eq!(context.cursor, None);
    }

    #[test]
    #[serial]
    fn process_stale_id_batch_returns_cursor_for_large_db() {
        reset_test_state();

        populate_db_for_stale_batch(1, STALE_ID_BATCH_SIZE + 10);

        let next_cursor = process_stale_id_batch(1, None);
        assert!(next_cursor.is_some());
    }

    #[test]
    #[serial]
    fn process_db_until_done_exhausts_cursor() {
        reset_test_state();

        populate_db_for_stale_batch(1, STALE_ID_BATCH_SIZE + 10);

        let final_cursor = process_db_until_done(1, None);
        assert_eq!(final_cursor, None);
    }
}
