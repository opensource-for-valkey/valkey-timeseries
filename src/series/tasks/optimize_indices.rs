use crate::common::logging::log_debug;
use crate::common::sync::lock;
use crate::common::threads::spawn;
use crate::is_shutting_down;
use crate::series::index::{IndexKey, TIMESERIES_INDEX, get_db_index};
use crate::series::tasks::utils::find_next_db;
use std::sync::{LazyLock, Mutex};

const INDEX_OPTIMIZE_BATCH_SIZE: usize = 128;
/// Lists per write-lock acquisition in a full pass ([`optimize_index_fully`]).
const FULL_PASS_BATCH_SIZE: usize = 256;

#[derive(Default)]
struct OptimizeContext {
    db: i32,
    cursor: Option<IndexKey>,
}

static LAZY_OPTIMIZE_CURSOR: LazyLock<Mutex<OptimizeContext>> =
    LazyLock::new(|| Mutex::new(OptimizeContext::default()));

#[inline]
fn set_optimize_cursor(db: i32, cursor: Option<IndexKey>) {
    let mut context = lock(&LAZY_OPTIMIZE_CURSOR);
    if cursor.is_none() {
        context.db = find_next_db(db).unwrap_or(0);
    } else {
        context.db = db;
    }
    context.cursor = cursor;
}

pub fn optimize_indices_for_db() {
    spawn(optimize_indices_internal);
}

/// Optimizes all of `db`'s posting lists now, rather than leaving them to the periodic task.
///
/// Meant for a bulk change to the index, such as an atomic slot migration indexing or retiring
/// every series of the migrated slots. The periodic task visits [`INDEX_OPTIMIZE_BATCH_SIZE`]
/// lists a minute across all dbs, so reaching a large index would take hours. Blocks until
/// done, so call it from a background thread.
pub(in crate::series) fn optimize_index_fully(db: i32) {
    let map = TIMESERIES_INDEX.pin();
    let Some(index) = map.get(&db) else {
        return;
    };
    // A flush drops the db's index; there is no point finishing a detached one.
    let replaced = || {
        !TIMESERIES_INDEX
            .pin()
            .get(&db)
            .is_some_and(|current| std::ptr::eq(current, index))
    };
    if index.optimize_all(FULL_PASS_BATCH_SIZE, || is_shutting_down() || replaced()) {
        log_debug(format!("Optimized every posting list of db {db}"));
    }
}

/// Process optimization for a specific database, called by the dispatcher.
pub(in crate::series) fn optimize_indices_internal() {
    let (db, cursor) = {
        let mut context = lock(&LAZY_OPTIMIZE_CURSOR);
        (context.db, context.cursor.take())
    };

    let db_exists = {
        let index = TIMESERIES_INDEX.pin();
        index.get(&db).is_some()
    };

    if !db_exists {
        if cursor.is_some() {
            log_debug(format!(
                "Skipping optimize indices for db {db}: tracked db is missing"
            ));
        }
        set_optimize_cursor(db, None);
        return;
    }

    let index = get_db_index(db);
    let new_cursor = index.optimize_incremental(cursor, INDEX_OPTIMIZE_BATCH_SIZE);

    set_optimize_cursor(db, new_cursor);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::series::TimeSeries;
    use crate::series::index::next_timeseries_id;
    use serial_test::serial;

    fn reset_test_state() {
        TIMESERIES_INDEX.pin().retain(|_, _| false);

        let mut context = lock(&LAZY_OPTIMIZE_CURSOR);
        context.db = 0;
        context.cursor = None;
    }

    fn make_series(id: u64, idx: usize) -> TimeSeries {
        let mut series = TimeSeries::new();
        series.id = id;
        series.labels = format!(r#"metric_1{{region="us-east-1",host="h{idx}"}}"#)
            .parse()
            .unwrap();
        series
    }

    fn populate_db(db: i32, entries: usize) {
        let index = get_db_index(db);
        for i in 0..entries {
            let id = next_timeseries_id();
            let key = format!("series:{db}:{i}");
            let series = make_series(id, i);
            index.index_timeseries(&series, key.as_bytes());
        }
    }

    fn current_context() -> (i32, Option<IndexKey>) {
        let context = lock(&LAZY_OPTIMIZE_CURSOR);
        (context.db, context.cursor.clone())
    }

    fn set_context(db: i32, cursor: Option<IndexKey>) {
        let mut context = lock(&LAZY_OPTIMIZE_CURSOR);
        context.db = db;
        context.cursor = cursor;
    }

    #[test]
    #[serial]
    fn optimize_stays_on_same_db_while_cursor_exists() {
        reset_test_state();
        populate_db(1, INDEX_OPTIMIZE_BATCH_SIZE + 25);
        populate_db(2, 5);
        set_context(1, None);

        optimize_indices_internal();

        let (db, cursor) = current_context();
        assert_eq!(db, 1);
        assert!(cursor.is_some());
    }

    #[test]
    #[serial]
    fn optimize_advances_to_next_db_when_cursor_exhausted() {
        reset_test_state();
        populate_db(1, 2);
        populate_db(2, 2);
        set_context(1, None);

        optimize_indices_internal();

        let (db, cursor) = current_context();
        assert_eq!(db, 2);
        assert_eq!(cursor, None);
    }

    #[test]
    #[serial]
    fn optimize_db_zero_is_processed_as_valid_db() {
        reset_test_state();
        populate_db(0, INDEX_OPTIMIZE_BATCH_SIZE + 10);
        populate_db(2, 3);

        optimize_indices_internal();

        let (db, cursor) = current_context();
        assert_eq!(db, 0);
        assert!(cursor.is_some());
    }

    #[test]
    #[serial]
    fn missing_db_with_cursor_advances_without_panicking() {
        reset_test_state();
        populate_db(4, 3);

        set_context(3, Some(IndexKey::from("host=h1")));

        optimize_indices_internal();

        let (db, cursor) = current_context();
        assert_eq!(db, 4);
        assert_eq!(cursor, None);
    }
}
