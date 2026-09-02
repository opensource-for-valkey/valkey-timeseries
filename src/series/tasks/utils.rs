use crate::common::context::create_key_string;
use crate::series::index::{TIMESERIES_INDEX, get_timeseries_index, with_timeseries_postings};
use crate::series::{SeriesGuardMut, SeriesRef, TimeSeries, get_timeseries_mut};
use std::sync::atomic::{AtomicI32, Ordering};
use valkey_module::Context;

pub(super) fn get_used_dbs() -> Vec<i32> {
    let index = TIMESERIES_INDEX.pin();
    let mut keys: Vec<i32> = index.keys().copied().collect();
    keys.sort_unstable();
    keys
}

pub(super) fn advance_db(cursor: &AtomicI32) -> i32 {
    let used_dbs = get_used_dbs();
    let current = cursor.load(Ordering::Relaxed);

    let next = used_dbs
        .iter()
        .find(|&&d| d > current)
        .copied()
        .or_else(|| used_dbs.first().copied())
        .unwrap_or(0);

    cursor.store(next, Ordering::Relaxed);
    next
}

pub(super) fn find_next_db(current: i32) -> Option<i32> {
    let index = TIMESERIES_INDEX.pin();
    // get the db with the lowest id greater than the current db
    index.keys().filter(|&&db| db > current).min().copied()
}

const SERIES_TRIM_BATCH_SIZE: usize = 25;
pub(super) fn fetch_series_batch(
    ctx: &'_ Context,
    start_id: SeriesRef,
    pred: fn(&TimeSeries) -> bool,
) -> Vec<SeriesGuardMut<'_>> {
    let (result, stale_ids) = with_timeseries_postings(ctx, |postings| {
        let all_postings = &postings.all_postings;
        let mut cursor = all_postings.cursor();
        cursor.reset_at_or_after(start_id);
        let mut stale_ids = Vec::new();
        let mut result = Vec::new();

        // loop through a max of 3 times to fill the batch, to allow skipping stale ids without returning a smaller batch
        for _ in 0..3 {
            let mut buf = [0_u64; SERIES_TRIM_BATCH_SIZE];
            let n = cursor.read_many(&mut buf);
            if n == 0 {
                break;
            }
            result.reserve(n);
            for &id in &buf[..n] {
                let Some(k) = postings.get_key_by_id(id) else {
                    continue;
                };

                let key = create_key_string(ctx, k.as_ref());
                let Ok(Some(series)) = get_timeseries_mut(ctx, &key, false, None) else {
                    stale_ids.push(id);
                    continue;
                };

                if pred(&series) {
                    result.push(series);
                    if result.len() >= SERIES_TRIM_BATCH_SIZE {
                        break;
                    }
                }
            }
        }

        (result, stale_ids)
    });

    if !stale_ids.is_empty() {
        let index = get_timeseries_index(ctx);
        for id in stale_ids {
            index.mark_id_as_stale(id)
        }
    }

    result
}
