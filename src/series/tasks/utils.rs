use crate::common::context::create_key_string;
use crate::series::index::{TIMESERIES_INDEX, get_timeseries_index, with_timeseries_postings};
use crate::series::{SeriesGuardMut, SeriesRef, TimeSeries, try_get_timeseries_mut};
use smallvec::SmallVec;
use valkey_module::Context;

pub(super) fn find_next_db(current: i32) -> Option<i32> {
    let index = TIMESERIES_INDEX.pin();
    // get the db with the lowest id greater than the current db
    index.keys().filter(|&&db| db > current).min().copied()
}

const SCAN_ROUNDS: usize = 3;
const BUF_SIZE: usize = 256;

/// One bounded scan of a db's series, starting at a given id.
pub(super) struct SeriesBatch<'a> {
    /// The series accepted by the predicate, at most `batch_size` of them, in id order.
    pub series: Vec<SeriesGuardMut<'a>>,
    /// The highest id whose fate this scan settled (accepted, rejected or stale), or `None` when
    /// no id at or after the start was found. Resume from the id after it.
    pub last_scanned: Option<SeriesRef>,
    /// The scan reached the end of the db's ids: nothing past `last_scanned` remains.
    pub exhausted: bool,
}

/// Reads up to [`SCAN_ROUNDS`] buffers of ids starting at `start_id`, returning the series that
/// pass `pred` (at most `batch_size`). Rejected or dangling ids don't count toward
/// `batch_size`, so a run of them doesn't end the scan early; instead `last_scanned` and
/// `exhausted` tell the caller whether an empty batch means "keep going" or "db is done".
pub(super) fn fetch_series_batch(
    ctx: &'_ Context,
    start_id: SeriesRef,
    batch_size: usize,
    pred: fn(&TimeSeries) -> bool,
) -> SeriesBatch<'_> {
    let mut stale_ids = SmallVec::<[SeriesRef; 8]>::new();
    let mut last_scanned = None;
    let mut exhausted = false;

    // Resolve the ids to keys under the postings read guard, then open them *after* the guard is
    // gone. Opening a key runs the server's lazy-expiry check, which reaps an expired series
    // through this module's `unlink` callback -- and that takes the postings write lock on this
    // same thread. See `series::index::querier::resolve_series_keys`.
    let resolved = with_timeseries_postings(ctx, |postings| {
        let mut cursor = postings.all_postings.cursor();
        cursor.reset_at_or_after(start_id);

        // Read up to `SCAN_ROUNDS` buffers of ids, so that entries skipped below (dangling keys,
        // or series rejected by `pred`) do not shrink the batch that is returned.
        let mut resolved = Vec::with_capacity(batch_size);

        for _ in 0..SCAN_ROUNDS {
            let mut buf = [0_u64; BUF_SIZE];
            let n = cursor.read_many(&mut buf);
            if n > 0 {
                last_scanned = Some(buf[n - 1]);
            }
            resolved.reserve(n);
            for &id in &buf[..n] {
                if let Some(k) = postings.get_key_by_id(id) {
                    resolved.push((id, create_key_string(ctx, k.as_ref())));
                } else {
                    stale_ids.push(id);
                }
            }
            // `read_many` fills the buffer unless it ran out of ids.
            if n < BUF_SIZE {
                exhausted = true;
                break;
            }
        }

        resolved
    });

    let mut series = Vec::with_capacity(batch_size);
    for (id, key) in resolved {
        if series.len() >= batch_size {
            // Ids from here on were not examined: resume at this one next time.
            last_scanned = id.checked_sub(1);
            exhausted = false;
            break;
        }
        let Ok(Some(s)) = try_get_timeseries_mut(ctx, &key, None) else {
            stale_ids.push(id);
            continue;
        };
        if pred(&s) {
            series.push(s);
        }
    }

    if !stale_ids.is_empty() {
        let index = get_timeseries_index(ctx);
        index.mark_ids_as_stale(&stale_ids);
    }

    SeriesBatch {
        series,
        last_scanned,
        exhausted,
    }
}
