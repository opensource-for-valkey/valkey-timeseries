use crate::common::Timestamp;
use crate::common::acl::KeyAccess;
use crate::common::context::{create_key_string, notify_keyspace_event};
use crate::common::threads::{ParMutRayon, request_par_threads};
use crate::config::num_threads;
use crate::error::TsdbResult;
use crate::labels::filters::SeriesSelector;
use crate::series::index::{PostingsBitmap, get_timeseries_index, with_timeseries_postings};
use crate::series::{
    SeriesGuardMut, SeriesRef, TimeSeries, TimestampRange, batch_feeds_itself, is_key_in_use,
    resync_rules_after_removal, try_get_timeseries_mut,
};
use ahash::AHashSet;
use blart::AsBytes;
use croaring::bitmap64::Bitmap64Iterator;
use orx_parallel::Par;
use smallvec::SmallVec;
use std::ops::{Deref, DerefMut};
use valkey_module::{AclPermissions, Context, ValkeyError, ValkeyResult, ValkeyString};

/// Apply `TS.MDEL` on this node and propagate its *effects* to this node's replicas and AOF.
///
/// The command itself is deliberately not replicated verbatim by any caller:
///
/// * In cluster mode each shard runs its own slice inside a fanout RPC handler. That context has
///   no client argv for `ReplicateVerbatim` to copy, and a replica replaying `TS.MDEL` would fan
///   the command out across the cluster a second time. Before this, the clustered branch simply
///   returned without replicating at all, so a cluster-mode `TS.MDEL` never reached any replica.
/// * Even standalone, a verbatim replay re-resolves both the filter and the timestamp bounds on
///   the replica. Wall-clock bounds (`TimestampValue::Now` and `Relative`) resolve through
///   `as_timestamp` against whichever node is evaluating them, so a replica applying the replayed
///   command can delete a different window than the primary did.
///
/// Propagating resolved effects avoids both: `DEL <key>` per key removed, and
/// `TS.DEL <key> <start> <end>` with absolute millisecond bounds per series whose range changed.
/// `TS.DEL` is the exact operation performed here (`remove_range` followed by
/// `CompactionOp::RemoveRange`), so the replica re-derives the same downstream compaction the
/// primary did, the same way it does for a client-issued `TS.DEL`.
pub fn delete_series_by_selectors(
    ctx: &Context,
    selectors: &[SeriesSelector],
    date_range: Option<TimestampRange>,
) -> ValkeyResult<usize> {
    match date_range {
        Some(range) => {
            let (start_ts, end_ts) = range.get_timestamps(None);
            handle_delete_range(ctx, selectors, start_ts, end_ts)
        }
        None => handle_delete_keys(ctx, selectors),
    }
}

fn delete_key(ctx: &Context, key: &ValkeyString) -> ValkeyResult<usize> {
    match ctx.open_key_writable(key).and_then(|k| k.delete()) {
        Ok(_) => Ok(1),
        Err(e) => {
            let msg = format!(
                "multi-del: error deleting key {}: {:?}",
                key.to_string_lossy(),
                e
            );
            ctx.log_warning(&msg);
            Ok(0)
        }
    }
}

fn handle_delete_keys(ctx: &Context, filters: &[SeriesSelector]) -> ValkeyResult<usize> {
    // get keys from ids
    let index = get_timeseries_index(ctx);
    let keys = index.keys_for_selectors(ctx, filters, Some(AclPermissions::DELETE))?;
    let mut total_deleted = 0;
    for key in keys {
        let key = create_key_string(ctx, key.as_ref());
        if delete_key(ctx, &key)? == 0 {
            continue;
        }
        total_deleted += 1;
        ctx.replicate("DEL", &[&key]);
    }
    Ok(total_deleted)
}

fn handle_delete_range(
    ctx: &Context,
    filters: &[SeriesSelector],
    start: Timestamp,
    end: Timestamp,
) -> ValkeyResult<usize> {
    // we iterate over ids instead of keys to be able to do parallel deletions
    let ids = with_timeseries_postings(ctx, |index| {
        let ids = index.postings_for_selectors(filters)?.into_owned();
        Ok::<PostingsBitmap, ValkeyError>(ids)
    })?;

    let num_threads = usize::max(num_threads(), 2);
    let mut total_deleted = 0;
    ctx.log_notice(&format!(
        "Starting deletion of range [{start}, {end}] for {} series. Num threads: {num_threads}",
        ids.cardinality()
    ));

    let batch_size = usize::max(ids.cardinality() as usize / num_threads, 32);
    let mut iter = ids.iter();
    loop {
        let (series_batch, keys_batch) = fetch_series_batch(ctx, &mut iter, batch_size);
        if series_batch.is_empty() {
            break;
        }

        let deleted = delete_range_batch(ctx, series_batch, &keys_batch, start, end)?;
        total_deleted += deleted;
    }
    Ok(total_deleted)
}

fn delete_range_batch(
    ctx: &Context,
    series: Vec<SeriesGuardMut>,
    keys: &[ValkeyString],
    start_ts: Timestamp,
    end_ts: Timestamp,
) -> ValkeyResult<usize> {
    let start_arg = ctx.create_string(start_ts.to_string());
    let end_arg = ctx.create_string(end_ts.to_string());
    let range = RemovedRange {
        start: start_ts,
        end: end_ts,
        start_arg: &start_arg,
        end_arg: &end_arg,
    };

    // A batch holding a rule's source and, further down, its destination is applied one series
    // at a time, in id order: remove, resync the rules, replicate. That is the order a replica
    // replays the `TS.DEL`s in, and the source's resync rewrites destination buckets the
    // destination's own removal must then see. Removing every range first and resyncing
    // afterwards kept a bucket the replica deleted, and the source's cascade opened the
    // destination while the batch still held its guard.
    let members: AHashSet<&[u8]> = keys.iter().map(|key| key.as_slice()).collect();
    if batch_feeds_itself(ctx, series.iter().map(|guard| guard.deref()), |key| {
        members.contains(key)
    }) {
        drop(series);
        let mut total_deleted = 0;
        for key in keys {
            let Ok(Some(mut guard)) = try_get_timeseries_mut(ctx, key, None) else {
                continue;
            };
            let removed = guard.remove_range(start_ts, end_ts);
            total_deleted += finish_removal(ctx, key, &mut guard, removed, &range);
        }
        return Ok(total_deleted);
    }

    // Otherwise each series' resync writes only series outside the batch, so the removals can
    // run in parallel ahead of it.
    let mut series = series;
    let threads = request_par_threads(series.len(), series.iter().map(|g| g.total_samples).sum());
    let res = series
        .par_mut_rayon()
        .num_threads(threads)
        .map(|guard| guard.remove_range(start_ts, end_ts))
        .collect::<Vec<_>>();

    let mut total_deleted = 0;
    for ((removed, ts), key) in res.into_iter().zip(series.iter_mut()).zip(keys) {
        total_deleted += finish_removal(ctx, key, ts, removed, &range);
    }
    Ok(total_deleted)
}

/// The range a `TS.MDEL` removes, with its replicated arguments.
struct RemovedRange<'a> {
    start: Timestamp,
    end: Timestamp,
    start_arg: &'a ValkeyString,
    end_arg: &'a ValkeyString,
}

/// What follows one series' removal: the event, the rule resync and the replicated `TS.DEL`.
/// Returns the number of samples removed.
fn finish_removal(
    ctx: &Context,
    key: &ValkeyString,
    series: &mut TimeSeries,
    removed: TsdbResult<usize>,
    range: &RemovedRange,
) -> usize {
    match removed {
        Err(err) => {
            ctx.log_warning(&format!(
                "Got error removing range from series {}: {err:?}",
                key.to_string_lossy()
            ));
            0
        }
        Ok(0) => 0,
        Ok(deleted) => {
            notify_keyspace_event(ctx, c"ts.del", key);
            // The removal is committed: a rule failure is logged and the batch carries on, so
            // this series and every later one is still replicated.
            resync_rules_after_removal(ctx, key, series, range.start, range.end);
            // Propagate the resolved effect; see `delete_series_by_selectors`.
            ctx.replicate("TS.DEL", &[key, range.start_arg, range.end_arg]);
            deleted
        }
    }
}

fn fetch_series_batch<'a>(
    ctx: &'a Context,
    cursor: &mut Bitmap64Iterator<'_>,
    batch_size: usize,
) -> (Vec<SeriesGuardMut<'a>>, Vec<ValkeyString>) {
    // Resolves the caller's identity once (reusing a fan-out request's already-resolved
    // handle, when there is one) and precomputes whether it can reach every key, instead
    // of re-resolving the ACL user by name on every `acl_check_key_permission` call below.
    let access = KeyAccess::new(ctx, AclPermissions::DELETE);

    let index = get_timeseries_index(ctx);

    let mut stale_ids: SmallVec<[SeriesRef; 8]> = SmallVec::new();
    let mut result: Vec<SeriesGuardMut<'a>> = Vec::with_capacity(batch_size);
    let mut keys: Vec<ValkeyString> = Vec::with_capacity(batch_size);

    // Two phases per round, and the split is important: opening a key runs the server's
    // lazy-expiry check, which reaps an expired series through this module's `unlink` callback
    // and takes the postings *write* lock on this same thread. Holding the read guard across
    // `get_timeseries` therefore deadlocks. See `series::index::querier::resolve_series_keys`.
    //
    // Keep going until the batch is full or the cursor runs dry, so an all-stale round still
    // reports progress rather than looking like the end of the postings.
    while result.len() < batch_size {
        let wanted = batch_size - result.len();
        let mut resolved: Vec<(SeriesRef, ValkeyString)> = Vec::with_capacity(wanted);

        {
            let postings_guard = index.get_postings();
            let postings = postings_guard.deref();
            for id in cursor.by_ref() {
                match postings.get_key_by_id(id) {
                    Some(k) => resolved.push((id, create_key_string(ctx, k.as_bytes()))),
                    None => stale_ids.push(id),
                }
                if resolved.len() == wanted {
                    break;
                }
            }
        }

        let exhausted = resolved.len() < wanted;

        for (id, key) in resolved {
            if !access.allows(&key) {
                continue;
            }

            match try_get_timeseries_mut(ctx, &key, None) {
                // The series is still there; only this write is refused. Marking its id stale
                // would drop a live series from the index. Skip it, as FILTER mode skips a key
                // it cannot delete.
                Err(err) if is_key_in_use(&err) => {
                    ctx.log_warning(&format!(
                        "multi-del: skipping key {}: in use",
                        key.to_string_lossy()
                    ));
                }
                // Missing, or now holds another kind of value.
                Err(_) | Ok(None) => stale_ids.push(id),
                Ok(Some(series)) => {
                    result.push(series);
                    keys.push(key);
                }
            }
        }

        if exhausted {
            break;
        }
    }

    if !stale_ids.is_empty() {
        let mut postings_guard = index.get_postings_mut();
        let postings = postings_guard.deref_mut();
        for id in stale_ids {
            postings.mark_id_as_stale(id);
        }
    }

    (result, keys)
}
