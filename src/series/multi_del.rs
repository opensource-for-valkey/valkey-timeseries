use crate::common::Timestamp;
use crate::common::context::{get_acl_user, is_acl_enforced};
use crate::config::num_threads;
use crate::labels::filters::SeriesSelector;
use crate::series::index::{PostingsBitmap, get_timeseries_index, with_timeseries_postings};
use crate::series::series_data_type::VK_TIME_SERIES_TYPE;
use crate::series::{
    CompactionOp, SeriesGuardMut, SeriesRef, TimeSeries, TimestampRange, apply_compaction,
};
use blart::AsBytes;
use croaring::bitmap64::Bitmap64Iterator;
use orx_parallel::ParIter;
use orx_parallel::ParallelizableCollectionMut;
use smallvec::SmallVec;
use std::ops::{Deref, DerefMut};
use valkey_module::{
    AclPermissions, Context, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString,
};

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
    match ctx.open_key_writable(key).delete() {
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
        let key = ctx.create_string(key.as_ref());
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
    let mut total_deleted = 0;
    let start_arg = ctx.create_string(start_ts.to_string());
    let end_arg = ctx.create_string(end_ts.to_string());
    let mut series = series;
    let res = series
        .par_mut()
        .map(|guard| guard.remove_range(start_ts, end_ts))
        .collect::<Vec<_>>();

    // Run compaction after deletions
    for (i, (deleted, ts)) in res.iter().zip(series.iter_mut()).enumerate() {
        if let Err(err) = deleted {
            ctx.log_warning(&format!(
                "Got error removing range from series {}: {err:?}",
                keys[i].to_string_lossy()
            ));
        }
        if let Ok(deleted) = deleted {
            if *deleted == 0 {
                continue;
            }

            total_deleted += *deleted;
            ctx.notify_keyspace_event(NotifyEvent::MODULE, "ts.del", &keys[i]);
            // run compaction if needed
            apply_compaction(
                ctx,
                ts,
                CompactionOp::RemoveRange {
                    start: start_ts,
                    end: end_ts,
                },
            )?;
            // Propagate the resolved effect; see `delete_series_by_selectors`.
            ctx.replicate("TS.DEL", &[&keys[i], &start_arg, &end_arg]);
        }
    }

    Ok(total_deleted)
}

fn fetch_series_batch<'a>(
    ctx: &'a Context,
    cursor: &mut Bitmap64Iterator<'_>,
    batch_size: usize,
) -> (Vec<SeriesGuardMut<'a>>, Vec<ValkeyString>) {
    let user = get_acl_user(ctx);
    let is_user_client = is_acl_enforced(ctx);
    let has_all_keys_permission = if !is_user_client {
        true
    } else {
        ctx.acl_check_key_permission(&user, &ctx.create_string("*"), &AclPermissions::DELETE)
            .is_ok()
    };

    let index = get_timeseries_index(ctx);

    let mut stale_ids: SmallVec<[SeriesRef; 8]> = SmallVec::new();
    let mut result: Vec<SeriesGuardMut<'a>> = Vec::with_capacity(batch_size);
    let mut keys: Vec<ValkeyString> = Vec::with_capacity(batch_size);

    // Two phases per round, and the split is load-bearing: opening a key runs the server's
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
                    Some(k) => resolved.push((id, ctx.create_string(k.as_bytes()))),
                    None => stale_ids.push(id),
                }
                if resolved.len() == wanted {
                    break;
                }
            }
        }

        let exhausted = resolved.len() < wanted;

        for (id, key) in resolved {
            if is_user_client
                && !has_all_keys_permission
                && ctx
                    .acl_check_key_permission(&user, &key, &AclPermissions::DELETE)
                    .is_err()
            {
                continue;
            }

            match get_timeseries(ctx, &key) {
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

fn get_timeseries<'a>(
    ctx: &'a Context,
    key: &ValkeyString,
) -> ValkeyResult<Option<SeriesGuardMut<'a>>> {
    let value_key = ctx.open_key_writable(key);
    match value_key.get_value::<TimeSeries>(&VK_TIME_SERIES_TYPE) {
        Ok(Some(series)) => Ok(Some(SeriesGuardMut { series })),
        Ok(None) => Ok(None),
        Err(_e) => Err(ValkeyError::WrongType),
    }
}
