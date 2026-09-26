//! This module subscribes to Valkey events to ensure secondary index consistency.
//!
use crate::common::context::{create_key_string, get_current_db, register_server_event_handler};
use crate::common::sync::lock;
use crate::series::index::asm::{
    add_delayed_indexing_key, clear_delayed_keys_in_db, clear_delayed_keys_map,
    is_key_in_slot_import, register_asm_event_handler,
};
use crate::series::index::bulk_build;
use crate::series::index::persistence::{
    on_loading_ended, on_loading_failed, on_loading_started, should_skip_load_indexing,
};
use crate::series::index::{
    TIMESERIES_INDEX, clear_all_timeseries_indexes, clear_timeseries_index, get_db_index,
    get_timeseries_index, get_timeseries_index_for_db, index_loaded_series, index_series_by_key,
};
use crate::series::{relink_renamed_series, try_get_timeseries_mut};
use std::os::raw::c_void;
use std::sync::Mutex;
use valkey_module::server_events::{
    FLUSH_SERVER_EVENTS_LIST, FlushSubevent, LoadingSubevent, PersistenceSubevent,
    SWAPDB_SERVER_EVENTS_LIST,
};
use valkey_module::{Context, NotifyEvent, ValkeyResult, logging, raw};
use valkey_module_macros::{loading_event_handler, persistence_event_handler};

/// Logs persistence activity. Nothing here touches the ASM delayed-indexing queue: that queue
/// belongs to a slot import on this node, and a save that fails (a BGSAVE out of disk, say)
/// says nothing about the import. Clearing it on a failed save — as this did — left every
/// imported series unindexed, invisible to MRANGE/MGET until a restart rebuilt the index.
#[persistence_event_handler]
fn __persistence_event_handler(ctx: &Context, persistence_event: PersistenceSubevent) {
    match persistence_event {
        PersistenceSubevent::RdbStart => ctx.log_notice("RDB persistence started"),
        PersistenceSubevent::AofStart => ctx.log_notice("AOF persistence started"),
        PersistenceSubevent::SyncRdbStart => ctx.log_notice("Sync RDB persistence started"),
        PersistenceSubevent::SyncAofStart => ctx.log_notice("Sync AOF persistence started"),
        PersistenceSubevent::Ended => ctx.log_notice("Persistence operation ended"),
        PersistenceSubevent::Failed => ctx.log_warning("Persistence operation failed"),
    }
}

/// Load lifecycle for the persisted postings index (see `persistence.rs`): a `*Started` event
/// opens the load window (enabling the loaded-series counter and the `loaded`-event fast path);
/// after a successful load, preloaded indexes are swept for dangling ids and verified against
/// the loaded count; after a failed load, the preloaded state cannot be trusted, so drop it and
/// let the natural indexing paths rebuild.
#[loading_event_handler]
fn __loading_event_handler(ctx: &Context, loading_event: LoadingSubevent) {
    match loading_event {
        LoadingSubevent::RdbStarted | LoadingSubevent::ReplStarted => {
            on_loading_started();
            bulk_build::on_load_started(false);
        }
        LoadingSubevent::AofStarted => {
            on_loading_started();
            // AOF loads keep the per-key indexing path (see bulk_build.rs module docs).
            bulk_build::on_load_started(true);
        }
        LoadingSubevent::Ended => {
            // Drain the bulk-build buffer first (synchronous — the index must be complete
            // before serving resumes); the aux-preload reconciliation sweep then runs in the
            // background for dbs that took the preload fast path instead.
            bulk_build::on_load_ended();
            on_loading_ended(load_keys_expired(ctx));
        }
        LoadingSubevent::Failed => {
            bulk_build::on_load_failed();
            on_loading_failed();
        }
    }
}

/// Keys the load that just ended discarded as already expired (`rdb_last_load_keys_expired`).
/// Unreadable counts as zero: the reconciliation digest still guards every other drift.
fn load_keys_expired(ctx: &Context) -> u64 {
    ctx.server_info("persistence")
        .field_c("rdb_last_load_keys_expired")
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0)
}

fn handle_key_move(ctx: &Context, key: &[u8], old_db: i32) {
    let new_db = get_current_db(ctx);
    // fetch the series from the new
    let valkey_key = create_key_string(ctx, key);
    let Ok(Some(mut series)) = try_get_timeseries_mut(ctx, &valkey_key, None) else {
        logging::log_warning("Failed to load series for key move");
        return;
    };

    // MOVE deletes the source key, so `unlink` has normally retired it from the old db's
    // index already; the key-checked removal makes this a quiet no-op then.
    let old_index = get_db_index(old_db);
    old_index.remove_timeseries_for_key(&series, key);

    // add the series to the new db index
    series._db = Some(new_db);
    let new_index = get_db_index(new_db);
    let mut postings = new_index.get_postings_mut();
    index_loaded_series(&mut postings, &mut series, key);
}

fn handle_key_rename(ctx: &Context, old_key: &[u8], new_key: &[u8]) {
    let index = get_timeseries_index(ctx);
    let key = create_key_string(ctx, new_key);
    let Ok(Some(mut series)) = try_get_timeseries_mut(ctx, &key, None) else {
        logging::log_warning("Failed to load series for key rename");
        return;
    };
    series.key = new_key.into();
    index.reindex_timeseries(&series, old_key, new_key);
    // Compaction partners link to each other by key.
    relink_renamed_series(ctx, &series, old_key);
}

/// Handle the "restore" event, which is triggered for each key restored from disk during server startup
/// or slot migration. It collects the keys for later indexing if we're in the middle of an ASM slot import,
/// otherwise it indexes them immediately.
fn handle_key_restore(ctx: &Context, key: &[u8]) {
    let db = get_current_db(ctx);
    if is_key_in_slot_import(key) {
        add_delayed_indexing_key(db, key);
        return;
    }
    // Fast path: during an RDB load whose index for this db was preloaded from the aux payload,
    // skip the per-key work entirely (string allocation, keyspace lookup, index lock).
    // `rdb_load` has already assigned `_db` and counted the series; post-load verification in
    // `persistence.rs` catches any aux/keyspace drift.
    if should_skip_load_indexing(db) {
        return;
    }
    // Fallback (bulk_build.rs): during an RDB/replication load window whose index was
    // not preloaded, buffer the series for one sorted bulk build at load end instead of paying
    // per-label ART inserts per key. Falls through when buffering is inactive for this window
    // (AOF load, memory cap crossed, or runtime `restore` outside a load window).
    if bulk_build::try_buffer_loaded_key(ctx, db, key) {
        return;
    }
    index_series_by_key(ctx, key);
}

/// Indexes the destination of a `COPY` command. The type `copy` callback cannot maintain the index
/// itself, so this runs from the `copy_to` keyspace notification. `COPY` fires this event for every
/// key type, so non-timeseries keys are ignored quietly.
fn handle_key_copy(ctx: &Context, key: &[u8]) {
    let db = get_current_db(ctx);
    let valkey_key = create_key_string(ctx, key);
    let Ok(Some(mut series)) = try_get_timeseries_mut(ctx, &valkey_key, None) else {
        return;
    };
    series._db = Some(db);
    let index = get_db_index(db);
    let mut postings = index.get_postings_mut();
    index_loaded_series(&mut postings, &mut series, key);
}

static RENAME_FROM_KEY: Mutex<Vec<u8>> = Mutex::new(vec![]);
static MOVE_FROM_DB: Mutex<i32> = Mutex::new(-1);

pub(crate) fn generic_key_events_handler(
    ctx: &Context,
    _event_type: NotifyEvent,
    event: &str,
    key: &[u8],
) {
    hashify::fnc_map!(event.as_bytes(),
        "loaded" => {
            // Handle the "loaded" event, which is triggered for each key loaded from disk during server startup.
            handle_key_restore(ctx, key);
        },
        "move_from" => {
            *lock(&MOVE_FROM_DB) = get_current_db(ctx);
        },
        "move_to" => {
            let mut guard = lock(&MOVE_FROM_DB);
            let old_db = *guard;
            *guard = -1;
            if old_db != -1 {
                 handle_key_move(ctx, key, old_db);
            }
        },
        "rename_from" => {
            *lock(&RENAME_FROM_KEY) = key.to_vec();
        },
        "rename_to" => {
            let mut old_key = lock(&RENAME_FROM_KEY);
            if !old_key.is_empty() {
                handle_key_rename(ctx, &old_key, key);
                old_key.clear();
            }
        },
        "restore" => {
            handle_key_restore(ctx, key);
        },
        "copy_to" => {
            // The type `copy` callback cannot touch the index (it runs on the main thread with the
            // GIL held). Index the freshly-copied destination key here, where we have a valid
            // context with the destination db selected.
            handle_key_copy(ctx, key);
        },
        _ => {}
    );
}

/// Re-dispatch a server event to the `valkey-module` handlers this module's own subscription
/// displaced.
///
/// `RM_SubscribeToServerEvent` keeps exactly one callback per (module, event) and *replaces* an
/// existing one rather than chaining. `valkey-module` subscribes its own dispatcher for any event
/// that has `#[..._event_handler]` functions, and it does so before `initialize` runs — so every
/// raw subscription taken here silently discards that dispatcher and everything behind it. This
/// module subscribes to FLUSHDB and SWAPDB directly because it needs the event payload
/// (`RedisModuleFlushInfo::dbnum`, `RedisModuleSwapDbInfo`), which the macro surface does not
/// expose, so the dispatcher has to be driven from here instead.
///
/// This is not hypothetical: `series_data_type::flushed_event_handler` — the `IS_FLUSHING` flag
/// that lets the per-key `free`/`unlink` callbacks skip index maintenance during a flush — never
/// ran at all, leaving the flag permanently false and that fast path dead.
fn dispatch_displaced_handlers<T: Copy>(
    ctx: *mut raw::RedisModuleCtx,
    handlers: &[fn(&Context, T)],
    payload: T,
) {
    if handlers.is_empty() {
        return;
    }
    let ctx = Context::new(ctx);
    for handler in handlers {
        handler(&ctx, payload);
    }
}

unsafe extern "C" fn on_flush_event(
    ctx: *mut raw::RedisModuleCtx,
    _eid: raw::RedisModuleEvent,
    sub_event: u64,
    data: *mut c_void,
) {
    if sub_event == raw::REDISMODULE_SUBEVENT_FLUSHDB_END {
        let fi: &raw::RedisModuleFlushInfo = unsafe { &*(data as *mut raw::RedisModuleFlushInfo) };

        if fi.dbnum == -1 {
            // FLUSHALL, and the implicit flush a replica performs on a full resync: every
            // database is gone. The per-key `free`/`unlink` callbacks that would otherwise
            // retire each series are deliberately skipped while a flush is in progress, so
            // dropping the indexes here is the only thing that retires them — leaving this to
            // clear the delayed-keys map alone left the whole index dangling, every id pointing
            // at a key that no longer exists.
            clear_all_timeseries_indexes();
            clear_delayed_keys_map();
        } else {
            clear_timeseries_index(fi.dbnum);
            clear_delayed_keys_in_db(fi.dbnum);
        }
    };

    // After the index work, so `IS_FLUSHING` stays set across it and is cleared only once the
    // index agrees with the (now empty) keyspace.
    let flush_sub_event = if sub_event == raw::REDISMODULE_SUBEVENT_FLUSHDB_START {
        FlushSubevent::Started
    } else {
        FlushSubevent::Ended
    };
    dispatch_displaced_handlers(ctx, &FLUSH_SERVER_EVENTS_LIST, flush_sub_event);
}

fn swap_timeseries_index_dbs(from_db: i32, to_db: i32) {
    let guard = TIMESERIES_INDEX.guard();

    let first = get_timeseries_index_for_db(from_db, &guard);
    let second = get_timeseries_index_for_db(to_db, &guard);
    first.swap(second)
}

unsafe extern "C" fn on_swap_db_event(
    ctx: *mut raw::RedisModuleCtx,
    eid: raw::RedisModuleEvent,
    sub_event: u64,
    data: *mut c_void,
) {
    if eid.id == raw::REDISMODULE_EVENT_SWAPDB {
        let ei: &raw::RedisModuleSwapDbInfo =
            unsafe { &*(data as *mut raw::RedisModuleSwapDbInfo) };

        let from_db = ei.dbnum_first;
        let to_db = ei.dbnum_second;

        swap_timeseries_index_dbs(from_db, to_db);
    }

    // This module has no `#[swapdb_event_handler]` today, so this list is empty and the call
    // costs nothing — but see `dispatch_displaced_handlers`: without it, the first one added
    // would silently never run.
    dispatch_displaced_handlers(ctx, &SWAPDB_SERVER_EVENTS_LIST, sub_event);
}

pub(crate) fn register_server_event_handlers(ctx: &Context) -> ValkeyResult<()> {
    register_asm_event_handler(ctx);
    register_server_event_handler(ctx, raw::REDISMODULE_EVENT_FLUSHDB, Some(on_flush_event))?;
    register_server_event_handler(ctx, raw::REDISMODULE_EVENT_SWAPDB, Some(on_swap_db_event))?;
    Ok(())
}
