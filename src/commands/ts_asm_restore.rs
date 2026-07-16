//! Internal command emitted by the module's `aof_rewrite` callback.
//!
//! During an atomic slot migration (ASM), the source node snapshots each owned slot in a forked
//! child process using the module type's `aof_rewrite` callback. That callback cannot lock the
//! module GIL or invoke normal commands (e.g. `DUMP`) — doing so deadlocks the child, which has
//! inherited the GIL mutex in a locked state. Instead, `aof_rewrite` serializes the value directly
//! via the type's `rdb_save` callback (`RedisModule_SaveDataTypeToString`) and emits this command,
//! carrying the key and the serialized blob.
//!
//! On the destination node the emitted command stream is replayed like a replication feed, so this
//! handler reconstructs the `TimeSeries` via `rdb_load` (`RedisModule_LoadDataTypeFromString`),
//! stores it under the key, and indexes it. Indexing is deferred while a slot import is in progress
//! (see [`crate::series::index::server_events`]) so that "phantom" keys are not visible to queries
//! until the import completes.
//!
//! The same command is emitted during ordinary AOF rewrites when the AOF is written in command
//! format (i.e. without the RDB preamble), so this path is also exercised on plain AOF load.

use crate::common::context::get_current_db;
use crate::series::TimeSeries;
use crate::series::index::index_series_by_key;
use crate::series::index::server_events::{add_delayed_indexing_key, is_in_asm_slot_import};
use crate::series::series_data_type::{TIMESERIES_TYPE_ENCODING_VERSION, VK_TIME_SERIES_TYPE};
use std::os::raw::c_void;
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue, raw};

/// `TS._RESTORE key <serialized-payload>`
pub fn ts_asm_restore_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 3 {
        return Err(ValkeyError::WrongArity);
    }
    let key = &args[1];
    let payload = &args[2];

    // Reconstruct the series via the type's `rdb_load`. This mirrors `RESTORE` but reads the
    // buffer produced by `RedisModule_SaveDataTypeToString` (no version/CRC footer). The string
    // carries no encoding version of its own, so pass ours explicitly — the plain
    // `LoadDataTypeFromString` supplies encver 0, which `rdb_load_series`'s foreign-payload
    // guard would (correctly) reject.
    let raw_type = *VK_TIME_SERIES_TYPE.raw_type.borrow();
    let series_ptr: *mut c_void = unsafe {
        raw::RedisModule_LoadDataTypeFromStringEncver.unwrap()(
            payload.inner,
            raw_type,
            TIMESERIES_TYPE_ENCODING_VERSION,
        )
    };
    if series_ptr.is_null() {
        return Err(ValkeyError::Str("TSDB: failed to deserialize series"));
    }

    // `rdb_load` returns a `Box<TimeSeries>` leaked as a raw pointer; take ownership back.
    let mut series = unsafe { *Box::from_raw(series_ptr as *mut TimeSeries) };

    let db = get_current_db(ctx);
    series._db = Some(db);

    let writable_key = ctx.open_key_writable(key);
    if !writable_key.is_empty() {
        // A value already exists for this key; treat the restore as authoritative and replace it.
        writable_key.delete()?;
    }
    writable_key.set_value(&VK_TIME_SERIES_TYPE, series)?;

    // During a slot import we defer indexing until the import completes to avoid phantom reads.
    // Outside of an import (e.g. plain AOF command replay) we index immediately.
    if is_in_asm_slot_import() {
        add_delayed_indexing_key(db, key.as_slice());
    } else {
        index_series_by_key(ctx, key.as_slice());
    }

    Ok(ValkeyValue::SimpleStringStatic("OK"))
}
