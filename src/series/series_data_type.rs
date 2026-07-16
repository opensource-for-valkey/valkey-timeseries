use std::ffi::CString;
use valkey_module::{Context, REDISMODULE_AUX_BEFORE_RDB};
use valkey_module::{
    RedisModuleDefragCtx, RedisModuleDigest, RedisModuleString, native_types::ValkeyType,
};
use valkey_module::{RedisModuleIO, RedisModuleTypeMethods, logging, raw};

use crate::common::logging::log_debug;
use crate::series::TimeSeries;
use crate::series::defrag_series;
use crate::series::index::persistence::{load_index_from_rdb, save_index_to_rdb};
use crate::series::index::{get_db_index, next_timeseries_id};
use crate::series::serialization::{rdb_load_series, rdb_save_series};
use std::os::raw::{c_int, c_void};
use std::sync::atomic::{AtomicBool, Ordering};
use valkey_module::digest::Digest;
use valkey_module::logging::{ValkeyLogLevel, log_io_error};
use valkey_module::server_events::FlushSubevent;
use valkey_module_macros::flush_event_handler;

/// TimeSeries Module data type RDB encoding version.
///
/// We register the same module type name as RedisTimeSeries (`TSDB-TYPE`)
/// with an incompatible payload format; RTS 8.6 uses encver 9. The loaders
/// reject any encver other than this one (see `rdb_load_series`) so a
/// foreign payload fails cleanly instead of being misparsed — RTS→valkey
/// RDB/DUMP migration is explicitly not supported (compat plan §7.4).
pub(crate) const TIMESERIES_TYPE_ENCODING_VERSION: i32 = 1;

pub static VK_TIME_SERIES_TYPE: ValkeyType = ValkeyType::new(
    "TSDB-TYPE",
    TIMESERIES_TYPE_ENCODING_VERSION,
    RedisModuleTypeMethods {
        version: valkey_module::TYPE_METHOD_VERSION,
        rdb_load: Some(rdb_load),
        rdb_save: Some(rdb_save),
        aof_rewrite: Some(aof_rewrite),
        free: Some(free),
        mem_usage: Some(mem_usage),
        digest: Some(series_digest),
        aux_load: Some(aux_load),
        aux_save: None,
        aux_save_triggers: REDISMODULE_AUX_BEFORE_RDB as i32,
        free_effort: None,
        unlink: Some(unlink),
        copy: Some(copy),
        defrag: Some(defrag),
        mem_usage2: None,
        free_effort2: None,
        unlink2: None,
        copy2: None,
        aux_save2: Some(aux_save),
    },
);

/// This variable is used to indicate that the module is in the midst of a `FLUSHALL` or `FLUSHDB` operation.
/// It is set to true when the operation starts and set to false when it ends.
///
/// We use this to optimize index management during deletes. To be specific, if all keys are being removed, there's
/// no need for individual updates as happens per single `free`. However, in Valkey, the "flushdb" notification is
/// only sent after ALL the appropriate keys are deleted, so from the `free` callback alone it is not obvious if
/// all keys are being removed.
///
/// All interested parties should use the `is_flushing_in_process` function to check if a flush is in process.
static IS_FLUSHING: AtomicBool = AtomicBool::new(false);

#[flush_event_handler]
fn flushed_event_handler(_ctx: &Context, flush_event: FlushSubevent) {
    IS_FLUSHING.store(FlushSubevent::Started == flush_event, Ordering::SeqCst);
}

#[inline]
pub fn is_flushing_in_process() -> bool {
    IS_FLUSHING.load(Ordering::Relaxed)
}

fn remove_series_from_index(ts: &TimeSeries) {
    // if we are in the middle of a flush, we don't want to remove the series from the index
    // since the entire index will be cleared by the flush operation.
    if is_flushing_in_process() {
        return;
    }
    let Some(db) = ts._db else {
        log_debug(format!(
            "Skipping index removal for series id {} because _db is unassigned",
            ts.id
        ));
        return;
    };
    let index = get_db_index(db);
    index.remove_timeseries(ts);
}

unsafe extern "C" fn rdb_save(rdb: *mut RedisModuleIO, value: *mut c_void) {
    let series = unsafe { &*value.cast::<TimeSeries>() };
    rdb_save_series(series, rdb);
}

unsafe extern "C" fn rdb_load(rdb: *mut RedisModuleIO, enc_ver: c_int) -> *mut c_void {
    match rdb_load_series(rdb, enc_ver) {
        Ok(series) => Box::into_raw(Box::new(series)) as *mut std::ffi::c_void,
        Err(e) => {
            // Warning level: this aborts the RESTORE / RDB load, and the
            // message is the operator's only clue (e.g. a RedisTimeSeries
            // payload rejected by the encoding-version guard).
            logging::log_warning(format!("Failed to load series from RDB. {e:?}"));
            std::ptr::null_mut()
        }
    }
}

/// Persists the postings index as an RDB aux field (`AUX_BEFORE_RDB`). Runs in the BGSAVE fork
/// child, so all locking inside is non-blocking; on contention nothing is written and the loader
/// falls back to the per-key rebuild. Registered as `aux_save2`, so writing nothing omits the aux
/// field from the RDB entirely.
unsafe extern "C" fn aux_save(rdb: *mut RedisModuleIO, when: c_int) {
    if when != REDISMODULE_AUX_BEFORE_RDB as c_int {
        return;
    }
    save_index_to_rdb(rdb);
}

/// Preloads the postings index from the RDB aux field. With `AUX_BEFORE_RDB` this runs before any
/// key loads; the per-key `loaded` path then reduces to a `has_id` check per series.
unsafe extern "C" fn aux_load(rdb: *mut RedisModuleIO, encver: c_int, when: c_int) -> c_int {
    if when != REDISMODULE_AUX_BEFORE_RDB as c_int {
        return raw::Status::Ok as c_int;
    }
    // Same guard as `rdb_load`: an aux field written by a foreign TSDB-TYPE
    // (or a future encoding) cannot be consumed positionally — fail the load
    // cleanly rather than misparse the stream.
    if encver != TIMESERIES_TYPE_ENCODING_VERSION {
        logging::log_warning(format!(
            "Refusing TSDB-TYPE aux RDB payload with encoding version {encver} \
             (this module writes version {TIMESERIES_TYPE_ENCODING_VERSION})"
        ));
        return raw::Status::Err as c_int;
    }
    load_index_from_rdb(rdb)
}

unsafe extern "C" fn mem_usage(value: *const c_void) -> usize {
    let series = unsafe { &*(value as *mut TimeSeries) };
    series.memory_usage()
}

#[allow(unused)]
unsafe extern "C" fn free(value: *mut c_void) {
    let sm = value.cast::<TimeSeries>();
    let series = unsafe { Box::from_raw(sm) };
    // todo: it may be helpful to push index deletion to a background thread
    remove_series_from_index(&series);
    drop(series);
}

#[allow(non_snake_case, unused)]
unsafe extern "C" fn copy(
    from_key: *mut RedisModuleString,
    to_key: *mut RedisModuleString,
    value: *const c_void,
) -> *mut c_void {
    // NOTE: this callback runs on the main thread, which already holds the module GIL. It must
    // therefore NOT lock a thread-safe/detached context (that would deadlock) and must not invoke
    // commands. We only clone the value here and defer index maintenance to the `copy_to`
    // keyspace-notification handler, which runs with a valid context and the destination db
    // already selected.
    let old_series = unsafe { &*value.cast::<TimeSeries>() };
    let mut new_series = old_series.clone();
    // The destination is a brand-new series: assign a fresh id and drop rule/source linkage.
    // `_db` is intentionally left unset; the `copy_to` handler assigns it while indexing.
    new_series._db = None;
    new_series.id = next_timeseries_id();
    new_series.src_series = None;
    new_series.rules.clear();
    Box::into_raw(Box::new(new_series)).cast::<c_void>()
}

unsafe extern "C" fn unlink(_key: *mut RedisModuleString, value: *const c_void) {
    if value.is_null() {
        return;
    }
    let series = unsafe { &*value.cast::<TimeSeries>() };
    remove_series_from_index(series);
}

unsafe extern "C" fn defrag(
    _ctx: *mut RedisModuleDefragCtx,
    _key: *mut RedisModuleString,
    value: *mut *mut c_void,
) -> c_int {
    if value.is_null() {
        return 0;
    }
    // Convert the pointer to a TimeSeries so we can operate on it.
    let series: &mut TimeSeries = unsafe { &mut *(*value).cast::<TimeSeries>() };
    match defrag_series(series) {
        Ok(_) => 0,
        Err(_) => 1,
    }
}

/// # Safety
/// Raw handler for the Timeseries digest callback.
unsafe extern "C" fn series_digest(md: *mut RedisModuleDigest, value: *mut c_void) {
    let mut digest = Digest::new(md);
    let val = unsafe { &*(value.cast::<TimeSeries>()) };
    val.debug_digest(&mut digest);
    digest.end_sequence();
}

unsafe extern "C" fn aof_rewrite(
    aof: *mut RedisModuleIO,
    key: *mut RedisModuleString,
    value: *mut c_void,
) {
    // IMPORTANT: this callback may run inside a forked snapshot child (both for ordinary AOF
    // rewrites and for atomic slot migration). In the child the module GIL mutex is inherited in a
    // locked state, so we must NOT lock a context or invoke commands (e.g. `DUMP`). Instead we
    // serialize the value directly through the type's own `rdb_save` callback, which needs no lock,
    // and emit `TS._RESTORE key <payload>` — reconstructed by `ts_asm_restore_cmd` on replay.
    let raw_type = *VK_TIME_SERIES_TYPE.raw_type.borrow();
    let payload = unsafe {
        raw::RedisModule_SaveDataTypeToString.unwrap()(std::ptr::null_mut(), value, raw_type)
    };
    if payload.is_null() {
        log_io_error(
            aof,
            ValkeyLogLevel::Warning,
            "Failed to serialize series for AOF rewrite",
        );
        return;
    }

    let restore_cmd = CString::new("TS._RESTORE").unwrap();
    let format_str = CString::new("ss").unwrap(); // two RedisModuleString arguments
    unsafe {
        raw::RedisModule_EmitAOF.unwrap()(
            aof,
            restore_cmd.as_ptr(),
            format_str.as_ptr(),
            key,
            payload,
        );
        // We passed a NULL context to SaveDataTypeToString, so the string is not auto-managed;
        // release the reference we own.
        raw::RedisModule_FreeString.unwrap()(std::ptr::null_mut(), payload);
    }
}
