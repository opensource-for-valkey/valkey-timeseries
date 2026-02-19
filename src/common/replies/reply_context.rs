use crate::common::context::replies::{
    reply, reply_error_string, reply_with_bulk_string, reply_with_key, reply_with_simple_string,
};
use crate::series::index::{TimeSeriesIndexGuard, get_db_index};
use std::ops::Deref;
use std::os::raw::c_long;
use valkey_module::logging::ValkeyLogLevel;
use valkey_module::redisvalue::ValkeyValueKey;
use valkey_module::{
    Context, RedisModule_GetSelectedDb, RedisModule_SelectDb, Status,
    VALKEYMODULE_POSTPONED_ARRAY_LEN, ValkeyResult, raw,
};

/// Fanout reply context
///
/// A thin wrapper around the underlying `RedisModuleCtx` that provides convenience
/// helpers for generating replies and managing the selected database while handling
/// fanout responses.
///
/// This struct restores the original selected DB when dropped if it changed during
/// the lifetime of the `FanoutContext`.
///
/// # Invariant
/// The `FanoutContext` must ALWAYS be created and used from the Valkey main thread.
/// In the codebase this is guaranteed because the only way to get a `FanoutContext`
/// is from a callback invoked by `exec_command`, which executes on the main thread.
pub struct FanoutContext {
    save_db: Option<i32>,
    ctx: Context,
    raw_ctx: *mut raw::RedisModuleCtx,
}

impl FanoutContext {
    pub(crate) fn new(ctx: *mut raw::RedisModuleCtx) -> Self {
        Self {
            save_db: None,
            ctx: Context { ctx },
            raw_ctx: ctx,
        }
    }

    /// Log a message at the specified `level` using the underlying context.
    pub fn log(&self, level: ValkeyLogLevel, message: &str) {
        let context = Context { ctx: self.raw_ctx };
        context.log(level, message);
    }

    /// Convenience logging helpers
    pub fn log_debug(&self, message: &str) {
        self.log(ValkeyLogLevel::Debug, message);
    }

    pub fn log_notice(&self, message: &str) {
        self.log(ValkeyLogLevel::Notice, message);
    }

    pub fn log_verbose(&self, message: &str) {
        self.log(ValkeyLogLevel::Verbose, message);
    }

    pub fn log_warning(&self, message: &str) {
        self.log(ValkeyLogLevel::Warning, message);
    }

    /// Switch the selected DB for this context. The original DB is saved on first
    /// switch so it can be restored when the `FanoutContext` is dropped.
    pub fn set_current_db(&mut self, db: i32) {
        if self.save_db.is_none() {
            self.save_db = Some(self.get_current_db());
        }
        unsafe {
            RedisModule_SelectDb.unwrap()(self.raw_ctx, db);
        }
    }

    /// Return the currently selected DB index from the underlying context.
    pub fn get_current_db(&self) -> i32 {
        unsafe { RedisModule_GetSelectedDb.unwrap()(self.raw_ctx) }
    }

    /// Reply with a 64-bit integer value.
    pub fn reply_with_i64(&self, value: i64) -> Status {
        raw::reply_with_long_long(self.raw_ctx, value)
    }

    /// Reply with a double-precision floating point value.
    pub fn reply_with_f64(&self, value: f64) -> Status {
        raw::reply_with_double(self.raw_ctx, value)
    }

    /// Reply with a boolean value.
    pub fn reply_with_bool(&self, value: bool) -> Status {
        raw::reply_with_bool(self.raw_ctx, value.into())
    }

    /// Reply with a simple string.
    pub fn reply_with_simple_string(&self, s: &str) -> Status {
        reply_with_simple_string(self.raw_ctx, s)
    }

    /// Reply with an error string.
    pub fn reply_error_string(&self, s: &str) -> Status {
        reply_error_string(self.raw_ctx, s)
    }

    /// Reply with a bulk string.
    pub fn reply_with_bulk_string(&self, value: &str) -> Status {
        reply_with_bulk_string(self.raw_ctx, value)
    }

    /// Reply with a NULL value.
    pub fn reply_with_null(&self) -> Status {
        raw::reply_with_null(self.raw_ctx)
    }

    /// Start an array reply with the given length.
    pub fn reply_with_array(&self, len: usize) -> Status {
        raw::reply_with_array(self.raw_ctx, len as c_long)
    }

    /// Start a map reply with the given length.
    pub fn reply_with_map(&self, len: usize) -> Status {
        raw::reply_with_map(self.raw_ctx, len as c_long)
    }

    /// Start a postponed-length array reply.
    pub fn reply_with_postponed_array(&self) -> Status {
        raw::reply_with_array(self.raw_ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN as c_long)
    }

    /// Reply with a `ValkeyValueKey` (integer/string/bulk/etc.).
    pub fn reply_with_key(&self, result: ValkeyValueKey) -> Status {
        reply_with_key(self.raw_ctx, result)
    }

    /// Forward a `ValkeyResult` to the reply machinery.
    #[allow(clippy::must_use_candidate)]
    pub fn reply(&self, result: ValkeyResult) -> Status {
        reply(self.raw_ctx, result)
    }

    /// Get the index guard for the currently selected DB.
    pub fn get_db_index(&self) -> TimeSeriesIndexGuard<'_> {
        let db = self.get_current_db();
        get_db_index(db)
    }
}

impl Drop for FanoutContext {
    fn drop(&mut self) {
        if let Some(db) = self.save_db {
            self.set_current_db(db);
        }
    }
}

impl Deref for FanoutContext {
    type Target = Context;

    fn deref(&self) -> &Self::Target {
        &self.ctx
    }
}
