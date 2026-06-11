use super::raw_replies::{
    IntoRawCtx, reply, reply_error_string, reply_with_array_len, reply_with_bulk_string,
    reply_with_simple_string,
};
use crate::series::index::{TimeSeriesIndexGuard, get_db_index};
use std::ops::Deref;
use std::os::raw::c_long;
use valkey_module::logging::ValkeyLogLevel;
use valkey_module::{
    Context, RedisModule_GetSelectedDb, Status, VALKEYMODULE_POSTPONED_ARRAY_LEN, ValkeyResult, raw,
};

/// `ReplyContext` is a thin wrapper around `RedisModuleCtx` that provides efficient,
/// zero-allocation reply helpers and automatic database state management.
pub struct ReplyContext {
    ctx: Context,
    raw_ctx: *mut raw::RedisModuleCtx,
}

impl ReplyContext {
    pub(crate) fn new(ctx: *mut raw::RedisModuleCtx) -> Self {
        Self {
            ctx: Context { ctx },
            raw_ctx: ctx,
        }
    }

    /// Log a message at the specified `level` using the underlying context.
    pub fn log(&self, level: ValkeyLogLevel, message: &str) {
        self.ctx.log(level, message);
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

    /// Return the currently selected DB index from the underlying context.
    pub fn get_current_db(&self) -> i32 {
        unsafe { RedisModule_GetSelectedDb.unwrap()(self.raw_ctx) }
    }

    /// Reply with a 64-bit integer value.
    pub fn reply_with_integer(&self, value: i64) -> Status {
        raw::reply_with_long_long(self.raw_ctx, value)
    }

    /// Reply with a double-precision floating point value.
    pub fn reply_with_double(&self, value: f64) -> Status {
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
    pub fn reply_with_string(&self, value: &str) -> Status {
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

    /// Set the length of a previously started postponed-length array reply.
    pub fn reply_with_array_len(&self, len: usize) -> Status {
        reply_with_array_len(self.raw_ctx, len)
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

impl Deref for ReplyContext {
    type Target = Context;

    fn deref(&self) -> &Self::Target {
        &self.ctx
    }
}

impl IntoRawCtx for &ReplyContext {
    fn into_raw(self) -> *mut raw::RedisModuleCtx {
        self.raw_ctx
    }
}
