use super::raw_replies::{
    IntoRawCtx, is_resp3_client, reply, reply_error_string, reply_with_bulk_string,
};
use std::ops::Deref;
use std::os::raw::c_long;
use valkey_module::logging::ValkeyLogLevel;
use valkey_module::{Context, Status, ValkeyResult, raw};

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

    /// The underlying raw context, for the free reply helpers in
    /// [`super::raw_replies`] that take an [`IntoRawCtx`] rather than a `ReplyContext`.
    #[inline]
    pub(crate) fn raw(&self) -> *mut raw::RedisModuleCtx {
        self.raw_ctx
    }

    /// The wrapped [`Context`], for helpers that need the crate type — key access, context flags,
    /// and ACL identity. Inside a blocked-client reply or timeout callback this carries the real
    /// blocked client, so both protocol detection and ACL lookups behave as they do on the
    /// original command call.
    #[inline]
    pub(crate) fn context(&self) -> &Context {
        &self.ctx
    }

    /// Log a message at the specified `level` using the underlying context.
    pub fn log(&self, level: ValkeyLogLevel, message: &str) {
        let context = Context { ctx: self.raw_ctx };
        context.log(level, message);
    }

    /// Convenience logging helpers
    #[allow(dead_code)]
    pub fn log_debug(&self, message: &str) {
        self.log(ValkeyLogLevel::Debug, message);
    }
    pub fn log_warning(&self, message: &str) {
        self.log(ValkeyLogLevel::Warning, message);
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

    /// Reply with an error string.
    pub fn reply_error_string(&self, s: &str) -> Status {
        reply_error_string(self.raw_ctx, s)
    }

    /// Reply with a bulk string.
    pub fn reply_with_bulk_string(&self, value: &str) -> Status {
        reply_with_bulk_string(self.raw_ctx, value)
    }

    /// Start an array reply with the given length.
    pub fn reply_with_array(&self, len: usize) -> Status {
        raw::reply_with_array(self.raw_ctx, len as c_long)
    }

    /// Start a map reply with the given length.
    pub fn reply_with_map(&self, len: usize) -> Status {
        raw::reply_with_map(self.raw_ctx, len as c_long)
    }

    /// Start a set reply (RESP3) or array reply (RESP2) with the given length.
    ///
    /// `TS.QUERYLABELS` replies with a set of distinct label names/values; RESP2
    /// clients receive the equivalent array form.
    pub fn reply_with_set(&self, len: usize) -> Status {
        if is_resp3_client(self.raw_ctx) {
            raw::reply_with_set(self.raw_ctx, len as c_long)
        } else {
            self.reply_with_array(len)
        }
    }

    /// Forward a `ValkeyResult` to the reply machinery.
    #[allow(clippy::must_use_candidate)]
    pub fn reply(&self, result: ValkeyResult) -> Status {
        reply(self.raw_ctx, result)
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
