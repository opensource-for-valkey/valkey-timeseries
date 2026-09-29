use super::raw_replies::{
    IntoRawCtx, is_resp3_client, reply, reply_error_string, reply_with_bulk_string,
};
use crate::common::Sample;
use std::os::raw::c_long;
use valkey_module::logging::ValkeyLogLevel;
use valkey_module::{Context, Status, ValkeyResult, raw};

/// `ReplyContext` is a thin wrapper around `RedisModuleCtx` that provides efficient,
/// zero-allocation reply helpers, writing straight to the client rather than through an
/// intermediate `ValkeyValue`.
///
/// Also the writer a worker thread answers a blocked client with (see
/// [`ThreadSafeReplyContext`](super::ThreadSafeReplyContext)): replies need no GIL, so its reply
/// methods are safe there. It deliberately does not dereference to [`Context`]: anything else a
/// `Context` offers — key access, flags, ACL lookups — needs the GIL, and goes through the
/// explicit [`Self::context`] on the main thread only.
pub struct ReplyContext {
    ctx: Context,
}

impl ReplyContext {
    pub(crate) fn new(ctx: *mut raw::RedisModuleCtx) -> Self {
        Self {
            ctx: Context { ctx },
        }
    }

    /// The underlying raw context, for the free reply helpers in
    /// [`super::raw_replies`] that take an [`IntoRawCtx`] rather than a `ReplyContext`.
    #[inline]
    pub(crate) fn raw(&self) -> *mut raw::RedisModuleCtx {
        self.ctx.ctx
    }

    /// The wrapped [`Context`], for helpers that need the crate type — key access, context flags,
    /// and ACL identity. Inside a blocked-client reply or timeout callback this carries the real
    /// blocked client, so both protocol detection and ACL lookups behave as they do on the
    /// original command call.
    ///
    /// Main thread only (or with the GIL held): never from a worker answering a blocked client.
    #[inline]
    pub(crate) fn context(&self) -> &Context {
        &self.ctx
    }

    /// Log a message at the specified `level` using the underlying context.
    pub fn log(&self, level: ValkeyLogLevel, message: &str) {
        self.ctx.log(level, message);
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
    pub fn reply_with_integer(&self, value: i64) -> Status {
        raw::reply_with_long_long(self.ctx.ctx, value)
    }

    /// Reply with a double-precision floating point value.
    pub fn reply_with_double(&self, value: f64) -> Status {
        raw::reply_with_double(self.ctx.ctx, value)
    }

    /// Reply with a boolean value.
    pub fn reply_with_bool(&self, value: bool) -> Status {
        raw::reply_with_bool(self.ctx.ctx, value.into())
    }

    /// Reply with an error string.
    pub fn reply_error_string(&self, s: &str) -> Status {
        reply_error_string(self.ctx.ctx, s)
    }

    /// Reply with a bulk string.
    pub fn reply_with_string(&self, value: &str) -> Status {
        reply_with_bulk_string(self.ctx.ctx, value)
    }

    /// Reply with a simple string; `\r`, `\n` and NUL become spaces.
    pub fn reply_with_simple_string(&self, value: &str) -> Status {
        reply_with_simple_string(self.ctx.ctx, value)
    }

    /// Reply with a `[timestamp, value]` pair.
    pub fn reply_with_sample(&self, sample: &Sample) -> Status {
        reply_with_sample(self.ctx.ctx, sample);
        Status::Ok
    }

    /// Start an array reply with the given length.
    pub fn reply_with_array(&self, len: usize) -> Status {
        raw::reply_with_array(self.ctx.ctx, len as c_long)
    }

    /// Start a map reply with the given length.
    pub fn reply_with_map(&self, len: usize) -> Status {
        raw::reply_with_map(self.ctx.ctx, len as c_long)
    }

    /// Start a set reply (RESP3) or array reply (RESP2) with the given length.
    ///
    /// `TS.QUERYLABELS` replies with a set of distinct label names/values; RESP2
    /// clients receive the equivalent array form.
    pub fn reply_with_set(&self, len: usize) -> Status {
        if is_resp3_client(self.ctx.ctx) {
            raw::reply_with_set(self.ctx.ctx, len as c_long)
        } else {
            self.reply_with_array(len)
        }
    }

    /// Forward a `ValkeyResult` to the reply machinery.
    #[allow(clippy::must_use_candidate)]
    pub fn reply(&self, result: ValkeyResult) -> Status {
        reply(self.ctx.ctx, result)
    }
}

impl IntoRawCtx for &ReplyContext {
    fn into_raw(self) -> *mut raw::RedisModuleCtx {
        self.ctx.ctx
    }
}
