use super::raw_replies::{
    IntoRawCtx, is_resp3_client, reply, reply_error_string, reply_with_bulk_string,
};
use std::marker::PhantomData;
use std::ops::Deref;
use std::os::raw::c_long;
use valkey_module::logging::ValkeyLogLevel;
use valkey_module::{Context, Status, ValkeyResult, raw};

/// `ReplyContext` is a thin wrapper around `RedisModuleCtx` that provides efficient,
/// zero-allocation reply helpers and automatic database state management.
///
/// It carries the client's protocol, so the reply helpers that pick a RESP2 or RESP3 shape
/// ([`Self::reply_with_set`], [`Self::is_resp3`]) never ask the server for it. That is what
/// makes them safe on a thread-safe context written off the main thread: the server's own
/// answer (`RM_GetContextFlags`) reads server state that only the GIL holder may read.
///
/// `'a` is the lifetime of whatever owns the server context: a writer handed out by
/// [`super::ThreadSafeReplyContext::get_reply_context`] cannot outlive the context it writes
/// through, which is freed when its owner drops.
pub struct ReplyContext<'a> {
    ctx: Context,
    raw_ctx: *mut raw::RedisModuleCtx,
    resp3: bool,
    _owner: PhantomData<&'a ()>,
}

impl<'a> ReplyContext<'a> {
    /// A writer for a context the server handed in: a command, or a blocked client's reply,
    /// timeout or disconnect callback. Main thread only, with the GIL held: it reads the
    /// client's protocol from the server. Off the main thread, use [`Self::with_protocol`].
    pub(crate) fn new(ctx: *mut raw::RedisModuleCtx) -> Self {
        Self::with_protocol(ctx, is_resp3_client(ctx))
    }

    /// A writer for a thread-safe context bound to a blocked client, with the protocol read
    /// on the main thread when the client was blocked. Safe from any one thread.
    pub(crate) fn with_protocol(ctx: *mut raw::RedisModuleCtx, resp3: bool) -> Self {
        Self {
            ctx: Context { ctx },
            raw_ctx: ctx,
            resp3,
            _owner: PhantomData,
        }
    }

    /// Whether the client negotiated RESP3 (HELLO 3), as carried by this context.
    #[inline]
    pub(crate) fn is_resp3(&self) -> bool {
        self.resp3
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

    /// Reply with an error string.
    pub fn reply_error_string(&self, s: &str) -> Status {
        reply_error_string(self.raw_ctx, s)
    }

    /// Reply with a bulk string.
    pub fn reply_with_string(&self, value: &str) -> Status {
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
    /// clients receive the equivalent array form. The one place the two are chosen between.
    pub fn reply_with_set(&self, len: usize) -> Status {
        if self.resp3 {
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

impl Deref for ReplyContext<'_> {
    type Target = Context;

    fn deref(&self) -> &Self::Target {
        &self.ctx
    }
}

impl IntoRawCtx for &ReplyContext<'_> {
    fn into_raw(self) -> *mut raw::RedisModuleCtx {
        self.raw_ctx
    }
}
