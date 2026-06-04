use crate::common::replies::{IntoRawCtx, ReplyContext};
use std::borrow::Borrow;
use std::ops::Deref;
use std::os::raw::c_int;
use std::ptr;
use valkey_module::logging::ValkeyLogLevel;
use valkey_module::{Context, ValkeyError, ValkeyResult, raw};

/// A lightweight "fork" of the BlockedClient in `valkey_module` to allow raw client replies from background threads
/// without needing to lock the context. This is safe, since the Valkey modules API does not require locking for
/// `Reply` functions,
pub struct BlockedClient {
    pub(crate) inner: *mut raw::RedisModuleBlockedClient,
}

// We need to be able to send the inner pointer to another thread
unsafe impl Send for BlockedClient {}

impl BlockedClient {
    pub(crate) fn new(inner: *mut raw::RedisModuleBlockedClient) -> Self {
        Self { inner }
    }

    /// Aborts the blocked client operation
    ///
    /// # Returns
    /// * `Ok(())` - If the blocked client was successfully aborted
    /// * `Err(ValkeyError)` - If the abort operation failed
    pub fn abort(mut self) -> Result<(), ValkeyError> {
        unsafe {
            if raw::RedisModule_AbortBlock.unwrap()(self.inner) == raw::REDISMODULE_OK as c_int {
                // Prevent the normal Drop from running
                self.inner = ptr::null_mut();
                Ok(())
            } else {
                Err(ValkeyError::Str("Failed to abort blocked client"))
            }
        }
    }
}

impl Drop for BlockedClient {
    fn drop(&mut self) {
        if !self.inner.is_null() {
            unsafe {
                raw::RedisModule_UnblockClient.unwrap()(self.inner, ptr::null_mut());
            }
        }
    }
}

pub(crate) fn block_client(ctx: &Context) -> BlockedClient {
    let blocked_client = unsafe {
        raw::RedisModule_BlockClient.unwrap()(
            ctx.ctx, // ctx
            None,    // reply_func
            None,    // timeout_func
            None, 0,
        )
    };

    BlockedClient::new(blocked_client)
}

pub struct ThreadSafeReplyContext {
    pub(crate) ctx: *mut raw::RedisModuleCtx,

    /// 'Drop' only uses this field implicitly, so avoid a compiler warning
    #[allow(dead_code)]
    blocked_client: BlockedClient,
}

pub struct ContextGuard {
    ctx: Context,
}

impl Drop for ContextGuard {
    fn drop(&mut self) {
        unsafe {
            raw::RedisModule_ThreadSafeContextUnlock.unwrap()(self.ctx.ctx);
            raw::RedisModule_FreeThreadSafeContext.unwrap()(self.ctx.ctx);
        };
    }
}

impl Deref for ContextGuard {
    type Target = Context;

    fn deref(&self) -> &Self::Target {
        &self.ctx
    }
}

impl Borrow<Context> for ContextGuard {
    fn borrow(&self) -> &Context {
        &self.ctx
    }
}

/// SAFETY:
/// This is copied from the implementation of `ThreadSafeContext` in `thread_safe.rs`, with the same safety guarantees.
/// The Valkey modules API does not require locking for `Reply` functions, and the `ReplyContext` constructed has its context
/// as private.
unsafe impl Send for ThreadSafeReplyContext {}
unsafe impl Sync for ThreadSafeReplyContext {}

impl ThreadSafeReplyContext {
    #[must_use]
    pub fn with_blocked_client(blocked_client: BlockedClient) -> Self {
        let ctx = unsafe { raw::RedisModule_GetThreadSafeContext.unwrap()(blocked_client.inner) };
        Self {
            ctx,
            blocked_client,
        }
    }

    /// The Valkey modules API does not require locking for `Reply` functions,
    /// so we pass through its functionality directly.
    #[allow(clippy::must_use_candidate)]
    pub fn reply(&self, r: ValkeyResult) -> raw::Status {
        let ctx = Context::new(self.ctx);
        ctx.reply(r)
    }

    pub fn get_reply_context(&self) -> ReplyContext {
        ReplyContext::new(self.ctx)
    }

    /// All non-reply APIs require locking, so we mirror
    /// `valkey_module::ThreadSafeContext::lock` semantics.
    pub fn lock(&self) -> ContextGuard {
        unsafe { raw::RedisModule_ThreadSafeContextLock.unwrap()(self.ctx) };
        let ctx = unsafe { raw::RedisModule_GetThreadSafeContext.unwrap()(ptr::null_mut()) };
        let ctx = Context::new(ctx);
        ContextGuard { ctx }
    }

    /// Log a message at the specified `level` using the underlying context.
    pub fn log(&self, level: ValkeyLogLevel, message: &str) {
        Context::new(self.ctx).log(level, message);
    }

    /// Convenience logging helpers.
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
}

impl Drop for ThreadSafeReplyContext {
    fn drop(&mut self) {
        unsafe { raw::RedisModule_FreeThreadSafeContext.unwrap()(self.ctx) };
    }
}

impl IntoRawCtx for &ThreadSafeReplyContext {
    fn into_raw(self) -> *mut raw::RedisModuleCtx {
        self.ctx
    }
}
