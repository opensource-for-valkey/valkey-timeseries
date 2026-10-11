use super::client_cancellation::{CancelToken, Registration, mark_disconnected};
use crate::common::replies::{IntoRawCtx, ReplyContext, reply};
use crate::error_consts;
use std::cell::Cell;
use std::ptr;
use valkey_module::{Context, ValkeyError, ValkeyResult, raw};

/// A lightweight "fork" of the BlockedClient in `valkey_module` to allow raw client replies from background threads
/// without needing to lock the context. This is safe, since the Valkey modules API does not require locking for
/// `Reply` functions,
///
/// A blocked client must be answered before it is unblocked: the server delivers only what was
/// written to the client's thread-safe context, and unblocking with nothing written leaves the
/// client waiting forever (or, if it pipelines, reading the next command's reply as this one's).
/// So a handle dropped before anyone took responsibility for the reply (a job that panicked, or
/// one the executor dropped without running) answers with an error itself.
pub struct BlockedClient {
    pub(crate) inner: *mut raw::RedisModuleBlockedClient,
    /// Whether a reply has been written or handed to a writer.
    answered: bool,
    /// Set by [`Self::cancel_on_disconnect`]; dropped before the client is unblocked.
    cancellation: Option<Registration>,
}

// We need to be able to send the inner pointer to another thread
unsafe impl Send for BlockedClient {}

impl BlockedClient {
    pub(crate) fn new(inner: *mut raw::RedisModuleBlockedClient) -> Self {
        Self {
            inner,
            answered: false,
            cancellation: None,
        }
    }

    /// Have the server report this client's disconnect, and return the token that reads it.
    ///
    /// Call on the main thread, straight after blocking: the callback is installed on the
    /// handle, and the server only calls it from the main thread. The registration lives as
    /// long as this handle and is removed before the client is unblocked.
    pub(crate) fn cancel_on_disconnect(&mut self) -> CancelToken {
        if let Some(registration) = &self.cancellation {
            return registration.token();
        }
        let registration = Registration::new(self.inner as u64);
        let token = registration.token();
        self.cancellation = Some(registration);
        unsafe {
            raw::RedisModule_SetDisconnectCallback.unwrap()(self.inner, Some(on_disconnect));
        }
        token
    }
}

/// The server's disconnect callback: runs on the main thread while `bc` is still blocked.
extern "C" fn on_disconnect(
    _ctx: *mut raw::RedisModuleCtx,
    bc: *mut raw::RedisModuleBlockedClient,
) {
    mark_disconnected(bc as u64);
}

impl Drop for BlockedClient {
    fn drop(&mut self) {
        // Before the unblock below: once unblocked the server may reuse the handle's address
        // for another blocked client, which must not find this one's registration.
        drop(self.cancellation.take());
        if self.inner.is_null() {
            return;
        }
        unsafe {
            if !self.answered {
                let ctx = raw::RedisModule_GetThreadSafeContext.unwrap()(self.inner);
                reply(ctx, Err(ValkeyError::Str(error_consts::NO_REPLY_WRITTEN)));
                raw::RedisModule_FreeThreadSafeContext.unwrap()(ctx);
            }
            raw::RedisModule_UnblockClient.unwrap()(self.inner, ptr::null_mut());
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
    blocked_client: BlockedClient,
    /// Set once a reply is written or a [`ReplyContext`] is handed out. If it is still unset
    /// when this is dropped, the client is answered with an error (see [`BlockedClient`]).
    ///
    /// A writer that panics after starting its reply leaves a partial reply that nothing
    /// here can repair; this covers the far likelier failure, before any reply.
    ///
    /// A `Cell`: the context is `Send` but not `Sync`, so only its owning thread reads or
    /// sets this, and a move to another thread already synchronizes.
    answered: Cell<bool>,
}

/// SAFETY: a thread-safe context belongs to one blocked client and may be used from any one
/// thread at a time; the Valkey modules API does not require locking for `Reply` functions.
/// Moving it to the thread that writes the reply is sound. Sharing it is not: two threads
/// writing replies through one `&ThreadSafeReplyContext` would interleave them. So `Send`
/// only; nothing needs `Sync`.
unsafe impl Send for ThreadSafeReplyContext {}

impl ThreadSafeReplyContext {
    #[must_use]
    pub fn with_blocked_client(blocked_client: BlockedClient) -> Self {
        let ctx = unsafe { raw::RedisModule_GetThreadSafeContext.unwrap()(blocked_client.inner) };
        Self {
            ctx,
            blocked_client,
            answered: Cell::new(false),
        }
    }

    /// The Valkey modules API does not require locking for `Reply` functions,
    /// so we pass through its functionality directly.
    #[allow(clippy::must_use_candidate)]
    pub fn reply(&self, r: ValkeyResult) -> raw::Status {
        self.answered.set(true);
        reply(self.ctx, r)
    }

    /// A context to write the reply through. The caller takes responsibility for writing it.
    pub fn get_reply_context(&self) -> ReplyContext {
        self.answered.set(true);
        ReplyContext::new(self.ctx)
    }
}

impl Drop for ThreadSafeReplyContext {
    fn drop(&mut self) {
        if !self.answered.get() {
            reply(
                self.ctx,
                Err(ValkeyError::Str(error_consts::NO_REPLY_WRITTEN)),
            );
        }
        unsafe { raw::RedisModule_FreeThreadSafeContext.unwrap()(self.ctx) };
        // Answered above if it was not already; the handle must not answer again.
        self.blocked_client.answered = true;
    }
}

impl IntoRawCtx for &ThreadSafeReplyContext {
    /// Writing through the raw context is answering too: the caller takes responsibility for
    /// the reply, so the drop must not add an error after it.
    fn into_raw(self) -> *mut raw::RedisModuleCtx {
        self.answered.set(true);
        self.ctx
    }
}
