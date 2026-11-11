use crate::common::replies::{IntoRawCtx, ReplyContext};
use crate::common::sync::lock;
use crate::error_consts;
use std::collections::HashMap;
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use valkey_module::{Context, ValkeyError, ValkeyResult, raw};

/// The disconnect flag of every client blocked through [`block_client`], by its handle. The
/// server reports a disconnect with the handle alone, so this is how the callback finds the
/// flag; the handle's private data would do too, but the server writes that from the thread
/// that unblocks the client, which can race the callback reading it.
///
/// An entry lives from [`block_client`] until its [`BlockedClient`] unblocks the client, and
/// the server frees a handle only after that, so a handle never names two clients here.
static DISCONNECTED: LazyLock<Mutex<HashMap<usize, Arc<AtomicBool>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Runs on the main thread when a blocked client's connection closes (the peer went away, or
/// `CLIENT KILL`), never for a client unblocked normally.
unsafe extern "C" fn on_disconnect(
    _ctx: *mut raw::RedisModuleCtx,
    bc: *mut raw::RedisModuleBlockedClient,
) {
    if let Some(flag) = lock(&DISCONNECTED).get(&(bc as usize)) {
        flag.store(true, Ordering::Relaxed);
    }
}

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
    /// Set once the client's connection closes: nobody will read the reply, so the work
    /// answering it can stop.
    disconnected: Arc<AtomicBool>,
}

// We need to be able to send the inner pointer to another thread
unsafe impl Send for BlockedClient {}

impl Drop for BlockedClient {
    fn drop(&mut self) {
        if self.inner.is_null() {
            return;
        }
        // Before the unblock, after which the server may free the handle and reuse its address.
        lock(&DISCONNECTED).remove(&(self.inner as usize));
        unsafe {
            if !self.answered {
                let ctx = raw::RedisModule_GetThreadSafeContext.unwrap()(self.inner);
                reply_no_reply_written(ctx);
                raw::RedisModule_FreeThreadSafeContext.unwrap()(ctx);
            }
            raw::RedisModule_UnblockClient.unwrap()(self.inner, ptr::null_mut());
        }
    }
}

/// Answers a blocked client whose worker never wrote a reply.
fn reply_no_reply_written(ctx: *mut raw::RedisModuleCtx) {
    let _ = Context::new(ctx).reply(Err(ValkeyError::Str(error_consts::NO_REPLY_WRITTEN)));
}

/// Blocks the client of `ctx` until the returned handle is dropped, with no timeout: the work
/// answering it runs to the end unless the client disconnects first, which
/// [`ThreadSafeReplyContext::disconnected`] reports. Main thread only.
pub(crate) fn block_client(ctx: &Context) -> BlockedClient {
    let inner = unsafe {
        raw::RedisModule_BlockClient.unwrap()(
            ctx.ctx, // ctx
            None,    // reply_func
            None,    // timeout_func
            None, 0,
        )
    };
    let flag = Arc::new(AtomicBool::new(false));
    lock(&DISCONNECTED).insert(inner as usize, Arc::clone(&flag));
    unsafe { raw::RedisModule_SetDisconnectCallback.unwrap()(inner, Some(on_disconnect)) };
    BlockedClient {
        inner,
        answered: false,
        disconnected: flag,
    }
}

pub struct ThreadSafeReplyContext {
    pub(crate) ctx: *mut raw::RedisModuleCtx,
    blocked_client: BlockedClient,
    /// Set once a reply is written or a [`ReplyContext`] is handed out. If it is still unset
    /// when this is dropped, the client is answered with an error (see [`BlockedClient`]).
    ///
    /// A writer that panics after starting its reply leaves a partial reply that nothing
    /// here can repair; this covers the far likelier failure, before any reply.
    answered: AtomicBool,
}

/// SAFETY: a thread-safe context belongs to one blocked client and may be used from any one
/// thread at a time; the Valkey modules API does not require locking for `Reply` functions.
/// Moving it to the thread that writes the reply is sound. Sharing it is not: two threads
/// writing replies through one `&ThreadSafeReplyContext` would interleave them. So `Send`
/// only; nothing needs `Sync`.
unsafe impl Send for ThreadSafeReplyContext {}

impl ThreadSafeReplyContext {
    /// Takes over answering `blocked_client` on the calling thread. From here until the
    /// context drops counts as the command's own time, so `INFO commandstats`, latency
    /// tracking and the slow log see the work done off the main thread; the time the client
    /// spent queued does not count, as a blocking command's wait never does.
    #[must_use]
    pub fn with_blocked_client(blocked_client: BlockedClient) -> Self {
        let ctx = unsafe {
            raw::RedisModule_BlockedClientMeasureTimeStart.unwrap()(blocked_client.inner);
            raw::RedisModule_GetThreadSafeContext.unwrap()(blocked_client.inner)
        };
        Self {
            ctx,
            blocked_client,
            answered: AtomicBool::new(false),
        }
    }

    /// Set once the client has disconnected: the reply will not be read, so the work can
    /// stop.
    pub(crate) fn disconnected(&self) -> &AtomicBool {
        &self.blocked_client.disconnected
    }

    /// The Valkey modules API does not require locking for `Reply` functions,
    /// so we pass through its functionality directly.
    #[allow(clippy::must_use_candidate)]
    pub fn reply(&self, r: ValkeyResult) -> raw::Status {
        self.answered.store(true, Ordering::Relaxed);
        let ctx = Context::new(self.ctx);
        ctx.reply(r)
    }

    /// A context to write the reply through. The caller takes responsibility for writing it.
    pub fn get_reply_context(&self) -> ReplyContext {
        self.answered.store(true, Ordering::Relaxed);
        ReplyContext::new(self.ctx)
    }
}

impl Drop for ThreadSafeReplyContext {
    fn drop(&mut self) {
        if !self.answered.load(Ordering::Relaxed) {
            reply_no_reply_written(self.ctx);
        }
        unsafe {
            raw::RedisModule_BlockedClientMeasureTimeEnd.unwrap()(self.blocked_client.inner);
            raw::RedisModule_FreeThreadSafeContext.unwrap()(self.ctx);
        }
        // Answered above if it was not already; the handle must not answer again.
        self.blocked_client.answered = true;
    }
}

impl IntoRawCtx for &ThreadSafeReplyContext {
    /// Writing through the raw context is answering too: the caller takes responsibility for
    /// the reply, so the drop must not add an error after it.
    fn into_raw(self) -> *mut raw::RedisModuleCtx {
        self.answered.store(true, Ordering::Relaxed);
        self.ctx
    }
}
