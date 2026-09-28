use super::blocked::BlockedClient;
use crate::common::context::client_reply_context::ClientReplyContext;
use valkey_module::{Context, ValkeyResult, raw};

pub(crate) struct ClientThreadSafeContext {
    ctx: *mut raw::RedisModuleCtx,

    /// This field is only used implicitly by `Drop`, so avoid a compiler warning
    #[allow(dead_code)]
    blocked_client: BlockedClient,
}

unsafe impl Send for ClientThreadSafeContext {}
unsafe impl Sync for ClientThreadSafeContext {}

impl ClientThreadSafeContext {
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

    pub fn get_write_context(&self) -> ClientReplyContext {
        ClientReplyContext::new(self.ctx)
    }
}

impl Drop for ClientThreadSafeContext {
    fn drop(&mut self) {
        unsafe { raw::RedisModule_FreeThreadSafeContext.unwrap()(self.ctx) };
    }
}
