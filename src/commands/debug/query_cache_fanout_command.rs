use super::ensure_debug_enabled;
use crate::commands::fanout_codec::generated::{ClearQueryCacheRequest, ClearQueryCacheResponse};
use crate::common::replies::{ReplyContext, reply_with_usize};
use crate::fanout::{
    FanoutClientCommand, FanoutCommandResult, FanoutContext, FanoutTarget, NodeInfo,
};
use crate::promql::engine::query_cache;
use valkey_module::{Context, Status, ValkeyResult};

/// `TS._DEBUG CLEAR_QUERY_CACHE` across the cluster: each `target` node empties its own
/// parsed-query cache, which is per process, so `CLUSTER` asks every node, replicas included.
/// The coordinator replies with the entries cleared, summed over the nodes.
///
/// Each node gates its own internals on `debug-mode`, as for a direct `TS._DEBUG` call, so one
/// node with it off fails the command; the nodes that answered before it have still cleared.
#[derive(Default)]
pub struct ClearQueryCacheFanoutCommand {
    cleared: u64,
    target: FanoutTarget,
}

impl ClearQueryCacheFanoutCommand {
    pub fn new(target: FanoutTarget) -> Self {
        Self {
            target,
            ..Default::default()
        }
    }
}

impl FanoutClientCommand for ClearQueryCacheFanoutCommand {
    type Request = ClearQueryCacheRequest;
    type Response = ClearQueryCacheResponse;

    fn name() -> &'static str {
        "cmd::clear_query_cache"
    }

    /// Takes only the cache's own lock; no GIL, no keys.
    fn get_local_response(
        _ctx: &FanoutContext,
        _req: ClearQueryCacheRequest,
    ) -> ValkeyResult<ClearQueryCacheResponse> {
        ensure_debug_enabled()?;
        Ok(ClearQueryCacheResponse {
            cleared: query_cache::clear() as u64,
        })
    }

    fn generate_request(&self) -> ClearQueryCacheRequest {
        ClearQueryCacheRequest {}
    }

    fn get_targets(&self, _ctx: &Context) -> FanoutTarget {
        self.target.clone()
    }

    fn on_response(&mut self, resp: Self::Response, _target: &NodeInfo) -> FanoutCommandResult {
        self.cleared = self.cleared.saturating_add(resp.cleared);
        Ok(())
    }

    fn reply(&mut self, ctx: &ReplyContext) -> Status {
        reply_with_usize(ctx.context(), self.cleared as usize);
        Status::Ok
    }
}
