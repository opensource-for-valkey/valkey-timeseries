use crate::common::context::is_blocking_denied;
use crate::common::metrics::fanout as metrics;
use crate::common::replies::ReplyContext;
use crate::common::sync::lock;
use crate::fanout::FanoutCommandResult;
use crate::fanout::blocked_client::FanoutBlockedClient;
use crate::fanout::cluster_rpc::get_cluster_command_timeout;
use crate::fanout::fanout_error::FANOUT_BLOCKING_DENIED;
use crate::fanout::serialization::Serializable;
use crate::fanout::{FanoutCommand, FanoutContext, FanoutResult, FanoutTarget, NodeInfo};
use metered::Counter;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use valkey_module::{Context, Status, ValkeyError, ValkeyResult, ValkeyValue};

/// A trait for cluster-mode commands which send results back to clients after receiving responses from other nodes.
/// This is a higher-level abstraction over `FanoutCommand` that includes client response handling logic.
pub trait FanoutClientCommand: Default + Send + 'static {
    type Request: Serializable + Send + 'static;
    type Response: Serializable + Send;

    fn name() -> &'static str;

    /// Get the target nodes for the fanout operation, bound to the cluster-map
    /// fingerprint of the snapshot they were selected from.
    fn get_targets(&self, ctx: &Context) -> FanoutTarget {
        super::compute_query_fanout_mode(ctx)
    }

    /// Handle a local request on the current node. See
    /// [`FanoutCommand::get_local_response`] for the locking contract.
    fn get_local_response(ctx: &FanoutContext, req: Self::Request) -> ValkeyResult<Self::Response>;

    /// See [`FanoutCommand::cancellable`]: `false` for a command that writes.
    fn cancellable() -> bool {
        true
    }

    fn generate_request(&self) -> Self::Request;

    fn on_response(&mut self, resp: Self::Response, target: &NodeInfo) -> FanoutCommandResult;

    /// NOTE: Use the provided `ReplyContext` reply helpers. This is already
    /// running on the main thread and does not require locking.
    fn reply(&mut self, ctx: &ReplyContext) -> Status;

    /// Execute the fanout operation across cluster nodes.
    /// The `where Self: FanoutCommand` bound is always satisfied via the blanket impl below.
    fn exec(self, ctx: &Context) -> ValkeyResult<ValkeyValue>
    where
        Self: FanoutCommand,
    {
        // A fan-out completes asynchronously, so it has to block the client — which the server
        // refuses inside MULTI, a script, or a module call without the `K` flag (and asserts on
        // for the last). Refuse up front, before anything runs: the local share of a
        // local-only fan-out (a single-shard cluster, a HASHTAG owned by this node) used to be
        // executed anyway, so TS.MDEL deleted keys after the client had already been told the
        // command failed, outside the transaction it was queued in.
        if is_blocking_denied(ctx) {
            metrics::FANOUT_BLOCKING_DENIED.incr();
            return Err(ValkeyError::Str(FANOUT_BLOCKING_DENIED));
        }
        // One budget for the whole operation, counted from now (see `exec_command`).
        let started = Instant::now();
        let timeout = get_cluster_command_timeout();
        let blocked_client = Arc::new(Mutex::new(FanoutBlockedClient::<Self>::new(ctx, timeout)));
        let bc_for_closure = Arc::clone(&blocked_client);

        let handle_response = move |op: Self, result: FanoutResult| {
            // Runs once, on whichever thread completes the fanout: the main thread
            // for the last remote answer, a send failure or the RPC timer, a worker
            // for the last local answer.
            lock(&bc_for_closure).set_private_data(op, result);
        };

        let exec_result = Self::exec_command(self, ctx, started, timeout, handle_response);
        match exec_result {
            Ok(()) => Ok(ValkeyValue::NoReply),
            Err(err) => {
                // Set an error payload so the reply_callback sends back the
                // proper error message instead of "No reply data".
                let op = Self::default();
                let fanout_result: FanoutResult = Err(err);
                lock(&blocked_client).set_private_data(op, fanout_result);

                // Dropping the last Arc triggers unblock(), and the reply_callback
                // will send the error reply based on the stored fanout_result.
                drop(blocked_client);
                Ok(ValkeyValue::NoReply)
            }
        }
    }
}

// ── Blanket impl of FanoutCommand ────────────────────────────────────────────
impl<T: FanoutClientCommand> FanoutCommand for T {
    type Request = T::Request;
    type Response = T::Response;

    fn name() -> &'static str {
        T::name()
    }

    fn get_local_response(ctx: &FanoutContext, req: Self::Request) -> ValkeyResult<Self::Response> {
        T::get_local_response(ctx, req)
    }

    fn cancellable() -> bool {
        T::cancellable()
    }

    /// Get the target nodes for the fanout operation, bound to the cluster-map
    /// fingerprint of the snapshot they were selected from.
    /// By default, it retrieves a random replica per shard.
    fn get_targets(&self, ctx: &Context) -> FanoutTarget {
        FanoutClientCommand::get_targets(self, ctx)
    }

    fn generate_request(&self) -> Self::Request {
        // Fully-qualified to avoid infinite recursion
        FanoutClientCommand::generate_request(self)
    }

    fn on_response(&mut self, resp: Self::Response, target: &NodeInfo) -> FanoutCommandResult {
        FanoutClientCommand::on_response(self, resp, target)
    }
}
