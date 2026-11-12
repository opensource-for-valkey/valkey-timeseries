use crate::common::context::is_blocking_denied;
use crate::common::metrics::fanout as metrics;
use crate::common::replies::ReplyContext;
use crate::common::sync::lock;
use crate::fanout::FanoutCommandResult;
use crate::fanout::blocked_client::FanoutBlockedClient;
use crate::fanout::cluster_rpc::get_cluster_command_timeout;
use crate::fanout::fanout_error::FANOUT_BLOCKING_DENIED;
use crate::fanout::serialization::Serializable;
use crate::fanout::workers::REPLY_EXECUTOR;
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

    /// NOTE: Use the provided `ReplyContext` reply helpers. This runs on the main thread,
    /// unless [`Self::should_reply_off_main_thread`] moves it, and does not require locking.
    fn reply(&mut self, ctx: &ReplyContext) -> Status;

    /// Whether a successful fan-out's [`Self::reply`] may run on the fan-out reply lane
    /// instead of the main thread, writing through a thread-safe context bound to the
    /// blocked client. Only for a `reply` that merges what the shards sent and writes it:
    /// no keyspace access, no ACL checks, nothing that needs the GIL or the real client.
    /// Errors, and replies the lane has no room for, still go through the main thread.
    ///
    /// Asked once, after every shard's answer has gone through [`Self::on_response`] and
    /// before [`Self::reply`], so it can weigh what `reply` will write: a reply small
    /// enough to cost less than the hand-off is better written on the main thread.
    fn should_reply_off_main_thread(&self) -> bool {
        false
    }

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
            // Runs once, with no fanout lock held, on whichever thread completes the
            // fanout: the main thread for the last remote answer, a send failure or the
            // RPC timer, a `ts-fanout-request` worker for the last local answer.
            //
            // The reply goes to the reply lane from either. A worker could build it in
            // place and save the hand-off, but it would then hold a request-lane slot
            // that peer requests and other local shares are queued for: measured, that
            // saved ~20 µs of CPU per reply and no throughput, and cost peer requests
            // 5–9 % latency under contention.
            //
            // A client that disconnected reads no reply (the fan-out may have ended early
            // for that reason, see `FanoutCancel`). What the shards sent is freed on the
            // reply lane, as after a reply built there, rather than on the main thread,
            // which runs this when the disconnect ended the fan-out. With the lane full they
            // are freed here instead: no worse than a live client's fallback, which builds
            // and frees its reply on the main thread, and a thread per call (the only other
            // place to send them) is what an overloaded server can least afford.
            let gone = {
                let mut bc = lock(&bc_for_closure);
                let gone = bc.is_disconnected();
                if gone {
                    bc.discard_reply();
                }
                gone
            };
            if gone {
                if let Ok(slot) = REPLY_EXECUTOR.try_reserve() {
                    slot.spawn(move || {
                        drop(op);
                        drop(bc_for_closure);
                    });
                }
                return;
            }
            if result.is_ok()
                && op.should_reply_off_main_thread()
                && let Ok(slot) = REPLY_EXECUTOR.try_reserve()
            {
                slot.spawn(move || {
                    let replied = lock(&bc_for_closure).reply_off_main_thread(op);
                    // The merged results are freed here rather than on the main thread;
                    // dropping the last handle then unblocks the client.
                    drop(replied);
                    drop(bc_for_closure);
                });
                return;
            }
            lock(&bc_for_closure).set_private_data(op, result);
        };

        let exec_result = Self::exec_command(self, ctx, started, timeout, handle_response);
        match exec_result {
            Ok(cancel) => {
                // Still on the main thread, inside the command, so the client cannot have
                // disconnected yet: the hook is in place before the server could run it.
                if let Some(cancel) = cancel {
                    lock(&blocked_client).on_disconnect(cancel.into_hook());
                }
                Ok(ValkeyValue::NoReply)
            }
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
