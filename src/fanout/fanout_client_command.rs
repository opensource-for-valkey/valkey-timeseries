use crate::common::replies::ReplyContext;
use crate::fanout::FanoutCommandResult;
use crate::fanout::blocked_client::FanoutBlockedClient;
use crate::fanout::serialization::Serializable;
use crate::fanout::{
    FanoutCommand, FanoutResult, FanoutTargetMode, FanoutTargets, NodeInfo, get_fanout_targets,
};
use std::sync::{Arc, Mutex};
use valkey_module::{Context, Status, ValkeyResult, ValkeyValue};

pub type FanoutContext = ReplyContext;

/// A trait for cluster-mode commands which send results back to clients after receiving responses from other nodes.
/// This is a higher-level abstraction over `FanoutCommand` that includes client response handling logic.
pub trait FanoutClientCommand: Default + Send + 'static {
    type Request: Serializable + Send + 'static;
    type Response: Serializable + Default + Send;

    fn name() -> &'static str;

    /// Get the target nodes for the fanout operation, bound to the cluster-map
    /// fingerprint of the snapshot they were selected from.
    /// By default, it retrieves a random replica per shard.
    fn get_targets(&self, ctx: &Context) -> FanoutTargets {
        get_fanout_targets(ctx, FanoutTargetMode::Random)
    }

    fn get_local_response(ctx: &Context, req: Self::Request) -> ValkeyResult<Self::Response>;

    fn generate_request(&self) -> Self::Request;

    fn on_response(&mut self, resp: Self::Response, target: &NodeInfo) -> FanoutCommandResult;

    /// Return the final response after the fanout operation is complete.
    /// By default, it returns a default instance of the response type.
    fn get_response(self) -> Self::Response {
        Self::Response::default()
    }

    /// NOTE: Use the provided `FanoutContext` reply helpers. This is already
    /// running on the main thread and does not require locking.
    fn reply(&mut self, ctx: &FanoutContext) -> Status;

    /// Execute the fanout operation across cluster nodes.
    /// The `where Self: FanoutCommand` bound is always satisfied via the blanket impl below.
    fn exec(self, ctx: &Context) -> ValkeyResult<ValkeyValue>
    where
        Self: FanoutCommand,
    {
        let blocked_client: Arc<Mutex<FanoutBlockedClient<Self>>> =
            Arc::new(Mutex::new(FanoutBlockedClient::<Self>::new(ctx)));
        let bc_for_closure: Arc<Mutex<FanoutBlockedClient<Self>>> = Arc::clone(&blocked_client);

        let handle_response = move |op: Self, result: FanoutResult| {
            // Runs on a background thread once all shard responses arrive.
            if let Ok(mut bc) = bc_for_closure.lock() {
                bc.set_private_data(op, result);
            }
        };

        let exec_result = Self::exec_command(self, ctx, handle_response);
        match exec_result {
            Ok(()) => Ok(ValkeyValue::NoReply),
            Err(err) => {
                // Set an error payload so the reply_callback sends back the
                // proper error message instead of "No reply data".
                let op: Self = Self::default();
                let fanout_result: FanoutResult = Err(err);
                if let Ok(mut bc) = blocked_client.lock() {
                    bc.set_private_data(op, fanout_result);
                }

                // Dropping the last Arc triggers unblock(), and the reply_callback
                // will send the error reply based on the stored fanout_result.
                drop(blocked_client);
                Ok(ValkeyValue::NoReply)
            }
        }
    }
}

// ── Blanket impl of FanoutCommand ────────────────────────────────────────────
impl<T: FanoutClientCommand> FanoutCommand for T
where
    <T as FanoutClientCommand>::Response: std::default::Default,
{
    type Request = T::Request;
    type Response = T::Response;

    fn name() -> &'static str {
        T::name()
    }

    fn get_local_response(ctx: &Context, req: Self::Request) -> ValkeyResult<Self::Response> {
        T::get_local_response(ctx, req)
    }

    /// Get the target nodes for the fanout operation, bound to the cluster-map
    /// fingerprint of the snapshot they were selected from.
    /// By default, it retrieves a random replica per shard.
    fn get_targets(&self, ctx: &Context) -> FanoutTargets {
        FanoutClientCommand::get_targets(self, ctx)
    }

    fn generate_request(&self) -> Self::Request {
        // Fully-qualified to avoid infinite recursion
        FanoutClientCommand::generate_request(self)
    }

    fn on_response(&mut self, resp: Self::Response, target: &NodeInfo) -> FanoutCommandResult {
        FanoutClientCommand::on_response(self, resp, target)
    }

    fn get_response(self) -> Self::Response {
        FanoutClientCommand::get_response(self)
    }
}
