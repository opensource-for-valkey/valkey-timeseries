use super::acl::get_fanout_user;
use super::cluster_rpc::{get_cluster_command_timeout, invoke_rpc};
use super::fanout_error::{ErrorKind, FanoutError};
use crate::common::context::get_current_db;
use crate::common::sync::lock;
use crate::common::threads::spawn;
use crate::fanout::fanout_context::FanoutContext;
use crate::fanout::serialization::{Deserialized, Serializable};
use crate::fanout::{
    FanoutResult, FanoutTarget, NodeInfo, compute_query_fanout_mode, get_fanout_targets,
};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use valkey_module::{Context, ValkeyResult};

pub(super) type FanoutResponseCallback = Box<dyn Fn(FanoutResult<&[u8]>, &NodeInfo) + Send + Sync>;

pub type FanoutCommandResult<T = ()> = Result<T, FanoutError>;

/// A trait representing a fanout operation that can be performed across cluster nodes.
/// It handles processing node-specific requests, managing responses, and generating the
/// final reply to the client.
pub trait FanoutCommand: Default + Send + 'static {
    /// The request type.
    type Request: Serializable + Send + 'static;
    /// The response type.
    type Response: Serializable + Send;

    /// Return the name of the fanout operation.
    fn name() -> &'static str;

    /// Handle a local request on the current node, returning the response or an error.
    ///
    /// Runs on a worker thread with the GIL *not* held. Implementations take it
    /// with [`FanoutContext::lock`], which selects the request's database and
    /// installs its ACL identity for the duration of the lock, and only around
    /// the work that touches the keyspace or index: decode the request before
    /// locking and build the response after releasing, so the server stays
    /// responsive while the shard-local reply is materialized.
    fn get_local_response(ctx: &FanoutContext, req: Self::Request) -> ValkeyResult<Self::Response>;

    /// Return the timeout duration for the entire fanout operation.
    /// This timeout applies to the overall operation, not individual RPC calls.
    fn get_timeout(&self) -> Duration {
        get_cluster_command_timeout()
    }

    /// Get the target nodes for the fanout operation, bound to the cluster-map
    /// fingerprint of the snapshot they were selected from.
    /// By default, it retrieves a random replica per shard.
    fn get_targets(&self, ctx: &Context) -> FanoutTarget {
        compute_query_fanout_mode(ctx)
    }

    /// Execute the fanout operation across cluster nodes.
    fn exec_command<F>(self, ctx: &Context, f: F) -> FanoutResult
    where
        F: FnOnce(Self, FanoutCommandResult) + Send + 'static,
    {
        let timeout = self.get_timeout();
        let targets = self.get_targets(ctx);
        exec_command(ctx, self, targets, timeout, f)
    }

    /// Generate the request to be sent to each target node.
    fn generate_request(&self) -> Self::Request;

    /// Called once per response from a target node.
    ///
    /// Returning `Err(FanoutError)` will be treated as a
    /// per-shard failure (it increments the aggregated error count and will cause the
    /// overall fanout to reply with an error at completion). Implementations should
    /// return `Ok(())` on success.
    fn on_response(&mut self, resp: Self::Response, target: &NodeInfo) -> FanoutCommandResult;

    fn on_error(&mut self, error: FanoutError, target: &NodeInfo) {
        // Log the error with context
        let msg = format!(
            "Fanout operation {}, failed for target {}: {error}",
            Self::name(),
            target.socket_address,
        );
        crate::common::logging::log_warning(&msg)
    }

    /// Called once all responses have been received, or on timeout.
    fn on_completion(&mut self) {}

    /// If true, the fanout operation should abort immediately on the first
    /// failing `on_response`. Default is `false` to preserve existing
    /// per-shard error aggregation behavior.
    fn fail_fast(&self) -> bool {
        false
    }

    fn generate_error_reply(&self) -> FanoutError {
        FanoutError::custom(format!(
            "Internal error in fanout operation '{}'",
            Self::name()
        ))
    }
}

/// Execute the fanout operation across cluster nodes.
pub fn exec_command<OP: FanoutCommand, F>(
    ctx: &Context,
    command: OP,
    targets: FanoutTarget,
    timeout: Duration,
    f: F,
) -> FanoutResult
where
    F: FnOnce(OP, FanoutCommandResult) + Send + 'static,
{
    let op = command;
    let (targets, cluster_fingerprint) = get_fanout_targets(ctx, targets);

    let req = op.generate_request();
    let outstanding = targets.len();
    let fanout_user = get_fanout_user(ctx);
    let db = get_current_db(ctx);

    let local_node = targets.iter().find(|x| x.is_local()).copied();

    // The local share always goes through the thread pool: `get_local_response`
    // takes the GIL itself, which this thread already holds, and the pool keeps
    // the main thread free while the shard-local reply is materialized.
    let local_req = match local_node {
        // Local-only fanout: there is no RPC to set up, so the local share is
        // the whole operation.
        Some(local) if outstanding == 1 => {
            let state = Arc::new(FanoutState::new(op, outstanding, f));
            spawn_local_request(state, req, local, fanout_user, db);
            return Ok(());
        }
        Some(_) => Some(op.generate_request()),
        None => None,
    };

    let state = Arc::new(FanoutState::new(op, outstanding, f));

    let rpc_state = state.clone();
    let response_handler = move |res: Result<&[u8], FanoutError>, target: &NodeInfo| {
        let Ok(buf) = res else {
            rpc_state.on_error(res.err().unwrap(), target);
            return;
        };
        match OP::Response::deserialize(buf) {
            Ok(resp) => rpc_state.on_response(resp, target),
            Err(e) => {
                let err =
                    FanoutError::serialization(format!("Failed to deserialize response: {e}"));
                rpc_state.on_error(err, target);
            }
        }
    };

    if let Err(e) = invoke_rpc(
        ctx,
        OP::name(),
        req,
        targets,
        cluster_fingerprint,
        Box::new(response_handler),
        timeout,
    ) {
        // RPC invocation failed before the fanout could be set up. The local
        // share has deliberately not been spawned yet, so no callback can run
        // and the state's lifecycle is still `Pending`: dropping `state` (and
        // the handler's clone, released by `invoke_rpc`) discards the completion
        // callback without invoking it, which releases whatever it retains —
        // for client commands, the blocked client — so the caller can reply
        // with this error right away instead of waiting on a local response
        // that would complete the fanout with a partial result.
        return Err(FanoutError::from(e));
    }

    // Only now that the remote side is in flight is it safe to spawn the local
    // share: from here on the fanout completes through the normal
    // response/timeout path.
    if let Some((local, req_local)) = local_node.zip(local_req) {
        spawn_local_request(state, req_local, local, fanout_user, db);
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FanoutLifecycleState {
    /// Fanout state exists but no response/error callback has run yet.
    Pending,
    /// At least one callback has run and completion may be needed on drop.
    Active,
    /// Completion callback already ran (or was explicitly finalized).
    Completed,
}

/// Internal structure to manage the state of an ongoing fanout operation.
struct FanoutStateInner<OP, F>
where
    OP: FanoutCommand + 'static,
    F: FnOnce(OP, FanoutCommandResult) + Send + 'static,
{
    operation: OP,
    /// Explicit lifecycle for drop/completion handling.
    lifecycle: FanoutLifecycleState,
    outstanding: usize,
    timed_out: bool,
    error_count: usize,
    /// When set, the fanout has been aborted fail-fast (e.g. a cluster-map
    /// mismatch) and this exact error is returned to the client, bypassing the
    /// per-shard error aggregation.
    abort_error: Option<FanoutError>,
    callback: Option<F>,
}

impl<OP, F> FanoutStateInner<OP, F>
where
    OP: FanoutCommand + 'static,
    F: FnOnce(OP, FanoutCommandResult) + Send + 'static,
{
    fn activate(&mut self) {
        if self.lifecycle == FanoutLifecycleState::Pending {
            self.lifecycle = FanoutLifecycleState::Active;
        }
    }

    fn rpc_done(&mut self) {
        self.outstanding = self.outstanding.saturating_sub(1);
        if self.outstanding == 0 {
            self.on_completion();
        }
    }

    fn on_error(&mut self, error: FanoutError, target: &NodeInfo) {
        // Once the fanout has completed, `self.operation` has been moved out via
        // `mem::take` and replaced with `OP::default()`, which may not be a
        // valid accumulator. Ignore any late-arriving callbacks so we never
        // invoke the operation on that placeholder.
        if self.lifecycle == FanoutLifecycleState::Completed {
            return;
        }
        self.activate();
        // Invoke the handler's error callback for custom error handling
        self.operation.on_error(error.clone(), target);
        self.error_count += 1;
        if error.kind == ErrorKind::Timeout {
            self.timed_out = true;
            // Zero out outstanding so that any late-arriving responses become
            // no-ops — rpc_done saturates at 0 and won't retrigger completion.
            self.outstanding = 0;
            self.on_completion();
            return;
        }
        // A cluster-map mismatch means the topology moved underneath us; the
        // aggregate result is unreliable, so abort the whole fanout immediately
        // and surface the mismatch error to the client.
        if error.kind == ErrorKind::ClusterMapMismatch {
            self.abort_error = Some(error);
            self.on_completion();
            return;
        }
        // A read/key-permission denial on any shard fails the whole multi-shard
        // command closed: data-returning commands (MGET/MRANGE/MREVRANGE) must not
        // silently drop keys the caller cannot read. Surface the shard's permission
        // error to the client verbatim (bypassing the generic aggregate error) and
        // stop waiting on the remaining shards.
        if matches!(
            error.kind,
            ErrorKind::KeyPermissions | ErrorKind::Permissions
        ) {
            self.abort_error = Some(error);
            self.on_completion();
            return;
        }
        self.rpc_done();
    }

    fn on_response(&mut self, resp: OP::Response, target: &NodeInfo) {
        // See `on_error`: after completion `self.operation` is a `mem::take`
        // placeholder, so drop late responses instead of accumulating into it.
        if self.lifecycle == FanoutLifecycleState::Completed {
            return;
        }
        self.activate();
        if self.timed_out {
            // We already timed out; ignore responses but mark RPC as done.
            self.rpc_done();
            return;
        }

        // Call the operation's on_response and treat any error as a per-shard error, maintaining
        // consistent bookkeeping (error_count, timed_out handling, logging).
        match self.operation.on_response(resp, target) {
            Ok(()) => self.rpc_done(),
            Err(err) => {
                // If the operation requests fail-fast behavior, abort the fanout
                // immediately after invoking the command's on_error hook and
                // incrementing the error count. Otherwise, treat it as a normal
                // per-shard error and continue.
                if self.operation.fail_fast() {
                    // Allow the operation to run its error handler for diagnostics
                    self.operation.on_error(err.clone(), target);
                    self.error_count += 1;
                    // Zero out outstanding so late responses are no-ops, then
                    // immediately complete the fanout (do not wait for other shards).
                    self.outstanding = 0;
                    self.on_completion();
                } else {
                    self.on_error(err, target);
                }
            }
        }
    }

    fn on_completion(&mut self) {
        if self.lifecycle == FanoutLifecycleState::Completed {
            return;
        }
        self.lifecycle = FanoutLifecycleState::Completed;

        let Some(callback) = self.callback.take() else {
            // we've already responded
            return;
        };

        let result = if let Some(err) = self.abort_error.take() {
            Err(err)
        } else if self.timed_out {
            Err(FanoutError::timeout())
        } else if self.error_count > 0 {
            Err(self.operation.generate_error_reply())
        } else {
            self.operation.on_completion();
            Ok(())
        };

        let operation = std::mem::take(&mut self.operation);
        callback(operation, result);
    }
}

impl<OP, F> Drop for FanoutStateInner<OP, F>
where
    OP: FanoutCommand + 'static,
    F: FnOnce(OP, FanoutCommandResult) + Send + 'static,
{
    fn drop(&mut self) {
        if self.lifecycle == FanoutLifecycleState::Active {
            self.on_completion();
        }
    }
}

/// Internal structure to manage the state of an ongoing fanout operation.
/// It tracks outstanding RPCs, errors, and coordinates the final reply generation.
struct FanoutState<OP, F>
where
    OP: FanoutCommand,
    F: FnOnce(OP, FanoutCommandResult) + Send + 'static,
{
    inner: Mutex<FanoutStateInner<OP, F>>,
}

impl<OP, F> FanoutState<OP, F>
where
    OP: FanoutCommand + 'static,
    F: FnOnce(OP, FanoutCommandResult) + Send + 'static,
{
    fn new(operation: OP, outstanding: usize, f: F) -> Self {
        Self {
            inner: Mutex::new(FanoutStateInner {
                operation,
                outstanding,
                lifecycle: FanoutLifecycleState::Pending,
                error_count: 0,
                timed_out: false,
                abort_error: None,
                callback: Some(f),
            }),
        }
    }

    fn on_error(&self, error: FanoutError, target: &NodeInfo) {
        let mut inner = lock(&self.inner);
        inner.on_error(error, target);
    }

    fn on_response(&self, resp: OP::Response, target: &NodeInfo) {
        let mut inner = lock(&self.inner);
        inner.on_response(resp, target);
    }
}

/// Spawn a local request handler in a separate thread.
///
/// `user` and `db` are the coordinator-side client's ACL identity and selected
/// database; the [`FanoutContext`] built from them applies both each time
/// `get_local_response` takes the GIL.
fn spawn_local_request<OP, F>(
    state: Arc<FanoutState<OP, F>>,
    req: OP::Request,
    target: NodeInfo,
    user: Option<String>,
    db: i32,
) where
    OP: FanoutCommand,
    OP::Request: Send + 'static,
    OP::Response: Send + 'static,
    F: FnOnce(OP, FanoutCommandResult) + Send + 'static,
{
    spawn(move || {
        let fanout_ctx = FanoutContext::new(user, db);
        match OP::get_local_response(&fanout_ctx, req) {
            Ok(response) => state.on_response(response, &target),
            Err(err) => state.on_error(err.into(), &target),
        }
    });
}
