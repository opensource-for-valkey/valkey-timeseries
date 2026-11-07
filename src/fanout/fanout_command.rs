use super::cluster_rpc::{get_cluster_command_timeout, invoke_rpc};
use super::fanout_error::{ErrorKind, FanoutError};
use crate::common::acl::caller_user_name;
use crate::common::context::get_current_db;
use crate::common::logging::{log_debug, log_warning};
use crate::common::metrics::fanout as metrics;
use crate::common::sync::lock;
use crate::common::threads::Rejected;
use crate::fanout::fanout_context::FanoutContext;
use crate::fanout::serialization::{Deserialized, Serializable};
use crate::fanout::workers::PEER_REQUEST_EXECUTOR;
use crate::fanout::{
    FanoutResult, FanoutTarget, NodeInfo, compute_query_fanout_mode, get_fanout_targets,
};
use metered::Counter;
use std::panic::{self, AssertUnwindSafe};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
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

    /// Whether a share whose requester has given up may be abandoned: dropped unrun when it
    /// waited in the queue past the operation's timeout, or stopped part-way through its read.
    ///
    /// `false` for an operation that writes: abandoning it on some shards and not others
    /// would leave a partial write behind a reply that already said the command timed out,
    /// so it runs to the end on every shard that received it.
    fn cancellable() -> bool {
        true
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
        let timeout = get_cluster_command_timeout();
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
}

/// The reply for a fanout that failed on some share with an error that does not
/// abort it. Names the first such share, so the client sees more than that it failed.
fn generic_error_reply(name: &str, first_error: Option<&(String, FanoutError)>) -> FanoutError {
    match first_error {
        Some((node, error)) => FanoutError::custom(format!(
            "Internal error in fanout operation '{name}': {node}: {error}"
        )),
        None => FanoutError::custom(format!("Internal error in fanout operation '{name}'")),
    }
}

fn describe_target(target: &NodeInfo) -> String {
    if target.is_local() {
        "local node".to_string()
    } else {
        format!("node {}", target.socket_address)
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
    let started = Instant::now();
    let (targets, cluster_fingerprint) = get_fanout_targets(ctx, targets);
    metrics::FANOUT_OPERATIONS.incr();
    metrics::FANOUT_TARGETS.incr_by(targets.len() as u64);

    let req = op.generate_request();
    let outstanding = targets.len();
    let fanout_user = caller_user_name(ctx);
    let db = get_current_db(ctx);
    let deadline = Instant::now() + timeout;

    let local_node = targets.iter().find(|x| x.is_local()).copied();

    // The local share always runs off the main thread: `get_local_response`
    // takes the GIL itself, which this thread already holds, and running it on
    // a worker keeps the main thread free while the reply is materialized.
    //
    // Local-only fanout: there is no RPC to set up, so the local share is the
    // whole operation. If the workers are saturated, nothing has run: dropping
    // `state` while it is still `Pending` discards the callback without
    // invoking it, as on an RPC setup failure below.
    if let Some(local) = local_node
        && outstanding == 1
    {
        metrics::FANOUT_LOCAL_ONLY.incr();
        let state = Arc::new(FanoutState::new(op, outstanding, f, started));
        return spawn_local_request(state, req, local, fanout_user, db, deadline).map_err(|_| {
            metrics::FANOUT_LOCAL_SHARE_BUSY.incr();
            metrics::FANOUT_DURATION.observe_duration(started.elapsed());
            FanoutError::busy()
        });
    }

    let state = Arc::new(FanoutState::new(op, outstanding, f, started));

    let rpc_state = state.clone();
    let response_handler = move |res: Result<&[u8], FanoutError>, target: &NodeInfo| {
        let buf = match res {
            Ok(buf) => buf,
            Err(err) => {
                rpc_state.on_error(err, target);
                return;
            }
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
        &req,
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
        metrics::FANOUT_SETUP_FAILURES.incr();
        metrics::FANOUT_DURATION.observe_duration(started.elapsed());
        return Err(FanoutError::from(e));
    }

    // The local share is spawned only once the RPC is set up, so a setup
    // failure above leaves the state `Pending` with no callback able to run.
    // A send that failed synchronously inside `invoke_rpc` may already have
    // completed the fanout, though; the share then sees it completed and, if
    // cancellable, skips its work, while a write still runs and its answer is
    // ignored.
    if let Some(local) = local_node {
        let local_state = Arc::clone(&state);
        if spawn_local_request(local_state, req, local, fanout_user, db, deadline).is_err() {
            // Remote shares are already in flight, so this one fails through
            // the fanout: `Busy` aborts it with that error.
            metrics::FANOUT_LOCAL_SHARE_BUSY.incr();
            state.on_error(FanoutError::busy(), &local);
        }
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
    /// The first error that did not abort the fanout, with the share it came
    /// from, for the generic error reply.
    first_error: Option<(String, FanoutError)>,
    /// When set, the fanout has been aborted fail-fast (e.g. a cluster-map
    /// mismatch) and this exact error is returned to the client, bypassing the
    /// per-shard error aggregation.
    abort_error: Option<FanoutError>,
    callback: Option<F>,
    /// When the operation started, for `fanout_duration_seconds`.
    started: Instant,
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
            // A deadline arriving after the result is not a shard's answer.
            if error.kind != ErrorKind::Timeout {
                metrics::FANOUT_IGNORED_AFTER_COMPLETION.incr();
            }
            return;
        }
        self.activate();
        metrics::record_error(error.kind);
        let msg = format!(
            "Fanout operation {} failed for {}: {error}",
            OP::name(),
            describe_target(target)
        );
        if error.kind.is_client_visible() {
            log_debug(msg);
        } else {
            log_warning(msg);
        }
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
        // stop waiting on the remaining shards. A shard that was too busy to take
        // its share, or whose database `SWAPDB` kept exchanging under its read,
        // likewise leaves the result incomplete, and the client should see that it
        // can retry rather than a generic aggregate error.
        if matches!(
            error.kind,
            ErrorKind::KeyPermissions
                | ErrorKind::Permissions
                | ErrorKind::Busy
                | ErrorKind::DatabaseSwapped
        ) {
            self.abort_error = Some(error);
            self.on_completion();
            return;
        }
        if self.first_error.is_none() {
            self.first_error = Some((describe_target(target), error));
        }
        self.rpc_done();
    }

    fn on_response(&mut self, resp: OP::Response, target: &NodeInfo) {
        // See `on_error`: after completion `self.operation` is a `mem::take`
        // placeholder, so drop late responses instead of accumulating into it.
        if self.lifecycle == FanoutLifecycleState::Completed {
            metrics::FANOUT_IGNORED_AFTER_COMPLETION.incr();
            return;
        }
        self.activate();

        // Treat an error from the operation as a per-shard error, with the same
        // bookkeeping (error_count, timed_out handling, logging).
        match self.operation.on_response(resp, target) {
            Ok(()) => self.rpc_done(),
            Err(err) => self.on_error(err, target),
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

        metrics::FANOUT_DURATION.observe_duration(self.started.elapsed());
        let result = if let Some(err) = self.abort_error.take() {
            metrics::FANOUT_ABORTS.incr();
            Err(err)
        } else if self.timed_out {
            Err(FanoutError::timeout())
        } else if self.error_count > 0 {
            metrics::FANOUT_GENERIC_ERROR_REPLIES.incr();
            Err(generic_error_reply(OP::name(), self.first_error.as_ref()))
        } else {
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
    /// The last reference went away while the fanout was still waiting on
    /// shares (a share's owner was lost without answering). Complete it as a
    /// failure: the answers it has are not the whole result.
    fn drop(&mut self) {
        if self.lifecycle == FanoutLifecycleState::Active {
            if self.outstanding > 0 {
                self.error_count += self.outstanding;
                if self.first_error.is_none() {
                    let error = FanoutError::custom(format!(
                        "{} share(s) ended without an answer",
                        self.outstanding
                    ));
                    self.first_error = Some(("fanout".to_string(), error));
                }
            }
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
    fn new(operation: OP, outstanding: usize, f: F, started: Instant) -> Self {
        Self {
            inner: Mutex::new(FanoutStateInner {
                operation,
                outstanding,
                lifecycle: FanoutLifecycleState::Pending,
                error_count: 0,
                timed_out: false,
                first_error: None,
                abort_error: None,
                callback: Some(f),
                started,
            }),
        }
    }

    fn on_error(&self, error: FanoutError, target: &NodeInfo) {
        let mut inner = lock(&self.inner);
        inner.on_error(error, target);
    }

    fn is_completed(&self) -> bool {
        lock(&self.inner).lifecycle == FanoutLifecycleState::Completed
    }

    fn on_response(&self, resp: OP::Response, target: &NodeInfo) {
        let mut inner = lock(&self.inner);
        inner.on_response(resp, target);
    }
}

/// Queue the local share on the fanout workers.
///
/// `user` and `db` are the coordinator-side client's ACL identity and selected
/// database; the [`FanoutContext`] built from them applies both each time
/// `get_local_response` takes the GIL.
///
/// On `Err` the share was not queued and `state` has been dropped; the caller
/// decides how the fanout fails.
fn spawn_local_request<OP, F>(
    state: Arc<FanoutState<OP, F>>,
    req: OP::Request,
    target: NodeInfo,
    user: Option<String>,
    db: i32,
    deadline: Instant,
) -> Result<(), Rejected>
where
    OP: FanoutCommand,
    OP::Request: Send + 'static,
    OP::Response: Send + 'static,
    F: FnOnce(OP, FanoutCommandResult) + Send + 'static,
{
    // A blocking thread: `get_local_response` takes the GIL (R1 in `common::threads`).
    PEER_REQUEST_EXECUTOR.try_spawn(move || {
        // A share that waited in the queue may no longer be wanted: the fanout
        // already completed (an RPC timeout or a fail-fast error), or its
        // deadline passed and the blocked client has had the timeout error.
        // A write runs regardless (`FanoutCommand::cancellable`); a late answer
        // is ignored once the fanout has completed.
        let fanout_ctx = FanoutContext::new(user, db);
        if OP::cancellable() {
            if state.is_completed() {
                return;
            }
            if Instant::now() >= deadline {
                metrics::FANOUT_LOCAL_SHARE_EXPIRED.incr();
                state.on_error(FanoutError::timeout(), &target);
                return;
            }
            fanout_ctx.set_deadline(deadline);
        }
        // A panic in the handler would otherwise unwind past the share and leave
        // the fanout waiting on it; make it this share's failure instead.
        let outcome = panic::catch_unwind(AssertUnwindSafe(|| {
            OP::get_local_response(&fanout_ctx, req)
        }));
        match outcome {
            Ok(Ok(response)) => state.on_response(response, &target),
            Ok(Err(err)) => state.on_error(err.into(), &target),
            Err(_) => {
                let err =
                    FanoutError::custom(format!("{} panicked on the local share", OP::name()));
                state.on_error(err, &target);
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fanout::cluster_map::{NodeLocation, NodeRole, SocketAddress};
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::mpsc::{self, Receiver};

    #[derive(Clone, PartialEq, prost::Message)]
    struct Count {
        #[prost(uint64, tag = "1")]
        count: u64,
    }

    /// Sums the counts it is sent; fails a share whose count is `u64::MAX`.
    #[derive(Default)]
    struct Sum {
        total: u64,
    }

    impl FanoutCommand for Sum {
        type Request = Count;
        type Response = Count;

        fn name() -> &'static str {
            "sum"
        }

        fn get_local_response(_: &FanoutContext, req: Count) -> ValkeyResult<Count> {
            Ok(req)
        }

        fn generate_request(&self) -> Count {
            Count::default()
        }

        fn on_response(&mut self, resp: Count, _: &NodeInfo) -> FanoutCommandResult {
            if resp.count == u64::MAX {
                return Err(FanoutError::custom("bad count"));
            }
            self.total += resp.count;
            Ok(())
        }
    }

    type Outcome = (u64, FanoutCommandResult);

    fn node(port: u16) -> NodeInfo {
        NodeInfo {
            id: Default::default(),
            shard_id: Default::default(),
            socket_address: SocketAddress {
                primary_endpoint: IpAddr::V4(Ipv4Addr::LOCALHOST),
                port,
            },
            role: NodeRole::Primary,
            location: NodeLocation::Remote,
            healthy: true,
        }
    }

    fn fanout(
        shares: usize,
    ) -> (
        FanoutState<Sum, impl FnOnce(Sum, FanoutCommandResult) + Send + 'static>,
        Receiver<Outcome>,
    ) {
        let (tx, rx) = mpsc::channel();
        let callback = move |op: Sum, result| tx.send((op.total, result)).unwrap();
        (
            FanoutState::new(Sum::default(), shares, callback, Instant::now()),
            rx,
        )
    }

    fn answer(count: u64) -> Count {
        Count { count }
    }

    #[test]
    fn test_completes_once_after_every_share() {
        let (state, rx) = fanout(2);
        state.on_response(answer(3), &node(1));
        assert!(rx.try_recv().is_err());
        state.on_response(answer(4), &node(2));
        let (total, result) = rx.try_recv().unwrap();
        assert_eq!(total, 7);
        assert!(result.is_ok());

        // A late or duplicate answer neither completes again nor touches the placeholder.
        state.on_response(answer(5), &node(2));
        state.on_error(FanoutError::busy(), &node(2));
        drop(state);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test_drop_with_shares_outstanding_fails() {
        let (state, rx) = fanout(2);
        state.on_response(answer(7), &node(1));
        drop(state);
        let (total, result) = rx.try_recv().unwrap();
        assert_eq!(total, 7);
        let err = result.unwrap_err();
        assert!(err.to_string().contains("ended without an answer"), "{err}");
    }

    #[test]
    fn test_drop_before_any_share_discards_the_callback() {
        let (state, rx) = fanout(2);
        drop(state);
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::TryRecvError::Disconnected)
        ));
    }

    #[test]
    fn test_generic_error_names_the_first_failed_share() {
        let (state, rx) = fanout(3);
        state.on_response(answer(u64::MAX), &node(7001));
        state.on_error(FanoutError::custom("send failed"), &node(7002));
        state.on_response(answer(1), &node(7003));
        let err = rx.try_recv().unwrap().1.unwrap_err();
        let text = err.to_string();
        assert!(
            text.contains("Internal error in fanout operation 'sum'"),
            "{text}"
        );
        assert!(text.contains("node 127.0.0.1:7001: bad count"), "{text}");
    }

    #[test]
    fn test_aborting_error_wins_over_earlier_share_errors() {
        let (state, rx) = fanout(3);
        state.on_error(FanoutError::custom("send failed"), &node(1));
        state.on_error(FanoutError::busy(), &node(2));
        let err = rx.try_recv().unwrap().1.unwrap_err();
        assert_eq!(err.kind, ErrorKind::Busy);
    }

    #[test]
    fn test_timeout_completes_and_ignores_later_answers() {
        let (state, rx) = fanout(2);
        state.on_response(answer(1), &node(1));
        state.on_error(FanoutError::timeout(), &node(0));
        assert_eq!(
            rx.try_recv().unwrap().1.unwrap_err().kind,
            ErrorKind::Timeout
        );
        state.on_response(answer(1), &node(2));
        drop(state);
        assert!(rx.try_recv().is_err());
    }
}
