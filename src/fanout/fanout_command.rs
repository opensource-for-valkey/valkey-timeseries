use super::cluster_rpc::{get_cluster_command_timeout, invoke_rpc, validate_cluster_exec};
use super::fanout_error::{ErrorKind, FanoutError};
use crate::common::acl::caller_user_name;
use crate::common::context::get_current_db;
use crate::common::metrics::fanout as metrics;
use crate::common::sync::lock;
use crate::common::threads::{BoundedExecutor, Rejected};
use crate::fanout::fanout_context::FanoutContext;
use crate::fanout::serialization::{Deserialized, Serializable};
use crate::fanout::workers::PEER_REQUEST_EXECUTOR;
use crate::fanout::{
    FanoutResult, FanoutTarget, NodeInfo, compute_query_fanout_target, get_fanout_targets,
};
use metered::Counter;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use valkey_module::{Context, ValkeyResult};

/// Where a fan-out stands once a shard's answer has been handed to it — what the RPC layer
/// checks before it stops the deadline timer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FanoutProgress {
    /// The fan-out has completed: nothing it runs still needs the deadline.
    Done,
    /// The fan-out has not completed. After the last remote answer, that means work for it
    /// still waits on the worker lane — a deferred decode, or the local share — and only the
    /// deadline timer can end it if the lane is slow to get there.
    Pending,
}

pub(super) type FanoutResponseCallback =
    Box<dyn Fn(FanoutResult<&[u8]>, &NodeInfo) -> FanoutProgress + Send + Sync>;

pub type FanoutCommandResult<T = ()> = Result<T, FanoutError>;

/// Log a per-target fanout failure — the warning [`FanoutCommand::on_error`]
/// emits by default.
///
/// Exposed because Rust has no way to call a trait method's default body from an
/// override: an implementation that overrides `on_error` only to latch some
/// state still has to report the failure, and should report it identically.
pub fn log_fanout_failure(operation: &str, target: &NodeInfo, error: &FanoutError) {
    crate::common::logging::log_warning(format!(
        "Fanout operation {operation}, failed for target {}: {error}",
        target.socket_address,
    ))
}

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

    /// How long the requester of `req` waits for its answer, if the request says.
    ///
    /// A receiving node counts it from when the request arrived: a request still queued
    /// past it is dropped unrun, and a read past it stops between batches. Measured from
    /// arrival rather than carried as a time of day, so it needs no clock agreement, and
    /// the receiver's deadline can only fall later than the requester's, never earlier.
    /// `None` (the default) never expires.
    fn time_budget(_req: &Self::Request) -> Option<Duration> {
        None
    }

    /// Return the timeout duration for the entire fanout operation.
    /// This timeout applies to the overall operation, not individual RPC calls.
    fn get_timeout(&self) -> Duration {
        get_cluster_command_timeout()
    }

    /// Get the target nodes for the fanout operation, bound to the cluster-map
    /// fingerprint of the snapshot they were selected from.
    /// By default, it retrieves a random replica per shard.
    fn get_targets(&self, ctx: &Context) -> FanoutTarget {
        compute_query_fanout_target(ctx)
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
        log_fanout_failure(Self::name(), target, &error);
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
/// todo: pass in nodes to target instead of letting the command decide, for better separation of concerns.
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
    // Validate up front (cluster mode enabled, not in MULTI/Lua) so we fail early
    // with a clear error before setting up any fanout machinery or dispatching
    // local/remote requests.
    validate_cluster_exec(ctx)?;

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
    let local_req = match local_node {
        // Local-only fanout: there is no RPC to set up, so the local share is
        // the whole operation. If the workers are saturated, nothing has run:
        // dropping `state` while it is still `Pending` discards the callback
        // without invoking it, as on an RPC setup failure below.
        Some(local) if outstanding == 1 => {
            metrics::FANOUT_LOCAL_ONLY.incr();
            let state = Arc::new(FanoutState::new(op, outstanding, f, started));
            return spawn_local_request(state, req, local, fanout_user, db, deadline).map_err(
                |_| {
                    metrics::FANOUT_LOCAL_SHARE_BUSY.incr();
                    metrics::FANOUT_DURATION.observe_duration(started.elapsed());
                    FanoutError::busy()
                },
            );
        }
        Some(_) => Some(op.generate_request()),
        None => None,
    };

    let state = Arc::new(FanoutState::new(op, outstanding, f, started));

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
        metrics::FANOUT_SETUP_FAILURES.incr();
        metrics::FANOUT_DURATION.observe_duration(started.elapsed());
        return Err(FanoutError::from(e));
    }

    // Only now that the remote side is in flight is it safe to spawn the local
    // share: from here on the fanout completes through the normal
    // response/timeout path.
    if let Some((local, req_local)) = local_node.zip(local_req) {
        let local_state = Arc::clone(&state);
        if spawn_local_request(local_state, req_local, local, fanout_user, db, deadline).is_err() {
            // Remote shares are already in flight, so this one fails through
            // the fanout: `Busy` aborts it with that error.
            metrics::FANOUT_LOCAL_SHARE_BUSY.incr();
            state.on_error(FanoutError::busy(), &local);
        }
    }

    Ok(())
}

/// The smallest shard response decoded off the main thread. A smaller one decodes in
/// well under the ~50 µs a hand-off to a worker costs; at this size decoding is about
/// 0.1 ms, and a range or grid response can be megabytes.
///
/// A deferred response counts as its shard's answer as soon as it arrives, but the fan-out
/// is not done with it until the decode has run, so the RPC keeps its deadline timer armed
/// meanwhile (see [`FanoutProgress`]): a lane too busy to reach the decode in time ends the
/// fan-out with a timeout at its deadline, not whenever the lane's queue drains.
const DEFERRED_DECODE_MIN_BYTES: usize = 64 * 1024;

/// The RPC layer's handler for one fan-out's remote answers, and for its deadline. Runs on
/// the main thread, inside the cluster-bus callback or the timeout timer.
fn rpc_response_handler<OP, F>(
    state: Arc<FanoutState<OP, F>>,
    lane: &'static BoundedExecutor,
    deadline: Instant,
) -> impl Fn(FanoutResult<&[u8]>, &NodeInfo) -> FanoutProgress + Send + Sync + 'static
where
    OP: FanoutCommand + 'static,
    F: FnOnce(OP, FanoutCommandResult) + Send + 'static,
{
    move |res, target| {
        // Already answered (a deadline, a fail-fast error): nothing would read this one, so
        // don't decode it either.
        if state.is_completed() {
            return FanoutProgress::Done;
        }
        match res {
            Err(err) => state.on_error(err, target),
            Ok(buf) if buf.len() >= DEFERRED_DECODE_MIN_BYTES => {
                // Decoding and merging a large response here stalls every client of the
                // node for as long as they take; copying it out is a fraction of that.
                if defer_response(lane, Arc::clone(&state), buf, *target, deadline).is_ok() {
                    return FanoutProgress::Pending;
                }
                // The lane is full: decode here, as before, rather than fail the fan-out.
                deliver_response(&state, buf, target);
            }
            Ok(buf) => deliver_response(&state, buf, target),
        }
        state.progress()
    }
}

/// Queue a shard's response for decoding on `lane`. A decode that is only reached past the
/// fan-out's `deadline`, or after the fan-out has ended, is dropped unread (see
/// [`FanoutState::still_wanted`]).
///
/// On `Err` nothing was queued — or copied: the place in the queue is claimed first.
fn defer_response<OP, F>(
    lane: &'static BoundedExecutor,
    state: Arc<FanoutState<OP, F>>,
    buf: &[u8],
    target: NodeInfo,
    deadline: Instant,
) -> Result<(), Rejected>
where
    OP: FanoutCommand + 'static,
    F: FnOnce(OP, FanoutCommandResult) + Send + 'static,
{
    let slot = lane.try_reserve()?;
    let owned = buf.to_vec();
    slot.spawn(move || {
        if state.still_wanted(deadline, &target) {
            deliver_response(&state, &owned, &target);
        }
    });
    Ok(())
}

/// Decode one shard's response and hand it to the fan-out.
fn deliver_response<OP, F>(state: &FanoutState<OP, F>, buf: &[u8], target: &NodeInfo)
where
    OP: FanoutCommand + 'static,
    F: FnOnce(OP, FanoutCommandResult) + Send + 'static,
{
    match OP::Response::deserialize(buf) {
        Ok(resp) => state.on_response(resp, target),
        Err(e) => {
            let err = FanoutError::serialization(format!("Failed to deserialize response: {e}"));
            state.on_error(err, target);
        }
    }
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
        // stop waiting on the remaining shards. A shard that was too busy to take
        // its share, or whose database `SWAPDB` kept exchanging under its read,
        // likewise leaves the result incomplete, and the client should see that it
        // can retry rather than a generic aggregate error. A query limit a shard
        // enforced on its share is surfaced the same way: its message names the
        // limit and both numbers, and the other shards' answers cannot rescue a read
        // that is already refused.
        if matches!(
            error.kind,
            ErrorKind::KeyPermissions
                | ErrorKind::Permissions
                | ErrorKind::Busy
                | ErrorKind::DatabaseSwapped
                | ErrorKind::QueryLimit
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
            metrics::FANOUT_IGNORED_AFTER_COMPLETION.incr();
            return;
        }
        self.activate();
        if self.timed_out {
            // We already timed out; ignore responses but mark RPC as done.
            metrics::FANOUT_IGNORED_AFTER_COMPLETION.incr();
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
                    metrics::record_error(err.kind);
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

        metrics::FANOUT_DURATION.observe_duration(self.started.elapsed());
        let result = if let Some(err) = self.abort_error.take() {
            metrics::FANOUT_ABORTS.incr();
            Err(err)
        } else if self.timed_out {
            Err(FanoutError::timeout())
        } else if self.error_count > 0 {
            metrics::FANOUT_GENERIC_ERROR_REPLIES.incr();
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
    fn new(operation: OP, outstanding: usize, f: F, started: Instant) -> Self {
        Self {
            inner: Mutex::new(FanoutStateInner {
                operation,
                outstanding,
                lifecycle: FanoutLifecycleState::Pending,
                error_count: 0,
                timed_out: false,
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

    fn progress(&self) -> FanoutProgress {
        if self.is_completed() {
            FanoutProgress::Done
        } else {
            FanoutProgress::Pending
        }
    }

    /// Whether work queued for this fan-out — the local share, a deferred decode — is still
    /// wanted when a worker gets to it. Not once the fan-out has completed (a deadline or a
    /// fail-fast error), and not past `deadline`, where it ends the fan-out with the timeout
    /// instead: the requester has its answer, or is owed that one.
    fn still_wanted(&self, deadline: Instant, target: &NodeInfo) -> bool {
        if self.is_completed() {
            return false;
        }
        if Instant::now() >= deadline {
            self.on_error(FanoutError::timeout(), target);
            return false;
        }
        true
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
        if state.is_completed() {
            return;
        }
        if Instant::now() >= deadline {
            metrics::FANOUT_LOCAL_SHARE_EXPIRED.incr();
            state.on_error(FanoutError::timeout(), &target);
            return;
        }
        let fanout_ctx = FanoutContext::new(user, db);
        // Past it the fan-out has answered with a timeout, so a long read stops there.
        fanout_ctx.set_deadline(deadline);
        match OP::get_local_response(&fanout_ctx, req) {
            Ok(response) => state.on_response(response, &target),
            Err(err) => state.on_error(err.into(), &target),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::threads::Capacity;
    use crate::promql::generated::LabelProfileQuery;
    use std::sync::mpsc;

    const WAIT: Duration = Duration::from_secs(5);

    /// Counts the responses it was handed.
    #[derive(Default)]
    struct Counting {
        responses: usize,
    }

    impl FanoutCommand for Counting {
        type Request = LabelProfileQuery;
        type Response = LabelProfileQuery;

        fn name() -> &'static str {
            "test-counting"
        }

        fn get_local_response(
            _ctx: &FanoutContext,
            _req: LabelProfileQuery,
        ) -> ValkeyResult<LabelProfileQuery> {
            unreachable!("no local share in these tests")
        }

        fn generate_request(&self) -> LabelProfileQuery {
            LabelProfileQuery::default()
        }

        fn on_response(&mut self, _: LabelProfileQuery, _: &NodeInfo) -> FanoutCommandResult {
            self.responses += 1;
            Ok(())
        }
    }

    /// What the fan-out completed with: how many responses it took in, and the error kind if
    /// it failed.
    type Outcome = (usize, Option<ErrorKind>);

    type Completion = Box<dyn FnOnce(Counting, FanoutCommandResult) + Send>;

    /// A one-shard fan-out whose completion is sent to the returned receiver.
    fn fanout() -> (
        Arc<FanoutState<Counting, Completion>>,
        mpsc::Receiver<Outcome>,
    ) {
        let (tx, rx) = mpsc::channel();
        let completion: Completion = Box::new(move |op, res| {
            let _ = tx.send((op.responses, res.err().map(|e| e.kind)));
        });
        (
            Arc::new(FanoutState::new(Counting::default(), 1, completion)),
            rx,
        )
    }

    /// A valid response of at least `len` bytes: one unknown field, which decoding skips.
    fn response_of(len: usize) -> Vec<u8> {
        const UNKNOWN_BYTES_FIELD: u8 = (15 << 3) | 2;
        let mut buf = vec![UNKNOWN_BYTES_FIELD];
        prost::encoding::encode_varint(len as u64, &mut buf);
        buf.resize(buf.len() + len, 0);
        buf
    }

    fn large_response() -> Vec<u8> {
        response_of(DEFERRED_DECODE_MIN_BYTES)
    }

    fn one() -> usize {
        1
    }

    /// Occupies `lane`'s only worker until the returned sender is dropped.
    fn occupy(lane: &'static BoundedExecutor) -> mpsc::Sender<()> {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        lane.try_spawn(move || {
            entered_tx.send(()).unwrap();
            let _ = release_rx.recv();
        })
        .unwrap();
        entered_rx.recv_timeout(WAIT).unwrap();
        release_tx
    }

    /// Waits until every job queued on `lane` before this call has run.
    fn drain(lane: &'static BoundedExecutor) {
        let (tx, rx) = mpsc::channel();
        lane.try_spawn(move || tx.send(()).unwrap()).unwrap();
        rx.recv_timeout(WAIT).unwrap();
    }

    fn target() -> NodeInfo {
        NodeInfo::for_test(7001)
    }

    #[test]
    fn a_small_response_is_delivered_inline() {
        static LANE: BoundedExecutor =
            BoundedExecutor::new("test-fanout-inline", one, Capacity::Fixed(4));
        let (state, done) = fanout();
        let handler = rpc_response_handler(state, &LANE, Instant::now() + WAIT);

        assert_eq!(
            handler(Ok(&response_of(16)), &target()),
            FanoutProgress::Done
        );
        assert_eq!(done.try_recv(), Ok((1, None)));
    }

    #[test]
    fn a_large_response_is_decoded_on_the_lane() {
        static LANE: BoundedExecutor =
            BoundedExecutor::new("test-fanout-deferred", one, Capacity::Fixed(4));
        let (state, done) = fanout();
        let handler = rpc_response_handler(state, &LANE, Instant::now() + WAIT);

        // Answered, but not yet done with: the RPC must keep the deadline armed.
        assert_eq!(
            handler(Ok(&large_response()), &target()),
            FanoutProgress::Pending
        );
        assert_eq!(done.recv_timeout(WAIT), Ok((1, None)));
    }

    /// With the lane occupied, the RPC's deadline timer stays armed for a deferred response:
    /// firing ends the fan-out with a timeout while the decode is still queued, and the
    /// decode, when the lane does reach it, is dropped unread.
    #[test]
    fn the_deadline_ends_a_fanout_whose_decode_is_stuck_behind_a_busy_lane() {
        static LANE: BoundedExecutor =
            BoundedExecutor::new("test-fanout-busy", one, Capacity::Fixed(4));
        let release = occupy(&LANE);
        let (state, done) = fanout();
        let handler = rpc_response_handler(state, &LANE, Instant::now() + WAIT);

        assert_eq!(
            handler(Ok(&large_response()), &target()),
            FanoutProgress::Pending
        );
        assert!(done.try_recv().is_err(), "the decode cannot have run yet");

        // The timer fires (as `InFlightRequest::deliver_timeout` delivers it).
        assert_eq!(
            handler(Err(FanoutError::timeout()), &target()),
            FanoutProgress::Done
        );
        assert_eq!(
            done.try_recv(),
            Ok((0, Some(ErrorKind::Timeout))),
            "answered at the deadline, with the lane still occupied"
        );

        drop(release);
        drain(&LANE);
        assert!(done.try_recv().is_err(), "completed exactly once");
    }

    #[test]
    fn a_decode_the_lane_reaches_past_the_deadline_is_timed_out_unread() {
        static LANE: BoundedExecutor =
            BoundedExecutor::new("test-fanout-expired", one, Capacity::Fixed(4));
        let release = occupy(&LANE);
        let (state, done) = fanout();
        // Already due by the time the lane gets to the decode.
        let handler = rpc_response_handler(state, &LANE, Instant::now());

        assert_eq!(
            handler(Ok(&large_response()), &target()),
            FanoutProgress::Pending
        );
        drop(release);
        // A valid response: had it been decoded, the fan-out would have succeeded with it.
        assert_eq!(done.recv_timeout(WAIT), Ok((0, Some(ErrorKind::Timeout))));
    }

    #[test]
    fn a_full_lane_decodes_inline() {
        static LANE: BoundedExecutor =
            BoundedExecutor::new("test-fanout-full", one, Capacity::Fixed(1));
        let release = occupy(&LANE);
        LANE.try_spawn(|| {}).unwrap();
        let (state, done) = fanout();
        let handler = rpc_response_handler(state, &LANE, Instant::now() + WAIT);

        assert_eq!(
            handler(Ok(&large_response()), &target()),
            FanoutProgress::Done
        );
        assert_eq!(done.try_recv(), Ok((1, None)));
        drop(release);
    }

    #[test]
    fn an_answer_for_a_completed_fanout_is_not_decoded() {
        static LANE: BoundedExecutor =
            BoundedExecutor::new("test-fanout-late", one, Capacity::Fixed(4));
        let (state, done) = fanout();
        let handler = rpc_response_handler(state, &LANE, Instant::now() + WAIT);
        handler(Err(FanoutError::timeout()), &target());
        assert_eq!(done.try_recv(), Ok((0, Some(ErrorKind::Timeout))));

        // Late, and large: nothing is queued for it.
        assert_eq!(
            handler(Ok(&large_response()), &target()),
            FanoutProgress::Done
        );
        assert_eq!(LANE.stats().queued, 0);
        assert!(done.try_recv().is_err());
    }
}
