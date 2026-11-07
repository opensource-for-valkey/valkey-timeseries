use super::fanout_error::{ErrorKind, FanoutError, NO_CLUSTER_NODES_AVAILABLE};
use super::fanout_message::{
    FanoutMessage, FanoutMessageHeader, has_unsupported_features, serialize_request_message,
};
use super::utils::{is_clustered, is_multi_or_lua};
use crate::common::acl::caller_user_name;
use crate::common::context::{get_current_db, set_current_db};
use crate::common::hash::BuildNoHashHasher;
use crate::common::logging::{log_debug, log_error, log_warning};
use crate::common::metrics::clustermap as clustermap_metrics;
use crate::common::metrics::fanout::{self as metrics, WireCounters};
use crate::common::pool::get_pooled_buffer;
use crate::common::sync::lock;
use crate::common::threads::LockGil;
use crate::config::FANOUT_COMMAND_TIMEOUT;
use crate::fanout::cluster_map::{CURRENT_NODE_ID, NodeId, NodeRole, SocketAddress};
use crate::fanout::fanout_command::FanoutResponseCallback;
use crate::fanout::fanout_context::FanoutContext;
use crate::fanout::registry::{RegisteredOperation, get_fanout_request_handler};
use crate::fanout::serialization::Serializable;
use crate::fanout::workers::PEER_REQUEST_EXECUTOR;
use crate::fanout::{
    FanoutResult, NodeInfo, current_or_refreshed_cluster_map, get_cluster_map,
    mark_cluster_map_stale, refresh_cluster_map,
};
use ahash::HashSet;
use core::time::Duration;
use metered::Counter;
use papaya::HashMap;
use std::hash::{BuildHasher, RandomState};
use std::net::{IpAddr, Ipv4Addr};
use std::os::raw::{c_char, c_int, c_uchar};
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Instant;
use valkey_module::{
    Context, DetachedContext, MODULE_CONTEXT, RedisModuleCtx, Status, VALKEYMODULE_OK, ValkeyError,
    ValkeyModule_RegisterClusterMessageReceiver, ValkeyModule_SendClusterMessage,
    ValkeyModuleClusterMessageReceiver, ValkeyModuleCtx, ValkeyResult,
};

const FANOUT_REQUEST_MESSAGE: u8 = 0x01;
const FANOUT_RESPONSE_MESSAGE: u8 = 0x02;
const FANOUT_ERROR_MESSAGE: u8 = 0x03;
/// The requester has stopped waiting for a request's answer. A node without a receiver for this
/// type drops the message, and its share then ends at its time budget instead.
const FANOUT_CANCEL_MESSAGE: u8 = 0x04;

/// Default buffer size for serializing fanout request messages (header + small protobuf payload).
const FANOUT_RPC_BUFFER_SIZE: usize = 512;
/// Default buffer size for serializing fanout response messages (header + response payload).
const FANOUT_RPC_RESPONSE_BUFFER_SIZE: usize = 1024;

struct InFlightRequest {
    id: u64,
    /// The fanout operation's name, for `TS._DEBUG INFLIGHT`.
    command: &'static str,
    started: Instant,
    /// How many remote targets the request was sent to.
    remote_targets: usize,
    targets: Arc<HashSet<NodeInfo>>,
    response_handler: FanoutResponseCallback,
    outstanding: AtomicU64,
    timer_id: u64,
    timed_out: AtomicBool,
    /// Remote targets that have answered (a response, an error, or a send failure). A target
    /// answers once: anything more from it, or anything from a node that is not a remote
    /// target, is dropped rather than counted.
    responded: Mutex<HashSet<NodeId>>,
}

impl InFlightRequest {
    fn rpc_done(&self) -> Result<u64, u64> {
        // Decrement outstanding only when it's greater than 0 to avoid underflow.
        self.outstanding
            .try_update(Ordering::AcqRel, Ordering::Acquire, |curr| {
                if curr > 0 { Some(curr - 1) } else { None }
            })
    }

    /// Stop the pending timeout timer. Only for a timer that has *not* fired:
    /// the firing timer is consumed by `on_request_timeout` itself, and stopping
    /// it from inside its own callback double-frees its data (see there).
    ///
    /// `stop_timer::<u64>` reclaims the callback data with `u64`'s layout. That
    /// is exact here because the timer was created with a fn item (a ZST) as
    /// the callback, so the wrapper's `CallbackData` is one `u64`; a closure
    /// with captures would make this free with the wrong layout.
    fn cancel_timer(&self, ctx: &Context) {
        let _ = ctx.stop_timer::<u64>(self.timer_id);
    }

    fn get_target_node_opt(&self, sender_id: *const c_char) -> Option<&NodeInfo> {
        let sender = NodeId::from_raw(sender_id);
        self.targets.get(&sender)
    }

    /// Delivers one remote target's answer to the fan-out. Returns whether it was accepted, i.e.
    /// whether it counts toward completing the request.
    ///
    /// Only the first answer from each remote target counts. A message from a node that is not
    /// a remote target, or a second one from a target that already answered, used to be
    /// delivered too — the former as a `Custom` error — and so counted as a shard's reply in both
    /// this request's and the fan-out's bookkeeping, completing the fan-out while real shards
    /// were still outstanding.
    fn handle_response(&self, resp: FanoutResult<&[u8]>, sender_id: *const c_char) -> bool {
        let sender = NodeId::from_raw(sender_id);
        let Some(target_node) = self
            .get_target_node_opt(sender_id)
            .filter(|node| !node.is_local())
        else {
            metrics::FANOUT_IGNORED_UNKNOWN_SENDER.incr();
            log_debug(format!(
                "cluster rpc: ignoring response for request {} from unknown sender {sender}",
                self.id
            ));
            return false;
        };
        if !lock(&self.responded).insert(sender) {
            metrics::FANOUT_IGNORED_DUPLICATE.incr();
            log_debug(format!(
                "cluster rpc: ignoring duplicate response for request {} from {sender}",
                self.id
            ));
            return false;
        }

        // The handler runs the command's own `deserialize` and `on_response`. A
        // panic there must not unwind into the server: it fails this share instead.
        let handled = panic::catch_unwind(AssertUnwindSafe(|| {
            (self.response_handler)(resp, target_node)
        }));
        if handled.is_err() {
            log_error(format!(
                "cluster rpc: processing the answer to {} request {} from {sender} panicked",
                self.command, self.id
            ));
            let err = FanoutError::custom("failed to process the node's answer");
            (self.response_handler)(Err(err), target_node);
        }
        true
    }

    /// Deliver the fanout deadline to the response handler.
    ///
    /// Bypasses the sender lookup in [`Self::handle_response`]: the timeout has no
    /// sender, and attributing it to the local node turned it into an
    /// "unknown sender" `Custom` error whenever the local node was not itself a
    /// target (random/replica routing, a READONLY replica coordinator, a HASHTAG
    /// owned by another shard) — which counted as one shard error instead of
    /// ending the fanout, so the client got a generic error or waited out the
    /// blocked-client timeout. A timeout ends the whole fanout regardless of the
    /// node it is reported against.
    fn deliver_timeout(&self) {
        (self.response_handler)(Err(FanoutError::timeout()), &placeholder_node());
    }
}

/// Stand-in target for a callback that has no real sender (an unknown sender, or
/// the fanout deadline).
fn placeholder_node() -> NodeInfo {
    NodeInfo {
        id: Default::default(),
        shard_id: Default::default(),
        socket_address: SocketAddress {
            port: 0,
            primary_endpoint: IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
        },
        role: NodeRole::Primary,
        location: Default::default(),
        healthy: true,
    }
}

type InFlightRequestMap = HashMap<u64, InFlightRequest, BuildNoHashHasher<u64>>;

static INFLIGHT_REQUESTS: LazyLock<InFlightRequestMap> = LazyLock::new(InFlightRequestMap::default);

/// How many fanout RPCs have remote shares outstanding (`fanout_inflight`).
pub(crate) fn inflight_request_count() -> usize {
    INFLIGHT_REQUESTS.pin().len()
}

/// One in-flight fanout RPC, as `TS._DEBUG INFLIGHT` reports it.
pub(crate) struct InflightSummary {
    pub id: u64,
    pub command: &'static str,
    pub age: Duration,
    pub remote_targets: usize,
    pub outstanding: u64,
}

/// The fanout RPCs with remote shares outstanding, oldest first. A fanout whose only target is
/// this node has no RPC and does not appear; nor does one whose timer has fired, since it is
/// removed then.
pub(crate) fn inflight_requests() -> Vec<InflightSummary> {
    let mut requests: Vec<_> = INFLIGHT_REQUESTS
        .pin()
        .values()
        .map(|request| InflightSummary {
            id: request.id,
            command: request.command,
            age: request.started.elapsed(),
            remote_targets: request.remote_targets,
            outstanding: request.outstanding.load(Ordering::Acquire),
        })
        .collect();
    requests.sort_by_key(|request| std::cmp::Reverse(request.age));
    requests
}

/// Per-node request id counter, seeded once on first use (see [`initial_request_id`]).
static REQUEST_ID: LazyLock<AtomicU64> = LazyLock::new(|| AtomicU64::new(initial_request_id()));

/// The first request id this process hands out.
///
/// Ids only need to be unique per node, but a node that restarts must not reuse
/// the ids of requests its previous incarnation still had in flight, or a
/// late response from a peer could be matched to the wrong request. Seeding
/// from a randomly keyed hash of the node id makes each incarnation start at a
/// different, unpredictable point in the id space.
fn initial_request_id() -> u64 {
    RandomState::new().hash_one(CURRENT_NODE_ID.as_bytes())
}

/// Generate a unique request ID
///
/// This id only needs to be unique per node, so a simple atomic counter is enough.
///
/// The node that initiates a request is always the one waiting for responses, so there's no ambiguity
/// about which node "owns" a particular request ID.
///
/// There's no need for global uniqueness because:
///
/// - Each node only looks up requests in its own INFLIGHT_REQUESTS map
/// - Request IDs never need to be coordinated across nodes
/// - Two different nodes can safely use the same ID simultaneously for different requests
///
fn generate_id() -> u64 {
    next_id(&REQUEST_ID)
}

/// The counter's next value other than 0, which a seed or a wrap can land on. An error reply
/// in db 0 carries nothing else in its header, and an empty header does not decode, so the
/// requester would never see the error.
fn next_id(counter: &AtomicU64) -> u64 {
    loop {
        let id = counter.fetch_add(1, Ordering::Relaxed);
        if id != 0 {
            return id;
        }
    }
}

/// The timeout timer's callback. Runs on the main thread inside
/// `moduleTimerHandler`, for a timer that is in the middle of firing.
///
/// It must not stop that timer. The module-API wrapper has already reclaimed
/// and freed the timer's callback data before calling in here, and on the
/// server side the timer is still registered while its callback runs — so
/// `stop_timer` would succeed, hand back the same freed pointer, and free it
/// again (`free_tiny_botch` → abort, taking the coordinator and its quorum
/// with it; observed on the first fanout timeout of a large cluster range
/// read). The server removes and frees the firing timer itself once this
/// returns.
fn on_request_timeout(ctx: &Context, id: u64) {
    guard_callback("fanout request timer", || request_timed_out(ctx, id));
}

fn request_timed_out(ctx: &Context, id: u64) {
    let map = INFLIGHT_REQUESTS.pin();
    if let Some(request) = map.get(&id) {
        // Timeout can race with responses; only one path should complete the request.
        if request.timed_out.swap(true, Ordering::AcqRel) {
            return;
        }

        metrics::FANOUT_RPC_TIMEOUTS.incr();
        request.deliver_timeout();
        send_cancels(ctx, request);

        map.remove(&id);
    }
}

/// Tells the targets yet to answer that nobody is waiting any more. A peer's time budget counts
/// from when it reads the request, so a peer whose main thread stalled would otherwise give it a
/// full budget after the requester has gone. This reaches it behind the request, on the same
/// link. A lost message only leaves that budget in place, so a failed send is not an error.
///
/// A write always runs to the end on the shards that received it, so it gets no cancel.
fn send_cancels(ctx: &Context, request: &InFlightRequest) {
    if !get_fanout_request_handler(request.command).is_some_and(|op| op.cancellable) {
        return;
    }
    let answered = lock(&request.responded);
    for node in request.targets.iter() {
        if node.is_local() || answered.contains(&node.id) {
            continue;
        }
        let sent = send_message_internal(
            ctx,
            FANOUT_CANCEL_MESSAGE,
            request.id,
            0,
            node.id.raw_ptr(),
            "",
            &[],
        );
        if sent == Status::Ok {
            metrics::FANOUT_CANCELS_SENT.incr();
        }
    }
}

fn dispatch_send_failure(ctx: &Context, request_id: u64, target_node_id: *const c_char) {
    metrics::FANOUT_SEND_FAILURES.incr();
    with_inflight_request(ctx, request_id, |request| {
        let err = FanoutError::custom("Failed to send fanout request to target node");
        request.handle_response(Err(err), target_node_id)
    });
}

fn finish_inflight_request(ctx: &Context, request: &InFlightRequest) {
    if let Ok(v) = request.rpc_done()
        && v == 1
    {
        // Every shard answered before the deadline: the timer is still pending
        // and this is the one place it is stopped.
        request.cancel_timer(ctx);
        let map = INFLIGHT_REQUESTS.pin();
        map.remove(&request.id);
    }
}

fn validate_cluster_exec(ctx: &Context) -> ValkeyResult<()> {
    if !is_clustered(ctx) {
        return Err(ValkeyError::Str("Cluster mode is not enabled"));
    }
    if is_multi_or_lua(ctx) {
        return Err(ValkeyError::Str("Cannot execute in MULTI or Lua context"));
    }
    Ok(())
}

pub fn get_cluster_command_timeout() -> Duration {
    let timeout = FANOUT_COMMAND_TIMEOUT.load(Ordering::Relaxed);
    Duration::from_millis(timeout)
}

pub fn invoke_rpc<Request: Serializable>(
    ctx: &Context,
    name: &'static str,
    req: Request,
    targets: Arc<HashSet<NodeInfo>>,
    cluster_fingerprint: u64,
    response_handler: FanoutResponseCallback,
    timeout: Duration,
) -> ValkeyResult<()> {
    let mut buf = get_pooled_buffer(FANOUT_RPC_BUFFER_SIZE);
    req.serialize(&mut buf);

    send_cluster_request(
        ctx,
        &buf,
        targets,
        name,
        cluster_fingerprint,
        response_handler,
        Some(timeout),
    )
}

pub(super) fn send_cluster_request(
    ctx: &Context,
    request_buf: &[u8],
    targets: Arc<HashSet<NodeInfo>>,
    handler: &'static str,
    cluster_fingerprint: u64,
    response_handler: FanoutResponseCallback,
    timeout: Option<Duration>,
) -> ValkeyResult<()> {
    validate_cluster_exec(ctx)?;

    let id = generate_id();
    let db = get_current_db(ctx);
    let user = caller_user_name(ctx);

    let timeout = timeout.unwrap_or_else(get_cluster_command_timeout);

    // The timeout travels as the request's time budget: a peer drops a request still
    // queued past it instead of running it for a requester that has given up.
    let mut buf = get_pooled_buffer(FANOUT_RPC_BUFFER_SIZE);
    serialize_request_message(
        &mut buf,
        id,
        db,
        handler,
        user,
        cluster_fingerprint,
        Some(timeout),
        request_buf,
    );

    let remote_targets: Vec<NodeInfo> = targets
        .iter()
        .filter(|node| !node.is_local())
        .copied()
        .collect();

    if remote_targets.is_empty() {
        return Err(ValkeyError::Str(NO_CLUSTER_NODES_AVAILABLE));
    }

    let node_count = remote_targets.len();
    let timer_id = ctx.create_timer(timeout, on_request_timeout, id);

    let request = InFlightRequest {
        id,
        command: handler,
        started: Instant::now(),
        remote_targets: node_count,
        response_handler,
        timer_id,
        outstanding: AtomicU64::new(node_count as u64),
        timed_out: AtomicBool::new(false),
        targets: targets.clone(),
        responded: Mutex::new(HashSet::default()),
    };

    {
        let map = INFLIGHT_REQUESTS.pin();
        map.insert(id, request);
    }

    for node in remote_targets {
        let target_id = node.id.raw_ptr();
        let status = send_cluster_message(ctx, target_id, FANOUT_REQUEST_MESSAGE, buf.as_slice());
        if status == Status::Err {
            let msg = format!(
                "Failed to send fanout request id {} (handler '{}', db {}) to node {} (target_id: {:p})",
                id,
                handler,
                db,
                node.address(),
                target_id
            );
            log_warning(msg);
            dispatch_send_failure(ctx, id, target_id);
        }
    }
    Ok(())
}

fn send_message_internal(
    ctx: &Context,
    msg_type: u8,
    request_id: u64,
    db: i32,
    sender_id: *const c_char,
    handler: &str,
    buf: &[u8],
) -> Status {
    let mut dest = get_pooled_buffer(FANOUT_RPC_RESPONSE_BUFFER_SIZE);
    // Responses and errors don't carry a meaningful cluster-map fingerprint or
    // ACL user; the requester matches them by request id and reacts to the error kind.
    serialize_request_message(&mut dest, request_id, db, handler, None, 0, None, buf);
    send_cluster_message(ctx, sender_id, msg_type, dest.as_slice())
}

fn send_response_message(
    ctx: &Context,
    request_id: u64,
    db: i32,
    sender_id: *const c_char,
    handler: &str,
    buf: &[u8],
) -> Status {
    send_message_internal(
        ctx,
        FANOUT_RESPONSE_MESSAGE,
        request_id,
        db,
        sender_id,
        handler,
        buf,
    )
}

fn send_error_response(
    ctx: &Context,
    request_id: u64,
    db: i32,
    target_node: *const c_char,
    error: FanoutError,
) -> Status {
    let mut buf = get_pooled_buffer(FANOUT_RPC_BUFFER_SIZE);
    error.serialize(&mut buf);
    send_message_internal(
        ctx,
        FANOUT_ERROR_MESSAGE,
        request_id,
        db,
        target_node,
        "",
        &buf,
    )
}

fn parse_fanout_message<'a>(
    sender_id: *const c_char,
    payload: *const c_uchar,
    len: u32,
) -> Option<FanoutMessage<'a>> {
    // SAFETY: `payload` is expected to be a non-null, valid pointer to `len` bytes of
    // initialized memory provided by Valkey's C API for the duration of this callback,
    // so creating a shared slice from it with `from_raw_parts` is sound.
    let buffer = unsafe { std::slice::from_raw_parts(payload, len as usize) };
    match FanoutMessage::new(buffer) {
        Ok(msg) => Some(msg),
        Err(err) => {
            let node = NodeId::from_raw(sender_id);
            log_warning(format!(
                "Failed to parse fanout message from node {node}: {err}"
            ));
            None
        }
    }
}

/// Allocates the specified database if it is not already the current one.
fn alloc_db_if_needed(ctx: &Context, db: i32) {
    if db != get_current_db(ctx) {
        set_current_db(ctx, db);
    }
}

/// Checks the requester's cluster-map fingerprint against our own view of the
/// cluster topology.
///
/// A fingerprint of `0` means the sender had no map when it built the request,
/// so the check is skipped. Otherwise, we compare against a fresh local map: if
/// they disagree and that map was not just rebuilt, it may merely be stale, so
/// we force a single refresh and re-compare before declaring a mismatch. Building the map issues
/// `CLUSTER NODES` (not `CLUSTER SLOTS`), whose reply does not depend on a
/// client, so this is safe on the worker thread that runs it; the GIL is taken
/// only for that call, and the reply is parsed with it released.
///
/// Returns `true` when the topologies agree (request accepted).
fn cluster_fingerprint_matches(ctx: &DetachedContext, expected: u64) -> bool {
    if expected == 0 {
        return true;
    }

    let (local, refreshed) = current_or_refreshed_cluster_map(ctx);
    if local.cluster_slots_fingerprint() == expected {
        return true;
    }
    if refreshed {
        // Already built from a `CLUSTER NODES` reply taken just now.
        return false;
    }

    // Our map might just be stale; force one refresh and re-check.
    clustermap_metrics::FORCED_REFRESHES.incr();
    refresh_cluster_map(ctx)
        .into_map()
        .unwrap_or_else(|| get_cluster_map().clone())
        .cluster_slots_fingerprint()
        == expected
}

/// When a peer's request stops being worth running: `budget` after it `received` it, for an
/// operation that may be abandoned (`FanoutCommand::cancellable`). `None` never expires — no
/// budget, a write, or a budget past what `Instant` can represent.
fn request_deadline(
    operation: RegisteredOperation,
    budget: Option<Duration>,
    received: Instant,
) -> Option<Instant> {
    if !operation.cancellable {
        return None;
    }
    received.checked_add(budget?)
}

/// Processes a valid request by executing the command and sending back the response.
///
/// Runs on a worker thread. The GIL is taken twice here — to validate the
/// request's database and to send the reply — and otherwise only by the
/// cluster-map check (for `CLUSTER NODES`) and the handler itself, for as long
/// as its keyspace/index work needs (see `FanoutCommand::get_local_response`).
/// Request decoding and response encoding to happen with the GIL released.
///
/// `received` is when the request arrived, before it waited in the lane's queue: its
/// time budget counts from then.
fn process_request_message(
    header: FanoutMessageHeader,
    operation: RegisteredOperation,
    request_buf: &[u8],
    sender_id: NodeId,
    received: Instant,
    registration: Option<PeerRequest>,
) {
    let request_id = header.request_id;

    // The handler re-selects `db` and resolves `user` on every GIL acquisition
    // (`FanoutContext::lock`); the context tells it which ones, and when to give up.
    let fanout_ctx = FanoutContext::new(header.user, header.db);
    if let Some(deadline) = request_deadline(operation, header.time_budget, received) {
        fanout_ctx.set_deadline(deadline);
    }
    if let Some(registration) = &registration {
        fanout_ctx.set_cancel_flag(registration.cancelled.clone());
    }
    if fanout_ctx.expired() {
        // Cancelled, or past a deadline which, counted from arrival, is never earlier than the
        // requester's. Either way the requester has already given up: any answer, an error
        // included, would only be logged there as a reply to an unknown request.
        metrics::FANOUT_DROPPED_EXPIRED.incr();
        return;
    }

    // Reject the request if the cluster topology changed between the requester
    // generating it and us receiving it. This runs on the worker thread (not the
    // cluster-message callback), so the potential `CLUSTER NODES` refresh stays
    // off the main thread, and outside the lock below so the refresh holds the
    // GIL only for the call itself. The aggregate result would otherwise be
    // built from inconsistent per-node views.
    if !cluster_fingerprint_matches(&MODULE_CONTEXT, header.cluster_fingerprint) {
        metrics::FANOUT_REJECTED_CLUSTER_MAP_MISMATCH.incr();
        let ctx = MODULE_CONTEXT.lock_gil();
        log_debug(format!(
            "cluster rpc: rejecting request {request_id} from node {sender_id}: cluster-map fingerprint mismatch"
        ));
        send_error_response(
            &ctx,
            request_id,
            header.db,
            sender_id.raw_ptr(),
            FanoutError::cluster_map_mismatch(),
        );
        return;
    }

    let mut dest = get_pooled_buffer(FANOUT_RPC_RESPONSE_BUFFER_SIZE);

    match (operation.run)(&fanout_ctx, request_buf, &mut dest) {
        Ok(()) => {
            // Cancelled, or finished after the deadline: either way nobody is
            // waiting, so don't take the GIL to send what would be ignored.
            if fanout_ctx.expired() {
                metrics::FANOUT_ANSWERS_DISCARDED.incr();
                return;
            }
            metrics::FANOUT_SERVED_OK.incr();
            let ctx = MODULE_CONTEXT.lock_gil();
            if send_response_message(
                &ctx,
                request_id,
                header.db,
                sender_id.raw_ptr(),
                &header.handler,
                &dest,
            ) == Status::Err
            {
                metrics::FANOUT_REPLY_SEND_FAILURES.incr();
                log_warning(format!(
                    "Failed to send response message to node {sender_id}"
                ));
            }
        }
        // A handler times out only by abandoning its read at the deadline, which the requester
        // has already passed, so as above there is no one to answer. Its own timer ends the
        // fan-out: a reply could not beat that to the client, since the main thread runs both
        // and unblocks clients only between commands.
        Err(e) if e.kind == ErrorKind::Timeout => {
            metrics::FANOUT_ABANDONED_EXPIRED.incr();
        }
        Err(e) => {
            metrics::FANOUT_SERVED_ERRORS.incr();
            let msg = format!(
                "cluster rpc: {} request {request_id} from node {sender_id} failed: {e}",
                header.handler
            );
            if e.kind.is_client_visible() {
                log_debug(msg);
            } else {
                log_warning(msg);
            }
            let ctx = MODULE_CONTEXT.lock_gil();
            send_error_response(&ctx, request_id, header.db, sender_id.raw_ptr(), e);
        }
    }
}

/// Sends a message to a specific cluster node.
///
/// # Arguments
///
/// * `target_node_id` - The 40-byte hex ID of the target node.
/// * `msg_type` - The type of the message to send.
/// * `message_body` - The raw byte payload of the message.
///
/// # Returns
///
/// `Status::Ok` on success, `Status::Err` with a message on failure.
pub fn send_cluster_message(
    ctx: &Context,
    target_node_id: *const c_char,
    msg_type: u8,
    message_body: &[u8],
) -> Status {
    let sent = unsafe {
        ValkeyModule_SendClusterMessage.expect("ValkeyModule_SendClusterMessage is not available")(
            ctx.ctx as *mut ValkeyModuleCtx,
            target_node_id,
            msg_type,
            message_body.as_ptr().cast::<c_char>(),
            message_body.len() as u32,
        ) == VALKEYMODULE_OK as c_int
    };
    if !sent {
        return Status::Err;
    }
    if let Some(wire) = wire_counters(msg_type) {
        wire.record(message_body.len());
    }
    Status::Ok
}

/// Where a sent message of `msg_type` is counted (`TS._DEBUG METRICS fanout`).
fn wire_counters(msg_type: u8) -> Option<&'static WireCounters> {
    match msg_type {
        FANOUT_REQUEST_MESSAGE => Some(&metrics::FANOUT_REQUESTS_SENT),
        FANOUT_RESPONSE_MESSAGE => Some(&metrics::FANOUT_RESPONSES_SENT),
        FANOUT_ERROR_MESSAGE => Some(&metrics::FANOUT_ERROR_RESPONSES_SENT),
        _ => None,
    }
}

/// Handles incoming requests from requester nodes in the cluster.
extern "C" fn on_request_received(
    ctx: *mut ValkeyModuleCtx,
    sender_id: *const c_char,
    _type: u8,
    payload: *const c_uchar,
    len: u32,
) {
    guard_callback("fanout request intake", || {
        request_received(ctx, sender_id, payload, len)
    });
}

fn request_received(
    ctx: *mut ValkeyModuleCtx,
    sender_id: *const c_char,
    payload: *const c_uchar,
    len: u32,
) {
    let ctx = Context::new(ctx as *mut RedisModuleCtx);
    let Some(FanoutMessage {
        header,
        buf: request_buf,
    }) = parse_fanout_message(sender_id, payload, len)
    else {
        metrics::FANOUT_REJECTED_PARSE.incr();
        return;
    };

    // Feature gate: a newer peer may demand envelope features via the header;
    // reject explicitly (addressable, fast) rather than mis-process the
    // request. See docs/topics/fanout-compatibility-handshake.md.
    if has_unsupported_features(header.required_features) {
        metrics::FANOUT_REJECTED_UNSUPPORTED_FEATURES.incr();
        let e = FanoutError::unsupported_features();
        send_error_response(&ctx, header.request_id, header.db, sender_id, e);
        let msg = format!(
            "Rejecting fanout request {} from node {}: unsupported required_features {:#06x}",
            header.request_id,
            NodeId::from_raw(sender_id),
            header.required_features
        );
        log_warning(msg);
        return;
    }

    let received = Instant::now();

    let Some(operation) = get_fanout_request_handler(&header.handler) else {
        metrics::FANOUT_REJECTED_NO_HANDLER.incr();
        let e = FanoutError::invalid_message();
        send_error_response(&ctx, header.request_id, header.db, sender_id, e);
        let msg = format!(
            "No handler registered for fanout operation '{}'",
            header.handler
        );
        log_warning(msg);
        return;
    };

    let sender = NodeId::from_raw(sender_id);

    let mut buf = get_pooled_buffer(len as usize);
    buf.extend_from_slice(request_buf);

    alloc_db_if_needed(&ctx, header.db);

    let (request_id, db) = (header.request_id, header.db);

    // A blocking thread: the handler takes the GIL (R1 in `common::threads`).
    let registration = operation
        .cancellable
        .then(|| PeerRequest::register(sender, request_id));
    let queued = PEER_REQUEST_EXECUTOR.try_spawn(move || {
        process_request_message(header, operation, &buf, sender, received, registration);
    });
    if queued.is_err() {
        metrics::FANOUT_REJECTED_BUSY.incr();
        // Answer now rather than leave the requester waiting out its timeout.
        send_error_response(&ctx, request_id, db, sender_id, FanoutError::busy());
        log_debug(format!(
            "Rejecting fanout request {request_id} from node {sender}: workers busy"
        ));
    }
}

/// Peer requests being served that their requester may cancel, by requester and request id.
/// Request ids are only unique per requester.
static PEER_REQUESTS: LazyLock<HashMap<(NodeId, u64), Arc<AtomicBool>>> =
    LazyLock::new(HashMap::new);

/// A peer request's entry in [`PEER_REQUESTS`], for as long as the request is queued or running.
/// Dropping it removes the entry, so a request that never ran (the queue was full) leaves none.
struct PeerRequest {
    key: (NodeId, u64),
    cancelled: Arc<AtomicBool>,
}

impl PeerRequest {
    fn register(sender: NodeId, request_id: u64) -> Self {
        let key = (sender, request_id);
        let cancelled = Arc::new(AtomicBool::new(false));
        PEER_REQUESTS.pin().insert(key, cancelled.clone());
        Self { key, cancelled }
    }
}

impl Drop for PeerRequest {
    fn drop(&mut self) {
        PEER_REQUESTS.pin().remove(&self.key);
    }
}

/// Marks a request as no longer wanted. A request that has finished has no entry, which is fine:
/// its answer has gone already.
fn cancel_peer_request(sender: NodeId, request_id: u64) {
    if let Some(cancelled) = PEER_REQUESTS.pin().get(&(sender, request_id)) {
        cancelled.store(true, Ordering::Relaxed);
    }
}

extern "C" fn on_cancel_received(
    _ctx: *mut ValkeyModuleCtx,
    sender_id: *const c_char,
    _type: u8,
    payload: *const c_uchar,
    len: u32,
) {
    guard_callback("fanout cancel intake", || {
        let Some(message) = parse_fanout_message(sender_id, payload, len) else {
            return;
        };
        metrics::FANOUT_CANCELS_RECEIVED.incr();
        cancel_peer_request(NodeId::from_raw(sender_id), message.header.request_id);
    });
}

/// Runs `f` against an in-flight request. `f` reports whether it delivered an answer that
/// counts (see [`InFlightRequest::handle_response`]); only then is the request's outstanding
/// count decremented.
fn with_inflight_request<F>(ctx: &Context, request_id: u64, f: F)
where
    F: FnOnce(&InFlightRequest) -> bool,
{
    let map = INFLIGHT_REQUESTS.pin();
    let Some(request) = map.get(&request_id) else {
        // Expected after a timeout: answers still arrive for a request that is gone.
        metrics::FANOUT_IGNORED_UNKNOWN_REQUEST.incr();
        log_debug(format!(
            "Failed to find inflight request for id {request_id}. Possible timeout.",
        ));
        return;
    };

    if f(request) {
        finish_inflight_request(ctx, request);
    }
}

/// Handles responses from other nodes in the cluster. The receiver is the original sender of
/// the request.
extern "C" fn on_response_received(
    ctx: *mut ValkeyModuleCtx,
    sender_id: *const c_char,
    _type: u8,
    payload: *const c_uchar,
    len: u32,
) {
    guard_callback("fanout response intake", || {
        response_received(ctx, sender_id, payload, len)
    });
}

fn response_received(
    ctx: *mut ValkeyModuleCtx,
    sender_id: *const c_char,
    payload: *const c_uchar,
    len: u32,
) {
    let ctx = Context::new(ctx as *mut RedisModuleCtx);

    let Some(message) = parse_fanout_message(sender_id, payload, len) else {
        return;
    };

    with_inflight_request(&ctx, message.header.request_id, |request| {
        // Feature gate: a newer peer's response may demand envelope features
        // (e.g., a payload encoding) we cannot decode; fail this node's slice
        // of the request instead of misinterpreting the payload.
        if has_unsupported_features(message.header.required_features) {
            let err = FanoutError::unsupported_features();
            return request.handle_response(Err(err), sender_id);
        }
        request.handle_response(Ok(message.buf), sender_id)
    });
}

extern "C" fn on_error_received(
    ctx: *mut ValkeyModuleCtx,
    sender_id: *const c_char,
    _type: u8,
    payload: *const c_uchar,
    len: u32,
) {
    guard_callback("fanout error intake", || {
        error_received(ctx, sender_id, payload, len)
    });
}

fn error_received(
    ctx: *mut ValkeyModuleCtx,
    sender_id: *const c_char,
    payload: *const c_uchar,
    len: u32,
) {
    let ctx = Context::new(ctx as *mut RedisModuleCtx);

    let Some(message) = parse_fanout_message(sender_id, payload, len) else {
        return;
    };

    with_inflight_request(&ctx, message.header.request_id, |request| {
        // Feature gate: mirror the response path — an error payload we cannot
        // decode per the demanded features still fails this node's slice.
        if has_unsupported_features(message.header.required_features) {
            let err = FanoutError::unsupported_features();
            return request.handle_response(Err(err), sender_id);
        }

        match FanoutError::deserialize(message.buf) {
            Ok(error) => {
                // A peer rejected us because its view of the cluster topology
                // differs from ours. Invalidate our local map so the next fanout
                // rebuilds it before targeting nodes.
                if error.kind == ErrorKind::ClusterMapMismatch {
                    mark_cluster_map_stale();
                }
                request.handle_response(Err(error), sender_id)
            }
            Err(_) => {
                metrics::FANOUT_ERROR_DECODE_FAILURES.incr();
                log_warning(format!(
                    "Failed to deserialize error response from node {}",
                    NodeId::from_raw(sender_id)
                ));
                let err = FanoutError::invalid_message();
                request.handle_response(Err(err), sender_id)
            }
        }
    });
}

/// Runs the body of a callback the server calls from C (a cluster-message
/// receiver or the request timer). Unwinding out of it would abort the server,
/// so a panic is logged and the callback returns instead.
fn guard_callback(what: &str, body: impl FnOnce()) {
    if panic::catch_unwind(AssertUnwindSafe(body)).is_err() {
        log_error(format!("cluster rpc: {what} panicked"));
    }
}

/// Registers a callback function to handle incoming cluster messages.
/// This should typically be called during module initialization (e.g., ValkeyModule_OnLoad).
///
/// ## Arguments
///
/// * `receiver_func` - The function pointer matching the expected signature
///   `ValkeyModuleClusterMessageReceiverFunc`.
///
/// ## Safety
///
/// The provided `receiver_func` must be valid for the lifetime of the module
/// and correctly handle the arguments passed by Valkey.
fn register_message_receiver(
    ctx: &Context,
    type_: u8,
    receiver_func: ValkeyModuleClusterMessageReceiver,
) {
    unsafe {
        ValkeyModule_RegisterClusterMessageReceiver
            .expect("ValkeyModule_RegisterClusterMessageReceiver is not available")(
            ctx.ctx as *mut ValkeyModuleCtx,
            type_,
            receiver_func,
        );
    }
}

/// Registers the cluster message handlers for request, response, and error messages.
/// This function should be called during module initialization
pub fn register_cluster_message_handlers(ctx: &Context) {
    // Register the cluster message handlers
    register_message_receiver(ctx, FANOUT_REQUEST_MESSAGE, Some(on_request_received));
    register_message_receiver(ctx, FANOUT_RESPONSE_MESSAGE, Some(on_response_received));
    register_message_receiver(ctx, FANOUT_ERROR_MESSAGE, Some(on_error_received));
    register_message_receiver(ctx, FANOUT_CANCEL_MESSAGE, Some(on_cancel_received));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error_consts;

    fn never_called(_: &FanoutContext, _: &[u8], _: &mut Vec<u8>) -> FanoutResult {
        panic!("an expired request must not reach its handler");
    }

    fn operation(cancellable: bool) -> RegisteredOperation {
        RegisteredOperation {
            run: never_called,
            cancellable,
        }
    }

    fn header(time_budget: Option<Duration>) -> FanoutMessageHeader {
        FanoutMessageHeader {
            request_id: 1,
            db: 0,
            handler: "mrange".to_string(),
            user: None,
            cluster_fingerprint: 0,
            required_features: 0,
            time_budget,
        }
    }

    /// The deadline counts from arrival, only for an operation that may be abandoned, and
    /// a budget no `Instant` can hold means none rather than a panic on the worker.
    #[test]
    fn request_deadline_counts_from_arrival_for_cancellable_operations() {
        let received = Instant::now();
        let budget = Duration::from_millis(250);

        assert_eq!(
            request_deadline(operation(true), Some(budget), received),
            Some(received + budget)
        );
        assert_eq!(request_deadline(operation(true), None, received), None);
        assert_eq!(
            request_deadline(operation(false), Some(budget), received),
            None,
            "a write never expires"
        );
        assert_eq!(
            request_deadline(operation(true), Some(Duration::MAX), received),
            None
        );
    }

    /// A cancellable request that waited out its budget in the queue is dropped before any
    /// GIL is taken or reply sent: its handler never runs.
    #[test]
    fn an_expired_request_is_dropped_unrun() {
        let before = metrics::FANOUT_DROPPED_EXPIRED.load(Ordering::Relaxed);

        let received = Instant::now() - Duration::from_millis(50);
        process_request_message(
            header(Some(Duration::from_millis(10))),
            operation(true),
            &[],
            NodeId::default(),
            received,
            None,
        );

        assert!(metrics::FANOUT_DROPPED_EXPIRED.load(Ordering::Relaxed) > before);
    }

    /// 0 is skipped whether the counter was seeded with it or wrapped onto it.
    #[test]
    fn request_ids_skip_zero() {
        let seeded_with_zero = AtomicU64::new(0);
        assert_eq!(next_id(&seeded_with_zero), 1);
        assert_eq!(next_id(&seeded_with_zero), 2);

        let about_to_wrap = AtomicU64::new(u64::MAX);
        assert_eq!(next_id(&about_to_wrap), u64::MAX);
        assert_eq!(next_id(&about_to_wrap), 1);
    }

    /// An error reply to the first id of a zero-seeded counter, in db 0, has no other field
    /// set, so its id is all that keeps the header from being empty, which the decoder rejects.
    #[test]
    fn an_error_reply_to_a_generated_id_decodes() {
        let id = next_id(&AtomicU64::new(0));
        let mut buf = Vec::new();
        serialize_request_message(&mut buf, id, 0, "", None, 0, None, &[]);

        let message = FanoutMessage::new(&buf).unwrap();
        assert_eq!(message.header.request_id, id);
    }

    /// `from_raw` reads a full node id, however short a string it is handed.
    fn node_id(hex_digit: u8) -> NodeId {
        NodeId::from_raw([hex_digit; 41].as_ptr().cast())
    }

    #[test]
    fn a_panicking_answer_handler_fails_its_share() {
        let sender = node_id(b'1');
        let target = NodeInfo {
            id: sender,
            shard_id: sender,
            location: crate::fanout::cluster_map::NodeLocation::Remote,
            ..placeholder_node()
        };
        let errors = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&errors);
        let request = InFlightRequest {
            id: 1,
            command: "test",
            started: Instant::now(),
            remote_targets: 1,
            targets: Arc::new(HashSet::from_iter([target])),
            response_handler: Box::new(move |res, _| match res {
                Ok(_) => panic!("command code panicked"),
                Err(e) => lock(&seen).push(e.to_string()),
            }),
            outstanding: AtomicU64::new(1),
            timer_id: 0,
            timed_out: AtomicBool::new(false),
            responded: Mutex::new(HashSet::default()),
        };

        // Counted as the node's answer, delivered as an error.
        assert!(request.handle_response(Ok(&[]), sender.raw_ptr()));
        assert_eq!(lock(&errors).len(), 1);
        // A second answer from the same node is still a duplicate.
        assert!(!request.handle_response(Ok(&[]), sender.raw_ptr()));
    }

    #[test]
    fn a_panicking_callback_body_returns() {
        guard_callback("test callback", || panic!("boom"));
    }

    /// A cancel marks the request it names, from the requester that sent it, and nothing else:
    /// request ids are only unique per requester.
    #[test]
    fn a_cancel_marks_only_the_request_it_names() {
        let sender = node_id(b'a');
        let other = node_id(b'b');
        let request = PeerRequest::register(sender, 7);
        let same_id_elsewhere = PeerRequest::register(other, 7);
        let another_id = PeerRequest::register(sender, 8);

        cancel_peer_request(sender, 7);

        assert!(request.cancelled.load(Ordering::Relaxed));
        assert!(!same_id_elsewhere.cancelled.load(Ordering::Relaxed));
        assert!(!another_id.cancelled.load(Ordering::Relaxed));
    }

    /// A cancel can arrive after the answer has gone, and a finished request leaves no entry.
    #[test]
    fn a_finished_request_is_forgotten() {
        let sender = node_id(b'c');
        let request = PeerRequest::register(sender, 9);
        assert!(PEER_REQUESTS.pin().contains_key(&(sender, 9)));

        drop(request);

        assert!(!PEER_REQUESTS.pin().contains_key(&(sender, 9)));
        cancel_peer_request(sender, 9);
    }

    /// A request cancelled while it waited in the queue is dropped unrun, with its budget intact.
    #[test]
    fn a_cancelled_request_is_dropped_unrun() {
        let before = metrics::FANOUT_DROPPED_EXPIRED.load(Ordering::Relaxed);
        let sender = node_id(b'd');
        let registration = PeerRequest::register(sender, 11);
        cancel_peer_request(sender, 11);

        process_request_message(
            header(Some(Duration::from_secs(60))),
            operation(true),
            &[],
            sender,
            Instant::now(),
            Some(registration),
        );

        assert!(metrics::FANOUT_DROPPED_EXPIRED.load(Ordering::Relaxed) > before);
        assert!(!PEER_REQUESTS.pin().contains_key(&(sender, 11)));
    }

    fn cancelled_while_running(_: &FanoutContext, _: &[u8], _: &mut Vec<u8>) -> FanoutResult {
        cancel_peer_request(node_id(b'e'), 1);
        Ok(())
    }

    /// A read that finishes after its cancel has an answer nobody wants: it is not sent, which
    /// would need the GIL here, and not counted as served.
    #[test]
    fn an_answer_finished_after_a_cancel_is_discarded() {
        let discarded = metrics::FANOUT_ANSWERS_DISCARDED.load(Ordering::Relaxed);
        let served = metrics::FANOUT_SERVED_OK.load(Ordering::Relaxed);
        let sender = node_id(b'e');

        process_request_message(
            header(Some(Duration::from_secs(60))),
            RegisteredOperation {
                run: cancelled_while_running,
                cancellable: true,
            },
            &[],
            sender,
            Instant::now(),
            Some(PeerRequest::register(sender, 1)),
        );

        assert_eq!(
            metrics::FANOUT_ANSWERS_DISCARDED.load(Ordering::Relaxed),
            discarded + 1
        );
        assert_eq!(metrics::FANOUT_SERVED_OK.load(Ordering::Relaxed), served);
    }

    fn gives_up_mid_read(_: &FanoutContext, _: &[u8], _: &mut Vec<u8>) -> FanoutResult {
        Err(ValkeyError::Str(error_consts::FANOUT_REQUESTER_GAVE_UP).into())
    }

    /// A read abandoned at its deadline is not a handler failure, and like a request dropped
    /// from the queue it gets no reply: nothing here takes the GIL to send one.
    #[test]
    fn a_read_abandoned_at_its_deadline_is_counted_and_not_answered() {
        let abandoned = metrics::FANOUT_ABANDONED_EXPIRED.load(Ordering::Relaxed);
        let failed = metrics::FANOUT_SERVED_ERRORS.load(Ordering::Relaxed);

        process_request_message(
            header(Some(Duration::from_secs(60))),
            RegisteredOperation {
                run: gives_up_mid_read,
                cancellable: true,
            },
            &[],
            NodeId::default(),
            Instant::now(),
            None,
        );

        assert!(metrics::FANOUT_ABANDONED_EXPIRED.load(Ordering::Relaxed) > abandoned);
        assert_eq!(
            metrics::FANOUT_SERVED_ERRORS.load(Ordering::Relaxed),
            failed
        );
    }
}
