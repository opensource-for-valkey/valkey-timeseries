use super::fanout_error::{ErrorKind, FanoutError, NO_CLUSTER_NODES_AVAILABLE};
use super::fanout_message::{
    FanoutMessage, FanoutMessageHeader, has_unsupported_features, serialize_request_message,
};
use super::utils::{is_clustered, is_multi_or_lua};
use crate::common::context::{get_current_db, set_current_db};
use crate::common::hash::BuildNoHashHasher;
use crate::common::pool::get_pooled_buffer;
use crate::common::threads::spawn_with_context;
use crate::config::FANOUT_COMMAND_TIMEOUT;
use crate::fanout::acl::get_fanout_user;
use crate::fanout::cluster_map::{CURRENT_NODE_ID, NodeId, NodeRole, SocketAddress};
use crate::fanout::fanout_command::FanoutResponseCallback;
use crate::fanout::registry::{RequestHandlerCallback, get_fanout_request_handler};
use crate::fanout::serialization::Serializable;
use crate::fanout::{
    FanoutResult, NodeInfo, get_cluster_map, get_or_refresh_cluster_map, mark_cluster_map_stale,
    refresh_cluster_map, with_fanout_user,
};
use ahash::HashSet;
use core::time::Duration;
use papaya::HashMap;
use std::hash::{BuildHasher, RandomState};
use std::net::{IpAddr, Ipv4Addr};
use std::os::raw::{c_char, c_int, c_uchar};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use valkey_module::{
    Context, RedisModuleCtx, Status, VALKEYMODULE_OK, ValkeyError,
    ValkeyModule_RegisterClusterMessageReceiver, ValkeyModule_SendClusterMessage,
    ValkeyModuleClusterMessageReceiver, ValkeyModuleCtx, ValkeyResult,
};

const FANOUT_REQUEST_MESSAGE: u8 = 0x01;
const FANOUT_RESPONSE_MESSAGE: u8 = 0x02;
const FANOUT_ERROR_MESSAGE: u8 = 0x03;

/// Default buffer size for serializing fanout request messages (header + small protobuf payload).
const FANOUT_RPC_BUFFER_SIZE: usize = 512;
/// Default buffer size for serializing fanout response messages (header + response payload).
const FANOUT_RPC_RESPONSE_BUFFER_SIZE: usize = 1024;

struct InFlightRequest {
    id: u64,
    targets: Arc<HashSet<NodeInfo>>,
    response_handler: FanoutResponseCallback,
    outstanding: AtomicU64,
    timer_id: u64,
    timed_out: AtomicBool,
}

impl InFlightRequest {
    fn rpc_done(&self) -> Result<u64, u64> {
        // Decrement outstanding only when it's greater than 0 to avoid underflow.
        self.outstanding
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |curr| {
                if curr > 0 { Some(curr - 1) } else { None }
            })
    }

    fn cancel_timer(&self, ctx: &Context) {
        let _ = ctx.stop_timer::<u64>(self.timer_id);
    }

    fn get_target_node_opt(&self, sender_id: *const c_char) -> Option<&NodeInfo> {
        let sender = NodeId::from_raw(sender_id);
        self.targets.get(&sender)
    }

    fn handle_response(&self, ctx: &Context, resp: FanoutResult<&[u8]>, sender_id: *const c_char) {
        let Some(target_node) = self.get_target_node_opt(sender_id) else {
            let sender = NodeId::from_raw(sender_id);
            let msg = format!(
                "cluster rpc: received response for request {} from unknown sender {}",
                self.id, sender
            );
            ctx.log_warning(&msg);

            let resp = Err(FanoutError::custom(msg));

            let node_info = NodeInfo {
                id: Default::default(),
                shard_id: Default::default(),
                socket_address: SocketAddress {
                    port: 0,
                    primary_endpoint: IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
                },
                role: NodeRole::Primary,
                location: Default::default(),
            };

            (self.response_handler)(resp, &node_info);
            return;
        };

        (self.response_handler)(resp, target_node);
    }
}

type InFlightRequestMap = HashMap<u64, InFlightRequest, BuildNoHashHasher<u64>>;

static INFLIGHT_REQUESTS: LazyLock<InFlightRequestMap> = LazyLock::new(InFlightRequestMap::default);

static REQUEST_ID: AtomicU64 = AtomicU64::new(0);

/// Generate a unique request ID
///
/// This id only needs to be unique per node, so a simple atomic counter is enough.
///
/// The node that initiates a request is always the one waiting for responses, so there's no ambiguity
/// about which node "owns" a particular request ID.
///
/// There's no need for global uniqueness because:
///
/// - Each node only looks up requests in its own `INFLIGHT_REQUESTS` map.
/// - Request IDs never need to be coordinated across nodes.
/// - Two different nodes can safely use the same ID simultaneously for different requests.
///
fn generate_id() -> u64 {
    loop {
        // Fast path: counter already initialized, just increment and return the previous value.
        let current = REQUEST_ID.load(Ordering::Acquire);
        if current != 0 {
            return REQUEST_ID.fetch_add(1, Ordering::AcqRel);
        }
        // Slow path: initialize the counter exactly once based on the current node ID.

        let curr_id = *CURRENT_NODE_ID;
        let hasher = RandomState::new();
        // Seed the first request ID from a hash of the node ID, while avoiding races between threads.
        let initial_id = hasher.hash_one(curr_id.as_bytes());
        // Set the counter to the next value after `initial_id` so future calls
        // get unique IDs strictly greater than the first one we return here.
        match REQUEST_ID.compare_exchange(
            0,
            initial_id.wrapping_add(1),
            Ordering::SeqCst,
            Ordering::Relaxed,
        ) {
            Ok(_) => {
                // We won the race to initialize; return the first ID.
                return initial_id;
            }
            Err(_) => {
                // Another thread initialized `REQUEST_ID` first; retry and take the fast path.
                continue;
            }
        }
    }
}

fn on_request_timeout(ctx: &Context, id: u64) {
    let map = INFLIGHT_REQUESTS.pin();
    if let Some(request) = map.get(&id) {
        // Timeout can race with responses; only one path should complete the request.
        if request.timed_out.swap(true, Ordering::AcqRel) {
            return;
        }

        request.cancel_timer(ctx);

        let local_node_id = CURRENT_NODE_ID.raw_ptr();
        request.handle_response(ctx, Err(FanoutError::timeout()), local_node_id);

        map.remove(&id);
    }
}

fn dispatch_send_failure(ctx: &Context, request_id: u64, target_node_id: *const c_char) {
    with_inflight_request(ctx, request_id, |ctx, request| {
        let err = FanoutError::custom("Failed to send fanout request to target node");
        request.handle_response(ctx, Err(err), target_node_id);
    });
}

fn finish_inflight_request(ctx: &Context, request: &InFlightRequest) {
    if let Ok(v) = request.rpc_done()
        && v == 1
    {
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
    name: &str,
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
    handler: &str,
    cluster_fingerprint: u64,
    response_handler: FanoutResponseCallback,
    timeout: Option<Duration>,
) -> ValkeyResult<()> {
    validate_cluster_exec(ctx)?;

    let id = generate_id();
    let db = get_current_db(ctx);
    let user = get_fanout_user(ctx);

    let mut buf = get_pooled_buffer(FANOUT_RPC_BUFFER_SIZE);
    serialize_request_message(
        &mut buf,
        id,
        db,
        handler,
        user.as_deref(),
        cluster_fingerprint,
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
    let timeout = timeout.unwrap_or_else(get_cluster_command_timeout);
    let timer_id = ctx.create_timer(timeout, on_request_timeout, id);

    let request = InFlightRequest {
        id,
        response_handler,
        timer_id,
        outstanding: AtomicU64::new(node_count as u64),
        timed_out: AtomicBool::new(false),
        targets: targets.clone(),
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
            ctx.log_warning(&msg);
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
    serialize_request_message(&mut dest, request_id, db, handler, None, 0, buf);
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

fn parse_fanout_message(
    ctx: &'_ Context,
    sender_id: *const c_char,
    payload: *const c_uchar,
    len: u32,
) -> Option<FanoutMessage<'_>> {
    // SAFETY: `payload` is expected to be a non-null, valid pointer to `len` bytes of
    // initialized memory provided by Valkey's C API for the duration of this callback,
    // so creating a shared slice from it with `from_raw_parts` is sound.
    let buffer = unsafe { std::slice::from_raw_parts(payload, len as usize) };
    match FanoutMessage::new(buffer) {
        Ok(msg) => Some(msg),
        Err(err) => {
            let node = NodeId::from_raw(sender_id);
            let msg = format!("Failed to parse fanout message from node {node}: {err}");
            ctx.log_warning(&msg);
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
/// so the check is skipped. Otherwise we compare against a fresh local map: if
/// they disagree, our map may merely be stale, so we force a single refresh and
/// re-compare before declaring a mismatch. Building the map issues
/// `CLUSTER NODES` (not `CLUSTER SLOTS`), whose reply does not depend on a
/// client, so this is safe on the worker thread that runs it.
///
/// Returns `true` when the topologies agree (request accepted).
fn cluster_fingerprint_matches(ctx: &Context, expected: u64) -> bool {
    if expected == 0 {
        return true;
    }

    let local = get_or_refresh_cluster_map(ctx);
    if local.cluster_slots_fingerprint() == expected {
        return true;
    }

    // Our map might just be stale; force one refresh and re-check.
    refresh_cluster_map(ctx);
    get_cluster_map().cluster_slots_fingerprint() == expected
}

/// Processes a valid request by executing the command and sending back the response.
fn process_request_message(
    ctx: &Context,
    header: FanoutMessageHeader,
    handler: RequestHandlerCallback,
    request_buf: &[u8],
    sender_id: NodeId,
) {
    let request_id = header.request_id;
    let db = header.db;

    // Reject the request if the cluster topology changed between the requester
    // generating it and us receiving it. This runs on the worker thread (not the
    // cluster-message callback) so the potential `CLUSTER NODES` refresh stays
    // off the main thread. The aggregate result would otherwise be built from
    // inconsistent per-node views.
    if !cluster_fingerprint_matches(ctx, header.cluster_fingerprint) {
        let msg = format!(
            "cluster rpc: rejecting request {request_id} from node {sender_id}: cluster-map fingerprint mismatch"
        );
        ctx.log_warning(&msg);
        send_error_response(
            ctx,
            request_id,
            db,
            sender_id.raw_ptr(),
            FanoutError::cluster_map_mismatch(),
        );
        return;
    }

    let mut dest = get_pooled_buffer(FANOUT_RPC_RESPONSE_BUFFER_SIZE);
    let _ = set_current_db(ctx, db);

    let user = header.user.as_deref();
    let res = with_fanout_user(ctx, user, |ctx| {
        handler(ctx, request_buf, &mut dest).map_err(ValkeyError::from)
    });

    if let Err(e) = res {
        let msg = e.to_string();
        send_error_response(ctx, request_id, db, sender_id.raw_ptr(), e.into());
        ctx.log_warning(&msg);
        return;
    };

    if send_response_message(
        ctx,
        request_id,
        db,
        sender_id.raw_ptr(),
        &header.handler,
        &dest,
    ) == Status::Err
    {
        let msg = format!("Failed to send response message to node {sender_id:?}");
        ctx.log_warning(&msg);
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
    unsafe {
        if ValkeyModule_SendClusterMessage
            .expect("ValkeyModule_SendClusterMessage is not available")(
            ctx.ctx as *mut ValkeyModuleCtx,
            target_node_id,
            msg_type,
            message_body.as_ptr().cast::<c_char>(),
            message_body.len() as u32,
        ) == VALKEYMODULE_OK as c_int
        {
            Status::Ok
        } else {
            Status::Err
        }
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
    let ctx = Context::new(ctx as *mut RedisModuleCtx);
    let Some(mut message) = parse_fanout_message(&ctx, sender_id, payload, len) else {
        return;
    };

    // Feature gate: a newer peer may demand envelope features via the header;
    // reject explicitly (addressable, fast) rather than mis-process the
    // request. See docs/fanout-compatibility-handshake.md.
    if has_unsupported_features(message.required_features) {
        let e = FanoutError::unsupported_features();
        send_error_response(&ctx, message.request_id, message.db, sender_id, e);
        let msg = format!(
            "Rejecting fanout request {} from node {}: unsupported required_features {:#06x}",
            message.request_id,
            NodeId::from_raw(sender_id),
            message.required_features
        );
        ctx.log_warning(&msg);
        return;
    }

    let Some(handler) = get_fanout_request_handler(&message.handler) else {
        let e = FanoutError::invalid_message();
        send_error_response(&ctx, message.request_id, message.db, sender_id, e);
        let msg = format!(
            "No handler registered for fanout operation '{}'",
            message.handler
        );
        ctx.log_warning(&msg);
        return;
    };

    let sender = NodeId::from_raw(sender_id);

    let mut buf = get_pooled_buffer(len as usize);
    buf.extend_from_slice(message.buf);

    let header = FanoutMessageHeader {
        version: message.version,
        required_features: message.required_features,
        request_id: message.request_id,
        db: message.db,
        handler: std::mem::take(&mut message.handler),
        user: message.user.take(),
        cluster_fingerprint: message.cluster_fingerprint,
    };

    alloc_db_if_needed(&ctx, message.db);

    spawn_with_context(move |ctx| {
        process_request_message(ctx, header, handler, &buf, sender);
    });
}

fn with_inflight_request<F>(ctx: &Context, request_id: u64, f: F)
where
    F: FnOnce(&Context, &InFlightRequest),
{
    let map = INFLIGHT_REQUESTS.pin();
    let Some(request) = map.get(&request_id) else {
        ctx.log_warning(&format!(
            "Failed to find inflight request for id {request_id}. Possible timeout.",
        ));
        return;
    };

    f(ctx, request);
    finish_inflight_request(ctx, request);
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
    let ctx = Context::new(ctx as *mut RedisModuleCtx);

    let Some(message) = parse_fanout_message(&ctx, sender_id, payload, len) else {
        ctx.log_warning("Failed to parse response message");
        return;
    };

    with_inflight_request(&ctx, message.request_id, |ctx, request| {
        let _ = set_current_db(ctx, message.db);
        // Feature gate: a newer peer's response may demand envelope features
        // (e.g. a payload encoding) we cannot decode; fail this node's slice
        // of the request instead of misinterpreting the payload.
        if has_unsupported_features(message.required_features) {
            let err = FanoutError::unsupported_features();
            request.handle_response(ctx, Err(err), sender_id);
            return;
        }
        request.handle_response(ctx, Ok(message.buf), sender_id);
    });
}

extern "C" fn on_error_received(
    ctx: *mut ValkeyModuleCtx,
    sender_id: *const c_char,
    _type: u8,
    payload: *const c_uchar,
    len: u32,
) {
    let ctx = Context::new(ctx as *mut RedisModuleCtx);

    let local_node_id = CURRENT_NODE_ID.raw_ptr();
    let Some(message) = parse_fanout_message(&ctx, local_node_id, payload, len) else {
        return;
    };

    with_inflight_request(&ctx, message.request_id, |ctx, request| {
        let _ = set_current_db(ctx, message.db);

        // Feature gate: mirror the response path — an error payload we cannot
        // decode per the demanded features still fails this node's slice.
        if has_unsupported_features(message.required_features) {
            let err = FanoutError::unsupported_features();
            request.handle_response(ctx, Err(err), sender_id);
            return;
        }

        match FanoutError::deserialize(message.buf) {
            Ok((error, _)) => {
                // A peer rejected us because its view of the cluster topology
                // differs from ours. Invalidate our local map so the next fanout
                // rebuilds it before targeting nodes.
                if error.kind == ErrorKind::ClusterMapMismatch {
                    mark_cluster_map_stale();
                }
                request.handle_response(ctx, Err(error), sender_id)
            }
            Err(_) => {
                ctx.log_warning("Failed to deserialize error response");
                let err = FanoutError::invalid_message();
                request.handle_response(ctx, Err(err), sender_id);
            }
        }
    });
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
}
