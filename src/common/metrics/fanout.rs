//! `fanout`: operations this node coordinates, requests it serves for peers, and the messages and
//! bytes it sends over the cluster bus.
//!
//! Coordinator-side counts are taken in `fanout::fanout_command` (one fanout operation, however
//! many targets) and `fanout::cluster_rpc` (the RPC that carries its remote shares); serving-side
//! counts in `cluster_rpc`'s request path. A fanout whose only target is this node never reaches
//! the RPC layer, so it is counted as an operation but sends nothing.
//!
//! Two timeouts can end one fanout, and are counted apart: the blocked client's (the deadline the
//! client sees; the only one for a local-only fanout) and the RPC timer's (remote targets only).
//! They usually both fire for one timed-out fanout, so don't add them up.

use super::duration_histogram;
use crate::fanout::{ErrorKind, inflight_request_count};
use metered::entry::{counter, gauge_value, metric};
use metered::{BucketHistogram, Counter, Registry};
use std::sync::LazyLock;
use std::sync::atomic::AtomicU64;

// --- coordinator --------------------------------------------------------------------------

/// Fanout operations started, one per command that fans out.
pub static FANOUT_OPERATIONS: AtomicU64 = AtomicU64::new(0);
/// Targets of those operations, the local node included.
pub static FANOUT_TARGETS: AtomicU64 = AtomicU64::new(0);
/// Operations whose only target was this node: no RPC, nothing on the bus.
pub static FANOUT_LOCAL_ONLY: AtomicU64 = AtomicU64::new(0);
/// Operations that failed before any remote request was sent.
pub static FANOUT_SETUP_FAILURES: AtomicU64 = AtomicU64::new(0);
/// Local shares refused because the fanout lane's queue was full.
pub static FANOUT_LOCAL_SHARE_BUSY: AtomicU64 = AtomicU64::new(0);
/// Local shares that waited in the queue past the deadline and were answered with a timeout.
pub static FANOUT_LOCAL_SHARE_EXPIRED: AtomicU64 = AtomicU64::new(0);
/// Commands refused because the client could not be blocked (MULTI, a script, a module call).
pub static FANOUT_BLOCKING_DENIED: AtomicU64 = AtomicU64::new(0);
/// Requests the cluster bus refused to send.
pub static FANOUT_SEND_FAILURES: AtomicU64 = AtomicU64::new(0);
/// Blocked clients answered with the timeout error.
pub static FANOUT_CLIENT_TIMEOUTS: AtomicU64 = AtomicU64::new(0);
/// RPCs whose timer fired with shares outstanding.
pub static FANOUT_RPC_TIMEOUTS: AtomicU64 = AtomicU64::new(0);
/// Operations ended early by an error returned to the client as it is (a cluster-map mismatch,
/// a permission denial, a busy shard).
pub static FANOUT_ABORTS: AtomicU64 = AtomicU64::new(0);
/// Operations answered with the generic "Internal error in fanout operation" reply, because at
/// least one shard failed with a kind that is not returned as it is.
pub static FANOUT_GENERIC_ERROR_REPLIES: AtomicU64 = AtomicU64::new(0);
/// Error responses from peers that could not be decoded.
pub static FANOUT_ERROR_DECODE_FAILURES: AtomicU64 = AtomicU64::new(0);
/// Answers dropped because no request with their id is in flight (it completed or timed out).
pub static FANOUT_IGNORED_UNKNOWN_REQUEST: AtomicU64 = AtomicU64::new(0);
/// Answers dropped because their sender is not one of the request's remote targets.
pub static FANOUT_IGNORED_UNKNOWN_SENDER: AtomicU64 = AtomicU64::new(0);
/// Answers dropped because their sender had already answered.
pub static FANOUT_IGNORED_DUPLICATE: AtomicU64 = AtomicU64::new(0);
/// Answers dropped because the operation had already completed (after an abort, say).
pub static FANOUT_IGNORED_AFTER_COMPLETION: AtomicU64 = AtomicU64::new(0);
/// `TS.MRANGE` series aggregated on the coordinator because their shard ignored aggregation
/// push-down (an older peer).
pub static FANOUT_PUSHDOWN_FALLBACK_SERIES: AtomicU64 = AtomicU64::new(0);
/// `TS.MRANGE` series reduced into group partials on the coordinator because their shard ignored
/// group-reduce push-down.
pub static FANOUT_PUSHDOWN_GROUP_FALLBACK_SERIES: AtomicU64 = AtomicU64::new(0);

/// Time from the start of an operation to its result, on every path that produces one.
pub static FANOUT_DURATION: LazyLock<BucketHistogram> = duration_histogram();

/// Shard errors seen as coordinator, by kind: one counter per [`ErrorKind::ALL`] entry, in that
/// order. Errors from the local share count too.
pub static FANOUT_ERRORS: [AtomicU64; ErrorKind::ALL.len()] =
    [const { AtomicU64::new(0) }; ErrorKind::ALL.len()];

/// Counts one shard error of `kind`.
#[inline]
pub fn record_error(kind: ErrorKind) {
    FANOUT_ERRORS[kind.metric_index()].incr();
}

// --- serving ------------------------------------------------------------------------------

/// Peer requests whose handler succeeded.
pub static FANOUT_SERVED_OK: AtomicU64 = AtomicU64::new(0);
/// Peer requests whose handler failed (answered with an error response).
pub static FANOUT_SERVED_ERRORS: AtomicU64 = AtomicU64::new(0);
/// Successful answers the cluster bus refused to send back.
pub static FANOUT_REPLY_SEND_FAILURES: AtomicU64 = AtomicU64::new(0);
/// Peer requests that could not be parsed.
pub static FANOUT_REJECTED_PARSE: AtomicU64 = AtomicU64::new(0);
/// Peer requests demanding envelope features this node lacks.
pub static FANOUT_REJECTED_UNSUPPORTED_FEATURES: AtomicU64 = AtomicU64::new(0);
/// Peer requests for an operation this node has no handler for.
pub static FANOUT_REJECTED_NO_HANDLER: AtomicU64 = AtomicU64::new(0);
/// Peer requests refused because the fanout lane's queue was full.
pub static FANOUT_REJECTED_BUSY: AtomicU64 = AtomicU64::new(0);
/// Peer requests rejected because the sender's cluster map disagrees with ours.
pub static FANOUT_REJECTED_CLUSTER_MAP_MISMATCH: AtomicU64 = AtomicU64::new(0);

// --- wire ---------------------------------------------------------------------------------

/// Fanout messages of one type this node handed to the cluster bus, and their payload bytes.
///
/// The payload is what the module passes to `ValkeyModule_SendClusterMessage`; the bus adds its
/// own framing, which the module cannot see. A request to several peers is sent, and counted,
/// once per peer. A fanout's local share never touches the bus and is not counted.
pub struct WireCounters {
    pub messages: AtomicU64,
    pub bytes: AtomicU64,
}

impl WireCounters {
    pub const fn new() -> Self {
        Self {
            messages: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn record(&self, payload_len: usize) {
        self.messages.incr();
        self.bytes.incr_by(payload_len as u64);
    }
}

impl Default for WireCounters {
    fn default() -> Self {
        Self::new()
    }
}

/// Requests sent to peers, as coordinator.
pub static FANOUT_REQUESTS_SENT: WireCounters = WireCounters::new();
/// Successful responses sent back to coordinators, as a peer.
pub static FANOUT_RESPONSES_SENT: WireCounters = WireCounters::new();
/// Error responses sent back to coordinators, as a peer.
pub static FANOUT_ERROR_RESPONSES_SENT: WireCounters = WireCounters::new();

// --- registration -------------------------------------------------------------------------

/// Plain counters: registered name, source, help.
static COUNTERS: &[(&str, &AtomicU64, &str)] = &[
    (
        "operations",
        &FANOUT_OPERATIONS,
        "Fanout operations started, one per command that fans out",
    ),
    (
        "targets",
        &FANOUT_TARGETS,
        "Targets of the fanout operations started, the local node included",
    ),
    (
        "local_only",
        &FANOUT_LOCAL_ONLY,
        "Fanout operations whose only target was this node (no RPC)",
    ),
    (
        "setup_failures",
        &FANOUT_SETUP_FAILURES,
        "Fanout operations that failed before any remote request was sent",
    ),
    (
        "local_share_busy",
        &FANOUT_LOCAL_SHARE_BUSY,
        "Local fanout shares refused because the fanout lane's queue was full",
    ),
    (
        "local_share_expired",
        &FANOUT_LOCAL_SHARE_EXPIRED,
        "Local fanout shares that waited in the queue past the deadline",
    ),
    (
        "blocking_denied",
        &FANOUT_BLOCKING_DENIED,
        "Fanout commands refused because the client could not be blocked (MULTI, script, module call)",
    ),
    (
        "send_failures",
        &FANOUT_SEND_FAILURES,
        "Fanout requests the cluster bus refused to send",
    ),
    (
        "client_timeouts",
        &FANOUT_CLIENT_TIMEOUTS,
        "Blocked fanout clients answered with the timeout error",
    ),
    (
        "rpc_timeouts",
        &FANOUT_RPC_TIMEOUTS,
        "Fanout RPCs whose timer fired with remote shares outstanding",
    ),
    (
        "aborts",
        &FANOUT_ABORTS,
        "Fanout operations ended early by an error returned as it is (cluster-map mismatch, permission denial, busy shard)",
    ),
    (
        "generic_error_replies",
        &FANOUT_GENERIC_ERROR_REPLIES,
        "Fanout operations answered with the generic internal-error reply after a shard failed",
    ),
    (
        "error_decode_failures",
        &FANOUT_ERROR_DECODE_FAILURES,
        "Error responses from peers that could not be decoded",
    ),
    (
        "ignored_unknown_request",
        &FANOUT_IGNORED_UNKNOWN_REQUEST,
        "Peer answers dropped because no request with their id is in flight",
    ),
    (
        "ignored_unknown_sender",
        &FANOUT_IGNORED_UNKNOWN_SENDER,
        "Peer answers dropped because their sender is not a remote target of the request",
    ),
    (
        "ignored_duplicate",
        &FANOUT_IGNORED_DUPLICATE,
        "Peer answers dropped because their sender had already answered",
    ),
    (
        "ignored_after_completion",
        &FANOUT_IGNORED_AFTER_COMPLETION,
        "Shard answers dropped because the fanout operation had already completed",
    ),
    (
        "pushdown_fallback_series",
        &FANOUT_PUSHDOWN_FALLBACK_SERIES,
        "TS.MRANGE series aggregated on the coordinator because their shard ignored aggregation push-down",
    ),
    (
        "pushdown_group_fallback_series",
        &FANOUT_PUSHDOWN_GROUP_FALLBACK_SERIES,
        "TS.MRANGE series reduced on the coordinator because their shard ignored group-reduce push-down",
    ),
    (
        "served_ok",
        &FANOUT_SERVED_OK,
        "Peer fanout requests this node served successfully",
    ),
    (
        "served_errors",
        &FANOUT_SERVED_ERRORS,
        "Peer fanout requests whose handler failed on this node",
    ),
    (
        "reply_send_failures",
        &FANOUT_REPLY_SEND_FAILURES,
        "Successful answers to peers the cluster bus refused to send",
    ),
    (
        "rejected_parse",
        &FANOUT_REJECTED_PARSE,
        "Peer fanout requests rejected because they could not be parsed",
    ),
    (
        "rejected_unsupported_features",
        &FANOUT_REJECTED_UNSUPPORTED_FEATURES,
        "Peer fanout requests rejected for demanding envelope features this node lacks",
    ),
    (
        "rejected_no_handler",
        &FANOUT_REJECTED_NO_HANDLER,
        "Peer fanout requests rejected because this node has no handler for the operation",
    ),
    (
        "rejected_busy",
        &FANOUT_REJECTED_BUSY,
        "Peer fanout requests rejected because the fanout lane's queue was full",
    ),
    (
        "rejected_cluster_map_mismatch",
        &FANOUT_REJECTED_CLUSTER_MAP_MISMATCH,
        "Peer fanout requests rejected because the sender's cluster map disagrees with this node's",
    ),
    (
        "requests_sent",
        &FANOUT_REQUESTS_SENT.messages,
        "Fanout requests sent to peers over the cluster bus, one per peer",
    ),
    (
        "responses_sent",
        &FANOUT_RESPONSES_SENT.messages,
        "Fanout responses sent back to coordinators over the cluster bus",
    ),
    (
        "error_responses_sent",
        &FANOUT_ERROR_RESPONSES_SENT.messages,
        "Fanout error responses sent back to coordinators over the cluster bus",
    ),
];

/// Byte counters: registered name, source, help.
static BYTE_COUNTERS: &[(&str, &AtomicU64, &str)] = &[
    (
        "request_sent_bytes",
        &FANOUT_REQUESTS_SENT.bytes,
        "Payload bytes of the fanout requests sent to peers",
    ),
    (
        "response_sent_bytes",
        &FANOUT_RESPONSES_SENT.bytes,
        "Payload bytes of the fanout responses sent back to coordinators",
    ),
    (
        "error_response_sent_bytes",
        &FANOUT_ERROR_RESPONSES_SENT.bytes,
        "Payload bytes of the fanout error responses sent back to coordinators",
    ),
];

pub(super) fn register(registry: &mut Registry<'static>) {
    for &(name, source, help) in COUNTERS {
        registry.register(counter(name).source(source).help(help));
    }
    for &(name, source, help) in BYTE_COUNTERS {
        registry.register(counter(name).source(source).help(help).unit("bytes"));
    }
    for kind in ErrorKind::ALL {
        registry.register(
            counter(format!("errors_{}", kind.metric_name()))
                .source(&FANOUT_ERRORS[kind.metric_index()])
                .help(format!(
                    "Shard errors of kind {} seen by this node as fanout coordinator",
                    kind.metric_name()
                )),
        );
    }
    registry
        .register(
            gauge_value("inflight")
                .read(|_: &()| inflight_request_count())
                .help(
                    "Fanout RPCs with remote shares outstanding (local-only operations have none)",
                ),
        )
        .register(
            metric("duration_seconds")
                .source(LazyLock::force(&FANOUT_DURATION))
                .help("Time from the start of a fanout operation to its result")
                .unit("seconds"),
        );
}
