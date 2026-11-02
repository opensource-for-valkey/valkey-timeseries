use crate::commands::parse_mrange_options;
use crate::commands::ts_mrange_fanout_command::MRangeFanoutCommand;
use crate::commands::utils::{MRangeReplyShape, reply_with_mrange_series_results};
use crate::common::context::is_blocking_denied;
use crate::common::replies::{IntoRawCtx, ThreadSafeReplyContext, block_client};
use crate::common::threads::{BoundedExecutor, Capacity, lane_workers};
use crate::error_consts;
use crate::fanout::{FanoutClientCommand, is_clustered};
use crate::series::mrange::{
    LocalMRange, decode_snapshot_series, is_snapshot_answerable, local_mrange_query,
    process_mrange_query,
};
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

/// EXPERIMENT (not for merge as-is): `TS_MRANGE_DEFER=1` answers non-clustered,
/// non-grouped TS.MRANGE off the main thread — chunks are copied under the
/// command, the client is blocked, and decode + reply happen on the
/// [`MRANGE_EXECUTOR`] lane through a thread-safe reply context, the way
/// TS.OUTLIERS already works. Read once so the two modes can be A/B'd from one
/// binary.
static DEFER_MRANGE: LazyLock<bool> =
    LazyLock::new(|| std::env::var("TS_MRANGE_DEFER").is_ok_and(|v| v == "1"));

/// Deferred TS.MRANGE jobs waiting for a worker before new requests run inline.
const MRANGE_QUEUE_CAPACITY: usize = 256;

/// The lane deferred TS.MRANGE requests decode and reply on. A [`BoundedExecutor`]
/// rather than the shared pool: a pool job may not be spawned unbounded, and a large
/// decode would hold a shared worker that GIL holders wait on (R2 in `common::threads`).
static MRANGE_EXECUTOR: BoundedExecutor = BoundedExecutor::new(
    "ts-mrange",
    lane_workers,
    Capacity::Fixed(MRANGE_QUEUE_CAPACITY),
);

/// Window within which another client's TS.MRANGE marks the main thread as
/// contended. Under contention the block/unblock round trip is hidden by other
/// clients' work, and deferring even a one-series request measured 1.2–1.3×
/// throughput at 16 connections; with no other client it costs a lone small
/// request ~11 %.
const CONTENTION_WINDOW_MICROS: u64 = 1_000;

/// The client and arrival time (µs since [`CONTENTION_EPOCH`]) of the last TS.MRANGE.
/// Only the main thread touches these; atomics just make the statics `Sync`.
static LAST_MRANGE_CLIENT: AtomicU64 = AtomicU64::new(0);
static LAST_MRANGE_MICROS: AtomicU64 = AtomicU64::new(0);
static CONTENTION_EPOCH: LazyLock<Instant> = LazyLock::new(Instant::now);

/// Records this request and reports whether a different client's TS.MRANGE
/// arrived within [`CONTENTION_WINDOW_MICROS`]. Comparing clients matters: one
/// client issuing back-to-back requests arrives just as often, and gains nothing.
fn mrange_contended(ctx: &Context) -> bool {
    let client = ctx.get_client_id();
    // `+ 1` keeps 0 free as "no previous request".
    let now = CONTENTION_EPOCH.elapsed().as_micros() as u64 + 1;
    let last_client = LAST_MRANGE_CLIENT.swap(client, Ordering::Relaxed);
    let last = LAST_MRANGE_MICROS.swap(now, Ordering::Relaxed);
    last != 0 && last_client != client && now - last <= CONTENTION_WINDOW_MICROS
}

acl_categories!(TS_MRANGE, "ts.mrange", "read timeseries");
/// TS.MRANGE fromTimestamp toTimestamp
//   [LATEST]
//   [FILTER_BY_TS ts...]
//   [FILTER_BY_VALUE min max]
//   [WITHLABELS | <SELECTED_LABELS label...>]
//   [COUNT count]
//   [HASHTAG hash_tag,...]
//   [[ALIGN align] AGGREGATION aggregator bucketDuration [CONDITION op value] [BUCKETTIMESTAMP bt] [EMPTY]]
//   FILTER filterExpr...
//   [GROUPBY label REDUCE reducer]
//   [EXCLUDEEMPTY]
#[valkey_module_macros::command({
    name: "ts.mrange",
    flags: [ReadOnly],
    summary: "Query a range across multiple time series selected by a filter, in forward order.",
    complexity: "O(N*M) where N is the number of matching series and M the number of samples in the range.",
    since: "1.0.0",
    arity: -4,
    key_spec: []
})]
pub fn ts_mrange_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    mrange_internal(ctx, args, false)
}

acl_categories!(TS_MREVRANGE, "ts.mrevrange", "read timeseries");
#[valkey_module_macros::command({
    name: "ts.mrevrange",
    flags: [ReadOnly],
    summary: "Query a range across multiple time series selected by a filter, in reverse order.",
    complexity: "O(N*M) where N is the number of matching series and M the number of samples in the range.",
    since: "1.0.0",
    arity: -4,
    key_spec: []
})]
pub fn ts_mrevrange_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    mrange_internal(ctx, args, true)
}

fn mrange_internal(ctx: &Context, args: Vec<ValkeyString>, reverse: bool) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();
    let mut options = parse_mrange_options(&mut args)?;

    if options.filters.is_empty() {
        return Err(ValkeyError::Str(error_consts::MISSING_FILTER));
    }

    options.is_reverse = reverse;

    args.done()?;

    if is_clustered(ctx) {
        let operation = MRangeFanoutCommand::new(options);
        return operation.exec(ctx);
    }

    if *DEFER_MRANGE && !is_blocking_denied(ctx) && is_snapshot_answerable(&options) {
        return mrange_deferred(ctx, options);
    }

    let shape = MRangeReplyShape::from_options(&options);
    let result_rows = process_mrange_query(ctx, options, false, None, &|| false)?;
    reply_with_mrange_series_results(ctx, &result_rows, &shape)
}

/// Answers inline unless other clients keep the main thread busy
/// ([`mrange_contended`]) or the request carries enough work to pay for the
/// block/unblock round trip ([`DEFER_MIN_WORK`](crate::series::mrange::DEFER_MIN_WORK));
/// then snapshots on the main thread and decodes and replies on the
/// [`MRANGE_EXECUTOR`] lane.
///
/// The lane slot is reserved before the client is blocked; when the lane is full the
/// request is answered inline instead, since deferring is only an optimization.
fn mrange_deferred(
    ctx: &Context,
    options: crate::series::request_types::MRangeOptions,
) -> ValkeyResult {
    let shape = MRangeReplyShape::from_options(&options);
    let contended = mrange_contended(ctx);
    let (slot, series) = match local_mrange_query(ctx, &options, contended, || {
        MRANGE_EXECUTOR.try_reserve().ok()
    })? {
        LocalMRange::Answered(rows) => {
            return reply_with_mrange_series_results(ctx, &rows, &shape);
        }
        LocalMRange::Deferred(slot, series) => (slot, series),
    };
    let blocked_client = block_client(ctx);
    slot.spawn(move || {
        let thread_ctx = ThreadSafeReplyContext::with_blocked_client(blocked_client);
        let rows = decode_snapshot_series(series, &options);
        // Through `into_raw`, which marks the client answered: otherwise dropping
        // `thread_ctx` would append an error after the complete reply.
        let reply_ctx = Context::new((&thread_ctx).into_raw());
        let _ = reply_with_mrange_series_results(&reply_ctx, &rows, &shape);
    });
    Ok(ValkeyValue::NoReply)
}
