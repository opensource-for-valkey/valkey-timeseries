use crate::commands::parse_mrange_options;
use crate::commands::ts_mrange_fanout_command::MRangeFanoutCommand;
use crate::commands::utils::{MRangeReplyShape, reply_with_mrange_series_results};
use crate::common::context::is_blocking_denied;
use crate::common::replies::{IntoRawCtx, ThreadSafeReplyContext, block_client};
use crate::common::threads::{BoundedExecutor, Capacity, lane_workers};
use crate::error_consts;
use crate::fanout::{FanoutClientCommand, is_clustered};
use crate::series::mrange::{
    decode_snapshot_series, is_snapshot_answerable, process_mrange_query, snapshot_mrange_query,
};
use std::sync::LazyLock;
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

/// Snapshot on the main thread, decode and reply on the [`MRANGE_EXECUTOR`] lane.
///
/// The lane slot is reserved before the client is blocked; when the lane is full the
/// request is answered inline instead, since deferring is only an optimization.
fn mrange_deferred(
    ctx: &Context,
    options: crate::series::request_types::MRangeOptions,
) -> ValkeyResult {
    let Ok(slot) = MRANGE_EXECUTOR.try_reserve() else {
        let shape = MRangeReplyShape::from_options(&options);
        let result_rows = process_mrange_query(ctx, options, false, None, &|| false)?;
        return reply_with_mrange_series_results(ctx, &result_rows, &shape);
    };
    let (series, _copied) = snapshot_mrange_query(ctx, &options)?;
    let shape = MRangeReplyShape::from_options(&options);
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
