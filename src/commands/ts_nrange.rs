use crate::commands::command_parser::parse_nrange_options;
use crate::common::replies::reply_with_pivot_rows;
use crate::series::nrange::process_nrange_query;
use valkey_module::{Context, NextArg, ValkeyResult, ValkeyString, ValkeyValue};

/// TS.NRANGE numkeys key [key ...] fromTimestamp toTimestamp
//   [LATEST]
//   [FILTER_BY_TS ts...]
//   [FILTER_BY_VALUE min max]
//   [COUNT count]
//   [[ALIGN align] AGGREGATION aggregators [aggregators ...] bucketDuration [BUCKETTIMESTAMP bt] [EMPTY]]
//
// Unlike TS.MRANGE, which selects its series with a filter and replies per series, TS.NRANGE
// takes the keys explicitly and replies per timestamp: each row carries one value per key (or
// one per aggregator, under AGGREGATION), NaN where a key has nothing to report.
//
// The keys are declared through the command's key spec, so in cluster mode the server routes
// the command by slot and rejects a cross-slot key list before the handler runs — there is no
// fan-out to do here.
#[valkey_module_macros::command({
    name: "ts.nrange",
    flags: [ReadOnly],
    summary: "Query a range across an explicit list of same-slot time series, grouping the results by timestamp, in forward order.",
    complexity: "O(N*M) where N is the number of keys and M the number of samples in the range.",
    since: "1.0.0",
    arity: -5,
    key_spec: [{
        flags: [ReadOnly, Access],
        begin_search: Index({ index: 1 }),
        find_keys: Keynum({ key_num_idx: 0, first_key: 1, key_step: 1 })
    }]
})]
pub fn ts_nrange_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    nrange_internal(ctx, args, false)
}

/// TS.NREVRANGE numkeys key [key ...] fromTimestamp toTimestamp
//   [LATEST]
//   [FILTER_BY_TS ts...]
//   [FILTER_BY_VALUE min max]
//   [COUNT count]
//   [[ALIGN align] AGGREGATION aggregators [aggregators ...] bucketDuration [BUCKETTIMESTAMP bt] [EMPTY]]
//
// TS.NRANGE with the rows in descending timestamp order. Only the output order changes: the
// join still runs ascending, and COUNT still limits rows in the *requested* order, so it keeps
// the highest timestamps here rather than the lowest.
#[valkey_module_macros::command({
    name: "ts.nrevrange",
    flags: [ReadOnly],
    summary: "Query a range across an explicit list of same-slot time series, grouping the results by timestamp, in reverse order.",
    complexity: "O(N*M) where N is the number of keys and M the number of samples in the range.",
    since: "1.0.0",
    arity: -5,
    key_spec: [{
        flags: [ReadOnly, Access],
        begin_search: Index({ index: 1 }),
        find_keys: Keynum({ key_num_idx: 0, first_key: 1, key_step: 1 })
    }]
})]
pub fn ts_nrevrange_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    nrange_internal(ctx, args, true)
}

fn nrange_internal(ctx: &Context, args: Vec<ValkeyString>, is_reverse: bool) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();
    let mut options = parse_nrange_options(&mut args)?;

    options.is_reverse = is_reverse;

    args.done()?;

    let rows = process_nrange_query(ctx, &options)?;
    reply_with_pivot_rows(ctx, rows.iter());

    Ok(ValkeyValue::NoReply)
}
