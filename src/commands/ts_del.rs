use crate::commands::command_parser::parse_timestamp_range;
use crate::common::context::notify_keyspace_event;
use crate::series::with_timeseries_mut;
use std::ffi::CStr;
use valkey_module::{
    AclPermissions, Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue,
};

const DEL_EVENT: &CStr = c"ts.del";

acl_categories!(TS_DEL, "ts.del", "write timeseries");
///
/// TS.DEL key fromTimestamp toTimestamp
///
#[valkey_module_macros::command({
    name: "ts.del",
    flags: [Write, DenyOOM],
    summary: "Delete samples of a time series within a timestamp range.",
    complexity: "O(N) where N is the number of samples removed.",
    since: "1.0.0",
    // Exactly `TS.DEL key fromTimestamp toTimestamp`: both bounds are required, and nothing
    // may follow them. The server rejects any other count before the handler runs, with the
    // same "wrong number of arguments" reply as the reference; a variable arity let
    // `TS.DEL key 5` (end defaulting to `+`) and `TS.DEL key - + garbage` through.
    arity: 4,
    key_spec: [{
        flags: [ReadWrite, Delete],
        begin_search: Index({ index: 1 }),
        find_keys: Range({ last_key: 0, steps: 1, limit: 0 })
    }]
})]
pub fn ts_del_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();
    let key = args.next_arg()?;

    let date_range = parse_timestamp_range(&mut args)?;
    let (count, start_ts, end_ts) =
        with_timeseries_mut(ctx, &key, Some(AclPermissions::DELETE), |series| {
            let (start_ts, end_ts) = date_range.get_series_range(series, None, false);

            // A range below the retention window is not an error: RedisTimeSeries deletes
            // whatever the range intersects and reports the count (0 when it covers only
            // already-expired time). Rejecting it here was an over-strict divergence found by
            // the differential fuzzer.
            let count = series
                .remove_range_with_compaction(ctx, start_ts, end_ts)
                .map_err(|_e| ValkeyError::String("TSDB: error deleting range".to_string()))?;
            // todo: better error
            Ok((count, start_ts, end_ts))
        })?;

    // Propagate the resolved bounds, as TS.MDEL does. The range grammar accepts relative and
    // symbolic bounds (`-1h`, `*`, `-`, `+`); replicated verbatim, a replica or an AOF replay
    // resolves them against its own clock and series and deletes a different window.
    ctx.replicate(
        "TS.DEL",
        &[
            &key,
            &ctx.create_string(start_ts.to_string()),
            &ctx.create_string(end_ts.to_string()),
        ],
    );
    notify_keyspace_event(ctx, DEL_EVENT, &key);

    Ok(ValkeyValue::from(count))
}
