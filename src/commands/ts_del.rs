use crate::commands::command_parser::parse_timestamp_range;
use crate::series::with_timeseries_mut;
use valkey_module::{
    AclPermissions, Context, NextArg, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString,
    ValkeyValue,
};

///
/// TS.DEL key fromTimestamp toTimestamp
///
#[valkey_module_macros::command({
    name: "ts.del",
    flags: [Write, DenyOOM],
    summary: "Delete samples of a time series within a timestamp range.",
    complexity: "O(N) where N is the number of samples removed.",
    since: "1.0.0",
    arity: -3,
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
    let count = with_timeseries_mut(ctx, &key, Some(AclPermissions::DELETE), |series| {
        let (start_ts, end_ts) = date_range.get_series_range(series, None, false);

        // A range below the retention window is not an error: RedisTimeSeries deletes whatever
        // the range intersects and reports the count (0 when it covers only already-expired
        // time). Rejecting it here was an over-strict divergence found by the differential fuzzer.
        series
            .remove_range_with_compaction(ctx, start_ts, end_ts)
            .map_err(|_e| ValkeyError::String("TSDB: error deleting range".to_string()))
        // todo: better error
    })?;

    ctx.replicate_verbatim();
    ctx.notify_keyspace_event(NotifyEvent::MODULE, "ts.del", &key);

    Ok(ValkeyValue::from(count))
}
