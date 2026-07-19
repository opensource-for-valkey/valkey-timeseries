use crate::series::{get_latest_compaction_sample, with_timeseries};
use valkey_module::ValkeyError::WrongArity;
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

/// TS.GET key [LATEST]
#[valkey_module_macros::command({
    name: "ts.get",
    flags: [ReadOnly, Fast],
    summary: "Get the last sample of a time series.",
    complexity: "O(1)",
    since: "1.0.0",
    arity: -2,
    key_spec: [{
        flags: [ReadOnly, Access],
        begin_search: Index({ index: 1 }),
        find_keys: Range({ last_key: 0, steps: 1, limit: 0 })
    }]
})]
pub fn ts_get_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 2 || args.len() > 3 {
        return Err(WrongArity);
    }

    let latest = args
        .get(2)
        .map(|arg| arg.eq_ignore_ascii_case("latest".as_ref()))
        .unwrap_or(false);

    if args.len() == 3 && !latest {
        return Err(ValkeyError::Str("TSDB: wrong 3rd argument"));
    }

    let key = &args[1];
    let sample = with_timeseries(ctx, key, true, |series| {
        if latest && let Some(value) = get_latest_compaction_sample(ctx, series) {
            Ok(Some(value))
        } else {
            Ok(series.reported_last_sample())
        }
    })?;

    Ok(sample.map_or_else(|| ValkeyValue::Array(vec![]), Into::into))
}
