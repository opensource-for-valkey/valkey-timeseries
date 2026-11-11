use crate::common::replies::reply_with_sample;
use crate::series::{get_latest_compaction_sample, with_timeseries};
use valkey_module::ValkeyError::WrongArity;
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

acl_categories!(TS_GET, "ts.get", "fast read timeseries");
/// TS.GET key [LATEST]
#[valkey_module_macros::command({
    name: "ts.get",
    flags: [ReadOnly],
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
    // No module-side ACL check: the key spec covers `key` (see TS.RANGE).
    let sample = with_timeseries(ctx, key, None, |series| {
        if latest && let Some(value) = get_latest_compaction_sample(ctx, series) {
            Ok(Some(value))
        } else {
            Ok(series.last_sample)
        }
    })?;

    // Reply directly rather than through a `ValkeyValue` tree: no allocation
    // for the row, and the same helper every other sample reply uses.
    match sample {
        Some(sample) => {
            reply_with_sample(ctx, &sample);
            Ok(ValkeyValue::NoReply)
        }
        None => Ok(ValkeyValue::Array(vec![])),
    }
}
