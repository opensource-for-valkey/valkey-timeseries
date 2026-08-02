use crate::commands::command_parser::parse_query_labels_command_args;
use crate::commands::ts_querylabels_fanout_command::QueryLabelsFanoutCommand;
use crate::common::replies::reply_with_string_set;
use crate::fanout::{FanoutClientCommand, is_clustered};
use crate::series::index::query_labels_distinct;
use valkey_module::ValkeyError::WrongArity;
use valkey_module::{Context, ValkeyResult, ValkeyString, ValkeyValue};

/// TS.QUERYLABELS <LABELS | VALUES label> [FILTER filterExpr [filterExpr ...]]
///
/// Returns the distinct label names (with `LABELS`) or the distinct values of a single
/// label (with `VALUES label`) across the time series matching the optional `FILTER`
/// list — or across every indexed series when `FILTER` is omitted. Each name or value
/// appears at most once; the order is not defined.
///
/// Unlike `TS.QUERYINDEX`, which reveals every matching key regardless of the caller's
/// read access, `TS.QUERYLABELS` silently omits series the caller may not read, so label
/// names and values belonging only to unreadable series do not appear in the result.
#[valkey_module_macros::command({
    name: "ts.querylabels",
    flags: [ReadOnly],
    summary: "Get all label names, or all values of a given label, for time series matching a filter list, or all series",
    complexity: "O(N) where N is the number of time series that match the filters (all indexed series when FILTER is omitted).",
    since: "0.10.0",
    arity: -2,
    key_spec: []
})]
pub fn ts_querylabels_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 2 {
        return Err(WrongArity);
    }
    let options = parse_query_labels_command_args(&mut args.into_iter().skip(1).peekable())?;

    if is_clustered(ctx) {
        let operation = QueryLabelsFanoutCommand::new(options);
        return operation.exec(ctx);
    }

    // Deduplicate while collecting; `BTreeSet` also gives the stable, sorted emission
    // order the reply serializer relies on (the docs leave the order undefined, but a
    // deterministic reply keeps the compat diff and tests reproducible).
    let values = query_labels_distinct(ctx, &options.matchers, options.label.as_deref())?;
    let values: Vec<String> = values.into_iter().collect();
    reply_with_string_set(ctx, &values);
    Ok(ValkeyValue::NoReply)
}
