use crate::commands::command_parser::parse_query_index_command_args;
use crate::commands::ts_queryindex_fanout_command::QueryIndexFanoutCommand;
use crate::common::replies::{reply_with_array, reply_with_valkey_string};
use crate::fanout::{FanoutClientCommand, is_clustered};
use crate::series::index::series_keys_by_selectors;
use valkey_module::ValkeyError::WrongArity;
use valkey_module::{Context, ValkeyResult, ValkeyString, ValkeyValue};

#[valkey_module_macros::command({
    name: "ts.queryindex",
    flags: [ReadOnly],
    summary: "Return the keys of time series matching a filter.",
    complexity: "O(N) where N is the number of time series that match the filters.",
    since: "1.0.0",
    arity: -2,
    key_spec: []
})]
pub fn ts_queryindex_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 2 {
        return Err(WrongArity);
    }
    let mut args = args.into_iter().skip(1).peekable();

    let options = parse_query_index_command_args(&mut args)?;

    if is_clustered(ctx) {
        // in cluster mode, we need to send the request to all nodes
        let operation = QueryIndexFanoutCommand::new(options);
        return operation.exec(ctx);
    }

    let mut keys = series_keys_by_selectors(ctx, &options.matchers, options.date_range)?;

    keys.sort_unstable();

    reply_with_array(ctx, keys.len());
    for key in keys.iter() {
        reply_with_valkey_string(ctx, key);
    }

    Ok(ValkeyValue::NoReply)
}
