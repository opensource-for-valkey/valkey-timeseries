use crate::commands::command_parser::{ParsedPromqlQuery, parse_query_range_command_args};
use crate::commands::promql_utils::{get_promql_querier, reply_with_query_value};
use crate::common::context::get_current_db;
use crate::common::time::current_time_millis;
use crate::promql::QueryValue;
use crate::promql::engine::query_workers::submit_evaluation;
use crate::promql::engine::{evaluate_range, promql_config};
use std::ops::Deref;
use valkey_module::{Context, ValkeyResult, ValkeyString};

// todo: limit number - limit the number of returned series
acl_categories!(TS_QUERYRANGE, "ts.queryrange", "read timeseries");
///
/// TS.QUERYRANGE <query>
///     STEP duration
///     [START rfc3339 | unix_timestamp | + | - | * ]
///     [END rfc3339 | unix_timestamp | + | - | * ]
///     [LOOKBACK_DELTA lookback]
///     [TIMEOUT duration]
///     [HASHTAG hash_tag,...]
///
#[valkey_module_macros::command({
    name: "TS.QUERYRANGE",
    flags: [ReadOnly],
    summary: "Evaluate a PromQL query over a range of time at a fixed step interval.",
    complexity: "O(N*M*S) where N is the number of matching series, M the samples examined and S the number of steps.",
    since: "1.0.0",
    arity: -4,
    key_spec: []
})]
pub fn ts_queryrange_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();
    let config_guard = promql_config();
    let promql_config = config_guard.deref();
    let ParsedPromqlQuery {
        eval_stmt,
        mut options,
        hash_tags,
    } = parse_query_range_command_args(promql_config, &mut args)?;
    // Capture the client's selected database from the per-client command context
    // before we move into a background thread. This ensures the query is evaluated
    // against the correct database regardless of what the module-global context
    // happens to have selected at the time the worker thread runs.
    options.db = get_current_db(ctx);

    let querier = get_promql_querier(ctx, hash_tags);
    submit_evaluation(
        ctx,
        options.deadline,
        move || evaluate_range(querier, eval_stmt, options),
        |ctx, result| {
            reply_with_query_value(ctx, QueryValue::Matrix(result), current_time_millis());
        },
    )
}
