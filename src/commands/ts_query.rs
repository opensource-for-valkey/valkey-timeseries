use crate::commands::command_parser::{ParsedPromqlQuery, parse_query_command_args};
use crate::commands::promql_utils::{get_promql_querier, reply_with_query_value};
use crate::common::context::get_current_db;
use crate::common::time::system_time_to_millis;
use crate::promql::engine::query_workers::submit_evaluation;
use crate::promql::engine::{evaluate_instant, promql_config};
use std::ops::Deref;
use valkey_module::{Context, ValkeyResult, ValkeyString};

acl_categories!(TS_QUERY, "ts.query", "read timeseries");
///
/// TS.QUERY <query>
///         [TIME rfc3339 | unix_timestamp | * | + ]
///         [LOOKBACK_DELTA lookback]
///         [TIMEOUT duration]
///         [HASHTAG hash_tag,...]
///
#[valkey_module_macros::command({
    name: "TS.QUERY",
    flags: [ReadOnly],
    summary: "Evaluate a PromQL query at a single point in time.",
    complexity: "O(N*M) where N is the number of matching series and M the number of samples examined.",
    since: "1.0.0",
    arity: -2,
    key_spec: []
})]
pub fn ts_query_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let config_guard = promql_config();
    let mut args = args.into_iter().skip(1).peekable();
    let promql_config = config_guard.deref();
    let ParsedPromqlQuery {
        eval_stmt,
        mut options,
        hash_tags,
    } = parse_query_command_args(promql_config, &mut args)?;
    // Capture the client's selected database from the per-client command context
    // before we move into a background thread. This ensures the query is evaluated
    // against the correct database regardless of what the module-global context
    // happens to have selected at the time the worker thread runs.
    options.db = get_current_db(ctx);

    let eval_ts = eval_stmt.start;
    let querier = get_promql_querier(ctx, hash_tags);
    submit_evaluation(
        ctx,
        options.deadline,
        move || evaluate_instant(querier, eval_stmt, eval_ts, options),
        move |ctx, result| {
            reply_with_query_value(ctx, result, system_time_to_millis(eval_ts));
        },
    )
}
