use crate::commands::command_parser::{ParsedPromqlQuery, parse_query_command_args};
use crate::commands::promql_utils::{get_promql_querier, reply_with_query_value};
use crate::common::context::get_current_db;
use crate::common::context::{ClientThreadSafeContext, create_blocked_client, is_blocking_denied};
use crate::common::time::{current_time_millis, system_time_to_millis};
use crate::error_consts;
use crate::promql::QueryError;
use crate::promql::engine::query_workers::{run_evaluation, submit_query};
use crate::promql::engine::{PROMQL_CONFIG, evaluate_instant};
use std::ops::Deref;
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

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
    let config_guard = PROMQL_CONFIG.read()?;
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

    // Checked before blocking: the server asserts on a blocked deny-blocking
    // client (a module `RM_Call` without the K flag) and aborts, and inside
    // MULTI or a script the client would get the server's own error while the
    // query still ran with nobody to answer.
    if is_blocking_denied(ctx) {
        return Err(ValkeyError::Str(error_consts::PROMQL_BLOCKING_NOT_ALLOWED));
    }
    let blocked_client = create_blocked_client(ctx);
    let eval_ts = eval_stmt.start;

    let querier = get_promql_querier(ctx, hash_tags);
    let queued = submit_query(move || {
        let thread_ctx = ClientThreadSafeContext::with_blocked_client(blocked_client);

        // The wait for a worker counts against the query's budget: one that
        // expired in the queue is answered as a timeout, not evaluated.
        if options.deadline.is_some_and(|d| current_time_millis() > d) {
            thread_ctx.reply(Err(ValkeyError::String(QueryError::Timeout.to_string())));
            return;
        }

        let result = match run_evaluation(|| evaluate_instant(querier, eval_stmt, eval_ts, options))
        {
            Ok(eval_stmt) => eval_stmt,
            Err(err) => {
                let e = ValkeyError::String(err.to_string());
                thread_ctx.reply(Err(e));
                return;
            }
        };

        let ctx = thread_ctx.get_write_context();
        reply_with_query_value(&ctx, result, system_time_to_millis(eval_ts));
    });
    if let Err(err) = queued {
        return Err(ValkeyError::String(format!("TSDB: {err}")));
    }

    // We will reply later, from a query worker
    Ok(ValkeyValue::NoReply)
}
