use crate::commands::command_parser::parse_query_command_args;
use crate::commands::promql_utils::{get_promql_querier, reply_with_query_value};
use crate::common::context::get_current_db;
use crate::common::context::{ClientThreadSafeContext, create_blocked_client};
use crate::common::time::system_time_to_millis;
use crate::promql::engine::{PROMQL_CONFIG, evaluate_instant};
use std::ops::Deref;
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

///
/// TS.QUERY <query>
///         [TIME rfc3339 | unix_timestamp | * | + ]
///         [LOOKBACK_DELTA lookback]
///         [TIMEOUT duration]
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
    let (eval_stmt, mut opts) = parse_query_command_args(promql_config, &mut args)?;
    // Capture the client's selected database from the per-client command context
    // before we move into a background thread. This ensures the query is evaluated
    // against the correct database regardless of what the module-global context
    // happens to have selected at the time the worker thread runs.
    opts.db = get_current_db(ctx);

    let blocked_client = create_blocked_client(ctx);
    let eval_ts = eval_stmt.start;

    let querier = get_promql_querier(ctx);
    std::thread::spawn(move || {
        let thread_ctx = ClientThreadSafeContext::with_blocked_client(blocked_client);

        let result = match evaluate_instant(querier, eval_stmt, eval_ts, opts) {
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

    // We will reply later, from the thread
    Ok(ValkeyValue::NoReply)
}
