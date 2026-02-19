use crate::commands::command_parser::parse_query_command_args;
use crate::commands::promql_utils::{get_promql_querier, reply_with_query_value};
use crate::common::context::{ClientThreadSafeContext, create_blocked_client};
use crate::common::threads::spawn;
use crate::common::time::system_time_to_millis;
use crate::promql::engine::config::PROMQL_CONFIG;
use crate::promql::engine::evaluate_instant;
use std::ops::Deref;
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

///
/// TS.QUERY <query>
///         [TIME rfc3339 | unix_timestamp | * | + ]
///         [LOOKBACK_DELTA lookback]
///         [TIMEOUT duration]
///
pub fn ts_query_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let config_guard = PROMQL_CONFIG.read()?;
    let mut args = args.into_iter().skip(1).peekable();
    let promql_config = config_guard.deref();
    let eval_stmt = parse_query_command_args(promql_config, &mut args)?;

    let blocked_client = create_blocked_client(ctx);
    let eval_ts = eval_stmt.start;

    let querier = get_promql_querier(ctx);
    spawn(move || {
        let thread_ctx = ClientThreadSafeContext::with_blocked_client(blocked_client);

        let result = match evaluate_instant(querier, eval_stmt, eval_ts) {
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
