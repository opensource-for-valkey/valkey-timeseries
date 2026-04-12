use crate::commands::command_parser::parse_query_range_command_args;
use crate::commands::promql_utils::{get_promql_querier, reply_with_query_value};
use crate::common::context::{ClientThreadSafeContext, create_blocked_client};
use crate::common::threads::spawn;
use crate::common::time::current_time_millis;
use crate::promql::QueryValue;
use crate::promql::engine::config::PROMQL_CONFIG;
use crate::promql::engine::evaluate_range;
use std::ops::Deref;
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

// todo: limit number - limit the number of returned series
///
/// TS.QUERYRANGE <query>
///     STEP duration
///     [START rfc3339 | unix_timestamp | + | - | * ]
///     [END rfc3339 | unix_timestamp | + | - | * ]
///     [LOOKBACK_DELTA lookback]
///
pub fn ts_queryrange_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();
    let config_guard = PROMQL_CONFIG
        .read()
        .expect("Failed to acquire read lock on PROMQL_CONFIG");
    let promql_config = config_guard.deref();
    let (eval_stmt, opts) = parse_query_range_command_args(promql_config, &mut args)?;

    let blocked_client = create_blocked_client(ctx);
    let querier = get_promql_querier(ctx);

    spawn(move || {
        let thread_ctx = ClientThreadSafeContext::with_blocked_client(blocked_client);

        let result = match evaluate_range(querier, eval_stmt, opts) {
            Ok(res) => res,
            Err(err) => {
                let e = ValkeyError::String(err.to_string());
                thread_ctx.reply(Err(e));
                return;
            }
        };

        let ctx = thread_ctx.get_write_context();
        reply_with_query_value(&ctx, QueryValue::Matrix(result), current_time_millis());
    });

    // We will reply later, from the thread
    Ok(ValkeyValue::NoReply)
}
