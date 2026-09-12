use crate::commands::command_parser::{ParsedPromqlQuery, parse_query_range_command_args};
use crate::commands::promql_utils::{get_promql_querier, reply_with_query_value};
use crate::common::context::get_current_db;
use crate::common::context::{ClientThreadSafeContext, create_blocked_client};
use crate::common::time::current_time_millis;
use crate::promql::QueryError;
use crate::promql::QueryValue;
use crate::promql::engine::query_workers::submit_query;
use crate::promql::engine::{PROMQL_CONFIG, evaluate_range};
use std::ops::Deref;
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

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
    let config_guard = PROMQL_CONFIG
        .read()
        .expect("Failed to acquire read lock on PROMQL_CONFIG");
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

    let blocked_client = create_blocked_client(ctx);
    let querier = get_promql_querier(ctx, hash_tags);

    let queued = submit_query(move || {
        let thread_ctx = ClientThreadSafeContext::with_blocked_client(blocked_client);

        // The wait for a worker counts against the query's budget: one that
        // expired in the queue is answered as a timeout, not evaluated.
        if options.deadline.is_some_and(|d| current_time_millis() > d) {
            thread_ctx.reply(Err(ValkeyError::String(QueryError::Timeout.to_string())));
            return;
        }

        let result = match evaluate_range(querier, eval_stmt, options) {
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
    if !queued {
        return Err(ValkeyError::Str("TSDB: query workers are not running"));
    }

    // We will reply later, from a query worker
    Ok(ValkeyValue::NoReply)
}
