use crate::commands::analysis_runner::{
    SeriesReadPlacement, run_read_on_analysis_lane, schedule_series_read,
};
use crate::commands::command_parser::parse_query_index_command_args;
use crate::commands::ts_queryindex_fanout_command::QueryIndexFanoutCommand;
use crate::common::replies::{IntoRawCtx, reply_with_array, reply_with_slice};
use crate::common::threads::GilSource;
use crate::fanout::{FanoutClientCommand, is_clustered};
use crate::series::index::{
    KeyList, SeriesCursor, SeriesSelection, series_keys_by_selectors,
    series_keys_by_selectors_planned,
};
use crate::series::request_types::MatchFilterOptions;
use valkey_module::ValkeyError::WrongArity;
use valkey_module::{Context, ValkeyResult, ValkeyString, ValkeyValue};

acl_categories!(TS_QUERYINDEX, "ts.queryindex", "read timeseries");
/// TS.QUERYINDEX [FILTER_BY_RANGE [NOT] fromTimestamp toTimestamp] [HASHTAG hash_tag,...] selector [selector ...]
///
/// Returns the keys of the time series matching every supplied selector. Selectors are bare
/// (there is no `FILTER` keyword), so `FILTER_BY_RANGE` and `HASHTAG` must come before the first
/// selector; anything after it is parsed as a selector expression.
///
/// `HASHTAG` scopes only the cluster fan-out — it selects the nodes that are asked, never the
/// keys or labels that match — and is accepted and ignored on a standalone server.
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

    let (options, tags) = parse_query_index_command_args(&mut args)?;

    if is_clustered(ctx) {
        // in cluster mode, we need to send the request to all nodes (or, with HASHTAG,
        // only the nodes owning the given tags)
        let operation = QueryIndexFanoutCommand::new(options, tags);
        return operation.exec(ctx);
    }

    // A large read runs on the analysis lane. Only resolving keys needs the module lock, and
    // that is done in batches; building the reply and freeing the keys then leave the main
    // thread entirely. Keys are replied in index order: the contract leaves order undefined,
    // and sorting cost a large read about 40%.
    let selection = SeriesSelection::Matching(&options.matchers);
    match schedule_series_read(ctx, selection, options.date_range)? {
        SeriesReadPlacement::Background(plan) => run_read_on_analysis_lane(
            ctx,
            "series query",
            move |src| read_keys(src, plan, &options),
            |reply_ctx, keys| reply_with_keys(reply_ctx, &keys),
        ),
        SeriesReadPlacement::Inline(plan) => {
            let keys = read_keys(ctx, plan, &options)?;
            reply_with_keys(ctx, &keys);
            Ok(ValkeyValue::NoReply)
        }
    }
}

/// The read itself: over `plan` when the command planned it, otherwise planned here.
fn read_keys<S: GilSource + ?Sized>(
    src: &S,
    plan: Option<SeriesCursor>,
    options: &MatchFilterOptions,
) -> ValkeyResult<KeyList> {
    match plan {
        Some(cursor) => series_keys_by_selectors_planned(src, cursor),
        None => series_keys_by_selectors(src, &options.matchers, options.date_range),
    }
}

fn reply_with_keys<C: IntoRawCtx + Copy>(ctx: C, keys: &KeyList) {
    reply_with_array(ctx, keys.len());
    for key in keys.iter() {
        reply_with_slice(ctx, key);
    }
}
