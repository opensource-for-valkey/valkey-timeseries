use crate::commands::analysis_runner::{
    SeriesReadPlacement, run_read_on_analysis_lane, schedule_series_read,
};
use crate::commands::command_parser::{QueryLabelsOptions, parse_query_labels_command_args};
use crate::commands::ts_querylabels_fanout_command::QueryLabelsFanoutCommand;
use crate::common::replies::reply_with_string_set;
use crate::common::threads::GilSource;
use crate::fanout::{FanoutClientCommand, is_clustered};
use crate::series::index::{SeriesCursor, query_labels_distinct_planned, query_labels_selection};
use std::collections::BTreeSet;
use valkey_module::ValkeyError::WrongArity;
use valkey_module::{Context, ValkeyResult, ValkeyString, ValkeyValue};

acl_categories!(TS_QUERYLABELS, "ts.querylabels", "read timeseries");
/// TS.QUERYLABELS <LABELS | VALUES label> [HASHTAG hash_tag,...] [FILTER filterExpr [filterExpr ...]]
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

    // A large read runs on the analysis lane, which gives the module lock back between
    // batches, so the main thread keeps serving other clients. The lane reads through the
    // caller's ACL user, so unreadable series are omitted per series exactly as inline.
    match schedule_series_read(ctx, query_labels_selection(&options.matchers), None)? {
        SeriesReadPlacement::Background(plan) => run_read_on_analysis_lane(
            ctx,
            "label query",
            move |src| read_labels(src, plan, &options),
            |reply_ctx, values| {
                reply_with_string_set(reply_ctx, &values);
            },
        ),
        SeriesReadPlacement::Inline(plan) => {
            let values = read_labels(ctx, plan, &options)?;
            reply_with_string_set(ctx, &values);
            Ok(ValkeyValue::NoReply)
        }
    }
}

/// The read itself: over `plan` when the command planned it, otherwise planned here.
fn read_labels<S: GilSource + ?Sized>(
    src: &S,
    plan: Option<SeriesCursor>,
    options: &QueryLabelsOptions,
) -> ValkeyResult<BTreeSet<String>> {
    query_labels_distinct_planned(src, plan, &options.matchers, options.label.as_deref())
}
