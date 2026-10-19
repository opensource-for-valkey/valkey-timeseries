use super::ts_card_fanout_command::CardFanoutCommand;
use crate::commands::analysis_runner::{
    SeriesReadPlacement, run_read_on_analysis_lane, schedule_series_read,
};
use crate::commands::command_parser::parse_metadata_command_args;
use crate::common::threads::GilSource;
use crate::fanout::{FanoutClientCommand, is_clustered};
use crate::series::index::{
    SeriesCursor, SeriesSelection, count_matched_series, count_series_by_selectors,
    count_series_by_selectors_planned,
};
use crate::series::request_types::MatchFilterOptions;
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

acl_categories!(TS_CARD, "ts.card", "read timeseries");
///
/// TS.CARD [FILTER_BY_RANGE fromTimestamp toTimestamp] [HASHTAG hash_tag,...] [FILTER filter...]
///
/// returns the number of unique time series that match a certain label set.
#[valkey_module_macros::command({
    name: "ts.card",
    flags: [ReadOnly],
    summary: "Count the time series matching a filter.",
    complexity: "O(N) where N is the number of time series that match the filters.",
    since: "1.0.0",
    arity: -1,
    key_spec: []
})]
pub fn ts_card_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();
    let (options, tags) = parse_metadata_command_args(&mut args, false)?;

    if is_clustered(ctx) {
        if options.matchers.is_empty() {
            return Err(ValkeyError::Str(
                "TS.CARD in cluster mode requires at least one matcher",
            ));
        }
        let operation = CardFanoutCommand::new(options, tags);
        return operation.exec(ctx);
    }

    // Index-only counts do not open series, so they stay inline, as does an invalid
    // range-only request so it keeps its ordinary error.
    if options.date_range.is_none() || options.matchers.is_empty() {
        return count_matched_series(ctx, options.date_range, &options.matchers)
            .map(ValkeyValue::from);
    }

    let selection = SeriesSelection::Matching(&options.matchers);
    match schedule_series_read(ctx, selection, options.date_range)? {
        SeriesReadPlacement::Background(plan) => run_read_on_analysis_lane(
            ctx,
            "series count",
            move |src| read_count(src, plan, &options),
            |reply_ctx, count| {
                reply_ctx.reply_with_integer(count as i64);
            },
        ),
        SeriesReadPlacement::Inline(plan) => read_count(ctx, plan, &options).map(ValkeyValue::from),
    }
}

/// The batched count itself: over `plan` when the command planned it, otherwise planned here.
fn read_count<S: GilSource + ?Sized>(
    src: &S,
    plan: Option<SeriesCursor>,
    options: &MatchFilterOptions,
) -> ValkeyResult<usize> {
    match plan {
        Some(cursor) => count_series_by_selectors_planned(src, cursor),
        None => count_series_by_selectors(src, &options.matchers, options.date_range),
    }
}
