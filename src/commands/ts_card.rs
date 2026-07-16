use super::ts_card_fanout_command::CardFanoutCommand;
use crate::commands::command_parser::parse_metadata_command_args;
use crate::fanout::{FanoutClientCommand, is_clustered};
use crate::series::index::count_matched_series;
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

///
/// TS.CARD [FILTER_BY_RANGE fromTimestamp toTimestamp] [FILTER filter...]
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
    let options = parse_metadata_command_args(&mut args, false)?;

    if is_clustered(ctx) {
        if options.matchers.is_empty() {
            return Err(ValkeyError::Str(
                "TS.CARD in cluster mode requires at least one matcher",
            ));
        }
        let operation = CardFanoutCommand::new(options);
        return operation.exec(ctx);
    }

    let counter = count_matched_series(ctx, options.date_range, &options.matchers)?;

    Ok(ValkeyValue::from(counter))
}
