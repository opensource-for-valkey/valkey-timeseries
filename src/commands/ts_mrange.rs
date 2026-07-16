use crate::commands::parse_mrange_options;
use crate::commands::ts_mrange_fanout_command::MRangeFanoutCommand;
use crate::commands::utils::{MRangeReplyShape, reply_with_mrange_series_results};
use crate::error_consts;
use crate::fanout::{FanoutClientCommand, is_clustered};
use crate::series::mrange::process_mrange_query;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString};

/// TS.MRANGE fromTimestamp toTimestamp
//   [LATEST]
//   [FILTER_BY_TS ts...]
//   [FILTER_BY_VALUE min max]
//   [WITHLABELS | <SELECTED_LABELS label...>]
//   [COUNT count]
//   [[ALIGN align] AGGREGATION aggregator bucketDuration [CONDITION op value] [BUCKETTIMESTAMP bt] [EMPTY]]
//   FILTER filterExpr...
//   [GROUPBY label REDUCE reducer]
#[valkey_module_macros::command({
    name: "ts.mrange",
    flags: [ReadOnly],
    summary: "Query a range across multiple time series selected by a filter, in forward order.",
    complexity: "O(N*M) where N is the number of matching series and M the number of samples in the range.",
    since: "1.0.0",
    arity: -4,
    key_spec: []
})]
pub fn ts_mrange_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    mrange_internal(ctx, args, false)
}

#[valkey_module_macros::command({
    name: "ts.mrevrange",
    flags: [ReadOnly],
    summary: "Query a range across multiple time series selected by a filter, in reverse order.",
    complexity: "O(N*M) where N is the number of matching series and M the number of samples in the range.",
    since: "1.0.0",
    arity: -4,
    key_spec: []
})]
pub fn ts_mrevrange_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    mrange_internal(ctx, args, true)
}

fn mrange_internal(ctx: &Context, args: Vec<ValkeyString>, reverse: bool) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();
    let mut options = parse_mrange_options(&mut args)?;

    if options.filters.is_empty() {
        return Err(ValkeyError::Str(error_consts::MISSING_FILTER));
    }

    options.is_reverse = reverse;

    args.done()?;

    if is_clustered(ctx) {
        let operation = MRangeFanoutCommand::new(options);
        return operation.exec(ctx);
    }

    let shape = MRangeReplyShape::from_options(&options);
    let result_rows = process_mrange_query(ctx, options, false, None)?;
    reply_with_mrange_series_results(ctx, &result_rows, &shape)
}
