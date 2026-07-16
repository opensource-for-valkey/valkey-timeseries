use crate::commands::fanout_codec::LabelSearchType;
use crate::commands::label_search_utils::run_label_search;
use valkey_module::ValkeyError::WrongArity;
use valkey_module::{Context, ValkeyResult, ValkeyString};

// see https://github.com/tcp13equals2/proposals/blob/c56fb4b25f1151de148f58f0a51799c339185922/proposals/0074-new-labels-values-api.md

/// TS.LABELVALUES label
/// [SEARCH term [term...]]
/// [FUZZY_THRESHOLD 0.0..1.0]
/// [FUZZY_ALGORITHM jarowinkler|subsequence]
/// [IGNORE_CASE]
/// [INCLUDE_METADATA]
/// [SORTBY <value|score|cardinality> [ASC|DESC]]
/// [FILTER_BY_RANGE [NOT] fromTimestamp toTimestamp]
/// [LIMIT limit]
/// [FILTER seriesMatcher...]
#[valkey_module_macros::command({
    name: "ts.labelvalues",
    flags: [ReadOnly],
    summary: "Return the values of a label across time series, optionally filtered.",
    complexity: "O(N) where N is the number of values for the label.",
    since: "1.0.0",
    arity: -2,
    key_spec: []
})]
pub fn ts_labelvalues_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 2 {
        return Err(WrongArity);
    }

    run_label_search(ctx, args, LabelSearchType::Value)
}
