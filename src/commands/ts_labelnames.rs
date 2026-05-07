use crate::commands::fanout_codec::LabelSearchType;
use crate::commands::label_search_utils::run_label_search;
use valkey_module::{Context, ValkeyResult, ValkeyString};
// see https://github.com/tcp13equals2/proposals/blob/c56fb4b25f1151de148f58f0a51799c339185922/proposals/0074-new-labels-values-api.md

/// TS.LABELNAMES
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
    name: "TS.LABELNAMES",
    flags: [ReadOnly],
    summary: "Return label names across time series, optionally filtered.",
    complexity: "O(N) where N is the number of label names in the index.",
    since: "1.0.0",
    arity: -1,
    key_spec: []
})]
pub fn ts_labelnames_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    run_label_search(ctx, args, LabelSearchType::Name)
}
