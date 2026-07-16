use crate::commands::command_parser::parse_range_options;
use crate::common::replies::{reply_with_multi_samples, reply_with_samples};
use crate::iterators::{TimeSeriesRangeIterator, TimeSeriesRangeRowIterator};
use crate::series::get_timeseries;
use valkey_module::{
    AclPermissions, Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue,
};

/// TS.RANGE key fromTimestamp toTimestamp
//   [LATEST]
//   [FILTER_BY_TS ts...]
//   [FILTER_BY_VALUE min max]
//   [COUNT count]
//   [[ALIGN align] AGGREGATION aggregator bucketDuration [CONDITION op value] [BUCKETTIMESTAMP bt] [EMPTY]]
#[valkey_module_macros::command({
    name: "ts.range",
    flags: [ReadOnly],
    summary: "Query a range of samples from a time series in forward order.",
    complexity: "O(N) where N is the number of samples in the requested range.",
    since: "1.0.0",
    arity: -4,
    key_spec: [{
        flags: [ReadOnly, Access],
        begin_search: Index({ index: 1 }),
        find_keys: Range({ last_key: 0, steps: 1, limit: 0 })
    }]
})]
pub fn ts_range_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    range_internal(ctx, args, false)
}

/// TS.REVRANGE key fromTimestamp toTimestamp
//   [LATEST]
//   [FILTER_BY_TS ts...]
//   [FILTER_BY_VALUE min max]
//   [COUNT count]
//   [[ALIGN align] AGGREGATION aggregator bucket_duration [CONDITION op value] [BUCKETTIMESTAMP bt] [EMPTY]]
#[valkey_module_macros::command({
    name: "ts.revrange",
    flags: [ReadOnly],
    summary: "Query a range of samples from a time series in reverse order.",
    complexity: "O(N) where N is the number of samples in the requested range.",
    since: "1.0.0",
    arity: -4,
    key_spec: [{
        flags: [ReadOnly, Access],
        begin_search: Index({ index: 1 }),
        find_keys: Range({ last_key: 0, steps: 1, limit: 0 })
    }]
})]
pub fn ts_revrange_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    range_internal(ctx, args, true)
}

fn range_internal(ctx: &Context, args: Vec<ValkeyString>, is_reverse: bool) -> ValkeyResult {
    if args.len() < 4 {
        return Err(ValkeyError::WrongArity);
    }
    let mut args = args.into_iter().skip(1).peekable();

    let key = args.next_arg()?;
    let options = parse_range_options(&mut args)?;

    args.done()?;

    // In both cases we pass true for must_exist, meaning that if the series does not exist, we will
    // propagate an error. Because of this, unwrap is safe to use here.
    let series = get_timeseries(ctx, &key, Some(AclPermissions::ACCESS), true)?.unwrap();

    if options.aggregation.as_ref().is_some_and(|a| a.is_multi()) {
        let iter = TimeSeriesRangeRowIterator::new(Some(ctx), &series, &options, is_reverse);
        reply_with_multi_samples(ctx, iter);
    } else {
        let iter = TimeSeriesRangeIterator::new(Some(ctx), &series, &options, is_reverse);
        reply_with_samples(ctx, iter);
    }
    Ok(ValkeyValue::NoReply)
}
