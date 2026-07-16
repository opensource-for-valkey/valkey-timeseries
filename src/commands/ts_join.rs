use crate::commands::command_parser::{parse_join_args, parse_timestamp_range};
use crate::common::replies::{reply_with_array, reply_with_null, reply_with_sample};
use crate::error_consts;
use crate::join::{JoinOptions, JoinResultType, process_join};
use crate::series::get_timeseries;
use joinkit::EitherOrBoth;
use valkey_module::{
    AclPermissions, Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue,
};

/// TS.JOIN key1 key2 fromTimestamp toTimestamp
///   [INNER | FULL | LEFT | RIGHT | ANTI | SEMI | ASOF [PREVIOUS | NEXT | NEAREST] tolerance [ALLOW_EXACT_MATCH [true|false]]]
///   [FILTER_BY_TS ts...]
///   [FILTER_BY_VALUE min max]
///   [COUNT count]
///   [REDUCE op]
///   [AGGREGATION aggregator bucket_duration [ALIGN align] [BUCKETTIMESTAMP timestamp] [EMPTY]]
#[valkey_module_macros::command({
    name: "ts.join",
    flags: [ReadOnly],
    summary: "Join the samples of two time series over a timestamp range.",
    complexity: "O(N+M) where N and M are the number of samples in each series within the range.",
    since: "1.0.0",
    arity: -4,
    key_spec: [{
        flags: [ReadOnly, Access],
        begin_search: Index({ index: 1 }),
        find_keys: Range({ last_key: 1, steps: 1, limit: 0 })
    }]
})]
pub fn ts_join_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();

    let left_key = args.next_arg()?;
    let right_key = args.next_arg()?;
    let date_range = parse_timestamp_range(&mut args)?;

    let mut options = JoinOptions {
        date_range,
        ..Default::default()
    };

    parse_join_args(&mut args, &mut options)?;

    if left_key == right_key {
        return Err(ValkeyError::Str(error_consts::DUPLICATE_JOIN_KEYS));
    }

    // In both cases we pass true for must_exist, meaning that if the series does not exist, we will
    // propagate an error. Because of this, unwrap is safe to use here.
    let left_series = get_timeseries(ctx, &left_key, Some(AclPermissions::ACCESS), true)?.unwrap();
    let right_series =
        get_timeseries(ctx, &right_key, Some(AclPermissions::ACCESS), true)?.unwrap();

    let result = process_join(&left_series, &right_series, &options)?;
    match result {
        JoinResultType::Samples(samples) => {
            reply_with_array(ctx, samples.len());
            for sample in samples.iter() {
                reply_with_sample(ctx, sample);
            }
        }
        JoinResultType::Values(values) => {
            reply_with_array(ctx, values.len());
            for value in values.iter() {
                reply_with_array(ctx, 2);
                match value.0 {
                    EitherOrBoth::Both(ref l, ref r) => {
                        reply_with_sample(ctx, l);
                        reply_with_sample(ctx, r);
                    }
                    EitherOrBoth::Left(ref l) => {
                        reply_with_sample(ctx, l);
                        reply_with_null(ctx);
                    }
                    EitherOrBoth::Right(ref r) => {
                        reply_with_null(ctx);
                        reply_with_sample(ctx, r);
                    }
                }
            }
        }
    }

    Ok(ValkeyValue::NoReply)
}
