use crate::commands::analysis_runner::{reserve_analysis_slot, run_reserved_on_analysis_lane};
use crate::commands::command_parser::{parse_join_args, parse_timestamp_range};
use crate::common::context::is_blocking_denied;
use crate::common::replies::{IntoRawCtx, reply_with_array, reply_with_null, reply_with_sample};
use crate::error_consts;
use crate::join::{JoinInputs, JoinOptions, JoinResultType};
use crate::series::get_timeseries;
use itertools::EitherOrBoth;
use valkey_module::{
    AclPermissions, Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue,
};

acl_categories!(TS_JOIN, "ts.join", "read timeseries");
/// TS.JOIN key1 key2 fromTimestamp toTimestamp
///   [INNER | FULL | LEFT | RIGHT | ANTI | SEMI | ASOF [PREVIOUS | NEXT | NEAREST] [tolerance] [ALLOW_EXACT_MATCH [true|false]]]
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
    arity: -5,
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

    // A missing key is an error here, not an empty join.
    let left_series = get_timeseries(ctx, &left_key, Some(AclPermissions::ACCESS))?;
    let right_series = get_timeseries(ctx, &right_key, Some(AclPermissions::ACCESS))?;
    let inputs = JoinInputs::read(&left_series, &right_series, &options)?;

    // A large join answers from the analysis lane, so it does not hold the main thread for the
    // decode: the windows are copied out here, while the series are open, and joined there.
    // A client that cannot block (MULTI, a script, a module call) is answered inline.
    if inputs.estimated_samples() > INLINE_JOIN_SAMPLES && !is_blocking_denied(ctx) {
        // Under COUNT the join usually reads only a prefix of each window, so try it here
        // first, within the inline budget. COUNT does not bound what the join reads (an INNER
        // join with few common timestamps reads both windows to fill it), so a join that
        // needs more falls back to the lane, having spent at most the budget.
        if options.count.is_some()
            && let Some(result) = inputs.join_within(&options, INLINE_JOIN_SAMPLES)
        {
            reply_with_join_result(ctx, &result);
            return Ok(ValkeyValue::NoReply);
        }
        // Reserve the queue place first, so a busy lane refuses the join before the copy.
        let slot = reserve_analysis_slot(ctx, "TS.JOIN")?;
        // Owns copies of both windows: nothing in the job borrows the series.
        let inputs = inputs.into_owned();
        return run_reserved_on_analysis_lane(ctx, slot, move |thread_ctx| {
            reply_with_join_result(thread_ctx, &inputs.join(&options));
        });
    }
    reply_with_join_result(ctx, &inputs.join(&options));
    Ok(ValkeyValue::NoReply)
}

/// Samples a join reads before it moves to the analysis lane. Inline, a join of that size
/// takes about 0.6 ms (`benches/join.rs`, `join/end_to_end`: ~9 ns per input sample); the
/// lane hand-off costs tens of microseconds.
const INLINE_JOIN_SAMPLES: usize = 65_536;

fn reply_with_join_result<C: IntoRawCtx + Copy>(ctx: C, result: &JoinResultType) {
    match result {
        JoinResultType::Samples(samples) => {
            reply_with_array(ctx, samples.len());
            for sample in samples {
                reply_with_sample(ctx, sample);
            }
        }
        JoinResultType::Values(values) => {
            reply_with_array(ctx, values.len());
            for value in values {
                reply_with_array(ctx, 2);
                match value {
                    EitherOrBoth::Both(l, r) => {
                        reply_with_sample(ctx, l);
                        reply_with_sample(ctx, r);
                    }
                    EitherOrBoth::Left(l) => {
                        reply_with_sample(ctx, l);
                        reply_with_null(ctx);
                    }
                    EitherOrBoth::Right(r) => {
                        reply_with_null(ctx);
                        reply_with_sample(ctx, r);
                    }
                }
            }
        }
    }
}
