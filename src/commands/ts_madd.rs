use crate::commands::command_parser::{parse_timestamp, parse_value_arg};
use crate::common::time::current_time_millis;
use crate::common::{Sample, Timestamp};
use crate::error_consts;
use crate::series::{
    PerSeriesSamples, SampleAddResult, SeriesGuardMut, get_timeseries_mut,
    multi_series_merge_samples,
};
use ahash::AHashMap;
use smallvec::SmallVec;
use std::ops::DerefMut;
use valkey_module::{
    AclPermissions, Context, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue,
};

#[derive(Debug)]
struct ParsedInput<'a> {
    key: &'a ValkeyString,
    raw_timestamp: &'a ValkeyString,
    raw_value: &'a ValkeyString,
    timestamp: Timestamp,
    value: f64,
    index: usize,
    res: SampleAddResult,
}

#[derive(Default)]
struct SeriesSamples<'a> {
    series: Option<SeriesGuardMut<'a>>,
    err: SampleAddResult,
    samples: Vec<ParsedInput<'a>>,
}

/// TS.MADD key timestamp value [key timestamp value ...]
///
/// The code is a bit involved, but the goal of this implementation is to parallelize the
/// processing of the samples. The idea is to split the input into groups of samples that
/// belong to the same series, and then process each group in parallel. `TimeSeries::merge_samples`
/// allows us to add multiple samples at once per series, while parallelizing across series blocks.
/// Because of that there is extra bookkeeping to do, including mapping results back to the
/// original input and returning results in input order.
#[valkey_module_macros::command({
    name: "ts.madd",
    flags: [Write, DenyOOM],
    summary: "Append new samples to one or more time series.",
    complexity: "O(N) where N is the number of samples added.",
    since: "1.0.0",
    arity: -4,
    key_spec: [{
        flags: [ReadWrite, Update],
        begin_search: Index({ index: 1 }),
        find_keys: Range({ last_key: -1, steps: 3, limit: 0 })
    }]
})]
pub fn ts_madd_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let arg_count = args.len() - 1;

    if arg_count < 3 || !arg_count.is_multiple_of(3) {
        return Err(ValkeyError::WrongArity);
    }

    let sample_count = arg_count / 3;

    let now = current_time_millis();
    let current_ts = ctx.create_string(now.to_string());

    // Parse once, keep inputs already in original order (no regroup+sort later)
    let (mut input_map, all_inputs) = parse_args(ctx, &args[1..], &current_ts)?;

    // Results aligned by original input order
    let results = handle_update(ctx, &mut input_map, &all_inputs, sample_count)?;

    handle_replication(ctx, &all_inputs);

    Ok(ValkeyValue::Array(
        results.into_iter().map(ValkeyValue::from).collect(),
    ))
}

fn handle_update(
    ctx: &Context,
    input_map: &mut AHashMap<&ValkeyString, SeriesSamples>,
    all_inputs: &[ParsedInput],
    sample_count: usize,
) -> ValkeyResult<Vec<SampleAddResult>> {
    // Start with parse-time results; merge-time results will overwrite only successful parses.
    let mut results: Vec<SampleAddResult> = all_inputs.iter().map(|i| i.res).collect();
    results.resize(
        sample_count,
        SampleAddResult::Error(error_consts::INVALID_VALUE),
    );

    let mut per_series_samples: Vec<PerSeriesSamples> = Vec::with_capacity(4);

    for (_key, samples) in input_map.iter_mut() {
        let res = samples.err;
        if !res.is_ok() {
            // Series-level error applies to every sample in that series
            for input in samples.samples.iter() {
                results[input.index] = res;
            }
            continue;
        }

        let Some(series) = &mut samples.series else {
            continue;
        };

        let mut s = PerSeriesSamples::new(series.deref_mut());
        for input in samples.samples.iter() {
            if input.res.is_ok() {
                s.add_sample(Sample::new(input.timestamp, input.value), input.index);
            }
            // parse errors are already in `results` from initialization above
        }

        if !s.is_empty() {
            per_series_samples.push(s);
        }
    }

    // Merge results overwrite the OK entries with final add results
    let merged: SmallVec<[(usize, SampleAddResult); 8]> =
        multi_series_merge_samples(per_series_samples, Some(ctx))?;
    for (index, res) in merged {
        if let Some(slot) = results.get_mut(index) {
            *slot = res;
        }
    }

    Ok(results)
}

fn parse_args<'a>(
    ctx: &'a Context,
    args: &'a [ValkeyString],
    current_ts: &'a ValkeyString,
) -> ValkeyResult<(
    AHashMap<&'a ValkeyString, SeriesSamples<'a>>,
    Vec<ParsedInput<'a>>,
)> {
    let sample_count = args.len() / 3;

    let mut input_map: AHashMap<&ValkeyString, SeriesSamples> =
        AHashMap::with_capacity(sample_count);
    let mut all_inputs: Vec<ParsedInput<'a>> = Vec::with_capacity(sample_count);

    for (sample_index, chunk) in args.chunks_exact(3).enumerate() {
        let key = &chunk[0];

        let raw_timestamp_in = &chunk[1];
        let raw_value = &chunk[2];

        // Normalize replication timestamp first ("*" becomes the concrete current timestamp)
        let (raw_timestamp, timestamp_str) = {
            let s = raw_timestamp_in.try_as_str()?;
            if s == "*" {
                (current_ts, "*")
            } else {
                (raw_timestamp_in, s)
            }
        };

        let series_samples = input_map.entry(key).or_default();

        // Resolve per-series guard once (first time we see a key); cache series-level error.
        if series_samples.samples.is_empty() {
            series_samples.err =
                match get_timeseries_mut(ctx, key, false, Some(AclPermissions::UPDATE)) {
                    Ok(Some(guard)) => {
                        series_samples.series = Some(guard);
                        SampleAddResult::Ok(Sample::default())
                    }
                    // Unlike TS.ADD, TS.MADD does not create the series: a missing
                    // key is a per-item error and the keyspace is left alone
                    // (RedisTimeSeries parity — a mistyped key in a batch must not
                    // silently materialize a series).
                    Ok(None) => SampleAddResult::Error(error_consts::INVALID_TIMESERIES_KEY),
                    Err(ValkeyError::WrongType) => {
                        SampleAddResult::Error(error_consts::INVALID_TIMESERIES_KEY)
                    }
                    Err(ValkeyError::Str(err)) => SampleAddResult::Error(err),
                    Err(_) => SampleAddResult::Error(error_consts::PERMISSION_DENIED),
                };
        }

        // Parse timestamp/value only if the series is usable.
        let mut res = series_samples.err;
        let (timestamp, value) = if !res.is_ok() {
            (0, 0.0)
        } else {
            let ts = match parse_timestamp(timestamp_str) {
                Ok(ts) => ts,
                Err(ValkeyError::Str(msg)) => {
                    res = SampleAddResult::Error(msg);
                    0
                }
                Err(_) => {
                    res = SampleAddResult::Error(error_consts::INVALID_TIMESTAMP);
                    0
                }
            };

            let v = match parse_value_arg(raw_value) {
                Ok(v) => v,
                Err(_) => {
                    res = SampleAddResult::Error(error_consts::INVALID_VALUE);
                    0.0
                }
            };

            (ts, v)
        };

        let input = ParsedInput {
            key,
            raw_timestamp,
            raw_value,
            timestamp,
            value,
            index: sample_index,
            res,
        };

        let second = ParsedInput {
            key,
            raw_timestamp,
            raw_value,
            timestamp,
            value,
            index: sample_index,
            res,
        };

        // Keep both collections in input order (same struct, duplicated storage).
        series_samples.samples.push(input);
        all_inputs.push(second);
    }

    Ok((input_map, all_inputs))
}

fn handle_replication(ctx: &Context, inputs: &[ParsedInput]) {
    let mut replication_args: SmallVec<[_; 24]> = SmallVec::new();
    for input in inputs.iter() {
        if input.res.is_ok() {
            replication_args.push(input.key);
            replication_args.push(input.raw_timestamp);
            replication_args.push(input.raw_value);
        }
    }

    if !replication_args.is_empty() {
        ctx.replicate("TS.MADD", &*replication_args);
        for key in replication_args.into_iter().step_by(3) {
            ctx.notify_keyspace_event(NotifyEvent::MODULE, "ts.add", key);
        }
    }
}
