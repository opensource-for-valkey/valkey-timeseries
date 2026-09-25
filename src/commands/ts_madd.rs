use crate::commands::command_parser::{parse_timestamp, parse_value_arg};
use crate::common::acl::KeyAccess;
use crate::common::block_on_keys::signal_timeseries_ready;
use crate::common::context::notify_keyspace_event;
use crate::common::replies::{reply_error_string, reply_with_array, reply_with_integer};
use crate::common::time::current_time_millis;
use crate::common::{Sample, Timestamp};
use crate::error_consts;
use crate::series::{
    PerSeriesSamples, SampleAddResult, SeriesGuardMut, multi_series_merge_samples,
    try_get_timeseries_mut,
};
use ahash::AHashMap;
use smallvec::SmallVec;
use std::ops::DerefMut;
use valkey_module::{
    AclPermissions, Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue,
};

#[derive(Debug)]
struct ParsedInput<'a> {
    key: &'a ValkeyString,
    raw_timestamp: &'a ValkeyString,
    raw_value: &'a ValkeyString,
    timestamp: Timestamp,
    value: f64,
    res: SampleAddResult,
}

#[derive(Default)]
struct SeriesSamples<'a> {
    series: Option<SeriesGuardMut<'a>>,
    err: SampleAddResult,
    /// Indices into the command's `all_inputs`, in argument order.
    samples: SmallVec<[usize; 4]>,
}

/// Everything parsed from the arguments, in argument order, plus the per-key
/// groups that reference it by index.
struct ParsedCommand<'a> {
    input_map: AHashMap<&'a ValkeyString, SeriesSamples<'a>>,
    all_inputs: Vec<ParsedInput<'a>>,
    /// True when some `*` timestamp was resolved to a concrete time: the
    /// replica must then see the resolved arguments, not the original ones.
    rewrote_args: bool,
}

acl_categories!(TS_MADD, "ts.madd", "write timeseries");
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
    let ParsedCommand {
        mut input_map,
        all_inputs,
        rewrote_args,
    } = parse_args(ctx, &args[1..], &current_ts)?;

    // Results aligned by original input order
    let results = handle_update(ctx, &mut input_map, &all_inputs, sample_count)?;

    handle_replication(ctx, &all_inputs, rewrote_args);

    // Reply straight from the results rather than through a `ValkeyValue` tree: one array
    // header and one integer (or error) per item, no per-item enum and no `Vec` of them.
    reply_with_array(ctx, results.len());
    for res in results {
        match res {
            SampleAddResult::Ok(sample) => reply_with_integer(ctx, sample.timestamp),
            SampleAddResult::Ignored(ts) => reply_with_integer(ctx, ts),
            // Per-item failures must be real RESP error replies inside the array (clients
            // type-check the elements), matching RTS.
            SampleAddResult::Duplicate => reply_error_string(ctx, error_consts::DUPLICATE_SAMPLE),
            SampleAddResult::TooOld => reply_error_string(ctx, error_consts::SAMPLE_TOO_OLD),
            SampleAddResult::Error(e) => reply_error_string(ctx, e),
        };
    }
    Ok(ValkeyValue::NoReply)
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
    // Sample counts before the merge, so blocked `TS.READ` readers are woken only for series
    // that actually gained a sample. Collected here because the mutable borrows taken by
    // `PerSeriesSamples` below run until the merge returns.
    let mut counts_before: SmallVec<[(&ValkeyString, usize); 8]> = SmallVec::new();

    for (key, samples) in input_map.iter_mut() {
        let res = samples.err;
        if !res.is_ok() {
            // Series-level error applies to every sample in that series
            for &idx in samples.samples.iter() {
                results[idx] = res;
            }
            continue;
        }

        let Some(series) = &mut samples.series else {
            continue;
        };

        counts_before.push((*key, series.total_samples));

        let mut s = PerSeriesSamples::new(series.deref_mut());
        for &idx in samples.samples.iter() {
            let input = &all_inputs[idx];
            if input.res.is_ok() {
                s.add_sample(Sample::new(input.timestamp, input.value), idx);
            }
            // parse errors are already in `results` from initialization above
        }

        if !s.is_empty() {
            per_series_samples.push(s);
        }
    }

    // Merge results overwrite the OK entries with final add results
    let merged: SmallVec<[(usize, SampleAddResult); 8]> =
        multi_series_merge_samples(per_series_samples, Some(ctx));
    for (index, res) in merged {
        if let Some(slot) = results.get_mut(index) {
            *slot = res;
        }
    }

    // The merge leaves the retention trim to us so the counts above still compare like for
    // like: a trim can drop more samples than the batch added.
    for (key, count_before) in counts_before {
        if let Some(samples) = input_map.get_mut(key)
            && let Some(series) = &mut samples.series
        {
            if series.total_samples > count_before {
                signal_timeseries_ready(ctx, key);
            }
            series.apply_retention();
        }
    }

    Ok(results)
}

fn parse_args<'a>(
    ctx: &'a Context,
    args: &'a [ValkeyString],
    current_ts: &'a ValkeyString,
) -> ValkeyResult<ParsedCommand<'a>> {
    let sample_count = args.len() / 3;

    let mut input_map: AHashMap<&ValkeyString, SeriesSamples> =
        AHashMap::with_capacity(sample_count);
    let mut all_inputs: Vec<ParsedInput<'a>> = Vec::with_capacity(sample_count);
    let mut rewrote_args = false;
    // The caller's identity is resolved once for the command; each key then
    // costs one permission check (or none when the user may reach every key).
    let access = KeyAccess::new(ctx, AclPermissions::UPDATE);

    for (sample_index, chunk) in args.chunks_exact(3).enumerate() {
        let key = &chunk[0];

        let raw_timestamp_in = &chunk[1];
        let raw_value = &chunk[2];

        // Normalize replication timestamp first ("*" becomes the concrete current timestamp)
        let (raw_timestamp, timestamp_str) = {
            let s = raw_timestamp_in.try_as_str()?;
            if s == "*" {
                rewrote_args = true;
                (current_ts, "*")
            } else {
                (raw_timestamp_in, s)
            }
        };

        let series_samples = input_map.entry(key).or_default();

        // Resolve per-series guard once (first time we see a key); cache series-level error.
        if series_samples.samples.is_empty() {
            series_samples.err = match access
                .check(key)
                .and_then(|()| try_get_timeseries_mut(ctx, key, None))
            {
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

        series_samples.samples.push(sample_index);
        all_inputs.push(ParsedInput {
            key,
            raw_timestamp,
            raw_value,
            timestamp,
            value,
            res,
        });
    }

    Ok(ParsedCommand {
        input_map,
        all_inputs,
        rewrote_args,
    })
}

fn handle_replication(ctx: &Context, inputs: &[ParsedInput], rewrote_args: bool) {
    let ok_count = inputs.iter().filter(|i| i.res.is_ok()).count();
    if ok_count == 0 {
        return;
    }

    if ok_count == inputs.len() && !rewrote_args {
        // Every item was accepted as given: replicate the command as the client
        // sent it instead of re-marshalling every argument.
        ctx.replicate_verbatim();
    } else {
        let mut replication_args: SmallVec<[_; 24]> = SmallVec::new();
        for input in inputs.iter().filter(|i| i.res.is_ok()) {
            replication_args.push(input.key);
            replication_args.push(input.raw_timestamp);
            replication_args.push(input.raw_value);
        }
        ctx.replicate("TS.MADD", &*replication_args);
    }

    for input in inputs.iter().filter(|i| i.res.is_ok()) {
        notify_keyspace_event(ctx, c"ts.add", input.key);
    }
}
