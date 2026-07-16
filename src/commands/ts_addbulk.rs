use crate::commands::ts_create::parse_series_options;
use crate::common::Sample;
use crate::common::replies::{reply_with_array, reply_with_integer};
use crate::series::{
    IngestedSamples, TimeSeries, bulk_insert_samples, create_and_store_series, get_timeseries_mut,
};
use valkey_module::{AclPermissions, Context, ValkeyResult, ValkeyString, ValkeyValue};

///
/// TS.ADDBULK key data
///     [RETENTION duration]
///     [DUPLICATE_POLICY policy]
///     [ON_DUPLICATE policy_ovr]
///     [ENCODING <COMPRESSED|UNCOMPRESSED>]
///     [CHUNK_SIZE chunkSize]
///     [METRIC metric | LABELS labelName labelValue ...]
///     [IGNORE ignoreMaxTimediff ignoreMaxValDiff]
///     [SIGNIFICANT_DIGITS significantDigits | DECIMAL_DIGITS decimalDigits]
///
#[valkey_module_macros::command({
    name: "ts.addbulk",
    flags: [Write, DenyOOM],
    summary: "Append multiple samples supplied as a JSON payload to a time series.",
    complexity: "O(N) where N is the number of samples in the payload.",
    since: "1.0.0",
    arity: -3,
    key_spec: [{
        flags: [ReadWrite, Update],
        begin_search: Index({ index: 1 }),
        find_keys: Range({ last_key: 0, steps: 1, limit: 0 })
    }]
})]
pub fn ts_addbulk_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 3 {
        return Err(valkey_module::ValkeyError::WrongArity);
    }

    let key = args[1].clone();
    // I don't like the copying here, but we need a mutable buffer
    let samples = {
        let data = args[2].as_slice();
        let mut buf = data.to_vec();
        let sample_data = IngestedSamples::from_json_lines(&mut buf)?;
        sample_data.samples
    };

    let options = parse_series_options(args, 4, &[])?;

    if let Some(mut guard) = get_timeseries_mut(ctx, &key, false, Some(AclPermissions::UPDATE))? {
        return process_series(ctx, &mut guard, samples);
    }

    // notify=false: auto-create emits no ts.create event, consistent with the
    // TS.ADD family (and RTS's behavior for auto-creating writes).
    let mut series = create_and_store_series(ctx, &key, options, false, true)?;
    process_series(ctx, &mut series, samples)
}

#[inline]
fn process_series(ctx: &Context, series: &mut TimeSeries, samples: Vec<Sample>) -> ValkeyResult {
    match handle_ingest(ctx, series, samples) {
        Ok(val) => {
            ctx.replicate_verbatim();
            Ok(val)
        }
        Err(err) => Err(err),
    }
}

fn handle_ingest(ctx: &Context, series: &mut TimeSeries, samples: Vec<Sample>) -> ValkeyResult {
    let sample_count = samples.len();
    let duplicate_policy = series.sample_duplicates.resolve_policy(None);

    let results = bulk_insert_samples(ctx, series, &samples, Some(duplicate_policy));

    let success_count = results.iter().filter(|res| res.is_ok()).count();

    reply_with_array(ctx, 2);
    reply_with_integer(ctx, success_count as i64);
    reply_with_integer(ctx, sample_count as i64);

    Ok(ValkeyValue::NoReply)
}
