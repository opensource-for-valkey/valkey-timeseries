use crate::commands::command_parser::{
    CommandArgToken, parse_command_arg_token, parse_timestamp, parse_value_arg,
};
use crate::commands::ts_create::parse_series_options;
use crate::common::block_on_keys::signal_timeseries_ready;
use crate::common::{Sample, Timestamp};
use crate::error_consts;
use crate::series::{
    DuplicatePolicy, SampleAddResult, TimeSeries, create_and_store_series, get_timeseries_mut,
};
use valkey_module::{
    AclPermissions, Context, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue,
};

///
/// TS.ADD key timestamp value
///     [RETENTION duration]
///     [DUPLICATE_POLICY policy]
///     [ON_DUPLICATE policy_ovr]
///     [ENCODING <COMPRESSED|UNCOMPRESSED>]
///     [CHUNK_SIZE chunkSize]
///     [METRIC metric | LABELS labelName labelValue ...]
///     [IGNORE ignoreMaxTimediff ignoreMaxValDiff]
///     [SIGNIFICANT_DIGITS significantDigits | DECIMAL_DIGITS decimalDigits]
///     [LABELS label1=value1 label2=value2 ...]
///
#[valkey_module_macros::command({
    name: "ts.add",
    flags: [Write, DenyOOM],
    summary: "Append a sample to a time series, creating it if it does not exist.",
    complexity: "O(1)",
    since: "1.0.0",
    arity: -4,
    key_spec: [{
        flags: [ReadWrite, Update],
        begin_search: Index({ index: 1 }),
        find_keys: Range({ last_key: 0, steps: 1, limit: 0 })
    }]
})]
pub fn ts_add_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 4 {
        return Err(ValkeyError::WrongArity);
    }

    let timestamp_str = args[2].try_as_str()?;
    let timestamp = parse_timestamp(timestamp_str)?;

    let value = parse_value_arg(&args[3])?;

    // RTS replies "TSDB: the key is not a TSDB key" for the auto-create write
    // commands (TS.ADD/TS.MADD) but keeps the standard WRONGTYPE error for the
    // read/modify commands — match that split (compat finding #7).
    let guard = get_timeseries_mut(ctx, &args[1], false, Some(AclPermissions::UPDATE)).map_err(
        |e| match e {
            ValkeyError::WrongType => ValkeyError::Str(error_consts::INVALID_TIMESERIES_KEY),
            e => e,
        },
    )?;

    if let Some(mut guard) = guard {
        // The series already exists, so the creation options are inert — RTS does
        // not even validate them on this path. ON_DUPLICATE is the exception: it is
        // a per-call override, so it must still be read (and rejected if invalid).
        let on_duplicate = parse_on_duplicate(&args)?;
        return handle_add(
            ctx,
            &mut guard,
            args,
            timestamp,
            timestamp_str,
            value,
            on_duplicate,
        );
    }

    // clones because of replicate_and_notify
    let original_args = args.clone();

    let options = parse_series_options(args, 4, &[])?;
    let on_duplicate = options.on_duplicate;

    let key = &original_args[1];
    // Auto-create: no ts.create event (RTS parity) and no replication from
    // the create helper — this command replicates itself.
    let mut series = create_and_store_series(ctx, key, options, false, true)?;

    handle_add(
        ctx,
        &mut series,
        original_args,
        timestamp,
        timestamp_str,
        value,
        on_duplicate,
    )
}

/// Reads `ON_DUPLICATE <policy>` out of an argument list without interpreting any
/// of the creation options around it.
fn parse_on_duplicate(args: &[ValkeyString]) -> ValkeyResult<Option<DuplicatePolicy>> {
    let mut iter = args.iter().skip(4);
    while let Some(arg) = iter.next() {
        if parse_command_arg_token(arg.as_slice()) != Some(CommandArgToken::OnDuplicate) {
            continue;
        }
        let Some(value) = iter.next() else {
            return Err(ValkeyError::Str(error_consts::MISSING_DUPLICATE_POLICY));
        };
        return Ok(Some(DuplicatePolicy::try_from(value.as_slice())?));
    }
    Ok(None)
}

#[allow(clippy::too_many_arguments)]
fn handle_add(
    ctx: &Context,
    series: &mut TimeSeries,
    args: Vec<ValkeyString>,
    timestamp: Timestamp,
    timestamp_str: &str,
    value: f64,
    on_duplicate: Option<DuplicatePolicy>,
) -> ValkeyResult {
    let mut ignored = false;

    let last_ts = series.last_sample.map(|s| s.timestamp);
    // Compared after the add to decide whether to wake blocked `TS.READ` readers. A count
    // comparison rather than `!is_upsert`: an out-of-order insert *below* the tail is not an
    // append, but it does add a sample that a reader's cursor may already cover. Duplicates,
    // ignored writes, and value-only updates leave the count unchanged and correctly do not
    // signal.
    let samples_before = series.total_samples;

    let (replication_timestamp, ts, value) = match series.add(timestamp, value, on_duplicate) {
        SampleAddResult::Ignored(res_ts) => {
            ignored = true;
            let timestamp = if timestamp_str == "*" {
                Some(res_ts)
            } else {
                None
            };
            (timestamp, res_ts, value)
        }
        SampleAddResult::Ok(added) => {
            let timestamp = if timestamp_str == "*" {
                Some(added.timestamp)
            } else {
                None
            };
            (timestamp, added.timestamp, added.value)
        }
        res => {
            return res.into();
        }
    };

    let is_upsert = if let Some(last_ts) = last_ts {
        ts <= last_ts
    } else {
        false
    };

    // `ignored` is true when the sample is not added because the timestamp or value delta is below
    // the appropriate threshold.
    // If there's not a change, we don't want to run compaction.
    // Question: should we replicate the command in this case?
    if !ignored && !series.rules.is_empty() {
        let sample = Sample::new(ts, value);
        if is_upsert {
            if let Err(res) = series.upsert_compaction(ctx, sample) {
                let msg = format!(
                    "TSDB: error running compaction upsert for key '{}': {}",
                    args[1], res
                );
                return Err(ValkeyError::String(msg));
            }
            // Fall through to replicate_and_notify: an upsert is still a
            // successful TS.ADD — it must replicate and emit `ts.add` like
            // any other add (the early return here previously skipped both,
            // leaving replicas without the sample).
        } else {
            let sample = series.last_sample.unwrap_or(Sample::new(ts, value));
            // If the sample is not an upsert, we run compaction
            series.run_compaction(ctx, sample)?;
        }
    }

    if series.total_samples > samples_before {
        signal_timeseries_ready(ctx, &args[1]);
    }

    replicate_and_notify(ctx, args, replication_timestamp);
    Ok(ValkeyValue::Integer(ts))
}

fn replicate_and_notify(ctx: &Context, args: Vec<ValkeyString>, timestamp: Option<Timestamp>) {
    if let Some(ts) = timestamp {
        // "*" could have a completely different value on a replica, so send the current value instead
        let ts_str = ts.to_string();
        let mut args = args;
        args.remove(0);
        args[1] = ctx.create_string(ts_str.as_bytes());
        let replication_args = args.iter().collect::<Vec<_>>();
        ctx.replicate("TS.ADD", &*replication_args);
        let key = args.swap_remove(0);
        ctx.notify_keyspace_event(NotifyEvent::MODULE, "ts.add", &key);
    } else {
        ctx.replicate_verbatim();
        ctx.notify_keyspace_event(NotifyEvent::MODULE, "ts.add", &args[1]);
    }
}
