use crate::commands::command_parser::{
    CommandArgToken, parse_command_arg_token, parse_timestamp, parse_value_arg,
};
use crate::commands::ts_create::parse_series_options;
use crate::common::block_on_keys::signal_timeseries_ready;
use crate::common::context::notify_keyspace_event;
use crate::common::{Sample, Timestamp};
use crate::error_consts;
use crate::series::{
    DuplicatePolicy, SampleAddResult, TimeSeries, create_and_store_series, try_get_timeseries_mut,
};
use valkey_module::{
    AclPermissions, Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue,
};

acl_categories!(TS_ADD, "ts.add", "write timeseries");
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
    let guard = try_get_timeseries_mut(ctx, &args[1], Some(AclPermissions::UPDATE)).map_err(
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
            false,
        );
    }

    // clones because of replicate_and_notify
    let original_args = args.clone();

    let options = parse_series_options(args, 4, &[])?;
    let on_duplicate = options.on_duplicate;

    let key = &original_args[1];
    // Auto-create: no ts.create event (RTS parity). The create helper replicates the new series
    // as a resolved `TS.CREATE`; this command then replicates itself.
    let mut series = create_and_store_series(ctx, key, options, false, true)?;

    handle_add(
        ctx,
        &mut series,
        original_args,
        timestamp,
        timestamp_str,
        value,
        on_duplicate,
        true,
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
    created: bool,
) -> ValkeyResult {
    let mut ignored = false;

    let last_ts = series.last_sample.map(|s| s.timestamp);
    // `inserted` decides whether to wake blocked `TS.READ` readers. An insert rather than
    // `!is_upsert`: an out-of-order insert *below* the tail is not an append, but it does add a
    // sample that a reader's cursor may already cover. Duplicates, ignored writes, and
    // value-only updates insert nothing and correctly do not signal.
    let (result, inserted) = series.add_reporting_insert(timestamp, value, on_duplicate);
    let (replication_timestamp, ts, value) = match result {
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

    // `ignored` is true when the sample is not added because the timestamp or value delta is below
    // the appropriate threshold.
    // If there's not a change, we don't want to run compaction.
    // Question: should we replicate the command in this case?
    //
    // The sample is already stored, so a compaction failure is logged rather than returned (as
    // TS.MADD and TS.ADDBULK do): an error here would skip replication and leave the replica
    // without a sample the primary keeps. The replica re-runs the same compaction on replay.
    if !ignored
        && !series.rules.is_empty()
        && let Err(err) = series.compact_sample(ctx, Sample::new(ts, value), last_ts)
    {
        let key = &args[1];
        ctx.log_warning(&format!(
            "TSDB: error running compaction for key '{key}': {err}"
        ));
    }

    if inserted {
        signal_timeseries_ready(ctx, &args[1]);
    }

    replicate_and_notify(ctx, &args, replication_timestamp, created);
    Ok(ValkeyValue::Integer(ts))
}

/// Propagates the write. `timestamp` is the resolved timestamp when the client sent `*`, which
/// could resolve differently on a replica.
///
/// A write that `created` its series is propagated bare, as `TS.ADD key timestamp value`: the
/// series has already gone out as a resolved `TS.CREATE`, so the replayed write runs on the
/// existing-key path, which scans the arguments for `ON_DUPLICATE` without stopping at `LABELS`
/// (as the reference does). Verbatim, `LABELS tag ON_DUPLICATE` — a label the create path
/// accepted — would fail there and drop the sample on every replica and AOF replay. Nothing
/// the bare form omits matters: the creation options travel in the `TS.CREATE`, and neither
/// `ON_DUPLICATE` nor `IGNORE` can affect the first sample of an empty series.
fn replicate_and_notify(
    ctx: &Context,
    args: &[ValkeyString],
    timestamp: Option<Timestamp>,
    created: bool,
) {
    let key = &args[1];
    let resolved_ts = timestamp.map(|ts| ctx.create_string(ts.to_string().as_bytes()));
    let ts_arg = resolved_ts.as_ref().unwrap_or(&args[2]);
    if created {
        ctx.replicate("TS.ADD", &[key, ts_arg, &args[3]]);
    } else if resolved_ts.is_some() {
        let mut replication_args = args[1..].iter().collect::<Vec<_>>();
        replication_args[1] = ts_arg;
        ctx.replicate("TS.ADD", &*replication_args);
    } else {
        ctx.replicate_verbatim();
    }
    notify_keyspace_event(ctx, c"ts.add", key);
}
