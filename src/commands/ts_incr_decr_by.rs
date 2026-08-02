use crate::commands::CommandArgToken;
use crate::commands::command_parser::{parse_timestamp, parse_value_arg};
use crate::commands::ts_create::parse_series_options;
use crate::common::block_on_keys::signal_timeseries_ready;
use crate::common::{Sample, Timestamp};
use crate::error_consts;
use crate::series::{SampleAddResult, TimeSeries, create_and_store_series, get_timeseries_mut};
use valkey_module::{
    AclPermissions, Context, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue,
};

#[valkey_module_macros::command({
    name: "ts.incrby",
    flags: [Write, DenyOOM],
    summary: "Increase the value of the last sample, creating the series if needed.",
    complexity: "O(1)",
    since: "1.0.0",
    arity: -3,
    key_spec: [{
        flags: [ReadWrite, Update],
        begin_search: Index({ index: 1 }),
        find_keys: Range({ last_key: 0, steps: 1, limit: 0 })
    }]
})]
pub fn ts_incrby_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    incr_decr(ctx, args, true)
}

#[valkey_module_macros::command({
    name: "ts.decrby",
    flags: [Write, DenyOOM],
    summary: "Decrease the value of the last sample, creating the series if needed.",
    complexity: "O(1)",
    since: "1.0.0",
    arity: -3,
    key_spec: [{
        flags: [ReadWrite, Update],
        begin_search: Index({ index: 1 }),
        find_keys: Range({ last_key: 0, steps: 1, limit: 0 })
    }]
})]
pub fn ts_decrby_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    incr_decr(ctx, args, false)
}

fn incr_decr(ctx: &Context, args: Vec<ValkeyString>, is_increment: bool) -> ValkeyResult {
    if args.len() < 3 {
        return Err(ValkeyError::WrongArity);
    }

    let mut args = args;
    // RTS reports every unusable increment operand — unparseable, empty, NaN —
    // with one message, distinct from TS.ADD's "invalid value".
    let delta = parse_value_arg(&args[2])
        .map_err(|_| ValkeyError::Str(error_consts::INVALID_INCREMENT_VALUE))?;
    let timestamp = handle_parse_timestamp(&mut args)?;
    let key_name = &args[1];

    if let Some(mut series) = get_timeseries_mut(
        ctx,
        key_name,
        false,
        Some(AclPermissions::UPDATE | AclPermissions::ACCESS),
    )? {
        handle_update(ctx, &mut series, key_name, timestamp, delta, is_increment)
    } else {
        create_series_and_update(ctx, args, timestamp, delta, is_increment)
    }
}

fn create_series_and_update(
    ctx: &Context,
    mut args: Vec<ValkeyString>,
    timestamp: Option<Timestamp>,
    delta: f64,
    is_increment: bool,
) -> ValkeyResult {
    let key_name = args.remove(1);
    const INVALID_ARGS: &[CommandArgToken] = &[CommandArgToken::OnDuplicate];

    let options = parse_series_options(args, 2, INVALID_ARGS)?;
    // Auto-create: no ts.create event (RTS parity) and no replication from
    // the create helper — this command replicates itself (a second
    // propagation would double the increment on replicas).
    let mut series = create_and_store_series(ctx, &key_name, options, false, true)?;

    handle_update(ctx, &mut series, &key_name, timestamp, delta, is_increment)
}

fn handle_parse_timestamp(args: &mut Vec<ValkeyString>) -> ValkeyResult<Option<Timestamp>> {
    if let Some(index) = args
        .iter()
        .position(|x| x.eq_ignore_ascii_case(b"timestamp"))
    {
        return if index < args.len() - 1 {
            args.remove(index);
            let timestamp_str = args.remove(index).to_string_lossy();
            let value = parse_timestamp(&timestamp_str)?;
            Ok(Some(value))
        } else {
            Err(ValkeyError::Str("TSDB: missing timestamp value"))
        };
    }
    Ok(None)
}

fn handle_update(
    ctx: &Context,
    series: &mut TimeSeries,
    key_name: &ValkeyString,
    timestamp: Option<Timestamp>,
    delta: f64,
    is_increment: bool,
) -> ValkeyResult {
    let delta = if !is_increment { -delta } else { delta };

    // Captured before the write: an increment at exactly the last timestamp updates the
    // existing sample in place, which compaction must treat as an upsert rather than a
    // fresh append (see `run_compaction_for_increment`).
    let prev_last_ts = series.last_sample.map(|s| s.timestamp);
    // An increment at the last timestamp updates in place and adds nothing readable, so only a
    // genuine count increase wakes blocked `TS.READ` readers.
    let samples_before = series.total_samples;

    let result = series.increment_sample_value(timestamp, delta)?;
    match result {
        SampleAddResult::Ok(added) => {
            // An increment is a write like any other and must drive the series'
            // compaction rules; without this a counter maintained by
            // TS.INCRBY/TS.DECRBY never reaches its downstream series.
            run_compaction_for_increment(ctx, series, key_name, added, prev_last_ts)?;
            if series.total_samples > samples_before {
                signal_timeseries_ready(ctx, key_name);
            }
            replicate_and_notify(ctx, key_name, is_increment, added.timestamp)
        }
        SampleAddResult::Ignored(_ts) => {
            let last_ts = series.last_timestamp();
            Ok(ValkeyValue::Integer(last_ts))
        }
        SampleAddResult::Duplicate => Err(ValkeyError::Str(error_consts::DUPLICATE_SAMPLE_BLOCKED)),
        SampleAddResult::Error(err) => Err(ValkeyError::Str(err)),
        _ => {
            unreachable!("BUG: invalid return value from TimeSeries::add() in TS.INCRBY/TS.DECRBY")
        }
    }
}

/// Drive the series' compaction rules after a successful increment.
///
/// TS.INCRBY/TS.DECRBY reject a timestamp *older* than the last sample, but not one equal
/// to it: `TS.INCRBY key <d> TIMESTAMP <last_ts>` updates the existing sample in place. That
/// is an upsert, so the affected bucket must be recalculated from the source instead of the
/// new value being streamed into the open bucket as an additional sample — otherwise the old
/// and new values are both aggregated (e.g. `TS.ADD k 0 0` then `TS.INCRBY k 1 TIMESTAMP 0`
/// gave an `avg` rollup of 0.5 instead of 1). Mirrors the is_upsert split in TS.ADD.
fn run_compaction_for_increment(
    ctx: &Context,
    series: &mut TimeSeries,
    key_name: &ValkeyString,
    added: Sample,
    prev_last_ts: Option<Timestamp>,
) -> ValkeyResult<()> {
    if series.rules.is_empty() {
        return Ok(());
    }
    let is_upsert = prev_last_ts.is_some_and(|last_ts| added.timestamp <= last_ts);
    let result = if is_upsert {
        series.upsert_compaction(ctx, added)
    } else {
        let sample = series.last_sample.unwrap_or(added);
        series.run_compaction(ctx, sample)
    };
    result.map_err(|err| {
        ValkeyError::String(format!(
            "TSDB: error running compaction for key '{key_name}': {err}"
        ))
    })
}

fn replicate_and_notify(
    ctx: &Context,
    key_name: &ValkeyString,
    is_increment: bool,
    ts: Timestamp,
) -> ValkeyResult {
    let event = if is_increment {
        "ts.incrby"
    } else {
        "ts.decrby"
    };
    ctx.replicate_verbatim();
    ctx.notify_keyspace_event(NotifyEvent::MODULE, event, key_name);
    Ok(ValkeyValue::Integer(ts))
}
