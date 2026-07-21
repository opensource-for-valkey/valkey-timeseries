use crate::analysis::forecasting::infer_frequency_from_samples;
use crate::commands::command_parser::{
    parse_duration_arg, parse_store_clause, parse_timestamp_range,
};
use crate::commands::{CommandArgIterator, parse_timestamp, parse_value_arg};
use crate::common::replies::reply_with_samples;
use crate::common::{Sample, Timestamp};
use crate::error_consts;
use crate::series::{create_or_update_series_with_samples, get_timeseries_mut};
use std::collections::BTreeSet;
use std::time::Duration;
use valkey_module::{
    AclPermissions, Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue,
};

/// ```text
/// TS.FILLGAPS key startTimestamp endTimestamp
///   [VALUE value]
///   [FREQUENCY duration|"auto"]
///   [ALIGN alignment_timestamp|start|-]
///   [STORE destinationKey
///     [MERGE]
///     [RETENTION retentionPeriod]
///     [ENCODING <pco|gorilla|uncompressed|compressed>]
///     [CHUNK_SIZE chunkSize]
///     [DUPLICATE_POLICY duplicatePolicy]
///     [SIGNIFICANT_DIGITS significantDigits | DECIMAL_DIGITS decimalDigits]
///     [METRIC metric]
///     [IGNORE ignoreMaxTimediff ignoreMaxValDiff]
///   ]
///```
/// Fills missing timestamps in the time series with a fill value (default is NaN)
/// between startTimestamp and endTimestamp (inclusive).
///
/// If FREQUENCY is not specified, the frequency is inferred from the existing data.
///
/// If ALIGN is specified, timestamps are snapped to a frequency grid anchored at the
/// given alignment reference. Use `ALIGN 0` to align to epoch, `ALIGN <timestamp>` for
/// a custom reference, or `ALIGN start` (or `-`) to align to startTimestamp.
/// Only timestamps within [startTimestamp, endTimestamp] are filled.
///
/// The source series itself is never modified. Without STORE, the filled gap
/// samples are returned as an array of [timestamp, value] pairs and are not
/// persisted anywhere. With STORE, the filled gap samples are written to the
/// destination key and the number of samples written is returned.
#[valkey_module_macros::command({
    name: "TS.FILLGAPS",
    flags: [Write, DenyOOM],
    summary: "Fill missing timestamps in a time series over a range.",
    complexity: "O(N) where N is the number of timestamps in the range.",
    since: "1.0.0",
    arity: -4,
    key_spec: [
        {
            flags: [ReadOnly, Access],
            begin_search: Index({ index: 1 }),
            find_keys: Range({ last_key: 0, steps: 1, limit: 0 })
        },
        {
            notes: "Optional destination series written by the STORE clause.",
            flags: [ReadWrite, Update],
            begin_search: Keyword({ keyword: "STORE", startfrom: 1 }),
            find_keys: Range({ last_key: 0, steps: 1, limit: 0 })
        }
    ]
})]
pub fn ts_fillgaps_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 4 {
        return Err(ValkeyError::WrongArity);
    }

    let mut args = args.into_iter().skip(1).peekable();

    let key = args.next_arg()?;
    let date_range = parse_timestamp_range(&mut args)?;
    // Get the series (must exist)
    let series = get_timeseries_mut(ctx, &key, true, Some(AclPermissions::UPDATE))?.unwrap();

    let (start_ts, end_ts) = date_range.get_series_range(&series, None, false);

    // Get existing samples in the range
    let existing_samples = series.get_range(start_ts, end_ts);

    let mut frequency: Option<Duration> = None;
    let mut align_timestamp: Option<Timestamp> = None;
    let mut fill_value = f64::NAN;
    let mut destination = None;

    // Parse optional arguments
    while let Some(arg) = args.next() {
        hashify::fnc_map_ignore_case!(
            arg.as_slice(),
            "FREQUENCY" => {
                let freq_arg = args.next_arg()?;
                if freq_arg.eq_ignore_ascii_case(b"auto") {
                    // Infer frequency from existing samples
                    frequency = Some(infer_frequency_from_samples(&existing_samples)?);
                } else {
                    frequency = Some(parse_duration_arg(&freq_arg)?);
                }
            },
            "ALIGN" => {
                align_timestamp = Some(parse_align(&mut args, start_ts)?);
            },
            "VALUE" => {
                let value_arg = args.next_arg()?;
                fill_value = parse_value_arg(&value_arg)?;
            },
            "STORE" => {
                destination = Some(parse_store_clause(&mut args)?);
            },
            _ => {
                return Err(ValkeyError::Str(error_consts::INVALID_ARGUMENT));
            }
        );
    }

    // Build a set of existing timestamps for O(1) lookup
    let existing_timestamps: BTreeSet<Timestamp> =
        existing_samples.iter().map(|s| s.timestamp).collect();

    // Determine frequency
    let frequency = match frequency {
        Some(dur) => dur,
        None => {
            // Infer frequency from existing samples
            infer_frequency_from_samples(&existing_samples)
        }?,
    };

    if frequency.is_zero() {
        return Err(ValkeyError::String(
            "TSDB: frequency must be positive".to_string(),
        ));
    }

    // Generate expected timestamps and find gaps
    let mut gap_samples = Vec::new();
    let aligned_start = calc_range_start(start_ts, align_timestamp, frequency);

    let freq_ms = frequency.as_millis() as i64;
    let mut current_ts = aligned_start;
    while current_ts <= end_ts {
        if current_ts >= start_ts && !existing_timestamps.contains(&current_ts) {
            gap_samples.push(Sample::new(current_ts, fill_value));
        }

        match current_ts.checked_add(freq_ms) {
            Some(next) => current_ts = next,
            None => break,
        }
    }

    let gaps_filled = gap_samples.len();

    if let Some(dest) = destination {
        if gaps_filled == 0 {
            return Ok(ValkeyValue::from(0_i64));
        }
        let written = create_or_update_series_with_samples(
            ctx,
            &dest.key,
            Some(dest.options),
            dest.write_mode,
            &gap_samples,
            None,
        )?;
        return Ok(ValkeyValue::from(written));
    }

    // No STORE clause: the filled gaps are returned to the caller only; the
    // source series is left untouched.
    reply_with_samples(ctx, gap_samples.iter().cloned());

    Ok(ValkeyValue::NoReply)
}

fn parse_align(args: &mut CommandArgIterator, start_ts: Timestamp) -> ValkeyResult<Timestamp> {
    // ALIGN token already seen
    let alignment_str = args.next_str()?.to_lowercase();
    // accept "start", "-", or a timestamp as alignment options
    if alignment_str == "start" || alignment_str == "-" {
        return Ok(start_ts);
    }
    parse_timestamp(&alignment_str)
        .map_err(|_| ValkeyError::Str("TSDB: invalid ALIGN timestamp value"))
}

fn calc_range_start(
    start_ts: Timestamp,
    align_timestamp: Option<Timestamp>,
    freq: Duration,
) -> Timestamp {
    let align_ts = match align_timestamp {
        None => return start_ts,
        Some(ts) => ts,
    };
    let diff = start_ts - align_ts;
    let delta = freq.as_millis() as i64;
    (start_ts - ((diff % delta + delta) % delta)).max(0)
}
