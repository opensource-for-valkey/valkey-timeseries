use crate::commands::command_parser::CommandArgToken;
use crate::commands::ts_create::parse_series_options_onto;
use crate::labels::MetricName;
use crate::series::index::get_timeseries_index;
use crate::series::{TimeSeries, TimeSeriesOptions, with_timeseries_mut};
use std::ops::Deref;
use valkey_module::{
    AclPermissions, Context, NotifyEvent, VALKEY_OK, ValkeyError, ValkeyResult, ValkeyString,
};

/// Alter a time series
///
/// TS.ALTER key
///   [RETENTION retentionPeriod]
///   [DUPLICATE_POLICY duplicatePolicy]
///   [SIGNIFICANT_DIGITS significantDigits | DECIMAL_DIGITS decimalDigits]
///   [IGNORE ignoreMaxTimediff ignoreMaxValDiff]
///   [LABELS label1=value1 label2=value2 ...]
#[valkey_module_macros::command({
    name: "ts.alter",
    flags: [Write, DenyOOM],
    summary: "Update the configuration of an existing time series.",
    complexity: "O(1)",
    since: "1.0.0",
    arity: -2,
    key_spec: [{
        flags: [ReadWrite, Update],
        begin_search: Index({ index: 1 }),
        find_keys: Range({ last_key: 0, steps: 1, limit: 0 })
    }]
})]
pub fn ts_alter_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 2 {
        return Err(ValkeyError::WrongArity);
    }

    let mut args = args;
    let key = args.remove(1);

    with_timeseries_mut(ctx, &key, Some(AclPermissions::UPDATE), |series| {
        // Parse onto an empty baseline, not the module configuration: an option the
        // command did not name must stay `None` so it can be left untouched.
        let options = parse_series_options_onto(
            TimeSeriesOptions::empty(),
            args,
            1,
            &[CommandArgToken::Encoding, CommandArgToken::OnDuplicate],
        )?;

        let changed = update_series(ctx, series, options, &key);

        ctx.replicate_verbatim();
        if changed {
            ctx.notify_keyspace_event(NotifyEvent::MODULE, "ts.alter", &key);
        }
        VALKEY_OK
    })
}

fn update_series(
    ctx: &Context,
    series: &mut TimeSeries,
    options: TimeSeriesOptions,
    key: &ValkeyString,
) -> bool {
    let mut has_changed = false;

    if let Some(chunk_size) = options.chunk_size
        && chunk_size != series.chunk_size_bytes
    {
        // todo: recompress the chunks
        series.chunk_size_bytes = chunk_size;
        has_changed = true;
    }

    if let Some(labels) = options.labels {
        has_changed = true;

        // reindex the series
        let guard = get_timeseries_index(ctx);
        let ts_index = guard.deref();

        ts_index.remove_timeseries(series);
        // update labels in series
        series.labels = if labels.is_empty() {
            MetricName::default()
        } else {
            MetricName::new(&labels)
        };

        ts_index.index_timeseries(series, key.as_slice());
    }

    if let Some(retention) = options.retention
        && retention != series.retention
    {
        series.retention = retention;
        has_changed = true;
        // Retention is applied eagerly on the write path so that TS.INFO agrees
        // with TS.RANGE; tightening the window here has to trim what is already
        // stored for the same reason.
        series.apply_retention();
    }

    if let Some(policy) = options.duplicate_policy
        && Some(policy) != series.sample_duplicates.policy
    {
        series.sample_duplicates.policy = Some(policy);
        has_changed = true;
    }

    // IGNORE and DUPLICATE_POLICY are independent properties: altering one must
    // leave the other as it was.
    if let Some((max_time_delta, max_value_delta)) = options.ignore
        && (max_time_delta != series.sample_duplicates.max_time_delta
            || max_value_delta != series.sample_duplicates.max_value_delta)
    {
        series.sample_duplicates.max_time_delta = max_time_delta;
        series.sample_duplicates.max_value_delta = max_value_delta;
        has_changed = true;
    }

    has_changed
}
