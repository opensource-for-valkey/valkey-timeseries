use crate::common::constants::METRIC_NAME_LABEL;
use crate::common::context::get_current_db;
use crate::error_consts;
use crate::labels::{InternedLabel, Label};
use crate::series::acl::check_key_permissions;
use crate::series::chunks::ChunkEncoding;
use crate::series::index::{get_db_index, next_timeseries_id};
use crate::series::series_data_type::VK_TIME_SERIES_TYPE;
use crate::series::{
    SeriesGuard, SeriesGuardMut, TimeSeries, TimeSeriesOptions, create_compaction_rules_from_config,
};
use std::ops::Deref;
use std::time::Duration;
use valkey_module::key::ValkeyKeyWritable;
use valkey_module::{
    AclPermissions, Context, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString,
};

pub fn with_timeseries<R>(
    ctx: &Context,
    key: &ValkeyString,
    check_acl: bool,
    f: impl FnOnce(&TimeSeries) -> ValkeyResult<R>,
) -> ValkeyResult<R> {
    let redis_key = ctx.open_key(key);
    // A key holding a non-TSDB value must surface the standard WRONGTYPE error,
    // not valkey-module-rs's raw "Existing key has wrong Valkey type" — the `?`
    // shortcut leaked the latter, diverging from RTS (and from TS.RANGE/TS.INFO,
    // which report WRONGTYPE). A missing key is KEY_NOT_FOUND.
    match redis_key.get_value::<TimeSeries>(&VK_TIME_SERIES_TYPE) {
        Ok(Some(series)) => {
            if check_acl {
                check_key_permissions(ctx, key, &AclPermissions::ACCESS)?;
            }
            f(series)
        }
        Ok(None) => Err(invalid_series_key_error()),
        Err(_) => Err(ValkeyError::WrongType),
    }
}

pub fn with_timeseries_mut<R>(
    ctx: &Context,
    key: &ValkeyString,
    permissions: Option<AclPermissions>,
    f: impl FnOnce(&mut TimeSeries) -> ValkeyResult<R>,
) -> ValkeyResult<R> {
    let perms = permissions.unwrap_or(AclPermissions::UPDATE);
    // expect should not panic, since must_exist will cause an error if the key is non-existent, and `?` will ensure it propagates
    let mut series =
        get_timeseries_mut(ctx, key, true, Some(perms))?.expect("expected key to exist");
    f(&mut series)
}

pub fn get_timeseries<'a>(
    ctx: &'a Context,
    key: &ValkeyString,
    permissions: Option<AclPermissions>,
    must_exist: bool,
) -> ValkeyResult<Option<SeriesGuard<'a>>> {
    if let Some(permissions) = permissions {
        check_key_permissions(ctx, key, &permissions)?;
    }
    match SeriesGuard::from_key(ctx, key) {
        Ok(guard) => Ok(Some(guard)),
        Err(e) => match e {
            ValkeyError::Str(err) if err == error_consts::KEY_NOT_FOUND => {
                if must_exist {
                    return Err(ValkeyError::Str(error_consts::KEY_NOT_FOUND));
                }
                Ok(None)
            }
            _ => Err(e),
        },
    }
}

pub fn get_timeseries_mut<'a>(
    ctx: &'a Context,
    key: &ValkeyString,
    must_exist: bool,
    permissions: Option<AclPermissions>,
) -> ValkeyResult<Option<SeriesGuardMut<'a>>> {
    let value_key = ctx.open_key_writable(key);
    match value_key.get_value::<TimeSeries>(&VK_TIME_SERIES_TYPE) {
        Ok(Some(series)) => {
            if let Some(permissions) = permissions {
                check_key_permissions(ctx, key, &permissions)?;
            }
            Ok(Some(SeriesGuardMut { series }))
        }
        Ok(None) => {
            if must_exist {
                return Err(ValkeyError::Str(error_consts::KEY_NOT_FOUND));
            }
            Ok(None)
        }
        Err(_e) => Err(ValkeyError::WrongType),
    }
}

pub(crate) fn invalid_series_key_error() -> ValkeyError {
    ValkeyError::Str(error_consts::KEY_NOT_FOUND)
}

pub fn create_series(
    key: &ValkeyString,
    options: TimeSeriesOptions,
    ctx: &Context,
) -> ValkeyResult<TimeSeries> {
    let mut ts = TimeSeries::with_options(options)?;
    if ts.id == 0 {
        ts.id = next_timeseries_id();
    }

    let db = get_current_db(ctx);

    ts._db = Some(db);
    let guard = get_db_index(db);

    let index = guard.deref();

    // Check if this refers to an existing series (a pre-existing series with the same label-value pairs)
    // We do this only in the case where we have a __name__ label, signaling that the user is
    // opting in to Prometheus semantics, meaning a metric name is unique to a series.
    if ts.labels.get_value(METRIC_NAME_LABEL).is_some() {
        let labels = ts.labels.to_label_vec();
        // will return an error if the series already exists
        if index.series_id_by_labels(&labels).is_some() {
            return Err(ValkeyError::Str(error_consts::DUPLICATE_SERIES));
        }
    }

    index.index_timeseries(&ts, key.iter().as_slice());
    Ok(ts)
}

pub fn create_and_store_internal(
    ctx: &Context,
    key: &ValkeyString,
    options: TimeSeriesOptions,
    replicate: bool,
    notify: bool,
) -> ValkeyResult<()> {
    let _key = ValkeyKeyWritable::open(ctx.ctx, key);
    // check if this refers to an existing series
    if !_key.is_empty() {
        return Err(ValkeyError::Str(error_consts::DUPLICATE_KEY));
    }

    let ts = create_series(key, options, ctx)?;
    _key.set_value(&VK_TIME_SERIES_TYPE, ts)?;

    if replicate {
        ctx.replicate_verbatim();
    }
    // Only an explicit TS.CREATE emits `ts.create`; auto-creating write
    // commands (TS.ADD/TS.MADD/TS.INCRBY/...) emit just their own write
    // event, matching RedisTimeSeries (compat plan §7.3).
    if notify {
        ctx.notify_keyspace_event(NotifyEvent::MODULE, "ts.create", key);
        ctx.log_verbose("series created");
    }

    Ok(())
}

/// `explicit_create` distinguishes a client-issued `TS.CREATE` from the
/// auto-create performed by write commands (TS.ADD/TS.MADD/TS.INCRBY/...):
///
/// - explicit: this call replicates verbatim and emits `ts.create`.
/// - auto-create: neither — the calling command replicates *itself*, and the
///   replica re-creates the series by re-running that command. Replicating
///   here as well would propagate the write twice (`alsoPropagate` queues
///   every call), which doubles non-idempotent effects like TS.INCRBY on
///   replicas. (Corner case accepted: if the command errors *after* the
///   auto-create, nothing propagates and the primary keeps an empty series
///   the replica never sees.)
pub fn create_and_store_series<'a>(
    ctx: &'a Context,
    key: &ValkeyString,
    options: TimeSeriesOptions,
    explicit_create: bool,
    add_compactions: bool,
) -> ValkeyResult<SeriesGuardMut<'a>> {
    create_and_store_internal(ctx, key, options, explicit_create, explicit_create)?;

    let Some(mut series) = get_timeseries_mut(ctx, key, true, Some(AclPermissions::INSERT))? else {
        return Err(ValkeyError::Str(error_consts::KEY_NOT_FOUND));
    };

    if add_compactions {
        // If compactions are enabled, add the default compaction rules
        add_default_compactions(ctx, &mut series, key)?
    }
    Ok(series)
}

fn add_default_compactions(
    ctx: &Context,
    series: &mut TimeSeries,
    key: &ValkeyString,
) -> ValkeyResult<()> {
    let key_str = key.to_string_lossy();
    let Some(compaction_rules) = create_compaction_rules_from_config(&key_str) else {
        // No compaction rules available for this key
        return Ok(());
    };

    let mut base_config = TimeSeriesOptions::from_config();
    base_config.chunk_encoding = ChunkEncoding::Uncompressed;

    // create a new series for each compaction rule
    let mut rules = Vec::with_capacity(compaction_rules.len());
    for (dest_key, (mut rule, retention)) in compaction_rules.into_iter() {
        let bucket_duration = rule.bucket_duration;
        let agg_type = rule.aggregator.aggregation_type();

        let mut labels = series.labels.to_label_vec();

        labels.push(Label::new("aggregation", agg_type.name()));
        let duration_str = bucket_duration.to_string();
        labels.push(Label::new("time_bucket", &duration_str));

        let child_key = ctx.create_string(dest_key);
        let options = TimeSeriesOptions {
            src_id: Some(series.id),
            retention: Some(Duration::from_millis(retention)),
            labels: Some(labels),
            ..base_config
        };

        let value_key = ValkeyKeyWritable::open(ctx.ctx, &child_key);
        if !value_key.is_empty() {
            let msg = format!("TSDB: compaction series for key '{child_key}' already exists.");
            ctx.log_warning(&msg);
            continue;
        }

        let destination = match create_series(&child_key, options, ctx) {
            Ok(dest_series) => dest_series,
            Err(e) => {
                let msg =
                    format!("TSDB: error creating compaction series for key \"{child_key}\": {e}");
                ctx.log_warning(&msg);
                continue;
            }
        };

        rule.dest_id = destination.id;
        value_key.set_value(&VK_TIME_SERIES_TYPE, destination)?;

        rules.push(rule);
    }
    series.rules = rules;

    Ok(())
}

pub fn get_series_labels<'a>(
    series: &'a TimeSeries,
    with_labels: bool,
    selected_labels: &[String],
) -> Vec<Option<InternedLabel<'a>>> {
    if !with_labels && selected_labels.is_empty() {
        return vec![];
    }

    if selected_labels.is_empty() {
        series.labels.iter().map(Some).collect::<Vec<_>>()
    } else {
        selected_labels
            .iter()
            .map(|name| series.get_label(name))
            .collect::<Vec<_>>()
    }
}
