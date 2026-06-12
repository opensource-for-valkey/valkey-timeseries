use crate::common::Sample;
use crate::common::constants::METRIC_NAME_LABEL;
use crate::common::context::get_current_db;
use crate::error_consts;
use crate::fanout::key_belongs_to_local_node;
use crate::labels::{InternedLabel, Label};
use crate::series::acl::check_key_permissions;
use crate::series::chunks::ChunkEncoding;
use crate::series::index::{get_db_index, next_timeseries_id};
use crate::series::series_data_type::VK_TIME_SERIES_TYPE;
use crate::series::{
    DuplicatePolicy, SampleAddResult, SeriesGuard, SeriesGuardMut, TimeSeries, TimeSeriesOptions,
    create_compaction_rules_from_config,
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
    if let Some(series) = redis_key.get_value::<TimeSeries>(&VK_TIME_SERIES_TYPE)? {
        if check_acl {
            check_key_permissions(ctx, key, &AclPermissions::ACCESS)?;
        }
        f(series)
    } else {
        Err(invalid_series_key_error())
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
    // Ensure that the key belongs to the current node in cluster mode, to prevent creating a series on the wrong shard.
    //  In non-cluster mode, this is a no-op.
    if !key_belongs_to_local_node(ctx, key.as_slice()) {
        return Err(ValkeyError::Str(error_consts::WRONG_SLOT));
    }

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
    notify: bool,
) -> ValkeyResult<()> {
    let _key = ValkeyKeyWritable::open(ctx.ctx, key);
    // check if this refers to an existing series
    if !_key.is_empty() {
        return Err(ValkeyError::Str(error_consts::DUPLICATE_KEY));
    }

    let ts = create_series(key, options, ctx)?;
    _key.set_value(&VK_TIME_SERIES_TYPE, ts)?;

    if notify {
        ctx.replicate_verbatim();
        ctx.notify_keyspace_event(NotifyEvent::MODULE, "ts.create", key);
        ctx.log_verbose("series created");
    }

    Ok(())
}

pub fn create_and_store_series<'a>(
    ctx: &'a Context,
    key: &ValkeyString,
    options: TimeSeriesOptions,
    notify: bool,
    add_compactions: bool,
) -> ValkeyResult<SeriesGuardMut<'a>> {
    create_and_store_internal(ctx, key, options, notify)?;

    let Some(mut series) = get_timeseries_mut(ctx, key, true, Some(AclPermissions::INSERT))? else {
        return Err(ValkeyError::Str(error_consts::KEY_NOT_FOUND));
    };

    if add_compactions {
        // If compactions are enabled, add the default compaction rules
        add_default_compactions(ctx, &mut series, key)?
    }
    Ok(series)
}

pub fn get_or_create_series<'a>(
    ctx: &'a Context,
    key: &ValkeyString,
    options: Option<TimeSeriesOptions>,
) -> ValkeyResult<SeriesGuardMut<'a>> {
    match get_timeseries_mut(ctx, key, false, Some(AclPermissions::UPDATE))? {
        Some(series) => Ok(series),
        None => create_and_store_series(ctx, key, options.unwrap_or_default(), true, true),
    }
}

pub fn create_or_update_series_with_samples<'a>(
    ctx: &'a Context,
    key: &ValkeyString,
    creation_options: Option<TimeSeriesOptions>,
    samples: &[Sample],
    policy_override: Option<DuplicatePolicy>,
) -> ValkeyResult<(SeriesGuardMut<'a>, Vec<SampleAddResult>)> {
    let mut series = get_or_create_series(ctx, key, creation_options)?;

    let merge_results = if !samples.is_empty() {
        let mut sorted_samples = samples.to_vec();
        sorted_samples.sort_by_key(|sample| sample.timestamp);
        series.merge_samples(&sorted_samples, policy_override)?
    } else {
        Vec::new()
    };

    Ok((series, merge_results))
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
