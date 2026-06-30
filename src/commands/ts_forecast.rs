use crate::analysis::forecasting::DynForecaster;
use crate::analysis::forecasting::build_models_from_specs;
use crate::commands::CommandArgIterator;
use crate::commands::command_parser::{
    parse_forecast_confidence_level, parse_forecast_horizon_value,
};
use crate::commands::forecast_utils::{
    ForecastOutput, handle_forecast_key_pos_request, parse_timeseries_for_forecast,
    reply_with_forecast_output, run_forecast,
};
use crate::commands::parse_store_clause;
use crate::common::replies::{ThreadSafeReplyContext, block_client, reply_with_array};
use crate::common::time::compute_median_step_ms;
use crate::common::Sample;
use crate::series::create_or_update_series_with_samples;
use crate::series::DestinationWriteMode;
use crate::series::TimeSeriesOptions;
use crate::series::TimestampRange;
use anofox_forecast::core::TimeSeries as ForecastTimeSeries;
use anofox_forecast::models::BoxedForecaster;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

#[derive(Default)]
struct ForecastOptions {
    series_key: String,
    models_spec: String,
    timestamp_range: TimestampRange,
    horizon: usize,
    include_metrics: bool,
    level: Option<f64>,
    destination_key: Option<Vec<u8>>,
    series_options: Option<TimeSeriesOptions>,
    write_mode: DestinationWriteMode
}

/// Forecasts future values of a time series using a specified model.
///
/// ```
///  TS.FORECAST key start_timestamp end_timestamp
///   MODELS model spec, ..
///   HORIZON horizon
///   [WITH_METRICS]
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
/// ```
///
pub(crate) fn ts_forecast_command(ctx: &mut Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 8 {
        return Err(ValkeyError::WrongArity);
    }

    if handle_forecast_key_pos_request(ctx, &args)? {
        return Ok(ValkeyValue::NoReply);
    }

    let mut args = args.into_iter().skip(1).peekable();
    let series = parse_timeseries_for_forecast(ctx, &mut args)?;
    let options = parse_forecast_args(&mut args)?;

    let blocked_client = block_client(ctx);
    std::thread::spawn(move || {
        let thread_ctx = ThreadSafeReplyContext::with_blocked_client(blocked_client);
        process_forecast(thread_ctx, series, options);
    });

    // Reply will be sent from the background thread
    Ok(ValkeyValue::NoReply)
}

fn process_forecast(
    ctx: ThreadSafeReplyContext,
    series: ForecastTimeSeries,
    options: ForecastOptions,
) {
    // Capture timestamp metadata before models consume the series reference.
    let last_timestamp_ms = series.timestamps().last().map(|dt| dt.timestamp_millis());
    let step_ms = options.destination_key.as_ref().and_then(|_| {
        series
            .frequency()
            .map(|d| d.num_milliseconds())
            .or_else(|| {
                let timestamps: Vec<i64> = series
                    .timestamps()
                    .iter()
                    .map(|dt| dt.timestamp_millis())
                    .collect();
                compute_median_step_ms(&timestamps)
            })
    });

    let models = match build_models_from_specs(&options.models_spec) {
        Ok(models) => models,
        Err(e) => {
            let err = ValkeyError::String(format!("TSDB: error parsing MODELS: {:?}", e));
            ctx.reply(Err(err));
            return;
        }
    };

    let results = match process_models(&series, models, &options) {
        Ok(results) => results,
        Err(e) => {
            ctx.reply(Err(e));
            return;
        }
    };

    // If STORE was specified, persist the predicted values into the destination key.
    if store_forecast_if_necessary(&ctx, &options, &results, last_timestamp_ms, step_ms) {
        return;
    }

    reply_with_array(&ctx, results.len());
    for output in results {
        reply_with_forecast_output(&ctx, &output);
    }
}

/// Attempts to store forecasted values into the destination key specified in `options`.
/// Returns `true` if the STORE was handled (reply already sent), `false` if the caller
/// should proceed with the standard forecast reply.
fn store_forecast_if_necessary(
    ctx: &ThreadSafeReplyContext,
    options: &ForecastOptions,
    results: &[ForecastOutput],
    last_timestamp_ms: Option<i64>,
    step_ms: Option<i64>,
) -> bool {
    let (Some(dest_key), Some(step), Some(last_ts)) =
        (options.destination_key.as_ref(), step_ms, last_timestamp_ms)
    else {
        if options.destination_key.is_some() && step_ms.is_none() {
            ctx.log_warning(
                "TSDB: STORE skipped — could not determine forecast step from input series",
            );
        }
        return false;
    };

    let mut samples: Vec<Sample> = Vec::new();
    for output in results {
        let predicted = output.forecast.primary();
        let offset = samples.len() as i64;
        for (i, &value) in predicted.iter().enumerate() {
            samples.push(Sample::new(
                last_ts + step * (offset + i as i64 + 1),
                value,
            ));
        }
    }

    let lock = ctx.lock();
    let key = lock.create_string(dest_key.as_slice());
    match create_or_update_series_with_samples(
        &lock,
        &key,
        options.series_options.clone(),
        options.write_mode,
        &samples,
        None,
    ) {
        Ok(written) => {
            let _ = ctx.reply(Ok(ValkeyValue::Integer(written as i64)));
        }
        Err(e) => {
            let msg = format!(
                "TSDB: failed to store forecast in key '{}': {}",
                key, e
            );
            ctx.log_warning(&msg);
            let _ = ctx.reply(Err(ValkeyError::String(msg)));
        }
    }
    true
}

fn process_models(
    series: &ForecastTimeSeries,
    models: Vec<BoxedForecaster>,
    options: &ForecastOptions,
) -> ValkeyResult<Vec<ForecastOutput>> {
    let mut results = Vec::new();
    let mut models: Vec<DynForecaster> = models.into_iter().map(DynForecaster::from).collect();
    for model in models.iter_mut() {
        let output = run_forecast(
            series,
            model,
            options.horizon,
            options.level,
            options.include_metrics,
            None, // seasonal_period can be added as an option if needed
        )?;
        results.push(output);
    }
    Ok(results)
}

fn parse_forecast_args(args: &mut CommandArgIterator) -> ValkeyResult<ForecastOptions> {
    let mut options = ForecastOptions::default();
    let mut horizon_set = false;

    while let Some(arg) = args.next() {
        hashify::fnc_map_ignore_case!(
                arg.as_slice(),
                "HORIZON" => {
                    options.horizon = parse_forecast_horizon_value(args)?;
                    horizon_set = true;
                },
                "MODELS" => {
                    let models = args.next_string().map_err(|_| ValkeyError::Str("TSDB: missing value for MODELS"))?;
                    options.models_spec = models;
                },
                "LEVEL" => {
                    let value = parse_forecast_confidence_level(args)?;
                    options.level = Some(value);
                },
                "WITH_METRICS" => {
                    options.include_metrics = true;
                },
                "STORE" => {
                    let store_options = parse_store_clause(args)?;
                    options.destination_key = Some(store_options.key.into());
                    options.series_options = Some(store_options.options);
                    options.write_mode = store_options.write_mode;
                },
            _ => {
                return Err(ValkeyError::String(format!("TSDB: Unknown argument: {}", arg)));
            }
        );
    }

    if !horizon_set {
        return Err(ValkeyError::Str("TSDB: HORIZON is required"));
    }

    if options.models_spec.is_empty() {
        return Err(ValkeyError::Str(
            "TSDB: MODELS must contain at least one model specification",
        ));
    }

    if options.destination_key.is_some() {
        let model_count = build_models_from_specs(&options.models_spec)
            .map_err(|e| {
                ValkeyError::String(format!("TSDB: error parsing MODELS: {:?}", e))
            })?
            .len();
        if model_count > 1 {
            return Err(ValkeyError::Str(
                "TSDB: STORE is only supported with a single model",
            ));
        }
    }

    Ok(options)
}
