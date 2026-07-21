use crate::analysis::forecasting::normalize_model_name;
use crate::commands::CommandArgIterator;
use crate::commands::command_parser::{
    parse_forecast_confidence_level, parse_forecast_horizon_value, parse_store_clause,
};
use crate::commands::forecast_utils::{
    handle_forecast_key_pos_request, parse_timeseries_for_forecast, reply_with_forecast_output,
    run_forecast,
};
use crate::commands::utils::reply_with_double_array;
use crate::common::replies::{ThreadSafeReplyContext, block_client, reply_with_str};
use crate::common::time::compute_median_step_ms;
use crate::common::{Sample, Timestamp};
use crate::series::{
    DestinationWriteMode, TimeSeriesOptions, TimestampRange, create_or_update_series_with_samples,
};
use anofox_forecast::core::TimeSeries as ForecastTimeSeries;
use anofox_forecast::detection::detect_dominant_period;
use anofox_forecast::models::auto_forecast::{AutoForecast, AutoForecastConfig};
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

struct AutoForecastOptions {
    date_range: TimestampRange,
    horizon: usize,
    level: Option<f64>,
    metrics: bool,
    destination_key: Option<String>,
    create_options: Option<TimeSeriesOptions>,
    write_mode: Option<DestinationWriteMode>,
    config: AutoForecastConfig,
    auto_seasonality: bool,
}

impl Default for AutoForecastOptions {
    fn default() -> Self {
        Self {
            date_range: TimestampRange::default(),
            horizon: 5,
            level: None,
            metrics: false,
            destination_key: None,
            create_options: None,
            write_mode: None,
            config: AutoForecastConfig::default(),
            auto_seasonality: false,
        }
    }
}

/// ```text
/// TS.AUTOFORECAST key fromTimestamp toTimestamp
///     HORIZON <horizon>
///     [SEASONALITY <period>]
///     [MODELS <family1>,<family2> ...]
///     [LEVEL <confidence_level>]
///     [METRICS]
///     [STORE destinationKey
///         [MERGE]
///         [RETENTION retentionPeriod]
///         [ENCODING encoding]
///         [CHUNK_SIZE chunkSize]
///         [DUPLICATE_POLICY duplicatePolicy]
///         [SIGNIFICANT_DIGITS significantDigits | DECIMAL_DIGITS decimalDigits]
///         [METRIC metric]
///         [IGNORE ignoreMaxTimediff ignoreMaxValDiff]
///     ]
///```
/// `TS.AUTOFORECAST` fits all enabled auto models (AutoARIMA, AutoETS, AutoTheta)
/// and selects the best one based on cross-validation error.
#[valkey_module_macros::command({
    name: "TS.AUTOFORECAST",
    flags: [Write, DenyOOM],
    summary: "Forecast a time series, automatically selecting the best-fitting model.",
    complexity: "O(N*M) where N is the number of samples in the range and M is the number of candidate models.",
    since: "1.0.0",
    arity: -5,
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
pub(crate) fn ts_autoforecast_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 5 {
        return Err(ValkeyError::WrongArity);
    }

    if handle_forecast_key_pos_request(ctx, &args)? {
        return Ok(ValkeyValue::NoReply);
    }

    let mut args = args.into_iter().skip(1).peekable();

    let series = parse_timeseries_for_forecast(ctx, &mut args)?;
    let options = parse_autoforecast_args(&mut args)?;

    let blocked_client = block_client(ctx);
    std::thread::spawn(move || {
        let thread_ctx = ThreadSafeReplyContext::with_blocked_client(blocked_client);
        process_forecast(thread_ctx, series, options);
    });

    // Reply will be sent from the background thread
    Ok(ValkeyValue::NoReply)
}

fn parse_autoforecast_args(args: &mut CommandArgIterator) -> ValkeyResult<AutoForecastOptions> {
    let mut options = AutoForecastOptions::default();
    let mut horizon_set = false;

    while let Some(arg) = args.next() {
        hashify::fnc_map_ignore_case!(
                arg.as_slice(),
                "HORIZON" => {
                    options.horizon = parse_forecast_horizon_value(args)?;
                    horizon_set = true;
                },
                "SEASONALITY" => {
                    // parse seasonality value
                    if let Some(str) = args.peek()
                        && str.as_slice().eq_ignore_ascii_case(b"AUTO") {
                            options.auto_seasonality = true;
                            args.next(); // consume AUTO
                            continue;
                        }
                    let period = parse_single_value(args, "SEASONALITY")? as usize;
                    options.config.seasonal_period = Some(period);
                },
                "MODELS" => {
                    let models = args.next_str().map_err(|_| ValkeyError::Str("TSDB: Missing value for MODELS"))?;
                    parse_models(models, &mut options.config)?;
                },
                "LEVEL" => {
                    let value = parse_forecast_confidence_level(args)?;
                    options.level = Some(value);
                },
                "METRICS" => {
                    options.metrics = true;
                },
                "STORE" => {
                    let store_options = parse_store_clause(args)?;
                    // todo: this is fishy. Keys are binary safe
                    options.destination_key = Some(store_options.key.to_string_lossy());
                    options.create_options = Some(store_options.options);
                    options.write_mode = Some(store_options.write_mode);
                },
            _ => {
                return Err(ValkeyError::String(format!("TSDB: Unknown argument: {}", arg)));
            }
        );
    }

    if !horizon_set {
        return Err(ValkeyError::Str("TSDB: HORIZON is required"));
    }

    // Theta models don

    Ok(options)
}

fn process_forecast(
    ctx: ThreadSafeReplyContext,
    series: ForecastTimeSeries,
    mut options: AutoForecastOptions,
) {
    // Capture timestamp metadata before the model consumes the series reference.
    let last_timestamp_ms = series.timestamps().last().map(|dt| dt.timestamp_millis());
    let step_ms = if options.destination_key.is_some() {
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
    } else {
        None
    };

    if options.config.seasonal_period.is_none() && options.auto_seasonality {
        options.config.seasonal_period = detect_dominant_period(series.primary_values());
    }

    let seasonal_period = options.config.seasonal_period;

    let mut model = AutoForecast::with_config(options.config.clone());

    // { model: "ARIMA", horizon: 5, forecast: [...], lower_interval: [...], upper_interval: [...] }
    let output = run_forecast(
        &series,
        &mut model,
        options.horizon,
        options.level,
        options.metrics,
        seasonal_period,
    );

    let mut output = match output {
        Ok(o) => o,
        Err(err) => {
            let msg = err.to_string();
            ctx.log_warning(&msg);
            ctx.reply(Err(err));
            return;
        }
    };

    // selected_model_name() must be called AFTER fit_predict so the best model is known
    let model_name = model
        .selected_model_name()
        .map(|s| s.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let selected_model = normalize_model_name(&model_name);
    output.model_name = selected_model.to_string();

    let forecast = &output.forecast;
    // If STORE was specified, persist the predicted values into the target timeseries key.
    if options.destination_key.is_some() {
        store_if_necessary(
            &ctx,
            &options,
            forecast.primary(),
            last_timestamp_ms,
            step_ms,
        );
    }

    reply_with_forecast_output(&ctx, &output);
}

fn store_if_necessary(
    ctx: &ThreadSafeReplyContext,
    store_options: &AutoForecastOptions,
    forecast: &[f64],
    last_ts: Option<Timestamp>,
    step_ms: Option<i64>,
) {
    let key = match store_options.destination_key {
        Some(ref k) => k,
        None => "",
    };
    // Attempt to store the forecasted values in the specified key, but don't fail the entire command if this doesn't work.
    let lock = ctx.lock();
    let key = lock.create_string(key.as_bytes());
    let mode = store_options.write_mode.unwrap_or_default();

    if let (Some(last_ts), Some(step)) = (last_ts, step_ms) {
        let samples: Vec<Sample> = forecast
            .iter()
            .enumerate()
            .map(|(i, &value)| Sample::new(last_ts + step * (i as i64 + 1), value))
            .collect();

        if let Err(e) =
            create_or_update_series_with_samples(&lock, &key, None, mode, &samples, None)
        {
            let msg = format!("TSDB: failed to store forecast in key '{}': {}", key, e);
            ctx.log_warning(&msg);
        }
    } else {
        ctx.log_warning(
            "TSDB: STORE skipped — could not determine forecast step from input series",
        );
    }
}

pub(super) fn reply_with_interval_array(
    ctx: &ThreadSafeReplyContext,
    name: &'static str,
    values: &[f64],
) {
    reply_with_str(ctx, name);
    reply_with_double_array(ctx, values);
}

fn parse_single_value(iter: &mut CommandArgIterator, option_name: &str) -> ValkeyResult<f64> {
    let Ok(value_str) = iter.next_str() else {
        return Err(ValkeyError::String(format!(
            "TSDB: Missing value for {option_name}"
        )));
    };

    let value = value_str.parse().map_err(|_e| {
        ValkeyError::String(format!(
            "TSDB: invalid value for {option_name}: {value_str}"
        ))
    })?;

    Ok(value)
}

fn parse_models(model_str: &str, config: &mut AutoForecastConfig) -> ValkeyResult<()> {
    // remove all models first; we'll add back the ones specified by the user
    config.include_arima = false;
    config.include_ets = false;
    config.include_theta = false;

    for family in model_str.split(',') {
        let model = family.trim().to_uppercase();
        match model.as_str() {
            "ARIMA" | "AUTOARIMA" => {
                config.include_arima = true;
            }
            "ETS" | "AUTOETS" => {
                config.include_ets = true;
            }
            "THETA" | "AUTOTHETA" => {
                config.include_theta = true;
            }
            "TBATS" => {
                config.include_tbats = true;
            }
            "MFLES" => {
                config.include_mfles = true;
            }
            "MSTL" => {
                config.include_mstl = true;
            }
            _ => {
                return Err(ValkeyError::String(format!(
                    "TSDB: unknown auto-forecast model: {}",
                    family
                )));
            }
        }
    }

    if !config.include_arima
        && !config.include_ets
        && !config.include_theta
        && !config.include_tbats
        && !config.include_mfles
        && !config.include_mstl
    {
        return Err(ValkeyError::Str(
            "TSDB: at least one valid model must be specified in MODELS",
        ));
    }
    Ok(())
}
