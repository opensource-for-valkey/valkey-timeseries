use crate::analysis::forecasting::{make_forecast_time_series, normalize_model_name};
use crate::commands::{CommandArgIterator, parse_timestamp_range};
use crate::common::Sample;
use crate::common::replies::{
    ThreadSafeReplyContext, block_client, reply_with_array, reply_with_double, reply_with_map,
    reply_with_str,
};
use crate::common::time::compute_median_step_ms;
use crate::error_consts;
use crate::series::{TimestampRange, create_or_update_series_with_samples, get_timeseries};
use anofox_forecast::core::TimeSeries;
use anofox_forecast::detection::detect_dominant_period;
use anofox_forecast::models::Forecaster;
use anofox_forecast::models::auto_forecast::{AutoForecast, AutoForecastConfig};
use anofox_forecast::prelude::Forecast;
use anofox_forecast::utils::{AccuracyMetrics, calculate_metrics};
use valkey_module::{
    AclPermissions, Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue,
};

struct AutoForecastOptions {
    date_range: TimestampRange,
    horizon: usize,
    level: Option<f64>,
    metrics: bool,
    destination: Option<String>,
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
            destination: None,
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
///     [STORE <key1>]
///```
/// `TS.AUTOFORECAST` fits all enabled auto models (AutoARIMA, AutoETS, AutoTheta)
/// and selects the best one based on cross-validation error.
pub(crate) fn ts_autoforecast_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 5 {
        return Err(ValkeyError::WrongArity);
    }

    if ctx.is_keys_position_request() {
        ctx.key_at_pos(1); // key is always at position 1
        if let Some(store_pos) = get_store_key_pos(&args)? {
            ctx.key_at_pos(store_pos as i32);
        }
        return Ok(ValkeyValue::NoReply);
    }

    let mut args = args.into_iter().skip(1).peekable();
    let key = args.next_arg()?;
    // Parse timestamps
    let date_range = parse_timestamp_range(&mut args)?;
    let mut options = parse_autoforecast_args(&mut args)?;
    options.date_range = date_range;
    process_request(ctx, key, options)
}

fn get_store_key_pos(args: &[ValkeyString]) -> ValkeyResult<Option<usize>> {
    for (i, arg) in args.iter().enumerate() {
        if arg.eq_ignore_ascii_case(b"store") {
            if i + 1 >= args.len() {
                return Err(ValkeyError::Str("TSDB: Missing value for STORE argument"));
            }
            return Ok(Some(i + 1));
        }
    }
    Ok(None)
}

fn parse_autoforecast_args(args: &mut CommandArgIterator) -> ValkeyResult<AutoForecastOptions> {
    let mut options = AutoForecastOptions::default();
    let mut horizon_set = false;

    while let Some(arg) = args.next() {
        hashify::fnc_map_ignore_case!(
                arg.as_slice(),
                "HORIZON" => {
                    options.horizon = parse_single_value(args, "HORIZON")? as usize;
                    if options.horizon == 0 {
                        return Err(ValkeyError::Str("TSDB: HORIZON must be greater than 0"));
                    }
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
                    let value = parse_single_value(args, "LEVEL")?;
                    if value <= 0.0 || value >= 100.0 {
                        return Err(ValkeyError::String(format!("TSDB: LEVEL must be between 0 and 100, got {}", value)));
                    }
                    options.level = Some(value);
                },
                "METRICS" => {
                    options.metrics = true;
                },
                "STORE" => {
                    let value = args.next_string()
                        .map_err(|_| ValkeyError::Str("TSDB: Missing value for STORE"))?;
                    options.destination = Some(value);
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

fn process_request(ctx: &Context, key: ValkeyString, options: AutoForecastOptions) -> ValkeyResult {
    let key = ctx.create_string(key);

    let samples = match get_timeseries(ctx, &key, Some(AclPermissions::ACCESS), false) {
        Ok(Some(series)) => {
            let (start, end) = options.date_range.get_series_range(&series, None, false);
            series.get_range(start, end)
        }
        Ok(None) => return Err(ValkeyError::Str(error_consts::KEY_NOT_FOUND)),
        Err(e) => return Err(e),
    };

    let series = make_forecast_time_series(samples.into_iter())
        .map_err(|_e| ValkeyError::Str("TSDB: Failed to prepare time series for forecasting"))?;

    let blocked_client = block_client(ctx);
    std::thread::spawn(move || {
        let thread_ctx = ThreadSafeReplyContext::with_blocked_client(blocked_client);
        run_forecasting_thread(thread_ctx, series, options);
    });

    // Reply will be sent from the background thread
    Ok(ValkeyValue::NoReply)
}

fn run_forecasting_thread(
    ctx: ThreadSafeReplyContext,
    series: TimeSeries,
    mut options: AutoForecastOptions,
) {
    // Capture timestamp metadata before the model consumes the series reference.
    let last_timestamp_ms = series.timestamps().last().map(|dt| dt.timestamp_millis());
    let step_ms = if options.destination.is_some() {
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

    let mut model = AutoForecast::with_config(options.config);

    let res = if let Some(level) = options.level {
        model.fit_predict_with_intervals(&series, options.horizon, level / 100.0)
    } else {
        model.fit_predict(&series, options.horizon)
    };

    let forecast = match res {
        Ok(prediction) => prediction,
        Err(e) => {
            let msg = format!("TSDB: {}", e);
            // write error
            ctx.log_warning(&msg);
            ctx.reply(Err(ValkeyError::String(msg)));
            return;
        }
    };

    // { model: "ARIMA", horizon: 5, forecast: [...], lower_interval: [...], upper_interval: [...] }
    let selected_model = normalize_model_name(model.selected_model_name().unwrap_or("unknown"));
    let predicted_values = forecast.primary();
    let lower_interval = get_lower_interval(&forecast);
    let upper_interval = get_upper_interval(&forecast);
    let metrics = if options.metrics {
        let fitted = match model.fitted_values() {
            Some(v) => v,
            None => {
                let msg = "TSDB: metrics error: fitted values are unavailable for selected model";
                ctx.log_warning(msg);
                let _ = ctx.reply(Err(ValkeyError::Str(msg)));
                return;
            }
        };

        let actual = series.primary_values();
        let actual = if fitted.len() < actual.len() {
            &actual[actual.len() - fitted.len()..]
        } else {
            actual
        };
        match calculate_metrics(actual, fitted, seasonal_period) {
            Ok(m) => Some(m),
            Err(e) => {
                let msg = format!("TSDB: metrics error: {}", e);
                ctx.log_warning(&msg);
                let _ = ctx.reply(Err(ValkeyError::String(msg)));
                return;
            }
        }
    } else {
        None
    };

    // If STORE was specified, persist the predicted values into the target timeseries key.
    if let Some(destination) = options.destination {
        match (last_timestamp_ms, step_ms) {
            (Some(last_ts), Some(step)) => {
                let samples: Vec<Sample> = predicted_values
                    .iter()
                    .enumerate()
                    .map(|(i, &value)| Sample::new(last_ts + step * (i as i64 + 1), value))
                    .collect();

                let lock = ctx.lock();
                let key = lock.create_string(destination);

                if let Err(e) =
                    create_or_update_series_with_samples(&lock, &key, None, &samples, None)
                {
                    let msg = format!("TSDB: failed to store forecast in key '{}': {}", key, e);
                    ctx.log_warning(&msg);
                    let _ = ctx.reply(Err(ValkeyError::String(msg)));
                    return;
                }
            }
            _ => {
                ctx.log_warning(
                    "TSDB: STORE skipped — could not determine forecast step from input series",
                );
            }
        }
    }

    let mut map_len: usize = 3; // model, horizon, forecast are always included
    if lower_interval.is_some() || upper_interval.is_some() {
        map_len += 1; // "level" is only emitted when intervals exist
    }
    if lower_interval.is_some() {
        map_len += 1;
    }
    if upper_interval.is_some() {
        map_len += 1;
    }
    if metrics.is_some() {
        map_len += 1;
    }
    reply_with_map(&ctx, map_len);

    reply_with_str(&ctx, "selected_model");
    reply_with_str(&ctx, selected_model);
    reply_with_str(&ctx, "horizon");
    reply_with_str(&ctx, &options.horizon.to_string());
    reply_with_str(&ctx, "forecast");
    reply_with_double_array(&ctx, predicted_values);

    if forecast.has_lower() || forecast.has_upper() {
        reply_with_str(&ctx, "level");
        reply_with_double(&ctx, options.level.unwrap());
    }

    if let Some(lower_values) = lower_interval {
        reply_with_interval_array(&ctx, "lower_interval", lower_values);
    }
    if let Some(upper_values) = upper_interval {
        reply_with_interval_array(&ctx, "upper_interval", upper_values);
    }

    if let Some(m) = metrics.as_ref() {
        reply_with_accuracy_metrics(&ctx, m);
    }
}

fn get_lower_interval(forecast: &Forecast) -> Option<&[f64]> {
    let lower_values = forecast.lower()?;
    if lower_values.is_empty() {
        return None;
    }
    Some(lower_values[0].as_slice())
}

fn get_upper_interval(forecast: &Forecast) -> Option<&[f64]> {
    let upper_values = forecast.upper()?;
    if upper_values.is_empty() {
        return None;
    }
    Some(upper_values[0].as_slice())
}

fn reply_with_interval_array(ctx: &ThreadSafeReplyContext, name: &'static str, values: &[f64]) {
    reply_with_str(ctx, name);
    reply_with_double_array(ctx, values);
}

fn reply_with_double_array(ctx: &ThreadSafeReplyContext, values: &[f64]) {
    reply_with_array(ctx, values.len());
    for value in values {
        reply_with_double(ctx, *value);
    }
}

fn reply_with_accuracy_metrics(ctx: &ThreadSafeReplyContext, metrics: &AccuracyMetrics) {
    reply_with_str(ctx, "metrics");
    reply_with_map(ctx, 7);

    reply_with_str(ctx, "mae");
    reply_with_double(ctx, metrics.mae);

    reply_with_str(ctx, "mse");
    reply_with_double(ctx, metrics.mse);

    reply_with_str(ctx, "rmse");
    reply_with_double(ctx, metrics.rmse);

    reply_with_str(ctx, "mape");
    match metrics.mape {
        Some(v) => reply_with_double(ctx, v),
        None => crate::common::replies::reply_with_null(ctx),
    };

    reply_with_str(ctx, "smape");
    reply_with_double(ctx, metrics.smape);

    reply_with_str(ctx, "mase");
    match metrics.mase {
        Some(v) => reply_with_double(ctx, v),
        None => crate::common::replies::reply_with_null(ctx),
    };

    reply_with_str(ctx, "r_squared");
    reply_with_double(ctx, metrics.r_squared);
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
