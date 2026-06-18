use crate::analysis::forecasting::make_forecast_time_series;
use crate::commands::CommandArgIterator;
use crate::commands::command_parser::parse_series_range_samples;
use crate::commands::ts_autoforecast::reply_with_interval_array;
use crate::commands::utils::{get_store_key_pos, reply_with_double_array};
use crate::common::replies::{
    ThreadSafeReplyContext, reply_with_double, reply_with_map, reply_with_str, reply_with_usize,
};
use anofox_forecast::core::{Forecast, TimeSeries as ForecastTimeSeries};
use anofox_forecast::models::Forecaster;
use anofox_forecast::prelude::{AccuracyMetrics, calculate_metrics};
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString};

pub(super) fn handle_forecast_key_pos_request(
    ctx: &Context,
    args: &[ValkeyString],
) -> ValkeyResult<bool> {
    if ctx.is_keys_position_request() {
        ctx.key_at_pos(1); // key is always at position 1
        if let Some(store_pos) = get_store_key_pos(args)? {
            ctx.key_at_pos(store_pos as i32);
        }
        return Ok(true);
    }
    Ok(false)
}

pub(super) fn parse_timeseries_for_forecast(
    ctx: &Context,
    args: &mut CommandArgIterator,
) -> ValkeyResult<ForecastTimeSeries> {
    let samples = parse_series_range_samples(ctx, args)?;
    make_forecast_time_series(samples.into_iter())
        .map_err(|_e| ValkeyError::Str("TSDB: Failed to prepare time series for forecasting"))
}

pub struct ForecastOutput {
    pub(crate) model_name: String,
    horizon: usize,
    level: Option<f64>,
    pub(crate) forecast: Forecast,
    metrics: Option<AccuracyMetrics>,
}

pub(super) fn run_forecast<T: Forecaster>(
    series: &ForecastTimeSeries,
    model: &mut T,
    horizon: usize,
    level: Option<f64>,
    with_metrics: bool,
    seasonal_period: Option<usize>,
) -> Result<ForecastOutput, ValkeyError> {
    let res = if let Some(level) = level {
        model.fit_predict_with_intervals(series, horizon, level / 100.0)
    } else {
        model.fit_predict(series, horizon)
    };

    let forecast = match res {
        Ok(prediction) => prediction,
        Err(e) => {
            let msg = format!("TSDB: {}", e);
            return Err(ValkeyError::String(msg));
        }
    };

    let metrics = if with_metrics {
        let fitted = match model.fitted_values() {
            Some(v) => v,
            None => {
                let msg = "TSDB: metrics error: fitted values are unavailable for selected model";
                return Err(ValkeyError::Str(msg));
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
                return Err(ValkeyError::String(msg));
            }
        }
    } else {
        None
    };

    let forecast_output = ForecastOutput {
        model_name: model.name().to_string(),
        horizon,
        level,
        forecast,
        metrics,
    };

    Ok(forecast_output)
}

pub(super) fn get_lower_interval(forecast: &Forecast) -> Option<&[f64]> {
    let lower_values = forecast.lower()?;
    if lower_values.is_empty() {
        return None;
    }
    Some(lower_values[0].as_slice())
}

pub(super) fn get_upper_interval(forecast: &Forecast) -> Option<&[f64]> {
    let upper_values = forecast.upper()?;
    if upper_values.is_empty() {
        return None;
    }
    Some(upper_values[0].as_slice())
}

pub(super) fn reply_with_accuracy_metrics(ctx: &ThreadSafeReplyContext, metrics: &AccuracyMetrics) {
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

pub(super) fn reply_with_forecast_output(
    ctx: &ThreadSafeReplyContext,
    forecast_output: &ForecastOutput,
) {
    let mut map_len: usize = 3; // model, horizon, forecast are always included

    let forecast = &forecast_output.forecast;

    let predicted_values = forecast.primary();
    let lower_interval = get_lower_interval(forecast);
    let upper_interval = get_upper_interval(forecast);

    if lower_interval.is_some() || upper_interval.is_some() {
        map_len += 1; // "level" is only emitted when intervals exist
    }
    if lower_interval.is_some() {
        map_len += 1;
    }
    if upper_interval.is_some() {
        map_len += 1;
    }
    if forecast_output.metrics.is_some() {
        map_len += 1;
    }
    reply_with_map(ctx, map_len);

    reply_with_str(ctx, "model");
    reply_with_str(ctx, &forecast_output.model_name);
    reply_with_str(ctx, "horizon");
    reply_with_usize(ctx, forecast_output.horizon);
    reply_with_str(ctx, "forecast");
    reply_with_double_array(ctx, predicted_values);

    if forecast.has_lower() || forecast.has_upper() {
        reply_with_str(ctx, "level");
        reply_with_double(
            ctx,
            forecast_output.level.expect("confidence level must be set"),
        );
    }

    if let Some(lower_values) = lower_interval {
        reply_with_interval_array(ctx, "lower_interval", lower_values);
    }
    if let Some(upper_values) = upper_interval {
        reply_with_interval_array(ctx, "upper_interval", upper_values);
    }

    if let Some(m) = forecast_output.metrics.as_ref() {
        reply_with_accuracy_metrics(ctx, m);
    }
}
