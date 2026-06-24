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
use crate::common::replies::{ThreadSafeReplyContext, block_client, reply_with_array};
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
}

/// Forecasts future values of a time series using a specified model.
///
/// ```
///  TS.FORECAST key start_timestamp end_timestamp
///     MODELS model spec, ..
///     HORIZON horizon
///     [WITH_METRICS]
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

    reply_with_array(&ctx, results.len());
    for output in results {
        reply_with_forecast_output(&ctx, &output);
    }
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

    Ok(options)
}
