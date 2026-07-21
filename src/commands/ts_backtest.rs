use crate::analysis::forecasting::{BacktestModelSpec, DynForecaster, parse_backtest_model_specs};
use crate::commands::CommandArgIterator;
use crate::commands::command_parser::parse_forecast_horizon_value;
use crate::commands::forecast_utils::{
    handle_forecast_key_pos_request, parse_timeseries_for_forecast, reply_with_accuracy_metrics,
};
use crate::commands::utils::reply_with_double_array;
use crate::common::replies::{
    ThreadSafeReplyContext, block_client, reply_with_array, reply_with_integer, reply_with_map,
    reply_with_null, reply_with_str, reply_with_usize,
};
use anofox_forecast::core::TimeSeries as ForecastTimeSeries;
use anofox_forecast::models::Forecaster;
use anofox_forecast::prelude::{AccuracyMetrics, calculate_metrics};
use anofox_forecast::utils::cross_validation::{
    CVStrategy, ConstraintViolation, CvFoldGenerator, Fold,
};
use orx_parallel::{ParIter, ParIterResult, Parallelizable};
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

struct BacktestOptions {
    models_spec: String,
    horizon: usize,
    initial_window: usize,
    strategy: CVStrategy,
    step: Option<usize>,
    n_folds: usize,
    gap: usize,
    purge: usize,
    embargo: usize,
    seasonal_period: Option<usize>,
    with_predictions: bool,
}

impl Default for BacktestOptions {
    fn default() -> Self {
        Self {
            models_spec: String::new(),
            horizon: 0,
            initial_window: 0, // 0 = "not set"; resolved once HORIZON is known
            strategy: CVStrategy::Expanding,
            step: None, // None = defaults to horizon (non-overlapping folds)
            n_folds: 5,
            gap: 0,
            purge: 0,
            embargo: 0,
            seasonal_period: None,
            with_predictions: false,
        }
    }
}

struct FoldResult {
    fold: Fold,
    metrics: AccuracyMetrics,
    predictions: Vec<f64>,
    actuals: Vec<f64>,
}

struct AggregatedSummary {
    mae: f64,
    rmse: f64,
    smape: f64,
    mape: Option<f64>,
    mae_std: f64,
    rmse_std: f64,
}

enum BacktestModelResult {
    Success {
        model_name: String,
        horizon: usize,
        strategy: CVStrategy,
        fold_results: Vec<FoldResult>,
        aggregated: AggregatedSummary,
    },
    Failure {
        model_name: String,
        error: String,
    },
}

/// ```text
/// TS.BACKTEST key fromTimestamp toTimestamp
///   MODELS model_spec[,model_spec ...]
///   HORIZON horizon
///   [INITIAL_WINDOW size]
///   [STRATEGY EXPANDING|ROLLING]
///   [STEP stepSize]
///   [N_FOLDS n]
///   [GAP gap]
///   [PURGE purge]
///   [EMBARGO embargo]
///   [SEASONAL_PERIOD period]
///   [WITH_PREDICTIONS]
/// ```
#[valkey_module_macros::command({
    name: "TS.BACKTEST",
    flags: [ReadOnly, DenyOOM],
    summary: "Backtest forecasting models over historical windows of a time series.",
    complexity: "O(N*M*F) where N is the number of samples in the range, M is the number of models and F is the number of folds.",
    since: "1.0.0",
    arity: -8,
    key_spec: [{
        flags: [ReadOnly, Access],
        begin_search: Index({ index: 1 }),
        find_keys: Range({ last_key: 0, steps: 1, limit: 0 })
    }]
})]
pub(crate) fn ts_backtest_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 8 {
        return Err(ValkeyError::WrongArity);
    }

    if handle_forecast_key_pos_request(ctx, &args)? {
        return Ok(ValkeyValue::NoReply);
    }

    let mut args = args.into_iter().skip(1).peekable();
    let series = parse_timeseries_for_forecast(ctx, &mut args)?;
    let options = parse_backtest_args(&mut args)?;

    let blocked_client = block_client(ctx);
    std::thread::spawn(move || {
        let thread_ctx = ThreadSafeReplyContext::with_blocked_client(blocked_client);
        process_backtest(thread_ctx, series, options);
    });

    // Reply will be sent from the background thread
    Ok(ValkeyValue::NoReply)
}

fn parse_backtest_args(args: &mut CommandArgIterator) -> ValkeyResult<BacktestOptions> {
    let mut options = BacktestOptions::default();
    let mut horizon_set = false;

    while let Some(arg) = args.next() {
        hashify::fnc_map_ignore_case!(
            arg.as_slice(),
            "HORIZON" => {
                options.horizon = parse_forecast_horizon_value(args)?;
                horizon_set = true;
            },
            "MODELS" => {
                options.models_spec = args.next_string()
                    .map_err(|_| ValkeyError::Str("TSDB: missing value for MODELS"))?;
            },
            "INITIAL_WINDOW" => {
                let value = args.next_i64()
                    .map_err(|_| ValkeyError::Str("TSDB: missing value for INITIAL_WINDOW"))?;
                if value <= 0 {
                    return Err(ValkeyError::Str("TSDB: INITIAL_WINDOW must be greater than 0"));
                }
                options.initial_window = value as usize;
            },
            "STRATEGY" => {
                let s = args.next_string()
                    .map_err(|_| ValkeyError::Str("TSDB: missing value for STRATEGY"))?;
                options.strategy = match s.to_uppercase().as_str() {
                    "EXPANDING" => CVStrategy::Expanding,
                    "ROLLING" => CVStrategy::Rolling,
                    _ => return Err(ValkeyError::Str("TSDB: STRATEGY must be EXPANDING or ROLLING")),
                };
            },
            "STEP" => {
                let value = args.next_i64()
                    .map_err(|_| ValkeyError::Str("TSDB: missing value for STEP"))?;
                if value <= 0 {
                    return Err(ValkeyError::Str("TSDB: STEP must be greater than 0"));
                }
                options.step = Some(value as usize);
            },
            "N_FOLDS" => {
                let value = args.next_i64()
                    .map_err(|_| ValkeyError::Str("TSDB: missing value for N_FOLDS"))?;
                if value <= 0 {
                    return Err(ValkeyError::Str("TSDB: N_FOLDS must be greater than 0"));
                }
                options.n_folds = value as usize;
            },
            "GAP" => {
                let value = args.next_i64()
                    .map_err(|_| ValkeyError::Str("TSDB: missing value for GAP"))?;
                if value < 0 {
                    return Err(ValkeyError::Str("TSDB: GAP must be non-negative"));
                }
                options.gap = value as usize;
            },
            "PURGE" => {
                let value = args.next_i64()
                    .map_err(|_| ValkeyError::Str("TSDB: missing value for PURGE"))?;
                if value < 0 {
                    return Err(ValkeyError::Str("TSDB: PURGE must be non-negative"));
                }
                options.purge = value as usize;
            },
            "EMBARGO" => {
                let value = args.next_i64()
                    .map_err(|_| ValkeyError::Str("TSDB: missing value for EMBARGO"))?;
                if value < 0 {
                    return Err(ValkeyError::Str("TSDB: EMBARGO must be non-negative"));
                }
                options.embargo = value as usize;
            },
            "SEASONAL_PERIOD" => {
                let value = args.next_i64()
                    .map_err(|_| ValkeyError::Str("TSDB: missing value for SEASONAL_PERIOD"))?;
                if value <= 0 {
                    return Err(ValkeyError::Str("TSDB: SEASONAL_PERIOD must be greater than 0"));
                }
                options.seasonal_period = Some(value as usize);
            },
            "WITH_PREDICTIONS" => {
                options.with_predictions = true;
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
    if options.initial_window == 0 {
        options.initial_window = (options.horizon * 3).max(10);
    }

    Ok(options)
}

fn process_backtest(
    ctx: ThreadSafeReplyContext,
    series: ForecastTimeSeries,
    options: BacktestOptions,
) {
    let specs = match parse_backtest_model_specs(&options.models_spec) {
        Ok(s) => s,
        Err(e) => {
            let err = ValkeyError::String(format!("TSDB: error parsing MODELS: {e}"));
            ctx.reply(Err(err));
            return;
        }
    };

    let generator = CvFoldGenerator::new()
        .n_folds(options.n_folds)
        .horizon(options.horizon)
        .min_initial_window(options.initial_window)
        .step_size(options.step.unwrap_or(options.horizon))
        .gap(options.gap)
        .purge(options.purge)
        .embargo(options.embargo)
        .strategy(options.strategy)
        .on_constraint_violation(ConstraintViolation::ReduceFolds);

    let folds = match generator.generate(series.len()) {
        Ok(folds) if !folds.is_empty() => folds,
        _ => {
            let err = ValkeyError::Str(
                "TSDB: not enough data for backtest with the given HORIZON/INITIAL_WINDOW",
            );
            ctx.reply(Err(err));
            return;
        }
    };

    // Folds are evaluated in parallel per model (the expensive part: one full fit+predict per
    // fold), while models themselves are dispatched sequentially — MODELS lists are typically
    // small, and this keeps a failing model's folds from aborting other models' results.
    let results: Vec<BacktestModelResult> = specs
        .iter()
        .map(|spec| evaluate_model(spec, &series, &folds, &options))
        .collect();

    reply_with_array(&ctx, results.len());
    for result in &results {
        reply_with_backtest_result(&ctx, &series, result, options.with_predictions);
    }
}

fn evaluate_model(
    spec: &BacktestModelSpec,
    series: &ForecastTimeSeries,
    folds: &[Fold],
    options: &BacktestOptions,
) -> BacktestModelResult {
    let result: Result<Vec<FoldResult>, ValkeyError> = folds
        .par()
        .map(|fold| evaluate_fold(spec, series, fold, options.seasonal_period))
        .into_fallible_result()
        .collect();

    match result {
        Ok(fold_results) => {
            let aggregated = aggregate_fold_metrics(&fold_results);
            BacktestModelResult::Success {
                model_name: spec.display_name().to_string(),
                horizon: options.horizon,
                strategy: options.strategy,
                fold_results,
                aggregated,
            }
        }
        Err(e) => BacktestModelResult::Failure {
            model_name: spec.display_name().to_string(),
            error: e.to_string(),
        },
    }
}

/// Builds, trains, and predicts a fresh model instance entirely on the thread that runs this
/// closure, and never sends the model itself across a thread boundary — only the inputs
/// (`spec`, `series`, `fold`, all `Send`/`Sync`-safe plain data) and outputs (`FoldResult`,
/// likewise plain data) cross. `anofox_forecast::models::Forecaster` has no `Send` bound, so
/// `Box<dyn Forecaster>` can't be proven `Send`; this design sidesteps the need for that bound
/// rather than requiring one.
fn evaluate_fold(
    spec: &BacktestModelSpec,
    series: &ForecastTimeSeries,
    fold: &Fold,
    seasonal_period: Option<usize>,
) -> ValkeyResult<FoldResult> {
    let train = series
        .slice(fold.train_start, fold.train_end)
        .map_err(|e| ValkeyError::String(format!("TSDB: {e}")))?;

    let boxed = spec
        .build()
        .map_err(|e| ValkeyError::String(format!("TSDB: {e}")))?;
    let mut model = DynForecaster::from(boxed);

    let forecast = model
        .fit_predict(&train, fold.test_size())
        .map_err(|e| ValkeyError::String(format!("TSDB: {e}")))?;

    let predictions = forecast.primary().to_vec();
    let actuals = series.primary_values()[fold.test_start..fold.test_end].to_vec();

    let metrics = calculate_metrics(&actuals, &predictions, seasonal_period)
        .map_err(|e| ValkeyError::String(format!("TSDB: metrics error: {e}")))?;

    Ok(FoldResult {
        fold: fold.clone(),
        metrics,
        predictions,
        actuals,
    })
}

fn aggregate_fold_metrics(folds: &[FoldResult]) -> AggregatedSummary {
    let n = folds.len() as f64;
    let mae_values: Vec<f64> = folds.iter().map(|f| f.metrics.mae).collect();
    let rmse_values: Vec<f64> = folds.iter().map(|f| f.metrics.rmse).collect();
    let smape_values: Vec<f64> = folds.iter().map(|f| f.metrics.smape).collect();

    let mae = mae_values.iter().sum::<f64>() / n;
    let rmse = rmse_values.iter().sum::<f64>() / n;
    let smape = smape_values.iter().sum::<f64>() / n;

    let mape = if folds.iter().all(|f| f.metrics.mape.is_some()) {
        Some(folds.iter().filter_map(|f| f.metrics.mape).sum::<f64>() / n)
    } else {
        None
    };

    AggregatedSummary {
        mae,
        rmse,
        smape,
        mape,
        mae_std: std_dev(&mae_values, mae),
        rmse_std: std_dev(&rmse_values, rmse),
    }
}

fn std_dev(values: &[f64], mean: f64) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
    variance.sqrt()
}

fn strategy_name(strategy: CVStrategy) -> &'static str {
    match strategy {
        CVStrategy::Expanding => "expanding",
        CVStrategy::Rolling => "rolling",
    }
}

fn reply_with_backtest_result(
    ctx: &ThreadSafeReplyContext,
    series: &ForecastTimeSeries,
    result: &BacktestModelResult,
    with_predictions: bool,
) {
    match result {
        BacktestModelResult::Failure { model_name, error } => {
            reply_with_map(ctx, 2);
            reply_with_str(ctx, "model");
            reply_with_str(ctx, model_name);
            reply_with_str(ctx, "error");
            reply_with_str(ctx, error);
        }
        BacktestModelResult::Success {
            model_name,
            horizon,
            strategy,
            fold_results,
            aggregated,
        } => {
            reply_with_map(ctx, 6);

            reply_with_str(ctx, "model");
            reply_with_str(ctx, model_name);

            reply_with_str(ctx, "horizon");
            reply_with_usize(ctx, *horizon);

            reply_with_str(ctx, "strategy");
            reply_with_str(ctx, strategy_name(*strategy));

            reply_with_str(ctx, "n_folds");
            reply_with_usize(ctx, fold_results.len());

            reply_with_str(ctx, "metrics");
            reply_with_aggregated_metrics(ctx, aggregated);

            reply_with_str(ctx, "folds");
            reply_with_array(ctx, fold_results.len());
            for fold_result in fold_results {
                reply_with_fold_result(ctx, series, fold_result, with_predictions);
            }
        }
    }
}

fn reply_with_aggregated_metrics(ctx: &ThreadSafeReplyContext, m: &AggregatedSummary) {
    reply_with_map(ctx, 6);

    reply_with_str(ctx, "mae");
    crate::common::replies::reply_with_double(ctx, m.mae);
    reply_with_str(ctx, "rmse");
    crate::common::replies::reply_with_double(ctx, m.rmse);
    reply_with_str(ctx, "smape");
    crate::common::replies::reply_with_double(ctx, m.smape);
    reply_with_str(ctx, "mape");
    match m.mape {
        Some(v) => {
            crate::common::replies::reply_with_double(ctx, v);
        }
        None => {
            reply_with_null(ctx);
        }
    }
    reply_with_str(ctx, "mae_std");
    crate::common::replies::reply_with_double(ctx, m.mae_std);
    reply_with_str(ctx, "rmse_std");
    crate::common::replies::reply_with_double(ctx, m.rmse_std);
}

fn reply_with_fold_result(
    ctx: &ThreadSafeReplyContext,
    series: &ForecastTimeSeries,
    fold_result: &FoldResult,
    with_predictions: bool,
) {
    let timestamps = series.timestamps();
    let fold = &fold_result.fold;
    let train_start_ts = timestamps[fold.train_start].timestamp_millis();
    let train_end_ts = timestamps[fold.train_end - 1].timestamp_millis();
    let test_start_ts = timestamps[fold.test_start].timestamp_millis();
    let test_end_ts = timestamps[fold.test_end - 1].timestamp_millis();

    let map_len = if with_predictions { 7 } else { 5 };
    reply_with_map(ctx, map_len);

    reply_with_str(ctx, "train_start");
    reply_with_integer(ctx, train_start_ts);
    reply_with_str(ctx, "train_end");
    reply_with_integer(ctx, train_end_ts);
    reply_with_str(ctx, "test_start");
    reply_with_integer(ctx, test_start_ts);
    reply_with_str(ctx, "test_end");
    reply_with_integer(ctx, test_end_ts);

    // Emits the "metrics" key and its map value itself.
    reply_with_accuracy_metrics(ctx, &fold_result.metrics);

    if with_predictions {
        reply_with_str(ctx, "predictions");
        reply_with_double_array(ctx, &fold_result.predictions);
        reply_with_str(ctx, "actuals");
        reply_with_double_array(ctx, &fold_result.actuals);
    }
}
