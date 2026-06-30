use crate::analysis::forecasting::try_parse_trend_criterion;
use crate::commands::CommandArgIterator;
use crate::commands::command_parser::{
    StoreOptions, parse_series_range_samples, parse_store_clause,
};
use crate::commands::utils::reply_with_accuracy_metrics;
use crate::common::Sample;
use crate::common::replies::{
    reply_with_array, reply_with_double, reply_with_integer, reply_with_map, reply_with_str,
};
use crate::common::time::compute_median_step_ms;
use crate::series::{create_or_update_series_with_samples};
use anofox_forecast::seasonality::auto_trend::{AutoTrend, TrendCriterion};
use anofox_forecast::seasonality::traits::{Recency, TrendComponent};
use anofox_forecast::seasonality::{
    AutoRecencyConfig, ExponentialTrend, LogisticTrend, PolynomialTrend, TheilSenTrend,
};
use anofox_forecast::utils::{AccuracyMetrics, calculate_metrics};
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

/// Trend model selection: either Auto (with optional criterion) or a specific model.
#[derive(Debug, Clone)]
enum TrendModel {
    Auto(Option<TrendCriterion>),
    Exponential,
    Logistic,
    Polynomial,
    TheilSen,
}

impl Default for TrendModel {
    fn default() -> Self {
        TrendModel::Auto(None)
    }
}

/// ```text
/// TS.TREND key fromTimestamp toTimestamp
///     [MODEL <Exponential|Logistic|Polynomial|TheilSen|Auto> [AICc|BIC|HOLDOUT]]
///     [RECENCY <FULL|WINDOW n|FRACTION f|AUTO>]
///     [PREDICT <horizon>]
///     [FEATURES]
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
/// ```
///
/// `TS.TREND` fits one or more trend components to a time series.
///
/// When `MODEL` is `Auto` (the default), multiple candidate trend components
/// (Linear, Quadratic, Exponential, TheilSen, PiecewiseLinear) are fitted and
/// the best one is selected using an information criterion (AICc by default).
/// When a specific `MODEL` is given, only that trend component is fitted.
///
/// Returns a map. For **Auto** mode:
/// - `model`: name of the selected trend model
/// - `criterion`: criterion used for selection (AICc, BIC, HOLDOUT)
/// - `fitted_trend`: the in-sample fitted trend values
/// - `scores`: array of [name, score] pairs for all candidates, sorted by score
/// - `n_params`: number of free parameters of the fitted model
///
/// For **specific model** mode:
/// - `model`: name of the trend model used
/// - `fitted_trend`: the in-sample fitted trend values
/// - `n_params`: number of free parameters of the fitted model
///
/// Optional response fields:
/// - `predicted_trend`: predicted trend values (when PREDICT is specified)
/// - `features`: map of named features from the fitted component (when FEATURES is specified)
/// - `metrics`: accuracy metrics between observed and fitted values (when METRICS is specified)
pub fn ts_trend_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 4 {
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

    // Get the time series and extract sample values
    let samples = parse_series_range_samples(ctx, &mut args)?;
    let values: Vec<f64> = samples.iter().map(|s| s.value).collect();

    if values.len() < 4 {
        return Err(ValkeyError::Str(
            "TSDB: insufficient data for trend fitting. Need at least 4 samples.",
        ));
    }

    let options = parse_trend_args(&mut args)?;

    args.done()?;

    // Fit the trend based on the selected model
    if let TrendModel::Auto(criterion) = options.model {
        execute_auto_trend(ctx, options, criterion, &samples, &values)
    } else {
        let (model_name, trend) = build_specific_trend(&options.model, &options.recency)
            .ok_or(ValkeyError::Str("TSDB: invalid trend model configuration"))?;
        execute_specific_trend(ctx, options, model_name, &samples, &values, trend)
    }
}

fn build_specific_trend(
    model: &TrendModel,
    recency: &Recency,
) -> Option<(&'static str, Box<dyn TrendComponent>)> {
    match model {
        TrendModel::Exponential => Some((
            "Exponential",
            Box::new(ExponentialTrend::new().with_recency(recency.clone())),
        )),
        TrendModel::Logistic => Some((
            "Logistic",
            Box::new(LogisticTrend::new().with_recency(recency.clone())),
        )),
        TrendModel::Polynomial => Some((
            "Polynomial",
            Box::new(PolynomialTrend::new(2).with_recency(recency.clone())),
        )),
        TrendModel::TheilSen => Some((
            "TheilSen",
            Box::new(TheilSenTrend::new().with_recency(recency.clone())),
        )),
        TrendModel::Auto(_) => None,
    }
}

/// Execute auto-trend: fit all candidates and select the best one.
fn execute_auto_trend(
    ctx: &Context,
    options: TrendOptions,
    criterion: Option<TrendCriterion>,
    samples: &[Sample],
    values: &[f64],
) -> ValkeyResult {
    let criterion = criterion.unwrap_or(TrendCriterion::AICc);
    let mut auto_trend = AutoTrend::new()
        .with_recency(options.recency.clone())
        .with_criterion(criterion);

    auto_trend
        .fit_trend(values)
        .map_err(|e| ValkeyError::String(format!("TSDB: trend fitting error: {}", e)))?;

    let fitted_trend = auto_trend.fitted_trend();
    let selection = auto_trend.selection_result();
    let trend_name = auto_trend.trend_name();
    let selected_model = selection
        .map(|result| result.selected.as_str())
        .unwrap_or(trend_name);
    let n_params = auto_trend.n_params();
    let predicted = if options.predict > 0 {
        Some(auto_trend.predict_trend(options.predict))
    } else {
        None
    };
    let features = if options.features {
        Some(auto_trend.trend_features())
    } else {
        None
    };
    let metrics = if options.metrics {
        Some(compute_accuracy_metrics(values, fitted_trend)?)
    } else {
        None
    };

    // If STORE was specified, persist the fitted (and optionally predicted) trend values.
    if let Some(destination) = options.store_options {
        return store_trend(
            ctx,
            destination,
            samples,
            fitted_trend,
            predicted.as_deref(),
        );
    }

    // Build the response map for Auto mode
    let map_len = response_map_len(
        5,
        predicted.as_deref(),
        features.as_deref(),
        metrics.as_ref(),
    );

    reply_with_map(ctx, map_len);

    // selected_model
    reply_with_str(ctx, "model");
    reply_with_str(ctx, selected_model);

    // criterion
    reply_with_str(ctx, "criterion");
    let criterion_str = match criterion {
        TrendCriterion::AICc => "AICc",
        TrendCriterion::BIC => "BIC",
        TrendCriterion::Holdout => "HOLDOUT",
    };
    reply_with_str(ctx, criterion_str);

    reply_with_fitted_trend(ctx, fitted_trend);

    // scores
    reply_with_str(ctx, "scores");
    if let Some(result) = selection {
        reply_with_scores(ctx, &result.scores);
    } else {
        reply_with_array(ctx, 0);
    }

    reply_with_common_tail(
        ctx,
        predicted.as_deref(),
        features.as_deref(),
        metrics.as_ref(),
        n_params,
    );

    Ok(ValkeyValue::NoReply)
}

/// Execute a specific trend model (Exponential, Logistic, Polynomial, TheilSen).
fn execute_specific_trend(
    ctx: &Context,
    options: TrendOptions,
    model_name: &str,
    samples: &[Sample],
    values: &[f64],
    mut trend: Box<dyn TrendComponent>,
) -> ValkeyResult {
    trend
        .fit_trend(values)
        .map_err(|e| ValkeyError::String(format!("TSDB: trend fitting error: {}", e)))?;

    let fitted_trend = trend.fitted_trend();
    let n_params = trend.n_params();
    let predicted = if options.predict > 0 {
        Some(trend.predict_trend(options.predict))
    } else {
        None
    };
    let features = if options.features {
        Some(trend.trend_features())
    } else {
        None
    };
    let metrics = if options.metrics {
        Some(compute_accuracy_metrics(values, fitted_trend)?)
    } else {
        None
    };

    // If STORE was specified, persist the fitted (and optionally predicted) trend values.
    if let Some(store_options) = options.store_options {
        return store_trend(
            ctx,
            store_options,
            samples,
            fitted_trend,
            predicted.as_deref(),
        );
    }

    // Build the response map for specific model mode
    let map_len = response_map_len(
        3,
        predicted.as_deref(),
        features.as_deref(),
        metrics.as_ref(),
    );

    reply_with_map(ctx, map_len);

    // model
    reply_with_str(ctx, "model");
    reply_with_str(ctx, model_name);

    reply_with_fitted_trend(ctx, fitted_trend);

    reply_with_common_tail(
        ctx,
        predicted.as_deref(),
        features.as_deref(),
        metrics.as_ref(),
        n_params,
    );

    Ok(ValkeyValue::NoReply)
}

fn response_map_len(
    base_fields: usize,
    predicted: Option<&[f64]>,
    features: Option<&[(&str, f64)]>,
    metrics: Option<&AccuracyMetrics>,
) -> usize {
    base_fields
        + usize::from(predicted.is_some())
        + usize::from(features.is_some())
        + usize::from(metrics.is_some())
}

fn reply_with_fitted_trend(ctx: &Context, fitted_trend: &[f64]) {
    reply_with_str(ctx, "fitted_trend");
    reply_with_double_array(ctx, fitted_trend);
}

fn reply_with_common_tail(
    ctx: &Context,
    predicted: Option<&[f64]>,
    features: Option<&[(&str, f64)]>,
    metrics: Option<&AccuracyMetrics>,
    n_params: usize,
) {
    // predicted_trend (optional)
    if let Some(p) = predicted {
        reply_with_str(ctx, "predicted_trend");
        reply_with_double_array(ctx, p);
    }

    // features (optional)
    if let Some(f) = features {
        reply_with_str(ctx, "features");
        reply_with_trend_features(ctx, f);
    }

    // metrics (optional)
    if let Some(m) = metrics {
        reply_with_str(ctx, "accuracy_metrics");
        reply_with_accuracy_metrics(ctx, m);
    }

    // n_params
    reply_with_str(ctx, "n_params");
    reply_with_integer(ctx, n_params as i64);
}

/// Persist fitted (and optionally predicted) trend values to a destination key.
fn store_trend(
    ctx: &Context,
    store_options: StoreOptions,
    samples: &[Sample],
    fitted_trend: &[f64],
    predicted: Option<&[f64]>,
) -> ValkeyResult<ValkeyValue> {
    let destination = store_options.key;
    let mut store_samples: Vec<Sample> = fitted_trend
        .iter()
        .enumerate()
        .map(|(i, &value)| Sample::new(samples[i].timestamp, value))
        .collect();

    if let Some(predicted_values) = predicted {
        let timestamps: Vec<i64> = samples.iter().map(|s| s.timestamp).collect();
        if let Some(step) = compute_median_step_ms(&timestamps) {
            let last_ts = samples.last().map(|s| s.timestamp).unwrap_or(0);
            let predicted_samples = predicted_values
                .iter()
                .enumerate()
                .map(|(i, &value)| Sample::new(last_ts + step * (i as i64 + 1), value));
            store_samples.extend(predicted_samples);
        } else {
            ctx.log_warning(
                "TSDB: STORE predicted values skipped — could not determine step from input series",
            );
        }
    }

    let written = create_or_update_series_with_samples(
        ctx,
        &destination,
        Some(store_options.options),
        store_options.write_mode,
        &store_samples,
        None,
    )?;
    Ok(ValkeyValue::Integer(written as i64))
}

fn compute_accuracy_metrics(actual: &[f64], predicted: &[f64]) -> ValkeyResult<AccuracyMetrics> {
    calculate_metrics(actual, predicted, None)
        .map_err(|e| ValkeyError::String(format!("TSDB: metrics calculation error: {}", e)))
}

struct TrendOptions {
    model: TrendModel,
    recency: Recency,
    predict: usize,
    features: bool,
    metrics: bool,
    store_options: Option<StoreOptions>,
}

impl Default for TrendOptions {
    fn default() -> Self {
        Self {
            model: TrendModel::default(),
            recency: Recency::Fraction(0.3),
            predict: 0,
            features: false,
            metrics: false,
            store_options: None,
        }
    }
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

fn parse_trend_args(args: &mut CommandArgIterator) -> ValkeyResult<TrendOptions> {
    let mut options = TrendOptions::default();

    while let Some(arg) = args.next() {
        let arg_slice = arg.as_slice();
        hashify::fnc_map_ignore_case!(
            arg_slice,
            "MODEL" => {
                let val = args.next_str()
                    .map_err(|_| ValkeyError::Str("TSDB: Missing value for MODEL"))?;
                options.model = parse_trend_model(val, args)?;
            },
            "RECENCY" => {
                let val = args.next_str()
                    .map_err(|_| ValkeyError::Str("TSDB: Missing value for RECENCY"))?;
                options.recency = match val.to_ascii_uppercase().as_str() {
                    "AUTO" => Recency::Auto(AutoRecencyConfig::default()),
                    "FULL" => Recency::Full,
                    "WINDOW" => {
                        let n_str = args.next_str()
                            .map_err(|_| ValkeyError::Str("TSDB: Missing window size for RECENCY WINDOW"))?;
                        let n: usize = n_str.parse().map_err(|_| {
                            ValkeyError::Str("TSDB: invalid window size for RECENCY WINDOW")
                        })?;
                        if n < 4 {
                            return Err(ValkeyError::Str(
                                "TSDB: RECENCY WINDOW must be at least 4"
                            ));
                        }
                        Recency::Window(n)
                    },
                    "FRACTION" => {
                        let f_str = args.next_str()
                            .map_err(|_| ValkeyError::Str("TSDB: Missing fraction value for RECENCY FRACTION"))?;
                        let f: f64 = f_str.parse().map_err(|_| {
                            ValkeyError::Str("TSDB: invalid fraction for RECENCY FRACTION")
                        })?;
                        if f <= 0.0 || f > 1.0 {
                            return Err(ValkeyError::Str(
                                "TSDB: RECENCY FRACTION must be between 0 and 1"
                            ));
                        }
                        Recency::Fraction(f)
                    },
                    other => return Err(ValkeyError::String(format!(
                        "TSDB: invalid RECENCY '{}'. Expected FULL, WINDOW, or FRACTION",
                        other
                    ))),
                };
            },
            "PREDICT" => {
                let val = args.next_str()
                    .map_err(|_| ValkeyError::Str("TSDB: Missing value for PREDICT"))?;
                let n: i64 = val.parse().map_err(|_| {
                    ValkeyError::Str("TSDB: invalid value for PREDICT")
                })?;
                if n <= 0 {
                    return Err(ValkeyError::Str("TSDB: PREDICT must be greater than 0"));
                }
                options.predict = n as usize;
            },
            "FEATURES" => {
                options.features = true;
            },
            "METRICS" => {
                options.metrics = true;
            },
            "STORE" => {
                let opts = parse_store_clause(args)?;
                options.store_options = Some(opts);
            },
            _ => {
                // Unknown argument
                return Err(ValkeyError::String(format!(
                    "TSDB: Unknown argument: {}",
                    arg
                )));
            }
        );
    }

    Ok(options)
}

/// Parse the MODEL argument value and optional criterion.
///
/// Accepted values: Exponential, Logistic, Polynomial, TheilSen, Auto [criterion]
fn parse_trend_model(val: &str, args: &mut CommandArgIterator) -> ValkeyResult<TrendModel> {
    match val.to_ascii_uppercase().as_str() {
        "EXPONENTIAL" => Ok(TrendModel::Exponential),
        "LOGISTIC" => Ok(TrendModel::Logistic),
        "POLYNOMIAL" => Ok(TrendModel::Polynomial),
        "THEILSEN" => Ok(TrendModel::TheilSen),
        "AUTO" => {
            // Optionally parse a criterion after Auto
            if let Some(next) = args.peek() {
                let maybe_criterion = next.try_as_str().map_err(|_| {
                    ValkeyError::Str(
                        "TSDB: Invalid argument after MODEL Auto. Expected AICc, BIC, or HOLDOUT.",
                    )
                })?;
                if let Ok(criterion) = try_parse_trend_criterion(maybe_criterion) {
                    args.next(); // consume the criterion argument
                    Ok(TrendModel::Auto(Some(criterion)))
                } else {
                    Ok(TrendModel::Auto(None))
                }
            } else {
                Ok(TrendModel::Auto(None))
            }
        }
        other => Err(ValkeyError::String(format!(
            "TSDB: Invalid MODEL '{}'. Expected Exponential, Logistic, Polynomial, TheilSen, or Auto.",
            other
        ))),
    }
}

/// Reply with an array of doubles, using the optimized raw API.
fn reply_with_double_array(ctx: &Context, values: &[f64]) {
    reply_with_array(ctx, values.len());
    for &v in values {
        reply_with_double(ctx, v);
    }
}

/// Reply with scores as nested arrays: [[name, score], ...]
fn reply_with_scores(ctx: &Context, scores: &[(String, f64)]) {
    reply_with_array(ctx, scores.len());
    for (name, score) in scores {
        reply_with_array(ctx, 2);
        reply_with_str(ctx, name);
        reply_with_double(ctx, *score);
    }
}

/// Reply with trend features as a flat map (alternating key-value pairs).
fn reply_with_trend_features(ctx: &Context, features: &[(&str, f64)]) {
    reply_with_map(ctx, features.len());
    for (name, value) in features {
        reply_with_str(ctx, name);
        reply_with_double(ctx, *value);
    }
}
