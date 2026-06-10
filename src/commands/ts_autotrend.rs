use crate::commands::{CommandArgIterator, parse_timestamp_range};
use crate::common::Sample;
use crate::common::replies::{
    reply_with_array, reply_with_double, reply_with_integer, reply_with_map, reply_with_str,
};
use crate::common::time::compute_median_step_ms;
use crate::error_consts;
use crate::series::{create_or_update_series_with_samples, get_timeseries};
use anofox_forecast::seasonality::auto_trend::{AutoTrend, TrendCriterion};
use anofox_forecast::seasonality::traits::{Recency, TrendComponent};
use valkey_module::{
    AclPermissions, Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue,
};

/// ```text
/// TS.AUTOTREND key fromTimestamp toTimestamp
///     [CRITERION <AICc|BIC|HOLDOUT>]
///     [RECENCY <FULL|WINDOW n|FRACTION f>]
///     [PREDICT <horizon>]
///     [FEATURES]
///     [STORE <destination>]
/// ```
///
/// `TS.AUTOTREND` fits multiple candidate trend components (Linear, Quadratic,
/// Exponential, TheilSen, PiecewiseLinear) and selects the best one using an
/// information criterion (AICc by default).
///
///
/// Returns a map with:
/// - `selected_trend`: name of the selected trend model
/// - `criterion`: criterion used for selection (AICc, BIC, HOLDOUT)
/// - `fitted_trend`: the in-sample fitted trend values
/// - `scores`: array of [name, score] pairs for all candidates, sorted by score
/// - `n_params`: number of free parameters of the fitted model
///
/// Optional response fields:
/// - `predicted_trend`: predicted trend values (when PREDICT is specified)
/// - `features`: map of named features from the fitted component (when FEATURES is specified)
/// - `destination`: if STORE is specified, the key where fitted (and optionally predicted) values are stored
pub fn ts_autotrend_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
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

    let key = args.next_arg()?;
    let date_range = parse_timestamp_range(&mut args)?;
    let options = parse_autotrend_args(&mut args)?;

    args.done()?;

    let samples = match get_timeseries(ctx, &key, Some(AclPermissions::ACCESS), false) {
        Ok(Some(series)) => {
            let (start, end) = date_range.get_series_range(&series, None, false);
            series.get_range(start, end)
        }
        Ok(None) => return Err(ValkeyError::Str(error_consts::KEY_NOT_FOUND)),
        Err(e) => return Err(e),
    };

    let values: Vec<f64> = samples.iter().map(|s| s.value).collect();

    if values.len() < 4 {
        return Err(ValkeyError::Str(
            "TSDB: insufficient data for trend fitting. Need at least 4 samples.",
        ));
    }

    // Build and fit the auto trend model
    let mut auto_trend = AutoTrend::new()
        .with_criterion(options.criterion)
        .with_recency(options.recency);

    auto_trend
        .fit_trend(&values)
        .map_err(|e| ValkeyError::String(format!("TSDB: trend fitting error: {}", e)))?;

    // Gather results
    let fitted_trend = auto_trend.fitted_trend();
    let selection = auto_trend.selection_result();
    let trend_name = auto_trend.trend_name();
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

    // If STORE was specified, persist the fitted (and optionally predicted) trend values.
    if let Some(destination) = options.destination.clone() {
        let destination = ctx.create_string(destination);
        let mut store_samples: Vec<Sample> = fitted_trend
            .iter()
            .enumerate()
            .map(|(i, &value)| Sample::new(samples[i].timestamp, value))
            .collect();

        if let Some(ref predicted_values) = predicted {
            let timestamps: Vec<i64> = samples.iter().map(|s| s.timestamp).collect();
            if let Some(step) = compute_median_step_ms(&timestamps) {
                let last_ts = samples.last().map(|s| s.timestamp).unwrap_or(0);
                let predicted_samples: Vec<Sample> = predicted_values
                    .iter()
                    .enumerate()
                    .map(|(i, &value)| Sample::new(last_ts + step * (i as i64 + 1), value))
                    .collect();
                store_samples.extend(predicted_samples);
            } else {
                ctx.log_warning(
                    "TSDB: STORE predicted values skipped — could not determine step from input series",
                );
            }
        }

        create_or_update_series_with_samples(ctx, &destination, None, &store_samples, None)?;
    }

    // Build the response map
    let mut map_len = 4; // selected_trend, criterion, fitted_trend, scores
    if predicted.is_some() {
        map_len += 1;
    }
    if features.is_some() {
        map_len += 1;
    }
    // Always include n_params
    map_len += 1;

    reply_with_map(ctx, map_len);

    // selected_trend
    reply_with_str(ctx, "selected_trend");
    reply_with_str(ctx, trend_name);

    // criterion
    reply_with_str(ctx, "criterion");
    let criterion_str = match options.criterion {
        TrendCriterion::AICc => "AICc",
        TrendCriterion::BIC => "BIC",
        TrendCriterion::Holdout => "HOLDOUT",
    };
    reply_with_str(ctx, criterion_str);

    // fitted_trend
    reply_with_str(ctx, "fitted_trend");
    reply_with_double_array(ctx, fitted_trend);

    // scores
    reply_with_str(ctx, "scores");
    if let Some(result) = selection {
        reply_with_scores(ctx, &result.scores);
    } else {
        reply_with_array(ctx, 0);
    }

    // predicted_trend (optional)
    if let Some(ref p) = predicted {
        reply_with_str(ctx, "predicted_trend");
        reply_with_double_array(ctx, p);
    }

    // features (optional)
    if let Some(ref f) = features {
        reply_with_str(ctx, "features");
        reply_with_trend_features(ctx, f);
    }

    // n_params
    reply_with_str(ctx, "n_params");
    reply_with_integer(ctx, n_params as i64);

    Ok(ValkeyValue::NoReply)
}

struct AutoTrendOptions {
    criterion: TrendCriterion,
    recency: Recency,
    predict: usize,
    features: bool,
    destination: Option<String>,
}

impl Default for AutoTrendOptions {
    fn default() -> Self {
        Self {
            criterion: TrendCriterion::AICc,
            recency: Recency::Fraction(0.3),
            predict: 0,
            features: false,
            destination: None,
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

fn parse_autotrend_args(args: &mut CommandArgIterator) -> ValkeyResult<AutoTrendOptions> {
    let mut options = AutoTrendOptions::default();

    while let Some(arg) = args.next() {
        let arg_slice = arg.as_slice();
        hashify::fnc_map_ignore_case!(
            arg_slice,
            "CRITERION" => {
                let val = args.next_str()
                    .map_err(|_| ValkeyError::Str("TSDB: Missing value for CRITERION"))?;
                options.criterion = match val.to_ascii_uppercase().as_str() {
                    "AICC" => TrendCriterion::AICc,
                    "AIC" => TrendCriterion::AICc,
                    "BIC" => TrendCriterion::BIC,
                    "HOLDOUT" => TrendCriterion::Holdout,
                    other => return Err(ValkeyError::String(format!(
                        "TSDB: invalid CRITERION '{}'. Expected AICc, BIC, or HOLDOUT",
                        other
                    ))),
                };
            },
            "RECENCY" => {
                let val = args.next_str()
                    .map_err(|_| ValkeyError::Str("TSDB: Missing value for RECENCY"))?;
                options.recency = match val.to_ascii_uppercase().as_str() {
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
            "STORE" => {
                let key = args.next_string()
                    .map_err(|_| ValkeyError::Str("TSDB: Missing value for STORE"))?;
                options.destination = Some(key);
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
