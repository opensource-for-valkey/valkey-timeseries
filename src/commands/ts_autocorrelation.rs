use crate::commands::command_parser::parse_series_range_samples;
use anofox_forecast::features::autocorrelation;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

/// ```text
/// TS.AUTOCORRELATION key startTime endTime lag
/// [PARTIAL | TRA | AGGREGATED <mean|var|std|median>]
/// ```
///
/// `TS.AUTOCORRELATION` computes autocorrelation-based statistics on a time series.
///
/// By default, returns the autocorrelation function (ACF) value at the specified lag.
///
/// Options:
/// - `PARTIAL`: Returns the partial autocorrelation (PACF) at the specified lag.
/// - `TRA`: Returns the time reversal asymmetry statistic at the specified lag.
/// - `AGGREGATED <mean|var|std|median>`: Returns aggregated autocorrelation across lags
///   1..=lag, using the specified aggregation function.
#[valkey_module_macros::command({
    name: "TS.AUTOCORRELATION",
    flags: [ReadOnly, DenyOOM],
    summary: "Compute autocorrelation statistics for a time series at a given lag.",
    complexity: "O(N*L) where N is the number of samples in the range and L is the lag.",
    since: "1.0.0",
    arity: -5,
    key_spec: [{
        flags: [ReadOnly, Access],
        begin_search: Index({ index: 1 }),
        find_keys: Range({ last_key: 0, steps: 1, limit: 0 })
    }]
})]
pub fn ts_autocorrelation_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 5 {
        return Err(ValkeyError::WrongArity);
    }

    let mut args = args.into_iter().skip(1).peekable();

    // Get the time series and extract sample values
    let samples = parse_series_range_samples(ctx, &mut args)?;
    let values: Vec<f64> = samples.iter().map(|s| s.value).collect();

    // Parse lag
    let lag_str = args.next_arg()?;
    let lag: i64 = lag_str
        .parse_integer()
        .map_err(|_| ValkeyError::Str("TSDB: invalid lag value"))?;

    if lag < 0 {
        return Err(ValkeyError::Str("TSDB: lag must be a non-negative integer"));
    }

    let lag = lag as usize;

    if values.len() <= lag {
        return Err(ValkeyError::String(format!(
            "TSDB: insufficient data for lag {lag}. Need at least {} samples, got {}",
            lag + 1,
            values.len()
        )));
    }

    let mut result = f64::NAN;
    if let Some(arg) = args.peek() {
        let arg = arg.as_slice();
        hashify::fnc_map_ignore_case!(
            arg,
            "PARTIAL" => {
                args.next();
                result = autocorrelation::partial_autocorrelation(&values, lag);
            },
            "TRA" => {
                args.next();
                if values.len() <= 2 * lag {
                    return Err(ValkeyError::String(format!(
                        "TSDB: insufficient data for TRA with lag {lag}. Need at least {} samples, got {}",
                        2 * lag + 1,
                        values.len()
                    )));
                }
                result = autocorrelation::time_reversal_asymmetry_statistic(&values, lag);
            },
            "AGGREGATED" => {
                args.next();
                let agg_str = args.next_str()?;
                let valid =  hashify::tiny_set_ignore_case! {
                    agg_str.as_bytes(),
                    "MEAN",
                    "VAR",
                    "STD",
                    "MEDIAN",
                };
                if !valid {
                    return Err(ValkeyError::Str(
                        "TSDB: invalid AGGREGATED function. Expected mean, var, std, or median"
                    ));
                }
                let agg = agg_str.to_ascii_lowercase();
                result = autocorrelation::agg_autocorrelation(&values, lag, &agg);
            },
            _ => return Err(ValkeyError::String("TSDB: unrecognized option".to_string()))
        )
    } else {
        result = autocorrelation::autocorrelation(&values, lag)
    };

    args.done()?;

    if result.is_nan() {
        return Err(ValkeyError::Str(
            "TSDB: autocorrelation computation returned NaN",
        ));
    }

    Ok(ValkeyValue::Float(result))
}
