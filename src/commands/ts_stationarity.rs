use crate::commands::parse_timestamp_range;
use crate::common::replies::{
    reply_with_double, reply_with_integer, reply_with_map, reply_with_str,
};
use crate::error_consts;
use crate::series::get_timeseries;
use anofox_forecast::validation::stationarity::{self, StationarityResult};
use valkey_module::{
    AclPermissions, Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue,
};

/// ```text
/// TS.STATIONARITY key startTime endTime
///     [TEST adf|kpss|combined]
///     [LAGS n]
/// ```
///
/// `TS.STATIONARITY` tests whether a time series is stationary.
///
/// Stationarity is a key property for many forecasting models. A stationary series
/// has constant mean, variance, and autocorrelation over time.
///
/// Options:
/// - `TEST`: Which test to run. Defaults to `combined`.
///   - `adf` — Augmented Dickey-Fuller test (null: series has unit root, i.e., non-stationary)
///   - `kpss` — KPSS test (null: series is stationary)
///   - `combined` — Runs both ADF and KPSS and returns an overall conclusion
/// - `LAGS n`: Number of lags for the test (integer ≥ 0). Only valid with `TEST adf` or `TEST kpss`.
///   If omitted, a sensible default is used automatically.
///
/// Returns a map with test statistics, p-values, critical values, and a conclusion.
pub fn ts_stationarity_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 4 {
        return Err(ValkeyError::WrongArity);
    }

    let mut args = args.into_iter().skip(1).peekable();

    let key = args.next_arg()?;

    let date_range = parse_timestamp_range(&mut args)?;

    // Parse optional TEST and LAGS
    let mut test_type = TestType::Combined;
    let mut lags: Option<usize> = None;

    while let Some(arg) = args.peek() {
        let arg_str = arg
            .try_as_str()
            .map_err(|_| ValkeyError::Str("TSDB: invalid argument"))?;

        match arg_str.to_uppercase().as_str() {
            "TEST" => {
                args.next();
                let test_str = args.next_str()?;
                test_type = parse_test_type(test_str)?;
            }
            "LAGS" => {
                args.next();
                let lag_str = args.next_arg()?;
                let lag: i64 = lag_str.parse_integer().map_err(|_| {
                    ValkeyError::Str("TSDB: invalid LAGS value, expected a non-negative integer")
                })?;
                if lag < 0 {
                    return Err(ValkeyError::Str(
                        "TSDB: LAGS must be a non-negative integer",
                    ));
                }
                lags = Some(lag as usize);
            }
            _ => break,
        }
    }

    args.done()?;

    // LAGS is incompatible with combined test
    if test_type == TestType::Combined && lags.is_some() {
        return Err(ValkeyError::Str(
            "TSDB: LAGS option is not supported with TEST combined",
        ));
    }

    // Get the time series and extract sample values
    let series = match get_timeseries(ctx, &key, Some(AclPermissions::ACCESS), false) {
        Ok(Some(series)) => series,
        Ok(None) => return Err(ValkeyError::Str(error_consts::KEY_NOT_FOUND)),
        Err(e) => return Err(e),
    };

    let (start, end) = date_range.get_series_range(&series, None, false);
    let samples = series.get_range(start, end);

    let values: Vec<f64> = samples.iter().map(|s| s.value).collect();

    // Minimum data check
    const MIN_SAMPLES: usize = 10;
    if values.len() < MIN_SAMPLES {
        return Err(ValkeyError::String(format!(
            "TSDB: insufficient data for stationarity test. Need at least {MIN_SAMPLES} samples, got {}",
            values.len()
        )));
    }

    // Constant series (all values identical) is trivially stationary.
    // The ADF/KPSS regression would fail with zero variance, so handle
    // this edge case by returning a stationary result directly.
    let is_constant = values.len() >= 2
        && values
            .windows(2)
            .all(|w| (w[0] - w[1]).abs() < f64::EPSILON);

    if is_constant {
        let const_result = StationarityResult {
            statistic: 0.0,
            p_value: 1.0,
            lags: 0,
            is_stationary: true,
            critical_values: stationarity::CriticalValues::default(),
        };
        return match test_type {
            TestType::Combined => {
                reply_with_map(ctx, 4);
                reply_with_str(ctx, "test");
                reply_with_str(ctx, "combined");
                reply_with_str(ctx, "conclusion");
                reply_with_str(ctx, "stationary");
                reply_with_str(ctx, "adf");
                reply_with_map(ctx, 7);
                reply_result_fields(ctx, &const_result);
                reply_with_str(ctx, "kpss");
                reply_with_map(ctx, 7);
                reply_result_fields(ctx, &const_result);
                Ok(ValkeyValue::NoReply)
            }
            TestType::Adf => reply_single_test(ctx, &const_result, "adf"),
            TestType::Kpss => reply_single_test(ctx, &const_result, "kpss"),
        };
    }

    // Run test(s) and build response
    match test_type {
        TestType::Combined => reply_combined_test(ctx, &values),
        TestType::Adf => {
            let result = stationarity::adf_test(&values, lags);
            reply_single_test(ctx, &result, "adf")
        }
        TestType::Kpss => {
            let result = stationarity::kpss_test(&values, lags);
            reply_single_test(ctx, &result, "kpss")
        }
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TestType {
    Adf,
    Kpss,
    Combined,
}

/// Emit the map fields shared by all stationarity test results —
/// `statistic`, `pValue`, `lags`, `isStationary`, and the three
/// critical-value keys.  The caller is responsible for opening the map
/// and writing the `test` / `conclusion` fields before calling this.
fn reply_result_fields(ctx: &Context, result: &StationarityResult) {
    reply_with_str(ctx, "statistic");
    reply_with_double(ctx, result.statistic);

    reply_with_str(ctx, "pValue");
    reply_with_double(ctx, result.p_value);

    reply_with_str(ctx, "lags");
    reply_with_integer(ctx, result.lags as i64);

    reply_with_str(ctx, "isStationary");
    reply_with_integer(ctx, i64::from(result.is_stationary));

    reply_with_str(ctx, "cv1pct");
    reply_with_double(ctx, result.critical_values.cv_1pct);

    reply_with_str(ctx, "cv5pct");
    reply_with_double(ctx, result.critical_values.cv_5pct);

    reply_with_str(ctx, "cv10pct");
    reply_with_double(ctx, result.critical_values.cv_10pct);
}

fn reply_single_test(ctx: &Context, result: &StationarityResult, test_name: &str) -> ValkeyResult {
    reply_with_map(ctx, 9);

    reply_with_str(ctx, "test");
    reply_with_str(ctx, test_name);

    let conclusion = if result.is_stationary {
        "stationary"
    } else {
        "non_stationary"
    };
    reply_with_str(ctx, "conclusion");
    reply_with_str(ctx, conclusion);

    reply_result_fields(ctx, result);

    Ok(ValkeyValue::NoReply)
}

fn reply_combined_test(ctx: &Context, values: &[f64]) -> ValkeyResult {
    let (adf_result, kpss_result, conclusion) = stationarity::test_stationarity(values);

    // Top-level map: 4 keys — test, conclusion, adf (nested), kpss (nested)
    reply_with_map(ctx, 4);

    reply_with_str(ctx, "test");
    reply_with_str(ctx, "combined");

    reply_with_str(ctx, "conclusion");
    reply_with_str(ctx, conclusion);

    // ADF nested map — 7 keys: statistic, pValue, lags, isStationary, cv1pct, cv5pct, cv10pct
    reply_with_str(ctx, "adf");
    reply_with_map(ctx, 7);
    reply_result_fields(ctx, &adf_result);

    // KPSS nested map — 7 keys
    reply_with_str(ctx, "kpss");
    reply_with_map(ctx, 7);
    reply_result_fields(ctx, &kpss_result);

    Ok(ValkeyValue::NoReply)
}

fn parse_test_type(arg: &str) -> ValkeyResult<TestType> {
    match arg.len() {
        3 if arg.eq_ignore_ascii_case("adf") => Ok(TestType::Adf),
        4 if arg.eq_ignore_ascii_case("kpss") => Ok(TestType::Kpss),
        8 if arg.eq_ignore_ascii_case("combined") => Ok(TestType::Combined),
        _ => Err(ValkeyError::String(format!(
            "TSDB: invalid TEST value '{arg}'. Expected adf, kpss, or combined"
        ))),
    }
}
