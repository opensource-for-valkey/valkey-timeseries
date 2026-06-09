use crate::commands::parse_timestamp_range;
use crate::common::replies::{reply_with_array, reply_with_double, reply_with_integer};
use crate::error_consts;
use crate::series::get_timeseries;
use anofox_forecast::detection::period::{PeriodDetectionConfig, detect_periods};
use valkey_module::{
    AclPermissions, Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue,
};

/// ```text
/// TS.PERIODS key startTimestamp endTimestamp [MIN_STRENGTH minStrength] [DOMINANT]
/// ```
///
/// `TS.PERIODS` detects seasonal periods in a time series using periodogram-based
/// spectral analysis (with the `seasonal-detection` feature, the SAZED ensemble
/// from fdars-core is used instead).
///
/// By default, all detected periods are returned as an array of arrays with
/// metadata about each period (period, power, strength, acf, n_cycles).
///
/// If the `DOMINANT` option is specified, only the single dominant period
/// (integer) is returned, or `nil` if no significant period is found.
///
/// `MIN_STRENGTH` sets the minimum seasonal differencing strength (0–1) for a
/// period to be accepted. Default is 0.05. Set to 0 to disable strength filtering.
pub fn ts_periods_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 4 {
        return Err(ValkeyError::WrongArity);
    }

    let mut args = args.into_iter().skip(1).peekable();

    let key = args.next_arg()?;
    let date_range = parse_timestamp_range(&mut args)?;

    // Parse optional arguments: MIN_STRENGTH and DOMINANT
    let mut min_strength: Option<f64> = None;
    let mut dominant = false;

    while let Some(arg) = args.peek() {
        if arg.as_slice().eq_ignore_ascii_case(b"MIN_STRENGTH") {
            args.next(); // consume MIN_STRENGTH
            let val = args.next_arg()?;
            let strength = val
                .parse_float()
                .map_err(|_| ValkeyError::Str("TSDB: invalid MIN_STRENGTH value"))?;
            if strength < 0.0 || strength > 1.0 {
                return Err(ValkeyError::String(format!(
                    "TSDB: MIN_STRENGTH must be between 0 and 1, got {}",
                    strength
                )));
            }
            min_strength = Some(strength);
        } else if arg.as_slice().eq_ignore_ascii_case(b"DOMINANT") {
            args.next(); // consume DOMINANT
            dominant = true;
        } else {
            return Err(ValkeyError::String(format!(
                "TSDB: Unknown argument: {}",
                arg
            )));
        }
    }

    args.done()?;

    let samples = match get_timeseries(ctx, &key, Some(AclPermissions::ACCESS), false) {
        Ok(Some(series)) => {
            let (start, end) = date_range.get_series_range(&series, None, false);
            series.get_range(start, end)
        }
        Ok(None) => return Err(ValkeyError::Str(error_consts::KEY_NOT_FOUND)),
        Err(e) => return Err(e),
    };

    // Extract values
    let values: Vec<f64> = samples.iter().map(|s| s.value).collect();

    if values.len() < 4 {
        return Err(ValkeyError::Str(
            "TSDB: insufficient data for period detection. Need at least 4 samples.",
        ));
    }

    let config = PeriodDetectionConfig {
        min_strength: min_strength.unwrap_or(0.05),
        max_periods: if dominant { 1 } else { 5 },
        ..Default::default()
    };

    let periods = detect_periods(&values, &config);

    if dominant {
        match periods.first() {
            Some(p) => {
                reply_with_integer(ctx, p.period as i64);
            }
            None => {
                valkey_module::raw::reply_with_null(ctx.ctx);
            }
        }
    } else {
        reply_with_array(ctx, periods.len());
        for p in &periods {
            // Each period returned as an array: [period, power, strength, acf, n_cycles]
            reply_with_array(ctx, 5);
            reply_with_integer(ctx, p.period as i64);
            reply_with_double(ctx, p.power);
            reply_with_double(ctx, p.strength);
            reply_with_double(ctx, p.acf);
            reply_with_integer(ctx, p.n_cycles as i64);
        }
    }

    Ok(ValkeyValue::NoReply)
}
