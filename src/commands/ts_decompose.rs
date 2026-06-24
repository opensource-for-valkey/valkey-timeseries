use crate::analysis::seasonality::{
    Seasonality,
};
use crate::commands::command_parser::parse_series_range_samples;
use crate::commands::CommandArgIterator;
use crate::common::replies::{
    reply_with_array, reply_with_double, reply_with_integer, reply_with_str,
};
use anofox_forecast::detection::{detect_periods, PeriodDetectionConfig};
use anofox_forecast::seasonality::{MSTL, STL, MSTLResult, STLResult};
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

const MAX_SEASONALITY_PERIODS: usize = 4;

/// ```text
/// TS.DECOMPOSE key startTimestamp endTimestamp
///     [SEASONALITY "auto"|period [period...]]
/// ```
///
/// `TS.DECOMPOSE` decomposes a time series into its constituent components:
/// trend, seasonality, and residual.
///
/// If SEASONALITY is "auto", the seasonal period(s) are automatically inferred
/// from the data.
///
/// If a single period is specified, STL decomposition is used.
/// If multiple periods are specified, MSTL decomposition is used.
pub fn ts_decompose_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 4 {
        return Err(ValkeyError::WrongArity);
    }

    let mut args = args.into_iter().skip(1).peekable();

    // Get the time series and extract sample values
    let samples = parse_series_range_samples(ctx, &mut args)?;
    let values: Vec<f64> = samples.iter().map(|s| s.value).collect();

    let seasonality = parse_seasonality(&mut args)?;

    args.done()?;

    // Determine periods
    let periods = match &seasonality {
        Seasonality::Periods(periods) => periods.clone(),
        Seasonality::Auto => {
            let config = PeriodDetectionConfig::default();
            let periods = detect_periods(&values, &config);
            periods.iter().map(|p| p.period).collect()
        }
    };

    if periods.is_empty() {
        return Err(ValkeyError::Str(
            "TSDB: at least one seasonality period is required",
        ));
    }

    let n = values.len();
    let timestamps: Vec<i64> = samples.iter().map(|s| s.timestamp).collect();

    if periods.len() == 1 {
        // Single period: use STL
        let period = periods[0];
        if n < 2 * period {
            return Err(ValkeyError::String(format!(
                "TSDB: insufficient data for STL decomposition. Need at least {} samples, got {}",
                2 * period,
                n
            )));
        }

        let result = STL::new(period)
            .robust()
            .decompose(&values)
            .ok_or(ValkeyError::Str("TSDB: STL decomposition failed"))?;

        reply_stl_result(ctx, &timestamps, &values, &result)
    } else {
        // Multiple periods: use MSTL
        let max_period = *periods.iter().max().unwrap_or(&0);
        if n < 2 * max_period {
            return Err(ValkeyError::String(format!(
                "TSDB: insufficient data for MSTL decomposition. Need at least {} samples, got {}",
                2 * max_period,
                n
            )));
        }

        let result = MSTL::new(periods)
            .robust()
            .decompose(&values)
            .ok_or(ValkeyError::Str("TSDB: MSTL decomposition failed"))?;

        reply_mstl_result(ctx, &timestamps, &values, &result)
    }
}

fn parse_seasonality(args: &mut CommandArgIterator) -> ValkeyResult<Seasonality> {
    // If no more args, default to Auto
    let Some(arg) = args.peek() else {
        return Ok(Seasonality::Auto);
    };

    // Check if the next argument is SEASONALITY
    if !arg.as_slice().eq_ignore_ascii_case(b"SEASONALITY") {
        // No explicit SEASONALITY keyword — default to Auto
        return Ok(Seasonality::Auto);
    }

    args.next(); // consume SEASONALITY

    // Check for "auto"
    if let Some(next_arg) = args.peek()
        && next_arg.as_slice().eq_ignore_ascii_case(b"auto")
    {
        args.next(); // consume auto
        return Ok(Seasonality::Auto);
    }

    let mut periods: Vec<usize> = Vec::with_capacity(4);

    // Loop while the next token is a number
    while let Some(v) = args.peek() {
        if let Ok(value) = v.parse_unsigned_integer() {
            periods.push(value as usize);
            args.next();
            continue;
        }
        break;
    }

    if periods.is_empty() || periods.len() > MAX_SEASONALITY_PERIODS {
        return Err(ValkeyError::Str(
            "TSDB: invalid SEASONALITY periods. Expected 1-4 period values or 'auto'",
        ));
    }

    // Periods should be unique and sorted
    periods.sort_unstable();
    if !periods.windows(2).all(|w| w[0] != w[1]) {
        return Err(ValkeyError::Str("TSDB: SEASONALITY periods must be unique"));
    }

    Ok(Seasonality::Periods(periods))
}

/// Reply with STL decomposition result.
///
/// Response format (array of 4):
///   "original" -> [[ts, val], ...]
///   "trend" -> [[ts, val], ...]
///   "seasonal" -> [[ts, val], ...]
///   "residual" -> [[ts, val], ...]
fn reply_stl_result(
    ctx: &Context,
    timestamps: &[i64],
    original: &[f64],
    result: &STLResult,
) -> ValkeyResult {
    reply_with_array(ctx, 8);

    // original
    reply_with_str(ctx, "original");
    reply_sample_array(ctx, timestamps, original);

    // trend
    reply_with_str(ctx, "trend");
    reply_sample_array(ctx, timestamps, &result.trend);

    // seasonal
    reply_with_str(ctx, "seasonal");
    reply_sample_array(ctx, timestamps, &result.seasonal);

    // residual
    reply_with_str(ctx, "residual");
    reply_sample_array(ctx, timestamps, &result.remainder);

    Ok(ValkeyValue::NoReply)
}

/// Reply with MSTL decomposition result.
///
/// Response format (array of 8):
///   "original"              -> [[ts, val], ...]
///   "trend"                 -> [[ts, val], ...]
///   "seasonal_components"   -> [ [period, [[ts, val], ...]], ... ]
///   "residual"              -> [[ts, val], ...]
fn reply_mstl_result(
    ctx: &Context,
    timestamps: &[i64],
    original: &[f64],
    result: &MSTLResult,
) -> ValkeyResult {
    reply_with_array(ctx, 8);

    // original
    reply_with_str(ctx, "original");
    reply_sample_array(ctx, timestamps, original);

    // trend
    reply_with_str(ctx, "trend");
    reply_sample_array(ctx, timestamps, &result.trend);

    // seasonal_components (array of [period, [samples]])
    reply_with_str(ctx, "seasonal_components");
    reply_with_array(ctx, result.seasonal_components.len());
    for (idx, seasonal) in result.seasonal_components.iter().enumerate() {
        reply_with_array(ctx, 2);
        reply_with_integer(ctx, result.seasonal_periods[idx] as i64);
        reply_sample_array(ctx, timestamps, seasonal);
    }

    // residual
    reply_with_str(ctx, "residual");
    reply_sample_array(ctx, timestamps, &result.remainder);

    Ok(ValkeyValue::NoReply)
}

/// Reply with an array of [timestamp, value] pairs.
fn reply_sample_array(ctx: &Context, timestamps: &[i64], values: &[f64]) {
    reply_with_array(ctx, timestamps.len());
    for (ts, val) in timestamps.iter().zip(values.iter()) {
        reply_with_array(ctx, 2);
        reply_with_integer(ctx, *ts);
        reply_with_double(ctx, *val);
    }
}
