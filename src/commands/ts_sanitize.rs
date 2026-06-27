use crate::analysis::forecasting::imputation::{ImputationPolicy, sanitize};
use crate::commands::command_parser::{
    CommandArgToken, parse_command_arg_token, parse_store_clause,
};
use crate::commands::parse_series_range_samples;
use crate::common::Sample;
use crate::common::replies::reply_with_samples;
use crate::error_consts;
use crate::series::create_or_update_series_with_samples;
use anofox_forecast::detection::detect_dominant_period;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

/// ```text
/// TS.SANITIZE key fromTimestamp toTimestamp
///     [POLICY <policy> [options]]
///     [STORE destKey
///         [MERGE]
///         [RETENTION retentionPeriod]
///         [ENCODING encoding]
///         [CHUNK_SIZE chunkSize]
///         [DUPLICATE_POLICY duplicatePolicy]
///         [SIGNIFICANT_DIGITS significantDigits | DECIMAL_DIGITS decimalDigits]
///         [METRIC metric]
///         [IGNORE ignoreMaxTimediff ignoreMaxValDiff]
///     ]
///```
/// Sanitizes missing (NaN/infinite) values in a time series within the given
/// timestamp range (inclusive).
///
/// POLICY is optional and defaults to DROP. When specified, POLICY is one of:
///   - DROP                   - Drop all samples with missing values.
///   - FILL value             - Replace missing values with a constant fill value.
///   - FORWARDFILL            - Forward-fill missing values.
///   - BACKWARDFILL           - Backward-fill missing values.
///   - FILLMEAN               - Replace missing with the mean of valid values.
///   - FILLMEDIAN             - Replace missing with the median of valid values.
///   - INTERPOLATE            - Linearly interpolate between valid neighbors.
///   - FORWARDBACKWARDFILL    - Forward-fill then backward-fill.
///   - MOVINGAVERAGE window   - Replace with moving average (window must be odd > 0).
///   - SEASONAL period|<auto> - Replace with seasonal median (period must be > 0).
///
/// If STORE is specified, results are written to the destination key instead of
/// being returned inline. With MERGE, samples are merged into an existing
/// destination series; without MERGE (overwrite mode), the destination is
/// cleared first. Returns the number of samples written.
///
/// Without STORE, returns the number of samples that were sanitized (imputed or dropped).
pub fn ts_sanitize_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 4 {
        return Err(ValkeyError::WrongArity);
    }

    let mut args = args.into_iter().skip(1).peekable();

    // Get the time series and extract sample values
    let mut samples = parse_series_range_samples(ctx, &mut args)?;

    // POLICY is optional; defaults to DROP.
    let policy = if args
        .peek()
        .is_some_and(|s| s.as_slice().eq_ignore_ascii_case(b"policy"))
    {
        args.next(); // consume POLICY
        let policy_token = args.next_str()?.to_uppercase();
        parse_policy(&policy_token, &mut args, &samples)?
    } else {
        ImputationPolicy::Drop
    };

    // STORE destination (optional)
    let destination = if args
        .peek()
        .is_some_and(|s| parse_command_arg_token(s) == Some(CommandArgToken::Store))
    {
        args.next(); // consume STORE
        Some(parse_store_clause(&mut args)?)
    } else {
        None
    };

    // Capture policy variant before moving `policy` into sanitize().
    // - is_ma_or_seasonal: samples is NOT modified; `map` is the full imputed result.
    // - InPlace (all others): samples is modified in-place; `map` has changed entries.
    let is_ma_or_seasonal = matches!(
        &policy,
        ImputationPolicy::MovingAverage(_) | ImputationPolicy::Seasonal(_)
    );

    // Apply the sanitization policy
    let sanitized = sanitize(&mut samples, policy)
        .map_err(|e| ValkeyError::String(format!("TSDB: sanitize error: {e}")))?;

    // - MovingAverage/Seasonal: sanitized is the full imputed result.
    // - All others (including Drop): samples has been modified in-place.
    let to_return: &Vec<Sample> = if is_ma_or_seasonal {
        &sanitized
    } else {
        &samples
    };

    if let Some(dest) = destination {
        let written = create_or_update_series_with_samples(
            ctx,
            &dest.key,
            Some(dest.options),
            dest.write_mode,
            to_return,
            None,
        )?;

        return Ok(ValkeyValue::from(written));
    }

    reply_with_samples(ctx, to_return.iter().cloned());
    Ok(ValkeyValue::NoReply)
}

/// Parse the imputation policy and any policy-specific arguments.
fn parse_policy(
    token: &str,
    args: &mut impl Iterator<Item = ValkeyString>,
    samples: &[Sample],
) -> ValkeyResult<ImputationPolicy> {
    let mut policy = ImputationPolicy::Error;
    hashify::fnc_map_ignore_case!(
        token.as_bytes(),
        "Drop" => { policy = ImputationPolicy::Drop; },
        "Fill" => {
            let value_str = args.next_str()?;
            let value: f64 = value_str
                .parse()
                .map_err(|_| ValkeyError::Str("TSDB: invalid fill value"))?;
            policy = ImputationPolicy::Fill(value);
        },
        "ForwardFill" => { policy = ImputationPolicy::ForwardFill; },
        "BackwardFill" => { policy = ImputationPolicy::BackwardFill; },
        "FillMean" => { policy = ImputationPolicy::FillMean; },
        "FillMedian" => { policy = ImputationPolicy::FillMedian; },
        "Interpolate" => { policy = ImputationPolicy::Interpolate; },
        "ForwardBackwardFill" => { policy = ImputationPolicy::ForwardBackwardFill; },
        "MovingAverage" => {
            let window_str = args.next_str()?;
            let window: usize = window_str
                .parse()
                .map_err(|_| ValkeyError::Str("TSDB: invalid MovingAverage window"))?;
            if window == 0 || window.is_multiple_of(2) {
                return Err(ValkeyError::Str("TSDB: MovingAverage window must be an odd positive integer"));
            }
            policy = ImputationPolicy::MovingAverage(window);
        },
        "Seasonal" => {
            let period_str = args.next_str()?;
            let period = if period_str.eq_ignore_ascii_case("auto") {
                infer_seasonal_period(samples)?
            } else {
                period_str
                    .parse()
                    .map_err(|_| ValkeyError::Str("TSDB: invalid Seasonal period"))?
            };
            if period == 0 {
                return Err(ValkeyError::Str("TSDB: Seasonal period must be a positive integer"));
            }
            policy = ImputationPolicy::Seasonal(period);
        },
        _ => { return Err(ValkeyError::Str(error_consts::INVALID_ARGUMENT)); }
    );
    Ok(policy)
}

fn infer_seasonal_period(args: &[Sample]) -> ValkeyResult<usize> {
    let values: Vec<f64> = args.iter().map(|s| s.value).collect();
    detect_dominant_period(&values).ok_or(ValkeyError::Str(
        "TSDB: unable to detect dominant period for seasonal imputation",
    ))
}
