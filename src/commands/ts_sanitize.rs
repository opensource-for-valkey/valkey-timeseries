use crate::analysis::forecasting::imputation::{ImputationPolicy, sanitize};
use crate::commands::command_parser::parse_timestamp_range;
use crate::common::Sample;
use crate::error_consts;
use crate::series::{DuplicatePolicy, get_timeseries_mut};
use anofox_forecast::detection::detect_dominant_period;
use valkey_module::{
    AclPermissions, Context, NextArg, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString,
    ValkeyValue,
};

/// ```text
/// TS.SANITIZE key fromTimestamp toTimestamp POLICY <policy> [options]
///```
/// Sanitizes missing (NaN/infinite) values in a time series within the given
/// timestamp range (inclusive).
///
/// POLICY is one of:
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
/// Returns the number of samples that were sanitized (imputed or dropped).
pub fn ts_sanitize_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 5 {
        return Err(ValkeyError::WrongArity);
    }

    let mut args = args.into_iter().skip(1).peekable();

    let key = args.next_arg()?;
    let date_range = parse_timestamp_range(&mut args)?;

    // POLICY keyword
    let policy_kw = args.next_str()?;
    if !policy_kw.eq_ignore_ascii_case("policy") {
        return Err(ValkeyError::Str(error_consts::INVALID_ARGUMENT));
    }

    // Get the series (must exist)
    let mut series = get_timeseries_mut(ctx, &key, true, Some(AclPermissions::UPDATE))?.unwrap();

    let (start_ts, end_ts) = date_range.get_series_range(&series, None, false);

    // Get existing samples in the range
    let mut samples = series.get_range(start_ts, end_ts);

    // Parse the policy name and any policy-specific options
    let policy_token = args.next_str()?.to_uppercase();
    let policy = parse_policy(&policy_token, &mut args, &samples)?;

    if samples.is_empty() {
        return Ok(ValkeyValue::from(0_i64));
    }

    // Determine the policy category before moving `policy` into sanitize().
    // - ReplaceAll: Drop, MovingAverage, Seasonal — need remove+reinsert
    // - InPlace: all others — modify in-place, merge map back
    enum PolicyCategory {
        ReplaceAll,
        InPlace,
    }

    let category = match &policy {
        ImputationPolicy::Drop
        | ImputationPolicy::MovingAverage(_)
        | ImputationPolicy::Seasonal(_) => PolicyCategory::ReplaceAll,
        _ => PolicyCategory::InPlace,
    };

    // For Drop specifically, we need to know if the policy is Drop to decide
    // whether to use `samples` (modified in-place) or `map` (full result).
    let is_drop = matches!(&policy, ImputationPolicy::Drop);

    // Apply the sanitization policy
    let map = sanitize(&mut samples, policy)
        .map_err(|e| ValkeyError::String(format!("TSDB: sanitize error: {e}")))?;

    let sanitized_count = map.len();

    if sanitized_count > 0 {
        match category {
            PolicyCategory::ReplaceAll => {
                // Remove the entire range and re-insert the sanitized samples
                series
                    .remove_range(start_ts, end_ts)
                    .map_err(|e| ValkeyError::String(format!("TSDB: {e}")))?;

                // For Drop: samples has been filtered to only valid entries.
                // For MovingAverage/Seasonal: map is the full imputed result.
                let to_merge = if is_drop { &samples } else { &map };

                if !to_merge.is_empty() {
                    let mut sorted = to_merge.to_vec();
                    sorted.sort_by_key(|s| s.timestamp);
                    series
                        .merge_samples(&sorted, Some(DuplicatePolicy::KeepLast))
                        .map_err(|e| ValkeyError::String(format!("TSDB: {e}")))?;
                }
            }
            PolicyCategory::InPlace => {
                // Merge the imputed samples back into the series.
                // Since these samples share timestamps with existing samples,
                // they will update the values in-place via upsert with KeepLast.
                let mut sorted = map.clone();
                sorted.sort_by_key(|s| s.timestamp);
                series
                    .merge_samples(&sorted, Some(DuplicatePolicy::KeepLast))
                    .map_err(|e| ValkeyError::String(format!("TSDB: {e}")))?;
            }
        }

        handle_replication(ctx, &key, is_drop);
    }

    Ok(ValkeyValue::from(sanitized_count as i64))
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

fn handle_replication(ctx: &Context, key: &ValkeyString, is_drop: bool) {
    ctx.replicate_verbatim();
    let action = if is_drop { "ts.del" } else { "ts.add" };
    ctx.notify_keyspace_event(NotifyEvent::MODULE, action, key);
}

fn infer_seasonal_period(args: &[Sample]) -> ValkeyResult<usize> {
    let values: Vec<f64> = args.iter().map(|s| s.value).collect();
    detect_dominant_period(&values).ok_or(ValkeyError::Str(
        "TSDB: unable to detect dominant period for seasonal imputation",
    ))
}
