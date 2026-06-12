use crate::analysis::forecasting::features::{
    FeatureCategory, compute_features_map, parse_feature,
};
use crate::commands::parse_timestamp_range;
use crate::common::replies::{
    ThreadSafeReplyContext, block_client, reply_with_double, reply_with_map, reply_with_str,
};
use crate::common::threads::spawn;
use crate::error_consts;
use crate::series::get_timeseries;
use anofox_forecast::features::Feature;
use valkey_module::{
    AclPermissions, Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue,
};

/// ```text
/// TS.FEATURES key startTimestamp endTimestamp
///     [CATEGORY <basic|distribution|autocorrelation|trend>,..]
///     [FEATURE feature1,feature2,feature3..]
/// ```
///
/// `TS.FEATURES` computes a set of statistical features on a time series.
///
/// Categories:
/// - `basic`: mean, median, variance, variance_sample, minimum, maximum, length
/// - `distribution`: skewness, kurtosis, quantiles (0.25, 0.5, 0.75)
/// - `autocorrelation`: autocorrelation at lags 1, 2, 3
/// - `trend`: linear trend intercept, slope, p-value, r-squared
///
/// Features are specified by name (case-insensitive). Parameterized features
/// use the format `name:value`:
/// - `quantile:<q>` — q is a float between 0.0 and 1.0
/// - `autocorrelation:<lag>` — lag is a positive integer
/// - `partial_autocorrelation:<lag>` or `pacf:<lag>` — lag is a positive integer
///
/// The final feature list is the union of features from CATEGORY and FEATURE,
/// with duplicates removed.
///
/// Returns a map of `{feature_name: value}`.
pub fn ts_features_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 4 {
        return Err(ValkeyError::WrongArity);
    }

    let mut args = args.into_iter().skip(1).peekable();

    let key = args.next_arg()?;
    let date_range = parse_timestamp_range(&mut args)?;

    // Parse optional CATEGORY and FEATURE arguments
    let mut categories: Vec<FeatureCategory> = Vec::new();
    let mut features: Vec<Feature> = Vec::new();

    while args.peek().is_some() {
        let arg = args.peek().unwrap();
        let arg_str = arg
            .try_as_str()
            .map_err(|_| ValkeyError::Str("TSDB: invalid argument"))?;

        match arg_str.to_uppercase().as_str() {
            "CATEGORY" => {
                args.next(); // consume CATEGORY
                let cat_str = args.next_str()?;
                categories = parse_categories(cat_str)?;
            }
            "FEATURE" => {
                args.next(); // consume FEATURE
                let feat_str = args.next_str()?;
                features = parse_features(feat_str)?;
            }
            other => {
                return Err(ValkeyError::String(format!(
                    "TSDB: unrecognized argument '{}'",
                    other
                )));
            }
        }
    }

    // If no categories or features specified, return error
    if categories.is_empty() && features.is_empty() {
        return Err(ValkeyError::Str(
            "TSDB: at least one of CATEGORY or FEATURE must be specified",
        ));
    }

    // Collect features from categories
    for cat in &categories {
        features.extend_from_slice(cat.features());
    }

    // Deduplicate by canonical name (keeping first occurrence)
    let mut seen = std::collections::HashSet::new();
    let unique_features: Vec<Feature> = features
        .into_iter()
        .filter(|f| seen.insert(f.name()))
        .collect();

    if unique_features.is_empty() {
        return Err(ValkeyError::Str("TSDB: no features to compute"));
    }

    // Get the time series and extract sample values
    let series = match get_timeseries(ctx, &key, Some(AclPermissions::ACCESS), false) {
        Ok(Some(series)) => series,
        Ok(None) => return Err(ValkeyError::Str(error_consts::KEY_NOT_FOUND)),
        Err(e) => return Err(e),
    };

    let (start, end) = date_range.get_series_range(&series, None, false);
    let samples = series.get_range(start, end);

    if samples.is_empty() {
        return Err(ValkeyError::Str(
            "TSDB: no samples in the specified time range",
        ));
    }

    let values: Vec<f64> = samples.iter().map(|s| s.value).collect();

    // Run feature computation on a background thread
    let blocked_client = block_client(ctx);
    spawn(move || {
        let thread_ctx = ThreadSafeReplyContext::with_blocked_client(blocked_client);

        let result_map = compute_features_map(&values, &unique_features);

        let map_len = result_map.len();
        reply_with_map(&thread_ctx, map_len);

        for (name, value) in &result_map {
            reply_with_str(&thread_ctx, name);
            if value.is_nan() {
                crate::common::replies::reply_with_null(&thread_ctx);
            } else {
                reply_with_double(&thread_ctx, *value);
            }
        }
    });

    // Reply will be sent from the background thread
    Ok(ValkeyValue::NoReply)
}

/// Parse a comma-separated list of category names, rejecting duplicates.
fn parse_categories(input: &str) -> Result<Vec<FeatureCategory>, ValkeyError> {
    let mut categories = Vec::new();
    let mut seen = std::collections::HashSet::new();

    for part in input.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let category = FeatureCategory::try_from(part)
            .map_err(|e| ValkeyError::String(format!("TSDB: {e}")))?;

        if !seen.insert(category) {
            return Err(ValkeyError::String(format!(
                "TSDB: duplicate category '{}'",
                category.as_str()
            )));
        }

        categories.push(category);
    }

    if categories.is_empty() {
        return Err(ValkeyError::Str("TSDB: empty category list"));
    }

    Ok(categories)
}

/// Parse a comma-separated list of feature names.
fn parse_features(input: &str) -> Result<Vec<Feature>, ValkeyError> {
    let mut features = Vec::new();

    for part in input.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let feature = parse_feature(part).map_err(|e| ValkeyError::String(format!("TSDB: {e}")))?;
        features.push(feature);
    }

    if features.is_empty() {
        return Err(ValkeyError::Str("TSDB: empty feature list"));
    }

    Ok(features)
}
