use crate::analysis::forecasting::stats::{SeriesStats, calculate_stats};
use crate::commands::parse_timestamp_range;
use crate::error_consts;
use crate::series::get_timeseries;
use std::collections::HashMap;
use valkey_module::redisvalue::ValkeyValueKey;
use valkey_module::{
    AclPermissions, Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue,
};

/// ```text
/// TS.STATS key [fromTimestamp toTimestamp]
/// ```
///
/// Computes per-series statistical metrics for exploratory data analysis.
///
/// If no timestamp range is specified, stats are computed over the entire series.
///
/// Returns an array of key-value pairs with the following fields:
///
/// | Field                  | Type    | Description                                   |
/// |------------------------|---------|-----------------------------------------------|
/// | length                 | Integer | Number of observations in the range           |
/// | start_timestamp        | Integer | First timestamp in the range                  |
/// | end_timestamp          | Integer | Last timestamp in the range                   |
/// | mean                   | Float   | Average value (excluding NaN/Inf)             |
/// | std                    | Float   | Population standard deviation                 |
/// | min                    | Float   | Minimum value                                 |
/// | max                    | Float   | Maximum value                                 |
/// | range                  | Float   | Difference between max and min                |
/// | median                 | Float   | Median value                                  |
/// | n_nans                 | Integer | Count of NaN/Inf (null) values                |
/// | n_zeros                | Integer | Count of exactly-zero values                  |
/// | n_positive             | Integer | Count of positive values                      |
/// | n_negative             | Integer | Count of negative values                      |
/// | n_unique_values        | Integer | Count of distinct non-null values             |
/// | is_constant            | Integer | 1 if all non-null values are identical, else 0|
/// | plateau_size           | Integer | Longest run of consecutive identical values   |
/// | plateau_size_non_zero  | Integer | Longest run of consecutive identical non-zero |
/// | n_zeros_start          | Integer | Count of leading zeros                        |
/// | n_zeros_end            | Integer | Count of trailing zeros                       |
/// | skewness               | Float   | Sample skewness                               |
/// | kurtosis               | Float   | Sample excess kurtosis                        |
#[valkey_module_macros::command({
    name: "TS.STATS",
    flags: [ReadOnly, DenyOOM],
    summary: "Compute descriptive statistics for a time series.",
    complexity: "O(N log N) where N is the number of samples in the range.",
    since: "1.0.0",
    arity: -2,
    key_spec: [{
        flags: [ReadOnly, Access],
        begin_search: Index({ index: 1 }),
        find_keys: Range({ last_key: 0, steps: 1, limit: 0 })
    }]
})]
pub fn ts_stats_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 2 {
        return Err(ValkeyError::WrongArity);
    }

    let mut args = args.into_iter().skip(1).peekable();

    let key = args.next_arg()?;

    // Optional timestamp range
    let date_range = if args.peek().is_some() {
        Some(parse_timestamp_range(&mut args)?)
    } else {
        None
    };

    args.done()?;

    let series = get_timeseries(ctx, &key, Some(AclPermissions::ACCESS), false)?;
    let series = series.ok_or(ValkeyError::Str(error_consts::KEY_NOT_FOUND))?;

    let (start_ts, end_ts) = if let Some(ref dr) = date_range {
        dr.get_series_range(&series, None, false)
    } else {
        (series.first_timestamp, series.last_timestamp())
    };

    let samples = series.get_range(start_ts, end_ts);
    let values = samples
        .iter()
        .filter_map(|s| {
            if s.value.is_finite() {
                Some(s.value)
            } else {
                None
            }
        })
        .collect::<Vec<f64>>();

    let mut stats = calculate_stats(&values);

    stats.length = samples.len();
    stats.n_nans = samples.len() - values.len();
    stats.start_timestamp = samples.first().map_or(0, |s| s.timestamp);
    stats.end_timestamp = samples.last().map_or(0, |s| s.timestamp);

    Ok(stats_to_response(&stats))
}

// ---------------------------------------------------------------------------
// Response serialization — converts `SeriesStats` into a `ValkeyValue` map.
// ---------------------------------------------------------------------------

/// Build the Valkey response map from a `SeriesStats` struct.
fn stats_to_response(stats: &SeriesStats) -> ValkeyValue {
    let mut map: HashMap<ValkeyValueKey, ValkeyValue> = HashMap::new();

    map.insert("length".into(), ValkeyValue::Integer(stats.length as i64));
    map.insert(
        "start_timestamp".into(),
        ValkeyValue::Integer(stats.start_timestamp),
    );
    map.insert(
        "end_timestamp".into(),
        ValkeyValue::Integer(stats.end_timestamp),
    );
    map.insert("mean".into(), ValkeyValue::Float(stats.mean));
    map.insert("std".into(), ValkeyValue::Float(stats.std));
    map.insert("min".into(), ValkeyValue::Float(stats.min));
    map.insert("max".into(), ValkeyValue::Float(stats.max));
    map.insert("median".into(), ValkeyValue::Float(stats.median));
    map.insert("range".into(), ValkeyValue::Float(stats.range));
    map.insert("n_nans".into(), ValkeyValue::Integer(stats.n_nans as i64));
    map.insert("n_zeros".into(), ValkeyValue::Integer(stats.n_zeros as i64));
    map.insert(
        "n_positive".into(),
        ValkeyValue::Integer(stats.n_positive as i64),
    );
    map.insert(
        "n_negative".into(),
        ValkeyValue::Integer(stats.n_negative as i64),
    );
    map.insert(
        "n_unique_values".into(),
        ValkeyValue::Integer(stats.n_unique_values as i64),
    );
    map.insert(
        "is_constant".into(),
        ValkeyValue::Integer(if stats.is_constant { 1 } else { 0 }),
    );
    map.insert(
        "plateau_size".into(),
        ValkeyValue::Integer(stats.plateau_size as i64),
    );
    map.insert(
        "plateau_size_non_zero".into(),
        ValkeyValue::Integer(stats.plateau_size_non_zero as i64),
    );
    map.insert(
        "n_zeros_start".into(),
        ValkeyValue::Integer(stats.n_zeros_start as i64),
    );
    map.insert(
        "n_zeros_end".into(),
        ValkeyValue::Integer(stats.n_zeros_end as i64),
    );
    map.insert("skewness".into(), ValkeyValue::Float(stats.skewness));
    map.insert("kurtosis".into(), ValkeyValue::Float(stats.kurtosis));

    ValkeyValue::Map(map)
}
