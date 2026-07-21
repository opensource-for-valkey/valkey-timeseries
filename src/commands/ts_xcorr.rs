use crate::commands::command_parser::parse_timestamp_range;
use crate::common::replies::{
    reply_with_array, reply_with_double, reply_with_integer, reply_with_map, reply_with_str,
};
use crate::error_consts;
use crate::join::{JoinOptions, JoinResultType, process_join};
use crate::series::get_timeseries;
use anofox_forecast::simd;
use joinkit::EitherOrBoth;
use valkey_module::{
    AclPermissions, Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue,
};

/// ```text
/// TS.XCORR key1 key2 fromTimestamp toTimestamp maxLag
/// ```
///
/// `TS.XCORR` computes the cross-correlation function (CCF) between two time series
/// over a range of lags `-maxLag..=maxLag`.
///
/// The two series are first timestamp-aligned via an inner join (only samples with
/// matching timestamps in both series are used — see `TS.JOIN`). The Pearson
/// correlation coefficient is then computed between the aligned value sequences at
/// each lag.
///
/// Lag convention: for lag `h`, `key1[i]` is compared against `key2[i + h]`.
/// - `h > 0`: `key1` at time `t` is compared to `key2` at time `t + h` — a positive
///   peak means `key1` **leads** `key2` by `h` steps.
/// - `h < 0`: `key2` at time `t` is compared to `key1` at time `t + |h|` — a positive
///   peak means `key2` **leads** `key1` by `|h|` steps.
/// - `h == 0`: contemporaneous correlation.
///
/// Returns a map with:
/// - `lags` — array of integers, `-maxLag..=maxLag`
/// - `values` — array of doubles, the correlation coefficient at each corresponding lag
/// - `peak_lag` — the lag with the largest absolute correlation
/// - `peak_correlation` — the (signed) correlation value at `peak_lag`
/// - `n` — the number of timestamp-aligned sample pairs used
#[valkey_module_macros::command({
    name: "TS.XCORR",
    flags: [ReadOnly, DenyOOM],
    summary: "Compute the cross-correlation function between two time series.",
    complexity: "O(N*L) where N is the number of timestamp-aligned sample pairs and L is the number of lags (2*maxLag+1).",
    since: "1.0.0",
    arity: -6,
    key_spec: [{
        flags: [ReadOnly, Access],
        begin_search: Index({ index: 1 }),
        find_keys: Range({ last_key: 1, steps: 1, limit: 0 })
    }]
})]
pub fn ts_xcorr_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 6 {
        return Err(ValkeyError::WrongArity);
    }

    let mut args = args.into_iter().skip(1).peekable();

    let key1 = args.next_arg()?;
    let key2 = args.next_arg()?;

    if key1 == key2 {
        return Err(ValkeyError::Str(error_consts::DUPLICATE_JOIN_KEYS));
    }

    let date_range = parse_timestamp_range(&mut args)?;

    let maxlag_arg = args.next_arg()?;
    let maxlag: i64 = maxlag_arg.parse_integer().map_err(|_| {
        ValkeyError::Str("TSDB: invalid MAXLAG value, expected a non-negative integer")
    })?;
    if maxlag < 0 {
        return Err(ValkeyError::Str(
            "TSDB: MAXLAG must be a non-negative integer",
        ));
    }
    let maxlag = maxlag as usize;

    args.done()?;

    // Both series must exist; propagate an error otherwise (must_exist = true).
    let left_series = get_timeseries(ctx, &key1, Some(AclPermissions::ACCESS), true)?.unwrap();
    let right_series = get_timeseries(ctx, &key2, Some(AclPermissions::ACCESS), true)?.unwrap();

    let options = JoinOptions {
        date_range,
        ..Default::default()
    };

    // Default join_type is Inner, so every element of the result is a matched
    // (Both) pair — the timestamp alignment that a Pearson CCF requires.
    let (x, y): (Vec<f64>, Vec<f64>) = match process_join(&left_series, &right_series, &options)? {
        JoinResultType::Values(values) => values
            .iter()
            .filter_map(|jv| match jv.0 {
                EitherOrBoth::Both(l, r) => Some((l.value, r.value)),
                _ => None,
            })
            .unzip(),
        JoinResultType::Samples(_) => unreachable!("TS.XCORR does not set a join reducer"),
    };

    let n = x.len();
    if n < maxlag + 2 {
        return Err(ValkeyError::String(format!(
            "TSDB: insufficient aligned samples for MAXLAG {maxlag}. Need at least {} \
             timestamp-aligned samples between the two series, got {n}",
            maxlag + 2
        )));
    }

    let maxlag = maxlag as i64;
    let width = (2 * maxlag + 1) as usize;
    let mut lags: Vec<i64> = Vec::with_capacity(width);
    let mut values: Vec<f64> = Vec::with_capacity(width);
    let mut peak_lag = 0i64;
    let mut peak_correlation = f64::NAN;

    for h in -maxlag..=maxlag {
        let corr = match h.cmp(&0) {
            std::cmp::Ordering::Equal => simd::correlation(&x, &y),
            std::cmp::Ordering::Greater => {
                let h = h as usize;
                simd::correlation(&x[..n - h], &y[h..])
            }
            std::cmp::Ordering::Less => {
                let k = (-h) as usize;
                simd::correlation(&x[k..], &y[..n - k])
            }
        };

        if peak_correlation.is_nan() || corr.abs() > peak_correlation.abs() {
            peak_correlation = corr;
            peak_lag = h;
        }

        lags.push(h);
        values.push(corr);
    }

    reply_with_map(ctx, 5);

    reply_with_str(ctx, "lags");
    reply_with_array(ctx, lags.len());
    for &l in &lags {
        reply_with_integer(ctx, l);
    }

    reply_with_str(ctx, "values");
    reply_with_array(ctx, values.len());
    for &v in &values {
        reply_with_double(ctx, v);
    }

    reply_with_str(ctx, "peak_lag");
    reply_with_integer(ctx, peak_lag);

    reply_with_str(ctx, "peak_correlation");
    reply_with_double(ctx, peak_correlation);

    reply_with_str(ctx, "n");
    reply_with_integer(ctx, n as i64);

    Ok(ValkeyValue::NoReply)
}
