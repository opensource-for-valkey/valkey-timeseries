use crate::common::Timestamp;
use crate::common::humanize::humanize_duration;
use crate::promql::EvalResult;
use num_traits::Zero;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::Duration;

const EMPTY_STRING: &str = "";

/// The maximum interval without previous rows.
pub const MAX_SILENCE_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Default, Clone, Debug)]
pub struct RollupWindow<'a> {
    /// The value preceding values if it fits the staleness interval.
    pub(super) prev_value: f64,

    /// The timestamp for prev_value.
    pub(super) prev_timestamp: Timestamp,

    /// Values that fit the window ending at curr_timestamp.
    pub(crate) values: &'a [f64],

    /// Timestamps for values.
    pub(crate) timestamps: &'a [Timestamp],

    /// Real value preceding value
    /// Populated if the preceding value is within the staleness interval.
    pub(super) real_prev_value: f64,

    /// Real value that goes after values.
    pub(crate) real_next_value: f64,

    /// Current timestamp for rollup evaluation.
    pub(super) curr_timestamp: Timestamp,

    /// Index for the currently evaluated point relative to the time range for query evaluation.
    pub(super) idx: usize,

    /// Time window for rollup calculations.
    pub(super) window: i64,
}

pub(crate) type RollupFunc = fn(rfa: &RollupWindow) -> f64;

#[derive(Clone)]
pub(crate) struct RollupConfig {
    pub start: Timestamp,
    pub end: Timestamp,
    pub step: Duration,
    pub window: Duration,

    /// Whether the window may be adjusted to 2 x interval between data points.
    /// This is needed for functions which have dt in the denominator
    /// such as rate, deriv, etc.
    /// Without the adjustment, their value would jump in unexpected directions
    /// when using a window smaller than 2 x `scrape_interval`.
    pub may_adjust_window: bool,

    pub timestamps: Arc<Vec<i64>>,

    /// lookback_delta is the analog to `-query.lookback-delta` from the Prometheus world.
    pub lookback_delta: Duration,

    /// The maximum number of points that can be generated per each series.
    pub max_points_per_series: usize,

    /// The minimum interval for staleness calculations. This could be useful for removing gaps on
    /// graphs generated from time series with irregular intervals between samples.
    pub min_staleness_interval: Duration,

    /// The estimated number of samples scanned per Func call.
    ///
    /// If zero, then it is considered that Func scans all the samples passed to it.
    pub samples_scanned_per_call: usize,
}

impl Default for RollupConfig {
    fn default() -> Self {
        Self {
            start: 0,
            end: 0,
            step: Duration::ZERO,
            window: Duration::ZERO,
            may_adjust_window: false,
            timestamps: Arc::new(vec![]),
            lookback_delta: Duration::ZERO,
            max_points_per_series: 0,
            min_staleness_interval: Duration::ZERO,
            samples_scanned_per_call: 0,
        }
    }
}

impl Display for RollupConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RollupConfig(start={}, end={}, step={}, window={}, points={}, max_points_per_series={})",
            self.start,
            self.end,
            humanize_duration(&self.step),
            humanize_duration(&self.window),
            self.timestamps.len(),
            self.max_points_per_series
        )
    }
}

/// calculates rollup for the given timestamps and values, appends
/// them to dst_values and returns results.
///
/// rc.timestamps are used as timestamps for dst_values.
///
/// timestamps must cover time range `[rc.start - rc.window - MAX_SILENCE_INTERVAL ... rc.end]`.
pub(super) fn bucket_samples(
    cfg: &RollupConfig,
    values: &[f64],
    timestamps: &[Timestamp],
    mut range: Duration,
    lookback_delta: Duration,
) -> EvalResult<u64> {
    let max_prev_interval = cfg.step;
    if range.is_zero() {
        // Implicit window exceeds -search.maxStalenessInterval, so limit it to -search.maxStalenessInterval
        // according to https://github.com/VictoriaMetrics/VictoriaMetrics/issues/784
        range = lookback_delta;
    }

    let samples_scanned = values.len() as u64;
    let window = range.as_millis() as i64;
    let max_prev_interval = max_prev_interval.as_millis() as i64;
    let lookback = lookback_delta.as_millis() as i64;
    let sample_len = values.len();

    let mut i = 0;
    let mut j = 0;
    let mut ni = 0;
    let mut nj = 0;

    // todo: get from a pool
    let mut func_args = Vec::with_capacity(cfg.timestamps.len());

    let mut prev_timestamp: i64 = 0;
    for (idx, &t_end) in cfg.timestamps.iter().enumerate() {
        let t_start = t_end - window;

        ni = seek_first_timestamp_idx_after(&timestamps[i..], t_start, ni);
        i += ni;
        if j < i {
            j = i;
        }

        nj = seek_first_timestamp_idx_after(&timestamps[j..], t_end, nj);
        j += nj;

        let mut rfa = RollupWindow {
            window,
            prev_value: f64::NAN,
            prev_timestamp: t_start - max_prev_interval,
            real_prev_value: f64::NAN,
            ..Default::default()
        };

        if i < sample_len && i > 0 && timestamps[i - 1] > rfa.prev_timestamp {
            // SAFETY: range is checked above
            unsafe {
                let prev_idx = i - 1;
                rfa.prev_value = *values.get_unchecked(prev_idx);
                rfa.prev_timestamp = *timestamps.get_unchecked(prev_idx);
            }
        }

        rfa.values = &values[i..j];
        rfa.timestamps = &timestamps[i..j];

        if i > 0 {
            let idx = i - 1;

            // SAFETY: i > 0 is checked above
            unsafe {
                // set real_prev_value if rc.lookback_delta == 0
                // or if the distance between datapoint in the prev interval and the beginning of this interval
                // doesn't exceed lookback_delta.
                // https://github.com/VictoriaMetrics/VictoriaMetrics/pull/1381
                // https://github.com/VictoriaMetrics/VictoriaMetrics/issues/894
                // https://github.com/VictoriaMetrics/VictoriaMetrics/issues/8045
                // https://github.com/VictoriaMetrics/VictoriaMetrics/issues/8935

                let mut curr_timestamp = t_start;
                if !rfa.timestamps.is_empty() {
                    curr_timestamp = rfa.timestamps[0];
                }

                if lookback.is_zero() || (curr_timestamp - prev_timestamp) < lookback {
                    let prev_value = values.get_unchecked(idx);
                    rfa.real_prev_value = *prev_value;
                }
            }
        }

        rfa.real_next_value = if j < values.len() {
            values[j]
        } else {
            f64::NAN
        };
        rfa.curr_timestamp = t_end;
        rfa.idx = idx;
        prev_timestamp = t_end;

        func_args.push(rfa);
    }

    Ok(samples_scanned)
}

fn seek_first_timestamp_idx_after(
    timestamps: &[Timestamp],
    seek_timestamp: Timestamp,
    n_hint: usize,
) -> usize {
    let count = timestamps.len();
    if count == 0 || timestamps[0] > seek_timestamp {
        return 0;
    }

    let start_idx = n_hint.saturating_sub(2).min(count - 1);
    let end_idx = (n_hint + 2).min(count);

    let slice_start = if timestamps[start_idx] <= seek_timestamp {
        start_idx
    } else {
        0
    };

    let slice_end = if end_idx < count && timestamps[end_idx] > seek_timestamp {
        end_idx
    } else {
        count
    };

    let slice = &timestamps[slice_start..slice_end];

    if slice.len() < 32 {
        slice
            .iter()
            .position(|&t| t > seek_timestamp)
            .map_or(slice.len(), |pos| pos)
    } else {
        match slice.binary_search(&(seek_timestamp + 1)) {
            Ok(pos) | Err(pos) => pos,
        }
    }
        .saturating_add(slice_start)
}

const fn get_max_prev_interval(scrape_interval: Duration) -> Duration {
    let scrape_interval = scrape_interval.as_millis() as i64;
    // Increase scrape_interval more for smaller scrape intervals to hide possible gaps
    // when high jitter is present.
    // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/139
    let interval = if scrape_interval <= 2_000i64 {
        scrape_interval + 4 * scrape_interval
    } else if scrape_interval <= 4_000i64 {
        scrape_interval + 2 * scrape_interval
    } else if scrape_interval <= 8_000i64 {
        scrape_interval + scrape_interval
    } else if scrape_interval <= 16_000i64 {
        scrape_interval + scrape_interval / 2
    } else if scrape_interval <= 32_000i64 {
        scrape_interval + scrape_interval / 4
    } else {
        scrape_interval + scrape_interval / 8
    };

    if interval < 0 {
        Duration::ZERO
    } else {
        Duration::from_millis(interval as u64)
    }
}
