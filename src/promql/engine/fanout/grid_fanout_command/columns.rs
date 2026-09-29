//! The columnar form of a grid series on the wire: a presence bitmap over the
//! request's window ends, the present windows' values, and optionally each
//! pick's lag behind its window end.

use crate::promql::EvalLabels;
use crate::promql::generated::GridSeries as ProtoGridSeries;
use crate::promql::model::RangeSample;

/// A series' points as the wire's columns: a presence bitmap over
/// `window_ends`, the values of the present windows in order, and — when the
/// request asked for them — each pick's lag behind its window end.
///
/// `points` must arrive in window order, as the per-series stage produces
/// them; a point whose step is not a window end is dropped (it cannot be
/// addressed), which never happens for a stage run over these ends.
pub(super) fn encode_columns(
    window_ends: &[i64],
    points: impl Iterator<Item = (i64, i64, f64)>,
    with_lag: bool,
) -> (Vec<u8>, Vec<f64>, Vec<i64>) {
    let mut presence = vec![0u8; window_ends.len().div_ceil(8)];
    let mut values = Vec::new();
    let mut lags = Vec::new();
    let mut cursor = 0usize;
    for (step_ts, sample_ts, value) in points {
        while cursor < window_ends.len() && window_ends[cursor] < step_ts {
            cursor += 1;
        }
        if cursor >= window_ends.len() || window_ends[cursor] != step_ts {
            debug_assert!(false, "grid point {step_ts} is not a window end");
            continue;
        }
        presence[cursor / 8] |= 1 << (cursor % 8);
        values.push(value);
        if with_lag {
            lags.push(step_ts - sample_ts);
        }
        cursor += 1;
    }
    (presence, values, lags)
}

/// The inverse of [`encode_columns`]: `(step_ts, sample_ts, value)` per
/// present window. A bitmap that reaches past the grid, or a value count that
/// disagrees with the bitmap, is a corrupt response rather than a short one.
pub(super) fn decode_columns<'a>(
    window_ends: &'a [i64],
    series: &'a ProtoGridSeries,
) -> Result<impl Iterator<Item = (i64, i64, f64)> + 'a, String> {
    let present: Vec<usize> = series
        .presence
        .iter()
        .enumerate()
        .flat_map(|(byte, bits)| {
            (0..8)
                .filter(move |bit| bits & (1 << bit) != 0)
                .map(move |bit| byte * 8 + bit)
        })
        .collect();
    if present.last().is_some_and(|&i| i >= window_ends.len()) {
        return Err(format!(
            "presence bitmap addresses window {} of {}",
            present.last().unwrap(),
            window_ends.len()
        ));
    }
    if present.len() != series.values.len() {
        return Err(format!(
            "{} present windows but {} values",
            present.len(),
            series.values.len()
        ));
    }
    if !series.sample_lag.is_empty() && series.sample_lag.len() != series.values.len() {
        return Err(format!(
            "{} values but {} sample lags",
            series.values.len(),
            series.sample_lag.len()
        ));
    }
    Ok(present.into_iter().enumerate().map(move |(k, i)| {
        let step_ts = window_ends[i];
        let lag = series.sample_lag.get(k).copied().unwrap_or(0);
        // The lag comes from a peer; a corrupt one must not wrap the timestamp.
        (step_ts, step_ts.saturating_sub(lag), series.values[k])
    }))
}

/// Per-entry `(step, value)` points as columnar series: rollup output, or a
/// fused selection's per-step picks and counts. No lag column — a value here
/// belongs to its window.
pub(super) fn columnar_series(
    window_ends: &[i64],
    series: Vec<RangeSample<EvalLabels>>,
) -> Vec<ProtoGridSeries> {
    series
        .into_iter()
        .map(|s| {
            let (presence, values, sample_lag) = encode_columns(
                window_ends,
                s.samples
                    .iter()
                    .map(|p| (p.timestamp, p.timestamp, p.value)),
                false,
            );
            ProtoGridSeries {
                labels: (&s.labels).into(),
                presence,
                values,
                sample_lag,
            }
        })
        .collect()
}
