pub mod mstl;
mod periodogram;
pub mod stl;
#[cfg(test)]
pub mod test_data;

use crate::analysis::math::is_constant;
use crate::analysis::seasonality::mstl::Mstl;
use crate::analysis::seasonality::stl::Stl;
use crate::analysis::{INSUFFICIENT_DATA_ERROR, TimeSeriesAnalysisError, TimeSeriesAnalysisResult};
pub use periodogram::Detector as PeriodogramDetector;

#[derive(Clone, Debug)]
pub enum Seasonality {
    Auto,
    Periods(Vec<usize>),
}

/// Maximum number of seasonal periods accepted by `SEASONALITY`.
pub const MAX_SEASONALITY_PERIODS: usize = 4;

/// Smallest meaningful seasonal period: a period of 0 or 1 has no cycle to remove.
pub const MIN_SEASONALITY_PERIOD: usize = 2;

/// Seasonal adjustment using (M)Stl decomposition
pub fn seasonally_adjust(
    ts: &[f64],
    seasonality: &Seasonality,
) -> TimeSeriesAnalysisResult<Vec<f64>> {
    let mut periods = match seasonality {
        Seasonality::Periods(periods) => periods.clone(),
        Seasonality::Auto => {
            // use periodogram to detect periods; `detect` already caps the list at
            // MAX_SEASONALITY_PERIODS, strongest first
            let detector = PeriodogramDetector::default();
            detector.detect(ts).iter().map(|&x| x as usize).collect()
        }
    };

    if periods.is_empty() {
        return Ok(ts.to_vec());
    }

    periods.sort_unstable();
    validate_periods(&periods, ts.len())?;

    // Nothing to remove, and decomposing would only leave rounding noise that
    // detectors then read as signal.
    if is_constant(ts) {
        return Ok(ts.to_vec());
    }

    // Both decompositions scale linearly, so this only keeps their arithmetic inside the f64
    // range; the remainder comes back in the units of the series.
    let scale = overflow_safe_scale(ts);
    if scale == 1.0 {
        return decompose(ts, periods);
    }
    let scaled: Vec<f64> = ts.iter().map(|x| x * scale).collect();
    let mut remainder = decompose(&scaled, periods)?;
    remainder.iter_mut().for_each(|x| *x /= scale);
    Ok(remainder)
}

fn decompose(ts: &[f64], periods: Vec<usize>) -> TimeSeriesAnalysisResult<Vec<f64>> {
    if periods.len() == 1 {
        Stl::new(periods[0])
            .robust()
            .decompose(ts)
            .map(|res| res.remainder)
            .ok_or_else(|| {
                TimeSeriesAnalysisError::DecompositionError("STL decomposition failed".to_string())
            })
    } else {
        Mstl::new(periods)
            .robust()
            .decompose(ts)
            .map(|res| res.remainder)
            .ok_or_else(|| {
                TimeSeriesAnalysisError::DecompositionError("MSTL decomposition failed".to_string())
            })
    }
}

/// The factor that brings huge samples back into a range where sums, differences and squares
/// cannot overflow: once any magnitude exceeds 2^400, the power of two that puts the peak near 1,
/// otherwise 1. A power of two scales exactly, and ordinary series are left alone. Scaling to the
/// peak rather than by a fixed factor keeps samples clear of the absolute tolerances in STL.
fn overflow_safe_scale(data: &[f64]) -> f64 {
    let peak = data.iter().fold(0.0_f64, |peak, x| peak.max(x.abs()));
    if peak.is_finite() && peak > 2f64.powi(400) {
        2f64.powi(-(peak.log2().floor() as i32))
    } else {
        1.0
    }
}

/// Checks every period against a series of `n` samples before any arithmetic or allocation
/// depends on it. A period must be at least [`MIN_SEASONALITY_PERIOD`] and the series must
/// hold at least two full cycles of it. `2 * period` saturates at `usize::MAX`, so an
/// oversized period reports `required: usize::MAX` instead of wrapping.
pub fn validate_periods(periods: &[usize], n: usize) -> TimeSeriesAnalysisResult<()> {
    for &period in periods {
        if period < MIN_SEASONALITY_PERIOD {
            return Err(TimeSeriesAnalysisError::InvalidParameter {
                name: "period".to_string(),
                message: format!(
                    "seasonal periods must be at least {MIN_SEASONALITY_PERIOD}, got {period}"
                ),
            });
        }
        let required = period.saturating_mul(2);
        if n < required {
            return Err(TimeSeriesAnalysisError::InsufficientData {
                message: INSUFFICIENT_DATA_ERROR.to_string(),
                required,
                actual: n,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::TimeSeriesAnalysisError;
    use crate::analysis::seasonality::test_data::SEASON_SEVEN;

    #[test]
    fn constant_series_is_returned_undecomposed() {
        let values = vec![0.1; 100];
        for periods in [vec![24], vec![7, 24]] {
            let adjusted = seasonally_adjust(&values, &Seasonality::Periods(periods)).unwrap();
            assert_eq!(adjusted, values);
        }
    }

    #[test]
    fn constant_series_still_has_its_periods_validated() {
        let values = vec![5.0; 10];
        let too_long = seasonally_adjust(&values, &Seasonality::Periods(vec![24])).unwrap_err();
        assert_insufficient_data(&too_long, 48, 10);

        let too_short = seasonally_adjust(&values, &Seasonality::Periods(vec![1])).unwrap_err();
        assert!(matches!(
            too_short,
            TimeSeriesAnalysisError::InvalidParameter { .. }
        ));
    }

    #[test]
    fn huge_series_decompose_like_ordinary_ones() {
        let cycle = |exp: i32| -> Vec<f64> {
            let factor = 2f64.powi(exp);
            (0..256)
                .map(|i| [0.1, 0.3, 0.8, 0.5][i % 4] * factor)
                .collect()
        };
        for seasonality in [
            Seasonality::Auto,
            Seasonality::Periods(vec![4]),
            Seasonality::Periods(vec![4, 8]),
        ] {
            let expected = seasonally_adjust(&cycle(0), &seasonality).unwrap();
            for exp in [512, 1000, 1023] {
                let factor = 2f64.powi(exp);
                let adjusted = seasonally_adjust(&cycle(exp), &seasonality).unwrap();
                for (got, want) in adjusted.iter().zip(&expected) {
                    assert!(got.is_finite(), "{seasonality:?} 2^{exp}");
                    assert!(
                        (got / factor - want).abs() < 1e-9,
                        "{seasonality:?} 2^{exp}: {} vs {want}",
                        got / factor
                    );
                }
            }
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    fn assert_insufficient_data(
        err: &TimeSeriesAnalysisError,
        expected_required: usize,
        expected_actual: usize,
    ) {
        match err {
            TimeSeriesAnalysisError::InsufficientData {
                required, actual, ..
            } => {
                assert_eq!(
                    *required, expected_required,
                    "required mismatch: got {required}, want {expected_required}"
                );
                assert_eq!(
                    *actual, expected_actual,
                    "actual mismatch: got {actual}, want {expected_actual}"
                );
            }
            other => panic!("expected InsufficientData, got {other:?}"),
        }
    }

    // ── Seasonality::Periods(vec![]) ─────────────────────────────────────────

    #[test]
    fn test_empty_periods_returns_input_unchanged() {
        let data: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let result = seasonally_adjust(&data, &Seasonality::Periods(vec![])).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn test_empty_periods_on_empty_slice_returns_empty() {
        let result = seasonally_adjust(&[], &Seasonality::Periods(vec![])).unwrap();
        assert!(result.is_empty());
    }

    // ── single period — insufficient data ────────────────────────────────────

    #[test]
    fn test_single_period_insufficient_data_returns_error() {
        let period = 7_usize;
        // n < 2 * period → error
        let data: Vec<f64> = vec![1.0; period * 2 - 1];
        let err = seasonally_adjust(&data, &Seasonality::Periods(vec![period])).unwrap_err();
        assert_insufficient_data(&err, 2 * period, data.len());
    }

    #[test]
    fn test_single_period_zero_samples_returns_error() {
        let period = 4_usize;
        let err = seasonally_adjust(&[], &Seasonality::Periods(vec![period])).unwrap_err();
        assert_insufficient_data(&err, 2 * period, 0);
    }

    // ── single period — boundary: exactly 2 * period ────────────────────────

    #[test]
    fn test_single_period_exactly_minimum_samples_succeeds() {
        let period = 4_usize;
        // exactly 2 * period — should succeed (>= check)
        let data: Vec<f64> = (0..(2 * period) as i64)
            .map(|i| (i % period as i64) as f64)
            .collect();
        let result = seasonally_adjust(&data, &Seasonality::Periods(vec![period]));
        assert!(
            result.is_ok(),
            "expected Ok with exactly 2*period samples, got {result:?}"
        );
        assert_eq!(result.unwrap().len(), data.len());
    }

    // ── single period — sufficient data ─────────────────────────────────────

    #[test]
    fn test_single_period_returns_remainder_same_length() {
        let data = SEASON_SEVEN;
        let result = seasonally_adjust(data, &Seasonality::Periods(vec![7])).unwrap();
        assert_eq!(
            result.len(),
            data.len(),
            "remainder length must equal input length"
        );
    }

    #[test]
    fn test_single_period_remainder_values_are_finite() {
        let data = SEASON_SEVEN;
        let result = seasonally_adjust(data, &Seasonality::Periods(vec![7])).unwrap();
        assert!(
            result.iter().all(|v| v.is_finite()),
            "all remainder values should be finite"
        );
    }

    // ── multiple periods — insufficient data ─────────────────────────────────

    #[test]
    fn test_multi_period_insufficient_data_returns_error() {
        let periods = vec![4_usize, 7];
        let max_period = *periods.last().unwrap();
        // n < 2 * max_period → error
        let data: Vec<f64> = vec![1.0; 2 * max_period - 1];
        let err = seasonally_adjust(&data, &Seasonality::Periods(periods)).unwrap_err();
        assert_insufficient_data(&err, 2 * max_period, data.len());
    }

    #[test]
    fn test_multi_period_period_ordering_determines_max() {
        // periods = [7, 8] — max is the last element (8) as used by the code
        // with exactly 2*8=16 samples it should succeed
        let periods = vec![7_usize, 8];
        let max_period = 8_usize;
        let data: Vec<f64> = (0..(2 * max_period) as i64)
            .map(|i| (i % 7) as f64)
            .collect();
        let result = seasonally_adjust(&data, &Seasonality::Periods(periods));
        assert!(result.is_ok(), "expected Ok, got {result:?}");
        assert_eq!(result.unwrap().len(), data.len());
    }

    // ── Seasonality::Auto ────────────────────────────────────────────────────

    #[test]
    fn test_auto_empty_periods_returns_input_unchanged() {
        // With n=10: default_max_period = floor(10/3) = 3, which is less than min_period=4, so
        // the period filter (per >= 4 && per < 3) is always false.  The detector returns an empty
        // vec and seasonally_adjust returns the original slice unchanged.
        let data: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0, 4.0, 3.0, 2.0, 1.0, 2.0];
        let result = seasonally_adjust(&data, &Seasonality::Auto).unwrap();
        assert_eq!(
            result, data,
            "with no detectable period the input must be returned unchanged"
        );
    }

    #[test]
    fn test_auto_three_samples_returns_input_unchanged() {
        let data = vec![1.0, 2.0, 1.0];
        let result = seasonally_adjust(&data, &Seasonality::Auto).unwrap();

        assert_eq!(result, data);
    }

    // ── period validation ────────────────────────────────────────────────────

    #[test]
    fn oversized_period_is_rejected_not_hung() {
        let err = seasonally_adjust(&[1.0, 2.0, 1.0], &Seasonality::Periods(vec![1usize << 63]))
            .unwrap_err();
        assert!(matches!(
            err,
            TimeSeriesAnalysisError::InsufficientData {
                required: usize::MAX,
                actual: 3,
                ..
            }
        ));
    }

    #[test]
    fn periods_below_two_are_rejected() {
        for p in [0usize, 1] {
            let err =
                seasonally_adjust(&vec![1.0; 50], &Seasonality::Periods(vec![p])).unwrap_err();
            assert!(
                matches!(err, TimeSeriesAnalysisError::InvalidParameter { ref name, .. } if name == "period"),
                "{p}"
            );
        }
    }

    #[test]
    fn period_above_half_the_series_is_rejected() {
        let err = seasonally_adjust(&vec![1.0; 50], &Seasonality::Periods(vec![26])).unwrap_err();
        assert!(matches!(
            err,
            TimeSeriesAnalysisError::InsufficientData {
                required: 52,
                actual: 50,
                ..
            }
        ));
    }
}
