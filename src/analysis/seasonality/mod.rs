mod test_data;

use crate::analysis::{TimeSeriesAnalysisError, TimeSeriesAnalysisResult};
use anofox_forecast::detection::{PeriodDetectionConfig, detect_periods};
use anofox_forecast::seasonality::{MSTL, STL};

/// A detector of periodic signals in a time series.
pub trait SeasonalityDetector {
    /// Detects the periods of a time series.
    fn detect(&self, data: &[f64]) -> Vec<u32>;
}

#[derive(Clone, Debug)]
pub enum Seasonality {
    Auto,
    Periods(Vec<usize>),
}

/// Seasonal adjustment using (M)Stl decomposition
pub fn seasonally_adjust(
    ts: &[f64],
    seasonality: &Seasonality,
) -> TimeSeriesAnalysisResult<Vec<f64>> {
    let mut periods = match seasonality {
        Seasonality::Periods(periods) => periods.clone(),
        Seasonality::Auto => {
            let config = PeriodDetectionConfig::default();
            let periods = detect_periods(ts, &config);
            periods.iter().map(|x| x.period).collect()
        }
    };

    if periods.is_empty() {
        return Ok(ts.to_vec());
    }

    periods.sort_unstable();

    let n = ts.len();
    if periods.len() == 1 {
        let required = 2 * periods[0];
        validate_insufficient_data::<Vec<f64>>(required, n)?;

        STL::new(periods[0])
            .robust()
            .decompose(ts)
            .map(|res| res.remainder)
            .ok_or_else(|| {
                TimeSeriesAnalysisError::DecompositionError("STL decomposition failed".to_string())
            })
    } else {
        let max_period = periods[periods.len() - 1];
        let required = 2 * max_period;
        validate_insufficient_data::<Vec<f64>>(required, n)?;

        // MSTL alternates between fitting the trend and each seasonal component; its
        // default of 2 outer iterations often hasn't converged by the time it stops,
        // which can leave point-anomalies partially absorbed into trend/seasonal
        // rather than showing up in the remainder. A few extra iterations noticeably
        // stabilizes convergence at negligible extra cost.
        MSTL::new(periods.to_vec())
            .robust()
            .with_iterations(5)
            .decompose(ts)
            .map(|res| res.remainder)
            .ok_or_else(|| {
                TimeSeriesAnalysisError::DecompositionError("MSTL decomposition failed".to_string())
            })
    }
}

fn validate_insufficient_data<T: Default>(
    required: usize,
    actual: usize,
) -> TimeSeriesAnalysisResult<T> {
    if actual >= required {
        return Ok(T::default());
    }
    Err(TimeSeriesAnalysisError::InsufficientData {
        message: "TSDB: insufficient samples for anomaly detection".to_string(),
        required,
        actual,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::TimeSeriesAnalysisError;
    use crate::analysis::seasonality::test_data::SEASON_SEVEN;

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
}
