use crate::common::hash::IntSet;
use anofox_forecast::features::Feature;

/// All statistical measures computed for a time series.
#[derive(Debug, Default, Clone)]
pub struct SeriesStats {
    pub length: usize,
    pub start_timestamp: i64,
    pub end_timestamp: i64,
    pub mean: f64,
    pub std: f64,
    pub min: f64,
    pub max: f64,
    pub range: f64,
    pub median: f64,
    pub n_nans: usize,
    pub n_zeros: usize,
    pub n_positive: usize,
    pub n_negative: usize,
    pub n_unique_values: usize,
    pub is_constant: bool,
    pub plateau_size: usize,
    pub plateau_size_non_zero: usize,
    pub n_zeros_start: usize,
    pub n_zeros_end: usize,
    pub skewness: f64,
    pub kurtosis: f64,
}

/// Compute all statistical measures from the sample slice.
pub fn calculate_stats(values: &[f64]) -> SeriesStats {
    let mut stats = SeriesStats::default();

    if values.is_empty() {
        return stats;
    }

    let mut unique_values: IntSet<i64> = IntSet::default();

    let mut n_zeros: usize = 0;
    let mut n_positive: usize = 0;
    let mut n_negative: usize = 0;

    for &v in values {
        if v == 0.0 {
            n_zeros += 1;
        } else if v > 0.0 {
            n_positive += 1;
        } else {
            n_negative += 1;
        }
        unique_values.insert(v.to_bits() as i64);
    }

    stats.n_zeros = n_zeros;
    stats.n_positive = n_positive;
    stats.n_negative = n_negative;
    stats.n_unique_values = unique_values.len();
    stats.is_constant = unique_values.len() == 1;

    calc_central_tendency(values, &mut stats);
    calc_plateaus(values, &mut stats);
    calc_leading_trailing_zeros(values, &mut stats);

    stats.range = stats.max - stats.min;

    stats
}

/// Central tendency and dispersion: mean, std, min, max, median, skewness, kurtosis.
fn calc_central_tendency(values: &[f64], stats: &mut SeriesStats) {
    if values.is_empty() {
        return;
    }

    stats.mean = Feature::Mean.compute(values);
    stats.std = Feature::StandardDeviation.compute(values);
    stats.min = Feature::Minimum.compute(values);
    stats.max = Feature::Maximum.compute(values);
    stats.median = Feature::Median.compute(values);
    stats.skewness = Feature::Skewness.compute(values);
    stats.kurtosis = Feature::Kurtosis.compute(values);
}

/// Longest consecutive run of identical values:
/// plateauSize, plateauSizeNonZero.
fn calc_plateaus(samples: &[f64], stats: &mut SeriesStats) {
    let mut max_plateau: usize = 0;
    let mut max_nonzero_plateau: usize = 0;
    let mut run_start: usize = 0;

    for i in 1..samples.len() {
        if samples[i - 1] != samples[i] {
            let run_len = i - run_start;
            let run_val = samples[run_start];

            max_plateau = max_plateau.max(run_len);
            if run_val != 0.0 {
                max_nonzero_plateau = max_nonzero_plateau.max(run_len);
            }
            run_start = i;
        }
    }

    // Final run
    let run_len = samples.len() - run_start;
    let run_val = samples[run_start];
    max_plateau = max_plateau.max(run_len);
    if run_val != 0.0 {
        max_nonzero_plateau = max_nonzero_plateau.max(run_len);
    }

    stats.plateau_size = max_plateau;
    stats.plateau_size_non_zero = max_nonzero_plateau;
}

/// Leading and trailing zeros
fn calc_leading_trailing_zeros(samples: &[f64], stats: &mut SeriesStats) {
    stats.n_zeros_start = samples.iter().take_while(|&&v| v == 0.0).count();

    stats.n_zeros_end = samples.iter().rev().take_while(|&&v| v == 0.0).count();
}
