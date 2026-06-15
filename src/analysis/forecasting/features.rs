use crate::error::TsdbError;
use anofox_forecast::features::Feature;
use orx_parallel::ParIter;
use orx_parallel::Parallelizable;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FeatureCategory {
    Basic,
    Distribution,
    Autocorrelation,
    Trend,
}

impl FeatureCategory {
    pub fn as_str(&self) -> &'static str {
        match self {
            FeatureCategory::Basic => "basic",
            FeatureCategory::Distribution => "distribution",
            FeatureCategory::Autocorrelation => "autocorrelation",
            FeatureCategory::Trend => "trend",
        }
    }

    pub fn features(&self) -> &'static [Feature] {
        use Feature::*;
        match self {
            FeatureCategory::Basic => &[
                Mean,
                Median,
                Variance,
                VarianceSample,
                Minimum,
                Maximum,
                Length,
            ],
            FeatureCategory::Distribution => &[
                Skewness,
                Kurtosis,
                Quantile { q: 0.25 },
                Quantile { q: 0.5 },
                Quantile { q: 0.75 },
                Quantile { q: 0.9 },
                Quantile { q: 0.95 },
                Quantile { q: 0.99 },
            ],
            FeatureCategory::Autocorrelation => &[
                Autocorrelation { lag: 1 },
                Autocorrelation { lag: 2 },
                Autocorrelation { lag: 3 },
            ],
            FeatureCategory::Trend => &[
                LinearTrendIntercept,
                LinearTrendSlope,
                LinearTrendPValue,
                LinearTrendRSquared,
            ],
        }
    }
}

impl TryFrom<&str> for FeatureCategory {
    type Error = TsdbError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "basic" => Ok(FeatureCategory::Basic),
            "distribution" => Ok(FeatureCategory::Distribution),
            "autocorrelation" => Ok(FeatureCategory::Autocorrelation),
            "trend" => Ok(FeatureCategory::Trend),
            _ => {
                let msg = format!("Unknown feature category: {}", value);
                let err = TsdbError::ForecastError(msg);
                Err(err)
            }
        }
    }
}

impl std::fmt::Display for FeatureCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Parse a single feature string into a `Feature` enum variant.
///
/// Simple features (no parameters):
///   `mean`, `median`, `variance`, `skewness`, `kurtosis`, etc.
///
/// Parameterized features (name:value):
///   `quantile:0.5`  — q must be in [0.0, 1.0]
///   `autocorrelation:3`  — lag must be > 0
///   `partial_autocorrelation:3` or `pacf:3`  — lag must be > 0
pub fn parse_feature(s: &str) -> Result<Feature, TsdbError> {
    let s = s.trim();
    if s.is_empty() {
        return Err(TsdbError::ForecastError("Empty feature name".into()));
    }

    // Parameterized features: name:value
    if let Some((name, param)) = s.split_once(':') {
        let name_lower = name.trim().to_lowercase();
        let param = param.trim();

        match name_lower.as_str() {
            "quantile" => {
                let q: f64 = param.parse().map_err(|_| {
                    TsdbError::ForecastError(format!(
                        "Invalid quantile parameter '{}': expected a float between 0.0 and 1.0",
                        param
                    ))
                })?;
                if !(0.0..=1.0).contains(&q) {
                    return Err(TsdbError::ForecastError(format!(
                        "Quantile parameter must be between 0.0 and 1.0, got {}",
                        q
                    )));
                }
                Ok(Feature::Quantile { q })
            }
            "autocorrelation" => {
                let lag: usize = param.parse().map_err(|_| {
                    TsdbError::ForecastError(format!(
                        "Invalid autocorrelation lag '{}': expected a positive integer",
                        param
                    ))
                })?;
                if lag == 0 {
                    return Err(TsdbError::ForecastError(
                        "Autocorrelation lag must be greater than 0".into(),
                    ));
                }
                Ok(Feature::Autocorrelation { lag })
            }
            "partial_autocorrelation" | "partialautocorrelation" | "pacf" => {
                let lag: usize = param.parse().map_err(|_| {
                    TsdbError::ForecastError(format!(
                        "Invalid partial autocorrelation lag '{}': expected a positive integer",
                        param
                    ))
                })?;
                if lag == 0 {
                    return Err(TsdbError::ForecastError(
                        "Partial autocorrelation lag must be greater than 0".into(),
                    ));
                }
                Ok(Feature::PartialAutocorrelation { lag })
            }
            other => Err(TsdbError::ForecastError(format!(
                "Unknown parameterized feature '{}'. Supported: quantile, autocorrelation, partial_autocorrelation (pacf)",
                other
            ))),
        }
    } else {
        // Non-parameterized features
        parse_simple_feature(&s.to_lowercase())
    }
}

/// Parse a non-parameterized feature name.
fn parse_simple_feature(name: &str) -> Result<Feature, TsdbError> {
    match name {
        // Basic
        "mean" => Ok(Feature::Mean),
        "median" => Ok(Feature::Median),
        "variance" => Ok(Feature::Variance),
        "variance_sample" => Ok(Feature::VarianceSample),
        "standard_deviation" => Ok(Feature::StandardDeviation),
        "minimum" => Ok(Feature::Minimum),
        "maximum" => Ok(Feature::Maximum),
        "abs_energy" => Ok(Feature::AbsEnergy),
        "absolute_maximum" => Ok(Feature::AbsoluteMaximum),
        "absolute_sum_of_changes" => Ok(Feature::AbsoluteSumOfChanges),
        "length" => Ok(Feature::Length),
        "mean_abs_change" => Ok(Feature::MeanAbsChange),
        "mean_change" => Ok(Feature::MeanChange),
        "mean_second_derivative_central" => Ok(Feature::MeanSecondDerivativeCentral),
        "root_mean_square" => Ok(Feature::RootMeanSquare),
        "sum_values" => Ok(Feature::SumValues),

        // Distribution
        "skewness" => Ok(Feature::Skewness),
        "kurtosis" => Ok(Feature::Kurtosis),
        "variance_larger_than_std" => Ok(Feature::VarianceLargerThanStd),
        "variation_coefficient" => Ok(Feature::VariationCoefficient),

        // Autocorrelation (non-parameterized)
        "time_reversal_asymmetry" => Ok(Feature::TimeReversalAsymmetry { lag: 1 }),

        // Counting
        "count_above_mean" => Ok(Feature::CountAboveMean),
        "count_below_mean" => Ok(Feature::CountBelowMean),
        "number_crossing_mean" => Ok(Feature::NumberCrossingMean),
        "longest_strike_above_mean" => Ok(Feature::LongestStrikeAboveMean),
        "longest_strike_below_mean" => Ok(Feature::LongestStrikeBelowMean),
        "first_location_of_maximum" => Ok(Feature::FirstLocationOfMaximum),
        "first_location_of_minimum" => Ok(Feature::FirstLocationOfMinimum),
        "last_location_of_maximum" => Ok(Feature::LastLocationOfMaximum),
        "last_location_of_minimum" => Ok(Feature::LastLocationOfMinimum),
        "has_duplicate" => Ok(Feature::HasDuplicate),
        "has_duplicate_max" => Ok(Feature::HasDuplicateMax),
        "has_duplicate_min" => Ok(Feature::HasDuplicateMin),

        // Entropy
        "fourier_entropy" => Ok(Feature::FourierEntropy),

        // Trend
        "linear_trend_slope" => Ok(Feature::LinearTrendSlope),
        "linear_trend_intercept" => Ok(Feature::LinearTrendIntercept),
        "linear_trend_r_squared" => Ok(Feature::LinearTrendRSquared),
        "linear_trend_p_value" => Ok(Feature::LinearTrendPValue),
        "augmented_dickey_fuller" => Ok(Feature::AugmentedDickeyFuller),

        // Change
        "percentage_reoccurring_datapoints" => Ok(Feature::PercentageReoccurringDatapoints),
        "percentage_reoccurring_values" => Ok(Feature::PercentageReoccurringValues),
        "ratio_value_number_to_length" => Ok(Feature::RatioValueNumberToLength),
        "sum_of_reoccurring_data_points" => Ok(Feature::SumOfReoccurringDataPoints),
        "sum_of_reoccurring_values" => Ok(Feature::SumOfReoccurringValues),

        _ => Err(TsdbError::ForecastError(format!(
            "Unknown feature '{}'",
            name
        ))),
    }
}

/// Compute features and return a map of feature name → value.
///
/// Features are deduplicated by their canonical name before computation.
pub fn compute_features_map(data: &[f64], features: &[Feature]) -> BTreeMap<String, f64> {
    // Deduplicate by canonical name (first occurrence wins)
    let mut seen = std::collections::HashSet::new();
    let unique: Vec<Feature> = features
        .iter()
        .filter(|f| seen.insert(f.name()))
        .cloned()
        .collect();

    let pairs: Vec<(String, f64)> = unique
        .as_slice()
        .par()
        .map(|feature: &Feature| (feature.name(), feature.compute(data)))
        .collect();

    pairs.into_iter().collect()
}

/// Compute features and return a flat vector of values (legacy).
pub fn compute_features(data: &[f64], features: &[Feature]) -> Vec<f64> {
    features
        .par()
        .map(|feature| feature.compute(data))
        .collect()
}
