use std::cmp::Ordering;

use itertools::Itertools;
use welch_sde::{Build, SpectralDensity};

use super::{MAX_SEASONALITY_PERIODS, overflow_safe_scale};

/// Default number of cycles of data assumed when establishing FFT window sizes.
const DEFAULT_MIN_FFT_CYCLES: f64 = 3.0;

/// Default maximum period assumed when establishing FFT window sizes.
const DEFAULT_MAX_FFT_PERIOD: f64 = 512.0;

/// A builder for a periodogram detector.
#[derive(Debug, Clone)]
pub struct Builder {
    min_period: u32,
    max_period: Option<u32>,
    threshold: f64,
}

impl Default for Builder {
    fn default() -> Self {
        Self {
            min_period: 4,
            max_period: None,
            threshold: 0.9,
        }
    }
}

impl Builder {
    /// Build the periodogram detector.
    ///
    /// The data is the time series to detect seasonal periods in.
    #[must_use]
    pub fn build(self) -> Detector {
        Detector {
            min_period: self.min_period,
            max_period: self.max_period,
            threshold: self.threshold,
        }
    }
}

fn default_max_period(data: &[f64]) -> u32 {
    (data.len() as f64 / DEFAULT_MIN_FFT_CYCLES).min(DEFAULT_MAX_FFT_PERIOD) as u32
}

/// The samples as welch-sde needs them: zero-mean, and small enough to transform.
///
/// A large mean leaks DC power into the low frequencies (long periods), where it can outrank a
/// real cycle. Huge samples are scaled down first, because the mean's sum, the differences from
/// it and the squared magnitudes inside the transform can each overflow on finite input. Only
/// the relative powers matter for peak picking, and a power of two scales samples exactly, so
/// the detected periods do not change.
fn centered_samples(data: &[f64]) -> Vec<f64> {
    let scale = overflow_safe_scale(data);
    let mean = data.iter().map(|x| x * scale).sum::<f64>() / data.len() as f64;
    data.iter().map(|x| x * scale - mean).collect()
}

/// A periodogram of a time series.
#[derive(Debug, Clone, PartialEq)]
pub struct Periodogram {
    /// The periods of the periodogram.
    pub periods: Vec<u32>,
    /// The powers of the periodogram.
    pub powers: Vec<f64>,
}

impl Periodogram {
    /// Find the peaks in the periodogram.
    ///
    /// A peak is a strict local maximum with a positive power of at least `threshold` times the
    /// maximum power in the periodogram: it must exceed its lower neighbour and be at least as
    /// large as its upper neighbour, so the shoulders of a broad peak are not reported as peaks of
    /// their own. Peaks are returned strongest first.
    pub fn peaks(&self, threshold: f64) -> impl Iterator<Item = Period> {
        // Scale the threshold so that it's relative to the maximum power.
        let keep = self
            .powers
            .iter()
            .copied()
            .max_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal))
            .unwrap_or(1.0)
            * threshold;

        let powers = &self.powers;
        let len = self.periods.len().min(powers.len());

        (0..len)
            .filter_map(|i| {
                let power = powers[i];
                let rises = i == 0 || power > powers[i - 1];
                let falls = i + 1 == len || power >= powers[i + 1];
                (power > 0.0 && power >= keep && rises && falls).then(|| Period {
                    power,
                    period: self.periods[i],
                    // The period just outside either end of the periodogram is reported as 0.
                    prev_period: if i > 0 { self.periods[i - 1] } else { 0 },
                    next_period: if i + 1 < len { self.periods[i + 1] } else { 0 },
                })
            })
            .sorted_by(|a, b| b.power.partial_cmp(&a.power).unwrap_or(Ordering::Equal))
    }
}

/// A peak in the periodogram.
#[derive(Debug, Clone, PartialEq)]
pub struct Period {
    /// The power of the peak.
    pub power: f64,
    /// The period of the peak.
    pub period: u32,
    /// The previous period in the periodogram.
    pub prev_period: u32,
    /// The next period in the periodogram.
    pub next_period: u32,
}

/// A season detector which uses a periodogram to identify seasonal periods.
///
/// The detector works by calculating a robust periodogram of the data using
/// Welch's method. The peaks in the periodogram represent likely seasonal periods
/// in the data.
#[derive(Debug)]
pub struct Detector {
    min_period: u32,
    max_period: Option<u32>,
    threshold: f64,
}

impl Detector {
    /// Create a new detector builder.
    #[must_use]
    pub fn builder() -> Builder {
        Builder::default()
    }

    /// Calculate the periodogram of the data.
    ///
    /// The periodogram is a frequency domain representation of the data, and is calculated using the
    /// Welch method.
    ///
    /// The periodogram can then be used to identify peaks, which are returned as periods which
    /// correspond to likely seasonal periods in the data.
    #[must_use]
    pub fn periodogram(&self, data: &[f64]) -> Periodogram {
        let max_period = self.max_period.unwrap_or_else(|| default_max_period(data));
        let frequency = 1.0;
        let data_len = data.len();
        let n_per_segment = (max_period * 2).min(data_len as u32 / 2).max(1);
        let max_fft_size = (n_per_segment as f64).log2().floor() as usize;

        // welch-sde computes `2 << (dft_log2_max_size - 1)` while building
        // the transform. A one-point segment produces a zero log-size, so
        // return the empty result that auto-seasonality already treats as
        // "no detectable period" instead of passing an invalid size to it.
        //
        // A constant series has no cycle either; centering it would leave only the rounding
        // error of its mean, which the relative peak threshold would then report as a period.
        if max_fft_size == 0 || crate::analysis::math::is_constant(data) {
            return Periodogram {
                periods: Vec::new(),
                powers: Vec::new(),
            };
        }

        let n_segments = (data_len as f64 / n_per_segment as f64).ceil() as usize;

        let centered = centered_samples(data);

        let welch: SpectralDensity<'_, f64> = SpectralDensity::builder(&centered, frequency)
            .n_segment(n_segments)
            .dft_log2_max_size(max_fft_size)
            .build();
        let sd = welch.periodogram();

        let freqs = sd.frequency();
        // Periods are the reciprocal of the frequency, since we've used a frequency of 1.
        // Make sure we skip the first one, which is 0, and the first power, which corresponds to
        // that.
        let periods = freqs.iter().skip(1).map(|x| x.recip().round() as u32);
        let power = sd.iter().skip(1).copied();

        let (periods, powers) = periods
            .zip(power)
            .filter(|(per, _)| {
                // Filter out periods that are too short or too long, and the period corresponding to the
                // segment length.
                *per >= self.min_period && *per < max_period && *per != n_per_segment
            })
            // Group by period, and keep the maximum power for each period.
            .chunk_by(|(per, _)| *per)
            .into_iter()
            .map(|(per, group)| {
                let max_power = group
                    .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(Ordering::Equal))
                    .unwrap_or((0, 0.0));
                (per, max_power.1)
            })
            .unzip();
        Periodogram { periods, powers }
    }

    /// Detect likely seasonal periods in the data.
    ///
    /// Returns at most [`MAX_SEASONALITY_PERIODS`] periods, strongest first.
    pub fn detect(&self, data: &[f64]) -> Vec<u32> {
        self.periodogram(data)
            .peaks(self.threshold)
            .map(|x| x.period)
            .take(MAX_SEASONALITY_PERIODS)
            .collect()
    }
}

impl Default for Detector {
    fn default() -> Self {
        Self::builder().build()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::analysis::seasonality::test_data::{CASES, TestCase};

    #[test]
    fn smoke() {
        #[rustfmt::skip]
        let y = &[
            0.1, 0.3, 0.8, 0.5,
            0.1, 0.31, 0.79, 0.48,
            0.09, 0.29, 0.81, 0.49,
            0.11, 0.28, 0.78, 0.53,
            0.1, 0.3, 0.8, 0.5,
            0.1, 0.31, 0.79, 0.48,
            0.09, 0.29, 0.81, 0.49,
            0.11, 0.28, 0.78, 0.53,
        ];
        let periods = Detector::default().detect(y);
        assert_eq!(periods[0], 4);
    }

    #[test]
    fn test_detect() {
        for (i, test_case) in CASES.iter().enumerate() {
            let TestCase {
                data,
                season_lengths: expected,
            } = test_case;
            let detector = Detector::default();
            // Peaks come back strongest first, so compare as sorted lists.
            assert_eq!(
                detector
                    .periodogram(data)
                    .peaks(0.5)
                    .map(|x| x.period)
                    .sorted()
                    .collect_vec(),
                expected.iter().copied().sorted().collect_vec(),
                "Test case {i}"
            );
        }
    }

    #[test]
    fn flat_series_yields_no_periods() {
        assert!(Detector::default().detect(&vec![0.0; 1000]).is_empty());
    }

    #[test]
    fn constant_series_yields_no_periods() {
        // 0.1 has no exact binary mean, so centering alone would leave rounding residue.
        for c in [0.1, 5.0, -7.3] {
            assert!(Detector::default().detect(&vec![c; 1000]).is_empty(), "{c}");
        }
    }

    #[test]
    fn peaks_are_local_maxima_strongest_first_and_capped() {
        let p = Periodogram {
            periods: vec![4, 5, 6, 7, 8, 9],
            powers: vec![1.0, 0.95, 0.96, 0.97, 0.5, 0.98],
        };
        let got: Vec<u32> = p.peaks(0.9).map(|x| x.period).collect();
        // 5 and 6 are shoulders of 7; order is by power descending.
        assert_eq!(got, vec![4, 9, 7]);
        assert!(
            Detector::default()
                .detect(crate::analysis::seasonality::test_data::SEASON_SEVEN)
                .len()
                <= MAX_SEASONALITY_PERIODS
        );
    }

    #[test]
    fn auto_finds_the_daily_period_at_two_thousand_samples() {
        // Daily (24) and weekly (168) cycles on a large mean, with a sprinkle of spikes.
        let series: Vec<f64> = (0..2000)
            .map(|i| {
                let t = i as f64;
                100.0
                    + 10.0 * (t * std::f64::consts::TAU / 24.0).sin()
                    + 3.0 * (t * std::f64::consts::TAU / 168.0).sin()
                    + ((i * 7919) % 41) as f64 / 10.0
                    + if i % 997 == 0 { 60.0 } else { 0.0 }
            })
            .collect();
        assert_eq!(Detector::default().detect(&series)[0], 24);
    }

    /// The smoke pattern, each sample multiplied by the exact power of two `2^exp`.
    fn scaled_cycle(exp: i32) -> Vec<f64> {
        let factor = 2f64.powi(exp);
        (0..256)
            .map(|i| [0.1, 0.3, 0.8, 0.5][i % 4] * factor)
            .collect()
    }

    fn assert_same_detection(data: &[f64], expected: &[u32], label: &str) {
        let detector = Detector::default();
        let periodogram = detector.periodogram(data);
        assert!(!periodogram.powers.is_empty(), "{label}: no powers");
        assert!(
            periodogram.powers.iter().all(|p| p.is_finite()),
            "{label}: non-finite power in {:?}",
            periodogram.powers
        );
        assert_eq!(detector.detect(data), expected, "{label}");
    }

    #[test]
    fn huge_finite_samples_detect_like_ordinary_ones() {
        let expected = Detector::default().detect(&scaled_cycle(0));
        assert_eq!(expected[0], 4);
        // 2^1023 overflows the sum behind the mean; 2^600 survives the mean but overflows the
        // squared magnitudes inside the transform.
        for exp in [400, 460, 512, 600, 1000, 1023] {
            assert_same_detection(&scaled_cycle(exp), &expected, &format!("2^{exp}"));
        }
    }

    #[test]
    fn opposed_extremes_do_not_overflow_centering() {
        // The mean sits next to -MAX, so MAX minus the mean exceeds the f64 range.
        let data: Vec<f64> = (0..256)
            .map(|i| if i % 16 == 0 { f64::MAX } else { -f64::MAX })
            .collect();
        let powers = Detector::default().periodogram(&data).powers;
        assert!(!powers.is_empty());
        assert!(powers.iter().all(|p| p.is_finite()), "{powers:?}");
    }

    #[test]
    fn short_input_has_no_periodogram() {
        let data = [1.0, 2.0, 1.0];
        let periodogram = Detector::default().periodogram(&data);

        assert!(periodogram.periods.is_empty());
        assert!(periodogram.powers.is_empty());
        assert!(Detector::default().detect(&data).is_empty());
    }
}
