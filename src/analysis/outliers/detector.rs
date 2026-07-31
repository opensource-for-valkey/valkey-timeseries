//! Static dispatch over the closed set of anomaly detectors.
//!
//! The set of methods is fixed by [`AnomalyMethod`] and there is no plugin path,
//! so a trait object buys nothing here. [`Detector`] enumerates the
//! implementations instead, which keeps dispatch static and — more usefully —
//! lets callers ask whether a detector supports a narrower interface via
//! [`Detector::as_point_detector`], with no downcasting.

use super::cusum_outlier_detector::CusumOutlierDetector;
use super::double_mad_outlier_detector::DoubleMadOutlierDetector;
use super::esd_outlier_detector::{ESDOutlierDetector, ESDOutlierOptions};
use super::ewma_outlier_detector::{EWMA_DEFAULT_ALPHA, EwmaOutlierDetector};
use super::iqr_outlier_detector::{IQR_DEFAULT_THRESHOLD, IQROutlierDetector};
use super::mad_outlier_detector::MadOutlierDetector;
use super::modified_zscore_outlier_detector::{
    MODIFIED_ZSCORE_DEFAULT_THRESHOLD, ModifiedZScoreOutlierDetector,
};
use super::rcf_outlier_detector::RcfOutlierDetector;
use super::smoothed_zscores::SmoothedZScoreAnomalyDetector;
use super::zscore_outlier_detector::ZScoreOutlierDetector;
use super::{
    AnomalyDetectionMethodOptions, AnomalyDetector, AnomalyMethod, AnomalyResult, MethodInfo,
    PointDetector,
};
use crate::analysis::{TimeSeriesAnalysisError, TimeSeriesAnalysisResult};

/// One constructed detector, ready to fit and run.
///
/// The random cut forest is boxed: it is ~800 bytes against ~120 for the next
/// largest variant, and an enum is as big as its widest arm.
pub enum Detector {
    Cusum(CusumOutlierDetector),
    DoubleMad(DoubleMadOutlierDetector),
    Esd(ESDOutlierDetector),
    Ewma(EwmaOutlierDetector),
    InterquartileRange(IQROutlierDetector),
    Mad(MadOutlierDetector),
    ModifiedZScore(ModifiedZScoreOutlierDetector),
    RandomCutForest(Box<RcfOutlierDetector>),
    SmoothedZScore(SmoothedZScoreAnomalyDetector),
    ZScore(ZScoreOutlierDetector),
}

/// Run `$body` against the detector inside any variant.
///
/// Every arm binds the same identifier, so the body is written once and the
/// compiler still resolves the call statically per variant.
macro_rules! with_detector {
    ($this:expr, |$inner:ident| $body:expr) => {
        match $this {
            Detector::Cusum($inner) => $body,
            Detector::DoubleMad($inner) => $body,
            Detector::Esd($inner) => $body,
            Detector::Ewma($inner) => $body,
            Detector::InterquartileRange($inner) => $body,
            Detector::Mad($inner) => $body,
            Detector::ModifiedZScore($inner) => $body,
            Detector::RandomCutForest($inner) => $body,
            Detector::SmoothedZScore($inner) => $body,
            Detector::ZScore($inner) => $body,
        }
    };
}

impl Detector {
    /// Construct the detector described by `options`.
    ///
    /// `values` is needed up front by the detectors that derive their model from
    /// the series at construction (IQR's quartiles, EWMA's baseline) rather than
    /// in [`AnomalyDetector::train`].
    pub fn build(
        values: &[f64],
        options: &AnomalyDetectionMethodOptions,
    ) -> TimeSeriesAnalysisResult<Self> {
        let detector = match options {
            AnomalyDetectionMethodOptions::Cusum => {
                Detector::Cusum(CusumOutlierDetector::default())
            }
            AnomalyDetectionMethodOptions::Ewma(alpha) => Detector::Ewma(
                EwmaOutlierDetector::from_series(values, alpha.unwrap_or(EWMA_DEFAULT_ALPHA)),
            ),
            AnomalyDetectionMethodOptions::InterQuartileRange(threshold) => {
                Detector::InterquartileRange(IQROutlierDetector::new(
                    values,
                    threshold.unwrap_or(IQR_DEFAULT_THRESHOLD),
                ))
            }
            AnomalyDetectionMethodOptions::ZScore(threshold) => {
                Detector::ZScore(ZScoreOutlierDetector::new(
                    threshold.unwrap_or(ZScoreOutlierDetector::DEFAULT_THRESHOLD),
                ))
            }
            AnomalyDetectionMethodOptions::SmoothedZScore(options) => {
                Detector::SmoothedZScore(SmoothedZScoreAnomalyDetector::new(
                    options.influence,
                    options.threshold,
                    options.lag,
                )?)
            }
            AnomalyDetectionMethodOptions::ModifiedZScore(threshold) => {
                Detector::ModifiedZScore(ModifiedZScoreOutlierDetector::new(
                    threshold.unwrap_or(MODIFIED_ZSCORE_DEFAULT_THRESHOLD),
                ))
            }
            AnomalyDetectionMethodOptions::Mad(options) => {
                Detector::Mad(MadOutlierDetector::new(options.k, options.estimator))
            }
            AnomalyDetectionMethodOptions::DoubleMAD(options) => {
                Detector::DoubleMad(DoubleMadOutlierDetector::with_options(options))
            }
            AnomalyDetectionMethodOptions::Rcf(opts) => {
                let detector = RcfOutlierDetector::new(*opts)
                    .map_err(|e| TimeSeriesAnalysisError::InvalidModel(format!("{e:?}")))?;
                Detector::RandomCutForest(Box::new(detector))
            }
            AnomalyDetectionMethodOptions::Esd(options) => {
                let ESDOutlierOptions {
                    alpha,
                    estimator,
                    max_outliers,
                } = options.clone().unwrap_or_default();
                Detector::Esd(ESDOutlierDetector::new(alpha, estimator, max_outliers))
            }
        };

        Ok(detector)
    }

    /// The narrower interface, when this method supports it.
    ///
    /// Returns `Some` only for detectors whose verdict for a point is a pure
    /// function of the fitted model. Sequential and whole-sample methods return
    /// `None` — for them a per-point answer either does not exist or would
    /// disagree with [`AnomalyDetector::detect`].
    pub fn as_point_detector(&self) -> Option<&dyn PointDetector> {
        match self {
            Detector::DoubleMad(d) => Some(d),
            Detector::InterquartileRange(d) => Some(d),
            Detector::Mad(d) => Some(d),
            Detector::ModifiedZScore(d) => Some(d),
            Detector::ZScore(d) => Some(d),
            Detector::Cusum(_)
            | Detector::Esd(_)
            | Detector::Ewma(_)
            | Detector::RandomCutForest(_)
            | Detector::SmoothedZScore(_) => None,
        }
    }
}

impl AnomalyDetector for Detector {
    fn method(&self) -> AnomalyMethod {
        with_detector!(self, |d| d.method())
    }

    fn train(&mut self, data: &[f64]) -> TimeSeriesAnalysisResult<()> {
        with_detector!(self, |d| d.train(data))
    }

    fn model_info(&self) -> Option<MethodInfo> {
        with_detector!(self, |d| d.model_info())
    }

    fn detect(&mut self, ts: &[f64]) -> TimeSeriesAnalysisResult<AnomalyResult> {
        with_detector!(self, |d| d.detect(ts))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::outliers::{
        AnomalySignal, MADAnomalyOptions, RCFOptions, SmoothedZScoreOptions,
    };

    fn build(options: AnomalyDetectionMethodOptions) -> Detector {
        let values: Vec<f64> = (0..40).map(|i| (i % 7) as f64).collect();
        Detector::build(&values, &options).expect("detector should build")
    }

    /// Every variant must report the method its options asked for; a copy-paste
    /// slip in `build` would otherwise surface as a mislabeled reply.
    #[test]
    fn build_reports_the_requested_method() {
        let cases = [
            (AnomalyDetectionMethodOptions::Cusum, AnomalyMethod::Cusum),
            (
                AnomalyDetectionMethodOptions::Ewma(None),
                AnomalyMethod::Ewma,
            ),
            (
                AnomalyDetectionMethodOptions::InterQuartileRange(None),
                AnomalyMethod::InterquartileRange,
            ),
            (
                AnomalyDetectionMethodOptions::ZScore(None),
                AnomalyMethod::ZScore,
            ),
            (
                AnomalyDetectionMethodOptions::SmoothedZScore(SmoothedZScoreOptions::default()),
                AnomalyMethod::SmoothedZScore,
            ),
            (
                AnomalyDetectionMethodOptions::ModifiedZScore(None),
                AnomalyMethod::ModifiedZScore,
            ),
            (
                AnomalyDetectionMethodOptions::Mad(MADAnomalyOptions::default()),
                AnomalyMethod::Mad,
            ),
            (
                AnomalyDetectionMethodOptions::DoubleMAD(MADAnomalyOptions::default()),
                AnomalyMethod::DoubleMAD,
            ),
            (AnomalyDetectionMethodOptions::Esd(None), AnomalyMethod::Esd),
            (
                AnomalyDetectionMethodOptions::Rcf(RCFOptions::default()),
                AnomalyMethod::RandomCutForest,
            ),
        ];

        for (options, expected) in cases {
            let requested = options.method();
            assert_eq!(requested, expected, "options.method() disagrees with case");
            assert_eq!(
                build(options).method(),
                expected,
                "built detector reports the wrong method"
            );
        }
    }

    #[test]
    fn point_detector_capability_matches_the_method_family() {
        let pointwise = [
            AnomalyDetectionMethodOptions::InterQuartileRange(None),
            AnomalyDetectionMethodOptions::ZScore(None),
            AnomalyDetectionMethodOptions::ModifiedZScore(None),
            AnomalyDetectionMethodOptions::Mad(MADAnomalyOptions::default()),
            AnomalyDetectionMethodOptions::DoubleMAD(MADAnomalyOptions::default()),
        ];
        for options in pointwise {
            let method = options.method();
            assert!(
                build(options).as_point_detector().is_some(),
                "{method:?} is fence-based and should expose a point detector"
            );
        }

        let sequential_or_whole_sample = [
            AnomalyDetectionMethodOptions::Cusum,
            AnomalyDetectionMethodOptions::Ewma(None),
            AnomalyDetectionMethodOptions::SmoothedZScore(SmoothedZScoreOptions::default()),
            AnomalyDetectionMethodOptions::Esd(None),
            AnomalyDetectionMethodOptions::Rcf(RCFOptions::default()),
        ];
        for options in sequential_or_whole_sample {
            let method = options.method();
            assert!(
                build(options).as_point_detector().is_none(),
                "{method:?} has no position-independent per-point verdict"
            );
        }
    }

    /// The capability accessor is only useful if the interface it hands back
    /// actually works through the wrapper.
    #[test]
    fn point_detector_classifies_through_the_wrapper() {
        let mut detector = build(AnomalyDetectionMethodOptions::ZScore(Some(3.0)));
        detector.train(&[1.0, 1.0, 1.0, 1.0, 5.0]).unwrap();

        let point = detector.as_point_detector().expect("z-score is pointwise");
        assert_eq!(point.classify(1.0), AnomalySignal::None);
        assert!((0.0..=1.0).contains(&point.score(1.0)));
    }

    /// Boxing the forest is what keeps this small; if someone unboxes it the
    /// enum grows ~7x and every detector pays for it.
    #[test]
    fn enum_stays_small() {
        assert!(
            std::mem::size_of::<Detector>() <= 192,
            "Detector grew to {} bytes — is a large variant unboxed?",
            std::mem::size_of::<Detector>()
        );
    }
}
