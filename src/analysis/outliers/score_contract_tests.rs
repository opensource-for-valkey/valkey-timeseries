//! The cross-method score contract, held against every detector.
//!
//! Each method anchored its detection boundary at a different score before this
//! suite existed — `mad` flagged at `1.0`, `iqr` at anything above `0.0`,
//! `zscore` at `0.75` for `T=3` but `0.857` for `T=6` — so no property spanning
//! the methods was expressible. The contract on
//! [`AnomalyDetector::detect`](super::AnomalyDetector::detect) makes one
//! expressible, and this is where it is enforced:
//!
//! > A sample's score is `0.5` exactly at the method's detection boundary.
//! > `score > 0.5` ⟺ flagged.
//!
//! Every detector runs against every fixture below. The exceptions in
//! [`Carveout`] are named rather than absorbed into a tolerance, because each
//! one is a window where a sample is *scored without being classified* — the
//! contract has nothing to say there, and blurring that into a fudge factor
//! would hide real disagreements elsewhere.

use super::{
    AnomalyDetectionMethodOptions, AnomalyDetector, AnomalyMethod, AnomalyResult, AnomalySignal,
    Detector, MADAnomalyOptions, RCFOptions, RCFThreshold, SmoothedZScoreOptions,
};
use crate::analysis::outliers::esd_outlier_detector::ESDOutlierOptions;

/// Warmup length shared by the RCF cases, so the carve-out and the options
/// cannot drift apart.
const RCF_WARMUP: usize = 16;
/// Lag shared by the smoothed z-score cases, for the same reason.
const SMOOTHED_LAG: usize = 8;

/// A window in which samples are scored but never classified, so
/// [`Invariant 3`](check_score_partitions_anomalies) cannot apply to them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Carveout {
    /// Every sample is both scored and classified.
    None,
    /// Indices `0..n` are scored but suppressed from flagging.
    LeadingWindow(usize),
}

impl Carveout {
    fn exempts(&self, index: usize) -> bool {
        match self {
            Carveout::None => false,
            Carveout::LeadingWindow(n) => index < *n,
        }
    }
}

struct MethodCase {
    name: &'static str,
    options: AnomalyDetectionMethodOptions,
    carveout: Carveout,
}

impl MethodCase {
    /// RCF cannot be run against a fixture containing NaN *in a unit test*: the
    /// forest rejects the update and the module logs a warning, and the logging
    /// shim needs a live Valkey context that no unit test has. The limitation is
    /// the harness's, not the detector's, so it is skipped by fixture rather
    /// than dropped from the suite.
    fn skips(&self, fixture: &Fixture) -> bool {
        fixture.values.iter().any(|v| v.is_nan())
            && self.options.method() == AnomalyMethod::RandomCutForest
    }
}

/// Every scoring path in the module. RCF appears twice because its two
/// threshold modes used to be two different score scales; keeping both here is
/// what stops them drifting apart again.
fn method_cases() -> Vec<MethodCase> {
    vec![
        MethodCase {
            name: "zscore",
            options: AnomalyDetectionMethodOptions::ZScore(Some(3.0)),
            carveout: Carveout::None,
        },
        MethodCase {
            name: "modified-zscore",
            options: AnomalyDetectionMethodOptions::ModifiedZScore(Some(3.5)),
            carveout: Carveout::None,
        },
        MethodCase {
            name: "mad",
            options: AnomalyDetectionMethodOptions::Mad(MADAnomalyOptions::default()),
            carveout: Carveout::None,
        },
        MethodCase {
            name: "double-mad",
            options: AnomalyDetectionMethodOptions::DoubleMAD(MADAnomalyOptions::default()),
            carveout: Carveout::None,
        },
        MethodCase {
            name: "iqr",
            options: AnomalyDetectionMethodOptions::InterQuartileRange(Some(1.5)),
            carveout: Carveout::None,
        },
        MethodCase {
            name: "cusum",
            options: AnomalyDetectionMethodOptions::Cusum,
            carveout: Carveout::None,
        },
        MethodCase {
            name: "ewma",
            options: AnomalyDetectionMethodOptions::Ewma(Some(0.3)),
            carveout: Carveout::None,
        },
        MethodCase {
            name: "smoothed-zscore",
            options: AnomalyDetectionMethodOptions::SmoothedZScore(SmoothedZScoreOptions {
                threshold: 3.0,
                influence: 0.0,
                lag: SMOOTHED_LAG,
            }),
            // The first `lag` samples form the initial window: they are padded
            // with 0.0 and never classified.
            carveout: Carveout::LeadingWindow(SMOOTHED_LAG),
        },
        MethodCase {
            name: "esd",
            options: AnomalyDetectionMethodOptions::Esd(Some(ESDOutlierOptions::default())),
            carveout: Carveout::None,
        },
        MethodCase {
            name: "rcf/stddev",
            options: AnomalyDetectionMethodOptions::Rcf(RCFOptions {
                threshold: Some(RCFThreshold::StdDev(3.0)),
                output_after: Some(RCF_WARMUP),
                ..Default::default()
            }),
            // The forest has not learned the distribution yet, so detections in
            // this region are suppressed while the scores are still recorded.
            carveout: Carveout::LeadingWindow(RCF_WARMUP),
        },
        MethodCase {
            name: "rcf/contamination",
            options: AnomalyDetectionMethodOptions::Rcf(RCFOptions {
                threshold: Some(RCFThreshold::Contamination(0.05)),
                output_after: Some(RCF_WARMUP),
                ..Default::default()
            }),
            carveout: Carveout::LeadingWindow(RCF_WARMUP),
        },
    ]
}

struct Fixture {
    name: &'static str,
    values: Vec<f64>,
}

/// Deterministic pseudo-noise. A fixed LCG rather than a `rand` dependency, so
/// a failure reported by CI reproduces exactly.
fn noise(i: usize) -> f64 {
    let x = (i as u64)
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    ((x >> 33) as f64 / (1u64 << 31) as f64) - 0.5
}

fn baseline(n: usize) -> Vec<f64> {
    (0..n).map(|i| 50.0 + noise(i)).collect()
}

fn fixtures() -> Vec<Fixture> {
    let mut single_spike = baseline(64);
    single_spike[40] = 500.0;

    let mut bilateral = baseline(64);
    bilateral[20] = 500.0;
    bilateral[45] = -400.0;

    // Right-skewed: the half the distribution spreads on is the half
    // `double-mad` exists to measure separately.
    let skewed: Vec<f64> = (0..64)
        .map(|i| {
            let base = 10.0 + noise(i);
            if i % 4 == 0 { base * 3.0 } else { base }
        })
        .collect();

    let mut with_nan = baseline(64);
    with_nan[10] = f64::NAN;
    with_nan[33] = f64::NAN;

    // More than half constant, so every robust scale estimate collapses to zero
    // while a real spike is still present.
    let mut flat_with_spike = vec![7.0; 64];
    flat_with_spike[50] = 900.0;

    vec![
        Fixture {
            name: "clean",
            values: baseline(64),
        },
        Fixture {
            name: "single-spike",
            values: single_spike,
        },
        Fixture {
            name: "bilateral-spikes",
            values: bilateral,
        },
        Fixture {
            name: "skewed",
            values: skewed,
        },
        Fixture {
            name: "constant",
            values: vec![42.0; 64],
        },
        Fixture {
            name: "flat-with-spike",
            values: flat_with_spike,
        },
        Fixture {
            name: "with-nan",
            values: with_nan,
        },
    ]
}

fn run(case: &MethodCase, values: &[f64]) -> AnomalyResult {
    let mut detector = Detector::build(values, &case.options)
        .unwrap_or_else(|e| panic!("{}: detector should build: {e:?}", case.name));
    detector
        .train(values)
        .unwrap_or_else(|e| panic!("{}: train should succeed: {e:?}", case.name));
    detector
        .detect(values)
        .unwrap_or_else(|e| panic!("{}: detect should succeed: {e:?}", case.name))
}

/// Invariant 1: one score per observation. This is `debug_assert`ed on the
/// dispatch path; here it is a real assertion.
#[test]
fn scores_have_one_entry_per_observation() {
    for case in method_cases() {
        for fixture in fixtures() {
            if case.skips(&fixture) {
                continue;
            }
            let result = run(&case, &fixture.values);
            assert_eq!(
                result.scores.len(),
                fixture.values.len(),
                "{}/{}: expected one score per observation",
                case.name,
                fixture.name
            );
            for anomaly in &result.anomalies {
                assert!(
                    anomaly.index < fixture.values.len(),
                    "{}/{}: anomaly index {} out of bounds",
                    case.name,
                    fixture.name,
                    anomaly.index
                );
            }
        }
    }
}

/// Invariant 2: every score is finite and inside `[0, 1]`.
#[test]
fn scores_are_finite_and_in_the_unit_interval() {
    for case in method_cases() {
        for fixture in fixtures() {
            if case.skips(&fixture) {
                continue;
            }
            let result = run(&case, &fixture.values);
            for (i, &score) in result.scores.iter().enumerate() {
                assert!(
                    score.is_finite() && (0.0..=1.0).contains(&score),
                    "{}/{}: score[{i}] = {score} is outside [0, 1]",
                    case.name,
                    fixture.name
                );
            }
        }
    }
}

/// Invariant 3, the one the whole contract exists for: the score partitions the
/// series exactly the way the classifier does.
///
/// Note which side the boundary belongs to. Every classifier here compares
/// strictly, so a sample sitting *exactly* on the fence is not flagged and must
/// score exactly `0.5` — `> 0.5 ⟺ flagged` paired with `<= 0.5 ⟺ not flagged`,
/// which is a biconditional. `< 0.5 ⟺ not flagged` would leave the fence case
/// unspecified.
#[test]
fn check_score_partitions_anomalies() {
    for case in method_cases() {
        for fixture in fixtures() {
            if case.skips(&fixture) {
                continue;
            }
            let result = run(&case, &fixture.values);
            let flagged: Vec<usize> = result.anomalies.iter().map(|a| a.index).collect();

            for (i, &score) in result.scores.iter().enumerate() {
                if case.carveout.exempts(i) {
                    continue;
                }

                let is_flagged = flagged.contains(&i);
                assert_eq!(
                    score > 0.5,
                    is_flagged,
                    "{}/{}: score[{i}] = {score} (value {}) says {}, but the detector says {}",
                    case.name,
                    fixture.name,
                    fixture.values[i],
                    if score > 0.5 { "anomaly" } else { "normal" },
                    if is_flagged { "anomaly" } else { "normal" },
                );
            }
        }
    }
}

/// Invariant 4: the score reported alongside an anomaly is the score recorded
/// for its index. Two fields, one number.
#[test]
fn anomaly_score_matches_the_score_vector() {
    for case in method_cases() {
        for fixture in fixtures() {
            if case.skips(&fixture) {
                continue;
            }
            let result = run(&case, &fixture.values);
            for anomaly in &result.anomalies {
                assert_eq!(
                    anomaly.score, result.scores[anomaly.index],
                    "{}/{}: Anomaly::score disagrees with scores[{}]",
                    case.name, fixture.name, anomaly.index
                );
            }
        }
    }
}

/// Invariant 5: monotone in evidence. Only the [`PointDetector`]s can be asked
/// this, since only they answer about a value independently of its position.
#[test]
fn point_detectors_score_monotonically_away_from_center() {
    let values = baseline(64);

    for case in method_cases() {
        let mut detector = match Detector::build(&values, &case.options) {
            Ok(d) => d,
            Err(e) => panic!("{}: detector should build: {e:?}", case.name),
        };
        detector.train(&values).unwrap();
        let Some(point) = detector.as_point_detector() else {
            continue;
        };

        // Walk outward from the middle of the fitted data in both directions.
        for direction in [1.0f64, -1.0] {
            let mut previous = f64::NEG_INFINITY;
            for step in 0..40 {
                let probe = 50.0 + direction * (step as f64) * 2.0;
                let score = point.score(probe);
                assert!(
                    score >= previous - 1e-12,
                    "{}: score fell from {previous} to {score} while moving further from center (probe {probe})",
                    case.name
                );
                previous = score;
            }
        }
    }
}

/// Invariant 6: a constant series has no anomalies and no evidence of any.
/// `double-mad` and `smoothed-zscore` used to score `1.0` here where the others
/// scored `0.0`.
///
/// Scoped to the methods that fit a *scale* to the data. RCF fits a forest
/// instead, and the partition a forest cuts through identical points is
/// randomized, so its raw scores vary even on a constant series; there is no
/// zero-variance branch for it to take.
#[test]
fn constant_series_scores_zero_everywhere() {
    let values = vec![42.0; 64];

    for case in method_cases() {
        if case.options.method() == AnomalyMethod::RandomCutForest {
            continue;
        }

        let result = run(&case, &values);

        assert!(
            result.anomalies.is_empty(),
            "{}: constant series must yield no anomalies, got {:?}",
            case.name,
            result.anomalies
        );
        for (i, &score) in result.scores.iter().enumerate() {
            assert_eq!(
                score, 0.0,
                "{}: constant series scored {score} at index {i}",
                case.name
            );
        }
    }
}

/// Invariant 7: `±∞` is maximally anomalous — flagged, scored `1.0`, with the
/// direction it actually points. `NaN` is the opposite case: a missing reading
/// is not evidence, so it scores `0.0` and is never flagged.
///
/// Both used to be broken in the same place. `detect_pointwise` skips only NaN,
/// so `±∞` reached `classify`, crossed every finite fence, and was flagged —
/// while the normalizer handed back `0.0` for non-finite evidence. An infinite
/// observation was therefore *flagged with score 0.0*.
#[test]
fn infinities_are_maximal_and_nan_is_never_flagged() {
    for case in method_cases() {
        // Only the pointwise detectors have a position-independent answer here;
        // the sequential ones propagate an infinity into their running
        // statistics, which is a separate question from how it is scored.
        let values = baseline(64);
        let mut detector = Detector::build(&values, &case.options).unwrap();
        detector.train(&values).unwrap();
        let Some(point) = detector.as_point_detector() else {
            continue;
        };

        for infinity in [f64::INFINITY, f64::NEG_INFINITY] {
            let score = point.score(infinity);
            assert_eq!(
                score, 1.0,
                "{}: {infinity} should score 1.0, got {score}",
                case.name
            );

            let expected = if infinity.is_sign_positive() {
                AnomalySignal::Positive
            } else {
                AnomalySignal::Negative
            };
            assert_eq!(
                point.classify(infinity),
                expected,
                "{}: {infinity} should be flagged {expected:?}",
                case.name
            );
        }

        assert_eq!(
            point.score(f64::NAN),
            0.0,
            "{}: NaN should score 0.0",
            case.name
        );
    }

    // And through the batch path, where `detect_pointwise` skips NaN outright.
    //
    // Scoped to the pointwise detectors. The sequential ones still substitute
    // `0.0` for a missing reading and let the substitute enter their running
    // statistic, so an EWMA or CUSUM baseline is dragged toward zero by a gap in
    // the data — a separate defect, tracked as out of scope for the score
    // contract because it concerns what NaN contributes to a *fitted baseline*,
    // not how it is scored.
    for case in method_cases() {
        let mut values = baseline(64);
        values[7] = f64::NAN;

        let probe = Detector::build(&values, &case.options).unwrap();
        if probe.as_point_detector().is_none() {
            continue;
        }

        let result = run(&case, &values);
        assert_eq!(result.scores[7], 0.0, "{}: NaN should score 0.0", case.name);
        assert!(
            !result.anomalies.iter().any(|a| a.index == 7),
            "{}: NaN must never be flagged",
            case.name
        );
    }
}

/// Invariant 8: direction is reported on the data's own scale.
///
/// The probe values are deliberately far from any internal statistic's scale —
/// RCF's `StdDev` path used to pick a direction by comparing the *data value*
/// to the mean of the *forest scores*, which are unrelated units. On a series
/// living near zero that mistake is invisible; on one living near 10,000 it
/// reports every anomaly as `Positive`.
#[test]
fn direction_follows_the_data_not_an_internal_statistic() {
    let mut values: Vec<f64> = (0..96).map(|i| 10_000.0 + noise(i)).collect();
    values[40] = 90_000.0; // unmistakable high spike
    values[70] = -70_000.0; // unmistakable low dip

    for case in method_cases() {
        let result = run(&case, &values);

        for anomaly in &result.anomalies {
            if anomaly.index == 40 {
                assert_eq!(
                    anomaly.signal,
                    AnomalySignal::Positive,
                    "{}: the high spike must be Positive",
                    case.name
                );
            }
            if anomaly.index == 70 {
                assert_eq!(
                    anomaly.signal,
                    AnomalySignal::Negative,
                    "{}: the low dip must be Negative",
                    case.name
                );
            }
        }
    }
}

/// Invariant 9: the threshold enters only as a divisor on a fixed evidence
/// statistic, so raising it rescales scores without reordering them.
///
/// Scoped deliberately. It does **not** hold for `esd`, where `alpha` moves each
/// iteration's critical value and therefore the truncation point, nor for
/// `smoothed-zscore` with `influence > 0`, where classification feeds back into
/// the rolling baseline that later evidence is measured against. In both, the
/// threshold changes the evidence itself rather than its scale.
#[test]
fn threshold_rescales_scores_without_reordering_them() {
    let mut values = baseline(64);
    values[15] = 300.0;
    values[38] = 180.0;
    values[52] = -220.0;

    let cases: Vec<(
        &str,
        AnomalyDetectionMethodOptions,
        AnomalyDetectionMethodOptions,
    )> = vec![
        (
            "zscore",
            AnomalyDetectionMethodOptions::ZScore(Some(2.0)),
            AnomalyDetectionMethodOptions::ZScore(Some(6.0)),
        ),
        (
            "modified-zscore",
            AnomalyDetectionMethodOptions::ModifiedZScore(Some(2.0)),
            AnomalyDetectionMethodOptions::ModifiedZScore(Some(7.0)),
        ),
        (
            "mad",
            AnomalyDetectionMethodOptions::Mad(MADAnomalyOptions {
                k: 2.0,
                ..Default::default()
            }),
            AnomalyDetectionMethodOptions::Mad(MADAnomalyOptions {
                k: 6.0,
                ..Default::default()
            }),
        ),
        (
            "double-mad",
            AnomalyDetectionMethodOptions::DoubleMAD(MADAnomalyOptions {
                k: 2.0,
                ..Default::default()
            }),
            AnomalyDetectionMethodOptions::DoubleMAD(MADAnomalyOptions {
                k: 6.0,
                ..Default::default()
            }),
        ),
        (
            "iqr",
            AnomalyDetectionMethodOptions::InterQuartileRange(Some(1.5)),
            AnomalyDetectionMethodOptions::InterQuartileRange(Some(4.5)),
        ),
    ];

    for (name, tight, loose) in cases {
        let tight_scores = run(
            &MethodCase {
                name,
                options: tight,
                carveout: Carveout::None,
            },
            &values,
        )
        .scores;
        let loose_scores = run(
            &MethodCase {
                name,
                options: loose,
                carveout: Carveout::None,
            },
            &values,
        )
        .scores;

        for i in 0..values.len() {
            for j in 0..values.len() {
                let tight_order = tight_scores[i].partial_cmp(&tight_scores[j]).unwrap();
                let loose_order = loose_scores[i].partial_cmp(&loose_scores[j]).unwrap();
                assert_eq!(
                    tight_order, loose_order,
                    "{name}: raising the threshold reordered scores[{i}] and scores[{j}] \
                     ({} vs {} became {} vs {})",
                    tight_scores[i], tight_scores[j], loose_scores[i], loose_scores[j]
                );
            }
        }

        // And a looser threshold must lower every score, never raise one.
        for i in 0..values.len() {
            assert!(
                loose_scores[i] <= tight_scores[i] + 1e-12,
                "{name}: a looser threshold raised scores[{i}] from {} to {}",
                tight_scores[i],
                loose_scores[i]
            );
        }
    }
}

/// The boundary case, constructed exactly rather than approached: a value
/// sitting on the fence is not flagged, and scores exactly `0.5`.
#[test]
fn a_value_exactly_on_the_fence_scores_one_half_and_is_not_flagged() {
    use super::MethodInfo;

    let values = baseline(64);

    for case in method_cases() {
        let mut detector = Detector::build(&values, &case.options).unwrap();
        detector.train(&values).unwrap();

        let Some(MethodInfo::Fenced {
            lower_fence,
            upper_fence,
            ..
        }) = detector.model_info()
        else {
            continue;
        };
        let Some(point) = detector.as_point_detector() else {
            continue;
        };

        for fence in [lower_fence, upper_fence] {
            if !fence.is_finite() {
                continue;
            }
            let score = point.score(fence);
            assert!(
                (score - 0.5).abs() < 1e-9,
                "{}: the fence at {fence} scored {score}, expected 0.5",
                case.name
            );
            assert_eq!(
                point.classify(fence),
                AnomalySignal::None,
                "{}: the fence at {fence} must sit on the not-flagged side",
                case.name
            );
        }
    }
}

/// The fitted upper fence and the distance from the center out to it, for the
/// methods that report fences. Probes are expressed as multiples of that
/// distance so each case is tested on its own scale.
fn fence_and_span(detector: &Detector) -> Option<(f64, f64)> {
    use super::MethodInfo;

    let MethodInfo::Fenced {
        lower_fence,
        upper_fence,
        ..
    } = detector.model_info()?
    else {
        return None;
    };

    if !lower_fence.is_finite() || !upper_fence.is_finite() || upper_fence <= lower_fence {
        return None;
    }

    let span = (upper_fence - lower_fence) / 2.0;
    Some((upper_fence, span))
}

/// `mad` used to `clamp(…, 0, 1)` at the fence, so a point 3.1 MADs out and one
/// 300 MADs out both scored exactly `1.0` — the score column carried no ranking
/// at all among the samples it had flagged.
#[test]
fn scores_rank_samples_beyond_the_fence() {
    let values = baseline(64);

    for case in method_cases() {
        let mut detector = Detector::build(&values, &case.options).unwrap();
        detector.train(&values).unwrap();
        let (Some((fence, span)), Some(point)) =
            (fence_and_span(&detector), detector.as_point_detector())
        else {
            continue;
        };

        let near = point.score(fence + span * 0.5);
        let far = point.score(fence + span * 50.0);
        let further = point.score(fence + span * 5_000.0);

        assert!(
            0.5 < near && near < far && far < further && further < 1.0,
            "{}: expected a strict ranking past the fence, got {near} < {far} < {further}",
            case.name
        );
    }
}

/// `iqr` measured the distance *beyond* the fence, so every in-range sample —
/// the majority of the series, and the whole point of `FULL` output — scored
/// exactly `0.0`. A near-miss was indistinguishable from a sample at the median.
#[test]
fn scores_rank_samples_inside_the_fence() {
    let values = baseline(64);

    for case in method_cases() {
        let mut detector = Detector::build(&values, &case.options).unwrap();
        detector.train(&values).unwrap();
        let (Some((fence, span)), Some(point)) =
            (fence_and_span(&detector), detector.as_point_detector())
        else {
            continue;
        };

        // Walk in from the fence toward the center, staying strictly inside.
        let center = fence - span;
        let just_inside = point.score(fence - span * 0.05);
        let halfway = point.score(fence - span * 0.5);
        let at_center = point.score(center);

        assert!(
            at_center < halfway && halfway < just_inside && just_inside < 0.5,
            "{}: expected in-range samples to be graded, got {at_center} < {halfway} < {just_inside}",
            case.name
        );
    }
}

/// The methods that report fences must agree with themselves: the score crosses
/// `0.5` at exactly the fence they advertise in `method_info`.
#[test]
fn reported_fences_agree_with_the_score_scale() {
    use super::MethodInfo;

    let values = baseline(64);

    for case in method_cases() {
        let mut detector = Detector::build(&values, &case.options).unwrap();
        detector.train(&values).unwrap();

        let Some(MethodInfo::Fenced {
            lower_fence,
            upper_fence,
            ..
        }) = detector.model_info()
        else {
            continue;
        };
        let Some(point) = detector.as_point_detector() else {
            continue;
        };

        if !lower_fence.is_finite() || !upper_fence.is_finite() {
            continue;
        }

        let width = upper_fence - lower_fence;
        assert!(
            point.score(upper_fence + width * 0.01) > 0.5,
            "{}: just past the upper fence must read as flagged",
            case.name
        );
        assert!(
            point.score(upper_fence - width * 0.01) < 0.5,
            "{}: just inside the upper fence must read as normal",
            case.name
        );
        assert!(
            point.score(lower_fence - width * 0.01) > 0.5,
            "{}: just past the lower fence must read as flagged",
            case.name
        );
        assert!(
            point.score(lower_fence + width * 0.01) < 0.5,
            "{}: just inside the lower fence must read as normal",
            case.name
        );
    }
}

/// Detectors must not silently disagree about how many methods exist. If a
/// scoring path is added without a case here, the contract stops covering it.
#[test]
fn every_method_has_a_case() {
    let covered: Vec<AnomalyMethod> = method_cases()
        .iter()
        .map(|case| case.options.method())
        .collect();

    for method in [
        AnomalyMethod::Ewma,
        AnomalyMethod::Cusum,
        AnomalyMethod::ZScore,
        AnomalyMethod::ModifiedZScore,
        AnomalyMethod::SmoothedZScore,
        AnomalyMethod::Mad,
        AnomalyMethod::DoubleMAD,
        AnomalyMethod::InterquartileRange,
        AnomalyMethod::RandomCutForest,
        AnomalyMethod::Esd,
    ] {
        assert!(
            covered.contains(&method),
            "{method:?} has no score-contract case"
        );
    }
}
