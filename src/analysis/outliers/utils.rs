use crate::analysis::outliers::AnomalySignal;

/// Map `evidence` against a detection `boundary` onto `[0, 1]`, with `0.5` at the
/// boundary.
///
/// Every method reduces its verdict to one dimensionless ratio
/// `r = evidence / boundary`, where `r == 1` exactly at the point the method
/// starts flagging. `score = r / (r + 1)` then puts `0.0` at the center, `0.5`
/// at the boundary, and approaches `1.0` as evidence grows without bound. See
/// [`AnomalyDetector::detect`](super::AnomalyDetector::detect) for the contract
/// this implements.
///
/// Taking the boundary as an argument — rather than exposing a bare
/// `normalize(r)` each detector must remember to pre-divide — keeps the division
/// and the degenerate guard in one place, so a detector cannot normalize raw
/// evidence by accident.
///
/// # Edges
///
/// - No evidence (`<= 0`, or NaN) yields `0.0`, whatever the boundary. A sample
///   sitting at the center is the least anomalous thing there is, and a missing
///   reading is not evidence of an anomaly. Callers must also not flag NaN — see
///   [`detect_pointwise`](super::detect_pointwise).
/// - An unusable boundary — NaN (nothing fitted yet), negative, or `+∞` (a fence
///   nothing can cross) — yields `0.0`: there is no test to be past.
/// - A *collapsed* boundary (exactly `0.0`, i.e. a fitted scale of zero) with
///   positive evidence yields `1.0`. Every deviation is then infinitely many
///   units of a zero scale, and the fences have collapsed onto the center, so
///   the classifiers flag it; scoring it `0.0` would put "flagged" and
///   "score > 0.5" in direct contradiction. This is what a robust scale does on
///   a series that is more than half constant — a lone spike over a flat
///   baseline still has to be reported.
/// - Infinite evidence against a usable boundary yields `1.0`, matching the
///   classifiers, which flag infinities against any finite fence.
///
/// Those last two are the only places the contract admits an exact `1.0`, and
/// both exist so that "flagged" and "score > 0.5" cannot disagree.
#[inline]
pub(super) fn normalize_evidence(evidence: f64, boundary: f64) -> f64 {
    if evidence.is_nan() || evidence <= 0.0 {
        return 0.0;
    }
    if boundary.is_nan() || boundary < 0.0 || boundary == f64::INFINITY {
        return 0.0;
    }
    if boundary == 0.0 || evidence.is_infinite() {
        return 1.0;
    }

    // `evidence / (evidence + boundary)` rather than `r / (r + 1.0)` with
    // `r = evidence / boundary`: algebraically identical, but the latter
    // overflows to infinity (and then NaN from inf/inf) when `evidence` is
    // huge and `boundary` is tiny, since the intermediate ratio can exceed
    // `f64::MAX` even though neither input, nor the true score, is anywhere
    // near it.
    let score = evidence / (evidence + boundary);

    if score <= 0.5 && evidence > boundary {
        // `evidence / boundary` rounds to exactly 1.0 when evidence is a single
        // ULP past the boundary, which would report 0.5 — the not-flagged side —
        // for a sample its own classifier flags. Nudge it back across.
        return f64::from_bits(0.5f64.to_bits() + 1);
    }

    score
}

/// Signed deviation from `center`, and the distance out to the fence on the
/// value's own side.
///
/// The boundary is taken as the fence-minus-center subtraction rather than a
/// recomputation (e.g. `k * mad`), so a value sitting *exactly on the
/// reported fence* yields evidence and boundary from the identical
/// subtraction and scores exactly `0.5` via [`normalize_evidence`]. Shared by
/// every fenced detector (MAD, double MAD, IQR, Z-score, modified Z-score) so
/// scoring and classification read the same pair and cannot disagree about
/// which side of the fence a value falls on.
///
/// A misconfigured detector (a non-positive threshold) inverts its fences,
/// which would otherwise make this subtraction negative. `classify`'s
/// `deviation.abs() > boundary` is then true for essentially every value —
/// even the center itself — while [`normalize_evidence`] already treats a
/// negative boundary as unusable and reports `0.0`. Mapping it to `NaN` here
/// keeps the two in agreement: both sides read the fences as having nothing
/// to be past, rather than classify flagging what scoring calls maximally
/// normal.
#[inline]
pub(super) fn deviation_and_fence_distance(
    value: f64,
    center: f64,
    lower_fence: f64,
    upper_fence: f64,
) -> (f64, f64) {
    let deviation = value - center;
    let boundary = if deviation >= 0.0 {
        upper_fence - center
    } else {
        center - lower_fence
    };
    let boundary = if boundary < 0.0 { f64::NAN } else { boundary };
    (deviation, boundary)
}

#[inline]
pub(super) fn normalize_value(v: f64) -> f64 {
    if v.is_nan() { 0.0 } else { v }
}

#[inline]
pub(super) fn get_anomaly_direction(
    low_threshold: f64,
    hi_threshold: f64,
    value: f64,
) -> AnomalySignal {
    if value < low_threshold {
        AnomalySignal::Negative
    } else if value > hi_threshold {
        AnomalySignal::Positive
    } else {
        AnomalySignal::None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A non-positive threshold inverts the fences (`upper < center < lower`),
    /// which used to make this return a negative boundary — `classify`'s
    /// `deviation.abs() > boundary` is then true for essentially every value,
    /// including the center itself, while `normalize_evidence` already reports
    /// `0.0` for a negative boundary. Mapping it to NaN keeps both readings
    /// "nothing to be past" in agreement.
    #[test]
    fn inverted_fences_from_a_negative_threshold_yield_no_usable_boundary() {
        let center = 10.0;
        // As if built from `median - k * mad` / `median + k * mad` with a
        // negative `k`: the fences swap sides.
        let lower_fence = 16.0;
        let upper_fence = 4.0;

        for value in [center, center + 5.0, center - 5.0] {
            let (_, boundary) =
                deviation_and_fence_distance(value, center, lower_fence, upper_fence);
            assert!(
                boundary.is_nan(),
                "expected an unusable boundary for value {value}, got {boundary}"
            );
        }
    }

    /// The anchor the whole contract rests on: evidence sitting exactly on the
    /// boundary scores `0.5`, whatever the boundary happens to be.
    #[test]
    fn evidence_at_the_boundary_scores_one_half() {
        for boundary in [0.25, 1.0, 3.0, 3.5, 5.0, 1e6] {
            let s = normalize_evidence(boundary, boundary);
            assert!(
                (s - 0.5).abs() < 1e-12,
                "boundary {boundary} should score 0.5, got {s}"
            );
        }
    }

    /// The score must partition the same way the classifiers do: strictly above
    /// the boundary is flagged territory, at or below it is not.
    #[test]
    fn score_straddles_one_half_at_the_boundary() {
        assert!(normalize_evidence(2.9, 3.0) < 0.5);
        assert!(normalize_evidence(3.1, 3.0) > 0.5);
    }

    #[test]
    fn normalize_evidence_range_and_edges() {
        assert_eq!(normalize_evidence(f64::NAN, 3.0), 0.0);
        assert_eq!(normalize_evidence(-1.0, 3.0), 0.0);
        assert_eq!(normalize_evidence(0.0, 3.0), 0.0);

        // No usable boundary: nothing to be past.
        assert_eq!(normalize_evidence(100.0, -1.0), 0.0);
        assert_eq!(normalize_evidence(100.0, f64::NAN), 0.0);
        assert_eq!(normalize_evidence(100.0, f64::INFINITY), 0.0);
        assert_eq!(normalize_evidence(f64::INFINITY, f64::NAN), 0.0);

        // The two exact 1.0s the contract admits.
        assert_eq!(normalize_evidence(f64::INFINITY, 3.0), 1.0);
        assert_eq!(normalize_evidence(100.0, 0.0), 1.0);
    }

    /// Huge evidence against a vanishingly small (but usable) boundary must
    /// still land in `[0, 1]`. The naive `r = evidence / boundary` overflows to
    /// infinity here even though neither input is infinite, which then turns
    /// `r / (r + 1.0)` into `inf / inf`, i.e. NaN.
    #[test]
    fn huge_evidence_against_a_tiny_boundary_stays_finite_and_in_range() {
        let score = normalize_evidence(1e300, f64::MIN_POSITIVE / 2.0);
        assert!(
            score.is_finite() && (0.0..=1.0).contains(&score),
            "expected a finite score in [0, 1], got {score}"
        );
    }

    /// A collapsed scale is what a robust estimator reports for a series that is
    /// more than half constant. The fences collapse onto the center, so the
    /// classifiers flag every deviation — the score has to agree, or a lone
    /// spike over a flat baseline gets flagged while scoring "maximally normal".
    #[test]
    fn collapsed_boundary_makes_any_deviation_maximal() {
        assert_eq!(normalize_evidence(9.0, 0.0), 1.0);
        assert_eq!(normalize_evidence(1e-300, 0.0), 1.0);
        // ...but a sample sitting exactly on the collapsed center is not.
        assert_eq!(normalize_evidence(0.0, 0.0), 0.0);
    }

    /// The score must never land on the not-flagged side for evidence that is
    /// past the boundary, however narrowly — the ratio rounds to exactly 1.0
    /// within an ULP of the fence.
    #[test]
    fn evidence_one_ulp_past_the_boundary_scores_above_one_half() {
        for boundary in [1.0f64, 3.0, 3.5, 1e-8, 1e8] {
            let evidence = f64::from_bits(boundary.to_bits() + 1);
            assert!(
                evidence > boundary,
                "test setup: expected a larger neighbor"
            );

            let s = normalize_evidence(evidence, boundary);
            assert!(
                s > 0.5,
                "evidence {evidence} past boundary {boundary} scored {s}, which reads as not-flagged"
            );
        }
    }

    #[test]
    fn normalize_evidence_is_monotone_and_stays_below_one() {
        let scores: Vec<f64> = [0.5, 1.0, 2.0, 10.0, 1e12]
            .iter()
            .map(|&e| normalize_evidence(e, 1.0))
            .collect();

        for pair in scores.windows(2) {
            assert!(
                pair[0] < pair[1],
                "expected monotone increase, got {scores:?}"
            );
        }
        assert!(
            *scores.last().unwrap() < 1.0,
            "finite evidence must stay strictly below 1.0, got {scores:?}"
        );
    }

    /// The threshold enters only as a divisor, so raising it rescales scores
    /// without reordering them.
    #[test]
    fn normalize_evidence_preserves_ordering_across_boundaries() {
        let evidence = [0.5, 1.0, 4.0, 9.0];
        let tight: Vec<f64> = evidence
            .iter()
            .map(|&e| normalize_evidence(e, 1.0))
            .collect();
        let loose: Vec<f64> = evidence
            .iter()
            .map(|&e| normalize_evidence(e, 5.0))
            .collect();

        for i in 0..evidence.len() - 1 {
            assert!(tight[i] < tight[i + 1]);
            assert!(loose[i] < loose[i + 1]);
            assert!(
                loose[i] < tight[i],
                "a looser boundary must lower every score"
            );
        }
    }
}
