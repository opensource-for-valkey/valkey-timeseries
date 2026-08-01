#[cfg(test)]
mod tests {
    use crate::analysis::outliers::AnomalyMADEstimator;
    use crate::analysis::outliers::double_mad_outlier_detector::DoubleMadOutlierDetector;
    use crate::analysis::outliers::outlier_test_data::{
        EMPTY_DATASET, SAME_DATASET, TestData, beta_data_set, check_outliers,
        modified_beta_data_set, real_data_set,
    };
    use crate::analysis::outliers::{AnomalyDetector, AnomalySignal, MethodInfo, PointDetector};
    use std::collections::HashMap;

    /// A negative threshold inverts the fences. Before `deviation_and_boundary`
    /// mapped a negative fence distance to NaN, this flagged essentially every
    /// point — including the median itself — while still scoring it `0.0`,
    /// since `normalize_evidence` already treated the negative boundary as
    /// unusable. Both must now agree that there is nothing to be past.
    #[test]
    fn negative_threshold_does_not_flag_the_median() {
        let data = [1.0, 4.0, 4.0, 4.0, 5.0, 5.0, 5.0, 5.0, 7.0, 7.0, 8.0, 10.0];
        let mut detector = DoubleMadOutlierDetector::new(-3.0, AnomalyMADEstimator::Simple);
        detector.train(&data).unwrap();

        let Some(MethodInfo::Fenced {
            center_line: Some(median),
            ..
        }) = detector.model_info()
        else {
            panic!("expected a fitted center line");
        };

        assert_eq!(detector.classify(median), AnomalySignal::None);
        assert_eq!(detector.score(median), 0.0);
    }

    /// Data cases for SimpleQuantileEstimator
    fn simple_qe_test_data_map() -> HashMap<&'static str, TestData<'static>> {
        let mut map = HashMap::new();
        map.insert("Empty", TestData::new(&EMPTY_DATASET, &[]));
        map.insert("Same", TestData::new(&SAME_DATASET, &[]));
        map.insert(
            "Case1",
            TestData::new(
                &[
                    1.0, 4.0, 4.0, 4.0, 5.0, 5.0, 5.0, 5.0, 7.0, 7.0, 8.0, 10.0, 16.0, 30.0,
                ],
                &[1.0, 16.0, 30.0],
            ),
        );
        map.insert(
            "Real0",
            TestData::new(&real_data_set::X0, &[38594.0, 39075.0]),
        );
        map.insert(
            "Real1",
            TestData::new(&real_data_set::X1, &[0.0, 0.0, 0.0, 0.0, 1821.0]),
        );
        map.insert("Real2", TestData::new(&real_data_set::X2, &[95.0, 4364.0]));
        map.insert(
            "Real3",
            TestData::new(
                &real_data_set::X3,
                &[1067.0, 1085.0, 1133.0, 1643.0, 4642.0],
            ),
        );
        map.insert("Real4", TestData::new(&real_data_set::X4, &[]));
        map.insert("Beta0", TestData::new(&beta_data_set::X0, &[]));
        map.insert("Beta1", TestData::new(&beta_data_set::X1, &[3071.0]));
        map.insert("Beta2", TestData::new(&beta_data_set::X2, &[3642.0]));
        map.insert(
            "MBeta_Lower1",
            TestData::new(&modified_beta_data_set::LOWER1, &[-2000.0, 3612.0]),
        );
        map.insert(
            "MBeta_Lower2",
            TestData::new(&modified_beta_data_set::LOWER2, &[-2001.0, -2000.0, 3612.0]),
        );
        map.insert(
            "MBeta_Lower3",
            TestData::new(
                &modified_beta_data_set::LOWER3,
                &[-2002.0, -2001.0, -2000.0, 3612.0],
            ),
        );
        map.insert(
            "MBeta_Upper1",
            TestData::new(&modified_beta_data_set::UPPER1, &[3612.0, 6000.0]),
        );
        map.insert(
            "MBeta_Upper2",
            TestData::new(&modified_beta_data_set::UPPER2, &[6000.0, 6001.0]),
        );
        map.insert(
            "MBeta_Upper3",
            TestData::new(&modified_beta_data_set::UPPER3, &[6000.0, 6001.0, 6002.0]),
        );
        map.insert(
            "MBeta_Both0",
            TestData::new(&modified_beta_data_set::BOTH0, &[-2000.0, 6000.0]),
        );
        map.insert(
            "MBeta_Both1",
            TestData::new(
                &modified_beta_data_set::BOTH1,
                &[-2001.0, -2000.0, 6000.0, 6001.0],
            ),
        );
        map.insert(
            "MBeta_Both2",
            TestData::new(
                &modified_beta_data_set::BOTH2,
                &[-2002.0, -2001.0, -2000.0, 6000.0, 6001.0, 6002.0],
            ),
        );
        map
    }

    /// Data cases for HarrellDavisQuantileEstimator
    fn hd_qe_test_data_map() -> HashMap<&'static str, TestData<'static>> {
        let mut map = HashMap::new();
        map.insert("Empty", TestData::new(&EMPTY_DATASET, &[]));
        map.insert("Same", TestData::new(&SAME_DATASET, &[]));
        map.insert(
            "Real0",
            TestData::new(&real_data_set::X0, &[38594.0, 39075.0]),
        );
        map.insert(
            "Real1",
            TestData::new(&real_data_set::X1, &[0.0, 0.0, 0.0, 0.0, 1821.0]),
        );
        map.insert("Real2", TestData::new(&real_data_set::X2, &[95.0, 4364.0]));
        map.insert(
            "Real3",
            TestData::new(
                &real_data_set::X3,
                &[1067.0, 1085.0, 1133.0, 1643.0, 4642.0],
            ),
        );
        map.insert("Real4", TestData::new(&real_data_set::X4, &[]));
        map.insert("Beta0", TestData::new(&beta_data_set::X0, &[]));
        map.insert("Beta1", TestData::new(&beta_data_set::X1, &[3071.0]));
        map.insert("Beta2", TestData::new(&beta_data_set::X2, &[3642.0]));
        map.insert(
            "MBeta_Lower1",
            TestData::new(&modified_beta_data_set::LOWER1, &[-2000.0]),
        );
        map.insert(
            "MBeta_Lower2",
            TestData::new(&modified_beta_data_set::LOWER2, &[-2001.0, -2000.0]),
        );
        map.insert(
            "MBeta_Lower3",
            TestData::new(
                &modified_beta_data_set::LOWER3,
                &[-2002.0, -2001.0, -2000.0],
            ),
        );
        map.insert(
            "MBeta_Upper1",
            TestData::new(&modified_beta_data_set::UPPER1, &[6000.0]),
        );
        map.insert(
            "MBeta_Upper2",
            TestData::new(&modified_beta_data_set::UPPER2, &[6000.0, 6001.0]),
        );
        map.insert(
            "MBeta_Upper3",
            TestData::new(&modified_beta_data_set::UPPER3, &[6000.0, 6001.0, 6002.0]),
        );
        map.insert(
            "MBeta_Both0",
            TestData::new(&modified_beta_data_set::BOTH0, &[-2000.0, 6000.0]),
        );
        map.insert(
            "MBeta_Both1",
            TestData::new(
                &modified_beta_data_set::BOTH1,
                &[-2001.0, -2000.0, 6000.0, 6001.0],
            ),
        );
        map.insert(
            "MBeta_Both2",
            TestData::new(
                &modified_beta_data_set::BOTH2,
                &[-2002.0, -2001.0, -2000.0, 6000.0, 6001.0, 6002.0],
            ),
        );
        map
    }

    #[test]
    fn double_mad_outlier_detector_simple_qe_test() {
        let test_data_map = simple_qe_test_data_map();

        for (test_data_key, test_data) in test_data_map.iter() {
            let action = || {
                let threshold = DoubleMadOutlierDetector::DEFAULT_K;
                check_outliers(test_data_key, test_data, |_values| {
                    DoubleMadOutlierDetector::new(threshold, AnomalyMADEstimator::Simple)
                })
            };

            if test_data.values.is_empty() {
                assert!(
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(action)).is_err(),
                    "Expected panic for test case: {}",
                    test_data_key
                );
            } else {
                action();
            }
        }
    }

    #[test]
    fn double_mad_outlier_detector_hd_qe_test() {
        let test_data_map = hd_qe_test_data_map();

        for (&test_data_key, test_data) in test_data_map.iter() {
            let action = || {
                let threshold = DoubleMadOutlierDetector::DEFAULT_K;
                check_outliers(test_data_key, test_data, |_values| {
                    DoubleMadOutlierDetector::new(threshold, AnomalyMADEstimator::HarrellDavis)
                })
            };

            if test_data.values.is_empty() {
                assert!(
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(action)).is_err(),
                    "Expected panic for test case: {}",
                    test_data_key
                );
            } else {
                action();
            }
        }
    }
}
