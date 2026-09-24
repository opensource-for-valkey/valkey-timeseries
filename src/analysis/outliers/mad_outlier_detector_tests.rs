use super::mad_outlier_detector::MadOutlierDetector;
use super::outlier_test_data::{
    EMPTY_DATASET, SAME_DATASET, TestData, beta_data_set, modified_beta_data_set, real_data_set,
    yang_data_set,
};
use std::collections::HashMap;
use std::sync::LazyLock;

/// Data cases for SimpleQuantileEstimator
static SIMPLE_QE_TEST_DATA_MAP: LazyLock<HashMap<&'static str, TestData>> = LazyLock::new(|| {
    let mut map = HashMap::new();

    map.insert("Empty", TestData::new(&EMPTY_DATASET, &[]));
    map.insert("Same", TestData::new(&SAME_DATASET, &[]));
    map.insert("Yang_X0", TestData::new(&yang_data_set::X0, &[]));
    map.insert("Yang_X1", TestData::new(&yang_data_set::X1, &[1000.0]));
    map.insert(
        "Yang_X2",
        TestData::new(&yang_data_set::X2, &[500.0, 1000.0]),
    );
    map.insert("Yang_X3", TestData::new(&yang_data_set::X3, &[1000.0]));
    map.insert(
        "Yang_X4",
        TestData::new(&yang_data_set::X4, &[300.0, 500.0, 1000.0, 1500.0]),
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
    map.insert(
        "Real4",
        TestData::new(
            &real_data_set::X4,
            &[
                14893.0, 15056.0, 15364.0, 15483.0, 16504.0, 17208.0, 17446.0,
            ],
        ),
    );
    map.insert(
        "Beta0",
        TestData::new(
            &beta_data_set::X0,
            &[
                2308.0, 2504.0, 2556.0, 2569.0, 2591.0, 2604.0, 2754.0, 2899.0, 3262.0, 3325.0,
                3329.0,
            ],
        ),
    );
    map.insert("Beta1", TestData::new(&beta_data_set::X1, &[3071.0]));
    map.insert("Beta2", TestData::new(&beta_data_set::X2, &[3642.0]));
    map.insert(
        "MBeta_Lower1",
        TestData::new(&modified_beta_data_set::LOWER1, &[-2000.0, 2919.0, 3612.0]),
    );
    map.insert(
        "MBeta_Lower2",
        TestData::new(
            &modified_beta_data_set::LOWER2,
            &[-2001.0, -2000.0, 2919.0, 3612.0],
        ),
    );
    map.insert(
        "MBeta_Lower3",
        TestData::new(
            &modified_beta_data_set::LOWER3,
            &[-2002.0, -2001.0, -2000.0, 2919.0, 3612.0],
        ),
    );
    map.insert(
        "MBeta_Upper1",
        TestData::new(&modified_beta_data_set::UPPER1, &[2919.0, 3612.0, 6000.0]),
    );
    map.insert(
        "MBeta_Upper2",
        TestData::new(
            &modified_beta_data_set::UPPER2,
            &[2919.0, 3612.0, 6000.0, 6001.0],
        ),
    );
    map.insert(
        "MBeta_Upper3",
        TestData::new(
            &modified_beta_data_set::UPPER3,
            &[2919.0, 3612.0, 6000.0, 6001.0, 6002.0],
        ),
    );
    map.insert(
        "MBeta_Both0",
        TestData::new(
            &modified_beta_data_set::BOTH0,
            &[-2000.0, 2919.0, 3612.0, 6000.0],
        ),
    );
    map.insert(
        "MBeta_Both1",
        TestData::new(
            &modified_beta_data_set::BOTH1,
            &[-2001.0, -2000.0, 2919.0, 3612.0, 6000.0, 6001.0],
        ),
    );
    map.insert(
        "MBeta_Both2",
        TestData::new(
            &modified_beta_data_set::BOTH2,
            &[
                -2002.0, -2001.0, -2000.0, 2919.0, 3612.0, 6000.0, 6001.0, 6002.0,
            ],
        ),
    );

    map
});

/// Data cases for HarrellDavisQuantileEstimator
static HD_QE_TEST_DATA_MAP: LazyLock<HashMap<&'static str, TestData>> = LazyLock::new(|| {
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
    map.insert(
        "Real4",
        TestData::new(
            &real_data_set::X4,
            &[
                14893.0, 15056.0, 15364.0, 15483.0, 16504.0, 17208.0, 17446.0,
            ],
        ),
    );
    map.insert(
        "Beta0",
        TestData::new(
            &beta_data_set::X0,
            &[
                2504.0, 2556.0, 2569.0, 2591.0, 2604.0, 2754.0, 2899.0, 3262.0, 3325.0, 3329.0,
            ],
        ),
    );
    map.insert("Beta1", TestData::new(&beta_data_set::X1, &[3071.0]));
    map.insert("Beta2", TestData::new(&beta_data_set::X2, &[3642.0]));
    map.insert(
        "MBeta_Lower1",
        TestData::new(&modified_beta_data_set::LOWER1, &[-2000.0, 2919.0, 3612.0]),
    );
    map.insert(
        "MBeta_Lower2",
        TestData::new(
            &modified_beta_data_set::LOWER2,
            &[-2001.0, -2000.0, 2919.0, 3612.0],
        ),
    );
    map.insert(
        "MBeta_Lower3",
        TestData::new(
            &modified_beta_data_set::LOWER3,
            &[-2002.0, -2001.0, -2000.0, 2919.0, 3612.0],
        ),
    );
    map.insert(
        "MBeta_Upper1",
        TestData::new(&modified_beta_data_set::UPPER1, &[2919.0, 3612.0, 6000.0]),
    );
    map.insert(
        "MBeta_Upper2",
        TestData::new(
            &modified_beta_data_set::UPPER2,
            &[2919.0, 3612.0, 6000.0, 6001.0],
        ),
    );
    map.insert(
        "MBeta_Upper3",
        TestData::new(
            &modified_beta_data_set::UPPER3,
            &[2919.0, 3612.0, 6000.0, 6001.0, 6002.0],
        ),
    );
    map.insert(
        "MBeta_Both0",
        TestData::new(
            &modified_beta_data_set::BOTH0,
            &[-2000.0, 2919.0, 3612.0, 6000.0],
        ),
    );
    map.insert(
        "MBeta_Both1",
        TestData::new(
            &modified_beta_data_set::BOTH1,
            &[-2001.0, -2000.0, 2919.0, 3612.0, 6000.0, 6001.0],
        ),
    );
    map.insert(
        "MBeta_Both2",
        TestData::new(
            &modified_beta_data_set::BOTH2,
            &[
                -2002.0, -2001.0, -2000.0, 2919.0, 3612.0, 6000.0, 6001.0, 6002.0,
            ],
        ),
    );

    map
});

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::outliers::AnomalyMADEstimator;
    use crate::analysis::outliers::outlier_test_data::check_outliers;

    #[test]
    fn mad_outlier_detector_simple_qe_tests() {
        let mut keys: Vec<&&str> = SIMPLE_QE_TEST_DATA_MAP.keys().collect();
        keys.sort();

        for &test_data_key in keys {
            let test_data = &SIMPLE_QE_TEST_DATA_MAP[test_data_key];

            if test_data.values.is_empty() {
                let result = std::panic::catch_unwind(|| {
                    MadOutlierDetector::with_estimator(AnomalyMADEstimator::Simple)
                });
                assert!(
                    result.is_err(),
                    "Expected panic for test case: {}",
                    test_data_key
                );
            } else {
                check_outliers(test_data_key, test_data, |_values| {
                    MadOutlierDetector::with_estimator(AnomalyMADEstimator::Simple)
                });
            }
        }
    }

    #[test]
    fn mad_outlier_detector_hd_qe_tests() {
        let mut keys: Vec<&&str> = HD_QE_TEST_DATA_MAP.keys().collect();
        keys.sort();

        for &test_data_key in keys {
            let test_data = &HD_QE_TEST_DATA_MAP[test_data_key];
            if test_data.values.is_empty() {
                let result = std::panic::catch_unwind(|| {
                    MadOutlierDetector::with_estimator(AnomalyMADEstimator::HarrellDavis)
                });
                assert!(
                    result.is_err(),
                    "Expected panic for test case: {}",
                    test_data_key
                );
            } else {
                check_outliers(test_data_key, test_data, |_values| {
                    MadOutlierDetector::with_estimator(AnomalyMADEstimator::HarrellDavis)
                });
            }
        }
    }
}
