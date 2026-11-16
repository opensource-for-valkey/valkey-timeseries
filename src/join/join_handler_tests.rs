#[cfg(test)]
mod tests {
    use crate::aggregators::{AggregationType, BucketAlignment, BucketTimestamp};
    use crate::common::Sample;
    use crate::join::join_handler::{JoinInputs, join_samples, process_join};
    use crate::join::{
        AsOfJoinOptions, AsOfJoinStrategy, JoinOptions, JoinReducer, JoinResultType, JoinType,
        JoinValue,
    };
    use crate::series::TimeSeries;
    use crate::series::request_types::AggregationOptions;
    use crate::series::types::ValueFilter;
    use itertools::EitherOrBoth;
    use std::time::Duration;
    use valkey_module::ValkeyResult;

    /// Validates, then joins, as `process_join` does once it has fetched both inputs.
    fn join_internal(
        left: Vec<Sample>,
        right: Vec<Sample>,
        options: &JoinOptions,
    ) -> ValkeyResult<JoinResultType> {
        options.validate()?;
        Ok(join_samples(left, right, options, 0))
    }

    fn create_basic_samples() -> (Vec<Sample>, Vec<Sample>) {
        let left = vec![
            Sample::new(10, 1.0),
            Sample::new(20, 2.0),
            Sample::new(30, 3.0),
        ];
        let right = vec![
            Sample::new(15, 10.0),
            Sample::new(20, 20.0),
            Sample::new(25, 30.0),
        ];
        (left, right)
    }

    fn create_basic_options() -> JoinOptions {
        JoinOptions {
            join_type: JoinType::Inner,
            reducer: None,
            aggregation: None,
            count: None,
            date_range: Default::default(),
            timestamp_filter: None,
            value_filter: None,
        }
    }

    fn get_left_sample(value: &JoinValue) -> Sample {
        match value {
            EitherOrBoth::Left(l) => *l,
            _ => panic!("Expected Left value"),
        }
    }

    fn get_right_sample(value: &JoinValue) -> Sample {
        match value {
            EitherOrBoth::Right(r) => *r,
            _ => panic!("Expected Right value"),
        }
    }

    fn get_left_value(value: &JoinValue) -> f64 {
        get_left_sample(value).value
    }

    fn get_both_samples(value: &JoinValue) -> (Sample, Sample) {
        match value {
            EitherOrBoth::Both(l, r) => (*l, *r),
            _ => panic!("Expected Both value"),
        }
    }

    fn get_timestamp(value: &JoinValue) -> i64 {
        match value {
            EitherOrBoth::Left(l) => l.timestamp,
            EitherOrBoth::Right(r) => r.timestamp,
            EitherOrBoth::Both(l, _) => l.timestamp,
        }
    }

    #[test]
    fn test_join_inner_no_reducer() {
        let (left, right) = create_basic_samples();
        let options = create_basic_options();

        let result = join_internal(left, right, &options).unwrap();

        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 1); // Only timestamp 20 exists in both series
            let (left, right) = get_both_samples(&values[0]);
            assert_eq!(left.timestamp, 20);
            assert_eq!(right.timestamp, 20);
            assert_eq!(left.value, 2.0);
            assert_eq!(right.value, 20.0);
        } else {
            panic!("Expected Values result type");
        }
    }

    #[test]
    fn test_join_left_no_reducer() {
        let (left, right) = create_basic_samples();
        let mut options = create_basic_options();
        options.join_type = JoinType::Left;

        let result = join_internal(left, right, &options).unwrap();

        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 3); // All the left timestamps

            // Check the first value (left-only)
            let left = get_left_sample(&values[0]);
            assert_eq!(left.timestamp, 10);
            assert_eq!(left.value, 1.0);

            // Check the second value (both)
            let (l, r) = get_both_samples(&values[1]);
            assert_eq!(l.timestamp, 20);
            assert_eq!(l.value, 2.0);
            assert_eq!(r.value, 20.0);

            // Check the third value (left-only)
            let left = get_left_sample(&values[2]);
            assert_eq!(left.timestamp, 30);
            assert_eq!(left.value, 3.0);
        } else {
            panic!("Expected Values result type");
        }
    }

    #[test]
    fn test_join_right_no_reducer() {
        let (left, right) = create_basic_samples();
        let mut options = create_basic_options();
        options.join_type = JoinType::Right;

        let result = join_internal(left, right, &options).unwrap();

        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 3); // All right timestamps

            // Check the middle value (has both left and right)
            let (l, r) = get_both_samples(&values[1]);
            assert_eq!(l.timestamp, 20);
            assert_eq!(r.timestamp, 20);
            assert_eq!(l.value, 2.0);
            assert_eq!(r.value, 20.0);

            // Check the other values (right-only)
            let right = get_right_sample(&values[0]);
            assert_eq!(right.timestamp, 15);
            assert_eq!(right.value, 10.0);

            let r = get_right_sample(&values[2]);
            assert_eq!(r.timestamp, 25);
            assert_eq!(r.value, 30.0);
        } else {
            panic!("Expected Values result type");
        }
    }

    #[test]
    fn test_join_full_no_reducer() {
        let (left, right) = create_basic_samples();
        let mut options = create_basic_options();
        options.join_type = JoinType::Full;

        let result = join_internal(left, right, &options).unwrap();

        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 5); // All unique timestamps from both series

            // Verify timestamps are in order

            // Check value types at each timestamp
            match &values[0] {
                EitherOrBoth::Left(l) => {
                    assert_eq!(l.timestamp, 10);
                }
                _ => panic!("Expected Left value for timestamp 10"),
            }

            match &values[1] {
                EitherOrBoth::Right(r) => {
                    assert_eq!(r.timestamp, 15);
                }
                _ => panic!("Expected Right value for timestamp 15"),
            }

            match &values[2] {
                EitherOrBoth::Both(l, r) => {
                    assert_eq!(l.timestamp, 20);
                    assert_eq!(r.timestamp, 20);
                }
                _ => panic!("Expected Both values for timestamp 20"),
            }

            match &values[3] {
                EitherOrBoth::Right(r) => {
                    assert_eq!(r.timestamp, 25);
                }
                _ => panic!("Expected Right value for timestamp 25"),
            }

            match &values[4] {
                EitherOrBoth::Left(l) => {
                    assert_eq!(l.timestamp, 30);
                }
                _ => panic!("Expected Left value for timestamp 30"),
            }
        } else {
            panic!("Expected Values result type");
        }
    }

    #[test]
    fn test_join_with_reducer() {
        let (left, right) = create_basic_samples();
        let mut options = create_basic_options();
        options.reducer = Some(JoinReducer::Sum);

        let result = join_internal(left, right, &options).unwrap();

        if let JoinResultType::Samples(samples) = result {
            assert_eq!(samples.len(), 1); // Only timestamp 20 exists in both series
            assert_eq!(samples[0].timestamp, 20);
            assert_eq!(samples[0].value, 22.0); // 2.0 + 20.0 = 22.0
        } else {
            panic!("Expected Samples result type");
        }
    }

    #[test]
    fn test_join_with_reducer_left_join() {
        let (left, right) = create_basic_samples();
        let mut options = create_basic_options();
        options.join_type = JoinType::Left;
        options.reducer = Some(JoinReducer::Sum);

        let result = join_internal(left, right, &options).unwrap();

        if let JoinResultType::Samples(samples) = result {
            assert_eq!(samples.len(), 3); // All left values

            // Check values - NaN for missing right values
            assert_eq!(samples[0].timestamp, 10);
            assert!(samples[0].value.is_nan()); // 1.0 + NaN = NaN

            assert_eq!(samples[1].timestamp, 20);
            assert_eq!(samples[1].value, 22.0); // 2.0 + 20.0 = 22.0

            assert_eq!(samples[2].timestamp, 30);
            assert!(samples[2].value.is_nan()); // 3.0 + NaN = NaN
        } else {
            panic!("Expected Samples result type");
        }
    }

    #[test]
    fn test_join_with_reducer_and_aggregation() {
        let (left, right) = create_basic_samples();
        let mut options = create_basic_options();
        options.reducer = Some(JoinReducer::Sum);
        options.aggregation = Some(AggregationOptions {
            aggregations: smallvec::smallvec![AggregationType::Sum.into()],
            bucket_duration: 15,
            timestamp_output: BucketTimestamp::Start,
            alignment: BucketAlignment::Start,
            report_empty: false,
        });

        let result = join_internal(left, right, &options).unwrap();

        if let JoinResultType::Samples(samples) = result {
            // With bucket size 15, buckets would be [15-30), [30-45), ...
            assert_eq!(samples.len(), 1);
            assert_eq!(samples[0].timestamp, 15);
            assert_eq!(samples[0].value, 22.0); // The only value is 22.0 from timestamp 20
        } else {
            panic!("Expected Samples result type");
        }
    }

    #[test]
    fn test_join_with_count_limit() {
        let (left, right) = create_basic_samples();
        let mut options = create_basic_options();
        options.join_type = JoinType::Full;
        options.count = Some(3);

        let result = join_internal(left, right, &options).unwrap();

        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 3); // Limited to 3 values
            assert_eq!(get_timestamp(&values[0]), 10);
            assert_eq!(get_timestamp(&values[1]), 15);
            assert_eq!(get_timestamp(&values[2]), 20);
        } else {
            panic!("Expected Values result type");
        }
    }

    #[test]
    fn test_join_with_different_reducer_operations() {
        let (left, right) = create_basic_samples();
        let mut options = create_basic_options();

        // Test Plus reducer
        options.reducer = Some(JoinReducer::Sum);
        let result = join_internal(left.clone(), right.clone(), &options).unwrap();
        if let JoinResultType::Samples(samples) = result {
            assert_eq!(samples[0].value, 22.0); // 2.0 + 20.0 = 22.0
        } else {
            panic!("Expected Samples result type");
        }

        // Test Minus reducer
        options.reducer = Some(JoinReducer::Sub);
        let result = join_internal(left.clone(), right.clone(), &options).unwrap();
        if let JoinResultType::Samples(samples) = result {
            assert_eq!(samples[0].value, -18.0); // 2.0 - 20.0 = -18.0
        } else {
            panic!("Expected Samples result type");
        }

        // Test Mul reducer
        options.reducer = Some(JoinReducer::Mul);
        let result = join_internal(left.clone(), right.clone(), &options).unwrap();
        if let JoinResultType::Samples(samples) = result {
            assert_eq!(samples[0].value, 40.0); // 2.0 * 20.0 = 40.0
        } else {
            panic!("Expected Samples result type");
        }

        // Test Div reducer
        options.reducer = Some(JoinReducer::Div);
        let result = join_internal(left.clone(), right.clone(), &options).unwrap();
        if let JoinResultType::Samples(samples) = result {
            assert_eq!(samples[0].value, 0.1); // 2.0 / 20.0 = 0.1
        } else {
            panic!("Expected Samples result type");
        }
    }

    #[test]
    fn test_join_empty_inputs() {
        let left: Vec<Sample> = vec![];
        let right: Vec<Sample> = vec![];
        let options = create_basic_options();

        let result = join_internal(left, right, &options).unwrap();

        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 0);
        } else {
            panic!("Expected empty Values result type");
        }
    }

    #[test]
    fn test_join_empty_left_input() {
        let left: Vec<Sample> = vec![];
        let right = vec![Sample::new(15, 10.0), Sample::new(20, 20.0)];
        let mut options = create_basic_options();
        // right outer join
        options.join_type = JoinType::Right;

        let result = join_internal(left, right, &options).unwrap();

        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 2);

            // All should be right-only values
            for value in values {
                match value {
                    EitherOrBoth::Right(_) => (),
                    _ => panic!("Expected only Right values"),
                }
            }
        } else {
            panic!("Expected Values result type");
        }
    }

    #[test]
    fn test_join_empty_right_input() {
        let left = vec![Sample::new(10, 1.0), Sample::new(20, 2.0)];
        let right: Vec<Sample> = vec![];
        let mut options = create_basic_options();
        options.join_type = JoinType::Left;

        let result = join_internal(left, right, &options).unwrap();

        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 2);

            // All should be left-only values
            for value in values {
                match value {
                    EitherOrBoth::Left(_) => (),
                    _ => panic!("Expected only Left values"),
                }
            }
        } else {
            panic!("Expected Values result type");
        }
    }

    /// SEMI JOIN
    #[test]
    fn test_join_semi_no_reducer() {
        let (left, right) = create_basic_samples();
        let mut options = create_basic_options();
        options.join_type = JoinType::Semi;

        let result = join_internal(left, right, &options).unwrap();

        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 1); // Only timestamp 20 exists in both series

            // In a semi-join, only left values are returned for matches
            let l = get_left_sample(&values[0]);
            assert_eq!(l.timestamp, 20);
            assert_eq!(l.value, 2.0);
        } else {
            panic!("Expected Values result type");
        }
    }

    #[test]
    fn test_join_semi_with_reducer_raises_error() {
        let (left, right) = create_basic_samples();
        let mut options = create_basic_options();
        options.join_type = JoinType::Semi;
        options.reducer = Some(JoinReducer::Sum);

        assert!(join_internal(left, right, &options).is_err());
    }

    #[test]
    fn test_join_semi_with_aggregation() {
        let (left, right) = create_basic_samples();
        let mut options = create_basic_options();
        options.join_type = JoinType::Semi;
        options.aggregation = Some(AggregationOptions {
            aggregations: smallvec::smallvec![AggregationType::Sum.into()],
            bucket_duration: 30,
            timestamp_output: BucketTimestamp::Start,
            alignment: BucketAlignment::Start,
            report_empty: false,
        });

        let result = join_internal(left, right, &options).unwrap();

        if let JoinResultType::Samples(samples) = result {
            assert_eq!(samples.len(), 1);
            assert_eq!(samples[0].timestamp, 0);
            assert_eq!(samples[0].value, 2.0);
        } else {
            panic!("Expected Samples result type");
        }
    }

    #[test]
    fn test_join_semi_no_matches() {
        // Create datasets with no matching timestamps
        let left = vec![
            Sample::new(10, 1.0),
            Sample::new(30, 3.0),
            Sample::new(50, 5.0),
        ];
        let right = vec![
            Sample::new(20, 2.0),
            Sample::new(40, 4.0),
            Sample::new(60, 6.0),
        ];

        let mut options = create_basic_options();
        options.join_type = JoinType::Semi;

        let result = join_internal(left, right, &options).unwrap();

        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 0); // No matching timestamps, so no results
        } else {
            panic!("Expected Values result type");
        }
    }

    #[test]
    fn test_join_semi_multiple_matches() {
        // Create datasets with multiple matching timestamps
        let left = vec![
            Sample::new(10, 1.0),
            Sample::new(20, 2.0),
            Sample::new(30, 3.0),
            Sample::new(40, 4.0),
        ];
        let right = vec![
            Sample::new(20, 20.0),
            Sample::new(30, 30.0),
            Sample::new(50, 50.0),
        ];

        let mut options = create_basic_options();
        options.join_type = JoinType::Semi;

        let result = join_internal(left, right, &options).unwrap();

        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 2); // Two matching timestamps: 20 and 30

            // Check first match
            let l = get_left_sample(&values[0]);
            assert_eq!(l.value, 2.0);
            assert_eq!(l.timestamp, 20);

            // Check the second match
            let l = get_left_sample(&values[1]);
            assert_eq!(l.value, 3.0);
            assert_eq!(l.timestamp, 30);
        } else {
            panic!("Expected Values result type");
        }
    }

    #[test]
    fn test_join_semi_empty_inputs() {
        // Test with empty left input
        let left: Vec<Sample> = vec![];
        let right = vec![Sample::new(15, 10.0), Sample::new(20, 20.0)];

        let mut options = create_basic_options();
        options.join_type = JoinType::Semi;

        let result = join_internal(left, right, &options).unwrap();
        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 0); // No left samples, so no results
        } else {
            panic!("Expected Values result type");
        }

        // Test with empty right input
        let left = vec![Sample::new(10, 1.0), Sample::new(20, 2.0)];
        let right: Vec<Sample> = vec![];

        let mut options = create_basic_options();
        options.join_type = JoinType::Semi;

        let result = join_internal(left, right, &options).unwrap();
        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 0); // No right samples to match with, so no results
        } else {
            panic!("Expected Values result type");
        }
    }

    #[test]
    fn test_join_semi_with_count_limit() {
        // Create datasets with multiple matching timestamps
        let left = vec![
            Sample::new(10, 1.0),
            Sample::new(20, 2.0),
            Sample::new(30, 3.0),
            Sample::new(40, 4.0),
        ];
        let right = vec![
            Sample::new(20, 20.0),
            Sample::new(30, 30.0),
            Sample::new(40, 40.0),
        ];

        let mut options = create_basic_options();
        options.join_type = JoinType::Semi;
        options.count = Some(2); // Limit to first 2 results

        let result = join_internal(left, right, &options).unwrap();

        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 2); // Limited to 2 results
            assert_eq!(get_timestamp(&values[0]), 20);
            assert_eq!(get_timestamp(&values[1]), 30);
            // 40 is also a match but should be excluded due to the count limit
        } else {
            panic!("Expected Values result type");
        }
    }

    #[test]
    fn test_join_semi_vs_inner_join() {
        // Check that semi-join returns only left values for matching timestamps,
        // unlike inner join which returns both left and right values
        let left = vec![Sample::new(20, 2.0)];
        let right = vec![Sample::new(20, 20.0)];

        // Test semi-join
        let mut options = create_basic_options();
        options.join_type = JoinType::Semi;

        let result = join_internal(left.clone(), right.clone(), &options).unwrap();
        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 1);
            let value = get_left_value(&values[0]);
            assert_eq!(value, 2.0);
        } else {
            panic!("Expected Values result type");
        }

        // Test inner join for comparison
        let mut options = create_basic_options();
        options.join_type = JoinType::Inner;

        let result = join_internal(left, right, &options).unwrap();
        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 1);
            let (l, r) = get_both_samples(&values[0]);
            assert_eq!(l.value, 2.0);
            assert_eq!(r.value, 20.0);
        } else {
            panic!("Expected Values result type");
        }
    }

    #[test]
    fn test_join_semi_vs_anti_join() {
        // Create datasets to compare semi and anti joins
        let left = vec![
            Sample::new(10, 1.0),
            Sample::new(20, 2.0),
            Sample::new(30, 3.0),
        ];
        let right = vec![Sample::new(20, 20.0), Sample::new(40, 40.0)];

        // Test semi-join (should return rows where timestamps match)
        let mut options = create_basic_options();
        options.join_type = JoinType::Semi;

        let result = join_internal(left.clone(), right.clone(), &options).unwrap();
        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 1);
            assert_eq!(get_timestamp(&values[0]), 20); // Only timestamp 20 matches
        } else {
            panic!("Expected Values result type");
        }

        // Test anti-join (should return rows where timestamps don't match)
        let mut options = create_basic_options();
        options.join_type = JoinType::Anti;

        let result = join_internal(left, right, &options).unwrap();
        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 2);
            assert_eq!(get_timestamp(&values[0]), 10); // Timestamps 10 and 30 don't match
            assert_eq!(get_timestamp(&values[1]), 30);
        } else {
            panic!("Expected Values result type");
        }
    }

    // ANTI JOIN
    #[test]
    fn test_join_anti_no_reducer() {
        let (left, right) = create_basic_samples();
        let mut options = create_basic_options();
        options.join_type = JoinType::Anti;

        let result = join_internal(left, right, &options).unwrap();

        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 2); // Timestamps 10 and 30 from the left don't exist in right

            // Check the first value
            let l = get_left_sample(&values[0]);
            assert_eq!(l.timestamp, 10);
            assert_eq!(l.value, 1.0);

            // Check the second value
            let l = get_left_sample(&values[1]);
            assert_eq!(l.timestamp, 30);
            assert_eq!(l.value, 3.0);

            // Timestamp 20 should not be present since it exists in both left and right
        } else {
            panic!("Expected Values result type");
        }
    }

    #[test]
    fn test_join_anti_with_reducer_returns_err() {
        let (left, right) = create_basic_samples();
        let mut options = create_basic_options();
        options.join_type = JoinType::Anti;
        options.reducer = Some(JoinReducer::Sum);

        assert!(join_internal(left, right, &options).is_err());
    }

    #[test]
    fn test_join_anti_with_aggregation() {
        let (left, right) = create_basic_samples();
        let mut options = create_basic_options();
        options.join_type = JoinType::Anti;
        options.aggregation = Some(AggregationOptions {
            aggregations: smallvec::smallvec![AggregationType::Sum.into()],
            bucket_duration: 25, // Bucket size to group timestamps 10 and 30
            timestamp_output: BucketTimestamp::Start,
            alignment: BucketAlignment::Start,
            report_empty: false,
        });

        let result = join_internal(left, right, &options).unwrap();

        if let JoinResultType::Samples(samples) = result {
            // With bucket size 25, we should get two buckets: [0-25) and [25-50)
            assert_eq!(samples.len(), 2);

            // First bucket contains timestamp 10
            assert_eq!(samples[0].timestamp, 0);
            assert_eq!(samples[0].value, 1.0);

            // The second bucket contains timestamp 30
            assert_eq!(samples[1].timestamp, 25);
            assert_eq!(samples[1].value, 3.0);
        } else {
            panic!("Expected Samples result type");
        }
    }

    #[test]
    fn test_join_anti_no_matches() {
        // Test when all left timestamps match right timestamps
        // In this case, anti-join should return no rows
        let left = vec![
            Sample::new(10, 1.0),
            Sample::new(20, 2.0),
            Sample::new(30, 3.0),
        ];
        let right = vec![
            Sample::new(10, 10.0),
            Sample::new(20, 20.0),
            Sample::new(30, 30.0),
        ];

        let mut options = create_basic_options();
        options.join_type = JoinType::Anti;

        let result = join_internal(left, right, &options).unwrap();

        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 0); // No rows should be returned
        } else {
            panic!("Expected Values result type");
        }
    }

    #[test]
    fn test_join_anti_all_matches() {
        // Test when no left timestamps match right timestamps
        // In this case, anti-join should return all left rows
        let left = vec![
            Sample::new(10, 1.0),
            Sample::new(20, 2.0),
            Sample::new(30, 3.0),
        ];
        let right = vec![
            Sample::new(15, 15.0),
            Sample::new(25, 25.0),
            Sample::new(35, 35.0),
        ];

        let mut options = create_basic_options();
        options.join_type = JoinType::Anti;

        let result = join_internal(left, right, &options).unwrap();

        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 3); // All left rows should be returned

            // Check all timestamps and values
            assert_eq!(get_timestamp(&values[0]), 10);
            assert_eq!(get_timestamp(&values[1]), 20);
            assert_eq!(get_timestamp(&values[2]), 30);

            // Verify all values are from the left side
            for value in values {
                match value {
                    EitherOrBoth::Left(_) => (),
                    _ => panic!("Expected Left value"),
                }
            }
        } else {
            panic!("Expected Values result type");
        }
    }

    #[test]
    fn test_join_anti_empty_inputs() {
        // Test with empty left input
        let left: Vec<Sample> = vec![];
        let right = vec![Sample::new(15, 10.0), Sample::new(20, 20.0)];

        let mut options = create_basic_options();
        options.join_type = JoinType::Anti;

        let result = join_internal(left, right, &options).unwrap();
        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 0); // No left samples, so no results
        } else {
            panic!("Expected Values result type");
        }

        // Test with empty right input
        // Anti join should return all left rows when right is empty
        let left = vec![Sample::new(10, 1.0), Sample::new(20, 2.0)];
        let right: Vec<Sample> = vec![];

        let mut options = create_basic_options();
        options.join_type = JoinType::Anti;

        let result = join_internal(left, right, &options).unwrap();
        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 2); // All left samples should be returned
        } else {
            panic!("Expected Values result type");
        }
    }

    #[test]
    fn test_join_anti_with_count_limit() {
        let left = vec![
            Sample::new(10, 1.0),
            Sample::new(20, 2.0),
            Sample::new(30, 3.0),
            Sample::new(40, 4.0),
        ];
        let right = vec![Sample::new(20, 20.0), Sample::new(40, 40.0)];

        let mut options = create_basic_options();
        options.join_type = JoinType::Anti;
        options.count = Some(1); // Limit to the first result only

        let result = join_internal(left, right, &options).unwrap();

        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 1); // Limited to 1 result
            assert_eq!(get_timestamp(&values[0]), 10); // Only the first non-matching timestamp
        } else {
            panic!("Expected Values result type");
        }
    }

    #[test]
    fn test_join_anti_vs_semi_join() {
        // Create datasets to compare anti and semi joins
        let left = vec![
            Sample::new(10, 1.0),
            Sample::new(20, 2.0),
            Sample::new(30, 3.0),
        ];
        let right = vec![Sample::new(20, 20.0), Sample::new(40, 40.0)];

        // Test anti-join (should return rows where timestamps don't match)
        let mut options = create_basic_options();
        options.join_type = JoinType::Anti;

        let result = join_internal(left.clone(), right.clone(), &options).unwrap();
        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 2);
            assert_eq!(get_timestamp(&values[0]), 10); // Timestamps 10 and 30 don't match
            assert_eq!(get_timestamp(&values[1]), 30);
        } else {
            panic!("Expected Values result type");
        }

        // Test semi-join (should return rows where timestamps match)
        let mut options = create_basic_options();
        options.join_type = JoinType::Semi;

        let result = join_internal(left, right, &options).unwrap();
        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 1);
            assert_eq!(get_timestamp(&values[0]), 20); // Only timestamp 20 matches
        } else {
            panic!("Expected Values result type");
        }
    }

    #[test]
    fn test_join_anti_large_datasets() {
        // Test with larger datasets to verify performance and correctness
        let mut left = Vec::with_capacity(100);
        let mut right = Vec::with_capacity(50);

        // Create left samples with timestamps 0, 2, 4, ..., 198
        for i in 0..100 {
            left.push(Sample::new(i * 2, i as f64));
        }

        // Create right samples with timestamps 0, 4, 8, ..., 196
        for i in 0..50 {
            right.push(Sample::new(i * 4, (i * 10) as f64));
        }

        let mut options = create_basic_options();
        options.join_type = JoinType::Anti;

        let result = join_internal(left, right, &options);

        if let Ok(JoinResultType::Values(values)) = result {
            // Anti-join should return left rows with timestamps that are not multiples of 4
            // So we expect timestamps 2, 6, 10, ..., 198 (50 values)
            assert_eq!(values.len(), 50);

            // Verify that all returned timestamps are not multiples of 4
            for value in values {
                let EitherOrBoth::Left(left) = value else {
                    panic!("Expected Left value")
                };
                assert_eq!(
                    left.timestamp % 4,
                    2,
                    "Timestamp {} should not be a multiple of 4",
                    left.timestamp
                );
            }
        } else {
            panic!("Expected Values result type");
        }
    }

    fn samples(timestamps: &[i64]) -> Vec<Sample> {
        timestamps
            .iter()
            .map(|&ts| Sample::new(ts, ts as f64))
            .collect()
    }

    fn asof_options(
        strategy: AsOfJoinStrategy,
        tolerance: Option<Duration>,
        allow_exact_match: bool,
    ) -> JoinOptions {
        let mut options = create_basic_options();
        options.join_type = JoinType::AsOf(AsOfJoinOptions {
            strategy,
            tolerance,
            allow_exact_match,
        });
        options
    }

    /// The (left, right) timestamp pairs an ASOF join matched.
    fn asof_pairs(left: &[i64], right: &[i64], options: &JoinOptions) -> Vec<(i64, i64)> {
        match join_internal(samples(left), samples(right), options).unwrap() {
            JoinResultType::Values(values) => values
                .iter()
                .map(|v| {
                    let (l, r) = get_both_samples(v);
                    (l.timestamp, r.timestamp)
                })
                .collect(),
            JoinResultType::Samples(_) => panic!("Expected Values result type"),
        }
    }

    #[test]
    fn test_join_count_limits_output_not_inputs() {
        // COUNT used to truncate each input to COUNT samples before joining, so matches past
        // the first COUNT samples of either side were never considered.
        let mut left = TimeSeries::new();
        let mut right = TimeSeries::new();
        for ts in [10, 20, 30, 40] {
            assert!(left.add(ts, ts as f64, None).is_ok());
        }
        for ts in [30, 40] {
            assert!(right.add(ts, ts as f64, None).is_ok());
        }
        let mut options = create_basic_options();
        options.count = Some(2);

        let JoinResultType::Values(values) = process_join(&left, &right, &options).unwrap() else {
            panic!("Expected Values result type");
        };
        let timestamps: Vec<i64> = values
            .iter()
            .map(|v| get_both_samples(v).0.timestamp)
            .collect();
        assert_eq!(timestamps, vec![30, 40]);

        options.count = Some(1);
        let JoinResultType::Values(values) = process_join(&left, &right, &options).unwrap() else {
            panic!("Expected Values result type");
        };
        assert_eq!(values.len(), 1);
    }

    #[test]
    fn test_asof_without_tolerance_is_unlimited() {
        // An omitted TOLERANCE used to mean 0 — exact matches only.
        let options = asof_options(AsOfJoinStrategy::Backward, None, true);
        assert_eq!(
            asof_pairs(&[10, 20], &[5, 15], &options),
            vec![(10, 5), (20, 15)]
        );

        let options = asof_options(AsOfJoinStrategy::Backward, Some(Duration::ZERO), true);
        assert_eq!(asof_pairs(&[10, 20], &[5, 15], &options), vec![]);

        let options = asof_options(
            AsOfJoinStrategy::Backward,
            Some(Duration::from_millis(5)),
            true,
        );
        assert_eq!(
            asof_pairs(&[10, 20], &[5, 16], &options),
            vec![(10, 5), (20, 16)]
        );
        assert_eq!(asof_pairs(&[10, 20], &[4, 16], &options), vec![(20, 16)]);
    }

    #[test]
    fn test_asof_nearest_honours_allow_exact_match_false() {
        let options = asof_options(AsOfJoinStrategy::Nearest, None, false);
        // The exact match at 10 is excluded; 12 is the nearest remaining.
        assert_eq!(asof_pairs(&[10], &[10, 12], &options), vec![(10, 12)]);
        // Excluded for the left sample at 10, but still the nearest for the one at 12.
        assert_eq!(
            asof_pairs(&[10, 12], &[10, 15], &options),
            vec![(10, 15), (12, 10)]
        );
        // Nothing but the exact match: no row.
        assert_eq!(asof_pairs(&[10], &[10], &options), vec![]);

        let options = asof_options(AsOfJoinStrategy::Nearest, None, true);
        assert_eq!(asof_pairs(&[10], &[10, 12], &options), vec![(10, 10)]);
    }

    #[test]
    fn test_asof_nearest_breaks_ties_towards_the_later_sample() {
        for allow_exact_match in [true, false] {
            let options = asof_options(AsOfJoinStrategy::Nearest, None, allow_exact_match);
            assert_eq!(
                asof_pairs(&[10], &[8, 12], &options),
                vec![(10, 12)],
                "allow_exact_match={allow_exact_match}"
            );
        }
    }

    fn reduced(
        left: &[(i64, f64)],
        right: &[(i64, f64)],
        options: &JoinOptions,
    ) -> Vec<(i64, f64)> {
        let to_samples =
            |pairs: &[(i64, f64)]| pairs.iter().map(|&(t, v)| Sample::new(t, v)).collect();
        let JoinResultType::Samples(samples) =
            join_internal(to_samples(left), to_samples(right), options).unwrap()
        else {
            panic!("Expected Samples result type");
        };
        samples.iter().map(|s| (s.timestamp, s.value)).collect()
    }

    #[test]
    fn test_comparison_reducers_return_booleans_and_nan_for_missing() {
        // FULL join: 10 left-only, 20 both (7 vs 2), 30 both (7 vs 9), 40 right-only.
        let left = [(10, 7.0), (20, 7.0), (30, 7.0)];
        let right = [(20, 2.0), (30, 9.0), (40, 1.0)];
        let mut options = create_basic_options();
        options.join_type = JoinType::Full;
        for (reducer, at_20, at_30) in [
            (JoinReducer::Gt, 1.0, 0.0),
            (JoinReducer::Lt, 0.0, 1.0),
            (JoinReducer::Eq, 0.0, 0.0),
            (JoinReducer::Ne, 1.0, 1.0),
            (JoinReducer::Cmp, 1.0, -1.0),
        ] {
            options.reducer = Some(reducer);
            let rows = reduced(&left, &right, &options);
            let timestamps: Vec<i64> = rows.iter().map(|r| r.0).collect();
            assert_eq!(timestamps, vec![10, 20, 30, 40], "{}", reducer.name());
            assert!(rows[0].1.is_nan(), "{}: left-only row", reducer.name());
            assert_eq!(rows[1].1, at_20, "{}", reducer.name());
            assert_eq!(rows[2].1, at_30, "{}", reducer.name());
            assert!(rows[3].1.is_nan(), "{}: right-only row", reducer.name());
        }

        // max/min/coalesce use the side that is present.
        for reducer in [JoinReducer::Max, JoinReducer::Min, JoinReducer::Coalesce] {
            options.reducer = Some(reducer);
            let rows = reduced(&left, &right, &options);
            assert_eq!(rows[0], (10, 7.0), "{}", reducer.name());
            assert_eq!(rows[3], (40, 1.0), "{}", reducer.name());
        }
    }

    #[test]
    fn test_validate_rejects_aggregation_without_reducer() {
        let (left, right) = create_basic_samples();
        let mut options = create_basic_options();
        options.aggregation = Some(AggregationOptions {
            aggregations: smallvec::smallvec![AggregationType::Sum.into()],
            bucket_duration: 10,
            timestamp_output: BucketTimestamp::Start,
            alignment: BucketAlignment::Default,
            report_empty: false,
        });
        // An INNER join's rows are pairs: without a reducer there is nothing to aggregate.
        assert!(join_internal(left.clone(), right.clone(), &options).is_err());

        // SEMI/ANTI aggregate their left samples.
        for join_type in [JoinType::Semi, JoinType::Anti] {
            options.join_type = join_type;
            assert!(join_internal(left.clone(), right.clone(), &options).is_ok());
        }
    }

    #[test]
    fn test_aggregation_count_stops_after_the_buckets_it_returns() {
        // Two samples 40 seconds apart with 1 ms EMPTY buckets: 40 million buckets in all.
        // COUNT 2 must stop after two of them instead of building every bucket first.
        let left = vec![Sample::new(0, 1.0), Sample::new(40_000_000, 2.0)];
        let right = left.clone();
        let mut options = create_basic_options();
        options.reducer = Some(JoinReducer::Sum);
        options.count = Some(2);
        options.aggregation = Some(AggregationOptions {
            aggregations: smallvec::smallvec![AggregationType::Sum.into()],
            bucket_duration: 1,
            timestamp_output: BucketTimestamp::Start,
            alignment: BucketAlignment::Default,
            report_empty: true,
        });

        let started = std::time::Instant::now();
        let JoinResultType::Samples(samples) = join_internal(left, right, &options).unwrap() else {
            panic!("Expected Samples result type");
        };
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0], Sample::new(0, 2.0));
        assert_eq!(samples[1].timestamp, 1);
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "aggregation built buckets past COUNT"
        );
    }

    #[test]
    fn test_semi_join_keeps_left_samples_of_inner_matches() {
        let left = samples(&[10, 20, 30, 40, 50]);
        let right = samples(&[5, 20, 40, 60]);
        let mut options = create_basic_options();
        options.join_type = JoinType::Semi;
        let JoinResultType::Values(values) = join_internal(left, right, &options).unwrap() else {
            panic!("Expected Values result type");
        };
        let timestamps: Vec<i64> = values
            .iter()
            .map(|v| get_left_sample(v).timestamp)
            .collect();
        assert_eq!(timestamps, vec![20, 40]);
    }

    fn series(points: impl IntoIterator<Item = (i64, f64)>) -> TimeSeries {
        let mut series = TimeSeries::new();
        for (ts, value) in points {
            assert!(series.add(ts, value, None).is_ok());
        }
        series
    }

    fn left_timestamps(result: JoinResultType) -> Vec<i64> {
        let JoinResultType::Values(values) = result else {
            panic!("Expected Values result type");
        };
        values
            .iter()
            .map(|v| v.as_ref().left().expect("left side").timestamp)
            .collect()
    }

    #[test]
    fn test_filters_apply_to_both_inputs() {
        let left = series((0..10).map(|i| (i * 10, i as f64)));
        let right = series((0..10).map(|i| (i * 10, (i * 10) as f64)));
        let mut options = create_basic_options();
        options.join_type = JoinType::Left;

        // FILTER_BY_TS: only the listed timestamps, on both sides; unlisted and out-of-range
        // timestamps are ignored.
        options.timestamp_filter = Some(vec![30, 10, 55, 70, 10_000]);
        assert_eq!(
            left_timestamps(process_join(&left, &right, &options).unwrap()),
            vec![10, 30, 70]
        );

        // FILTER_BY_VALUE [2, 50] on both sides: the left keeps timestamps 20..=90 (values
        // 2..=9), the right 10..=50 (values 10..=50), so LEFT rows 60..=90 have no right sample.
        options.timestamp_filter = None;
        options.value_filter = Some(ValueFilter::new(2.0, 50.0).unwrap());
        let JoinResultType::Values(values) = process_join(&left, &right, &options).unwrap() else {
            panic!("Expected Values result type");
        };
        let rows: Vec<(i64, bool)> = values
            .iter()
            .map(|v| (v.as_ref().left().unwrap().timestamp, v.has_right()))
            .collect();
        assert_eq!(
            rows,
            vec![
                (20, true),
                (30, true),
                (40, true),
                (50, true),
                (60, false),
                (70, false),
                (80, false),
                (90, false)
            ]
        );

        // Both filters together.
        options.timestamp_filter = Some(vec![10, 20, 90]);
        assert_eq!(
            left_timestamps(process_join(&left, &right, &options).unwrap()),
            vec![20, 90]
        );
    }

    #[test]
    fn test_parallel_and_lazy_decoding_agree() {
        // Enough samples that a join without COUNT decodes both inputs in parallel; with COUNT
        // it decodes lazily. Both must give the same rows.
        let n = 20_000;
        let left = series((0..n).map(|i| (i * 2, i as f64)));
        let right = series((0..n).map(|i| (i * 3, -(i as f64))));
        for join_type in [
            JoinType::Inner,
            JoinType::Full,
            JoinType::Semi,
            JoinType::Anti,
            JoinType::AsOf(AsOfJoinOptions {
                strategy: AsOfJoinStrategy::Nearest,
                tolerance: None,
                allow_exact_match: false,
            }),
        ] {
            let mut options = create_basic_options();
            options.join_type = join_type;
            let JoinResultType::Values(parallel) = process_join(&left, &right, &options).unwrap()
            else {
                panic!("Expected Values result type");
            };
            options.count = Some(usize::MAX);
            let JoinResultType::Values(lazy) = process_join(&left, &right, &options).unwrap()
            else {
                panic!("Expected Values result type");
            };
            assert!(!parallel.is_empty(), "{join_type:?}");
            assert_eq!(parallel, lazy, "{join_type:?}");
        }
    }
    fn values(result: Option<JoinResultType>) -> Option<Vec<JoinValue>> {
        result.map(|result| match result {
            JoinResultType::Values(values) => values,
            JoinResultType::Samples(_) => panic!("Expected Values result type"),
        })
    }

    #[test]
    fn join_within_answers_a_count_join_that_fits_its_budget() {
        // INNER over two windows that share every timestamp: COUNT 10 reads ten samples a side.
        let left = series((0..100_000).map(|ts| (ts, 1.0)));
        let right = series((0..100_000).map(|ts| (ts, 2.0)));
        let mut options = create_basic_options();
        options.count = Some(10);
        let inputs = JoinInputs::read(&left, &right, &options).unwrap();

        let full = values(Some(inputs.join(&options))).unwrap();
        assert_eq!(full.len(), 10);
        assert_eq!(values(inputs.join_within(&options, 1_000)), Some(full));
    }

    #[test]
    fn join_within_gives_up_when_the_join_reads_past_its_budget() {
        // The only common timestamp lies past the budget: an input cut there would answer
        // "no rows", which is wrong, so the join must give up instead.
        let left = series((0..10_000).map(|i| (i * 2, 1.0)).chain([(1_000_001, 1.0)]));
        let right = series(
            (0..10_000)
                .map(|i| (i * 2 + 1, 2.0))
                .chain([(1_000_001, 2.0)]),
        );
        let mut options = create_basic_options();
        options.count = Some(1);
        let inputs = JoinInputs::read(&left, &right, &options).unwrap();

        assert_eq!(values(Some(inputs.join(&options))).unwrap().len(), 1);
        assert_eq!(values(inputs.join_within(&options, 1_000)), None);
        // Filtered-out samples count too: they were read.
        options.value_filter = Some(ValueFilter::new(5.0, 6.0).unwrap());
        assert_eq!(values(inputs.join_within(&options, 1_000)), None);
    }

    #[test]
    fn join_within_accepts_a_budget_spent_exactly() {
        // Both inputs end with the budget: every sample was read, none was cut, so the answer
        // stands.
        let left = series((0..50).map(|ts| (ts, 1.0)));
        let right = series((0..50).map(|ts| (ts, 2.0)));
        let mut options = create_basic_options();
        options.count = Some(usize::MAX);
        let inputs = JoinInputs::read(&left, &right, &options).unwrap();

        let full = values(Some(inputs.join(&options))).unwrap();
        assert_eq!(values(inputs.join_within(&options, 100)), Some(full));
        assert_eq!(values(inputs.join_within(&options, 99)), None);
    }
}
