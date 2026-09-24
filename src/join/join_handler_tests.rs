#[cfg(test)]
mod tests {
    use crate::aggregators::{AggregationType, BucketAlignment, BucketTimestamp};
    use crate::common::Sample;
    use crate::join::join_handler::{join_internal, process_join};
    use crate::join::{
        AsOfJoinOptions, AsOfJoinStrategy, JoinOptions, JoinReducer, JoinResultType, JoinType,
        JoinValue,
    };
    use crate::series::TimeSeries;
    use crate::series::request_types::AggregationOptions;
    use itertools::EitherOrBoth;
    use std::ops::Deref;
    use std::time::Duration;

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
        match value.deref() {
            EitherOrBoth::Left(l) => *l,
            _ => panic!("Expected Left value"),
        }
    }

    fn get_right_sample(value: &JoinValue) -> Sample {
        match value.deref() {
            EitherOrBoth::Right(r) => *r,
            _ => panic!("Expected Right value"),
        }
    }

    fn get_left_value(value: &JoinValue) -> f64 {
        get_left_sample(value).value
    }

    fn get_both_samples(value: &JoinValue) -> (Sample, Sample) {
        match value.deref() {
            EitherOrBoth::Both(l, r) => (*l, *r),
            _ => panic!("Expected Both value"),
        }
    }

    fn get_timestamp(value: &JoinValue) -> i64 {
        match value.deref() {
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
            match values[0].deref() {
                EitherOrBoth::Left(l) => {
                    assert_eq!(l.timestamp, 10);
                }
                _ => panic!("Expected Left value for timestamp 10"),
            }

            match values[1].deref() {
                EitherOrBoth::Right(r) => {
                    assert_eq!(r.timestamp, 15);
                }
                _ => panic!("Expected Right value for timestamp 15"),
            }

            match values[2].deref() {
                EitherOrBoth::Both(l, r) => {
                    assert_eq!(l.timestamp, 20);
                    assert_eq!(r.timestamp, 20);
                }
                _ => panic!("Expected Both values for timestamp 20"),
            }

            match values[3].deref() {
                EitherOrBoth::Right(r) => {
                    assert_eq!(r.timestamp, 25);
                }
                _ => panic!("Expected Right value for timestamp 25"),
            }

            match values[4].deref() {
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
        }

        // Test Minus reducer
        options.reducer = Some(JoinReducer::Sub);
        let result = join_internal(left.clone(), right.clone(), &options).unwrap();
        if let JoinResultType::Samples(samples) = result {
            assert_eq!(samples[0].value, -18.0); // 2.0 - 20.0 = -18.0
        }

        // Test Mul reducer
        options.reducer = Some(JoinReducer::Mul);
        let result = join_internal(left.clone(), right.clone(), &options).unwrap();
        if let JoinResultType::Samples(samples) = result {
            assert_eq!(samples[0].value, 40.0); // 2.0 * 20.0 = 40.0
        }

        // Test Div reducer
        options.reducer = Some(JoinReducer::Div);
        let result = join_internal(left.clone(), right.clone(), &options).unwrap();
        if let JoinResultType::Samples(samples) = result {
            assert_eq!(samples[0].value, 0.1); // 2.0 / 20.0 = 0.1
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
                match value.deref() {
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
                match value.deref() {
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
        }

        // Test anti-join (should return rows where timestamps don't match)
        let mut options = create_basic_options();
        options.join_type = JoinType::Anti;

        let result = join_internal(left, right, &options).unwrap();
        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 2);
            assert_eq!(get_timestamp(&values[0]), 10); // Timestamps 10 and 30 don't match
            assert_eq!(get_timestamp(&values[1]), 30);
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
                match value.deref() {
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
        }

        // Test semi-join (should return rows where timestamps match)
        let mut options = create_basic_options();
        options.join_type = JoinType::Semi;

        let result = join_internal(left, right, &options).unwrap();
        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 1);
            assert_eq!(get_timestamp(&values[0]), 20); // Only timestamp 20 matches
        }
    }

    // #[test]
    // fn test_join_anti_different_reducer_operations() {
    //     let left = vec![Sample::new(10, 5.0)];
    //     let right: Vec<Sample> = vec![]; // Empty right to ensure all left rows are included
    //
    //     // Test with different reducers to ensure they handle NaN correctly
    //     let reducers = [
    //         JoinReducer::Sum, JoinReducer::Sub, JoinReducer::Mul, JoinReducer::Div,
    //         JoinReducer::Max, JoinReducer::Min, JoinReducer::Avg
    //     ];
    //
    //     for reducer in reducers {
    //         let mut options = create_basic_options();
    //         options.join_type = JoinType::Anti;
    //         options.reducer = Some(reducer);
    //
    //         let result = join_internal(left.clone(), right.clone(), &options);
    //
    //         if let JoinResultType::Samples(samples) = result {
    //             assert_eq!(samples.len(), 1);
    //             assert_eq!(samples[0].timestamp, 10);
    //
    //             // For any reducer operation with NaN as one operand, the result should be NaN
    //             assert!(samples[0].value.is_nan(),
    //                     "Expected NaN result for reducer {:?}, got {}", reducer, samples[0].value);
    //         } else {
    //             panic!("Expected Samples result type");
    //         }
    //     }
    // }

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
                let EitherOrBoth::Left(left) = &value.0 else {
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
}
