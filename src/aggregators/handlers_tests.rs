#[cfg(test)]
mod tests {
    use crate::aggregators::{
        AggregationHandler, AvgAggregator, CountAggregator, CountAllAggregator, CountIfAggregator,
        CountNanAggregator, FirstAggregator, IRateAggregator, IncreaseAggregator, LastAggregator,
        MaxAggregator, MinAggregator, RangeAggregator, RateAggregator, ShareAggregator,
        StdPAggregator, StdSAggregator, SumAggregator, SumIfAggregator, VarPAggregator,
        VarSAggregator,
    };
    use crate::common::binop::ComparisonOperator;
    use std::time::Duration;

    #[test]
    fn test_first_aggregator() {
        let mut agg = FirstAggregator::default();
        assert_eq!(agg.current(), None);

        agg.update(1000, 10.0);
        assert_eq!(agg.current(), Some(10.0));

        agg.update(2000, 20.0);
        assert_eq!(agg.current(), Some(10.0)); // Should keep first value

        let result = agg.finalize();
        assert_eq!(result, 10.0);
        assert_eq!(agg.current(), None); // Should reset after finalize
    }

    #[test]
    fn test_last_aggregator() {
        let mut agg = LastAggregator::default();
        assert_eq!(agg.current(), None);

        agg.update(1000, 10.0);
        assert_eq!(agg.current(), Some(10.0));

        agg.update(2000, 20.0);
        assert_eq!(agg.current(), Some(20.0)); // Should keep last value

        agg.update(3000, 15.0);
        assert_eq!(agg.current(), Some(15.0));

        let result = agg.finalize();
        assert_eq!(result, 15.0);
    }

    #[test]
    fn test_min_aggregator() {
        let mut agg = MinAggregator::default();
        assert_eq!(agg.current(), None);

        agg.update(1000, 10.0);
        agg.update(2000, 5.0);
        agg.update(3000, 15.0);
        agg.update(4000, 3.0);

        assert_eq!(agg.current(), Some(3.0));

        agg.reset();
        assert_eq!(agg.current(), None);
    }

    #[test]
    fn test_max_aggregator() {
        let mut agg = MaxAggregator::default();
        assert_eq!(agg.current(), None);

        agg.update(1000, 10.0);
        agg.update(2000, 25.0);
        agg.update(3000, 15.0);
        agg.update(4000, 20.0);

        assert_eq!(agg.current(), Some(25.0));

        agg.reset();
        assert_eq!(agg.current(), None);
    }

    #[test]
    fn test_range_aggregator() {
        let mut agg = RangeAggregator::default();
        assert_eq!(agg.current(), None);

        agg.update(1000, 10.0);
        assert_eq!(agg.current(), Some(0.0)); // Single value: range is 0

        agg.update(2000, 25.0);
        assert_eq!(agg.current(), Some(15.0)); // 25 - 10 = 15

        agg.update(3000, 5.0);
        assert_eq!(agg.current(), Some(20.0)); // 25 - 5 = 20

        agg.update(4000, 30.0);
        assert_eq!(agg.current(), Some(25.0)); // 30 - 5 = 25
    }

    #[test]
    fn test_avg_aggregator() {
        let mut agg = AvgAggregator::default();
        assert_eq!(agg.current(), None);

        agg.update(1000, 10.0);
        assert_eq!(agg.current(), Some(10.0));

        agg.update(2000, 20.0);
        assert_eq!(agg.current(), Some(15.0)); // (10 + 20) / 2

        agg.update(3000, 30.0);
        assert_eq!(agg.current(), Some(20.0)); // (10 + 20 + 30) / 3

        agg.update(4000, 40.0);
        assert_eq!(agg.current(), Some(25.0)); // (10 + 20 + 30 + 40) / 4
    }

    #[test]
    fn test_sum_aggregator() {
        let mut agg = SumAggregator::default();
        assert_eq!(agg.current(), None);
        assert_eq!(agg.empty_value(), 0.0);

        agg.update(1000, 10.0);
        assert_eq!(agg.current(), Some(10.0));

        agg.update(2000, 20.0);
        assert_eq!(agg.current(), Some(30.0));

        agg.update(3000, 15.0);
        assert_eq!(agg.current(), Some(45.0));

        let result = agg.finalize();
        assert_eq!(result, 45.0);
    }

    /// A sum of zero is still a sum: one accepted zero, or values that cancel exactly, have a
    /// current value, which is what a compaction rule's open bucket reports.
    #[test]
    fn test_sum_of_zero_has_a_value() {
        for values in [&[0.0][..], &[1.0, -1.0][..], &[-0.0][..]] {
            let mut agg = SumAggregator::default();
            for (ts, &value) in values.iter().enumerate() {
                agg.update(ts as i64, value);
            }
            assert_eq!(agg.current(), Some(0.0), "{values:?}");
            agg.reset();
            assert_eq!(agg.current(), None, "reset leaves nothing");
        }
        let mut nans = SumAggregator::default();
        nans.update(0, f64::NAN);
        assert_eq!(nans.current(), None);
    }

    #[test]
    fn test_sum_aggregator_empty_value() {
        let mut agg = SumAggregator::default();
        let result = agg.finalize();
        assert_eq!(result, 0.0); // Should return 0.0 instead of NaN
    }

    #[test]
    fn test_count_aggregator() {
        let mut agg = CountAggregator::default();
        assert_eq!(agg.current(), None);
        assert_eq!(agg.empty_value(), 0.0);

        agg.update(1000, 10.0);
        assert_eq!(agg.current(), Some(1.0));

        agg.update(2000, 20.0);
        assert_eq!(agg.current(), Some(2.0));

        agg.update(3000, 30.0);
        assert_eq!(agg.current(), Some(3.0));

        agg.reset();
        assert_eq!(agg.current(), None);

        let result = agg.finalize();
        assert_eq!(result, 0.0);
    }

    #[test]
    fn test_varp_aggregator() {
        let mut agg = VarPAggregator::default();
        assert_eq!(agg.current(), None);

        // Dataset: [2, 4, 4, 4, 5, 5, 7, 9]
        let values = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        for (i, &val) in values.iter().enumerate() {
            agg.update(i as i64, val);
        }

        // Population variance = 4.0
        let result = agg.current().unwrap();
        assert!((result - 4.0).abs() < 0.0001);
    }

    #[test]
    fn test_vars_aggregator() {
        let mut agg = VarSAggregator::default();
        assert_eq!(agg.current(), None);

        // Single value should have variance 0
        agg.update(1000, 10.0);
        assert_eq!(agg.current(), Some(0.0));

        agg.reset();

        // Dataset: [2, 4, 4, 4, 5, 5, 7, 9]
        let values = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        for (i, &val) in values.iter().enumerate() {
            agg.update(i as i64, val);
        }

        // Sample variance ≈ 4.571
        let result = agg.current().unwrap();
        assert!((result - 4.571428).abs() < 0.0001);
    }

    #[test]
    fn test_stdp_aggregator() {
        let mut agg = StdPAggregator::default();
        assert_eq!(agg.current(), None);

        let values = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        for (i, &val) in values.iter().enumerate() {
            agg.update(i as i64, val);
        }

        // Population std dev = 2.0
        let result = agg.current().unwrap();
        assert!((result - 2.0).abs() < 0.0001);
    }

    #[test]
    fn test_stds_aggregator() {
        let mut agg = StdSAggregator::default();

        // Single value should have std dev 0
        agg.update(1000, 10.0);
        assert_eq!(agg.current(), Some(0.0));

        agg.reset();

        let values = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        for (i, &val) in values.iter().enumerate() {
            agg.update(i as i64, val);
        }

        // Sample std dev ≈ 2.138
        let result = agg.current().unwrap();
        assert!((result - 2.138089).abs() < 0.0001);
    }

    #[test]
    fn test_increase_aggregator() {
        let mut agg = IncreaseAggregator::default();
        assert_eq!(agg.current(), None);

        // Monotonically increasing counter
        agg.update(1000, 100.0);
        assert_eq!(agg.current(), None); // First point: no baseline, no increase yet

        agg.update(2000, 110.0);
        assert_eq!(agg.current(), Some(10.0)); // Increased by 10

        agg.update(3000, 125.0);
        assert_eq!(agg.current(), Some(25.0)); // Total increase: 25

        // Counter reset
        agg.update(4000, 5.0);
        assert_eq!(agg.current(), Some(25.0)); // No change, reset detected

        agg.update(5000, 15.0);
        assert_eq!(agg.current(), Some(35.0)); // 25 + 10 = 35
    }

    /// A counter that jumps by 1e16 and then resets a hundred times, each time climbing by 1:
    /// a plain running sum absorbs every 1 into the 1e16 (its ulp is 2), the compensated one
    /// keeps them. `1e16 + 100` is representable, so it is the exact answer.
    #[test]
    fn test_counter_delta_sum_is_compensated() {
        let mut values = vec![0.0, 1e16];
        for _ in 0..100 {
            values.extend([0.0, 1.0]);
        }
        let expected = 1e16 + 100.0;

        let mut increase = IncreaseAggregator::default();
        let mut rate = RateAggregator::new(Duration::from_secs(1));
        for (ts, &value) in values.iter().enumerate() {
            increase.update(ts as i64, value);
            rate.update(ts as i64, value);
        }
        assert_eq!(increase.current(), Some(expected));
        assert_eq!(rate.current(), Some(expected));
    }

    #[test]
    #[should_panic(expected = "rate window")]
    #[cfg(debug_assertions)]
    fn test_rate_without_a_window_is_a_bug() {
        let mut agg = RateAggregator::default();
        agg.update(0, 1.0);
    }

    #[test]
    fn test_increase_aggregator_negative_delta() {
        let mut agg = IncreaseAggregator::default();

        agg.update(1000, 100.0);
        agg.update(2000, 110.0);

        // Counter went backwards (reset)
        agg.update(3000, 50.0);
        assert_eq!(agg.current(), Some(10.0)); // Only counts previous increase

        agg.update(4000, 60.0);
        assert_eq!(agg.current(), Some(20.0)); // 10 + 10 = 20
    }

    #[test]
    fn test_rate_aggregator() {
        let mut agg = RateAggregator::new(Duration::from_secs(10));
        assert_eq!(agg.current(), None);

        agg.update(0, 100.0);
        agg.update(1000, 110.0);
        agg.update(2000, 125.0);

        // Total increase: 25 over 10-second window = 2.5/sec
        let result = agg.current().unwrap();
        assert_eq!(result, 2.5);
    }

    #[test]
    fn test_rate_aggregator_sub_second_windows() {
        // The window used to be kept in whole seconds: 1500 ms divided by 1 s, and anything
        // under a second by zero (no value at all).
        let mut agg = RateAggregator::default();
        agg.set_window_ms(1500);
        agg.update(0, 0.0);
        agg.update(1000, 15.0);
        assert_eq!(agg.current(), Some(10.0));

        let mut agg = RateAggregator::default();
        agg.set_window_ms(500);
        agg.update(0, 0.0);
        agg.update(400, 5.0);
        assert_eq!(agg.current(), Some(10.0));
    }

    #[test]
    fn test_rate_aggregator_with_reset() {
        let mut agg = RateAggregator::new(Duration::from_secs(5));

        agg.update(0, 100.0);
        agg.update(1000, 110.0);
        // Counter reset
        agg.update(2000, 10.0);
        agg.update(3000, 20.0);

        // Total valid increase: 10 + 10 = 20 over 5 seconds = 4.0/sec
        let result = agg.current().unwrap();
        assert_eq!(result, 4.0);
    }

    #[test]
    fn test_irate_aggregator() {
        let mut agg = IRateAggregator::default();
        assert_eq!(agg.current(), None);

        // First point
        agg.update(0, 100.0);
        assert_eq!(agg.current(), None);

        // Second point: increase of 10 over 1 second = 10/sec
        agg.update(1000, 110.0);
        assert_eq!(agg.current(), Some(10.0));

        // Third point: increase of 20 over 1 second = 20/sec
        agg.update(2000, 130.0);
        assert_eq!(agg.current(), Some(20.0));
    }

    #[test]
    fn test_irate_aggregator_negative_delta() {
        let mut agg = IRateAggregator::default();

        agg.update(0, 100.0);
        agg.update(1000, 110.0);

        // Counter went backwards - should not update
        agg.update(2000, 50.0);
        assert_eq!(agg.current(), None); // Reset and keeps previous rate. Only one valid increase.

        agg.update(3000, 70.0);
        assert_eq!(agg.current(), Some(20.0)); // 20 over 1 second
    }

    /// Feeds `buckets` through one aggregator, finalizing after each, as the bucket engine does.
    fn per_bucket<A: AggregationHandler>(mut agg: A, buckets: &[&[(i64, f64)]]) -> Vec<f64> {
        buckets
            .iter()
            .map(|bucket| {
                for &(ts, value) in *bucket {
                    assert!(agg.update(ts, value), "every non-NaN sample is accepted");
                }
                agg.finalize()
            })
            .collect()
    }

    /// The counter aggregators carry their last sample across a bucket boundary, so the delta
    /// that crosses it lands in the later bucket and the per-bucket increases sum to the total.
    #[test]
    fn test_counter_aggregators_carry_the_baseline() {
        let buckets: &[&[(i64, f64)]] = &[
            &[(1, 1.0), (2, 2.0)],
            &[(3, 3.0), (4, 4.0), (5, 5.0)],
            &[(6, 6.0), (7, 7.0), (8, 8.0)],
            &[(9, 9.0)],
        ];
        let increase = per_bucket(IncreaseAggregator::default(), buckets);
        assert_eq!(increase, [1.0, 3.0, 3.0, 1.0]);
        assert_eq!(increase.iter().sum::<f64>(), 8.0);

        let mut rate = RateAggregator::default();
        rate.set_window_ms(1000);
        assert_eq!(per_bucket(rate, buckets), [1.0, 3.0, 3.0, 1.0]);

        assert_eq!(
            per_bucket(IRateAggregator::default(), buckets),
            [1000.0, 1000.0, 1000.0, 1000.0]
        );
    }

    /// One sample and no earlier one: no delta was observed, and all three agree on it.
    #[test]
    fn test_counter_aggregators_agree_without_a_baseline() {
        let mut rate = RateAggregator::default();
        rate.set_window_ms(1000);
        let only: &[&[(i64, f64)]] = &[&[(5, 7.0)]];
        assert!(per_bucket(IncreaseAggregator::default(), only)[0].is_nan());
        assert!(per_bucket(rate, only)[0].is_nan());
        assert!(per_bucket(IRateAggregator::default(), only)[0].is_nan());
    }

    /// A bucket whose only sample is a counter reset relative to the carried baseline is still a
    /// bucket the counter took a sample from: `increase` and `rate` report 0, `irate` no rate.
    #[test]
    fn test_counter_reset_at_a_bucket_boundary() {
        let buckets: &[&[(i64, f64)]] = &[&[(1, 10.0), (2, 20.0)], &[(3, 5.0)], &[(4, 8.0)]];
        assert_eq!(
            per_bucket(IncreaseAggregator::default(), buckets),
            [10.0, 0.0, 3.0]
        );
        let irate = per_bucket(IRateAggregator::default(), buckets);
        assert_eq!(irate[0], 10_000.0);
        assert!(irate[1].is_nan());
        assert_eq!(irate[2], 3000.0);
    }

    /// `clear` starts over, dropping the carried baseline that `reset` keeps.
    #[test]
    fn test_counter_clear_drops_the_baseline() {
        let mut agg = IncreaseAggregator::default();
        agg.update(1, 1.0);
        agg.update(2, 4.0);
        agg.reset();
        agg.update(3, 6.0);
        assert_eq!(agg.current(), Some(2.0));
        agg.clear();
        agg.update(4, 9.0);
        assert_eq!(agg.current(), None);

        let mut irate = IRateAggregator::default();
        irate.update(1, 1.0);
        irate.clear();
        irate.update(2, 2.0);
        assert_eq!(irate.current(), None);
    }

    #[test]
    fn test_aggregator_reset() {
        let mut agg = SumAggregator::default();
        agg.update(1000, 10.0);
        agg.update(2000, 20.0);
        assert_eq!(agg.current(), Some(30.0));

        agg.reset();
        assert_eq!(agg.current(), None);
    }

    #[test]
    fn test_aggregator_finalize() {
        let mut agg = AvgAggregator::default();
        agg.update(1000, 10.0);
        agg.update(2000, 20.0);

        let result = agg.finalize();
        assert_eq!(result, 15.0);

        // After finalize, aggregator should be reset
        assert_eq!(agg.current(), None);
    }

    #[test]
    fn test_aggregator_empty_finalize() {
        let mut avg = AvgAggregator::default();
        let result = avg.finalize();
        assert!(result.is_nan());

        let mut sum = SumAggregator::default();
        let result = sum.finalize();
        assert_eq!(result, 0.0);

        let mut count = CountAggregator::default();
        let result = count.finalize();
        assert_eq!(result, 0.0);
    }

    #[test]
    fn test_aggregator_with_nan_values() {
        let mut agg = SumAggregator::default();
        agg.update(1000, 10.0);
        agg.update(2000, f64::NAN);
        agg.update(3000, 20.0);

        let result = agg.current().unwrap();
        assert_eq!(result, 30.0);
    }

    #[test]
    fn test_aggregator_with_infinity() {
        let mut agg = MaxAggregator::default();
        agg.update(1000, 10.0);
        agg.update(2000, f64::INFINITY);
        agg.update(3000, 20.0);

        assert_eq!(agg.current(), Some(f64::INFINITY));
    }

    #[test]
    fn test_counter_aggregator_zero_delta() {
        let mut agg = IncreaseAggregator::default();
        agg.update(1000, 100.0);
        agg.update(2000, 100.0); // No change
        agg.update(3000, 100.0); // No change

        assert_eq!(agg.current(), Some(0.0));
    }

    // ========== CountIfAggregator Tests ==========

    #[test]
    fn test_countif_aggregator_basic() {
        let mut agg = CountIfAggregator::new(ComparisonOperator::GreaterThan, 10.0);
        assert_eq!(agg.current(), None);

        agg.update(1000, 5.0);
        assert_eq!(agg.current(), None); // Doesn't match condition

        agg.update(2000, 15.0);
        assert_eq!(agg.current(), Some(1.0)); // First match

        agg.update(3000, 20.0);
        assert_eq!(agg.current(), Some(2.0)); // Second match

        agg.update(4000, 8.0);
        assert_eq!(agg.current(), Some(2.0)); // Doesn't match

        assert_eq!(agg.finalize(), 2.0);
    }

    #[test]
    fn test_countif_aggregator_all_match() {
        let mut agg = CountIfAggregator::new(ComparisonOperator::GreaterThanOrEqual, 0.0);

        agg.update(1000, 10.0);
        agg.update(2000, 20.0);
        agg.update(3000, 30.0);

        assert_eq!(agg.current(), Some(3.0));
    }

    #[test]
    fn test_countif_aggregator_none_match() {
        let mut agg = CountIfAggregator::new(ComparisonOperator::LessThan, 0.0);

        agg.update(1000, 10.0);
        agg.update(2000, 20.0);
        agg.update(3000, 30.0);

        assert_eq!(agg.current(), None);
        assert_eq!(agg.finalize(), 0.0);
    }

    #[test]
    fn test_countif_aggregator_equal() {
        let mut agg = CountIfAggregator::new(ComparisonOperator::Equal, 10.0);

        agg.update(1000, 10.0);
        agg.update(2000, 20.0);
        agg.update(3000, 10.0);
        agg.update(4000, 10.0);

        assert_eq!(agg.current(), Some(3.0));
    }

    #[test]
    fn test_countif_aggregator_not_equal() {
        let mut agg = CountIfAggregator::new(ComparisonOperator::NotEqual, 10.0);

        agg.update(1000, 10.0);
        agg.update(2000, 20.0);
        agg.update(3000, 30.0);

        assert_eq!(agg.current(), Some(2.0));
    }

    #[test]
    fn test_countif_aggregator_reset() {
        let mut agg = CountIfAggregator::new(ComparisonOperator::GreaterThan, 10.0);

        agg.update(1000, 15.0);
        agg.update(2000, 20.0);
        assert_eq!(agg.current(), Some(2.0));

        agg.reset();
        assert_eq!(agg.current(), None);
    }

    #[test]
    fn test_countif_aggregator_with_nan() {
        let mut agg = CountIfAggregator::new(ComparisonOperator::GreaterThan, 10.0);

        agg.update(1000, 15.0);
        agg.update(2000, f64::NAN);
        agg.update(3000, 20.0);

        // NaN comparisons are always false
        assert_eq!(agg.current(), Some(2.0));
    }

    // ========== SumIfAggregator Tests ==========

    #[test]
    fn test_sumif_aggregator_basic() {
        let mut agg = SumIfAggregator::new(ComparisonOperator::GreaterThan, 10.0);
        assert_eq!(agg.current(), None);

        agg.update(1000, 5.0);
        assert_eq!(agg.current(), None); // Doesn't match

        agg.update(2000, 15.0);
        assert_eq!(agg.current(), Some(15.0)); // First match

        agg.update(3000, 20.0);
        assert_eq!(agg.current(), Some(35.0)); // 15 + 20

        agg.update(4000, 8.0);
        assert_eq!(agg.current(), Some(35.0)); // Doesn't match

        assert_eq!(agg.finalize(), 35.0);
    }

    #[test]
    fn test_sumif_aggregator_negative_values() {
        let mut agg = SumIfAggregator::new(ComparisonOperator::LessThan, 0.0);

        agg.update(1000, -5.0);
        agg.update(2000, 10.0);
        agg.update(3000, -3.0);
        agg.update(4000, -7.0);

        assert_eq!(agg.current(), Some(-15.0)); // -5 + -3 + -7
    }

    #[test]
    fn test_sumif_aggregator_none_match() {
        let mut agg = SumIfAggregator::new(ComparisonOperator::GreaterThan, 100.0);

        agg.update(1000, 10.0);
        agg.update(2000, 20.0);

        assert_eq!(agg.current(), None);
        assert_eq!(agg.finalize(), 0.0);
    }

    /// `[values, expected]` per aggregator, condition `> 5`, through the trait contract: the
    /// result, then a reset that leaves nothing to report.
    fn assert_quantifier<A: AggregationHandler>(mut agg: A, cases: &[(&[f64], f64)]) {
        for &(values, expected) in cases {
            assert_eq!(agg.current(), None);
            for (ts, &value) in values.iter().enumerate() {
                assert!(agg.update(ts as i64, value), "every sample is accepted");
            }
            assert_eq!(agg.current(), Some(expected), "{values:?}");
            assert_eq!(agg.finalize(), expected, "{values:?}");
        }
        assert_eq!(agg.current(), None);
        assert!(agg.finalize().is_nan());
    }

    #[test]
    fn test_all_aggregator() {
        let agg = crate::aggregators::AllAggregator::new(ComparisonOperator::GreaterThan, 5.0);
        assert_quantifier(
            agg,
            &[
                (&[10.0, 20.0], 1.0),
                (&[10.0, 1.0, 20.0], 0.0),
                (&[1.0], 0.0),
            ],
        );
    }

    #[test]
    fn test_any_aggregator() {
        let agg = crate::aggregators::AnyAggregator::new(ComparisonOperator::GreaterThan, 5.0);
        assert_quantifier(
            agg,
            &[(&[1.0, 10.0, 2.0], 1.0), (&[1.0, 2.0], 0.0), (&[10.0], 1.0)],
        );
    }

    #[test]
    fn test_none_aggregator() {
        let agg = crate::aggregators::NoneAggregator::new(ComparisonOperator::GreaterThan, 5.0);
        assert_quantifier(
            agg,
            &[(&[1.0, 2.0], 1.0), (&[1.0, 10.0, 2.0], 0.0), (&[10.0], 0.0)],
        );
    }

    #[test]
    fn test_sumif_aggregator_all_values() {
        let mut agg = SumIfAggregator::new(ComparisonOperator::GreaterThanOrEqual, 0.0);

        agg.update(1000, 10.0);
        agg.update(2000, 20.0);
        agg.update(3000, 30.0);

        assert_eq!(agg.current(), Some(60.0));
    }

    #[test]
    fn test_sumif_aggregator_reset() {
        let mut agg = SumIfAggregator::new(ComparisonOperator::GreaterThan, 10.0);

        agg.update(1000, 15.0);
        agg.update(2000, 20.0);
        assert_eq!(agg.current(), Some(35.0));

        agg.reset();
        assert_eq!(agg.current(), None);
    }

    #[test]
    fn test_sumif_aggregator_with_nan() {
        let mut agg = SumIfAggregator::new(ComparisonOperator::GreaterThan, 10.0);

        agg.update(1000, 15.0);
        agg.update(2000, f64::NAN);
        agg.update(3000, 20.0);

        // NaN is not added to sum
        assert_eq!(agg.current(), Some(35.0));
    }

    #[test]
    fn test_sumif_aggregator_with_infinity() {
        let mut agg = SumIfAggregator::new(ComparisonOperator::GreaterThan, 0.0);

        agg.update(1000, 10.0);
        agg.update(2000, f64::INFINITY);
        agg.update(3000, 20.0);

        assert_eq!(agg.current(), Some(f64::INFINITY));
    }

    // ========== ShareAggregator Tests ==========

    #[test]
    fn test_share_aggregator_basic() {
        let mut agg = ShareAggregator::new(ComparisonOperator::GreaterThan, 10.0);
        assert_eq!(agg.current(), None);

        agg.update(1000, 5.0);
        assert_eq!(agg.current(), Some(0.0)); // 0/1 = 0%

        agg.update(2000, 15.0);
        assert_eq!(agg.current(), Some(0.5)); // 1/2 = 50%

        agg.update(3000, 20.0);
        assert!((agg.current().unwrap() - 2.0 / 3.0).abs() < 1e-10); // 2/3 ≈ 66.67%

        agg.update(4000, 25.0);
        assert_eq!(agg.current(), Some(0.75)); // 3/4 = 75%
    }

    #[test]
    fn test_share_aggregator_all_match() {
        let mut agg = ShareAggregator::new(ComparisonOperator::GreaterThanOrEqual, 0.0);

        agg.update(1000, 10.0);
        agg.update(2000, 20.0);
        agg.update(3000, 30.0);

        assert_eq!(agg.current(), Some(1.0)); // 100%
    }

    #[test]
    fn test_share_aggregator_none_match() {
        let mut agg = ShareAggregator::new(ComparisonOperator::LessThan, 0.0);

        agg.update(1000, 10.0);
        agg.update(2000, 20.0);

        assert_eq!(agg.current(), Some(0.0)); // 0%
    }

    #[test]
    fn test_share_aggregator_50_percent() {
        let mut agg = ShareAggregator::new(ComparisonOperator::GreaterThan, 15.0);

        agg.update(1000, 10.0);
        agg.update(2000, 20.0);
        agg.update(3000, 12.0);
        agg.update(4000, 25.0);

        assert_eq!(agg.current(), Some(0.5)); // 2/4 = 50%
    }

    #[test]
    fn test_share_aggregator_reset() {
        let mut agg = ShareAggregator::new(ComparisonOperator::GreaterThan, 10.0);

        agg.update(1000, 15.0);
        agg.update(2000, 20.0);
        assert_eq!(agg.current(), Some(1.0));

        agg.reset();
        assert_eq!(agg.current(), None);
    }

    #[test]
    fn test_share_aggregator_with_nan() {
        let mut agg = ShareAggregator::new(ComparisonOperator::GreaterThan, 10.0);

        agg.update(1000, 15.0);
        assert!(!agg.update(2000, f64::NAN), "NaN is skipped, not counted");
        agg.update(3000, 20.0);

        assert_eq!(agg.current(), Some(1.0));
    }

    /// The whole filtered family skips NaN, as `countif`/`sumif` do: under `!=` a NaN would
    /// otherwise satisfy the condition, and under `==` it would count against the share.
    #[test]
    fn test_filtered_family_skips_nan() {
        use crate::aggregators::{AllAggregator, AnyAggregator, NoneAggregator};
        let ne = ComparisonOperator::NotEqual;
        let values = [f64::NAN, 10.0, f64::NAN, 20.0];

        fn run<A: AggregationHandler>(mut agg: A, values: &[f64]) -> (Vec<bool>, Option<f64>) {
            let accepted = values
                .iter()
                .enumerate()
                .map(|(ts, &v)| agg.update(ts as i64, v))
                .collect();
            (accepted, agg.current())
        }
        let expect_accepted = vec![false, true, false, true];

        assert_eq!(
            run(ShareAggregator::new(ne, 5.0), &values),
            (expect_accepted.clone(), Some(1.0))
        );
        assert_eq!(
            run(AllAggregator::new(ne, 5.0), &values),
            (expect_accepted.clone(), Some(1.0))
        );
        assert_eq!(
            run(AnyAggregator::new(ne, 5.0), &values),
            (expect_accepted.clone(), Some(1.0))
        );
        assert_eq!(
            run(NoneAggregator::new(ne, 5.0), &values),
            (expect_accepted.clone(), Some(0.0))
        );
        assert_eq!(
            run(CountIfAggregator::new(ne, 5.0), &values),
            (expect_accepted, Some(2.0))
        );

        // A bucket of NaNs alone gives none of them anything to report.
        let nans = [f64::NAN, f64::NAN];
        let eq = ComparisonOperator::Equal;
        assert_eq!(
            run(ShareAggregator::new(eq, 5.0), &nans),
            (vec![false; 2], None)
        );
        assert_eq!(
            run(AllAggregator::new(eq, 5.0), &nans),
            (vec![false; 2], None)
        );
        assert_eq!(
            run(AnyAggregator::new(eq, 5.0), &nans),
            (vec![false; 2], None)
        );
        assert_eq!(
            run(NoneAggregator::new(eq, 5.0), &nans),
            (vec![false; 2], None)
        );
    }

    #[test]
    fn test_share_aggregator_single_value_match() {
        let mut agg = ShareAggregator::new(ComparisonOperator::Equal, 10.0);

        agg.update(1000, 10.0);
        assert_eq!(agg.current(), Some(1.0)); // 100%
    }

    #[test]
    fn test_share_aggregator_single_value_no_match() {
        let mut agg = ShareAggregator::new(ComparisonOperator::Equal, 10.0);

        agg.update(1000, 5.0);
        assert_eq!(agg.current(), Some(0.0)); // 0%
    }

    // ========== All Comparison Operators Tests ==========

    #[test]
    fn test_all_comparison_operators_countif() {
        let test_cases = vec![
            (
                ComparisonOperator::Equal,
                10.0,
                vec![(10.0, true), (11.0, false)],
            ),
            (
                ComparisonOperator::NotEqual,
                10.0,
                vec![(10.0, false), (11.0, true)],
            ),
            (
                ComparisonOperator::LessThan,
                10.0,
                vec![(9.0, true), (10.0, false), (11.0, false)],
            ),
            (
                ComparisonOperator::LessThanOrEqual,
                10.0,
                vec![(9.0, true), (10.0, true), (11.0, false)],
            ),
            (
                ComparisonOperator::GreaterThan,
                10.0,
                vec![(9.0, false), (10.0, false), (11.0, true)],
            ),
            (
                ComparisonOperator::GreaterThanOrEqual,
                10.0,
                vec![(9.0, false), (10.0, true), (11.0, true)],
            ),
        ];

        for (op, threshold, test_values) in test_cases {
            let mut agg = CountIfAggregator::new(op, threshold);
            let mut expected_count = 0.0;

            for (i, (value, should_match)) in test_values.iter().enumerate() {
                agg.update(i as i64, *value);
                if *should_match {
                    expected_count += 1.0;
                }
            }

            let result = agg.current();
            if expected_count > 0.0 {
                assert_eq!(
                    result,
                    Some(expected_count),
                    "Failed for operator {:?} with threshold {}",
                    op,
                    threshold
                );
            } else {
                assert_eq!(
                    result, None,
                    "Failed for operator {:?} with threshold {} (expected None)",
                    op, threshold
                );
            }
        }
    }

    #[test]
    fn test_share_returns_results_in_range() {
        let operators = vec![
            ComparisonOperator::Equal,
            ComparisonOperator::NotEqual,
            ComparisonOperator::LessThan,
            ComparisonOperator::LessThanOrEqual,
            ComparisonOperator::GreaterThan,
            ComparisonOperator::GreaterThanOrEqual,
        ];

        for op in operators {
            let mut agg = ShareAggregator::new(op, 10.0);
            agg.update(1000, 5.0);
            agg.update(2000, 10.0);
            agg.update(3000, 15.0);

            let result = agg.current().unwrap();
            assert!((0.0..=1.0).contains(&result), "Share must be in [0, 1]");
        }
    }

    // ========== Edge Cases ==========

    #[test]
    fn test_share_aggregator_empty_finalize() {
        let agg = ShareAggregator::new(ComparisonOperator::GreaterThan, 10.0);
        assert!(agg.share().is_nan());
    }

    #[test]
    fn test_share_aggregator_large_dataset() {
        let mut agg = ShareAggregator::new(ComparisonOperator::GreaterThan, 50.0);

        // Add 1000 values: 0..999
        for i in 0..1000 {
            agg.update(i, i as f64);
        }

        // Values > 50: 51..999 = 949 values
        // Share = 949/1000 = 0.949
        let share = agg.current().unwrap();
        assert!((share - 0.949).abs() < 0.001);
    }

    // ========== CountNanAggregator Tests ==========

    #[test]
    fn test_count_nan_aggregator_empty() {
        let mut agg = CountNanAggregator::default();
        assert_eq!(agg.current(), None);
        assert_eq!(agg.empty_value(), 0.0);
        assert_eq!(agg.finalize(), 0.0);
    }

    #[test]
    fn test_count_nan_aggregator_no_nans() {
        let mut agg = CountNanAggregator::default();
        agg.update(1000, 1.0);
        agg.update(2000, 2.0);
        agg.update(3000, 3.0);
        assert_eq!(agg.current(), None);
        assert_eq!(agg.finalize(), 0.0);
    }

    #[test]
    fn test_count_nan_aggregator_all_nans() {
        let mut agg = CountNanAggregator::default();
        agg.update(1000, f64::NAN);
        agg.update(2000, f64::NAN);
        agg.update(3000, f64::NAN);
        assert_eq!(agg.current(), Some(3.0));
    }

    #[test]
    fn test_count_nan_aggregator_mixed() {
        let mut agg = CountNanAggregator::default();
        agg.update(1000, 1.0);
        agg.update(2000, f64::NAN);
        agg.update(3000, 2.0);
        agg.update(4000, f64::NAN);
        agg.update(5000, f64::NAN);
        assert_eq!(agg.current(), Some(3.0));
    }

    #[test]
    fn test_count_nan_aggregator_ignores_infinity() {
        let mut agg = CountNanAggregator::default();
        agg.update(1000, f64::INFINITY);
        agg.update(2000, f64::NEG_INFINITY);
        agg.update(3000, f64::NAN);
        // Only NaN should be counted; infinities are not NaN
        assert_eq!(agg.current(), Some(1.0));
    }

    #[test]
    fn test_count_nan_aggregator_reset() {
        let mut agg = CountNanAggregator::default();
        agg.update(1000, f64::NAN);
        agg.update(2000, f64::NAN);
        assert_eq!(agg.current(), Some(2.0));
        agg.reset();
        assert_eq!(agg.current(), None);
    }

    #[test]
    fn test_count_nan_aggregator_finalize_resets() {
        let mut agg = CountNanAggregator::default();
        agg.update(1000, f64::NAN);
        agg.update(2000, f64::NAN);
        assert_eq!(agg.finalize(), 2.0);
        assert_eq!(agg.current(), None);
    }

    // ========== CountAllAggregator Tests ==========

    #[test]
    fn test_count_all_aggregator_empty() {
        let mut agg = CountAllAggregator::default();
        assert_eq!(agg.current(), None);
        assert_eq!(agg.empty_value(), 0.0);
        assert_eq!(agg.finalize(), 0.0);
    }

    #[test]
    fn test_count_all_aggregator_counts_all_values() {
        let mut agg = CountAllAggregator::default();
        agg.update(1000, 1.0);
        agg.update(2000, f64::NAN);
        agg.update(3000, 3.0);
        // Should count all 3 values, including non-NaN
        assert_eq!(agg.current(), Some(3.0));
    }

    #[test]
    fn test_count_all_aggregator_counts_nan() {
        let mut agg = CountAllAggregator::default();
        agg.update(1000, f64::NAN);
        agg.update(2000, f64::NAN);
        assert_eq!(agg.current(), Some(2.0));
    }

    #[test]
    fn test_count_all_aggregator_counts_mixed() {
        let mut agg = CountAllAggregator::default();
        agg.update(1000, 1.0);
        agg.update(2000, f64::NAN);
        agg.update(3000, 3.0);
        agg.update(4000, f64::NAN);
        // Should count all 4 values regardless of whether they are NaN
        assert_eq!(agg.current(), Some(4.0));
    }

    #[test]
    fn test_count_all_aggregator_counts_infinity() {
        let mut agg = CountAllAggregator::default();
        agg.update(1000, f64::INFINITY);
        agg.update(2000, f64::NEG_INFINITY);
        agg.update(3000, 0.0);
        assert_eq!(agg.current(), Some(3.0));
    }

    #[test]
    fn test_count_all_aggregator_reset() {
        let mut agg = CountAllAggregator::default();
        agg.update(1000, 1.0);
        agg.update(2000, 2.0);
        assert_eq!(agg.current(), Some(2.0));
        agg.reset();
        assert_eq!(agg.current(), None);
    }

    #[test]
    fn test_count_all_aggregator_finalize_resets() {
        let mut agg = CountAllAggregator::default();
        agg.update(1000, 1.0);
        agg.update(2000, 2.0);
        assert_eq!(agg.finalize(), 2.0);
        assert_eq!(agg.current(), None);
    }
}
