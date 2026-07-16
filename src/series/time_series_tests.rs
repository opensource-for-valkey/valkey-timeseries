#[cfg(test)]
mod tests {
    use crate::common::rounding::RoundingStrategy;
    use crate::common::time::current_time_millis;
    use crate::common::{Sample, Timestamp};
    use crate::series::chunks::{ChunkEncoding, ChunkOps, GorillaChunk, TimeSeriesChunk};
    use crate::series::{
        DuplicatePolicy, SampleAddResult, TimeSeries, TimeSeriesOptions, ValueFilter,
    };
    use crate::tests::generators::{DataGenerator, ValueWorkload};
    use std::time::Duration;

    fn create_test_series() -> TimeSeries {
        let options = TimeSeriesOptions {
            chunk_size: Some(1024),
            ..Default::default()
        };
        TimeSeries::with_options(options).unwrap()
    }

    fn create_chunk(size: Option<usize>) -> TimeSeriesChunk {
        TimeSeriesChunk::Gorilla(GorillaChunk::with_max_size(size.unwrap_or(1024)))
    }

    fn create_chunk_with_samples(samples: Vec<Sample>) -> TimeSeriesChunk {
        let mut chunk = create_chunk(None);
        for sample in samples {
            chunk.add_sample(&sample).unwrap();
        }
        chunk
    }

    fn create_chunk_with_timestamps(start: Timestamp, end: Timestamp) -> TimeSeriesChunk {
        let mut chunk = create_chunk(None);
        for ts in start..=end {
            chunk
                .add_sample(&Sample {
                    timestamp: ts,
                    value: 1.0 + ts as f64,
                })
                .unwrap();
        }
        chunk
    }

    fn assert_sample_added(result: SampleAddResult, expected_sample: Sample) {
        match result {
            SampleAddResult::Ok(sample) => assert_eq!(sample, expected_sample),
            _ => panic!("Expected SampleAddResult::Ok, got {result:?}"),
        }
    }

    fn assert_duplicate(result: SampleAddResult) {
        match result {
            SampleAddResult::Duplicate => {}
            _ => panic!("Expected SampleAddResult::Duplicate, got {result:?}"),
        }
    }

    #[test]
    fn test_add_first_sample() {
        let mut ts = TimeSeries::new();
        // Add first sample
        let result = ts.add(100, 200.0, None);

        assert!(result.is_ok());
        assert_eq!(ts.total_samples, 1);
        assert_eq!(ts.first_timestamp, 100);
        assert_eq!(ts.last_timestamp(), 100);
        assert_eq!(
            ts.last_sample,
            Some(Sample {
                timestamp: 100,
                value: 200.0
            })
        );
    }

    #[test]
    fn test_add_multiple_samples_in_order() {
        let mut ts = TimeSeries::new();

        // Add samples in chronological order
        assert!(ts.add(100, 200.0, None).is_ok());
        assert!(ts.add(200, 300.0, None).is_ok());
        assert!(ts.add(300, 400.0, None).is_ok());

        assert_eq!(ts.total_samples, 3);
        assert_eq!(ts.first_timestamp, 100);
        assert_eq!(ts.last_timestamp(), 300);
        assert_eq!(
            ts.last_sample,
            Some(Sample {
                timestamp: 300,
                value: 400.0
            })
        );
    }

    #[test]
    fn test_add_with_rounding() {
        let mut ts = TimeSeries::new();
        ts.rounding = Some(RoundingStrategy::DecimalDigits(1));

        // Value should be rounded to 1 decimal place
        let result = ts.add(100, 200.123, None);

        assert!(result.is_ok());
        assert_eq!(
            ts.last_sample,
            Some(Sample {
                timestamp: 100,
                value: 200.1
            })
        );
    }

    #[test]
    fn test_add_duplicate_timestamp() {
        let mut ts = TimeSeries::new();
        ts.sample_duplicates.policy = Some(DuplicatePolicy::KeepLast);

        // Add first sample
        assert!(ts.add(100, 200.0, None).is_ok());

        // Add sample with same timestamp but different value
        let result = ts.add(100, 300.0, None);

        assert!(result.is_ok());
        assert_eq!(ts.total_samples, 1); // Should replace, not add
        assert_eq!(
            ts.last_sample,
            Some(Sample {
                timestamp: 100,
                value: 300.0
            })
        );
    }

    #[test]
    fn test_add_duplicate_with_override_policy() {
        let mut ts = TimeSeries::new();
        ts.sample_duplicates.policy = Some(DuplicatePolicy::Block);

        // Add first sample
        assert!(ts.add(100, 200.0, None).is_ok());

        // Add duplicate but override policy to KeepLast
        let result = ts.add(100, 300.0, Some(DuplicatePolicy::KeepLast));

        assert!(result.is_ok());
        assert_eq!(
            ts.last_sample,
            Some(Sample {
                timestamp: 100,
                value: 300.0
            })
        );
    }

    #[test]
    fn test_add_older_sample() {
        let mut ts = TimeSeries::new();

        // Add first sample
        assert!(ts.add(200, 200.0, None).is_ok());

        // Add older sample
        let result = ts.add(100, 100.0, None);

        assert!(result.is_ok());

        assert_eq!(ts.total_samples, 2);
        assert_eq!(ts.first_timestamp, 100); // First timestamp should update
        assert_eq!(ts.last_timestamp(), 200); // Last timestamp unchanged
    }

    #[test]
    fn test_add_sample_too_old() {
        let mut ts = TimeSeries::new();
        ts.retention = Duration::from_millis(1000);

        // Add a sample
        let now = current_time_millis();
        assert!(ts.add(now, 100.0, None).is_ok());

        // Try to add a sample older than retention period
        let old_ts = now - 2000;
        let result = ts.add(old_ts, 50.0, None);

        assert!(matches!(result, SampleAddResult::TooOld));
        assert_eq!(ts.total_samples, 1); // Sample count unchanged
    }

    #[test]
    fn test_add_extreme_values() {
        let mut series = TimeSeries::new();

        // Test with a very large timestamp (i64::MAX)
        let max_timestamp = i64::MAX as Timestamp;
        let result = series.add(max_timestamp, 100.0, None);
        assert!(result.is_ok());

        // Test with a very large value (close to f64::MAX)
        let large_value = 1.797_693_134_862_315_7e308_f64;
        let result = series.add(160000, large_value, None);
        assert!(result.is_ok());

        // Verify the samples were added correctly
        let range = series.get_range(Timestamp::MIN, Timestamp::MAX);
        assert_eq!(range.len(), 2);

        // Check that the large timestamp sample was added
        let max_ts_sample = range.iter().find(|s| s.timestamp == max_timestamp);
        assert!(max_ts_sample.is_some());
        assert_eq!(max_ts_sample.unwrap().value, 100.0);

        // Check that the large value sample was added
        let large_value_sample = range.iter().find(|s| s.timestamp == 160000);
        assert!(large_value_sample.is_some());
        assert!((large_value_sample.unwrap().value - large_value).abs() < 1e300);
    }

    #[test]
    fn test_add_causes_chunk_split() {
        let mut ts = TimeSeries::new();
        // Set a very small chunk size to force a split
        ts.chunk_size_bytes = 64;

        let data = DataGenerator::builder()
            .start(1000)
            .interval(Duration::from_millis(1000))
            .algorithm(ValueWorkload::Deriv)
            .samples(60)
            .build()
            .generate();

        let sample_count = data.len();
        // Add samples until we trigger a split
        for sample in data {
            assert!(ts.add(sample.timestamp, sample.value, None).is_ok());
        }

        // Verify we have more than one chunk
        assert!(ts.chunks.len() > 1);
        assert_eq!(ts.total_samples, sample_count);
    }

    #[test]
    fn test_add_ignores_duplicate_per_policy() {
        let mut ts = TimeSeries::new();
        ts.sample_duplicates.policy = Some(DuplicatePolicy::Block);

        // Add first sample
        assert!(ts.add(100, 200.0, None).is_ok());

        // Try to add duplicate
        let result = ts.add(100, 300.0, None);

        assert!(matches!(result, SampleAddResult::Duplicate));
        assert_eq!(
            ts.last_sample,
            Some(Sample {
                timestamp: 100,
                value: 200.0
            })
        );
    }

    #[test]
    fn test_add_to_empty_series() {
        let mut timeseries = TimeSeries::new();
        let timestamp = Timestamp::from(1_000_000);
        let value = 42.0;

        let result = timeseries.add(timestamp, value, None);

        assert!(result.is_ok(), "Adding to an empty series should succeed");
        assert_eq!(
            timeseries.total_samples, 1,
            "Total samples should increment after adding a sample"
        );
        assert_eq!(
            timeseries.first_timestamp, timestamp,
            "First timestamp should match the added sample"
        );
    }

    #[test]
    fn test_add_duplicate_sample_with_override() {
        let mut timeseries = TimeSeries::new();
        let timestamp = Timestamp::from(1_000_000);
        let value1 = 42.0;
        let value2 = 50.0;
        let duplicate_policy = Some(DuplicatePolicy::KeepLast);

        // Add the first sample
        timeseries.add(timestamp, value1, None);

        // Add a duplicate sample and override the value
        let result = timeseries.add(timestamp, value2, duplicate_policy);

        assert!(
            result.is_ok(),
            "Adding a duplicate sample with override should succeed"
        );
        assert_eq!(
            timeseries.total_samples, 1,
            "Total samples count should remain the same for duplicate policy"
        );
        assert_eq!(
            timeseries.last_sample.unwrap().value,
            value2,
            "Last sample value should be updated with the duplicate policy override"
        );
    }

    #[test]
    fn test_add_sample_before_first() {
        let mut timeseries = TimeSeries::new();
        // set a retention period to allow adding older samples
        timeseries.retention = Duration::from_millis(1000);

        let timestamp1 = Timestamp::from(1_000);
        let timestamp2 = Timestamp::from(500); // Out-of-order timestamp
        let value1 = 42.0;
        let value2 = 24.0;

        // Add the first sample
        timeseries.add(timestamp1, value1, None);

        // Attempt to add an out-of-order sample
        let result = timeseries.add(timestamp2, value2, None);

        assert!(result.is_ok(), "Should add a sample before the first");
    }

    #[test]
    fn test_add_sample_updates_boundaries() {
        let mut timeseries = TimeSeries::new();
        let timestamp1 = Timestamp::from(1_000);
        let timestamp2 = Timestamp::from(2_000);
        let value1 = 42.0;
        let value2 = 50.0;

        // Add the first sample
        timeseries.add(timestamp1, value1, None);

        // Add a second sample
        timeseries.add(timestamp2, value2, None);

        assert_eq!(
            timeseries.first_timestamp, timestamp1,
            "First timestamp should remain unchanged after adding later samples"
        );
        assert_eq!(
            timeseries.last_sample.unwrap().timestamp,
            timestamp2,
            "Last sample should update after adding a later sample"
        );
    }

    #[test]
    fn test_upsert_sample_into_empty_series() {
        let mut series = create_test_series();

        let sample = Sample {
            timestamp: 100,
            value: 1.0,
        };
        let result = series.add(sample.timestamp, sample.value, None);

        assert_sample_added(result, sample);
        assert_eq!(series.len(), 1);
        assert_eq!(series.first_timestamp, 100);
        assert_eq!(series.last_sample, Some(sample));
    }

    #[test]
    fn test_upsert_sample_before_existing_samples() {
        let mut series = create_test_series();

        // Add initial samples in order
        series.add(100, 1.0, None);
        series.add(200, 2.0, None);
        series.add(300, 3.0, None);

        // Add a sample before all existing samples
        let early_sample = Sample {
            timestamp: 50,
            value: 0.5,
        };
        let result = series.add(early_sample.timestamp, early_sample.value, None);

        assert_sample_added(result, early_sample);
        assert_eq!(series.len(), 4);
        assert_eq!(series.first_timestamp, 50);

        // Verify samples are in chronological order
        let samples = series.get_range(0, 400);
        assert_eq!(samples.len(), 4);
        assert_eq!(samples[0].timestamp, 50);
        assert_eq!(samples[1].timestamp, 100);
        assert_eq!(samples[2].timestamp, 200);
        assert_eq!(samples[3].timestamp, 300);
    }

    #[test]
    fn test_upsert_sample_in_middle_of_existing_samples() {
        let mut series = create_test_series();

        // Add initial samples with gaps
        series.add(100, 1.0, None);
        series.add(300, 3.0, None);
        series.add(500, 5.0, None);

        // Insert sample in the middle
        let middle_sample = Sample {
            timestamp: 200,
            value: 2.0,
        };
        let result = series.add(middle_sample.timestamp, middle_sample.value, None);

        assert_sample_added(result, middle_sample);
        assert_eq!(series.len(), 4);

        // Verify insertion order
        let samples = series.get_range(0, 600);
        assert_eq!(samples.len(), 4);
        assert_eq!(samples[0].timestamp, 100);
        assert_eq!(samples[1].timestamp, 200);
        assert_eq!(samples[2].timestamp, 300);
        assert_eq!(samples[3].timestamp, 500);
    }

    #[test]
    fn test_upsert_multiple_out_of_order_samples() {
        let mut series = create_test_series();

        // Add samples in random order
        let mut samples_to_add = [(300, 3.0), (100, 1.0), (500, 5.0), (200, 2.0), (400, 4.0)];

        for (ts, value) in samples_to_add.iter() {
            series.add(*ts, *value, None);
        }

        assert_eq!(series.len(), 5);

        samples_to_add.sort_by_key(|(ts, _)| *ts);
        // Verify they're stored in chronological order
        samples_to_add.iter().zip(series.iter()).for_each(|(a, b)| {
            assert_eq!(a.0, b.timestamp);
            assert_eq!(a.1, b.value);
        });
    }

    #[test]
    fn test_upsert_with_duplicate_policy() {
        let mut series = create_test_series();

        // Add an initial sample
        series.add(100, 1.0, None);

        // Add duplicate with KeepLast policy (default)
        let result = series.add(100, 2.0, Some(DuplicatePolicy::KeepLast));
        assert!(result.is_ok());
        assert_eq!(series.len(), 1);

        // Verify the value was updated
        let samples = series.get_range(0, 200);
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].value, 2.0);

        // Add duplicate with KeepFirst policy
        let result = series.add(100, 10.0, Some(DuplicatePolicy::KeepFirst));
        assert!(result.is_ok());
        assert_eq!(series.len(), 1);

        // Verify the value was updated
        let samples = series.get_range(0, 200);
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].value, 2.0);

        // Add duplicate with Sum policy
        let result = series.add(100, 10.0, Some(DuplicatePolicy::Sum));
        assert!(result.is_ok());
        assert_eq!(series.len(), 1);

        // Verify the value was updated
        let samples = series.get_range(0, 200);
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].value, 12.0);

        // Add duplicate with Block
        let result = series.add(100, 500.0, Some(DuplicatePolicy::Block));
        assert_duplicate(result);
        assert_eq!(series.len(), 1);

        // Verify the value was NOT updated
        let samples = series.get_range(0, 200);
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].value, 12.0);
    }

    #[test]
    fn test_upsert_updates_series_metadata() {
        let mut series = create_test_series();

        // Add an initial sample
        series.add(200, 2.0, None);
        assert_eq!(series.first_timestamp, 200);
        assert_eq!(series.last_sample.unwrap().timestamp, 200);

        // Add an earlier sample (should update first_timestamp)
        series.add(100, 1.0, None);
        assert_eq!(series.first_timestamp, 100);
        assert_eq!(series.last_sample.unwrap().timestamp, 200); // last should remain

        // Add a later sample (should update last_sample)
        series.add(300, 3.0, None);
        assert_eq!(series.first_timestamp, 100); // first should remain
        assert_eq!(series.last_sample.unwrap().timestamp, 300);

        // Add a middle sample (should not change boundaries)
        series.add(150, 1.5, None);
        assert_eq!(series.first_timestamp, 100);
        assert_eq!(series.last_sample.unwrap().timestamp, 300);
    }

    #[test]
    fn test_add_1000_entries() {
        let mut ts = TimeSeries::new();
        let data = DataGenerator::builder()
            .samples(1000)
            .start(0)
            .interval(Duration::from_millis(1000))
            .build()
            .generate();

        for sample in data.iter() {
            assert!(ts.add(sample.timestamp, sample.value, None).is_ok());
        }

        assert_eq!(ts.total_samples, 1000);
        assert_eq!(ts.first_timestamp, data[0].timestamp);
        assert_eq!(ts.last_timestamp(), data[data.len() - 1].timestamp);

        for (sample, orig) in ts.iter().zip(data.iter()) {
            assert_eq!(sample.timestamp, orig.timestamp);
            assert_eq!(sample.value, orig.value);
        }
    }

    fn assert_ok(result: SampleAddResult, expected: SampleAddResult) {
        match (result, expected) {
            (SampleAddResult::Ok(actual), SampleAddResult::Ok(expected)) => {
                assert_eq!(actual, expected);
            }
            _ => panic!("Unexpected result: {result:?}"),
        }
    }

    #[test]
    fn test_merge_samples_empty_timeseries() {
        let mut ts = TimeSeries::new();
        let samples = vec![
            Sample {
                timestamp: 100,
                value: 1.0,
            },
            Sample {
                timestamp: 200,
                value: 2.0,
            },
        ];

        let results = ts.merge_samples(&samples, None).unwrap();

        assert_eq!(results.len(), 2);
        assert_ok(results[0], SampleAddResult::Ok(samples[0]));
        assert_ok(results[1], SampleAddResult::Ok(samples[1]));
        assert_eq!(ts.len(), 2);
        assert_eq!(ts.first_timestamp, 100);
        assert_eq!(ts.last_timestamp(), 200);
    }

    #[test]
    fn test_merge_samples_non_empty_timeseries() {
        let mut ts = TimeSeries::new();
        ts.add(150, 1.5, None);

        let samples = vec![
            Sample {
                timestamp: 100,
                value: 1.0,
            }, // Before existing
            Sample {
                timestamp: 200,
                value: 2.0,
            }, // After existing
        ];

        let results = ts.merge_samples(&samples, None).unwrap();

        assert_eq!(results.len(), 2);
        assert_ok(results[0], SampleAddResult::Ok(samples[0]));
        assert_ok(results[1], SampleAddResult::Ok(samples[1]));
        assert_eq!(ts.len(), 3); // 1 initial + 2 merged
        assert_eq!(ts.first_timestamp, 100);
        assert_eq!(ts.last_timestamp(), 200);
        assert_eq!(
            ts.get_range(0, 300),
            vec![
                Sample {
                    timestamp: 100,
                    value: 1.0
                },
                Sample {
                    timestamp: 150,
                    value: 1.5
                },
                Sample {
                    timestamp: 200,
                    value: 2.0
                },
            ]
        );
    }

    #[test]
    fn test_merge_samples_out_of_order_input() {
        let mut ts = TimeSeries::new();
        let samples = vec![
            Sample {
                timestamp: 200,
                value: 2.0,
            },
            Sample {
                timestamp: 100,
                value: 1.0,
            },
        ];

        let results = ts.merge_samples(&samples, None).unwrap();

        assert_eq!(results.len(), 2);
        // Note: The order in results corresponds to the input order
        assert_ok(results[0], SampleAddResult::Ok(samples[0]));
        assert_ok(results[1], SampleAddResult::Ok(samples[1]));
        assert_eq!(ts.len(), 2);
        assert_eq!(ts.first_timestamp, 100);
        assert_eq!(ts.last_timestamp(), 200);
        assert_eq!(
            ts.get_range(0, 300),
            vec![
                Sample {
                    timestamp: 100,
                    value: 1.0
                },
                Sample {
                    timestamp: 200,
                    value: 2.0
                },
            ]
        );
    }

    #[test]
    fn test_merge_samples_upsert_does_not_inflate_total_samples() {
        let mut ts = TimeSeries::new();
        ts.add(100, 1.0, None);
        ts.add(200, 2.0, None);
        ts.add(300, 3.0, None);
        assert_eq!(ts.total_samples, 3);

        // One sample updates an existing timestamp (upsert), one is genuinely new.
        // The upsert must not be counted as an added sample.
        let samples = vec![
            Sample {
                timestamp: 200,
                value: 20.0,
            },
            Sample {
                timestamp: 400,
                value: 4.0,
            },
        ];
        let results = ts
            .merge_samples(&samples, Some(DuplicatePolicy::KeepLast))
            .unwrap();

        assert!(results.iter().all(|r| r.is_ok()));
        assert_eq!(ts.total_samples, 4);
        assert_eq!(
            ts.get_range(0, 500),
            vec![
                Sample {
                    timestamp: 100,
                    value: 1.0
                },
                Sample {
                    timestamp: 200,
                    value: 20.0
                },
                Sample {
                    timestamp: 300,
                    value: 3.0
                },
                Sample {
                    timestamp: 400,
                    value: 4.0
                },
            ]
        );
    }

    // #[test]
    fn test_merge_samples_spanning_multiple_chunks() {
        // Force small chunks
        let mut ts = TimeSeries::with_options(TimeSeriesOptions {
            chunk_encoding: ChunkEncoding::Uncompressed,
            chunk_size: Some(64), // Small chunk size to force splitting
            ..Default::default()
        })
        .unwrap();

        // Add initial data to create multiple chunks
        let mut samples_to_add = vec![];
        let mut len = 0;

        let data = DataGenerator::builder()
            .start(1000)
            .interval(Duration::from_millis(1000))
            .algorithm(ValueWorkload::Deriv)
            .samples(1000)
            .build()
            .generate();

        while ts.chunks.len() < 3 {
            for sample in data.iter() {
                ts.add(sample.timestamp, sample.value, None);
                if len != ts.chunks.len() {
                    samples_to_add.push(Sample::new(sample.timestamp + 500, sample.value));
                    len = ts.chunks.len();
                    break;
                }
            }
        }

        assert!(ts.chunks.len() > 1, "Test requires multiple chunks");
        let initial_len = ts.len();

        let samples_to_add = DataGenerator::builder()
            .start(1500)
            .interval(Duration::from_millis(1000))
            .algorithm(ValueWorkload::StdNorm)
            .samples(5)
            .build()
            .generate();

        let expected = samples_to_add
            .iter()
            .map(|sample| SampleAddResult::Ok(*sample))
            .collect::<Vec<_>>();

        let results = ts.merge_samples(&samples_to_add, None).unwrap();

        assert_eq!(results.len(), samples_to_add.len());
        assert_eq!(expected, results);
        assert_eq!(ts.len(), initial_len + samples_to_add.len());
    }

    #[test]
    fn test_merge_samples_older_than_retention() {
        let mut ts = TimeSeries::with_options(TimeSeriesOptions {
            retention: Some(Duration::from_millis(100)),
            ..Default::default()
        })
        .unwrap();
        ts.add(200, 2.0, None); // Sets last_timestamp to 200
        // Minimum timestamp allowed is 200 - 100 = 100

        let samples = vec![
            Sample {
                timestamp: 50,
                value: 0.5,
            }, // Too old
            Sample {
                timestamp: 150,
                value: 1.5,
            }, // Within retention
        ];

        let results = ts.merge_samples(&samples, None).unwrap();

        assert_eq!(results.len(), 2);
        assert!(matches!(results[0], SampleAddResult::TooOld));
        assert_ok(results[1], SampleAddResult::Ok(samples[1]));
        assert_eq!(ts.len(), 2); // Only the valid sample was added
        assert!(ts.get_sample(50).unwrap().is_none());
        assert!(ts.get_sample(150).unwrap().is_some());
    }

    #[test]
    fn test_merge_empty_sample_list() {
        let mut ts = TimeSeries::new();
        ts.add(100, 1.0, None);
        let initial_len = ts.len();

        let samples: Vec<Sample> = vec![];
        let results = ts.merge_samples(&samples, None).unwrap();

        assert!(results.is_empty());
        assert_eq!(ts.len(), initial_len); // No change
    }

    #[test]
    fn test_merge_samples_with_rounding() {
        let mut ts = TimeSeries::with_options(TimeSeriesOptions {
            rounding: Some(RoundingStrategy::DecimalDigits(2)), // Round to 2 decimal places
            ..Default::default()
        })
        .unwrap();

        let samples = vec![
            Sample {
                timestamp: 100,
                value: 1.234,
            },
            Sample {
                timestamp: 200,
                value: 5.678,
            },
        ];

        let results = ts.merge_samples(&samples, None).unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(ts.len(), 2);
        assert_ok(results[0], SampleAddResult::Ok(Sample::new(100, 1.23)));
        assert_ok(results[1], SampleAddResult::Ok(Sample::new(200, 5.68)));
    }

    #[test]
    fn test_samples_by_timestamps_exact_match_one_chunk() {
        // Set up a TimeSeries instance with a single chunk containing specific timestamps
        let mut time_series = TimeSeries::default();
        let mut chunk = TimeSeriesChunk::Gorilla(GorillaChunk::with_max_size(4096));
        let timestamps = vec![1000, 2000, 3000];
        for &ts in &timestamps {
            chunk
                .add_sample(&Sample {
                    timestamp: ts,
                    value: 1.0,
                })
                .unwrap();
        }
        time_series.chunks.push(chunk);

        let mut chunk = TimeSeriesChunk::Gorilla(GorillaChunk::with_max_size(4096));
        chunk
            .add_sample(&Sample {
                timestamp: 4000,
                value: 1.0,
            })
            .unwrap();
        time_series.chunks.push(chunk);

        time_series.update_state_from_chunks();

        // Define the timestamps to fetch, which match exactly one chunk
        let fetch_timestamps = vec![1000, 2000, 3000];

        let result = time_series.samples_by_timestamps(&fetch_timestamps);

        // Verify the results
        assert!(result.is_ok());
        let samples = result.unwrap();
        assert_eq!(samples.len(), fetch_timestamps.len());
        for (i, sample) in samples.iter().enumerate() {
            assert_eq!(sample.timestamp, fetch_timestamps[i]);
            assert_eq!(sample.value, 1.0);
        }
    }

    #[test]
    fn test_samples_by_timestamps_multiple_chunks() {
        // Set up a TimeSeries with multiple chunks
        let mut time_series = TimeSeries::default();

        // Assume create_chunk_with_samples is a helper function to create a chunk with given samples
        let chunk1 = create_chunk_with_samples(vec![
            Sample {
                timestamp: 100,
                value: 1.0,
            },
            Sample {
                timestamp: 200,
                value: 2.0,
            },
        ]);
        let chunk2 = create_chunk_with_samples(vec![
            Sample {
                timestamp: 300,
                value: 3.0,
            },
            Sample {
                timestamp: 400,
                value: 4.0,
            },
        ]);

        time_series.chunks.push(chunk1);
        time_series.chunks.push(chunk2);
        time_series.update_state_from_chunks();

        // Timestamps that match samples across multiple chunks
        let timestamps = vec![100, 300, 400];

        // Fetch samples by timestamps
        let result = time_series.samples_by_timestamps(&timestamps).unwrap();

        // Expected samples
        let expected_samples = vec![
            Sample {
                timestamp: 100,
                value: 1.0,
            },
            Sample {
                timestamp: 300,
                value: 3.0,
            },
            Sample {
                timestamp: 400,
                value: 4.0,
            },
        ];

        // Assert that the fetched samples match the expected samples
        assert_eq!(result, expected_samples);
    }

    #[test]
    fn test_samples_by_timestamps_no_matching_timestamps() {
        // Create a TimeSeries instance with no chunks
        let time_series = TimeSeries::default();

        // Define a set of timestamps that do not match any chunk
        let timestamps = vec![100, 200, 300];

        // Call the samples_by_timestamps method
        let result = time_series.samples_by_timestamps(&timestamps);

        // Assert that the result is an empty vector
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_samples_by_timestamps_with_duplicates_in_same_chunk() {
        // Set up a TimeSeries instance with a single chunk containing duplicate timestamps
        let mut time_series = TimeSeries::default();
        let timestamp = 1000;
        let sample1 = Sample {
            timestamp,
            value: 1.0,
        };
        let sample2 = Sample {
            timestamp,
            value: 2.0,
        };

        // Add samples to the time series
        time_series.add_sample_internal(sample1);
        time_series.add_sample_internal(sample2);

        // Request samples by timestamps, including duplicates
        let timestamps = vec![timestamp, timestamp];
        let result = time_series.samples_by_timestamps(&timestamps).unwrap();

        // Verify that the result contains both samples with the duplicate timestamp
        assert_eq!(result.len(), 2);
        assert!(result.contains(&sample1));
        assert!(result.contains(&sample2));
    }

    #[test]
    fn test_samples_by_timestamps_across_multiple_chunks() {
        // Set up a TimeSeries with multiple chunks
        let mut time_series = TimeSeries::default();

        let chunk1 = create_chunk(None);
        let chunk2 = create_chunk(None);
        let chunk3 = create_chunk(None);

        // Add chunks to the time series
        time_series.chunks.push(chunk1);
        time_series.chunks.push(chunk2);
        time_series.chunks.push(chunk3);

        // Add samples to each chunk
        time_series.chunks[0]
            .add_sample(&Sample {
                timestamp: 1,
                value: 10.0,
            })
            .unwrap();
        time_series.chunks[1]
            .add_sample(&Sample {
                timestamp: 2,
                value: 20.0,
            })
            .unwrap();
        time_series.chunks[2]
            .add_sample(&Sample {
                timestamp: 3,
                value: 30.0,
            })
            .unwrap();
        time_series.total_samples = 3;

        // Define timestamps to fetch
        let timestamps = vec![1, 2, 3];

        // Fetch samples by timestamps
        let result = time_series.samples_by_timestamps(&timestamps);

        // Verify the result
        assert!(result.is_ok());
        let samples = result.unwrap();
        assert_eq!(samples.len(), 3);
        assert_eq!(samples[0].timestamp, 1);
        assert_eq!(samples[0].value, 10.0);
        assert_eq!(samples[1].timestamp, 2);
        assert_eq!(samples[1].value, 20.0);
        assert_eq!(samples[2].timestamp, 3);
        assert_eq!(samples[2].value, 30.0);
    }

    #[test]
    fn test_trim_on_empty_timeseries() {
        let mut timeseries = TimeSeries::new();
        let result = timeseries.trim();
        assert_eq!(result.unwrap(), 0);
        assert_eq!(timeseries.total_samples, 0);
        assert!(timeseries.chunks.is_empty());
    }

    #[test]
    fn test_trim_all_chunks_before_min_timestamp() {
        let mut time_series = TimeSeries::new();
        let mut chunk1 = create_chunk(None);
        let mut chunk2 = create_chunk(None);

        // Add samples to chunks such that they are all before the min_timestamp
        let sample1 = Sample {
            timestamp: 10,
            value: 1.0,
        };
        let sample2 = Sample {
            timestamp: 20,
            value: 2.0,
        };
        let sample3 = Sample {
            timestamp: 60,
            value: 4.0,
        };
        chunk1.add_sample(&sample1).unwrap();
        chunk2.add_sample(&sample2).unwrap();
        chunk2.add_sample(&sample3).unwrap();

        time_series.chunks.push(chunk1);
        time_series.chunks.push(chunk2);

        time_series.update_state_from_chunks();

        // Set retention so that min_timestamp is greater than any sample timestamp
        time_series.retention = Duration::from_millis(30);

        // Perform the trim operation
        let deleted_count = time_series.trim().unwrap();

        // Check that all chunks are removed
        assert_eq!(deleted_count, 2);
        assert_eq!(time_series.chunks.len(), 1);
        assert_eq!(time_series.total_samples, 1);
        assert_eq!(time_series.first_timestamp, 60);
        assert_eq!(time_series.last_sample, Some(sample3));
    }

    #[test]
    fn test_trim_partial_chunks() {
        // Set up a TimeSeries with chunks such that some are before the min_timestamp
        let mut time_series = TimeSeries::default();

        // Assume we have a helper function to create a chunk with given timestamps
        let chunk1 = create_chunk_with_timestamps(0, 10); // Entirely before min_timestamp
        let chunk2 = create_chunk_with_timestamps(14, 18); // Partially before min_timestamp
        let chunk3 = create_chunk_with_timestamps(20, 30); // After min_timestamp

        let chunk1_len = chunk1.len();

        time_series.chunks.push(chunk1);
        time_series.chunks.push(chunk2);
        time_series.chunks.push(chunk3);
        time_series.update_state_from_chunks();

        // Set a retention period such that min_timestamp is 15
        time_series.retention = Duration::from_millis(15);
        time_series.last_sample = Some(Sample {
            timestamp: 30,
            value: 0.0,
        });

        // Call trim and check the results
        let deleted_count = time_series.trim().expect("Trim should succeed");

        // min_timestamp = last(30) - retention(15) = 15. Retention keeps samples
        // with timestamp >= 15, so only 14 is dropped from chunk2 (15 is kept).
        assert_eq!(deleted_count, chunk1_len + 1); // all from chunk1 and just 14 from chunk2
        assert_eq!(time_series.chunks.len(), 2); // Only chunk2 and chunk3 should remain
        assert_eq!(time_series.chunks[0].first_timestamp(), 15); // chunk2 trimmed to the window edge
        assert_eq!(time_series.chunks[1].first_timestamp(), 20); // chunk3 remains unchanged
    }

    #[test]
    fn test_trim_no_chunks_before_min_timestamp() {
        let mut time_series = TimeSeries::new();

        // Create a chunk with timestamps starting from 1000
        let mut chunk = create_chunk(None);
        chunk
            .add_sample(&Sample {
                timestamp: 1000,
                value: 1.0,
            })
            .unwrap();
        chunk
            .add_sample(&Sample {
                timestamp: 1001,
                value: 2.0,
            })
            .unwrap();
        time_series.chunks.push(chunk);

        time_series.update_state_from_chunks();

        // Call trim and assert that no samples are deleted
        let deleted_count = time_series.trim().unwrap();
        assert_eq!(deleted_count, 0);
        assert_eq!(time_series.total_samples, 2);
        assert_eq!(time_series.chunks.len(), 1);
    }

    #[test]
    fn test_trim_partial_overlap_with_min_timestamp() {
        let mut time_series = TimeSeries::new();

        // Create a chunk that is completely before min_timestamp
        let mut chunk = create_chunk(None);
        chunk
            .add_sample(&Sample {
                timestamp: 50,
                value: 0.5,
            })
            .unwrap();
        time_series.chunks.push(chunk);

        // Create a chunk that partially overlaps with min_timestamp
        let mut chunk1 = create_chunk(None);
        chunk1
            .add_sample(&Sample {
                timestamp: 100,
                value: 1.0,
            })
            .unwrap();
        chunk1
            .add_sample(&Sample {
                timestamp: 200,
                value: 2.0,
            })
            .unwrap();
        time_series.chunks.push(chunk1);

        // Set retention to ensure min_timestamp is 150
        time_series.retention = Duration::from_millis(50);

        time_series.update_state_from_chunks();

        // Perform trim operation
        let deleted_count = time_series.trim().unwrap();

        // Verify the results
        assert_eq!(deleted_count, 2); // Only one sample should be deleted from chunk1
        assert_eq!(time_series.total_samples, 1); // Total samples should reflect the deletion
        assert_eq!(time_series.chunks.len(), 1); // Only one chunk should remain
        assert_eq!(time_series.chunks[0].first_timestamp(), 200); // First timestamp should be updated
    }

    #[test]
    fn test_remove_range_partial_overlap_multiple_chunks() {
        // Set up a TimeSeries with multiple chunks
        let mut time_series = TimeSeries::default();

        // Create and add samples to the time series
        for i in 1..15 {
            time_series.add(i as Timestamp, i as f64, None);
        }

        // Remove a range that partially overlaps multiple chunks
        let start_ts = 3;
        let end_ts = 11;
        let removed_samples = time_series.remove_range(start_ts, end_ts).unwrap();

        // Verify the correct number of samples were removed
        assert_eq!(removed_samples, 9);

        // Verify the remaining samples are correct
        let remaining_samples: Vec<_> = time_series.iter().collect();
        let expected_samples = vec![
            Sample {
                timestamp: 1,
                value: 1.0,
            },
            Sample {
                timestamp: 2,
                value: 2.0,
            },
            Sample {
                timestamp: 12,
                value: 12.0,
            },
            Sample {
                timestamp: 13,
                value: 13.0,
            },
            Sample {
                timestamp: 14,
                value: 14.0,
            },
        ];
        assert_eq!(remaining_samples, expected_samples);
    }

    #[test]
    fn test_remove_range_no_overlap() {
        // Arrange
        let mut time_series = TimeSeries::default();
        let sample1 = Sample {
            timestamp: 1000,
            value: 1.0,
        };
        let sample2 = Sample {
            timestamp: 2000,
            value: 2.0,
        };
        let sample3 = Sample {
            timestamp: 3000,
            value: 3.0,
        };

        time_series.add(sample1.timestamp, sample1.value, None);
        time_series.add(sample2.timestamp, sample2.value, None);
        time_series.add(sample3.timestamp, sample3.value, None);

        // Act
        let deleted_samples = time_series.remove_range(4000, 5000).unwrap();

        // Assert
        assert_eq!(deleted_samples, 0);
        assert_eq!(time_series.total_samples, 3);
        assert_eq!(time_series.first_timestamp, 1000);
        assert_eq!(time_series.last_timestamp(), 3000);
    }

    #[test]
    fn test_remove_range_updates_total_samples_correctly() {
        // Set up a TimeSeries with multiple chunks and samples
        let mut time_series = TimeSeries::default();
        let sample1 = Sample {
            timestamp: 1,
            value: 10.0,
        };
        let sample2 = Sample {
            timestamp: 2,
            value: 20.0,
        };
        let sample3 = Sample {
            timestamp: 3,
            value: 30.0,
        };
        let sample4 = Sample {
            timestamp: 4,
            value: 40.0,
        };
        let sample5 = Sample {
            timestamp: 5,
            value: 50.0,
        };

        // Add samples to the time series
        time_series.add(sample1.timestamp, sample1.value, None);
        time_series.add(sample2.timestamp, sample2.value, None);
        time_series.add(sample3.timestamp, sample3.value, None);
        time_series.add(sample4.timestamp, sample4.value, None);
        time_series.add(sample5.timestamp, sample5.value, None);

        // Ensure total_samples is correct before removal
        assert_eq!(time_series.total_samples, 5);

        // Remove a range of samples
        let removed_samples = time_series.remove_range(2, 4).unwrap();

        // Verify the number of samples removed
        assert_eq!(removed_samples, 3);

        // Verify total_samples is updated correctly
        assert_eq!(time_series.total_samples, 2);

        // Verify remaining samples are correct
        let remaining_samples = time_series.get_range(1, 5);
        assert_eq!(remaining_samples.len(), 2);
        assert_eq!(remaining_samples[0].timestamp, 1);
        assert_eq!(remaining_samples[1].timestamp, 5);
    }

    #[test]
    fn test_remove_range_exactly_matches_chunk_boundaries() {
        // Set up a TimeSeries with multiple chunks
        let mut time_series = TimeSeries {
            // Assume each chunk can hold 2 samples for simplicity
            chunk_size_bytes: 2 * size_of::<Sample>(),
            ..TimeSeries::default()
        };

        // Add samples to create multiple chunks
        let samples = vec![
            Sample {
                timestamp: 1,
                value: 10.0,
            },
            Sample {
                timestamp: 2,
                value: 20.0,
            },
            Sample {
                timestamp: 3,
                value: 30.0,
            },
            Sample {
                timestamp: 4,
                value: 40.0,
            },
        ];

        for sample in samples {
            time_series.add(sample.timestamp, sample.value, None);
        }

        // Verify initial state
        assert_eq!(time_series.chunks.len(), 1);
        assert_eq!(time_series.total_samples, 4);

        // Remove range that exactly matches the boundaries of the chunks
        let removed_samples = time_series.remove_range(1, 4).unwrap();

        // Verify that all samples are removed
        assert_eq!(removed_samples, 4);
        assert_eq!(time_series.total_samples, 0);
        assert!(time_series.chunks.is_empty());
    }

    #[test]
    fn test_remove_range_updates_first_last_timestamps() {
        let mut time_series = TimeSeries::default();
        let samples = vec![
            Sample {
                timestamp: 1,
                value: 10.0,
            },
            Sample {
                timestamp: 2,
                value: 20.0,
            },
            Sample {
                timestamp: 3,
                value: 30.0,
            },
            Sample {
                timestamp: 4,
                value: 40.0,
            },
        ];

        for sample in &samples {
            time_series.add(sample.timestamp, sample.value, None);
        }

        // Remove samples with timestamps 2 and 3
        let removed_count = time_series
            .remove_range(2, 3)
            .expect("Failed to remove range");
        assert_eq!(removed_count, 2);

        // Check if first and last timestamps are updated correctly
        assert_eq!(time_series.first_timestamp, 1);
        assert_eq!(time_series.last_timestamp(), 4);
    }

    // get_range()
    #[test]
    fn test_get_range_returns_empty_when_end_time_less_than_min_timestamp() {
        let mut time_series = TimeSeries::default();

        // Add some samples to the time series
        let sample1 = Sample {
            timestamp: 100,
            value: 1.0,
        };
        let sample2 = Sample {
            timestamp: 200,
            value: 2.0,
        };
        time_series.add(sample1.timestamp, sample1.value, None);
        time_series.add(sample2.timestamp, sample2.value, None);

        // Define a range where end_time is less than the minimum timestamp in the series
        let start_time = 50;
        let end_time = 75;

        // Call get_range and assert that it returns an empty vector
        let result = time_series.get_range(start_time, end_time);
        assert!(
            result.is_empty(),
            "Expected an empty vector, got {result:?}"
        );
    }

    #[test]
    fn test_get_range_with_matching_start_and_end_time() {
        let mut time_series = TimeSeries::default();

        // Assuming Sample and Timestamp are properly defined and Sample has a constructor
        let timestamp = 1000; // Example timestamp
        let sample = Sample {
            value: 42.0,
            timestamp,
        };

        // Add a sample to the time series
        time_series.add(timestamp, sample.value, None);

        // Call get_range with start_time and end_time being the same and matching the sample timestamp
        let result = time_series.get_range(timestamp, timestamp);

        // Assert that the result contains exactly one sample with the expected timestamp and value
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].timestamp, timestamp);
        assert_eq!(result[0].value, sample.value);
    }

    #[test]
    fn test_get_range_across_multiple_chunks() {
        let mut time_series = TimeSeries::default();

        let start_time = 1000;
        let mut count: usize = 0;
        for _ in 0..4 {
            let mut chunk = create_chunk(None);
            for _ in 0..5 {
                count += 1;
                let timestamp = start_time + (count * 1000) as Timestamp;
                chunk
                    .add_sample(&Sample {
                        timestamp,
                        value: count as f64,
                    })
                    .unwrap();
            }
            time_series.chunks.push(chunk);
        }

        time_series.update_state_from_chunks();

        // Define the range that spans across multiple chunks
        let start_time = 2000;
        let end_time = 14000;

        // Get the range of samples
        let result = time_series.get_range(start_time, end_time);

        // Expected samples in the range
        let expected_samples = vec![
            Sample {
                timestamp: 2000,
                value: 1.0,
            },
            Sample {
                timestamp: 3000,
                value: 2.0,
            },
            Sample {
                timestamp: 4000,
                value: 3.0,
            },
            Sample {
                timestamp: 5000,
                value: 4.0,
            },
            Sample {
                timestamp: 6000,
                value: 5.0,
            },
            Sample {
                timestamp: 7000,
                value: 6.0,
            },
            Sample {
                timestamp: 8000,
                value: 7.0,
            },
            Sample {
                timestamp: 9000,
                value: 8.0,
            },
            Sample {
                timestamp: 10000,
                value: 9.0,
            },
            Sample {
                timestamp: 11000,
                value: 10.0,
            },
            Sample {
                timestamp: 12000,
                value: 11.0,
            },
            Sample {
                timestamp: 13000,
                value: 12.0,
            },
            Sample {
                timestamp: 14000,
                value: 13.0,
            },
        ];

        assert_eq!(result, expected_samples);
    }

    #[test]
    fn test_get_range_filtered_empty_no_filters_no_samples() {
        let ts = TimeSeries::new();
        let start_timestamp = 100;
        let end_timestamp = 200;

        let result = ts.get_range_filtered(start_timestamp, end_timestamp, None, None);
        assert!(result.is_empty());
    }

    #[test]
    fn test_get_range_filtered_with_timestamp_filter() {
        let mut ts = TimeSeries::new();
        let data = DataGenerator::builder()
            .start(1000)
            .interval(Duration::from_millis(1000))
            .algorithm(ValueWorkload::Deriv)
            .samples(10)
            .build()
            .generate();

        for sample in data.iter() {
            assert!(ts.add(sample.timestamp, sample.value, None).is_ok());
        }

        let timestamp_filter = vec![2000, 4000, 6000, 8000];
        let value_filter: Option<ValueFilter> = None;

        let filtered_samples =
            ts.get_range_filtered(1000, 10000, Some(&timestamp_filter), value_filter);

        assert_eq!(filtered_samples.len(), 4);
        assert_eq!(filtered_samples[0].timestamp, 2000);
        assert_eq!(filtered_samples[1].timestamp, 4000);
        assert_eq!(filtered_samples[2].timestamp, 6000);
        assert_eq!(filtered_samples[3].timestamp, 8000);
    }

    #[test]
    fn test_get_range_filtered_with_value_filter_only() {
        let mut ts = TimeSeries::new();
        let data = vec![
            Sample {
                timestamp: 100,
                value: 10.0,
            },
            Sample {
                timestamp: 200,
                value: 20.0,
            },
            Sample {
                timestamp: 300,
                value: 30.0,
            },
        ];

        for sample in &data {
            assert!(ts.add(sample.timestamp, sample.value, None).is_ok());
        }

        let value_filter = ValueFilter::new(15.0, 25.0).unwrap(); // Assuming ValueFilter has a constructor like this
        let filtered_samples = ts.get_range_filtered(0, 400, None, Some(value_filter));

        assert_eq!(filtered_samples.len(), 1);
        assert_eq!(filtered_samples[0].timestamp, 200);
        assert_eq!(filtered_samples[0].value, 20.0);
    }

    #[test]
    fn test_get_range_filtered_with_empty_timestamp_filter() {
        let mut ts = TimeSeries::new();
        let start_timestamp = 1000;
        let end_timestamp = 5000;

        let data = DataGenerator::builder()
            .start(1000)
            .interval(Duration::from_millis(1000))
            .algorithm(ValueWorkload::StdNorm)
            .samples(5)
            .build()
            .generate();

        for sample in data.iter() {
            assert!(ts.add(sample.timestamp, sample.value, None).is_ok());
        }

        let timestamp_filter: Vec<Timestamp> = vec![];
        let value_filter: Option<ValueFilter> = None;

        let result = ts.get_range_filtered(
            start_timestamp,
            end_timestamp,
            Some(&timestamp_filter),
            value_filter,
        );

        assert_eq!(result, vec![]);
    }

    #[test]
    fn test_get_range_filtered_with_value_filter_no_results() {
        let mut ts = TimeSeries::new();
        let data = DataGenerator::builder()
            .start(1000)
            .interval(Duration::from_millis(1000))
            .algorithm(ValueWorkload::StdNorm)
            .samples(10)
            .build()
            .generate();

        for sample in data.iter() {
            assert!(ts.add(sample.timestamp, sample.value, None).is_ok());
        }

        // Define a value filter that will filter out all samples
        let value_filter = Some(ValueFilter::new(20000f64, 50000f64).unwrap());

        // Call get_range_filtered with a value filter that results in no samples
        let filtered_samples = ts.get_range_filtered(1000, 10000, None, value_filter);

        // Assert that no samples are returned
        assert!(filtered_samples.is_empty());
    }

    #[test]
    fn test_get_range_filtered_with_both_filters() {
        let mut ts = TimeSeries::new();
        let samples = vec![
            Sample {
                timestamp: 100,
                value: 1.0,
            },
            Sample {
                timestamp: 200,
                value: 2.0,
            },
            Sample {
                timestamp: 300,
                value: 3.0,
            },
            Sample {
                timestamp: 400,
                value: 4.0,
            },
            Sample {
                timestamp: 500,
                value: 5.0,
            },
        ];

        for sample in &samples {
            assert!(ts.add(sample.timestamp, sample.value, None).is_ok());
        }

        let timestamp_filter = vec![200, 300, 400];
        let value_filter = Some(ValueFilter { min: 2.5, max: 4.5 });

        let filtered_samples =
            ts.get_range_filtered(100, 500, Some(&timestamp_filter), value_filter);

        assert_eq!(filtered_samples.len(), 2);
        assert_eq!(filtered_samples[0].timestamp, 300);
        assert_eq!(filtered_samples[0].value, 3.0);
        assert_eq!(filtered_samples[1].timestamp, 400);
        assert_eq!(filtered_samples[1].value, 4.0);
    }

    #[test]
    fn test_get_range_filtered_with_out_of_range_timestamps() {
        let mut ts = TimeSeries::new();
        // Add some samples to the time series
        for i in 0..10 {
            assert!(ts.add(i as i64 * 10, i as f64, None).is_ok());
        }

        // Define a timestamp filter with some timestamps outside the range
        let timestamp_filter = vec![5, 15, 20, 25, 30, 35, 40, 55, 65, 70, 85, 95, 105, 115];

        // Get filtered range
        let filtered_samples = ts.get_range_filtered(20, 80, Some(&timestamp_filter), None);

        // Check that only the samples within the range 20 to 80 are returned
        assert_eq!(filtered_samples.len(), 4);
        assert_eq!(filtered_samples[0].timestamp, 20);
        assert_eq!(filtered_samples[1].timestamp, 30);
        assert_eq!(filtered_samples[2].timestamp, 40);
        assert_eq!(filtered_samples[3].timestamp, 70);
    }

    #[test]
    fn test_get_range_filtered_no_filter() {
        let mut ts = TimeSeries::new();

        let data = DataGenerator::builder()
            .start(1000)
            .interval(Duration::from_millis(1000))
            .algorithm(ValueWorkload::StdNorm)
            .samples(10)
            .build()
            .generate();

        for sample in data.iter() {
            assert!(ts.add(sample.timestamp, sample.value, None).is_ok());
        }

        let start_timestamp = data.first().unwrap().timestamp;
        let end_timestamp = data.last().unwrap().timestamp;

        let result = ts.get_range_filtered(start_timestamp, end_timestamp, None, None);

        assert_eq!(result.len(), data.len());

        for (result_sample, original_sample) in result.iter().zip(data.iter()) {
            assert_eq!(result_sample.timestamp, original_sample.timestamp);
            assert_eq!(result_sample.value, original_sample.value);
        }
    }

    #[test]
    fn test_increment_sample_value_with_existing_sample() {
        let mut ts = TimeSeries::new();
        ts.add(100, 200.0, None);

        let result = ts.increment_sample_value(Some(100), 50.0);
        assert!(result.is_ok());
        let sample = ts.get_sample(100).unwrap().unwrap();
        assert_eq!(sample.value, 250.0);
    }

    #[test]
    fn test_increment_sample_value_with_new_sample() {
        let mut ts = TimeSeries::new();

        let result = ts.increment_sample_value(Some(100), 50.0);
        assert!(result.is_ok());
        let sample = ts.get_sample(100).unwrap().unwrap();
        assert_eq!(sample.value, 50.0);
    }

    #[test]
    fn test_increment_sample_value_with_no_timestamp() {
        let mut ts = TimeSeries::new();
        ts.add(100, 200.0, None);

        let result = ts.increment_sample_value(None, 50.0);
        assert!(result.is_ok());
        let sample = ts.get_sample(100).unwrap().unwrap();
        assert_eq!(sample.value, 250.0);
    }

    #[test]
    fn test_increment_sample_value_with_lower_timestamp() {
        let mut ts = TimeSeries::new();
        ts.add(100, 200.0, None);

        let result = ts.increment_sample_value(Some(50), 50.0);
        assert!(result.is_err());
    }

    #[test]
    fn test_increment_sample_value_with_existing_sample_and_nan_increment() {
        let mut ts = TimeSeries::new();
        ts.add(100, 200.0, None);

        let result = ts.increment_sample_value(Some(100), f64::NAN);
        assert!(result.is_err());
    }

    #[test]
    fn test_increment_sample_value_on_nan_raises_error() {
        let mut ts = TimeSeries::new();

        let result = ts.increment_sample_value(Some(100), f64::NAN);
        assert!(result.is_err());

        let result = ts.increment_sample_value(Some(100), 50.0);
        assert!(result.is_ok());
    }
}
