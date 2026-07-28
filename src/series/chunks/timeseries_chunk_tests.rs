#[cfg(test)]
#[allow(clippy::excessive_precision)] // TODO: Audit test values for truncation
mod tests {
    use crate::common::Sample;
    use crate::config::SPLIT_FACTOR;
    use crate::error::TsdbError;
    use crate::series::chunks::merge::merge_by_capacity;
    use crate::series::{
        DuplicatePolicy, SampleAddResult,
        chunks::{Chunk, ChunkEncoding, ChunkOps, TimeSeriesChunk},
    };
    use crate::tests::chunk_utils::encoded_size;
    use crate::tests::generators::{DataGenerator, ValueWorkload};
    use std::time::Duration;

    /// A test helper function for asserting floating point numbers are within the
    /// machine epsilon because strict comparison of floating point numbers is
    /// incorrect
    pub fn approximately_equal(f1: f64, f2: f64) -> bool {
        (f1 - f2).abs() < f64::EPSILON
    }

    const CHUNK_TYPES: [ChunkEncoding; 3] = [
        ChunkEncoding::Uncompressed,
        ChunkEncoding::Gorilla,
        ChunkEncoding::Chimp,
    ];

    fn generate_random_samples(count: usize) -> Vec<Sample> {
        DataGenerator::builder()
            .samples(count)
            .start(1000)
            .algorithm(ValueWorkload::StdNorm)
            .interval(Duration::from_millis(1000))
            .build()
            .generate()
    }

    #[test]
    fn test_clear_chunk_with_multiple_samples() {
        let samples = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 20,
                value: 2.0,
            },
            Sample {
                timestamp: 30,
                value: 3.0,
            },
            Sample {
                timestamp: 40,
                value: 4.0,
            },
        ];

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 400);
            chunk.set_data(&samples).unwrap();

            assert_eq!(chunk.len(), 4);

            chunk.clear();

            assert_eq!(chunk.len(), 0);
            assert_eq!(chunk.get_range(0, 100).unwrap(), vec![]);
        }
    }

    #[test]
    fn test_get_range_empty_chunk() {
        for chunk_type in CHUNK_TYPES {
            let chunk = TimeSeriesChunk::new(chunk_type, 100);

            assert!(chunk.is_empty());

            let result = chunk.get_range(0, 100).unwrap();

            assert!(result.is_empty());
        }
    }

    #[test]
    fn test_get_range_single_sample() {
        let sample = Sample {
            timestamp: 10,
            value: 1.0,
        };

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 100);
            chunk.add_sample(&sample).unwrap();

            assert_eq!(chunk.len(), 1);

            let result = chunk.get_range(0, 20).unwrap();
            assert_eq!(
                result.len(),
                1,
                "{}: get_range_single_sample - expected 1 sample, got {}",
                chunk_type,
                result.len()
            );
            assert_eq!(result[0], sample);

            let empty_result = chunk.get_range(20, 30).unwrap();
            assert!(empty_result.is_empty());
        }
    }

    #[test]
    fn test_get_range_start_equals_end() {
        let samples = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 20,
                value: 2.0,
            },
            Sample {
                timestamp: 30,
                value: 3.0,
            },
        ];

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 100);
            chunk.set_data(&samples).unwrap();

            assert_eq!(chunk.len(), 3);

            let result = chunk.get_range(20, 20).unwrap();
            assert_eq!(
                result.len(),
                1,
                "{}: Expected 1 result, got {}",
                chunk_type,
                result.len()
            );
            assert_eq!(
                result[0],
                Sample {
                    timestamp: 20,
                    value: 2.0
                }
            );

            let empty_result = chunk.get_range(15, 15).unwrap();
            assert!(
                empty_result.is_empty(),
                "{chunk_type}: Expected empty result, got {empty_result:?}"
            );
        }
    }

    #[test]
    fn test_get_range_full_range() {
        let samples = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 20,
                value: 2.0,
            },
            Sample {
                timestamp: 30,
                value: 3.0,
            },
        ];

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 100);
            chunk.set_data(&samples).unwrap();

            assert_eq!(
                chunk.len(),
                3,
                "{}: Expected 3 results, got {}",
                chunk_type,
                chunk.len()
            );

            let result = chunk.get_range(0, 40).unwrap();
            assert_eq!(
                result.len(),
                3,
                "{}: Expected 3 results, got {}",
                chunk_type,
                result.len()
            );
            assert_eq!(result, samples);
        }
    }

    #[test]
    fn test_get_range_between_samples() {
        let samples = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 20,
                value: 2.0,
            },
            Sample {
                timestamp: 30,
                value: 3.0,
            },
            Sample {
                timestamp: 40,
                value: 4.0,
            },
        ];

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 400);
            chunk.set_data(&samples).unwrap();

            assert_eq!(chunk.len(), 4);

            let result = chunk.get_range(15, 35).unwrap();
            assert_eq!(
                result.len(),
                2,
                "{}: Expected 2 results, got {}",
                chunk_type,
                result.len()
            );
            assert_eq!(
                result[0],
                Sample {
                    timestamp: 20,
                    value: 2.0
                }
            );
            assert_eq!(
                result[1],
                Sample {
                    timestamp: 30,
                    value: 3.0
                }
            );
        }
    }

    #[test]
    fn test_remove_range_chunk() {
        let samples = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 20,
                value: 2.0,
            },
            Sample {
                timestamp: 30,
                value: 3.0,
            },
            Sample {
                timestamp: 40,
                value: 4.0,
            },
        ];

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 400);
            chunk.set_data(&samples).unwrap();

            assert_eq!(chunk.len(), 4);
            chunk.remove_range(20, 30).unwrap();

            assert_eq!(chunk.len(), 2);

            let _ = chunk.remove_range(20, 30).unwrap();
            let current = chunk.get_range(0, 100).unwrap();
            let expected = vec![
                Sample {
                    timestamp: 10,
                    value: 1.0,
                },
                Sample {
                    timestamp: 40,
                    value: 4.0,
                },
            ];

            assert_eq!(
                current, expected,
                "{chunk_type}: Expected range {expected:?} after removing [20, 30], got {current:?}"
            );
        }
    }

    #[test]
    fn test_remove_range_single_sample() {
        let sample = Sample {
            timestamp: 10,
            value: 1.0,
        };

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 100);

            chunk.add_sample(&sample).unwrap();

            assert_eq!(chunk.len(), 1);

            chunk.remove_range(10, 20).unwrap();

            assert_eq!(
                chunk.len(),
                0,
                "{}: Expected 0 results, got {}",
                chunk_type,
                chunk.len()
            );
            assert_eq!(chunk.get_range(0, 100).unwrap(), vec![]);
        }
    }

    #[test]
    fn test_remove_range_same_timestamp() {
        let samples = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 20,
                value: 2.0,
            },
            Sample {
                timestamp: 30,
                value: 3.0,
            },
        ];

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 100);
            chunk.set_data(&samples).unwrap();

            assert_eq!(chunk.len(), 3);

            let removed = chunk.remove_range(10, 10).unwrap();

            assert_eq!(removed, 1, "{chunk_type}: Expected 1 result, got {removed}");
            assert_eq!(
                chunk.get_range(0, 100).unwrap(),
                vec![
                    Sample {
                        timestamp: 20,
                        value: 2.0
                    },
                    Sample {
                        timestamp: 30,
                        value: 3.0
                    }
                ]
            );
        }
    }

    #[test]
    fn test_merge_samples_duplicate_policy_keep_first() {
        let samples = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 20,
                value: 2.0,
            },
            Sample {
                timestamp: 20,
                value: 3.0,
            },
            Sample {
                timestamp: 30,
                value: 4.0,
            },
        ];

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 100);
            chunk
                .set_data(&[Sample {
                    timestamp: 20,
                    value: 5.0,
                }])
                .unwrap();

            let result = chunk
                .merge_samples(&samples, Some(DuplicatePolicy::KeepFirst))
                .unwrap();

            assert_eq!(result.len(), 3);
            assert_eq!(
                chunk.len(),
                4,
                "{chunk_type}: Expected 3 results, got {}",
                chunk.len()
            );

            let expected = vec![
                Sample {
                    timestamp: 10,
                    value: 1.0,
                },
                Sample {
                    timestamp: 20,
                    value: 5.0,
                },
                Sample {
                    timestamp: 20,
                    value: 3.0,
                },
                Sample {
                    timestamp: 30,
                    value: 4.0,
                },
            ];

            assert_eq!(chunk.get_range(0, 100).unwrap(), expected,);
        }
    }

    #[test]
    fn test_merge_samples_duplicate_policy_keep_last() {
        let samples = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 20,
                value: 2.0,
            },
            Sample {
                timestamp: 25,
                value: 2.5,
            },
            Sample {
                timestamp: 30,
                value: 4.0,
            },
        ];

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 100);
            chunk
                .set_data(&[Sample {
                    timestamp: 20,
                    value: 5.0,
                }])
                .unwrap();

            let result = chunk
                .merge_samples(&samples, Some(DuplicatePolicy::KeepLast))
                .unwrap();

            assert_eq!(
                result.len(),
                4,
                "{chunk_type}: Expected 3 results. Found {}",
                result.len()
            );

            assert_eq!(
                chunk.len(),
                4,
                "{chunk_type}: Expected chunk len of 3. Found {}",
                result.len()
            );
            assert_eq!(
                chunk.get_range(0, 100).unwrap(),
                vec![
                    Sample {
                        timestamp: 10,
                        value: 1.0
                    },
                    Sample {
                        timestamp: 20,
                        value: 2.0
                    },
                    Sample {
                        timestamp: 25,
                        value: 2.5,
                    },
                    Sample {
                        timestamp: 30,
                        value: 4.0
                    },
                ]
            );
            // assert_eq!(blocked.len(), 1);
        }
    }

    #[test]
    fn test_merge_samples_duplicate_policy_sum() {
        let samples = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 15,
                value: 1.5,
            },
            Sample {
                timestamp: 20,
                value: 3.0,
            },
            Sample {
                timestamp: 30,
                value: 4.0,
            },
        ];

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 100);
            chunk
                .add_sample(&Sample {
                    timestamp: 20,
                    value: 5.0,
                })
                .unwrap();

            let result = chunk
                .merge_samples(&samples, Some(DuplicatePolicy::Sum))
                .unwrap();

            assert_eq!(result.len(), samples.len());
            assert_eq!(
                chunk.len(),
                4,
                "{}: Expected 3 results, got {}",
                chunk_type,
                chunk.len()
            );

            let merged_samples = chunk.get_range(0, 40).unwrap();
            assert_eq!(
                merged_samples,
                vec![
                    Sample {
                        timestamp: 10,
                        value: 1.0
                    },
                    Sample {
                        timestamp: 15,
                        value: 1.5,
                    },
                    Sample {
                        timestamp: 20,
                        value: 8.0
                    },
                    Sample {
                        timestamp: 30,
                        value: 4.0
                    },
                ]
            );
        }
    }

    #[test]
    fn test_merge_samples_duplicate_policy_min() {
        let samples = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 15,
                value: 4.5,
            },
            Sample {
                timestamp: 20,
                value: 3.0,
            },
            Sample {
                timestamp: 30,
                value: 4.0,
            },
        ];

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 100);
            chunk
                .add_sample(&Sample {
                    timestamp: 15,
                    value: 15.0,
                })
                .unwrap();

            let result = chunk
                .merge_samples(&samples, Some(DuplicatePolicy::Min))
                .unwrap();

            assert_eq!(result.len(), samples.len());
            assert_eq!(
                chunk.len(),
                4,
                "{}: Expected 3 results, got {}",
                chunk_type,
                chunk.len()
            );

            let merged_samples = chunk.get_range(0, 40).unwrap();
            assert_eq!(
                merged_samples,
                vec![
                    Sample {
                        timestamp: 10,
                        value: 1.0
                    },
                    Sample {
                        timestamp: 15,
                        value: 4.5,
                    },
                    Sample {
                        timestamp: 20,
                        value: 3.0
                    },
                    Sample {
                        timestamp: 30,
                        value: 4.0
                    },
                ]
            );
        }
    }

    #[test]
    fn test_merge_samples_duplicate_policy_max() {
        let samples = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 15,
                value: 4.5,
            },
            Sample {
                timestamp: 20,
                value: 3.0,
            },
            Sample {
                timestamp: 30,
                value: 4.0,
            },
        ];

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 100);
            chunk
                .add_sample(&Sample {
                    timestamp: 15,
                    value: 16.5,
                })
                .unwrap();

            let result = chunk
                .merge_samples(&samples, Some(DuplicatePolicy::Max))
                .unwrap();

            assert_eq!(result.len(), samples.len());
            assert_eq!(
                chunk.len(),
                4,
                "{}: Expected 3 results, got {}",
                chunk_type,
                chunk.len()
            );

            let merged_samples = chunk.get_range(0, 40).unwrap();
            assert_eq!(
                merged_samples,
                vec![
                    Sample {
                        timestamp: 10,
                        value: 1.0
                    },
                    Sample {
                        timestamp: 15,
                        value: 16.5,
                    },
                    Sample {
                        timestamp: 20,
                        value: 3.0
                    },
                    Sample {
                        timestamp: 30,
                        value: 4.0
                    },
                ]
            );
        }
    }

    #[test]
    fn test_merge_samples_duplicate_policy_block() {
        let samples = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 20,
                value: 2.0,
            },
            Sample {
                timestamp: 30,
                value: 3.0,
            },
        ];

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 100);

            chunk
                .set_data(&[
                    Sample {
                        timestamp: 20,
                        value: 5.0,
                    },
                    Sample {
                        timestamp: 50,
                        value: 5.0,
                    },
                ])
                .unwrap();

            // The first merge should add all samples
            let result = chunk
                .merge_samples(&samples, Some(DuplicatePolicy::Block))
                .unwrap();

            assert_eq!(
                result.len(),
                samples.len(),
                "{chunk_type}: Expected {} samples to be merged",
                samples.len()
            );

            let second = result[1];
            assert_eq!(second, SampleAddResult::Duplicate);

            let expected = vec![
                Sample {
                    timestamp: 10,
                    value: 1.0,
                },
                Sample {
                    timestamp: 20,
                    value: 5.0,
                },
                Sample {
                    timestamp: 30,
                    value: 3.0,
                },
                Sample {
                    timestamp: 50,
                    value: 5.0,
                },
            ];

            let current_samples = chunk.get_range(0, 100).unwrap();
            assert_eq!(current_samples, expected,);
        }
    }

    #[test]
    fn test_merge_single_sample_honors_duplicate_policy() {
        // A single-sample merge must honor the duplicate policy like the multi-sample path
        // (the Uncompressed chunk used to hardcode KeepLast here).
        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 100);
            chunk
                .set_data(&[
                    Sample {
                        timestamp: 20,
                        value: 5.0,
                    },
                    Sample {
                        timestamp: 50,
                        value: 6.0,
                    },
                ])
                .unwrap();

            // Block: the duplicate is rejected and the stored value is unchanged.
            let result = chunk
                .merge_samples(
                    &[Sample {
                        timestamp: 20,
                        value: 9.0,
                    }],
                    Some(DuplicatePolicy::Block),
                )
                .unwrap();
            assert_eq!(
                result,
                vec![SampleAddResult::Duplicate],
                "{chunk_type}: Block must reject a single-sample duplicate"
            );
            assert_eq!(
                chunk.get_range(20, 20).unwrap(),
                vec![Sample {
                    timestamp: 20,
                    value: 5.0
                }],
                "{chunk_type}: Block must leave the stored value unchanged"
            );

            // KeepLast: the duplicate overwrites the stored value.
            let result = chunk
                .merge_samples(
                    &[Sample {
                        timestamp: 20,
                        value: 9.0,
                    }],
                    Some(DuplicatePolicy::KeepLast),
                )
                .unwrap();
            assert!(
                result.len() == 1 && result[0].is_ok(),
                "{chunk_type}: KeepLast must accept a single-sample duplicate, got {result:?}"
            );
            assert_eq!(
                chunk.get_range(20, 20).unwrap(),
                vec![Sample {
                    timestamp: 20,
                    value: 9.0
                }],
                "{chunk_type}: KeepLast must overwrite the stored value"
            );
        }
    }

    #[test]
    fn test_merge_samples_outside_range() {
        let samples = vec![
            Sample {
                timestamp: 5,
                value: 1.0,
            },
            Sample {
                timestamp: 15,
                value: 2.0,
            },
            Sample {
                timestamp: 25,
                value: 3.0,
            },
            Sample {
                timestamp: 35,
                value: 4.0,
            },
        ];

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 100);
            chunk
                .set_data(&[
                    Sample {
                        timestamp: 10,
                        value: 0.0,
                    },
                    Sample {
                        timestamp: 20,
                        value: 0.0,
                    },
                ])
                .unwrap();

            assert_eq!(chunk.len(), 2);
            assert_eq!(chunk.first_timestamp(), 10);
            assert_eq!(chunk.last_timestamp(), 20);

            let result = chunk
                .merge_samples(&samples, Some(DuplicatePolicy::Block))
                .unwrap();

            assert_eq!(
                result.len(),
                samples.len(),
                "{chunk_type}: Expected 2 results, got {}",
                result.len()
            );
            assert_eq!(chunk.len(), 6);
            assert_eq!(
                chunk.first_timestamp(),
                5,
                "{chunk_type}: expected first timestamp 5, got {}",
                chunk.first_timestamp()
            );
            assert_eq!(
                chunk.last_timestamp(),
                35,
                "{chunk_type}: expected last timestamp 35, got {}",
                chunk.last_timestamp()
            );

            let range = chunk.get_range(0, 40).unwrap();
            assert_eq!(
                range,
                vec![
                    Sample {
                        timestamp: 5,
                        value: 1.0
                    },
                    Sample {
                        timestamp: 10,
                        value: 0.0
                    },
                    Sample {
                        timestamp: 15,
                        value: 2.0,
                    },
                    Sample {
                        timestamp: 20,
                        value: 0.0
                    },
                    Sample {
                        timestamp: 25,
                        value: 3.0,
                    },
                    Sample {
                        timestamp: 35,
                        value: 4.0
                    },
                ]
            );
        }
    }

    #[test]
    fn test_merge_samples_with_mixed_timestamps() {
        let existing_samples = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 20,
                value: 2.0,
            },
            Sample {
                timestamp: 30,
                value: 3.0,
            },
        ];

        let new_samples = vec![
            Sample {
                timestamp: 15,
                value: 1.5,
            },
            Sample {
                timestamp: 20,
                value: 2.5,
            },
            Sample {
                timestamp: 25,
                value: 2.5,
            },
            Sample {
                timestamp: 35,
                value: 3.5,
            },
        ];

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 1024);
            chunk.set_data(&existing_samples).unwrap();

            assert_eq!(chunk.len(), 3);

            let result = chunk
                .merge_samples(&new_samples, Some(DuplicatePolicy::KeepLast))
                .unwrap();

            assert_eq!(
                result.len(),
                new_samples.len(),
                "{chunk_type}: Expected {} results, got {}",
                new_samples.len(),
                result.len()
            );

            assert_eq!(
                chunk.len(),
                6,
                "{chunk_type}: expected 6 results, found {}",
                chunk.len()
            );

            let all_samples = chunk.get_range(0, 40).unwrap();
            assert_eq!(
                all_samples,
                vec![
                    Sample {
                        timestamp: 10,
                        value: 1.0
                    },
                    Sample {
                        timestamp: 15,
                        value: 1.5
                    },
                    Sample {
                        timestamp: 20,
                        value: 2.5
                    },
                    Sample {
                        timestamp: 25,
                        value: 2.5
                    },
                    Sample {
                        timestamp: 30,
                        value: 3.0
                    },
                    Sample {
                        timestamp: 35,
                        value: 3.5
                    },
                ]
            );
        }
    }

    const ELEMENTS_PER_CHUNK: usize = 60;

    fn saturate_chunk(chunk: &mut TimeSeriesChunk) {
        loop {
            let samples = generate_random_samples(100);
            match chunk.merge_samples(&samples, Some(DuplicatePolicy::KeepLast)) {
                Ok(_) => {}
                Err(TsdbError::CapacityFull(_)) => break,
                Err(e) => panic!("unexpected error: {e:?}"),
            }
            let threshold = (chunk.max_size() as f64 * SPLIT_FACTOR) as usize;
            if chunk.size() > threshold {
                break;
            }
        }
    }

    #[test]
    fn test_merge_by_capacity_with_empty_source_chunk() {
        let mut dest_chunk = TimeSeriesChunk::new(ChunkEncoding::Uncompressed, 1024);
        let mut src_chunk = TimeSeriesChunk::new(ChunkEncoding::Uncompressed, 1024);

        // Ensure the source chunk is empty
        assert!(src_chunk.is_empty());

        let result = merge_by_capacity(
            &mut dest_chunk,
            &mut src_chunk,
            0,
            Some(DuplicatePolicy::KeepLast),
        );

        assert_eq!(result, Ok(None));
    }

    #[test]
    fn test_merge_by_capacity_partial_merge() {
        let mut dest_chunk = TimeSeriesChunk::new(ChunkEncoding::Uncompressed, 1024);
        let mut src_chunk = TimeSeriesChunk::new(ChunkEncoding::Uncompressed, 1024);

        let capacity = dest_chunk.estimate_remaining_sample_capacity();

        // Fill the destination chunk to have more than a quarter but less than full capacity of the source
        let dest_samples = generate_random_samples(capacity / 2);
        dest_chunk.set_data(&dest_samples).unwrap();

        // Fill the source chunk with samples
        saturate_chunk(&mut src_chunk);

        let saved_count = src_chunk.len();

        // Perform the merge
        let result = merge_by_capacity(
            &mut dest_chunk,
            &mut src_chunk,
            0,
            Some(DuplicatePolicy::KeepLast),
        )
        .unwrap();

        // Check that a partial merge occurred
        assert!(result.is_some());

        let count_merged = dest_chunk.len() + src_chunk.len();

        // Verify that the source chunk still contains the remaining samples
        assert_eq!(count_merged, saved_count);
    }

    #[test]
    fn test_merge_by_capacity_with_empty_destination() {
        let mut dest_chunk = TimeSeriesChunk::new(ChunkEncoding::Uncompressed, 1024);
        let mut src_chunk = TimeSeriesChunk::new(ChunkEncoding::Uncompressed, 1024);

        // Add some samples to the source chunk
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
        ];
        src_chunk.set_data(&samples).unwrap();

        // Ensure destination chunk is empty
        assert!(dest_chunk.is_empty());

        // Perform the merge
        let result = merge_by_capacity(
            &mut dest_chunk,
            &mut src_chunk,
            0,
            Some(DuplicatePolicy::KeepLast),
        );

        // Verify the merge result
        assert_eq!(result.unwrap(), Some(samples.len()));
        assert!(src_chunk.is_empty());
        assert_eq!(dest_chunk.len(), samples.len());

        // Verify the samples in the destination chunk
        let merged_samples = dest_chunk.get_range(100, 300).unwrap();
        assert_eq!(merged_samples, samples);
    }

    #[test]
    fn test_merge_by_capacity_clears_source_after_full_merge() {
        let mut dest_chunk = TimeSeriesChunk::new(ChunkEncoding::Uncompressed, 1024);
        let mut src_chunk = TimeSeriesChunk::new(ChunkEncoding::Uncompressed, 1024);

        // Populate source chunk with samples
        let samples = generate_random_samples(ELEMENTS_PER_CHUNK);
        src_chunk.set_data(&samples).unwrap();

        // Ensure the destination chunk has enough capacity for a full merge
        let remaining_capacity = dest_chunk.estimate_remaining_sample_capacity();
        assert!(remaining_capacity >= ELEMENTS_PER_CHUNK);

        // Perform the merge
        let min_timestamp = samples[0].timestamp;
        let result = merge_by_capacity(
            &mut dest_chunk,
            &mut src_chunk,
            min_timestamp,
            Some(DuplicatePolicy::KeepLast),
        );

        // Verify the result
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(ELEMENTS_PER_CHUNK));
        assert!(src_chunk.is_empty());
    }

    #[test]
    fn test_iter_all() {
        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 16384);
            let samples = generate_random_samples(2500);
            chunk.set_data(&samples).unwrap();

            let actual_samples = chunk.iter().collect::<Vec<Sample>>();

            assert_eq!(
                samples.len(),
                actual_samples.len(),
                "{} : expected samples len {}, got {}",
                chunk_type,
                samples.len(),
                actual_samples.len()
            );
            assert_eq!(samples, actual_samples);
        }
    }

    #[test]
    fn test_samples_by_timestamps() {
        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 16384);
            let samples = generate_random_samples(100);

            for sample in samples.iter() {
                chunk.add_sample(sample).unwrap();
            }

            // Test with a subset of timestamps
            let timestamps: Vec<_> = samples.iter().map(|s| s.timestamp).collect();
            let selected_timestamps = &timestamps[10..20];
            let expected_samples: Vec<_> = samples[10..20].to_vec();

            let result_samples = chunk.samples_by_timestamps(selected_timestamps).unwrap();
            assert_eq!(result_samples, expected_samples);

            // Test with timestamps that are not present
            let missing_timestamps = vec![-2000, -3000, -4000];
            let result_samples = chunk.samples_by_timestamps(&missing_timestamps).unwrap();
            assert!(result_samples.is_empty());

            // Test with an empty timestamp list
            let result_samples = chunk.samples_by_timestamps(&[]).unwrap();
            assert!(result_samples.is_empty());
        }
    }

    #[test]
    fn test_samples_by_timestamps_partial_overlap() {
        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 16384);
            let samples = generate_random_samples(100);

            for sample in samples.iter() {
                chunk.add_sample(sample).unwrap();
            }

            // Test with a mix of present and absent timestamps
            let timestamps = vec![
                samples[5].timestamp,
                2000, // not present
                samples[15].timestamp,
            ];
            let expected_samples = vec![samples[5], samples[15]];

            let result_samples = chunk.samples_by_timestamps(&timestamps).unwrap();
            assert_eq!(result_samples, expected_samples);
        }
    }

    /// `memory_usage` must account for the encoded payload on the heap.
    ///
    /// A manual `GetSize` impl that overrides only `get_size` is invisible to
    /// the derived impl of any type containing it, which silently reported a
    /// bare `size_of::<TimeSeriesChunk>()` for uncompressed and gorilla
    /// series regardless of how many samples they held.
    #[test]
    fn test_memory_usage_counts_encoded_payload() {
        const SAMPLE_COUNT: usize = 2000;

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 1024 * 1024);
            for sample in generate_random_samples(SAMPLE_COUNT).iter() {
                chunk.add_sample(sample).unwrap();
            }
            assert_eq!(chunk.len(), SAMPLE_COUNT, "{chunk_type:?} lost samples");

            let empty = TimeSeriesChunk::new(chunk_type, 1024 * 1024).memory_usage();
            let filled = chunk.memory_usage();
            let payload = encoded_size(&chunk);

            // The payload lives on the heap, so a filled chunk has to outweigh
            // an empty one by at least the bytes the encoder wrote.
            assert!(
                filled >= empty + payload,
                "{chunk_type:?}: memory_usage {filled} does not cover {payload} encoded \
                 bytes over the empty baseline of {empty}",
            );
        }
    }

    #[test]
    fn test_encode() {
        struct Test {
            name: &'static str,
            input: Vec<f64>,
        }

        let tests = [
            Test {
                name: "from reference paper",
                input: vec![12.0, 12.0, 24.0, 13.0, 24.0, 24.0, 24.0, 23.0],
            },
            Test {
                name: "random",
                input: vec![
                    -3.8970913068231994e+307,
                    -9.036931257783943e+307,
                    1.7173073833490201e+308,
                    -9.312369166661538e+307,
                    -2.2435523083555231e+307,
                    1.4779121287289644e+307,
                    1.771273431601434e+308,
                    8.140360378221364e+307,
                    4.783405048208089e+307,
                    -2.8044680049605344e+307,
                    4.412915337205696e+307,
                    -1.2779380602005046e+308,
                    1.6235802318921885e+308,
                    -1.3402901846299688e+307,
                    1.6961015582104055e+308,
                    -1.067980796435633e+308,
                    -3.02868987458268e+307,
                    1.7641793640790284e+308,
                    1.6587191845856813e+307,
                    -1.786073304985983e+308,
                    1.0694549382051123e+308,
                    3.5635180996210295e+307,
                ],
            },
            Test {
                name: "previous example as natural numbers",
                input: vec![
                    -38970913068231994.0,
                    -9036931257783943.0,
                    171730738334902010.0,
                    -9312369166661538.0,
                    -22435523083555231.0,
                    14779121287289644.0,
                    17712734316014340.0,
                    8140360378221364.0,
                    4783405048208089.0,
                    -28044680049605344.0,
                    4412915337205696.0,
                    -127793806020050460.0,
                    162358023189218850.0,
                    -13402901846299688.0,
                    169610155821040550.0,
                    -10679807964356330.0,
                    -302868987458268.0,
                    176417936407902840.0,
                    16587191845856813.0,
                    -17860733049859830.0,
                    106945493820511230.0,
                    35635180996210295.0,
                ],
            },
            Test {
                name: "similar values",
                input: vec![
                    6.00065e+06,
                    6.000656e+06,
                    6.000657e+06,
                    6.000659e+06,
                    6.000661e+06,
                ],
            },
            Test {
                name: "two hours data",
                input: vec![
                    761.0, 727.0, 763.0, 706.0, 700.0, 679.0, 757.0, 708.0, 739.0, 707.0, 699.0,
                    740.0, 729.0, 766.0, 730.0, 715.0, 705.0, 693.0, 765.0, 724.0, 799.0, 761.0,
                    737.0, 766.0, 756.0, 719.0, 722.0, 801.0, 747.0, 731.0, 742.0, 744.0, 791.0,
                    750.0, 759.0, 809.0, 751.0, 705.0, 770.0, 792.0, 727.0, 762.0, 772.0, 721.0,
                    748.0, 753.0, 744.0, 716.0, 776.0, 659.0, 789.0, 766.0, 758.0, 690.0, 795.0,
                    770.0, 758.0, 723.0, 767.0, 765.0, 693.0, 706.0, 681.0, 727.0, 724.0, 780.0,
                    678.0, 696.0, 758.0, 740.0, 735.0, 700.0, 742.0, 747.0, 752.0, 734.0, 743.0,
                    732.0, 746.0, 770.0, 780.0, 710.0, 731.0, 712.0, 712.0, 741.0, 770.0, 770.0,
                    754.0, 718.0, 670.0, 775.0, 749.0, 795.0, 756.0, 741.0, 787.0, 721.0, 745.0,
                    782.0, 765.0, 780.0, 811.0, 790.0, 836.0, 743.0, 858.0, 739.0, 762.0, 770.0,
                    752.0, 763.0, 795.0, 792.0, 746.0, 786.0, 785.0, 774.0, 786.0, 718.0,
                ],
            },
            Test {
                name: "identical values",
                input: vec![12123.1234; 1000],
            },
            Test {
                name: "1000 real CPU values",
                input: vec![
                    11.286653185035389,
                    3.7310629773381745,
                    1.6102858569466982,
                    1.691305437233776,
                    1.8957345971563981,
                    3.625453181647706,
                    10.073740782402199,
                    7.99398571607568,
                    4.598130841121495,
                    5.293527345709985,
                    6.247661803217359,
                    4.296777416937297,
                    1.3373328333958254,
                    1.5998000249968753,
                    3.0139394700489763,
                    2.0558185895838523,
                    3.18630513557416,
                    4.133882852504059,
                    3.3033033033033035,
                    3.6824366496067906,
                    2.9378672334041753,
                    2.112764095511939,
                    1.5896858179997497,
                    5.252626313156578,
                    2.908137793310035,
                    3.1738098213170063,
                    3.3120859892513437,
                    2.189415738771425,
                    2.9650944576504443,
                    2.826195219123506,
                    2.618391380606364,
                    2.739897410233955,
                    2.886056971514243,
                    2.25140712945591,
                    3.5843636817784437,
                    2.7236381809095453,
                    3.5138176816306115,
                    3.5348488633524857,
                    1.5920772220132882,
                    2.5954579485899676,
                    2.4918607563235664,
                    1.9355644355644355,
                    2.240580798598072,
                    1.8293446936474127,
                    1.1938813580400447,
                    3.0753844230528817,
                    3.2399299474605954,
                    5.420326223337516,
                    6.535540893813021,
                    5.47158026233604,
                    2.55,
                    2.0336429826763744,
                    2.638489433537577,
                    2.8251400124455506,
                    2.3505876469117277,
                    2.9632408102025507,
                    2.7243189202699325,
                    1.712285964254468,
                    1.6506189821182944,
                    1.1495689116581282,
                    1.4140908522087348,
                    1.8733608092918697,
                    2.13696575856036,
                    3.0644152595372107,
                    3.649087728067983,
                    3.649087728067983,
                    2.4799599198396796,
                    3.4676312835225147,
                    2.9992501874531365,
                    2.8371453568303964,
                    3.6660389202762085,
                    3.5487485991781846,
                    3.8509627406851714,
                    4.373981701967665,
                    3.8153615211408556,
                    2.3548467480687765,
                    2.575321915239405,
                    1.4267834793491865,
                    1.2625,
                    1.2353381582231096,
                    3.055243795984537,
                    3.6958155850663994,
                    2.0645645645645647,
                    1.7491254372813594,
                    1.8865567216391803,
                    4.563640910227557,
                    3.6805207811717575,
                    3.3983008495752123,
                    3.6117381489841986,
                    2.832650018635855,
                    1.0390585878818228,
                    3.930131004366812,
                    5.608531994981179,
                    6.253900886281363,
                    5.43640897755611,
                    2.4558326024307733,
                    2.272727272727273,
                    4.708829054477144,
                    3.233458177278402,
                    2.9246344206974126,
                    2.651325662831416,
                    2.6993251687078232,
                    3.994990607388854,
                    2.220558882235529,
                    1.275797373358349,
                    2.0367362239160314,
                    2.8375,
                    1.676257192894671,
                    2.237779722465308,
                    2.135098014733425,
                    1.2259194395796849,
                    1.2489072061945798,
                    1.2503125781445361,
                    1.3375,
                    1.3413563996489908,
                    1.3102071375093587,
                    1.1620642259152818,
                    1.6577340147077153,
                    2.6081504702194356,
                    1.9233170975396527,
                    3.6754594324290535,
                    1.936773709858803,
                    2.766649974962444,
                    2.3833416959357754,
                    2.1663346613545817,
                    2.722957781663752,
                    1.5119330251155816,
                    2.075,
                    2.340132649230384,
                    1.3647176662075873,
                    2.5884706765036887,
                    3.3445231878652244,
                    4.001505268439538,
                    1.9852665751030092,
                    2.9267679939706066,
                    2.8447204968944098,
                    1.9831806200577382,
                    2.695619618120554,
                    3.9003115264797508,
                    2.9944640161046805,
                    2.6381284221005474,
                    2.651657285803627,
                    1.8745313671582104,
                    1.5650431951921873,
                    1.6974538192710933,
                    1.7769991240145164,
                    3.06809678223996,
                    3.730128927275003,
                    4.101221640488657,
                    3.045112781954887,
                    4.24037134612972,
                    3.2709113607990012,
                    1.546713234376949,
                    3.7937019588395735,
                    1.639344262295082,
                    1.1609037573336662,
                    0.7003501750875438,
                    3.4275706780085065,
                    4.846587351283657,
                    8.203907815631263,
                    18.056599048334586,
                    10.340050377833753,
                    4.5375,
                    9.898889027587067,
                    3.705564627559352,
                    4.7613112302131375,
                    7.604467310829464,
                    5.090230242688239,
                    3.286915067118304,
                    4.54602223054827,
                    0.9040247678018576,
                    2.383955600403633,
                    2.8744889109156238,
                    6.1522945032778615,
                    5.16641828117238,
                    5.089218396582056,
                    5.886028492876781,
                    7.0792017070415465,
                    5.167894145549869,
                    2.3025501361723197,
                    0.8625,
                    1.0116148370176097,
                    0.5875734466808351,
                    0.703694395576778,
                    0.7711442786069652,
                    1.587896974243561,
                    3.3358509566968784,
                    1.9875,
                    2.5736203909923288,
                    0.7769423558897243,
                    2.013506753376688,
                    0.7248187953011747,
                    0.7130347760820616,
                    0.8994378513429107,
                    0.7498125468632841,
                    0.7125890736342043,
                    0.9572994079858924,
                    0.6337765626941717,
                    0.8108782435129741,
                    0.7625953244155519,
                    6.323510813203491,
                    4.336989032901296,
                    3.547782635852592,
                    3.8667830859423726,
                    3.95742016280526,
                    1.7510944340212633,
                    0.9375,
                    2.3729236917697016,
                    4.491017964071856,
                    4.435836561289516,
                    5.663939584644431,
                    1.8381893209953732,
                    1.1118051217988758,
                    2.2338699613128665,
                    2.1189081391000872,
                    1.4509068167604753,
                    1.2650300601202404,
                    1.347305389221557,
                    2.108169155477475,
                    3.7398373983739837,
                    3.3872976338729766,
                    1.1,
                    2.059925093632959,
                    1.4130298862073278,
                    1.375171896487061,
                    1.5011258443832876,
                    1.7312758750470456,
                    1.0823587957203284,
                    1.1912225705329154,
                    1.6826623457559517,
                    2.3415977961432506,
                    1.2849301397205588,
                    1.7127140892611576,
                    2.01325497061398,
                    2.049487628092977,
                    1.7157169693174703,
                    1.410736579275905,
                    1.3989507869098177,
                    1.4273193940152749,
                    1.4474669328674818,
                    1.4509068167604753,
                    1.5257628814407203,
                    1.6370907273181705,
                    1.9113054341036853,
                    1.3625,
                    1.2266866942045311,
                    1.6485575121768452,
                    1.311680199875078,
                    1.0628985869701137,
                    1.11236095488064,
                    1.2996750812296927,
                    1.4398397395768123,
                    1.6344354335620712,
                    2.364568997873139,
                    2.7979015738196353,
                    2.4262131065532766,
                    2.11065317846884,
                    2.8639319659829914,
                    1.5248093988251468,
                    2.0497437820272464,
                    1.4380392647242717,
                    1.4507253626813408,
                    1.50018752344043,
                    1.3238416385662546,
                    1.6741629185407296,
                    2.3517638228671505,
                    1.6870782304423895,
                    1.85,
                    1.226379677136779,
                    1.536155863619333,
                    2.5541504945536495,
                    1.963727329580988,
                    2.543640897755611,
                    2.276138069034517,
                    1.4133833646028768,
                    2.4362818590704647,
                    1.7875,
                    1.400525196948856,
                    1.0990383414512301,
                    1.625,
                    2.3744063984004,
                    1.1126390798849857,
                    1.7883941970985493,
                    1.5121219695076231,
                    1.4126765845730715,
                    1.136789506558401,
                    1.2384288216162123,
                    1.1376422052756594,
                    1.236418134132634,
                    1.6754188547136784,
                    1.6416040100250626,
                    2.1614192903548224,
                    2.2227772227772227,
                    1.0744627686156922,
                    1.3126640830103764,
                    2.5378172271533943,
                    3.3125,
                    3.511622094476381,
                    1.5632816408204102,
                    3.620474406991261,
                    2.9863801074597025,
                    1.2648716343143394,
                    1.1611936571357222,
                    1.6129032258064515,
                    3.588845817181443,
                    1.3243378310844578,
                    2.5072082236429734,
                    2.395209580838323,
                    6.315657828914457,
                    3.0976767424431677,
                    2.8646157678415745,
                    3.5776832624468353,
                    2.1532298447671505,
                    2.465890599574415,
                    2.3985009369144286,
                    4.2473454091193,
                    3.889931207004378,
                    2.4518388791593697,
                    3.0016191306513886,
                    2.814610958218664,
                    3.019247704113725,
                    2.7127924340467895,
                    2.9375,
                    2.4390243902439024,
                    3.1791547188629847,
                    2.01325497061398,
                    3.1568356181612374,
                    2.774667164364813,
                    3.9639864949356007,
                    1.8900988859682062,
                    3.0257751214045574,
                    1.4541807697129248,
                    4.20042378162782,
                    6.213981458281133,
                    5.3235257449195865,
                    2.3436520867276602,
                    3.890759446315002,
                    3.973426924041113,
                    1.670015067805123,
                    4.529053129277093,
                    2.0648229257915154,
                    4.3825696091896615,
                    3.890759446315002,
                    8.32288794184006,
                    16.313295086056375,
                    3.9313885063227745,
                    3.1746031746031744,
                    4.294863744819792,
                    4.854730568661535,
                    4.84901641398321,
                    3.4904013961605584,
                    4.512409125094009,
                    3.8639489808678253,
                    3.8600874453466583,
                    3.6819770344483276,
                    3.9308963445167753,
                    2.601951463597698,
                    4.79820067474697,
                    4.549431321084865,
                    4.902083073468878,
                    5.147891755821271,
                    3.2209924138788706,
                    3.2060878243512976,
                    4.336957880264967,
                    3.152758663830852,
                    2.704056084126189,
                    3.0117470632341914,
                    4.2705072010018785,
                    4.996252810392206,
                    3.784661503872096,
                    2.6743314171457135,
                    4.125,
                    3.463365841460365,
                    4.259837601499063,
                    2.6200873362445414,
                    2.7948364456698833,
                    3.1179845347967072,
                    3.411086029596188,
                    3.2104934415990005,
                    3.59685275384039,
                    4.465510789572159,
                    2.5952858575727182,
                    4.499437570303712,
                    3.248375812093953,
                    2.4878109763720464,
                    3.965365792445727,
                    2.7535509593820087,
                    3.1855090568394755,
                    1.4865708931917552,
                    2.5209035317608888,
                    1.8934169278996866,
                    1.949025487256372,
                    2.875359419927491,
                    1.6360684401148995,
                    2.5387693846923463,
                    2.20247778751095,
                    2.0867174809446456,
                    1.440561192534135,
                    1.1214953271028036,
                    1.002004008016032,
                    1.1479910157224857,
                    0.9778112072207596,
                    1.0719182350741618,
                    1.7133566783391696,
                    0.924191332583989,
                    2.3267450587940957,
                    1.7747781527309086,
                    1.5994002249156567,
                    1.0393188079138493,
                    1.621351958094288,
                    1.518955561134823,
                    0.9715994020926756,
                    1.1489946296990134,
                    0.901352028042063,
                    2.127659574468085,
                    1.2106839740389417,
                    1.0360753963300462,
                    1.8509254627313656,
                    1.4882441220610305,
                    1.0870923403723605,
                    1.4262479669710997,
                    5.1077613055936215,
                    4.774931609052475,
                    5.52555569508979,
                    5.446161515453639,
                    6.3678361842926705,
                    4.3364158960259935,
                    1.1625,
                    1.825912956478239,
                    1.2125,
                    1.1605903872839662,
                    0.9479855307471623,
                    0.9656383245548031,
                    1.1587341141290806,
                    2.0210896309314585,
                    2.302140368342459,
                    1.0884523958463657,
                    0.9534562790114164,
                    0.9715994020926756,
                    1.098901098901099,
                    1.9784623090408215,
                    1.073389915127309,
                    1.56289072268067,
                    1.4886164623467601,
                    1.3994751968011996,
                    1.7609591607343575,
                    0.9498812648418947,
                    1.7649267743146826,
                    1.136221750530653,
                    0.9379689844922461,
                    2.1111805121798874,
                    2.2252781597699713,
                    2.1630407601900474,
                    1.115987460815047,
                    2.0125,
                    1.5451713395638629,
                    1.5625,
                    1.2642383277005884,
                    0.9364464976900987,
                    1.3744845682868925,
                    1.09104589917231,
                    0.8164275111331024,
                    1.0117411941044216,
                    1.4132066033016508,
                    0.8380237648530331,
                    0.7878939469734867,
                    1.075,
                    1.0613060307154452,
                    1.2125,
                    1.50018752344043,
                    1.4501812726590824,
                    1.0494752623688155,
                    1.1255627813906954,
                    0.7126781695423856,
                    1.1118051217988758,
                    1.0768845479589282,
                    0.9616585487698264,
                    0.8116883116883117,
                    1.5501937742217777,
                    1.1011011011011012,
                    0.9752438109527382,
                    0.9737827715355806,
                    1.0623672040994876,
                    1.7517517517517518,
                    1.5988008993255058,
                    2.4091826437941473,
                    1.1982026959560659,
                    1.2507817385866167,
                    0.974512743628186,
                    1.0889973713856553,
                    1.23688155922039,
                    1.735330836454432,
                    1.1008256192144108,
                    1.1502875718929733,
                    1.049082053203447,
                    1.040230605339015,
                    1.8573921715282973,
                    1.2879829936226084,
                    1.9262038774233896,
                    2.7843675864652266,
                    0.9283653243005896,
                    1.7957351290684624,
                    1.2492192379762648,
                    0.9248843894513186,
                    1.075268817204301,
                    1.3897583573306622,
                    0.9732967307212378,
                    1.1255627813906954,
                    1.2274549098196392,
                    0.9489324509926332,
                    1.2402906539714358,
                    1.2084215771770275,
                    0.9496438835436711,
                    1.0755377688844423,
                    1.236572570572071,
                    0.8443009684628756,
                    1.002004008016032,
                    1.047250966213689,
                    1.1,
                    0.9746345120579782,
                    1.5157209069272204,
                    1.0480349344978166,
                    5.388173521690211,
                    3.325774754346183,
                    2.424090965887792,
                    1.9879969992498125,
                    4.252400548696845,
                    5.122745490981964,
                    8.461827754795035,
                    9.561852452877293,
                    5.677564262540554,
                    6.4795087103647075,
                    11.7683763883689,
                    11.491809428535701,
                    11.861655637407916,
                    8.579289644822412,
                    3.53661584603849,
                    2.1997250343707035,
                    2.7125455230440787,
                    2.0734449163127655,
                    2.398201348988259,
                    2.8830620106872127,
                    2.2080040145527535,
                    2.3607294529103173,
                    4.101025256314078,
                    2.214160620465349,
                    2.205057929487978,
                    2.719298245614035,
                    4.217396761641773,
                    1.9920318725099602,
                    1.8165982331715815,
                    1.5831134564643798,
                    2.156739811912226,
                    2.106706556968337,
                    2.346480279580629,
                    4.129645851583031,
                    2.575,
                    2.0994751312171958,
                    2.6368407898025494,
                    2.261651880544796,
                    2.0940438871473352,
                    5.609573672400898,
                    3.127354935945742,
                    2.3625963690624223,
                    2.954431647471207,
                    2.8963795255930087,
                    2.0277882087870824,
                    1.6936488169364883,
                    2.3749685850716262,
                    2.330508474576271,
                    1.9772243774246028,
                    2.507522567703109,
                    2.4533001245330013,
                    3.6293339985033675,
                    4.437202306342441,
                    3.948519305260527,
                    2.93823455863966,
                    2.6739972510308636,
                    3.373734849431463,
                    3.779724655819775,
                    2.9798422436459244,
                    4.555097965805566,
                    3.9132070738743256,
                    2.130841121495327,
                    1.424287856071964,
                    1.78682993877296,
                    2.439634680345302,
                    3.381763527054108,
                    2.4922118380062304,
                    2.8814833375093962,
                    1.2358007739358383,
                    1.1126390798849857,
                    1.2484394506866416,
                    2.5378172271533943,
                    4.085967762089217,
                    10.723659542557181,
                    6.651662915728933,
                    4.898163188804198,
                    4.215134459036898,
                    4.111472131967008,
                    5.3895210704014005,
                    5.248031988004498,
                    6.167896909796071,
                    7.418508804795803,
                    7.763819095477387,
                    3.333747978604304,
                    7.185703574106474,
                    4.7011752938234554,
                    5.547919530176184,
                    5.1016589746788075,
                    6.253916530893596,
                    3.986503374156461,
                    4.254522769806613,
                    4.059133049361062,
                    4.02650994122796,
                    3.139795664091702,
                    1.455092824887105,
                    1.2118940529735132,
                    1.6364772017489069,
                    2.31278909863733,
                    2.627956451007383,
                    3.7217434744598474,
                    4.866483653606189,
                    4.683195592286501,
                    13.934016495876032,
                    4.038509627406852,
                    9.072706795144537,
                    8.952618453865338,
                    16.64580725907384,
                    4.65,
                    3.150393799224903,
                    3.7643821910955477,
                    3.409090909090909,
                    3.9929903617474025,
                    2.1972534332084894,
                    2.4621922259717537,
                    1.8506940102538452,
                    2.1284587454613746,
                    1.2983770287141074,
                    1.3537227375282026,
                    2.2429906542056073,
                    2.6503312914114265,
                    1.3141426783479349,
                    2.1483887084686484,
                    1.4536340852130325,
                    1.545941902505922,
                    1.3640345388562132,
                    1.6235793680529538,
                    2.0119970007498127,
                    3.310430980637102,
                    1.1252813203300824,
                    2.612080874042446,
                    1.7170586039567002,
                    2.8778778778778777,
                    2.7753469183647956,
                    2.3363318340829586,
                    2.0606968902210565,
                    1.0505252626313157,
                    1.261553834624032,
                    2.238339377266475,
                    3.426284856821308,
                    2.3505876469117277,
                    1.5746063484128967,
                    1.791755419120411,
                    2.4940765681506423,
                    1.3616489693941287,
                    1.988991743807856,
                    1.1641006383777694,
                    0.8989886377824947,
                    1.5130674002751032,
                    1.4498187726534184,
                    1.5720524017467248,
                    1.1009633429250594,
                    1.9129782445611403,
                    3.3629203650456305,
                    3.2383095773943484,
                    2.1125,
                    1.0744627686156922,
                    1.5873015873015872,
                    1.3391739674593242,
                    0.8868348738446166,
                    3.15,
                    3.1996000499937507,
                    5.376344086021505,
                    3.092269326683292,
                    2.291510142749812,
                    2.230576441102757,
                    3.7752305008721656,
                    6.034051076614922,
                    5.11988011988012,
                    4.2390896586219835,
                    4.489183443791422,
                    4.744616925388082,
                    1.921876949956321,
                    1.9879969992498125,
                    1.749562609347663,
                    2.1540388227927365,
                    3.834624031976018,
                    5.069297040829067,
                    4.37172120909318,
                    2.2439513601604615,
                    3.181137724550898,
                    3.399150212446888,
                    4.019975031210986,
                    2.077337004129646,
                    1.5992003998000999,
                    4.775,
                    3.3516758379189593,
                    3.313328332083021,
                    4.101732951003616,
                    4.184414933600602,
                    9.849812265331664,
                    4.359775140537164,
                    2.3238380809595203,
                    3.4767383691845923,
                    4.55743879472693,
                    4.677780542423489,
                    4.172932330827067,
                    2.8550056102730332,
                    2.5253156644580574,
                    3.8061850507073993,
                    3.7129641205150645,
                    1.550581468050519,
                    2.28236467947119,
                    3.4,
                    3.651825912956478,
                    2.224443889027743,
                    3.479236812570146,
                    3.221358736525445,
                    1.0622344413896525,
                    2.52784382430234,
                    1.798426376920195,
                    0.825,
                    1.0381488430268917,
                    2.3755938984746185,
                    3.655189620758483,
                    1.863431715857929,
                    1.3386713374202428,
                    3.2866783304173954,
                    2.8317253477007895,
                    2.143035135808622,
                    2.1627703462932866,
                    3.3654447641686476,
                    0.8876109513689211,
                    1.4128532133033258,
                    2.6993251687078232,
                    3.1222680154864495,
                    2.8489316506310134,
                    2.1638524077548467,
                    2.325,
                    2.937132858392701,
                    3.717611716109651,
                    2.919525888958203,
                    1.7129282320580146,
                    3.9453907815631264,
                    3.1694534564512105,
                    2.7965889139704037,
                    2.0622422197225347,
                    2.460347196203322,
                    3.121878121878122,
                    3.1464602322387316,
                    1.8667000751691305,
                    2.2072577628133185,
                    2.774653168353956,
                    2.638489433537577,
                    1.5248093988251468,
                    3.313328332083021,
                    3.123828564288392,
                    3.224193951512122,
                    3.425,
                    2.4875,
                    3.3012379642365888,
                    2.9790962573538615,
                    2.6351942050705635,
                    2.9742564358910273,
                    3.2495938007749032,
                    2.9279279279279278,
                    2.6838097615778307,
                    2.488122030507627,
                    2.074222166687492,
                    2.8399849868635054,
                    5.220432121893343,
                    4.981226533166458,
                    3.946546771574872,
                    2.8646484863647736,
                    2.9839518555667,
                    2.4523839163450765,
                    3.2626427406199023,
                    3.9775561097256857,
                    5.620082427875609,
                    2.0119970007498127,
                    4.761904761904762,
                    4.061992250968629,
                    3.675918979744936,
                    2.6490066225165565,
                    3.500437554694337,
                    3.7911122269645996,
                    3.5242839352428392,
                    2.784048156508653,
                    4.30937850292689,
                    2.401500938086304,
                    1.875,
                    2.008284172210368,
                    1.7037681880363138,
                    2.9146860145108833,
                    1.8625,
                    1.461769115442279,
                    1.3758599124452784,
                    1.3920240782543265,
                    0.7597459210362436,
                    1.3628407101775444,
                    1.5246188452886777,
                    2.274431392151962,
                    2.331380127166189,
                    1.5809284818067755,
                    2.1234074444166873,
                    2.614133833646029,
                    1.650825412706353,
                    1.536155863619333,
                    1.5740162398500936,
                    1.5138245965219568,
                    1.2112887112887112,
                    2.567635270541082,
                    1.5730337078651686,
                    2.0635317658829413,
                    1.2510947078693857,
                    1.3729405891163255,
                    0.7745159275452842,
                    1.3050570962479608,
                    1.6879219804951238,
                    2.105132037867464,
                    2.65,
                    2.770812437311936,
                    3.339147769748318,
                    1.9257221458046767,
                    1.3269904857285928,
                    1.3850761167956076,
                    1.2121969507623094,
                    1.026026026026026,
                    2.495944090852365,
                    2.9761160435163188,
                    1.125703564727955,
                    1.3120079970011245,
                    1.2152342771235278,
                    1.4722395508421708,
                    1.5242378810594703,
                    1.693002257336343,
                    1.058926124330385,
                    0.900225056264066,
                    0.9623797025371829,
                    1.026539809714572,
                    1.3358302122347065,
                    1.9460138104205902,
                    1.5978030208463363,
                    1.0223164193990775,
                    1.0126265783222903,
                    1.0501312664083011,
                    4.408091908091908,
                    5.026885081905714,
                    2.551594746716698,
                    2.443280977312391,
                    2.7433295753476137,
                    1.525,
                    2.325,
                    2.9287138584247256,
                    2.7342280195660353,
                    1.4996250937265683,
                    1.6122984626921635,
                    3.3291770573566084,
                    7.73286467486819,
                    1.2983770287141074,
                    1.0755377688844423,
                    1.4244658253155067,
                    0.7869098176367724,
                    1.0257693269952464,
                    0.6994753934549088,
                    0.8875,
                    1.025384519194698,
                    1.7745563609097725,
                    2.456448176463216,
                    2.655860349127182,
                    0.7375921990248782,
                    0.8386531480786081,
                    0.998377636340946,
                    0.7999000124984377,
                    2.186406796601699,
                    3.5146966854283925,
                    2.0372453443319585,
                    1.9259629814907453,
                    2.4359775140537163,
                    0.7634543178973717,
                    0.7987021090727567,
                    0.9007881896659578,
                    0.7125,
                    1.3123359580052494,
                    1.262342207224097,
                    2.40180135101326,
                    0.877742946708464,
                    0.9596211365902293,
                    0.9365634365634365,
                    1.2248468941382327,
                    0.9508319779807332,
                    0.7767476822851416,
                    0.7220216606498195,
                    0.9540547326136078,
                    0.9349289454001496,
                    0.7508447002878238,
                    1.11236095488064,
                    0.6118117118241978,
                    0.8258258258258259,
                    1.614922383575363,
                    1.9815553339980059,
                    1.9136960600375235,
                    1.6112915313514864,
                    0.6762680025046963,
                    0.7628814407203601,
                    0.9367974019485386,
                    1.3368315842078962,
                    1.8978649019852665,
                    1.2274549098196392,
                    1.1858694295343901,
                    3.311672081979505,
                    3.7783060177655448,
                    3.3462354850792857,
                    2.6378297287160897,
                    2.412198475190601,
                    1.927891837756635,
                    2.1510755377688846,
                    1.0998625171853518,
                    1.2613962782565256,
                    1.1864618458848508,
                    1.3013013013013013,
                    1.2862137862137861,
                    5.7117860267466565,
                    3.140640640640641,
                    4.909431605246721,
                    5.711072231942015,
                    4.078568747654198,
                    1.8245438640339915,
                    1.387326584176978,
                    1.1998500187476566,
                    0.9640666082383874,
                    1.0728542914171657,
                    1.2625,
                    0.914099674430253,
                    0.8983156581409857,
                    1.0635635635635636,
                    1.2740444666500126,
                    1.5531062124248498,
                    2.3678276121272863,
                    2.7514940239043826,
                    0.8877219304826206,
                    1.0391886816076124,
                    0.8237643534697953,
                    1.4376797099637455,
                    1.799550112471882,
                    0.9272021049993735,
                    1.0340102155226112,
                    1.0256410256410255,
                    0.9248843894513186,
                    1.425891181988743,
                    1.1120829688866676,
                    1.0384086075315901,
                    1.1134742900037533,
                    1.4223331253898939,
                    1.1009633429250594,
                    1.2612387612387612,
                    1.537307836520435,
                    1.809272521673577,
                    15.410406059853472,
                    12.409668577124346,
                    5.008117896840265,
                    5.541624874623872,
                    6.655056599079487,
                    2.993861956657898,
                    1.4595808383233533,
                    1.1397795591182365,
                    0.8129064532266133,
                    0.9484587545238987,
                    1.261553834624032,
                    1.137215696075981,
                    2.5827482447342027,
                    1.244031163608947,
                    1.3625,
                    1.4621344663834042,
                    1.8309505894156006,
                    1.0860067407314942,
                    5.251340900586254,
                    2.262217222847144,
                    2.0127515939492437,
                    2.899637545306837,
                    2.126063031515758,
                    1.4869423966012745,
                    1.5121219695076231,
                    1.6497937757780277,
                    0.9754877438719359,
                    1.6993627389728851,
                    1.687289088863892,
                    1.50018752344043,
                    1.537307836520435,
                ],
            },
        ];

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 16384);

            for test in tests.iter() {
                for (i, value) in test.input.iter().enumerate() {
                    let sample = Sample {
                        timestamp: 1000 + (i * 1000) as i64,
                        value: *value,
                    };
                    chunk.add_sample(&sample).unwrap();
                }

                let last_ts = chunk.last_timestamp();

                let got = chunk.get_range(0, last_ts + 1).unwrap();
                let actual = got.iter().map(|x| x.value).collect::<Vec<f64>>();

                // verify got same values back
                assert_eq!(actual, test.input, "{chunk_type}: {}", test.name);

                chunk.clear();
            }
        }
    }

    #[test]
    fn test_encode_special_values() {
        let src: Vec<f64> = vec![
            100.0,
            222.12,
            f64::from_bits(0x7ff8000000000001), // Go representation of signaling NaN
            45.324,
            f64::NAN,
            2453.023,
            -1234.235312132,
            f64::INFINITY,
            f64::NEG_INFINITY,
            9123419329123.1234,
            f64::from_bits(0x7ff0000000000002), // Prometheus stale NaN
            -19292929929292929292.22,
            -0.0000000000000000000000000092,
        ];

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 16384);

            for (i, value) in src.iter().enumerate() {
                let sample = Sample {
                    timestamp: 1000 + (i * 1000) as i64,
                    value: *value,
                };
                chunk.add_sample(&sample).unwrap();
            }

            let last_ts = chunk.last_timestamp();

            let got = chunk.get_range(0, last_ts + 1).unwrap();
            // Verify decoded values.
            assert_eq!(got.len(), src.len());

            for (i, sample) in got.iter().enumerate() {
                let v = sample.value;
                let orig = src[i];
                if v.is_nan() || v.is_infinite() {
                    assert_eq!(
                        orig.to_bits(),
                        v.to_bits(),
                        "{chunk_type}: Error encoding value {orig}"
                    );
                } else {
                    assert!(
                        approximately_equal(orig, v),
                        "{chunk_type}: Error encoding value {orig}"
                    );
                }
            }
        }
    }

    #[test]
    fn test_has_samples_in_range_no_overlap() {
        // Create a chunk with samples from 10 to 50
        for encoding in [
            ChunkEncoding::Uncompressed,
            ChunkEncoding::Gorilla,
            ChunkEncoding::Chimp,
        ] {
            let mut chunk = TimeSeriesChunk::new(encoding, 1024);
            let samples = vec![
                Sample {
                    timestamp: 10,
                    value: 1.0,
                },
                Sample {
                    timestamp: 20,
                    value: 2.0,
                },
                Sample {
                    timestamp: 30,
                    value: 3.0,
                },
                Sample {
                    timestamp: 40,
                    value: 4.0,
                },
                Sample {
                    timestamp: 50,
                    value: 5.0,
                },
            ];
            chunk.set_data(&samples).unwrap();

            // Test range before chunk
            assert!(!chunk.has_samples_in_range(0, 5));

            // Test range after chunk
            assert!(!chunk.has_samples_in_range(60, 100));
        }
    }

    #[test]
    fn test_has_samples_in_range_with_samples() {
        let mut chunk = TimeSeriesChunk::new(ChunkEncoding::Gorilla, 1024);
        let samples = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 20,
                value: 2.0,
            },
            Sample {
                timestamp: 30,
                value: 3.0,
            },
            Sample {
                timestamp: 40,
                value: 4.0,
            },
            Sample {
                timestamp: 50,
                value: 5.0,
            },
        ];
        chunk.set_data(&samples).unwrap();

        // Test range that includes samples
        assert!(chunk.has_samples_in_range(15, 25));
        assert!(chunk.has_samples_in_range(10, 30));
        assert!(chunk.has_samples_in_range(10, 10)); // Exact match on first
        assert!(chunk.has_samples_in_range(50, 50)); // Exact match on last
        assert!(chunk.has_samples_in_range(5, 55)); // Wider range including all samples
    }

    #[test]
    fn test_has_samples_in_range_empty_chunk() {
        let chunk = TimeSeriesChunk::new(ChunkEncoding::Uncompressed, 100);

        // Empty chunk should always return false
        assert!(!chunk.has_samples_in_range(0, 100));
    }

    #[test]
    fn test_has_samples_in_range_gaps_in_data() {
        for encoding in [
            ChunkEncoding::Uncompressed,
            ChunkEncoding::Gorilla,
            ChunkEncoding::Chimp,
        ] {
            let mut chunk = TimeSeriesChunk::new(encoding, 1024);
            let samples = vec![
                Sample {
                    timestamp: 10,
                    value: 1.0,
                },
                Sample {
                    timestamp: 30,
                    value: 3.0,
                },
                Sample {
                    timestamp: 50,
                    value: 5.0,
                },
            ];
            chunk.set_data(&samples).unwrap();

            // Test ranges in gaps
            assert!(!chunk.has_samples_in_range(15, 25)); // Gap between 10 and 30
            assert!(!chunk.has_samples_in_range(35, 45)); // Gap between 30 and 50

            // Test ranges that include samples
            assert!(chunk.has_samples_in_range(25, 35)); // Includes 30
        }
    }

    #[test]
    fn test_has_samples_in_range_different_encodings() {
        // Test with different chunk encodings to ensure behavior is consistent
        for encoding in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(encoding, 1024);
            let samples = vec![
                Sample {
                    timestamp: 10,
                    value: 1.0,
                },
                Sample {
                    timestamp: 20,
                    value: 2.0,
                },
            ];
            chunk.set_data(&samples).unwrap();

            assert!(chunk.has_samples_in_range(10, 20));
            assert!(!chunk.has_samples_in_range(30, 40));
        }
    }

    #[test]
    fn test_has_samples_in_range_edge_cases() {
        for encoding in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(encoding, 1024);
            let samples = vec![
                Sample {
                    timestamp: 10,
                    value: 1.0,
                },
                Sample {
                    timestamp: 20,
                    value: 2.0,
                },
            ];
            chunk.set_data(&samples).unwrap();

            // Edge case: start == end
            assert!(chunk.has_samples_in_range(10, 10));
            assert!(chunk.has_samples_in_range(20, 20));
            assert!(!chunk.has_samples_in_range(15, 15));

            // Edge case: overlaps but no sample in range
            assert!(!chunk.has_samples_in_range(15, 15)); // Overlaps chunk but no sample at exactly 15
        }
    }

    // ----- Serialization Tests -----
    /// Serializes a TimeSeriesChunk for fanout.
    fn serialize_chunk(chunk: TimeSeriesChunk) -> Vec<u8> {
        // estimate of the size needed
        let capacity = chunk.len() * chunk.bytes_per_sample();
        let mut data: Vec<u8> = Vec::with_capacity(capacity);
        chunk.serialize(&mut data);
        data
    }

    /// Deserializes TimeSeriesChunk.
    pub fn deserialize_chunk(data: &[u8]) -> TimeSeriesChunk {
        match TimeSeriesChunk::deserialize(data) {
            Ok(chunk) => chunk,
            Err(e) => {
                eprintln!("[deserialize_chunk] Failed to deserialize chunk: {:?}", e);
                eprintln!(
                    "[deserialize_chunk] data_len={}, first_byte={:?}",
                    data.len(),
                    data.first()
                );
                if !data.is_empty() {
                    eprintln!(
                        "[deserialize_chunk] data (hex, up to 128 bytes): {:02x?}",
                        &data[..data.len().min(128)]
                    );
                }
                panic!("ChunkDecoding");
            }
        }
    }

    #[test]
    fn test_timeseries_chunk_serialization_empty_chunks() {
        for &encoding in CHUNK_TYPES.iter() {
            // Test serialization of empty chunk
            let empty_chunk = TimeSeriesChunk::new(encoding, 1024);
            assert!(empty_chunk.is_empty());

            let serialized = serialize_chunk(empty_chunk.clone());
            let deserialized = deserialize_chunk(&serialized);

            assert_eq!(empty_chunk, deserialized);
            assert_eq!(empty_chunk.get_encoding(), deserialized.get_encoding());
            assert!(deserialized.is_empty());
        }
    }

    #[test]
    fn test_timeseries_chunk_serialization_single_sample() {
        for &encoding in CHUNK_TYPES.iter() {
            let mut chunk = TimeSeriesChunk::new(encoding, 1024);
            let sample = Sample {
                timestamp: 1000,
                value: 42.5,
            };
            chunk.add_sample(&sample).unwrap();

            let serialized = serialize_chunk(chunk.clone());
            let deserialized = deserialize_chunk(&serialized);

            assert_eq!(chunk, deserialized);
            assert_eq!(chunk.get_encoding(), deserialized.get_encoding());
            assert_eq!(chunk.len(), deserialized.len());
            assert_eq!(chunk.first_timestamp(), deserialized.first_timestamp());
            assert_eq!(chunk.last_timestamp(), deserialized.last_timestamp());

            let original_samples = chunk.get_range(0, i64::MAX).unwrap();
            let deserialized_samples = deserialized.get_range(0, i64::MAX).unwrap();
            assert_eq!(original_samples, deserialized_samples);
        }
    }

    #[test]
    fn test_timeseries_chunk_serialization_multiple_samples() {
        for &encoding in CHUNK_TYPES.iter() {
            let mut chunk = TimeSeriesChunk::new(encoding, 1024);
            let samples = generate_random_samples(50);

            for sample in &samples {
                chunk.add_sample(sample).unwrap();
            }

            let serialized = serialize_chunk(chunk.clone());
            let deserialized = deserialize_chunk(&serialized);

            assert_eq!(chunk, deserialized);
            assert_eq!(chunk.get_encoding(), deserialized.get_encoding());
            assert_eq!(chunk.len(), deserialized.len());
            assert_eq!(chunk.first_timestamp(), deserialized.first_timestamp());
            assert_eq!(chunk.last_timestamp(), deserialized.last_timestamp());

            let original_samples = chunk.get_range(0, i64::MAX).unwrap();
            let deserialized_samples = deserialized.get_range(0, i64::MAX).unwrap();
            assert_eq!(original_samples, deserialized_samples);
        }
    }

    #[test]
    fn test_timeseries_chunk_serialization_large_dataset() {
        for &encoding in CHUNK_TYPES.iter() {
            let mut chunk = TimeSeriesChunk::new(encoding, 16384); // Larger chunk size
            let samples = DataGenerator::builder()
                .samples(2000)
                .start(1000)
                .algorithm(ValueWorkload::MackeyGlass)
                .interval(Duration::from_millis(1000))
                .decimal_digits(3)
                .build()
                .generate();

            for sample in &samples {
                if let Err(TsdbError::CapacityFull(_)) = chunk.add_sample(sample) {
                    break; // Stop when chunk is full
                }
            }

            if chunk.is_empty() {
                continue;
            }

            let serialized = serialize_chunk(chunk.clone());

            let deserialized = deserialize_chunk(&serialized);

            assert_eq!(chunk, deserialized);
            assert_eq!(chunk.get_encoding(), deserialized.get_encoding());
            assert_eq!(chunk.len(), deserialized.len());

            let original_samples = chunk.get_range(0, i64::MAX).unwrap();
            let deserialized_samples = deserialized.get_range(0, i64::MAX).unwrap();
            assert_eq!(original_samples, deserialized_samples);
        }
    }
}
