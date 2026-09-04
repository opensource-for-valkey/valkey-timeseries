use crate::common::{Sample, Timestamp};
use crate::iterators::SampleIter;
use crate::series::TimeSeries;
use crate::series::chunks::{ChunkOps, TimeSeriesChunk};
use crate::series::request_types::RangeOptions;

/// An iterator over samples in a TimeSeries within a specified timestamp range.
/// Supports both forward and reverse iteration.
pub struct SeriesSampleIterator<'a> {
    series: &'a TimeSeries,
    is_reverse: bool,
    /// Buffer for reverse iteration
    buffer: Vec<Sample>,
    /// Current position in buffer (for reverse iteration)
    buffer_pos: usize,
    /// Sample iterator for the current chunk
    sample_iter: SampleIter<'a>,
    chunk_index: usize,
    chunks_done: bool,
    start: Timestamp,
    end: Timestamp,
}

impl<'a> SeriesSampleIterator<'a> {
    pub fn from_range_options(
        series: &'a TimeSeries,
        options: &RangeOptions,
        is_reverse: bool,
    ) -> Self {
        let (start, end) = options.get_timestamp_range();
        let start = start.max(series.get_min_timestamp());
        Self::new(series, start, end, is_reverse)
    }

    pub(crate) fn new(
        series: &'a TimeSeries,
        start: Timestamp,
        end: Timestamp,
        is_reverse: bool,
    ) -> Self {
        // Match TimestampRange semantics: an inverted window contains no samples.
        // Marking it done here prevents every iterator caller from having to rely
        // on its underlying chunk encoding to handle the range safely.
        let chunks_done = series.chunks.is_empty() || start > end;
        let chunk_index: usize = if is_reverse && !series.is_empty() {
            series.chunks.len() - 1
        } else {
            0
        };

        let mut this = Self {
            series,
            start,
            end,
            chunk_index,
            chunks_done,
            sample_iter: Default::default(),
            is_reverse,
            buffer: Vec::new(),
            buffer_pos: 0,
        };

        this.load_next_chunk();

        this
    }

    fn next_chunk(&mut self) -> Option<&'a TimeSeriesChunk> {
        if self.chunks_done {
            return None;
        }
        if self.is_reverse {
            loop {
                let chunk = &self.series.chunks[self.chunk_index];
                if chunk.last_timestamp() < self.start {
                    self.chunks_done = true;
                    return None;
                }
                let current_idx = self.chunk_index;
                if self.chunk_index == 0 {
                    self.chunks_done = true;
                } else {
                    self.chunk_index -= 1;
                }
                if chunk.first_timestamp() <= self.end && !chunk.is_empty() {
                    return Some(chunk);
                }
                if current_idx == 0 {
                    return None;
                }
            }
        } else {
            let len = self.series.chunks.len();
            while self.chunk_index < len {
                let chunk = &self.series.chunks[self.chunk_index];
                if chunk.first_timestamp() > self.end {
                    self.chunk_index = len + 1;
                    self.chunks_done = true;
                    return None;
                }
                self.chunk_index += 1;
                if chunk.last_timestamp() >= self.start && !chunk.is_empty() {
                    return Some(chunk);
                }
            }
            self.chunks_done = true;
            None
        }
    }

    fn fill_buffer_reverse(&mut self, chunk: &TimeSeriesChunk) -> bool {
        let end_ts = chunk.last_timestamp().min(self.end);
        let start_ts = chunk.first_timestamp().max(self.start);

        self.buffer.clear();
        self.buffer_pos = 0;

        if start_ts > end_ts {
            return false;
        }

        // Reuse existing buffer capacity
        if let TimeSeriesChunk::Uncompressed(c) = chunk {
            if let Some(slice) = c.get_range_as_ref(start_ts, end_ts) {
                self.buffer.extend_from_slice(slice);
            }
        } else {
            let it = chunk.range_iter(start_ts, end_ts);
            self.buffer.extend(it);
        }
        self.buffer_pos = self.buffer.len();

        self.buffer_pos > 0
    }

    fn next_reverse(&mut self) -> Option<Sample> {
        if self.buffer_pos > 0 {
            self.buffer_pos -= 1;
            Some(self.buffer[self.buffer_pos])
        } else {
            None
        }
    }

    fn load_next_chunk(&mut self) -> bool {
        let Some(chunk) = self.next_chunk() else {
            return false;
        };

        if self.is_reverse {
            // we could have an empty buffer because of filtering
            self.fill_buffer_reverse(chunk);
        } else {
            self.sample_iter = chunk.range_iter(self.start, self.end);
        }

        true
    }
}

impl Iterator for SeriesSampleIterator<'_> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.is_reverse {
                if let Some(sample) = self.next_reverse() {
                    return Some(sample);
                }
            } else if let Some(sample) = self.sample_iter.next() {
                return Some(sample);
            }

            if !self.load_next_chunk() {
                return None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{Sample, Timestamp};
    use crate::series::TimeSeries;
    use crate::series::request_types::RangeOptions;

    // Helper to build a series from timestamp/value pairs.
    // Assumes `TimeSeries::new()` and `append_sample(sample: Sample)` exist.
    fn build_series(pairs: &[(Timestamp, f64)]) -> TimeSeries {
        let mut series = TimeSeries::new();
        for &(ts, val) in pairs {
            series.add(ts, val, None);
        }
        series
    }

    fn create_range(start: Timestamp, end: Timestamp) -> RangeOptions {
        RangeOptions::with_range(start, end).unwrap()
    }

    #[test]
    fn forward_iteration_returns_expected_samples() {
        let data = vec![(10, 1.0), (20, 2.0), (30, 3.0), (40, 4.0), (50, 5.0)];
        let series = build_series(&data);

        let opts = create_range(20, 40);
        let it = SeriesSampleIterator::from_range_options(&series, &opts, false);

        let got: Vec<Sample> = it.collect();
        let expected: Vec<Sample> = data
            .into_iter()
            .filter(|&(ts, _)| (20..=40).contains(&ts))
            .map(|(ts, v)| Sample {
                timestamp: ts,
                value: v,
            })
            .collect();

        assert_eq!(got, expected);
    }

    #[test]
    fn reverse_iteration_returns_expected_samples() {
        let data = vec![(100, 10.0), (200, 20.0), (300, 30.0)];
        let series = build_series(&data);

        let opts = create_range(0, 1000);
        let it = SeriesSampleIterator::from_range_options(&series, &opts, true);

        let got: Vec<Sample> = it.collect();
        let mut expected: Vec<Sample> = data
            .into_iter()
            .map(|(ts, v)| Sample {
                timestamp: ts,
                value: v,
            })
            .collect();
        expected.reverse();

        assert_eq!(got, expected);
    }

    #[test]
    fn empty_series_returns_no_samples() {
        let series = TimeSeries::new();
        let opts = create_range(0, 100);
        let mut it = SeriesSampleIterator::from_range_options(&series, &opts, false);
        assert_eq!(it.next(), None);

        let mut it_rev = SeriesSampleIterator::from_range_options(&series, &opts, true);
        assert_eq!(it_rev.next(), None);
    }

    #[test]
    fn forward_iteration_with_partial_range_overlap() {
        let data = vec![(10, 1.0), (20, 2.0), (30, 3.0), (40, 4.0)];
        let series = build_series(&data);

        // Range starts before the first sample
        let opts = create_range(5, 25);
        let got: Vec<Sample> =
            SeriesSampleIterator::from_range_options(&series, &opts, false).collect();
        let expected: Vec<Sample> = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 20,
                value: 2.0,
            },
        ];
        assert_eq!(got, expected);
    }

    #[test]
    fn reverse_iteration_with_partial_range_overlap() {
        let data = vec![(10, 1.0), (20, 2.0), (30, 3.0), (40, 4.0)];
        let series = build_series(&data);

        let opts = create_range(15, 35);
        let got: Vec<Sample> =
            SeriesSampleIterator::from_range_options(&series, &opts, true).collect();
        let expected: Vec<Sample> = vec![
            Sample {
                timestamp: 30,
                value: 3.0,
            },
            Sample {
                timestamp: 20,
                value: 2.0,
            },
        ];
        assert_eq!(got, expected);
    }

    #[test]
    fn range_with_no_matching_samples() {
        let data = vec![(10, 1.0), (20, 2.0), (30, 3.0)];
        let series = build_series(&data);

        // Range completely outside data
        let opts = create_range(50, 100);
        let got: Vec<Sample> =
            SeriesSampleIterator::from_range_options(&series, &opts, false).collect();
        assert!(got.is_empty());

        // Reverse iteration with no matches
        let got_rev: Vec<Sample> =
            SeriesSampleIterator::from_range_options(&series, &opts, true).collect();
        assert!(got_rev.is_empty());
    }

    #[test]
    fn range_before_all_samples() {
        let data = vec![(100, 1.0), (200, 2.0)];
        let series = build_series(&data);

        let opts = create_range(10, 50);
        let got: Vec<Sample> =
            SeriesSampleIterator::from_range_options(&series, &opts, false).collect();
        assert!(got.is_empty());
    }

    #[test]
    fn range_after_all_samples() {
        let data = vec![(10, 1.0), (20, 2.0)];
        let series = build_series(&data);

        let opts = create_range(100, 200);
        let got: Vec<Sample> =
            SeriesSampleIterator::from_range_options(&series, &opts, false).collect();
        assert!(got.is_empty());
    }

    #[test]
    fn single_sample_in_range() {
        let data = vec![(10, 1.0), (20, 2.0), (30, 3.0)];
        let series = build_series(&data);

        let opts = create_range(20, 20);
        let got: Vec<Sample> =
            SeriesSampleIterator::from_range_options(&series, &opts, false).collect();
        let expected = vec![Sample {
            timestamp: 20,
            value: 2.0,
        }];
        assert_eq!(got, expected);
    }

    #[test]
    fn reverse_iteration_single_sample() {
        let data = vec![(10, 1.0), (20, 2.0), (30, 3.0)];
        let series = build_series(&data);

        let opts = create_range(20, 20);
        let got: Vec<Sample> =
            SeriesSampleIterator::from_range_options(&series, &opts, true).collect();
        let expected = vec![Sample {
            timestamp: 20,
            value: 2.0,
        }];
        assert_eq!(got, expected);
    }

    #[test]
    fn exact_boundary_match_forward() {
        let data = vec![(10, 1.0), (20, 2.0), (30, 3.0)];
        let series = build_series(&data);

        let opts = create_range(10, 30);
        let got: Vec<Sample> =
            SeriesSampleIterator::from_range_options(&series, &opts, false).collect();
        let expected: Vec<Sample> = data
            .iter()
            .map(|&(ts, v)| Sample {
                timestamp: ts,
                value: v,
            })
            .collect();
        assert_eq!(got, expected);
    }

    #[test]
    fn exact_boundary_match_reverse() {
        let data = vec![(10, 1.0), (20, 2.0), (30, 3.0)];
        let series = build_series(&data);

        let opts = create_range(10, 30);
        let got: Vec<Sample> =
            SeriesSampleIterator::from_range_options(&series, &opts, true).collect();
        let mut expected: Vec<Sample> = data
            .iter()
            .map(|&(ts, v)| Sample {
                timestamp: ts,
                value: v,
            })
            .collect();
        expected.reverse();
        assert_eq!(got, expected);
    }

    #[test]
    fn large_dataset_forward() {
        let data: Vec<(Timestamp, f64)> = (0..1000).map(|i| (i * 10, i as f64)).collect();
        let series = build_series(&data);

        let opts = create_range(2500, 7500);
        let got: Vec<Sample> =
            SeriesSampleIterator::from_range_options(&series, &opts, false).collect();

        let expected: Vec<Sample> = data
            .into_iter()
            .filter(|&(ts, _)| (2500..=7500).contains(&ts))
            .map(|(ts, v)| Sample {
                timestamp: ts,
                value: v,
            })
            .collect();

        assert_eq!(got.len(), expected.len());
        assert_eq!(got, expected);
    }

    #[test]
    fn large_dataset_reverse() {
        let data: Vec<(Timestamp, f64)> = (0..1000).map(|i| (i * 10, i as f64)).collect();
        let series = build_series(&data);

        let opts = create_range(2500, 7500);
        let got: Vec<Sample> =
            SeriesSampleIterator::from_range_options(&series, &opts, true).collect();

        let mut expected: Vec<Sample> = data
            .into_iter()
            .filter(|&(ts, _)| (2500..=7500).contains(&ts))
            .map(|(ts, v)| Sample {
                timestamp: ts,
                value: v,
            })
            .collect();
        expected.reverse();

        assert_eq!(got.len(), expected.len());
        assert_eq!(got, expected);
    }

    #[test]
    fn retention_clips_start_boundary() {
        let data = vec![(100, 1.0), (200, 2.0), (300, 3.0), (400, 4.0)];
        let series = build_series(&data);

        // Request range 0-500, but retention should clip to min_timestamp
        let opts = create_range(0, 500);
        let got: Vec<Sample> =
            SeriesSampleIterator::from_range_options(&series, &opts, false).collect();

        // All samples should be returned since min_timestamp is 100
        let expected: Vec<Sample> = data
            .iter()
            .map(|&(ts, v)| Sample {
                timestamp: ts,
                value: v,
            })
            .collect();

        assert_eq!(got, expected);
    }

    #[test]
    fn zero_length_range() {
        let data = vec![(10, 1.0), (20, 2.0)];
        let series = build_series(&data);

        let opts = create_range(15, 15);
        let got: Vec<Sample> =
            SeriesSampleIterator::from_range_options(&series, &opts, false).collect();
        assert!(got.is_empty());
    }

    mod next_chunk_tests {
        use super::*;
        use crate::series::chunks::{TimeSeriesChunk, UncompressedChunk};

        /// Helper to create a chunk with given samples
        fn create_chunk(samples: &[(Timestamp, f64)]) -> TimeSeriesChunk {
            let samples = samples
                .iter()
                .map(|(t, v)| Sample::new(*t, *v))
                .collect::<Vec<Sample>>();
            let uncompressed = UncompressedChunk::from_vec(samples);
            TimeSeriesChunk::Uncompressed(uncompressed)
        }

        /// Helper to build a series from pre-built chunks
        fn build_series_from_chunks(chunks: Vec<TimeSeriesChunk>) -> TimeSeries {
            TimeSeries::from_chunks(chunks).unwrap()
        }

        #[test]
        fn next_chunk_forward_returns_chunks_in_order() {
            let chunk1 = create_chunk(&[(10, 1.0), (20, 2.0)]);
            let chunk2 = create_chunk(&[(30, 3.0), (40, 4.0)]);
            let chunk3 = create_chunk(&[(50, 5.0), (60, 6.0)]);

            let series = build_series_from_chunks(vec![chunk1, chunk2, chunk3]);

            let iter = SeriesSampleIterator::new(&series, 0, 100, false);

            // Collect all samples - this exercises next_chunk internally
            let samples: Vec<Sample> = iter.collect();

            assert_eq!(samples.len(), 6);
            assert_eq!(samples[0].timestamp, 10);
            assert_eq!(samples[2].timestamp, 30);
            assert_eq!(samples[4].timestamp, 50);
        }

        #[test]
        fn next_chunk_reverse_returns_chunks_in_reverse_order() {
            let chunk1 = create_chunk(&[(10, 1.0), (20, 2.0)]);
            let chunk2 = create_chunk(&[(30, 3.0), (40, 4.0)]);
            let chunk3 = create_chunk(&[(50, 5.0), (60, 6.0)]);

            let series = build_series_from_chunks(vec![chunk1, chunk2, chunk3]);

            let iter = SeriesSampleIterator::new(&series, 0, 100, true);

            let samples: Vec<Sample> = iter.collect();

            assert_eq!(samples.len(), 6);
            // The first sample should be from the last chunk (reverse order)
            assert_eq!(samples[0].timestamp, 60);
            assert_eq!(samples[1].timestamp, 50);
            assert_eq!(samples[2].timestamp, 40);
            assert_eq!(samples[5].timestamp, 10);
        }

        #[test]
        fn next_chunk_forward_skips_chunks_before_range() {
            let chunk1 = create_chunk(&[(10, 1.0), (20, 2.0)]);
            let chunk2 = create_chunk(&[(30, 3.0), (40, 4.0)]);
            let chunk3 = create_chunk(&[(50, 5.0), (60, 6.0)]);

            let series = build_series_from_chunks(vec![chunk1, chunk2, chunk3]);

            // Range starts at 35, should skip chunk1 entirely
            let iter = SeriesSampleIterator::new(&series, 35, 100, false);
            let samples: Vec<Sample> = iter.collect();

            assert_eq!(samples.len(), 3);
            assert_eq!(samples[0].timestamp, 40);
            assert_eq!(samples[1].timestamp, 50);
            assert_eq!(samples[2].timestamp, 60);
        }

        #[test]
        fn next_chunk_forward_skips_chunks_after_range() {
            let chunk1 = create_chunk(&[(10, 1.0), (20, 2.0)]);
            let chunk2 = create_chunk(&[(30, 3.0), (40, 4.0)]);
            let chunk3 = create_chunk(&[(50, 5.0), (60, 6.0)]);

            let series = build_series_from_chunks(vec![chunk1, chunk2, chunk3]);

            // Range ends at 25, should skip chunk2 and chunk3
            let iter = SeriesSampleIterator::new(&series, 0, 25, false);
            let samples: Vec<Sample> = iter.collect();

            assert_eq!(samples.len(), 2);
            assert_eq!(samples[0].timestamp, 10);
            assert_eq!(samples[1].timestamp, 20);
        }

        #[test]
        fn next_chunk_reverse_skips_chunks_after_range() {
            let chunk1 = create_chunk(&[(10, 1.0), (20, 2.0)]);
            let chunk2 = create_chunk(&[(30, 3.0), (40, 4.0)]);
            let chunk3 = create_chunk(&[(50, 5.0), (60, 6.0)]);

            let series = build_series_from_chunks(vec![chunk1, chunk2, chunk3]);

            // Range ends at 35, should skip chunk3 in reverse iteration
            let iter = SeriesSampleIterator::new(&series, 0, 35, true);
            let samples: Vec<Sample> = iter.collect();

            assert_eq!(samples.len(), 3);
            assert_eq!(samples[0].timestamp, 30);
            assert_eq!(samples[1].timestamp, 20);
            assert_eq!(samples[2].timestamp, 10);
        }

        #[test]
        fn next_chunk_reverse_skips_chunks_before_range() {
            let chunk1 = create_chunk(&[(10, 1.0), (20, 2.0)]);
            let chunk2 = create_chunk(&[(30, 3.0), (40, 4.0)]);
            let chunk3 = create_chunk(&[(50, 5.0), (60, 6.0)]);

            let series = build_series_from_chunks(vec![chunk1, chunk2, chunk3]);

            // Range starts at 45, should skip chunk1 and chunk2 in reverse
            let iter = SeriesSampleIterator::new(&series, 45, 100, true);
            let samples: Vec<Sample> = iter.collect();

            assert_eq!(samples.len(), 2);
            assert_eq!(samples[0].timestamp, 60);
            assert_eq!(samples[1].timestamp, 50);
        }

        #[test]
        fn next_chunk_with_single_chunk() {
            let chunk = create_chunk(&[(100, 10.0), (200, 20.0), (300, 30.0)]);
            let series = build_series_from_chunks(vec![chunk]);

            // Forward
            let iter = SeriesSampleIterator::new(&series, 0, 500, false);
            let samples: Vec<Sample> = iter.collect();
            assert_eq!(samples.len(), 3);

            // Reverse
            let iter = SeriesSampleIterator::new(&series, 0, 500, true);
            let samples: Vec<Sample> = iter.collect();
            assert_eq!(samples.len(), 3);
            assert_eq!(samples[0].timestamp, 300);
        }

        #[test]
        fn next_chunk_range_spans_partial_chunks() {
            let chunk1 = create_chunk(&[(10, 1.0), (20, 2.0), (30, 3.0)]);
            let chunk2 = create_chunk(&[(40, 4.0), (50, 5.0), (60, 6.0)]);
            let chunk3 = create_chunk(&[(70, 7.0), (80, 8.0), (90, 9.0)]);

            let series = build_series_from_chunks(vec![chunk1, chunk2, chunk3]);

            // Range 25-75 should include partial data from all three chunks
            let iter = SeriesSampleIterator::new(&series, 25, 75, false);
            let samples: Vec<Sample> = iter.collect();

            assert_eq!(samples.len(), 5);
            assert_eq!(samples[0].timestamp, 30);
            assert_eq!(samples[1].timestamp, 40);
            assert_eq!(samples[2].timestamp, 50);
            assert_eq!(samples[3].timestamp, 60);
            assert_eq!(samples[4].timestamp, 70);
        }

        #[test]
        fn next_chunk_range_spans_partial_chunks_reverse() {
            let chunk1 = create_chunk(&[(10, 1.0), (20, 2.0), (30, 3.0)]);
            let chunk2 = create_chunk(&[(40, 4.0), (50, 5.0), (60, 6.0)]);
            let chunk3 = create_chunk(&[(70, 7.0), (80, 8.0), (90, 9.0)]);

            let series = build_series_from_chunks(vec![chunk1, chunk2, chunk3]);

            let iter = SeriesSampleIterator::new(&series, 25, 75, true);
            let samples: Vec<Sample> = iter.collect();

            assert_eq!(samples.len(), 5);
            assert_eq!(samples[0].timestamp, 70);
            assert_eq!(samples[1].timestamp, 60);
            assert_eq!(samples[2].timestamp, 50);
            assert_eq!(samples[3].timestamp, 40);
            assert_eq!(samples[4].timestamp, 30);
        }

        #[test]
        fn next_chunk_with_gap_between_chunks() {
            // Chunks with gaps in timestamps
            let chunk1 = create_chunk(&[(10, 1.0), (20, 2.0)]);
            let chunk2 = create_chunk(&[(100, 10.0), (110, 11.0)]);
            let chunk3 = create_chunk(&[(500, 50.0), (510, 51.0)]);

            let series = build_series_from_chunks(vec![chunk1, chunk2, chunk3]);

            // Range that falls in the gap
            let iter = SeriesSampleIterator::new(&series, 50, 90, false);
            let samples: Vec<Sample> = iter.collect();
            assert!(samples.is_empty());

            // Range that spans the gap
            let iter = SeriesSampleIterator::new(&series, 15, 105, false);
            let samples: Vec<Sample> = iter.collect();
            assert_eq!(samples.len(), 2);
            assert_eq!(samples[0].timestamp, 20);
            assert_eq!(samples[1].timestamp, 100);
        }

        #[test]
        fn next_chunk_range_exactly_matches_chunk_boundaries() {
            let chunk1 = create_chunk(&[(10, 1.0), (20, 2.0)]);
            let chunk2 = create_chunk(&[(30, 3.0), (40, 4.0)]);

            let series = build_series_from_chunks(vec![chunk1, chunk2]);

            // Range exactly matches chunk2 boundaries
            let iter = SeriesSampleIterator::new(&series, 30, 40, false);
            let samples: Vec<Sample> = iter.collect();

            assert_eq!(samples.len(), 2);
            assert_eq!(samples[0].timestamp, 30);
            assert_eq!(samples[1].timestamp, 40);
        }

        #[test]
        fn next_chunk_forward_stops_at_end_boundary() {
            let chunk1 = create_chunk(&[(10, 1.0)]);
            let chunk2 = create_chunk(&[(20, 2.0)]);
            let chunk3 = create_chunk(&[(30, 3.0)]);
            let chunk4 = create_chunk(&[(40, 4.0)]);

            let series = build_series_from_chunks(vec![chunk1, chunk2, chunk3, chunk4]);

            // End at 25 - should only get chunks 1 and 2
            let iter = SeriesSampleIterator::new(&series, 0, 25, false);
            let samples: Vec<Sample> = iter.collect();

            assert_eq!(samples.len(), 2);
            assert_eq!(samples[0].timestamp, 10);
            assert_eq!(samples[1].timestamp, 20);
        }

        #[test]
        fn next_chunk_reverse_stops_at_start_boundary() {
            let chunk1 = create_chunk(&[(10, 1.0)]);
            let chunk2 = create_chunk(&[(20, 2.0)]);
            let chunk3 = create_chunk(&[(30, 3.0)]);
            let chunk4 = create_chunk(&[(40, 4.0)]);

            let series = build_series_from_chunks(vec![chunk1, chunk2, chunk3, chunk4]);

            // Start at 25 - should only get chunks 3 and 4 in reverse
            let iter = SeriesSampleIterator::new(&series, 25, 100, true);
            let samples: Vec<Sample> = iter.collect();

            assert_eq!(samples.len(), 2);
            assert_eq!(samples[0].timestamp, 40);
            assert_eq!(samples[1].timestamp, 30);
        }

        #[test]
        fn next_chunk_many_small_chunks() {
            let chunks: Vec<TimeSeriesChunk> = (0..10)
                .map(|i| create_chunk(&[(i * 10, i as f64)]))
                .collect();

            let series = build_series_from_chunks(chunks);

            let iter = SeriesSampleIterator::new(&series, 0, 100, false);
            let samples: Vec<Sample> = iter.collect();

            assert_eq!(samples.len(), 10);
            for (i, sample) in samples.iter().enumerate() {
                assert_eq!(sample.timestamp, (i * 10) as Timestamp);
            }
        }

        #[test]
        fn next_chunk_many_small_chunks_reverse() {
            let chunks: Vec<TimeSeriesChunk> = (0..10)
                .map(|i| create_chunk(&[(i * 10, i as f64)]))
                .collect();

            let series = build_series_from_chunks(chunks);

            let iter = SeriesSampleIterator::new(&series, 0, 100, true);
            let samples: Vec<Sample> = iter.collect();

            assert_eq!(samples.len(), 10);
            for (i, sample) in samples.iter().enumerate() {
                assert_eq!(sample.timestamp, ((9 - i) * 10) as Timestamp);
            }
        }
    }
}
