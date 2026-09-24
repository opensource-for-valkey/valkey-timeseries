use crate::common::{Sample, Timestamp};
use crate::iterators::SampleIter;
use crate::series::TimeSeries;
use crate::series::chunks::{ChunkOps, TimeSeriesChunk};
use smallvec::SmallVec;
use std::borrow::Cow;

/// Specialized iterator to handle timestamp filtering for a time series. For each timestamp it
/// returns a `sample` if a sample with an exact timestamp match exists in the series,
/// Timestamps are iterated in the order determined by `is_reverse`.
///
/// This iterator is optimized for sparse timestamp lookups over a time series. It processes only relevant
/// chunks and reuses chunk lookup state to avoid redundant searches across chunks.
///
/// ### Example
/// Imagine we have a time series with 12 chunks, each with 500 samples (6000 in total).
/// If we want to filter by the following timestamps:
///
/// `[150, 250, 750, 1250, 1750, 2250, 2750, 3500, 4500, 5500]`
///
/// The iterator will efficiently seek to the relevant chunks for each timestamp:
/// - Timestamps 150 and 250 are found in chunk 1.
/// - Timestamp 750 is found in chunk 2.
/// - Timestamps 1250, 1750, 2250, and 2750 are found in chunks 3, 4, 5, and 6 respectively.
/// - Timestamp 3500 is found in chunk 8.
/// - Timestamps 4500 and 5500 are found in chunks 10 and 11 respectively.
///
/// This way, the iterator avoids unnecessary scans of chunks that do not contain any of the requested timestamps,
/// making it efficient for sparse lookups.
pub struct TimestampFilterIterator<'a> {
    series: &'a TimeSeries,
    chunk: Option<&'a TimeSeriesChunk>,
    sample_iter: SampleIter<'a>,
    timestamps: SmallVec<[Timestamp; 16]>,
    is_reverse: bool,
    /// reusable sample buffer for reverse iteration
    buffer: Cow<'a, [Sample]>,
    /// index into timestamps vector for next output
    pos: usize,
    /// chunk search hint/current index
    chunk_idx: isize,
    random_access: bool,
}

impl<'a> TimestampFilterIterator<'a> {
    /// Create a new iterator.
    /// - `chunks` should be the series chunks in chronological order (earliest -> latest).
    /// - `timestamps` is the list of timestamps to look up.
    /// - `is_reverse` controls whether iteration goes from the end of `timestamps` to the start.
    pub fn new(series: &'a TimeSeries, timestamps: &[Timestamp], is_reverse: bool) -> Self {
        let mut timestamps: SmallVec<[Timestamp; 16]> = SmallVec::from(timestamps);
        timestamps.sort_unstable();
        timestamps.dedup();
        // Expired samples the write path has not trimmed yet are not part of the series.
        let min_timestamp = series.get_min_timestamp();
        timestamps.retain(|ts| *ts >= min_timestamp);

        let chunk_idx = if series.is_empty() {
            -1
        } else if is_reverse {
            (series.chunks.len() - 1) as isize
        } else {
            0
        };
        let pos = if is_reverse { timestamps.len() } else { 0 };

        Self {
            series,
            chunk: None,
            sample_iter: Default::default(),
            timestamps,
            is_reverse,
            pos,
            chunk_idx,
            buffer: Cow::Owned(Vec::new()),
            random_access: false,
        }
    }

    fn pop_timestamp(&mut self) -> Option<Timestamp> {
        if self.is_reverse {
            if self.pos == 0 {
                return None;
            }
            self.pos -= 1;
            Some(self.timestamps[self.pos])
        } else {
            let ts = *self.timestamps.get(self.pos)?;
            self.pos += 1;
            Some(ts)
        }
    }

    fn seek_chunk(&mut self, ts: Timestamp) -> Option<&'a TimeSeriesChunk> {
        let chunks = &self.series.chunks;
        loop {
            let idx = self.chunk_idx;
            if idx < 0 {
                return None;
            }
            let idx = idx as usize;
            let chunk = chunks.get(idx)?;
            if ts < chunk.first_timestamp() {
                if self.is_reverse {
                    self.chunk_idx -= 1;
                    continue;
                }
                return None;
            }
            if ts > chunk.last_timestamp() {
                if self.is_reverse {
                    return None;
                }
                self.chunk_idx += 1;
                continue;
            }
            return Some(chunk);
        }
    }

    // fill the buffer with samples from the given chunk for random-access lookups
    fn fill_buffer(&mut self, chunk: &'a TimeSeriesChunk) {
        if let TimeSeriesChunk::Uncompressed(chunk_) = chunk {
            self.buffer = Cow::Borrowed(&chunk_.samples);
            return;
        }

        let it = chunk.range_iter(chunk.first_timestamp(), chunk.last_timestamp());

        if let Cow::Owned(ref mut buf) = self.buffer {
            buf.clear();
            buf.extend(it);
        } else {
            let mut buf = Vec::with_capacity(chunk.len());
            buf.extend(it);
            self.buffer = Cow::Owned(buf);
        }
    }

    fn ensure_chunk_for_timestamp(&mut self, ts: Timestamp) -> bool {
        if let Some(chunk) = self.chunk
            && chunk.is_timestamp_in_range(ts)
        {
            // the current chunk covers the timestamp
            return true;
        }

        if let Some(chunk) = self.seek_chunk(ts) {
            if self.is_reverse || !chunk.is_compressed() {
                self.random_access = true;
                self.sample_iter = SampleIter::Empty;
                self.fill_buffer(chunk);
            } else {
                self.random_access = false;
                self.sample_iter = chunk.range_iter(ts, chunk.last_timestamp());
            }

            self.chunk = Some(chunk);
            return true;
        }

        self.chunk = None;
        false
    }

    fn get_sample(&mut self, ts: Timestamp) -> Option<Sample> {
        if !self.ensure_chunk_for_timestamp(ts) {
            return None;
        }

        if self.random_access {
            if let Ok(idx) = self.buffer.binary_search_by_key(&ts, |s| s.timestamp)
                && let Some(sample) = self.buffer.get(idx).copied()
            {
                return Some(sample);
            }
            return None;
        }

        for sample in self.sample_iter.by_ref() {
            if sample.timestamp == ts {
                return Some(sample);
            } else if sample.timestamp > ts {
                break;
            }
        }
        self.chunk = None;
        None
    }
}

impl<'a> Iterator for TimestampFilterIterator<'a> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(ts) = self.pop_timestamp() {
            if let Some(sample) = self.get_sample(ts) {
                return Some(sample);
            }
        }
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.timestamps.len()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Sample;
    use crate::series::TimeSeries;
    use crate::series::chunks::{GorillaChunk, TimeSeriesChunk, UncompressedChunk};

    // helper to create a sample
    fn s(ts: Timestamp, v: f64) -> Sample {
        Sample {
            timestamp: ts,
            value: v,
        }
    }

    // Build a series with two uncompressed chunks:
    // chunk1: timestamps 1..=3, chunk2: timestamps 5..=7
    fn build_series() -> TimeSeries {
        let c1_samples = vec![s(1, 10.0), s(2, 20.0), s(3, 30.0)];
        let c2_samples = vec![s(5, 50.0), s(6, 60.0), s(7, 70.0)];

        let c1 = TimeSeriesChunk::Uncompressed(UncompressedChunk::from_vec(c1_samples));
        let c2 = TimeSeriesChunk::Uncompressed(UncompressedChunk::from_vec(c2_samples));

        TimeSeries::from_chunks(vec![c1, c2]).unwrap()
    }

    #[test]
    fn finds_exact_matches_forward() {
        let series = build_series();
        // timestamps include matches and a missing entry (4)
        let timestamps = &[1, 4, 6];
        let it = TimestampFilterIterator::new(&series, timestamps, false);

        let out: Vec<Sample> = it.collect();
        // Expect samples for 1 and 6 only
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].timestamp, 1);
        assert_eq!(out[1].timestamp, 6);
    }

    #[test]
    fn finds_exact_matches_reverse() {
        let series = build_series();
        let timestamps = &[2, 5, 7];
        let it = TimestampFilterIterator::new(&series, timestamps, true);

        let out: Vec<Sample> = it.collect();
        // Expect samples for 7,5,2 in that order
        let ts: Vec<Timestamp> = out.into_iter().map(|s| s.timestamp).collect();
        assert_eq!(ts, vec![7, 5, 2]);
    }

    #[test]
    fn missing_timestamps_are_skipped() {
        let series = build_series();
        let timestamps = &[0, 4, 8]; // none exist
        let mut it = TimestampFilterIterator::new(&series, timestamps, false);
        assert!(it.next().is_none());
    }

    #[test]
    fn reuses_chunk_lookup_hint_across_timestamps() {
        let series = build_series();
        // timestamps mostly increasing, ensures iterator moves forward across chunks
        let timestamps = &[1, 2, 3, 5, 6, 7];
        let mut it = TimestampFilterIterator::new(&series, timestamps, false);

        // consume partially to exercise chunk switching
        assert_eq!(it.next().map(|s| s.timestamp), Some(1));
        assert_eq!(it.next().map(|s| s.timestamp), Some(2));
        assert_eq!(it.next().map(|s| s.timestamp), Some(3));
        // chunk hint should advance to the second chunk and continue
        assert_eq!(it.next().map(|s| s.timestamp), Some(5));
        assert_eq!(it.next().map(|s| s.timestamp), Some(6));
        assert_eq!(it.next().map(|s| s.timestamp), Some(7));
        assert!(it.next().is_none());
    }

    fn build_compressed_series() -> TimeSeries {
        let c1_samples = vec![s(1, 10.0), s(2, 20.0), s(3, 30.0)];
        let c2_samples = vec![s(5, 50.0), s(6, 60.0), s(7, 70.0)];

        // Compress chunks using Gorilla encoding
        let mut c1 = TimeSeriesChunk::Gorilla(GorillaChunk::default());
        let mut c2 = TimeSeriesChunk::Gorilla(GorillaChunk::default());

        c1.set_data(&c1_samples).unwrap();
        c2.set_data(&c2_samples).unwrap();

        TimeSeries::from_chunks(vec![c1, c2]).unwrap()
    }

    #[test]
    fn finds_exact_matches_forward_compressed() {
        let series = build_compressed_series();
        let timestamps = &[1, 4, 6];
        let it = TimestampFilterIterator::new(&series, timestamps, false);

        let out: Vec<Sample> = it.collect();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].timestamp, 1);
        assert_eq!(out[1].timestamp, 6);

        // ensure that we can find consecutive timestamps
        let timestamps = &[2, 3];
        let it = TimestampFilterIterator::new(&series, timestamps, false);
        let out: Vec<Sample> = it.collect();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].timestamp, 2);
        assert_eq!(out[1].timestamp, 3);
    }

    #[test]
    fn finds_exact_matches_reverse_compressed() {
        let series = build_compressed_series();
        let timestamps = &[2, 5, 7];
        let it = TimestampFilterIterator::new(&series, timestamps, true);

        let out: Vec<Sample> = it.collect();
        let ts: Vec<Timestamp> = out.into_iter().map(|s| s.timestamp).collect();
        assert_eq!(ts, vec![7, 5, 2]);
    }

    #[test]
    fn missing_timestamps_are_skipped_compressed() {
        let series = build_compressed_series();
        let timestamps = &[0, 4, 8];
        let mut it = TimestampFilterIterator::new(&series, timestamps, false);
        assert!(it.next().is_none());
    }

    #[test]
    fn reuses_chunk_lookup_hint_across_timestamps_compressed() {
        let series = build_compressed_series();
        let timestamps = &[1, 2, 3, 5, 6, 7];
        let mut it = TimestampFilterIterator::new(&series, timestamps, false);

        assert_eq!(it.next().map(|s| s.timestamp), Some(1));
        assert_eq!(it.next().map(|s| s.timestamp), Some(2));
        assert_eq!(it.next().map(|s| s.timestamp), Some(3));
        assert_eq!(it.next().map(|s| s.timestamp), Some(5));
        assert_eq!(it.next().map(|s| s.timestamp), Some(6));
        assert_eq!(it.next().map(|s| s.timestamp), Some(7));
        assert!(it.next().is_none());
    }
}
