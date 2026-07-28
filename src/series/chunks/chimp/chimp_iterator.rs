//! Sample-level ElfOnChimp iterator.
//!
//! Reads the interleaved timestamp/value records of a [`ChimpCompressor`] back
//! in order.

use super::compressor::{ChimpCompressor, ChimpDecompressor};
use crate::common::Sample;
use crate::error::{TsdbError, TsdbResult};

pub struct ChimpIterator<'a> {
    inner: ChimpDecompressor<'a>,
    idx: usize,
    num_samples: usize,
}

impl ChimpIterator<'_> {
    pub fn new(compressor: &'_ ChimpCompressor) -> ChimpIterator<'_> {
        let count = compressor.count();
        ChimpIterator {
            inner: ChimpDecompressor::new(compressor.bytes(), count),
            idx: 0,
            num_samples: count as usize,
        }
    }
}

impl Iterator for ChimpIterator<'_> {
    type Item = TsdbResult<Sample>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.num_samples {
            return None;
        }
        self.idx += 1;

        match self.inner.next_sample() {
            Ok(Some(sample)) => Some(Ok(sample)),
            // The decompressor is bounded by the same sample count, so this is
            // unreachable unless the two disagree.
            Ok(None) => Some(Err(TsdbError::DecodingError(
                "chimp: stream ended early".to_string(),
            ))),
            Err(err) => Some(Err(TsdbError::DecodingError(format!(
                "chimp: error decoding sample: {err}"
            )))),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.num_samples - self.idx;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for ChimpIterator<'_> {
    fn len(&self) -> usize {
        self.num_samples - self.idx
    }
}
