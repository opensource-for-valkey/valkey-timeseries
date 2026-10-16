//! Range filtering shared by the compressed chunk encodings.
//!
//! The codec iterators decode every sample from the start of the chunk and report decode
//! errors in-band. [`RangeFiltered`] turns that into the plain `Sample` stream a range read
//! wants: samples before `start` are skipped, and the stream ends at the first sample past
//! `end` or at the first decode error.

use crate::common::{Sample, Timestamp};
use crate::error::TsdbResult;

/// Yields the samples of `inner` with timestamps in `[start, end]`. Returns `None` at the first
/// sample past `end` or the first decode error (logged in debug builds); like the per-codec
/// wrappers it replaces it is not fused, so callers stop at the first `None`.
///
/// The loop shape mirrors those wrappers on purpose: a variant with a `done` flag measured
/// 5-19 % slower on Chimp decode.
pub struct RangeFiltered<I> {
    inner: I,
    start: Timestamp,
    end: Timestamp,
    started: bool,
}

impl<I> RangeFiltered<I> {
    pub fn new(inner: I, start: Timestamp, end: Timestamp) -> Self {
        Self {
            inner,
            start,
            end,
            started: false,
        }
    }
}

impl<I: Iterator<Item = TsdbResult<Sample>>> RangeFiltered<I> {
    /// The next decoded sample at or before `end`; `None` past `end`, on a decode error, or
    /// when the codec runs out.
    fn next_in_window(&mut self) -> Option<Sample> {
        match self.inner.next() {
            Some(Ok(sample)) => {
                if sample.timestamp > self.end {
                    return None;
                }
                Some(sample)
            }
            #[cfg(debug_assertions)]
            Some(Err(err)) => {
                crate::common::logging::log_warning(format!(
                    "error decoding chunk sample: {err:?}"
                ));
                None
            }
            #[cfg(not(debug_assertions))]
            Some(Err(_)) => None,
            None => None,
        }
    }
}

impl<I> Iterator for RangeFiltered<I>
where
    I: Iterator<Item = TsdbResult<Sample>> + ExactSizeIterator,
{
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.started {
            self.started = true;
            while let Some(sample) = self.next_in_window() {
                if sample.timestamp < self.start {
                    continue;
                }
                if sample.timestamp <= self.end {
                    return Some(sample);
                }
            }
            return None;
        }
        self.next_in_window()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.inner.len()))
    }
}
