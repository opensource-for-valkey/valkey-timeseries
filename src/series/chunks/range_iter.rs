//! Range filtering shared by the compressed chunk encodings.
//!
//! The codec iterators decode every sample from the start of the chunk and report decode
//! errors in-band. [`RangeFiltered`] turns that into the plain `Sample` stream a range read
//! wants: samples before `start` are skipped, and the stream ends at the first sample past
//! `end` or at the first decode error.

use crate::common::{Sample, Timestamp};
use crate::error::TsdbResult;

/// Yields the samples of `inner` with timestamps in `[start, end]`. A decode error ends the
/// iteration (logged in debug builds).
pub struct RangeFiltered<I> {
    inner: I,
    start: Timestamp,
    end: Timestamp,
    started: bool,
    done: bool,
}

impl<I> RangeFiltered<I> {
    pub fn new(inner: I, start: Timestamp, end: Timestamp) -> Self {
        Self {
            inner,
            start,
            end,
            started: false,
            done: false,
        }
    }
}

impl<I: Iterator<Item = TsdbResult<Sample>>> RangeFiltered<I> {
    /// The next decoded sample at or before `end`; `None` (and done) past `end`, on a decode
    /// error, or when the codec runs out.
    #[inline]
    fn next_in_window(&mut self) -> Option<Sample> {
        match self.inner.next() {
            Some(Ok(sample)) if sample.timestamp <= self.end => Some(sample),
            Some(Ok(_)) | None => {
                self.done = true;
                None
            }
            Some(Err(_err)) => {
                #[cfg(debug_assertions)]
                crate::common::logging::log_warning(format!(
                    "error decoding chunk sample: {_err:?}"
                ));
                self.done = true;
                None
            }
        }
    }
}

impl<I> Iterator for RangeFiltered<I>
where
    I: Iterator<Item = TsdbResult<Sample>> + ExactSizeIterator,
{
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        if !self.started {
            self.started = true;
            while let Some(sample) = self.next_in_window() {
                if sample.timestamp >= self.start {
                    return Some(sample);
                }
            }
            return None;
        }
        self.next_in_window()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.done {
            (0, Some(0))
        } else {
            (0, Some(self.inner.len()))
        }
    }
}
