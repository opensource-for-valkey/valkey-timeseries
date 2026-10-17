use crate::common::Sample;
use crate::iterators::TimeSeriesRangeIterator;
use crate::series::chunks::{ChimpChunkIterator, GorillaChunkIterator};

#[derive(Default)]
pub enum SampleIter<'a> {
    Slice(std::slice::Iter<'a, Sample>),
    Gorilla(GorillaChunkIterator<'a>),
    Chimp(ChimpChunkIterator<'a>),
    Range(TimeSeriesRangeIterator<'a>),
    #[default]
    Empty,
}

impl<'a> SampleIter<'a> {
    pub fn slice(slice: &'a [Sample]) -> Self {
        let iter = slice.iter();
        SampleIter::Slice(iter)
    }
}

impl Iterator for SampleIter<'_> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            SampleIter::Slice(slice) => slice.next().copied(),
            SampleIter::Gorilla(iter) => iter.next(),
            SampleIter::Chimp(iter) => iter.next(),
            SampleIter::Range(range) => range.next(),
            SampleIter::Empty => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            SampleIter::Slice(slice) => slice.size_hint(),
            SampleIter::Gorilla(iter) => iter.size_hint(),
            SampleIter::Chimp(iter) => iter.size_hint(),
            SampleIter::Range(range) => range.size_hint(),
            SampleIter::Empty => (0, Some(0)),
        }
    }

    /// Dispatch on the variant once, then let the inner iterator drive the loop. Without
    /// this, `for_each`/`collect`/`extend` re-match the variant on every sample, which
    /// costs a slice-backed scan more than the copy it avoids.
    fn fold<B, F>(self, init: B, f: F) -> B
    where
        F: FnMut(B, Self::Item) -> B,
    {
        match self {
            SampleIter::Slice(slice) => slice.copied().fold(init, f),
            SampleIter::Gorilla(iter) => iter.fold(init, f),
            SampleIter::Chimp(iter) => iter.fold(init, f),
            SampleIter::Range(range) => range.fold(init, f),
            SampleIter::Empty => init,
        }
    }
}
