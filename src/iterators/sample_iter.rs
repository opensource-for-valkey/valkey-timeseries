use crate::common::Sample;
use crate::iterators::TimeSeriesRangeIterator;
use crate::series::chunks::{ChimpChunkIterator, GorillaChunkIterator};

#[derive(Default)]
pub enum SampleIter<'a> {
    Slice(std::slice::Iter<'a, Sample>),
    Gorilla(GorillaChunkIterator<'a>),
    Chimp(Box<ChimpChunkIterator<'a>>),
    Range(TimeSeriesRangeIterator<'a>),
    #[default]
    Empty,
}

impl<'a> SampleIter<'a> {
    pub fn slice(slice: &'a [Sample]) -> Self {
        let iter = slice.iter();
        SampleIter::Slice(iter)
    }

    pub fn gorilla(iter: GorillaChunkIterator<'a>) -> Self {
        SampleIter::Gorilla(iter)
    }
    pub fn chimp(iter: ChimpChunkIterator<'a>) -> Self {
        SampleIter::Chimp(Box::new(iter))
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
}

impl<'a> From<GorillaChunkIterator<'a>> for SampleIter<'a> {
    fn from(value: GorillaChunkIterator<'a>) -> Self {
        Self::Gorilla(value)
    }
}

impl<'a> From<ChimpChunkIterator<'a>> for SampleIter<'a> {
    fn from(value: ChimpChunkIterator<'a>) -> Self {
        Self::Chimp(Box::new(value))
    }
}
