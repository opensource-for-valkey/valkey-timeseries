use crate::common::Sample;
use crate::iterators::TimeSeriesRangeIterator;
use crate::iterators::vec_sample_iterator::VecSampleIterator;
use crate::series::chunks::{
    DeXorChunkIterator, GorillaChunkIterator, PcoSampleIterator, TsXorChunkIterator,
    Xor2RangeIterator,
};

#[derive(Default)]
pub enum SampleIter<'a> {
    Slice(std::slice::Iter<'a, Sample>),
    Vec(VecSampleIterator),
    Gorilla(GorillaChunkIterator<'a>),
    DeXor(Box<DeXorChunkIterator<'a>>),
    Pco(Box<PcoSampleIterator<'a>>),
    TSXor(TsXorChunkIterator<'a>),
    Range(TimeSeriesRangeIterator<'a>),
    XOR2(Box<Xor2RangeIterator<'a>>),
    #[default]
    Empty,
}

impl<'a> SampleIter<'a> {
    pub fn slice(slice: &'a [Sample]) -> Self {
        let iter = slice.iter();
        SampleIter::Slice(iter)
    }

    pub fn vec(samples: Vec<Sample>) -> Self {
        SampleIter::Vec(VecSampleIterator::new(samples))
    }
    pub fn gorilla(iter: GorillaChunkIterator<'a>) -> Self {
        SampleIter::Gorilla(iter)
    }
    pub fn dexor(iter: DeXorChunkIterator<'a>) -> Self {
        SampleIter::DeXor(Box::new(iter))
    }
    pub fn pco(iter: PcoSampleIterator<'a>) -> Self {
        SampleIter::Pco(Box::new(iter))
    }
    pub fn tsxor(iter: TsXorChunkIterator<'a>) -> Self {
        SampleIter::TSXor(iter)
    }
    pub fn xor2(iter: Xor2RangeIterator<'a>) -> Self {
        SampleIter::XOR2(Box::new(iter))
    }
}

impl Iterator for SampleIter<'_> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            SampleIter::Slice(slice) => slice.next().copied(),
            SampleIter::Vec(iter) => iter.next(),
            SampleIter::Gorilla(iter) => iter.next(),
            SampleIter::DeXor(iter) => iter.next(),
            SampleIter::Pco(iter) => iter.next(),
            SampleIter::TSXor(iter) => iter.next(),
            SampleIter::Range(range) => range.next(),
            SampleIter::Empty => None,
            SampleIter::XOR2(iter) => iter.next(),
        }
    }
}

impl From<VecSampleIterator> for SampleIter<'_> {
    fn from(value: VecSampleIterator) -> Self {
        Self::Vec(value)
    }
}

impl From<Vec<Sample>> for SampleIter<'_> {
    fn from(value: Vec<Sample>) -> Self {
        Self::Vec(VecSampleIterator::new(value))
    }
}

impl<'a> From<GorillaChunkIterator<'a>> for SampleIter<'a> {
    fn from(value: GorillaChunkIterator<'a>) -> Self {
        Self::Gorilla(value)
    }
}

impl<'a> From<DeXorChunkIterator<'a>> for SampleIter<'a> {
    fn from(value: DeXorChunkIterator<'a>) -> Self {
        Self::DeXor(Box::new(value))
    }
}

impl<'a> From<TsXorChunkIterator<'a>> for SampleIter<'a> {
    fn from(value: TsXorChunkIterator<'a>) -> Self {
        Self::TSXor(value)
    }
}

impl<'a> From<PcoSampleIterator<'a>> for SampleIter<'a> {
    fn from(value: PcoSampleIterator<'a>) -> Self {
        Self::Pco(Box::new(value))
    }
}
