//! `ChimpChunk`: a timeseries chunk whose values go through the ELF erasing
//! layer on top of the Chimp XOR codec, with delta-of-delta timestamps
//! interleaved in the same bit stream. The codec is a [`SampleCodec`]; the chunk
//! itself is [`XorChunk`].

use super::chimp_iterator::ChimpIterator;
use super::compressor::ChimpCompressor;
use crate::common::{Sample, Timestamp};
use crate::error::TsdbResult;
use crate::iterators::SampleIter;
use crate::series::chunks::chunk::ChunkEncoding;
use crate::series::chunks::range_iter::RangeFiltered;
use crate::series::chunks::xor_chunk::{SampleCodec, XorChunk};
use valkey_module::digest::Digest;
use valkey_module::{RedisModuleIO, ValkeyResult};

pub type ChimpChunk = XorChunk<ChimpCompressor>;

pub type ChimpChunkIterator<'a> = RangeFiltered<ChimpIterator<'a>>;

impl SampleCodec for ChimpCompressor {
    type Iter<'a> = ChimpIterator<'a>;
    const ENCODING: ChunkEncoding = ChunkEncoding::Chimp;

    fn with_soft_cap(max_size: usize) -> Self {
        ChimpCompressor::with_soft_cap(max_size)
    }
    fn set_soft_cap(&mut self, max_size: usize) {
        ChimpCompressor::set_soft_cap(self, max_size)
    }
    #[inline]
    fn push(&mut self, sample: &Sample) -> std::io::Result<()> {
        self.add(*sample)
    }
    fn iter(&self) -> ChimpIterator<'_> {
        ChimpCompressor::iter(self)
    }
    fn bytes(&self) -> &[u8] {
        ChimpCompressor::bytes(self)
    }
    fn len(&self) -> usize {
        self.count() as usize
    }
    fn first_timestamp(&self) -> Timestamp {
        ChimpCompressor::first_timestamp(self)
    }
    fn last_timestamp(&self) -> Timestamp {
        ChimpCompressor::last_timestamp(self)
    }
    fn last_value(&self) -> f64 {
        ChimpCompressor::last_value(self)
    }
    fn clear(&mut self) {
        ChimpCompressor::clear(self)
    }
    fn shrink_to_fit(&mut self) {
        ChimpCompressor::shrink_to_fit(self)
    }
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        ChimpCompressor::rdb_save(self, rdb)
    }
    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self> {
        Ok(ChimpCompressor::rdb_load(rdb)?)
    }
    fn serialize(&self, dest: &mut Vec<u8>) {
        ChimpCompressor::serialize(self, dest)
    }
    fn deserialize(buf: &[u8]) -> TsdbResult<Self> {
        ChimpCompressor::deserialize(buf)
    }
    fn debug_digest(&self, dig: &mut Digest) {
        ChimpCompressor::debug_digest(self, dig)
    }
    fn into_sample_iter<'a>(iter: RangeFiltered<ChimpIterator<'a>>) -> SampleIter<'a>
    where
        Self: 'a,
    {
        SampleIter::Chimp(iter)
    }
}
