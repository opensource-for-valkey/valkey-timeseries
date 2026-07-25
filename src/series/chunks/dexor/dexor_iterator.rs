//! Sample-level DeXOR iterator.
//!
//! Mirrors [`GorillaIterator`](crate::series::chunks::GorillaIterator): reads
//! the interleaved timestamp/value records back in order.

use super::dexor_encoder::{CHUNK_CODEC_CONFIG, DeXorEncoder};
use super::value_decoder::DeXorValueDecoder;
use crate::common::Sample;
use crate::error::{TsdbError, TsdbResult};
use crate::series::chunks::stream::bitstream_reader::BitStreamReader;
use crate::series::chunks::stream::varbit::read_varbit_int;

pub struct DeXorIterator<'a> {
    reader: BitStreamReader<'a>,
    values: DeXorValueDecoder,
    idx: usize,
    num_samples: usize,
    timestamp_delta: i64,
    timestamp: i64,
    last_timestamp: i64,
    last_value: f64,
    last_idx: usize,
}

impl DeXorIterator<'_> {
    pub fn new(encoder: &'_ DeXorEncoder) -> DeXorIterator<'_> {
        let num_samples = encoder.num_samples;

        DeXorIterator {
            reader: BitStreamReader::new(encoder.buf()),
            values: DeXorValueDecoder::new(CHUNK_CODEC_CONFIG),
            idx: 0,
            num_samples,
            timestamp_delta: 0,
            timestamp: 0,
            last_timestamp: encoder.last_ts,
            last_value: encoder.last_value,
            last_idx: num_samples.saturating_sub(1),
        }
    }

    fn read_first_sample(&mut self) -> TsdbResult<Sample> {
        self.timestamp = self
            .reader
            .read_varint()
            .map_err(|_| TsdbError::DecodingError("Error decoding timestamp".to_string()))?;

        let value = self.read_value()?;
        self.idx += 1;

        Ok(Sample {
            timestamp: self.timestamp,
            value,
        })
    }

    fn read_second_sample(&mut self) -> TsdbResult<Sample> {
        let delta = self
            .reader
            .read_uvarint()
            .map_err(|_| TsdbError::DecodingError("Eof reading timestamp delta".to_string()))?;
        let delta = i64::try_from(delta)
            .map_err(|_| TsdbError::DecodingError("Timestamp delta too large".to_string()))?;

        let value = self.read_value()?;

        self.timestamp += delta;
        self.timestamp_delta = delta;
        self.idx += 1;

        Ok(Sample {
            timestamp: self.timestamp,
            value,
        })
    }

    fn read_nth_sample(&mut self) -> TsdbResult<Sample> {
        let delta_of_delta = read_varbit_int(&mut self.reader).map_err(|_| {
            TsdbError::DecodingError("Error decoding timestamp delta-of-delta".to_string())
        })?;

        let value = self.read_value()?;

        self.timestamp_delta += delta_of_delta;
        self.timestamp += self.timestamp_delta;
        self.idx += 1;

        Ok(Sample {
            timestamp: self.timestamp,
            value,
        })
    }

    /// The final sample is served from the encoder's cached `last_ts` /
    /// `last_value` rather than decoded, matching the Gorilla iterator.
    fn read_last_sample(&mut self) -> TsdbResult<Sample> {
        self.idx += 1;
        Ok(Sample {
            timestamp: self.last_timestamp,
            value: self.last_value,
        })
    }

    fn read_value(&mut self) -> TsdbResult<f64> {
        self.values
            .decode(&mut self.reader)
            .map_err(|_| TsdbError::DecodingError("Error reading value".to_string()))
    }
}

impl Iterator for DeXorIterator<'_> {
    type Item = TsdbResult<Sample>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.num_samples {
            return None;
        }

        Some(match self.idx {
            0 => self.read_first_sample(),
            1 => self.read_second_sample(),
            n if n == self.last_idx => self.read_last_sample(),
            _ => self.read_nth_sample(),
        })
    }
}

impl ExactSizeIterator for DeXorIterator<'_> {
    fn len(&self) -> usize {
        self.num_samples - self.idx
    }
}
