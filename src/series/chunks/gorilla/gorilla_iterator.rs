use super::GorillaEncoder;
use super::varbit_xor::read_varbit_xor;
use crate::common::Sample;
use crate::error::{TsdbError, TsdbResult};
use crate::series::chunks::stream::bitstream_reader::BitStreamReader;
use crate::series::chunks::stream::varbit::read_varbit_int;

pub struct GorillaIterator<'a> {
    reader: BitStreamReader<'a>,
    idx: usize,
    num_samples: usize,
    leading_bits: u8,
    trailing_bits: u8,
    timestamp_delta: u64,
    timestamp: i64,
    value: f64,
    last_timestamp: i64,
    last_value: f64,
    last_idx: usize,
}

impl GorillaIterator<'_> {
    pub fn new(encoder: &'_ GorillaEncoder) -> GorillaIterator<'_> {
        let buf = encoder.buf();
        let num_samples = encoder.num_samples;
        let last_timestamp = encoder.last_ts;
        let last_value = encoder.last_value;
        let reader = BitStreamReader::new(buf);
        // Saturating: an empty encoder has no last sample, and this is reachable. `GorillaChunk`
        // guards `get_range` with `is_empty`, but `iter`/`range_iter` do not, and a series always
        // holds at least one chunk -- a freshly appended one is empty until the first sample
        // lands. A plain `- 1` panicked there ("attempt to subtract with overflow") in any debug
        // build, which without an FFI panic barrier takes the whole server down. `next` returns
        // `None` immediately when `num_samples` is zero, so the value is never read.
        let last_idx = num_samples.saturating_sub(1);

        GorillaIterator {
            reader,
            idx: 0,
            num_samples,
            timestamp: 0,
            value: 0.0,
            leading_bits: 0,
            trailing_bits: 0,
            timestamp_delta: 0,
            last_timestamp,
            last_value,
            last_idx,
        }
    }

    fn read_first_sample(&mut self) -> TsdbResult<Sample> {
        let timestamp = self
            .reader
            .read_varint()
            .map_err(|_| TsdbError::DecodingError("Error decoding timestamp".to_string()))?;

        let value = self
            .reader
            .read_f64()
            .map_err(|_| TsdbError::DecodingError("Error decoding value".to_string()))?;

        self.timestamp = timestamp;
        self.value = value;

        self.idx += 1;
        Ok(Sample {
            timestamp: self.timestamp,
            value: self.value,
        })
    }

    fn read_second_sample(&mut self) -> TsdbResult<Sample> {
        let timestamp_delta = self
            .reader
            .read_uvarint()
            .map_err(|_| TsdbError::DecodingError("Eof reading delta-of-delta".to_string()))?;

        let (value, leading_bits, trailing_bits) = self.read_value()?;

        self.timestamp += i64::try_from(timestamp_delta)
            .map_err(|_| TsdbError::DecodingError("Timestamp delta too large".to_string()))?;

        self.value = value;
        self.leading_bits = leading_bits;
        self.trailing_bits = trailing_bits;
        self.timestamp_delta = timestamp_delta;

        self.idx += 1;
        Ok(Sample {
            timestamp: self.timestamp,
            value: self.value,
        })
    }

    fn read_nth_sample(&mut self) -> TsdbResult<Sample> {
        let prev_timestamp = self.timestamp;
        let prev_timestamp_delta = self.timestamp_delta;

        let timestamp_delta_of_delta = read_varbit_int(&mut self.reader)
            .map_err(|_| TsdbError::DecodingError("XOR encoder".to_string()))?;

        let (value, leading_bits, trailing_bits) = self.read_value()?;

        let timestamp_delta = ((prev_timestamp_delta as i64) + timestamp_delta_of_delta) as u64;

        self.timestamp_delta = timestamp_delta;
        self.timestamp = prev_timestamp + timestamp_delta as i64;
        self.leading_bits = leading_bits;
        self.trailing_bits = trailing_bits;
        self.value = value;
        self.idx += 1;

        Ok(Sample {
            timestamp: self.timestamp,
            value: self.value,
        })
    }

    fn read_last_sample(&mut self) -> TsdbResult<Sample> {
        self.idx += 1;
        Ok(Sample {
            timestamp: self.last_timestamp,
            value: self.last_value,
        })
    }

    fn read_value(&mut self) -> TsdbResult<(f64, u8, u8)> {
        read_varbit_xor(
            &mut self.reader,
            self.value,
            self.leading_bits,
            self.trailing_bits,
        )
        .map_err(|_| TsdbError::DecodingError("Error reading value".to_string()))
    }
}

impl Iterator for GorillaIterator<'_> {
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

impl ExactSizeIterator for GorillaIterator<'_> {
    fn len(&self) -> usize {
        self.num_samples - self.idx
    }
}
#[cfg(test)]
mod tests {}
