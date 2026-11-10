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

/// Compact, transient seek state for the opt-in evaluation tool. An index must
/// borrow the encoder immutably so appends cannot invalidate its checkpoints.
#[cfg(feature = "test-utils")]
#[derive(Clone, Copy)]
pub struct GorillaCheckpoint {
    position: usize,
    idx: usize,
    timestamp: i64,
    timestamp_delta: u64,
    value: f64,
    leading_bits: u8,
    trailing_bits: u8,
}

#[cfg(feature = "test-utils")]
impl GorillaCheckpoint {
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }
}

#[cold]
#[inline(never)]
fn decoding_error(what: &str) -> TsdbError {
    TsdbError::DecodingError(what.to_string())
}

impl GorillaIterator<'_> {
    #[cfg(feature = "test-utils")]
    pub fn checkpoint(&self) -> GorillaCheckpoint {
        GorillaCheckpoint {
            position: self.reader.bit_position(),
            idx: self.idx,
            timestamp: self.timestamp,
            timestamp_delta: self.timestamp_delta,
            value: self.value,
            leading_bits: self.leading_bits,
            trailing_bits: self.trailing_bits,
        }
    }

    #[cfg(feature = "test-utils")]
    pub fn resume(
        encoder: &'_ GorillaEncoder,
        checkpoint: GorillaCheckpoint,
    ) -> std::io::Result<GorillaIterator<'_>> {
        let mut iter = GorillaIterator::new(encoder);
        iter.reader = BitStreamReader::from_bit_position(encoder.buf(), checkpoint.position)?;
        iter.idx = checkpoint.idx;
        iter.timestamp = checkpoint.timestamp;
        iter.timestamp_delta = checkpoint.timestamp_delta;
        iter.value = checkpoint.value;
        iter.leading_bits = checkpoint.leading_bits;
        iter.trailing_bits = checkpoint.trailing_bits;
        Ok(iter)
    }

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
            .map_err(|_| decoding_error("Error decoding timestamp"))?;

        let value = self
            .reader
            .read_f64()
            .map_err(|_| decoding_error("Error decoding value"))?;

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
            .map_err(|_| decoding_error("Eof reading delta-of-delta"))?;

        let (value, leading_bits, trailing_bits) = self.read_value()?;

        self.timestamp = i64::try_from(timestamp_delta)
            .ok()
            .and_then(|delta| self.timestamp.checked_add(delta))
            .ok_or_else(|| decoding_error("Timestamp delta too large"))?;

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

        let timestamp_delta_of_delta =
            read_varbit_int(&mut self.reader).map_err(|_| decoding_error("XOR encoder"))?;

        let (value, leading_bits, trailing_bits) = self.read_value()?;

        // Wrapping, as the stream is untrusted and release builds wrap anyway: a corrupt
        // stream decodes to wrong timestamps (which the load check rejects), never a panic.
        let timestamp_delta =
            (prev_timestamp_delta as i64).wrapping_add(timestamp_delta_of_delta) as u64;

        self.timestamp_delta = timestamp_delta;
        self.timestamp = prev_timestamp.wrapping_add(timestamp_delta as i64);
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
        .map_err(|_| decoding_error("Error reading value"))
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
mod tests {
    #[cfg(feature = "test-utils")]
    #[test]
    fn checkpoints_resume_at_every_bit_alignment() {
        use super::{GorillaEncoder, GorillaIterator};
        use crate::tests::generators::{
            DatasetKey, TimestampModel, ValueWorkload, dataset_seed, generate_dataset,
        };
        for &model in TimestampModel::all() {
            let key = DatasetKey::new(ValueWorkload::Noisy, model);
            let samples = generate_dataset(key, 257, dataset_seed(key));
            let mut encoder = GorillaEncoder::new();
            for sample in &samples {
                encoder.add_sample(sample).unwrap();
            }
            let mut iter = encoder.iter();
            for i in 0..=samples.len() {
                let resumed = GorillaIterator::resume(&encoder, iter.checkpoint()).unwrap();
                let got: Vec<_> = resumed.map(Result::unwrap).collect();
                assert_eq!(got, samples[i..]);
                iter.next();
            }
        }
    }
}
