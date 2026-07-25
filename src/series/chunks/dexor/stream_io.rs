//! Zero-width-safe wrappers over the shared bit stream.
//!
//! DeXOR routinely emits fields of width zero (a residual with no digits, or a
//! dictionary id in single-slot mode). `BitStream::write_bits_fast` shifts by
//! `64 - nbits` and `BitStreamReader::read_bits` touches the underlying buffer,
//! so both need the empty case handled before the call.

use crate::series::chunks::stream::bitstream::BitStream;
use crate::series::chunks::stream::bitstream_reader::BitStreamReader;
use std::io;

#[inline]
pub(super) fn write_bits(out: &mut BitStream, bits: u32, value: u64) {
    if bits > 0 {
        out.write_bits_fast(value, bits as usize);
    }
}

#[inline]
pub(super) fn read_bits(input: &mut BitStreamReader, bits: u32) -> io::Result<u64> {
    if bits == 0 {
        return Ok(0);
    }
    input.read_bits(bits as u8)
}
