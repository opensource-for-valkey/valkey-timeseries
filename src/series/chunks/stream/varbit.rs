// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::common::encoding::sign_extend;
use crate::series::chunks::stream::{
    bitstream::BitStream,
    bitstream_reader::BitStreamReader,
    traits::{BitRead, BitWrite},
    utils::{read_bits, read_bool},
};
use std::io::{self};

/// Bit value constants
const ZERO: u8 = 0;

/// Error type for varbit operations
#[derive(Debug)]
pub enum VarbitError {
    Io(io::Error),
    InvalidBitPattern(u8),
}

impl From<io::Error> for VarbitError {
    fn from(err: io::Error) -> Self {
        VarbitError::Io(err)
    }
}

impl std::fmt::Display for VarbitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VarbitError::Io(err) => write!(f, "IO error: {}", err),
            VarbitError::InvalidBitPattern(d) => write!(f, "invalid bit pattern {:08b}", d),
        }
    }
}

impl std::error::Error for VarbitError {}

/// Writes an i64 using varbit encoding with a bit bucketing
/// optimized for the dod's observed in histogram buckets, plus a few additional
/// buckets for large numbers.
///
/// Encoding layout (prefix + payload):
/// - 0:              1 bit   (just bit 0)
/// - -3..=3:         2+5=7   bits (prefix 0b10 + 5-bit signed payload)
/// - -31..=31:       3+9=12  bits (prefix 0b110 + 9-bit signed payload)
/// - -255..=255:     4+13=17 bits (prefix 0b1110 + 13-bit signed payload)
/// - -2047..=2047:   5+17=22 bits (prefix 0b11110 + 17-bit signed payload)
/// - -131071..=131071:       6+24=30 bits (prefix 0b111110 + 24-bit signed payload)
/// - -16777215..=16777215:   7+32=39 bits (prefix 0b1111110 + 32-bit signed payload)
/// - -36028797018963967..=36028797018963967: 8+56=64 bits (prefix 0b11111110 + 56-bit signed)
/// - Everything else: 8+64=72 bits (prefix 0b11111111 + full 64-bit signed)
///
/// For optimal space utilization, each branch didn't need to support any values
/// of any of the prior branches. So we could expand the range of each branch. Do
/// more with fewer bits. It would come at the price of more expensive encoding
/// and decoding (cutting out and later adding back that center-piece we
/// skip). With the distributions of values we see in practice, we would reduce
/// the size by around 1%. A more detailed study would be needed for precise
/// values, but it's appears quite certain that we would end up far below 10%,
/// which would maybe convince us to invest the increased coding/decoding cost.
pub fn put_varbit_int(b: &mut BitStream, val: i64) {
    let _ = write_varbit(b, val);
}

/// Writes an i64 using varbit encoding with a bit bucketing
/// optimized for the dod's observed in histogram buckets, plus a few additional
/// buckets for large numbers.
///
/// For optimal space utilization, each branch didn't need to support any values
/// of the prior branches, so we could expand the range of each branch. Do
/// more with fewer bits. It would come at the price of more expensive encoding
/// and decoding (cutting out and later adding back that center-piece we
/// skip). With the distributions of values we see in practice, we would reduce
/// the size by around 1%. A more detailed study would be needed for precise
/// values, but it appears quite certain that we would end up far below 10%,
/// which would maybe convince us to invest the increased coding/decoding cost.
pub(crate) fn write_varbit<W: BitWrite>(writer: &mut W, value: i64) -> io::Result<()> {
    match value {
        0 => writer.write_bit(false)?, // Precisely 0, needs 1 bit.
        // -3 <= val <= 4, needs 5 bits.
        -3..=3 => {
            writer.write_out::<2, u8>(0b10)?;
            writer.write_out::<5, u64>(value as u64 & 0x1F)?;
        }
        // -31 <= val <= 32, 9 bits.
        -31..=31 => {
            writer.write_out::<3, u8>(0b110)?;
            writer.write_out::<9, u64>(value as u64 & 0x1FF)?;
        }
        // -255 <= val <= 256, 13 bits.
        -255..=255 => {
            writer.write_out::<4, u8>(0b1110)?;
            writer.write_out::<13, u64>(value as u64 & 0x1FFF)?;
        }
        // -2047 <= val <= 2048, 17 bits.
        -2047..=2047 => {
            writer.write_out::<5, u8>(0b11110)?;
            writer.write_out::<17, u64>(value as u64 & 0x1FFFF)?;
        }
        // -131071 <= val <= 131072, 3 bytes.
        -131071..=131071 => {
            writer.write_out::<6, u8>(0b111110)?;
            writer.write_out::<24, u64>(value as u64 & 0x0FFFFFF)?;
        }
        // -16777215 <= val <= 16777215, 4 bytes.
        -16777215..=16777215 => {
            writer.write_out::<7, u8>(0b1111110)?;
            writer.write_out::<32, u64>(value as u64 & 0x0FFFFFFFF)?;
        }
        // -36028797018963967 <= val <= 36028797018963968, 8 bytes.
        -36028797018963967..=36028797018963967 => {
            writer.write_out::<8, u8>(0b11111110)?;
            writer.write_out::<56, u64>(value as u64 & 0xFFFFFFFFFFFFFF)?;
        }
        _ => {
            writer.write_out::<8, u8>(0b11111111)?; // The worst case, needs 9 bytes.
            writer.write_out::<64, u64>(value as u64)?; // ??? test this !!!
        }
    }
    Ok(())
}

/// Reads a varbit-encoded integer from the input.
///
/// Decoding layout (reads variable-length prefix + fixed payload):
/// Prefix is read as a sequence of 1 bits followed by a 0 bit (unary encoding):
/// - 0 bit alone:    value = 0 (1 bit total)
/// - 0b10 (10):      5-bit signed payload follows (7 bits total, bucket for -3..=3)
/// - 0b110 (110):    9-bit signed payload follows (12 bits total, bucket for -31..=31)
/// - 0b1110 (1110):  13-bit signed payload follows (17 bits total, bucket for -255..=255)
/// - 0b11110:        17-bit signed payload follows (22 bits total, bucket for -2047..=2047)
/// - 0b111110:       24-bit signed payload follows (30 bits total)
/// - 0b1111110:      32-bit signed payload follows (39 bits total)
/// - 0b11111110:     56-bit signed payload follows (64 bits total)
/// - 0b11111111:     full 64-bit signed value follows (72 bits total, worst case)
///
/// The prefix is read bit-by-bit, accumulating 1s until a 0 is encountered.
/// This Prometheus varbit format consists of 9 categories (buckets 0-8).
fn read_varbit_int_bucket<R: BitRead>(reader: &mut R) -> io::Result<u8> {
    for i in 0..8 {
        let bit = read_bool(reader)?;
        // If we read a 0, it's a sign that we reached the end of the bucket category.
        if !bit {
            return Ok(i);
        }
    }

    // If we read 8 bits already, there is no final 0.
    Ok(8)
}

#[inline]
fn varbit_bucket_to_num_bits(bucket: u8) -> u8 {
    match bucket {
        0 => 0,
        1 => 5,
        2 => 9,
        3 => 13,
        4 => 17,
        5 => 24,
        6 => 32,
        7 => 56,
        8 => 64,
        _ => unreachable!("gorilla decoding: Invalid varbit bucket value"),
    }
}

/// Decodes a varbit-encoded i64 by reading the prefix bucket and then the payload.
/// Combines read_varbit_int_bucket + payload reading to reconstruct the original signed value.
pub(crate) fn read_varbit_int<R: BitRead>(reader: &mut R) -> io::Result<i64> {
    let bucket = read_varbit_int_bucket(reader)?;
    let num_bits = varbit_bucket_to_num_bits(bucket);

    // Shortcut for the 0 use case as nothing more has to be read.
    if bucket == 0 {
        return Ok(0);
    }

    let value = read_bits(reader, num_bits as u32)?;
    if num_bits != 64 && value > (1 << (num_bits - 1)) {
        //value -= 1 << num_bits;
        return Ok(sign_extend(value, num_bits as u32));
    }

    Ok(value as i64)
}

/// put_varbit_uint writes a uint64 using varbit encoding. It uses the same bit
/// buckets as put_varbit_int.
pub fn put_varbit_uint(b: &mut BitStream, val: u64) {
    match val {
        0 => {
            // Precisely 0, needs 1 bit.
            b.write_bit(false);
        }
        _ if val < (1u64 << 3) => {
            // val <= 7, needs 5 bits (2-bit prefix + 3-bit payload).
            b.write_bits_fast(0b10, 2);
            b.write_bits_fast(val, 3);
        }
        _ if val < (1u64 << 6) => {
            // val <= 63, needs 9 bits (3-bit prefix + 6-bit payload).
            b.write_bits_fast(0b110, 3);
            b.write_bits_fast(val, 6);
        }
        _ if val < (1u64 << 9) => {
            // val <= 511, needs 13 bits (4-bit prefix + 9-bit payload).
            b.write_bits_fast(0b1110, 4);
            b.write_bits_fast(val, 9);
        }
        _ if val < (1u64 << 12) => {
            // val <= 4095, needs 17 bits (5-bit prefix + 12-bit payload).
            b.write_bits_fast(0b11110, 5);
            b.write_bits_fast(val, 12);
        }
        _ if val < (1u64 << 18) => {
            // val <= 262143, needs 24 bits (6-bit prefix + 18-bit payload).
            b.write_bits_fast(0b111110, 6);
            b.write_bits_fast(val, 18);
        }
        _ if val < (1u64 << 25) => {
            // val <= 33554431, needs 32 bits (7-bit prefix + 25-bit payload).
            b.write_bits_fast(0b1111110, 7);
            b.write_bits_fast(val, 25);
        }
        _ if val < (1u64 << 56) => {
            // val <= 72057594037927935, needs 64 bits (8-bit prefix + 56-bit payload).
            b.write_bits_fast(0b11111110, 8);
            b.write_bits_fast(val, 56);
        }
        _ => {
            // Worst case, needs 72 bits (8-bit prefix + 64-bit payload).
            b.write_bits_fast(0b11111111, 8);
            b.write_bits_fast(val, 64);
        }
    }
}

/// read_varbit_uint reads a u64 encoded with put_varbit_uint.
/// The prefix is reconstructed from a sequence of variable-length bits terminated by a 0.
/// For example: 0b10 is read as two bits [1, 0], 0b110 as three bits [1, 1, 0], etc.
pub fn read_varbit_uint(b: &mut BitStreamReader) -> Result<u64, VarbitError> {
    // Read prefix bits until we hit a 0 (stopping bit)
    let mut prefix: u8 = 0;
    for _ in 0..8 {
        prefix <<= 1;
        let bit = b.read_bit_fast().or_else(|_| b.read_bit())?;
        if !bit {
            break;
        }
        prefix |= 1;
    }

    // Map prefix pattern to payload size in bits (0 = value is zero, no payload).
    let payload_bits: u8 = match prefix {
        0b0 => return Ok(0),
        0b10 => 3,
        0b110 => 6,
        0b1110 => 9,
        0b11110 => 12,
        0b111110 => 18,
        0b1111110 => 25,
        0b11111110 => 56,
        0b11111111 => 64,
        _ => return Err(VarbitError::InvalidBitPattern(prefix)),
    };

    b.read_bits_fast(payload_bits)
        .or_else(|_| b.read_bits(payload_bits))
        .map_err(VarbitError::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    const VARBIT_INT_BOUNDARY_VALUES: [i64; 33] = [
        i64::MIN,
        -36028797018963968,
        -36028797018963967,
        -16777216,
        -16777215,
        -131072,
        -131071,
        -2048,
        -2047,
        -256,
        -255,
        -32,
        -31,
        -4,
        -3,
        -1,
        0,
        1,
        4,
        5,
        32,
        33,
        256,
        257,
        2048,
        2049,
        131072,
        131073,
        16777216,
        16777217,
        36028797018963968,
        36028797018963969,
        i64::MAX,
    ];

    #[test]
    fn test_write_varbit() {
        let mut bit_writer = BitStream::new();

        for number in VARBIT_INT_BOUNDARY_VALUES.iter().cloned() {
            write_varbit(&mut bit_writer, number).unwrap();
        }

        let binding = bit_writer.get_ref();
        let mut reader = BitStreamReader::new(binding);
        // Read again
        for want in VARBIT_INT_BOUNDARY_VALUES {
            let got = read_varbit_int(&mut reader).unwrap();
            assert_eq!(want, got)
        }
    }

    #[test]
    fn test_varbit_uint_zero() {
        let mut b = BitStream::new();
        put_varbit_uint(&mut b, 0);
        let bytes = b.into_bytes();

        let mut reader = BitStreamReader::new(&bytes);
        let val = read_varbit_uint(&mut reader).unwrap();
        assert_eq!(val, 0);
    }

    #[test]
    fn test_varbit_int_small_range() {
        let mut b = BitStream::new();
        put_varbit_int(&mut b, 2);

        let mut reader = BitStreamReader::new(b.bytes());
        let val = read_varbit_int(&mut reader).unwrap();
        assert_eq!(val, 2);
    }

    #[test]
    fn test_varbit_int_negative() {
        let mut b = BitStream::new();
        put_varbit_int(&mut b, -10);

        let mut reader = BitStreamReader::new(b.bytes());
        let val = read_varbit_int(&mut reader).unwrap();
        assert_eq!(val, -10);
    }

    #[test]
    fn test_varbit_int_large() {
        let mut b = BitStream::new();
        put_varbit_int(&mut b, 1000000);
        let bytes = b.bytes();

        let mut reader = BitStreamReader::new(bytes);
        let val = read_varbit_int(&mut reader).unwrap();
        assert_eq!(val, 1000000);
    }

    #[test]
    fn test_put_varbit_int_write_varbit_compat() {
        // Verify that put_varbit_int and write_varbit use the same encoding
        // and can roundtrip via read_varbit_int.
        let values = vec![0, 1, -1, 2, -3, 4, -31, 31, -255, 255, -2047, 2047, 123];

        for v in values {
            let mut b = BitStream::new();
            write_varbit(&mut b, v).unwrap();

            let mut reader = BitStreamReader::new(b.bytes());
            let out = read_varbit_int(&mut reader).unwrap();
            assert_eq!(out, v, "write_varbit roundtrip failed for value {}", v);
        }
    }

    #[test]
    fn test_varbit_uint() {
        let numbers: Vec<u64> = vec![
            0,
            1,
            7,
            8,
            63,
            64,
            511,
            512,
            4095,
            4096,
            262143,
            262144,
            33554431,
            33554432,
            72057594037927935,
            72057594037927936,
            u64::MAX,
        ];

        let mut bs = BitStream::new();

        for &n in numbers.iter() {
            put_varbit_uint(&mut bs, n);
        }

        let mut bsr = BitStreamReader::new(bs.bytes());

        for want in &numbers {
            let got = read_varbit_uint(&mut bsr).expect("Error reading varbit uint");
            assert_eq!(want, &got);
        }
    }
    #[test]
    fn test_varbit_uint_small() {
        let mut b = BitStream::new();
        put_varbit_uint(&mut b, 5);

        let mut reader = BitStreamReader::new(b.bytes());
        let val = read_varbit_uint(&mut reader).unwrap();
        assert_eq!(val, 5);
    }

    #[test]
    fn test_varbit_uint_large() {
        let mut b = BitStream::new();
        put_varbit_uint(&mut b, u64::MAX);

        let mut reader = BitStreamReader::new(b.bytes());

        let val = read_varbit_uint(&mut reader).unwrap();
        assert_eq!(val, u64::MAX);
    }

    #[test]
    fn test_put_varbit_int_fuzz() {
        // Fuzz test with pseudo-random i64 values covering all buckets.
        // Verifies that put_varbit_int (non-fast) also round-trips correctly.
        let mut seed: u64 = 54321;
        let lcg_next = |s: &mut u64| {
            *s = s.wrapping_mul(1103515245).wrapping_add(12345);
            (*s / 65536) as i64
        };

        // Generate ~1000 fuzz values
        for _ in 0..1000 {
            let val = lcg_next(&mut seed);
            let mut b = BitStream::new();
            put_varbit_int(&mut b, val);

            let mut reader = BitStreamReader::new(b.bytes());
            let out = read_varbit_int(&mut reader).unwrap();
            assert_eq!(
                out, val,
                "put_varbit_int fuzz test failed for value {}",
                val
            );
        }
    }

    #[test]
    fn test_put_varbit_uint_fuzz() {
        // Fuzz test with pseudo-random u64 values covering all unsigned buckets.
        let mut seed: u64 = 99999;
        let lcg_next = |s: &mut u64| {
            *s = s.wrapping_mul(1103515245).wrapping_add(12345);
            *s
        };

        // Generate ~1000 fuzz values
        for _ in 0..1000 {
            let val = lcg_next(&mut seed);
            let mut b = BitStream::new();
            put_varbit_uint(&mut b, val);
            let bytes = b.into_bytes();

            let mut reader = BitStreamReader::new(&bytes);
            let out = read_varbit_uint(&mut reader).unwrap();
            assert_eq!(
                out, val,
                "put_varbit_uint fuzz test failed for value {}",
                val
            );
        }
    }
}
