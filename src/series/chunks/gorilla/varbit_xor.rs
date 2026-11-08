use crate::series::chunks::stream::traits::{BitRead, BitWrite};
use crate::series::chunks::stream::utils::{read_bits, read_bool};

/// Writes a Gorilla XOR value, including this module's extended windows.
///
/// It also needs the previous value, the previous leading and trailing bits count.
///
/// The first time it is called, use 0xff (or 255) for the leading bits counts,
/// and 0 for the trailing bits count.
///
/// `inline(always)`: measured, see `f624ea039`. Without it, regular-timestamp rows encoded
/// 5-23 % slower than with the pre-extension writer; with it they are 6-12 % faster. The
/// likely cause, inferred from timings and not checked in disassembly, is that the cost
/// compare and extended branch, with two call sites in the encoder, pushed the function past
/// LLVM's inlining threshold. Re-measure before removing it.
#[inline(always)]
pub fn write_varbit_xor<W: BitWrite>(
    value: f64,
    previous_value: f64,
    previous_leading_bits_count: u8,
    previous_trailing_bits_count: u8,
    bit_writer: &mut W,
) -> std::io::Result<(u8, u8)> {
    let delta = value.to_bits() ^ previous_value.to_bits();

    if delta == 0 {
        bit_writer.write_bit(false)?;
        return Ok((previous_leading_bits_count, previous_trailing_bits_count));
    }

    let leading = delta.leading_zeros() as u8;
    let new_trailing = delta.trailing_zeros() as u8;
    // The v1 header stores at most 31 leading zeros. Reserve its impossible
    // combinations (leading=31, width>33) as extended leading counts:
    // width code N means leading N-1 (zero means 63), then a five-bit width.
    // The extra five header bits pay off beyond 36 leading zeros.
    let extended = leading > 36;
    let new_leading = if extended { leading } else { leading.min(31) };
    let header_bits = if extended { 18 } else { 13 };
    let sig = (64 - new_leading - new_trailing) as u32;

    // Compare actual bit costs. A narrower window is useful even when it fits
    // inside the old window; preserve the old state on equal cost.
    if previous_leading_bits_count != 0xff
        && leading >= previous_leading_bits_count
        && new_trailing >= previous_trailing_bits_count
    {
        let old_sig = (64 - previous_leading_bits_count - previous_trailing_bits_count) as u32;
        // A 32..=57-bit payload already uses the reader's single-word path.
        // Reopening these windows on isolated narrow XORs creates header churn
        // on quantized noise. Keep them until they no longer fit, unless the
        // extended leading count offers a persistent escape from the 31-bit cap.
        // Compact windows and 58..=64-bit payloads still compare exact bit costs.
        let keep_fast_window = !extended && (32..=57).contains(&old_sig);
        if keep_fast_window || 2 + old_sig <= header_bits + sig {
            let payload = delta >> previous_trailing_bits_count;
            if old_sig <= 62 {
                bit_writer.write(2 + old_sig, (0b10u64 << old_sig) | payload)?;
            } else {
                bit_writer.write(2, 0b10u64)?;
                bit_writer.write(old_sig, payload)?;
            }
            return Ok((previous_leading_bits_count, previous_trailing_bits_count));
        }
    }

    if extended {
        // 11 | 11111 | encoded leading (6) | actual width (5).
        // leading > 36 implies sig <= 27, so the whole write fits in one word.
        let leading_code = (new_leading as u64 + 1) & 63;
        let header = (0b11u64 << 16) | (31u64 << 11) | (leading_code << 5) | sig as u64;
        bit_writer.write(18 + sig, (header << sig) | (delta >> new_trailing))?;
        return Ok((new_leading, new_trailing));
    }

    // New window: control bits `11`, 5-bit leading count, 6-bit width, then the payload.
    let sig_bits = (64_u64 - new_leading as u64) - new_trailing as u64;
    // Overflow 64 to 0 is fine because if 0 sig_bits, we would have written a "same number"
    // bit a bit earlier.
    // The reason is that only 6 bits are available, and the maximum value is 63.
    let encoded_sig_bits = if sig_bits > 63 { 0 } else { sig_bits };
    let header = (0b11u64 << 11) | ((new_leading as u64) << 6) | encoded_sig_bits;
    let sig = sig_bits as u32;
    let payload = delta >> new_trailing;
    if sig <= 51 {
        bit_writer.write(13 + sig, (header << sig) | payload)?;
    } else {
        bit_writer.write(13, header)?;
        bit_writer.write(sig, payload)?;
    }

    Ok((new_leading, new_trailing))
}

/// Reads a Gorilla XOR value, accepting legacy and extended windows.
///
/// The first time it is called, use 0 for both leading and trailing bits count.
///
/// It returns the new value, and also the new leading and trailing bits count.
#[inline]
pub fn read_varbit_xor<R: BitRead>(
    reader: &mut R,
    previous_value: f64,
    previous_leading_bits_count: u8,
    previous_trailing_bits_count: u8,
) -> std::io::Result<(f64, u8, u8)> {
    // Read the bit saying whether we use the previous value or not
    let different_value_bit = read_bool(reader)?;
    if !different_value_bit {
        return Ok((
            previous_value,
            previous_leading_bits_count,
            previous_trailing_bits_count,
        ));
    }

    let leading_bits_count: u8;
    let middle_bits_count: u8;
    let trailing_bits_count: u8;

    // Read the bit saying whether we reuse the previous leading and trailing bits count or not
    let different_leading_and_trailing_bits_count = read_bool(reader)?;
    if different_leading_and_trailing_bits_count {
        // Both fields are fixed width; consume their eleven bits together.
        let header = read_bits(reader, 11)?;
        let leading = (header >> 6) as u8;
        let width = (header & 63) as u8;
        if leading == 31 && (width == 0 || width > 33) {
            leading_bits_count = if width == 0 { 63 } else { width - 1 };
            middle_bits_count = read_bits(reader, 5)? as u8;
            if middle_bits_count == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "corrupt gorilla chunk: zero extended width",
                ));
            }
        } else {
            leading_bits_count = leading;
            middle_bits_count = if width == 0 { 64 } else { width };
        }
        // leading_bits_count is 0..=63 and middle_bits_count is 1..=64, so this sum never
        // overflows u8, but a corrupt/adversarial stream can still claim a combination that
        // exceeds 64 bits total; reject it here instead of underflowing the subtraction below.
        if leading_bits_count + middle_bits_count > 64 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "corrupt gorilla chunk: leading + middle bit counts exceed 64",
            ));
        }
        trailing_bits_count = 64 - leading_bits_count - middle_bits_count;
    } else {
        // Safe: `previous_leading_bits_count`/`previous_trailing_bits_count` are either the
        // caller's initial (0, 0) or a previously-returned `(leading_bits_count,
        // trailing_bits_count)` pair from this same function, which the check above already
        // guarantees sums to at most 64.
        leading_bits_count = previous_leading_bits_count;
        trailing_bits_count = previous_trailing_bits_count;
        middle_bits_count = 64 - leading_bits_count - trailing_bits_count;
    }

    // Read the right number of bits
    let value_bits: u64 = read_bits(reader, middle_bits_count as u32)?;

    // Compute the new value
    let new_value = f64::from_bits(previous_value.to_bits() ^ (value_bits << trailing_bits_count));

    Ok((new_value, leading_bits_count, trailing_bits_count))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::series::chunks::stream::bitstream::BitStream;
    use crate::series::chunks::stream::bitstream_reader::BitStreamReader;
    use rand::{RngExt, SeedableRng};

    #[test]
    fn wide_window_can_shrink_even_when_the_xor_fits() {
        let mut writer = BitStream::new();
        let value = f64::from_bits(0xf << 36);
        let state = write_varbit_xor(value, 0.0, 0, 0, &mut writer).unwrap();
        assert_eq!(state, (24, 36));
        assert_eq!(writer.len() * 8 - writer.count as usize, 17);
        let mut reader = BitStreamReader::new(writer.bytes());
        let (got, leading, trailing) = read_varbit_xor(&mut reader, 0.0, 0, 0).unwrap();
        assert_eq!(got.to_bits(), value.to_bits());
        assert_eq!((leading, trailing), state);
    }

    #[test]
    fn equal_cost_prefers_reusing_the_window() {
        let mut writer = BitStream::new();
        // Old width 15, new width 4: both choices cost 17 bits.
        let state = write_varbit_xor(f64::from_bits(0xf << 36), 0.0, 20, 29, &mut writer).unwrap();
        assert_eq!(state, (20, 29));
        assert_eq!(writer.len() * 8 - writer.count as usize, 17);
    }

    #[test]
    fn middle_width_windows_avoid_header_churn() {
        for width in [38, 37, 20] {
            let mut writer = BitStream::new();
            let xor = ((1u64 << width) - 1) << 12;
            let state = write_varbit_xor(f64::from_bits(xor), 0.0, 4, 4, &mut writer).unwrap();
            assert_eq!(state, (4, 4));
            assert_eq!(writer.len() * 8 - writer.count as usize, 58);
        }
    }

    #[test]
    fn extended_window_can_shrink_a_middle_width_legacy_window() {
        let mut writer = BitStream::new();
        let state = write_varbit_xor(f64::from_bits(2), 0.0, 31, 1, &mut writer).unwrap();
        assert_eq!(state, (62, 1));
        assert_eq!(writer.len() * 8 - writer.count as usize, 19);
    }

    #[test]
    fn full_width_window_can_shrink_for_one_saved_bit() {
        let mut writer = BitStream::new();
        let xor = ((1u64 << 52) - 1) << 6;
        let state = write_varbit_xor(f64::from_bits(xor), 0.0, 0, 0, &mut writer).unwrap();
        assert_eq!(state, (6, 6));
        assert_eq!(writer.len() * 8 - writer.count as usize, 65);
    }

    #[test]
    fn extended_window_preserves_low_mantissa_bits() {
        for leading in 32..64u8 {
            let value = f64::from_bits(1u64 << (63 - leading));
            let mut writer = BitStream::new();
            let state = write_varbit_xor(value, 0.0, 0xff, 0, &mut writer).unwrap();
            // The escape's extra five bits only pay off above 36 leading zeros.
            assert_eq!(
                state.0,
                if leading > 36 {
                    leading
                } else {
                    leading.min(31)
                }
            );
            let mut reader = BitStreamReader::new(writer.bytes());
            let (got, l, t) = read_varbit_xor(&mut reader, 0.0, 0, 0).unwrap();
            assert_eq!(got.to_bits(), value.to_bits());
            assert_eq!((l, t), state);
        }
    }

    #[test]
    fn extended_windows_reject_invalid_and_truncated_headers() {
        for fields in [None, Some(0), Some(2)] {
            let mut writer = BitStream::new();
            // A zero width code with leading=31 escapes to actual leading=63.
            writer.write_bits(13, (0b11 << 11) | (31 << 6)).unwrap();
            if let Some(width) = fields {
                writer.write_bits(5, width).unwrap();
            }
            let mut reader = BitStreamReader::new(writer.bytes());
            assert!(read_varbit_xor(&mut reader, 0.0, 0, 0).is_err());
        }
    }

    fn generate_random_test_data(seed: u64) -> Vec<Vec<f64>> {
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);

        let mut test_cases = Vec::with_capacity(128);
        for _ in 0..128 {
            let vec_size = rng.random_range(1..129);
            let mut vec = Vec::with_capacity(vec_size);

            let mut value: f64 = rng.random();
            vec.push(value);

            for _ in 1..vec_size {
                if rng.random_bool(0.33) {
                    value += 1.0;
                } else if rng.random_bool(0.33) {
                    value = rng.random();
                }
                vec.push(value);
            }
            test_cases.push(vec);
        }
        test_cases
    }

    #[test]
    fn test_write_varbit_xor() {
        let mut test_cases = generate_random_test_data(42);

        // add just a test case with the weird clamping
        test_cases.push(vec![f64::MAX, 0.0, f64::MIN, f64::MAX, f64::MIN]);

        for test_case in test_cases {
            // Writing first
            let mut bit_writer = BitStream::new();

            let mut value = 0.0;
            let mut leading = 0xff;
            let mut trailing = 0;

            for number in test_case.iter().cloned() {
                let (new_leading, new_trailing) =
                    write_varbit_xor(number, value, leading, trailing, &mut bit_writer).unwrap();
                value = number;
                leading = new_leading;
                trailing = new_trailing;
            }

            // bit_writer.byte_align().unwrap();

            // Read again
            value = 0.0;
            leading = 0;
            trailing = 0;

            let buffer = bit_writer.get_ref();
            let mut reader = BitStreamReader::new(buffer);

            for (i, number) in test_case.iter().enumerate() {
                let (new_value, new_leading, new_trailing) =
                    read_varbit_xor(&mut reader, value, leading, trailing).unwrap();

                assert_eq!(new_value, *number, "Failed at index {i}");
                value = new_value;
                leading = new_leading;
                trailing = new_trailing;
            }
        }
    }
}
