use crate::error::TsdbError;
use std::fmt;
use valkey_module::ValkeyError;

/// Decoding varint error.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum DecodeError {
    /// The buffer does not contain a valid LEB128 encoding.
    Overflow,
    /// The buffer does not contain enough data to decode.
    InsufficientData {
        /// Requested number of bytes to decode the value.
        requested: usize,
        /// The number of bytes available in the buffer.
        available: usize,
    },
    InvalidUTF8,
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DecodeError::Overflow => write!(f, "decoded value would overflow the target type"),
            DecodeError::InsufficientData {
                available,
                requested,
            } => write!(
                f,
                "not enough bytes to decode value: {available} available, but {requested} requested",
            ),
            DecodeError::InvalidUTF8 => write!(f, "invalid UTF-8 sequence"),
        }
    }
}

impl From<DecodeError> for TsdbError {
    fn from(err: DecodeError) -> Self {
        TsdbError::DecodingError(err.to_string())
    }
}

impl From<DecodeError> for ValkeyError {
    fn from(err: DecodeError) -> Self {
        let msg = format!("TSDB: decoding error: {err}");
        ValkeyError::String(msg)
    }
}

impl DecodeError {
    /// Creates a new `DecodeError::InsufficientData` indicating that the buffer does not have enough data
    /// to decode a value.
    #[inline]
    pub const fn insufficient_data(available: usize, requested: usize) -> Self {
        Self::InsufficientData {
            available,
            requested,
        }
    }
}

pub type DecodeResult<T> = Result<T, DecodeError>;
/// Writes an unsigned varint to the buffer
pub(crate) fn write_uvarint(buf: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        buf.push((value as u8) | 0x80);
        value >>= 7;
    }
    buf.push(value as u8);
}

/// Writes a single u8 byte to the buffer.
pub(crate) fn write_u8(buf: &mut Vec<u8>, value: u8) {
    buf.push(value);
}

/// Writes a signed varint using zigzag encoding
pub(crate) fn write_signed_varint(buf: &mut Vec<u8>, value: i64) {
    // Use zigzag encoding for signed values
    let unsigned = zigzag_encode(value);
    write_uvarint(buf, unsigned);
}

pub(crate) fn write_byte_slice(buf: &mut Vec<u8>, slice: &[u8]) {
    buf.reserve(slice.len() + 3);
    write_uvarint(buf, slice.len() as u64);
    if slice.is_empty() {
        return;
    }
    buf.extend_from_slice(slice);
}

pub(crate) fn try_read_byte_slice<'a>(buf: &mut &'a [u8]) -> DecodeResult<&'a [u8]> {
    let len = usize::try_from(try_read_uvarint(buf)?).map_err(|_| DecodeError::Overflow)?;

    if len > buf.len() {
        return Err(DecodeError::insufficient_data(buf.len(), len)); // Not enough data
    }

    let slice = &buf[..len];
    *buf = &buf[len..];
    Ok(slice)
}

/// Most bytes a `u64` LEB128 encoding can take: nine 7-bit groups and a tenth holding bit 63.
const MAX_UVARINT_LEN: usize = 10;

/// Reads an unsigned LEB128 varint from the buffer, advancing it past the encoding on success.
///
/// Fails with [`DecodeError::InsufficientData`] when the buffer ends before the terminal byte,
/// and with [`DecodeError::Overflow`] when the encoding cannot be a `u64`: more than ten bytes,
/// or a tenth byte carrying payload bits above bit 63. On failure the buffer is left unchanged.
pub(crate) fn try_read_uvarint(buf: &mut &[u8]) -> DecodeResult<u64> {
    let mut value: u64 = 0;

    for (i, &byte) in buf.iter().enumerate() {
        if i == MAX_UVARINT_LEN - 1 && byte > 0x01 {
            // The tenth byte has room for bit 63 only: anything else (a continuation bit or
            // more payload) would be silently shifted out and alias a smaller value.
            return Err(DecodeError::Overflow);
        }
        value |= ((byte & 0x7F) as u64) << (7 * i);
        if byte & 0x80 == 0 {
            *buf = &buf[i + 1..];
            return Ok(value);
        }
    }

    Err(DecodeError::insufficient_data(buf.len(), buf.len() + 1))
}

/// Reads a single u8 byte from the buffer.
pub(crate) fn try_read_u8(buf: &mut &[u8]) -> DecodeResult<u8> {
    if buf.is_empty() {
        return Err(DecodeError::insufficient_data(buf.len(), 1));
    }
    let val = buf[0];
    *buf = &buf[1..];
    Ok(val)
}

/// Reads a signed varint from the buffer using zigzag encoding
pub(crate) fn try_read_signed_varint(buf: &mut &[u8]) -> DecodeResult<i64> {
    try_read_uvarint(buf).map(zigzag_decode)
}

pub(crate) fn write_u64_le(buf: &mut Vec<u8>, value: u64) {
    buf.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn try_read_u64_le(buf: &mut &[u8]) -> DecodeResult<u64> {
    const U64_BYTE_SIZE: usize = size_of::<u64>();

    if buf.len() < U64_BYTE_SIZE {
        return Err(DecodeError::insufficient_data(buf.len(), U64_BYTE_SIZE));
    }
    let mut array = [0u8; U64_BYTE_SIZE];
    array.copy_from_slice(&buf[..U64_BYTE_SIZE]);
    *buf = &buf[U64_BYTE_SIZE..];
    Ok(u64::from_le_bytes(array))
}

pub(crate) fn write_f64_le(buf: &mut Vec<u8>, value: f64) {
    let bits = value.to_bits();
    write_u64_le(buf, bits)
}

#[inline]
pub(crate) fn try_read_f64_le(buf: &mut &[u8]) -> DecodeResult<f64> {
    try_read_u64_le(buf).map(f64::from_bits)
}

// see: http://stackoverflow.com/a/2211086/56332
// casting required because operations like unary negation
// cannot be performed on unsigned integers
#[inline]
pub(crate) fn zigzag_decode(from: u64) -> i64 {
    ((from >> 1) ^ (-((from & 1) as i64)) as u64) as i64
}

#[inline]
pub(crate) fn zigzag_encode(from: i64) -> u64 {
    ((from << 1) ^ (from >> 63)) as u64
}

const BYTE_WIDTH: usize = size_of::<u64>();
const BIT_WIDTH: usize = BYTE_WIDTH * 8;

// from bitter crate
#[inline]
pub(crate) fn sign_extend(val: u64, bits: u32) -> i64 {
    // Branchless sign extension from bit twiddling hacks:
    // https://graphics.stanford.edu/~seander/bithacks.html#VariableSignExtend
    //
    // The 3 operation approach with division turned out to be significantly slower,
    // and so was not used.
    debug_assert!(val.leading_zeros() as usize >= (BIT_WIDTH - bits as usize));
    let m = 1i64.wrapping_shl(bits.wrapping_sub(1));
    #[allow(clippy::cast_possible_wrap)]
    let val = val as i64;
    (val ^ m) - m
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn encode(value: u64) -> Vec<u8> {
        let mut buf = Vec::new();
        write_uvarint(&mut buf, value);
        buf
    }

    #[test]
    fn uvarint_max_round_trips_in_ten_bytes() {
        let bytes = encode(u64::MAX);
        assert_eq!(bytes.len(), MAX_UVARINT_LEN);
        assert_eq!(bytes[MAX_UVARINT_LEN - 1], 0x01);
        let mut buf = bytes.as_slice();
        assert_eq!(try_read_uvarint(&mut buf), Ok(u64::MAX));
        assert!(buf.is_empty());
    }

    #[test]
    fn uvarint_rejects_tenth_byte_payload_beyond_u64() {
        // Nine continuation bytes leave room for bit 63 only. 0x02 used to decode as 0, and
        // 0x7f as the same value as 0x01.
        for last in [0x02u8, 0x7f] {
            let mut bytes = vec![0x80u8; 9];
            bytes.push(last);
            let mut buf = bytes.as_slice();
            assert_eq!(
                try_read_uvarint(&mut buf),
                Err(DecodeError::Overflow),
                "{last:#x}"
            );
            assert_eq!(
                buf.len(),
                bytes.len(),
                "a failed read must not consume input"
            );
        }

        let mut bytes = vec![0x80u8; 9];
        bytes.push(0x01);
        assert_eq!(try_read_uvarint(&mut bytes.as_slice()), Ok(1 << 63));
    }

    #[test]
    fn uvarint_rejects_more_than_ten_bytes() {
        for len in [11, 12, 20] {
            let mut bytes = vec![0xffu8; len - 1];
            bytes.push(0x00);
            assert_eq!(
                try_read_uvarint(&mut bytes.as_slice()),
                Err(DecodeError::Overflow),
                "{len} bytes"
            );
        }
        // A tenth byte that is still a continuation fails even with nothing after it.
        let bytes = [0x80u8; 10];
        assert_eq!(
            try_read_uvarint(&mut bytes.as_slice()),
            Err(DecodeError::Overflow)
        );
    }

    #[test]
    fn uvarint_truncation_is_insufficient_data() {
        let mut empty: &[u8] = &[];
        assert!(matches!(
            try_read_uvarint(&mut empty),
            Err(DecodeError::InsufficientData { available: 0, .. })
        ));

        let full = encode(u64::MAX);
        for cut in 1..full.len() {
            let mut buf = &full[..cut];
            assert!(
                matches!(
                    try_read_uvarint(&mut buf),
                    Err(DecodeError::InsufficientData { available, .. }) if available == cut
                ),
                "cut at {cut}"
            );
            assert_eq!(buf.len(), cut, "a failed read must not consume input");
        }
    }

    #[test]
    fn byte_slice_length_past_buffer_fails() {
        let mut bytes = encode(u64::MAX);
        bytes.extend_from_slice(b"abc");
        let result = try_read_byte_slice(&mut bytes.as_slice());
        if usize::BITS < u64::BITS {
            assert_eq!(result, Err(DecodeError::Overflow));
        } else {
            assert!(matches!(
                result,
                Err(DecodeError::InsufficientData { available: 3, .. })
            ));
        }
    }

    #[test]
    fn byte_slice_round_trips() {
        let mut bytes = Vec::new();
        write_byte_slice(&mut bytes, b"hello");
        write_byte_slice(&mut bytes, b"");
        let mut buf = bytes.as_slice();
        assert_eq!(try_read_byte_slice(&mut buf), Ok(&b"hello"[..]));
        assert_eq!(try_read_byte_slice(&mut buf), Ok(&b""[..]));
        assert!(buf.is_empty());
    }

    proptest! {
        #[test]
        fn uvarint_round_trips(value: u64, tail in proptest::collection::vec(any::<u8>(), 0..4)) {
            let mut bytes = encode(value);
            prop_assert!(bytes.len() <= MAX_UVARINT_LEN);
            bytes.extend_from_slice(&tail);
            let mut buf = bytes.as_slice();
            prop_assert_eq!(try_read_uvarint(&mut buf), Ok(value));
            prop_assert_eq!(buf, &tail[..]);
        }

        #[test]
        fn signed_varint_round_trips(value: i64) {
            let mut bytes = Vec::new();
            write_signed_varint(&mut bytes, value);
            let mut buf = bytes.as_slice();
            prop_assert_eq!(try_read_signed_varint(&mut buf), Ok(value));
            prop_assert!(buf.is_empty());
        }
    }
}
