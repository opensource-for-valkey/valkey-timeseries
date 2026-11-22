use crate::common::encoding::{try_read_uvarint, write_uvarint};
use crate::common::rdb::RdbSerializable;
use crate::series::chunks::stream::traits::BitWrite;
use get_size2::GetSize;
use num_traits::PrimInt;
use std::io;
use valkey_module::digest::Digest;
use valkey_module::{RedisModuleIO, ValkeyError, ValkeyResult, raw};

/// A stream of bits for writing.
#[derive(Clone, Debug, GetSize)]
pub struct BitStream {
    /// The data stream.
    pub(in crate::series::chunks) stream: Vec<u8>,
    /// How many right-most bits are available for writing in the current byte.
    pub(in crate::series::chunks) count: u8,
    /// Allocation budget, see [`set_soft_cap`](Self::set_soft_cap). Not part of the
    /// stream's identity: equality, hashing and persistence ignore it.
    soft_cap: usize,
}

/// Bytes the allocation may run past `soft_cap`: enough for the append that crosses it.
const SOFT_CAP_SLACK: usize = 32;

impl Default for BitStream {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for BitStream {
    fn eq(&self, other: &Self) -> bool {
        self.count == other.count && self.stream == other.stream
    }
}

impl Eq for BitStream {}

impl std::hash::Hash for BitStream {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.stream.hash(state);
        self.count.hash(state);
    }
}

/// Free bits in a partial trailing byte: 0 when the stream is byte-aligned, never all 8 (the
/// writer starts a byte only to put a bit in it). A count of 8 made `write_byte_raw` shift a
/// `u8` by 8.
const MAX_FREE_BITS: u64 = 7;

fn validate_rdb_state(bytes: &[u8], count: u64) -> ValkeyResult<u8> {
    if count > MAX_FREE_BITS {
        return Err(ValkeyError::String(format!("Invalid chunk bits: {count}")));
    }

    // A partial byte can only exist after at least one byte has been
    // written. Without this check, the next append indexes the last byte of
    // an empty stream and panics.
    if count != 0 && bytes.is_empty() {
        return Err(ValkeyError::String(
            "Invalid chunk bits: non-zero count with empty stream".to_owned(),
        ));
    }

    Ok(count as u8)
}

impl BitStream {
    pub fn new() -> Self {
        Self {
            stream: Vec::new(),
            count: 0,
            soft_cap: usize::MAX,
        }
    }

    /// Cap the allocation at `cap` while the stream fits it, and at `cap` plus a few bytes of
    /// slack for the write that crosses it. Growth stays geometric below the cap, and goes
    /// back to doubling past the slack.
    ///
    /// Plain `Vec` doubling left every sealed chunk holding up to twice its data: a 4 KiB
    /// chunk crossed `max_size` on its last append and jumped to an 8 KiB allocation it
    /// never used, which `memory_usage` then reported. The slack is not free either: a
    /// 4,096-byte cap plus slack lands in jemalloc's 5,120-byte size class, so a stream within
    /// budget never allocates into it. A chunk passes its `max_size` here.
    pub fn set_soft_cap(&mut self, cap: usize) {
        self.soft_cap = cap;
    }

    /// Grows the stream when fewer than `min` bytes are spare, then returns the spare capacity.
    /// Growth doubles as for single writes (see `grow`), to at least `min` spare but never past
    /// the soft cap (nor into its slack), so the allocation stays within twice the data plus
    /// `min`. The round sizes itself from the return value, which is below `min` only when the
    /// soft cap is.
    pub fn reserve_spare(&mut self, min: usize) -> usize {
        let len = self.stream.len();
        let cap = self.stream.capacity();
        if cap - len < min {
            let doubled = cap.saturating_mul(2).max(8);
            let target = doubled.max(len.saturating_add(min)).min(self.soft_cap);
            if target > cap {
                self.stream.reserve_exact(target - len);
            }
        }
        self.stream.capacity() - len
    }

    /// Reset the stream around the provided byte slice.
    pub fn reset(&mut self, stream: Vec<u8>) {
        self.stream = stream;
        self.count = 0;
    }

    /// Room for `need` more bytes; the growth policy lives in the cold half.
    #[inline(always)]
    fn ensure_spare(&mut self, need: usize) {
        if self.stream.capacity() - self.stream.len() < need {
            self.grow(need);
        }
    }

    #[cold]
    #[inline(never)]
    fn grow(&mut self, need: usize) {
        let len = self.stream.len();
        let cap = self.stream.capacity();
        let floor = len + need;
        let doubled = cap.saturating_mul(2).max(8);
        let capped = self.soft_cap.saturating_add(SOFT_CAP_SLACK);
        // Within budget the allocation stops at the cap itself; a write that
        // crosses it takes the slack; only once the data needs more than that
        // does doubling resume. The tests are on `floor`, not `len`: a stream
        // that lands exactly on the cap with a full trailing byte grows for its
        // next single bit, and that must not read as "past the budget" and double.
        let target = if floor <= self.soft_cap {
            doubled.min(self.soft_cap).max(floor)
        } else if floor <= capped {
            capped
        } else {
            doubled.max(floor)
        };
        self.stream.reserve_exact(target - len);
    }

    pub(crate) fn serialize(&self, dest: &mut Vec<u8>) {
        write_uvarint(dest, self.count as u64);
        write_uvarint(dest, self.stream.len() as u64);
        dest.extend_from_slice(&self.stream);
    }

    pub(crate) fn deserialize(src: &mut &[u8]) -> io::Result<Self> {
        let count = try_read_uvarint(src)
            .map_err(|_| io::Error::new(io::ErrorKind::UnexpectedEof, "EOF"))?;

        if count > MAX_FREE_BITS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid bit count",
            ));
        }

        let len = try_read_uvarint(src)
            .map_err(|_| io::Error::new(io::ErrorKind::UnexpectedEof, "EOF"))?
            as usize;

        if src.len() < len {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF"));
        }

        if count != 0 && len == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid chunk bits: non-zero count with empty stream",
            ));
        }

        let stream = src[..len].to_vec();
        *src = &src[len..];

        Ok(Self {
            stream,
            count: count as u8,
            soft_cap: usize::MAX,
        })
    }

    /// Get the underlying bytes.
    pub fn bytes(&self) -> &[u8] {
        &self.stream
    }

    /// Alias used by Gorilla call sites.
    pub fn get_ref(&self) -> &[u8] {
        self.bytes()
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.stream
    }

    pub fn clear(&mut self) {
        self.stream.clear();
        self.count = 0;
    }

    pub fn len(&self) -> usize {
        self.stream.len()
    }

    /// Cuts the stream to its first `len` bytes, as a truncated load would; the partial-byte
    /// count is reset because the byte it described is gone.
    #[cfg(test)]
    pub(crate) fn truncate(&mut self, len: usize) {
        self.stream.truncate(len);
        self.count = 0;
    }

    pub fn is_empty(&self) -> bool {
        self.stream.is_empty()
    }

    pub fn position(&self) -> u8 {
        // Return the number of "used" bits in the last byte to match the
        if self.count == 0 { 0 } else { 8 - self.count }
    }

    pub fn shrink_to_fit(&mut self) {
        self.stream.shrink_to_fit();
    }

    pub fn debug_digest(&self, dig: &mut Digest) {
        dig.add_string_buffer(&self.stream);
        dig.add_long_long(self.count as i64);
    }

    /// Write a single bit to the stream.
    pub fn write_bit_raw(&mut self, bit: bool) {
        if self.count == 0 {
            self.ensure_spare(1);
            self.stream.push(0);
            self.count = 8;
        }

        let i = self.stream.len() - 1;

        if bit {
            self.stream[i] |= 1 << (self.count - 1);
        }

        self.count -= 1;
    }

    /// [`write_bit_raw`](Self::write_bit_raw); with `R`, the caller has reserved the room
    /// (see [`reserve_spare`](Self::reserve_spare)), so the growth policy is skipped.
    #[inline(always)]
    pub fn write_bit_r<const R: bool>(&mut self, bit: bool) {
        if !R {
            return self.write_bit_raw(bit);
        }
        if self.count == 0 {
            self.stream.push(0);
            self.count = 8;
        }
        let i = self.stream.len() - 1;
        if bit {
            self.stream[i] |= 1 << (self.count - 1);
        }
        self.count -= 1;
    }

    /// Write a single bit to the stream.
    pub fn write_bit(&mut self, bit: bool) {
        self.write_bit_raw(bit);
    }

    /// Write a single byte to the stream.
    pub(in crate::series::chunks) fn write_byte_raw(&mut self, byte: u8) {
        self.ensure_spare(1);
        if self.count == 0 {
            self.stream.push(byte);
            return;
        }

        let i = self.stream.len() - 1;

        // Complete the last byte with the leftmost count bits from byte.
        self.stream[i] |= byte >> (8 - self.count);

        // Write the remainder.
        self.stream.push(byte << self.count);
    }

    /// Write a single byte to the stream.
    pub fn write_byte(&mut self, byte: u8) {
        self.write_byte_raw(byte);
    }

    /// Write the right-most `bits` bits of `value` in left-to-right order.
    ///
    /// One pass: whatever fits in the partial trailing byte goes there with a
    /// single OR, and the rest lands with one `extend_from_slice` of the
    /// big-endian bytes of the shifted value -- the final byte of which is the
    /// new partial byte, its unused low bits already zero. This replaced a loop
    /// of one `push` per whole byte plus one call per remaining bit, which was
    /// two thirds of Gorilla encode time. Output is bit-identical.
    ///
    /// `inline(always)`: the codecs are the only callers and they issue two or
    /// three of these per sample; left to itself LLVM stopped inlining the
    /// function once it grew the whole-byte path, and a Chimp repeat (three
    /// writes of 1–2 bits) nearly doubled in cost from the calls alone.
    #[inline(always)]
    pub fn write_bits(&mut self, bits: u32, value: u64) -> io::Result<()> {
        self.write_bits_r::<false>(bits, value)
    }

    /// [`write_bits`](Self::write_bits); with `R`, the caller has reserved the room for the
    /// 8-byte store (see [`reserve_spare`](Self::reserve_spare)), so neither the growth policy
    /// nor the short write near the soft cap applies. Both forms produce the same bytes.
    #[inline(always)]
    pub fn write_bits_r<const R: bool>(&mut self, bits: u32, value: u64) -> io::Result<()> {
        let mut nbits = bits.min(64) as usize;
        if nbits == 0 {
            return Ok(());
        }
        // Left-align: bit `nbits - 1` of `value` becomes bit 63, and every bit
        // below the payload is zero.
        let mut u = value << (64 - nbits);

        if self.count > 0 {
            let free = self.count as usize;
            let last = self.stream.len() - 1;
            self.stream[last] |= (u >> (64 - free)) as u8;
            if nbits < free {
                self.count = (free - nbits) as u8;
                return Ok(());
            }
            u <<= free;
            nbits -= free;
            self.count = 0;
            if nbits == 0 {
                return Ok(());
            }
        }

        // Always store the full 8-byte word and then cut the length back: a
        // variable-length `extend_from_slice(&bytes[..nbytes])` compiles to a
        // libc `memcpy` call, which was ~17 % of Chimp encode; a fixed 8-byte
        // copy is a single store. `truncate` on `u8` is just a length write.
        let nbytes = nbits.div_ceil(8);
        let len = self.stream.len();
        let bytes = u.to_be_bytes();
        if R {
            // Reserved: the 8-byte store fits in spare capacity within the cap, so this never
            // reallocates. Vec still checks; skipping that bought only 2–6 % in measurement.
            self.stream.extend_from_slice(&bytes);
            self.stream.truncate(len + nbytes);
        } else if nbytes < 8 && len.saturating_add(8) > self.soft_cap {
            // A short write near the cap must not reserve the temporary eight-byte
            // copy: that would grow past the cap (or double past the slack) even when
            // the committed write fits.
            self.ensure_spare(nbytes);
            self.stream.extend_from_slice(&bytes[..nbytes]);
        } else {
            self.ensure_spare(8);
            self.stream.extend_from_slice(&bytes);
            self.stream.truncate(len + nbytes);
        }
        let rem = nbits % 8;
        if rem != 0 {
            self.count = (8 - rem) as u8;
        }
        Ok(())
    }

    pub fn write_u64(&mut self, value: u64) {
        for byte in value.to_be_bytes() {
            self.write_byte_raw(byte);
        }
    }

    pub fn write_f64(&mut self, value: f64) {
        self.write_u64(value.to_bits());
    }

    pub fn write_uvarint(&mut self, mut value: u64) -> io::Result<()> {
        while value >= 0x80 {
            self.write_byte_raw((value as u8) | 0x80);
            value >>= 7;
        }
        self.write_byte_raw(value as u8);
        Ok(())
    }

    pub fn write_varint(&mut self, value: i64) -> io::Result<()> {
        let n: u64 = ((value << 1) ^ (value >> 63)) as u64;
        self.write_uvarint(n)
    }
}

/// A [`BitStream`] in a reserved round: `BitWrite` through the reserved writes
/// (`write_bits_r::<true>`), for encoder helpers generic over `BitWrite`.
pub(crate) struct Reserved<'a>(pub(crate) &'a mut BitStream);

impl BitWrite for Reserved<'_> {
    #[inline(always)]
    fn write_bit(&mut self, bit: bool) -> io::Result<()> {
        self.0.write_bit_r::<true>(bit);
        Ok(())
    }

    #[inline(always)]
    fn write<U>(&mut self, bits: u32, value: U) -> io::Result<()>
    where
        U: PrimInt,
    {
        self.0
            .write_bits_r::<true>(bits, value.to_u64().expect("Invalid u64 cast"))
    }
}

impl BitWrite for BitStream {
    fn write_bit(&mut self, bit: bool) -> io::Result<()> {
        self.write_bit_raw(bit);
        Ok(())
    }

    fn write<U>(&mut self, bits: u32, value: U) -> io::Result<()>
    where
        U: PrimInt,
    {
        self.write_bits(bits, value.to_u64().expect("Invalid u64 cast"))
    }
}

impl RdbSerializable for BitStream {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        raw::save_slice(rdb, self.bytes());
        raw::save_unsigned(rdb, self.count as u64);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        let bytes = raw::load_string_buffer(rdb)?.as_ref().to_vec();
        let count = raw::load_unsigned(rdb)?;
        let count = validate_rdb_state(&bytes, count)?;

        Ok(Self {
            stream: bytes,
            count: count as u8,
            soft_cap: usize::MAX,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{BitStream, validate_rdb_state};
    use crate::common::encoding::write_uvarint;

    /// Landing exactly on the cap with a full trailing byte, then writing one more bit,
    /// must grow into the slack rather than double. This is the byte-boundary case a
    /// randomised chunk-fill test hit about one run in four.
    #[test]
    fn growth_at_the_exact_cap_uses_the_slack() {
        let mut bs = BitStream::new();
        bs.set_soft_cap(64);
        for _ in 0..(64 * 8) {
            bs.write_bit(false);
        }
        assert_eq!(bs.len(), 64);
        assert_eq!(bs.count, 0);
        bs.write_bit(true);
        assert!(
            bs.stream.capacity() <= 64 + super::SOFT_CAP_SLACK,
            "capacity {} after crossing the cap by one bit",
            bs.stream.capacity()
        );
        // And an 8-byte write from the same position.
        let mut bs = BitStream::new();
        bs.set_soft_cap(64);
        for _ in 0..8 {
            bs.write_bits(64, u64::MAX).unwrap();
        }
        assert_eq!(bs.len(), 64);
        bs.write_bits(64, 1).unwrap();
        assert!(bs.stream.capacity() <= 64 + super::SOFT_CAP_SLACK);
        // Well past the budget, growth is geometric again.
        for _ in 0..64 {
            bs.write_bits(64, 1).unwrap();
        }
        assert!(bs.stream.capacity() >= bs.len());
        assert!(bs.stream.capacity() > 64 + super::SOFT_CAP_SLACK);
    }

    #[test]
    fn short_write_near_cap_does_not_reserve_temporary_bytes() {
        let mut bs = BitStream::new();
        bs.set_soft_cap(64);
        for _ in 0..90 {
            bs.write_bits(8, 0).unwrap();
        }
        assert_eq!(bs.len(), 90);
        assert_eq!(bs.stream.capacity(), 64 + super::SOFT_CAP_SLACK);

        bs.write_bits(1, 1).unwrap();

        assert_eq!(bs.len(), 91);
        assert!(bs.stream.capacity() <= 64 + super::SOFT_CAP_SLACK);
    }

    /// A stream that fills its cap exactly holds exactly the cap, never the slack: a 4,096-byte
    /// chunk allocated at 4,128 bytes sat in jemalloc's 5,120-byte class. Covers a stream
    /// regrown from an odd length (a loaded chunk), whose doubling overshoots the cap.
    #[test]
    fn filling_to_the_cap_never_allocates_the_slack() {
        for start in [0usize, 9, 1500, 3000] {
            let mut bs = BitStream::new();
            bs.reset(vec![0; start]);
            bs.stream.shrink_to_fit();
            bs.set_soft_cap(4096);
            while bs.len() + 3 <= 4096 {
                bs.write_bits(23, 0x5a5a5a).unwrap();
                assert!(
                    bs.stream.capacity() <= 4096,
                    "start {start}: capacity {} at len {}",
                    bs.stream.capacity(),
                    bs.len()
                );
            }
            while bs.len() < 4096 {
                bs.write_byte(0xa5);
            }
            assert_eq!(bs.stream.capacity(), 4096, "start {start}");
        }
    }

    #[test]
    fn rdb_load_rejects_nonzero_count_with_empty_stream() {
        // This state makes the first append index stream[len - 1].
        assert!(validate_rdb_state(&[], 1).is_err());
    }

    #[test]
    fn rdb_load_accepts_byte_aligned_and_partial_streams() {
        assert!(validate_rdb_state(&[], 0).is_ok());
        assert!(validate_rdb_state(&[0], 0).is_ok());
        assert!(validate_rdb_state(&[0], 1).is_ok());
    }

    /// The writer leaves 0..=7 free bits; 8 made the next `write_byte_raw` shift a `u8` by 8.
    #[test]
    fn a_whole_free_byte_is_rejected() {
        assert!(validate_rdb_state(&[0], 7).is_ok());
        assert!(validate_rdb_state(&[0], 8).is_err());

        let mut buf = Vec::new();
        write_uvarint(&mut buf, 8); // count
        write_uvarint(&mut buf, 1); // len
        buf.push(0);
        let mut src = buf.as_slice();
        assert!(BitStream::deserialize(&mut src).is_err());
    }

    #[test]
    fn deserialize_rejects_nonzero_count_with_empty_stream() {
        let mut buf = Vec::new();
        write_uvarint(&mut buf, 1); // count
        write_uvarint(&mut buf, 0); // len
        let mut src = buf.as_slice();
        assert!(BitStream::deserialize(&mut src).is_err());
    }

    #[test]
    fn deserialize_rejects_out_of_range_count_without_truncating() {
        let mut buf = Vec::new();
        // 264 truncates to 8 as u8, which would otherwise pass the `> 8` check.
        write_uvarint(&mut buf, 264); // count
        write_uvarint(&mut buf, 0); // len
        let mut src = buf.as_slice();
        assert!(BitStream::deserialize(&mut src).is_err());
    }

    #[test]
    fn deserialize_accepts_byte_aligned_and_partial_streams() {
        let mut buf = Vec::new();
        write_uvarint(&mut buf, 3); // count
        write_uvarint(&mut buf, 2); // len
        buf.extend_from_slice(&[0xAB, 0xCD]);
        let mut src = buf.as_slice();
        let bs = BitStream::deserialize(&mut src).expect("valid partial stream");
        assert_eq!(bs.count, 3);
        assert_eq!(bs.stream, vec![0xAB, 0xCD]);
    }
}
