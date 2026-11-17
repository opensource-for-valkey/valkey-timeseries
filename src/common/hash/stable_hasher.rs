use core::hash::Hasher;

const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// A hasher whose output is fully specified: 64-bit FNV-1a over the written bytes, with every
/// integer written as little-endian bytes (`usize`/`isize` widened to 64 bits).
///
/// Use it for hashes compared between processes or nodes (the cluster slot fingerprint,
/// `DEBUG DIGEST` contributions), where two builds must agree whatever their dependency
/// versions, target features or byte order. [`DeterministicHasher`](super::DeterministicHasher)
/// is fixed-seeded `ahash`, whose output is not specified and differs across ahash versions and
/// between AES and non-AES builds: keep it for values that never leave the process.
///
/// Not collision-resistant against chosen inputs; it is an equality check between trusted peers.
#[derive(Clone, Copy, Debug)]
pub struct StableHasher(u64);

impl StableHasher {
    pub const fn new() -> Self {
        Self(FNV_OFFSET_BASIS)
    }
}

impl Default for StableHasher {
    fn default() -> Self {
        Self::new()
    }
}

impl Hasher for StableHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        let mut hash = self.0;
        for &byte in bytes {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        self.0 = hash;
    }

    fn write_u8(&mut self, n: u8) {
        self.write(&[n]);
    }
    fn write_u16(&mut self, n: u16) {
        self.write(&n.to_le_bytes());
    }
    fn write_u32(&mut self, n: u32) {
        self.write(&n.to_le_bytes());
    }
    fn write_u64(&mut self, n: u64) {
        self.write(&n.to_le_bytes());
    }
    fn write_u128(&mut self, n: u128) {
        self.write(&n.to_le_bytes());
    }
    fn write_usize(&mut self, n: usize) {
        self.write_u64(n as u64);
    }
    fn write_i8(&mut self, n: i8) {
        self.write_u8(n as u8);
    }
    fn write_i16(&mut self, n: i16) {
        self.write_u16(n as u16);
    }
    fn write_i32(&mut self, n: i32) {
        self.write_u32(n as u32);
    }
    fn write_i64(&mut self, n: i64) {
        self.write_u64(n as u64);
    }
    fn write_i128(&mut self, n: i128) {
        self.write_u128(n as u128);
    }
    fn write_isize(&mut self, n: isize) {
        self.write_i64(n as i64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash_bytes(bytes: &[u8]) -> u64 {
        let mut hasher = StableHasher::new();
        hasher.write(bytes);
        hasher.finish()
    }

    #[test]
    fn matches_fnv1a_64_reference_vectors() {
        assert_eq!(hash_bytes(b""), 0xcbf29ce484222325);
        assert_eq!(hash_bytes(b"a"), 0xaf63dc4c8601ec8c);
        assert_eq!(hash_bytes(b"foobar"), 0x85944171f73967e8);
    }

    #[test]
    fn integers_hash_as_little_endian_bytes() {
        let mut hasher = StableHasher::new();
        hasher.write_u16(0x0102);
        assert_eq!(hasher.finish(), hash_bytes(&[0x02, 0x01]));
        assert_eq!(hasher.finish(), 0x08395307b4f1348c);

        let mut a = StableHasher::new();
        a.write_usize(7);
        let mut b = StableHasher::new();
        b.write_u64(7);
        assert_eq!(a.finish(), b.finish());

        let mut a = StableHasher::new();
        a.write_isize(-3);
        let mut b = StableHasher::new();
        b.write(&(-3i64).to_le_bytes());
        assert_eq!(a.finish(), b.finish());
    }
}
