use core::hash::Hasher;
use xxhash_rust::xxh3::Xxh3Default;

/// A hasher whose output is fully specified: 64-bit XXH3 (seed 0, default secret) over the
/// written bytes, with every integer written as little-endian bytes (`usize`/`isize` widened to
/// 64 bits).
///
/// Use it for hashes compared between processes or nodes (the cluster slot fingerprint,
/// `DEBUG DIGEST` contributions), where two builds must agree whatever their dependency
/// versions, target features or byte order. The XXH3 output is fixed by its specification, and
/// the integer overrides below replace `Hasher`'s native-endian defaults. Collections keep
/// `ahash`, whose randomized state this deliberately lacks.
///
/// Not collision-resistant against chosen inputs; it is an equality check between trusted peers.
#[derive(Clone)]
pub struct StableHasher(Xxh3Default);

impl StableHasher {
    pub const fn new() -> Self {
        Self(Xxh3Default::new())
    }
}

impl Default for StableHasher {
    fn default() -> Self {
        Self::new()
    }
}

impl core::fmt::Debug for StableHasher {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_tuple("StableHasher").field(&self.finish()).finish()
    }
}

impl Hasher for StableHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0.digest()
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
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

    /// 1,000 bytes: past the 240-byte short-input path and several 256-byte stripes.
    fn long_input() -> Vec<u8> {
        (0..1000u32).map(|i| (i * 31 % 251) as u8).collect()
    }

    /// Values from independent XXH3 implementations, not from the crate under test
    /// (`xxhash-rust`): `twox-hash` 2.1.5 (`XxHash3_64::oneshot`), cross-checked against the C
    /// reference libxxhash 0.8.4 (via Python `xxhash` 4.1.0). The empty-input value is the one in
    /// the XXH3 specification. The `u16` value in `integers_hash_as_little_endian_bytes` comes
    /// from the same sources.
    #[test]
    fn matches_xxh3_64_reference_vectors() {
        assert_eq!(hash_bytes(b""), 0x2d06800538d394c2);
        assert_eq!(hash_bytes(b"a"), 0xe6c632b61e964e1f);
        assert_eq!(hash_bytes(b"foobar"), 0xd78fda63144c5c84);
        assert_eq!(hash_bytes(&long_input()), 0x404e1417800eb486);
    }

    /// The hash depends on the byte stream only, never on how it was split into writes.
    #[test]
    fn split_writes_hash_like_one_write() {
        let input = long_input();
        let whole = hash_bytes(&input);
        for chunk in [1, 7, 64, 239, 240, 241, 255, 256, 257, 999] {
            let mut hasher = StableHasher::new();
            for part in input.chunks(chunk) {
                hasher.write(part);
            }
            assert_eq!(hasher.finish(), whole, "chunk size {chunk}");
        }
    }

    #[test]
    fn integers_hash_as_little_endian_bytes() {
        let mut hasher = StableHasher::new();
        hasher.write_u16(0x0102);
        assert_eq!(hasher.finish(), hash_bytes(&[0x02, 0x01]));
        assert_eq!(hasher.finish(), 0x66e9c4aa7c53ef78);

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
