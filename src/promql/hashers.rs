use crate::labels::Label;
use std::collections::HashSet;
use std::hash::{BuildHasherDefault, Hasher};
use twox_hash::xxhash3_128;

/// Series fingerprint (hash of a label set)
pub(crate) type SeriesFingerprint = u128;

pub(crate) trait HasFingerprint {
    fn fingerprint(&self) -> SeriesFingerprint;
}

impl HasFingerprint for Vec<Label> {
    fn fingerprint(&self) -> SeriesFingerprint {
        self.as_slice().fingerprint()
    }
}

impl HasFingerprint for [Label] {
    fn fingerprint(&self) -> SeriesFingerprint {
        let mut hasher = xxhash3_128::Hasher::new();
        for label in self {
            hash_key_value(&mut hasher, &label.name, &label.value);
        }
        hasher.finish_128()
    }
}

impl HasFingerprint for &str {
    fn fingerprint(&self) -> SeriesFingerprint {
        xxhash3_128::Hasher::oneshot(self.as_bytes())
    }
}

impl HasFingerprint for String {
    fn fingerprint(&self) -> SeriesFingerprint {
        self.as_str().fingerprint()
    }
}

pub(in crate::promql) type FingerprintHashMap<V> =
halfbrown::HashMap<SeriesFingerprint, V, BuildHasherDefault<FingerprintHasher>>;

pub(in crate::promql) type FingerprintHashSet =
    HashSet<SeriesFingerprint, BuildHasherDefault<FingerprintHasher>>;

fn hash_key_value(hasher: &mut xxhash3_128::Hasher, key: &str, value: &str) {
    hasher.write(key.as_bytes());
    hasher.write(b"0xfe");
    hasher.write(value.as_bytes());
}

#[derive(Default)]
pub struct FingerprintHasher(u64);

impl Hasher for FingerprintHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        let mut hash = 0u64;
        for chunk in bytes.chunks(8) {
            let mut padded = [0u8; 8];
            padded[..chunk.len()].copy_from_slice(chunk);
            hash ^= u64::from_le_bytes(padded).rotate_left(13);
        }
        self.0 = hash;
    }

    fn write_u64(&mut self, value: u64) {
        self.0 = value;
    }

    fn write_u128(&mut self, value: u128) {
        let lower = value as u64;
        let upper = (value >> 64) as u64;
        self.0 = lower ^ upper.rotate_left(32);
    }

    fn write_usize(&mut self, value: usize) {
        self.0 = value as u64;
    }
}
