use crate::labels::Label;
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

const HASH_SEED: u64 = 0xa4d3f1c2b7e98d5f;
#[inline]
pub(crate) fn create_hasher() -> xxhash3_128::Hasher {
    xxhash3_128::Hasher::with_seed(HASH_SEED)
}

pub(crate) fn hash_key_value(hasher: &mut xxhash3_128::Hasher, key: &str, value: &str) {
    hasher.write(key.as_bytes());
    hasher.write(b"0xfe");
    hasher.write(value.as_bytes());
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
