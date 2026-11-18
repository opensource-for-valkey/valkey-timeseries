mod no_hash;
mod stable_hasher;

pub use no_hash::*;
pub use stable_hasher::*;

use crate::common::Timestamp;

/// A `HashSet` of timestamps, keyed with a mixing hash.
///
/// Timestamps must not go in an [`IntSet`]: under the identity hash every millisecond timestamp
/// has the same `hashbrown` probe tag (its top 7 bits are 0), and aligned timestamps share the
/// low bits that pick the bucket, so inserts and lookups degrade to quadratic.
pub type TimestampSet = std::collections::HashSet<Timestamp, ahash::RandomState>;

/// Hash a 64-bit float value using bitwise representation
///
/// The only reason this function exists is to provide a consistent way to hash f64 values
/// for use in the module type's `digest()` function. F64s in rust do not implement
/// the `std::hash::Hash` trait, so we need our own implementation. The concern is not
/// uniqueness or collision resistance, but consistency.
pub(crate) fn hash_f64<H: std::hash::Hasher>(value: f64, hasher: &mut H) {
    if value.is_nan() {
        // Handle NaN as a special case. In rust there are multiple representations of NaN,
        // so we use a specific representation for consistency.
        hasher.write_u64(0x7FF8000000000000); // IEEE 754 representation of NaN
        return;
    }
    let bits = value.to_bits();
    hasher.write_u64(bits);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::hash::BuildHasher;

    /// `hashbrown` takes the probe tag from the top 7 bits of the hash and the bucket from the
    /// low bits. Aligned timestamps must spread over both, or every probe compares keys.
    #[test]
    fn timestamp_set_spreads_aligned_timestamps() {
        const COUNT: i64 = 20_000;
        const STRIDE: i64 = 1 << 20;

        let set = TimestampSet::default();
        let hashes: Vec<u64> = (1..=COUNT)
            .map(|i| set.hasher().hash_one(i * STRIDE))
            .collect();

        let tags: HashSet<u64> = hashes.iter().map(|h| h >> 57).collect();
        assert!(tags.len() > 120, "only {} of 128 tags used", tags.len());

        // 2^15 buckets for 20,000 keys: a uniform hash fills ~46 % of them.
        let buckets: HashSet<u64> = hashes.iter().map(|h| h & ((1 << 15) - 1)).collect();
        assert!(
            buckets.len() > 12_000,
            "only {} buckets used",
            buckets.len()
        );

        // The identity hash used by `IntSet` fails both checks on the same keys.
        let identity = BuildNoHashHasher::<i64>::default();
        let identity_tags: HashSet<u64> = (1..=COUNT)
            .map(|i| identity.hash_one(i * STRIDE) >> 57)
            .collect();
        assert_eq!(identity_tags.len(), 1);
    }

    #[test]
    fn timestamp_set_aligned_inserts_and_lookups() {
        const STRIDE: i64 = 1 << 20;
        let mut set = TimestampSet::with_capacity_and_hasher(20_000, Default::default());
        for i in 0..20_000 {
            assert!(set.insert(i * STRIDE));
        }
        assert_eq!(set.len(), 20_000);
        assert!((0..20_000).all(|i| set.contains(&(i * STRIDE))));
        assert!(!set.contains(&(STRIDE + 1)));
    }
}
