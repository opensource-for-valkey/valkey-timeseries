use crate::labels::Label;
use std::sync::LazyLock;
use twox_hash::xxhash3_128::{self, DEFAULT_SECRET_LENGTH, RawHasher, SecretBuffer};

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

/// The streaming xxhash3 hasher every label hash in the crate uses.
///
/// `xxhash3_128::Hasher` boxes a 192-byte secret on every construction —
/// one heap allocation per fingerprint, which on a fan-out coordinator is one
/// per series per query. A `RawHasher` over a `&'static` secret keeps the
/// same state, and so the same hash values, without touching the heap.
pub(crate) type LabelHasher = RawHasher<&'static [u8; DEFAULT_SECRET_LENGTH]>;

/// `xxhash3_128::Hasher::with_seed(HASH_SEED)` derives this from the default
/// secret on every call; derive it once.
static SEEDED_SECRET: LazyLock<[u8; DEFAULT_SECRET_LENGTH]> = LazyLock::new(|| {
    SecretBuffer::with_seed(HASH_SEED, [0u8; DEFAULT_SECRET_LENGTH])
        .unwrap_or_else(|_| unreachable!("the default secret length satisfies with_seed"))
        .into_secret()
});

/// A hasher seeded with [`HASH_SEED`]; hashes equal `Hasher::with_seed(HASH_SEED)`.
#[inline]
pub(crate) fn create_hasher() -> LabelHasher {
    let secret: &'static [u8; DEFAULT_SECRET_LENGTH] = &SEEDED_SECRET;
    RawHasher::new(
        SecretBuffer::new(HASH_SEED, secret)
            .unwrap_or_else(|_| unreachable!("the default secret length satisfies new")),
    )
}

/// An unseeded hasher; hashes equal `Hasher::new()`.
#[inline]
pub(crate) fn create_unseeded_hasher() -> LabelHasher {
    RawHasher::new(SecretBuffer::default())
}

/// Written after every label name and every label value.
///
/// The byte 0xFF never occurs in UTF-8, so a sequence of framed labels decodes
/// one way only: `{a="x", b="y"}`, `{a="xb", ""="y"}` and `{a="", xb="y"}`
/// all hash differently. (The same framing as Prometheus' label hash.)
pub(crate) const LABEL_SEP: u8 = 0xff;

/// Feed one `name=value` pair into a label-set hash, framed by [`LABEL_SEP`].
pub(crate) fn hash_key_value(hasher: &mut LabelHasher, key: &str, value: &str) {
    hasher.write(key.as_bytes());
    hasher.write(&[LABEL_SEP]);
    hasher.write(value.as_bytes());
    hasher.write(&[LABEL_SEP]);
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The borrowed-secret hashers must produce exactly what the allocating
    /// Label boundaries must survive hashing: the separator used to be the
    /// four ASCII bytes `0xfe`, with nothing after a value, so a value
    /// containing that text (or an empty value) could stand in for a label
    /// boundary and two different label sets shared a fingerprint.
    #[test]
    fn label_boundaries_are_part_of_the_fingerprint() {
        use crate::labels::{Label, fingerprint_labels};
        let set = |pairs: &[(&str, &str)]| -> Vec<Label> {
            pairs.iter().map(|(n, v)| Label::new(*n, *v)).collect()
        };
        let fp = |pairs: &[(&str, &str)]| fingerprint_labels(set(pairs).iter());
        let two = fp(&[("a", "x"), ("b", "y")]);
        assert_ne!(two, fp(&[("a", "xb0xfey")]));
        assert_ne!(two, fp(&[("a", ""), ("xb", "y")]));
        assert_ne!(two, fp(&[("a", "xb"), ("", "y")]));
        assert_ne!(fp(&[("a", "0xfeb")]), fp(&[("a0xfe", "b")]));

        // `impl Hash for Label` frames the same way.
        let std_hash = |pairs: &[(&str, &str)]| {
            use std::hash::{BuildHasher, RandomState};
            thread_local!(static STATE: RandomState = RandomState::new());
            STATE.with(|s| s.hash_one(set(pairs)))
        };
        let two = std_hash(&[("a", "x"), ("b", "y")]);
        assert_ne!(two, std_hash(&[("a", ""), ("xb", "y")]));
        assert_ne!(two, std_hash(&[("a", "xb"), ("", "y")]));
    }

    /// `xxhash3_128::Hasher` constructors produce: fingerprints are compared
    /// across shards and used as map keys, so they cannot drift.
    #[test]
    fn raw_hashers_match_allocating_hashers() {
        let inputs: [&[u8]; 4] = [
            b"",
            b"host",
            b"__name__=cpu,host=h1,region=us",
            &[7u8; 1024],
        ];
        for input in inputs {
            let mut seeded = create_hasher();
            seeded.write(input);
            let mut boxed = xxhash3_128::Hasher::with_seed(HASH_SEED);
            boxed.write(input);
            assert_eq!(
                seeded.finish_128(),
                boxed.finish_128(),
                "seeded, {} bytes",
                input.len()
            );

            let mut unseeded = create_unseeded_hasher();
            unseeded.write(input);
            let mut boxed = xxhash3_128::Hasher::new();
            boxed.write(input);
            assert_eq!(
                unseeded.finish_128(),
                boxed.finish_128(),
                "unseeded, {} bytes",
                input.len()
            );
        }
    }
}
