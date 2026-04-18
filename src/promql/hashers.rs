use crate::common::time::system_time_to_millis;
use crate::labels::Label;
use crate::promql::generated::Label as ProtoLabel;
use blart::AsBytes;
use promql_parser::label::{MatchOp, Matcher};
use promql_parser::parser::{AtModifier, Offset, VectorSelector};
use smallvec::{SmallVec, smallvec};
use std::cmp::Ordering;
use std::collections::HashSet;
use std::hash::{BuildHasherDefault, Hasher};
use twox_hash::xxhash3_128;

/// Hashable representation of Offset
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub(in crate::promql) enum OffsetKey {
    Pos(i64),
    Neg(i64),
}

impl From<&Offset> for OffsetKey {
    fn from(offset: &Offset) -> Self {
        match offset {
            Offset::Pos(d) => OffsetKey::Pos(d.as_millis() as i64),
            Offset::Neg(d) => OffsetKey::Neg(d.as_millis() as i64),
        }
    }
}

/// Hashable representation of AtModifier
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub(in crate::promql) enum AtKey {
    At(i64),
    Start,
    End,
}

impl From<&AtModifier> for AtKey {
    fn from(at: &AtModifier) -> Self {
        match at {
            AtModifier::At(t) => AtKey::At(system_time_to_millis(*t)),
            AtModifier::Start => AtKey::Start,
            AtModifier::End => AtKey::End,
        }
    }
}

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
fn create_hasher() -> xxhash3_128::Hasher {
    xxhash3_128::Hasher::with_seed(HASH_SEED)
}

impl HasFingerprint for [Label] {
    fn fingerprint(&self) -> SeriesFingerprint {
        let mut hasher = create_hasher();
        for label in self {
            hash_key_value(&mut hasher, &label.name, &label.value);
        }
        hasher.finish_128()
    }
}

impl HasFingerprint for [ProtoLabel] {
    fn fingerprint(&self) -> SeriesFingerprint {
        let mut hasher = create_hasher();
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

impl HasFingerprint for VectorSelector {
    fn fingerprint(&self) -> SeriesFingerprint {
        let mut hasher = create_hasher();
        update_hasher_for_vector_selector(self, &mut hasher);
        hasher.finish_128()
    }
}

pub(in crate::promql) fn update_hasher_for_vector_selector(
    vs: &VectorSelector,
    hasher: &mut xxhash3_128::Hasher,
) {
    fn update_list(list: &Vec<Matcher>, hasher: &mut xxhash3_128::Hasher) {
        let mut keys: SmallVec<&Matcher, 6> = smallvec![];
        for m in list {
            keys.push(m);
        }

        keys.sort_by(|&a, &b| a.name.cmp(&b.name).then(a.value.cmp(&b.value)));

        for m in keys {
            update_hash_for_matcher(m, hasher);
        }
    }

    if let Some(name) = &vs.name {
        hasher.write(name.as_bytes());
    }

    update_list(&vs.matchers.matchers, hasher);

    // Sort or_matchers lists to ensure order-independent hashing
    let mut sorted_or_matchers: SmallVec<&Vec<Matcher>, 8> = smallvec![];
    for m in &vs.matchers.or_matchers {
        sorted_or_matchers.push(m);
    }

    sorted_or_matchers.sort_by(|&a, &b| {
        // Compare lists lexicographically
        for (ma, mb) in a.iter().zip(b.iter()) {
            match ma.name.cmp(&mb.name) {
                Ordering::Equal => match ma.value.cmp(&mb.value) {
                    Ordering::Equal => continue,
                    other => return other,
                },
                other => return other,
            }
        }
        a.len().cmp(&b.len())
    });

    for m in &sorted_or_matchers {
        update_list(m, hasher);
    }
}

fn update_hash_for_matcher(m: &Matcher, hasher: &mut xxhash3_128::Hasher) {
    hash_key_value(hasher, &m.name, &m.value);
    match &m.op {
        MatchOp::Equal => {
            hasher.write(b"=");
        }
        MatchOp::NotEqual => {
            hasher.write(b"!");
        }
        MatchOp::Re(r) => {
            hasher.write(b"=");
            hasher.write(r.as_str().as_bytes())
        }
        MatchOp::NotRe(regex) => {
            hasher.write(b"!");
            hasher.write(regex.as_str().as_bytes())
        }
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

/// Canonical key for caching selector results across steps.
/// Derived from VectorSelector's matchers (which determine which series match).
/// Offset and @ modifiers are excluded — they affect time windows, not series selection.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Copy)]
pub(in crate::promql) struct SelectorKey(u128);

impl SelectorKey {
    pub(crate) fn from_selector(selector: &VectorSelector) -> Self {
        let signature = selector.fingerprint();
        Self(signature)
    }
}

/// Structural key for preloaded instant vector data.
/// Captures selector identity + time modifiers that affect which samples map to which steps.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(in crate::promql) struct PreloadKey {
    selector: SelectorKey,
    offset: Option<OffsetKey>,
    at: Option<AtKey>,
}

impl PreloadKey {
    pub(crate) fn from_selector(vs: &VectorSelector) -> Self {
        Self {
            selector: SelectorKey::from_selector(vs),
            offset: vs.offset.as_ref().map(OffsetKey::from),
            at: vs.at.as_ref().map(AtKey::from),
        }
    }
}

impl HasFingerprint for PreloadKey {
    fn fingerprint(&self) -> SeriesFingerprint {
        let mut hasher = xxhash3_128::Hasher::new();
        hasher.write(self.selector.0.to_le_bytes().as_ref());
        if let Some(offset) = &self.offset {
            match offset {
                OffsetKey::Pos(p) => hasher.write(&p.to_le_bytes()),
                OffsetKey::Neg(p) => hasher.write(&p.to_le_bytes()),
            }
        }
        if let Some(at) = &self.at {
            match at {
                AtKey::Start => hasher.write(b"-"),
                AtKey::End => hasher.write(b"+"),
                AtKey::At(v) => hasher.write(&v.to_le_bytes()),
            }
        }
        hasher.finish_128()
    }
}
