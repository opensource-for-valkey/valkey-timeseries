//! Key names collected into one buffer.
//!
//! A large `TS.QUERYINDEX` resolves tens of thousands of keys. Copying each into its own
//! `Vec<u8>` cost an allocation per key to collect and a free per key to drop, together more
//! than half of such a read. A [`KeyList`] holds every key's bytes in one buffer, so both
//! grow by doubling instead.

use std::ops::Range;

/// Key names in one buffer, in the order pushed until [`KeyList::sort_unstable`].
#[derive(Default, Clone)]
pub struct KeyList {
    bytes: Vec<u8>,
    /// Where each key's bytes are in `bytes`.
    spans: Vec<Range<usize>>,
}

impl KeyList {
    pub fn new() -> Self {
        Self::default()
    }

    /// Room for `keys` more keys, without guessing their bytes.
    pub fn reserve(&mut self, keys: usize) {
        self.spans.reserve(keys);
    }

    pub fn push(&mut self, key: &[u8]) {
        let start = self.bytes.len();
        self.bytes.extend_from_slice(key);
        self.spans.push(start..self.bytes.len());
    }

    pub fn len(&self) -> usize {
        self.spans.len()
    }

    pub fn is_empty(&self) -> bool {
        self.spans.is_empty()
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = &[u8]> {
        self.spans.iter().map(|span| &self.bytes[span.clone()])
    }

    /// Byte-wise ascending, as sorting `Vec<u8>` keys orders them.
    pub fn sort_unstable(&mut self) {
        let bytes = &self.bytes;
        self.spans
            .sort_unstable_by(|a, b| bytes[a.clone()].cmp(&bytes[b.clone()]));
    }

    /// One owned `Vec<u8>` per key, for a caller that must own them separately.
    pub fn into_vecs(self) -> Vec<Vec<u8>> {
        self.iter().map(<[u8]>::to_vec).collect()
    }
}

impl std::fmt::Debug for KeyList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn list(keys: &[&[u8]]) -> KeyList {
        let mut list = KeyList::new();
        for key in keys {
            list.push(key);
        }
        list
    }

    #[test]
    fn keeps_binary_and_empty_keys_apart() {
        let keys: [&[u8]; 4] = [b"a", b"", b"\xff\0b", b"a"];
        let list = list(&keys);
        assert_eq!(list.len(), 4);
        assert_eq!(list.iter().collect::<Vec<_>>(), keys);
        assert_eq!(list.into_vecs(), keys.map(<[u8]>::to_vec));
    }

    #[test]
    fn sorts_like_owned_keys() {
        let keys: [&[u8]; 6] = [b"series:10", b"series:9", b"\xff", b"", b"series:1", b"a\0"];
        let mut list = list(&keys);
        list.sort_unstable();
        let mut owned = keys.map(<[u8]>::to_vec);
        owned.sort_unstable();
        assert_eq!(list.into_vecs(), owned);
    }

    #[test]
    fn empty() {
        let mut list = KeyList::new();
        list.sort_unstable();
        assert!(list.is_empty());
        assert_eq!(list.iter().count(), 0);
    }
}
