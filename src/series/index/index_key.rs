use blart::{AsBytes, NoPrefixesBytes};
use get_size2::GetSize;
use std::borrow::Borrow;
use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::ops::Deref;

/// A `name=value` label-index key, NUL-terminated so no key is a prefix of another (the radix
/// tree requires that).
#[derive(Debug, Clone, PartialEq, Eq, GetSize)]
pub struct IndexKey {
    bytes: Box<[u8]>,
    /// Byte offset of the first `=`, cached so [`IndexKey::split`] needn't search for it. Always
    /// derived from `bytes`, so equal bytes imply an equal `split`. [`NO_SPLIT`] when the key has
    /// no `=` or the offset doesn't fit; `split` searches instead.
    split: u16,
}

const SENTINEL: u8 = 0;
const NO_SPLIT: u16 = u16::MAX;

impl IndexKey {
    pub fn for_label_value(label_name: &str, value: &str) -> Self {
        // The first `=`, as `split` has always reported it — inside the name if the name has one.
        let split = label_name.find('=').unwrap_or(label_name.len());
        let mut bytes = Vec::with_capacity(label_name.len() + value.len() + 2);
        bytes.extend_from_slice(label_name.as_bytes());
        bytes.push(b'=');
        bytes.extend_from_slice(value.as_bytes());
        Self::from_parts(bytes, Some(split))
    }

    /// `bytes` must be valid UTF-8 without the sentinel; `split` the offset of its first `=`.
    fn from_parts(mut bytes: Vec<u8>, split: Option<usize>) -> Self {
        debug_assert_eq!(split, bytes.iter().position(|&b| b == b'='));
        bytes.push(SENTINEL);
        let split = split
            .and_then(|i| u16::try_from(i).ok())
            .unwrap_or(NO_SPLIT);
        IndexKey {
            bytes: bytes.into_boxed_slice(),
            split,
        }
    }

    pub fn as_str(&self) -> &str {
        self.sub_string(0)
    }

    pub fn split(&self) -> Option<(&str, &str)> {
        let key = self.as_str();
        let index = match self.split {
            NO_SPLIT => key.find('=')?,
            index => index as usize,
        };
        debug_assert_eq!(key.as_bytes()[index], b'=');
        // SAFETY: `index` is the offset of an ASCII `=` in `key`, so both sides are char boundaries.
        unsafe { Some((key.get_unchecked(..index), key.get_unchecked(index + 1..))) }
    }

    pub(crate) fn sub_string(&self, start: usize) -> &str {
        let buf = &self.bytes[start..self.bytes.len() - 1];
        // SAFETY: We always ensure that the inner bytes are valid UTF-8 when constructing an IndexKey.
        debug_assert!(
            std::str::from_utf8(buf).is_ok(),
            "IndexKey::sub_string: buffer is not valid UTF-8"
        );
        unsafe { std::str::from_utf8_unchecked(buf) }
    }

    pub fn len(&self) -> usize {
        self.bytes.len() - 1
    }

    pub fn is_empty(&self) -> bool {
        // The inner buffer is always NUL-terminated, so an "empty" key is `[0]`.
        self.bytes.len() <= 1
    }
}

impl Display for IndexKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl Deref for IndexKey {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.bytes
    }
}

impl AsBytes for IndexKey {
    fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

impl AsRef<[u8]> for IndexKey {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl From<&[u8]> for IndexKey {
    fn from(key: &[u8]) -> Self {
        Self::from(String::from_utf8_lossy(key).as_ref())
    }
}

impl From<Vec<u8>> for IndexKey {
    fn from(key: Vec<u8>) -> Self {
        Self::from(key.as_bytes())
    }
}

impl From<&str> for IndexKey {
    fn from(key: &str) -> Self {
        Self::from_parts(key.as_bytes().to_vec(), key.find('='))
    }
}

impl From<String> for IndexKey {
    fn from(key: String) -> Self {
        let split = key.find('=');
        Self::from_parts(key.into_bytes(), split)
    }
}

impl Borrow<[u8]> for IndexKey {
    fn borrow(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl Hash for IndexKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.bytes.hash(state);
    }
}

unsafe impl NoPrefixesBytes for IndexKey {}

#[cfg(test)]
mod tests {
    use super::*;
    use blart::TreeMap;

    #[test]
    fn test_new() {
        let key = IndexKey::from("test_key");
        assert_eq!(key.as_str(), "test_key");
    }

    #[test]
    fn test_for_label_value() {
        let key = IndexKey::for_label_value("label", "value");
        assert_eq!(key.as_str(), "label=value");
    }

    #[test]
    fn test_as_str() {
        let key = IndexKey::from("test_key");
        assert_eq!(key.as_str(), "test_key");
    }

    #[test]
    fn test_split() {
        let key = IndexKey::from("label=value");
        let (label, value) = key.split().unwrap();
        assert_eq!(label, "label");
        assert_eq!(value, "value");
    }

    #[test]
    fn test_split_agrees_across_constructors() {
        // The cached offset is the first `=`, whichever constructor built the key, so a key
        // reloaded from bytes equals (and splits like) the one built from its name and value.
        for (name, value) in [
            ("job", "api"),
            ("a=b", "c"),
            ("eq", "x=y"),
            ("", "v"),
            ("n", ""),
        ] {
            let built = IndexKey::for_label_value(name, value);
            let text = format!("{name}={value}");
            let expected = text.split_once('=');
            for key in [
                IndexKey::from(text.as_str()),
                IndexKey::from(text.clone()),
                IndexKey::from(text.as_bytes()),
            ] {
                assert_eq!(key, built);
                assert_eq!(key.split(), expected);
            }
            assert_eq!(built.split(), expected);
        }
    }

    #[test]
    fn test_split_without_separator() {
        assert_eq!(IndexKey::from("bare").split(), None);
        assert_eq!(IndexKey::from("").split(), None);
    }

    #[test]
    fn test_split_past_u16_offset_falls_back_to_search() {
        let name = "n".repeat(u16::MAX as usize + 10);
        let key = IndexKey::for_label_value(&name, "v");
        assert_eq!(key.split, NO_SPLIT);
        assert_eq!(key.split(), Some((name.as_str(), "v")));
        assert_eq!(IndexKey::from(key.as_str()), key);

        let name = "n".repeat(u16::MAX as usize - 1);
        let key = IndexKey::for_label_value(&name, "v");
        assert_eq!(key.split as usize, name.len());
        assert_eq!(key.split(), Some((name.as_str(), "v")));
    }

    #[test]
    fn test_sub_string() {
        let key = IndexKey::from("label=value");
        assert_eq!(key.sub_string(6), "value");
    }

    #[test]
    fn test_len() {
        let key = IndexKey::from("test_key");
        assert_eq!(key.len(), 8);
    }

    #[test]
    fn test_display() {
        let key = IndexKey::from("test_key");
        assert_eq!(format!("{key}"), "test_key");
    }

    #[test]
    fn test_as_bytes() {
        let key = IndexKey::from("test_key");
        assert_eq!(key.as_bytes(), b"test_key\0");
    }

    #[test]
    fn test_from_u8_slice() {
        let key = IndexKey::from(b"test_key".as_ref());
        assert_eq!(key.as_str(), "test_key");
    }

    #[test]
    fn test_from_vec_u8() {
        let key = IndexKey::from(b"test_key".to_vec());
        assert_eq!(key.as_str(), "test_key");
    }

    #[test]
    fn test_from_str() {
        let key = IndexKey::from("test_key");
        assert_eq!(key.as_str(), "test_key");
    }

    #[test]
    fn test_borrow() {
        let key = IndexKey::from("test_key");
        let borrowed: &[u8] = key.borrow();
        assert_eq!(borrowed, b"test_key\0");
    }

    #[test]
    fn test_with_collection() {
        let mut tree: TreeMap<IndexKey, String> = TreeMap::new();

        let regions = ["US", "EU", "APAC"];
        let services = ["web", "api", "db"];
        let environments = ["prod", "staging", "dev"];

        for region in regions.iter() {
            let region_key = IndexKey::for_label_value("region", region);
            let _ = tree.try_insert(region_key, region.to_string()).unwrap();
        }

        for service in services.iter() {
            let key = IndexKey::for_label_value("service", service);
            let _ = tree.try_insert(key, service.to_string()).unwrap();
        }

        for environment in environments.iter() {
            let key = IndexKey::for_label_value("environment", environment);
            let _ = tree.try_insert(key, environment.to_string()).unwrap();
        }

        for region in regions.iter() {
            let search_key = IndexKey::for_label_value("region", region);
            let value = tree.get(search_key.as_bytes()).map(|v| v.as_str());
            assert_eq!(value, Some(*region));
        }

        for service in services.iter() {
            let search_key = IndexKey::for_label_value("service", service);
            let value = tree.get(search_key.as_bytes()).map(|v| v.as_str());
            assert_eq!(value, Some(*service));
        }

        for environment in environments.iter() {
            let search_key = IndexKey::for_label_value("environment", environment);
            let value = tree.get(search_key.as_bytes()).map(|v| v.as_str());
            assert_eq!(value, Some(*environment));
        }
    }
}
