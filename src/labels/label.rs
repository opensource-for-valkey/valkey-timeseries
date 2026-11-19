use crate::error_consts;
use crate::labels::InternedLabel;
use std::cmp::Ordering;
use std::fmt::Display;
use std::hash::{Hash, Hasher};
use valkey_module::{ValkeyError, ValkeyResult, ValkeyValue};

pub trait SeriesLabel: Sized {
    fn name(&self) -> &str;
    fn value(&self) -> &str;
}

/// So a slice of labels can be passed where an iterator of labels is taken.
impl<T: SeriesLabel> SeriesLabel for &T {
    fn name(&self) -> &str {
        (**self).name()
    }

    fn value(&self) -> &str {
        (**self).value()
    }
}

/// A label value as replies carry it: nil for an empty value, which is how a reply says a
/// selected label is absent from a series (`SELECTED_LABELS`, `TS.INFO`). Stored values are
/// never empty, since both write paths reject them.
pub fn label_value_reply(value: &str) -> ValkeyValue {
    if value.is_empty() {
        ValkeyValue::Null
    } else {
        ValkeyValue::from(value)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Label {
    pub name: String,
    pub value: String,
}

impl Label {
    pub fn new<S: Into<String>>(key: S, value: S) -> Self {
        Self {
            name: key.into(),
            value: value.into(),
        }
    }
}

impl PartialOrd for Label {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Label {
    fn cmp(&self, other: &Self) -> Ordering {
        let cmp = self.name.cmp(&other.name);
        if cmp != Ordering::Equal {
            cmp
        } else {
            self.value.cmp(&other.value)
        }
    }
}

impl Display for Label {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{name}={value}", name = self.name, value = self.value)
    }
}

impl From<InternedLabel<'_>> for Label {
    fn from(label: InternedLabel) -> Self {
        Self {
            name: label.name.to_string(),
            value: label.value.to_string(),
        }
    }
}

impl From<Label> for ValkeyValue {
    fn from(label: Label) -> Self {
        let label_value = label_value_reply(&label.value);
        ValkeyValue::from(vec![ValkeyValue::from(label.name), label_value])
    }
}

const SEP: u8 = 0xfe;

impl Hash for Label {
    fn hash<H: Hasher>(&self, state: &mut H) {
        hash_label(state, &self.name, &self.value);
    }
}

impl SeriesLabel for Label {
    fn name(&self) -> &str {
        &self.name
    }

    fn value(&self) -> &str {
        &self.value
    }
}

fn hash_label<H: Hasher>(state: &mut H, name: &str, value: &str) {
    state.write(name.as_bytes());
    state.write_u8(SEP);
    state.write(value.as_bytes());
}

/// Rejects a label name containing `=` (DIV-0063). No filter can select such a label (`a=b=2`
/// is a parse error here and matches nothing on RTS), and the label index keys `name=value`
/// split at the first `=`, so it would file `a=b 2` under the name `a`. Values may contain `=`.
///
/// Every way a label enters the keyspace checks this: the `LABELS` parser and RDB load
/// (`DUMP`/`RESTORE`, `TS._RESTORE`, replica sync), so no stored series has such a name.
pub fn check_label_name(name: &[u8]) -> ValkeyResult<()> {
    if name.contains(&b'=') {
        return Err(ValkeyError::Str(
            error_consts::LABEL_NAME_CONTAINS_SEPARATOR,
        ));
    }
    Ok(())
}

/// Rejects a label whose name or value contains a NUL byte (DIV-0068).
///
/// The label index stores `name=value` with a NUL terminator so that no key is a prefix of
/// another, which its radix tree requires: a value `b` beside `b\0c` gave `a=b\0` and
/// `a=b\0c\0`, and the second insert reached undefined behavior. A name cannot cause that, but
/// RedisTimeSeries indexes a name cut at its first NUL (`a=v` matches a series labelled `a\0b`)
/// and fails to parse a filter that names one, so names are refused too: no label holds a NUL,
/// and the filter parser rejects a NUL anywhere in a matcher (`parse_series_selector`).
///
/// The write path checks this wherever `LABELS` or `METRIC` is parsed. A foreign load
/// (`RESTORE`, a slot import) refuses a payload carrying one; an own load tolerates it
/// (`normalize_series`, and an own AOF's replayed commands), and the label index leaves it out.
pub fn check_label_nul(name: &[u8], value: &[u8]) -> ValkeyResult<()> {
    if name.contains(&0) {
        return Err(ValkeyError::Str(error_consts::LABEL_NAME_CONTAINS_NUL));
    }
    if value.contains(&0) {
        return Err(ValkeyError::Str(error_consts::LABEL_VALUE_CONTAINS_NUL));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn label_names_must_not_contain_the_separator() {
        for name in ["a=b", "=", "=a", "a=", "a==b"] {
            assert!(
                matches!(
                    check_label_name(name.as_bytes()),
                    Err(ValkeyError::Str(
                        error_consts::LABEL_NAME_CONTAINS_SEPARATOR
                    ))
                ),
                "{name:?} was accepted"
            );
        }
        for name in ["a", "host", "__name__", "a:b.c-d", "région"] {
            assert!(
                check_label_name(name.as_bytes()).is_ok(),
                "{name:?} was rejected"
            );
        }
    }

    #[test]
    fn label_names_and_values_must_not_contain_nul() {
        for bad in ["\0", "b\0c", "b\0", "\0b"] {
            assert!(
                matches!(
                    check_label_nul(b"a", bad.as_bytes()),
                    Err(ValkeyError::Str(error_consts::LABEL_VALUE_CONTAINS_NUL))
                ),
                "value {bad:?} was accepted"
            );
            assert!(
                matches!(
                    check_label_nul(bad.as_bytes(), b"v"),
                    Err(ValkeyError::Str(error_consts::LABEL_NAME_CONTAINS_NUL))
                ),
                "name {bad:?} was accepted"
            );
        }
        // `=` (in values), other control bytes and non-ASCII text stay legal.
        for good in ["b", "b=c", "\u{1}", "\n", "région", "\u{FFFD}"] {
            assert!(
                check_label_nul(b"a", good.as_bytes()).is_ok(),
                "value {good:?} was rejected"
            );
        }
        for good in ["b", "\u{1}", "\n", "région", "__name__"] {
            assert!(
                check_label_nul(good.as_bytes(), b"v").is_ok(),
                "name {good:?} was rejected"
            );
        }
    }
}
