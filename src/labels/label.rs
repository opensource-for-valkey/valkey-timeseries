use crate::labels::InternedLabel;
use std::cmp::Ordering;
use std::fmt::Display;
use std::hash::{Hash, Hasher};
use valkey_module::ValkeyValue;

pub trait SeriesLabel: Sized {
    fn name(&self) -> &str;
    fn value(&self) -> &str;
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
        let label_value = if label.value.is_empty() {
            ValkeyValue::Null
        } else {
            ValkeyValue::from(label.value)
        };
        let res = vec![ValkeyValue::from(label.name), label_value];
        ValkeyValue::from(res)
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
