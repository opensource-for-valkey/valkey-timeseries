use super::label::{Label, SeriesLabel};
use crate::common::constants::METRIC_NAME_LABEL;
use crate::common::rdb::{rdb_load_len, rdb_load_string, rdb_save_usize};
use crate::common::string_interner::InternedString;
use crate::parser::ParseError;
use crate::parser::metric_name::parse_metric_name;
use enquote::enquote;
use get_size2::GetSize;
use std::collections::HashMap;
use std::fmt::Display;
use std::str::FromStr;
use valkey_module::{ValkeyResult, ValkeyValue, raw};

const VALUE_SEPARATOR: &str = "=";
const EMPTY_LABEL: &str = "";

// Real series never come close to this many labels; it only rejects a corrupt/hostile
// count before it reaches `with_capacity`.
pub const MAX_LABELS_PER_SERIES: usize = 128;

pub struct InternedLabel<'a> {
    pub name: &'a str,
    pub value: &'a str,
}

impl From<InternedLabel<'_>> for ValkeyValue {
    fn from(label: InternedLabel) -> Self {
        let row = vec![
            ValkeyValue::from(label.name),
            ValkeyValue::from(label.value),
        ];
        ValkeyValue::from(row)
    }
}

impl SeriesLabel for InternedLabel<'_> {
    fn name(&self) -> &str {
        self.name
    }

    fn value(&self) -> &str {
        self.value
    }
}

/// A time series is optionally identified by a series of label-value pairs used to retrieve the
/// series in queries. Given that these labels are used to group semantically similar time series,
/// they are necessarily duplicated. We take advantage of this fact to intern label-value pairs,
/// meaning that only a single allocation is made per unique pair, irrespective of the number of
/// series it occurs in.
///
/// We choose to store the label/value pair as a single interned string in the format "key=value". This reduces
/// the memory overhead associated with storing separate strings for keys and values (8 bytes vs 16 bytes on 64-bit systems).
///
/// The labels are stored in a sorted order to allow for efficient comparison and retrieval.
#[derive(Debug, Clone, Default, Hash, PartialEq, Eq)]
pub struct MetricName(Vec<InternedString>);

impl GetSize for MetricName {
    fn get_size(&self) -> usize {
        self.0
            .iter()
            .map(|i| {
                // we count the full size (stack and heap) only for unique strings
                if i.is_unique() {
                    i.get_size()
                } else {
                    size_of::<InternedString>()
                }
            })
            .sum()
    }
}

impl MetricName {
    pub fn with_capacity(capacity: usize) -> Self {
        Self(Vec::with_capacity(capacity))
    }

    pub fn new(labels: &[Label]) -> Self {
        let mut metric_name = Self::with_capacity(labels.len());
        for label in labels {
            metric_name.add_label(label.name(), label.value());
        }
        metric_name.shrink_to_fit();
        metric_name
    }

    pub fn clear(&mut self) {
        self.0.clear();
    }

    fn split_kv(tag: &InternedString) -> Option<(&str, &str)> {
        tag.split_once(VALUE_SEPARATOR)
    }

    fn key_of(tag: &InternedString) -> &str {
        Self::split_kv(tag).map(|(k, _)| k).unwrap_or(EMPTY_LABEL)
    }

    fn find_index(&self, key: &str) -> Result<usize, usize> {
        self.0.binary_search_by_key(&key, Self::key_of)
    }

    /// adds a new label to mn with the given key and value.
    pub fn add_label(&mut self, key: &str, value: &str) {
        let full_label = format!("{key}{VALUE_SEPARATOR}{value}");
        let interned_value = InternedString::new(&full_label);

        match self.find_index(key) {
            Ok(idx) => self.0[idx] = interned_value,
            Err(idx) => self.0.insert(idx, interned_value),
        }
    }

    pub fn get_tag(&'_ self, key: &str) -> Option<InternedLabel<'_>> {
        let idx = self.find_index(key).ok()?;
        let (name, value) = Self::split_kv(&self.0[idx])?;
        Some(InternedLabel { name, value })
    }

    pub fn get_value(&self, key: &str) -> Option<&str> {
        let idx = self.find_index(key).ok()?;
        let (_, value) = Self::split_kv(&self.0[idx])?;
        Some(value)
    }

    pub fn remove_label(&mut self, key: &str) {
        if let Ok(idx) = self.find_index(key) {
            self.0.remove(idx);
        }
    }

    pub fn set_label(&mut self, key: &str, value: &str) {
        self.add_label(key, value);
    }

    pub fn get_measurement(&self) -> &str {
        self.get_value(METRIC_NAME_LABEL).unwrap_or(EMPTY_LABEL)
    }

    pub fn iter(&'_ self) -> impl Iterator<Item = InternedLabel<'_>> {
        self.0
            .iter()
            .filter_map(|x| Self::split_kv(x).map(|(name, value)| InternedLabel { name, value }))
    }

    pub fn to_label_vec(&self) -> Vec<Label> {
        self.iter()
            .map(|label| Label::new(label.name, label.value))
            .collect()
    }

    pub fn sort(&mut self) {
        self.0.sort();
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn to_rdb(&self, rdb: *mut raw::RedisModuleIO) {
        rdb_save_usize(rdb, self.0.len());
        for label in self.iter() {
            raw::save_string(rdb, label.name);
            raw::save_string(rdb, label.value);
        }
    }

    pub fn from_rdb(rdb: *mut raw::RedisModuleIO) -> ValkeyResult<Self> {
        let count = rdb_load_len(rdb, MAX_LABELS_PER_SERIES)?;
        let mut result = Self::with_capacity(count);
        for _ in 0..count {
            let name = rdb_load_string(rdb)?;
            let value = rdb_load_string(rdb)?;
            result.add_label(&name, &value);
        }
        Ok(result)
    }

    pub fn shrink_to_fit(&mut self) {
        self.0.shrink_to_fit();
    }
}

impl From<HashMap<String, String>> for MetricName {
    fn from(map: HashMap<String, String>) -> Self {
        let mut metric_name = MetricName::with_capacity(map.len());
        for (key, value) in map {
            metric_name.add_label(&key, &value);
        }
        metric_name
    }
}

impl From<&[Label]> for MetricName {
    fn from(labels: &[Label]) -> Self {
        MetricName::new(labels)
    }
}

impl From<Vec<Label>> for MetricName {
    fn from(labels: Vec<Label>) -> Self {
        MetricName::new(&labels)
    }
}

impl FromStr for MetricName {
    type Err = ParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let labels = parse_metric_name(s)?;
        Ok(MetricName::new(&labels))
    }
}

impl Display for MetricName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{{", self.get_measurement())?;

        let mut first = true;
        for label in self
            .iter()
            .filter(|l| !l.name.is_empty() && l.name != METRIC_NAME_LABEL)
        {
            if !first {
                write!(f, ",")?;
            }
            first = false;
            write!(f, "{}={}", label.name, enquote('"', label.value))?;
        }

        write!(f, "}}")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_label() {
        let mut metric_name = MetricName::with_capacity(2);
        metric_name.add_label("key1", "value1");
        metric_name.add_label("key2", "value2");

        assert_eq!(metric_name.get_value("key1"), Some("value1"));
        assert_eq!(metric_name.get_value("key2"), Some("value2"));
    }

    #[test]
    fn test_get_measurement() {
        let mut metric_name = MetricName::default();
        metric_name.add_label("key1", "value1");

        assert_eq!(metric_name.get_measurement(), "");

        metric_name.add_label(METRIC_NAME_LABEL, "metric_name");
        assert_eq!(metric_name.get_measurement(), "metric_name");
    }

    #[test]
    fn test_add_labels() {
        let mut metric_name = MetricName::with_capacity(2);

        metric_name.add_label("key1", "value1");
        metric_name.add_label("key2", "value2");

        assert_eq!(metric_name.get_value("key1"), Some("value1"));
        assert_eq!(metric_name.get_value("key2"), Some("value2"));
    }

    #[test]
    fn test_sort() {
        let mut metric_name = MetricName::default();
        metric_name.add_label("key2", "value2");
        metric_name.add_label("key1", "value1");
        metric_name.sort();

        let sorted_labels: Vec<_> = metric_name.iter().collect();
        assert_eq!(sorted_labels[0].name, "key1");
        assert_eq!(sorted_labels[1].name, "key2");
    }

    #[test]
    fn test_len_and_is_empty() {
        let mut metric_name = MetricName::default();
        metric_name.add_label("key1", "value1");
        metric_name.add_label("key2", "value2");

        assert_eq!(metric_name.len(), 2);
        assert!(!metric_name.is_empty());
    }

    #[test]
    fn test_display() {
        let mut metric_name = MetricName::with_capacity(3);
        metric_name.add_label(METRIC_NAME_LABEL, "metric");
        metric_name.add_label("key1", "value1");
        metric_name.add_label("key2", "value2");

        let display = format!("{metric_name}");

        assert_eq!(display, "metric{key1=\"value1\",key2=\"value2\"}");
    }
}
