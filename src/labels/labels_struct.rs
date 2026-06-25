use super::hash::HasFingerprint;
use crate::common::constants::METRIC_NAME_LABEL;
use crate::labels::{Label, MetricName, SeriesFingerprint};
use ahash::{AHashMap, AHashSet};
use enquote::enquote;
use promql_parser::parser::LabelModifier;
use serde::de::{MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;

/// An ordered set of labels identifying a series.
///
/// `Labels` wraps a sorted `Vec<Label>` and provides convenience accessors
/// for looking up label values and the metric name. This is the type returned
/// by read/query APIs to identify each result series.
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct Labels(pub(crate) Vec<Label>);

impl Labels {
    pub fn with_capacity(capacity: usize) -> Self {
        Labels(Vec::with_capacity(capacity))
    }

    pub fn push(&mut self, label: Label) {
        self.0.push(label);
    }

    /// Creates an empty `Labels`.
    pub fn empty() -> Self {
        Self(Vec::new())
    }

    /// Creates a new `Labels` from a vec of labels.
    pub fn new(labels: Vec<Label>) -> Self {
        let mut labels = labels;
        labels.sort_unstable();
        Self(labels)
    }

    /// Returns the number of labels.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if there are no labels.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the value of the label with the given name, if present.
    // TODO: labels are sorted, could use binary_search_by for O(log n)
    pub fn get(&self, name: &str) -> Option<&str> {
        self.0
            .iter()
            .find(|l| l.name == name)
            .map(|l| l.value.as_str())
    }

    pub fn set(&mut self, key: &str, value: String) {
        match self.0.binary_search_by(|l| l.name.as_str().cmp(key)) {
            Ok(i) => self.0[i].value = value,
            Err(i) => self.0.insert(
                i,
                Label {
                    name: key.to_string(),
                    value,
                },
            ),
        }
    }

    pub fn get_label_mut(&mut self, name: &str) -> Option<&mut Label> {
        if let Some(idx) = self.0.iter().position(|l| l.name == name) {
            self.0.get_mut(idx)
        } else {
            None
        }
    }

    pub fn insert(&mut self, key: &str, value: String) -> bool {
        match self.0.binary_search_by(|l| l.name.as_str().cmp(key)) {
            Ok(i) => {
                self.0[i].value = value;
                false
            }
            Err(i) => {
                self.0.insert(
                    i,
                    Label {
                        name: key.to_string(),
                        value,
                    },
                );
                true
            }
        }
    }

    pub fn remove(&mut self, key: &str) -> Option<Label> {
        self.0
            .iter()
            .position(|l| l.name == key)
            .map(|i| self.0.remove(i))
    }

    pub fn contains(&self, key: &str) -> bool {
        self.0
            .binary_search_by(|l| l.name.as_str().cmp(key))
            .is_ok()
    }

    /// Returns the metric name (value of the `__name__` label).
    ///
    /// Returns `""` if no `__name__` label is present.
    pub fn metric_name(&self) -> &str {
        self.get(METRIC_NAME_LABEL).unwrap_or("")
    }

    /// Iterates over the labels.
    pub fn iter(&self) -> impl Iterator<Item = &Label> {
        self.0.iter()
    }

    pub fn sort(&mut self) {
        self.0.sort();
    }

    pub fn signature(&self) -> u128 {
        self.0.fingerprint()
    }

    pub(crate) fn into_inner(self) -> Vec<Label> {
        self.0
    }

    pub fn retain<F>(&mut self, f: F)
    where
        F: Fn(&Label) -> bool,
    {
        self.0.retain(f)
    }

    pub fn retain_tags(&mut self, tags: &[String]) {
        if tags.is_empty() {
            self.0.clear();
            return;
        }
        if tags.len() >= 32 {
            // todo: make named constant
            let set: AHashSet<_> = AHashSet::from_iter(tags);
            self.0.retain(|tag| set.contains(&tag.name));
        } else {
            self.0.retain(|tag| tags.contains(&tag.name));
        }
    }

    pub fn remove_label(&mut self, name: &str) -> Option<Label> {
        self.0
            .iter()
            .position(|label| label.name == name)
            .map(|pos| self.0.remove(pos))
    }

    pub fn reset_metric_group(&mut self) {
        self.0.retain(|label| label.name != METRIC_NAME_LABEL);
    }

    pub(crate) fn into_grouping_labels(self, modifier: Option<&LabelModifier>) -> Self {
        let mut this = self;
        match modifier {
            None => Self(vec![]), // No grouping, return empty labels
            Some(LabelModifier::Include(label_list)) => {
                // Keep only specified labels
                this.retain(|k| label_list.labels.contains(&k.name));
                this
            }
            Some(LabelModifier::Exclude(label_list)) => {
                // Remove specified labels
                this.retain(|k| !label_list.labels.contains(&k.name));
                this
            }
        }
    }

    pub(crate) fn compute_grouping_labels(&self, modifier: Option<&LabelModifier>) -> Self {
        let this = self.clone();
        this.into_grouping_labels(modifier)
    }

    pub fn get_fingerprint(&self) -> SeriesFingerprint {
        self.0.fingerprint()
    }

    /// Construct from pairs (for tests). Sorts on construction.
    pub fn from_pairs(pairs: &[(&str, &str)]) -> Self {
        let mut vec: Vec<Label> = pairs
            .iter()
            .map(|(k, v)| Label {
                name: k.to_string(),
                value: v.to_string(),
            })
            .collect();
        vec.sort();
        Labels::new(vec)
    }
}

impl Display for Labels {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{{", self.metric_name())?;

        let mut first = true;
        for label in self
            .0
            .iter()
            .filter(|l| !l.name.is_empty() && l.name != METRIC_NAME_LABEL)
        {
            if !first {
                write!(f, ",")?;
            }
            first = false;
            write!(f, "{}={}", label.name, enquote('"', &label.value))?;
        }

        write!(f, "}}")?;
        Ok(())
    }
}

impl Ord for Labels {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

impl PartialOrd for Labels {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl AsRef<[Label]> for Labels {
    fn as_ref(&self) -> &[Label] {
        &self.0
    }
}

impl Serialize for Labels {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for label in &self.0 {
            map.serialize_entry(&label.name, &label.value)?;
        }
        map.end()
    }
}

impl From<AHashMap<String, String>> for Labels {
    fn from(map: AHashMap<String, String>) -> Self {
        let mut labels: Vec<Label> = map
            .into_iter()
            .map(|(name, value)| Label { name, value })
            .collect();
        labels.sort();
        Self(labels)
    }
}

impl From<Labels> for AHashMap<String, String> {
    fn from(labels: Labels) -> Self {
        labels.0.into_iter().map(|l| (l.name, l.value)).collect()
    }
}

impl From<&MetricName> for Labels {
    fn from(metric_name: &MetricName) -> Self {
        let mut labels = Vec::with_capacity(metric_name.len());
        for label in metric_name.iter() {
            labels.push(Label::new(label.name.to_string(), label.value.to_string()));
        }
        Self::new(labels)
    }
}

impl From<&Labels> for MetricName {
    fn from(labels: &Labels) -> Self {
        MetricName::from(labels.as_ref())
    }
}

impl<'de> Deserialize<'de> for Labels {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct LabelsVisitor;

        impl<'de> Visitor<'de> for LabelsVisitor {
            type Value = Labels;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a map of label name to label value")
            }

            fn visit_map<M: MapAccess<'de>>(self, mut access: M) -> Result<Labels, M::Error> {
                let mut labels = Vec::with_capacity(access.size_hint().unwrap_or(0));
                while let Some((name, value)) = access.next_entry::<String, String>()? {
                    labels.push(Label { name, value });
                }
                labels.sort();
                Ok(Labels(labels))
            }
        }

        deserializer.deserialize_map(LabelsVisitor)
    }
}

impl From<Labels> for HashMap<String, String> {
    fn from(labels: Labels) -> Self {
        labels.0.into_iter().map(|l| (l.name, l.value)).collect()
    }
}

impl From<HashMap<String, String>> for Labels {
    fn from(map: HashMap<String, String>) -> Self {
        let mut labels: Vec<Label> = map
            .into_iter()
            .map(|(name, value)| Label { name, value })
            .collect();
        labels.sort();
        Self(labels)
    }
}

impl From<&HashMap<String, String>> for Labels {
    fn from(map: &HashMap<String, String>) -> Self {
        let mut labels: Vec<Label> = map
            .iter()
            .map(|(name, value)| Label {
                name: name.clone(),
                value: value.clone(),
            })
            .collect();
        labels.sort();
        Self(labels)
    }
}

impl From<Vec<Label>> for Labels {
    fn from(labels: Vec<Label>) -> Self {
        let mut sorted_labels = labels;
        sorted_labels.sort();
        Self(sorted_labels)
    }
}

impl HasFingerprint for Labels {
    fn fingerprint(&self) -> SeriesFingerprint {
        self.0.fingerprint()
    }
}
