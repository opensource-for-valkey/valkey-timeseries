use crate::labels::Label;
use crate::promql::Labels;
use crate::promql::hashers::HasFingerprint;
use std::sync::Arc;

/// Cheap-to-clone label container for evaluator internals.
///
/// Storage provides labels as `Arc<[Label]>` (sorted). The `Shared` variant
/// wraps that Arc directly — cloning is an atomic refcount bump. Mutation
/// (remove/insert/retain) promotes to `Owned`, which copies the vec once.
#[derive(Debug, Clone)]
pub(crate) enum EvalLabels {
    /// Shared immutable labels from storage. Clone = O(1) refcount bump.
    Shared(Arc<[Label]>),
    /// Owned mutable sorted labels, materialized on the first mutation.
    Owned(Vec<Label>),
}

impl EvalLabels {
    /// Binary search on the sorted label slice.
    pub(crate) fn get(&self, key: &str) -> Option<&str> {
        let slice = self.as_slice();
        slice
            .binary_search_by(|l| l.name.as_str().cmp(key))
            .ok()
            .map(|i| slice[i].value.as_str())
    }

    /// Remove a label by name. Promotes Shared→Owned if needed.
    pub(crate) fn remove(&mut self, key: &str) {
        self.make_owned();
        if let EvalLabels::Owned(vec) = self
            && let Ok(i) = vec.binary_search_by(|l| l.name.as_str().cmp(key))
        {
            vec.remove(i);
        }
    }

    /// Insert or update a label. Maintains sort order. Promotes Shared→Owned.
    pub(crate) fn insert(&mut self, key: String, value: String) {
        self.make_owned();
        if let EvalLabels::Owned(vec) = self {
            match vec.binary_search_by(|l| l.name.as_str().cmp(key.as_str())) {
                Ok(i) => vec[i].value = value,
                Err(i) => vec.insert(i, Label { name: key, value }),
            }
        }
    }

    /// Retain only labels matching the predicate. Promotes Shared→Owned.
    pub(crate) fn retain(&mut self, f: impl FnMut(&Label) -> bool) {
        self.make_owned();
        if let EvalLabels::Owned(vec) = self {
            vec.retain(f);
        }
    }

    /// Returns true if there are no labels.
    pub(crate) fn is_empty(&self) -> bool {
        self.as_slice().is_empty()
    }

    /// Iterate over labels (sorted order in both variants).
    pub(crate) fn iter(&self) -> impl Iterator<Item = &Label> {
        self.as_slice().iter()
    }

    /// Convert into `Labels` for the output boundary. Both variants are
    /// already sorted, so `Labels::new()` does no extra work.
    pub(crate) fn into_labels(self) -> Labels {
        match self {
            EvalLabels::Shared(arc) => Labels::new(arc.to_vec()),
            EvalLabels::Owned(vec) => Labels::new(vec),
        }
    }

    /// Construct from pairs (for tests). Sorts on construction.
    #[cfg(test)]
    pub(crate) fn from_pairs(pairs: &[(&str, &str)]) -> Self {
        let mut vec: Vec<Label> = pairs
            .iter()
            .map(|(k, v)| Label {
                name: k.to_string(),
                value: v.to_string(),
            })
            .collect();
        vec.sort();
        EvalLabels::Owned(vec)
    }

    fn as_slice(&self) -> &[Label] {
        match self {
            EvalLabels::Shared(arc) => arc,
            EvalLabels::Owned(vec) => vec,
        }
    }

    fn make_owned(&mut self) {
        if let EvalLabels::Shared(arc) = self {
            *self = EvalLabels::Owned(arc.to_vec());
        }
    }

    pub fn signature(&self) -> u128 {
        match self {
            EvalLabels::Shared(arc) => arc.fingerprint(),
            EvalLabels::Owned(vec) => vec.fingerprint(),
        }
    }
}

impl PartialEq for EvalLabels {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}
