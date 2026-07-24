//! The id directory: the `SeriesRef -> key` forward map.
//!
//! This is the only part of the index that answers "which Valkey key holds series N?", and it is
//! also the authoritative membership set — [`Postings::count`] and [`Postings::has_id`] read it
//! rather than the posting lists, since a series with no labels still has a key.

use super::{KeyType, Postings};
use crate::series::SeriesRef;

impl Postings {
    pub(super) fn set_timeseries_key(&mut self, id: SeriesRef, new_key: &[u8]) {
        if let Some(existing) = self.id_to_key.get(&id)
            && existing.as_ref() == new_key
        {
            return;
        }
        let key = new_key.to_vec().into_boxed_slice();
        self.id_to_key.insert(id, key);
    }

    pub fn count(&self) -> usize {
        self.id_to_key.len()
    }

    pub(in crate::series::index) fn has_id(&self, id: SeriesRef) -> bool {
        self.id_to_key.contains_key(&id)
    }

    pub(crate) fn get_key_by_id(&self, id: SeriesRef) -> Option<&KeyType> {
        self.id_to_key.get(&id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_postings_set_timeseries_key() {
        let mut postings = Postings::default();

        postings.set_timeseries_key(1, b"key1");
        postings.set_timeseries_key(2, b"key2");

        assert_eq!(
            postings.get_key_by_id(1),
            Some(&b"key1".to_vec().into_boxed_slice())
        );
        assert_eq!(
            postings.get_key_by_id(2),
            Some(&b"key2".to_vec().into_boxed_slice())
        );
        assert_eq!(postings.get_key_by_id(3), None);
    }
}
