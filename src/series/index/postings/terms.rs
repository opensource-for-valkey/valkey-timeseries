//! Term-dictionary lookups: resolving label keys and prefixes to posting bitmaps.
//!
//! These are the primitives the query layers are built from. Everything here reads `label_index`
//! and therefore masks stale ids before returning; see [`super::stale`].

use super::{EMPTY_BITMAP, Postings, PostingsBitmap};
use crate::labels::SeriesLabel;
use crate::series::SeriesRef;
use crate::series::index::key_buffer::KeyBuffer;
use std::borrow::Cow;
use std::collections::BTreeSet;

impl Postings {
    /// Return postings for a key (borrowed if possible), applying stale removal.
    /// If stale_ids is non-empty, this returns Owned.
    fn postings_for_key(&'_ self, key: &[u8]) -> Cow<'_, PostingsBitmap> {
        match self.label_index.get(key) {
            Some(bmp) if self.stale_ids.is_empty() => Cow::Borrowed(bmp),
            Some(bmp) => Cow::Owned(bmp.andnot(&self.stale_ids)),
            None => Cow::Borrowed(&*EMPTY_BITMAP),
        }
    }

    /// Clone postings for a key (or empty), then remove stale IDs in-place.
    fn postings_for_key_owned(&self, key: &[u8]) -> PostingsBitmap {
        let mut out = self.label_index.get(key).cloned().unwrap_or_default();
        self.remove_stale_if_needed(&mut out);
        out
    }

    pub(in crate::series::index) fn postings_for_label_value<'a>(
        &'a self,
        name: &str,
        value: &str,
    ) -> Cow<'a, PostingsBitmap> {
        let key = KeyBuffer::for_label_value(name, value);
        self.postings_for_key(key.as_bytes())
    }

    pub fn get_label_names(&self) -> BTreeSet<String> {
        let mut names: BTreeSet<String> = BTreeSet::new();
        for (key, map) in self.label_index.iter() {
            if map.is_empty() {
                continue;
            }
            if let Some((name, _)) = key.split()
                && !names.contains(name)
            {
                names.insert(name.to_owned());
            }
        }
        names
    }

    pub fn get_label_values(&self, label_name: &str) -> Vec<String> {
        let prefix = KeyBuffer::for_prefix(label_name);
        let mut values = Vec::with_capacity(8);
        for (k, map) in self.label_index.prefix(prefix.as_bytes()) {
            if !map.is_empty()
                && let Some((_key, value)) = k.split()
                && !value.is_empty()
            {
                values.push(value.to_string());
            }
        }
        values
    }

    pub(super) fn postings_for_all_label_values(&self, label_name: &str) -> PostingsBitmap {
        let prefix = KeyBuffer::for_prefix(label_name);
        let mut result = PostingsBitmap::new();
        for (_, map) in self.label_index.prefix(prefix.as_bytes()) {
            result |= map;
        }
        self.remove_stale_if_needed(&mut result);
        result
    }

    /// `postings_for_label_values` returns a `PostingsBitmap` for the label pairs.
    /// The postings here contain the ids to the series inside the index.
    pub(super) fn postings_for_label_values(
        &self,
        name: &str,
        values: &[String],
    ) -> PostingsBitmap {
        if let [value] = values {
            let key = KeyBuffer::for_label_value(name, value);
            return self.postings_for_key_owned(key.as_bytes());
        }

        let mut result = PostingsBitmap::new();

        for value in values {
            let key = KeyBuffer::for_label_value(name, value);
            if let Some(bmp) = self.label_index.get(key.as_bytes()) {
                result |= bmp;
            }
        }

        self.remove_stale_if_needed(&mut result);
        result
    }

    /// `postings_for_label_matching` returns postings having a label with the given name and a value
    /// for which `match_fn` returns true. If no postings are found having at least one matching label,
    /// an empty bitmap is returned.
    pub(super) fn postings_for_label_matching<F, STATE>(
        &self,
        name: &str,
        state: &mut STATE,
        match_fn: F,
    ) -> PostingsBitmap
    where
        F: Fn(&str, &mut STATE) -> bool,
    {
        let prefix = KeyBuffer::for_prefix(name);
        let start_pos = prefix.len();
        let mut result = PostingsBitmap::new();

        for (key, map) in self.label_index.prefix(prefix.as_bytes()) {
            let value = key.sub_string(start_pos);
            if match_fn(value, state) {
                result |= map;
            }
        }

        self.remove_stale_if_needed(&mut result);
        result
    }

    /// Return all series ids corresponding to the given labels
    pub fn postings_by_labels<T: SeriesLabel>(&self, labels: &[T]) -> PostingsBitmap {
        let mut first = true;
        let mut acc = PostingsBitmap::new();

        for label in labels.iter() {
            let key = KeyBuffer::for_label_value(label.name(), label.value());
            if let Some(bmp) = self.label_index.get(key.as_bytes()) {
                if bmp.is_empty() {
                    break;
                }
                if first {
                    acc |= bmp;
                    first = false;
                } else {
                    acc &= bmp;
                }
            }
        }
        self.remove_stale_if_needed(&mut acc);

        acc
    }

    /// Retrieves a `PostingsBitmap` containing postings that match the specified label and prefix.
    ///
    /// This function searches for postings in the internal `label_index` where the keys start with
    /// a combination of the provided `label` and `prefix`. For each match, the function accumulates
    /// the corresponding postings into a `PostingsBitmap`. After the accumulation, it ensures
    /// that stale entries (if any) are removed from the resulting bitmap.
    ///
    /// # Parameters
    /// - `label`: A string slice representing the label to search for in the index.
    /// - `prefix`: A string slice representing the prefix to match against the label values in the index.
    ///
    /// # Returns
    /// A `PostingsBitmap` that contains the union of all postings that match the given label and prefix.
    pub(super) fn postings_by_prefix(&self, label: &str, prefix: &str) -> PostingsBitmap {
        let search_prefix = KeyBuffer::for_label_value_prefix(label, prefix);

        let mut result = PostingsBitmap::new();
        for (_key, map) in self.label_index.prefix(&search_prefix) {
            result |= map;
        }
        self.remove_stale_if_needed(&mut result);
        result
    }

    pub(super) fn postings_by_prefix_and_predicate<F>(
        &self,
        label: &str,
        prefix: &str,
        predicate: F,
    ) -> PostingsBitmap
    where
        F: Fn(&str) -> bool,
    {
        let search_prefix = KeyBuffer::for_label_value_prefix(label, prefix);

        let mut result = PostingsBitmap::new();
        let start_pos = label.len() + 1;
        for (key, map) in self.label_index.prefix(&search_prefix) {
            let value = key.sub_string(start_pos);
            if predicate(value) {
                result |= map;
            }
        }
        self.remove_stale_if_needed(&mut result);

        result
    }

    /// Get the unique series id for the given set of labels if it exists.
    ///
    /// This exists primarily to ensure that we disallow duplicate metric names
    pub fn posting_id_by_labels<T: SeriesLabel>(&self, labels: &[T]) -> Option<SeriesRef> {
        let mut it = labels.iter();

        let first = it.next()?;
        let first_key = KeyBuffer::for_label_value(first.name(), first.value());
        let mut acc = self.label_index.get(first_key.as_bytes())?.clone();

        for label in it {
            let key = KeyBuffer::for_label_value(label.name(), label.value());
            let bmp = self.label_index.get(key.as_bytes())?;
            acc.and_inplace(bmp);
            if acc.is_empty() {
                return None;
            }
        }

        self.remove_stale_if_needed(&mut acc);

        (acc.cardinality() == 1)
            .then(|| acc.iter().next())
            .flatten()
    }

    pub(super) fn postings_without_label(&'_ self, label: &str) -> Cow<'_, PostingsBitmap> {
        let to_remove = self.postings_for_all_label_values(label);
        if to_remove.is_empty() {
            Cow::Borrowed(&self.all_postings)
        } else {
            Cow::Owned(self.all_postings.andnot(&to_remove))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::labels::Label;

    #[test]
    fn test_postings_multiple_values_same_label() {
        let mut postings = Postings::default();

        // Add postings for multiple values of the same label
        postings.add_posting_for_label_value(1, "label1", "value1");
        postings.add_posting_for_label_value(2, "label1", "value2");
        postings.add_posting_for_label_value(3, "label1", "value3");
        postings.add_posting_for_label_value(4, "label1", "value1");

        // Query for multiple values of the same label
        let values = vec!["value1".to_string(), "value3".to_string()];
        let result = postings.postings_for_label_values("label1", &values);

        // Check that the result contains the correct series IDs
        assert_eq!(result.cardinality(), 3);
        assert!(result.contains(1));
        assert!(result.contains(3));
        assert!(result.contains(4));
        assert!(!result.contains(2));
    }

    #[test]
    fn test_postings_with_duplicate_values() {
        let mut postings = Postings::default();

        // Add some postings
        postings.add_posting_for_label_value(1, "label", "value1");
        postings.add_posting_for_label_value(2, "label", "value2");
        postings.add_posting_for_label_value(3, "label", "value1");

        // Create an array with duplicate values
        let values = vec![
            "value1".to_string(),
            "value2".to_string(),
            "value1".to_string(),
        ];

        // Call the postings method
        let result = postings.postings_for_label_values("label", &values);

        // Check the result
        assert_eq!(result.cardinality(), 3);
        assert!(result.contains(1));
        assert!(result.contains(2));
        assert!(result.contains(3));
    }

    #[test]
    fn test_postings_all_values_match() {
        let mut postings = Postings::default();

        // Add some postings
        postings.add_posting_for_label_value(1, "label", "value1");
        postings.add_posting_for_label_value(2, "label", "value2");
        postings.add_posting_for_label_value(3, "label", "value3");
        postings.add_posting_for_label_value(4, "label", "value1");

        // Create values to search for
        let values = vec![
            "value1".to_string(),
            "value2".to_string(),
            "value3".to_string(),
        ];

        // Get the postings
        let result = postings.postings_for_label_values("label", &values);

        // Check if the result contains all the expected series IDs
        assert_eq!(result.cardinality(), 4);
        assert!(result.contains(1));
        assert!(result.contains(2));
        assert!(result.contains(3));
        assert!(result.contains(4));
    }

    #[test]
    fn test_postings_with_large_number_of_values() {
        let mut postings = Postings::default();
        let label_name = "large_label";
        let num_values = 10_000;

        // Add postings for a large number of values
        for i in 0..num_values {
            postings.add_posting_for_label_value(i as SeriesRef, label_name, &format!("value_{i}"));
        }

        // Create a large array of values to search for
        let values: Vec<String> = (0..num_values).map(|i| format!("value_{i}")).collect();

        // Measure the time taken to execute the postings function
        let start_time = std::time::Instant::now();
        let result = postings.postings_for_label_values(label_name, &values);
        let duration = start_time.elapsed();

        // Assert that all series IDs are present in the result
        assert_eq!(result.cardinality() as usize, num_values);
        for i in 0..num_values {
            assert!(result.contains(i as SeriesRef));
        }

        // Check that the execution time is reasonable (adjust the threshold as needed)
        assert!(
            duration < std::time::Duration::from_secs(1),
            "Postings retrieval took too long: {duration:?}"
        );
    }

    #[test]
    fn test_postings_with_unicode_characters() {
        let mut postings = Postings::default();

        // Add postings with Unicode characters
        postings.add_posting_for_label_value(1, "标签", "值1");
        postings.add_posting_for_label_value(2, "标签", "値2");
        postings.add_posting_for_label_value(3, "标签", "🌟");

        // Test postings method with Unicode characters
        let values = vec!["值1".to_string(), "値2".to_string(), "🌟".to_string()];
        let result = postings.postings_for_label_values("标签", &values);

        assert_eq!(result.cardinality(), 3);
        assert!(result.contains(1));
        assert!(result.contains(2));
        assert!(result.contains(3));
    }

    #[test]
    fn test_postings_for_label_value_exceeds_stack_size() {
        let mut postings = Postings::default();

        // The STACK_SIZE in KeyBuffer is 64 bytes
        // We need label_name + "=" + value + "\0" to exceed 64 bytes
        // Let's create a label name and value that together exceed this

        // Create a label name of 30 characters
        let label_name = "very_long_label_name_here_1234";

        // Create a value of 40 characters, so the total length is:
        // 30 (label) + 1 (=) + 40 (value) + 1 (\0) = 72 bytes > 64
        let value = "this_is_a_very_long_value_string_12345";

        // Verify our assumption about the length
        let total_len = label_name.len() + 1 + value.len() + 1; // +1 for '=', +1 for '\0'
        assert!(
            total_len > 64,
            "Test setup error: combined length should exceed STACK_SIZE"
        );

        // Add a posting with this long label-value pair
        postings.add_posting_for_label_value(1, label_name, value);
        postings.add_posting_for_label_value(2, label_name, value);
        postings.add_posting_for_label_value(3, label_name, "short");

        // Test that we can retrieve postings for the long label-value pair
        let result = postings.postings_for_label_value(label_name, value);

        // Verify the result contains the correct series IDs
        assert_eq!(result.cardinality(), 2);
        assert!(result.contains(1));
        assert!(result.contains(2));
        assert!(!result.contains(3));

        // Test that we can also retrieve the short value
        let result_short = postings.postings_for_label_value(label_name, "short");
        assert_eq!(result_short.cardinality(), 1);
        assert!(result_short.contains(3));
    }

    #[test]
    fn test_postings_for_all_label_values_exceeds_stack_size() {
        let mut postings = Postings::default();

        // The STACK_SIZE in KeyBuffer is 64 bytes
        // KeyBuffer::for_prefix creates: label_name + "="
        // We need label_name + "=" to exceed 64 bytes

        // Create a label name of 70 characters to exceed the stack size
        let label_name = "very_long_label_name_here_that_definitely_exceeds_the_stack_buffer_size";

        // Verify our assumption about the length
        let prefix_len = label_name.len() + 1; // +1 for '='
        assert!(
            prefix_len > 64,
            "Test setup error: prefix length should exceed STACK_SIZE"
        );

        // Add multiple postings with different values for the same long label name
        postings.add_posting_for_label_value(1, label_name, "value1");
        postings.add_posting_for_label_value(2, label_name, "value2");
        postings.add_posting_for_label_value(3, label_name, "value3");
        postings.add_posting_for_label_value(4, "short_label", "value1");

        // Test that we can retrieve all postings for the long label name
        let result = postings.postings_for_all_label_values(label_name);

        // Verify the result contains all series IDs with the long label name
        assert_eq!(result.cardinality(), 3);
        assert!(result.contains(1));
        assert!(result.contains(2));
        assert!(result.contains(3));
        assert!(!result.contains(4)); // This has a different label

        // Also test that we can retrieve postings for the short label
        let result_short = postings.postings_for_all_label_values("short_label");
        assert_eq!(result_short.cardinality(), 1);
        assert!(result_short.contains(4));
    }

    #[test]
    fn test_memory_postings_postings_by_labels() {
        let mut postings = Postings::default();

        postings.add_posting_for_label_value(1, "label1", "value1");
        postings.add_posting_for_label_value(1, "label2", "value2");
        postings.add_posting_for_label_value(2, "label1", "value1");
        postings.add_posting_for_label_value(2, "label2", "value3");

        let labels = vec![
            Label::new("label1", "value1"),
            Label::new("label2", "value2"),
        ];
        let result = postings.postings_by_labels(&labels);

        assert_eq!(result.cardinality(), 1);
        assert!(result.contains(1));
    }
}
