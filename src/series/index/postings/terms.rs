//! Term-dictionary lookups: resolving label keys and prefixes to posting bitmaps.
//!
//! [`Terms`] is the read side of the index, and the only thing the query layers above it
//! ([`super::predicate`], [`super::planner`]) are handed. It carries three borrows — the term
//! index, the tombstone set, and the live-id set — which is exactly what reading requires and
//! nothing more: with no access to `id_to_key` and no `&mut`, a query cannot reach the id
//! directory or mutate the index even by accident.
//!
//! Everything here reads `label_index` and therefore masks stale ids before returning; see
//! [`super::stale`]. Results derived solely from [`Terms::all`] are exempt, since that set is
//! stale-free by construction.

use super::{EMPTY_BITMAP, Postings, PostingsBitmap, PostingsIndex, StaleSet};
use crate::labels::SeriesLabel;
use crate::series::SeriesRef;
use crate::series::index::key_buffer::KeyBuffer;
use smallvec::SmallVec;
use std::borrow::Cow;
use std::collections::BTreeSet;

/// A read-only view of the term dictionary. Cheap to copy — it is three references.
#[derive(Clone, Copy)]
pub(super) struct Terms<'a> {
    index: &'a PostingsIndex,
    stale: &'a StaleSet,
    all: &'a PostingsBitmap,
}

impl Postings {
    /// Borrow the read side of the index. This is the entry point for all querying.
    #[inline]
    pub(super) fn terms(&self) -> Terms<'_> {
        Terms {
            index: &self.label_index,
            stale: &self.stale_ids,
            all: &self.all_postings,
        }
    }

    #[cfg(test)]
    pub(in crate::series::index) fn postings_for_label_value<'a>(
        &'a self,
        name: &str,
        value: &str,
    ) -> Cow<'a, PostingsBitmap> {
        self.terms().postings_for_label_value(name, value)
    }

    pub fn get_label_names(&self) -> BTreeSet<String> {
        self.terms().get_label_names()
    }

    pub fn get_label_values(&self, label_name: &str) -> Vec<String> {
        self.terms().get_label_values(label_name)
    }

    /// Return all series ids corresponding to the given labels
    pub fn postings_by_labels<T: SeriesLabel>(&self, labels: &[T]) -> PostingsBitmap {
        self.terms().postings_by_labels(labels)
    }

    /// Get the unique series id for the given set of labels if it exists.
    ///
    /// This exists primarily to ensure that we disallow duplicate metric names
    pub fn posting_id_by_labels<T: SeriesLabel>(&self, labels: &[T]) -> Option<SeriesRef> {
        self.terms().posting_id_by_labels(labels)
    }
}

impl<'a> Terms<'a> {
    /// Every id currently in the index. Stale-free by construction, so bitmaps derived from this
    /// alone need no masking.
    #[inline]
    pub(super) fn all(self) -> &'a PostingsBitmap {
        self.all
    }

    /// Drop stale ids from `postings`. See [`StaleSet::mask`].
    #[inline]
    pub(super) fn mask(self, postings: &mut PostingsBitmap) {
        self.stale.mask(postings);
    }

    /// Drop stale ids from a `Cow`. See [`StaleSet::mask_cow`].
    #[inline]
    pub(super) fn mask_cow(self, postings: Cow<'a, PostingsBitmap>) -> Cow<'a, PostingsBitmap> {
        self.stale.mask_cow(postings)
    }

    /// Union of `bitmaps`, stale ids removed. Borrows when at most one of them is non-empty.
    fn union_of(
        self,
        bitmaps: impl Iterator<Item = &'a PostingsBitmap>,
    ) -> Cow<'a, PostingsBitmap> {
        let acc = bitmaps.fold(Cow::Borrowed(&*EMPTY_BITMAP), |acc, bmp| {
            union_cow(acc, Cow::Borrowed(bmp))
        });
        self.mask_cow(acc)
    }

    /// Return postings for a key (borrowed if possible), applying stale removal.
    /// If stale_ids is non-empty, this returns Owned.
    fn postings_for_key(self, key: &[u8]) -> Cow<'a, PostingsBitmap> {
        match self.index.get(key) {
            Some(bmp) => self.mask_cow(Cow::Borrowed(bmp)),
            None => Cow::Borrowed(&*EMPTY_BITMAP),
        }
    }

    pub(super) fn postings_for_label_value(
        self,
        name: &str,
        value: &str,
    ) -> Cow<'a, PostingsBitmap> {
        let key = KeyBuffer::for_label_value(name, value);
        self.postings_for_key(key.as_bytes())
    }

    pub(super) fn get_label_names(self) -> BTreeSet<String> {
        let mut names: BTreeSet<String> = BTreeSet::new();
        for (key, map) in self.index.iter() {
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

    pub(super) fn get_label_values(self, label_name: &str) -> Vec<String> {
        let prefix = KeyBuffer::for_prefix(label_name);
        let mut values = Vec::with_capacity(8);
        for (k, map) in self.index.prefix(prefix.as_bytes()) {
            if !map.is_empty()
                && let Some((_key, value)) = k.split()
                && !value.is_empty()
            {
                values.push(value.to_string());
            }
        }
        values
    }

    /// Postings of every series that has the label `label_name`, whatever its value.
    pub(super) fn postings_for_all_label_values(self, label_name: &str) -> Cow<'a, PostingsBitmap> {
        let prefix = KeyBuffer::for_prefix(label_name);
        self.union_of(self.index.prefix(prefix.as_bytes()).map(|(_, map)| map))
    }

    /// `postings_for_label_values` returns a `PostingsBitmap` for the label pairs.
    /// The postings here contain the ids to the series inside the index.
    pub(super) fn postings_for_label_values(
        self,
        name: &str,
        values: &[String],
    ) -> Cow<'a, PostingsBitmap> {
        if let [value] = values {
            let key = KeyBuffer::for_label_value(name, value);
            return self.postings_for_key(key.as_bytes());
        }

        self.union_of(values.iter().filter_map(|value| {
            let key = KeyBuffer::for_label_value(name, value);
            self.index.get(key.as_bytes())
        }))
    }

    /// `postings_for_label_matching` returns postings having a label with the given name and a value
    /// for which `match_fn` returns true. If no postings are found having at least one matching label,
    /// an empty bitmap is returned.
    pub(super) fn postings_for_label_matching<F, STATE>(
        self,
        name: &str,
        state: &mut STATE,
        match_fn: F,
    ) -> Cow<'a, PostingsBitmap>
    where
        F: Fn(&str, &mut STATE) -> bool,
    {
        let prefix = KeyBuffer::for_prefix(name);
        let start_pos = prefix.len();
        self.union_of(
            self.index
                .prefix(prefix.as_bytes())
                .filter(|(key, _)| match_fn(key.sub_string(start_pos), state))
                .map(|(_, map)| map),
        )
    }

    /// Return all series ids corresponding to the given labels
    pub(super) fn postings_by_labels<T: SeriesLabel>(self, labels: &[T]) -> PostingsBitmap {
        let mut first = true;
        let mut acc = PostingsBitmap::new();

        for label in labels.iter() {
            let key = KeyBuffer::for_label_value(label.name(), label.value());
            let Some(bmp) = self.index.get(key.as_bytes()) else {
                acc.clear();
                break;
            };
            if bmp.is_empty() {
                acc.clear();
                break;
            }

            if first {
                acc |= bmp;
                first = false;
            } else {
                acc &= bmp;
                if acc.is_empty() {
                    break;
                }
            }
        }
        self.mask(&mut acc);

        acc
    }

    /// Retrieves a `PostingsBitmap` containing postings that match the specified label and prefix.
    ///
    /// This function searches for postings in the internal term index where the keys start with
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
    pub(super) fn postings_by_prefix(self, label: &str, prefix: &str) -> Cow<'a, PostingsBitmap> {
        let search_prefix = KeyBuffer::for_label_value_prefix(label, prefix);
        self.union_of(self.index.prefix(&search_prefix).map(|(_, map)| map))
    }

    pub(super) fn postings_by_prefix_and_predicate<F>(
        self,
        label: &str,
        prefix: &str,
        predicate: F,
    ) -> Cow<'a, PostingsBitmap>
    where
        F: Fn(&str) -> bool,
    {
        let search_prefix = KeyBuffer::for_label_value_prefix(label, prefix);
        let start_pos = label.len() + 1;
        self.union_of(
            self.index
                .prefix(&search_prefix)
                .filter(|(key, _)| predicate(key.sub_string(start_pos)))
                .map(|(_, map)| map),
        )
    }

    /// Get the unique series id for the given set of labels if it exists.
    ///
    /// This exists primarily to ensure that we disallow duplicate metric names
    pub(super) fn posting_id_by_labels<T: SeriesLabel>(self, labels: &[T]) -> Option<SeriesRef> {
        // Look every bitmap up before intersecting any: a new series nearly always carries one
        // unseen value, which this answers with key lookups alone.
        let mut bitmaps: SmallVec<[&'a PostingsBitmap; 8]> = SmallVec::with_capacity(labels.len());
        for label in labels {
            let key = KeyBuffer::for_label_value(label.name(), label.value());
            let bmp = self.index.get(key.as_bytes())?;
            if bmp.is_empty() {
                return None;
            }
            bitmaps.push(bmp);
        }

        // Smallest first, so the cost no longer depends on how the label names sort.
        bitmaps.sort_unstable_by_key(|bmp| bmp.cardinality());
        let (&smallest, rest) = bitmaps.split_first()?;

        // Usually a per-series label (`host`, `instance`) pins one candidate: probe the rest for it
        // instead of intersecting.
        if smallest.cardinality() == 1 {
            let id = smallest.minimum()?;
            let unique = !self.stale.contains(id) && rest.iter().all(|bmp| bmp.contains(id));
            return unique.then_some(id);
        }

        let mut acc = Cow::Borrowed(smallest);
        for &bmp in rest {
            acc = match acc {
                Cow::Borrowed(first) => Cow::Owned(first.and(bmp)),
                Cow::Owned(mut acc) => {
                    acc.and_inplace(bmp);
                    Cow::Owned(acc)
                }
            };
            if acc.is_empty() {
                return None;
            }
        }

        let acc = self.mask_cow(acc);

        (acc.cardinality() == 1)
            .then(|| acc.iter().next())
            .flatten()
    }

    pub(super) fn postings_without_label(self, label: &str) -> Cow<'a, PostingsBitmap> {
        let to_remove = self.postings_for_all_label_values(label);
        if to_remove.is_empty() {
            Cow::Borrowed(self.all)
        } else {
            Cow::Owned(self.all.andnot(&to_remove))
        }
    }
}

/// `a | b`, borrowing whichever side is the whole answer and reusing an owned side's buffer
/// otherwise.
pub(super) fn union_cow<'a>(
    a: Cow<'a, PostingsBitmap>,
    b: Cow<'a, PostingsBitmap>,
) -> Cow<'a, PostingsBitmap> {
    if b.is_empty() {
        return a;
    }
    if a.is_empty() {
        return b;
    }
    match (a, b) {
        (Cow::Owned(mut a), b) | (b, Cow::Owned(mut a)) => {
            a |= b.as_ref();
            Cow::Owned(a)
        }
        (Cow::Borrowed(a), Cow::Borrowed(b)) => Cow::Owned(a.or(b)),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::labels::Label;

    fn single_value_fixture() -> Postings {
        let mut postings = Postings::default();
        postings.add_posting_for_label_value(1, "job", "api");
        postings.add_posting_for_label_value(2, "job", "api");
        postings.add_posting_for_label_value(3, "env", "prod");
        postings.add_posting_for_label_value(3, "env", "dev");
        postings
    }

    #[test]
    fn unions_over_one_key_stay_borrowed() {
        let postings = single_value_fixture();
        let terms = postings.terms();

        let all_values = terms.postings_for_all_label_values("job");
        assert!(matches!(all_values, Cow::Borrowed(_)));
        assert_eq!(all_values.iter().collect::<Vec<_>>(), vec![1, 2]);

        let values = vec!["api".to_string(), "web".to_string()];
        assert!(matches!(
            terms.postings_for_label_values("job", &values),
            Cow::Borrowed(_)
        ));
        assert!(matches!(
            terms.postings_for_label_matching("env", &mut (), |v, _| v == "dev"),
            Cow::Borrowed(_)
        ));
        assert!(matches!(
            terms.postings_by_prefix("job", "a"),
            Cow::Borrowed(_)
        ));
        assert!(matches!(
            terms.postings_by_prefix_and_predicate("env", "d", |_| true),
            Cow::Borrowed(_)
        ));
        assert!(terms.postings_by_prefix("job", "zzz").is_empty());
    }

    #[test]
    fn unions_over_several_keys_are_owned_and_correct() {
        let mut postings = single_value_fixture();
        postings.add_posting_for_label_value(4, "env", "prod");
        let terms = postings.terms();

        let env = terms.postings_for_all_label_values("env");
        assert!(matches!(env, Cow::Owned(_)));
        assert_eq!(env.iter().collect::<Vec<_>>(), vec![3, 4]);
    }

    #[test]
    fn unions_mask_stale_ids_even_when_borrowable() {
        let mut postings = single_value_fixture();
        postings.mark_ids_as_stale(&[2]);
        let terms = postings.terms();

        let job = terms.postings_for_all_label_values("job");
        assert_eq!(job.iter().collect::<Vec<_>>(), vec![1]);
        let by_prefix = terms.postings_by_prefix("job", "a");
        assert_eq!(by_prefix.iter().collect::<Vec<_>>(), vec![1]);
    }

    #[test]
    fn posting_id_by_labels_finds_unique_id() {
        let mut postings = Postings::default();
        for id in 1..=100 {
            postings.add_posting_for_label_value(id, "__name__", "cpu");
            postings.add_posting_for_label_value(id, "host", &format!("h{id}"));
        }
        let labels = [Label::new("__name__", "cpu"), Label::new("host", "h42")];
        assert_eq!(postings.posting_id_by_labels(&labels), Some(42));

        // a single label matching many series is not a unique id
        assert_eq!(
            postings.posting_id_by_labels(&[Label::new("__name__", "cpu")]),
            None
        );
        assert_eq!(
            postings.posting_id_by_labels(&[Label::new("host", "h7")]),
            Some(7)
        );

        postings.mark_ids_as_stale(&[42]);
        assert_eq!(postings.posting_id_by_labels(&labels), None);
    }

    #[test]
    fn posting_id_by_labels_ignores_label_order() {
        let mut postings = Postings::default();
        for id in 1..=100 {
            postings.add_posting_for_label_value(id, "__name__", "cpu");
            postings.add_posting_for_label_value(id, "host", &format!("h{id}"));
        }
        let forward = [Label::new("__name__", "cpu"), Label::new("host", "h42")];
        let reversed = [Label::new("host", "h42"), Label::new("__name__", "cpu")];
        assert_eq!(postings.posting_id_by_labels(&forward), Some(42));
        assert_eq!(postings.posting_id_by_labels(&reversed), Some(42));

        // the single candidate must carry every other label too
        let mismatch = [Label::new("__name__", "mem"), Label::new("host", "h42")];
        assert_eq!(postings.posting_id_by_labels(&mismatch), None);
        postings.add_posting_for_label_value(42, "__name__", "mem");
        postings.add_posting_for_label_value(43, "__name__", "mem");
        assert_eq!(postings.posting_id_by_labels(&mismatch), Some(42));
        let missing = [Label::new("__name__", "cpu"), Label::new("host", "h-new")];
        assert_eq!(postings.posting_id_by_labels(&missing), None);
    }

    #[test]
    fn posting_id_by_labels_without_a_unique_label() {
        let mut postings = Postings::default();
        for id in [1, 2, 3] {
            postings.add_posting_for_label_value(id, "a", "x");
        }
        for id in [2, 3, 4] {
            postings.add_posting_for_label_value(id, "b", "y");
        }
        for id in [3, 5] {
            postings.add_posting_for_label_value(id, "c", "z");
        }
        let labels = [
            Label::new("a", "x"),
            Label::new("b", "y"),
            Label::new("c", "z"),
        ];
        assert_eq!(postings.posting_id_by_labels(&labels), Some(3));

        // two candidates left after masking one of three
        let ab = [Label::new("a", "x"), Label::new("b", "y")];
        assert_eq!(postings.posting_id_by_labels(&ab), None);
        postings.mark_ids_as_stale(&[2]);
        assert_eq!(postings.posting_id_by_labels(&ab), Some(3));
    }

    #[test]
    fn union_cow_borrows_the_non_empty_side() {
        let a = PostingsBitmap::of(&[1, 2]);
        let b = PostingsBitmap::of(&[3]);
        let empty = PostingsBitmap::new();

        let only_a = union_cow(Cow::Borrowed(&a), Cow::Borrowed(&empty));
        assert!(matches!(only_a, Cow::Borrowed(x) if std::ptr::eq(x, &a)));
        let only_b = union_cow(Cow::Borrowed(&empty), Cow::Borrowed(&b));
        assert!(matches!(only_b, Cow::Borrowed(x) if std::ptr::eq(x, &b)));

        let both = union_cow(Cow::Borrowed(&a), Cow::Owned(b.clone()));
        assert_eq!(both.iter().collect::<Vec<_>>(), vec![1, 2, 3]);
    }

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
        let result = postings
            .terms()
            .postings_for_label_values("label1", &values);

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
        let result = postings.terms().postings_for_label_values("label", &values);

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
        let result = postings.terms().postings_for_label_values("label", &values);

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
        let result = postings
            .terms()
            .postings_for_label_values(label_name, &values);
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
        let result = postings.terms().postings_for_label_values("标签", &values);

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
        let result = postings.terms().postings_for_all_label_values(label_name);

        // Verify the result contains all series IDs with the long label name
        assert_eq!(result.cardinality(), 3);
        assert!(result.contains(1));
        assert!(result.contains(2));
        assert!(result.contains(3));
        assert!(!result.contains(4)); // This has a different label

        // Also test that we can retrieve postings for the short label
        let result_short = postings
            .terms()
            .postings_for_all_label_values("short_label");
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
