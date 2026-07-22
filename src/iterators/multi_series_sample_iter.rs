use crate::common::Sample;
use min_max_heap::MinMaxHeap;
use std::cmp::Ordering;

/// A pending sample tagged with the iterator it came from, so the merge can refill exactly
/// that iterator once the sample is emitted.
#[derive(Debug, Clone, Copy)]
struct Entry {
    sample: Sample,
    source: usize,
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Entry {}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Timestamp first. `source` only breaks ties, so equal timestamps come out in
        // series order rather than in whatever order the heap happens to hold them —
        // and values (which may be NaN) never take part in the comparison.
        self.sample
            .timestamp
            .cmp(&other.sample.timestamp)
            .then(self.source.cmp(&other.source))
    }
}

/// Iterate over multiple sorted `Sample` iterators, yielding samples in timestamp order.
///
/// A textbook k-way merge: the heap holds the next unread sample of **every** live iterator,
/// and emitting one refills from that same iterator. Holding that invariant is the whole
/// correctness argument — `pop_min` is only the global minimum if every iterator is
/// represented.
///
/// An earlier version pushed a bounded *prefix* of each iterator and drained the heap
/// completely before reloading, which broke the invariant: with `{0,10}` and `{1,2,3}` it
/// loaded `{0,1,2,10}`, emitted all four, and only then read `3` — yielding
/// `0,1,2,10,3`. That surfaced as out-of-order `TS.MRANGE ... GROUPBY ... REDUCE` replies
/// (found by the Tier C differential fuzzer).
pub struct MultiSeriesSampleIter<T: Iterator<Item = Sample>> {
    heap: MinMaxHeap<Entry>,
    inner: Vec<T>,
    primed: bool,
}

impl<T: Iterator<Item = Sample>> MultiSeriesSampleIter<T> {
    pub fn new(list: Vec<T>) -> Self {
        let len = list.len();
        Self {
            inner: list,
            heap: MinMaxHeap::with_capacity(len),
            primed: false,
        }
    }

    /// Seed the heap with the first sample of every iterator.
    fn prime(&mut self) {
        for source in 0..self.inner.len() {
            if let Some(sample) = self.inner[source].next() {
                self.heap.push(Entry { sample, source });
            }
        }
        self.primed = true;
    }
}

impl<T: Iterator<Item = Sample>> Iterator for MultiSeriesSampleIter<T> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.primed {
            self.prime();
        }

        let entry = self.heap.pop_min()?;
        // Refill from the iterator just consumed, keeping every live iterator represented.
        // An exhausted one simply stops contributing.
        if let Some(sample) = self.inner[entry.source].next() {
            self.heap.push(Entry {
                sample,
                source: entry.source,
            });
        }
        Some(entry.sample)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_heap_supports_duplicates() {
        let mut heap = MinMaxHeap::<u32>::new();

        heap.push(1);
        heap.push(1);
        assert_eq!(heap.len(), 2);
        heap.push(1);
        assert_eq!(heap.len(), 3);
        heap.push(2);
        heap.push(3);
        heap.push(3);
        assert_eq!(heap.len(), 6);
        let mut vec = heap.into_vec();
        vec.sort();
        assert_eq!(vec, vec![1, 1, 1, 2, 3, 3]);
    }

    // Helper constructor matching the project's Sample shape (timestamp, value).
    fn make_sample(ts: i64, val: f64) -> Sample {
        Sample {
            timestamp: ts,
            value: val,
        }
    }

    #[test]
    fn merge_two_series_in_order() {
        let a = vec![
            make_sample(1, 1.0),
            make_sample(4, 4.0),
            make_sample(5, 5.0),
        ];
        let b = vec![
            make_sample(2, 2.0),
            make_sample(3, 3.0),
            make_sample(6, 6.0),
        ];
        let mut iter = MultiSeriesSampleIter::new(vec![a.into_iter(), b.into_iter()]);

        let timestamps: Vec<i64> = iter.by_ref().map(|s| s.timestamp).collect();
        assert_eq!(timestamps, vec![1, 2, 3, 4, 5, 6]);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn preserves_duplicates_and_equal_timestamps() {
        let a = vec![
            make_sample(1, 1.0),
            make_sample(2, 2.0),
            make_sample(2, 2.5),
        ];
        let b = vec![make_sample(2, 9.0), make_sample(3, 3.0)];
        let mut iter = MultiSeriesSampleIter::new(vec![a.into_iter(), b.into_iter()]);

        let timestamps: Vec<i64> = iter.by_ref().map(|s| s.timestamp).collect();
        // three entries with timestamp 2 should be preserved
        assert_eq!(timestamps, vec![1, 2, 2, 2, 3]);
        assert_eq!(iter.next(), None);

        let b = vec![make_sample(2, 55.0)];
        let c = vec![make_sample(2, 40.0)];
        let a = vec![make_sample(1, 100.0)];
        let mut iter =
            MultiSeriesSampleIter::new(vec![a.into_iter(), b.into_iter(), c.into_iter()]);
        let timestamps: Vec<i64> = iter.by_ref().map(|s| s.timestamp).collect();
        assert_eq!(timestamps, vec![1, 2, 2]);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn empty_input_yields_none() {
        let mut iter: MultiSeriesSampleIter<std::vec::IntoIter<Sample>> =
            MultiSeriesSampleIter::new(vec![]);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn exhausted_iterators_removed_and_remaining_consumed() {
        let a = vec![make_sample(1, 1.0)]; // short series
        let b = vec![make_sample(2, 2.0), make_sample(3, 3.0)]; // longer series
        let mut iter = MultiSeriesSampleIter::new(vec![a.into_iter(), b.into_iter()]);

        let collected: Vec<i64> = iter.by_ref().map(|s| s.timestamp).collect();
        assert_eq!(collected, vec![1, 2, 3]);
        // ensure iterator is fully exhausted afterwards
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn single_series_iterator() {
        let a = vec![
            make_sample(1, 1.0),
            make_sample(2, 2.0),
            make_sample(3, 3.0),
        ];
        let mut iter = MultiSeriesSampleIter::new(vec![a.into_iter()]);

        let timestamps: Vec<i64> = iter.by_ref().map(|s| s.timestamp).collect();
        assert_eq!(timestamps, vec![1, 2, 3]);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn multiple_empty_iterators() {
        let empty_a: Vec<Sample> = vec![];
        let empty_b: Vec<Sample> = vec![];
        let empty_c: Vec<Sample> = vec![];
        let mut iter = MultiSeriesSampleIter::new(vec![
            empty_a.into_iter(),
            empty_b.into_iter(),
            empty_c.into_iter(),
        ]);

        assert_eq!(iter.next(), None);
    }

    #[test]
    fn mixed_empty_and_non_empty_iterators() {
        let empty: Vec<Sample> = vec![];
        let a = vec![make_sample(1, 1.0), make_sample(3, 3.0)];
        let b = vec![make_sample(2, 2.0)];

        let mut iter =
            MultiSeriesSampleIter::new(vec![empty.into_iter(), a.into_iter(), b.into_iter()]);

        let timestamps: Vec<i64> = iter.by_ref().map(|s| s.timestamp).collect();
        assert_eq!(timestamps, vec![1, 2, 3]);
    }

    #[test]
    fn reverse_order_timestamps() {
        // Each series is ordered, but one starts higher than the other
        let a = vec![make_sample(10, 10.0), make_sample(20, 20.0)];
        let b = vec![make_sample(1, 1.0), make_sample(5, 5.0)];

        let mut iter = MultiSeriesSampleIter::new(vec![a.into_iter(), b.into_iter()]);

        let timestamps: Vec<i64> = iter.by_ref().map(|s| s.timestamp).collect();
        assert_eq!(timestamps, vec![1, 5, 10, 20]);
    }

    #[test]
    fn many_series_with_interleaved_timestamps() {
        let a = vec![make_sample(1, 1.0), make_sample(7, 7.0)];
        let b = vec![make_sample(2, 2.0), make_sample(8, 8.0)];
        let c = vec![make_sample(3, 3.0), make_sample(9, 9.0)];
        let d = vec![make_sample(4, 4.0), make_sample(10, 10.0)];

        let mut iter = MultiSeriesSampleIter::new(vec![
            a.into_iter(),
            b.into_iter(),
            c.into_iter(),
            d.into_iter(),
        ]);

        let timestamps: Vec<i64> = iter.by_ref().map(|s| s.timestamp).collect();
        assert_eq!(timestamps, vec![1, 2, 3, 4, 7, 8, 9, 10]);
    }

    #[test]
    fn all_same_timestamp() {
        let a = vec![make_sample(5, 1.0), make_sample(5, 2.0)];
        let b = vec![make_sample(5, 3.0), make_sample(5, 4.0)];

        let iter = MultiSeriesSampleIter::new(vec![a.into_iter(), b.into_iter()]);

        let results: Vec<Sample> = iter.collect();
        assert_eq!(results.len(), 4);
        assert!(results.iter().all(|s| s.timestamp == 5));
    }

    #[test]
    fn values_preserved_correctly() {
        let a = vec![make_sample(1, 10.5), make_sample(3, 30.5)];
        let b = vec![make_sample(2, 20.5)];

        let iter = MultiSeriesSampleIter::new(vec![a.into_iter(), b.into_iter()]);

        let results: Vec<Sample> = iter.collect();
        assert_eq!(
            results[0],
            Sample {
                timestamp: 1,
                value: 10.5
            }
        );
        assert_eq!(
            results[1],
            Sample {
                timestamp: 2,
                value: 20.5
            }
        );
        assert_eq!(
            results[2],
            Sample {
                timestamp: 3,
                value: 30.5
            }
        );
    }

    #[test]
    fn large_timestamp_gap() {
        let a = vec![make_sample(1, 1.0), make_sample(1000000, 2.0)];
        let b = vec![make_sample(2, 2.0), make_sample(999999, 3.0)];

        let mut iter = MultiSeriesSampleIter::new(vec![a.into_iter(), b.into_iter()]);

        let timestamps: Vec<i64> = iter.by_ref().map(|s| s.timestamp).collect();
        assert_eq!(timestamps, vec![1, 2, 999999, 1000000]);
    }

    #[test]
    fn refills_the_consumed_iterator_before_emitting() {
        // Regression: the old prefix-loading merge emitted 0,1,2,10,3 here, because it
        // drained {0,1,2,10} from the heap before ever reading `3`. Found via
        // out-of-order TS.MRANGE GROUPBY REDUCE replies (Tier C fuzzer).
        let a = vec![make_sample(0, 0.0), make_sample(10, 10.0)];
        let b = vec![make_sample(1, 1.0), make_sample(2, 2.0), make_sample(3, 3.0)];

        let iter = MultiSeriesSampleIter::new(vec![a.into_iter(), b.into_iter()]);
        let timestamps: Vec<i64> = iter.map(|s| s.timestamp).collect();
        assert_eq!(timestamps, vec![0, 1, 2, 3, 10]);
    }

    #[test]
    fn one_iterator_outruns_the_others_by_many_samples() {
        // The gap between the short series' samples spans an arbitrary run of the long one.
        let a = vec![make_sample(0, 0.0), make_sample(100, 100.0)];
        let b: Vec<Sample> = (1..=20).map(|i| make_sample(i, i as f64)).collect();

        let iter = MultiSeriesSampleIter::new(vec![a.into_iter(), b.into_iter()]);
        let timestamps: Vec<i64> = iter.map(|s| s.timestamp).collect();

        let mut expected: Vec<i64> = (0..=20).collect();
        expected.push(100);
        assert_eq!(timestamps, expected);
    }

    #[test]
    fn output_is_sorted_for_many_ragged_series() {
        // Ragged lengths and offsets across more series than the heap's initial capacity.
        let series: Vec<Vec<Sample>> = (0..7)
            .map(|s| {
                (0..(s * 3 + 1))
                    .map(|i| make_sample((i * 7 + s * 2) as i64, 1.0))
                    .collect()
            })
            .collect();
        let mut expected: Vec<i64> = series
            .iter()
            .flat_map(|s| s.iter().map(|x| x.timestamp))
            .collect();
        expected.sort_unstable();

        let iter = MultiSeriesSampleIter::new(
            series.into_iter().map(|s| s.into_iter()).collect::<Vec<_>>(),
        );
        let timestamps: Vec<i64> = iter.map(|s| s.timestamp).collect();
        assert_eq!(timestamps, expected);
    }

    #[test]
    fn very_unbalanced_series_lengths() {
        let short = vec![make_sample(1, 1.0)];
        let long = vec![
            make_sample(2, 2.0),
            make_sample(3, 3.0),
            make_sample(4, 4.0),
            make_sample(5, 5.0),
            make_sample(6, 6.0),
        ];

        let mut iter = MultiSeriesSampleIter::new(vec![short.into_iter(), long.into_iter()]);

        let timestamps: Vec<i64> = iter.by_ref().map(|s| s.timestamp).collect();
        assert_eq!(timestamps, vec![1, 2, 3, 4, 5, 6]);
    }
}
