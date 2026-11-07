use crate::common::{MultiSample, Sample, Timestamp};
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::collections::binary_heap::PeekMut;

/// What a merge orders its items by.
pub(crate) trait Timestamped {
    fn timestamp(&self) -> Timestamp;
}

impl Timestamped for Sample {
    fn timestamp(&self) -> Timestamp {
        self.timestamp
    }
}

impl Timestamped for MultiSample {
    fn timestamp(&self) -> Timestamp {
        self.timestamp
    }
}

/// A pending item tagged with the iterator it came from, so a merge can refill exactly that
/// iterator once the item is consumed.
pub(super) struct MergeEntry<T> {
    pub(super) item: T,
    pub(super) source: usize,
}

impl<T: Timestamped> PartialEq for MergeEntry<T> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<T: Timestamped> Eq for MergeEntry<T> {}

impl<T: Timestamped> PartialOrd for MergeEntry<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Timestamped> Ord for MergeEntry<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Timestamp first. `source` only breaks ties, so equal timestamps come out in series
        // order rather than in whatever order the heap happens to hold them, and values (which
        // may be NaN) never take part in the comparison.
        self.item
            .timestamp()
            .cmp(&other.item.timestamp())
            .then(self.source.cmp(&other.source))
    }
}

/// `Reverse` because `BinaryHeap` is a max-heap.
pub(super) type MergeHeap<T> = BinaryHeap<Reverse<MergeEntry<T>>>;

/// Seed a heap with the first item of every iterator.
pub(super) fn seeded_heap<I>(sources: &mut [I]) -> MergeHeap<I::Item>
where
    I: Iterator,
    I::Item: Timestamped,
{
    let mut heap = BinaryHeap::with_capacity(sources.len());
    for (source, iter) in sources.iter_mut().enumerate() {
        if let Some(item) = iter.next() {
            heap.push(Reverse(MergeEntry { item, source }));
        }
    }
    heap
}

/// Iterate over multiple sorted iterators, yielding their items in timestamp order.
///
/// A textbook k-way merge: the heap holds the next unread item of **every** live iterator, and
/// emitting one refills from that same iterator. Holding that invariant is the whole correctness
/// argument: the heap's top is only the global minimum if every iterator is represented.
///
/// An earlier version pushed a bounded *prefix* of each iterator and drained the heap completely
/// before reloading, which broke the invariant: with `{0,10}` and `{1,2,3}` it loaded `{0,1,2,10}`,
/// emitted all four, and only then read `3`, yielding `0,1,2,10,3`. That surfaced as out-of-order
/// `TS.MRANGE ... GROUPBY ... REDUCE` replies (found by the Tier C differential fuzzer).
///
/// Emitting replaces the top in place with `peek_mut`, one sift rather than a pop and a push, so
/// a source that keeps winning costs almost nothing.
pub(crate) struct KWayMerge<I>
where
    I: Iterator,
    I::Item: Timestamped,
{
    heap: MergeHeap<I::Item>,
    inner: Vec<I>,
    primed: bool,
}

impl<I> KWayMerge<I>
where
    I: Iterator,
    I::Item: Timestamped,
{
    pub(crate) fn new(list: Vec<I>) -> Self {
        Self {
            inner: list,
            heap: BinaryHeap::new(),
            primed: false,
        }
    }
}

impl<I> Iterator for KWayMerge<I>
where
    I: Iterator,
    I::Item: Timestamped,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.primed {
            self.heap = seeded_heap(&mut self.inner);
            self.primed = true;
        }

        let mut top = self.heap.peek_mut()?;
        let source = top.0.source;
        // Refill from the iterator just consumed, keeping every live iterator represented.
        // An exhausted one simply stops contributing.
        match self.inner[source].next() {
            Some(item) => Some(std::mem::replace(&mut top.0.item, item)),
            None => Some(PeekMut::pop(top).0.item),
        }
    }
}

#[cfg(test)]
mod sample_tests {
    use super::*;

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
        let mut iter = KWayMerge::new(vec![a.into_iter(), b.into_iter()]);

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
        let mut iter = KWayMerge::new(vec![a.into_iter(), b.into_iter()]);

        let timestamps: Vec<i64> = iter.by_ref().map(|s| s.timestamp).collect();
        // three entries with timestamp 2 should be preserved
        assert_eq!(timestamps, vec![1, 2, 2, 2, 3]);
        assert_eq!(iter.next(), None);

        let b = vec![make_sample(2, 55.0)];
        let c = vec![make_sample(2, 40.0)];
        let a = vec![make_sample(1, 100.0)];
        let mut iter = KWayMerge::new(vec![a.into_iter(), b.into_iter(), c.into_iter()]);
        let timestamps: Vec<i64> = iter.by_ref().map(|s| s.timestamp).collect();
        assert_eq!(timestamps, vec![1, 2, 2]);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn empty_input_yields_none() {
        let mut iter: KWayMerge<std::vec::IntoIter<Sample>> = KWayMerge::new(vec![]);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn exhausted_iterators_removed_and_remaining_consumed() {
        let a = vec![make_sample(1, 1.0)]; // short series
        let b = vec![make_sample(2, 2.0), make_sample(3, 3.0)]; // longer series
        let mut iter = KWayMerge::new(vec![a.into_iter(), b.into_iter()]);

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
        let mut iter = KWayMerge::new(vec![a.into_iter()]);

        let timestamps: Vec<i64> = iter.by_ref().map(|s| s.timestamp).collect();
        assert_eq!(timestamps, vec![1, 2, 3]);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn multiple_empty_iterators() {
        let empty_a: Vec<Sample> = vec![];
        let empty_b: Vec<Sample> = vec![];
        let empty_c: Vec<Sample> = vec![];
        let mut iter = KWayMerge::new(vec![
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

        let mut iter = KWayMerge::new(vec![empty.into_iter(), a.into_iter(), b.into_iter()]);

        let timestamps: Vec<i64> = iter.by_ref().map(|s| s.timestamp).collect();
        assert_eq!(timestamps, vec![1, 2, 3]);
    }

    #[test]
    fn reverse_order_timestamps() {
        // Each series is ordered, but one starts higher than the other
        let a = vec![make_sample(10, 10.0), make_sample(20, 20.0)];
        let b = vec![make_sample(1, 1.0), make_sample(5, 5.0)];

        let mut iter = KWayMerge::new(vec![a.into_iter(), b.into_iter()]);

        let timestamps: Vec<i64> = iter.by_ref().map(|s| s.timestamp).collect();
        assert_eq!(timestamps, vec![1, 5, 10, 20]);
    }

    #[test]
    fn many_series_with_interleaved_timestamps() {
        let a = vec![make_sample(1, 1.0), make_sample(7, 7.0)];
        let b = vec![make_sample(2, 2.0), make_sample(8, 8.0)];
        let c = vec![make_sample(3, 3.0), make_sample(9, 9.0)];
        let d = vec![make_sample(4, 4.0), make_sample(10, 10.0)];

        let mut iter = KWayMerge::new(vec![
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

        let iter = KWayMerge::new(vec![a.into_iter(), b.into_iter()]);

        let results: Vec<Sample> = iter.collect();
        assert_eq!(results.len(), 4);
        assert!(results.iter().all(|s| s.timestamp == 5));
    }

    #[test]
    fn values_preserved_correctly() {
        let a = vec![make_sample(1, 10.5), make_sample(3, 30.5)];
        let b = vec![make_sample(2, 20.5)];

        let iter = KWayMerge::new(vec![a.into_iter(), b.into_iter()]);

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

        let mut iter = KWayMerge::new(vec![a.into_iter(), b.into_iter()]);

        let timestamps: Vec<i64> = iter.by_ref().map(|s| s.timestamp).collect();
        assert_eq!(timestamps, vec![1, 2, 999999, 1000000]);
    }

    #[test]
    fn refills_the_consumed_iterator_before_emitting() {
        // Regression: the old prefix-loading merge emitted 0,1,2,10,3 here, because it
        // drained {0,1,2,10} from the heap before ever reading `3`. Found via
        // out-of-order TS.MRANGE GROUPBY REDUCE replies (Tier C fuzzer).
        let a = vec![make_sample(0, 0.0), make_sample(10, 10.0)];
        let b = vec![
            make_sample(1, 1.0),
            make_sample(2, 2.0),
            make_sample(3, 3.0),
        ];

        let iter = KWayMerge::new(vec![a.into_iter(), b.into_iter()]);
        let timestamps: Vec<i64> = iter.map(|s| s.timestamp).collect();
        assert_eq!(timestamps, vec![0, 1, 2, 3, 10]);
    }

    #[test]
    fn one_iterator_outruns_the_others_by_many_samples() {
        // The gap between the short series' samples spans an arbitrary run of the long one.
        let a = vec![make_sample(0, 0.0), make_sample(100, 100.0)];
        let b: Vec<Sample> = (1..=20).map(|i| make_sample(i, i as f64)).collect();

        let iter = KWayMerge::new(vec![a.into_iter(), b.into_iter()]);
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

        let iter = KWayMerge::new(
            series
                .into_iter()
                .map(|s| s.into_iter())
                .collect::<Vec<_>>(),
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

        let mut iter = KWayMerge::new(vec![short.into_iter(), long.into_iter()]);

        let timestamps: Vec<i64> = iter.by_ref().map(|s| s.timestamp).collect();
        assert_eq!(timestamps, vec![1, 2, 3, 4, 5, 6]);
    }

    fn values(iter: impl Iterator<Item = Sample>) -> Vec<f64> {
        iter.map(|s| s.value).collect()
    }

    /// Order-sensitive reducers (`first`, `last`) see equal timestamps in this order, so it has
    /// to be series order rather than whatever order the heap happens to hold them in.
    #[test]
    fn equal_timestamps_come_out_in_source_order() {
        let sources = vec![
            vec![make_sample(1, 30.0)],
            vec![make_sample(1, 10.0)],
            vec![make_sample(1, 20.0)],
        ];
        let iter = KWayMerge::new(sources.into_iter().map(|s| s.into_iter()).collect());

        assert_eq!(values(iter), vec![30.0, 10.0, 20.0]);
    }

    /// A source that repeats a timestamp finishes it before the next source starts, because its
    /// refilled entry still sorts first.
    #[test]
    fn a_source_repeating_a_timestamp_stays_ahead_of_later_sources() {
        let a = vec![make_sample(5, 1.0), make_sample(5, 2.0)];
        let b = vec![make_sample(5, 3.0), make_sample(5, 4.0)];
        let iter = KWayMerge::new(vec![a.into_iter(), b.into_iter()]);

        assert_eq!(values(iter), vec![1.0, 2.0, 3.0, 4.0]);
    }

    /// Values never take part in the ordering, so NaN cannot disturb it.
    #[test]
    fn nan_values_do_not_affect_merge_order() {
        let a = vec![make_sample(1, f64::NAN), make_sample(3, 3.0)];
        let b = vec![make_sample(1, 1.0), make_sample(2, f64::NAN)];
        let iter = KWayMerge::new(vec![a.into_iter(), b.into_iter()]);

        let merged: Vec<Sample> = iter.collect();
        assert_eq!(
            merged.iter().map(|s| s.timestamp).collect::<Vec<_>>(),
            vec![1, 1, 2, 3]
        );
        assert!(merged[0].value.is_nan());
        assert_eq!(merged[1].value, 1.0);
        assert!(merged[2].value.is_nan());
        assert_eq!(merged[3].value, 3.0);
    }

    /// Full-sequence comparison against a stable sort: timestamps, ties and values together, over
    /// enough ragged sources to exercise refills, exhaustion and the heap's interior.
    #[test]
    fn matches_a_stable_sort_of_the_concatenated_sources() {
        let sources: Vec<Vec<Sample>> = (0..64i64)
            .map(|source| {
                (0..(source % 13))
                    .map(|i| make_sample((i * 3 + source % 5) * 10, (source * 1000 + i) as f64))
                    .collect()
            })
            .collect();

        let mut expected: Vec<Sample> = sources.iter().flatten().copied().collect();
        expected.sort_by_key(|s| s.timestamp);

        let merged: Vec<Sample> =
            KWayMerge::new(sources.into_iter().map(|s| s.into_iter()).collect()).collect();

        assert_eq!(merged, expected);
    }
}

#[cfg(test)]
mod row_tests {
    use super::*;
    use smallvec::smallvec;

    fn row(ts: i64, values: &[f64]) -> MultiSample {
        MultiSample {
            timestamp: ts,
            values: values.iter().copied().collect(),
        }
    }

    #[test]
    fn merge_two_series_in_order() {
        let a = vec![row(1, &[1.0, 10.0]), row(4, &[4.0, 40.0])];
        let b = vec![row(2, &[2.0, 20.0]), row(3, &[3.0, 30.0])];
        let iter = KWayMerge::new(vec![a.into_iter(), b.into_iter()]);

        let timestamps: Vec<i64> = iter.map(|r| r.timestamp).collect();
        assert_eq!(timestamps, vec![1, 2, 3, 4]);
    }

    #[test]
    fn preserves_equal_timestamps_and_values() {
        let a = vec![row(1, &[1.0]), row(2, &[2.0])];
        let b = vec![row(2, &[9.0]), row(3, &[3.0])];
        let rows: Vec<MultiSample> = KWayMerge::new(vec![a.into_iter(), b.into_iter()]).collect();

        assert_eq!(
            rows.iter().map(|r| r.timestamp).collect::<Vec<_>>(),
            vec![1, 2, 2, 3]
        );
        // both timestamp-2 rows survive with their values intact
        let mut ts2: Vec<f64> = rows
            .iter()
            .filter(|r| r.timestamp == 2)
            .map(|r| r.values[0])
            .collect();
        ts2.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(ts2, vec![2.0, 9.0]);
    }

    #[test]
    fn empty_input_yields_none() {
        let mut iter: KWayMerge<std::vec::IntoIter<MultiSample>> = KWayMerge::new(vec![]);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn values_preserved() {
        let a = vec![MultiSample {
            timestamp: 5,
            values: smallvec![1.5, 2.5, 3.5],
        }];
        let rows: Vec<MultiSample> = KWayMerge::new(vec![a.into_iter()]).collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values.as_slice(), &[1.5, 2.5, 3.5]);
    }

    /// See the sibling test on `the sample merge`: reducers rely on this order.
    #[test]
    fn equal_timestamps_come_out_in_source_order() {
        let sources = vec![
            vec![row(1, &[30.0])],
            vec![row(1, &[10.0])],
            vec![row(1, &[20.0])],
        ];
        let rows: Vec<MultiSample> =
            KWayMerge::new(sources.into_iter().map(|s| s.into_iter()).collect()).collect();

        assert_eq!(
            rows,
            vec![row(1, &[30.0]), row(1, &[10.0]), row(1, &[20.0])]
        );
    }

    #[test]
    fn a_source_repeating_a_timestamp_stays_ahead_of_later_sources() {
        let a = vec![row(5, &[1.0]), row(5, &[2.0])];
        let b = vec![row(5, &[3.0])];
        let rows: Vec<MultiSample> = KWayMerge::new(vec![a.into_iter(), b.into_iter()]).collect();

        assert_eq!(rows, vec![row(5, &[1.0]), row(5, &[2.0]), row(5, &[3.0])]);
    }

    /// Full-sequence comparison against a stable sort, over rows wide enough to spill to the
    /// heap, so a refill that mishandled the entry's payload would show up as a wrong value.
    #[test]
    fn matches_a_stable_sort_of_the_concatenated_sources() {
        let sources: Vec<Vec<MultiSample>> = (0..32i64)
            .map(|source| {
                (0..(source % 7))
                    .map(|i| {
                        let v = (source * 1000 + i) as f64;
                        row(
                            (i * 3 + source % 5) * 10,
                            &[v, v + 0.5, v + 1.0, v + 1.5, v + 2.0],
                        )
                    })
                    .collect()
            })
            .collect();

        let mut expected: Vec<MultiSample> = sources.iter().flatten().cloned().collect();
        expected.sort_by_key(|r| r.timestamp);

        let merged: Vec<MultiSample> =
            KWayMerge::new(sources.into_iter().map(|s| s.into_iter()).collect()).collect();

        assert_eq!(merged, expected);
    }
}
