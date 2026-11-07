use crate::common::MultiSample;
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::collections::binary_heap::PeekMut;

/// Heap entry ordering rows by bucket timestamp, with the source iterator index breaking
/// ties so equal timestamps come out in series order. `source` also lets the merge refill
/// exactly the iterator a row was taken from.
struct RowEntry {
    row: MultiSample,
    source: usize,
}

impl PartialEq for RowEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for RowEntry {}

impl PartialOrd for RowEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RowEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.row
            .timestamp
            .cmp(&other.row.timestamp)
            .then(self.source.cmp(&other.source))
    }
}

/// Iterate over multiple multi-aggregation row iterators, returning the rows in timestamp
/// order. Structural clone of `MultiSeriesSampleIter` — including its correctness argument:
/// the heap holds the next unread row of **every** live iterator, and emitting one refills
/// from that same iterator, so its top really is the global minimum. See the sibling for
/// the out-of-order bug that arose from loading prefixes instead.
pub struct MultiSeriesRowIter<T: Iterator<Item = MultiSample>> {
    heap: BinaryHeap<Reverse<RowEntry>>,
    inner: Vec<T>,
    primed: bool,
}

impl<T: Iterator<Item = MultiSample>> MultiSeriesRowIter<T> {
    pub fn new(list: Vec<T>) -> Self {
        let len = list.len();
        Self {
            inner: list,
            heap: BinaryHeap::with_capacity(len),
            primed: false,
        }
    }

    /// Seed the heap with the first row of every iterator.
    fn prime(&mut self) {
        for source in 0..self.inner.len() {
            if let Some(row) = self.inner[source].next() {
                self.heap.push(Reverse(RowEntry { row, source }));
            }
        }
        self.primed = true;
    }
}

impl<T: Iterator<Item = MultiSample>> Iterator for MultiSeriesRowIter<T> {
    type Item = MultiSample;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.primed {
            self.prime();
        }

        let mut top = self.heap.peek_mut()?;
        let source = top.0.source;
        match self.inner[source].next() {
            Some(row) => Some(std::mem::replace(&mut top.0.row, row)),
            None => Some(PeekMut::pop(top).0.row),
        }
    }
}

#[cfg(test)]
mod tests {
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
        let iter = MultiSeriesRowIter::new(vec![a.into_iter(), b.into_iter()]);

        let timestamps: Vec<i64> = iter.map(|r| r.timestamp).collect();
        assert_eq!(timestamps, vec![1, 2, 3, 4]);
    }

    #[test]
    fn preserves_equal_timestamps_and_values() {
        let a = vec![row(1, &[1.0]), row(2, &[2.0])];
        let b = vec![row(2, &[9.0]), row(3, &[3.0])];
        let rows: Vec<MultiSample> =
            MultiSeriesRowIter::new(vec![a.into_iter(), b.into_iter()]).collect();

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
        let mut iter: MultiSeriesRowIter<std::vec::IntoIter<MultiSample>> =
            MultiSeriesRowIter::new(vec![]);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn values_preserved() {
        let a = vec![MultiSample {
            timestamp: 5,
            values: smallvec![1.5, 2.5, 3.5],
        }];
        let rows: Vec<MultiSample> = MultiSeriesRowIter::new(vec![a.into_iter()]).collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values.as_slice(), &[1.5, 2.5, 3.5]);
    }

    /// See the sibling test on `MultiSeriesSampleIter`: reducers rely on this order.
    #[test]
    fn equal_timestamps_come_out_in_source_order() {
        let sources = vec![
            vec![row(1, &[30.0])],
            vec![row(1, &[10.0])],
            vec![row(1, &[20.0])],
        ];
        let rows: Vec<MultiSample> =
            MultiSeriesRowIter::new(sources.into_iter().map(|s| s.into_iter()).collect()).collect();

        assert_eq!(
            rows,
            vec![row(1, &[30.0]), row(1, &[10.0]), row(1, &[20.0])]
        );
    }

    #[test]
    fn a_source_repeating_a_timestamp_stays_ahead_of_later_sources() {
        let a = vec![row(5, &[1.0]), row(5, &[2.0])];
        let b = vec![row(5, &[3.0])];
        let rows: Vec<MultiSample> =
            MultiSeriesRowIter::new(vec![a.into_iter(), b.into_iter()]).collect();

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
            MultiSeriesRowIter::new(sources.into_iter().map(|s| s.into_iter()).collect()).collect();

        assert_eq!(merged, expected);
    }
}
