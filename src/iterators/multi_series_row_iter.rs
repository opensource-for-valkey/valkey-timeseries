use crate::common::MultiSample;
use min_max_heap::MinMaxHeap;
use std::cmp::Ordering;

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
/// from that same iterator, so `pop_min` really is the global minimum. See the sibling for
/// the out-of-order bug that arose from loading prefixes instead.
pub struct MultiSeriesRowIter<T: Iterator<Item = MultiSample>> {
    heap: MinMaxHeap<RowEntry>,
    inner: Vec<T>,
    primed: bool,
}

impl<T: Iterator<Item = MultiSample>> MultiSeriesRowIter<T> {
    pub fn new(list: Vec<T>) -> Self {
        let len = list.len();
        Self {
            inner: list,
            heap: MinMaxHeap::with_capacity(len),
            primed: false,
        }
    }

    /// Seed the heap with the first row of every iterator.
    fn prime(&mut self) {
        for source in 0..self.inner.len() {
            if let Some(row) = self.inner[source].next() {
                self.heap.push(RowEntry { row, source });
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

        let entry = self.heap.pop_min()?;
        if let Some(row) = self.inner[entry.source].next() {
            self.heap.push(RowEntry {
                row,
                source: entry.source,
            });
        }
        Some(entry.row)
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
}
