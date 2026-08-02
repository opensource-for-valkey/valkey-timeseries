use crate::common::{MultiSample, SampleValue};
use min_max_heap::MinMaxHeap;
use smallvec::{SmallVec, smallvec};
use std::cmp::Ordering;

/// A pending row tagged with the iterator it came from, so the merge can refill exactly that
/// iterator once the row is consumed, and knows which output columns to write it into.
struct Entry {
    row: MultiSample,
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
        // Timestamp first; `source` only breaks ties. Values (which may be NaN) never take
        // part in the comparison.
        self.row
            .timestamp
            .cmp(&other.row.timestamp)
            .then(self.source.cmp(&other.source))
    }
}

/// Outer-joins several ascending row streams on timestamp, emitting one row per distinct
/// timestamp with every source's values side by side.
///
/// Each source owns a fixed block of output columns (`widths[i]` of them, laid out in source
/// order), so a row's position in the reply identifies which input produced it. Sources that
/// have no row at an emitted timestamp leave their block as `NaN` — the outer join is what
/// distinguishes this from [`MultiSeriesRowIter`], which concatenates streams into one
/// timestamp-ordered sequence instead of widening them.
///
/// Backs TS.NRANGE, whose reply is exactly this pivot: raw mode gives every source one column,
/// `AGGREGATION` gives it one per aggregator.
///
/// The merge is the same k-way merge as [`MultiSeriesRowIter`] — the heap holds the next unread
/// row of *every* live source and emitting one refills from that same source, so `pop_min` is
/// the global minimum — with one addition: after taking the minimum, every other source sitting
/// at the same timestamp is drained into the same output row before it is emitted.
///
/// [`MultiSeriesRowIter`]: crate::iterators::MultiSeriesRowIter
pub struct PivotIter<T: Iterator<Item = MultiSample>> {
    heap: MinMaxHeap<Entry>,
    inner: Vec<T>,
    /// First output column of each source, i.e. the running sum of the widths.
    offsets: Vec<usize>,
    /// Columns owned by each source; a source's row is truncated to it.
    widths: Vec<usize>,
    /// Total row width, `widths.iter().sum()`.
    total_columns: usize,
    primed: bool,
}

impl<T: Iterator<Item = MultiSample>> PivotIter<T> {
    /// `widths[i]` is the number of output columns source `i` contributes; it must have one
    /// entry per source.
    pub fn new(sources: Vec<T>, widths: Vec<usize>) -> Self {
        debug_assert_eq!(
            sources.len(),
            widths.len(),
            "PivotIter needs one width per source"
        );
        let mut offsets = Vec::with_capacity(widths.len());
        let mut total_columns = 0usize;
        for width in widths.iter() {
            offsets.push(total_columns);
            total_columns += width;
        }
        Self {
            heap: MinMaxHeap::with_capacity(sources.len()),
            inner: sources,
            offsets,
            widths,
            total_columns,
            primed: false,
        }
    }

    /// Seed the heap with the first row of every source.
    fn prime(&mut self) {
        for source in 0..self.inner.len() {
            if let Some(row) = self.inner[source].next() {
                self.heap.push(Entry { row, source });
            }
        }
        self.primed = true;
    }

    /// Copy an entry's values into its column block and pull that source's next row.
    fn absorb(&mut self, entry: Entry, values: &mut [SampleValue]) {
        let start = self.offsets[entry.source];
        let width = self.widths[entry.source];
        // A source yields exactly `width` values per row (one per aggregator, or the single
        // raw sample value), but truncate rather than trust it: a short row leaves the rest of
        // the block NaN and a long one cannot spill into the neighbouring key's columns.
        let len = width.min(entry.row.values.len());
        values[start..start + len].copy_from_slice(&entry.row.values[..len]);

        if let Some(row) = self.inner[entry.source].next() {
            self.heap.push(Entry {
                row,
                source: entry.source,
            });
        }
    }
}

impl<T: Iterator<Item = MultiSample>> Iterator for PivotIter<T> {
    type Item = MultiSample;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.primed {
            self.prime();
        }

        let entry = self.heap.pop_min()?;
        let timestamp = entry.row.timestamp;

        let mut values: SmallVec<[SampleValue; 4]> = smallvec![f64::NAN; self.total_columns];
        self.absorb(entry, &mut values);

        // Every other source at this timestamp joins the same row. Refills happen inside
        // `absorb`, so a source that repeats a timestamp (which a series cannot do, but a
        // pipeline feeding this iterator might) is consumed here too rather than producing a
        // second row for a timestamp already emitted.
        while self
            .heap
            .peek_min()
            .is_some_and(|e| e.row.timestamp == timestamp)
        {
            let entry = self
                .heap
                .pop_min()
                .expect("peek_min matched, so pop_min cannot be empty");
            self.absorb(entry, &mut values);
        }

        Some(MultiSample { timestamp, values })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(ts: i64, values: &[f64]) -> MultiSample {
        MultiSample {
            timestamp: ts,
            values: values.iter().copied().collect(),
        }
    }

    /// `[(timestamp, values)]`, with NaN rendered as `None` so rows compare by value.
    fn collect(iter: impl Iterator<Item = MultiSample>) -> Vec<(i64, Vec<Option<f64>>)> {
        iter.map(|r| {
            (
                r.timestamp,
                r.values
                    .iter()
                    .map(|v| (!v.is_nan()).then_some(*v))
                    .collect(),
            )
        })
        .collect()
    }

    /// The join is an outer join: a timestamp produced by any source yields a row, and the
    /// sources that lack it report NaN.
    #[test]
    fn test_outer_join_fills_gaps_with_nan() {
        let a = vec![row(1000, &[10.0]), row(2000, &[12.0])];
        let b = vec![row(1000, &[20.0]), row(3000, &[25.0])];

        let iter = PivotIter::new(vec![a.into_iter(), b.into_iter()], vec![1, 1]);

        assert_eq!(
            collect(iter),
            vec![
                (1000, vec![Some(10.0), Some(20.0)]),
                (2000, vec![Some(12.0), None]),
                (3000, vec![None, Some(25.0)]),
            ]
        );
    }

    /// Column blocks are laid out in source order and sized by `widths`, so a multi-aggregation
    /// source keeps its values together and a gap blanks its whole block.
    #[test]
    fn test_column_blocks_follow_source_order() {
        let a = vec![row(1000, &[15.0, 20.0]), row(2000, &[30.0, 30.0])];
        let b = vec![row(1000, &[20.0])];

        let iter = PivotIter::new(vec![a.into_iter(), b.into_iter()], vec![2, 1]);

        assert_eq!(
            collect(iter),
            vec![
                (1000, vec![Some(15.0), Some(20.0), Some(20.0)]),
                (2000, vec![Some(30.0), Some(30.0), None]),
            ]
        );
    }

    /// The same key listed twice is two independent sources, each with its own column.
    #[test]
    fn test_repeated_source_gets_its_own_column() {
        let a = vec![row(1, &[1.0]), row(2, &[2.0])];
        let b = vec![row(1, &[1.0]), row(2, &[2.0])];

        let iter = PivotIter::new(vec![a.into_iter(), b.into_iter()], vec![1, 1]);

        assert_eq!(
            collect(iter),
            vec![
                (1, vec![Some(1.0), Some(1.0)]),
                (2, vec![Some(2.0), Some(2.0)]),
            ]
        );
    }

    /// A source that never yields still owns its columns, and an all-empty input yields nothing.
    #[test]
    fn test_empty_sources() {
        let a = vec![row(5, &[1.0])];
        let empty: Vec<MultiSample> = Vec::new();

        let iter = PivotIter::new(vec![a.into_iter(), empty.into_iter()], vec![1, 1]);
        assert_eq!(collect(iter), vec![(5, vec![Some(1.0), None])]);

        let all_empty: PivotIter<std::vec::IntoIter<MultiSample>> =
            PivotIter::new(vec![Vec::new().into_iter()], vec![1]);
        assert!(collect(all_empty).is_empty());

        let no_sources: PivotIter<std::vec::IntoIter<MultiSample>> =
            PivotIter::new(Vec::new(), Vec::new());
        assert!(collect(no_sources).is_empty());
    }

    /// A stored NaN is a value like any other: it occupies its column and is indistinguishable
    /// from the NaN written for a missing sample (which is what the TS.NRANGE reply documents).
    #[test]
    fn test_stored_nan_is_carried_through() {
        let a = vec![row(1, &[f64::NAN])];
        let b = vec![row(1, &[2.0])];

        let rows: Vec<MultiSample> =
            PivotIter::new(vec![a.into_iter(), b.into_iter()], vec![1, 1]).collect();

        assert_eq!(rows.len(), 1);
        assert!(rows[0].values[0].is_nan());
        assert_eq!(rows[0].values[1], 2.0);
    }

    /// Sources are merged, not concatenated: interleaved timestamps come out ascending
    /// regardless of which source they came from.
    #[test]
    fn test_interleaved_timestamps_are_ordered() {
        let a = vec![row(1, &[1.0]), row(4, &[4.0]), row(9, &[9.0])];
        let b = vec![row(2, &[2.0]), row(3, &[3.0])];
        let c = vec![row(5, &[5.0])];

        let iter = PivotIter::new(
            vec![a.into_iter(), b.into_iter(), c.into_iter()],
            vec![1, 1, 1],
        );

        let timestamps: Vec<i64> = iter.map(|r| r.timestamp).collect();
        assert_eq!(timestamps, vec![1, 2, 3, 4, 5, 9]);
    }
}
