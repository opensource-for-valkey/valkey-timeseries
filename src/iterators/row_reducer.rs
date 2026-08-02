use crate::aggregators::{AggregationHandler, Aggregator};
use crate::common::MultiSample;
use smallvec::SmallVec;

/// Column-wise GROUPBY/REDUCE over multi-aggregation rows: groups rows by
/// timestamp and reduces each aggregation column independently with its own
/// clone of the REDUCE aggregator. NaN inputs are skipped per column exactly
/// as `SampleReducer` does; an all-NaN column yields whatever the aggregator
/// reports for a group it accepted nothing from — 0 for the tallies, NaN for
/// the rest (see [`Aggregator::empty_group_value`]). A series that produced no
/// row for a timestamp simply contributes nothing to any column.
pub struct RowReducer<I>
where
    I: Iterator<Item = MultiSample>,
{
    inner: I,
    buffer: Option<MultiSample>,
    /// One reducer per aggregation column.
    aggregators: SmallVec<[Aggregator; 4]>,
    has_samples: SmallVec<[bool; 4]>,
    done: bool,
}

impl<I> RowReducer<I>
where
    I: Iterator<Item = MultiSample>,
{
    pub fn new(iter: I, aggregator: Aggregator, columns: usize) -> Self {
        Self {
            inner: iter,
            buffer: None,
            aggregators: (0..columns).map(|_| aggregator.clone()).collect(),
            has_samples: (0..columns).map(|_| false).collect(),
            done: false,
        }
    }

    fn update(&mut self, row: &MultiSample) {
        debug_assert_eq!(row.values.len(), self.aggregators.len());
        for ((aggregator, has_samples), value) in self
            .aggregators
            .iter_mut()
            .zip(self.has_samples.iter_mut())
            .zip(row.values.iter())
        {
            if aggregator.update(row.timestamp, *value) {
                *has_samples = true;
            }
        }
    }

    fn finalize_group(&mut self, timestamp: i64) -> MultiSample {
        let values = self
            .aggregators
            .iter_mut()
            .zip(self.has_samples.iter_mut())
            .map(|(aggregator, has_samples)| {
                let value = if *has_samples {
                    AggregationHandler::finalize(aggregator)
                } else {
                    aggregator.empty_group_value()
                };
                AggregationHandler::reset(aggregator);
                *has_samples = false;
                value
            })
            .collect();

        MultiSample { timestamp, values }
    }
}

impl<I> Iterator for RowReducer<I>
where
    I: Iterator<Item = MultiSample>,
{
    type Item = MultiSample;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        let current = match self.buffer.take() {
            Some(row) => row,
            None => match self.inner.next() {
                Some(row) => row,
                None => {
                    self.done = true;
                    return None;
                }
            },
        };

        let current_timestamp = current.timestamp;
        self.update(&current);

        loop {
            match self.inner.next() {
                Some(row) if row.timestamp == current_timestamp => {
                    self.update(&row);
                }
                Some(row) => {
                    self.buffer = Some(row);
                    return Some(self.finalize_group(current_timestamp));
                }
                None => {
                    self.done = true;
                    return Some(self.finalize_group(current_timestamp));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregators::AggregationType;

    fn row(ts: i64, values: &[f64]) -> MultiSample {
        MultiSample {
            timestamp: ts,
            values: values.iter().copied().collect(),
        }
    }

    fn reduce(rows: Vec<MultiSample>, ty: AggregationType, columns: usize) -> Vec<MultiSample> {
        RowReducer::new(rows.into_iter(), ty.into(), columns).collect()
    }

    #[test]
    fn reduces_columns_independently() {
        let rows = vec![
            row(1, &[1.0, 10.0]),
            row(1, &[3.0, 20.0]),
            row(2, &[5.0, 30.0]),
        ];

        let result = reduce(rows, AggregationType::Sum, 2);

        assert_eq!(result, vec![row(1, &[4.0, 30.0]), row(2, &[5.0, 30.0])]);
    }

    #[test]
    fn all_nan_column_yields_nan() {
        let rows = vec![row(1, &[f64::NAN, 2.0]), row(1, &[f64::NAN, 3.0])];

        let result = reduce(rows, AggregationType::Sum, 2);

        assert_eq!(result.len(), 1);
        assert!(result[0].values[0].is_nan());
        assert_eq!(result[0].values[1], 5.0);
    }

    #[test]
    fn single_row_groups_pass_through() {
        let rows = vec![row(1, &[1.0]), row(2, &[2.0]), row(3, &[3.0])];
        let result = reduce(rows, AggregationType::Max, 1);
        assert_eq!(result, vec![row(1, &[1.0]), row(2, &[2.0]), row(3, &[3.0])]);
    }

    #[test]
    fn empty_input() {
        let result = reduce(vec![], AggregationType::Sum, 2);
        assert!(result.is_empty());
    }
}
