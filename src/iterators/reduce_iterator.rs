use crate::aggregators::{AggregationHandler, Aggregator};
use crate::common::Sample;

/// Perform the GROUP BY REDUCE operation on the samples.
pub fn group_reduce(samples: impl Iterator<Item = Sample>, aggregation: Aggregator) -> Vec<Sample> {
    ReduceIterator::new(samples, aggregation).collect()
}

pub struct ReduceIterator<I: Iterator<Item = Sample>> {
    iter: I,
    is_init: bool,
    aggregator: Aggregator,
    current_sample: Option<Sample>,
}

impl<I: Iterator<Item = Sample>> ReduceIterator<I> {
    pub fn new(iter: I, aggregator: Aggregator) -> Self {
        Self {
            iter,
            aggregator,
            current_sample: None,
            is_init: false,
        }
    }
}

impl<I: Iterator<Item = Sample>> Iterator for ReduceIterator<I> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        let current = if self.is_init {
            self.current_sample.take()?
        } else {
            // lazy initialization
            self.is_init = true;
            self.iter.next()?
        };

        let mut has_samples = self.aggregator.update(current.timestamp, current.value);

        for next in self.iter.by_ref() {
            if next.timestamp == current.timestamp {
                if self.aggregator.update(next.timestamp, next.value) {
                    has_samples = true;
                }
            } else {
                // Finalize the current group
                let value = if has_samples {
                    AggregationHandler::finalize(&mut self.aggregator)
                } else {
                    self.aggregator.empty_group_value()
                };

                AggregationHandler::reset(&mut self.aggregator);
                let result = Sample {
                    timestamp: current.timestamp,
                    value,
                };

                // Prepare for the next group
                self.current_sample = Some(next);
                return Some(result);
            }
        }

        // Finalize the last group when the inner iterator is exhausted
        let value = if has_samples {
            AggregationHandler::finalize(&mut self.aggregator)
        } else {
            // Was `empty_value()`, which disagreed with the mid-stream branch above for `sum`:
            // the last group of an all-NaN run reported 0 while every earlier one reported NaN.
            self.aggregator.empty_group_value()
        };

        Some(Sample {
            timestamp: current.timestamp,
            value,
        })
    }
}
