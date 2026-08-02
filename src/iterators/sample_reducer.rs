use crate::aggregators::{AggregationHandler, Aggregator};
use crate::common::Sample;

/// Iterator that groups samples by timestamp
pub struct SampleReducer<I>
where
    I: Iterator<Item = Sample>,
{
    inner: I,
    buffer: Option<Sample>,
    aggregator: Aggregator,
    has_samples: bool,
    done: bool,
}

impl<I> SampleReducer<I>
where
    I: Iterator<Item = Sample>,
{
    pub fn new(iter: I, aggregator: Aggregator) -> Self {
        Self {
            inner: iter,
            buffer: None,
            done: false,
            has_samples: false,
            aggregator,
        }
    }

    fn finalize_group(&mut self, timestamp: i64) -> Sample {
        let value = if self.has_samples {
            // Finalize aggregator to get the aggregated value
            AggregationHandler::finalize(&mut self.aggregator)
        } else {
            // The aggregator accepted nothing from this group — every value was NaN, which
            // EMPTY makes routine, since the per-series fill contributes a NaN to the reduce.
            // Tallies report 0 for that; everything else reports NaN.
            self.aggregator.empty_group_value()
        };

        self.has_samples = false;

        Sample { timestamp, value }
    }
}

impl<I> Iterator for SampleReducer<I>
where
    I: Iterator<Item = Sample>,
{
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        // Get the first sample if the buffer is empty
        if self.buffer.is_none() {
            self.buffer = self.inner.next();
            if self.buffer.is_none() {
                self.done = true;
                return None;
            }
        }

        let (current_timestamp, current_value) = {
            let sample = self.buffer.as_ref().unwrap();
            (sample.timestamp, sample.value)
        };

        // Start a new group: reset the aggregator and include the first sample if it's not NaN.
        AggregationHandler::reset(&mut self.aggregator);
        self.has_samples =
            AggregationHandler::update(&mut self.aggregator, current_timestamp, current_value);

        // Continue consuming samples with the same timestamp
        loop {
            // Get the next sample
            let next_sample = self.inner.next();

            match next_sample {
                Some(sample) if sample.timestamp == current_timestamp => {
                    // Only aggregate non-NaN samples; still track "all_nans" for the group.
                    if AggregationHandler::update(
                        &mut self.aggregator,
                        sample.timestamp,
                        sample.value,
                    ) {
                        self.has_samples = true;
                    }
                    // Update the buffer with this sample
                    self.buffer = Some(sample);
                }
                Some(sample) => {
                    // Different timestamp: buffer the next sample for the next iteration
                    self.buffer = Some(sample);

                    // Emit current group
                    let result = self.finalize_group(current_timestamp);
                    return Some(result);
                }
                None => {
                    // No more samples, emit the current group and finish
                    self.buffer = None;
                    self.done = true;
                    let result = self.finalize_group(current_timestamp);
                    return Some(result);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregators::{AggregationType, Aggregator};

    fn reduce_using_sum(samples: Vec<Sample>) -> Vec<Sample> {
        let aggregator: Aggregator = AggregationType::Sum.into();
        SampleReducer::new(samples.into_iter(), aggregator).collect()
    }

    #[test]
    fn groups_samples_by_timestamp_using_sum() {
        let samples = vec![
            Sample {
                timestamp: 1,
                value: 10.0,
            },
            Sample {
                timestamp: 1,
                value: 15.0,
            },
            Sample {
                timestamp: 2,
                value: 5.0,
            },
            Sample {
                timestamp: 2,
                value: 20.0,
            },
            Sample {
                timestamp: 3,
                value: 7.0,
            },
        ];

        let result = reduce_using_sum(samples);

        assert_eq!(
            result,
            vec![
                Sample {
                    timestamp: 1,
                    value: 25.0
                },
                Sample {
                    timestamp: 2,
                    value: 25.0
                },
                Sample {
                    timestamp: 3,
                    value: 7.0
                },
            ]
        );
    }

    #[test]
    fn returns_nan_when_entire_group_is_nan() {
        let samples = vec![
            Sample {
                timestamp: 1,
                value: f64::NAN,
            },
            Sample {
                timestamp: 1,
                value: f64::NAN,
            },
            Sample {
                timestamp: 2,
                value: 5.0,
            },
        ];

        let result = reduce_using_sum(samples);

        assert_eq!(result.len(), 2);
        assert!(result[0].value.is_nan());
        assert_eq!(
            result[1],
            Sample {
                timestamp: 2,
                value: 5.0
            }
        );
    }

    #[test]
    fn count_reduces_an_all_nan_group_to_zero() {
        // A tally of nothing is 0, not "undefined". EMPTY makes this routine: the per-series
        // fill contributes a NaN to every reduce, so a bucket no series covered arrives here
        // as an all-NaN group. RedisTimeSeries reports 0 for it under `REDUCE count`.
        let aggregator: Aggregator = AggregationType::Count.into();
        let samples = vec![
            Sample {
                timestamp: 1,
                value: f64::NAN,
            },
            Sample {
                timestamp: 1,
                value: f64::NAN,
            },
            Sample {
                timestamp: 2,
                value: 5.0,
            },
        ];

        let result: Vec<Sample> = SampleReducer::new(samples.into_iter(), aggregator).collect();

        assert_eq!(
            result,
            vec![
                Sample {
                    timestamp: 1,
                    value: 0.0
                },
                Sample {
                    timestamp: 2,
                    value: 1.0
                },
            ]
        );
    }

    #[test]
    fn all_nan_group_at_end_of_stream_agrees_with_mid_stream() {
        // The final group takes a separate exit path in these reducers; it must not disagree
        // with the mid-stream branch. `sum` reports NaN in both, `count` reports 0 in both.
        for (aggregation, expected_is_nan) in [
            (AggregationType::Sum, true),
            (AggregationType::Count, false),
        ] {
            let samples = vec![
                Sample {
                    timestamp: 1,
                    value: f64::NAN,
                },
                Sample {
                    timestamp: 2,
                    value: f64::NAN,
                },
            ];
            let aggregator: Aggregator = aggregation.into();
            let result: Vec<Sample> = SampleReducer::new(samples.into_iter(), aggregator).collect();

            assert_eq!(result.len(), 2, "{aggregation:?}");
            assert_eq!(
                result[0].value.is_nan(),
                expected_is_nan,
                "mid-stream group for {aggregation:?}"
            );
            assert_eq!(
                result[1].value.is_nan(),
                expected_is_nan,
                "final group for {aggregation:?}"
            );
            if !expected_is_nan {
                assert_eq!(result[0].value, 0.0);
                assert_eq!(result[1].value, 0.0);
            }
        }
    }

    #[test]
    fn includes_first_sample_in_aggregation() {
        // Regression test: ensure the reducer aggregates the first sample of a group.
        // If the first sample is not fed into the aggregator, sums will be wrong.
        let samples = vec![
            Sample {
                timestamp: 1,
                value: 100.0,
            }, // only sample at ts=1
            Sample {
                timestamp: 2,
                value: 95.0,
            }, // first sample for ts=2
            Sample {
                timestamp: 2,
                value: 55.0,
            }, // second sample for ts=2
        ];

        let result = reduce_using_sum(samples);

        // ts=1 -> 100
        // ts=2 -> 95 + 55 = 150
        assert_eq!(
            result,
            vec![
                Sample {
                    timestamp: 1,
                    value: 100.0
                },
                Sample {
                    timestamp: 2,
                    value: 150.0
                },
            ]
        );
    }
}
