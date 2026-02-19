use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::cmp::{max, min};
use std::collections::BinaryHeap;
use std::collections::HashSet;
use std::iter::Sum;
use thiserror::Error;

/// Keep behavior compatible with C++ logic where missing values are represented as NaN.
pub const NAN_VALUE: f64 = f64::NAN;
pub const MAX_TIMESTAMP: u64 = u64::MAX;

#[derive(Debug, Error)]
pub enum TsAggError {
    #[error("invalid bucket duration: must be > 0")]
    InvalidBucketDuration,
    #[error("invalid aggregation arguments: {0}")]
    InvalidArgs(&'static str),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct TSSample {
    pub ts: u64,
    pub v: f64,
}

impl Eq for TSSample {}

impl Ord for TSSample {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.ts.cmp(&other.ts).then_with(|| self.v.total_cmp(&other.v))
    }
}

impl PartialOrd for TSSample {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum TSAggregatorType {
    None,
    Avg,
    Sum,
    Min,
    Max,
    Range,
    Count,
    First,
    Last,
    StdP,
    StdS,
    VarP,
    VarS,
    Twa,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum BucketTimestampType {
    Start,
    End,
    Mid,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum GroupReducerType {
    None,
    Sum,
    Avg,
    Min,
    Max,
    Range,
    Count,
    StdP,
    StdS,
    VarP,
    VarS,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TSAggregator {
    pub kind: TSAggregatorType,
    pub bucket_duration: u64,
    pub alignment: u64,
}

impl TSAggregator {
    pub fn validate(&self) -> Result<(), TsAggError> {
        if self.kind != TSAggregatorType::None && self.bucket_duration == 0 {
            return Err(TsAggError::InvalidBucketDuration);
        }
        Ok(())
    }

    pub fn calculate_aligned_bucket_left(&self, ts: u64) -> u64 {
        let mut x = 0u64;
        if ts >= self.alignment {
            let diff = ts - self.alignment;
            let k = diff / self.bucket_duration;
            x = self.alignment.saturating_add(k.saturating_mul(self.bucket_duration));
        } else {
            let diff = self.alignment - ts;
            let m0 = diff / self.bucket_duration + u64::from(diff % self.bucket_duration != 0);
            if m0 <= self.alignment / self.bucket_duration {
                x = self.alignment - m0 * self.bucket_duration;
            }
        }
        x
    }

    pub fn calculate_aligned_bucket_right(&self, ts: u64) -> u64 {
        let mut x = MAX_TIMESTAMP;
        if ts < self.alignment {
            let diff = self.alignment - ts;
            let k = diff / self.bucket_duration;
            x = self.alignment - k * self.bucket_duration;
        } else {
            let diff = ts - self.alignment;
            let m0 = diff / self.bucket_duration + 1;
            if m0 <= (MAX_TIMESTAMP - self.alignment) / self.bucket_duration {
                x = self.alignment + m0 * self.bucket_duration;
            }
        }
        x
    }

    /// Splits samples into buckets based on the aggregator's configuration.
    pub fn split_samples_to_buckets(&self, samples: &[TSSample]) -> Vec<Vec<TSSample>> {
        let mut spans = Vec::new();
        if self.kind == TSAggregatorType::None || samples.is_empty() {
            return spans;
        }

        let start_bucket = self.calculate_aligned_bucket_left(samples.first().unwrap().ts);
        let end_bucket = self.calculate_aligned_bucket_left(samples.last().unwrap().ts);
        let bucket_count = ((end_bucket - start_bucket) / self.bucket_duration) + 1;
        spans.reserve(bucket_count as usize);

        let mut bucket_left = start_bucket;
        let mut i = 0usize;
        while i < samples.len() {
            let bucket_right = self.calculate_aligned_bucket_right(bucket_left);
            let mut bucket = Vec::new();
            while i < samples.len() && samples[i].ts < bucket_right {
                if samples[i].ts >= bucket_left {
                    bucket.push(samples[i]);
                }
                i += 1;
            }
            spans.push(bucket);
            bucket_left = bucket_right;
        }

        spans
    }

    pub fn get_bucket_by_timestamp(
        &self,
        samples: &[TSSample],
        ts: u64,
        less_than: u64,
    ) -> Vec<TSSample> {
        if self.kind == TSAggregatorType::None || samples.is_empty() {
            return vec![];
        }
        let start_bucket = self.calculate_aligned_bucket_left(ts);
        let end_bucket = min(self.calculate_aligned_bucket_right(ts), less_than);
        samples
            .iter()
            .copied()
            .filter(|s| s.ts >= start_bucket && s.ts < end_bucket)
            .collect()
    }

    /// C++ AggregateSamplesValue
    pub fn aggregate_samples_value(&self, samples: &[TSSample]) -> f64 {
        if samples.is_empty() {
            return NAN_VALUE;
        }
        let n = samples.len() as f64;
        match self.kind {
            TSAggregatorType::Avg => Reducer::sum(samples) / n,
            TSAggregatorType::Sum => Reducer::sum(samples),
            TSAggregatorType::Min => Reducer::min(samples),
            TSAggregatorType::Max => Reducer::max(samples),
            TSAggregatorType::Range => Reducer::range(samples),
            TSAggregatorType::Count => n,
            TSAggregatorType::First => samples.first().unwrap().v,
            TSAggregatorType::Last => samples.last().unwrap().v,
            TSAggregatorType::StdP => Reducer::std_p(samples),
            TSAggregatorType::StdS => Reducer::std_s(samples),
            TSAggregatorType::VarP => Reducer::var_p(samples),
            TSAggregatorType::VarS => Reducer::var_s(samples),
            TSAggregatorType::Twa => Reducer::area(samples),
            TSAggregatorType::None => NAN_VALUE,
        }
    }
}

pub struct Reducer;

impl Reducer {
    pub fn sum(samples: &[TSSample]) -> f64 {
        samples.iter().map(|s| s.v).sum::<f64>()
    }

    pub fn square_sum(samples: &[TSSample]) -> f64 {
        samples.iter().map(|s| s.v * s.v).sum::<f64>()
    }

    pub fn min(samples: &[TSSample]) -> f64 {
        samples
            .iter()
            .min_by(|a, b| a.v.total_cmp(&b.v))
            .map(|s| s.v)
            .unwrap_or(NAN_VALUE)
    }

    pub fn max(samples: &[TSSample]) -> f64 {
        samples
            .iter()
            .max_by(|a, b| a.v.total_cmp(&b.v))
            .map(|s| s.v)
            .unwrap_or(NAN_VALUE)
    }

    pub fn var_p(samples: &[TSSample]) -> f64 {
        if samples.is_empty() {
            return NAN_VALUE;
        }
        let n = samples.len() as f64;
        let sum = Self::sum(samples);
        let sq = Self::square_sum(samples);
        (sq - (sum * sum / n)) / n
    }

    pub fn var_s(samples: &[TSSample]) -> f64 {
        if samples.len() <= 1 {
            return 0.0;
        }
        let n = samples.len() as f64;
        Self::var_p(samples) * n / (n - 1.0)
    }

    pub fn std_p(samples: &[TSSample]) -> f64 {
        Self::var_p(samples).sqrt()
    }

    pub fn std_s(samples: &[TSSample]) -> f64 {
        Self::var_s(samples).sqrt()
    }

    pub fn range(samples: &[TSSample]) -> f64 {
        if samples.is_empty() {
            return 0.0;
        }
        let min = Self::min(samples);
        let max = Self::max(samples);
        max - min
    }

    /// Trapezoid integration over sorted samples.
    pub fn area(samples: &[TSSample]) -> f64 {
        let mut result = 0.0;
        for i in 1..samples.len() {
            let t_diff = (samples[i].ts - samples[i - 1].ts) as f64;
            result += (t_diff * samples[i - 1].v)
                + (t_diff * (samples[i].v - samples[i - 1].v) * 0.5);
        }
        result
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct TWABounds {
    pub prev_sample: TSSample,
    pub next_sample: TSSample,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TSRangeOption {
    pub start_ts: u64,
    pub end_ts: u64,
    pub count_limit: usize,
    pub is_return_empty: bool,
    pub bucket_timestamp_type: BucketTimestampType,
    pub aggregator: TSAggregator,
    pub filter_by_ts: HashSet<u64>,
    pub filter_by_value: Option<(f64, f64)>,
}

impl Default for TSRangeOption {
    fn default() -> Self {
        Self {
            start_ts: 0,
            end_ts: MAX_TIMESTAMP,
            count_limit: 0,
            is_return_empty: false,
            bucket_timestamp_type: BucketTimestampType::Start,
            aggregator: TSAggregator {
                kind: TSAggregatorType::None,
                bucket_duration: 1,
                alignment: 0,
            },
            filter_by_ts: HashSet::new(),
            filter_by_value: None,
        }
    }
}

/// Equivalent to linear interpolation lambda.
fn interpolate_sample(left_nb: TSSample, ts: u64, right_nb: TSSample) -> TSSample {
    let y_diff = right_nb.v - left_nb.v;
    let x_diff = (right_nb.ts - left_nb.ts) as f64;
    let x_diff_prime = (ts - left_nb.ts) as f64;
    let y_diff_prime = (x_diff_prime * y_diff) / x_diff;
    TSSample {
        ts,
        v: y_diff_prime + left_nb.v,
    }
}

/// Equivalent to empty_bucket_twa lambda.
fn empty_bucket_twa(left_nb: TSSample, bucket_left: u64, bucket_right: u64, right_nb: TSSample) -> f64 {
    let left = interpolate_sample(left_nb, bucket_left, right_nb);
    let right = interpolate_sample(left_nb, bucket_right, right_nb);
    Reducer::area(&[left, right]) / (bucket_right - bucket_left) as f64
}

/// Translation of AggregateSamplesByRangeOption (aggregation + grouping by bucket).
pub fn aggregate_samples_by_range_option(
    mut samples: Vec<TSSample>,
    option: &TSRangeOption,
    twa_bounds: TWABounds,
) -> Result<Vec<TSSample>, TsAggError> {
    let aggregator = &option.aggregator;
    aggregator.validate()?;

    let get_bucket_ts = |left: u64| -> u64 {
        match option.bucket_timestamp_type {
            BucketTimestampType::Start => left,
            BucketTimestampType::End => left + aggregator.bucket_duration,
            BucketTimestampType::Mid => left + aggregator.bucket_duration / 2,
        }
    };

    let is_twa_aggregator = aggregator.kind == TSAggregatorType::Twa;
    let mut prev_sample = twa_bounds.prev_sample;
    let mut next_sample = twa_bounds.next_sample;
    let mut prev_available = false;
    let mut next_available = false;

    if is_twa_aggregator {
        let discard_boundaries = !option.filter_by_ts.is_empty() || option.filter_by_value.is_some();
        prev_available = if discard_boundaries {
            false
        } else {
            !samples.is_empty() && samples.first().unwrap().ts != prev_sample.ts
        };
        next_available = if discard_boundaries {
            false
        } else {
            !samples.is_empty() && samples.last().unwrap().ts != next_sample.ts
        };
    }

    if is_twa_aggregator && option.is_return_empty && samples.is_empty() {
        let early_return = prev_sample.ts == MAX_TIMESTAMP
            || next_sample.ts == MAX_TIMESTAMP
            || prev_sample.ts == next_sample.ts;
            
        if early_return {
            return Ok(samples);
        }

        let n_buckets_estimate = (option.end_ts - option.start_ts) / aggregator.bucket_duration;
        let mut res = Vec::with_capacity(n_buckets_estimate as usize + 1);
        let mut bucket_left = aggregator.calculate_aligned_bucket_left(option.start_ts);
        let mut bucket_right = aggregator.calculate_aligned_bucket_right(bucket_left);

        for _ in 0..n_buckets_estimate {
            bucket_left = max(bucket_left, option.start_ts);
            bucket_right = min(bucket_right, option.end_ts);

            res.push(TSSample {
                ts: bucket_left,
                v: empty_bucket_twa(prev_sample, bucket_left, bucket_right, next_sample),
            });

            bucket_left = bucket_right;
            bucket_right = aggregator.calculate_aligned_bucket_right(bucket_left);
        }

        let v = if bucket_left == option.end_ts {
            interpolate_sample(prev_sample, option.end_ts, next_sample).v
        } else {
            empty_bucket_twa(prev_sample, bucket_left, bucket_right, next_sample)
        };
        res.push(TSSample { ts: bucket_left, v });
        return Ok(res);
    } else if aggregator.kind == TSAggregatorType::None || samples.is_empty() {
        return Ok(samples);
    }

    let spans = aggregator.split_samples_to_buckets(&samples);
    let mut res = Vec::with_capacity(spans.len());

    // neighbor lookup table, equivalent to C++ vector<pair<TSSample,TSSample>>
    let mut neighbors = vec![
        (
            TSSample {
                ts: MAX_TIMESTAMP,
                v: NAN_VALUE,
            },
            TSSample {
                ts: MAX_TIMESTAMP,
                v: NAN_VALUE,
            },
        );
        spans.len()
    ];

    if !spans.is_empty() {
        let sz_last = spans.len() - 1;
        neighbors[0].0 = prev_sample;
        neighbors[sz_last].1 = next_sample;

        if spans.len() > 1 {
            let right0 = (1..spans.len()).find(|&i| !spans[i].is_empty()).unwrap_or(sz_last);
            let leftn = (0..sz_last).rev().find(|&i| !spans[i].is_empty()).unwrap_or(0);

            neighbors[0].1 = spans[right0].first().copied().unwrap_or(next_sample);
            neighbors[sz_last].0 = spans[leftn].last().copied().unwrap_or(prev_sample);
        }

        let mut rev_idx = sz_last.saturating_sub(1);
        for i in 1..spans.len().saturating_sub(1) {
            neighbors[i].0 = if spans[i - 1].is_empty() {
                neighbors[i - 1].0
            } else {
                *spans[i - 1].last().unwrap()
            };
            neighbors[rev_idx].1 = if spans[rev_idx + 1].is_empty() {
                neighbors[rev_idx + 1].1
            } else {
                *spans[rev_idx + 1].first().unwrap()
            };
            rev_idx = rev_idx.saturating_sub(1);
        }
    }

    let mut bucket_left = aggregator.calculate_aligned_bucket_left(samples.first().unwrap().ts);

    for (i, span) in spans.iter().enumerate() {
        if option.count_limit > 0 && res.len() >= option.count_limit {
            break;
        }

        if i != 0 {
            bucket_left = aggregator.calculate_aligned_bucket_right(bucket_left);
        }

        let mut out = TSSample {
            ts: get_bucket_ts(bucket_left),
            v: NAN_VALUE,
        };

        if option.is_return_empty && span.is_empty() {
            out.v = match aggregator.kind {
                TSAggregatorType::Sum | TSAggregatorType::Count => 0.0,
                TSAggregatorType::Twa => {
                    if (i == 0 && !prev_available) || (i == spans.len() - 1 && !next_available) {
                        NAN_VALUE
                    } else {
                        let bucket_right = aggregator.calculate_aligned_bucket_right(bucket_left);
                        empty_bucket_twa(neighbors[i].0, bucket_left, bucket_right, neighbors[i].1)
                    }
                }
                TSAggregatorType::Last => {
                    if i == 0 || spans[i - 1].is_empty() {
                        NAN_VALUE
                    } else {
                        spans[i].last().map(|s| s.v).unwrap_or(NAN_VALUE)
                    }
                }
                _ => NAN_VALUE,
            };
            res.push(out);
            continue;
        }

        if span.is_empty() {
            continue;
        }

        out.v = aggregator.aggregate_samples_value(span);

        if is_twa_aggregator {
            let mut bucket_right = aggregator.calculate_aligned_bucket_right(bucket_left);
            let mut cut_left = max(bucket_left, option.start_ts);
            bucket_right = min(bucket_right, option.end_ts);

            let front_available = span.first().unwrap().ts != cut_left && neighbors[i].0.ts <= cut_left;
            let back_available = span.last().unwrap().ts != bucket_right && bucket_right <= neighbors[i].1.ts;

            let mut area = 0.0;
            let mut l = span.first().unwrap().ts;
            let mut r = span.last().unwrap().ts;

            if front_available {
                let left_sample = interpolate_sample(neighbors[i].0, cut_left, *span.first().unwrap());
                area += Reducer::area(&[left_sample, *span.first().unwrap()]);
                l = cut_left;
            }
            if back_available {
                let right_sample = interpolate_sample(*span.last().unwrap(), bucket_right, neighbors[i].1);
                area += Reducer::area(&[*span.last().unwrap(), right_sample]);
                r = bucket_right;
            }

            if !front_available && !back_available && span.len() == 1 {
                area += span[0].v;
            }

            out.v = (out.v + area) / ((r - l) as f64).max(1.0);
        }

        res.push(out);
    }

    Ok(res)
}

/// Equivalent to C++ GroupSamplesAndReduce
pub fn group_samples_and_reduce(
    all_samples: &[Vec<TSSample>],
    reducer_type: GroupReducerType,
) -> Vec<TSSample> {
    if reducer_type == GroupReducerType::None {
        return vec![];
    }

    #[derive(Clone, Copy, Debug)]
    struct SamplePtr {
        sample: TSSample,
        vector_idx: usize,
        sample_idx: usize,
    }

    impl Eq for SamplePtr {}
    impl PartialEq for SamplePtr {
        fn eq(&self, other: &Self) -> bool {
            self.sample.ts == other.sample.ts
                && self.vector_idx == other.vector_idx
                && self.sample_idx == other.sample_idx
        }
    }
    impl Ord for SamplePtr {
        // reverse for min-heap behavior via BinaryHeap max-heap
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            other.sample.ts.cmp(&self.sample.ts)
        }
    }
    impl PartialOrd for SamplePtr {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }

    let mut heap = BinaryHeap::new();
    for (i, vec_) in all_samples.iter().enumerate() {
        if let Some(first) = vec_.first().copied() {
            heap.push(SamplePtr {
                sample: first,
                vector_idx: i,
                sample_idx: 0,
            });
        }
    }

    if heap.is_empty() {
        return vec![];
    }

    let reduce = |samples: &[TSSample]| -> f64 {
        let n = samples.len() as f64;
        match reducer_type {
            GroupReducerType::Sum => Reducer::sum(samples),
            GroupReducerType::Avg => {
                if samples.is_empty() {
                    0.0
                } else {
                    Reducer::sum(samples) / n
                }
            }
            GroupReducerType::Min => Reducer::min(samples),
            GroupReducerType::Max => Reducer::max(samples),
            GroupReducerType::Range => Reducer::range(samples),
            GroupReducerType::Count => n,
            GroupReducerType::StdP => Reducer::std_p(samples),
            GroupReducerType::StdS => Reducer::std_s(samples),
            GroupReducerType::VarP => Reducer::var_p(samples),
            GroupReducerType::VarS => Reducer::var_s(samples),
            GroupReducerType::None => 0.0,
        }
    };

    let mut result = Vec::new();
    let mut current_group: SmallVec<[TSSample; 8]> = SmallVec::new();

    while let Some(top) = heap.pop() {
        if !current_group.is_empty() && top.sample.ts != current_group.last().unwrap().ts {
            let group_ts = current_group.last().unwrap().ts;
            let reduced = reduce(&current_group);
            result.push(TSSample {
                ts: group_ts,
                v: reduced,
            });
            current_group.clear();
        }

        current_group.push(top.sample);

        let next_idx = top.sample_idx + 1;
        if next_idx < all_samples[top.vector_idx].len() {
            heap.push(SamplePtr {
                sample: all_samples[top.vector_idx][next_idx],
                vector_idx: top.vector_idx,
                sample_idx: next_idx,
            });
        }
    }

    if !current_group.is_empty() {
        let group_ts = current_group.last().unwrap().ts;
        let reduced = reduce(&current_group);
        result.push(TSSample {
            ts: group_ts,
            v: reduced,
        });
    }

    result
}