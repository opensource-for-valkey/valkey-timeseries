//! Mergeable (decomposable) reducer state for shard-side GROUPBY/REDUCE
//! push-down in MRANGE fanout.
//!
//! A shard pre-reduces its local members of a group per bucket timestamp into
//! a `PartialState` instead of a finalized value. The coordinator merges the
//! partial states of all shards per bucket and finalizes them. For every
//! decomposable reducer, `finalize(merge(states...))` equals the single-node
//! `SampleReducer` result over the concatenated inputs (up to floating-point
//! summation order).
//!
//! `increase` and `irate` reduce over same-timestamp values in merge order —
//! order-sensitive across series — so they are not decomposable and
//! [`PartialReducer::for_config`] returns `None` for them; such queries fall
//! back to per-series bucket transport (Phase 1).

use crate::aggregators::AggregationType;
use crate::aggregators::kahan::KahanSum;
use crate::common::{MultiSample, Sample, Timestamp};
use crate::series::request_types::{AggregatorConfig, ValueComparisonFilter};
use smallvec::SmallVec;

/// Which decomposable reducer a `PartialState` belongs to. Determines the
/// meaning of the accumulator fields and the merge/finalize formulas.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartialReducerKind {
    /// acc1 = sum of accepted values
    Sum,
    /// Like Sum, but zero accepted values finalizes to 0 instead of NaN
    /// (sumif's update always "succeeds", so its empty_value 0.0 applies).
    SumIf,
    /// count only (also countall, countnan — acceptance differs).
    /// Zero accepted values finalizes to 0: a tally of nothing is 0.
    Count,
    /// Like Count, but zero accepted values finalizes to 0 instead of NaN.
    CountIf,
    /// acc1 = min
    Min,
    /// acc1 = max
    Max,
    /// acc1 = min, acc2 = max; finalize max - min
    Range,
    /// acc1 = sum; finalize sum / count
    Avg,
    /// acc1 = running mean, acc2 = M2 (Welford); finalize per variant
    StdP,
    StdS,
    VarP,
    VarS,
    /// acc1 = value, ts = contributing timestamp; keep min ts
    First,
    /// acc1 = value, ts = contributing timestamp; keep max ts
    Last,
}

/// Mergeable accumulator for one bucket timestamp.
/// `count == 0` means no value was accepted (all rejected/NaN). The bucket then
/// finalizes to 0 for the tallying kinds and NaN for the rest, matching
/// `SampleReducer` via [`Aggregator::empty_group_value`].
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct PartialState {
    pub count: u64,
    pub acc1: f64,
    pub acc2: f64,
    /// Neumaier compensation for `acc1` (Sum/SumIf/Avg only, 0.0 elsewhere).
    /// Folded into the finalized value; carried separately so merges stay
    /// compensated. A partial without one (e.g. an older peer) is exactly
    /// "a sum with no accumulated error".
    pub acc1_c: f64,
    pub ts: Timestamp,
}

/// Accumulates one timestamp group shard-side. Created per group via
/// [`PartialReducer::for_config`]; `None` means the reducer is not
/// decomposable and the query must fall back to Phase 1 transport.
#[derive(Debug, Clone)]
pub struct PartialReducer {
    kind: PartialReducerKind,
    /// CONDITION filter for countif/sumif (count/sum with CONDITION are
    /// mapped to their `*if` variants at parse time).
    filter: Option<ValueComparisonFilter>,
    /// When true, NaN values are accepted (countall) or exclusively accepted
    /// (countnan) instead of rejected.
    acceptance: NanAcceptance,
    state: PartialState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NanAcceptance {
    /// NaN rejected (every reducer except countall/countnan)
    Reject,
    /// All values accepted (countall)
    All,
    /// Only NaN accepted (countnan)
    NanOnly,
}

impl PartialReducer {
    /// Returns a reducer for `config` if it is decomposable, `None` otherwise.
    /// This is also the coordinator's eligibility test for setting
    /// `apply_group_reduce`.
    pub fn for_config(config: &AggregatorConfig) -> Option<Self> {
        use AggregationType as At;
        let has_filter = config.value_filter.is_some();
        let (kind, acceptance) = match config.aggregation {
            // count/sum with a CONDITION run as their *if aggregators
            // (create_aggregator), whose zero-match buckets finalize to 0.0
            // instead of NaN — the kind must match that.
            At::Sum if has_filter => (PartialReducerKind::SumIf, NanAcceptance::Reject),
            At::Count if has_filter => (PartialReducerKind::CountIf, NanAcceptance::Reject),
            At::Sum => (PartialReducerKind::Sum, NanAcceptance::Reject),
            At::SumIf => (PartialReducerKind::SumIf, NanAcceptance::Reject),
            At::Count => (PartialReducerKind::Count, NanAcceptance::Reject),
            At::CountIf => (PartialReducerKind::CountIf, NanAcceptance::Reject),
            At::CountAll => (PartialReducerKind::Count, NanAcceptance::All),
            At::CountNan => (PartialReducerKind::Count, NanAcceptance::NanOnly),
            At::Min => (PartialReducerKind::Min, NanAcceptance::Reject),
            At::Max => (PartialReducerKind::Max, NanAcceptance::Reject),
            At::Range => (PartialReducerKind::Range, NanAcceptance::Reject),
            At::Avg => (PartialReducerKind::Avg, NanAcceptance::Reject),
            At::StdP => (PartialReducerKind::StdP, NanAcceptance::Reject),
            At::StdS => (PartialReducerKind::StdS, NanAcceptance::Reject),
            At::VarP => (PartialReducerKind::VarP, NanAcceptance::Reject),
            At::VarS => (PartialReducerKind::VarS, NanAcceptance::Reject),
            At::First => (PartialReducerKind::First, NanAcceptance::Reject),
            At::Last => (PartialReducerKind::Last, NanAcceptance::Reject),
            // Order-sensitive across series: not decomposable.
            At::Increase | At::IRate => return None,
            // Not valid reducers (rejected by the parser) — never pushed down.
            At::All | At::Any | At::None | At::Share | At::Rate => return None,
        };
        Some(Self {
            kind,
            filter: config.value_filter,
            acceptance,
            state: PartialState::default(),
        })
    }

    pub fn kind(&self) -> PartialReducerKind {
        self.kind
    }

    fn accepts(&self, value: f64) -> bool {
        match self.acceptance {
            NanAcceptance::Reject => {
                if value.is_nan() {
                    return false;
                }
                self.filter.as_ref().is_none_or(|f| f.compare(value))
            }
            NanAcceptance::All => true,
            NanAcceptance::NanOnly => value.is_nan(),
        }
    }

    pub fn update(&mut self, ts: Timestamp, value: f64) {
        if !self.accepts(value) {
            return;
        }
        let s = &mut self.state;
        let first_value = s.count == 0;
        s.count += 1;
        match self.kind {
            PartialReducerKind::Sum | PartialReducerKind::SumIf | PartialReducerKind::Avg => {
                let mut sum = KahanSum::from_parts(s.acc1, s.acc1_c);
                sum += value;
                (s.acc1, s.acc1_c) = sum.into_parts();
            }
            PartialReducerKind::Count | PartialReducerKind::CountIf => {}
            PartialReducerKind::Min => {
                s.acc1 = if first_value {
                    value
                } else {
                    s.acc1.min(value)
                }
            }
            PartialReducerKind::Max => {
                s.acc1 = if first_value {
                    value
                } else {
                    s.acc1.max(value)
                }
            }
            PartialReducerKind::Range => {
                if first_value {
                    s.acc1 = value;
                    s.acc2 = value;
                } else {
                    s.acc1 = s.acc1.min(value);
                    s.acc2 = s.acc2.max(value);
                }
            }
            PartialReducerKind::StdP
            | PartialReducerKind::StdS
            | PartialReducerKind::VarP
            | PartialReducerKind::VarS => {
                // Welford: acc1 = running mean, acc2 = M2. Unlike the
                // sum-of-squares form this cannot cancel catastrophically and
                // M2 is nonnegative by construction. `count` was already
                // incremented above.
                let delta = value - s.acc1;
                s.acc1 += delta / s.count as f64;
                s.acc2 += delta * (value - s.acc1);
            }
            PartialReducerKind::First => {
                if first_value || ts < s.ts {
                    s.acc1 = value;
                    s.ts = ts;
                }
            }
            PartialReducerKind::Last => {
                if first_value || ts >= s.ts {
                    s.acc1 = value;
                    s.ts = ts;
                }
            }
        }
    }

    /// Returns the accumulated state and resets for the next timestamp group.
    pub fn take_state(&mut self) -> PartialState {
        std::mem::take(&mut self.state)
    }

    /// Merges `other` into `into`. States must come from the same reducer
    /// config. For first/last, equal-timestamp ties across shards are broken
    /// arbitrarily (matches the merge-order artifact of single-node reduce).
    pub fn merge(kind: PartialReducerKind, into: &mut PartialState, other: &PartialState) {
        if other.count == 0 {
            return;
        }
        if into.count == 0 {
            *into = *other;
            return;
        }
        let (n_a, n_b) = (into.count as f64, other.count as f64);
        into.count += other.count;
        match kind {
            PartialReducerKind::Sum | PartialReducerKind::SumIf | PartialReducerKind::Avg => {
                let mut sum = KahanSum::from_parts(into.acc1, into.acc1_c);
                sum += KahanSum::from_parts(other.acc1, other.acc1_c);
                (into.acc1, into.acc1_c) = sum.into_parts();
            }
            PartialReducerKind::Count | PartialReducerKind::CountIf => {}
            PartialReducerKind::Min => into.acc1 = into.acc1.min(other.acc1),
            PartialReducerKind::Max => into.acc1 = into.acc1.max(other.acc1),
            PartialReducerKind::Range => {
                into.acc1 = into.acc1.min(other.acc1);
                into.acc2 = into.acc2.max(other.acc2);
            }
            PartialReducerKind::StdP
            | PartialReducerKind::StdS
            | PartialReducerKind::VarP
            | PartialReducerKind::VarS => {
                // Chan's parallel combine of two Welford states.
                let n = n_a + n_b;
                let delta = other.acc1 - into.acc1;
                into.acc1 += delta * (n_b / n);
                into.acc2 += other.acc2 + delta * delta * (n_a * n_b / n);
            }
            PartialReducerKind::First => {
                if other.ts < into.ts {
                    into.acc1 = other.acc1;
                    into.ts = other.ts;
                }
            }
            PartialReducerKind::Last => {
                if other.ts >= into.ts {
                    into.acc1 = other.acc1;
                    into.ts = other.ts;
                }
            }
        }
    }

    /// Finalizes a (merged) state into the reduced value. A state with
    /// `count == 0` yields 0 for the tallying kinds and NaN for the rest.
    pub fn finalize(kind: PartialReducerKind, state: &PartialState) -> f64 {
        if state.count == 0 {
            // A tally of nothing is 0, while a function of nothing is undefined. `Count`
            // covers count/countall/countnan; countif/sumif accept every sample (the
            // CONDITION merely gates accumulation), so zero matches is still a count of
            // zero. Everything else yields NaN. Must track `Aggregator::empty_group_value`,
            // which `test_partial_matches_sample_reducer_for_arbitrary_partitions` enforces.
            return match kind {
                PartialReducerKind::Count
                | PartialReducerKind::CountIf
                | PartialReducerKind::SumIf => 0.0,
                _ => f64::NAN,
            };
        }
        let n = state.count as f64;
        match kind {
            // The Neumaier result is running sum + accumulated compensation.
            PartialReducerKind::Sum | PartialReducerKind::SumIf => state.acc1 + state.acc1_c,
            // Selector kinds return acc1 untouched (acc1_c is always 0.0
            // here, and e.g. min must preserve a -0.0).
            PartialReducerKind::Min
            | PartialReducerKind::Max
            | PartialReducerKind::First
            | PartialReducerKind::Last => state.acc1,
            PartialReducerKind::Count | PartialReducerKind::CountIf => n,
            PartialReducerKind::Range => state.acc2 - state.acc1,
            PartialReducerKind::Avg => (state.acc1 + state.acc1_c) / n,
            // acc2 is the Welford M2 — the same statistic AggStd
            // (handlers.rs) accumulates, so push-down matches the
            // single-node dispersion aggregators. M2 is 0.0 for count == 1
            // by construction.
            PartialReducerKind::VarP => state.acc2 / n,
            PartialReducerKind::VarS => {
                if state.count == 1 {
                    0.0
                } else {
                    state.acc2 / (n - 1.0)
                }
            }
            PartialReducerKind::StdP => (state.acc2 / n).sqrt(),
            PartialReducerKind::StdS => {
                if state.count == 1 {
                    0.0
                } else {
                    (state.acc2 / (n - 1.0)).sqrt()
                }
            }
        }
    }
}

/// Groups an ascending sample stream by timestamp and yields one
/// `PartialState` per timestamp — the partial-state analogue of
/// `SampleReducer` (src/iterators/sample_reducer.rs).
pub struct PartialSampleReducer<I>
where
    I: Iterator<Item = Sample>,
{
    inner: I,
    buffer: Option<Sample>,
    reducer: PartialReducer,
}

impl<I> PartialSampleReducer<I>
where
    I: Iterator<Item = Sample>,
{
    pub fn new(inner: I, reducer: PartialReducer) -> Self {
        Self {
            inner,
            buffer: None,
            reducer,
        }
    }
}

impl<I> Iterator for PartialSampleReducer<I>
where
    I: Iterator<Item = Sample>,
{
    type Item = (Timestamp, PartialState);

    fn next(&mut self) -> Option<Self::Item> {
        let first = match self.buffer.take() {
            Some(sample) => sample,
            None => self.inner.next()?,
        };

        let timestamp = first.timestamp;
        self.reducer.update(first.timestamp, first.value);

        for next in self.inner.by_ref() {
            if next.timestamp == timestamp {
                self.reducer.update(next.timestamp, next.value);
            } else {
                self.buffer = Some(next);
                break;
            }
        }

        Some((timestamp, self.reducer.take_state()))
    }
}

/// Row twin of [`PartialSampleReducer`] for multi-aggregation GROUPBY/REDUCE
/// push-down: groups an ascending row stream by timestamp and accumulates each
/// aggregation column into its own clone of the partial reducer, yielding one
/// state per column per timestamp — the partial-state analogue of `RowReducer`
/// (src/iterators/row_reducer.rs). Column-wise NaN/CONDITION acceptance is
/// per-reducer, so an all-NaN column merges/finalizes to NaN independently.
pub struct PartialRowReducer<I>
where
    I: Iterator<Item = MultiSample>,
{
    inner: I,
    buffer: Option<MultiSample>,
    /// One reducer per aggregation column.
    reducers: SmallVec<[PartialReducer; 4]>,
}

impl<I> PartialRowReducer<I>
where
    I: Iterator<Item = MultiSample>,
{
    pub fn new(inner: I, reducer: PartialReducer, columns: usize) -> Self {
        Self {
            inner,
            buffer: None,
            reducers: (0..columns).map(|_| reducer.clone()).collect(),
        }
    }
}

fn update_columns(reducers: &mut [PartialReducer], row: &MultiSample) {
    debug_assert_eq!(row.values.len(), reducers.len());
    for (reducer, value) in reducers.iter_mut().zip(row.values.iter()) {
        reducer.update(row.timestamp, *value);
    }
}

impl<I> Iterator for PartialRowReducer<I>
where
    I: Iterator<Item = MultiSample>,
{
    type Item = (Timestamp, SmallVec<[PartialState; 4]>);

    fn next(&mut self) -> Option<Self::Item> {
        let first = match self.buffer.take() {
            Some(row) => row,
            None => self.inner.next()?,
        };

        let timestamp = first.timestamp;
        update_columns(&mut self.reducers, &first);

        for next in self.inner.by_ref() {
            if next.timestamp == timestamp {
                update_columns(&mut self.reducers, &next);
            } else {
                self.buffer = Some(next);
                break;
            }
        }

        let states = self.reducers.iter_mut().map(|r| r.take_state()).collect();
        Some((timestamp, states))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::binop::ComparisonOperator;
    use crate::iterators::SampleReducer;

    /// Deterministic LCG so failures are reproducible.
    struct Rng(u64);

    impl Rng {
        fn next_u64(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            self.0
        }

        /// Value in [-10, 10), NaN with ~1/8 probability.
        fn next_value(&mut self) -> f64 {
            let bits = self.next_u64();
            if bits.is_multiple_of(8) {
                f64::NAN
            } else {
                ((bits >> 11) as f64 / (1u64 << 53) as f64) * 20.0 - 10.0
            }
        }
    }

    fn decomposable_configs() -> Vec<AggregatorConfig> {
        use AggregationType as At;
        let filter = ValueComparisonFilter {
            operator: ComparisonOperator::GreaterThan,
            value: 0.0,
        };
        let mut configs: Vec<AggregatorConfig> = [
            At::Sum,
            At::Count,
            At::CountAll,
            At::CountNan,
            At::Min,
            At::Max,
            At::Range,
            At::Avg,
            At::StdP,
            At::StdS,
            At::VarP,
            At::VarS,
            At::First,
            At::Last,
        ]
        .iter()
        .map(|&ty| AggregatorConfig::new(ty, None).unwrap())
        .collect();
        configs.push(AggregatorConfig::new(At::CountIf, Some(filter)).unwrap());
        configs.push(AggregatorConfig::new(At::SumIf, Some(filter)).unwrap());
        // count/sum + CONDITION run as the *if aggregators; the partial kind
        // must match their 0.0-on-zero-matches finalization.
        configs.push(AggregatorConfig::new(At::Count, Some(filter)).unwrap());
        configs.push(AggregatorConfig::new(At::Sum, Some(filter)).unwrap());
        configs
    }

    fn assert_value_eq(actual: f64, expected: f64, context: &str) {
        if expected.is_nan() {
            assert!(actual.is_nan(), "{context}: expected NaN, got {actual}");
            return;
        }
        let tolerance = 1e-9 * expected.abs().max(1.0);
        assert!(
            (actual - expected).abs() <= tolerance,
            "{context}: expected {expected}, got {actual}"
        );
    }

    /// Invariant: for every decomposable reducer and arbitrary
    /// partitions of the input, finalize(merge(partial per partition)) equals
    /// the single-node SampleReducer result over the concatenated input.
    #[test]
    fn test_partial_matches_sample_reducer_for_arbitrary_partitions() {
        const TS: Timestamp = 1000;
        let mut rng = Rng(42);

        for config in decomposable_configs() {
            for n in [1usize, 2, 7, 40] {
                for round in 0..8 {
                    let values: Vec<f64> = (0..n).map(|_| rng.next_value()).collect();

                    // Reference: single-node reduce of the whole group
                    let samples = values.iter().map(|&value| Sample {
                        timestamp: TS,
                        value,
                    });
                    let expected = SampleReducer::new(samples, config.create_aggregator())
                        .next()
                        .expect("one timestamp group")
                        .value;

                    // Partition into up to 3 "shards" (possibly empty),
                    // accumulate partials, merge in partition order, finalize.
                    let cut1 = (rng.next_u64() as usize) % (n + 1);
                    let cut2 = cut1 + (rng.next_u64() as usize) % (n - cut1 + 1);
                    let template =
                        PartialReducer::for_config(&config).expect("decomposable reducer");
                    let kind = template.kind();
                    let mut merged = PartialState::default();
                    for part in [&values[..cut1], &values[cut1..cut2], &values[cut2..]] {
                        let mut reducer = template.clone();
                        for &value in part {
                            reducer.update(TS, value);
                        }
                        PartialReducer::merge(kind, &mut merged, &reducer.take_state());
                    }
                    let actual = PartialReducer::finalize(kind, &merged);

                    assert_value_eq(
                        actual,
                        expected,
                        &format!(
                            "{} n={n} round={round} cuts=({cut1},{cut2}) values={values:?}",
                            config.aggregation
                        ),
                    );
                }
            }
        }
    }

    /// Naive per-shard sums are 1e16 and -1e16, so an uncompensated merge
    /// finalizes to 0.0; the Neumaier compensation carried in `acc1_c`
    /// recovers the exact 2.0.
    #[test]
    fn test_sum_partials_survive_catastrophic_cancellation() {
        const TS: Timestamp = 1;
        let config = AggregatorConfig::new(AggregationType::Sum, None).unwrap();
        let template = PartialReducer::for_config(&config).unwrap();
        let kind = template.kind();

        let shards: [&[f64]; 2] = [&[1e16, 1.0], &[1.0, -1e16]];
        let mut merged = PartialState::default();
        for part in shards {
            let mut reducer = template.clone();
            for &value in part {
                reducer.update(TS, value);
            }
            PartialReducer::merge(kind, &mut merged, &reducer.take_state());
        }
        assert_eq!(PartialReducer::finalize(kind, &merged), 2.0);
    }

    /// Tiny variance around a huge mean: the sum-of-squares formula loses the
    /// variance entirely in Σx² rounding (ulp at 1e18 is 128, the M2 is 2);
    /// Welford accumulation + Chan merge keep it exact.
    #[test]
    fn test_variance_partials_survive_large_offset() {
        const TS: Timestamp = 1;
        for (ty, expected) in [
            (AggregationType::VarP, 2.0 / 3.0),
            (AggregationType::VarS, 1.0),
            (AggregationType::StdP, (2.0f64 / 3.0).sqrt()),
            (AggregationType::StdS, 1.0),
        ] {
            let config = AggregatorConfig::new(ty, None).unwrap();
            let template = PartialReducer::for_config(&config).unwrap();
            let kind = template.kind();

            let shards: [&[f64]; 2] = [&[1e9, 1e9 + 1.0], &[1e9 + 2.0]];
            let mut merged = PartialState::default();
            for part in shards {
                let mut reducer = template.clone();
                for &value in part {
                    reducer.update(TS, value);
                }
                PartialReducer::merge(kind, &mut merged, &reducer.take_state());
            }
            assert_value_eq(
                PartialReducer::finalize(kind, &merged),
                expected,
                &format!("{ty} around large offset"),
            );
        }
    }

    #[test]
    fn test_non_decomposable_reducers_rejected() {
        use AggregationType as At;
        for ty in [At::Increase, At::IRate, At::Rate] {
            let config = AggregatorConfig::new(ty, None).unwrap();
            assert!(
                PartialReducer::for_config(&config).is_none(),
                "{ty} must not be decomposable"
            );
        }
    }

    /// `AggregationType::is_decomposable` is the coordinator's cheap
    /// eligibility test; `for_config` is what actually builds the state. They
    /// must never disagree, or the coordinator sets `apply_group_reduce` for a
    /// reducer no shard can pre-reduce (or forgoes push-down for one it can).
    #[test]
    fn test_is_decomposable_agrees_with_for_config() {
        use AggregationType as At;

        for config in decomposable_configs() {
            assert!(
                config.aggregation.is_decomposable(),
                "{} is decomposable but is_decomposable() said no",
                config.aggregation
            );
        }

        for ty in [At::Increase, At::IRate, At::Rate] {
            let config = AggregatorConfig::new(ty, None).unwrap();
            assert!(
                !ty.is_decomposable(),
                "{ty} is not decomposable but is_decomposable() said yes"
            );
            assert!(PartialReducer::for_config(&config).is_none());
        }

        // Not valid reducers; is_decomposable() must not admit them either.
        for ty in [At::All, At::Any, At::None, At::Share] {
            assert!(!ty.is_decomposable(), "{ty} is not a valid reducer");
        }
    }

    #[test]
    fn test_partial_row_reducer_columns_independent() {
        let row = |ts: Timestamp, values: &[f64]| MultiSample {
            timestamp: ts,
            values: values.iter().copied().collect(),
        };
        // column 0 accumulates normally; column 1 is all-NaN at ts=1
        let rows = vec![
            row(1, &[1.0, f64::NAN]),
            row(1, &[3.0, f64::NAN]),
            row(2, &[5.0, 7.0]),
        ];

        let config = AggregatorConfig::new(AggregationType::Sum, None).unwrap();
        let reducer = PartialReducer::for_config(&config).unwrap();
        let kind = reducer.kind();
        let buckets: Vec<_> = PartialRowReducer::new(rows.into_iter(), reducer, 2).collect();

        assert_eq!(buckets.len(), 2);
        let (ts, states) = &buckets[0];
        assert_eq!(*ts, 1);
        assert_eq!(states.len(), 2);
        assert_value_eq(
            PartialReducer::finalize(kind, &states[0]),
            4.0,
            "col 0 ts=1",
        );
        assert!(
            PartialReducer::finalize(kind, &states[1]).is_nan(),
            "all-NaN column yields NaN independently"
        );
        let (ts, states) = &buckets[1];
        assert_eq!(*ts, 2);
        assert_value_eq(
            PartialReducer::finalize(kind, &states[0]),
            5.0,
            "col 0 ts=2",
        );
        assert_value_eq(
            PartialReducer::finalize(kind, &states[1]),
            7.0,
            "col 1 ts=2",
        );
    }

    /// Column i of `PartialRowReducer` equals `PartialSampleReducer` over that
    /// column's (ts, value) stream — the row form adds no cross-column
    /// coupling, for every decomposable reducer.
    #[test]
    fn test_partial_row_reducer_matches_sample_reducer_per_column() {
        const COLUMNS: usize = 3;
        let mut rng = Rng(7);

        for config in decomposable_configs() {
            let rows: Vec<MultiSample> = [1i64, 1, 2, 5, 5, 5, 9]
                .iter()
                .map(|&timestamp| MultiSample {
                    timestamp,
                    values: (0..COLUMNS).map(|_| rng.next_value()).collect(),
                })
                .collect();

            let template = PartialReducer::for_config(&config).unwrap();
            let kind = template.kind();
            let row_buckets: Vec<_> =
                PartialRowReducer::new(rows.iter().cloned(), template.clone(), COLUMNS).collect();

            for column in 0..COLUMNS {
                let samples = rows.iter().map(|r| Sample {
                    timestamp: r.timestamp,
                    value: r.values[column],
                });
                let expected: Vec<_> =
                    PartialSampleReducer::new(samples, template.clone()).collect();

                assert_eq!(row_buckets.len(), expected.len());
                for ((row_ts, states), (ts, state)) in row_buckets.iter().zip(expected.iter()) {
                    assert_eq!(row_ts, ts);
                    assert_value_eq(
                        PartialReducer::finalize(kind, &states[column]),
                        PartialReducer::finalize(kind, state),
                        &format!("{} column {column} ts {ts}", config.aggregation),
                    );
                }
            }
        }
    }

    #[test]
    fn test_partial_sample_reducer_groups_by_timestamp() {
        let samples = [(1, 1.0), (1, 3.0), (2, f64::NAN), (3, 5.0), (3, 7.0)]
            .iter()
            .map(|&(timestamp, value)| Sample { timestamp, value });

        let config = AggregatorConfig::new(AggregationType::Sum, None).unwrap();
        let reducer = PartialReducer::for_config(&config).unwrap();
        let kind = reducer.kind();
        let rows: Vec<_> = PartialSampleReducer::new(samples, reducer).collect();

        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].0, 1);
        assert_value_eq(PartialReducer::finalize(kind, &rows[0].1), 4.0, "ts=1");
        assert_eq!(rows[1].0, 2);
        // all-NaN timestamp group finalizes to NaN
        assert!(PartialReducer::finalize(kind, &rows[1].1).is_nan());
        assert_eq!(rows[2].0, 3);
        assert_value_eq(PartialReducer::finalize(kind, &rows[2].1), 12.0, "ts=3");
    }
}
