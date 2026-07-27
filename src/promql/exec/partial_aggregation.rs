//! Mergeable (decomposable) aggregation state for shard-side PromQL
//! aggregation push-down.
//!
//! A shard evaluating a pushed-down `sum by (job) (metric)` cannot send back a
//! finished sum: its slice of `metric` is only part of each group. Instead it
//! reduces its local members of a group into an [`AggregationPartial`] — an
//! accumulator that can be merged with the other shards' accumulators — and the
//! coordinator merges them per group and finalizes. For every reduction
//! operator, `finalize(merge(states…))` equals the single-node
//! `apply_aggregation` result over the concatenated inputs, up to
//! floating-point summation order (the inputs are partitioned differently, so
//! bit-exact parity with a single-node sum is not achievable in general).
//!
//! `quantile` has no such form — interpolating between a group's values needs
//! the values — so it is not pushed down; see
//! [`AggregationKind::pushdown_strategy`].
//!
//! This is the PromQL analogue of [`crate::aggregators::PartialReducer`], which
//! does the same job for `TS.MRANGE`'s GROUPBY/REDUCE. The two cannot be shared:
//! PromQL aggregation counts and propagates NaN where the TS reducers reject it,
//! and it groups by label set rather than by bucket timestamp.

use crate::common::math::kahan_inc;
use crate::common::{Sample, Timestamp};
use crate::labels::{HasFingerprint, Labels};
use crate::promql::EvalSample;
use crate::promql::exec::aggregations::{
    AggregationKind, PushdownStrategy, max_ignore_nan, min_ignore_nan,
};
use crate::promql::exec::types::EvalLabels;
use crate::promql::hashers::FingerprintHashMap;
use crate::promql::model::RangeSample;
use promql_parser::parser::LabelModifier;
use std::collections::BTreeMap;

/// Mergeable accumulator for one aggregation group.
///
/// Field meaning depends on the operator:
///
/// | operator          | `acc1`               | `acc2`  | `acc1_c`             |
/// |-------------------|----------------------|---------|----------------------|
/// | sum               | Kahan sum            | —       | sum compensation     |
/// | avg               | Kahan sum            | mean    | sum compensation     |
/// | min / max         | NaN-ignoring extremum| —       | —                    |
/// | count / group     | —                    | —       | —                    |
/// | stddev / stdvar   | Welford mean         | M2      | M2 compensation      |
///
/// `count` counts every sample in the group, NaN included: it is the
/// denominator `count`, `avg`, `stddev` and `stdvar` divide by on a single node.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(in crate::promql) struct AggregationPartial {
    pub count: u64,
    pub acc1: f64,
    pub acc2: f64,
    pub acc1_c: f64,
}

impl AggregationPartial {
    /// The zero state for `kind`: `count == 0`, with accumulators seeded so
    /// that folding the first value is indistinguishable from starting from it.
    pub fn empty(kind: AggregationKind) -> Self {
        let acc1 = match kind {
            // min/max fold with the NaN-ignoring comparators, for which NaN is
            // the identity — the same value the single-node path yields for an
            // empty group (`reduce(...).unwrap_or(NAN)`).
            AggregationKind::Min | AggregationKind::Max => f64::NAN,
            _ => 0.0,
        };
        Self {
            count: 0,
            acc1,
            acc2: 0.0,
            acc1_c: 0.0,
        }
    }

    /// Fold one sample value into the state.
    pub fn update(&mut self, kind: AggregationKind, value: f64) {
        self.count += 1;
        match kind {
            AggregationKind::Sum => {
                (self.acc1, self.acc1_c) = kahan_inc(value, self.acc1, self.acc1_c);
            }
            AggregationKind::Avg => {
                (self.acc1, self.acc1_c) = kahan_inc(value, self.acc1, self.acc1_c);
                // Carried alongside the sum so an overflowing sum can still
                // finalize to a representable mean, mirroring the hybrid
                // strategy of `kahan_avg`.
                self.acc2 += (value - self.acc2) / self.count as f64;
            }
            AggregationKind::Min => self.acc1 = min_ignore_nan(self.acc1, value),
            AggregationKind::Max => self.acc1 = max_ignore_nan(self.acc1, value),
            AggregationKind::Count | AggregationKind::Group => {}
            AggregationKind::Stddev | AggregationKind::Stdvar => {
                // Welford, compensated exactly as `kahan_variance` does:
                // acc1 = running mean, acc2 = M2, acc1_c = M2's compensation.
                let delta = value - self.acc1;
                self.acc1 += delta / self.count as f64;
                let new_delta = value - self.acc1;
                (self.acc2, self.acc1_c) = kahan_inc(delta * new_delta, self.acc2, self.acc1_c);
            }
            _ => unreachable!("BUG: non-reduction aggregation kind reached AggregationPartial"),
        }
    }

    /// Merge another shard's state for the same group into this one. Both
    /// states must come from the same operator.
    pub fn merge(&mut self, kind: AggregationKind, other: &Self) {
        if other.count == 0 {
            return;
        }
        if self.count == 0 {
            *self = *other;
            return;
        }
        let (n_a, n_b) = (self.count as f64, other.count as f64);
        let n = n_a + n_b;
        match kind {
            AggregationKind::Sum => {
                (self.acc1, self.acc1_c) =
                    merge_kahan_sums((self.acc1, self.acc1_c), (other.acc1, other.acc1_c));
            }
            AggregationKind::Avg => {
                (self.acc1, self.acc1_c) =
                    merge_kahan_sums((self.acc1, self.acc1_c), (other.acc1, other.acc1_c));
                self.acc2 += (other.acc2 - self.acc2) * (n_b / n);
            }
            AggregationKind::Min => self.acc1 = min_ignore_nan(self.acc1, other.acc1),
            AggregationKind::Max => self.acc1 = max_ignore_nan(self.acc1, other.acc1),
            AggregationKind::Count | AggregationKind::Group => {}
            AggregationKind::Stddev | AggregationKind::Stdvar => {
                // Chan's parallel combine of two Welford states. `other`'s M2
                // arrives as value + compensation and is folded as one term.
                let delta = other.acc1 - self.acc1;
                (self.acc2, self.acc1_c) =
                    kahan_inc(other.acc2 + other.acc1_c, self.acc2, self.acc1_c);
                (self.acc2, self.acc1_c) =
                    kahan_inc(delta * delta * (n_a * n_b / n), self.acc2, self.acc1_c);
                self.acc1 += delta * (n_b / n);
            }
            _ => unreachable!("BUG: non-reduction aggregation kind reached AggregationPartial"),
        }
        self.count += other.count;
    }

    /// Finalize a (merged) state into the group's aggregated value.
    pub fn finalize(&self, kind: AggregationKind) -> f64 {
        let n = self.count as f64;
        if self.count == 0 {
            // Unreachable in practice: a partial only exists for a group that
            // has at least one sample. Mirror the single-node values for an
            // empty input anyway rather than inventing something.
            return match kind {
                AggregationKind::Sum | AggregationKind::Count => 0.0,
                AggregationKind::Group => 1.0,
                _ => f64::NAN,
            };
        }
        match kind {
            AggregationKind::Sum => self.sum(),
            AggregationKind::Avg => {
                let sum = self.sum();
                if sum.is_finite() {
                    sum / n
                } else if sum.is_nan() {
                    f64::NAN
                } else if self.acc2.is_finite() {
                    // The sum overflowed to ±Inf but the mean is representable:
                    // fall back to the incrementally maintained mean, as
                    // `kahan_avg` does when its running sum overflows.
                    self.acc2
                } else {
                    // Genuinely infinite input: ±Inf is the answer.
                    sum
                }
            }
            AggregationKind::Min | AggregationKind::Max => self.acc1,
            AggregationKind::Count => n,
            AggregationKind::Group => 1.0,
            AggregationKind::Stdvar => self.variance(n),
            AggregationKind::Stddev => {
                // `kahan_std_dev` short-circuits a single value to 0.0 without
                // looking at it, so a lone NaN sample yields 0.0 there too.
                if self.count == 1 {
                    0.0
                } else {
                    self.variance(n).sqrt()
                }
            }
            _ => unreachable!("BUG: non-reduction aggregation kind reached AggregationPartial"),
        }
    }

    /// The compensated sum, folded the way `kahan_sum` folds it.
    fn sum(&self) -> f64 {
        if self.acc1.is_infinite() {
            self.acc1
        } else {
            self.acc1 + self.acc1_c
        }
    }

    fn variance(&self, n: f64) -> f64 {
        (self.acc2 + self.acc1_c) / n
    }
}

/// Add two transported Kahan sums, keeping the result compensated.
fn merge_kahan_sums(into: (f64, f64), other: (f64, f64)) -> (f64, f64) {
    let (sum, c) = kahan_inc(other.0, into.0, into.1);
    kahan_inc(other.1, sum, c)
}

/// Per-group mergeable state for one pushed-down reduction, keyed by the
/// fingerprint of the group's labels.
///
/// Used by both sides of the push-down: a shard [`accumulate`]s its local
/// samples and ships [`into_partials`]; the coordinator [`merge`]s what the
/// shards sent (and `accumulate`s the raw samples of any shard that did not
/// aggregate) and then [`finalize`]s.
///
/// [`accumulate`]: PartialGroups::accumulate
/// [`into_partials`]: PartialGroups::into_partials
/// [`merge`]: PartialGroups::merge
/// [`finalize`]: PartialGroups::finalize
pub(in crate::promql) struct PartialGroups {
    kind: AggregationKind,
    groups: FingerprintHashMap<(EvalLabels, AggregationPartial)>,
}

impl PartialGroups {
    /// # Panics
    /// If `kind` is not a reduction — the other operators use
    /// [`PushdownStrategy::Select`] or [`PushdownStrategy::CountValues`] and
    /// have no partial state.
    pub fn new(kind: AggregationKind) -> Self {
        assert_eq!(
            kind.pushdown_strategy(),
            Some(PushdownStrategy::Reduce),
            "BUG: {kind:?} has no mergeable partial state"
        );
        Self {
            kind,
            groups: FingerprintHashMap::default(),
        }
    }

    /// Fold raw instant-vector samples into the per-group states, grouping them
    /// with `modifier` exactly as the single-node path does.
    pub fn accumulate(&mut self, modifier: Option<&LabelModifier>, samples: Vec<EvalSample>) {
        let kind = self.kind;
        for sample in samples {
            let labels = sample.labels.compute_grouping_labels(modifier);
            self.entry(labels).update(kind, sample.value);
        }
    }

    /// Merge one shard's state for `labels`' group.
    pub fn merge(&mut self, labels: EvalLabels, state: AggregationPartial) {
        let kind = self.kind;
        self.entry(labels).merge(kind, &state);
    }

    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }

    /// The accumulated states, for transport to the coordinator.
    pub fn into_partials(self) -> impl Iterator<Item = (EvalLabels, AggregationPartial)> {
        self.groups.into_iter().map(|(_, entry)| entry)
    }

    /// Finalize every group into a result sample stamped at `eval_time`, as
    /// the single-node reduction path stamps its output.
    pub fn finalize(self, eval_time: Timestamp) -> Vec<EvalSample> {
        let kind = self.kind;
        self.groups
            .into_iter()
            .map(|(_, (labels, state))| EvalSample {
                labels,
                value: state.finalize(kind),
                timestamp_ms: eval_time,
                drop_name: false,
            })
            .collect()
    }

    pub(super) fn entry(&mut self, labels: EvalLabels) -> &mut AggregationPartial {
        let key = labels.fingerprint();
        let kind = self.kind;
        &mut self
            .groups
            .entry(key)
            .or_insert_with(|| (labels, AggregationPartial::empty(kind)))
            .1
    }
}

/// Per-`(group, step)` mergeable state, for a rollup fused with an outer
/// aggregation.
///
/// One [`PartialGroups`] per step. The merge algebra is per group and identical
/// at every step — steps never interact — so keying by step on the outside
/// reuses it whole rather than restating it. A `BTreeMap` keeps the steps
/// ordered, which is what makes [`finalize`] emit each group's points
/// chronologically without a sort.
///
/// [`finalize`]: SteppedPartialGroups::finalize
pub(in crate::promql) struct SteppedPartialGroups {
    kind: AggregationKind,
    steps: BTreeMap<Timestamp, PartialGroups>,
}

impl SteppedPartialGroups {
    /// # Panics
    /// If `kind` is not a reduction; see [`PartialGroups::new`].
    pub fn new(kind: AggregationKind) -> Self {
        Self {
            kind,
            steps: BTreeMap::new(),
        }
    }

    /// Fold per-series rollup output into the per-`(group, step)` states.
    ///
    /// Each series carries its own sparse `(step, value)` points, so a step at
    /// which a series produced nothing simply does not contribute to that step's
    /// groups — which is how a group comes to exist at some steps and not
    /// others.
    pub fn accumulate(&mut self, modifier: Option<&LabelModifier>, series: Vec<RangeSample>) {
        let kind = self.kind;
        for s in series {
            let labels = EvalLabels::from(s.labels).compute_grouping_labels(modifier);
            for point in s.samples {
                self.steps
                    .entry(point.timestamp)
                    .or_insert_with(|| PartialGroups::new(kind))
                    .entry(labels.clone())
                    .update(kind, point.value);
            }
        }
    }

    /// Merge one shard's state for a `(group, step)` pair.
    pub fn merge(&mut self, step_ts: Timestamp, labels: EvalLabels, state: AggregationPartial) {
        let kind = self.kind;
        self.steps
            .entry(step_ts)
            .or_insert_with(|| PartialGroups::new(kind))
            .merge(labels, state);
    }

    /// The accumulated states, for transport to the coordinator.
    pub fn into_partials(
        self,
    ) -> impl Iterator<Item = (Timestamp, EvalLabels, AggregationPartial)> {
        self.steps.into_iter().flat_map(|(step_ts, groups)| {
            groups
                .into_partials()
                .map(move |(labels, state)| (step_ts, labels, state))
        })
    }

    /// Finalize into one entry per group, each holding its sparse `(step,
    /// value)` points in ascending step order.
    pub fn finalize(self) -> Vec<RangeSample> {
        let kind = self.kind;
        let mut by_group: FingerprintHashMap<(Labels, Vec<Sample>)> = FingerprintHashMap::default();

        for (step_ts, groups) in self.steps {
            for (labels, state) in groups.into_partials() {
                let key = labels.fingerprint();
                by_group
                    .entry(key)
                    .or_insert_with(|| (labels.into_labels(), Vec::new()))
                    .1
                    .push(Sample {
                        timestamp: step_ts,
                        value: state.finalize(kind),
                    });
            }
        }

        by_group
            .into_iter()
            .map(|(_, (labels, samples))| RangeSample { labels, samples })
            .collect()
    }
}

/// Sum the shards' `count_values` counts by output label set.
///
/// Each shard counted only the samples it holds, so a value observed on several
/// shards arrives as several samples with identical labels (the group labels
/// plus the destination label). Counts are exact integers in `f64`, so summing
/// them is exact.
pub(in crate::promql) fn merge_count_values(samples: Vec<EvalSample>) -> Vec<EvalSample> {
    let mut counts: FingerprintHashMap<EvalSample> = FingerprintHashMap::default();
    for sample in samples {
        let key = sample.labels.fingerprint();
        match counts.get_mut(&key) {
            Some(existing) => existing.value += sample.value,
            None => {
                counts.insert(key, sample);
            }
        }
    }
    counts.into_iter().map(|(_, sample)| sample).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::promql::exec::aggregations::apply_aggregation;
    use promql_parser::label::Labels as ModifierLabels;

    const EVAL_TS: Timestamp = 1_700_000_000_000;

    fn sample(labels: &[(&str, &str)], value: f64) -> EvalSample {
        EvalSample {
            labels: EvalLabels::from_pairs(labels),
            value,
            timestamp_ms: EVAL_TS,
            drop_name: false,
        }
    }

    fn by(labels: &[&str]) -> LabelModifier {
        LabelModifier::Include(ModifierLabels::new(labels.to_vec()))
    }

    /// Single-node result of `kind` over `samples`, as `(labels, value)` pairs
    /// sorted for comparison.
    fn single_node(
        kind: AggregationKind,
        modifier: Option<&LabelModifier>,
        samples: Vec<EvalSample>,
    ) -> Vec<(EvalLabels, f64)> {
        sorted(apply_aggregation(kind, modifier, None, samples, EVAL_TS).unwrap())
    }

    fn sorted(samples: Vec<EvalSample>) -> Vec<(EvalLabels, f64)> {
        let mut out: Vec<(EvalLabels, f64)> =
            samples.into_iter().map(|s| (s.labels, s.value)).collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// Push-down result of `kind`: every shard accumulates its own slice into
    /// partials, the partials cross the wire, the coordinator merges and
    /// finalizes — the full round trip minus the serialization.
    fn pushed_down(
        kind: AggregationKind,
        modifier: Option<&LabelModifier>,
        shards: &[Vec<EvalSample>],
    ) -> Vec<(EvalLabels, f64)> {
        let mut coordinator = PartialGroups::new(kind);
        for shard in shards {
            let mut local = PartialGroups::new(kind);
            local.accumulate(modifier, shard.clone());
            for (labels, state) in local.into_partials() {
                coordinator.merge(labels, state);
            }
        }
        sorted(coordinator.finalize(EVAL_TS))
    }

    fn assert_close(left: &[(EvalLabels, f64)], right: &[(EvalLabels, f64)], what: &str) {
        assert_eq!(left.len(), right.len(), "{what}: group count");
        for ((l_labels, l_value), (r_labels, r_value)) in left.iter().zip(right.iter()) {
            assert_eq!(l_labels, r_labels, "{what}: labels");
            if l_value.is_nan() || r_value.is_nan() {
                assert_eq!(l_value.is_nan(), r_value.is_nan(), "{what}: NaN-ness");
                continue;
            }
            let tolerance = 1e-9 * l_value.abs().max(1.0);
            assert!(
                (l_value - r_value).abs() <= tolerance,
                "{what}: {l_value} != {r_value}"
            );
        }
    }

    const REDUCTIONS: [AggregationKind; 8] = [
        AggregationKind::Sum,
        AggregationKind::Avg,
        AggregationKind::Min,
        AggregationKind::Max,
        AggregationKind::Count,
        AggregationKind::Group,
        AggregationKind::Stddev,
        AggregationKind::Stdvar,
    ];

    /// The push-down identity: for every reduction, merging the shards' partial
    /// states and finalizing equals aggregating the concatenated input on one
    /// node. Covers grouped and ungrouped, and groups split across shards.
    #[test]
    fn test_pushdown_matches_single_node() {
        let shards = vec![
            vec![
                sample(&[("__name__", "m"), ("job", "a"), ("i", "1")], 1.0),
                sample(&[("__name__", "m"), ("job", "a"), ("i", "2")], 7.5),
                sample(&[("__name__", "m"), ("job", "b"), ("i", "3")], -2.0),
            ],
            vec![
                // same `job=a` group as shard 1: the group spans shards
                sample(&[("__name__", "m"), ("job", "a"), ("i", "4")], 4.25),
                sample(&[("__name__", "m"), ("job", "c"), ("i", "5")], 0.0),
            ],
            vec![sample(&[("__name__", "m"), ("job", "b"), ("i", "6")], 11.0)],
        ];
        let all: Vec<EvalSample> = shards.iter().flatten().cloned().collect();

        for kind in REDUCTIONS {
            for modifier in [None, Some(by(&["job"]))] {
                let what = format!("{kind:?} modifier={modifier:?}");
                assert_close(
                    &single_node(kind, modifier.as_ref(), all.clone()),
                    &pushed_down(kind, modifier.as_ref(), &shards),
                    &what,
                );
            }
        }
    }

    /// A single shard holding the whole input must reproduce the single-node
    /// result exactly — no partitioning, so no summation-order slack.
    #[test]
    fn test_single_shard_is_exact() {
        let samples = vec![
            sample(&[("job", "a")], 0.1),
            sample(&[("job", "a")], 0.2),
            sample(&[("job", "a")], 1e16),
            sample(&[("job", "a")], -1e16),
        ];

        for kind in REDUCTIONS {
            let expected = single_node(kind, None, samples.clone());
            let actual = pushed_down(kind, None, std::slice::from_ref(&samples));
            assert_eq!(expected.len(), 1);
            let (expected, actual) = (expected[0].1, actual[0].1);
            assert_eq!(
                expected.to_bits(),
                actual.to_bits(),
                "{kind:?}: {expected} != {actual}"
            );
        }
    }

    /// NaN handling must survive the split: sum/avg/stddev/stdvar propagate a
    /// NaN, min/max ignore it, count counts it.
    #[test]
    fn test_nan_propagation_matches_single_node() {
        let shards = vec![
            vec![sample(&[("job", "a")], 1.0), sample(&[("job", "a")], 3.0)],
            vec![sample(&[("job", "a")], f64::NAN)],
        ];
        let all: Vec<EvalSample> = shards.iter().flatten().cloned().collect();

        for kind in REDUCTIONS {
            assert_close(
                &single_node(kind, None, all.clone()),
                &pushed_down(kind, None, &shards),
                &format!("{kind:?}"),
            );
        }

        // An all-NaN group: min/max must yield NaN, not an arbitrary extremum.
        let nan_only = vec![vec![sample(&[("job", "a")], f64::NAN)]];
        for kind in [AggregationKind::Min, AggregationKind::Max] {
            let value = pushed_down(kind, None, &nan_only)[0].1;
            assert!(value.is_nan(), "{kind:?} of an all-NaN group must be NaN");
        }
    }

    /// A shard with no samples for a group contributes an empty state, which
    /// must merge as the identity rather than poisoning the group.
    #[test]
    fn test_empty_state_merges_as_identity() {
        for kind in REDUCTIONS {
            let mut merged = AggregationPartial::empty(kind);
            let mut populated = AggregationPartial::empty(kind);
            populated.update(kind, 4.0);
            populated.update(kind, 6.0);

            merged.merge(kind, &AggregationPartial::empty(kind));
            merged.merge(kind, &populated);
            merged.merge(kind, &AggregationPartial::empty(kind));

            assert_eq!(
                merged.finalize(kind).to_bits(),
                populated.finalize(kind).to_bits(),
                "{kind:?}"
            );
        }
    }

    /// avg keeps a running mean alongside the sum so that a sum overflowing to
    /// +Inf still finalizes to the representable mean.
    #[test]
    fn test_avg_survives_sum_overflow() {
        let big = f64::MAX / 2.0;
        let shards = vec![
            vec![sample(&[("job", "a")], big), sample(&[("job", "a")], big)],
            vec![sample(&[("job", "a")], big), sample(&[("job", "a")], big)],
        ];
        let value = pushed_down(AggregationKind::Avg, None, &shards)[0].1;
        assert!(value.is_finite(), "avg overflowed to {value}");
        assert!((value - big).abs() / big < 1e-12, "avg was {value}");
    }

    /// count_values counts are summed across shards by output label set.
    #[test]
    fn test_merge_count_values_sums_by_labelset() {
        let merged = merge_count_values(vec![
            sample(&[("job", "a"), ("v", "1")], 2.0),
            sample(&[("job", "a"), ("v", "1")], 3.0),
            sample(&[("job", "a"), ("v", "2")], 1.0),
            sample(&[("job", "b"), ("v", "1")], 4.0),
        ]);
        assert_eq!(
            sorted(merged),
            vec![
                (EvalLabels::from_pairs(&[("job", "a"), ("v", "1")]), 5.0),
                (EvalLabels::from_pairs(&[("job", "a"), ("v", "2")]), 1.0),
                (EvalLabels::from_pairs(&[("job", "b"), ("v", "1")]), 4.0),
            ]
        );
    }

    /// Only quantile lacks a decomposable form.
    #[test]
    fn test_pushdown_strategy_classification() {
        for kind in REDUCTIONS {
            assert_eq!(kind.pushdown_strategy(), Some(PushdownStrategy::Reduce));
        }
        for kind in [
            AggregationKind::Topk,
            AggregationKind::Bottomk,
            AggregationKind::Limitk,
            AggregationKind::LimitRatio,
        ] {
            assert_eq!(kind.pushdown_strategy(), Some(PushdownStrategy::Select));
        }
        assert_eq!(
            AggregationKind::CountValues.pushdown_strategy(),
            Some(PushdownStrategy::CountValues)
        );
        assert_eq!(AggregationKind::Quantile.pushdown_strategy(), None);
    }
}
