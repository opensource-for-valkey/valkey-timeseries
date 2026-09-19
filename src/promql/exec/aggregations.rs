use crate::common::Timestamp;
use crate::common::hash::IntMap;
use crate::common::math::{kahan_avg, kahan_std_dev, kahan_sum, kahan_variance, quantile};
use crate::labels::{HasFingerprint, SeriesFingerprint};
use crate::promql::exec::types::EvalLabels;
use crate::promql::functions::utils::is_valid_label_name;
use crate::promql::hashers::FingerprintHashMap;
use crate::promql::{EvalResult, EvalSample, EvaluationError, ExprResult};
use promql_parser::parser::token::{TokenType, *};
use promql_parser::parser::{AggregateExpr, LabelModifier};
use std::cmp::Ordering;
use std::collections::BinaryHeap;

#[derive(Clone, Copy, Eq, PartialEq)]
enum KAggregationOrder {
    Top,
    Bottom,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum KLimitType {
    Limit,
    LimitRatio,
}

/// A PromQL aggregation operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggregationKind {
    Sum,
    Avg,
    Min,
    Max,
    Count,
    Group,
    Stddev,
    Stdvar,
    Topk,
    Bottomk,
    CountValues,
    Quantile,
    Limitk,
    LimitRatio,
}

/// How an aggregation operator splits between the shards that hold the data and
/// the coordinator that answers the query. See
/// [`crate::promql::exec::partial_aggregation`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::promql) enum PushdownStrategy {
    /// Reductions: each shard ships one mergeable partial state per group and
    /// the coordinator merges the shards' states per group, then finalizes.
    Reduce,
    /// Idempotent selections (topk/bottomk/limitk/limit_ratio): each shard runs
    /// the operator over its local input and ships the surviving samples; the
    /// coordinator runs the same operator again over their union. Every sample
    /// the global answer could contain survives its own shard's selection, so
    /// re-selecting from the union yields that answer.
    Select,
    /// count_values: each shard ships its local per-(group, value) counts and
    /// the coordinator sums them by output label set. Re-applying the operator
    /// would count the counts, so this one gets its own merge step.
    CountValues,
}

impl AggregationKind {
    pub(in crate::promql) fn is_reduction(&self) -> bool {
        matches!(
            self,
            AggregationKind::Sum
                | AggregationKind::Avg
                | AggregationKind::Min
                | AggregationKind::Max
                | AggregationKind::Count
                | AggregationKind::Group
                | AggregationKind::Stddev
                | AggregationKind::Stdvar
        )
    }

    /// How this operator can be pushed down to the shards, or `None` when it
    /// has no decomposable form and must see the whole input on one node:
    /// `quantile` interpolates between a group's values, so no partial state
    /// smaller than the values themselves exists.
    pub(in crate::promql) fn pushdown_strategy(&self) -> Option<PushdownStrategy> {
        if self.is_reduction() {
            return Some(PushdownStrategy::Reduce);
        }
        match self {
            AggregationKind::Topk
            | AggregationKind::Bottomk
            | AggregationKind::Limitk
            | AggregationKind::LimitRatio => Some(PushdownStrategy::Select),
            AggregationKind::CountValues => Some(PushdownStrategy::CountValues),
            AggregationKind::Quantile => None,
            // Covered by is_reduction above.
            _ => Some(PushdownStrategy::Reduce),
        }
    }
}

impl TryFrom<TokenType> for AggregationKind {
    type Error = EvaluationError;

    fn try_from(token: TokenType) -> Result<Self, Self::Error> {
        match token.id() {
            T_SUM => Ok(AggregationKind::Sum),
            T_AVG => Ok(AggregationKind::Avg),
            T_MIN => Ok(AggregationKind::Min),
            T_MAX => Ok(AggregationKind::Max),
            T_COUNT => Ok(AggregationKind::Count),
            T_GROUP => Ok(AggregationKind::Group),
            T_STDDEV => Ok(AggregationKind::Stddev),
            T_STDVAR => Ok(AggregationKind::Stdvar),
            T_TOPK => Ok(AggregationKind::Topk),
            T_BOTTOMK => Ok(AggregationKind::Bottomk),
            T_COUNT_VALUES => Ok(AggregationKind::CountValues),
            T_QUANTILE => Ok(AggregationKind::Quantile),
            T_LIMITK => Ok(AggregationKind::Limitk),
            T_LIMIT_RATIO => Ok(AggregationKind::LimitRatio),
            _ => Err(EvaluationError::InternalError(format!(
                "BUG: not an aggregation operator token: {token}"
            ))),
        }
    }
}

pub(super) fn eval_aggregation(
    expr: &AggregateExpr,
    samples: Vec<EvalSample>,
    param: Option<ExprResult>,
    eval_time: Timestamp,
) -> EvalResult<ExprResult> {
    let kind = AggregationKind::try_from(expr.op)?;
    let samples = apply_aggregation(kind, expr.modifier.as_ref(), param, samples, eval_time)?;
    Ok(ExprResult::InstantVector(samples))
}

/// Apply an aggregation operator to an instant vector.
///
/// Driven by an explicit `(kind, modifier)` pair rather than an
/// [`AggregateExpr`] so that the same code serves the single-node evaluator and
/// both sides of aggregation push-down: a shard applies it to its local slice
/// of the input, and the coordinator re-applies the selection operators to the
/// union of the shards' candidates
/// (see `crate::promql::engine::AggregationFanoutCommand`).
pub(in crate::promql) fn apply_aggregation(
    kind: AggregationKind,
    modifier: Option<&LabelModifier>,
    param: Option<ExprResult>,
    samples: Vec<EvalSample>,
    eval_time: Timestamp,
) -> EvalResult<Vec<EvalSample>> {
    if samples.is_empty() {
        return Ok(Vec::new());
    }

    // A pending `__name__` drop is *not* materialized here. Prometheus removes
    // the name only when the final result is rendered, so an aggregation groups
    // on the name that is about to disappear and hands the pending drop to its
    // output groups. That is what makes
    //
    //     label_replace(sum by (__name__) (rate(m[5m])), "__name__", "$1", "__name__", "(.+)")
    //
    // able to recover the name, and it is why grouping by `__name__` over a
    // rolled-up vector can collapse two groups into one label set. Dropping the
    // name first would silently change both.
    //
    // `PartialGroups::accumulate` groups the same way, so pushed-down and local
    // aggregation agree on group membership.
    match kind {
        AggregationKind::Sum
        | AggregationKind::Avg
        | AggregationKind::Min
        | AggregationKind::Max
        | AggregationKind::Count
        | AggregationKind::Group
        | AggregationKind::Stddev
        | AggregationKind::Stdvar => Ok(eval_reduction_aggregation(
            modifier, kind, samples, eval_time,
        )),
        AggregationKind::Quantile => eval_quantile(modifier, param, samples, eval_time),
        AggregationKind::CountValues => eval_count_values(modifier, param, samples, eval_time),
        AggregationKind::Topk => {
            eval_top_bottom_k(modifier, param, samples, KAggregationOrder::Top)
        }
        AggregationKind::Bottomk => {
            eval_top_bottom_k(modifier, param, samples, KAggregationOrder::Bottom)
        }
        AggregationKind::Limitk => eval_limit_k(modifier, param, samples),
        AggregationKind::LimitRatio => eval_limit_ratio(modifier, param, samples),
    }
}

fn eval_count_values(
    modifier: Option<&LabelModifier>,
    param: Option<ExprResult>,
    samples: Vec<EvalSample>,
    timestamp_ms: Timestamp,
) -> EvalResult<Vec<EvalSample>> {
    let label_name = get_param_as_string(param, "count_values")?;
    if !is_valid_label_name(&label_name) {
        return Err(EvaluationError::InternalError(format!(
            "invalid label name {label_name:?}"
        )));
    }

    // Prometheus sets the value label on every sample *before* grouping, so a
    // value label that reuses an input label's name replaces it for grouping
    // too: `count_values without (instance) ("job", m)` groups by
    // `(group, job="<value>")`, merging `job="api-server"` and
    // `job="app-server"` samples that share a value. Grouping first and
    // setting the label afterwards emitted one series per original group and
    // tripped the duplicate-label-set check.
    //
    // Partitioning by the modifier with the value label's name taken out,
    // then by the value, is that same partition: the value label's value is
    // a function of the sample value alone.
    let adjusted = count_values_grouping(modifier, &label_name);
    let grouping = adjusted.as_ref().or(modifier);

    // Counts are keyed by the value's bit pattern and rendered once per
    // distinct value when its output series is built, rather than rendered
    // per input sample and keyed by the string. Rendering was the largest
    // single cost of the operator, and inputs usually repeat a few values.
    struct Bucket {
        labels: EvalLabels,
        value_key: u64,
        drop_name: bool,
        count: usize,
    }
    let mut buckets: IntMap<u64, Bucket> = IntMap::default();
    for sample in samples {
        let value_key = count_values_key(sample.value);
        let key = count_values_bucket_key(sample.labels.compute_grouping_key(grouping), value_key);
        let bucket = buckets.entry(key).or_insert_with(|| Bucket {
            labels: sample.labels.compute_grouping_labels(grouping),
            value_key,
            drop_name: false,
            count: 0,
        });
        bucket.drop_name |= sample.drop_name;
        bucket.count += 1;
    }

    let mut out = Vec::with_capacity(buckets.len());
    for bucket in buckets.into_values() {
        let mut labels = bucket.labels;
        labels.set(
            &label_name,
            sample_value_label(f64::from_bits(bucket.value_key)),
        );
        out.push(EvalSample {
            labels,
            timestamp_ms,
            value: bucket.count as f64,
            drop_name: bucket.drop_name,
        });
    }
    Ok(out)
}

/// The grouping `count_values` partitions by before its value label is set,
/// when that differs from `modifier`: `by` with the value label's name taken
/// out of the list, `without` with it added. `None` means `modifier` already
/// serves — it never names the value label (`by`), already excludes it
/// (`without`), or is absent (group by nothing, then set the label).
fn count_values_grouping(
    modifier: Option<&LabelModifier>,
    label_name: &str,
) -> Option<LabelModifier> {
    use promql_parser::label::Labels as List;
    match modifier {
        None => None,
        Some(LabelModifier::Include(list)) => {
            let named = list.labels.iter().any(|name| name == label_name);
            named.then(|| {
                let labels = list
                    .labels
                    .iter()
                    .filter(|name| *name != label_name)
                    .cloned()
                    .collect();
                LabelModifier::Include(List { labels })
            })
        }
        Some(LabelModifier::Exclude(list)) => {
            let named = list.labels.iter().any(|name| name == label_name);
            (!named).then(|| {
                let mut labels = list.labels.clone();
                labels.push(label_name.to_string());
                LabelModifier::Exclude(List { labels })
            })
        }
    }
}

/// One map key for a `(group, value)` pair, identifying the pair by a 64-bit
/// hash the way group fingerprints identify groups everywhere else.
///
/// The map does no hashing of its own, so this has to mix: float bit
/// patterns of nearby values share their high bits, which is where hashbrown
/// takes its tag byte from, and the fingerprint alone must not decide the
/// bucket. The finalizer is MurmurHash3's `fmix64`.
#[inline]
fn count_values_bucket_key(group: SeriesFingerprint, value_key: u64) -> u64 {
    // Fold the 128-bit fingerprint as `FingerprintHasher::write_u128` does.
    let group = (group as u64) ^ ((group >> 64) as u64).rotate_left(32);
    let mut x = group ^ value_key.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    x ^ (x >> 33)
}

/// The `count_values` key of a sample value: its bit pattern, with every NaN
/// folded onto one pattern.
///
/// Two values share a key exactly when [`sample_value_label`] renders them
/// the same. Distinct finite values render distinctly (shortest round-trip
/// formatting), `-0` and `0` render differently and have different bits, and
/// the two infinities are single patterns. Only NaN has many bit patterns
/// behind one rendering, so it is the one case that needs folding; without
/// it a group holding two NaN payloads would emit two series with the same
/// label set.
#[inline]
fn count_values_key(value: f64) -> u64 {
    if value.is_nan() {
        f64::NAN.to_bits()
    } else {
        value.to_bits()
    }
}

#[derive(Clone, Copy)]
struct KHeapEntry {
    value: f64,
    index: usize,
    order: KAggregationOrder,
}

impl PartialEq for KHeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index
            && self.order == other.order
            && self.value.to_bits() == other.value.to_bits()
    }
}

impl Eq for KHeapEntry {}

impl PartialOrd for KHeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for KHeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap. Define "greater" as "worse" so heap.peek()
        // returns the least desirable currently selected sample.
        compare_k_values(self.value, other.value, self.order)
            .then_with(|| self.index.cmp(&other.index))
    }
}

fn select_k_indices_with_heap(
    samples: &[EvalSample],
    keep: usize,
    order: KAggregationOrder,
) -> Vec<usize> {
    if keep == 0 || samples.is_empty() {
        return Vec::new();
    }
    let mut heap = BinaryHeap::with_capacity(keep);
    for (idx, sample) in samples.iter().enumerate() {
        let entry = KHeapEntry {
            value: sample.value,
            index: idx,
            order,
        };
        if heap.len() < keep {
            heap.push(entry);
            continue;
        }

        if let Some(worst) = heap.peek()
            && compare_k_values(sample.value, worst.value, order).is_lt()
        {
            // Replace only when the candidate outranks the current worst,
            // preserving the "peek is worst-kept" invariant.
            heap.pop();
            heap.push(entry);
        }
    }
    heap.into_iter().map(|entry| entry.index).collect()
}

fn eval_top_bottom_k(
    modifier: Option<&LabelModifier>,
    param: Option<ExprResult>,
    samples: Vec<EvalSample>,
    order: KAggregationOrder,
) -> EvalResult<Vec<EvalSample>> {
    let name = if order == KAggregationOrder::Top {
        "topk"
    } else {
        "bottomk"
    };
    let k = get_k_param(param, samples.len(), name)?;

    if k == 0 {
        return Ok(Vec::new());
    }

    if modifier.is_none() {
        return Ok(select_k_from_group(samples, k, order));
    }

    // Serial: see `eval_reduction_aggregation` for the measurements.
    let out: Vec<EvalSample> = group_samples(modifier, samples)
        .into_iter()
        .flat_map(|(_, group)| select_k_from_group(group.members, k, order))
        .collect();

    Ok(out)
}

fn select_k_from_group(
    mut samples: Vec<EvalSample>,
    k: usize,
    order: KAggregationOrder,
) -> Vec<EvalSample> {
    let keep = k.min(samples.len());
    let mut selected_indices = select_k_indices_with_heap(&samples, keep, order);
    // Remove from highest index first so swap_remove cannot invalidate
    // indices we still need to read.
    selected_indices.sort_unstable_by(|left, right| right.cmp(left));

    let mut result = Vec::with_capacity(selected_indices.len());
    for idx in selected_indices {
        result.push(samples.swap_remove(idx));
    }
    result.sort_by(|left, right| compare_k_values(left.value, right.value, order));
    result
}

fn eval_limit_k(
    modifier: Option<&LabelModifier>,
    param: Option<ExprResult>,
    samples: Vec<EvalSample>,
) -> EvalResult<Vec<EvalSample>> {
    let k = get_k_param(param, samples.len(), "limitk")?;

    // For each group take the k samples with the smallest label hashes, then
    // flatten the per-group selections into the output vector.
    // Serial: see `eval_reduction_aggregation` for the measurements.
    let out: Vec<EvalSample> = group_samples(modifier, samples)
        .into_iter()
        .flat_map(|(_, group)| select_limitk(group.members, k))
        .collect();

    Ok(out)
}

fn eval_limit_ratio(
    modifier: Option<&LabelModifier>,
    param: Option<ExprResult>,
    samples: Vec<EvalSample>,
) -> EvalResult<Vec<EvalSample>> {
    let k = get_param_as_scalar(param, "limit_ratio")?;

    let groups = group_samples(modifier, samples);
    let mut out = Vec::new();

    // todo: parallelize
    for (_, group) in groups.into_iter() {
        out.extend(select_limit_ratio(group.members, k)?);
    }

    Ok(out)
}

fn eval_quantile(
    modifier: Option<&LabelModifier>,
    param: Option<ExprResult>,
    samples: Vec<EvalSample>,
    eval_time: Timestamp,
) -> EvalResult<Vec<EvalSample>> {
    let phi = get_param_as_scalar(param, "quantile")?;
    // make sure it's in range
    if !(0.0..=1.0).contains(&phi) {
        return Err(EvaluationError::ArgumentError(
            "quantile must be between 0.0 and 1.0".to_string(),
        ));
    }
    let groups = group_samples(modifier, samples);

    // Serial: see `eval_reduction_aggregation` for the measurements.
    let out: Vec<EvalSample> = groups
        .into_iter()
        .map(|(_, group)| {
            let value = sample_quantile(&group.members, phi);
            EvalSample {
                labels: group.labels,
                timestamp_ms: eval_time,
                value,
                drop_name: group.drop_name,
            }
        })
        .collect();

    Ok(out)
}

fn sample_quantile(samples: &[EvalSample], phi: f64) -> f64 {
    let mut values = samples
        .iter()
        .map(|sample| sample.value)
        .collect::<Vec<_>>();
    quantile(&mut values, phi)
}

fn eval_reduction_aggregation(
    modifier: Option<&LabelModifier>,
    kind: AggregationKind,
    samples: Vec<EvalSample>,
    timestamp_ms: Timestamp,
) -> Vec<EvalSample> {
    let groups = group_sample_values(modifier, samples);

    // Serial, like every other per-group step in this file. These four sites
    // (this one, `eval_quantile`, `eval_top_bottom_k`, `eval_limit_k`) used to
    // fan the per-group work out with `iter_into_par()`. Measured serial
    // against parallel on the same build (M2, release), over `groups x
    // samples-per-group`, parallel never won:
    //
    // | op       | 10x11 | 1x22000 | 11x2000 | 2000x11 |
    // |----------|-------|---------|---------|---------|
    // | sum      | 2.0x  | 1.06x   | 1.00x   | 1.05x   |
    // | quantile | 6.7x  | 1.00x   | 4.2x    | 4.4x    |
    // | topk     | 6.0x  | 1.00x   | 4.2x    | 3.8x    |
    // | limitk   | 4.5x  | 0.99x   | 3.4x    | 3.1x    |
    //
    // (parallel time / serial time; >1 is a loss for parallel.)
    //
    // Small inputs lose to the fan-out itself, ~30us against a few
    // microseconds of work. The large multi-group shapes are the telling
    // ones: they are the only inputs where a fan-out has anything to split,
    // and the three selection operators lose 3-4x there. Their groups hold
    // whole samples, and the samples a group does *not* select are freed
    // inside the worker — an allocator round-trip on a thread that did not
    // allocate it, for every dropped sample. `sum` groups hold bare `f64`s,
    // free nothing, and sit at parity, which is the fan-out's ceiling here.
    //
    // That table was taken on orx-parallel's default runner, which spawned OS
    // threads per call. Re-measured 2026-09-18 on the persistent rayon pool
    // (`iter_into_par_rayon`, interleaved in one binary): the runner was not
    // the reason. quantile/topk/limitk still lost 1.4-5.4x at every shape up
    // to 100x2000 and 11x20000, `sum` was 1.0x at every large shape, and a
    // chunked parallel *grouping* lost 3x at 22k samples. What the fan-out
    // was spreading across threads was the per-sample drop of a label set
    // allocated per series per query (~40-65 ns, about half of this
    // function's ~90 ns/sample), which contends on shared interned-string
    // counters. `EvalLabels::interned` now shares the series' own slice
    // instead, which removed that cost outright (group_samples 22k-sample
    // shapes -26..-37%); the fan-out has even less to split than before.
    groups
        .into_iter()
        .map(|(_, group)| {
            let value = aggregate_group(kind, &group.members);
            EvalSample {
                labels: group.labels,
                value,
                timestamp_ms,
                drop_name: group.drop_name,
            }
        })
        .collect()
}

fn aggregate_group(kind: AggregationKind, samples: &[f64]) -> f64 {
    match kind {
        AggregationKind::Sum => kahan_sum(samples),
        AggregationKind::Avg => kahan_avg(samples),
        AggregationKind::Min => samples
            .iter()
            .copied()
            .reduce(min_ignore_nan)
            .unwrap_or(f64::NAN),
        AggregationKind::Max => samples
            .iter()
            .copied()
            .reduce(max_ignore_nan)
            .unwrap_or(f64::NAN),
        AggregationKind::Count => samples.len() as f64,
        AggregationKind::Group => 1.0,
        AggregationKind::Stddev => kahan_std_dev(samples),
        AggregationKind::Stdvar => kahan_variance(samples),
        _ => {
            unreachable!("BUG: non-reduction aggregation kind reached aggregate_group")
        }
    }
}

/// One aggregation group.
struct Group<T> {
    labels: EvalLabels,
    /// True when any member still owes a `__name__` drop, which the group
    /// inherits. See [`apply_aggregation`] for why the drop happens after the
    /// aggregation rather than before it.
    drop_name: bool,
    members: Vec<T>,
}

impl<T> Group<T> {
    fn new(labels: EvalLabels) -> Self {
        Self {
            labels,
            drop_name: false,
            members: Vec::new(),
        }
    }
}

/// Group whole samples, for the selection operators (topk/bottomk/limitk).
///
/// Sequential, like [`group_sample_values`] — which serves the far hotter
/// reduction path and always was. This used to fan the keying out with
/// `into_par()`, but the per-sample cost that fan-out was parallelizing was
/// mostly the label allocation that [`EvalLabels::compute_grouping_key`] now
/// avoids; what remains is a hash.
///
/// Measured, on the `group_samples` bench group (interleaved A/B, two rounds,
/// keeping the allocation-free key on both sides so only the parallelism
/// differs). Restoring the fan-out costs **+15%** at 110 samples and **+12%**
/// at 1100 — the ordinary cardinalities — and buys at most ~3.5% back at
/// 22000, and only for the many-small-groups shape (`without (le)`). It is
/// not worth a size threshold here: unlike the binary-op path, which pays the
/// fan-out twice per step inside an already-parallel step loop, this runs once
/// per evaluation, so the ceiling on the win is small and the floor is a
/// double-digit regression on the common case.
fn group_samples(
    modifier: Option<&LabelModifier>,
    samples: Vec<EvalSample>,
) -> FingerprintHashMap<Group<EvalSample>> {
    let mut groups: FingerprintHashMap<Group<EvalSample>> = FingerprintHashMap::default();
    for sample in samples {
        let key = sample.labels.compute_grouping_key(modifier);
        let entry = groups
            .entry(key)
            .or_insert_with(|| Group::new(sample.labels.compute_grouping_labels(modifier)));
        entry.drop_name |= sample.drop_name;
        entry.members.push(sample);
    }
    groups
}

fn group_sample_values(
    modifier: Option<&LabelModifier>,
    samples: Vec<EvalSample>,
) -> FingerprintHashMap<Group<f64>> {
    let mut groups: FingerprintHashMap<Group<f64>> = FingerprintHashMap::default();

    for sample in samples {
        // Key every sample without allocating; build the group's label set
        // only when the group is new. See `compute_grouping_key`.
        let key = sample.labels.compute_grouping_key(modifier);

        let entry = groups
            .entry(key)
            .or_insert_with(|| Group::new(sample.labels.compute_grouping_labels(modifier)));
        entry.drop_name |= sample.drop_name;
        entry.members.push(sample.value);
    }

    groups
}

fn sample_value_label(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value == f64::INFINITY {
        "+Inf".to_string()
    } else if value == f64::NEG_INFINITY {
        "-Inf".to_string()
    } else {
        value.to_string()
    }
}

/// NaN-ignoring minimum. NaN is the identity element (`min_ignore_nan(NaN, v)
/// == v`), which is what lets the push-down partial fold start from NaN and
/// still match the single-node `reduce(min_ignore_nan)`.
pub(super) fn min_ignore_nan(lhs: f64, rhs: f64) -> f64 {
    match (lhs.is_nan(), rhs.is_nan()) {
        (true, true) => f64::NAN,
        (true, false) => rhs,
        (false, true) => lhs,
        (false, false) => lhs.min(rhs),
    }
}

/// NaN-ignoring maximum; see [`min_ignore_nan`].
pub(super) fn max_ignore_nan(lhs: f64, rhs: f64) -> f64 {
    match (lhs.is_nan(), rhs.is_nan()) {
        (true, true) => f64::NAN,
        (true, false) => rhs,
        (false, true) => lhs,
        (false, false) => lhs.max(rhs),
    }
}

/// Compares values for topk/bottomk aggregation.
/// NaN values are always considered "greater" (sorted last) regardless of order.
/// Uses partial_cmp for IEEE 754 semantics (-0.0 == +0.0), matching Prometheus.
fn compare_k_values(left: f64, right: f64, order: KAggregationOrder) -> Ordering {
    match (left.is_nan(), right.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => match order {
            KAggregationOrder::Top => right.partial_cmp(&left).unwrap_or(Ordering::Equal),
            KAggregationOrder::Bottom => left.partial_cmp(&right).unwrap_or(Ordering::Equal),
        },
    }
}

// topk/bottomk/limitk params are scalar floats, but selection needs a bounded
// count. Mirrors Prometheus: k <= 0 selects nothing, +Inf keeps everything
// (`as i64` saturates, matching Prometheus's MaxInt64 clamp), and k is capped
// at the input size. NaN is rejected earlier by `get_k_param`.
fn coerce_k_size(k_param: f64, input_len: usize) -> usize {
    let max_k = input_len as i64;
    let coerced = (k_param as i64).min(max_k);
    if coerced < 1 { 0 } else { coerced as usize }
}

/// The `k` samples of a group whose label hashes are smallest, in hash order.
///
/// Hashing the labels is what costs here, so it happens exactly once per
/// sample. The previous `sort_by_key(sample_hash)` recomputed the fingerprint
/// on *every comparison* — O(n log n) label hashes to answer a question that
/// needs n of them, which is why `limitk` over 22000 samples cost ~2x what the
/// other selection operators did.
///
/// Selection is partial: only the boundary is resolved (`select_nth_unstable`,
/// O(n) average), and only the surviving `k` are ordered. Same subset and same
/// order as the full sort produced — the sole difference is which of two
/// samples wins a *tie* in the 128-bit label fingerprint, i.e. duplicate label
/// sets within one group, which selectors do not produce.
fn select_limitk(samples: Vec<EvalSample>, k: usize) -> Vec<EvalSample> {
    let keep = k.min(samples.len());
    if keep == 0 {
        return Vec::new();
    }
    let mut keyed: Vec<(u128, EvalSample)> = samples
        .into_iter()
        .map(|sample| (sample_hash(&sample), sample))
        .collect();
    if keep < keyed.len() {
        keyed.select_nth_unstable_by_key(keep, |(hash, _)| *hash);
        keyed.truncate(keep);
    }
    keyed.sort_unstable_by_key(|(hash, _)| *hash);
    keyed.into_iter().map(|(_, sample)| sample).collect()
}

fn select_limit_ratio(samples: Vec<EvalSample>, ratio: f64) -> EvalResult<Vec<EvalSample>> {
    // Prometheus rejects NaN ratios and clamps everything else (including
    // ±Inf) to [-1, 1], emitting a warning annotation we don't support.
    if ratio.is_nan() {
        return Err(EvaluationError::ArgumentError(
            "Ratio value is NaN".to_string(),
        ));
    }
    let ratio = ratio.clamp(-1.0, 1.0);

    if ratio == 0.0 {
        return Ok(Vec::new());
    }

    // One hash per sample, kept alongside it: the caller used to sort the whole
    // group by `sample_hash` before filtering, which recomputed the fingerprint
    // per comparison and then per surviving sample. The filter never depended
    // on that order — only the output order did, which sorting the survivors
    // reproduces for a fraction of the work.
    let max = u128::MAX as f64;
    let keep: Box<dyn Fn(u128) -> bool> = if ratio > 0.0 {
        Box::new(move |hash| (hash as f64) / max < ratio)
    } else {
        // For negative ratios, select the complement side of the hash space.
        let threshold = 1.0 + ratio;
        Box::new(move |hash| (hash as f64) / max >= threshold)
    };

    let mut selected: Vec<(u128, EvalSample)> = samples
        .into_iter()
        .map(|sample| (sample_hash(&sample), sample))
        .filter(|(hash, _)| keep(*hash))
        .collect();
    selected.sort_unstable_by_key(|(hash, _)| *hash);
    Ok(selected.into_iter().map(|(_, sample)| sample).collect())
}

fn get_param(param: Option<ExprResult>, function_name: &str) -> EvalResult<ExprResult> {
    if param.is_none() {
        return Err(EvaluationError::ArgumentError(format!(
            "{function_name} requires a parameter, but none was provided"
        )));
    }
    Ok(param.unwrap())
}

fn get_param_as_scalar(param: Option<ExprResult>, function_name: &str) -> EvalResult<f64> {
    let param = get_param(param, function_name)?;
    match param {
        ExprResult::Scalar(value) => Ok(value),
        other => {
            let value_type = other.value_type();
            Err(EvaluationError::ArgumentError(format!(
                "{function_name} parameter must evaluate to a scalar, but got {value_type}"
            )))
        }
    }
}

fn get_param_as_string(params: Option<ExprResult>, function_name: &str) -> EvalResult<String> {
    let param = get_param(params, function_name)?;
    match param {
        ExprResult::String(value) => Ok(value),
        other => {
            let value_type = other.value_type();
            Err(EvaluationError::ArgumentError(format!(
                "{function_name} parameter must evaluate to a string, but got {value_type}"
            )))
        }
    }
}

fn get_k_param(param: Option<ExprResult>, sample_len: usize, name: &str) -> EvalResult<usize> {
    let value = get_param_as_scalar(param, name)?;
    // Prometheus rejects NaN k parameters for topk/bottomk/limitk.
    if value.is_nan() {
        return Err(EvaluationError::ArgumentError(
            "Parameter value is NaN".to_string(),
        ));
    }
    Ok(coerce_k_size(value, sample_len))
}

fn sample_hash(sample: &EvalSample) -> u128 {
    sample.labels.fingerprint()
}

#[cfg(test)]
mod limit_selection_tests {
    use super::*;
    use crate::labels::Labels;

    fn sample(i: usize) -> EvalSample {
        EvalSample {
            timestamp_ms: 0,
            value: i as f64,
            labels: EvalLabels::from(
                Labels::from_pairs(&[("__name__", "m"), ("l", &i.to_string())]).0,
            ),
            drop_name: false,
        }
    }

    fn names(samples: &[EvalSample]) -> Vec<String> {
        samples.iter().map(|s| s.labels.to_string()).collect()
    }

    /// The oracle: what the previous implementation did — sort the whole group
    /// by label hash, then take the first `k`.
    fn full_sort_then_truncate(mut samples: Vec<EvalSample>, k: usize) -> Vec<EvalSample> {
        samples.sort_by_key(sample_hash);
        samples.truncate(k);
        samples
    }

    /// `select_limitk` replaced a full sort with a partial selection. It must
    /// pick the same subset *and* present it in the same order.
    #[test]
    fn limitk_matches_a_full_sort_for_every_k() {
        let samples: Vec<EvalSample> = (0..64).map(sample).collect();
        for k in [0, 1, 2, 7, 31, 63, 64, 65, 1000] {
            assert_eq!(
                names(&select_limitk(samples.clone(), k)),
                names(&full_sort_then_truncate(samples.clone(), k)),
                "k = {k}"
            );
        }
        // Degenerate group sizes.
        for n in [0usize, 1, 2] {
            let small: Vec<EvalSample> = (0..n).map(sample).collect();
            for k in [0, 1, 2, 3] {
                assert_eq!(
                    names(&select_limitk(small.clone(), k)),
                    names(&full_sort_then_truncate(small.clone(), k)),
                    "n = {n}, k = {k}"
                );
            }
        }
    }

    /// `limit_ratio` no longer sorts before filtering, because the filter never
    /// depended on the order — only the output did.
    #[test]
    fn limit_ratio_matches_sorting_before_filtering() {
        let samples: Vec<EvalSample> = (0..64).map(sample).collect();
        let max = u128::MAX as f64;
        for ratio in [1.0, 0.75, 0.5, 0.25, -0.25, -0.5, -1.0] {
            let mut sorted = samples.clone();
            sorted.sort_by_key(sample_hash);
            let want: Vec<EvalSample> = sorted
                .into_iter()
                .filter(|s| {
                    let h = sample_hash(s) as f64 / max;
                    if ratio > 0.0 {
                        h < ratio
                    } else {
                        h >= 1.0 + ratio
                    }
                })
                .collect();
            assert_eq!(
                names(&select_limit_ratio(samples.clone(), ratio).unwrap()),
                names(&want),
                "ratio = {ratio}"
            );
        }
        // A zero ratio selects nothing; NaN is rejected.
        assert!(select_limit_ratio(samples.clone(), 0.0).unwrap().is_empty());
        assert!(select_limit_ratio(samples, f64::NAN).is_err());
    }
}

#[cfg(test)]
mod count_values_tests {
    use super::*;
    use crate::labels::Labels;
    use std::collections::BTreeMap;

    fn sample(value: f64) -> EvalSample {
        EvalSample {
            timestamp_ms: 0,
            value,
            labels: EvalLabels::from(Labels::from_pairs(&[("__name__", "m")]).0),
            drop_name: false,
        }
    }

    /// `count_values` output as `rendered value -> count`, asserting along the
    /// way that no two output series share a label set.
    fn counts(values: &[f64]) -> BTreeMap<String, f64> {
        let out = eval_count_values(
            None,
            Some(ExprResult::String("v".to_string())),
            values.iter().copied().map(sample).collect(),
            0,
        )
        .unwrap();
        let mut by_label = BTreeMap::new();
        for s in out {
            let v = s.labels.get("v").unwrap().to_string();
            assert!(
                by_label.insert(v.clone(), s.value).is_none(),
                "duplicate output series for value {v}"
            );
        }
        by_label
    }

    /// Keys are bit patterns, so what must be shown is that they merge and
    /// split exactly as the rendered strings do.
    #[test]
    fn keys_agree_with_the_rendering() {
        let nan_with_payload = f64::from_bits(f64::NAN.to_bits() | 0x1234);
        assert!(nan_with_payload.is_nan());
        assert_ne!(nan_with_payload.to_bits(), f64::NAN.to_bits());

        let got = counts(&[
            1.5,
            1.5,
            2.0,
            f64::NAN,
            nan_with_payload,
            -f64::NAN,
            -0.0,
            0.0,
            f64::INFINITY,
            f64::NEG_INFINITY,
            0.1 + 0.2,
        ]);
        let want: BTreeMap<String, f64> = [
            ("1.5", 2.0),
            ("2", 1.0),
            ("NaN", 3.0),
            ("-0", 1.0),
            ("0", 1.0),
            ("+Inf", 1.0),
            ("-Inf", 1.0),
            ("0.30000000000000004", 1.0),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
        assert_eq!(got, want);
    }

    fn labeled(pairs: &[(&str, &str)], value: f64) -> EvalSample {
        EvalSample {
            timestamp_ms: 0,
            value,
            labels: EvalLabels::from(Labels::from_pairs(pairs).0),
            drop_name: false,
        }
    }

    /// Output as sorted `(label set, count)` strings.
    fn run(modifier: Option<&LabelModifier>, label: &str, samples: Vec<EvalSample>) -> Vec<String> {
        let mut out: Vec<String> = eval_count_values(
            modifier,
            Some(ExprResult::String(label.to_string())),
            samples,
            0,
        )
        .unwrap()
        .into_iter()
        .map(|s| format!("{} {}", s.labels, s.value))
        .collect();
        out.sort();
        out
    }

    fn fleet() -> Vec<EvalSample> {
        vec![
            labeled(&[("job", "api"), ("instance", "0"), ("group", "prod")], 6.0),
            labeled(&[("job", "api"), ("instance", "1"), ("group", "prod")], 6.0),
            labeled(
                &[("job", "api"), ("instance", "0"), ("group", "canary")],
                8.0,
            ),
            labeled(&[("job", "app"), ("instance", "0"), ("group", "prod")], 6.0),
            labeled(
                &[("job", "app"), ("instance", "0"), ("group", "canary")],
                7.0,
            ),
        ]
    }

    /// A value label that reuses an input label's name replaces it before
    /// grouping, as in Prometheus, so groups that differed only by that label
    /// merge instead of emitting duplicate label sets.
    #[test]
    fn value_label_overrides_a_grouping_label() {
        use promql_parser::label::Labels as List;

        let without_instance = LabelModifier::Exclude(List::new(vec!["instance"]));
        assert_eq!(
            run(Some(&without_instance), "job", fleet()),
            vec![
                "{group=\"canary\",job=\"7\"} 1",
                "{group=\"canary\",job=\"8\"} 1",
                "{group=\"prod\",job=\"6\"} 3",
            ]
        );

        let by_job_group = LabelModifier::Include(List::new(vec!["job", "group"]));
        assert_eq!(
            run(Some(&by_job_group), "job", fleet()),
            vec![
                "{group=\"canary\",job=\"7\"} 1",
                "{group=\"canary\",job=\"8\"} 1",
                "{group=\"prod\",job=\"6\"} 3",
            ]
        );

        // A fresh label name leaves the modifier's grouping untouched.
        assert_eq!(
            run(Some(&without_instance), "version", fleet()),
            vec![
                "{group=\"canary\",job=\"api\",version=\"8\"} 1",
                "{group=\"canary\",job=\"app\",version=\"7\"} 1",
                "{group=\"prod\",job=\"api\",version=\"6\"} 2",
                "{group=\"prod\",job=\"app\",version=\"6\"} 1",
            ]
        );
        assert_eq!(
            run(None, "version", fleet()),
            vec![
                "{version=\"6\"} 3",
                "{version=\"7\"} 1",
                "{version=\"8\"} 1",
            ]
        );
    }

    /// The key folds only NaN; every other value keeps its own bits.
    #[test]
    fn key_folds_nan_only() {
        assert_eq!(count_values_key(f64::NAN), count_values_key(-f64::NAN));
        assert_ne!(count_values_key(0.0), count_values_key(-0.0));
        assert_ne!(count_values_key(1.0), count_values_key(1.0 + f64::EPSILON));
        assert_eq!(count_values_key(2.5), 2.5f64.to_bits());
    }
}
