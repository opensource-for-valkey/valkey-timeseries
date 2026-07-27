use crate::common::Timestamp;
use crate::common::math::{kahan_avg, kahan_std_dev, kahan_sum, kahan_variance, quantile};
use crate::labels::HasFingerprint;
use crate::promql::exec::types::EvalLabels;
use crate::promql::hashers::FingerprintHashMap;
use crate::promql::{EvalResult, EvalSample, EvaluationError, ExprResult};
use orx_parallel::ParIter;
use orx_parallel::{IntoParIter, IterIntoParIter};
use promql_parser::parser::token::{TokenType, *};
use promql_parser::parser::{AggregateExpr, LabelModifier};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap};

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
    let groups = group_sample_values(modifier, samples);
    let mut out = Vec::new();
    for (_, group) in groups {
        let mut counts = BTreeMap::new();
        for value in group.members {
            *counts.entry(sample_value_label(value)).or_insert(0usize) += 1;
        }

        for (value_label, count) in counts {
            let mut labels = group.labels.clone();
            labels.set(&label_name, value_label);
            out.push(EvalSample {
                labels,
                timestamp_ms,
                value: count as f64,
                drop_name: group.drop_name,
            });
        }
    }
    Ok(out)
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

    let out: Vec<EvalSample> = group_samples(modifier, samples)
        .into_iter()
        .map(|(_, group)| group.members)
        .iter_into_par()
        .flat_map(|group| select_k_from_group(group, k, order))
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

    // For each group sort deterministically by hash and take the first k samples,
    // then flatten the per-group selections into the output vector.
    let out: Vec<EvalSample> = group_samples(modifier, samples)
        .into_iter()
        .iter_into_par()
        .flat_map(|(_, group)| {
            let mut group_samples = group.members;
            group_samples.sort_by_key(sample_hash);
            select_limitk(group_samples, k)
        })
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
        let mut group_samples = group.members;
        group_samples.sort_by_key(sample_hash);
        let selected = select_limit_ratio(group_samples, k)?;

        out.extend(selected);
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

    let out: Vec<EvalSample> = groups
        .into_iter()
        .map(|(_, group)| group)
        .iter_into_par()
        .map(|group| {
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

    groups
        .into_iter()
        .map(|(_, group)| group)
        .iter_into_par()
        .map(|group| {
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

fn group_samples(
    modifier: Option<&LabelModifier>,
    samples: Vec<EvalSample>,
) -> FingerprintHashMap<Group<EvalSample>> {
    let keyed: Vec<_> = samples
        .into_par()
        .map(|sample| {
            let labels = sample.labels.compute_grouping_labels(modifier);
            let key = labels.fingerprint();
            (key, labels, sample)
        })
        .collect();

    let mut groups: FingerprintHashMap<Group<EvalSample>> = FingerprintHashMap::default();
    for (key, labels, sample) in keyed {
        let entry = groups.entry(key).or_insert_with(|| Group::new(labels));
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
        let labels = sample.labels.compute_grouping_labels(modifier);
        let key = labels.fingerprint();

        let entry = groups.entry(key).or_insert_with(|| Group::new(labels));
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

fn select_limitk(samples: Vec<EvalSample>, k: usize) -> Vec<EvalSample> {
    let mut samples = samples;
    samples.truncate(k);
    samples
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

    let max = u128::MAX as f64;
    if ratio > 0.0 {
        Ok(samples
            .into_iter()
            .filter(|sample| (sample_hash(sample) as f64) / max < ratio)
            .collect())
    } else {
        // For negative ratios, select the complement side of the hash space.
        let threshold = 1.0 + ratio;
        Ok(samples
            .into_iter()
            .filter(|sample| (sample_hash(sample) as f64) / max >= threshold)
            .collect())
    }
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
