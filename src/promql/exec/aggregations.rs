use crate::common::Timestamp;
use crate::common::math::{kahan_avg, kahan_std_dev, kahan_sum, kahan_variance, quantile};
use crate::promql::hashers::FingerprintHashMap;
use crate::promql::{EvalResult, EvalSample, EvaluationError, ExprResult, Labels};
use ahash::AHasher;
use orx_parallel::{IntoParIter, IterIntoParIter};
use orx_parallel::ParIter;
use promql_parser::label::METRIC_NAME;
use promql_parser::parser::AggregateExpr;
use promql_parser::parser::token::{TokenType, *};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};

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

pub(super) fn eval_aggregation(
    expr: &AggregateExpr,
    mut samples: Vec<EvalSample>,
    param: Option<ExprResult>,
    eval_time: Timestamp,
) -> EvalResult<ExprResult> {
    if samples.is_empty() {
        return Ok(ExprResult::InstantVector(Vec::new()));
    }

    // Materialize any pending __name__ drops on inner expression results before aggregation
    // so that grouping and aggregation operate on the correct label sets (Prometheus semantics).
    for sample in samples.iter_mut() {
        if sample.drop_name {
            sample.labels.remove(METRIC_NAME);
        }
    }

    if is_reduction_aggregate(expr.op) {
        return eval_reduction_aggregation(expr, samples, eval_time);
    }

    match expr.op.id() {
        T_QUANTILE => eval_quantile(expr, param, samples, eval_time),
        T_COUNT_VALUES => eval_count_values(expr, param, samples, eval_time),
        T_TOPK => eval_top_bottom_k(expr, param, samples, KAggregationOrder::Top),
        T_BOTTOMK => eval_top_bottom_k(expr, param, samples, KAggregationOrder::Bottom),
        T_LIMITK => eval_limit_k(expr, param, samples),
        T_LIMIT_RATIO => eval_limit_ratio(expr, param, samples),
        _ => {
            let msg = format!(
                "BUG: Invalid token ID in eval_aggregation: {:?}",
                expr.op.id()
            );
            Err(EvaluationError::InternalError(msg))
        }
    }
}

fn eval_count_values(
    expr: &AggregateExpr,
    param: Option<ExprResult>,
    samples: Vec<EvalSample>,
    timestamp_ms: Timestamp,
) -> EvalResult<ExprResult> {
    let label_name = get_param_as_string(param, "count_values")?;
    let groups = group_sample_values(expr, samples);
    let mut out = Vec::new();
    for (_, (labels, group_samples)) in groups {
        let mut counts = BTreeMap::new();
        for value in group_samples {
            *counts.entry(sample_value_label(value)).or_insert(0usize) += 1;
        }

        for (value_label, count) in counts {
            let mut labels = labels.clone();
            labels.set(&label_name, value_label);
            out.push(EvalSample {
                labels,
                timestamp_ms,
                value: count as f64,
                drop_name: false,
            });
        }
    }
    Ok(ExprResult::InstantVector(out))
}

fn eval_top_bottom_k(
    expr: &AggregateExpr,
    param: Option<ExprResult>,
    samples: Vec<EvalSample>,
    order: KAggregationOrder,
) -> EvalResult<ExprResult> {
    let name = if order == KAggregationOrder::Top { "top_k" } else { "bottom_k" };
    let k = get_k_param(param, samples.len(), name)?;

    if k == 0 {
        return Ok(ExprResult::InstantVector(Vec::new()));
    }

    if expr.modifier.is_none() {
        return Ok(ExprResult::InstantVector(select_k_from_group(samples, k, order)));
    }

    let out: Vec<EvalSample> = group_samples(expr, samples)
        .into_iter()
        .map(|(_, (_, group))| group)
        .iter_into_par()
        .flat_map(|group| select_k_from_group(group, k, order))
        .collect();

    Ok(ExprResult::InstantVector(out))
}

fn select_k_from_group(
    mut group_samples: Vec<EvalSample>,
    k: usize,
    order: KAggregationOrder,
) -> Vec<EvalSample> {
    // If k is greater than or equal to the number of samples, return all samples.
    if k >= group_samples.len() {
        return group_samples;
    }

    group_samples.sort_by(|a, b| compare_k_values(a.value, b.value, order));
    group_samples.truncate(k);

    group_samples
}

fn eval_limit_k(
    expr: &AggregateExpr,
    param: Option<ExprResult>,
    samples: Vec<EvalSample>,
) -> EvalResult<ExprResult> {
    let k = get_k_param(param, samples.len(), "limitk")?;

    // For each group sort deterministically by hash and take the first k samples,
    // then flatten the per-group selections into the output vector.
    let out: Vec<EvalSample> = group_samples(expr, samples)
        .into_iter()
        .iter_into_par()
        .flat_map(|(_, (_, mut group_samples))| {
            group_samples.sort_by_key(sample_hash);
            select_limitk(group_samples, k)
        })
        .collect();

    Ok(ExprResult::InstantVector(out))
}

fn eval_limit_ratio(
    expr: &AggregateExpr,
    param: Option<ExprResult>,
    samples: Vec<EvalSample>,
) -> EvalResult<ExprResult> {
    let k = get_param_as_scalar(param, "limit_ratio")?;

    let groups = group_samples(expr, samples);
    let mut out = Vec::new();

    // todo: parallelize
    for (_, (_, mut group_samples)) in groups.into_iter() {
        group_samples.sort_by_key(sample_hash);
        let selected = select_limit_ratio(group_samples, k)?;

        out.extend(selected);
    }

    Ok(ExprResult::InstantVector(out))
}

fn eval_quantile(
    expr: &AggregateExpr,
    param: Option<ExprResult>,
    samples: Vec<EvalSample>,
    eval_time: Timestamp,
) -> EvalResult<ExprResult> {
    let phi = get_param_as_scalar(param, "quantile")?;
    // make sure it's in range
    if !(0.0..=1.0).contains(&phi) {
        return Err(EvaluationError::ArgumentError(
            "quantile must be between 0.0 and 1.0".to_string(),
        ));
    }
    // parallelize: compute quantile per-group in parallel
    let groups = group_samples(expr, samples);
    let groups_vec: Vec<(Labels, Vec<EvalSample>)> = groups
        .into_iter()
        .map(|(_, (labels, samples))| (labels, samples))
        .collect();

    let out: Vec<EvalSample> = groups_vec
        .into_par()
        .map(|(labels, samples)| {
            let value = sample_quantile(&samples, phi);
            EvalSample {
                labels,
                timestamp_ms: eval_time,
                value,
                drop_name: false,
            }
        })
        .collect();

    Ok(ExprResult::InstantVector(out))
}

fn sample_quantile(samples: &[EvalSample], phi: f64) -> f64 {
    let mut values = samples
        .iter()
        .map(|sample| sample.value)
        .collect::<Vec<_>>();
    quantile(&mut values, phi)
}

fn eval_reduction_aggregation(
    expr: &AggregateExpr,
    samples: Vec<EvalSample>,
    timestamp_ms: Timestamp,
) -> EvalResult<ExprResult> {
    let groups = group_sample_values(expr, samples);

    let out = groups
        .into_iter()
        .map(|(_, (labels, samples))| (labels, samples))
        .iter_into_par()
        .map(|(labels, samples)| {
            let value = aggregate_group(expr.op, &samples);
            EvalSample {
                labels,
                value,
                timestamp_ms,
                drop_name: false,
            }
        })
        .collect();

    Ok(ExprResult::InstantVector(out))
}

fn aggregate_group(op: TokenType, samples: &[f64]) -> f64 {
    match op.id() {
        T_SUM => kahan_sum(samples),
        T_AVG => kahan_avg(samples),
        T_MIN => samples
            .iter()
            .copied()
            .reduce(min_ignore_nan)
            .unwrap_or(f64::NAN),
        T_MAX => samples
            .iter()
            .copied()
            .reduce(max_ignore_nan)
            .unwrap_or(f64::NAN),
        T_COUNT => samples.len() as f64,
        T_GROUP => 1.0,
        T_STDDEV => kahan_std_dev(samples),
        T_STDVAR => kahan_variance(samples),
        _ => {
            unreachable!("BUG: Invalid token ID in aggregate group")
        }
    }
}

fn is_reduction_aggregate(op: TokenType) -> bool {
    matches!(
        op.id(),
        T_SUM | T_AVG | T_MIN | T_MAX | T_COUNT | T_GROUP | T_STDDEV | T_STDVAR
    )
}

fn group_samples(
    expr: &AggregateExpr,
    samples: Vec<EvalSample>,
) -> FingerprintHashMap<(Labels, Vec<EvalSample>)> {
    let modifier = expr.modifier.as_ref();

    let keyed: Vec<_> = samples
        .into_par()
        .map(|sample| {
            let labels = sample.labels.compute_grouping_labels(modifier);
            let key = labels.get_fingerprint();
            (key, labels, sample)
        })
        .collect();

    let mut groups: FingerprintHashMap<(Labels, Vec<EvalSample>)> = FingerprintHashMap::default();
    for (key, labels, sample) in keyed {
        let entry = groups.entry(key).or_insert_with(|| (labels, Vec::new()));
        entry.1.push(sample);
    }
    groups
}

fn group_sample_values(
    expr: &AggregateExpr,
    samples: Vec<EvalSample>,
) -> FingerprintHashMap<(Labels, Vec<f64>)> {
    let mut groups: FingerprintHashMap<(Labels, Vec<f64>)> = FingerprintHashMap::default();

    for sample in samples {
        let labels = sample
            .labels
            .compute_grouping_labels(expr.modifier.as_ref());
        let key = labels.get_fingerprint();

        let entry = groups.entry(key).or_insert_with(|| (labels, Vec::new()));
        entry.1.push(sample.value);
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

fn min_ignore_nan(lhs: f64, rhs: f64) -> f64 {
    match (lhs.is_nan(), rhs.is_nan()) {
        (true, true) => f64::NAN,
        (true, false) => rhs,
        (false, true) => lhs,
        (false, false) => lhs.min(rhs),
    }
}

fn max_ignore_nan(lhs: f64, rhs: f64) -> f64 {
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

// topk/bottomk params are scalar floats, but selection needs a bounded count.
// Coerce once to match PromQL-like behavior: clamp to input size and treat
// k < 1 as empty output.
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

fn select_limit_ratio(mut samples: Vec<EvalSample>, ratio: f64) -> EvalResult<Vec<EvalSample>> {
    if !ratio.is_finite() || ratio == 0.0 || !(-1.0..=1.0).contains(&ratio) {
        return Err(EvaluationError::ArgumentError(
            "limit_ratio parameter must be within [-1, 1] and not equal to 0".to_string(),
        ));
    }

    let len = samples.len();
    if len == 0 {
        return Ok(Vec::new());
    }

    let take = ((len as f64) * ratio.abs()).floor() as usize;
    if ratio < 0.0 {
        samples.reverse();
    }

    samples.truncate(take);
    Ok(samples)
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
    Ok(coerce_k_size(value, sample_len))
}

fn get_param_as_usize(params: Option<ExprResult>, name: &str) -> EvalResult<usize> {
    let value = get_param_as_scalar(params, name)?;
    if !value.is_finite() || value <= 0.0 {
        Ok(0)
    } else {
        Ok(value.floor() as usize)
    }
}

fn sample_hash(sample: &EvalSample) -> u64 {
    let mut hasher: AHasher = Default::default();
    sample.labels.hash(&mut hasher);
    hasher.finish()
}
