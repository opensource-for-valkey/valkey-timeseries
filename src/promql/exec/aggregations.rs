use crate::common::Timestamp;
use crate::common::math::{kahan_avg, kahan_std_dev, kahan_sum, kahan_variance, quantile};
use crate::promql::hashers::FingerprintHashMap;
use crate::promql::{EvalResult, EvalSample, EvaluationError, ExprResult, Labels};
use ahash::AHasher;
use orx_parallel::{IntoParIter, IterIntoParIter, ParIter};
use promql_parser::parser::AggregateExpr;
use promql_parser::parser::token::TokenType;
use promql_parser::parser::token::{
    T_AVG, T_BOTTOMK, T_COUNT, T_COUNT_VALUES, T_GROUP, T_LIMIT_RATIO, T_LIMITK, T_MAX, T_MIN,
    T_QUANTILE, T_STDDEV, T_STDVAR, T_SUM, T_TOPK,
};
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};

pub(crate) fn eval_aggregation(
    expr: &AggregateExpr,
    samples: Vec<EvalSample>,
    param: Option<ExprResult>,
    eval_time: Timestamp,
) -> EvalResult<ExprResult> {
    if samples.is_empty() {
        return Ok(ExprResult::InstantVector(Vec::new()));
    }

    if is_reduction_aggregate(expr.op) {
        return eval_reduction_aggregation(expr, samples, eval_time);
    }

    match expr.op.id() {
        T_QUANTILE => eval_quantile(expr, param, samples, eval_time),
        T_COUNT_VALUES => eval_count_values(expr, param, samples, eval_time),
        T_TOPK | T_BOTTOMK => eval_top_bottom_k(expr, param, samples),
        T_LIMITK | T_LIMIT_RATIO => eval_limit_k(expr, param, samples),
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
) -> EvalResult<ExprResult> {
    let function_name = expr.op.to_string();
    let k = get_param_as_usize(param, &function_name)?;
    let op_id = expr.op.id();

    let grouped = group_samples(expr, samples);

    // sort by the appropriate comparator and take k elements.
    let collected: Vec<EvalSample> = grouped
        .into_iter()
        .iter_into_par()
        .flat_map(|(_, (_, mut group_samples))| {
            match op_id {
                T_TOPK => {
                    group_samples.sort_by(|a, b| topk_value_cmp(a.value, b.value));
                }
                T_BOTTOMK => {
                    group_samples.sort_by(|a, b| bottomk_value_cmp(a.value, b.value));
                }
                _ => unreachable!(),
            }
            group_samples.into_iter().take(k)
        })
        .collect();

    Ok(ExprResult::InstantVector(collected))
}

fn eval_limit_k(
    expr: &AggregateExpr,
    param: Option<ExprResult>,
    samples: Vec<EvalSample>,
) -> EvalResult<ExprResult> {
    let function_name = expr.op.to_string();
    let k = get_param_as_scalar(param, &function_name)?;

    let k_n = if !k.is_finite() || k <= 0.0 {
        0
    } else {
        k.floor() as usize
    };

    let groups = group_samples(expr, samples);
    let mut out = Vec::new();

    // todo: parallelize
    for (_, (_, mut group_samples)) in groups.into_iter() {
        group_samples.sort_by_key(sample_hash);
        let selected = match expr.op.id() {
            T_LIMITK => select_limitk(group_samples, k_n),
            T_LIMIT_RATIO => select_limit_ratio(group_samples, k)?,
            _ => unreachable!(),
        };

        for sample in selected {
            out.push(sample);
        }
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
    // todo: parallelize
    let groups = group_samples(expr, samples);
    let mut out = Vec::new();
    for (_, (labels, samples)) in groups {
        let value = sample_quantile(&samples, phi);
        out.push(EvalSample {
            labels,
            timestamp_ms: eval_time,
            value,
            drop_name: false,
        });
    }
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
    let mut groups = group_sample_values(expr, samples);

    let out = groups
        .values_mut()
        .iter_into_par()
        .map(|(labels, samples)| {
            let value = aggregate_group(expr.op, samples);
            let labels = std::mem::take(labels);
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

fn quantile_value_cmp(lhs: f64, rhs: f64) -> std::cmp::Ordering {
    match (lhs.is_nan(), rhs.is_nan()) {
        (true, true) => std::cmp::Ordering::Equal,
        (true, false) => std::cmp::Ordering::Less,
        (false, true) => std::cmp::Ordering::Greater,
        (false, false) => lhs.total_cmp(&rhs),
    }
}

fn topk_value_cmp(lhs: f64, rhs: f64) -> std::cmp::Ordering {
    match (lhs.is_nan(), rhs.is_nan()) {
        (true, true) => std::cmp::Ordering::Equal,
        (true, false) => std::cmp::Ordering::Greater,
        (false, true) => std::cmp::Ordering::Less,
        (false, false) => rhs.total_cmp(&lhs),
    }
}

fn bottomk_value_cmp(lhs: f64, rhs: f64) -> std::cmp::Ordering {
    match (lhs.is_nan(), rhs.is_nan()) {
        (true, true) => std::cmp::Ordering::Equal,
        (true, false) => std::cmp::Ordering::Greater,
        (false, true) => std::cmp::Ordering::Less,
        (false, false) => lhs.total_cmp(&rhs),
    }
}

fn select_limitk(samples: Vec<EvalSample>, k: usize) -> Vec<EvalSample> {
    let mut samples = samples;
    samples.truncate(k);
    samples
}

fn select_limit_ratio(samples: Vec<EvalSample>, ratio: f64) -> EvalResult<Vec<EvalSample>> {
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
    if ratio > 0.0 {
        Ok(samples.into_iter().take(take).collect())
    } else {
        Ok(samples.into_iter().rev().take(take).collect())
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
    let ExprResult::Scalar(value) = param else {
        let value_type = param.value_type();
        return Err(EvaluationError::ArgumentError(format!(
            "{function_name} parameter must evaluate to a scalar, but got {value_type}"
        )));
    };
    Ok(value)
}

fn get_param_as_string(params: Option<ExprResult>, function_name: &str) -> EvalResult<String> {
    let param = get_param(params, function_name)?;
    let ExprResult::String(value) = param else {
        let value_type = param.value_type();
        return Err(EvaluationError::ArgumentError(format!(
            "{function_name} parameter must evaluate to a string, but got {value_type}"
        )));
    };
    Ok(value.clone())
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
