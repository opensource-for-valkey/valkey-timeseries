use super::labels::{compute_binary_match_key, result_metric};
use crate::promql::binops::apply_binary_op;
use crate::promql::hashers::FingerprintHashMap;
use crate::promql::{EvalResult, EvalSample, EvaluationError, ExprResult, Labels};
use orx_parallel::IntoParIter;
use orx_parallel::ParIter;
use promql_parser::label::METRIC_NAME;
use promql_parser::parser::token::{T_LAND, T_LOR, T_LUNLESS, TokenType};
use promql_parser::parser::{BinaryExpr, LabelModifier, VectorMatchCardinality};
use twox_hash::xxhash3_128;

// Vector-Vector operations
pub(super) fn eval_binop_vector_vector(
    expr: &BinaryExpr,
    left_vector: Vec<EvalSample>,
    right_vector: Vec<EvalSample>,
) -> EvalResult<ExprResult> {
    let matching = expr.modifier.as_ref().and_then(|m| m.matching.as_ref());

    match expr.op.id() {
        T_LOR => eval_set_or(left_vector, right_vector, matching),
        T_LAND => eval_set_and(left_vector, right_vector, matching),
        T_LUNLESS => eval_set_unless(left_vector, right_vector, matching),
        _ => eval_arith_ops(expr, left_vector, right_vector),
    }
}

fn drop_names_if_necessary(
    mut left_vector: Vec<EvalSample>,
    mut right_vector: Vec<EvalSample>,
) -> (Vec<EvalSample>, Vec<EvalSample>) {
    // Materialize pending __name__ drops before matching
    for sample in left_vector.iter_mut() {
        if sample.drop_name {
            sample.labels.remove(METRIC_NAME);
        }
    }
    for sample in right_vector.iter_mut() {
        if sample.drop_name {
            sample.labels.remove(METRIC_NAME);
        }
    }
    (left_vector, right_vector)
}

fn eval_arith_ops(
    expr: &BinaryExpr,
    mut left_vector: Vec<EvalSample>,
    mut right_vector: Vec<EvalSample>,
) -> EvalResult<ExprResult> {
    if left_vector.is_empty() || right_vector.is_empty() {
        return Ok(ExprResult::InstantVector(vec![]));
    }

    let card = match expr.modifier.as_ref().map(|m| &m.card) {
        Some(VectorMatchCardinality::ManyToMany) => {
            return Err(EvaluationError::InternalError(
                "many-to-many cardinality not supported for non-set operators".to_string(),
            ));
        }
        Some(c) => c,
        None => &VectorMatchCardinality::OneToOne,
    };

    let matching = expr.modifier.as_ref().and_then(|m| m.matching.as_ref());
    let operator = expr.op;
    let is_comparison = operator.is_comparison_operator();
    let return_bool = expr.return_bool();

    // Arithmetic (non-comparison) operations always drop `__name__`.
    if !is_comparison {
        for sample in left_vector.iter_mut() {
            if sample.drop_name {
                sample.labels.remove(METRIC_NAME);
            }
        }
        for sample in right_vector.iter_mut() {
            if sample.drop_name {
                sample.labels.remove(METRIC_NAME);
            }
        }
    }

    let is_group_right = matches!(card, VectorMatchCardinality::OneToMany(_));
    let is_one_to_one = matches!(card, VectorMatchCardinality::OneToOne);
    let group_labels = card.labels().map(|l| &l.labels);

    // Determine which side is "one" vs. "many" for matching purposes.
    // For one-to-one mappings, we treat the right-hand side as the "one" side.
    let (one_vec, many_vec) = if is_group_right {
        (left_vector, right_vector)
    } else {
        (right_vector, left_vector)
    };

    // Build "one" side index keyed by match signature.
    let mut one_map: FingerprintHashMap<Vec<EvalSample>> =
        FingerprintHashMap::with_capacity_and_hasher(one_vec.len(), Default::default());

    for sample in one_vec {
        let key = compute_binary_match_key(&sample.labels, matching);
        if !is_comparison && one_map.contains_key(&key) {
            return Err(EvaluationError::InternalError(format!(
                "many-to-many matching not allowed: found duplicate series on the {} side of the operation",
                if is_group_right { "left" } else { "right" }
            )));
        }
        one_map.entry(key).or_default().push(sample);
    }

    let mut result = Vec::with_capacity(many_vec.len());
    let mut one_to_one_seen: FingerprintHashMap<()> = Default::default();
    // Track output label fingerprints for grouped matching to detect duplicates.
    let mut grouped_result_seen: FingerprintHashMap<()> = Default::default();

    for many_sample in many_vec {
        let key = compute_binary_match_key(&many_sample.labels, matching);

        let one_samples = match one_map.get(&key) {
            Some(s) => s,
            None => continue,
        };

        if is_one_to_one && !is_comparison && one_to_one_seen.insert(key, ()).is_some() {
            return Err(EvaluationError::InternalError(
                "many-to-many matching not allowed: found duplicate series on the left side of the operation"
                    .to_string(),
            ));
        }

        result = one_samples
            .into_par()
            .filter_map(|one_sample| {
                let (lhs, rhs) = if is_group_right {
                    (one_sample.value, many_sample.value)
                } else {
                    (many_sample.value, one_sample.value)
                };

                let value = match apply_binary_op(operator, lhs, rhs) {
                    Ok(v) => v,
                    Err(e) => {
                        unreachable!(
                            "binary operator {:?} should not fail on valid f64 inputs: {}",
                            operator, e
                        );
                    }
                };

                // Comparison filter: skip false results unless `bool` modifier is used.
                if is_comparison && !return_bool && value == 0.0 {
                    return None;
                }

                // For comparison ops without `bool`, propagate the original LHS value.
                let output_value = if is_comparison && !return_bool {
                    lhs
                } else {
                    value
                };
                let drop_name = many_sample.drop_name || return_bool;

                let result_labels = build_result_labels(
                    &many_sample,
                    one_sample,
                    operator,
                    if is_one_to_one { matching } else { None },
                    group_labels,
                    is_group_right,
                );

                Some(EvalSample {
                    timestamp_ms: many_sample.timestamp_ms,
                    value: output_value,
                    labels: result_labels,
                    drop_name,
                })
            })
            .collect_into(result);
    }

    // Duplicate detection for grouped matching must occur after comparison
    // filtering so that comparisons can naturally reduce duplicates.
    if !is_one_to_one {
        for sample in result.iter() {
            let fp = result_fingerprint(&sample.labels, sample.drop_name);
            if grouped_result_seen.insert(fp, ()).is_some() {
                return Err(EvaluationError::InternalError(
                    "multiple matches for labels: grouping labels must ensure unique matches"
                        .to_string(),
                ));
            }
        }
    }

    Ok(ExprResult::InstantVector(result))
}

/// Build the result label set for a matched pair.
///
/// Handles `group_left(<labels>)` / `group_right(<labels>)` semantics:
/// - Explicit labels: copy from "one" side, or remove if absent (set-or-remove).
/// - No explicit labels: copy labels from "one" side that are absent on "many" side.
fn build_result_labels(
    many_sample: &EvalSample,
    one_sample: &EvalSample,
    operator: TokenType,
    matching: Option<&LabelModifier>,
    group_labels: Option<&Vec<String>>,
    is_group_right: bool,
) -> Labels {
    let mut labels = result_metric(many_sample.labels.clone(), operator, matching);

    match group_labels {
        Some(extra) if !extra.is_empty() => {
            for name in extra {
                match one_sample.labels.get(name) {
                    Some(v) => {
                        labels.insert(name, v.to_string());
                    }
                    None if !is_group_right => {
                        // group_left: right is "one" side — remove if absent.
                        labels.remove(name);
                    }
                    _ => {
                        // group_right: left is "one" side — preserve many-side label.
                    }
                }
            }
        }
        _ => {
            // Copy labels from "one" side not already present on "many" side.
            // Uses binary search via Labels::contains — no heap allocation.
            for label in one_sample.labels.iter() {
                if label.name != METRIC_NAME && !many_sample.labels.contains(&label.name) {
                    labels.insert(&label.name, label.value.clone());
                }
            }
        }
    }

    labels
}

/// Compute a fingerprint for duplicate detection in grouped matching results.
/// When `drop_name` is true, `__name__` is excluded from the hash to match
/// the effective output labels.
#[inline]
fn result_fingerprint(labels: &Labels, drop_name: bool) -> u128 {
    let mut hasher: xxhash3_128::Hasher = Default::default();
    for label in labels.iter() {
        if drop_name && label.name == METRIC_NAME {
            continue;
        }
        hasher.write(label.name.as_bytes());
        hasher.write(b"0xfe");
        hasher.write(label.value.as_bytes());
    }
    hasher.finish_128()
}

// ============================================================================
// Set operators: or, and, unless
// ============================================================================

/// `or`: returns all LHS samples, plus any RHS samples whose match key
/// does not appear on the LHS.
fn eval_set_or(
    left_vector: Vec<EvalSample>,
    right_vector: Vec<EvalSample>,
    matching: Option<&LabelModifier>,
) -> EvalResult<ExprResult> {
    if left_vector.is_empty() {
        return Ok(ExprResult::InstantVector(right_vector));
    }
    if right_vector.is_empty() {
        return Ok(ExprResult::InstantVector(left_vector));
    }

    let (left_vector, right_vector) = drop_names_if_necessary(left_vector, right_vector);

    // Build a set of match keys from the left side
    let left_keys: FingerprintHashMap<()> = left_vector
        .iter()
        .map(|s| (compute_binary_match_key(&s.labels, matching), ()))
        .collect();

    // Append right-side samples whose match key is NOT present on the left
    let result = right_vector
        .into_par()
        .filter(|s| {
            let key = compute_binary_match_key(&s.labels, matching);
            !left_keys.contains_key(&key)
        })
        .collect_into(left_vector);

    Ok(ExprResult::InstantVector(result))
}

/// `and`: returns LHS samples that have a matching label set on the RHS.
/// Values always come from the LHS.
fn eval_set_and(
    left_vector: Vec<EvalSample>,
    right_vector: Vec<EvalSample>,
    matching: Option<&LabelModifier>,
) -> EvalResult<ExprResult> {
    if left_vector.is_empty() || right_vector.is_empty() {
        return Ok(ExprResult::InstantVector(vec![]));
    }

    let (left_vector, right_vector) = drop_names_if_necessary(left_vector, right_vector);

    // Build a set of match keys from the right side
    let right_keys: FingerprintHashMap<()> = right_vector
        .iter()
        .map(|s| (compute_binary_match_key(&s.labels, matching), ()))
        .collect();

    let result: Vec<EvalSample> = left_vector
        .into_par()
        .filter(|s| {
            let key = compute_binary_match_key(&s.labels, matching);
            right_keys.contains_key(&key)
        })
        .collect();

    Ok(ExprResult::InstantVector(result))
}

/// `unless`: returns LHS samples that do NOT have a matching label set on the RHS.
fn eval_set_unless(
    left_vector: Vec<EvalSample>,
    right_vector: Vec<EvalSample>,
    matching: Option<&LabelModifier>,
) -> EvalResult<ExprResult> {
    if left_vector.is_empty() || right_vector.is_empty() {
        return Ok(ExprResult::InstantVector(left_vector));
    }

    let (left_vector, right_vector) = drop_names_if_necessary(left_vector, right_vector);

    // Build a set of match keys from the right side
    let right_keys: FingerprintHashMap<()> = right_vector
        .iter()
        .map(|s| (compute_binary_match_key(&s.labels, matching), ()))
        .collect();

    let result: Vec<EvalSample> = left_vector
        .into_par()
        .filter(|s| {
            let key = compute_binary_match_key(&s.labels, matching);
            !right_keys.contains_key(&key)
        })
        .collect();

    Ok(ExprResult::InstantVector(result))
}
