use super::labels::{compute_binary_match_key, result_metric};
use crate::promql::binops::apply_binary_op;
use crate::promql::hashers::FingerprintHashMap;
use crate::promql::{EvalResult, EvalSample, EvaluationError, ExprResult, Labels};
use ahash::{AHashSet, RandomState};
use orx_parallel::IntoParIter;
use orx_parallel::ParIter;
use promql_parser::label::METRIC_NAME;
use promql_parser::parser::token::{T_LAND, T_LOR, T_LUNLESS};
use promql_parser::parser::{BinaryExpr, VectorMatchCardinality};

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
    let is_comparison = expr.op.is_comparison_operator();
    // With `bool` modifier, comparison ops return 0/1 for all pairs instead of filtering
    let return_bool = expr.return_bool();

    // Arithmetic (non-comparison) operations always drop `__name__`, even for unmatched series that are carried through from the "many" side.
    // This matches Prometheus's behavior and ensures that stale metric names don't interfere with matching or result labels.
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

    // Determine which side is "one" vs "many" for matching purposes.
    // For one-to-one mappings, we treat the right-hand side as the "one" side.
    let (one_vec, many_vec) = if is_group_right {
        (left_vector, right_vector)
    } else {
        (right_vector, left_vector)
    };


    // Build "one" side index keyed by match signature
    // For comparisons, allow multiple samples with the same matching key
    let mut one_map: FingerprintHashMap<Vec<EvalSample>> = Default::default();

    for sample in one_vec {
        let key = compute_binary_match_key(&sample.labels, matching);
        if one_map.contains_key(&key) && !is_comparison {
            // For arithmetic operations, duplicates on the "one" side are an error.
            // For comparison operations, we allow them since filtering can resolve duplicates.
            return Err(EvaluationError::InternalError(format!(
                "many-to-many matching not allowed: found duplicate series on the {} side of the operation",
                if is_group_right { "left" } else { "right" }
            )));
        }
        one_map.entry(key).or_default().push(sample);
    }

    let mut result = Vec::new();
    let mut one_to_one_seen: FingerprintHashMap<()> = Default::default();
    // PromQL grouped matching (`group_left` / `group_right`) requires every
    // output time series to remain uniquely identifiable. Two different matches
    // are not allowed to collapse to the same final output labels.
    // Keep a set of final output label keys and fail on duplicates.
    let mut grouped_result_seen: AHashSet<Labels> = AHashSet::new();

    for many_sample in many_vec {
        let key = compute_binary_match_key(&many_sample.labels, matching);

        // Look up matching "one" sample
        let one_samples = match one_map.get(&key) {
            Some(s) => s,
            None => continue, // silently dropped if unmatched on "one" side
        };

        // For one-to-one matching, if we've already seen this matching key from the
        // many side and we're not doing a comparison, that's an error because we can't
        // match multiple many-side entries to the same one-side entry.
        // We check this here rather than upfront so that unmatched duplicates are
        // silently dropped (no error for unmatched many-side duplicates).
        if is_one_to_one && !is_comparison && one_to_one_seen.insert(key, ()).is_some() {
            return Err(EvaluationError::InternalError(
                "many-to-many matching not allowed: found duplicate series on the left side of the operation"
                    .to_string(),
            ));
        }

        // Preserve original lhs/rhs ordering for the operator.
        let operator = expr.op;

        // If multiple "one" samples match, we need to produce results for each
        // combination of "one" and "many" samples. This can happen with
        // `group_left`/`group_right` and also for comparison operations that
        // allow duplicates to remain.
        for one_sample in one_samples {
            let (lhs, rhs) = if is_group_right {
                (one_sample.value, many_sample.value)
            } else {
                (many_sample.value, one_sample.value)
            };

            match apply_binary_op(operator, lhs, rhs) {
                Ok(value) => {
                    let mut result_labels = result_metric(
                        many_sample.labels.clone(),
                        operator,
                        if is_one_to_one { matching } else { None }, // should only be filtered for one-to-one case
                    );
                    // For `group_left(<labels>)` / `group_right(<labels>)`, each listed
                    // label must come from the "one" side. Use set-or-remove semantics:
                    // - if present on "one" side, copy/overwrite it in output
                    // - if absent on "one" side, remove it from output
                    // This avoids leaking stale values from the "many" side.
                    //
                    // When group_left/group_right is used without parentheses or with empty labels,
                    // copy all non-matching labels from the "one" side.
                    match group_labels {
                        Some(extra) if !extra.is_empty() => {
                            // Explicit labels: only copy/remove those that come from the "one" side
                            for name in extra {
                                match one_sample.labels.get(name) {
                                    Some(v) => {
                                        result_labels.insert(name, v.to_string());
                                    }
                                    None => {
                                        // For explicit group labels, only remove if there's a cardinality mismatch
                                        // where we expect the label to come from one side but it doesn't exist.
                                        // However, if the label already exists on the many side, we preserve it.
                                        // This allows group_right(label) to work even if left side doesn't have the label.
                                        if is_group_right {
                                            // For group_right, left is "one" side. If it doesn't have the label,
                                            // preserve the many-side (right) label
                                            // (don't remove it)
                                        } else {
                                            // For group_left, right is "one" side. If it doesn't have the label,
                                            // remove it per set-or-remove semantics
                                            result_labels.remove(name);
                                        }
                                    }
                                }
                            }
                        }
                        _ => {
                            // No explicit labels or empty: copy labels from the "one" side that are not on the "many" side
                            // This preserves the many-side labels while adding only new labels from the one-side
                            let many_label_names: halfbrown::HashMap<&str, (), RandomState> =
                                many_sample
                                    .labels
                                    .iter()
                                    .map(|l| (l.name.as_str(), ()))
                                    .collect();
                            for label in one_sample.labels.iter() {
                                if label.name != METRIC_NAME
                                    && !many_label_names.contains_key(label.name.as_str())
                                {
                                    result_labels.insert(&label.name, label.value.clone());
                                }
                            }
                        }
                    }


                    let drop_name = many_sample.drop_name || return_bool;

                    // For comparison ops without bool, filter out false results.
                    if is_comparison && !return_bool && value == 0.0 {
                        continue;
                    }
                    // PromQL comparison operators without `bool` are filters.
                    // For vector-vector comparisons (one-to-one and grouped), keep
                    // matched true pairs and propagate the original LHS sample value
                    // instead of the computed predicate value (1/0).
                    let output_value = if is_comparison && !return_bool {
                        lhs
                    } else {
                        value
                    };

                    // Duplicate detection for grouped matching must occur after comparison filtering,
                    // so that comparisons can naturally reduce duplicate "one" side entries.
                    // This allows `group_left` comparisons with duplicates on the "one" side to work
                    // correctly: the comparison will filter out false matches, and only then do we
                    // check if the surviving results have unique output labels.
                    if !is_one_to_one {
                        let mut result_label_key = result_labels.clone();
                        if drop_name {
                            result_label_key.remove(METRIC_NAME);
                        }
                        if !grouped_result_seen.insert(result_label_key) {
                            return Err(EvaluationError::InternalError(
                                "multiple matches for labels: grouping labels must ensure unique matches"
                                    .to_string(),
                            ));
                        }
                    }

                    result.push(EvalSample {
                        timestamp_ms: many_sample.timestamp_ms,
                        value: output_value,
                        labels: result_labels,
                        drop_name,
                    });
                }
                Err(e) => return Err(e),
            }
        }
    }

    Ok(ExprResult::InstantVector(result))
}


// ============================================================================
// Set operators: or, and, unless
// ============================================================================

/// `or`: returns all LHS samples, plus any RHS samples whose match key
/// does not appear on the LHS.
fn eval_set_or(
    left_vector: Vec<EvalSample>,
    right_vector: Vec<EvalSample>,
    matching: Option<&promql_parser::parser::LabelModifier>,
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
    matching: Option<&promql_parser::parser::LabelModifier>,
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
    matching: Option<&promql_parser::parser::LabelModifier>,
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
