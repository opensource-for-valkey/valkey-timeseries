use super::labels::{compute_binary_match_key, result_metric};
use crate::promql::binops::apply_binary_op;
use crate::promql::{EvalResult, EvalSample, EvaluationError, ExprResult, Labels};
use ahash::{AHashMap, AHashSet};
use promql_parser::label::METRIC_NAME;
use promql_parser::parser::{BinaryExpr, VectorMatchCardinality};

// Vector-Vector operations: many-to-many not supported
pub(super) fn eval_binop_vector_vector(
    expr: &BinaryExpr,
    left_vector: Vec<EvalSample>,
    right_vector: Vec<EvalSample>,
) -> EvalResult<ExprResult> {
    let mut left_vector = left_vector;
    let mut right_vector = right_vector;

    let card = match expr.modifier.as_ref().map(|m| &m.card) {
        Some(VectorMatchCardinality::ManyToMany) => {
            return Err(EvaluationError::InternalError(
                "many-to-many cardinality not supported".to_string(),
            ));
        }
        Some(c) => c,
        None => &VectorMatchCardinality::OneToOne,
    };

    let matching = expr.modifier.as_ref().and_then(|m| m.matching.as_ref());

    // Materialize pending __name__ drops before matching so that
    // stale names don't participate in match keys or result labels
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
    let mut one_map: AHashMap<Labels, Vec<EvalSample>> = AHashMap::new();
    let is_comparison = expr.op.is_comparison_operator();
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
    let mut one_to_one_seen: AHashSet<Labels> = AHashSet::new();
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
        if is_one_to_one && !is_comparison && !one_to_one_seen.insert(key.clone()) {
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
                            let many_label_names: AHashSet<&str> =
                                many_sample.labels.iter().map(|l| l.name.as_str()).collect();
                            for label in one_sample.labels.iter() {
                                if label.name != METRIC_NAME
                                    && !many_label_names.contains(label.name.as_str())
                                {
                                    result_labels.insert(&label.name, label.value.clone());
                                }
                            }
                        }
                    }

                    // Check if this is a comparison operation (filters results in PromQL)
                    let is_comparison = operator.is_comparison_operator();
                    // With `bool` modifier, comparison ops return 0/1 for all pairs instead of filtering
                    let return_bool = expr.return_bool();

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
