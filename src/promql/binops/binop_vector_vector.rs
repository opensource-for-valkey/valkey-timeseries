use super::labels::{compute_binary_match_key, result_metric};
use crate::promql::binops::apply_binary_op;
use crate::promql::hashers::{FingerprintHashMap, SeriesFingerprint};
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
        T_LOR => {
            validate_non_fill(expr)?;
            eval_set_or(left_vector, right_vector, matching)
        }
        T_LAND => {
            validate_non_fill(expr)?;
            eval_set_and(left_vector, right_vector, matching)
        }
        T_LUNLESS => {
            validate_non_fill(expr)?;
            eval_set_unless(left_vector, right_vector, matching)
        }
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

struct ArithOpContext<'a> {
    card: &'a VectorMatchCardinality,
    matching: Option<&'a LabelModifier>,
    operator: TokenType,
    is_comparison: bool,
    return_bool: bool,
    has_fill: bool,
    is_group_right: bool,
    is_one_to_one: bool,
    group_labels: Option<&'a Vec<String>>,
    fill_for_one: Option<f64>,
    fill_for_many: Option<f64>,
}

fn build_arith_op_context(expr: &BinaryExpr) -> EvalResult<ArithOpContext<'_>> {
    let (fill_left, fill_right, card, matching) = match expr.modifier.as_ref() {
        None => (None, None, &VectorMatchCardinality::OneToOne, None),
        Some(modifier) => {
            let card = match &modifier.card {
                VectorMatchCardinality::ManyToMany => {
                    return Err(EvaluationError::InternalError(
                        "many-to-many cardinality not supported for non-set operators".to_string(),
                    ));
                }
                c => c,
            };
            (
                modifier.fill_values.lhs,
                modifier.fill_values.rhs,
                card,
                modifier.matching.as_ref(),
            )
        }
    };

    let operator = expr.op;
    let is_comparison = operator.is_comparison_operator();
    let return_bool = expr.return_bool();
    let has_fill = fill_left.is_some() || fill_right.is_some();

    let is_group_right = matches!(card, VectorMatchCardinality::OneToMany(_));

    Ok(ArithOpContext {
        card,
        matching,
        operator,
        is_comparison,
        return_bool,
        has_fill,
        is_group_right,
        is_one_to_one: matches!(card, VectorMatchCardinality::OneToOne),
        group_labels: card.labels().map(|l| &l.labels),
        // Fill values are operand-side based, not cardinality-side based:
        // - fill_left applies when LHS is missing
        // - fill_right applies when RHS is missing
        // This must remain true for both group_left and group_right.
        fill_for_one: fill_right,
        fill_for_many: fill_left,
    })
}

#[inline]
fn make_fill_one_sample(many_sample: &EvalSample, fill_value: f64) -> EvalSample {
    EvalSample {
        timestamp_ms: many_sample.timestamp_ms,
        value: fill_value,
        labels: Labels::empty(),
        drop_name: false,
    }
}

#[inline]
fn make_fill_many_sample(one_sample: &EvalSample, fill_value: f64) -> EvalSample {
    EvalSample {
        timestamp_ms: one_sample.timestamp_ms,
        value: fill_value,
        labels: one_sample.labels.clone(),
        drop_name: one_sample.drop_name,
    }
}

// ============================================================================
// Fast-path for no-modifier arithmetic / comparison ops
// ============================================================================

/// Returns `true` when the expression carries no modifier — meaning OneToOne
/// cardinality, no on/ignoring label matching, no fill values, and no
/// `return_bool`. In this state both sides are matched purely on their full
/// label set minus `__name__`, so a hashmap-free merge-join is safe and correct.
fn can_use_fast_path(ctx: &ArithOpContext<'_>) -> bool {
    ctx.is_one_to_one && ctx.matching.is_none() && !ctx.has_fill && !ctx.return_bool
}

/// Evaluates arithmetic or comparison operations on two vectors using a merge join, assuming the operation
/// has no modifiers (`fill`, `bool`, `on`/ `ignoring`, e.t.c).
///
fn eval_arith_ops_fast_path(
    operator: TokenType,
    is_comparison: bool,
    left_vector: Vec<EvalSample>,
    right_vector: Vec<EvalSample>,
) -> EvalResult<ExprResult> {
    use std::cmp::Ordering;

    let mut left_sorted: Vec<(SeriesFingerprint, EvalSample)> = left_vector
        .into_par()
        .map(|s| (compute_binary_match_key(&s.labels, None), s))
        .collect();

    let mut right_sorted: Vec<(SeriesFingerprint, EvalSample)> = right_vector
        .into_par()
        .map(|s| (compute_binary_match_key(&s.labels, None), s))
        .collect();

    left_sorted.sort_unstable_by_key(|x| x.0);
    right_sorted.sort_unstable_by_key(|x| x.0);

    let mut result: Vec<EvalSample> = Vec::new();
    let mut i = 0usize;
    let mut j = 0usize;

    while i < left_sorted.len() && j < right_sorted.len() {
        let lhs_fp = left_sorted[i].0;
        let rhs_fp = right_sorted[j].0;

        match lhs_fp.cmp(&rhs_fp) {
            Ordering::Less => i += 1,
            Ordering::Greater => j += 1,
            Ordering::Equal => {
                let i_end = i + left_sorted[i..].partition_point(|x| x.0 == lhs_fp);
                let j_end = j + right_sorted[j..].partition_point(|x| x.0 == rhs_fp);

                if !is_comparison && (i_end - i > 1 || j_end - j > 1) {
                    let side = if i_end - i > 1 { "left" } else { "right" };
                    return Err(EvaluationError::InternalError(format!(
                        "many-to-many matching not allowed: found duplicate series on the {side} side of the operation"
                    )));
                }

                for (_, lhs) in &left_sorted[i..i_end] {
                    for (_, rhs) in &right_sorted[j..j_end] {
                        let Some(value) = apply_binary_op(operator, lhs.value, rhs.value).ok()
                        else {
                            continue;
                        };

                        if is_comparison && value == 0.0 {
                            continue;
                        }

                        let output_value = if is_comparison { lhs.value } else { value };
                        let labels = result_metric(lhs.labels.clone(), operator, None);

                        result.push(EvalSample {
                            timestamp_ms: lhs.timestamp_ms,
                            value: output_value,
                            labels,
                            drop_name: lhs.drop_name,
                        });
                    }
                }

                i = i_end;
                j = j_end;
            }
        }
    }

    Ok(ExprResult::InstantVector(result))
}

fn build_result_sample(
    ctx: &ArithOpContext<'_>,
    many_sample: &EvalSample,
    one_sample: &EvalSample,
) -> Option<EvalSample> {
    // Determine operand order based on grouping, then apply the operator.
    let lhs_val = if ctx.is_group_right {
        one_sample.value
    } else {
        many_sample.value
    };
    let rhs_val = if ctx.is_group_right {
        many_sample.value
    } else {
        one_sample.value
    };

    let op_result = match apply_binary_op(ctx.operator, lhs_val, rhs_val) {
        Ok(v) => v,
        Err(e) => unreachable!(
            "binary operator {:?} should not fail on valid f64 inputs: {}",
            ctx.operator, e
        ),
    };

    // For non-bool comparisons, filter out false results (0.0).
    if ctx.is_comparison && !ctx.return_bool && op_result == 0.0 {
        return None;
    }

    let output_value = if ctx.is_comparison && !ctx.return_bool {
        lhs_val
    } else {
        op_result
    };

    let drop_name = many_sample.drop_name || ctx.return_bool;

    let result_labels = build_result_labels(
        many_sample,
        one_sample,
        ctx.operator,
        if ctx.is_one_to_one {
            ctx.matching
        } else {
            None
        },
        ctx.group_labels,
        ctx.is_group_right,
    );

    Some(EvalSample {
        timestamp_ms: many_sample.timestamp_ms,
        value: output_value,
        labels: result_labels,
        drop_name,
    })
}

fn eval_arith_ops(
    expr: &BinaryExpr,
    mut left_vector: Vec<EvalSample>,
    mut right_vector: Vec<EvalSample>,
) -> EvalResult<ExprResult> {
    if left_vector.is_empty() && right_vector.is_empty() {
        return Ok(ExprResult::InstantVector(vec![]));
    }

    let ctx = build_arith_op_context(expr)?;

    if left_vector.is_empty() || right_vector.is_empty() {
        // early return if we have no fill modifiers
        if !ctx.has_fill {
            return Ok(ExprResult::InstantVector(vec![]));
        }
    }

    // Arithmetic (non-comparison) operations always drop `__name__`.
    if !ctx.is_comparison {
        (left_vector, right_vector) = drop_names_if_necessary(left_vector, right_vector);
        for sample in left_vector.iter_mut() {
            sample.labels.remove(METRIC_NAME);
        }
        for sample in right_vector.iter_mut() {
            sample.labels.remove(METRIC_NAME);
        }
    }

    // Fast-path: no modifier (OneToOne, no matching, no fills, no bool) —
    // avoid building a hashmap by using a fingerprint-based merge.
    if can_use_fast_path(&ctx) {
        return eval_arith_ops_fast_path(
            ctx.operator,
            ctx.is_comparison,
            left_vector,
            right_vector,
        );
    }

    // Determine which side is "one" vs. "many" for matching purposes.
    // For one-to-one mappings, we treat the right-hand side as the "one" side.
    let (one_vec, many_vec) = if ctx.is_group_right {
        (left_vector, right_vector)
    } else {
        (right_vector, left_vector)
    };

    // Build "one" side index keyed by match signature.
    let mut one_map: FingerprintHashMap<Vec<EvalSample>> =
        FingerprintHashMap::with_capacity_and_hasher(one_vec.len(), Default::default());

    for sample in one_vec {
        let key = compute_binary_match_key(&sample.labels, ctx.matching);
        if !ctx.is_comparison && one_map.contains_key(&key) {
            return Err(EvaluationError::InternalError(format!(
                "many-to-many matching not allowed: found duplicate series on the {} side of the operation",
                if ctx.is_group_right { "left" } else { "right" }
            )));
        }
        one_map.entry(key).or_default().push(sample);
    }

    let mut result = Vec::with_capacity(many_vec.len());
    let mut one_to_one_seen: FingerprintHashMap<()> = Default::default();
    // Track output label fingerprints for grouped matching to detect duplicates.
    // todo: could these be simple bitsets ?
    let mut grouped_result_seen: FingerprintHashMap<()> = Default::default();
    // Track one_map keys that were matched during the main pass (used by the
    // fill_for_many pass below to find truly unmatched "one" entries).
    let mut one_map_matched: FingerprintHashMap<()> = Default::default();

    for many_sample in many_vec {
        let key = compute_binary_match_key(&many_sample.labels, ctx.matching);

        let one_samples = match one_map.get(&key) {
            Some(s) => s,
            None => {
                // No match found for this "many" sample.  If a fill value was
                // supplied for the "one" side, synthesize a phantom one_sample
                // with that value and empty labels, then emit the result.
                if let Some(fill_val) = ctx.fill_for_one {
                    let fill_one = make_fill_one_sample(&many_sample, fill_val);
                    if let Some(sample) = build_result_sample(&ctx, &many_sample, &fill_one) {
                        result.push(sample);
                    }
                }
                continue;
            }
        };

        // Record this key as matched so the fill_for_many pass skips it.
        if ctx.has_fill {
            one_map_matched.insert(key, ());
        }

        if ctx.is_one_to_one && !ctx.is_comparison && one_to_one_seen.insert(key, ()).is_some() {
            return Err(EvaluationError::InternalError(
                "many-to-many matching not allowed: found duplicate series on the left side of the operation"
                    .to_string(),
            ));
        }

        result = one_samples
            .into_par()
            .filter_map(|one_sample| build_result_sample(&ctx, &many_sample, one_sample))
            .collect_into(result);
    }

    // Fill pass for unmatched "one" side entries.
    //
    // For every series on the "one" side that had no matching "many" partner,
    // synthesize a phantom "many" sample using fill_for_many and emit a result.
    // The phantom sample uses the real "one" sample's labels so that result
    // label construction produces the correct output series identity.
    if let Some(fill_val) = ctx.fill_for_many {
        for (key, one_samples) in &one_map {
            if one_map_matched.contains_key(key) {
                continue; // already handled in the main pass
            }
            for one_sample in one_samples {
                // Synthesize a "many" sample whose labels match the "one" sample.
                // This ensures build_result_labels produces the right output labels
                // (they come from the "many" base, but since they equal the "one"
                // labels here, the correct series identity is preserved).
                let fill_many = make_fill_many_sample(one_sample, fill_val);
                if let Some(sample) = build_result_sample(&ctx, &fill_many, one_sample) {
                    result.push(sample);
                }
            }
        }
    }

    // Duplicate detection for grouped matching must occur after comparison
    // filtering so that comparisons can naturally reduce duplicates.
    if !ctx.is_one_to_one {
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

fn validate_non_fill(expr: &BinaryExpr) -> EvalResult<()> {
    // Fill modifiers are not meaningful for set operators (or / and / unless).
    let has_fill = expr
        .modifier
        .as_ref()
        .map(|m| m.fill_values.lhs.is_some() || m.fill_values.rhs.is_some())
        .unwrap_or(false);

    if has_fill {
        return Err(EvaluationError::InternalError(
            "fill modifiers (fill, fill_left, fill_right) are not supported on set operators (or, and, unless)".to_string(),
        ));
    }
    Ok(())
}

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

// ------------------------- Benchmark helpers -------------------------------
// These helpers are compiled only when the `bench` feature is enabled and are
// intended to be called from external Criterion benchmark crates. They live in
// this module so they can reuse internal types without duplicating logic.

#[cfg(feature = "bench")]
pub fn bench_eval_aligned(n: usize) -> usize {
    use promql_parser::parser::token::T_ADD;
    use promql_parser::parser::{BinaryExpr, Expr, NumberLiteral};

    // build a simple BinaryExpr with no modifier to hit the fast-path
    let expr = BinaryExpr {
        op: TokenType::new(T_ADD),
        lhs: Box::new(Expr::NumberLiteral(NumberLiteral { val: 0.0 })),
        rhs: Box::new(Expr::NumberLiteral(NumberLiteral { val: 0.0 })),
        modifier: None,
    };

    let mut left = Vec::with_capacity(n);
    let mut right = Vec::with_capacity(n);

    for i in 0..n {
        let mut labels = Labels::empty();
        labels.insert("id", i.to_string());

        left.push(EvalSample {
            timestamp_ms: 1,
            value: i as f64,
            labels: labels.clone(),
            drop_name: false,
        });

        right.push(EvalSample {
            timestamp_ms: 1,
            value: i as f64,
            labels,
            drop_name: false,
        });
    }

    match eval_binop_vector_vector(&expr, left, right) {
        Ok(ExprResult::InstantVector(v)) => v.len(),
        _ => 0,
    }
}

#[cfg(feature = "bench")]
pub fn bench_eval_unaligned(n: usize) -> usize {
    use promql_parser::parser::token::T_ADD;
    use promql_parser::parser::{BinaryExpr, Expr, NumberLiteral};

    // build a simple BinaryExpr with no modifier to hit the fast-path
    let expr = BinaryExpr {
        op: TokenType::new(T_ADD),
        lhs: Box::new(Expr::NumberLiteral(NumberLiteral { val: 0.0 })),
        rhs: Box::new(Expr::NumberLiteral(NumberLiteral { val: 0.0 })),
        modifier: None,
    };

    let mut left = Vec::with_capacity(n);
    let mut right = Vec::with_capacity(n);

    for i in 0..n {
        let mut labels = Labels::empty();
        labels.insert("id", i.to_string());

        left.push(EvalSample {
            timestamp_ms: 1,
            value: i as f64,
            labels: labels.clone(),
            drop_name: false,
        });

        right.push(EvalSample {
            timestamp_ms: 1,
            value: i as f64,
            labels,
            drop_name: false,
        });
    }

    // reverse the right vector to force the sort-merge path in the fast-path
    right.reverse();

    match eval_binop_vector_vector(&expr, left, right) {
        Ok(ExprResult::InstantVector(v)) => v.len(),
        _ => 0,
    }
}

#[cfg(feature = "bench")]
pub fn bench_eval_with_fill(n: usize) -> usize {
    use promql_parser::parser::token::T_ADD;
    use promql_parser::parser::{
        BinModifier, BinaryExpr, Expr, NumberLiteral, VectorMatchFillValues,
    };

    // build an expression with fill modifiers to force the hashmap-based path
    let modifier = BinModifier::default()
        .with_fill_values(VectorMatchFillValues::default().with_lhs(0.0).with_rhs(0.0));

    let expr = BinaryExpr {
        op: TokenType::new(T_ADD),
        lhs: Box::new(Expr::NumberLiteral(NumberLiteral { val: 0.0 })),
        rhs: Box::new(Expr::NumberLiteral(NumberLiteral { val: 0.0 })),
        modifier: Some(modifier),
    };

    let mut left = Vec::with_capacity(n);
    let mut right = Vec::with_capacity(n);

    for i in 0..n {
        let mut labels = Labels::empty();
        labels.insert("id", i.to_string());

        left.push(EvalSample {
            timestamp_ms: 1,
            value: i as f64,
            labels: labels.clone(),
            drop_name: false,
        });

        right.push(EvalSample {
            timestamp_ms: 1,
            value: i as f64,
            labels,
            drop_name: false,
        });
    }

    match eval_binop_vector_vector(&expr, left, right) {
        Ok(ExprResult::InstantVector(v)) => v.len(),
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use promql_parser::parser::token::{T_ADD, T_DIV, T_GTR, TokenType};
    use promql_parser::parser::{
        BinModifier, BinaryExpr, Expr, NumberLiteral, VectorMatchFillValues,
    };

    // ── helpers ─────────────────────────────────────────────────────────────

    fn dummy_expr() -> Box<Expr> {
        Box::new(Expr::NumberLiteral(NumberLiteral { val: 0.0 }))
    }

    /// Build a BinaryExpr for `op` (a raw token-id constant like `T_ADD`) with
    /// an optional BinModifier.
    fn make_expr(op: u16, modifier: Option<BinModifier>) -> BinaryExpr {
        BinaryExpr {
            op: TokenType::new(op),
            lhs: dummy_expr(),
            rhs: dummy_expr(),
            modifier,
        }
    }

    /// Build an EvalSample from a flat label list.
    fn sample(ts: i64, value: f64, labels: &[(&str, &str)]) -> EvalSample {
        EvalSample {
            timestamp_ms: ts,
            value,
            labels: Labels::from_pairs(labels),
            drop_name: false,
        }
    }

    fn find_sample<'a>(result: &'a [EvalSample], env: &str) -> Option<&'a EvalSample> {
        result.iter().find(|s| s.labels.get("env") == Some(env))
    }

    // ── fill_right: unmatched LHS series gets a fill value for the missing RHS ──

    #[test]
    fn test_fill_right_emits_unmatched_lhs_with_fill_value() {
        // LHS: {env="prod", v=10}, {env="staging", v=5}
        // RHS: {env="prod", v=3}   (no staging on the right)
        // fill_right(0): staging has no RHS match → use RHS=0, emit staging
        let lhs = vec![
            sample(1000, 10.0, &[("env", "prod")]),
            sample(1000, 5.0, &[("env", "staging")]),
        ];
        let rhs = vec![sample(1000, 3.0, &[("env", "prod")])];

        let expr = make_expr(
            T_ADD,
            Some(
                BinModifier::default()
                    .with_fill_values(VectorMatchFillValues::default().with_rhs(0.0)),
            ),
        );

        let result = eval_binop_vector_vector(&expr, lhs, rhs)
            .unwrap()
            .into_instant_vector()
            .unwrap();

        // prod: 10 + 3 = 13
        let prod = find_sample(&result, "prod").expect("prod sample missing");
        assert_eq!(prod.value, 13.0);

        // staging: 5 + fill(0) = 5
        let staging = find_sample(&result, "staging")
            .expect("staging sample missing (fill_right should emit it)");
        assert_eq!(staging.value, 5.0);

        assert_eq!(result.len(), 2);
    }

    // ── fill_left: unmatched RHS series gets a fill value for the missing LHS ──

    #[test]
    fn test_fill_left_emits_unmatched_rhs_with_fill_value() {
        // LHS: {env="prod", v=10}
        // RHS: {env="prod", v=3}, {env="staging", v=7}  (no staging on the left)
        // fill_left(1): staging has no LHS match → use LHS=1, emit staging
        let lhs = vec![sample(1000, 10.0, &[("env", "prod")])];
        let rhs = vec![
            sample(1000, 3.0, &[("env", "prod")]),
            sample(1000, 7.0, &[("env", "staging")]),
        ];

        let expr = make_expr(
            T_ADD,
            Some(
                BinModifier::default()
                    .with_fill_values(VectorMatchFillValues::default().with_lhs(1.0)),
            ),
        );

        let result = eval_binop_vector_vector(&expr, lhs, rhs)
            .unwrap()
            .into_instant_vector()
            .unwrap();

        // prod: 10 + 3 = 13
        let prod = find_sample(&result, "prod").expect("prod sample missing");
        assert_eq!(prod.value, 13.0);

        // staging: fill(1) + 7 = 8
        let staging = find_sample(&result, "staging")
            .expect("staging sample missing (fill_left should emit it)");
        assert_eq!(staging.value, 8.0);

        assert_eq!(result.len(), 2);
    }

    // ── fill (both sides simultaneously) ─────────────────────────────────────

    #[test]
    fn test_fill_both_sides() {
        // LHS: {env="a", v=2}, {env="b", v=4}
        // RHS: {env="a", v=1}, {env="c", v=9}
        // fill_left(0) fill_right(0):
        //   matched  a: 2+1=3
        //   unmatched b (no RHS): fill_right(0) → 4+0=4
        //   unmatched c (no LHS): fill_left(0)  → 0+9=9
        let lhs = vec![
            sample(1000, 2.0, &[("env", "a")]),
            sample(1000, 4.0, &[("env", "b")]),
        ];
        let rhs = vec![
            sample(1000, 1.0, &[("env", "a")]),
            sample(1000, 9.0, &[("env", "c")]),
        ];

        let expr = make_expr(
            T_ADD,
            Some(BinModifier::default().with_fill_values(VectorMatchFillValues::new(0.0, 0.0))),
        );

        let mut result = eval_binop_vector_vector(&expr, lhs, rhs)
            .unwrap()
            .into_instant_vector()
            .unwrap();
        result.sort_by(|x, y| x.labels.cmp(&y.labels));

        assert_eq!(result.len(), 3);
        assert_eq!(find_sample(&result, "a").map(|s| s.value), Some(3.0));
        assert_eq!(find_sample(&result, "b").map(|s| s.value), Some(4.0));
        assert_eq!(find_sample(&result, "c").map(|s| s.value), Some(9.0));
    }

    // ── no fill — existing behaviour unchanged ────────────────────────────────

    #[test]
    fn test_no_fill_drops_unmatched_series() {
        let lhs = vec![
            sample(1000, 10.0, &[("env", "prod")]),
            sample(1000, 5.0, &[("env", "staging")]),
        ];
        let rhs = vec![sample(1000, 3.0, &[("env", "prod")])];

        let expr = make_expr(T_ADD, None);

        let result = eval_binop_vector_vector(&expr, lhs, rhs)
            .unwrap()
            .into_instant_vector()
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 13.0);
    }

    // ── fill with comparison operator ─────────────────────────────────────────

    #[test]
    fn test_fill_right_with_comparison_filters_false() {
        // LHS: {env="prod", v=5}, {env="dev", v=2}
        // RHS: {env="prod", v=3}  (no dev on RHS)
        // fill_right(10):
        //   prod:  5 > 3 = true  → output value = lhs = 5
        //   dev:   2 > fill(10)  = 2 > 10 = false → filtered out (comparison, no bool)
        let lhs = vec![
            sample(1000, 5.0, &[("env", "prod")]),
            sample(1000, 2.0, &[("env", "dev")]),
        ];
        let rhs = vec![sample(1000, 3.0, &[("env", "prod")])];

        let expr = make_expr(
            T_GTR,
            Some(
                BinModifier::default()
                    .with_fill_values(VectorMatchFillValues::default().with_rhs(10.0)),
            ),
        );

        let result = eval_binop_vector_vector(&expr, lhs, rhs)
            .unwrap()
            .into_instant_vector()
            .unwrap();

        assert_eq!(
            result.len(),
            1,
            "false comparison result should be filtered"
        );
        let prod = find_sample(&result, "prod").expect("prod should pass the filter");
        assert_eq!(prod.value, 5.0); // propagates original LHS value
    }

    #[test]
    fn test_fill_right_with_comparison_passes_true() {
        // LHS: {env="dev", v=20}  (no RHS match)
        // fill_right(5):  20 > fill(5) = true → output value = 20
        let lhs = vec![sample(1000, 20.0, &[("env", "dev")])];
        // Keep RHS non-empty to exercise fill path (empty RHS may early-return).
        let rhs = vec![sample(1000, 1.0, &[("env", "other")])]; // no match for "dev"

        let expr = make_expr(
            T_GTR,
            Some(
                BinModifier::default()
                    .with_fill_values(VectorMatchFillValues::default().with_rhs(5.0)),
            ),
        );

        let result = eval_binop_vector_vector(&expr, lhs, rhs)
            .unwrap()
            .into_instant_vector()
            .unwrap();

        let dev = find_sample(&result, "dev").expect("dev should pass fill > comparison");
        assert_eq!(dev.value, 20.0);
    }

    // ── fill on set operators → error ─────────────────────────────────────────

    #[test]
    fn test_fill_on_set_operator_returns_error() {
        use promql_parser::parser::token::T_LOR;

        let lhs = vec![sample(1000, 1.0, &[("env", "prod")])];
        let rhs = vec![sample(1000, 2.0, &[("env", "staging")])];

        let expr = make_expr(
            T_LOR,
            Some(
                BinModifier::default()
                    .with_fill_values(VectorMatchFillValues::default().with_rhs(0.0)),
            ),
        );

        let err = eval_binop_vector_vector(&expr, lhs, rhs);
        assert!(err.is_err(), "fill on set op should return an error");
        match err.unwrap_err() {
            EvaluationError::InternalError(msg) => {
                assert!(
                    msg.contains("set operators"),
                    "error should mention set operators"
                );
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    // ── fill_right NaN: unmatched LHS emits NaN ───────────────────────────────

    #[test]
    fn test_fill_right_nan_emits_nan_for_unmatched() {
        let lhs = vec![
            sample(1000, 10.0, &[("env", "prod")]),
            sample(1000, 5.0, &[("env", "staging")]),
        ];
        let rhs = vec![sample(1000, 3.0, &[("env", "prod")])];

        let expr = make_expr(
            T_ADD,
            Some(
                BinModifier::default()
                    .with_fill_values(VectorMatchFillValues::default().with_rhs(f64::NAN)),
            ),
        );

        let result = eval_binop_vector_vector(&expr, lhs, rhs)
            .unwrap()
            .into_instant_vector()
            .unwrap();

        assert_eq!(result.len(), 2);
        let staging =
            find_sample(&result, "staging").expect("staging should be emitted via NaN fill");
        assert!(staging.value.is_nan(), "5 + NaN should be NaN");
    }

    // ── division by fill zero ─────────────────────────────────────────────────

    #[test]
    fn test_fill_right_zero_division_yields_nan() {
        // Prometheus: division by zero → NaN
        let lhs = vec![sample(1000, 10.0, &[("env", "prod")])];
        // No RHS match → fill_right(0) → 10 / 0 = NaN
        let rhs = vec![sample(1000, 1.0, &[("env", "other")])];

        let expr = make_expr(
            T_DIV,
            Some(
                BinModifier::default()
                    .with_fill_values(VectorMatchFillValues::default().with_rhs(0.0)),
            ),
        );

        let result = eval_binop_vector_vector(&expr, lhs, rhs)
            .unwrap()
            .into_instant_vector()
            .unwrap();

        let prod = find_sample(&result, "prod").expect("prod should be emitted");
        assert!(prod.value.is_nan(), "10 / fill(0) should be NaN");
    }
}
