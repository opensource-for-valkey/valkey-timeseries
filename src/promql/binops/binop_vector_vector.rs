use super::labels::{compute_binary_match_key, result_metric};
use crate::labels::SeriesFingerprint;
use crate::promql::binops::apply_binary_op;
use crate::promql::exec::types::EvalLabels;
use crate::promql::hashers::FingerprintHashSet;
use crate::promql::{EvalResult, EvalSample, EvaluationError, ExprResult};
use ahash::HashSetExt;
use orx_parallel::{IntoParIter, ParIter};
use promql_parser::label::METRIC_NAME;
use promql_parser::parser::token::{T_LAND, T_LOR, T_LUNLESS, TokenType};
use promql_parser::parser::{BinaryExpr, LabelModifier, VectorMatchCardinality};
use std::iter::Peekable;
use std::vec::IntoIter;
use twox_hash::xxhash3_128;

/// Operand size at which computing match keys is worth spreading across
/// threads — the one remaining fan-out in this module.
///
/// Below it the fan-out costs more than the hashing it splits: one
/// `into_par()` call is ~30-35us on an M2 (release, system allocator), against
/// a match key at ~165ns, so a 100-series operand is ~16us of real work.
///
/// Above it the fan-out is close to free but buys little: measured serial
/// against parallel, 10000 series went 5.3ms -> 4.8ms and 100000 went 75ms ->
/// 70ms, while 50000 went the other way (33ms -> 38ms). Hashing is the only
/// part of this path that allocates nothing, which is why it is the only part
/// left with a fan-out at all; the join beside it is serial at every size.
const PARALLEL_MATCH_KEY_THRESHOLD: usize = 2048;

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

/// True when an `on(...)` / `group_x(...)` label list names `__name__`, and a
/// pending (recorded but unmaterialized) `__name__` drop would therefore be
/// visible to it.
fn list_names_metric(labels: &[String]) -> bool {
    labels.iter().any(|l| l == METRIC_NAME)
}

/// Same question for a match modifier. `ignoring(...)` and the no-modifier case
/// never see `__name__` — [`compute_binary_match_key`] filters it out of both —
/// so only an `on(...)` list naming it counts.
fn matching_observes_metric_name(matching: Option<&LabelModifier>) -> bool {
    matches!(matching, Some(LabelModifier::Include(list)) if list_names_metric(&list.labels))
}

/// Materialize pending `__name__` drops on both operands, when the match key
/// can actually observe them.
///
/// Set operators carry their operands' samples through untouched, so a drop
/// materialized here is a drop applied *early*. That is visible: Prometheus
/// defers name removal to the end of evaluation, which is what lets
/// `sum by (__name__) (metric_total or rate(metric_total[5m]))` put both series
/// in one group and drop the name once, afterwards. Materializing the rate
/// side's pending drop up front splits that into two groups instead.
///
/// Skipping the pass is also what makes the common case free: it is a 2n walk
/// that promotes `Shared` label sets to `Owned` on every sample that owes a
/// drop.
fn drop_names_if_necessary(
    left_vector: &mut [EvalSample],
    right_vector: &mut [EvalSample],
    matching: Option<&LabelModifier>,
) {
    if !matching_observes_metric_name(matching) {
        return;
    }
    for sample in left_vector.iter_mut() {
        sample.drop_name_if_needed();
    }
    for sample in right_vector.iter_mut() {
        sample.drop_name_if_needed();
    }
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
        labels: EvalLabels::empty(),
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

#[inline]
fn duplicate_side_error(side: &str) -> EvaluationError {
    EvaluationError::InternalError(format!(
        "many-to-many matching not allowed: found duplicate series on the {} side of the operation",
        side
    ))
}

// ============================================================================
// Fast-path for no-modifier arithmetic / comparison ops
// ============================================================================

/// Returns `true` when the expression carries no modifier — meaning OneToOne
/// cardinality, no on/ignoring label matching, and no fill values.
/// In this state both sides are matched purely on their full label set
/// minus `__name__`, so a hashmap-free merge-join is safe and correct.
fn can_use_fast_path(ctx: &ArithOpContext<'_>) -> bool {
    // One-to-one, no on/ignoring matching, no fill values => safe merge-join.
    // The `bool` modifier needs no special handling here: it only changes the
    // output of matched pairs (0/1 instead of filter), while unmatched entries
    // drop out of the result exactly as they do without `bool`.
    ctx.is_one_to_one && ctx.matching.is_none() && !ctx.has_fill
}

/// True when a *pending* (recorded but unmaterialized) `__name__` drop on an
/// operand could still change the result.
///
/// `compute_binary_match_key` skips `__name__` for the no-modifier and
/// `ignoring(...)` cases, and `build_result_labels` skips it when copying
/// labels off the "one" side. That leaves two ways the name can be read: an
/// `on(...)` list that names it, and a `group_left(...)`/`group_right(...)`
/// list that names it.
fn observes_metric_name(ctx: &ArithOpContext<'_>) -> bool {
    matching_observes_metric_name(ctx.matching)
        || ctx.group_labels.is_some_and(|labels| list_names_metric(labels))
}

/// Evaluates arithmetic or comparison operations on two vectors, assuming the operation
/// has no modifiers (`fill`, `on`/ `ignoring`, e.t.c).
///
fn eval_arith_ops_fast_path(
    ctx: &ArithOpContext<'_>,
    left_vector: Vec<EvalSample>,
    right_vector: Vec<EvalSample>,
) -> EvalResult<ExprResult> {
    let left_sorted = collect_fingerprints(ctx, left_vector);
    let right_sorted = collect_fingerprints(ctx, right_vector);
    let operator = ctx.operator;
    let is_comparison = ctx.is_comparison;
    let return_bool = ctx.return_bool;

    // Join one LHS sample against the run of RHS samples sharing its match key,
    // appending to `out`.
    let join_one = |(lhs_fp, mut lhs): (SeriesFingerprint, EvalSample),
                    out: &mut Vec<EvalSample>| {
        let start = right_sorted.partition_point(|x| x.0 < lhs_fp);
        let mut k = start;

        while k < right_sorted.len() && right_sorted[k].0 == lhs_fp {
            let (_, rhs) = &right_sorted[k];
            k += 1;

            let Some(op_value) = apply_binary_op(operator, lhs.value, rhs.value).ok() else {
                continue;
            };

            // For non-bool comparisons, filter out false results (0.0).
            if is_comparison && !return_bool && op_value == 0.0 {
                continue;
            }

            // Output value:
            // - comparison & not bool: propagate LHS value when true
            // - comparison & bool: output op_value (1.0 or 0.0)
            // - arithmetic: output op_value
            let output_value = if is_comparison && !return_bool {
                lhs.value
            } else {
                op_value
            };

            let is_last = k == right_sorted.len() || right_sorted[k].0 != lhs_fp;

            // `result_metric` is what strips `__name__` from an arithmetic
            // result. It is the *only* place that happens now: the pass that
            // used to strip it from both operands up front is gone, so this
            // promotes one label set per emitted sample instead of one per
            // operand sample on both sides.
            let labels = if is_last {
                result_metric(std::mem::take(&mut lhs.labels), operator, None)
            } else {
                result_metric(lhs.labels.clone(), operator, None)
            };

            out.push(EvalSample {
                timestamp_ms: lhs.timestamp_ms,
                value: output_value,
                labels,
                drop_name: lhs.drop_name || return_bool,
            });
        }
    };

    // Serial, at every operand size.
    //
    // The join used to fan out across threads unconditionally. Measured serial
    // against parallel on the same build (M2, release, `a + b` and `a > b`,
    // operands shaped like selector output), serial wins at every size from
    // 1000 to 100000 series and there is no crossover above it either:
    //
    // | series  | serial  | parallel |
    // |---------|---------|----------|
    // | 1000    | 1.0 ms  | 3.9 ms   |
    // | 10000   | 12.9 ms | 34.4 ms  |
    // | 100000  | 217 ms  | 348 ms   |
    //
    // The fan-out has nothing left to amortize. The arithmetic is a handful of
    // instructions; the real per-sample cost is building the result label set,
    // which allocates — and allocating on eight threads at once contends far
    // worse than it parallelizes. On top of that, `flat_map` needs a `Vec` per
    // LHS sample and a merge, where this needs one output vector and no merge.
    // Callers are already parallel besides: a range query fans its step loop
    // out and lands here once per step.
    let mut result = Vec::with_capacity(left_sorted.len().min(right_sorted.len()));
    for entry in left_sorted {
        join_one(entry, &mut result);
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

    // Materialize a *pending* `__name__` drop on the operands, but only when
    // something downstream can actually observe the name.
    //
    // This used to be an unconditional `labels.drop_name()` over both operands
    // for every non-comparison operator, on the grounds that arithmetic drops
    // `__name__`. It does — but `result_metric` already strips the name from
    // matched *results*, so the operand pass changed no output. What it did
    // cost was a `Shared` -> `Owned` promotion of all 2n operand label sets:
    // storage hands labels over as an `Arc<[Label]>`, and removing a label
    // clones the whole set. Timed alone, those promotions were 65-73% of this
    // path's total at 1000 series or fewer.
    //
    // A pending drop can only change a result through the match key or the
    // grouping copy, and `compute_binary_match_key` already ignores `__name__`
    // for both the no-modifier and `ignoring(...)` cases — so the pass is
    // needed only under an `on(...)` or `group_x(...)` list naming `__name__`.
    if observes_metric_name(&ctx) {
        for sample in left_vector.iter_mut() {
            sample.drop_name_if_needed();
        }
        for sample in right_vector.iter_mut() {
            sample.drop_name_if_needed();
        }
    }

    // Fast-path: no modifier (OneToOne, no matching, no fills)
    if can_use_fast_path(&ctx) {
        return eval_arith_ops_fast_path(&ctx, left_vector, right_vector);
    }

    #[inline]
    fn handle_match(
        ctx: &ArithOpContext,
        many_sample: &EvalSample,
        one_samples: &[EvalSample],
    ) -> impl Iterator<Item = EvalSample> {
        one_samples
            .iter()
            .filter_map(move |one_sample| build_result_sample(ctx, many_sample, one_sample))
    }

    #[inline]
    fn handle_unmatched_many(
        ctx: &ArithOpContext,
        many_it: &mut Peekable<IntoIter<(SeriesFingerprint, EvalSample)>>,
        many_key: SeriesFingerprint,
        result: &mut Vec<EvalSample>,
    ) {
        if let Some(fill_val) = ctx.fill_for_one {
            let many_samples = take_group(many_it, many_key);
            emit_fill_for_one(ctx, many_samples, fill_val, result);
        } else {
            skip_group(many_it, many_key);
        }
    }

    #[inline]
    fn handle_unmatched_one(
        ctx: &ArithOpContext,
        one_it: &mut Peekable<IntoIter<(SeriesFingerprint, EvalSample)>>,
        one_key: SeriesFingerprint,
        result: &mut Vec<EvalSample>,
    ) -> EvalResult<()> {
        if let Some(fill_val) = ctx.fill_for_many {
            let one_samples = take_group(one_it, one_key);
            emit_fill_for_many(ctx, one_samples, fill_val, result)?;
        } else {
            let one_group_len = skip_group_count(one_it, one_key);
            validate_one_group_len(ctx, one_group_len)?;
        }
        Ok(())
    }

    #[inline]
    fn duplicate_many_side(ctx: &ArithOpContext) -> &'static str {
        if ctx.is_group_right { "right" } else { "left" }
    }

    #[inline]
    fn duplicate_one_side(ctx: &ArithOpContext) -> &'static str {
        if ctx.is_group_right { "left" } else { "right" }
    }

    // Determine which side is "one" vs. "many" for matching purposes.
    // For one-to-one mappings, we treat the right-hand side as the "one" side.
    let (one_vec, many_vec) = if ctx.is_group_right {
        (left_vector, right_vector)
    } else {
        (right_vector, left_vector)
    };

    let mut result = Vec::with_capacity(many_vec.len());

    // Convert both sides to sorted `(fingerprint, EvalSample)` vectors and run a
    // zip-merge (merge-join) over the two sorted sequences. Because both sides
    // are sorted by match key, unmatched items on either side fall out of the
    // merge naturally and are handled inline via the fill modifiers —
    // no separate "unmatched" pass is required.
    let mut many_it = collect_fingerprints(&ctx, many_vec).into_iter().peekable();
    let mut one_it = collect_fingerprints(&ctx, one_vec).into_iter().peekable();

    loop {
        match (
            many_it.peek().map(|(k, _)| *k),
            one_it.peek().map(|(k, _)| *k),
        ) {
            (None, None) => break,
            // Only "many" entries remain — all unmatched.
            (Some(many_key), None) => {
                handle_unmatched_many(&ctx, &mut many_it, many_key, &mut result);
            }
            // Only "one" entries remain — all unmatched.
            (None, Some(one_key)) => {
                handle_unmatched_one(&ctx, &mut one_it, one_key, &mut result)?;
            }
            (Some(many_key), Some(one_key)) => {
                if many_key < one_key {
                    // "many" key has no "one" partner — unmatched.
                    handle_unmatched_many(&ctx, &mut many_it, many_key, &mut result);
                } else if many_key > one_key {
                    // "one" key has no "many" partner — unmatched.
                    handle_unmatched_one(&ctx, &mut one_it, one_key, &mut result)?;
                } else {
                    // Matched key on both sides.
                    // Collect groups so we can safely inspect cardinality and then
                    // iterate over all combinations.
                    let one_samples: Vec<_> = take_group(&mut one_it, one_key).collect();
                    let many_samples = take_group(&mut many_it, many_key);

                    // Cardinality validation is determined purely by label
                    // matching (i.e. by the match-key groups formed above),
                    // not by whether the operator is a comparison or by the
                    // truth value of any individual comparison. A comparison
                    // that would evaluate false must still error on an
                    // ambiguous many-to-one/one-to-many match, exactly like
                    // an arithmetic operator would.
                    if one_samples.len() > 1 {
                        return Err(duplicate_side_error(duplicate_one_side(&ctx)));
                    }
                    if ctx.is_one_to_one {
                        let mut iter = many_samples.into_iter();
                        let sample = iter.next().unwrap();
                        if iter.next().is_some() {
                            return Err(duplicate_side_error(duplicate_many_side(&ctx)));
                        }
                        result.extend(handle_match(&ctx, &sample, &one_samples));
                        continue;
                    }

                    // `one_samples` holds exactly one element: the check
                    // above returns an error for any longer group. A fan-out
                    // over it was unreachable, so this is a single pass.
                    for many_sample in many_samples {
                        result.extend(handle_match(&ctx, &many_sample, &one_samples));
                    }
                }
            }
        }
    }

    // Duplicate detection for grouped matching must occur after comparison
    // filtering so that comparisons can naturally reduce duplicates.
    if !ctx.is_one_to_one {
        let mut seen = FingerprintHashSet::with_capacity(result.len());
        for sample in &result {
            let fp = result_fingerprint(&sample.labels, sample.drop_name);
            if !seen.insert(fp) {
                return Err(EvaluationError::InternalError(
                    "multiple matches for labels: grouping labels must ensure unique matches"
                        .to_string(),
                ));
            }
        }
    }

    Ok(ExprResult::InstantVector(result))
}

#[inline]
fn take_group(
    it: &mut Peekable<IntoIter<(SeriesFingerprint, EvalSample)>>,
    key: SeriesFingerprint,
) -> impl Iterator<Item = EvalSample> + '_ {
    std::iter::from_fn(move || it.next_if(|(next_key, _)| *next_key == key).map(|(_, s)| s))
}

#[inline]
fn skip_group(
    it: &mut Peekable<IntoIter<(SeriesFingerprint, EvalSample)>>,
    key: SeriesFingerprint,
) {
    while it.next_if(|(next_key, _)| *next_key == key).is_some() {}
}

/// Same as `skip_group`, but returns the number of consumed items.
#[inline]
fn skip_group_count(
    it: &mut Peekable<IntoIter<(SeriesFingerprint, EvalSample)>>,
    key: SeriesFingerprint,
) -> usize {
    let mut count = 0;
    while it.next_if(|(next_key, _)| *next_key == key).is_some() {
        count += 1;
    }
    count
}

#[inline]
fn validate_one_group_len(ctx: &ArithOpContext, one_group_len: usize) -> EvalResult<()> {
    // Cardinality is a property of the match-key grouping, independent of
    // whether the operator is a comparison — see the matched-key branch
    // above for the full rationale.
    if !ctx.is_one_to_one && one_group_len > 1 {
        return Err(duplicate_side_error(if ctx.is_group_right {
            "left"
        } else {
            "right"
        }));
    }
    Ok(())
}

/// Emit results for "many" samples whose match key had no "one" partner.
/// Uses `fill_for_one` to synthesize the missing "one" operand; a no-op when
/// no left/right fill is configured for the missing side.
#[inline]
fn emit_fill_for_one(
    ctx: &ArithOpContext,
    many_samples: impl IntoIterator<Item = EvalSample>,
    fill_val: f64,
    result: &mut Vec<EvalSample>,
) {
    for many_sample in many_samples {
        let fill_one = make_fill_one_sample(&many_sample, fill_val);
        if let Some(sample) = build_result_sample(ctx, &many_sample, &fill_one) {
            result.push(sample);
        }
    }
}

/// Emit results for "one" samples whose match key had no "many" partner.
/// Synthesizes a phantom "many" sample (using the "one" sample's labels so the
/// output series identity is preserved) filled with `fill_val`.
#[inline]
fn emit_fill_for_many(
    ctx: &ArithOpContext,
    one_samples: impl IntoIterator<Item = EvalSample>,
    fill_val: f64,
    result: &mut Vec<EvalSample>,
) -> EvalResult<()> {
    // Cardinality is a property of the match-key grouping, independent of
    // whether the operator is a comparison — see the matched-key branch in
    // `eval_arith_ops` for the full rationale.
    let should_check_duplicates = !ctx.is_one_to_one;

    fn process_one(
        ctx: &ArithOpContext,
        one_sample: &EvalSample,
        fill_val: f64,
        result: &mut Vec<EvalSample>,
    ) {
        let fill_many = make_fill_many_sample(one_sample, fill_val);
        if let Some(sample) = build_result_sample(ctx, &fill_many, one_sample) {
            result.push(sample);
        }
    }

    // Validate that a "one" side group does not contain duplicates when grouped
    // (non one-to-one) matching is in effect.
    if should_check_duplicates {
        for (i, sample) in one_samples.into_iter().enumerate() {
            process_one(ctx, &sample, fill_val, result);
            if i == 1 {
                return Err(duplicate_side_error(if ctx.is_group_right {
                    "right"
                } else {
                    "left"
                }));
            }
        }
        return Ok(());
    }

    for one_sample in one_samples {
        process_one(ctx, &one_sample, fill_val, result);
    }

    Ok(())
}

fn collect_fingerprints(
    ctx: &ArithOpContext,
    samples: Vec<EvalSample>,
) -> Vec<(SeriesFingerprint, EvalSample)> {
    // Only a genuinely large operand, where the per-sample hashing dominates
    // the fan-out, goes wide. See [`PARALLEL_MATCH_KEY_THRESHOLD`].
    let mut kvs: Vec<(SeriesFingerprint, EvalSample)> =
        if samples.len() >= PARALLEL_MATCH_KEY_THRESHOLD {
            samples
                .into_par()
                .map(|s| {
                    let key = compute_binary_match_key(&s.labels, ctx.matching);
                    (key, s)
                })
                .collect()
        } else {
            samples
                .into_iter()
                .map(|s| {
                    let key = compute_binary_match_key(&s.labels, ctx.matching);
                    (key, s)
                })
                .collect()
        };

    kvs.sort_unstable_by_key(|(key, _sample)| *key);

    kvs
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
) -> EvalLabels {
    let mut labels = result_metric(many_sample.labels.clone(), operator, matching);

    match group_labels {
        Some(extra) if !extra.is_empty() => {
            for name in extra {
                match one_sample.labels.get(name) {
                    Some(v) => {
                        labels.set(name, v.to_string());
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
            // Uses binary search via EvalLabels::contains — no heap allocation.
            let to_copy = one_sample
                .labels
                .iter()
                .filter(|l| l.name != METRIC_NAME && !many_sample.labels.contains(l.name))
                .map(|l| crate::Label::new(l.name, l.value));

            labels.extend(to_copy);
        }
    }

    labels
}

/// Compute a fingerprint for duplicate detection in grouped matching results.
/// When `drop_name` is true, `__name__` is excluded from the hash to match
/// the effective output labels.
#[inline]
fn result_fingerprint(labels: &EvalLabels, drop_name: bool) -> u128 {
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
    mut left_vector: Vec<EvalSample>,
    mut right_vector: Vec<EvalSample>,
    matching: Option<&LabelModifier>,
) -> EvalResult<ExprResult> {
    if left_vector.is_empty() {
        return Ok(ExprResult::InstantVector(right_vector));
    }
    if right_vector.is_empty() {
        return Ok(ExprResult::InstantVector(left_vector));
    }

    drop_names_if_necessary(&mut left_vector, &mut right_vector, matching);

    // Build a set of match keys from the left side
    let left_keys = get_sample_fingerprints(&left_vector, matching);

    // Append right-side samples whose match key is NOT present on the left.
    // Serial: see `eval_set_and` for why.
    left_vector.extend(
        right_vector
            .into_iter()
            .filter(|s| !left_keys.contains(&compute_binary_match_key(&s.labels, matching))),
    );

    Ok(ExprResult::InstantVector(left_vector))
}

/// `and`: returns LHS samples that have a matching label set on the RHS.
/// Values always come from the LHS.
fn eval_set_and(
    mut left_vector: Vec<EvalSample>,
    mut right_vector: Vec<EvalSample>,
    matching: Option<&LabelModifier>,
) -> EvalResult<ExprResult> {
    if left_vector.is_empty() || right_vector.is_empty() {
        return Ok(ExprResult::InstantVector(vec![]));
    }

    drop_names_if_necessary(&mut left_vector, &mut right_vector, matching);

    // Build a set of match keys from the right side
    let right_keys = get_sample_fingerprints(&right_vector, matching);

    // `retain` rather than a parallel `filter().collect()`. The fan-out lost at
    // every size measured (10 to 10000 series), by 3.3x even at 10000, because
    // it pays for two things `retain` does not: every *kept* sample is moved
    // into a freshly allocated vector, and every *dropped* sample is freed on a
    // worker thread rather than the thread that allocated it. Filtering in
    // place neither reallocates nor migrates a free.
    left_vector.retain(|s| right_keys.contains(&compute_binary_match_key(&s.labels, matching)));

    Ok(ExprResult::InstantVector(left_vector))
}

/// `unless`: returns LHS samples that do NOT have a matching label set on the RHS.
fn eval_set_unless(
    mut left_vector: Vec<EvalSample>,
    mut right_vector: Vec<EvalSample>,
    matching: Option<&LabelModifier>,
) -> EvalResult<ExprResult> {
    if left_vector.is_empty() || right_vector.is_empty() {
        return Ok(ExprResult::InstantVector(left_vector));
    }

    drop_names_if_necessary(&mut left_vector, &mut right_vector, matching);

    // Build a set of match keys from the right side
    let right_keys = get_sample_fingerprints(&right_vector, matching);

    // Serial: see `eval_set_and`.
    left_vector.retain(|s| !right_keys.contains(&compute_binary_match_key(&s.labels, matching)));

    Ok(ExprResult::InstantVector(left_vector))
}

/// Match keys of every sample, as a set.
///
/// Built straight into the set. This used to fan the hashing out into a
/// `Vec<SeriesFingerprint>` and then collect that into the set, which paid for
/// a fan-out and an intermediate allocation to save a hash that is a few
/// hundred nanoseconds.
fn get_sample_fingerprints(
    samples: &[EvalSample],
    matching: Option<&LabelModifier>,
) -> FingerprintHashSet {
    let mut keys = FingerprintHashSet::with_capacity(samples.len());
    keys.extend(
        samples
            .iter()
            .map(|s| compute_binary_match_key(&s.labels, matching)),
    );
    keys
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
        let mut labels = EvalLabels::empty();
        labels.set("id", i.to_string());

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
        let mut labels = EvalLabels::empty();
        labels.set("id", i.to_string());

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
        let mut labels = EvalLabels::empty();
        labels.set("id", i.to_string());

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
            labels: EvalLabels::from_pairs(labels),
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
        // RHS: {env="prod", v=3} (no staging on the right)
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
        // RHS: {env="prod", v=3}, {env="staging", v=7} (no staging on the left)
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

    // ── no fill — existing behavior unchanged ────────────────────────────────

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
        // RHS: {env="prod", v=3} (no dev on RHS)
        // fill_right(10):
        //   prod:  5 > 3 = true → output value = lhs = 5
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
    fn test_fill_right_zero_division_yields_infinity() {
        // PromQL arithmetic is IEEE 754: dividing a positive value by zero is
        // +Inf, not NaN.
        let lhs = vec![sample(1000, 10.0, &[("env", "prod")])];
        // No RHS match → fill_right(0) → 10 / 0 = +Inf
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
        assert_eq!(prod.value, f64::INFINITY, "10 / fill(0) should be +Inf");
    }

    // ── bool modifier fast-path ─────────────────────────────────────────────

    #[test]
    fn test_bool_fast_path_omits_unmatched_lhs() {
        // LHS: {env="prod", v=5}, {env="dev", v=2}
        // RHS: {env="prod", v=3} (no dev on RHS)
        // op: > bool
        // prod: 5 > 3 = 1.0 (true)
        // dev:  no match => omitted; `bool` only changes the output of matched
        //       pairs, it never turns unmatched entries into results
        let lhs = vec![
            sample(1000, 5.0, &[("env", "prod")]),
            sample(1000, 2.0, &[("env", "dev")]),
        ];
        let rhs = vec![sample(1000, 3.0, &[("env", "prod")])];

        let expr = make_expr(T_GTR, Some(BinModifier::default().with_return_bool(true)));

        let result = eval_binop_vector_vector(&expr, lhs, rhs)
            .unwrap()
            .into_instant_vector()
            .unwrap();

        assert_eq!(result.len(), 1);
        let prod = find_sample(&result, "prod").expect("prod sample missing");
        assert_eq!(prod.value, 1.0);
        assert!(prod.drop_name);

        assert!(
            find_sample(&result, "dev").is_none(),
            "unmatched lhs series must be omitted, even with bool"
        );
    }

    #[test]
    fn test_bool_fast_path_arithmetic() {
        // LHS: {env="prod", v=10}
        // RHS: {env="prod", v=3}
        // op: + bool
        // Prometheus: 10 + 3 = 13, but __name__ is dropped, and it behaves like bool in terms of drop_name
        let lhs = vec![sample(1000, 10.0, &[("env", "prod")])];
        let rhs = vec![sample(1000, 3.0, &[("env", "prod")])];

        let expr = make_expr(T_ADD, Some(BinModifier::default().with_return_bool(true)));

        let result = eval_binop_vector_vector(&expr, lhs, rhs)
            .unwrap()
            .into_instant_vector()
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 13.0);
        assert!(result[0].drop_name);
    }

    #[test]
    fn test_bool_fast_path_true_and_false() {
        // LHS: {id="1", v=10}, {id="2", v=5}, {id="3", v=1}
        // RHS: {id="1", v=2}, {id="2", v=7}, {id="4", v=10}
        // op: > bool
        // 1: 10 > 2 = 1.0
        // 2: 5 > 7 = 0.0
        // 3: no match => omitted
        // (RHS id=4 doesn't match anything on LHS, so it's dropped)
        let lhs = vec![
            sample(1000, 10.0, &[("id", "1")]),
            sample(1000, 5.0, &[("id", "2")]),
            sample(1000, 1.0, &[("id", "3")]),
        ];
        let rhs = vec![
            sample(1000, 2.0, &[("id", "1")]),
            sample(1000, 7.0, &[("id", "2")]),
            sample(1000, 10.0, &[("id", "4")]),
        ];

        let expr = make_expr(T_GTR, Some(BinModifier::default().with_return_bool(true)));

        let mut result = eval_binop_vector_vector(&expr, lhs, rhs)
            .unwrap()
            .into_instant_vector()
            .unwrap();
        result.sort_by(|x, y| x.labels.cmp(&y.labels));

        assert_eq!(result.len(), 2);
        assert_eq!(
            result
                .iter()
                .find(|s| s.labels.get("id") == Some("1"))
                .unwrap()
                .value,
            1.0
        );
        assert_eq!(
            result
                .iter()
                .find(|s| s.labels.get("id") == Some("2"))
                .unwrap()
                .value,
            0.0
        );
        assert!(
            result.iter().all(|s| s.labels.get("id") != Some("3")),
            "unmatched lhs series id=3 must be omitted, even with bool"
        );
        for s in &result {
            assert!(s.drop_name);
        }
    }

    // ── name dropping for %, ^ and atan2 ────────────────────────────────────

    /// Prometheus `shouldDropMetricName` covers `%`, `^` and `atan2` alongside
    /// the four basic arithmetic operators. Asserted here rather than in the
    /// promqltest suite because a `{...}` expectation there matches whether or
    /// not `__name__` is present.
    #[test]
    fn test_mod_pow_atan2_drop_metric_name() {
        use promql_parser::parser::token::{T_ATAN2, T_MOD, T_POW};

        for op in [T_MOD, T_POW, T_ATAN2] {
            let lhs = vec![sample(1000, 8.0, &[("__name__", "a"), ("env", "prod")])];
            let rhs = vec![sample(1000, 3.0, &[("__name__", "b"), ("env", "prod")])];

            let result = eval_binop_vector_vector(&make_expr(op, None), lhs, rhs)
                .unwrap()
                .into_instant_vector()
                .unwrap();

            assert_eq!(result.len(), 1, "op {op:?} should match one pair");
            let mut only = result.into_iter().next().unwrap();
            only.drop_name_if_needed();
            assert_eq!(
                only.labels.get("__name__"),
                None,
                "op {op:?} must drop __name__"
            );
            assert_eq!(only.labels.get("env"), Some("prod"));
        }
    }

    /// The basic arithmetic operators, for contrast: same expectation, and the
    /// case the old eager input pass was really covering.
    #[test]
    fn test_arithmetic_drops_metric_name() {
        let lhs = vec![sample(1000, 8.0, &[("__name__", "a"), ("env", "prod")])];
        let rhs = vec![sample(1000, 3.0, &[("__name__", "b"), ("env", "prod")])];

        let result = eval_binop_vector_vector(&make_expr(T_ADD, None), lhs, rhs)
            .unwrap()
            .into_instant_vector()
            .unwrap();

        let mut only = result.into_iter().next().unwrap();
        only.drop_name_if_needed();
        assert_eq!(only.labels.get("__name__"), None);
    }

    /// A non-bool comparison keeps the LHS name, including its metric name.
    #[test]
    fn test_comparison_keeps_metric_name() {
        let lhs = vec![sample(1000, 8.0, &[("__name__", "a"), ("env", "prod")])];
        let rhs = vec![sample(1000, 3.0, &[("__name__", "b"), ("env", "prod")])];

        let result = eval_binop_vector_vector(&make_expr(T_GTR, None), lhs, rhs)
            .unwrap()
            .into_instant_vector()
            .unwrap();

        let mut only = result.into_iter().next().unwrap();
        only.drop_name_if_needed();
        assert_eq!(only.labels.get("__name__"), Some("a"));
    }

    // ── comparison cardinality validation ───────────────────────────────────

    #[test]
    fn test_one_to_one_comparison_errors_on_ambiguous_many_side() {
        use promql_parser::label::Labels as ModifierLabels;
        use promql_parser::parser::LabelModifier;

        // LHS: two series sharing job="a" (differ by instance) — ambiguous
        // one-to-one match against a single RHS series with job="a". This
        // must error regardless of the comparison's truth value: cardinality
        // is decided by the match key, not by the comparison result.
        let lhs = vec![
            sample(1000, 5.0, &[("job", "a"), ("instance", "1")]),
            sample(1000, 3.0, &[("job", "a"), ("instance", "2")]),
        ];
        let rhs = vec![sample(1000, 1.0, &[("job", "a")])];

        let modifier = BinModifier::default().with_matching(Some(LabelModifier::Include(
            ModifierLabels::new(vec!["job"]),
        )));
        let expr = make_expr(T_GTR, Some(modifier));

        let err = eval_binop_vector_vector(&expr, lhs, rhs).expect_err(
            "ambiguous one-to-one match must error even though both sides compare true",
        );
        assert!(
            err.to_string()
                .contains("many-to-many matching not allowed"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_one_to_one_comparison_errors_even_when_all_matches_are_false() {
        use promql_parser::label::Labels as ModifierLabels;
        use promql_parser::parser::LabelModifier;

        // Same shape as above, but both comparisons would evaluate false.
        // Without bool, a false comparison is normally filtered out of the
        // result — but that filtering must not suppress the cardinality
        // error, since the ambiguity exists independent of any value.
        let lhs = vec![
            sample(1000, 1.0, &[("job", "a"), ("instance", "1")]),
            sample(1000, 2.0, &[("job", "a"), ("instance", "2")]),
        ];
        let rhs = vec![sample(1000, 100.0, &[("job", "a")])];

        let modifier = BinModifier::default().with_matching(Some(LabelModifier::Include(
            ModifierLabels::new(vec!["job"]),
        )));
        let expr = make_expr(T_GTR, Some(modifier));

        let err = eval_binop_vector_vector(&expr, lhs, rhs).expect_err(
            "ambiguous one-to-one match must error even though both sides compare false",
        );
        assert!(
            err.to_string()
                .contains("many-to-many matching not allowed"),
            "unexpected error: {err}"
        );
    }
}
