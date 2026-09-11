use super::labels::{compute_binary_match_key, get_metric_signature, result_metric};
use crate::labels::SeriesFingerprint;
use crate::promql::binops::binary_op_fn;
use crate::promql::exec::types::EvalLabels;
use crate::promql::hashers::{FingerprintHashMap, FingerprintHashSet};
use crate::promql::{EvalResult, EvalSample, EvaluationError, ExprResult};
use ahash::HashSetExt;
use orx_parallel::{IntoParIter, IterIntoParIter, ParIter};
use promql_parser::label::METRIC_NAME;
use promql_parser::parser::token::{T_LAND, T_LOR, T_LUNLESS, TokenType};
use promql_parser::parser::{BinaryExpr, LabelModifier, VectorMatchCardinality};
use std::iter::Peekable;
use std::vec::IntoIter;

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
    matching: Option<&'a LabelModifier>,
    operator: TokenType,
    /// `operator`, resolved to its scalar function once. Every per-sample
    /// loop calls this rather than re-dispatching on the token, and the one
    /// way dispatch can fail is reported by [`build_arith_op_context`] before
    /// any sample is touched.
    apply: fn(f64, f64) -> f64,
    is_comparison: bool,
    return_bool: bool,
    has_fill: bool,
    is_group_right: bool,
    is_one_to_one: bool,
    group_labels: Option<&'a Vec<String>>,
    fill_for_one: Option<f64>,
    fill_for_many: Option<f64>,
}

impl ArithOpContext<'_> {
    /// Which operand is the "one" side: the one that must be unique per match
    /// key. The right, unless `group_right` makes it the left.
    ///
    /// Every duplicate-series error names a side through these two, so the
    /// mapping is written once. It used to be repeated at four sites, and one
    /// of them had the branches the wrong way round.
    fn one_side(&self) -> &'static str {
        if self.is_group_right { "left" } else { "right" }
    }

    /// Which operand is the "many" side: the one allowed to repeat a match
    /// key under `group_left`/`group_right`.
    fn many_side(&self) -> &'static str {
        if self.is_group_right { "right" } else { "left" }
    }
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
    let apply = binary_op_fn(operator)?;
    let is_comparison = operator.is_comparison_operator();
    let return_bool = expr.return_bool();
    let has_fill = fill_left.is_some() || fill_right.is_some();

    let is_group_right = matches!(card, VectorMatchCardinality::OneToMany(_));

    Ok(ArithOpContext {
        matching,
        operator,
        apply,
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

/// The scalar outcome of one matched pair: the value to emit and whether the
/// result still owes a `__name__` drop, or `None` when the pair drops out.
///
/// The single home of the rule both joins share, so the hash-join fast path and
/// the general merge join cannot drift apart on it:
///
/// - A non-`bool` comparison is a *filter*, not a rewrite. A false result —
///   exactly `0.0`, which is what the comparison operators return — removes the
///   pair; a true one emits the left operand's value rather than the `1.0` the
///   operator produced.
/// - Everything else (arithmetic, and any comparison with `bool`) emits the
///   operator's result.
/// - `bool` also forces the name drop, as does a pending drop on the "many"
///   side.
///
/// `lhs_value` / `rhs_value` are in operand order; resolving which operand is
/// which is the caller's job, because the fast path always has the "many" side
/// on the left while the general path may have it on either.
#[inline]
fn pair_result(
    ctx: &ArithOpContext<'_>,
    lhs_value: f64,
    rhs_value: f64,
    many_drop_name: bool,
) -> Option<(f64, bool)> {
    let op_value = (ctx.apply)(lhs_value, rhs_value);
    let drop_name = many_drop_name || ctx.return_bool;

    if ctx.is_comparison && !ctx.return_bool {
        if op_value == 0.0 {
            return None;
        }
        return Some((lhs_value, drop_name));
    }

    Some((op_value, drop_name))
}

// ============================================================================
// Fast-path for no-modifier arithmetic / comparison ops
// ============================================================================

/// Returns `true` when the expression carries no modifier — meaning OneToOne
/// cardinality, no on/ignoring label matching, and no fill values.
/// In this state both sides are matched purely on their full label set
/// minus `__name__`, and nothing is emitted for an unmatched key on either
/// side, so [`eval_arith_ops_fast_path`] can do the whole job with one hash
/// probe per sample.
fn can_use_fast_path(ctx: &ArithOpContext<'_>) -> bool {
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
        || ctx
            .group_labels
            .is_some_and(|labels| list_names_metric(labels))
}

/// Sentinel stored in the probe map for an RHS match key seen more than once.
/// Only an error once an LHS sample actually matches the key; a repeated key
/// that matches nothing produces nothing and is legal.
const NO_MATCH: u32 = u32::MAX;
/// Sentinel stored in the probe map for an RHS match key already paired with
/// an LHS sample. Catches a repeated LHS key without a second set.
const CONSUMED: u32 = u32::MAX - 1;

/// The no-modifier case: a hash join, emitting one result per matched key.
///
/// The general path ([`eval_arith_ops_merge_join`]) has to gather each key's
/// group on both sides, because a group may be many-to-one and an unmatched
/// group may need a fill. Here a key has at most one partner on each side and
/// an unmatched key produces nothing, so no group is ever materialized: the
/// RHS is indexed in a hash map keyed by match key, then the LHS is probed
/// against it in a single pass, emitting as it goes.
///
/// This replaced a sort-merge that sorted both sides by match key and walked
/// them with a cursor. The two `sort_unstable_by_key` calls were the only
/// superlinear work in the path, and replacing them with `O(1)` inserts and
/// probes measured 10-17% faster from 1k to 100k series (M2, release
/// build, Criterion A/B against the sort version, which was removed once the
/// hash join won). Emission order is the one behavioural difference: LHS
/// input order rather than ascending match key — the same order the set
/// operators emit.
///
/// Two sentinels ride in the map value, which otherwise holds an index into
/// `right_vector`, so the join needs no auxiliary duplicate-key set.
fn eval_arith_ops_fast_path(
    ctx: &ArithOpContext<'_>,
    left_vector: Vec<EvalSample>,
    right_vector: Vec<EvalSample>,
) -> EvalResult<ExprResult> {
    let operator = ctx.operator;

    let right_keys = collect_match_keys(&right_vector, ctx.matching);

    // Build the probe side. A repeated key is recorded as NO_MATCH rather
    // than counted, so the duplicate is only reported if the LHS matches it.
    let mut index: FingerprintHashMap<u32> =
        FingerprintHashMap::with_capacity_and_hasher(right_vector.len(), Default::default());
    for (i, key) in right_keys.into_iter().enumerate() {
        let slot_i = u32::try_from(i).expect("operand has more than u32::MAX series");
        let slot = *index.entry(key).or_insert(slot_i);
        if slot != slot_i {
            index.insert(key, NO_MATCH);
        }
    }

    let left_keys = collect_match_keys(&left_vector, ctx.matching);

    let mut result = Vec::with_capacity(left_vector.len().min(right_vector.len()));
    for (mut lhs, key) in left_vector.into_iter().zip(left_keys) {
        let Some(&slot) = index.get(&key) else {
            continue;
        };

        // Cardinality. One-to-one means exactly one partner on each side of a
        // matched key. This is the same check the modifier path makes on its
        // match-key groups, and like there it is decided by the grouping
        // alone — a comparison that would come out false still errors.
        //
        // Without the RHS check the fast path emitted one result per RHS
        // partner, which for `a + {env="prod"}` — a bare selector matching
        // several metrics — meant several samples with identical labels. At
        // top level the uniqueness check caught that under a generic message;
        // inside an aggregation it was silently summed.
        if slot == NO_MATCH {
            return Err(duplicate_side_error(ctx.one_side()));
        }
        if slot == CONSUMED {
            return Err(duplicate_side_error(ctx.many_side()));
        }
        index.insert(key, CONSUMED);

        let rhs = &right_vector[slot as usize];

        // The shared per-pair rule, so this cannot drift from the general
        // path's [`build_result_sample`]: a false non-`bool` comparison drops
        // the pair, a true one keeps the LHS value, and everything else emits
        // the operator's result.
        let Some((output_value, drop_name)) = pair_result(ctx, lhs.value, rhs.value, lhs.drop_name)
        else {
            continue;
        };

        // `result_metric` is what strips `__name__` from an arithmetic
        // result. It is the *only* place that happens now: the pass that
        // used to strip it from both operands up front is gone, so this
        // promotes one label set per emitted sample instead of one per
        // operand sample on both sides. With exactly one partner per key the
        // LHS labels are consumed, never cloned.
        let labels = result_metric(std::mem::take(&mut lhs.labels), operator, None);

        result.push(EvalSample {
            timestamp_ms: lhs.timestamp_ms,
            value: output_value,
            labels,
            drop_name,
        });
    }

    Ok(ExprResult::InstantVector(result))
}

/// Match keys for a whole operand, spread across threads above
/// [`PARALLEL_MATCH_KEY_THRESHOLD`] exactly as [`collect_fingerprints`] does.
///
/// Kept separate from `collect_fingerprints` because the hash join needs the
/// samples to stay in place: it maps over a borrow and returns keys alone,
/// where the merge join consumes its operand into `(key, sample)` pairs so it
/// can sort them.
fn collect_match_keys(
    samples: &[EvalSample],
    matching: Option<&LabelModifier>,
) -> Vec<SeriesFingerprint> {
    if samples.len() >= PARALLEL_MATCH_KEY_THRESHOLD {
        samples
            .iter()
            .iter_into_par()
            .map(|s| compute_binary_match_key(&s.labels, matching))
            .collect()
    } else {
        samples
            .iter()
            .map(|s| compute_binary_match_key(&s.labels, matching))
            .collect()
    }
}

fn build_result_sample(
    ctx: &ArithOpContext<'_>,
    many_sample: &EvalSample,
    one_sample: &EvalSample,
) -> Option<EvalSample> {
    // Determine operand order based on grouping, then apply the operator.
    let (lhs_val, rhs_val) = if ctx.is_group_right {
        (one_sample.value, many_sample.value)
    } else {
        (many_sample.value, one_sample.value)
    };

    // The shared per-pair value rule; see [`pair_result`].
    let (output_value, drop_name) = pair_result(ctx, lhs_val, rhs_val, many_sample.drop_name)?;

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
    // Resolve the operator before looking at the operands. An unsupported
    // operator is a property of the expression, not of the data, so it is
    // reported for an empty operand too — and identically whether one side is
    // empty or both. Building the context used to sit below the both-empty
    // return, which made an unsupported operator error for `[] op x` but not
    // for `[] op []`.
    let ctx = build_arith_op_context(expr)?;

    if left_vector.is_empty() && right_vector.is_empty() {
        return Ok(ExprResult::InstantVector(vec![]));
    }

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

    // Determine which side is "one" vs. "many" for matching purposes.
    // For one-to-one mappings, we treat the right-hand side as the "one" side.
    let (one_vec, many_vec) = if ctx.is_group_right {
        (left_vector, right_vector)
    } else {
        (right_vector, left_vector)
    };

    let result = eval_arith_ops_merge_join(&ctx, one_vec, many_vec)?;
    Ok(ExprResult::InstantVector(result))
}

/// The general case: every modifier combination the fast path declines.
///
/// Both sides become sorted `(match key, sample)` vectors and are zip-merged.
/// Because both are ordered by key, an unmatched key on either side falls out
/// of the merge as a run with no partner and is handled inline through the
/// fill modifiers; there is no separate "unmatched" pass. A matched key's
/// group is gathered on each side so its cardinality can be checked before
/// the combinations are emitted.
fn eval_arith_ops_merge_join(
    ctx: &ArithOpContext<'_>,
    one_vec: Vec<EvalSample>,
    many_vec: Vec<EvalSample>,
) -> EvalResult<Vec<EvalSample>> {
    let mut result = Vec::with_capacity(many_vec.len());

    let mut many_it = collect_fingerprints(ctx, many_vec).into_iter().peekable();
    let mut one_it = collect_fingerprints(ctx, one_vec).into_iter().peekable();

    loop {
        match (
            many_it.peek().map(|(k, _)| *k),
            one_it.peek().map(|(k, _)| *k),
        ) {
            (None, None) => break,
            // Only "many" entries remain — all unmatched.
            (Some(many_key), None) => {
                handle_unmatched_many(ctx, &mut many_it, many_key, &mut result);
            }
            // Only "one" entries remain — all unmatched.
            (None, Some(one_key)) => {
                handle_unmatched_one(ctx, &mut one_it, one_key, &mut result)?;
            }
            (Some(many_key), Some(one_key)) => {
                if many_key < one_key {
                    // "many" key has no "one" partner — unmatched.
                    handle_unmatched_many(ctx, &mut many_it, many_key, &mut result);
                } else if many_key > one_key {
                    // "one" key has no "many" partner — unmatched.
                    handle_unmatched_one(ctx, &mut one_it, one_key, &mut result)?;
                } else {
                    // Matched key on both sides.
                    // Collect groups so we can safely inspect cardinality and then
                    // iterate over all combinations.
                    let one_samples: Vec<_> = take_group(&mut one_it, one_key).collect();
                    let many_samples = take_group(&mut many_it, many_key);

                    // Cardinality is a property of the match-key grouping, not
                    // of the operator, and not of any individual comparison's
                    // truth value: a comparison that would come out false must
                    // still error on an ambiguous match, exactly like an
                    // arithmetic operator would. See [`validate_one_side`] for
                    // when the "one" side has to be unique.
                    validate_one_side(ctx, one_samples.len(), true)?;
                    if ctx.is_one_to_one {
                        let mut iter = many_samples.into_iter();
                        let sample = iter.next().unwrap();
                        if iter.next().is_some() {
                            return Err(duplicate_side_error(ctx.many_side()));
                        }
                        result.extend(handle_match(ctx, &sample, &one_samples));
                        continue;
                    }

                    // `one_samples` holds exactly one element: the check
                    // above returns an error for any longer group.
                    for many_sample in many_samples {
                        result.extend(handle_match(ctx, &many_sample, &one_samples));
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
            let fp = get_metric_signature(&sample.labels, sample.drop_name);
            if !seen.insert(fp) {
                return Err(EvaluationError::InternalError(
                    "multiple matches for labels: grouping labels must ensure unique matches"
                        .to_string(),
                ));
            }
        }
    }

    Ok(result)
}

/// Every combination of one "many" sample with the "one" group it matched.
#[inline]
fn handle_match<'a>(
    ctx: &'a ArithOpContext<'_>,
    many_sample: &'a EvalSample,
    one_samples: &'a [EvalSample],
) -> impl Iterator<Item = EvalSample> + 'a {
    one_samples
        .iter()
        .filter_map(move |one_sample| build_result_sample(ctx, many_sample, one_sample))
}

/// A "many"-side run with no "one" partner: filled if the missing side has a
/// fill value, otherwise skipped.
#[inline]
fn handle_unmatched_many(
    ctx: &ArithOpContext<'_>,
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

/// A "one"-side run with no "many" partner: filled if the missing side has a
/// fill value, otherwise skipped — but its cardinality is still checked.
#[inline]
fn handle_unmatched_one(
    ctx: &ArithOpContext<'_>,
    one_it: &mut Peekable<IntoIter<(SeriesFingerprint, EvalSample)>>,
    one_key: SeriesFingerprint,
    result: &mut Vec<EvalSample>,
) -> EvalResult<()> {
    if let Some(fill_val) = ctx.fill_for_many {
        // Collected rather than streamed because the group has to be validated
        // before anything is emitted, and the fill path needs the samples
        // anyway.
        let one_samples: Vec<EvalSample> = take_group(one_it, one_key).collect();
        validate_one_side(ctx, one_samples.len(), false)?;
        emit_fill_for_many(ctx, one_samples, fill_val, result);
    } else {
        let one_group_len = skip_group_count(one_it, one_key);
        validate_one_side(ctx, one_group_len, false)?;
    }
    Ok(())
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

/// The one place that decides whether a repeated "one"-side match key is an
/// error.
///
/// The rule is a property of the match-key grouping, not of the operator and
/// not of any comparison's truth value:
///
/// - Under `group_left` / `group_right` the "one" side must be unique per
///   match key as soon as it is looked at, matched or not, because the grouping
///   is what keeps the output series identity unique.
/// - Under one-to-one an *unmatched* key emits nothing, so a repeat there is
///   harmless; only a matched key is ambiguous. That is the `matched` flag.
///
/// The three call sites (a matched key, an unmatched key with a fill, an
/// unmatched key without one) differ in when they run, not in the rule, so they
/// ask here rather than each spelling it out.
#[inline]
fn validate_one_side(ctx: &ArithOpContext, group_len: usize, matched: bool) -> EvalResult<()> {
    if group_len > 1 && (matched || !ctx.is_one_to_one) {
        return Err(duplicate_side_error(ctx.one_side()));
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
///
/// Validating the group is the caller's job — it runs [`validate_one_side`]
/// first — so this only emits.
#[inline]
fn emit_fill_for_many(
    ctx: &ArithOpContext,
    one_samples: impl IntoIterator<Item = EvalSample>,
    fill_val: f64,
    result: &mut Vec<EvalSample>,
) {
    for one_sample in one_samples {
        let fill_many = make_fill_many_sample(&one_sample, fill_val);
        if let Some(sample) = build_result_sample(ctx, &fill_many, &one_sample) {
            result.push(sample);
        }
    }
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
// Compiled only under the `bench` feature, for the external Criterion crate
// (`benches/fast_path.rs`). Follows `binop_vector_scalar::VectorScalarCase`:
// the case is built once, each iteration gets a fresh input from `input()`
// inside Criterion's untimed setup, and `run()` returns its result so the
// output is freed untimed too.

#[cfg(feature = "bench")]
mod bench_support {
    use super::eval_binop_vector_vector;
    use crate::labels::Label;
    use crate::promql::exec::types::EvalLabels;
    use crate::promql::{EvalSample, ExprResult};
    use promql_parser::parser::token::{T_ADD, TokenType};
    use promql_parser::parser::{
        BinModifier, BinaryExpr, Expr, NumberLiteral, VectorMatchFillValues,
    };

    /// Which shape of `a + b` to measure.
    ///
    /// The old helpers had an "unaligned" shape that reversed one operand.
    /// The join is keyed on the match key, so operand order does not select a
    /// different path and it measured exactly the same thing as "aligned".
    #[derive(Clone, Copy)]
    pub enum VectorVectorShape {
        /// Every key has a partner: the fast path, all matched.
        Aligned,
        /// Half the keys on each side have a partner. Exercises the
        /// probe-miss path, which the aligned shape never reaches.
        HalfOverlap,
        /// Half overlap under `fill_left`/`fill_right`, which forces the
        /// general merge-join and makes both fill branches emit.
        HalfOverlapWithFill,
    }

    /// A prepared vector-vector operation.
    pub struct VectorVectorCase {
        expr: BinaryExpr,
        n: usize,
        rhs_id_offset: usize,
    }

    impl VectorVectorCase {
        pub fn new(shape: VectorVectorShape, n: usize) -> Self {
            let fill = matches!(shape, VectorVectorShape::HalfOverlapWithFill);
            let expr = BinaryExpr {
                op: TokenType::new(T_ADD),
                lhs: Box::new(Expr::NumberLiteral(NumberLiteral { val: 0.0 })),
                rhs: Box::new(Expr::NumberLiteral(NumberLiteral { val: 0.0 })),
                modifier: fill.then(|| {
                    BinModifier::default().with_fill_values(VectorMatchFillValues::new(0.0, 0.0))
                }),
            };
            let rhs_id_offset = match shape {
                VectorVectorShape::Aligned => 0,
                VectorVectorShape::HalfOverlap | VectorVectorShape::HalfOverlapWithFill => n / 2,
            };
            Self {
                expr,
                n,
                rhs_id_offset,
            }
        }

        /// Untimed per-iteration setup: fresh operands.
        ///
        /// Built new each time rather than cloned, so every label set is a
        /// sole-owner `Shared` Arc — what an instant selector hands over — and
        /// the drop inside the join is a real free, not a refcount decrement.
        pub fn input(&self) -> (Vec<EvalSample>, Vec<EvalSample>) {
            (series(self.n, 0), series(self.n, self.rhs_id_offset))
        }

        /// Returns the result vector so Criterion frees it untimed; at a few
        /// heap allocations per sample the teardown would otherwise dwarf the
        /// join.
        pub fn run(&self, (left, right): (Vec<EvalSample>, Vec<EvalSample>)) -> Vec<EvalSample> {
            match eval_binop_vector_vector(&self.expr, left, right) {
                Ok(ExprResult::InstantVector(v)) => v,
                Ok(_) => unreachable!("vector-vector always yields an instant vector"),
                Err(e) => panic!("{e}"),
            }
        }
    }

    /// `n` series shaped like selector output: `__name__` plus a unique `id`
    /// and two shared labels, held as a sole-owner `Shared` Arc.
    fn series(n: usize, id_offset: usize) -> Vec<EvalSample> {
        (0..n)
            .map(|i| {
                let id = i + id_offset;
                let mut raw = vec![
                    Label::new("__name__".to_string(), "http_requests_total".to_string()),
                    Label::new("id".to_string(), id.to_string()),
                    Label::new("instance".to_string(), format!("10.0.0.{}:9100", id % 50)),
                    Label::new("job".to_string(), "api".to_string()),
                ];
                raw.sort();
                EvalSample {
                    timestamp_ms: 1,
                    value: id as f64,
                    labels: EvalLabels::shared(raw),
                    drop_name: false,
                }
            })
            .collect()
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        /// Guards the benchmark inputs: each shape must reach the path it
        /// claims to measure.
        #[test]
        fn shapes_match_as_configured() {
            let n = 1000;
            let aligned = VectorVectorCase::new(VectorVectorShape::Aligned, n);
            assert_eq!(aligned.run(aligned.input()).len(), n);

            let half = VectorVectorCase::new(VectorVectorShape::HalfOverlap, n);
            assert_eq!(half.run(half.input()).len(), n / 2);

            // Fill emits for every unmatched key on both sides, so all keys
            // from both operands come out: n/2 matched + n/2 + n/2 filled.
            let filled = VectorVectorCase::new(VectorVectorShape::HalfOverlapWithFill, n);
            assert_eq!(filled.run(filled.input()).len(), n + n / 2);
        }
    }
}

#[cfg(feature = "bench")]
pub use bench_support::{VectorVectorCase, VectorVectorShape};

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

    // ── operator validation is independent of the operands ────────────────

    /// An operator the evaluator cannot resolve is a property of the
    /// expression, not of the data, so every operand shape must report it the
    /// same way. Both operands empty used to return before the operator was
    /// even looked at.
    #[test]
    fn test_unsupported_operator_is_reported_for_every_operand_shape() {
        use promql_parser::parser::token::T_TOPK;

        // An aggregate token where a binary operator belongs: something
        // `binary_op_fn` cannot resolve, which the parser would never build.
        let expr = make_expr(T_TOPK, None);
        let one = || vec![sample(1000, 1.0, &[("env", "a")])];

        let shapes = [
            (vec![], vec![]),
            (one(), vec![]),
            (vec![], one()),
            (one(), one()),
        ];

        for (left, right) in shapes {
            let err = eval_arith_ops(&expr, left, right)
                .expect_err("an unsupported operator must be reported");
            assert!(
                err.to_string().contains("not yet implemented"),
                "unexpected error: {err}"
            );
        }
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

    /// A pending `__name__` drop is part of a sample's effective label set:
    /// under `on(__name__)` the side that owes a drop must not match a side
    /// that still carries the name.
    #[test]
    fn pending_name_drop_is_absent_from_on_name_matching() {
        use promql_parser::label::Labels as ModifierLabels;
        use promql_parser::parser::token::T_LAND;

        let modifier = BinModifier::default().with_matching(Some(LabelModifier::Include(
            ModifierLabels::new(vec!["__name__"]),
        )));
        let expr = make_expr(T_LAND, Some(modifier));
        let mut lhs = sample(1000, 1.0, &[("__name__", "left")]);
        lhs.drop_name = true;
        let rhs = sample(1000, 2.0, &[("__name__", "left")]);

        let result = eval_binop_vector_vector(&expr, vec![lhs], vec![rhs])
            .unwrap()
            .into_instant_vector()
            .unwrap();

        assert!(result.is_empty());
    }

    // ── fast-path cardinality ───────────────────────────────────────────────
    //
    // The no-modifier path must enforce one-to-one exactly as the modifier
    // path does. The realistic trigger is a bare selector matching several
    // metrics: `a + {env="prod"}` hands the right side one sample per metric,
    // all with the same match key.

    fn assert_duplicate_on(result: EvalResult<ExprResult>, side: &str) {
        let err = result.expect_err("ambiguous match must error");
        let msg = err.to_string();
        assert!(
            msg.contains(&format!("on the {side} side")),
            "expected the {side} side to be named, got: {msg}"
        );
    }

    #[test]
    fn test_fast_path_duplicate_rhs_errors() {
        let lhs = vec![sample(1000, 1.0, &[("__name__", "a"), ("env", "prod")])];
        let rhs = vec![
            sample(1000, 2.0, &[("__name__", "b"), ("env", "prod")]),
            sample(1000, 3.0, &[("__name__", "c"), ("env", "prod")]),
        ];
        assert_duplicate_on(
            eval_binop_vector_vector(&make_expr(T_ADD, None), lhs, rhs),
            "right",
        );
    }

    #[test]
    fn test_fast_path_duplicate_lhs_errors() {
        let lhs = vec![
            sample(1000, 1.0, &[("__name__", "a"), ("env", "prod")]),
            sample(1000, 2.0, &[("__name__", "b"), ("env", "prod")]),
        ];
        let rhs = vec![sample(1000, 3.0, &[("__name__", "c"), ("env", "prod")])];
        assert_duplicate_on(
            eval_binop_vector_vector(&make_expr(T_ADD, None), lhs, rhs),
            "left",
        );
    }

    #[test]
    fn test_fast_path_duplicate_errors_even_when_comparison_is_false() {
        // Same shape as the one-to-one modifier-path tests below: the
        // ambiguity exists independent of any value, so filtering must not
        // suppress the error.
        let lhs = vec![sample(1000, 1.0, &[("__name__", "a"), ("env", "prod")])];
        let rhs = vec![
            sample(1000, 100.0, &[("__name__", "b"), ("env", "prod")]),
            sample(1000, 200.0, &[("__name__", "c"), ("env", "prod")]),
        ];
        assert_duplicate_on(
            eval_binop_vector_vector(&make_expr(T_GTR, None), lhs, rhs),
            "right",
        );
    }

    #[test]
    fn test_fast_path_unmatched_duplicates_are_ignored() {
        // A repeated key that matches nothing produces nothing, so it is not
        // ambiguous — on either side.
        let lhs = vec![
            sample(1000, 1.0, &[("__name__", "a"), ("env", "prod")]),
            sample(1000, 2.0, &[("__name__", "b"), ("env", "prod")]),
            sample(1000, 5.0, &[("env", "staging")]),
        ];
        let rhs = vec![
            sample(1000, 3.0, &[("__name__", "c"), ("env", "dev")]),
            sample(1000, 4.0, &[("__name__", "d"), ("env", "dev")]),
            sample(1000, 7.0, &[("env", "staging")]),
        ];
        let result = eval_binop_vector_vector(&make_expr(T_ADD, None), lhs, rhs)
            .unwrap()
            .into_instant_vector()
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 12.0);
        assert_eq!(result[0].labels.get("env"), Some("staging"));
    }

    // ── duplicate-series errors name the same side with and without fill ────

    /// Under `group_left` the "one" side is the right. A repeated key there
    /// with no partner on the left is the same condition whether or not a fill
    /// modifier is present, and must be reported the same way. The fill path
    /// used to name the left.
    #[test]
    fn test_fill_duplicate_on_one_side_names_that_side() {
        use promql_parser::label::Labels as ModifierLabels;
        use promql_parser::parser::{LabelModifier, VectorMatchCardinality};

        let matching = || Some(LabelModifier::Include(ModifierLabels::new(vec!["job"])));
        let no_labels = || ModifierLabels::new(Vec::<&str>::new());

        // group_left: many = left, one = right. Duplicate on the right.
        let lhs = vec![sample(1000, 1.0, &[("job", "other")])];
        let rhs = vec![
            sample(1000, 2.0, &[("job", "x"), ("inst", "1")]),
            sample(1000, 3.0, &[("job", "x"), ("inst", "2")]),
        ];
        let group_left = || {
            BinModifier::default()
                .with_matching(matching())
                .with_card(VectorMatchCardinality::ManyToOne(no_labels()))
        };
        assert_duplicate_on(
            eval_binop_vector_vector(
                &make_expr(T_ADD, Some(group_left())),
                lhs.clone(),
                rhs.clone(),
            ),
            "right",
        );
        assert_duplicate_on(
            eval_binop_vector_vector(
                &make_expr(
                    T_ADD,
                    Some(
                        group_left()
                            .with_fill_values(VectorMatchFillValues::default().with_lhs(0.0)),
                    ),
                ),
                lhs,
                rhs,
            ),
            "right",
        );

        // group_right: mirror image. Duplicate on the left.
        let lhs = vec![
            sample(1000, 2.0, &[("job", "x"), ("inst", "1")]),
            sample(1000, 3.0, &[("job", "x"), ("inst", "2")]),
        ];
        let rhs = vec![sample(1000, 1.0, &[("job", "other")])];
        let group_right = || {
            BinModifier::default()
                .with_matching(matching())
                .with_card(VectorMatchCardinality::OneToMany(no_labels()))
        };
        assert_duplicate_on(
            eval_binop_vector_vector(
                &make_expr(T_ADD, Some(group_right())),
                lhs.clone(),
                rhs.clone(),
            ),
            "left",
        );
        assert_duplicate_on(
            eval_binop_vector_vector(
                &make_expr(
                    T_ADD,
                    Some(
                        group_right()
                            .with_fill_values(VectorMatchFillValues::default().with_rhs(0.0)),
                    ),
                ),
                lhs,
                rhs,
            ),
            "left",
        );
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
