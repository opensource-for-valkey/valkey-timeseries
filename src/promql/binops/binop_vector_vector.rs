use super::labels::get_metric_signature;
use crate::common::threads::IterIntoParRayon;
use crate::labels::{SeriesFingerprint, create_unseeded_hasher, hash_key_value};
use crate::promql::binops::binary_op_fn;
use crate::promql::exec::types::EvalLabels;
use crate::promql::hashers::{FingerprintHashMap, FingerprintHashSet};
use crate::promql::{EvalResult, EvalSample, EvaluationError, ExprResult};
use ahash::HashSetExt;
use orx_parallel::Par;
use promql_parser::label::METRIC_NAME;
use promql_parser::parser::token::{T_LAND, T_LOR, T_LUNLESS, TokenType};
use promql_parser::parser::{BinaryExpr, LabelModifier, VectorMatchCardinality};

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
    /// key. The right, unless `group_right` makes it the left. The duplicate
    /// error names it, so the mapping is written once.
    fn one_side(&self) -> &'static str {
        if self.is_group_right { "left" } else { "right" }
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
        // The fill values follow the cardinality, not the operand: `fill_left`
        // fills the "many" side and `fill_right` the "one" side. Under group_left
        // and one-to-one matching that is the left and the right operand. Under
        // group_right it is the reverse, so `node_meta * on(instance)
        // group_right fill_left(1) cpu_info` fills a missing cpu_info (the right
        // operand).
        //
        // That is what Prometheus implements (it swaps the operands for
        // group_right and keeps the fill values) and what its own
        // `fill-modifier.test` pins; this repository's copy has the same cases.
        // Prometheus' operator documentation describes the fills per operand
        // instead ("fill in missing matches on the left side"), which disagrees
        // with its implementation under group_right. The implementation is
        // followed here.
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

/// The "many" sample a fill stands in for: the fill value, labelled with the
/// "one" sample's *match* labels only, as Prometheus labels it
/// (`Labels.MatchLabels`): with `on(...)` just the listed labels, otherwise
/// everything but the ignored labels and `__name__`.
///
/// It used to take all of the "one" sample's labels, so under `group_left`
/// the filled series carried the one side's extra labels
/// (`{owner="team-c", status="404"}` where Prometheus returns
/// `{status="404"}`), and a comparison kept the one side's metric name.
#[inline]
fn make_fill_many_sample(
    matching: Option<&LabelModifier>,
    one_sample: &EvalSample,
    fill_value: f64,
) -> EvalSample {
    EvalSample {
        timestamp_ms: one_sample.timestamp_ms,
        value: fill_value,
        labels: match_labels(&one_sample.labels, matching),
        drop_name: one_sample.drop_name,
    }
}

/// The labels of `labels` that identify its match group, as Prometheus'
/// `Labels.MatchLabels` selects them: with `on(...)` just the listed labels,
/// otherwise everything but the ignored labels and `__name__`.
fn match_labels(labels: &EvalLabels, matching: Option<&LabelModifier>) -> EvalLabels {
    let mut labels = labels.clone();
    match matching {
        Some(LabelModifier::Include(on)) => {
            labels.retain(|l| on.labels.iter().any(|name| name == l.name));
        }
        Some(LabelModifier::Exclude(ignoring)) => labels.retain(|l| {
            l.name != METRIC_NAME && !ignoring.labels.iter().any(|name| name == l.name)
        }),
        None => labels.retain(|l| l.name != METRIC_NAME),
    }
    labels
}

/// Two series on the "one" side share a match key: Prometheus' error, word for
/// word, naming the match group and the two series in sorted order so the
/// message is the same on every run (upstream `operators.test` pins it).
#[cold]
fn one_side_duplicate_error(
    ctx: &ArithOpContext<'_>,
    a: &EvalSample,
    b: &EvalSample,
) -> EvaluationError {
    let group = label_set_string(&match_labels(&a.labels, ctx.matching), false);
    let (mut first, mut second) = (
        label_set_string(&a.labels, a.drop_name),
        label_set_string(&b.labels, b.drop_name),
    );
    if first > second {
        std::mem::swap(&mut first, &mut second);
    }
    EvaluationError::InternalError(format!(
        "found duplicate series for the match group {group} on the {} hand-side of the \
         operation: [{first}, {second}];many-to-many matching not allowed: matching labels must \
         be unique on one side",
        ctx.one_side()
    ))
}

/// Under one-to-one matching, a match key repeated on the "many" side.
#[cold]
fn many_side_duplicate_error() -> EvaluationError {
    EvaluationError::InternalError(
        "multiple matches for labels: many-to-one matching must be explicit (group_left/group_right)"
            .to_string(),
    )
}

/// A label set as Prometheus prints one: `{__name__="m", job="api"}`, names in
/// order, values quoted, a name that is not a legacy identifier quoted too. A
/// `__name__` that is pending removal is left out, as Prometheus has already
/// removed it.
fn label_set_string(labels: &EvalLabels, drop_name: bool) -> String {
    fn quote(out: &mut String, s: &str) {
        out.push('"');
        for c in s.chars() {
            match c {
                '"' => out.push_str("\\\""),
                '\\' => out.push_str("\\\\"),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                '\t' => out.push_str("\\t"),
                c if c.is_control() => out.push_str(&format!("\\u{:04x}", c as u32)),
                c => out.push(c),
            }
        }
        out.push('"');
    }
    fn is_legacy_name(name: &str) -> bool {
        let mut chars = name.chars();
        chars
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
            && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
    }
    let mut out = String::from("{");
    let mut first = true;
    for label in labels.iter() {
        if drop_name && label.name == METRIC_NAME {
            continue;
        }
        if !first {
            out.push_str(", ");
        }
        first = false;
        if is_legacy_name(label.name) {
            out.push_str(label.name);
        } else {
            quote(&mut out, label.name);
        }
        out.push('=');
        quote(&mut out, label.value);
    }
    out.push('}');
    out
}

/// The scalar outcome of one matched pair: the value to emit and whether the
/// result still owes a `__name__` drop, or `None` when the pair drops out.
///
/// The single home of the rule both joins share, so the no-modifier fast path
/// and the general join cannot drift apart on it:
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
/// that matches nothing produces nothing and is legal. (Prometheus rejects it
/// while indexing the right-hand side; see [`eval_arith_ops_hash_join`] for why this
/// engine does not.)
const NO_MATCH: u32 = u32::MAX;
/// Sentinel stored in the probe map for an RHS match key already paired with
/// an LHS sample. Catches a repeated LHS key without a second set.
const CONSUMED: u32 = u32::MAX - 1;

/// The no-modifier case: a hash join, emitting one result per matched key.
///
/// The general join ([`eval_arith_ops_hash_join`]) is a hash join too, but
/// carries what a modifier can ask for: many-to-one groups, fills on either
/// side, and a result-wide duplicate check. Here a key has at most one partner
/// on each side and an unmatched key produces nothing: the RHS is indexed by
/// match key, then the LHS is probed against it in a single pass, emitting as
/// it goes, and each LHS label set is moved into its result rather than cloned.
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
    for (i, &key) in right_keys.iter().enumerate() {
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
            // The index kept no slot for a repeated key; find the two series
            // again to name them.
            let mut repeats = right_keys
                .iter()
                .zip(&right_vector)
                .filter(|(k, _)| **k == key)
                .map(|(_, sample)| sample);
            let (Some(a), Some(b)) = (repeats.next(), repeats.next()) else {
                unreachable!("a NO_MATCH key occurs at least twice");
            };
            return Err(one_side_duplicate_error(ctx, a, b));
        }
        if slot == CONSUMED {
            return Err(many_side_duplicate_error());
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
        // result. It is the *only* place that happens: this
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
/// [`PARALLEL_MATCH_KEY_THRESHOLD`]. The operand stays in place: both joins
/// map over a borrow and keep the keys alongside.
fn collect_match_keys(
    samples: &[EvalSample],
    matching: Option<&LabelModifier>,
) -> Vec<SeriesFingerprint> {
    if samples.len() >= PARALLEL_MATCH_KEY_THRESHOLD {
        samples
            .iter()
            .iter_into_par_rayon()
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

    let result = eval_arith_ops_hash_join(&ctx, one_vec, many_vec)?;
    Ok(ExprResult::InstantVector(result))
}

/// Where a "one"-side match key sits in the operand: one position, or the first
/// two of a key that repeats (kept so the error can name both series).
#[derive(Clone, Copy)]
enum OneSlot {
    Unique(u32),
    Repeated(u32, u32),
}

/// The general case: every modifier combination the fast path declines.
///
/// A hash join. The "one" side is indexed by match key; the "many" side is
/// probed in input order, emitting as it goes (or filling a missing "one"
/// operand); a last sweep fills a missing "many" operand for every "one"
/// series nothing matched. It replaced a merge join that sorted both operands
/// by key and gathered each key's group into a `Vec`.
///
/// Cardinality is a property of the match-key grouping, never of the operator
/// or of a comparison's outcome (a false comparison still errors):
///
/// - Under `group_left` / `group_right` the "one" side must be unique per
///   match key, matched or not, because the grouping is what keeps the output
///   series identity unique.
/// - Under one-to-one a repeated "one" key is an error only once something
///   matches it, and a repeated "many" key only once it matches. An unmatched
///   repeat emits nothing, so it is harmless.
///
///   Prometheus is stricter: it rejects any repeat on the one side while
///   indexing it, matched or not. Here filter push-down reads each operand
///   narrowed by the other's labels, so an unmatched repeat is often never
///   read at all; erroring on it would make a query fail or succeed depending
///   on the data and the push-down settings. Tolerating it keeps the result
///   the same either way. A *filled* unmatched repeat does emit, once per
///   series; the result-wide duplicate check rejects it only if those series
///   collide.
fn eval_arith_ops_hash_join(
    ctx: &ArithOpContext<'_>,
    one_vec: Vec<EvalSample>,
    many_vec: Vec<EvalSample>,
) -> EvalResult<Vec<EvalSample>> {
    let one_keys = collect_match_keys(&one_vec, ctx.matching);
    let mut index: FingerprintHashMap<OneSlot> =
        FingerprintHashMap::with_capacity_and_hasher(one_vec.len(), Default::default());
    for (i, &key) in one_keys.iter().enumerate() {
        let i = u32::try_from(i).expect("operand has more than u32::MAX series");
        index
            .entry(key)
            .and_modify(|slot| {
                if let OneSlot::Unique(first) = *slot {
                    *slot = OneSlot::Repeated(first, i);
                }
            })
            .or_insert(OneSlot::Unique(i));
    }
    let repeated_error =
        |a: u32, b: u32| one_side_duplicate_error(ctx, &one_vec[a as usize], &one_vec[b as usize]);

    // Under grouping the "one" side must be unique per key, matched or not.
    if !ctx.is_one_to_one
        && let Some(&OneSlot::Repeated(a, b)) = index
            .values()
            .find(|slot| matches!(slot, OneSlot::Repeated(..)))
    {
        return Err(repeated_error(a, b));
    }

    let many_keys = collect_match_keys(&many_vec, ctx.matching);
    let mut matched = vec![false; one_vec.len()];
    let mut result = Vec::with_capacity(many_vec.len());
    for (many_sample, key) in many_vec.iter().zip(&many_keys) {
        match index.get(key) {
            None => {
                if let Some(fill_val) = ctx.fill_for_one {
                    let fill_one = make_fill_one_sample(many_sample, fill_val);
                    result.extend(build_result_sample(ctx, many_sample, &fill_one));
                }
            }
            Some(&OneSlot::Repeated(a, b)) => return Err(repeated_error(a, b)),
            Some(&OneSlot::Unique(i)) => {
                let i = i as usize;
                if ctx.is_one_to_one && matched[i] {
                    return Err(many_side_duplicate_error());
                }
                matched[i] = true;
                result.extend(build_result_sample(ctx, many_sample, &one_vec[i]));
            }
        }
    }

    // A "one" series nothing matched: filled if the missing "many" operand has
    // a fill value. A repeated key lands here only under one-to-one, once per
    // series; the check below rejects the fills if they collide.
    if let Some(fill_val) = ctx.fill_for_many {
        for (one_sample, _) in one_vec
            .iter()
            .zip(&matched)
            .filter(|(_, matched)| !**matched)
        {
            let fill_many = make_fill_many_sample(ctx.matching, one_sample, fill_val);
            result.extend(build_result_sample(ctx, &fill_many, one_sample));
        }
    }

    // Duplicate detection for grouped matching must occur after comparison
    // filtering so that comparisons can naturally reduce duplicates.
    //
    // One-to-one matching produces unique label sets by construction, except
    // through a fill: an unmatched match key repeated on either side (which is
    // tolerated when it emits nothing, see above) is filled once
    // per series, and the `on`/`ignoring` projection can reduce those to one
    // label set. Only a collision is an error: filled series that stay distinct
    // (a comparison keeps `__name__`) are fine.
    if !ctx.is_one_to_one || ctx.has_fill {
        let mut seen = FingerprintHashSet::with_capacity(result.len());
        for sample in &result {
            let fp = get_metric_signature(&sample.labels, sample.drop_name);
            if !seen.insert(fp) {
                let msg = if ctx.is_one_to_one {
                    "multiple matches for labels: a fill would emit the same labels more than \
                     once; matching labels must be unique on each side"
                } else {
                    "multiple matches for labels: grouping labels must ensure unique matches"
                };
                return Err(EvaluationError::InternalError(msg.to_string()));
            }
        }
    }

    Ok(result)
}

/// Compute a match signature for a sample's labels per Prometheus binary op semantics.
/// - No modifier: match on ALL labels except `__name__`
/// - `on(l1, l2)` (Include): match only on listed labels
/// - `ignoring(l1, l2)` (Exclude): match on all labels except listed ones and `__name__`
///
/// This is intentionally separated from `compute_grouping_labels` because their `None`
/// cases have opposite semantics (aggregation groups everything together; binary ops
/// match on all labels).
fn compute_binary_match_key(
    labels: &EvalLabels,
    matching: Option<&LabelModifier>,
) -> SeriesFingerprint {
    let mut hasher = create_unseeded_hasher();
    let listed = |name: &str, list: &LabelModifier| match list {
        LabelModifier::Include(l) | LabelModifier::Exclude(l) => l.labels.iter().any(|n| n == name),
    };
    match matching {
        None => labels
            .iter()
            .filter(|k| k.name != METRIC_NAME)
            .for_each(|label| hash_key_value(&mut hasher, label.name, label.value)),
        Some(m @ LabelModifier::Include(_)) => labels
            .iter()
            .filter(|l| listed(l.name, m))
            .for_each(|label| hash_key_value(&mut hasher, label.name, label.value)),
        Some(m @ LabelModifier::Exclude(_)) => labels
            .iter()
            .filter(|l| l.name != METRIC_NAME && !listed(l.name, m))
            .for_each(|label| hash_key_value(&mut hasher, label.name, label.value)),
    };
    hasher.finish_128()
}

/// Build the result label set for a matched pair.
///
/// The "many" side's labels per [`result_metric`], plus the labels listed by
/// `group_left(<labels>)` / `group_right(<labels>)` taken from the "one" side.
fn build_result_labels(
    many_sample: &EvalSample,
    one_sample: &EvalSample,
    operator: TokenType,
    matching: Option<&LabelModifier>,
    group_labels: Option<&Vec<String>>,
) -> EvalLabels {
    let mut labels = result_metric(many_sample.labels.clone(), operator, matching);

    // Only the labels a `group_left(...)` / `group_right(...)` modifier lists
    // come from the "one" side: set to its value, or deleted when it has none.
    // Nothing else is copied across, for one-to-one or many-to-one matching
    // alike — as in Prometheus's `resultMetric`.
    for name in group_labels.into_iter().flatten() {
        labels.copy_label_from(name, &one_sample.labels);
    }

    labels
}

/// Compute the result labels for a vector-vector binary operation.
/// Mirrors Prometheus's `resultMetric` (engine.go L3062-3104):
/// 1. Arithmetic ops always drop `__name__`
/// 2. `on()` keeps only listed labels; `ignoring()` removes listed labels
fn result_metric(
    mut labels: EvalLabels,
    op: TokenType,
    matching: Option<&LabelModifier>,
) -> EvalLabels {
    if super::changes_metric_schema(op) {
        labels.drop_name();
    }
    match matching {
        Some(LabelModifier::Include(label_list)) => {
            labels.retain(|k| label_list.labels.iter().any(|n| n == k.name));
        }
        Some(LabelModifier::Exclude(label_list)) => {
            labels.retain(|k| !label_list.labels.iter().any(|n| n == k.name));
        }
        None => {}
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
        /// general join and makes both fill branches emit.
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

    /// `requests * on(job) group_left(owner) info`: every result copies
    /// `owner` from its "one"-side series. Both operands carry interned
    /// labels, as a selector's results do, so this measures the path
    /// production takes (the shapes above use `Shared` labels).
    pub struct GroupLeftCase {
        expr: BinaryExpr,
        n: usize,
        groups: usize,
    }

    impl GroupLeftCase {
        pub fn new(n: usize) -> Self {
            use promql_parser::label::Labels as ModifierLabels;
            use promql_parser::parser::token::T_MUL;
            use promql_parser::parser::{LabelModifier, VectorMatchCardinality};
            let modifier = BinModifier::default()
                .with_matching(Some(LabelModifier::Include(ModifierLabels::new(vec![
                    "job",
                ]))))
                .with_card(VectorMatchCardinality::ManyToOne(ModifierLabels::new(
                    vec!["owner"],
                )));
            Self {
                expr: BinaryExpr {
                    op: TokenType::new(T_MUL),
                    lhs: Box::new(Expr::NumberLiteral(NumberLiteral { val: 0.0 })),
                    rhs: Box::new(Expr::NumberLiteral(NumberLiteral { val: 0.0 })),
                    modifier: Some(modifier),
                },
                n,
                groups: (n / 10).max(1),
            }
        }

        /// Untimed per-iteration setup: `n` request series over `n / 10` jobs,
        /// and one info series per job.
        pub fn input(&self) -> (Vec<EvalSample>, Vec<EvalSample>) {
            use crate::labels::MetricName;
            let interned = |pairs: &[(&str, &str)], value: f64| EvalSample {
                timestamp_ms: 1,
                value,
                labels: EvalLabels::interned(&MetricName::from_pairs(pairs.iter().copied())),
                drop_name: false,
            };
            let many = (0..self.n)
                .map(|i| {
                    let (job, instance) = (format!("job{}", i % self.groups), i.to_string());
                    interned(
                        &[
                            ("__name__", "requests"),
                            ("instance", &instance),
                            ("job", &job),
                            ("method", "GET"),
                        ],
                        i as f64,
                    )
                })
                .collect();
            let one = (0..self.groups)
                .map(|g| {
                    let (job, owner) = (format!("job{g}"), format!("team-{g}"));
                    interned(
                        &[
                            ("__name__", "info"),
                            ("job", &job),
                            ("owner", &owner),
                            ("version", "v1"),
                        ],
                        1.0,
                    )
                })
                .collect();
            (many, one)
        }

        pub fn run(&self, (left, right): (Vec<EvalSample>, Vec<EvalSample>)) -> Vec<EvalSample> {
            match eval_binop_vector_vector(&self.expr, left, right) {
                Ok(ExprResult::InstantVector(v)) => v,
                Ok(_) => unreachable!("vector-vector always yields an instant vector"),
                Err(e) => panic!("{e}"),
            }
        }
    }

    /// `a + on(l) b` against `a + on(l) group_right b` over the same operands:
    /// `n` series a side, one partner each, so both produce `n` results. Either
    /// interned labels (a local selector's output) or `Shared` ones (what a
    /// cluster range read decodes into).
    pub struct OnMatchCase {
        expr: BinaryExpr,
        n: usize,
        interned: bool,
    }

    impl OnMatchCase {
        pub fn new(group_right: bool, interned: bool, n: usize) -> Self {
            use promql_parser::label::Labels as ModifierLabels;
            use promql_parser::parser::{LabelModifier, VectorMatchCardinality};
            let mut modifier = BinModifier::default()
                .with_matching(Some(LabelModifier::Include(ModifierLabels::new(vec!["l"]))));
            if group_right {
                modifier = modifier.with_card(VectorMatchCardinality::OneToMany(
                    ModifierLabels::new(Vec::<&str>::new()),
                ));
            }
            Self {
                expr: BinaryExpr {
                    op: TokenType::new(T_ADD),
                    lhs: Box::new(Expr::NumberLiteral(NumberLiteral { val: 0.0 })),
                    rhs: Box::new(Expr::NumberLiteral(NumberLiteral { val: 0.0 })),
                    modifier: Some(modifier),
                },
                n,
                interned,
            }
        }

        pub fn input(&self) -> (Vec<EvalSample>, Vec<EvalSample>) {
            use crate::labels::MetricName;
            let make = |pairs: &[(&str, &str)], value: f64| EvalSample {
                timestamp_ms: 1,
                value,
                labels: if self.interned {
                    EvalLabels::interned(&MetricName::from_pairs(pairs.iter().copied()))
                } else {
                    let mut raw: Vec<Label> =
                        pairs.iter().map(|(n, v)| Label::new(*n, *v)).collect();
                    raw.sort();
                    EvalLabels::shared(raw)
                },
                drop_name: false,
            };
            let side = |name: &str| -> Vec<EvalSample> {
                (0..self.n)
                    .map(|i| {
                        let (l, inst) = (i.to_string(), format!("10.0.0.{}:9100", i % 50));
                        make(
                            &[
                                ("__name__", name),
                                ("instance", &inst),
                                ("job", "api"),
                                ("l", &l),
                            ],
                            i as f64,
                        )
                    })
                    .collect()
            };
            (side("a"), side("b"))
        }

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
pub use bench_support::{GroupLeftCase, OnMatchCase, VectorVectorCase, VectorVectorShape};

#[cfg(test)]
mod tests {
    use super::*;
    use promql_parser::parser::token::{T_ADD, T_DIV, T_GTR, T_NEQ, TokenType};
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

    // ── fill over a repeated, unmatched match key ─────────────────────────────

    fn on_job_with_fill(op: u16, fill: VectorMatchFillValues) -> BinaryExpr {
        use promql_parser::label::Labels as ModifierLabels;
        let modifier = BinModifier::default()
            .with_matching(Some(LabelModifier::Include(ModifierLabels::new(vec![
                "job",
            ]))))
            .with_fill_values(fill);
        make_expr(op, Some(modifier))
    }

    /// Two right-hand series share `job="x"` and nothing on the left matches it. Without a
    /// fill the repeat is tolerated (it emits nothing; see `eval_arith_ops_hash_join`). With
    /// `fill_left` both were emitted, and `on(job)` reduces both to `{job="x"}`: two series
    /// with one label set, which `sum(...)` would silently double-count.
    #[test]
    fn test_fill_rejects_duplicates_from_a_repeated_one_side_key() {
        let lhs = vec![sample(1000, 1.0, &[("job", "y")])];
        let rhs = vec![
            sample(1000, 2.0, &[("job", "x"), ("instance", "1")]),
            sample(1000, 3.0, &[("job", "x"), ("instance", "2")]),
        ];
        let filled = on_job_with_fill(T_ADD, VectorMatchFillValues::default().with_lhs(0.0));
        let err = eval_binop_vector_vector(&filled, lhs.clone(), rhs.clone())
            .expect_err("two {job=\"x\"} results");
        assert!(
            err.to_string().contains("multiple matches for labels"),
            "{err}"
        );

        let unfilled = on_job_with_fill(T_ADD, VectorMatchFillValues::default());
        assert!(eval_binop_vector_vector(&unfilled, lhs, rhs).is_ok());
    }

    /// The same on the left: under one-to-one, a repeated unmatched left key was filled once
    /// per series.
    #[test]
    fn test_fill_rejects_duplicates_from_a_repeated_many_side_key() {
        let lhs = vec![
            sample(1000, 2.0, &[("job", "x"), ("instance", "1")]),
            sample(1000, 3.0, &[("job", "x"), ("instance", "2")]),
        ];
        let rhs = vec![sample(1000, 1.0, &[("job", "y")])];
        let filled = on_job_with_fill(T_ADD, VectorMatchFillValues::default().with_rhs(0.0));
        let err = eval_binop_vector_vector(&filled, lhs.clone(), rhs.clone())
            .expect_err("two {job=\"x\"} results");
        assert!(
            err.to_string().contains("multiple matches for labels"),
            "{err}"
        );

        let unfilled = on_job_with_fill(T_ADD, VectorMatchFillValues::default());
        assert!(eval_binop_vector_vector(&unfilled, lhs, rhs).is_ok());
    }

    /// A repeated key is only a problem if the filled results collide. A comparison without
    /// `bool` keeps `__name__`, so two left series that differ only by name stay distinct.
    #[test]
    fn test_fill_keeps_repeated_keys_that_stay_distinct() {
        let lhs = vec![
            sample(1000, 2.0, &[("__name__", "a1"), ("job", "x")]),
            sample(1000, 3.0, &[("__name__", "a2"), ("job", "x")]),
        ];
        let rhs = vec![sample(1000, 1.0, &[("__name__", "c"), ("job", "y")])];
        let expr = make_expr(
            T_GTR,
            Some(
                BinModifier::default()
                    .with_fill_values(VectorMatchFillValues::default().with_rhs(0.0)),
            ),
        );
        let result = eval_binop_vector_vector(&expr, lhs, rhs)
            .unwrap()
            .into_instant_vector()
            .unwrap();
        assert_eq!(result.len(), 2);
    }

    // ── labels of a filled series ─────────────────────────────────────────────
    //
    // Prometheus labels a filled series with the partner's match labels only
    // (`MatchLabels`). The conformance DSL's label checks cannot see extra labels,
    // so these compare exact label sets, on the shapes of upstream's
    // fill-modifier.test.

    fn exact_labels(sample: &EvalSample) -> Vec<(String, String)> {
        sample
            .labels
            .iter()
            .map(|l| (l.name.to_string(), l.value.to_string()))
            .collect()
    }

    fn filled_labels(result: &[EvalSample], value: f64) -> Vec<(String, String)> {
        let filled: Vec<_> = result.iter().filter(|s| s.value == value).collect();
        assert_eq!(filled.len(), 1, "expected one result with value {value}");
        exact_labels(filled[0])
    }

    fn pairs(labels: &[(&str, &str)]) -> Vec<(String, String)> {
        labels
            .iter()
            .map(|(n, v)| (n.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn test_filled_many_series_takes_only_the_on_labels() {
        use promql_parser::label::Labels as ModifierLabels;
        use promql_parser::parser::VectorMatchCardinality;
        // requests + on(status) group_left fill_left(0) limits
        let lhs = vec![sample(
            0,
            100.0,
            &[
                ("__name__", "requests"),
                ("method", "GET"),
                ("status", "200"),
            ],
        )];
        let rhs = vec![
            sample(
                0,
                1000.0,
                &[
                    ("__name__", "limits"),
                    ("owner", "team-a"),
                    ("status", "200"),
                ],
            ),
            sample(
                0,
                500.0,
                &[
                    ("__name__", "limits"),
                    ("owner", "team-c"),
                    ("status", "404"),
                ],
            ),
        ];
        let modifier = BinModifier::default()
            .with_matching(Some(LabelModifier::Include(ModifierLabels::new(vec![
                "status",
            ]))))
            .with_card(VectorMatchCardinality::ManyToOne(ModifierLabels::new(
                vec![],
            )))
            .with_fill_values(VectorMatchFillValues::default().with_lhs(0.0));
        let result = eval_binop_vector_vector(&make_expr(T_ADD, Some(modifier)), lhs, rhs)
            .unwrap()
            .into_instant_vector()
            .unwrap();
        assert_eq!(filled_labels(&result, 500.0), pairs(&[("status", "404")]));
    }

    #[test]
    fn test_filled_many_series_drops_ignored_labels_and_the_name() {
        use promql_parser::label::Labels as ModifierLabels;
        use promql_parser::parser::VectorMatchCardinality;
        // left_vector + ignoring(job) group_left fill(0) right_vector
        let lhs = vec![sample(
            0,
            10.0,
            &[
                ("__name__", "left_vector"),
                ("instance", "a"),
                ("job", "foo"),
            ],
        )];
        let rhs = vec![
            sample(
                0,
                100.0,
                &[
                    ("__name__", "right_vector"),
                    ("instance", "a"),
                    ("job", "foo"),
                ],
            ),
            sample(
                0,
                300.0,
                &[
                    ("__name__", "right_vector"),
                    ("instance", "c"),
                    ("job", "foo"),
                ],
            ),
        ];
        let modifier = BinModifier::default()
            .with_matching(Some(LabelModifier::Exclude(ModifierLabels::new(vec![
                "job",
            ]))))
            .with_card(VectorMatchCardinality::ManyToOne(ModifierLabels::new(
                vec![],
            )))
            .with_fill_values(VectorMatchFillValues::default().with_lhs(0.0).with_rhs(0.0));
        let result = eval_binop_vector_vector(&make_expr(T_ADD, Some(modifier)), lhs, rhs)
            .unwrap()
            .into_instant_vector()
            .unwrap();
        assert_eq!(filled_labels(&result, 300.0), pairs(&[("instance", "c")]));
    }

    #[test]
    fn test_filled_series_in_a_comparison_has_no_metric_name() {
        // left_vector != fill(30) right_vector: the filled left operand for
        // label="d" has no name of its own, and a comparison keeps the left labels.
        let lhs = vec![sample(
            0,
            10.0,
            &[("__name__", "left_vector"), ("label", "a")],
        )];
        let rhs = vec![
            sample(0, 100.0, &[("__name__", "right_vector"), ("label", "a")]),
            sample(0, 400.0, &[("__name__", "right_vector"), ("label", "d")]),
        ];
        let expr = make_expr(
            T_NEQ,
            Some(
                BinModifier::default().with_fill_values(
                    VectorMatchFillValues::default()
                        .with_lhs(30.0)
                        .with_rhs(30.0),
                ),
            ),
        );
        let result = eval_binop_vector_vector(&expr, lhs, rhs)
            .unwrap()
            .into_instant_vector()
            .unwrap();
        assert_eq!(filled_labels(&result, 30.0), pairs(&[("label", "d")]));
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

    /// Two label sets that differ only in where one label ends and the next
    /// begins used to share a match key (the separator was the text `0xfe`,
    /// with nothing after a value), so these unrelated series were joined.
    #[test]
    fn test_label_boundaries_are_part_of_the_match_key() {
        // Label sets are hashed in name order, so the names are chosen to sort
        // the way the bytes must line up.
        for rhs_labels in [&[("a", "xb0xfey")][..], &[("a", ""), ("xb", "y")][..]] {
            let lhs = vec![sample(1000, 10.0, &[("a", "x"), ("b", "y")])];
            let rhs = vec![sample(1000, 3.0, rhs_labels)];
            let result = eval_binop_vector_vector(&make_expr(T_ADD, None), lhs, rhs)
                .unwrap()
                .into_instant_vector()
                .unwrap();
            assert!(
                result.is_empty(),
                "{rhs_labels:?} matched {{a=\"x\", b=\"y\"}}"
            );
        }
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

    /// A match key repeated on the "one" side: Prometheus' message, naming
    /// that side.
    fn assert_duplicate_on(result: EvalResult<ExprResult>, side: &str) {
        let err = result.expect_err("ambiguous match must error");
        let msg = err.to_string();
        assert!(
            msg.contains("found duplicate series for the match group")
                && msg.contains(&format!("on the {side} hand-side of the operation"))
                && msg.contains("many-to-many matching not allowed"),
            "expected a duplicate on the {side} side, got: {msg}"
        );
    }

    /// Under one-to-one matching, a match key repeated on the "many" side:
    /// Prometheus' many-to-one message, which names no side.
    fn assert_many_side_duplicate(result: EvalResult<ExprResult>) {
        let err = result.expect_err("ambiguous match must error");
        let msg = err.to_string();
        assert!(
            msg.contains(
                "multiple matches for labels: many-to-one matching must be explicit \
                 (group_left/group_right)"
            ),
            "expected the many-to-one error, got: {msg}"
        );
    }

    /// Upstream `operators.test` pins this message word for word, the two
    /// series sorted so it is the same on every run.
    #[test]
    fn test_one_side_duplicate_error_matches_prometheus() {
        use promql_parser::label::Labels as ModifierLabels;
        let lhs = vec![sample(0, 3.0, &[("__name__", "scalar_metric")])];
        let rhs = vec![
            sample(0, 2.0, &[("__name__", "dup_metric"), ("label", "beta")]),
            sample(0, 1.0, &[("__name__", "dup_metric"), ("label", "alpha")]),
        ];
        let modifier = BinModifier::default().with_matching(Some(LabelModifier::Include(
            ModifierLabels::new(Vec::<&str>::new()),
        )));
        let err = eval_binop_vector_vector(&make_expr(T_GTR, Some(modifier)), lhs, rhs)
            .expect_err("two dup_metric series on the one side");
        let expected = "found duplicate series for the match group {} on the right hand-side of \
             the operation: [{__name__=\"dup_metric\", label=\"alpha\"}, \
             {__name__=\"dup_metric\", label=\"beta\"}];many-to-many matching not allowed: \
             matching labels must be unique on one side";
        assert!(err.to_string().ends_with(expected), "got: {err}");
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
        assert_many_side_duplicate(eval_binop_vector_vector(&make_expr(T_ADD, None), lhs, rhs));
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
                .contains("many-to-one matching must be explicit"),
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
                .contains("many-to-one matching must be explicit"),
            "unexpected error: {err}"
        );
    }
}
