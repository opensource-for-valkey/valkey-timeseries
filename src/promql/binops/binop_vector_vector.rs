use super::labels::effective_fingerprint;
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
    mut left_vector: Vec<EvalSample>,
    mut right_vector: Vec<EvalSample>,
) -> EvalResult<ExprResult> {
    let matching = expr.modifier.as_ref().and_then(|m| m.matching.as_ref());

    // Only a pair that can match needs its operands' pending drops applied; with
    // an empty operand nothing is matched, so nothing is dropped early either.
    if !left_vector.is_empty() && !right_vector.is_empty() && observes_metric_name(expr) {
        drop_pending_names(&mut left_vector, &mut right_vector);
    }

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

/// True when a *pending* (recorded but unmaterialized) `__name__` drop on an
/// operand could change the result.
///
/// [`compute_binary_match_key`] skips `__name__` for the no-modifier and
/// `ignoring(...)` cases, and [`build_result_labels`] copies only the labels a
/// `group_x(...)` list names. That leaves two ways the name can be read: an
/// `on(...)` list naming it, and a `group_left(...)` / `group_right(...)` list
/// naming it. Set operators take no grouping list, so for them only the first
/// applies.
fn observes_metric_name(expr: &BinaryExpr) -> bool {
    let Some(modifier) = expr.modifier.as_ref() else {
        return false;
    };
    let names_metric = |labels: &[String]| labels.iter().any(|l| l == METRIC_NAME);
    matches!(&modifier.matching, Some(LabelModifier::Include(on)) if names_metric(&on.labels))
        || modifier
            .card
            .labels()
            .is_some_and(|l| names_metric(&l.labels))
}

/// Materialize pending `__name__` drops on both operands. Called only when
/// [`observes_metric_name`] says the match can see them.
///
/// Doing it unconditionally would be wrong as well as slow:
///
/// - A drop materialized here is a drop applied *early*, and that is visible.
///   Prometheus defers name removal to the end of evaluation, which is what
///   lets `sum by (__name__) (metric_total or rate(metric_total[5m]))` put both
///   series in one group and drop the name once, afterwards. Materializing the
///   rate side's pending drop up front splits that into two groups instead.
/// - Removing a label promotes a `Shared` label set to `Owned`, cloning the
///   whole set. Arithmetic used to do this for every operand sample; timed
///   alone, those promotions were 65-73 % of the path at 1000 series or fewer,
///   and changed no output, because [`result_metric`] strips the name from
///   each result anyway.
fn drop_pending_names(left_vector: &mut [EvalSample], right_vector: &mut [EvalSample]) {
    for sample in left_vector.iter_mut().chain(right_vector.iter_mut()) {
        sample.drop_name_if_needed();
    }
}

/// Which operand is the "one" side under the match cardinality: the side whose
/// match keys must be unique. The other is the "many" side.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Sides {
    /// One-to-one and `group_left`.
    OneIsRight,
    /// `group_right`.
    OneIsLeft,
}

impl Sides {
    /// The operand the duplicate error names.
    fn one_side_name(self) -> &'static str {
        match self {
            Sides::OneIsRight => "right",
            Sides::OneIsLeft => "left",
        }
    }

    /// The operands as `(one, many)`.
    fn one_and_many<T>(self, left: T, right: T) -> (T, T) {
        match self {
            Sides::OneIsRight => (right, left),
            Sides::OneIsLeft => (left, right),
        }
    }

    /// The `(many, one)` values of a pair, back in operand order.
    fn operand_order(self, many: f64, one: f64) -> (f64, f64) {
        match self {
            Sides::OneIsRight => (many, one),
            Sides::OneIsLeft => (one, many),
        }
    }
}

struct ArithOpContext<'a> {
    matching: Option<&'a LabelModifier>,
    operator: TokenType,
    /// `operator`, resolved to its scalar function once. Every per-sample
    /// loop calls this rather than re-dispatching on the token, and the one
    /// way dispatch can fail is reported by [`ArithOpContext::new`] before
    /// any sample is touched.
    apply: fn(f64, f64) -> f64,
    is_comparison: bool,
    return_bool: bool,
    has_fill: bool,
    sides: Sides,
    is_one_to_one: bool,
    group_labels: Option<&'a Vec<String>>,
    fill_for_one: Option<f64>,
    fill_for_many: Option<f64>,
}

impl<'a> ArithOpContext<'a> {
    /// Resolve everything the join needs from the expression, once.
    fn new(expr: &'a BinaryExpr) -> EvalResult<Self> {
        let (fill_left, fill_right, card, matching) = match expr.modifier.as_ref() {
            None => (None, None, &VectorMatchCardinality::OneToOne, None),
            Some(modifier) => {
                let card = match &modifier.card {
                    VectorMatchCardinality::ManyToMany => {
                        return Err(EvaluationError::InternalError(
                            "many-to-many cardinality not supported for non-set operators"
                                .to_string(),
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

        let sides = match card {
            VectorMatchCardinality::OneToMany(_) => Sides::OneIsLeft,
            _ => Sides::OneIsRight,
        };

        Ok(Self {
            matching,
            operator,
            apply,
            is_comparison,
            return_bool,
            has_fill,
            sides,
            is_one_to_one: matches!(card, VectorMatchCardinality::OneToOne),
            group_labels: card.labels().map(|l| &l.labels),
            // The fill values follow the cardinality, not the operand, so they are
            // keyed by side kind here rather than through `sides`: `fill_left`
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
        ctx.sides.one_side_name()
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
/// The per-pair rule of the join:
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
/// `lhs_value` / `rhs_value` are in operand order; the caller resolves which
/// operand is which through [`Sides::operand_order`].
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

/// Match keys for a whole operand, spread across threads above
/// [`PARALLEL_MATCH_KEY_THRESHOLD`]. The operand stays in place: both joins
/// map over a borrow and keep the keys alongside.
fn match_keys_vec(
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

/// The result of one pair, or `None` when a false comparison drops it. The
/// "many" sample is consumed: its labels become the result's, moved rather
/// than cloned.
fn build_result_sample(
    ctx: &ArithOpContext<'_>,
    many_sample: EvalSample,
    one_sample: &EvalSample,
) -> Option<EvalSample> {
    let (lhs_val, rhs_val) = ctx.sides.operand_order(many_sample.value, one_sample.value);

    // The shared per-pair value rule; see [`pair_result`].
    let (output_value, drop_name) = pair_result(ctx, lhs_val, rhs_val, many_sample.drop_name)?;

    let result_labels = build_result_labels(
        many_sample.labels,
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
    left_vector: Vec<EvalSample>,
    right_vector: Vec<EvalSample>,
) -> EvalResult<ExprResult> {
    // Resolve the operator before looking at the operands. An unsupported
    // operator is a property of the expression, not of the data, so it is
    // reported for an empty operand too — and identically whether one side is
    // empty or both.
    let ctx = ArithOpContext::new(expr)?;

    // Without a fill, a pair needs both operands.
    if left_vector.is_empty() && right_vector.is_empty()
        || (left_vector.is_empty() || right_vector.is_empty()) && !ctx.has_fill
    {
        return Ok(ExprResult::InstantVector(vec![]));
    }

    let (one_vec, many_vec) = ctx.sides.one_and_many(left_vector, right_vector);
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

/// Every arithmetic and comparison join, with or without modifiers.
///
/// A hash join. The "one" side is indexed by match key; the "many" side is
/// probed in input order, emitting as it goes (or filling a missing "one"
/// operand) and moving each "many" label set into its result; a last sweep
/// fills a missing "many" operand for every "one" series nothing matched. It
/// replaced a merge join that sorted both operands by key and gathered each
/// key's group into a `Vec`, and a separate no-modifier fast path, which is now
/// just the one-to-one, no-fill configuration of this loop.
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
    let one_keys = match_keys_vec(&one_vec, ctx.matching);
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

    let many_keys = match_keys_vec(&many_vec, ctx.matching);
    let mut matched = vec![false; one_vec.len()];
    let mut result = Vec::with_capacity(many_vec.len());
    for (many_sample, key) in many_vec.into_iter().zip(&many_keys) {
        match index.get(key) {
            None => {
                if let Some(fill_val) = ctx.fill_for_one {
                    let fill_one = make_fill_one_sample(&many_sample, fill_val);
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
            result.extend(build_result_sample(ctx, fill_many, one_sample));
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
            let fp = effective_fingerprint(&sample.labels, sample.drop_name);
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
    many_labels: EvalLabels,
    one_sample: &EvalSample,
    operator: TokenType,
    matching: Option<&LabelModifier>,
    group_labels: Option<&Vec<String>>,
) -> EvalLabels {
    let mut labels = result_metric(many_labels, operator, matching);

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
    // One pass: dropping the name first and filtering after rebuilt the set
    // twice, and on a `Shared` set the first rebuild cloned every label.
    let drop_name = super::changes_metric_schema(op);
    let kept_name = |name: &str| !(drop_name && name == METRIC_NAME);
    match matching {
        Some(LabelModifier::Include(label_list)) => {
            labels.retain(|k| kept_name(k.name) && label_list.labels.iter().any(|n| n == k.name));
        }
        Some(LabelModifier::Exclude(label_list)) => {
            labels.retain(|k| kept_name(k.name) && !label_list.labels.iter().any(|n| n == k.name));
        }
        None if drop_name => labels.drop_name(),
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
    right_vector: Vec<EvalSample>,
    matching: Option<&LabelModifier>,
) -> EvalResult<ExprResult> {
    if left_vector.is_empty() {
        return Ok(ExprResult::InstantVector(right_vector));
    }
    if right_vector.is_empty() {
        return Ok(ExprResult::InstantVector(left_vector));
    }

    // Build a set of match keys from the left side
    let left_keys = match_keys_set(&left_vector, matching);

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
    right_vector: Vec<EvalSample>,
    matching: Option<&LabelModifier>,
) -> EvalResult<ExprResult> {
    if left_vector.is_empty() || right_vector.is_empty() {
        return Ok(ExprResult::InstantVector(vec![]));
    }

    // Build a set of match keys from the right side
    let right_keys = match_keys_set(&right_vector, matching);

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
    right_vector: Vec<EvalSample>,
    matching: Option<&LabelModifier>,
) -> EvalResult<ExprResult> {
    if left_vector.is_empty() || right_vector.is_empty() {
        return Ok(ExprResult::InstantVector(left_vector));
    }

    // Build a set of match keys from the right side
    let right_keys = match_keys_set(&right_vector, matching);

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
fn match_keys_set(samples: &[EvalSample], matching: Option<&LabelModifier>) -> FingerprintHashSet {
    let mut keys = FingerprintHashSet::with_capacity(samples.len());
    keys.extend(
        samples
            .iter()
            .map(|s| compute_binary_match_key(&s.labels, matching)),
    );
    keys
}

#[cfg(test)]
mod tests;
