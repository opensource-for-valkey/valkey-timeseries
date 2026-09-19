use crate::labels::{HasFingerprint, SeriesFingerprint, fingerprint_labels};
use crate::promql::engine::label_profile::{
    MAX_PUSHDOWN_VALUES, join_regexp_values, regex_matcher,
};
use crate::promql::exec::types::EvalLabels;
use crate::promql::exec::utils::strip_parens;
use crate::promql::hashers::FingerprintHashSet;
use crate::promql::optimizer::pushdown;
use crate::promql::{EvalResult, EvalSample, EvaluationError, ExprResult};
use ahash::AHashSet;
use promql_parser::label::{METRIC_NAME, MatchOp, Matcher};
use promql_parser::parser::token::{
    T_ADD, T_ATAN2, T_BOTTOMK, T_DIV, T_LIMIT_RATIO, T_LIMITK, T_LOR, T_MOD, T_MUL, T_POW, T_SUB,
    T_TOPK, TokenType,
};
use promql_parser::parser::value::ValueType;
use promql_parser::parser::{AggregateExpr, BinaryExpr, Expr, LabelModifier};
use std::borrow::Cow;

/// Returns true if the binary operation changes the metric schema, meaning
/// `__name__` should be dropped from the result. Mirrors Prometheus's
/// `shouldDropMetricName` in engine.go, which lists `%`, `^` and `atan2`
/// alongside the four basic arithmetic operators.
pub(in crate::promql) fn changes_metric_schema(op: TokenType) -> bool {
    matches!(
        op.id(),
        T_ADD | T_SUB | T_MUL | T_DIV | T_POW | T_MOD | T_ATAN2
    )
}

/// Fingerprint of a sample's *effective* label set: the labels as they will
/// stand once a pending `__name__` drop is applied.
pub(crate) fn get_metric_signature(labels: &EvalLabels, drop_name: bool) -> SeriesFingerprint {
    if !drop_name {
        return labels.fingerprint();
    }
    fingerprint_labels(labels.iter().filter(|l| l.name != METRIC_NAME))
}

pub fn ensure_unique_labelsets(samples: &[EvalSample]) -> EvalResult<()> {
    let mut seen_label_sets = FingerprintHashSet::default();
    for sample in samples {
        let key = get_metric_signature(&sample.labels, sample.drop_name);
        if !seen_label_sets.insert(key) {
            return Err(EvaluationError::DuplicateLabelSet);
        }
    }

    Ok(())
}

// vector_contains_same_label_set checks if a vector has samples with the same labelset
// Such a behavior is semantically undefined
// https://github.com/prometheus/prometheus/issues/4562
pub fn vector_contains_same_label_set(v: &[EvalSample]) -> bool {
    match v {
        [] => false,
        [_first] => false,
        [first, second] => first.labels.fingerprint() == second.labels.fingerprint(),
        _ => {
            let mut seen = FingerprintHashSet::default();
            for sample in v {
                let hash = sample.labels.fingerprint();
                if !seen.insert(hash) {
                    return true;
                }
            }
            false
        }
    }
}

pub(in crate::promql) fn push_down_filters<'a>(
    expr: &'a BinaryExpr,
    first: &ExprResult,
    dest: &'a Expr,
) -> EvalResult<Cow<'a, Expr>> {
    let ExprResult::InstantVector(samples) = first else {
        return Ok(Cow::Borrowed(dest));
    };
    let mut common_filters = get_common_label_filters(samples);
    if !common_filters.is_empty() {
        if let Some(modifier) = &expr.modifier {
            pushdown::trim_filters_by_match_modifier(&mut common_filters, &modifier.matching);
        }
        let mut copy = dest.clone();
        pushdown::push_down_binary_op_filters_in_place(&mut copy, &mut common_filters);
        return Ok(Cow::Owned(copy));
    }
    Ok(Cow::Borrowed(dest))
}

/// Returns true when the aggregation collapses its input into label-less groups,
/// so its result carries no labels for `get_common_label_filters` to derive
/// filters from.
#[inline]
fn is_aggregate_non_grouping(agg: &AggregateExpr) -> bool {
    // topk/bottomk/limitk/limit_ratio select whole input series and pass them
    // through untouched, so their output always carries the input's labels
    // regardless of the modifier.
    if matches!(agg.op.id(), T_TOPK | T_BOTTOMK | T_LIMITK | T_LIMIT_RATIO) {
        return false;
    }
    match &agg.modifier {
        // `sum(x)` and `sum by () (x)` both collapse everything into a single
        // group whose label set is empty (see `EvalLabels::compute_grouping_labels`).
        None => true,
        Some(LabelModifier::Include(args)) => args.labels.is_empty(),
        // `without (...)` retains every label it does not list — including
        // `without ()`, which retains them all.
        Some(LabelModifier::Exclude(_)) => false,
    }
}

/// Returns true when this operand's result can yield common label filters worth
/// pushing into the other side.
fn can_derive_filters_from(expr: &Expr) -> bool {
    // Only instant vectors carry labels to match on. This also keeps the
    // rewriter away from scalar-valued subtrees: pushing a filter into the
    // selector under `scalar(x)` would change the value it yields rather than
    // prune series that could not have matched.
    if expr.value_type() != ValueType::Vector {
        return false;
    }
    match strip_parens(expr) {
        Expr::Aggregate(agg) => !is_aggregate_non_grouping(agg),
        _ => true,
    }
}

pub(in crate::promql) fn can_push_down_common_filters(be: &BinaryExpr) -> bool {
    // When fill modifiers are present, all series from both sides must be considered
    // (the fill pass synthesizes results for series that have no match on the other side).
    // Pushing label filters would incorrectly exclude series that should be included via fill.
    if be
        .modifier
        .as_ref()
        .map(|m| m.fill_values.lhs.is_some() || m.fill_values.rhs.is_some())
        .unwrap_or(false)
    {
        return false;
    }

    // `or` keeps series from both sides, so filters derived from one side must
    // not prune the other.
    if be.op.id() == T_LOR {
        return false;
    }

    // Both sides are checked: filters flow from whichever side is evaluated
    // first into the other, and either side alone being label-less makes the
    // pushdown pointless.
    can_derive_filters_from(&be.lhs) && can_derive_filters_from(&be.rhs)
}

pub(in crate::promql) fn get_common_label_filters(samples: &[EvalSample]) -> Vec<Matcher> {
    // Per label: how many series carry it, and the distinct values they carry.
    // The two are separate counts — a label every series shares with one value
    // (`namespace="prod"` on every pod) is the case this exists for, and it has
    // one distinct value however many series there are.
    let mut kv_map: halfbrown::HashMap<&str, (usize, AHashSet<&str>)> = halfbrown::HashMap::new();
    for ts in samples.iter() {
        for label in ts.labels.iter() {
            // Never push down __name__: binary-op matching always ignores __name__ by default
            // (unless an explicit `on(__name__)` modifier is used). Pushing it down would
            // incorrectly filter out the other side when the two sides have different metric names
            // (e.g. `cpu_usage + memory_bytes`).
            if label.name == METRIC_NAME {
                continue;
            }
            let entry = kv_map.entry(label.name).or_default();
            entry.0 += 1;
            entry.1.insert(label.value);
        }
    }

    let mut lfs: Vec<Matcher> = Vec::with_capacity(kv_map.len());
    for (key, (carried_by, values)) in kv_map {
        if carried_by != samples.len() {
            // Skip the tag, since it doesn't belong to all the time series.
            continue;
        }

        if values.len() > MAX_PUSHDOWN_VALUES {
            // Skip the filter on the given tag, since it needs to enumerate too many unique values.
            // This may slow down the provider for matching time series.
            continue;
        }

        let lf = if values.len() == 1 {
            // Safety: length checked above.
            let val = *values.iter().next().unwrap();
            Matcher::new(MatchOp::Equal, key, val)
        } else {
            regex_matcher(key, join_regexp_values(values))
        };

        lfs.push(lf);
    }

    lfs
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two samples whose labels are identical once pending `__name__` drops
    /// are applied are duplicates, whatever their `drop_name` flags say.
    #[test]
    fn unique_labelsets_compares_effective_labels_across_drop_name_flags() {
        let sample = |labels: &[(&str, &str)], drop_name: bool| EvalSample {
            timestamp_ms: 0,
            value: 1.0,
            labels: EvalLabels::from_pairs(labels),
            drop_name,
        };

        // Materialized state: the dropping sample has already lost its name.
        let materialized = [
            sample(&[("env", "1")], false),
            sample(&[("env", "1")], true),
        ];
        assert!(ensure_unique_labelsets(&materialized).is_err());

        // Pending state: the name is still there but owed.
        let pending = [
            sample(&[("env", "1")], false),
            sample(&[("__name__", "m"), ("env", "1")], true),
        ];
        assert!(ensure_unique_labelsets(&pending).is_err());

        // Genuinely distinct.
        let distinct = [
            sample(&[("env", "1")], false),
            sample(&[("__name__", "m"), ("env", "1")], false),
        ];
        assert!(ensure_unique_labelsets(&distinct).is_ok());
    }

    /// The filters derived from `samples`, as `name op value` strings, sorted.
    fn derived(samples: &[EvalSample]) -> Vec<String> {
        let mut out: Vec<String> = get_common_label_filters(samples)
            .into_iter()
            .map(|m| format!("{}{}{}", m.name, m.op, m.value))
            .collect();
        out.sort();
        out
    }

    fn labelled(labels: &[(&str, &str)]) -> EvalSample {
        EvalSample {
            timestamp_ms: 0,
            value: 1.0,
            labels: EvalLabels::from_pairs(labels),
            drop_name: false,
        }
    }

    /// A label every series carries becomes a filter: an equality when the
    /// value is shared, an alternation when it varies. A label some series
    /// lack, or `__name__`, never does. The shared-value case is the one the
    /// push-down exists for (`namespace="prod"` on every pod), and it must not
    /// depend on how many series there are.
    #[test]
    fn common_label_filters_come_from_labels_every_series_carries() {
        let samples = [
            labelled(&[
                ("__name__", "m"),
                ("region", "us"),
                ("host", "a"),
                ("rack", "1"),
            ]),
            labelled(&[("__name__", "m"), ("region", "us"), ("host", "b")]),
            labelled(&[
                ("__name__", "m"),
                ("region", "us"),
                ("host", "c"),
                ("rack", "2"),
            ]),
        ];
        assert_eq!(derived(&samples), vec!["host=~a|b|c", "region=us"]);

        // One series: the same rule.
        assert_eq!(
            derived(&samples[..1]),
            vec!["host=a", "rack=1", "region=us"]
        );

        // Too many distinct values to enumerate: the label is left out.
        let many: Vec<EvalSample> = (0..61)
            .map(|i| labelled(&[("region", "us"), ("host", &format!("h{i}"))]))
            .collect();
        assert_eq!(derived(&many), vec!["region=us"]);
    }

    /// Parse `query` and return its top-level binary expression.
    fn binary_expr(query: &str) -> BinaryExpr {
        match promql_parser::parser::parse(query).expect("query should parse") {
            Expr::Binary(be) => be,
            other => panic!("expected a binary expression, got {other:?}"),
        }
    }

    fn can_push_down(query: &str) -> bool {
        can_push_down_common_filters(&binary_expr(query))
    }

    #[test]
    fn pushdown_allowed_for_label_bearing_operands() {
        assert!(can_push_down("metric_a * metric_b"));
        assert!(can_push_down("rate(metric_a[5m]) * metric_b"));
    }

    #[test]
    fn or_never_pushes_down() {
        // `or` keeps unmatched series from both sides, so neither side's labels
        // may prune the other.
        assert!(!can_push_down("metric_a or metric_b"));
    }

    #[test]
    fn scalar_operands_carry_no_labels() {
        assert!(!can_push_down("metric_a * 2"));
        assert!(!can_push_down("2 * metric_a"));
        // `scalar(x)` is scalar-valued: pushing a filter into `metric_b` would
        // change the value it yields, not prune a series that could not match.
        assert!(!can_push_down("metric_a > scalar(metric_b)"));
        assert!(!can_push_down("scalar(metric_b) > metric_a"));
    }

    #[test]
    fn aggregation_without_grouping_carries_no_labels() {
        // `sum(x)` collapses to a single label-less group, so there is nothing
        // to derive filters from — even though it has no by/without modifier.
        assert!(!can_push_down("sum(metric_a) * metric_b"));
        assert!(!can_push_down("metric_a * sum(metric_b)"));
        // `by ()` is the explicit spelling of the same thing.
        assert!(!can_push_down("sum by () (metric_a) * metric_b"));
    }

    #[test]
    fn aggregation_with_grouping_keeps_labels() {
        assert!(can_push_down("sum by (job) (metric_a) * metric_b"));
        assert!(can_push_down(
            "sum without (instance) (metric_a) * metric_b"
        ));
        // `without ()` excludes nothing, so it retains every label — it is the
        // most grouping-preserving form, not a non-grouping one.
        assert!(can_push_down("sum without () (metric_a) * metric_b"));
    }

    #[test]
    fn selection_aggregations_pass_labels_through() {
        // topk/bottomk/limitk/limit_ratio return whole input series untouched,
        // so their output carries the input's labels whatever the modifier says.
        assert!(can_push_down("topk(3, metric_a) * metric_b"));
        assert!(can_push_down("bottomk(3, metric_a) * metric_b"));
        assert!(can_push_down("limitk(3, metric_a) by () * metric_b"));
        assert!(can_push_down("topk(3, metric_a) by () * metric_b"));
    }

    #[test]
    fn parenthesized_operands_are_seen_through() {
        assert!(!can_push_down("(sum(metric_a)) * metric_b"));
        assert!(can_push_down("(sum by (job) (metric_a)) * metric_b"));
    }
}
