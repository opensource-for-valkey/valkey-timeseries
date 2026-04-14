use crate::labels::Label;
use crate::promql::Labels;
use crate::promql::hashers::SeriesFingerprint;
use promql_parser::label::METRIC_NAME;
use promql_parser::parser::token::{T_ADD, T_DIV, T_LOR, T_MUL, T_SUB, TokenType};
use promql_parser::parser::{AggregateExpr, BinaryExpr, Expr, LabelModifier};
use twox_hash::xxhash3_128;

/// Returns true if the binary operation changes the metric schema, meaning
/// `__name__` should be dropped from the result. Mirrors Prometheus's `resultMetric`
/// logic in engine.go.
pub(in crate::promql) fn changes_metric_schema(op: TokenType) -> bool {
    matches!(op.id(), T_ADD | T_SUB | T_MUL | T_DIV)
}

/// Compute a match signature for a sample's labels per Prometheus binary op semantics.
/// - No modifier: match on ALL labels except `__name__`
/// - `on(l1, l2)` (Include): match only on listed labels
/// - `ignoring(l1, l2)` (Exclude): match on all labels except listed ones and `__name__`
///
/// This is intentionally separated from `compute_grouping_labels` because their `None`
/// cases have opposite semantics (aggregation groups everything together; binary ops
/// match on all labels).
pub(in crate::promql) fn compute_binary_match_key(
    labels: &Labels,
    matching: Option<&LabelModifier>,
) -> SeriesFingerprint {
    let mut hasher: xxhash3_128::Hasher = Default::default();
    // labels is assumed to be sorted, so no need for pre-processing
    match matching {
        None => labels
            .iter()
            .filter(|&k| k.name != METRIC_NAME)
            .for_each(|label| hash_label(&mut hasher, label)),
        Some(LabelModifier::Include(label_list)) => labels
            .iter()
            .filter(|&l| label_list.labels.contains(&l.name))
            .for_each(|label| hash_label(&mut hasher, label)),
        Some(LabelModifier::Exclude(label_list)) => labels
            .iter()
            .filter(|&l| l.name != METRIC_NAME && !label_list.labels.contains(&l.name))
            .for_each(|label| hash_label(&mut hasher, label)),
    };
    hasher.finish_128()
}

fn hash_label(hasher: &mut xxhash3_128::Hasher, label: &Label) {
    hasher.write(label.name.as_bytes());
    hasher.write(b"0xfe");
    hasher.write(label.value.as_bytes());
}

pub fn compute_grouping_labels(mut labels: Labels, modifier: Option<&LabelModifier>) -> Labels {
    match modifier {
        None => Labels::default(), // No grouping, return empty labels
        Some(LabelModifier::Include(label_list)) => {
            // Keep only specified labels
            labels.retain(|k| label_list.labels.contains(&k.name));
            labels
        }
        Some(LabelModifier::Exclude(label_list)) => {
            // Remove specified labels
            labels.retain(|k| !label_list.labels.contains(&k.name));
            labels
        }
    }
}

/// Compute the result labels for a vector-vector binary operation.
/// Mirrors Prometheus's `resultMetric` (engine.go L3062-3104):
/// 1. Arithmetic ops always drop `__name__`
/// 2. `on()` keeps only listed labels; `ignoring()` removes listed labels
pub(super) fn result_metric(
    mut labels: Labels,
    op: TokenType,
    matching: Option<&LabelModifier>,
) -> Labels {
    if changes_metric_schema(op) {
        labels.remove(METRIC_NAME);
    }
    match matching {
        Some(LabelModifier::Include(label_list)) => {
            labels.retain(|k| label_list.labels.contains(&k.name));
        }
        Some(LabelModifier::Exclude(label_list)) => {
            labels.retain(|k| !label_list.labels.contains(&k.name));
        }
        None => {}
    }
    labels
}

pub fn is_non_grouping(aggregate_expr: &AggregateExpr) -> bool {
    match &aggregate_expr.modifier {
        Some(modifier) => match modifier {
            LabelModifier::Include(l) => l.is_empty(),
            LabelModifier::Exclude(l) => l.is_empty(),
        },
        _ => true,
    }
}

pub(crate) fn can_push_down_common_filters(be: &BinaryExpr) -> bool {
    if be.op.id() == T_LOR {
        return false;
    }
    match (&be.lhs.as_ref(), &be.rhs.as_ref()) {
        (Expr::Aggregate(left), Expr::Aggregate(right)) => {
            !(is_non_grouping(left) || is_non_grouping(right))
        }
        _ => true,
    }
}
