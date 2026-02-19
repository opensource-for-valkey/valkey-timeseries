use crate::promql::{EvalResult, EvalSample, EvaluationError, Labels};
use ahash::AHashMap;
use orx_parallel::{IterIntoParIter, ParIter};
use promql_parser::label::METRIC_NAME;
use promql_parser::parser::token::{T_BOTTOMK, T_TOPK};
use promql_parser::parser::{AggregateExpr, LabelModifier};
use std::cmp::Ordering;
use std::collections::BinaryHeap;

#[derive(Clone, Copy, Eq, PartialEq)]
enum KAggregationOrder {
    Top,
    Bottom,
}

/// Compares values for topk/bottomk aggregation.
/// NaN values are always considered "greater" (sorted last) regardless of order.
/// Uses partial_cmp for IEEE 754 semantics (-0.0 == +0.0), matching Prometheus.
fn compare_k_values(left: f64, right: f64, order: KAggregationOrder) -> Ordering {
    match (left.is_nan(), right.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => match order {
            KAggregationOrder::Top => right.partial_cmp(&left).unwrap_or(Ordering::Equal),
            KAggregationOrder::Bottom => left.partial_cmp(&right).unwrap_or(Ordering::Equal),
        },
    }
}

#[derive(Clone, Copy)]
struct KHeapEntry {
    value: f64,
    index: usize,
    order: KAggregationOrder,
}

impl PartialEq for KHeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index
            && self.order == other.order
            && self.value.to_bits() == other.value.to_bits()
    }
}

impl Eq for KHeapEntry {}

impl PartialOrd for KHeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for KHeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap. Define "greater" as "worse" so heap.peek()
        // returns the least desirable currently selected sample.
        compare_k_values(self.value, other.value, self.order)
            .then_with(|| self.index.cmp(&other.index))
    }
}

pub(super) fn evaluate_k_aggregate(
    aggregate: &AggregateExpr,
    k: usize,
    mut samples: Vec<EvalSample>,
) -> EvalResult<Vec<EvalSample>> {
    let order = match aggregate.op.id() {
        T_TOPK => KAggregationOrder::Top,
        T_BOTTOMK => KAggregationOrder::Bottom,
        _ => {
            return Err(EvaluationError::InternalError(format!(
                "evaluate_k_aggregate called for non-k aggregation operator: {:?}",
                aggregate.op
            )));
        }
    };

    if k == 0 {
        return Ok(vec![]);
    }

    // k-aggregation without `by` / `without`: all samples belong to one
    // implicit group, so skip hashmap bucketing overhead.
    if aggregate.modifier.is_none() {
        if samples.iter().any(|sample| sample.drop_name) {
            for sample in &mut samples {
                if sample.drop_name {
                    sample.labels.remove(METRIC_NAME);
                    sample.drop_name = false;
                }
            }
        }
        return Ok(select_k_from_group(samples, k, order));
    }

    // Histogram samples are not yet represented in EvalSample.
    // This path is isolated so histogram handling can be added later
    // without changing k-aggregation grouping/selection flow.
    let grouped = group_samples_for_k_aggregation(samples, aggregate.modifier.as_ref());

    // Each group is fully independent so parallelize
    let collected: Vec<EvalSample> = grouped
        .into_values()
        .iter_into_par()
        .flat_map(|group_samples| select_k_from_group(group_samples, k, order))
        .collect();

    Ok(collected)
}

fn select_k_indices_with_heap(
    samples: &[EvalSample],
    keep: usize,
    order: KAggregationOrder,
) -> Vec<usize> {
    if keep == 0 || samples.is_empty() {
        return Vec::new();
    }
    let mut heap = BinaryHeap::with_capacity(keep);
    for (idx, sample) in samples.iter().enumerate() {
        let entry = KHeapEntry {
            value: sample.value,
            index: idx,
            order,
        };
        if heap.len() < keep {
            heap.push(entry);
            continue;
        }

        if let Some(worst) = heap.peek()
            && compare_k_values(sample.value, worst.value, order).is_lt()
        {
            // Replace only when the candidate outranks the current worst,
            // preserving the "peek is worst-kept" invariant.
            heap.pop();
            heap.push(entry);
        }
    }
    heap.into_iter().map(|entry| entry.index).collect()
}

// Group key construction clones label strings. This is acceptable since
// topk's dominant cost is sorting/selection. If profiling shows this is hot,
// consider using series fingerprints as group keys instead.
pub(crate) fn group_samples_for_k_aggregation(
    samples: Vec<EvalSample>,
    modifier: Option<&LabelModifier>,
) -> AHashMap<Labels, Vec<EvalSample>> {
    let mut groups: AHashMap<Labels, Vec<EvalSample>> = AHashMap::new();
    for mut sample in samples {
        // Materialize pending metric-name drops before grouping so __name__
        // doesn't incorrectly affect bucket assignment.
        if sample.drop_name {
            sample.labels.remove(METRIC_NAME);
            sample.drop_name = false;
        }

        let group_key = match modifier {
            None => Vec::new(),
            Some(LabelModifier::Include(label_list)) => sample
                .labels
                .iter()
                .filter(|label| label_list.labels.contains(&label.name))
                .cloned()
                .collect(),
            Some(LabelModifier::Exclude(label_list)) => sample
                .labels
                .iter()
                .filter(|label| !label_list.labels.contains(&label.name))
                .cloned()
                .collect(),
        };
        let key = Labels::new(group_key);
        groups.entry(key).or_default().push(sample);
    }
    groups
}

fn select_k_from_group(
    mut samples: Vec<EvalSample>,
    k: usize,
    order: KAggregationOrder,
) -> Vec<EvalSample> {
    let keep = k.min(samples.len());
    let mut selected_indices = select_k_indices_with_heap(&samples, keep, order);
    // Remove from highest index first so swap_remove cannot invalidate
    // indices we still need to read.
    selected_indices.sort_unstable_by(|left, right| right.cmp(left));

    let mut result = Vec::with_capacity(selected_indices.len());
    for idx in selected_indices {
        result.push(samples.swap_remove(idx));
    }
    result.sort_by(|left, right| compare_k_values(left.value, right.value, order));
    result
}
