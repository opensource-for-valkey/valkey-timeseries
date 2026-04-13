use crate::promql::QueryValue;
use crate::promql::model::RangeSample;
use promql_parser::parser::value::ValueType;

/// Compare actual results against expected results
///
/// IMPORTANT: Metric name handling follows Prometheus promqltest semantics:
/// - Prometheus represents the metric name as the __name__ label
/// - If expected sample omits __name__ → we don't check it (allows flexible matching)
/// - If expected sample includes __name__ → we verify it matches
///
/// This means test expectations can be written as:
///   {job="test"} 42          # Matches any metric with job="test"
///   {__name__="metric"} 42   # Must be exactly "metric"
///
/// The implementation achieves this by only checking labels that are present in the
/// expected sample, not all labels from the actual result.
pub(super) fn assert_results(
    results: QueryValue,
    expected: QueryValue,
    expect_ordered: bool,
    test_name: &str,
    eval_num: usize,
    query: &str,
) -> Result<(), String> {
    let result_type = results.value_type();
    let expected_type = expected.value_type();
    let (results, expected) = match (&results, &expected) {
        (QueryValue::Scalar { value: res_val, .. }, QueryValue::Scalar { value: exp_val, .. }) => {
            return compare_scalar_results(test_name, query, eval_num, *res_val, *exp_val);
        }
        (QueryValue::String(res_val), QueryValue::String(exp_val)) => {
            if res_val != exp_val {
                return Err(format!(
                    "{test_name} eval #{eval_num} (query: {query}): String mismatch: expected '{exp_val}', got '{res_val}'",
                ));
            }
            return Ok(());
        }
        _ => {
            const VECTOR_OR_MATRIX: [ValueType; 2] = [ValueType::Vector, ValueType::Matrix];
            if VECTOR_OR_MATRIX.contains(&result_type) && VECTOR_OR_MATRIX.contains(&expected_type)
            {
                let result = results.into_matrix().map_err(|e| e.to_string())?;
                let expected = expected.into_matrix().map_err(|e| e.to_string())?;
                (result, expected)
            } else {
                let err_msg = format!(
                    "{test_name} eval #{eval_num} (query: {query}): Type mismatch: expected {expected_type:?}, got {result_type:?}",
                );
                return Err(err_msg);
            }
        }
    };

    if results.len() != expected.len() {
        return Err(format!(
            "{test_name} eval #{eval_num} (query: {}): Expected {} samples, got {}",
            query,
            expected.len(),
            results.len()
        ));
    }

    // Most instant vectors are unordered in PromQL, but promqltest supports
    // `expect ordered` for order-sensitive checks (e.g. topk/bottomk).
    let mut results_sorted = results.to_vec();
    let mut expected_sorted = expected.to_vec();
    if !expect_ordered {
        results_sorted.sort_by(|a, b| a.labels.cmp(&b.labels));
        expected_sorted.sort_by(|a, b| a.labels.cmp(&b.labels));
    }

    for (i, exp) in expected_sorted.iter().enumerate() {
        let result = &results_sorted[i];

        // Check all expected labels are present and match
        for label in exp.labels.iter() {
            let actual = result.labels.get(&label.name).ok_or(format!(
                "{test_name} eval #{eval_num} (query: {query}): Missing label '{}'",
                label.name
            ))?;
            if actual != label.value {
                return Err(format!(
                    "{test_name} eval #{eval_num} (query: {query}): Label {} mismatch: expected '{}', got '{actual}'",
                    label.name, label.value,
                ));
            }
        }

        let exp_value = exp.samples[0].value;
        let result_value = result.samples[0].value;
        compare_scalar_results(test_name, query, eval_num, result_value, exp_value)?;
    }

    Ok(())
}

fn compare_scalar_results(
    test_name: &str,
    query: &str,
    eval_num: usize,
    result: f64,
    expected: f64,
) -> Result<(), String> {
    if (result - expected).abs() > 1e-6 {
        return Err(format!(
            "{test_name} eval #{eval_num} (query: {query}): Value mismatch: expected {expected}, got {result}",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Sample;
    use crate::labels::Label;
    use crate::promql::model::Labels;

    fn labels_from(pairs: &[(&str, &str)]) -> Labels {
        let mut labels: Vec<Label> = pairs.iter().map(|(k, v)| Label::new(*k, *v)).collect();
        labels.sort();
        Labels::new(labels)
    }

    fn range_sample(labels: Labels, value: f64) -> RangeSample {
        RangeSample {
            labels,
            samples: vec![Sample::new(0, value)],
        }
    }

    #[test]
    fn should_match_expected_results() {
        // given
        let results = QueryValue::Matrix(vec![range_sample(labels_from(&[("job", "test")]), 42.0)]);
        let expected =
            QueryValue::Matrix(vec![range_sample(labels_from(&[("job", "test")]), 42.0)]);

        // when
        let result = assert_results(results, expected, false, "test", 1, "metric");

        // then
        assert!(result.is_ok());
    }

    #[test]
    fn should_reject_count_mismatch() {
        // given
        let results = QueryValue::Matrix(vec![range_sample(Labels::empty(), 42.0)]);
        let expected = QueryValue::Matrix(vec![]);

        // when
        let result = assert_results(results, expected, false, "test", 1, "metric");

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Expected 0 samples, got 1"));
    }

    #[test]
    fn should_reject_mismatched_values() {
        // given
        let results = QueryValue::Matrix(vec![range_sample(Labels::empty(), 42.0)]);
        let expected = QueryValue::Matrix(vec![range_sample(Labels::empty(), 99.0)]);

        // when
        let result = assert_results(results, expected, false, "test", 1, "metric");

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Value mismatch"));
    }

    #[test]
    fn should_reject_order_mismatch_when_ordered() {
        // given
        let results = QueryValue::Matrix(vec![
            range_sample(labels_from(&[("instance", "b")]), 2.0),
            range_sample(labels_from(&[("instance", "a")]), 1.0),
        ]);
        let expected = QueryValue::Matrix(vec![
            range_sample(labels_from(&[("instance", "a")]), 1.0),
            range_sample(labels_from(&[("instance", "b")]), 2.0),
        ]);

        // when
        let result = assert_results(results, expected, true, "test", 1, "metric");

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Label instance mismatch"));
    }

    #[test]
    fn should_allow_order_mismatch_when_not_ordered() {
        // given
        let results = QueryValue::Matrix(vec![
            range_sample(labels_from(&[("instance", "b")]), 2.0),
            range_sample(labels_from(&[("instance", "a")]), 1.0),
        ]);
        let expected = QueryValue::Matrix(vec![
            range_sample(labels_from(&[("instance", "a")]), 1.0),
            range_sample(labels_from(&[("instance", "b")]), 2.0),
        ]);

        // when
        let result = assert_results(results, expected, false, "test", 1, "metric");

        // then
        assert!(result.is_ok());
    }
}
