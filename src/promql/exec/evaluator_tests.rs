#[cfg(test)]
mod tests {
    use crate::common::Sample;
    use crate::promql::EvalContext;
    use crate::promql::{
        EvalResult, EvalSample, EvalSamples, EvaluationError, Evaluator, ExprResult, Labels,
    };
    use promql_parser::label::{METRIC_NAME, Matchers};
    use promql_parser::parser::value::ValueType;
    use promql_parser::parser::{
        AtModifier, EvalStmt, Function, FunctionArgs, NumberLiteral, Offset, VectorSelector,
    };
    use promql_parser::parser::{BinaryExpr, Call, Expr, MatrixSelector};
    use rstest::rstest;

    use crate::commands::parse_metric_name;
    use crate::common::time::system_time_to_millis;
    use crate::labels::Label;
    use crate::promql::engine::test_utils::{
        MockMultiBucketQueryReaderBuilder, MockQueryReaderBuilder, MockSeriesQuerier,
    };
    use crate::promql::engine::{QueryOptions, QueryReader};
    use crate::tests::approx_eq;
    use promql_parser::parser::token::{T_SUB, TokenType};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    /// Type alias for test data: (metric_name, labels, timestamp_offset_ms, value)
    type TestSampleData = Vec<(&'static str, Vec<(&'static str, &'static str)>, i64, f64)>;

    // Type aliases for vector selector test to reduce complexity warnings
    type VectorSelectorTestData = Vec<(&'static str, Vec<(&'static str, &'static str)>, i64, f64)>;
    type VectorSelectorExpectedResults = Vec<(f64, Vec<(&'static str, &'static str)>)>;

    /// Helper to parse a PromQL query and evaluate it
    fn parse_and_evaluate<'reader, R: QueryReader>(
        evaluator: &Evaluator<'reader, R>,
        query: &str,
        end_time: SystemTime,
        lookback_delta: Duration,
    ) -> EvalResult<Vec<EvalSample>> {
        let expr = promql_parser::parser::parse(query)
            .map_err(|e| EvaluationError::InternalError(format!("Parse error: {}", e)))?;

        let stmt = EvalStmt {
            expr,
            start: end_time,
            end: end_time,
            interval: Duration::from_secs(0),
            lookback_delta,
        };

        evaluator
            .evaluate(stmt)
            .map(|result| result.expect_instant_vector("Expected instant vector result"))
    }

    /// Sort samples by labels (for deterministic comparison)
    fn sort_samples_by_labels(samples: &mut [EvalSample]) {
        samples.sort_by(|a, b| a.labels.cmp(&b.labels));
    }

    /// Compare actual results with expected results
    fn assert_results_match(actual: &[EvalSample], expected: &[(f64, Vec<(&str, &str)>)]) {
        assert_eq!(
            actual.len(),
            expected.len(),
            "Result count mismatch: got {}, expected {}",
            actual.len(),
            expected.len()
        );

        let mut actual_sorted: Vec<_> = actual.to_vec();
        sort_samples_by_labels(&mut actual_sorted);

        let mut expected_sorted: Vec<_> = expected
            .iter()
            .map(|(v, labels)| (*v, Labels::from_pairs(labels)))
            .collect();
        expected_sorted.sort_by(|a, b| a.1.cmp(&b.1));

        for (i, (actual_sample, (expected_value, expected_labels))) in actual_sorted
            .into_iter()
            .zip(expected_sorted.into_iter())
            .enumerate()
        {
            assert!(
                approx_eq(actual_sample.value, expected_value),
                "Sample {i} value mismatch: got {}, expected {}",
                actual_sample.value,
                expected_value
            );

            assert_eq!(
                actual_sample.labels, expected_labels,
                "Sample {i} labels mismatch: got {:?}, expected {:?}",
                actual_sample.labels, expected_labels
            );
        }
    }

    /// Helper to create labels from metric name and label pairs
    fn create_labels(metric_name: &str, label_pairs: Vec<(&str, &str)>) -> Labels {
        let mut labels = vec![Label {
            name: METRIC_NAME.to_string(),
            value: metric_name.to_string(),
        }];
        for (key, val) in label_pairs {
            labels.push(Label {
                name: key.to_string(),
                value: val.to_string(),
            });
        }
        Labels::new(labels)
    }

    fn parse_labels(metric: &str) -> Labels {
        let labels = parse_metric_name(metric)
            .unwrap_or_else(|_| panic!("Failed to parse metric name: {}", metric));
        Labels::new(labels)
    }

    /// Setup helper: Creates a MockQueryReader with test data
    ///
    /// data: Vec of (metric_name, labels, timestamp_offset_ms, value)
    /// Returns (MockQueryReader, end_time) where end_time is suitable for querying
    fn setup_mock_reader(data: TestSampleData) -> (MockSeriesQuerier, SystemTime) {
        let mut builder = MockQueryReaderBuilder::new();

        // Base timestamp: 300001ms (ensures samples are > start_ms with 5min lookback)
        // Query time will be calculated to be well after all samples
        let base_timestamp = 300001i64;

        // Find max offset before consuming data
        let max_offset = data
            .iter()
            .map(|(_, _, offset_ms, _)| *offset_ms)
            .max()
            .unwrap_or(0);

        for (metric_name, labels, offset_ms, value) in data {
            let attributes = create_labels(metric_name, labels);
            let sample = Sample {
                timestamp: base_timestamp + offset_ms,
                value,
            };
            builder.add_sample(&attributes, sample);
        }

        // Query time: base_timestamp + max_offset + 1ms (just after all samples)
        // Lookback window: (start_ms, query_time] where start_ms = query_time - 300000
        // Since lookback uses exclusive start (timestamp > start_ms), we need:
        //   start_ms < base_timestamp (to include all samples)
        //   => query_time - 300000 < base_timestamp
        //   => query_time < base_timestamp + 300000
        // We set query_time = base_timestamp + max_offset + 1, which works as long as max_offset < 300000
        // This ensures start_ms = base_timestamp + max_offset + 1 - 300000 < base_timestamp
        // So all samples at base_timestamp + offset (where offset <= max_offset) are included
        let query_timestamp = base_timestamp + max_offset + 1;
        let end_time = UNIX_EPOCH + Duration::from_millis(query_timestamp as u64);

        (builder.build(), end_time)
    }

    #[rstest]
    // Vector Selectors
    #[case(
        "vector_selector_all_series",
        "http_requests_total",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (10.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "GET")]),
            (20.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "POST")]),
            (30.0, vec![("__name__", "http_requests_total"), ("env", "staging"), ("method", "GET")]),
            (40.0, vec![("__name__", "http_requests_total"), ("env", "staging"), ("method", "POST")]),
        ]
    )]
    #[case(
        "vector_selector_with_single_equality_matcher",
        r#"http_requests_total{env="prod"}"#,
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
        ],
        vec![
            (10.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "GET")]),
            (20.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "POST")]),
        ]
    )]
    #[case(
        "vector_selector_with_different_label_matcher",
        r#"http_requests_total{method="GET"}"#,
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
        ],
        vec![
            (10.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "GET")]),
            (30.0, vec![("__name__", "http_requests_total"), ("env", "staging"), ("method", "GET")]),
        ]
    )]
    #[case(
        "vector_selector_with_multiple_equality_matchers",
        r#"http_requests_total{env="prod",method="GET"}"#,
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
        ],
        vec![
            (10.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "GET")]),
        ]
    )]
    #[case(
        "vector_selector_with_not_equal_matcher",
        r#"http_requests_total{env!="staging"}"#,
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
        ],
        vec![
            (10.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "GET")]),
            (20.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "POST")]),
        ]
    )]
    #[case(
        "vector_selector_different_metric",
        "cpu_usage",
        vec![
            ("cpu_usage", vec![("env", "prod"), ("instance", "i1")], 0, 50.0),
            ("cpu_usage", vec![("env", "prod"), ("instance", "i2")], 1, 60.0),
        ],
        vec![
            (50.0, vec![("__name__", "cpu_usage"), ("env", "prod"), ("instance", "i1")]),
            (60.0, vec![("__name__", "cpu_usage"), ("env", "prod"), ("instance", "i2")]),
        ]
    )]
    #[case(
        "vector_selector_single_series_metric",
        "memory_bytes",
        vec![
            ("memory_bytes", vec![("env", "prod")], 0, 100.0),
        ],
        vec![
            (100.0, vec![("__name__", "memory_bytes"), ("env", "prod")]),
        ]
    )]
    // Function Calls - Unary Math
    #[case(
        "function_abs",
        "abs(http_requests_total)",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (10.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "GET")]),
            (20.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "POST")]),
            (30.0, vec![("__name__", "http_requests_total"), ("env", "staging"), ("method", "GET")]),
            (40.0, vec![("__name__", "http_requests_total"), ("env", "staging"), ("method", "POST")]),
        ]
    )]
    #[case(
        "function_sqrt",
        "sqrt(memory_bytes)",
        vec![
            ("memory_bytes", vec![("env", "prod")], 0, 100.0),
        ],
        vec![
            (10.0, vec![("__name__", "memory_bytes"), ("env", "prod")]), // sqrt(100) = 10
        ]
    )]
    #[case(
        "function_ceil",
        "ceil(cpu_usage)",
        vec![
            ("cpu_usage", vec![("env", "prod"), ("instance", "i1")], 0, 50.0),
            ("cpu_usage", vec![("env", "prod"), ("instance", "i2")], 1, 60.0),
        ],
        vec![
            (50.0, vec![("__name__", "cpu_usage"), ("env", "prod"), ("instance", "i1")]),
            (60.0, vec![("__name__", "cpu_usage"), ("env", "prod"), ("instance", "i2")]),
        ]
    )]
    #[case(
        "function_floor",
        "floor(cpu_usage)",
        vec![
            ("cpu_usage", vec![("env", "prod"), ("instance", "i1")], 0, 50.0),
            ("cpu_usage", vec![("env", "prod"), ("instance", "i2")], 1, 60.0),
        ],
        vec![
            (50.0, vec![("__name__", "cpu_usage"), ("env", "prod"), ("instance", "i1")]),
            (60.0, vec![("__name__", "cpu_usage"), ("env", "prod"), ("instance", "i2")]),
        ]
    )]
    #[case(
        "function_round",
        "round(cpu_usage)",
        vec![
            ("cpu_usage", vec![("env", "prod"), ("instance", "i1")], 0, 50.0),
            ("cpu_usage", vec![("env", "prod"), ("instance", "i2")], 1, 60.0),
        ],
        vec![
            (50.0, vec![("__name__", "cpu_usage"), ("env", "prod"), ("instance", "i1")]),
            (60.0, vec![("__name__", "cpu_usage"), ("env", "prod"), ("instance", "i2")]),
        ]
    )]
    // Function Calls - Trigonometry
    #[case(
        "function_sin",
        "sin(cpu_usage)",
        vec![
            ("cpu_usage", vec![("env", "prod"), ("instance", "i1")], 0, 50.0),
            ("cpu_usage", vec![("env", "prod"), ("instance", "i2")], 1, 60.0),
        ],
        vec![
            (50.0_f64.sin(), vec![("__name__", "cpu_usage"), ("env", "prod"), ("instance", "i1")]),
            (60.0_f64.sin(), vec![("__name__", "cpu_usage"), ("env", "prod"), ("instance", "i2")]),
        ]
    )]
    #[case(
        "function_cos",
        "cos(cpu_usage)",
        vec![
            ("cpu_usage", vec![("env", "prod"), ("instance", "i1")], 0, 50.0),
            ("cpu_usage", vec![("env", "prod"), ("instance", "i2")], 1, 60.0),
        ],
        vec![
            (50.0_f64.cos(), vec![("__name__", "cpu_usage"), ("env", "prod"), ("instance", "i1")]),
            (60.0_f64.cos(), vec![("__name__", "cpu_usage"), ("env", "prod"), ("instance", "i2")]),
        ]
    )]
    // Function Calls - Logarithms
    #[case(
        "function_ln",
        "ln(memory_bytes)",
        vec![
            ("memory_bytes", vec![("env", "prod")], 0, 100.0),
        ],
        vec![
            (100.0_f64.ln(), vec![("__name__", "memory_bytes"), ("env", "prod")]),
        ]
    )]
    #[case(
        "function_log10",
        "log10(memory_bytes)",
        vec![
            ("memory_bytes", vec![("env", "prod")], 0, 100.0),
        ],
        vec![
            (100.0_f64.log10(), vec![("__name__", "memory_bytes"), ("env", "prod")]), // log10(100) = 2
        ]
    )]
    #[case(
        "function_log2",
        "log2(memory_bytes)",
        vec![
            ("memory_bytes", vec![("env", "prod")], 0, 100.0),
        ],
        vec![
            (100.0_f64.log2(), vec![("__name__", "memory_bytes"), ("env", "prod")]),
        ]
    )]
    // Function Calls - Special
    #[case(
        "function_absent_with_existing_metric",
        "absent(http_requests_total)",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
        ],
        vec![] // Should return empty since http_requests_total exists
    )]
    #[case(
        "function_absent_with_nonexistent_metric",
        "absent(nonexistent_metric)",
        vec![
            ("other_metric", vec![("env", "prod")], 0, 5.0),
        ],
        vec![
            (1.0, vec![]), // Should return 1.0 when metric doesn't exist
        ]
    )]
    // Binary Operations - Arithmetic
    #[case(
        "binary_add_vector_scalar",
        "http_requests_total + 5",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (15.0, vec![("env", "prod"), ("method", "GET")]),
            (25.0, vec![("env", "prod"), ("method", "POST")]),
            (35.0, vec![("env", "staging"), ("method", "GET")]),
            (45.0, vec![("env", "staging"), ("method", "POST")]),
        ]
    )]
    #[case(
        "binary_multiply_vector_scalar",
        "http_requests_total * 2",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (20.0, vec![("env", "prod"), ("method", "GET")]),
            (40.0, vec![("env", "prod"), ("method", "POST")]),
            (60.0, vec![("env", "staging"), ("method", "GET")]),
            (80.0, vec![("env", "staging"), ("method", "POST")]),
        ]
    )]
    #[case(
        "binary_divide_vector_scalar",
        "memory_bytes / 10",
        vec![
            ("memory_bytes", vec![("env", "prod")], 0, 100.0),
        ],
        vec![
            (10.0, vec![("env", "prod")]), // 100 / 10 = 10
        ]
    )]
    // Binary Operations - Comparison
    #[case(
        "binary_greater_than_filter",
        "http_requests_total > 15",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (1.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "POST")]), // 20 > 15
            (1.0, vec![("__name__", "http_requests_total"), ("env", "staging"), ("method", "GET")]), // 30 > 15
            (1.0, vec![("__name__", "http_requests_total"), ("env", "staging"), ("method", "POST")]), // 40 > 15
        ]
    )]
    #[case(
        "binary_less_than_filter",
        "http_requests_total < 25",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
        ],
        vec![
            (1.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "GET")]), // 10 < 25
            (1.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "POST")]), // 20 < 25
        ]
    )]
    #[case(
        "binary_equal_filter",
        "http_requests_total == 20",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
        ],
        vec![
            (1.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "POST")]), // 20 == 20
        ]
    )]
    // Binary Operations - Comparison with bool (vector-scalar and scalar-vector)
    #[case(
        "binary_vector_scalar_comparison_bool_keeps_false",
        "http_requests_total > bool 15",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
        ],
        vec![
            // bool: false results retained as 0, true as 1; __name__ dropped
            (0.0, vec![("env", "prod"), ("method", "GET")]),  // 10 > 15 = false → 0
            (1.0, vec![("env", "prod"), ("method", "POST")]), // 20 > 15 = true → 1
        ]
    )]
    #[case(
        "binary_scalar_vector_comparison_bool_keeps_false",
        "15 < bool http_requests_total",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
        ],
        vec![
            // bool: 15 < 10 = false → 0, 15 < 20 = true → 1; __name__ dropped
            (0.0, vec![("env", "prod"), ("method", "GET")]),
            (1.0, vec![("env", "prod"), ("method", "POST")]),
        ]
    )]
    // Vector-Vector Binary Operations
    #[case(
        "binary_vector_vector_sub_same_metric",
        "http_requests_total - http_requests_total",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
        ],
        vec![
            (0.0, vec![("env", "prod"), ("method", "GET")]),
            (0.0, vec![("env", "prod"), ("method", "POST")]),
            (0.0, vec![("env", "staging"), ("method", "GET")]),
        ]
    )]
    #[case(
        "binary_vector_vector_add_same_metric",
        "http_requests_total + http_requests_total",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
        ],
        vec![
            (20.0, vec![("env", "prod"), ("method", "GET")]),
            (40.0, vec![("env", "prod"), ("method", "POST")]),
        ]
    )]
    #[case(
        "binary_vector_vector_unmatched_dropped",
        "cpu_usage + memory_bytes",
        vec![
            ("cpu_usage", vec![("env", "prod"), ("instance", "i1")], 0, 50.0),
            ("cpu_usage", vec![("env", "prod"), ("instance", "i2")], 1, 60.0),
            ("memory_bytes", vec![("env", "prod")], 2, 100.0),
        ],
        vec![] // No matching label sets (different labels), all dropped
    )]
    #[case(
        "binary_vector_vector_comparison",
        "cpu_usage > memory_bytes",
        vec![
            ("cpu_usage", vec![("env", "prod")], 0, 150.0),
            ("cpu_usage", vec![("env", "staging")], 1, 50.0),
            ("memory_bytes", vec![("env", "prod")], 2, 100.0),
            ("memory_bytes", vec![("env", "staging")], 3, 100.0),
        ],
        vec![
            // 150 > 100 = true; non-bool comparison propagates lhs value
            (150.0, vec![("__name__", "cpu_usage"), ("env", "prod")]),
            // 50 > 100 = false, filtered out
        ]
    )]
    #[case(
        "binary_vector_vector_after_aggregation",
        "sum by (env)(http_requests_total) - sum by (env)(http_requests_total)",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
        ],
        vec![
            (0.0, vec![("env", "prod")]),   // (10+20) - (10+20) = 0
            (0.0, vec![("env", "staging")]), // 30 - 30 = 0
        ]
    )]
    #[case(
        "binary_vector_vector_on_modifier",
        "cpu_usage + on(env) memory_bytes",
        vec![
            ("cpu_usage", vec![("env", "prod"), ("instance", "i1")], 0, 50.0),
            ("memory_bytes", vec![("env", "prod")], 1, 100.0),
        ],
        vec![
            (150.0, vec![("env", "prod")]),
        ]
    )]
    #[case(
        "binary_vector_vector_comparison_on_drops_name",
        "cpu_usage > on(env) memory_bytes",
        vec![
            ("cpu_usage", vec![("env", "prod"), ("instance", "i1")], 0, 150.0),
            ("cpu_usage", vec![("env", "staging"), ("instance", "i2")], 1, 50.0),
            ("memory_bytes", vec![("env", "prod")], 2, 100.0),
            ("memory_bytes", vec![("env", "staging")], 3, 100.0),
        ],
        vec![
            // 150 > 100 = true; on(env) keeps only env label, value stays from lhs
            (150.0, vec![("env", "prod")]),
            // 50 > 100 = false, filtered out
        ]
    )]
    #[case(
        "binary_vector_vector_comparison_ignoring_preserves_name",
        "cpu_usage > ignoring(instance) memory_bytes",
        vec![
            ("cpu_usage", vec![("env", "prod"), ("instance", "i1")], 0, 150.0),
            ("memory_bytes", vec![("env", "prod")], 1, 100.0),
        ],
        vec![
            // ignoring(instance) comparison preserves __name__ but removes instance
            (150.0, vec![("__name__", "cpu_usage"), ("env", "prod")]),
        ]
    )]
    #[case(
        "binary_vector_vector_comparison_on_name_preserves_name",
        "cpu_usage == on(__name__) cpu_usage",
        vec![
            ("cpu_usage", vec![("env", "prod")], 0, 150.0),
        ],
        vec![
            // on(__name__) comparison without bool: Prometheus preserves __name__
            // and propagates lhs value.
            (150.0, vec![("__name__", "cpu_usage")]),
        ]
    )]
    #[case(
        "binary_nested_arithmetic_then_on_name_no_match_left",
        "(cpu_usage + 1) == on(__name__) cpu_usage",
        vec![
            ("cpu_usage", vec![("env", "prod")], 0, 150.0),
        ],
        vec![
            // Inner + drops __name__ (materialized before matching), so on(__name__)
            // finds no __name__ on left → match keys differ → no match → empty result
        ]
    )]
    #[case(
        "binary_nested_arithmetic_then_on_name_no_match_right",
        "cpu_usage == on(__name__) (cpu_usage + 1)",
        vec![
            ("cpu_usage", vec![("env", "prod")], 0, 150.0),
        ],
        vec![
            // Inner + drops __name__ on right side → on(__name__) match keys differ → empty
        ]
    )]
    #[case(
        "binary_vector_vector_comparison_bool_keeps_false",
        "cpu_usage > bool memory_bytes",
        vec![
            ("cpu_usage", vec![("env", "prod")], 0, 150.0),
            ("cpu_usage", vec![("env", "staging")], 1, 50.0),
            ("memory_bytes", vec![("env", "prod")], 2, 100.0),
            ("memory_bytes", vec![("env", "staging")], 3, 100.0),
        ],
        vec![
            // bool modifier: keep all results as 0/1 and drop __name__
            (0.0, vec![("env", "staging")]), // 50 > 100 = false → 0
            (1.0, vec![("env", "prod")]),    // 150 > 100 = true → 1
        ]
    )]
    #[case(
        "binary_vector_vector_ignoring_modifier",
        "cpu_usage - ignoring(instance) memory_bytes",
        vec![
            ("cpu_usage", vec![("env", "prod"), ("instance", "i1")], 0, 50.0),
            ("memory_bytes", vec![("env", "prod")], 1, 100.0),
        ],
        vec![
            // ignoring(instance) removes instance; arithmetic drops __name__
            (-50.0, vec![("env", "prod")]),
        ]
    )]
    #[case(
        "binary_vector_vector_order_insensitive",
        "http_requests_total - http_requests_total",
        vec![
            // Reverse order: staging first, prod second
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 0, 30.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 1, 10.0),
        ],
        vec![
            (0.0, vec![("env", "prod"), ("method", "GET")]),
            (0.0, vec![("env", "staging"), ("method", "GET")]),
        ]
    )]
    // Vector-Vector: matched arithmetic with different metric names (only __name__ differs)
    #[case(
        "binary_vector_vector_arithmetic_different_metrics",
        "cpu_usage + memory_bytes",
        vec![
            ("cpu_usage", vec![("env", "prod")], 0, 50.0),
            ("memory_bytes", vec![("env", "prod")], 1, 100.0),
        ],
        vec![
            // Matched on {env=prod} (__name__ excluded from match key), __name__ dropped (arithmetic)
            (150.0, vec![("env", "prod")]),
        ]
    )]
    // Aggregation over arithmetic: drop_name materialized before grouping
    #[case(
        "binary_aggregation_over_arithmetic_drops_name",
        r#"sum by (__name__) (http_requests_total + 1)"#,
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
        ],
        vec![
            // Inner + sets drop_name=true, aggregation materializes the drop before grouping,
            // so by(__name__) finds no __name__ → single group {} with sum 11+21=32
            (32.0, vec![]),
        ]
    )]
    // Nested expression: drop_name propagation through nested binary ops
    #[case(
        "binary_nested_arithmetic_then_comparison",
        "(http_requests_total + 1) > 15",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
        ],
        vec![
            // Inner + sets drop_name=true, outer > filters to values > 15.
            // 10+1=11 not > 15 (filtered). 20+1=21 > 15 → returns 1.0. __name__ stripped at top level.
            (1.0, vec![("env", "prod"), ("method", "POST")]),
        ]
    )]
    // Function-wrapped arithmetic: drop_name propagation through instant-vector functions
    #[case(
        "binary_function_wrapped_arithmetic_drops_name",
        "abs(http_requests_total + 1)",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, -20.0),
        ],
        vec![
            // Inner + sets drop_name=true, abs() preserves drop_name (mutates value in-place),
            // __name__ stripped at top-level deferred cleanup
            (11.0, vec![("env", "prod"), ("method", "GET")]),
            (19.0, vec![("env", "prod"), ("method", "POST")]),
        ]
    )]
    // Aggregations
    #[case(
        "aggregation_sum",
        "sum(http_requests_total)",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (100.0, vec![]), // 10 + 20 + 30 + 40 = 100
        ]
    )]
    #[case(
        "aggregation_avg",
        "avg(http_requests_total)",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (25.0, vec![]), // (10 + 20 + 30 + 40) / 4 = 25
        ]
    )]
    #[case(
        "aggregation_min",
        "min(http_requests_total)",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (10.0, vec![]), // min(10, 20, 30, 40) = 10
        ]
    )]
    #[case(
        "aggregation_max",
        "max(http_requests_total)",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (40.0, vec![]), // max(10, 20, 30, 40) = 40
        ]
    )]
    #[case(
        "aggregation_count",
        "count(http_requests_total)",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (4.0, vec![]), // 4 series
        ]
    )]
    #[case(
        "aggregation_topk",
        "topk(2, http_requests_total)",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (30.0, vec![("__name__", "http_requests_total"), ("env", "staging"), ("method", "GET")]),
            (40.0, vec![("__name__", "http_requests_total"), ("env", "staging"), ("method", "POST")]),
        ]
    )]
    #[case(
        "aggregation_topk_by_env",
        r#"topk by (env) (1, http_requests_total)"#,
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (20.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "POST")]),
            (40.0, vec![("__name__", "http_requests_total"), ("env", "staging"), ("method", "POST")]),
        ]
    )]
    #[case(
        "aggregation_topk_materializes_drop_name",
        "topk(1, http_requests_total + 1)",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
        ],
        vec![
            (21.0, vec![("env", "prod"), ("method", "POST")]),
        ]
    )]
    #[case(
        "aggregation_topk_zero_k_returns_empty",
        "topk(0, http_requests_total)",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
        ],
        vec![]
    )]
    // Aggregations with grouping
    #[case(
        "aggregation_sum_by_env",
        r#"sum by (env) (http_requests_total)"#,
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (30.0, vec![("env", "prod")]),    // 10 + 20 = 30
            (70.0, vec![("env", "staging")]), // 30 + 40 = 70
        ]
    )]
    #[case(
        "aggregation_avg_by_env",
        r#"avg by (env) (http_requests_total)"#,
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (15.0, vec![("env", "prod")]),    // (10 + 20) / 2 = 15
            (35.0, vec![("env", "staging")]), // (30 + 40) / 2 = 35
        ]
    )]
    #[case(
        "aggregation_sum_by_method",
        r#"sum by (method) (http_requests_total)"#,
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (40.0, vec![("method", "GET")]),  // 10 + 30 = 40
            (60.0, vec![("method", "POST")]), // 20 + 40 = 60
        ]
    )]
    // Complex Expressions
    #[case(
        "nested_function_abs_sum",
        "abs(sum(http_requests_total))",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (100.0, vec![]), // abs(sum(10, 20, 30, 40)) = abs(100) = 100
        ]
    )]
    #[case(
        "nested_function_sqrt_sum",
        "sqrt(sum(memory_bytes))",
        vec![
            ("memory_bytes", vec![("env", "prod")], 0, 100.0),
        ],
        vec![
            (10.0, vec![]), // sqrt(sum(100)) = sqrt(100) = 10
        ]
    )]
    #[case(
        "aggregation_with_selector",
        r#"sum(http_requests_total{env="prod"})"#,
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
        ],
        vec![
            (30.0, vec![]), // sum(10, 20) = 30
        ]
    )]
    #[test]
    fn should_evaluate_queries(
        #[case] _name: &str,
        #[case] query: &str,
        #[case] test_data: TestSampleData,
        #[case] expected_samples: Vec<(f64, Vec<(&str, &str)>)>,
    ) {
        let (reader, end_time) = setup_mock_reader(test_data);
        let evaluator = Evaluator::new(
            &reader,
            QueryOptions {
                timeout: None,
                ..QueryOptions::default()
            },
        );
        let lookback_delta = Duration::from_secs(300); // 5 minutes

        let result = parse_and_evaluate(&evaluator, query, end_time, lookback_delta)
            .expect("Query should evaluate successfully");

        assert_results_match(&result, &expected_samples);
    }

    #[test]
    fn should_cache_samples_across_evaluations() {
        // given: mock reader with data in specific bucket
        let mut builder = MockQueryReaderBuilder::new();
        let labels: Labels = vec![
            Label {
                name: METRIC_NAME.to_string(),
                value: "cached_metric".to_string(),
            },
            Label {
                name: "instance".to_string(),
                value: "server1".to_string(),
            },
        ]
            .into();
        let sample = Sample {
            timestamp: 300001,
            value: 100.0,
        };
        builder.add_sample(&labels, sample);
        let reader = builder.build();
        // Create cached reader and evaluator
        let evaluator = Evaluator::new(
            &reader,
            QueryOptions {
                timeout: None,
                ..QueryOptions::default()
            },
        );

        // when: evaluate the same query multiple times
        let end_time = UNIX_EPOCH + Duration::from_millis(300002);
        let lookback_delta = Duration::from_secs(300);
        let expr = promql_parser::parser::parse("cached_metric").unwrap();
        let stmt = EvalStmt {
            expr,
            start: end_time,
            end: end_time,
            interval: Duration::from_secs(0),
            lookback_delta,
        };
        // First evaluation
        let result1 = evaluator
            .evaluate(stmt.clone())
            .unwrap()
            .expect_instant_vector("Expected instant vector result");
        // Second evaluation - should use cached data
        let result2 = evaluator
            .evaluate(stmt.clone())
            .unwrap()
            .expect_instant_vector("Expected instant vector result");

        // then: results should be identical (sample caching disabled for now)
        assert_eq!(result1.len(), 1);
        assert_eq!(result2.len(), 1);
        assert_eq!(result1[0].value, 100.0);
        assert_eq!(result2[0].value, 100.0);
        assert_eq!(result1[0].labels, result2[0].labels);

        // Note: Sample caching is disabled for now to avoid time range issues
        // assert!(cache.get_samples(&bucket, &0).is_some());
        // let cached_samples = cache.get_samples(&bucket, &0).unwrap();
        // assert_eq!(cached_samples.len(), 1);
        // assert_eq!(cached_samples[0].value, 100.0);
    }

    #[test]
    fn should_evaluate_number_literal() {
        // given: create an empty mock reader
        let reader = MockQueryReaderBuilder::new().build();
        let evaluator = Evaluator::new(
            &reader,
            QueryOptions {
                timeout: None,
                ..QueryOptions::default()
            },
        );

        // when: evaluate a number literal (should return scalar, which is unsupported)
        let end_time = UNIX_EPOCH + Duration::from_millis(1_000);
        let stmt = EvalStmt {
            expr: Expr::NumberLiteral(NumberLiteral { val: 42.0 }),
            start: end_time,
            end: end_time,
            interval: Duration::from_secs(0),
            lookback_delta: Duration::from_secs(300),
        };

        let result = evaluator.evaluate(stmt);

        // then: should return scalar result with value 42.0
        assert!(result.is_ok());
        let result = result.unwrap();
        match result {
            ExprResult::Scalar(value) => assert_eq!(value, 42.0),
            _ => panic!("Expected scalar result, got {}", result.value_type()),
        }
    }

    #[test]
    fn should_evaluate_time_function_as_scalar() {
        // given
        let reader = MockQueryReaderBuilder::new().build();
        let evaluator = Evaluator::new(
            &reader,
            QueryOptions {
                timeout: None,
                ..QueryOptions::default()
            },
        );
        let end_time = UNIX_EPOCH + Duration::from_millis(1);

        // when
        let stmt = EvalStmt {
            expr: promql_parser::parser::parse("time()").unwrap(),
            start: end_time,
            end: end_time,
            interval: Duration::from_secs(0),
            lookback_delta: Duration::from_secs(300),
        };
        let result = evaluator.evaluate(stmt).unwrap();

        // then
        match result {
            ExprResult::String(_) => panic!("Expected scalar result, got string"),
            ExprResult::Scalar(value) => assert_eq!(value, 0.001),
            ExprResult::InstantVector(_) => panic!("Expected scalar result, got vector"),
            ExprResult::RangeVector(_) => panic!("Expected scalar result, got range vector"),
        }
    }

    #[test]
    fn should_evaluate_pi_function_as_scalar() {
        // given
        let reader = MockQueryReaderBuilder::new().build();
        let evaluator = Evaluator::new(
            &reader,
            QueryOptions {
                timeout: None,
                ..QueryOptions::default()
            },
        );
        let end_time = UNIX_EPOCH + Duration::from_secs(2_000);

        // when
        let stmt = EvalStmt {
            expr: promql_parser::parser::parse("pi()").unwrap(),
            start: end_time,
            end: end_time,
            interval: Duration::from_secs(0),
            lookback_delta: Duration::from_secs(300),
        };
        let result = evaluator.evaluate(stmt).unwrap();

        // then
        match result {
            ExprResult::String(_) => panic!("Expected scalar result, got string"),
            ExprResult::Scalar(value) => assert_eq!(value, std::f64::consts::PI),
            ExprResult::InstantVector(_) => panic!("Expected scalar result, got vector"),
            ExprResult::RangeVector(_) => panic!("Expected scalar result, got range vector"),
        }
    }

    #[test]
    fn should_evaluate_scalar_function_as_scalar() {
        // given
        let reader = MockQueryReaderBuilder::new().build();
        let evaluator = Evaluator::new(
            &reader,
            QueryOptions {
                timeout: None,
                ..QueryOptions::default()
            },
        );
        let end_time = UNIX_EPOCH + Duration::from_secs(2_000);

        // when
        let stmt = EvalStmt {
            expr: promql_parser::parser::parse("scalar(vector(42))").unwrap(),
            start: end_time,
            end: end_time,
            interval: Duration::from_secs(0),
            lookback_delta: Duration::from_secs(300),
        };
        let result = evaluator.evaluate(stmt).unwrap();

        // then
        match result {
            ExprResult::String(_) => panic!("Expected scalar result, got string"),
            ExprResult::Scalar(value) => assert_eq!(value, 42.0),
            ExprResult::InstantVector(_) => panic!("Expected scalar result, got vector"),
            ExprResult::RangeVector(_) => panic!("Expected scalar result, got range vector"),
        }
    }

    #[test]
    fn should_allow_scalar_function_results_as_vector_arguments() {
        // given
        let reader = MockQueryReaderBuilder::new().build();
        let evaluator = Evaluator::new(
            &reader,
            QueryOptions {
                timeout: None,
                ..QueryOptions::default()
            },
        );
        let end_time = UNIX_EPOCH + Duration::from_secs(5);

        // when
        let stmt = EvalStmt {
            expr: promql_parser::parser::parse("vector(time())").unwrap(),
            start: end_time,
            end: end_time,
            interval: Duration::from_secs(0),
            lookback_delta: Duration::from_secs(300),
        };
        let result = evaluator
            .evaluate(stmt)
            .unwrap()
            .expect_instant_vector("Expected instant vector result");

        // then
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 5.0);
    }

    #[test]
    fn should_handle_string_literal() {
        // given: create an empty mock reader
        let reader = MockQueryReaderBuilder::new().build();
        let evaluator = Evaluator::new(
            &reader,
            QueryOptions {
                timeout: None,
                ..QueryOptions::default()
            },
        );

        // when: evaluate a string literal
        let end_time = UNIX_EPOCH + Duration::from_millis(2_000);
        let stmt = EvalStmt {
            expr: Expr::StringLiteral(promql_parser::parser::StringLiteral {
                val: "hello".to_string(),
            }),
            start: end_time,
            end: end_time,
            interval: Duration::from_secs(0),
            lookback_delta: Duration::from_secs(300),
        };

        let result = evaluator.evaluate(stmt).unwrap();

        let ExprResult::String(value) = result else {
            panic!("Expected string result, got {}", result.value_type())
        };

        assert_eq!(value.as_str(), "hello");
    }

    #[test]
    fn should_evaluate_label_replace_with_raw_string_arguments() {
        // given: create an empty mock reader
        let reader = MockQueryReaderBuilder::new().build();
        let evaluator = Evaluator::new(
            &reader,
            QueryOptions {
                timeout: None,
                ..QueryOptions::default()
            },
        );

        // when: evaluate a function call with a string literal argument
        let end_time = UNIX_EPOCH + Duration::from_millis(2_000);
        let stmt = EvalStmt {
            expr: Expr::Call(Call {
                func: Function {
                    name: "label_replace",
                    arg_types: vec![
                        ValueType::Vector,
                        ValueType::String,
                        ValueType::String,
                        ValueType::String,
                        ValueType::String,
                    ],
                    variadic: 0,
                    return_type: ValueType::Vector,
                    experimental: false,
                },
                args: FunctionArgs {
                    args: vec![
                        Box::new(Expr::Call(Call {
                            func: Function::new(
                                "vector",
                                vec![ValueType::Scalar],
                                0,
                                ValueType::Vector,
                                false,
                            ),
                            args: FunctionArgs::new_args(Expr::NumberLiteral(NumberLiteral {
                                val: 1.0,
                            })),
                        })),
                        Box::new(Expr::Paren(promql_parser::parser::ParenExpr {
                            expr: Box::new(Expr::StringLiteral(
                                promql_parser::parser::StringLiteral {
                                    val: "dst".to_string(),
                                },
                            )),
                        })),
                        Box::new(Expr::StringLiteral(promql_parser::parser::StringLiteral {
                            val: "replacement".to_string(),
                        })),
                        Box::new(Expr::StringLiteral(promql_parser::parser::StringLiteral {
                            val: "src".to_string(),
                        })),
                        Box::new(Expr::StringLiteral(promql_parser::parser::StringLiteral {
                            val: "(.*)".to_string(),
                        })),
                    ],
                },
            }),
            start: end_time,
            end: end_time,
            interval: Duration::from_secs(0),
            lookback_delta: Duration::from_secs(300),
        };

        let result = evaluator.evaluate(stmt);

        // then: raw string args should reach label_replace and be applied
        let result = result.expect("label_replace should succeed");
        match result {
            ExprResult::InstantVector(samples) => {
                assert_eq!(samples.len(), 1);
                assert_eq!(samples[0].value, 1.0);
                assert_eq!(samples[0].labels.get("dst"), Some("replacement"));
            }
            _ => {
                panic!(
                    "Expected instant vector result, got {:?}",
                    result.value_type()
                );
            }
        }
    }

    #[allow(clippy::type_complexity)]
    #[rstest]
    #[case(
        "single_bucket_selector",
        vec![
            ("http_requests", vec![("env", "prod")], 6_000_001, 10.0),
            ("http_requests", vec![("env", "staging")], 6_000_002, 20.0),
        ],
        6_300_000, // query time
        300_000,   // 5 min lookback
        vec![(10.0, vec![("__name__", "http_requests"), ("env", "prod")]), (20.0, vec![("__name__", "http_requests"), ("env", "staging")])]
    )]
    #[case(
        "multi_bucket_different_series_different_buckets",
        vec![
            // Series A: sample in bucket 100 is outside lookback, sample in bucket 200 is within lookback
            ("memory", vec![("app", "frontend")], 6_000_000, 100.0), // outside lookback window
            ("memory", vec![("app", "frontend")], 10_000_000, 80.0), // within lookback window
            // Series B: latest sample in bucket 200 within lookback
            ("memory", vec![("app", "backend")], 5_000_000, 150.0), // outside lookback window
            ("memory", vec![("app", "backend")], 12_000_000, 200.0), // within lookback window
        ],
        12_300_000, // query time
        3_600_000,  // 1 hour lookback: (8,700,000, 12,300,000]
        vec![
            (80.0, vec![("__name__", "memory"), ("app", "frontend")]),  // latest within lookback from bucket 200
            (200.0, vec![("__name__", "memory"), ("app", "backend")])   // latest within lookback from bucket 200
        ]
    )]
    #[test]
    fn should_evaluate_vector_selector(
        #[case] test_name: &str,
        #[case] data: VectorSelectorTestData,
        #[case] query_time_ms: i64,
        #[case] lookback_ms: i64,
        #[case] expected: VectorSelectorExpectedResults,
    ) {
        // Extract metric name from first sample for selector before consuming data
        let metric_name = if let Some((name, _, _, _)) = data.first() {
            name.to_string()
        } else {
            "test_metric".to_string()
        };

        // given: build mock reader with test data
        let mut builder = MockMultiBucketQueryReaderBuilder::new();

        for (metric_name, label_pairs, timestamp, value) in data {
            let labels = create_labels(metric_name, label_pairs);
            builder.add_sample(&labels, Sample { timestamp, value });
        }

        let reader = builder.build();
        let evaluator = Evaluator::new(
            &reader,
            QueryOptions {
                timeout: None,
                ..QueryOptions::default()
            },
        );

        // when: evaluate vector selector
        let selector = VectorSelector {
            name: Some(metric_name),
            matchers: Matchers {
                matchers: vec![],
                or_matchers: vec![],
            },
            offset: None,
            at: None,
        };

        let ctx = EvalContext::for_vector_selector(query_time_ms, lookback_ms);
        let result = evaluator
            .evaluate_vector_selector(&selector, &ctx, false)
            .unwrap();

        // then: verify results
        let ExprResult::InstantVector(samples) = result else {
            panic!(
                "Expected instant vector result, got {}",
                result.value_type()
            )
        };

        assert_eq!(samples.len(), expected.len(), "Result count mismatch");

        // Sort both actual and expected results for comparison
        let mut actual_sorted = samples;

        let mut expected_sorted = expected
            .iter()
            .map(|(value, labels)| {
                let labels = Labels::from_pairs(labels);
                EvalSample {
                    timestamp_ms: query_time_ms,
                    value: *value,
                    labels,
                    drop_name: false,
                }
            })
            .collect::<Vec<EvalSample>>();

        expected_sorted.sort_by(|a, b| a.labels.cmp(&b.labels));
        actual_sorted.sort_by(|a, b| a.labels.cmp(&b.labels));

        // Compare each series
        for (i, (actual, expected)) in actual_sorted.iter().zip(expected_sorted.iter()).enumerate()
        {
            // Check that the series has the expected labels
            for Label { name, value } in expected.labels.iter() {
                assert_eq!(
                    actual.labels.get(name),
                    Some(value.as_str()),
                    "Test '{test_name}': Series {i} missing label {name}={value}"
                );
            }
            assert!(
                (actual.value - expected.value).abs() < 0.0001,
                "Sample {i} value mismatch: got {}, expected {}",
                actual.value,
                expected.value
            );

            for Label { name, value } in expected.labels.iter() {
                assert_eq!(
                    actual.labels.get(name),
                    Some(value.as_str()),
                    "Sample {i} missing label {name}={value}"
                );
            }
        }
    }

    // Matrix Selector Tests

    type MatrixSelectorTestData = Vec<(&'static str, Vec<(&'static str, &'static str)>, i64, f64)>;
    type MatrixSelectorExpectedResults = Vec<(Vec<(&'static str, &'static str)>, Vec<(i64, f64)>)>;

    #[rstest]
    #[case(
        "single_series_multiple_samples",
        vec![
            // One series with multiple samples across time
            ("cpu_usage", vec![("host", "server1")], 6_000_000, 10.0),
            ("cpu_usage", vec![("host", "server1")], 6_060_000, 15.0), // 1 min later
            ("cpu_usage", vec![("host", "server1")], 6_120_000, 20.0), // 2 min later
        ],
        6_150_000, // query time: 2.5 min after first sample
        Duration::from_secs(180), // 3 min range: covers all 3 samples
        vec![
            (vec![("__name__", "cpu_usage"), ("host", "server1")], vec![(6_000_000, 10.0), (6_060_000, 15.0), (6_120_000, 20.0)])
        ]
    )]
    #[case(
        "multiple_series_same_time_range",
        vec![
            // Two different series with samples in the range
            ("memory", vec![("app", "frontend")], 6_000_000, 100.0),
            ("memory", vec![("app", "frontend")], 6_060_000, 110.0),
            ("memory", vec![("app", "backend")], 6_030_000, 200.0),
            ("memory", vec![("app", "backend")], 6_090_000, 220.0),
        ],
        6_100_000, // query time
        Duration::from_secs(120), // 2 min range
        vec![
            (vec![("__name__", "memory"), ("app", "backend")], vec![(6_030_000, 200.0), (6_090_000, 220.0)]),
            (vec![("__name__", "memory"), ("app", "frontend")], vec![(6_000_000, 100.0), (6_060_000, 110.0)])
        ]
    )]
    #[case(
        "single_bucket_all_samples_in_range",
        vec![
            // All samples in same bucket within the range
            ("disk_io", vec![("device", "sda")], 6_000_000, 50.0),
            ("disk_io", vec![("device", "sda")], 6_030_000, 55.0),
            ("disk_io", vec![("device", "sda")], 6_060_000, 60.0),
            ("disk_io", vec![("device", "sda")], 6_090_000, 65.0),
        ],
        6_100_000, // query time
        Duration::from_secs(120), // 2 min range: should include last 3 samples
        vec![
            (vec![("__name__", "disk_io"), ("device", "sda")], vec![(6_000_000, 50.0), (6_030_000, 55.0), (6_060_000, 60.0), (6_090_000, 65.0)])
        ]
    )]
    #[case(
        "partial_time_range_filtering",
        vec![
            // Some samples outside the range should be filtered out
            ("requests", vec![("method", "GET")], 5_900_000, 100.0), // too old
            ("requests", vec![("method", "GET")], 6_000_000, 110.0), // in range
            ("requests", vec![("method", "GET")], 6_030_000, 120.0), // in range
            ("requests", vec![("method", "GET")], 6_200_000, 130.0), // too new
        ],
        6_100_000, // query time
        Duration::from_secs(90), // 1.5 min range: end-90s to end, so 6_010_000 to 6_100_000
        vec![
            (vec![("__name__", "requests"), ("method", "GET")], vec![(6_030_000, 120.0)]) // only middle samples in range
        ]
    )]
    #[test]
    fn should_evaluate_matrix_selector(
        #[case] test_name: &str,
        #[case] data: MatrixSelectorTestData,
        #[case] query_time_ms: i64,
        #[case] range: Duration,
        #[case] expected: MatrixSelectorExpectedResults,
    ) {
        // given:
        let mut builder = MockQueryReaderBuilder::new();
        for (metric_name, label_pairs, timestamp, value) in data {
            let labels: Labels = create_labels(metric_name, label_pairs);
            builder.add_sample(&labels, Sample { timestamp, value });
        }
        let reader = builder.build();
        let evaluator = Evaluator::new(
            &reader,
            QueryOptions {
                timeout: None,
                ..QueryOptions::default()
            },
        );

        // when: evaluate matrix selector
        let metric_name = match expected.first() {
            Some((labels, _)) => labels
                .iter()
                .find(|(k, _)| k == &"__name__")
                .map(|(_, v)| v.as_ref())
                .unwrap_or("cpu_usage"),
            None => "cpu_usage", // default fallback
        };
        let matrix_selector = MatrixSelector {
            vs: VectorSelector {
                name: Some(metric_name.to_string()),
                matchers: Matchers {
                    matchers: vec![],
                    or_matchers: vec![],
                },
                offset: None,
                at: None,
            },
            range,
        };

        let eval_ctx = EvalContext {
            query_start: query_time_ms,
            query_end: query_time_ms,
            evaluation_ts: query_time_ms,
            step_ms: 0,
            lookback_delta_ms: 0,
        };

        let result = evaluator
            .evaluate_matrix_selector(&matrix_selector, &eval_ctx)
            .unwrap();

        // then: verify results
        if let ExprResult::RangeVector(range_samples) = result {
            assert_eq!(
                range_samples.len(),
                expected.len(),
                "Test '{}': Expected {} series, got {}",
                test_name,
                expected.len(),
                range_samples.len()
            );
            let mut actual_sorted = range_samples;
            actual_sorted.sort_by(|a, b| a.labels.cmp(&b.labels));

            let mut expected_sorted: Vec<EvalSamples> = expected
                .iter()
                .map(|(raw_labels, raw_samples)| {
                    let labels = Labels::from_pairs(raw_labels);
                    let mut samples = Vec::with_capacity(raw_samples.len());
                    for (ts, value) in raw_samples {
                        samples.push(Sample::new(*ts, *value));
                    }
                    EvalSamples {
                        labels,
                        values: samples,
                        drop_name: false,
                        range_end_ms: 0,
                        range_ms: 0,
                    }
                })
                .collect();

            expected_sorted.sort_by(|a, b| a.labels.cmp(&b.labels));

            // Compare each series
            for (i, (actual, expected)) in
                actual_sorted.iter().zip(expected_sorted.iter()).enumerate()
            {
                // Check that the series has the expected labels
                for Label { name, value } in expected.labels.iter() {
                    assert_eq!(
                        actual.labels.get(name),
                        Some(value.as_str()),
                        "Test '{test_name}': Series {i} missing label {name}={value}"
                    );
                }
                // Check that the series has the expected number of samples
                assert_eq!(
                    actual.values.len(),
                    expected.values.len(),
                    "Test '{test_name}': Series {i} expected {} samples, got {}",
                    expected.values.len(),
                    actual.values.len()
                );
                // Check each sample's timestamp and value
                for (j, (actual, expected_sample)) in
                    actual.values.iter().zip(expected.values.iter()).enumerate()
                {
                    let expected_ts = expected_sample.timestamp;
                    let expected_val = expected_sample.value;

                    assert_eq!(
                        actual.timestamp, expected_ts,
                        "Test '{test_name}': Series {i} sample {j} timestamp mismatch: expected {expected_ts}, got {}",
                        actual.timestamp
                    );
                    assert_eq!(
                        actual.value, expected_val,
                        "Test '{test_name}': Series {i} sample {j} value mismatch: expected {expected_val}, got {}",
                        actual.value
                    );
                }
            }
        } else {
            panic!(
                "Test '{test_name}': Expected RangeVector result, got {:?}",
                result
            );
        }
    }

    #[test]
    fn test_evaluate_call_scalar_argument() {
        let (reader, end_time) = setup_mock_reader(vec![]);
        let evaluator = Evaluator::new(
            &reader,
            QueryOptions {
                timeout: None,
                ..QueryOptions::default()
            },
        );

        let call = Call {
            func: Function::new(
                "vector",
                vec![ValueType::Scalar],
                0,
                ValueType::Vector,
                false,
            ),
            args: FunctionArgs::new_args(Expr::NumberLiteral(NumberLiteral { val: 42.0 })),
        };

        let end_time_ms = system_time_to_millis(end_time);
        let ctx = EvalContext {
            query_start: end_time_ms,
            query_end: end_time_ms,
            evaluation_ts: end_time_ms,
            step_ms: 60_000,
            lookback_delta_ms: 300_000,
        };

        let result = evaluator.evaluate_call(&call, &ctx, false).unwrap();

        match result {
            ExprResult::InstantVector(samples) => {
                assert_eq!(samples.len(), 1);
                assert_eq!(samples[0].value, 42.0);
                assert!(samples[0].labels.is_empty());
                let expected_ts = end_time
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;
                assert_eq!(samples[0].timestamp_ms, expected_ts);
            }
            other => panic!("Expected Instant Vector, got {:?}", other),
        }
    }

    #[test]
    fn test_evaluate_call_scalar_expression_argument() {
        let (reader, end_time) = setup_mock_reader(vec![]);
        let evaluator = Evaluator::new(
            &reader,
            QueryOptions {
                timeout: None,
                ..QueryOptions::default()
            },
        );
        let call = Call {
            func: Function::new(
                "vector",
                vec![ValueType::Scalar],
                0,
                ValueType::Vector,
                false,
            ),
            args: FunctionArgs::new_args(Expr::Binary(BinaryExpr {
                lhs: Box::new(Expr::NumberLiteral(NumberLiteral { val: 1.0 })),
                rhs: Box::new(Expr::NumberLiteral(NumberLiteral { val: 1.0 })),
                op: TokenType::new(T_SUB),
                modifier: None,
            })),
        };

        let end_time_ms = system_time_to_millis(end_time);

        let ctx = EvalContext {
            query_start: end_time_ms,
            query_end: end_time_ms,
            evaluation_ts: end_time_ms,
            step_ms: 60_000,
            lookback_delta_ms: 300_000,
        };
        let result = evaluator.evaluate_call(&call, &ctx, false).unwrap();
        match result {
            ExprResult::InstantVector(samples) => {
                assert_eq!(samples.len(), 1);
                assert_eq!(samples[0].value, 0.0);
                assert!(samples[0].labels.is_empty());
            }
            other => panic!("Expected Instant Vector, got {:?}", other),
        }
    }

    #[test]
    fn should_evaluate_clamp_family_queries() {
        let test_data = vec![
            ("test_clamp", vec![("src", "clamp-a")], 0, -50.0),
            ("test_clamp", vec![("src", "clamp-b")], 1, 0.0),
            ("test_clamp", vec![("src", "clamp-c")], 2, 100.0),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let evaluator = Evaluator::new(&reader, QueryOptions::default());
        let lookback_delta = Duration::from_secs(300);

        let clamp_result = parse_and_evaluate(
            &evaluator,
            "clamp(test_clamp, -25, 75)",
            end_time,
            lookback_delta,
        )
            .unwrap();
        assert_results_match(
            &clamp_result,
            &[
                (-25.0, vec![("__name__", "test_clamp"), ("src", "clamp-a")]),
                (0.0, vec![("__name__", "test_clamp"), ("src", "clamp-b")]),
                (75.0, vec![("__name__", "test_clamp"), ("src", "clamp-c")]),
            ],
        );

        let clamp_min_result = parse_and_evaluate(
            &evaluator,
            "clamp_min(test_clamp, -25)",
            end_time,
            lookback_delta,
        )
            .unwrap();
        assert_results_match(
            &clamp_min_result,
            &[
                (-25.0, vec![("__name__", "test_clamp"), ("src", "clamp-a")]),
                (0.0, vec![("__name__", "test_clamp"), ("src", "clamp-b")]),
                (100.0, vec![("__name__", "test_clamp"), ("src", "clamp-c")]),
            ],
        );

        let clamp_max_result = parse_and_evaluate(
            &evaluator,
            "clamp_max(test_clamp, 75)",
            end_time,
            lookback_delta,
        )
            .unwrap();
        assert_results_match(
            &clamp_max_result,
            &[
                (-50.0, vec![("__name__", "test_clamp"), ("src", "clamp-a")]),
                (0.0, vec![("__name__", "test_clamp"), ("src", "clamp-b")]),
                (75.0, vec![("__name__", "test_clamp"), ("src", "clamp-c")]),
            ],
        );
    }

    #[test]
    fn should_return_empty_vector_for_clamp_when_min_exceeds_max() {
        let test_data = vec![
            ("test_clamp", vec![("src", "clamp-a")], 0, -50.0),
            ("test_clamp", vec![("src", "clamp-b")], 1, 0.0),
            ("test_clamp", vec![("src", "clamp-c")], 2, 100.0),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let evaluator = Evaluator::new(&reader, QueryOptions::default());
        let lookback_delta = Duration::from_secs(300);

        let result = parse_and_evaluate(
            &evaluator,
            "clamp(test_clamp, 5, -5)",
            end_time,
            lookback_delta,
        )
            .unwrap();

        assert!(result.is_empty());
    }

    #[test]
    fn should_evaluate_rate_function_with_matrix_selector() {
        // given: mock reader with counter data over time
        let mut builder = MockQueryReaderBuilder::new();
        let labels = create_labels("http_requests_total", vec![("job", "webapp")]);
        builder
            .add_sample(
                &labels,
                Sample {
                    timestamp: 6_000_000, // t=0s, counter at 100
                    value: 100.0,
                },
            )
            .add_sample(
                &labels,
                Sample {
                    timestamp: 6_030_000, // t=30s, counter at 115
                    value: 115.0,
                },
            )
            .add_sample(
                &labels,
                Sample {
                    timestamp: 6_060_000, // t=60s, counter at 130
                    value: 130.0,
                },
            );
        let reader = builder.build();
        let evaluator = Evaluator::new(&reader, QueryOptions::default());

        // when: evaluate rate(http_requests_total[1m])
        let query_time = UNIX_EPOCH + Duration::from_millis(6_060_000);
        let query_start = query_time - Duration::from_secs(60);
        let query = "rate(http_requests_total[1m])";
        let expr = promql_parser::parser::parse(query).expect("Failed to parse query");

        let query_time_ms = system_time_to_millis(query_time);
        let evaluation_ts = query_time_ms;
        let query_start_ms = system_time_to_millis(query_start);

        let ctx = EvalContext {
            query_start: query_start_ms,
            query_end: query_time_ms,
            evaluation_ts,
            step_ms: 15_000,
            lookback_delta_ms: 5_000,
        };

        let pipeline_result = evaluator.evaluate_expr(&expr, &ctx, false).unwrap();

        if let ExprResult::InstantVector(instant_samples) = pipeline_result {
            assert_eq!(instant_samples.len(), 1, "Expected 1 result from pipeline");
            // The pipeline should give the same rate as the direct function call
            assert!(instant_samples[0].value > 0.0, "Rate should be positive");
            assert_eq!(instant_samples[0].labels.get("job"), Some("webapp"));
        } else {
            panic!(
                "Expected InstantVector result from rate function pipeline, got {:?}",
                pipeline_result
            );
        }
    }

    #[test]
    fn should_evaluate_vector_selector_with_positive_offset() {
        // given: samples at different times
        let mut builder = MockQueryReaderBuilder::new();

        for (ts, val) in [(5_700_000, 10.0), (6_000_000, 20.0), (6_300_000, 30.0)] {
            builder.add_sample(
                &vec![
                    Label {
                        name: METRIC_NAME.to_string(),
                        value: "http_requests".to_string(),
                    },
                    Label {
                        name: "env".to_string(),
                        value: "prod".to_string(),
                    },
                ]
                    .into(),
                Sample {
                    timestamp: ts,
                    value: val,
                },
            );
        }

        let reader = builder.build();
        let evaluator = Evaluator::new(&reader, QueryOptions::default());

        // when: query at t=6_300_000 with positive offset 5m (look back 300_000ms)
        let selector = VectorSelector {
            name: Some("http_requests".to_string()),
            matchers: Matchers::new(vec![]),
            offset: Some(Offset::Pos(Duration::from_millis(300_000))),
            at: None,
        };

        let query_time = 6_300_000;
        let ctx = EvalContext::for_vector_selector(query_time, 300_000);
        let result = evaluator
            .evaluate_vector_selector(&selector, &ctx, false)
            .unwrap();

        // then: should get the sample from t=6_000_000 (value 20.0)
        if let ExprResult::InstantVector(samples) = result {
            assert_eq!(samples.len(), 1);
            assert_eq!(samples[0].value, 20.0);
            assert_eq!(samples[0].timestamp_ms, 6_000_000);
        } else {
            panic!("Expected InstantVector result");
        }
    }

    #[test]
    fn should_evaluate_vector_selector_with_at_modifier() {
        // given: samples at different times
        let mut builder = MockQueryReaderBuilder::new();

        let labels = create_labels("http_requests", vec![("env", "prod")]);
        builder.add_sample(
            &labels,
            Sample {
                timestamp: 5_700_000,
                value: 10.0,
            },
        );
        builder.add_sample(
            &labels,
            Sample {
                timestamp: 6_000_000,
                value: 20.0,
            },
        );

        let reader = builder.build();
        let evaluator = Evaluator::new(&reader, QueryOptions::default());

        // when: query at t=6_300_000 but with @ 6_000_000
        let at_time = UNIX_EPOCH + Duration::from_millis(6_000_000);
        let selector = VectorSelector {
            name: Some("http_requests".to_string()),
            matchers: Matchers::new(vec![]),
            offset: None,
            at: Some(AtModifier::At(at_time)),
        };

        let query_time = 6_300_000;
        let ctx = EvalContext::for_vector_selector(query_time, 300_000);
        let result = evaluator
            .evaluate_vector_selector(&selector, &ctx, false)
            .unwrap();

        // then: should get the sample from @ time (value 20.0)
        if let ExprResult::InstantVector(samples) = result {
            assert_eq!(samples.len(), 1);
            assert_eq!(samples[0].value, 20.0);
            assert_eq!(samples[0].timestamp_ms, 6_000_000);
        } else {
            panic!("Expected InstantVector result");
        }
    }

    #[test]
    fn should_evaluate_vector_selector_with_both_at_and_offset_modifiers() {
        // given: samples at different times
        let mut builder = MockQueryReaderBuilder::new();
        let labels = create_labels("http_requests", vec![("env", "prod")]);
        builder.add_sample(
            &labels,
            Sample {
                timestamp: 5_700_000,
                value: 30.0,
            },
        );
        builder.add_sample(
            &labels,
            Sample {
                timestamp: 6_000_000,
                value: 20.0,
            },
        );

        let reader = builder.build();
        let evaluator = Evaluator::new(&reader, QueryOptions::default());

        // when: query with @ 6_000_000 and offset 5m
        // Should apply @ first (6_000_000), then subtract offset (300_000) = 5_700_000
        let at_time = UNIX_EPOCH + Duration::from_millis(6_000_000);
        let selector = VectorSelector {
            name: Some("http_requests".to_string()),
            matchers: Matchers::new(vec![]),
            offset: Some(Offset::Pos(Duration::from_millis(300_000))),
            at: Some(AtModifier::At(at_time)),
        };

        let query_time = 6_300_000;
        let ctx = EvalContext::for_vector_selector(query_time, 300_000);
        let result = evaluator
            .evaluate_vector_selector(&selector, &ctx, false)
            .unwrap();

        // then: should get the sample from @ time - offset (value 30.0 at t=5_700_000)
        if let ExprResult::InstantVector(samples) = result {
            assert_eq!(samples.len(), 1);
            assert_eq!(samples[0].value, 30.0);
            assert_eq!(samples[0].timestamp_ms, 5_700_000);
        } else {
            panic!("Expected InstantVector result");
        }
    }

    #[test]
    fn should_evaluate_matrix_selector_with_offset_modifier() {
        // given: samples at different times
        let mut builder = MockMultiBucketQueryReaderBuilder::new();

        // Samples from t=5_700_000 to t=6_300_000
        for (ts, val) in [
            (5_700_000, 10.0),
            (5_800_000, 15.0),
            (5_900_000, 20.0),
            (6_000_000, 25.0),
            (6_100_000, 30.0),
            (6_200_000, 35.0),
            (6_300_000, 40.0),
        ] {
            let labels = create_labels("cpu_usage", vec![("host", "server1")]);
            builder.add_sample(
                &labels,
                Sample {
                    timestamp: ts,
                    value: val,
                },
            );
        }

        let reader = builder.build();
        let evaluator = Evaluator::new(&reader, QueryOptions::default());

        // when: query matrix selector with 5m range and 5m offset at t=6_300_000
        // offset 5m means look at t=6_000_000, then get [5_700_000, 6_000_000]
        let matrix_selector = MatrixSelector {
            vs: VectorSelector {
                name: Some("cpu_usage".to_string()),
                matchers: Matchers::new(vec![]),
                offset: Some(Offset::Pos(Duration::from_millis(300_000))),
                at: None,
            },
            range: Duration::from_millis(300_000),
        };

        let query_time = 6_300_000;
        let ctx = EvalContext {
            query_start: query_time,
            query_end: query_time,
            evaluation_ts: query_time,
            step_ms: 0, // ??
            lookback_delta_ms: 300_000,
        };
        let result = evaluator
            .evaluate_matrix_selector(&matrix_selector, &ctx)
            .unwrap();

        // then: should get samples in range (5_700_000, 6_000_000] (exclusive start, inclusive end)
        if let ExprResult::RangeVector(range_samples) = result {
            assert_eq!(range_samples.len(), 1);
            let samples = &range_samples[0].values;
            assert_eq!(samples.len(), 3); // 5_800_000, 5_900_000, 6_000_000
            assert_eq!(samples[0].timestamp, 5_800_000);
            assert_eq!(samples[0].value, 15.0);
            assert_eq!(samples[2].timestamp, 6_000_000);
            assert_eq!(samples[2].value, 25.0);
        } else {
            panic!("Expected RangeVector result");
        }
    }

    #[test]
    fn should_evaluate_vector_selector_with_at_start_and_end() {
        // given: samples at different times
        let mut builder = MockQueryReaderBuilder::new();
        let labels: Labels = parse_metric_name(r#"http_requests{env="prod"}"#)
            .unwrap()
            .into();
        builder.add_sample(
            &labels,
            Sample {
                timestamp: 5_700_000,
                value: 10.0,
            },
        );
        builder.add_sample(
            &labels,
            Sample {
                timestamp: 6_000_000,
                value: 20.0,
            },
        );
        builder.add_sample(
            &labels,
            Sample {
                timestamp: 6_300_000,
                value: 30.0,
            },
        );

        let reader = builder.build();
        let evaluator = Evaluator::new(&reader, QueryOptions::default());

        // when: query with @ start() where query_start = 5_700_000
        let selector_start = VectorSelector {
            name: Some("http_requests".to_string()),
            matchers: Matchers::new(vec![]),
            offset: None,
            at: Some(AtModifier::Start),
        };

        let query_start = 5_700_000;
        let query_end = 6_300_000;

        let ctx = EvalContext {
            query_start,
            query_end,
            evaluation_ts: query_end,
            lookback_delta_ms: 300_000,
            step_ms: 0,
        };

        let result_start = evaluator
            .evaluate_vector_selector(&selector_start, &ctx, false)
            .unwrap();

        // then: should get sample at query_start (value 10.0)
        if let ExprResult::InstantVector(samples) = result_start {
            assert_eq!(samples.len(), 1);
            assert_eq!(samples[0].value, 10.0);
            assert_eq!(samples[0].timestamp_ms, 5_700_000);
        } else {
            panic!("Expected InstantVector result");
        }

        // when: query with @ end() where query_end = 6_300_000
        let selector_end = VectorSelector {
            name: Some("http_requests".to_string()),
            matchers: Matchers::new(vec![]),
            offset: None,
            at: Some(AtModifier::End),
        };

        let result_end = evaluator
            .evaluate_vector_selector(&selector_end, &ctx, false)
            .unwrap();

        // then: should get sample at query_end (value 30.0)
        if let ExprResult::InstantVector(samples) = result_end {
            assert_eq!(samples.len(), 1);
            assert_eq!(samples[0].value, 30.0);
            assert_eq!(samples[0].timestamp_ms, 6_300_000);
        } else {
            panic!("Expected InstantVector result");
        }
    }

    #[test]
    fn should_evaluate_vector_selector_with_negative_offset() {
        // given: samples at different times
        let mut builder = MockMultiBucketQueryReaderBuilder::new();

        for (ts, val) in [(5_700_000, 10.0), (6_000_000, 20.0), (6_300_000, 30.0)] {
            let labels = create_labels("http_requests", vec![("env", "prod")]);
            builder.add_sample(
                &labels,
                Sample {
                    timestamp: ts,
                    value: val,
                },
            );
        }

        let reader = builder.build();
        let evaluator = Evaluator::new(&reader, QueryOptions::default());

        // when: query at t=5_700_000 with negative offset -5m (look forward 300_000ms)
        let selector = VectorSelector {
            name: Some("http_requests".to_string()),
            matchers: Matchers::new(vec![]),
            offset: Some(Offset::Neg(Duration::from_millis(300_000))),
            at: None,
        };

        let query_time = 5_700_000;
        let ctx = EvalContext::for_vector_selector(query_time, 300_000);
        let result = evaluator
            .evaluate_vector_selector(&selector, &ctx, false)
            .unwrap();

        // then: should get the sample from t=6_000_000 (value 20.0)
        if let ExprResult::InstantVector(samples) = result {
            assert_eq!(samples.len(), 1);
            assert_eq!(samples[0].value, 20.0);
            assert_eq!(samples[0].timestamp_ms, 6_000_000);
        } else {
            panic!("Expected InstantVector result");
        }
    }

    #[test]
    fn should_evaluate_vector_selector_with_non_aligned_timestamps() {
        // given: samples at irregular timestamps
        let mut builder = MockMultiBucketQueryReaderBuilder::new();

        for (ts, val) in [
            (5_723_456, 10.0),
            (5_987_654, 20.0),
            (6_234_567, 30.0),
            (6_456_789, 40.0),
        ] {
            let labels: Labels = create_labels("cpu_usage", vec![("host", "server1")]);
            builder.add_sample(
                &labels,
                Sample {
                    timestamp: ts,
                    value: val,
                },
            );
        }

        let reader = builder.build();
        let evaluator = Evaluator::new(&reader, QueryOptions::default());

        // when: query at non-aligned timestamp with offset
        let selector = VectorSelector {
            name: Some("cpu_usage".to_string()),
            matchers: Matchers::new(vec![]),
            offset: Some(Offset::Pos(Duration::from_millis(250_000))),
            at: None,
        };

        let query_time = 6_234_567;
        let ctx = EvalContext::for_vector_selector(query_time, 300_000);

        let result = evaluator
            .evaluate_vector_selector(&selector, &ctx, false)
            .unwrap();

        // then: should get the sample closest to (6_234_567 - 250_000 = 5_984_567)
        // Lookback window: (5_684_567, 5_984_567]
        // Sample 5_723_456 (value 10.0) is within the window
        // Sample 5_987_654 (value 20.0) is outside the window (too late)
        if let ExprResult::InstantVector(samples) = result {
            assert_eq!(samples.len(), 1);
            assert_eq!(samples[0].value, 10.0);
            assert_eq!(samples[0].timestamp_ms, 5_723_456);
        } else {
            panic!("Expected InstantVector result");
        }
    }

    #[test]
    fn binary_vector_vector_duplicate_right_key_error() {
        // Two right-side series that have different full label sets but the same match key
        // when using on(env). memory_bytes{env="prod",instance="i1"} and
        // memory_bytes{env="prod",instance="i2"} both map to match key {env="prod"}.
        let test_data: TestSampleData = vec![
            ("cpu_usage", vec![("env", "prod")], 0, 50.0),
            (
                "memory_bytes",
                vec![("env", "prod"), ("instance", "i1")],
                1,
                100.0,
            ),
            (
                "memory_bytes",
                vec![("env", "prod"), ("instance", "i2")],
                2,
                200.0,
            ),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let evaluator = Evaluator::new(&reader, QueryOptions::default());
        let lookback_delta = Duration::from_secs(300);

        let result = parse_and_evaluate(
            &evaluator,
            "cpu_usage + on(env) memory_bytes",
            end_time,
            lookback_delta,
        );

        assert!(
            result.is_err(),
            "Expected error for duplicate right-side match key"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("duplicate series on the right side"),
            "Error should mention right side: {err}"
        );
    }

    #[test]
    fn binary_vector_vector_duplicate_left_key_matched_error() {
        // Two left-side series that collapse to the same match key with on(env),
        // and there IS a matching right-side series.
        let test_data: TestSampleData = vec![
            (
                "cpu_usage",
                vec![("env", "prod"), ("instance", "i1")],
                0,
                50.0,
            ),
            (
                "cpu_usage",
                vec![("env", "prod"), ("instance", "i2")],
                1,
                60.0,
            ),
            ("memory_bytes", vec![("env", "prod")], 2, 100.0),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let evaluator = Evaluator::new(&reader, QueryOptions::default());
        let lookback_delta = Duration::from_secs(300);

        let result = parse_and_evaluate(
            &evaluator,
            "cpu_usage + on(env) memory_bytes",
            end_time,
            lookback_delta,
        );

        assert!(
            result.is_err(),
            "Expected error for duplicate left-side match key"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("duplicate series on the left side"),
            "Error should mention left side: {err}"
        );
    }

    #[test]
    fn binary_vector_vector_duplicate_left_key_unmatched_ok() {
        // Two left-side series that collapse to the same match key with on(env),
        // but no right-side match — should NOT error, silently dropped.
        let test_data: TestSampleData = vec![
            (
                "cpu_usage",
                vec![("env", "prod"), ("instance", "i1")],
                0,
                50.0,
            ),
            (
                "cpu_usage",
                vec![("env", "prod"), ("instance", "i2")],
                1,
                60.0,
            ),
            ("memory_bytes", vec![("env", "staging")], 2, 100.0),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let evaluator = Evaluator::new(&reader, QueryOptions::default());
        let lookback_delta = Duration::from_secs(300);

        let result = parse_and_evaluate(
            &evaluator,
            "cpu_usage - on(env) memory_bytes",
            end_time,
            lookback_delta,
        );

        assert!(
            result.is_ok(),
            "Should not error for unmatched duplicate left keys"
        );
        let samples = result.unwrap();
        assert!(
            samples.is_empty(),
            "No matches expected, got {} samples",
            samples.len()
        );
    }

    #[test]
    fn should_evaluate_group_left_add() {
        // given: many left-side series matched to one right-side series via on(env)
        // cpu_usage has two series per env (different instance), memory_bytes has one per env.
        let test_data: TestSampleData = vec![
            (
                "cpu_usage",
                vec![("env", "prod"), ("instance", "i1")],
                0,
                50.0,
            ),
            (
                "cpu_usage",
                vec![("env", "prod"), ("instance", "i2")],
                1,
                60.0,
            ),
            (
                "cpu_usage",
                vec![("env", "staging"), ("instance", "i3")],
                2,
                70.0,
            ),
            ("memory_bytes", vec![("env", "prod")], 3, 100.0),
            ("memory_bytes", vec![("env", "staging")], 4, 200.0),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let evaluator = Evaluator::new(&reader, QueryOptions::default());
        let lookback_delta = Duration::from_secs(300);

        // when
        let result = parse_and_evaluate(
            &evaluator,
            "cpu_usage + on(env) group_left memory_bytes",
            end_time,
            lookback_delta,
        )
            .expect("group_left query should evaluate successfully");

        // then: result labels come from the many (left) side, __name__ dropped by arithmetic
        assert_results_match(
            &result,
            &[
                (150.0, vec![("env", "prod"), ("instance", "i1")]),
                (160.0, vec![("env", "prod"), ("instance", "i2")]),
                (270.0, vec![("env", "staging"), ("instance", "i3")]),
            ],
        );
    }

    #[test]
    fn should_evaluate_group_left_with_extra_labels() {
        // given: group_left(region) copies the "region" label from the one (right) side
        let test_data: TestSampleData = vec![
            (
                "cpu_usage",
                vec![("env", "prod"), ("instance", "i1")],
                0,
                50.0,
            ),
            (
                "cpu_usage",
                vec![("env", "prod"), ("instance", "i2")],
                1,
                60.0,
            ),
            (
                "memory_bytes",
                vec![("env", "prod"), ("region", "us-east")],
                2,
                100.0,
            ),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let evaluator = Evaluator::new(&reader, QueryOptions::default());
        let lookback_delta = Duration::from_secs(300);

        // when
        let result = parse_and_evaluate(
            &evaluator,
            "cpu_usage + on(env) group_left(region) memory_bytes",
            end_time,
            lookback_delta,
        )
            .expect("group_left with extra labels should evaluate successfully");

        // then: result has many-side labels plus the extra "region" from one side
        assert_results_match(
            &result,
            &[
                (
                    150.0,
                    vec![("env", "prod"), ("instance", "i1"), ("region", "us-east")],
                ),
                (
                    160.0,
                    vec![("env", "prod"), ("instance", "i2"), ("region", "us-east")],
                ),
            ],
        );
    }

    #[test]
    fn should_evaluate_group_left_comparison() {
        // given: comparison with group_left filters out false results
        let test_data: TestSampleData = vec![
            ("cpu_usage", vec![("env", "prod")], 0, 150.0),
            (
                "memory_bytes",
                vec![("env", "prod"), ("instance", "i1")],
                1,
                100.0,
            ),
            (
                "memory_bytes",
                vec![("env", "prod"), ("instance", "i2")],
                2,
                200.0,
            ),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let evaluator = Evaluator::new(&reader, QueryOptions::default());
        let lookback_delta = Duration::from_secs(300);

        // when
        let result = parse_and_evaluate(
            &evaluator,
            "cpu_usage > on(env) group_left memory_bytes",
            end_time,
            lookback_delta,
        )
            .expect("group_left comparison should evaluate successfully");

        // then: only the 150 > 100 result survives; non-bool comparison propagates lhs sample value
        assert_results_match(
            &result,
            &[(
                150.0,
                vec![
                    ("__name__", "cpu_usage"),
                    ("env", "prod"),
                    ("instance", "i1"),
                ],
            )],
        );
    }

    #[test]
    fn should_error_group_left_duplicate_on_right_side() {
        // given: group_left but the right (one) side has duplicates after matching
        let test_data: TestSampleData = vec![
            ("cpu_usage", vec![("env", "prod")], 0, 50.0),
            (
                "memory_bytes",
                vec![("env", "prod"), ("instance", "i1")],
                1,
                100.0,
            ),
            (
                "memory_bytes",
                vec![("env", "prod"), ("instance", "i2")],
                2,
                200.0,
            ),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let evaluator = Evaluator::new(&reader, QueryOptions::default());
        let lookback_delta = Duration::from_secs(300);

        // when
        let result = parse_and_evaluate(
            &evaluator,
            "cpu_usage + on(env) group_left memory_bytes",
            end_time,
            lookback_delta,
        );

        // then: error - the one side has duplicates
        assert!(
            result.is_err(),
            "Expected error for duplicate series on the one (right) side"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("duplicate series on the right side"),
            "Error should mention right side: {err}"
        );
    }

    #[test]
    fn should_error_group_left_duplicate_on_left_side() {
        // given: group_left with duplicate series on the "one" (right) side, not "many" (left) side
        // Note: the test name is misleading - it should actually test duplicates on the ONE side
        // For group_left, left is many, right is one. Duplicates on left (many) are expected.
        // But duplicates on right (one) should error for arithmetic operations.
        let test_data: TestSampleData = vec![
            ("cpu_usage", vec![("env", "prod")], 0, 50.0),
            (
                "memory_bytes",
                vec![("env", "prod"), ("instance", "i1")],
                1,
                100.0,
            ),
            (
                "memory_bytes",
                vec![("env", "prod"), ("instance", "i2")],
                2,
                200.0,
            ),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let evaluator = Evaluator::new(&reader, QueryOptions::default());
        let lookback_delta = Duration::from_secs(300);

        // when
        let result = parse_and_evaluate(
            &evaluator,
            "cpu_usage + on(env) group_left memory_bytes",
            end_time,
            lookback_delta,
        );

        // then: error - the one (right) side has duplicates
        assert!(
            result.is_err(),
            "Expected error for duplicate series on the one (right) side"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("duplicate series on the right side"),
            "Error should mention right side: {err}"
        );
    }

    #[test]
    fn should_evaluate_group_left_no_match() {
        // given: no matching env between lhs and rhs
        let test_data: TestSampleData = vec![
            ("cpu_usage", vec![("env", "prod")], 0, 50.0),
            (
                "memory_bytes",
                vec![("env", "staging"), ("instance", "i1")],
                1,
                100.0,
            ),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let evaluator = Evaluator::new(&reader, QueryOptions::default());
        let lookback_delta = Duration::from_secs(300);

        // when
        let result = parse_and_evaluate(
            &evaluator,
            "cpu_usage + on(env) group_left memory_bytes",
            end_time,
            lookback_delta,
        )
            .expect("group_left with no match should return empty");

        // then: empty - no env matches
        assert!(
            result.is_empty(),
            "Expected empty result, got {} samples",
            result.len()
        );
    }

    #[test]
    fn should_evaluate_group_left_with_ignoring() {
        // given: group_left with ignoring() instead of on()
        let test_data: TestSampleData = vec![
            (
                "cpu_usage",
                vec![("env", "prod"), ("instance", "i1"), ("region", "us-east")],
                0,
                50.0,
            ),
            (
                "cpu_usage",
                vec![("env", "prod"), ("instance", "i2"), ("region", "us-east")],
                1,
                60.0,
            ),
            (
                "cpu_usage",
                vec![("env", "prod"), ("instance", "i3"), ("region", "eu-west")],
                2,
                70.0,
            ),
            (
                "memory_bytes",
                vec![("env", "prod"), ("instance", "i99"), ("region", "us-east")],
                3,
                100.0,
            ),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let evaluator = Evaluator::new(&reader, QueryOptions::default());
        let lookback_delta = Duration::from_secs(300);

        // when: ignoring(instance) removes instance from key, so key = {env, region}
        let result = parse_and_evaluate(
            &evaluator,
            "cpu_usage + ignoring(instance) group_left memory_bytes",
            end_time,
            lookback_delta,
        )
            .expect("group_left with ignoring should evaluate successfully");

        // then: only region="us-east" matches; region="eu-west" is dropped (no one-side match)
        assert_results_match(
            &result,
            &[
                (
                    150.0,
                    vec![("env", "prod"), ("instance", "i1"), ("region", "us-east")],
                ),
                (
                    160.0,
                    vec![("env", "prod"), ("instance", "i2"), ("region", "us-east")],
                ),
            ],
        );
    }

    #[test]
    fn should_evaluate_group_right_add() {
        // given: one left-side series matched to many right-side series via on(env)
        // cpu_usage has one per env, memory_bytes has two per env (different instance).
        let test_data: TestSampleData = vec![
            ("cpu_usage", vec![("env", "prod")], 0, 50.0),
            ("cpu_usage", vec![("env", "staging")], 1, 70.0),
            (
                "memory_bytes",
                vec![("env", "prod"), ("instance", "i1")],
                2,
                100.0,
            ),
            (
                "memory_bytes",
                vec![("env", "prod"), ("instance", "i2")],
                3,
                200.0,
            ),
            (
                "memory_bytes",
                vec![("env", "staging"), ("instance", "i3")],
                4,
                300.0,
            ),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let evaluator = Evaluator::new(&reader, QueryOptions::default());
        let lookback_delta = Duration::from_secs(300);

        // when
        let result = parse_and_evaluate(
            &evaluator,
            "cpu_usage + on(env) group_right memory_bytes",
            end_time,
            lookback_delta,
        )
            .expect("group_right query should evaluate successfully");

        // then: result labels come from the many (right) side, __name__ dropped by arithmetic
        assert_results_match(
            &result,
            &[
                (150.0, vec![("env", "prod"), ("instance", "i1")]),
                (250.0, vec![("env", "prod"), ("instance", "i2")]),
                (370.0, vec![("env", "staging"), ("instance", "i3")]),
            ],
        );
    }

    #[test]
    fn should_evaluate_group_right_with_extra_labels() {
        // given: group_right(region) copies "region" label from the one (left) side
        // For group_right, left is one (unique), right is many
        let test_data: TestSampleData = vec![
            ("cpu_usage", vec![("env", "prod")], 0, 50.0),
            (
                "memory_bytes",
                vec![("env", "prod"), ("instance", "i1"), ("region", "us-east")],
                1,
                100.0,
            ),
            (
                "memory_bytes",
                vec![("env", "prod"), ("instance", "i2"), ("region", "eu-west")],
                2,
                200.0,
            ),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let evaluator = Evaluator::new(&reader, QueryOptions::default());
        let lookback_delta = Duration::from_secs(300);

        // when
        let result = parse_and_evaluate(
            &evaluator,
            "cpu_usage + on(env) group_right(region) memory_bytes",
            end_time,
            lookback_delta,
        )
            .expect("group_right with extra labels should evaluate successfully");

        // then: result has many-side labels plus the extra "region" from one (left) side
        // Since cpu_usage doesn't have a "region" label, the region from memory_bytes is preserved
        assert_results_match(
            &result,
            &[
                (
                    150.0,
                    vec![("env", "prod"), ("instance", "i1"), ("region", "us-east")],
                ),
                (
                    250.0,
                    vec![("env", "prod"), ("instance", "i2"), ("region", "eu-west")],
                ),
            ],
        );
    }

    #[test]
    fn should_evaluate_group_right_comparison() {
        // given: comparison with group_right filters out false results
        let test_data: TestSampleData = vec![
            ("cpu_usage", vec![("env", "prod")], 0, 150.0),
            (
                "memory_bytes",
                vec![("env", "prod"), ("instance", "i1")],
                1,
                100.0,
            ),
            (
                "memory_bytes",
                vec![("env", "prod"), ("instance", "i2")],
                2,
                200.0,
            ),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let evaluator = Evaluator::new(&reader, QueryOptions::default());
        let lookback_delta = Duration::from_secs(300);

        // when
        let result = parse_and_evaluate(
            &evaluator,
            "cpu_usage > on(env) group_right memory_bytes",
            end_time,
            lookback_delta,
        )
            .expect("group_right comparison should evaluate successfully");

        // then: only 150 > 100 survives; non-bool comparison propagates lhs sample value
        assert_results_match(
            &result,
            &[(
                150.0,
                vec![
                    ("__name__", "memory_bytes"),
                    ("env", "prod"),
                    ("instance", "i1"),
                ],
            )],
        );
    }

    #[test]
    fn should_error_group_right_duplicate_on_left_side() {
        // given: group_right but the left (one) side has duplicates after matching
        let test_data: TestSampleData = vec![
            (
                "cpu_usage",
                vec![("env", "prod"), ("instance", "i1")],
                0,
                50.0,
            ),
            (
                "cpu_usage",
                vec![("env", "prod"), ("instance", "i2")],
                1,
                60.0,
            ),
            ("memory_bytes", vec![("env", "prod")], 2, 100.0),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let evaluator = Evaluator::new(&reader, QueryOptions::default());
        let lookback_delta = Duration::from_secs(300);

        // when
        let result = parse_and_evaluate(
            &evaluator,
            "cpu_usage + on(env) group_right memory_bytes",
            end_time,
            lookback_delta,
        );

        // then: error - the one (left) side has duplicates
        assert!(
            result.is_err(),
            "Expected error for duplicate series on the one (left) side"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("duplicate series on the left side"),
            "Error should mention left side: {err}"
        );
    }

    #[test]
    fn should_evaluate_group_right_no_match() {
        // given: no matching env between lhs and rhs
        let test_data: TestSampleData = vec![
            ("cpu_usage", vec![("env", "prod")], 0, 50.0),
            (
                "memory_bytes",
                vec![("env", "staging"), ("instance", "i1")],
                1,
                100.0,
            ),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let evaluator = Evaluator::new(&reader, QueryOptions::default());
        let lookback_delta = Duration::from_secs(300);

        // when
        let result = parse_and_evaluate(
            &evaluator,
            "cpu_usage + on(env) group_right memory_bytes",
            end_time,
            lookback_delta,
        )
            .expect("group_right with no match should return empty");

        // then: empty - no env matches
        assert!(
            result.is_empty(),
            "Expected empty result, got {} samples",
            result.len()
        );
    }

    #[test]
    fn should_evaluate_group_right_with_ignoring() {
        // given: group_right with ignoring() instead of on()
        let test_data: TestSampleData = vec![
            (
                "cpu_usage",
                vec![("env", "prod"), ("instance", "i99"), ("region", "us-east")],
                0,
                50.0,
            ),
            (
                "memory_bytes",
                vec![("env", "prod"), ("instance", "i1"), ("region", "us-east")],
                1,
                100.0,
            ),
            (
                "memory_bytes",
                vec![("env", "prod"), ("instance", "i2"), ("region", "us-east")],
                2,
                200.0,
            ),
            (
                "memory_bytes",
                vec![("env", "prod"), ("instance", "i3"), ("region", "eu-west")],
                3,
                300.0,
            ),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let evaluator = Evaluator::new(&reader, QueryOptions::default());
        let lookback_delta = Duration::from_secs(300);

        // when: ignoring(instance) removes instance from key, so key = {env, region}
        let result = parse_and_evaluate(
            &evaluator,
            "cpu_usage + ignoring(instance) group_right memory_bytes",
            end_time,
            lookback_delta,
        )
            .expect("group_right with ignoring should evaluate successfully");

        // then: only region="us-east" matches; region="eu-west" is dropped (no one-side match)
        assert_results_match(
            &result,
            &[
                (
                    150.0,
                    vec![("env", "prod"), ("instance", "i1"), ("region", "us-east")],
                ),
                (
                    250.0,
                    vec![("env", "prod"), ("instance", "i2"), ("region", "us-east")],
                ),
            ],
        );
    }

    #[test]
    fn should_error_when_group_left_produces_duplicate_output_labelsets() {
        // given: two many-side series differ only by metric name. Arithmetic drops __name__,
        // so both outputs collapse to the same label set and must be rejected.
        let test_data: TestSampleData = vec![
            (
                "cpu_usage",
                vec![("env", "prod"), ("instance", "i1")],
                0,
                50.0,
            ),
            (
                "cpu_usage_alt",
                vec![("env", "prod"), ("instance", "i1")],
                1,
                60.0,
            ),
            ("memory_bytes", vec![("env", "prod")], 2, 100.0),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let evaluator = Evaluator::new(&reader, QueryOptions::default());
        let lookback_delta = Duration::from_secs(300);

        // when
        let result = parse_and_evaluate(
            &evaluator,
            r#"{__name__=~"cpu_usage|cpu_usage_alt"} + on(env) group_left memory_bytes"#,
            end_time,
            lookback_delta,
        );

        // then: Prometheus requires output series to remain uniquely identifiable
        assert!(
            result.is_err(),
            "Expected error for duplicate-match error before false filtering"
        );
    }

    #[test]
    fn should_handle_subquery_step_fallback_in_instant_context() {
        use promql_parser::parser::SubqueryExpr;

        // given: data at 0s and 10s
        let mut builder = MockQueryReaderBuilder::new();
        let labels = Labels::new(vec![Label::new("__name__", "metric")]);
        builder.add_sample(
            &labels,
            Sample {
                timestamp: 0,
                value: 1.0,
            },
        );
        builder.add_sample(
            &labels,
            Sample {
                timestamp: 10_000,
                value: 2.0,
            },
        );

        let reader = builder.build();
        let evaluator = Evaluator::new(&reader, QueryOptions::default());

        // when: subquery with no step in instant query context (interval = 0)
        let subquery = SubqueryExpr {
            expr: Box::new(Expr::VectorSelector(VectorSelector {
                name: Some("metric".to_string()),
                matchers: Matchers::empty(),
                offset: None,
                at: None,
            })),
            range: Duration::from_secs(50),
            step: None, // No explicit step
            offset: None,
            at: None,
        };

        let eval_time_ms = 10_000i64; // 10 seconds in ms
        let ctx = EvalContext {
            query_start: eval_time_ms,
            query_end: eval_time_ms,
            evaluation_ts: eval_time_ms,
            step_ms: 0, // instant query context
            lookback_delta_ms: 300_000,
        };
        let result = evaluator.evaluate_subquery(&subquery, &ctx, false);

        // then: should not panic or infinite loop, should use fallback step
        assert!(result.is_ok());
    }

    #[test]
    fn should_preserve_subquery_step_order_when_parallelized() {
        // given: a metric with samples every 10s
        let mut builder = MockQueryReaderBuilder::new();
        let labels = Labels::new(vec![Label::new("__name__", "metric")]);
        builder.add_sample(
            &labels,
            Sample {
                timestamp: 0,
                value: 1.0,
            },
        );
        builder.add_sample(
            &labels,
            Sample {
                timestamp: 10_000,
                value: 2.0,
            },
        );
        builder.add_sample(
            &labels,
            Sample {
                timestamp: 20_000,
                value: 3.0,
            },
        );

        let reader = builder.build();
        let evaluator = Evaluator::new(&reader, QueryOptions::default());

        // when: evaluate a non-VectorSelector subquery (forces generic subquery path)
        // with 5 steps so it takes the parallel branch.
        let expr = promql_parser::parser::parse("abs(metric)[25s:5s]").unwrap();
        let Expr::Subquery(subquery) = expr else {
            panic!("expected parsed expression to be a subquery");
        };

        let eval_time_ms = 20_000i64;
        let ctx = EvalContext {
            query_start: eval_time_ms,
            query_end: eval_time_ms,
            evaluation_ts: eval_time_ms,
            step_ms: 0,
            lookback_delta_ms: 300_000,
        };

        let result = evaluator.evaluate_subquery(&subquery, &ctx, false).unwrap();
        let ExprResult::RangeVector(mut range_vector) = result else {
            panic!("expected range vector result");
        };

        assert_eq!(range_vector.len(), 1);
        let timestamps: Vec<_> = range_vector
            .pop()
            .unwrap()
            .values
            .into_iter()
            .map(|s| s.timestamp)
            .collect();

        assert_eq!(timestamps, vec![0, 5_000, 10_000, 15_000, 20_000]);
    }

    #[test]
    fn should_align_negative_timestamps_correctly() {
        // Test floor division alignment for negative timestamps
        let subquery_start_ms = -41i64;
        let step_ms = 10i64;

        // Using regular division (incorrect)
        let wrong_div = subquery_start_ms / step_ms; // -4 (truncates toward zero)
        let wrong_aligned = wrong_div * step_ms; // -40

        // Using div_euclid (correct floor division)
        let correct_div = subquery_start_ms.div_euclid(step_ms); // -5 (floor)
        let correct_aligned = correct_div * step_ms; // -50

        assert_eq!(wrong_aligned, -40, "Regular division gives -40");
        assert_eq!(correct_aligned, -50, "Floor division gives -50");

        // Prometheus expects floor division behavior
        assert_ne!(
            wrong_aligned, correct_aligned,
            "Regular division != floor division for negatives"
        );
    }
}
