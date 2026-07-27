#[cfg(test)]
mod tests {
    use crate::common::Sample;
    use crate::promql::EvalContext;
    use crate::promql::{
        EvalResult, EvalSample, EvalSamples, EvaluationError, Evaluator, ExprResult,
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
    use crate::labels::{Label, Labels};
    use crate::promql::engine::query_reader::{
        AggregationOutcome, AggregationParam, AggregationRequest, RollupOutcome, RollupRequest,
    };
    use crate::promql::engine::test_utils::{
        MemorySeriesQuerier, MockMultiBucketQueryReaderBuilder, MockQueryReaderBuilder,
    };
    use crate::promql::engine::{QueryOptions, QueryReader};
    use crate::promql::exec::aggregations::AggregationKind;
    use crate::promql::functions::RollupKind;
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

        for (i, (actual_sample, (expected_value, expected_labels))) in
            actual_sorted.into_iter().zip(expected_sorted).enumerate()
        {
            assert!(
                approx_eq(actual_sample.value, expected_value),
                "Sample {i} value mismatch: got {}, expected {}",
                actual_sample.value,
                expected_value
            );

            assert_eq!(
                actual_sample.labels.as_ref(),
                expected_labels.as_ref(),
                "Sample {i} labels mismatch: got {:?}, expected {:?}",
                actual_sample.labels,
                expected_labels
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
    fn setup_mock_reader(data: TestSampleData) -> (MemorySeriesQuerier, SystemTime) {
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
            (10.0, vec![("env", "prod"), ("method", "GET")]),
            (20.0, vec![("env", "prod"), ("method", "POST")]),
            (30.0, vec![("env", "staging"), ("method", "GET")]),
            (40.0, vec![("env", "staging"), ("method", "POST")]),
        ]
    )]
    #[case(
        "function_sqrt",
        "sqrt(memory_bytes)",
        vec![
            ("memory_bytes", vec![("env", "prod")], 0, 100.0),
        ],
        vec![
            (10.0, vec![("env", "prod")]), // sqrt(100) = 10
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
            (50.0, vec![("env", "prod"), ("instance", "i1")]),
            (60.0, vec![("env", "prod"), ("instance", "i2")]),
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
            (50.0, vec![("env", "prod"), ("instance", "i1")]),
            (60.0, vec![("env", "prod"), ("instance", "i2")]),
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
            (50.0, vec![("env", "prod"), ("instance", "i1")]),
            (60.0, vec![("env", "prod"), ("instance", "i2")]),
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
            (50.0_f64.sin(), vec![("env", "prod"), ("instance", "i1")]),
            (60.0_f64.sin(), vec![("env", "prod"), ("instance", "i2")]),
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
            (50.0_f64.cos(), vec![("env", "prod"), ("instance", "i1")]),
            (60.0_f64.cos(), vec![("env", "prod"), ("instance", "i2")]),
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
            (100.0_f64.ln(), vec![("env", "prod")]),
        ]
    )]
    #[case(
        "function_log10",
        "log10(memory_bytes)",
        vec![
            ("memory_bytes", vec![("env", "prod")], 0, 100.0),
        ],
        vec![
            (100.0_f64.log10(), vec![("env", "prod")]), // log10(100) = 2
        ]
    )]
    #[case(
        "function_log2",
        "log2(memory_bytes)",
        vec![
            ("memory_bytes", vec![("env", "prod")], 0, 100.0),
        ],
        vec![
            (100.0_f64.log2(), vec![("env", "prod")]),
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
    fn should_return_identical_results_across_evaluations() {
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
        // Second evaluation
        let result2 = evaluator
            .evaluate(stmt.clone())
            .unwrap()
            .expect_instant_vector("Expected instant vector result");

        // then: results should be identical
        assert_eq!(result1.len(), 1);
        assert_eq!(result2.len(), 1);
        assert_eq!(result1[0].value, 100.0);
        assert_eq!(result2[0].value, 100.0);
        assert_eq!(result1[0].labels, result2[0].labels);
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
                let labels = Labels::from_pairs(labels).into();
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
                        labels: labels.into(),
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
    fn k_aggregations_nan_parameter_should_error() {
        // Prometheus rejects NaN k parameters ("Parameter value is NaN")
        // instead of silently returning an empty result.
        let test_data: TestSampleData = vec![
            ("http_requests_total", vec![("env", "prod")], 0, 10.0),
            ("http_requests_total", vec![("env", "staging")], 1, 20.0),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let evaluator = Evaluator::new(&reader, QueryOptions::default());
        let lookback_delta = Duration::from_secs(300);

        for query in [
            "topk(NaN, http_requests_total)",
            "bottomk(NaN, http_requests_total)",
            "limitk(NaN, http_requests_total)",
        ] {
            let result = parse_and_evaluate(&evaluator, query, end_time, lookback_delta);
            assert!(result.is_err(), "{query} should error on NaN parameter");
            let err = result.unwrap_err().to_string();
            assert!(
                err.contains("Parameter value is NaN"),
                "unexpected error for {query}: {err}"
            );
        }
    }

    #[test]
    fn k_aggregations_infinite_k_keeps_all_series() {
        // Prometheus clamps k parameters above MaxInt64 (e.g. +Inf) rather
        // than erroring; with k = +Inf every series is kept.
        let test_data: TestSampleData = vec![
            ("http_requests_total", vec![("env", "prod")], 0, 10.0),
            ("http_requests_total", vec![("env", "staging")], 1, 20.0),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let evaluator = Evaluator::new(&reader, QueryOptions::default());
        let lookback_delta = Duration::from_secs(300);

        let result = parse_and_evaluate(
            &evaluator,
            "topk(Inf, http_requests_total)",
            end_time,
            lookback_delta,
        )
        .expect("topk(Inf, ...) should succeed");
        assert_eq!(result.len(), 2, "+Inf k should keep all series");
    }

    #[test]
    fn limit_ratio_nan_should_error_and_inf_should_clamp() {
        let test_data: TestSampleData = vec![
            ("http_requests_total", vec![("env", "prod")], 0, 10.0),
            ("http_requests_total", vec![("env", "staging")], 1, 20.0),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let evaluator = Evaluator::new(&reader, QueryOptions::default());
        let lookback_delta = Duration::from_secs(300);

        // NaN ratio is an error in Prometheus ("Ratio value is NaN").
        let result = parse_and_evaluate(
            &evaluator,
            "limit_ratio(NaN, http_requests_total)",
            end_time,
            lookback_delta,
        );
        assert!(result.is_err(), "limit_ratio(NaN, ...) should error");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Ratio value is NaN"),
            "unexpected error: {err}"
        );

        // Ratios outside [-1, 1] (including +Inf) are clamped, not rejected.
        // ratio clamped to 1.0 selects every series.
        let result = parse_and_evaluate(
            &evaluator,
            "limit_ratio(Inf, http_requests_total)",
            end_time,
            lookback_delta,
        )
        .expect("limit_ratio(Inf, ...) should clamp to 1.0 and succeed");
        assert_eq!(result.len(), 2, "clamped ratio 1.0 should keep all series");
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
        let result = evaluator.evaluate_subquery(&subquery, &ctx);

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

        let result = evaluator.evaluate_subquery(&subquery, &ctx).unwrap();
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

    // ────────────────────────────────────────────────────────────────────────────
    // eval_vector_vector_binop – filter-pushdown tests
    //
    // These tests exercise the fast path in `Evaluator::eval_vector_vector_binop`
    // where common label filters derived from the first-evaluated side are pushed
    // down into the selector for the second side, pruning unnecessary series before
    // the binary-op matching step.
    // ────────────────────────────────────────────────────────────────────────────

    /// Multiplication where the single LHS series has `env="prod"`.
    /// The pushdown injects `env="prod"` and `job="api"` into the RHS selector so
    /// the `env="staging"` RHS series is excluded *before* the binary-op matching
    /// step.  Uses no `on()`/`ignoring()` modifier so the fast-path is exercised
    /// and there are no grouped-cardinality duplicate-detection issues.
    #[test]
    fn pushdown_injects_common_lhs_label_into_rhs_selector() {
        // given
        let test_data: TestSampleData = vec![
            // LHS – single series, env="prod"
            ("metric_a", vec![("env", "prod"), ("job", "api")], 0, 10.0),
            // RHS – one matching (env="prod"), one non-matching (env="staging").
            // Without pushdown both RHS series are fetched; with pushdown the
            // common filters env="prod" and job="api" are injected into the
            // metric_b selector so the staging series is pruned before matching.
            ("metric_b", vec![("env", "prod"), ("job", "api")], 1, 2.0),
            (
                "metric_b",
                vec![("env", "staging"), ("job", "api")],
                2,
                99.0,
            ),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let evaluator = Evaluator::new(&reader, QueryOptions::default());
        let lookback_delta = Duration::from_secs(300);

        // when – no explicit on/ignoring: matches on all labels except __name__
        let mut result =
            parse_and_evaluate(&evaluator, "metric_a * metric_b", end_time, lookback_delta)
                .expect("pushdown multiplication should succeed");

        // then – only the prod pair matched; staging series must be absent
        sort_samples_by_labels(&mut result);
        assert_eq!(result.len(), 1, "staging series must not appear in result");
        assert_results_match(&result, &[(20.0, vec![("env", "prod"), ("job", "api")])]);
    }

    /// When every LHS series shares *multiple* labels the pushdown should derive
    /// a filter for each shared label and apply them all to the RHS selector.
    #[test]
    fn pushdown_injects_multiple_common_lhs_labels_into_rhs_selector() {
        // given
        let test_data: TestSampleData = vec![
            // LHS – all series share env="prod" AND region="us-east"
            (
                "requests",
                vec![("env", "prod"), ("region", "us-east"), ("job", "api")],
                0,
                100.0,
            ),
            (
                "requests",
                vec![("env", "prod"), ("region", "us-east"), ("job", "worker")],
                1,
                200.0,
            ),
            // RHS – two matching, one non-matching (different env+region)
            (
                "errors",
                vec![("env", "prod"), ("region", "us-east"), ("job", "api")],
                2,
                5.0,
            ),
            (
                "errors",
                vec![("env", "prod"), ("region", "us-east"), ("job", "worker")],
                3,
                10.0,
            ),
            (
                "errors",
                vec![("env", "staging"), ("region", "eu-west"), ("job", "api")],
                4,
                1.0,
            ),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let evaluator = Evaluator::new(&reader, QueryOptions::default());
        let lookback_delta = Duration::from_secs(300);

        // when
        let mut result = parse_and_evaluate(
            &evaluator,
            "requests - on(env, region, job) errors",
            end_time,
            lookback_delta,
        )
        .expect("multi-label pushdown subtraction should succeed");

        // then – only the two prod/us-east pairs survive
        sort_samples_by_labels(&mut result);
        assert_results_match(
            &result,
            &[
                (
                    95.0,
                    vec![("env", "prod"), ("job", "api"), ("region", "us-east")],
                ),
                (
                    190.0,
                    vec![("env", "prod"), ("job", "worker"), ("region", "us-east")],
                ),
            ],
        );
    }

    /// When LHS series do *not* share a common value for a label the pushdown
    /// must not add a restricting filter for that label, so all RHS series remain
    /// accessible for matching.
    #[test]
    fn pushdown_skips_filter_when_lhs_label_values_differ() {
        // given – LHS has both env="prod" and env="staging", so no common env filter
        let test_data: TestSampleData = vec![
            ("metric_a", vec![("env", "prod"), ("job", "api")], 0, 10.0),
            (
                "metric_a",
                vec![("env", "staging"), ("job", "api")],
                1,
                20.0,
            ),
            ("metric_b", vec![("env", "prod"), ("job", "api")], 2, 3.0),
            ("metric_b", vec![("env", "staging"), ("job", "api")], 3, 4.0),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let evaluator = Evaluator::new(&reader, QueryOptions::default());
        let lookback_delta = Duration::from_secs(300);

        // when
        let mut result = parse_and_evaluate(
            &evaluator,
            "metric_a + on(env, job) metric_b",
            end_time,
            lookback_delta,
        )
        .expect("mixed-label pushdown should still return all matched pairs");

        // then – both env pairs match
        sort_samples_by_labels(&mut result);
        assert_results_match(
            &result,
            &[
                (13.0, vec![("env", "prod"), ("job", "api")]),
                (24.0, vec![("env", "staging"), ("job", "api")]),
            ],
        );
    }

    /// For the AND operator the evaluator swaps evaluation order: RHS is fetched
    /// first and its common filters are pushed down into the LHS selector.
    /// Only LHS series that match an RHS series (by all labels except __name__)
    /// are returned.
    #[test]
    fn pushdown_and_operator_evaluates_rhs_first_and_filters_lhs() {
        // given
        let test_data: TestSampleData = vec![
            // LHS – two environments
            ("metric_a", vec![("env", "prod"), ("job", "api")], 0, 42.0),
            (
                "metric_a",
                vec![("env", "staging"), ("job", "api")],
                1,
                99.0,
            ),
            // RHS – only prod; pushdown should suppress the staging LHS series
            ("metric_b", vec![("env", "prod"), ("job", "api")], 2, 1.0),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let evaluator = Evaluator::new(&reader, QueryOptions::default());
        let lookback_delta = Duration::from_secs(300);

        // when
        let mut result = parse_and_evaluate(
            &evaluator,
            "metric_a and metric_b",
            end_time,
            lookback_delta,
        )
        .expect("AND pushdown should succeed");

        // then – only the prod series passes the AND filter; value and metric name come
        // from the LHS (metric_a), which is correct PromQL AND semantics.
        sort_samples_by_labels(&mut result);
        assert_results_match(
            &result,
            &[(
                42.0,
                vec![("__name__", "metric_a"), ("env", "prod"), ("job", "api")],
            )],
        );
    }

    /// Structural regression for `and`: even when selectors are wrapped in
    /// parentheses (still eligible for the vector-vector fast path), the result
    /// must remain LHS-owned.
    #[test]
    fn pushdown_and_with_parenthesized_selectors_preserves_lhs_ownership() {
        let test_data: TestSampleData = vec![
            ("metric_a", vec![("env", "prod"), ("job", "api")], 0, 11.0),
            (
                "metric_a",
                vec![("env", "staging"), ("job", "api")],
                1,
                88.0,
            ),
            ("metric_b", vec![("env", "prod"), ("job", "api")], 2, 7.0),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let evaluator = Evaluator::new(&reader, QueryOptions::default());

        let mut result = parse_and_evaluate(
            &evaluator,
            "(metric_a) and (metric_b)",
            end_time,
            Duration::from_secs(300),
        )
        .expect("parenthesized AND should succeed");

        sort_samples_by_labels(&mut result);
        assert_results_match(
            &result,
            &[(
                11.0,
                vec![("__name__", "metric_a"), ("env", "prod"), ("job", "api")],
            )],
        );
    }

    /// The OR operator must NOT push down filters: both sides are evaluated
    /// independently so that series present only on one side are still returned.
    #[test]
    fn pushdown_or_operator_returns_union_without_filter_restriction() {
        // given – disjoint label sets
        let test_data: TestSampleData = vec![
            ("metric_a", vec![("env", "prod")], 0, 10.0),
            ("metric_b", vec![("env", "staging")], 1, 20.0),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let evaluator = Evaluator::new(&reader, QueryOptions::default());
        let lookback_delta = Duration::from_secs(300);

        // when
        let mut result =
            parse_and_evaluate(&evaluator, "metric_a or metric_b", end_time, lookback_delta)
                .expect("OR should return union");

        // then – both series appear; OR never pushes a filter that would drop one
        sort_samples_by_labels(&mut result);
        assert_eq!(result.len(), 2, "OR must return series from both sides");
        assert_results_match(
            &result,
            &[
                (10.0, vec![("__name__", "metric_a"), ("env", "prod")]),
                (20.0, vec![("__name__", "metric_b"), ("env", "staging")]),
            ],
        );
    }

    /// UNLESS: only LHS series with *no* matching RHS series survive.
    /// The pushdown path is active (UNLESS is not LOR), so common LHS filters
    /// are injected into the RHS selector.  Because an RHS match *removes* the
    /// LHS series, the result must be empty when every LHS series has a
    /// corresponding RHS series.
    #[test]
    fn pushdown_unless_removes_lhs_series_matched_by_rhs() {
        // given
        let test_data: TestSampleData = vec![
            ("metric_a", vec![("env", "prod"), ("job", "api")], 0, 5.0),
            ("metric_b", vec![("env", "prod"), ("job", "api")], 1, 9.0),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let evaluator = Evaluator::new(&reader, QueryOptions::default());
        let lookback_delta = Duration::from_secs(300);

        // when
        let result = parse_and_evaluate(
            &evaluator,
            "metric_a unless metric_b",
            end_time,
            lookback_delta,
        )
        .expect("UNLESS with full overlap should return empty");

        // then – every LHS series was cancelled by its RHS counterpart
        assert!(
            result.is_empty(),
            "UNLESS should produce an empty result when all LHS series are matched; got {result:?}"
        );
    }

    /// UNLESS where only *some* LHS series have a matching RHS series:
    /// unmatched LHS series must survive.
    #[test]
    fn pushdown_unless_keeps_unmatched_lhs_series() {
        // given
        let test_data: TestSampleData = vec![
            // LHS – two series
            ("metric_a", vec![("env", "prod")], 0, 10.0),
            ("metric_a", vec![("env", "staging")], 1, 20.0),
            // RHS – only prod matches
            ("metric_b", vec![("env", "prod")], 2, 1.0),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let evaluator = Evaluator::new(&reader, QueryOptions::default());
        let lookback_delta = Duration::from_secs(300);

        // when
        let mut result = parse_and_evaluate(
            &evaluator,
            "metric_a unless metric_b",
            end_time,
            lookback_delta,
        )
        .expect("UNLESS partial overlap should succeed");

        // then – only the staging series (no RHS counterpart) survives
        sort_samples_by_labels(&mut result);
        assert_results_match(
            &result,
            &[(20.0, vec![("__name__", "metric_a"), ("env", "staging")])],
        );
    }

    /// When the LHS returns no series and the op is not OR, the result must be
    /// empty (nothing to match against, regardless of what RHS holds).
    #[test]
    fn pushdown_returns_empty_when_lhs_has_no_series() {
        // given – "ghost_metric" has no samples in the mock store
        let test_data: TestSampleData = vec![("real_metric", vec![("env", "prod")], 0, 42.0)];
        let (reader, end_time) = setup_mock_reader(test_data);
        let evaluator = Evaluator::new(&reader, QueryOptions::default());
        let lookback_delta = Duration::from_secs(300);

        // when – LHS is absent; the AND short-circuit path fires when AND is used,
        // but for multiplication the result is also empty because there is nothing
        // to match on the left side.
        let result = parse_and_evaluate(
            &evaluator,
            "ghost_metric * real_metric",
            end_time,
            lookback_delta,
        )
        .expect("empty LHS multiplication should return empty, not error");

        // then
        assert!(
            result.is_empty(),
            "Expected empty result when LHS is absent; got {result:?}"
        );
    }

    /// Comparison operator (==) with `bool` modifier: the pushdown path is taken
    /// for comparison ops, and the result value is 0.0 or 1.0 per PromQL bool semantics.
    #[test]
    fn pushdown_comparison_bool_modifier_produces_zero_one_values() {
        // given
        let test_data: TestSampleData = vec![
            ("threshold", vec![("env", "prod"), ("job", "api")], 0, 10.0),
            (
                "threshold",
                vec![("env", "prod"), ("job", "worker")],
                1,
                20.0,
            ),
            ("value", vec![("env", "prod"), ("job", "api")], 2, 10.0), // equal → 1
            ("value", vec![("env", "prod"), ("job", "worker")], 3, 99.0), // not equal → 0
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let evaluator = Evaluator::new(&reader, QueryOptions::default());
        let lookback_delta = Duration::from_secs(300);

        // when
        let mut result = parse_and_evaluate(
            &evaluator,
            "value == bool on(env, job) threshold",
            end_time,
            lookback_delta,
        )
        .expect("bool comparison via pushdown path should succeed");

        // then
        sort_samples_by_labels(&mut result);
        assert_results_match(
            &result,
            &[
                (1.0, vec![("env", "prod"), ("job", "api")]),
                (0.0, vec![("env", "prod"), ("job", "worker")]),
            ],
        );
    }

    /// group_left fan-out: one RHS series matched by multiple LHS series.
    /// The pushdown must not alter the fan-out semantics.
    #[test]
    fn pushdown_group_left_fanout_matches_multiple_lhs_to_single_rhs() {
        // given
        let test_data: TestSampleData = vec![
            // LHS – multiple instances, all env="prod"
            ("cpu", vec![("env", "prod"), ("instance", "i1")], 0, 10.0),
            ("cpu", vec![("env", "prod"), ("instance", "i2")], 1, 20.0),
            ("cpu", vec![("env", "prod"), ("instance", "i3")], 2, 30.0),
            // RHS – single series for the whole env
            ("scale", vec![("env", "prod")], 3, 2.0),
            // Decoy on RHS that should be suppressed by pushdown
            ("scale", vec![("env", "staging")], 4, 99.0),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let evaluator = Evaluator::new(&reader, QueryOptions::default());
        let lookback_delta = Duration::from_secs(300);

        // when – group_left() retains all many-side (cpu) labels including instance;
        // group_left(instance) would incorrectly try to copy instance from the one-side
        // (scale), which does not carry that label, collapsing all outputs to {env="prod"}.
        let mut result = parse_and_evaluate(
            &evaluator,
            "cpu * on(env) group_left() scale",
            end_time,
            lookback_delta,
        )
        .expect("group_left fan-out via pushdown path should succeed");

        // then – three output series, one per LHS instance, staging decoy absent
        sort_samples_by_labels(&mut result);
        assert_results_match(
            &result,
            &[
                (20.0, vec![("env", "prod"), ("instance", "i1")]),
                (40.0, vec![("env", "prod"), ("instance", "i2")]),
                (60.0, vec![("env", "prod"), ("instance", "i3")]),
            ],
        );
    }

    // ── Aggregation push-down hook ─────────────────────────────────────────
    // `evaluate_pushed_down_aggregate` offers each aggregation to the data
    // source before evaluating it here. These tests pin down which
    // aggregations are offered, what the source is told, and that its answer
    // is used verbatim.

    /// What the source was told about one offered aggregation: the operator,
    /// its parameter, the timestamp the input is selected at, and the timestamp
    /// the output is stamped with.
    type OfferedAggregation = (AggregationKind, Option<AggregationParam>, i64, i64);

    /// A reader that reports every aggregation it is offered as already
    /// evaluated, answering with a single sentinel sample. Records what it was
    /// asked so the request itself can be asserted on.
    struct PushdownReader {
        inner: MemorySeriesQuerier,
        offered: std::sync::Mutex<Vec<OfferedAggregation>>,
    }

    impl PushdownReader {
        const SENTINEL: f64 = -12345.0;

        fn new(inner: MemorySeriesQuerier) -> Self {
            Self {
                inner,
                offered: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn offered(&self) -> Vec<OfferedAggregation> {
            self.offered.lock().unwrap().clone()
        }
    }

    impl QueryReader for PushdownReader {
        fn query(
            &self,
            selector: &VectorSelector,
            timestamp: i64,
            options: QueryOptions,
        ) -> crate::promql::PromqlResult<Vec<crate::promql::InstantSample>> {
            self.inner.query(selector, timestamp, options)
        }

        fn query_range(
            &self,
            selector: &VectorSelector,
            start_ms: i64,
            end_ms: i64,
            options: QueryOptions,
        ) -> crate::promql::PromqlResult<Vec<crate::promql::RangeSample>> {
            self.inner.query_range(selector, start_ms, end_ms, options)
        }

        fn query_aggregation(
            &self,
            _selector: &VectorSelector,
            timestamp: i64,
            aggregation: &AggregationRequest,
            _options: QueryOptions,
        ) -> crate::promql::PromqlResult<AggregationOutcome> {
            self.offered.lock().unwrap().push((
                aggregation.kind,
                aggregation.param.clone(),
                timestamp,
                aggregation.eval_timestamp,
            ));
            Ok(AggregationOutcome::Aggregated(vec![
                crate::promql::InstantSample {
                    labels: Labels::from_pairs(&[("pushed", "down")]),
                    timestamp_ms: aggregation.eval_timestamp,
                    value: Self::SENTINEL,
                },
            ]))
        }
    }

    fn pushdown_reader() -> (PushdownReader, SystemTime) {
        let (inner, end_time) = setup_mock_reader(vec![
            ("metric", vec![("job", "a")], 0, 1.0),
            ("metric", vec![("job", "b")], 0, 2.0),
        ]);
        (PushdownReader::new(inner), end_time)
    }

    fn evaluate_with_pushdown(query: &str) -> (Vec<EvalSample>, Vec<OfferedAggregation>) {
        let (reader, end_time) = pushdown_reader();
        let evaluator = Evaluator::new(
            &reader,
            QueryOptions {
                timeout: None,
                ..QueryOptions::default()
            },
        );
        let result = parse_and_evaluate(&evaluator, query, end_time, Duration::from_secs(300))
            .expect("query should evaluate");
        (result, reader.offered())
    }

    /// A decomposable aggregation over a bare selector is handed to the source,
    /// and its answer is the result — the evaluator does not re-aggregate.
    #[test]
    fn should_use_source_evaluated_aggregation() {
        for query in [
            "sum by (job) (metric)",
            "sum(metric)",
            "avg without (job) (metric)",
            "stddev(metric)",
            "topk(1, metric)",
            "count_values(\"v\", metric)",
            // Parentheses around the operand do not change what is aggregated.
            "sum((metric))",
        ] {
            let (result, offered) = evaluate_with_pushdown(query);
            assert_eq!(offered.len(), 1, "{query}: offered once");
            assert_eq!(result.len(), 1, "{query}: source answer used verbatim");
            assert_eq!(result[0].value, PushdownReader::SENTINEL, "{query}");
        }
    }

    /// The operator, its evaluated parameter, and both timestamps reach the
    /// source. With an `offset`, the input is selected at the shifted
    /// timestamp while the output is stamped at the evaluation timestamp.
    #[test]
    fn should_describe_the_aggregation_to_the_source() {
        let (_, offered) = evaluate_with_pushdown("topk(3, metric)");
        let (kind, param, select_ts, eval_ts) = offered[0].clone();
        assert_eq!(kind, AggregationKind::Topk);
        assert_eq!(param, Some(AggregationParam::Scalar(3.0)));
        assert_eq!(select_ts, eval_ts, "no modifier: one timestamp");

        let (_, offered) = evaluate_with_pushdown("count_values(\"le\", metric)");
        let (kind, param, _, _) = offered[0].clone();
        assert_eq!(kind, AggregationKind::CountValues);
        assert_eq!(param, Some(AggregationParam::Label("le".to_string())));

        let (_, offered) = evaluate_with_pushdown("sum(metric offset 30s)");
        let (_, _, select_ts, eval_ts) = offered[0].clone();
        assert_eq!(
            select_ts,
            eval_ts - 30_000,
            "offset shifts selection, not the output stamp"
        );
    }

    /// What is never offered: quantile (no decomposable form) and anything
    /// whose operand is not a bare selector.
    #[test]
    fn should_not_push_down_ineligible_aggregations() {
        for query in [
            "quantile(0.5, metric)",
            "sum(rate(metric[5m]))",
            "sum(metric * 2)",
            "sum(sum by (job) (metric))",
        ] {
            let (_, offered) = evaluate_with_pushdown(query);
            assert!(
                offered
                    .iter()
                    .all(|(kind, ..)| *kind != AggregationKind::Quantile),
                "{query}: quantile must never be offered"
            );
            if query != "sum(sum by (job) (metric))" {
                assert!(offered.is_empty(), "{query}: nothing to push down");
            } else {
                // The inner aggregation is a bare selector and is offered; the
                // outer one aggregates its result and is not.
                assert_eq!(offered.len(), 1, "{query}: only the inner aggregation");
            }
        }
    }

    // ── Rollup semantics lock ───────────────────────────────────────────
    //
    // The gates that have to hold before rollup evaluation can be pushed to a
    // shard: which label set a rollup produces, and which `(series, step)`
    // pairs exist at all. Both are asserted exactly — a shard has to reproduce
    // them, not approximate them.

    /// Build a reader holding one series, `metric{job="api"}`, sampled every
    /// 10s from t=0 through t=300s with value `t/10`.
    fn rollup_reader() -> MemorySeriesQuerier {
        let mut builder = MockQueryReaderBuilder::new();
        let labels = create_labels("metric", vec![("job", "api")]);
        for i in 0..=30i64 {
            builder.add_sample(&labels, Sample::new(i * 10_000, i as f64));
        }
        builder.build()
    }

    fn eval_rollup(reader: &MemorySeriesQuerier, query: &str, at_ms: i64) -> Vec<EvalSample> {
        let evaluator = Evaluator::new(
            reader,
            QueryOptions {
                timeout: None,
                ..QueryOptions::default()
            },
        );
        parse_and_evaluate(
            &evaluator,
            query,
            UNIX_EPOCH + Duration::from_millis(at_ms as u64),
            Duration::from_secs(300),
        )
        .unwrap_or_else(|e| panic!("{query} should evaluate: {e}"))
    }

    /// Gate G2: a rollup's output label set. Every range-vector function strips
    /// `__name__`; `first_over_time` and `last_over_time` hand back an input
    /// sample and keep it. Non-name labels always survive.
    #[test]
    fn should_drop_metric_name_from_rollups() {
        let reader = rollup_reader();

        for query in [
            "sum_over_time(metric[1m])",
            "avg_over_time(metric[1m])",
            "count_over_time(metric[1m])",
            "min_over_time(metric[1m])",
            "max_over_time(metric[1m])",
            "stddev_over_time(metric[1m])",
            "present_over_time(metric[1m])",
            "quantile_over_time(0.5, metric[1m])",
            "ts_of_last_over_time(metric[1m])",
            "rate(metric[1m])",
            "irate(metric[1m])",
            "increase(metric[1m])",
            "delta(metric[1m])",
            "idelta(metric[1m])",
            "deriv(metric[1m])",
            "changes(metric[1m])",
            "resets(metric[1m])",
            "predict_linear(metric[1m], 60)",
        ] {
            let result = eval_rollup(&reader, query, 120_000);
            assert_eq!(result.len(), 1, "{query}: one output series");
            assert_eq!(
                result[0].labels.get(METRIC_NAME),
                None,
                "{query}: __name__ must be dropped"
            );
            assert_eq!(
                result[0].labels.get("job"),
                Some("api"),
                "{query}: other labels are kept"
            );
        }

        for query in ["first_over_time(metric[1m])", "last_over_time(metric[1m])"] {
            let result = eval_rollup(&reader, query, 120_000);
            assert_eq!(result.len(), 1, "{query}: one output series");
            assert_eq!(
                result[0].labels.get(METRIC_NAME),
                Some("metric"),
                "{query}: __name__ must be kept"
            );
            assert_eq!(
                result[0].labels.get("job"),
                Some("api"),
                "{query}: job kept"
            );
        }
    }

    /// Gate G1: a rollup evaluates the window the matrix selector loaded, once.
    /// Nothing about the enclosing query's step grid may leak into which samples
    /// it reduces, so the same instant must give the same answer whether or not
    /// a step is set.
    #[test]
    fn should_evaluate_rollup_over_the_selected_window_only() {
        let reader = rollup_reader();

        // Window (60s, 120s] holds the samples at 70s..120s: values 7..12.
        let expected: &[(&str, f64)] = &[
            ("count_over_time(metric[1m])", 6.0),
            ("sum_over_time(metric[1m])", 57.0),
            ("min_over_time(metric[1m])", 7.0),
            ("max_over_time(metric[1m])", 12.0),
            ("avg_over_time(metric[1m])", 9.5),
            ("last_over_time(metric[1m])", 12.0),
            ("first_over_time(metric[1m])", 7.0),
        ];

        for (query, want) in expected {
            let result = eval_rollup(&reader, query, 120_000);
            assert_eq!(result.len(), 1, "{query}: one output series");
            assert_eq!(result[0].value, *want, "{query}");
            assert_eq!(
                result[0].timestamp_ms, 120_000,
                "{query}: stamped at the evaluation instant"
            );
        }
    }

    /// Gate G1, continued: `@` and `offset` move the window the rollup reduces
    /// but not the timestamp its output carries. A shard is told the resolved
    /// window and must not re-derive either.
    #[test]
    fn should_report_shifted_rollups_at_the_query_instant() {
        let reader = rollup_reader();

        // At t=300s, `offset 3m` selects the window (60s, 120s] — values 7..12.
        for query in [
            "sum_over_time(metric[1m] offset 3m)",
            "sum_over_time(metric[1m] @ 120)",
        ] {
            let result = eval_rollup(&reader, query, 300_000);
            assert_eq!(result.len(), 1, "{query}: one output series");
            assert_eq!(result[0].value, 57.0, "{query}: shifted window");
            assert_eq!(
                result[0].timestamp_ms, 300_000,
                "{query}: reported at the query instant, not the shifted one"
            );
        }
    }

    /// Gate G3: an empty window produces no sample. The series is absent from
    /// the result rather than present with NaN, which is the distinction the
    /// pushed-down transport has to preserve.
    #[test]
    fn should_omit_series_whose_window_is_empty() {
        let reader = rollup_reader();

        // The series ends at t=300s, so a window past it holds nothing.
        for query in [
            "count_over_time(metric[1m])",
            "sum_over_time(metric[1m])",
            "present_over_time(metric[1m])",
            "last_over_time(metric[1m])",
        ] {
            let result = eval_rollup(&reader, query, 600_000);
            assert!(
                result.is_empty(),
                "{query}: an empty window emits nothing, got {result:?}"
            );
        }
    }

    /// Gate G3, continued: a NaN *value* is a result. It must survive as a
    /// sample rather than being mistaken for an absent one.
    #[test]
    fn should_keep_nan_valued_rollup_results() {
        let mut builder = MockQueryReaderBuilder::new();
        let labels = create_labels("metric", vec![("job", "api")]);
        builder.add_sample(&labels, Sample::new(10_000, f64::NAN));
        let reader = builder.build();

        for query in ["sum_over_time(metric[1m])", "last_over_time(metric[1m])"] {
            let result = eval_rollup(&reader, query, 10_000);
            assert_eq!(result.len(), 1, "{query}: the NaN sample is a result");
            assert!(result[0].value.is_nan(), "{query}: value stays NaN");
        }

        // …and counting it sees one sample, not zero.
        let result = eval_rollup(&reader, "count_over_time(metric[1m])", 10_000);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 1.0, "a NaN sample still counts");
    }

    // ── Rollup push-down ────────────────────────────────────────────────
    //
    // The coordinator side of `QueryReader::query_rollup`: which calls are
    // offered to the source, what the source is told about the window, and that
    // whichever side reduces, the answer is the same.

    /// What the source was told about one offered rollup.
    #[derive(Debug, Clone, PartialEq)]
    struct OfferedRollup {
        kind: RollupKind,
        range_ms: i64,
        step_ms: i64,
        range_end_ms: i64,
        param: Option<f64>,
    }

    /// A reader that records every rollup it is offered. `answer` decides
    /// whether it reduces the windows itself (the cluster's role) or hands them
    /// back raw (the single-node fallback).
    struct RollupPushdownReader {
        inner: MemorySeriesQuerier,
        answer: RollupAnswer,
        offered: std::sync::Mutex<Vec<OfferedRollup>>,
    }

    #[derive(Clone, Copy, PartialEq)]
    enum RollupAnswer {
        /// Do everything the request asks — reduce, and group when it carries an
        /// aggregation — as a current shard would.
        Rolled,
        /// Reduce but do not group, as a peer that predates fusion does.
        Reduced,
        /// Return the windows unreduced, as a single node does.
        Raw,
        /// Refuse, as a node without push-down does.
        Unsupported,
    }

    impl RollupPushdownReader {
        fn new(inner: MemorySeriesQuerier, answer: RollupAnswer) -> Self {
            Self {
                inner,
                answer,
                offered: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn offered(&self) -> Vec<OfferedRollup> {
            self.offered.lock().unwrap().clone()
        }
    }

    impl QueryReader for RollupPushdownReader {
        fn query(
            &self,
            selector: &VectorSelector,
            timestamp: i64,
            options: QueryOptions,
        ) -> crate::promql::PromqlResult<Vec<crate::promql::InstantSample>> {
            self.inner.query(selector, timestamp, options)
        }

        fn query_range(
            &self,
            selector: &VectorSelector,
            start_ms: i64,
            end_ms: i64,
            options: QueryOptions,
        ) -> crate::promql::PromqlResult<Vec<crate::promql::RangeSample>> {
            self.inner.query_range(selector, start_ms, end_ms, options)
        }

        fn query_rollup(
            &self,
            selector: &VectorSelector,
            rollup: &RollupRequest,
            options: QueryOptions,
        ) -> crate::promql::PromqlResult<RollupOutcome> {
            self.offered.lock().unwrap().push(OfferedRollup {
                kind: rollup.kind,
                range_ms: rollup.range_ms,
                step_ms: rollup.step_ms,
                range_end_ms: rollup.range_end_ms,
                param: rollup.param,
            });

            if self.answer == RollupAnswer::Unsupported {
                return Ok(RollupOutcome::Unsupported);
            }

            let raw = self.inner.query_rollup(selector, rollup, options)?;
            let RollupOutcome::Raw(windows) = raw else {
                panic!("the in-memory reader always answers Raw");
            };
            if self.answer == RollupAnswer::Raw {
                return Ok(RollupOutcome::Raw(windows));
            }

            // Reduce here, exactly as a shard does.
            let ends = rollup.window_ends();
            let reduced: Vec<crate::promql::RangeSample> = windows
                .into_iter()
                .filter_map(|s| {
                    let points = rollup.kind.eval_windows(
                        &s.samples,
                        rollup.range_ms,
                        rollup.lookback_delta_ms,
                        rollup.step_ms,
                        ends.iter().copied(),
                        rollup.param,
                    );
                    (!points.is_empty()).then_some(crate::promql::RangeSample {
                        labels: s.labels,
                        samples: points,
                    })
                })
                .collect();

            if self.answer == RollupAnswer::Reduced {
                return Ok(RollupOutcome::Reduced(reduced));
            }

            // …and group, when the request asks for it.
            let Some(aggregation) = rollup.aggregation.as_ref() else {
                return Ok(RollupOutcome::Rolled(reduced));
            };
            let mut partials = crate::promql::exec::partial_aggregation::SteppedPartialGroups::new(
                aggregation.kind,
            );
            partials.accumulate(aggregation.modifier.as_ref(), reduced);
            Ok(RollupOutcome::Rolled(partials.finalize()))
        }
    }

    /// Evaluate `query` at `at_ms` against a reader that answers rollups with
    /// `answer`, returning the result and what it was offered.
    fn evaluate_rollup_pushdown(
        query: &str,
        at_ms: i64,
        answer: RollupAnswer,
    ) -> (Vec<EvalSample>, Vec<OfferedRollup>) {
        evaluate_rollup_pushdown_on(rollup_reader(), query, at_ms, answer)
    }

    fn evaluate_rollup_pushdown_on(
        inner: MemorySeriesQuerier,
        query: &str,
        at_ms: i64,
        answer: RollupAnswer,
    ) -> (Vec<EvalSample>, Vec<OfferedRollup>) {
        let reader = RollupPushdownReader::new(inner, answer);
        let evaluator = Evaluator::new(
            &reader,
            QueryOptions {
                timeout: None,
                ..QueryOptions::default()
            },
        );
        let result = parse_and_evaluate(
            &evaluator,
            query,
            UNIX_EPOCH + Duration::from_millis(at_ms as u64),
            Duration::from_secs(300),
        )
        .unwrap_or_else(|e| panic!("{query} should evaluate: {e}"));
        (result, reader.offered())
    }

    /// Only a bare `f(selector[range])` whose function can be evaluated from one
    /// series' window is offered to the source. Everything else stays local.
    #[test]
    fn should_offer_only_pushable_rollups() {
        // Every kind is offered, with the window and function it names.
        for kind in RollupKind::all() {
            let query = rollup_query(kind, "1m");
            let (_, offered) = evaluate_rollup_pushdown(&query, 120_000, RollupAnswer::Rolled);
            assert_eq!(offered.len(), 1, "{query}: offered once");
            assert_eq!(offered[0].kind, kind, "{query}");
            assert_eq!(
                offered[0].range_ms, 60_000,
                "{query}: resolved window width"
            );
            assert_eq!(offered[0].step_ms, 0, "{query}: single evaluation");
        }

        // Parentheses around the selector do not change what is reduced.
        let (_, offered) =
            evaluate_rollup_pushdown("sum_over_time((metric[1m]))", 120_000, RollupAnswer::Rolled);
        assert_eq!(offered.len(), 1, "parenthesized selector is still pushable");

        for query in [
            // Not window-local: the answer depends on absence across the cluster.
            "absent_over_time(metric[1m])",
            // Predicts relative to the query's evaluation timestamp, which
            // `@`/`offset` divorce from the window end a shard is told.
            "predict_linear(metric[1m], 60)",
            // Two scalar parameters; the request carries one.
            "double_exponential_smoothing(metric[1m], 0.5, 0.5)",
            // A subquery brings its own step grid.
            "sum_over_time(metric[2m:30s])",
            // Not a range-vector function at all.
            "abs(metric)",
        ] {
            let (_, offered) = evaluate_rollup_pushdown(query, 120_000, RollupAnswer::Rolled);
            assert!(offered.is_empty(), "{query}: must not be pushed down");
        }
    }

    /// A subquery's *inner* rollup is a bare matrix selector evaluated at an
    /// instant, once per subquery step, so each step is pushed down on its own.
    /// The outer rollup, whose argument is the subquery, stays local.
    #[test]
    fn should_push_down_a_subquerys_inner_rollup_per_step() {
        let query = "max_over_time(sum_over_time(metric[1m])[2m:30s])";
        let (pushed, offered) = evaluate_rollup_pushdown(query, 120_000, RollupAnswer::Rolled);

        // Subquery range (0, 2m] at a 30s step: four evaluations, each its own
        // single-window request.
        assert_eq!(offered.len(), 4, "one offer per subquery step");
        assert!(
            offered
                .iter()
                .all(|o| o.kind == RollupKind::SumOverTime && o.step_ms == 0),
            "each step is a single evaluation of the inner rollup"
        );
        // Subquery steps are evaluated in parallel, so compare the set of
        // windows rather than the order they were requested in.
        let mut ends: Vec<i64> = offered.iter().map(|o| o.range_end_ms).collect();
        ends.sort_unstable();
        assert_eq!(
            ends,
            vec![30_000, 60_000, 90_000, 120_000],
            "one window per subquery step"
        );

        let local = eval_rollup(&rollup_reader(), query, 120_000);
        assert_eq!(local.len(), 1);
        assert_eq!(pushed.len(), 1);
        assert_eq!(pushed[0].value, local[0].value, "same answer either way");
    }

    /// The scalar parameter may sit on either side of the matrix argument:
    /// `quantile_over_time` takes phi first, `predict_linear` takes the matrix
    /// first. Neither ordering may disqualify a call from push-down.
    ///
    /// Neither function is in the pushable set yet, so this drives the shape
    /// check through `RollupKind::from_function_name` directly.
    #[test]
    fn should_accept_a_scalar_parameter_on_either_side_of_the_matrix() {
        let reader = RollupPushdownReader::new(rollup_reader(), RollupAnswer::Rolled);
        let evaluator = Evaluator::new(
            &reader,
            QueryOptions {
                timeout: None,
                ..QueryOptions::default()
            },
        );
        let ctx = crate::promql::EvalContext::for_vector_selector(120_000, 300_000);

        for (query, want_param) in [
            ("quantile_over_time(0.9, metric[1m])", Some(0.9)),
            ("predict_linear(metric[1m], 60)", Some(60.0)),
            ("sum_over_time(metric[1m])", None),
        ] {
            let Expr::Call(call) = promql_parser::parser::parse(query).unwrap() else {
                panic!("{query} should parse to a call");
            };
            let args = evaluator
                .rollup_arguments(&call, &ctx)
                .unwrap_or_else(|e| panic!("{query}: {e}"));
            let (matrix, param) = args.unwrap_or_else(|| panic!("{query}: should be pushable"));
            assert_eq!(matrix.range.as_millis() as i64, 60_000, "{query}");
            assert_eq!(param, want_param, "{query}");
        }

        // A subquery argument has no matrix selector to push.
        let Expr::Call(call) =
            promql_parser::parser::parse("sum_over_time(metric[2m:30s])").unwrap()
        else {
            panic!("should parse to a call");
        };
        assert!(evaluator.rollup_arguments(&call, &ctx).unwrap().is_none());
    }

    /// `@` and `offset` are resolved by the coordinator: the source is handed a
    /// window end and never a modifier to re-derive.
    #[test]
    fn should_resolve_modifiers_before_offering_a_rollup() {
        for (query, want_end) in [
            ("sum_over_time(metric[1m])", 300_000),
            ("sum_over_time(metric[1m] offset 3m)", 120_000),
            ("sum_over_time(metric[1m] @ 120)", 120_000),
        ] {
            let (_, offered) = evaluate_rollup_pushdown(query, 300_000, RollupAnswer::Rolled);
            assert_eq!(offered.len(), 1, "{query}");
            assert_eq!(offered[0].range_end_ms, want_end, "{query}");
        }
    }

    /// Whoever reduces, the answer is the same: a source that rolled up, a
    /// source that handed back raw windows, and a source with no push-down at
    /// all must all produce what local evaluation produces.
    #[test]
    fn should_match_local_evaluation_however_the_source_answers() {
        for (dataset, reader) in rollup_datasets() {
            for kind in RollupKind::all() {
                for range in ["10s", "1m", "3m"] {
                    let query = rollup_query(kind, range);
                    let local = eval_rollup(&reader(), &query, 120_000);

                    for answer in [
                        RollupAnswer::Rolled,
                        RollupAnswer::Reduced,
                        RollupAnswer::Raw,
                        RollupAnswer::Unsupported,
                    ] {
                        let (pushed, offered) =
                            evaluate_rollup_pushdown_on(reader(), &query, 120_000, answer);
                        assert_eq!(offered.len(), 1, "{dataset}/{query}: offered once");
                        assert_eq!(
                            rendered_samples(local.clone()),
                            rendered_samples(pushed),
                            "{dataset}/{query}: pushed-down result must equal the local one"
                        );
                    }
                }
            }
        }
    }

    /// The same conformance over a step grid, where consecutive windows overlap
    /// and the source answers once for all of them.
    #[test]
    fn should_match_local_evaluation_over_a_grid_for_every_kind() {
        for (dataset, reader) in rollup_datasets() {
            for kind in RollupKind::all() {
                for range in ["10s", "1m", "3m"] {
                    let query = rollup_query(kind, range);
                    let (local, offered) = evaluate_range_with_pushdown_on(
                        reader(),
                        &query,
                        0,
                        300_000,
                        30_000,
                        RollupAnswer::Unsupported,
                    );
                    assert_eq!(offered.len(), 1, "{dataset}/{query}: offered once");
                    let local = rendered_steps(local);

                    for answer in [
                        RollupAnswer::Rolled,
                        RollupAnswer::Reduced,
                        RollupAnswer::Raw,
                    ] {
                        let (pushed, _) = evaluate_range_with_pushdown_on(
                            reader(),
                            &query,
                            0,
                            300_000,
                            30_000,
                            answer,
                        );
                        assert_eq!(
                            local,
                            rendered_steps(pushed),
                            "{dataset}/{query}: grid result must equal step-by-step evaluation"
                        );
                    }
                }
            }
        }
    }

    /// Modifier handling has to hold for every kind too, not just the ones the
    /// hand-written cases happen to name.
    #[test]
    fn should_match_local_evaluation_under_modifiers_for_every_kind() {
        for kind in RollupKind::all() {
            for suffix in ["offset 2m", "@ 120", "@ start()", "@ end()"] {
                let query = rollup_query_with(kind, "1m", suffix);
                let (local, _) = evaluate_range_with_pushdown_on(
                    rollup_reader(),
                    &query,
                    60_000,
                    240_000,
                    60_000,
                    RollupAnswer::Unsupported,
                );
                let (pushed, _) = evaluate_range_with_pushdown_on(
                    rollup_reader(),
                    &query,
                    60_000,
                    240_000,
                    60_000,
                    RollupAnswer::Rolled,
                );
                assert_eq!(
                    rendered_steps(local),
                    rendered_steps(pushed),
                    "{query}: modifier parity"
                );
            }
        }
    }

    /// The label rule is applied once, on the coordinator, so a pushed-down
    /// rollup drops `__name__` exactly where a local one does.
    #[test]
    fn should_apply_the_label_rule_to_pushed_down_rollups() {
        for answer in [
            RollupAnswer::Rolled,
            RollupAnswer::Reduced,
            RollupAnswer::Raw,
        ] {
            let (result, _) =
                evaluate_rollup_pushdown("sum_over_time(metric[1m])", 120_000, answer);
            assert_eq!(result.len(), 1);
            assert_eq!(result[0].labels.get(METRIC_NAME), None, "name dropped");
            assert_eq!(result[0].labels.get("job"), Some("api"), "job kept");

            let (result, _) =
                evaluate_rollup_pushdown("last_over_time(metric[1m])", 120_000, answer);
            assert_eq!(result.len(), 1);
            assert_eq!(
                result[0].labels.get(METRIC_NAME),
                Some("metric"),
                "name kept"
            );
        }
    }

    /// A range query's steps stay local for now: pushing the grid down is a
    /// later phase, and until then the two paths must not both own the grid.
    #[test]
    fn should_not_push_down_rollups_in_range_queries() {
        let reader = RollupPushdownReader::new(rollup_reader(), RollupAnswer::Rolled);
        let evaluator = Evaluator::new(
            &reader,
            QueryOptions {
                timeout: None,
                ..QueryOptions::default()
            },
        );

        let expr = promql_parser::parser::parse("sum_over_time(metric[1m])").unwrap();
        let ctx = crate::promql::EvalContext {
            query_start: 60_000,
            query_end: 180_000,
            evaluation_ts: 120_000,
            step_ms: 30_000,
            lookback_delta_ms: 300_000,
        };
        evaluator.evaluate_with_context(&expr, ctx).unwrap();

        assert!(
            reader.offered().is_empty(),
            "a range-query step must not be pushed down yet"
        );
    }

    // ── Whole-grid rollup push-down ─────────────────────────────────────
    //
    // A range query resolves its rollups once, for every step, before the step
    // loop runs. These cover what that has to preserve: the number of requests,
    // which steps exist, and agreement with step-by-step local evaluation.

    /// Drive a range query the way `evaluate_range` does — preload, then one
    /// evaluation per step — returning `(step, samples)` pairs and what the
    /// source was offered.
    fn evaluate_range_with_pushdown(
        query: &str,
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
        answer: RollupAnswer,
    ) -> (Vec<(i64, Vec<EvalSample>)>, Vec<OfferedRollup>) {
        evaluate_range_with_pushdown_on(rollup_reader(), query, start_ms, end_ms, step_ms, answer)
    }

    fn evaluate_range_with_pushdown_on(
        inner: MemorySeriesQuerier,
        query: &str,
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
        answer: RollupAnswer,
    ) -> (Vec<(i64, Vec<EvalSample>)>, Vec<OfferedRollup>) {
        let reader = RollupPushdownReader::new(inner, answer);
        let evaluator = Evaluator::new(
            &reader,
            QueryOptions {
                timeout: None,
                ..QueryOptions::default()
            },
        );
        let expr = promql_parser::parser::parse(query).unwrap();
        let base = crate::promql::EvalContext {
            query_start: start_ms,
            query_end: end_ms,
            evaluation_ts: start_ms,
            step_ms,
            lookback_delta_ms: 300_000,
        };

        evaluator
            .preload_for_range(&expr, &base)
            .unwrap_or_else(|e| panic!("{query}: preload failed: {e}"));

        let mut steps = Vec::new();
        for step_ts in (start_ms..=end_ms).step_by(step_ms as usize) {
            let ctx = crate::promql::EvalContext {
                evaluation_ts: step_ts,
                ..base
            };
            let result = evaluator
                .evaluate_with_context(&expr, ctx)
                .unwrap_or_else(|e| panic!("{query} at {step_ts}: {e}"));
            let samples = result.expect_instant_vector("instant vector per step");
            steps.push((step_ts, samples));
        }

        (steps, reader.offered())
    }

    /// Results as sorted `(labels, timestamp, value)` triples for an instant
    /// evaluation. `{:?}` on the float so NaN compares equal to NaN — a rollup
    /// may legitimately produce one, and both paths must produce the same one.
    fn rendered_samples(samples: Vec<EvalSample>) -> Vec<(String, i64, String)> {
        let mut out: Vec<(String, i64, String)> = samples
            .into_iter()
            .map(|s| {
                (
                    s.labels.to_string(),
                    s.timestamp_ms,
                    format!("{:?}", s.value),
                )
            })
            .collect();
        out.sort();
        out
    }

    /// The query for a kind, with its parameter supplied where it takes one.
    fn rollup_query(kind: RollupKind, range: &str) -> String {
        rollup_query_with(kind, range, "")
    }

    fn rollup_query_with(kind: RollupKind, range: &str, suffix: &str) -> String {
        let selector = format!(
            "metric[{range}]{}{suffix}",
            if suffix.is_empty() { "" } else { " " }
        );
        match kind {
            // A literal parameter: the grid path requires one, because a
            // parameter that varied per step could not be one request.
            RollupKind::QuantileOverTime => format!("quantile_over_time(0.9, {selector})"),
            other => format!("{}({selector})", other.function_name()),
        }
    }

    /// A named dataset for the conformance suite, built fresh per case so the
    /// pushed-down and local runs cannot share mutated state.
    type RollupDataset = (&'static str, fn() -> MemorySeriesQuerier);

    /// Compare two runs' `(step, labels, value)` triples, exactly on shape and
    /// within a relative tolerance on value.
    ///
    /// Shape — which `(group, step)` pairs exist, and with which labels — is
    /// exact and must stay so. Values are not: a fused aggregation merges
    /// per-shard partials, so the summation order differs from a single-node
    /// reduction and the last bits with it. `partial_aggregation` documents that
    /// bit-exact parity is not achievable there; demanding it would be demanding
    /// something false.
    fn assert_steps_near(
        local: Vec<(i64, String, String)>,
        pushed: Vec<(i64, String, String)>,
        what: &str,
    ) {
        let shape = |rows: &[(i64, String, String)]| -> Vec<(i64, String)> {
            rows.iter().map(|(s, l, _)| (*s, l.clone())).collect()
        };
        assert_eq!(shape(&local), shape(&pushed), "{what}: shape");

        for ((step, labels, want), (_, _, got)) in local.iter().zip(&pushed) {
            let (want_f, got_f) = (want.parse::<f64>(), got.parse::<f64>());
            match (want_f, got_f) {
                (Ok(a), Ok(b)) => assert!(
                    (a - b).abs() <= 1e-12 * a.abs().max(b.abs()).max(1.0),
                    "{what}: step {step} {labels}: expected {want}, got {got}"
                ),
                // NaN and the infinities render as text; they must match exactly.
                _ => assert_eq!(want, got, "{what}: step {step} {labels}"),
            }
        }
    }

    /// The datasets the conformance suite runs against: a dense monotonic
    /// counter, and one with gaps and a counter reset so the reset-handling and
    /// empty-window paths are exercised too.
    fn rollup_datasets() -> Vec<RollupDataset> {
        vec![("dense", rollup_reader), ("gappy", gappy_rollup_reader)]
    }

    /// A series with a gap between 60s and 200s and a counter reset at 220s.
    fn gappy_rollup_reader() -> MemorySeriesQuerier {
        let mut builder = MockQueryReaderBuilder::new();
        let labels = create_labels("metric", vec![("job", "api")]);
        for (ts, value) in [
            (0i64, 5.0f64),
            (20_000, 7.0),
            (40_000, 9.0),
            (60_000, 9.0),
            // …gap…
            (200_000, 20.0),
            (210_000, 24.0),
            // counter reset
            (220_000, 3.0),
            (240_000, 8.0),
            (300_000, 8.0),
        ] {
            builder.add_sample(&labels, Sample::new(ts, value));
        }
        builder.build()
    }

    /// Results as `(step, labels, value)` triples, sorted, so a pushed-down run
    /// and a local one compare exactly — including *which* steps produced a
    /// sample at all.
    fn rendered_steps(steps: Vec<(i64, Vec<EvalSample>)>) -> Vec<(i64, String, String)> {
        let mut out: Vec<(i64, String, String)> = steps
            .into_iter()
            .flat_map(|(step, samples)| {
                samples
                    .into_iter()
                    .map(move |s| (step, s.labels.to_string(), format!("{:?}", s.value)))
            })
            .collect();
        out.sort();
        out
    }

    /// The whole point of the grid phase: one request covering every step, not
    /// one request per step.
    #[test]
    fn should_issue_one_request_for_the_whole_step_grid() {
        // 0..5m at 30s is eleven steps.
        let (steps, offered) = evaluate_range_with_pushdown(
            "sum_over_time(metric[1m])",
            0,
            300_000,
            30_000,
            RollupAnswer::Rolled,
        );

        assert_eq!(steps.len(), 11, "eleven steps evaluated");
        assert_eq!(offered.len(), 1, "one request for all of them");
        assert_eq!(offered[0].kind, RollupKind::SumOverTime);
        assert_eq!(offered[0].step_ms, 30_000, "the grid step is shipped");
        assert_eq!(offered[0].range_ms, 60_000);
    }

    /// Two different rollups over the same series are two requests; the same
    /// rollup written twice is one.
    #[test]
    fn should_deduplicate_grid_requests() {
        let (_, offered) = evaluate_range_with_pushdown(
            "sum_over_time(metric[1m]) + sum_over_time(metric[1m])",
            0,
            120_000,
            30_000,
            RollupAnswer::Rolled,
        );
        assert_eq!(offered.len(), 1, "the same rollup is requested once");

        let (_, offered) = evaluate_range_with_pushdown(
            "sum_over_time(metric[1m]) + count_over_time(metric[1m])",
            0,
            120_000,
            30_000,
            RollupAnswer::Rolled,
        );
        assert_eq!(
            offered.len(),
            2,
            "different functions are different requests"
        );

        let (_, offered) = evaluate_range_with_pushdown(
            "sum_over_time(metric[1m]) + sum_over_time(metric[2m])",
            0,
            120_000,
            30_000,
            RollupAnswer::Rolled,
        );
        assert_eq!(offered.len(), 2, "different windows are different requests");
    }

    /// Whoever reduces, every step must hold the same value — and the same
    /// steps must exist. This is the parity that lets the grid phase be turned
    /// on at all.
    #[test]
    fn should_match_step_by_step_evaluation_over_a_grid() {
        for query in [
            "sum_over_time(metric[1m])",
            "count_over_time(metric[1m])",
            "last_over_time(metric[1m])",
            // Range wider than the step: consecutive windows overlap heavily,
            // which is the shape the push-down exists for.
            "sum_over_time(metric[3m])",
            // Range narrower than the step: windows have gaps between them.
            "count_over_time(metric[10s])",
        ] {
            // `Unsupported` is the step-by-step local path.
            let (local, offered) =
                evaluate_range_with_pushdown(query, 0, 300_000, 30_000, RollupAnswer::Unsupported);
            assert_eq!(offered.len(), 1, "{query}: offered, then declined");
            let local = rendered_steps(local);

            for answer in [RollupAnswer::Rolled, RollupAnswer::Raw] {
                let (pushed, _) = evaluate_range_with_pushdown(query, 0, 300_000, 30_000, answer);
                assert_eq!(
                    local,
                    rendered_steps(pushed),
                    "{query}: grid result must equal step-by-step evaluation"
                );
            }
        }
    }

    /// The sparse shape survives the grid: a step whose window held no samples
    /// is absent from the result rather than present with NaN.
    #[test]
    fn should_preserve_sparse_shape_across_the_grid() {
        // `rollup_reader` stops at t=300s, so windows past it are empty.
        let (steps, _) = evaluate_range_with_pushdown(
            "count_over_time(metric[30s])",
            240_000,
            420_000,
            60_000,
            RollupAnswer::Rolled,
        );

        let present: Vec<i64> = steps
            .iter()
            .filter(|(_, samples)| !samples.is_empty())
            .map(|(step, _)| *step)
            .collect();
        assert_eq!(
            present,
            vec![240_000, 300_000],
            "steps past the end of the series produce no sample at all"
        );

        // …and the local path agrees about which steps those are.
        let (local, _) = evaluate_range_with_pushdown(
            "count_over_time(metric[30s])",
            240_000,
            420_000,
            60_000,
            RollupAnswer::Unsupported,
        );
        assert_eq!(rendered_steps(steps), rendered_steps(local));
    }

    /// `offset` shifts every window uniformly, so the grid stays a progression;
    /// `@` pins every step to one window, which the coordinator broadcasts.
    /// Neither modifier ever reaches the source.
    #[test]
    fn should_resolve_grid_modifiers_before_the_request() {
        let (steps, offered) = evaluate_range_with_pushdown(
            "sum_over_time(metric[1m] offset 1m)",
            120_000,
            240_000,
            60_000,
            RollupAnswer::Rolled,
        );
        assert_eq!(offered.len(), 1);
        // Windows end a minute before each step: 60s, 120s, 180s.
        assert_eq!(offered[0].range_end_ms, 180_000, "last window end, shifted");
        let (local, _) = evaluate_range_with_pushdown(
            "sum_over_time(metric[1m] offset 1m)",
            120_000,
            240_000,
            60_000,
            RollupAnswer::Unsupported,
        );
        assert_eq!(
            rendered_steps(steps),
            rendered_steps(local),
            "offset parity"
        );

        // `@` collapses the grid onto a single window, repeated at every step.
        let (steps, offered) = evaluate_range_with_pushdown(
            "sum_over_time(metric[1m] @ 120)",
            120_000,
            240_000,
            60_000,
            RollupAnswer::Rolled,
        );
        assert_eq!(offered.len(), 1);
        assert_eq!(offered[0].range_end_ms, 120_000, "one pinned window");
        let values: Vec<String> = steps
            .iter()
            .flat_map(|(_, s)| s.iter().map(|s| format!("{:?}", s.value)))
            .collect();
        assert_eq!(values.len(), 3, "every step reports the pinned window");
        assert!(
            values.windows(2).all(|w| w[0] == w[1]),
            "and reports the same value: {values:?}"
        );
        let (local, _) = evaluate_range_with_pushdown(
            "sum_over_time(metric[1m] @ 120)",
            120_000,
            240_000,
            60_000,
            RollupAnswer::Unsupported,
        );
        assert_eq!(rendered_steps(steps), rendered_steps(local), "@ parity");
    }

    /// A rollup inside a subquery keeps its own grid: the outer preload must not
    /// claim it, or every subquery step would read the outer query's windows.
    #[test]
    fn should_not_preload_rollups_inside_a_subquery() {
        let (_, offered) = evaluate_range_with_pushdown(
            "max_over_time(sum_over_time(metric[1m])[2m:1m])",
            120_000,
            240_000,
            60_000,
            RollupAnswer::Rolled,
        );

        // The inner rollup is still pushed down, but one instant at a time
        // against the subquery's grid — never as one outer-grid request.
        assert!(
            offered.iter().all(|o| o.step_ms == 0),
            "subquery steps are single evaluations, got {offered:?}"
        );
    }

    // ── Fusion with an outer aggregation ────────────────────────────────
    //
    // `sum by (job) (rate(m[5m]))` is one request: the source reduces each
    // series' windows and groups the result, so what comes back is one value per
    // group per step rather than one per series.

    /// A reader with several series per group, so fusion has something to fold.
    fn grouped_rollup_reader() -> MemorySeriesQuerier {
        let mut builder = MockQueryReaderBuilder::new();
        for (job, instance, base) in [
            ("api", "0", 0.0f64),
            ("api", "1", 100.0),
            ("db", "0", 1000.0),
        ] {
            let labels = create_labels("metric", vec![("job", job), ("instance", instance)]);
            for i in 0..=30i64 {
                builder.add_sample(&labels, Sample::new(i * 10_000, base + i as f64));
            }
        }
        builder.build()
    }

    /// The fused form must equal the unfused one: same values, same groups, same
    /// steps — whether the source grouped, only reduced, or did neither.
    #[test]
    fn should_match_local_evaluation_for_fused_aggregations() {
        let aggregations = [
            "sum", "avg", "min", "max", "count", "group", "stddev", "stdvar",
        ];
        for agg in aggregations {
            for grouping in [
                "",
                " by (job)",
                " by (job, instance)",
                " without (instance)",
            ] {
                for inner in ["rate(metric[1m])", "sum_over_time(metric[1m])"] {
                    let query = format!("{agg}{grouping} ({inner})");

                    let (local, _) = evaluate_range_with_pushdown_on(
                        grouped_rollup_reader(),
                        &query,
                        0,
                        300_000,
                        30_000,
                        RollupAnswer::Unsupported,
                    );
                    let local = rendered_steps(local);

                    for answer in [
                        RollupAnswer::Rolled,
                        RollupAnswer::Reduced,
                        RollupAnswer::Raw,
                    ] {
                        let (pushed, _) = evaluate_range_with_pushdown_on(
                            grouped_rollup_reader(),
                            &query,
                            0,
                            300_000,
                            30_000,
                            answer,
                        );
                        assert_steps_near(
                            local.clone(),
                            rendered_steps(pushed),
                            &format!("{query} (grid)"),
                        );
                    }

                    // …and the same at an instant.
                    let instant_local = eval_rollup(&grouped_rollup_reader(), &query, 120_000);
                    for answer in [
                        RollupAnswer::Rolled,
                        RollupAnswer::Reduced,
                        RollupAnswer::Raw,
                    ] {
                        let (pushed, _) = evaluate_rollup_pushdown_on(
                            grouped_rollup_reader(),
                            &query,
                            120_000,
                            answer,
                        );
                        assert_steps_near(
                            rendered_samples(instant_local.clone())
                                .into_iter()
                                .map(|(l, t, v)| (t, l, v))
                                .collect(),
                            rendered_samples(pushed)
                                .into_iter()
                                .map(|(l, t, v)| (t, l, v))
                                .collect(),
                            &format!("{query} (instant)"),
                        );
                    }
                }
            }
        }
    }

    /// One request for the whole grid, fused — not one per step, and not a
    /// separate one for the inner rollup.
    #[test]
    fn should_issue_one_fused_request_for_the_grid() {
        let (steps, offered) = evaluate_range_with_pushdown_on(
            grouped_rollup_reader(),
            "sum by (job) (rate(metric[1m]))",
            0,
            300_000,
            30_000,
            RollupAnswer::Rolled,
        );

        assert_eq!(steps.len(), 11, "eleven steps evaluated");
        assert_eq!(offered.len(), 1, "one fused request for all of them");
        assert_eq!(offered[0].kind, RollupKind::Rate);
        assert_eq!(offered[0].step_ms, 30_000);

        // Two jobs, so two groups per step that has data.
        for (step, samples) in &steps {
            assert!(
                samples.len() <= 2,
                "step {step}: grouped to at most one value per job, got {}",
                samples.len()
            );
        }
    }

    /// `__name__` handling has to survive fusion. The inner rollup's pending drop
    /// is inherited by the group, and grouping *by* `__name__` still sees the
    /// name that is about to disappear.
    #[test]
    fn should_carry_the_name_drop_through_fusion() {
        for answer in [
            RollupAnswer::Rolled,
            RollupAnswer::Reduced,
            RollupAnswer::Raw,
        ] {
            // `rate` drops the name, so the group has none.
            let (pushed, _) = evaluate_rollup_pushdown_on(
                grouped_rollup_reader(),
                "sum by (job) (rate(metric[1m]))",
                120_000,
                answer,
            );
            assert!(!pushed.is_empty());
            for sample in &pushed {
                assert_eq!(sample.labels.get(METRIC_NAME), None, "name dropped");
                assert!(sample.labels.get("job").is_some(), "job kept");
            }

            // `last_over_time` keeps it, so grouping by `__name__` keeps it too.
            let (pushed, _) = evaluate_rollup_pushdown_on(
                grouped_rollup_reader(),
                "sum by (__name__) (last_over_time(metric[1m]))",
                120_000,
                answer,
            );
            assert_eq!(pushed.len(), 1);
            assert_eq!(pushed[0].labels.get(METRIC_NAME), Some("metric"));

            // Grouping by `__name__` over a name-dropping rollup groups on the
            // name and *then* drops it — Prometheus's delayed removal.
            let (pushed, _) = evaluate_rollup_pushdown_on(
                grouped_rollup_reader(),
                "sum by (__name__) (rate(metric[1m]))",
                120_000,
                answer,
            );
            assert_eq!(pushed.len(), 1, "one group: all series share the name");
            assert_eq!(pushed[0].labels.get(METRIC_NAME), None, "then dropped");
        }
    }

    /// Only the reducing operators fuse. A selecting one leaves the rollup to be
    /// pushed down on its own and does the selection here.
    #[test]
    fn should_not_fuse_operators_without_partial_state() {
        for query in [
            "topk(1, rate(metric[1m]))",
            "bottomk(1, rate(metric[1m]))",
            "quantile(0.9, rate(metric[1m]))",
            "count_values(\"v\", rate(metric[1m]))",
        ] {
            let (pushed, offered) = evaluate_rollup_pushdown_on(
                grouped_rollup_reader(),
                query,
                120_000,
                RollupAnswer::Rolled,
            );
            assert_eq!(
                offered.len(),
                1,
                "{query}: the inner rollup is still pushed"
            );

            let local = eval_rollup(&grouped_rollup_reader(), query, 120_000);
            assert_eq!(
                rendered_samples(local),
                rendered_samples(pushed),
                "{query}: unfused result must equal the local one"
            );
        }
    }
}
