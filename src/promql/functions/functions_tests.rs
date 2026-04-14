#[cfg(test)]
mod tests {
    use crate::common::Sample;
    use crate::common::math::kahan_inc;
    use crate::promql::functions::PromQLFunctionImpl;
    use crate::promql::functions::types::FunctionCallContext;
    use crate::promql::functions::utils::variance_kahan;
    use crate::promql::functions::{PromQLArg, PromQLFunction, resolve_function};
    use crate::promql::{EvalResult, EvalSample, EvalSamples, ExprResult, Labels, is_stale_nan};
    use ahash::AHashMap as HashMap;
    use promql_parser::label::METRIC_NAME;
    use promql_parser::parser::{Expr, ParenExpr, StringLiteral};
    use rstest::rstest;
    use std::time::Duration;
    // ========================================================================
    // Test helpers
    // ========================================================================

    pub(crate) struct RangeFunctionAdapter {
        inner: PromQLFunctionImpl,
    }

    impl RangeFunctionAdapter {
        pub(crate) fn apply(
            &self,
            samples: Vec<EvalSamples>,
            eval_timestamp_ms: i64,
        ) -> EvalResult<ExprResult> {
            self.inner
                .apply(PromQLArg::RangeVector(samples), eval_timestamp_ms)
        }

        pub(crate) fn apply_with_range(
            &self,
            mut samples: Vec<EvalSamples>,
            eval_timestamp_ms: i64,
            range: Duration,
        ) -> EvalResult<ExprResult> {
            let range_ms = range.as_millis() as i64;
            for s in &mut samples {
                s.range_ms = range_ms;
                s.range_end_ms = eval_timestamp_ms;
            }
            self.apply(samples, eval_timestamp_ms)
        }
    }

    fn get_range_function(name: &str) -> Option<RangeFunctionAdapter> {
        resolve_function(name).map(|x| RangeFunctionAdapter { inner: x })
    }

    /// Converts f64 values to Sample structs with sequential timestamps
    fn test_samples(values: &[f64]) -> Vec<Sample> {
        values
            .iter()
            .enumerate()
            .map(|(i, &v)| Sample {
                timestamp: i as i64,
                value: v,
            })
            .collect()
    }

    /// Relative error allowed for sample values (matches Prometheus defaultEpsilon)
    const DEFAULT_EPSILON: f64 = 0.000001;

    /// Compare two floats with tolerance.
    ///
    /// Handles StaleNaN, NaN, exact equality, near-zero, and relative tolerance.
    fn almost_equal(a: f64, b: f64, epsilon: f64) -> bool {
        const MIN_NORMAL: f64 = f64::MIN_POSITIVE;

        if is_stale_nan(a) || is_stale_nan(b) {
            return is_stale_nan(a) && is_stale_nan(b);
        }

        if a.is_nan() && b.is_nan() {
            return true;
        }

        if a == b {
            return true;
        }

        let abs_sum = a.abs() + b.abs();
        let diff = (a - b).abs();

        if a == 0.0 || b == 0.0 || abs_sum < MIN_NORMAL {
            return diff < epsilon * MIN_NORMAL;
        }

        diff / abs_sum.min(f64::MAX) < epsilon
    }

    // ========================================================================
    // Tests for variance_kahan
    // ========================================================================

    fn create_sample(value: f64) -> EvalSample {
        EvalSample {
            timestamp_ms: 1000,
            value,
            labels: Default::default(),
            drop_name: false,
        }
    }

    fn create_sample_with_labels(value: f64, labels: &[(&str, &str)]) -> EvalSample {
        EvalSample {
            timestamp_ms: 1000,
            value,
            labels: Labels::from_pairs(labels),
            drop_name: false,
        }
    }

    fn string_arg(value: &str) -> Expr {
        Expr::StringLiteral(StringLiteral {
            val: value.to_string(),
        })
    }

    fn paren_string_arg(value: &str) -> Expr {
        Expr::Paren(ParenExpr {
            expr: Box::new(string_arg(value)),
        })
    }

    fn box_exprs(args: Vec<Expr>) -> Box<[Box<Expr>]> {
        args.into_iter().map(Box::new).collect()
    }

    fn label_replace_raw_args(dst: &str, replacement: &str, src: &str, regex: &str) -> Vec<Expr> {
        vec![
            Expr::NumberLiteral(promql_parser::parser::NumberLiteral { val: 0.0 }),
            paren_string_arg(dst),
            string_arg(replacement),
            string_arg(src),
            string_arg(regex),
        ]
    }

    fn label_join_raw_args(dst: &str, separator: &str, src_labels: &[&str]) -> Vec<Expr> {
        let mut args = vec![
            Expr::NumberLiteral(promql_parser::parser::NumberLiteral { val: 0.0 }),
            paren_string_arg(dst),
            string_arg(separator),
        ];

        args.extend(src_labels.iter().map(|label| string_arg(label)));
        args
    }

    fn call_apply(name: &str, arg: PromQLArg, eval_timestamp_ms: i64) -> Vec<EvalSample> {
        let func = resolve_function(name).unwrap();
        let result = func.apply(arg, eval_timestamp_ms).unwrap();
        let ExprResult::InstantVector(samples) = result else {
            panic!("expected instant vector result");
        };
        samples
    }

    fn call_apply_args(
        name: &str,
        args: Vec<PromQLArg>,
        eval_timestamp_ms: i64,
    ) -> Vec<EvalSample> {
        let func = resolve_function(name).unwrap();

        let result = func.apply_args(args, eval_timestamp_ms).unwrap();
        let ExprResult::InstantVector(samples) = result else {
            panic!("expected instant vector result");
        };
        samples
    }

    fn call_range_function(
        name: &str,
        samples: Vec<EvalSamples>,
        eval_timestamp_ms: i64,
    ) -> Vec<EvalSample> {
        let func = get_range_function(name).unwrap();
        // todo: pass in range
        let result = func
            .apply_with_range(samples, eval_timestamp_ms, Duration::default())
            .unwrap();
        let ExprResult::InstantVector(samples) = result else {
            panic!("expected instant vector result");
        };
        samples
    }

    fn create_vector_arg(_values: Vec<f64>) -> PromQLArg {
        PromQLArg::InstantVector(vec![create_sample_with_labels(
            1.0,
            &[(METRIC_NAME, "testmetric"), ("src", "source\nvalue-10")],
        )])
    }

    #[test]
    fn should_apply_abs_function() {
        let samples = vec![create_sample(-5.0), create_sample(3.0)];
        let result = call_apply("abs", PromQLArg::InstantVector(samples), 1000);

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].value, 5.0);
        assert_eq!(result[1].value, 3.0);
    }

    #[test]
    fn should_apply_round_with_optional_to_nearest() {
        let samples = vec![
            create_sample(1.24),
            create_sample(1.25),
            create_sample(1.26),
            create_sample(-1.25),
        ];
        let result = call_apply_args(
            "round",
            vec![PromQLArg::InstantVector(samples), PromQLArg::Scalar(0.1)],
            1000,
        );

        assert_eq!(result.len(), 4);
        assert_eq!(result[0].value, 1.2);
        assert_eq!(result[1].value, 1.3);
        assert_eq!(result[2].value, 1.3);
        assert_eq!(result[3].value, -1.2);
    }

    #[test]
    fn should_apply_round_when_optional_argument_is_omitted() {
        let result = call_apply_args(
            "round",
            vec![PromQLArg::InstantVector(vec![create_sample(1.6)])],
            1000,
        );

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 2.0);
    }

    #[test]
    fn should_error_when_round_has_too_many_arguments() {
        let func = resolve_function("round").unwrap();

        let err = func
            .apply_args(
                vec![
                    PromQLArg::InstantVector(vec![create_sample(1.0)]),
                    PromQLArg::Scalar(0.1),
                    PromQLArg::Scalar(0.2),
                ],
                1000,
            )
            .unwrap_err();

        assert!(
            err.to_string()
                .contains("round accepts at most two arguments"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn should_error_when_round_second_argument_is_not_scalar() {
        let func = resolve_function("round").unwrap();

        let err = func
            .apply_args(
                vec![
                    PromQLArg::InstantVector(vec![create_sample(1.0)]),
                    PromQLArg::InstantVector(vec![create_sample(0.1)]),
                ],
                1000,
            )
            .unwrap_err();

        assert!(
            err.to_string().contains("expected scalar"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn should_error_when_round_first_argument_is_not_vector() {
        let func = resolve_function("round").unwrap();

        let err = func
            .apply_args(vec![PromQLArg::Scalar(1.0), PromQLArg::Scalar(0.1)], 1000)
            .unwrap_err();

        assert!(
            err.to_string().contains("expected instant vector"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn should_apply_label_replace_with_full_string_match() {
        let func = resolve_function("label_replace").unwrap();

        let raw_args = box_exprs(label_replace_raw_args(
            "dst",
            "destination-value-$1",
            "src",
            "source-value-(.*)",
        ));
        let ctx = FunctionCallContext {
            eval_timestamp_ms: 1000,
            raw_args: &raw_args,
        };

        let result = func
            .apply_call(
                vec![
                    PromQLArg::InstantVector(vec![
                        create_sample_with_labels(
                            1.0,
                            &[(METRIC_NAME, "testmetric"), ("src", "source-value-10")],
                        ),
                        create_sample_with_labels(
                            2.0,
                            &[(METRIC_NAME, "testmetric"), ("src", "source-value-20")],
                        ),
                    ]),
                    "dst".into(),
                    "destination-value-$1".into(),
                    "src".into(),
                    "source-value-(.*)".into(),
                ],
                &ctx,
            )
            .unwrap();

        let ExprResult::InstantVector(result) = result else {
            panic!("expected InstantVector");
        };

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].labels.get("dst"), Some("destination-value-10"));
        assert_eq!(result[1].labels.get("dst"), Some("destination-value-20"));
    }

    #[test]
    fn should_apply_label_replace_with_dotall_regex() {
        let _func = resolve_function("label_replace").unwrap();
        let _raw_args = box_exprs(label_replace_raw_args(
            "dst",
            "matched",
            "src",
            "source.*10",
        ));

        let result = call_apply_args(
            "label_replace",
            vec![
                PromQLArg::InstantVector(vec![create_sample_with_labels(
                    1.0,
                    &[(METRIC_NAME, "testmetric"), ("src", "source\nvalue-10")],
                )]),
                "dst".into(),
                "matched".into(),
                "src".into(),
                "source.*10".into(),
            ],
            1000,
        );

        assert_eq!(result[0].labels.get("dst"), Some("matched"));
    }

    #[test]
    fn should_not_apply_label_replace_on_substring_match() {
        let func = resolve_function("label_replace").unwrap();
        let raw_args = box_exprs(label_replace_raw_args(
            "dst",
            "value-$1",
            "src",
            "value-(.*)",
        ));
        let ctx = FunctionCallContext {
            eval_timestamp_ms: 1000,
            raw_args: &raw_args,
        };

        let result = func
            .apply_call(
                vec![
                    PromQLArg::InstantVector(vec![create_sample_with_labels(
                        1.0,
                        &[
                            (METRIC_NAME, "testmetric"),
                            ("src", "source-value-10"),
                            ("dst", "original-destination-value"),
                        ],
                    )]),
                    "dst".into(),
                    "value-$1".into(),
                    "src".into(),
                    "value-(.*)".into(),
                ],
                &ctx,
            )
            .unwrap();

        let ExprResult::InstantVector(result) = result else {
            panic!("expected InstantVector");
        };

        assert_eq!(
            result[0].labels.get("dst"),
            Some("original-destination-value")
        );
    }

    #[test]
    fn should_drop_destination_label_when_label_replace_replacement_is_empty() {
        let func = resolve_function("label_replace").unwrap();
        let raw_args = box_exprs(label_replace_raw_args("dst", "", "dst", ".*"));
        let ctx = FunctionCallContext {
            eval_timestamp_ms: 1000,
            raw_args: &raw_args,
        };

        let result = func
            .apply_call(
                vec![
                    PromQLArg::InstantVector(vec![create_sample_with_labels(
                        1.0,
                        &[
                            (METRIC_NAME, "testmetric"),
                            ("src", "source-value-10"),
                            ("dst", "original-destination-value"),
                        ],
                    )]),
                    "dst".into(),
                    "".into(),
                    "dst".into(),
                    ".*".into(),
                ],
                &ctx,
            )
            .unwrap();

        let ExprResult::InstantVector(result) = result else {
            panic!("expected InstantVector");
        };

        assert!(!result[0].labels.contains("dst"));
    }

    #[test]
    fn should_apply_label_replace_with_utf8_destination_label_name() {
        let func = resolve_function("label_replace").unwrap();
        let raw_args = box_exprs(label_replace_raw_args(
            "\u{00ff}",
            "value-$1",
            "src",
            "source-value-(.*)",
        ));
        let ctx = FunctionCallContext {
            eval_timestamp_ms: 1000,
            raw_args: &raw_args,
        };

        let result = func
            .apply_call(
                vec![
                    PromQLArg::InstantVector(vec![create_sample_with_labels(
                        1.0,
                        &[(METRIC_NAME, "testmetric"), ("src", "source-value-10")],
                    )]),
                    "\u{00ff}".into(),
                    "value-$1".into(),
                    "src".into(),
                    "source-value-(.*)".into(),
                ],
                &ctx,
            )
            .unwrap();

        let ExprResult::InstantVector(result) = result else {
            panic!("expected InstantVector");
        };

        assert_eq!(result[0].labels.get("\u{00ff}"), Some("value-10"));
    }

    #[test]
    fn should_error_when_label_replace_destination_label_is_empty() {
        let func = resolve_function("label_replace").unwrap();
        let raw_args = box_exprs(label_replace_raw_args("", "", "src", "(.*)"));
        let ctx = FunctionCallContext {
            eval_timestamp_ms: 1000,
            raw_args: &raw_args,
        };

        let err = func
            .apply_call(
                vec![
                    PromQLArg::InstantVector(vec![create_sample_with_labels(
                        1.0,
                        &[(METRIC_NAME, "testmetric"), ("src", "source-value-10")],
                    )]),
                    "".into(),
                    "".into(),
                    "src".into(),
                    "(.*)".into(),
                ],
                &ctx,
            )
            .unwrap_err();

        assert!(
            err.to_string().contains("invalid label name"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn should_error_when_label_replace_produces_duplicate_output_labelsets() {
        let func = resolve_function("label_replace").unwrap();
        let raw_args = box_exprs(label_replace_raw_args("src", "", "", ""));
        let ctx = FunctionCallContext {
            eval_timestamp_ms: 1000,
            raw_args: &raw_args,
        };

        let err = func
            .apply_call(
                vec![
                    PromQLArg::InstantVector(vec![
                        create_sample_with_labels(
                            1.0,
                            &[(METRIC_NAME, "testmetric"), ("src", "source-value-10")],
                        ),
                        create_sample_with_labels(
                            2.0,
                            &[(METRIC_NAME, "testmetric"), ("src", "source-value-20")],
                        ),
                    ]),
                    "src".into(),
                    "".into(),
                    "".into(),
                    "".into(),
                ],
                &ctx,
            )
            .unwrap_err();

        assert!(
            err.to_string()
                .contains("vector cannot contain metrics with the same labelset"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn should_apply_label_join_with_source_labels_in_order() {
        let func = resolve_function("label_join").unwrap();
        let raw_args = box_exprs(label_join_raw_args("dst", "-", &["src", "src1", "src2"]));
        let ctx = FunctionCallContext {
            eval_timestamp_ms: 1000,
            raw_args: &raw_args,
        };

        let result = func
            .apply_call(
                vec![
                    PromQLArg::InstantVector(vec![
                        create_sample_with_labels(
                            1.0,
                            &[
                                (METRIC_NAME, "testmetric"),
                                ("src", "a"),
                                ("src1", "b"),
                                ("src2", "c"),
                            ],
                        ),
                        create_sample_with_labels(
                            2.0,
                            &[
                                (METRIC_NAME, "testmetric"),
                                ("src", "d"),
                                ("src1", "e"),
                                ("src2", "f"),
                            ],
                        ),
                    ]),
                    "".into(),
                    "".into(),
                    "".into(),
                    "".into(),
                    "".into(),
                ],
                &ctx,
            )
            .unwrap();

        let ExprResult::InstantVector(result) = result else {
            panic!("expected InstantVector");
        };

        assert_eq!(result[0].labels.get("dst"), Some("a-b-c"));
        assert_eq!(result[1].labels.get("dst"), Some("d-e-f"));
    }

    #[test]
    fn should_apply_label_join_without_source_labels() {
        let func = resolve_function("label_join").unwrap();
        let raw_args = box_exprs(label_join_raw_args("dst", ", ", &[]));
        let ctx = FunctionCallContext {
            eval_timestamp_ms: 1000,
            raw_args: &raw_args,
        };

        let result = func
            .apply_call(
                vec![
                    PromQLArg::InstantVector(vec![create_sample_with_labels(
                        1.0,
                        &[
                            (METRIC_NAME, "testmetric"),
                            ("src", "a"),
                            ("src1", "b"),
                            ("dst", "original-destination-value"),
                        ],
                    )]),
                    "dst".into(),
                    ",".into(),
                ],
                &ctx,
            )
            .unwrap();

        let ExprResult::InstantVector(result) = result else {
            panic!("expected InstantVector");
        };

        assert!(!result[0].labels.contains("dst"));
    }

    // #[test]
    // fn should_error_when_label_join_destination_label_is_empty() {
    //     let registry = FunctionRegistry::new();
    //     let func = registry.get("label_join").unwrap();
    //     let raw_args = box_exprs(label_join_raw_args("", "-", &["src"]));
    //     let ctx = FunctionCallContext {
    //         eval_timestamp_ms: 1000,
    //         raw_args: &raw_args,
    //     };
    //
    //     let err = func
    //         .apply_call(
    //             vec![
    //                 PromQLArg::InstantVector(vec![create_sample_with_labels(
    //                     1.0,
    //                     &[(METRIC_NAME, "testmetric"), ("src", "a")],
    //                 )]),
    //                 None,
    //                 None,
    //                 None,
    //             ],
    //             &ctx,
    //         )
    //         .unwrap_err();
    //
    //     assert!(
    //         err.to_string().contains("invalid label name"),
    //         "unexpected error: {}",
    //         err
    //     );
    // }

    // #[test]
    // fn should_error_when_label_join_produces_duplicate_output_labelsets() {
    //     let registry = FunctionRegistry::new();
    //     let func = registry.get("label_join").unwrap();
    //     let raw_args = box_exprs(label_join_raw_args("label", "", &["this"]));
    //     let ctx = FunctionCallContext {
    //         eval_timestamp_ms: 1000,
    //         raw_args: &raw_args,
    //     };
    //
    //     let err = func
    //         .apply_call(
    //             vec![
    //                 PromQLArg::InstantVector(vec![
    //                     create_sample_with_labels(
    //                         1.0,
    //                         &[(METRIC_NAME, "dup"), ("label", "a"), ("this", "a")],
    //                     ),
    //                     create_sample_with_labels(
    //                         2.0,
    //                         &[(METRIC_NAME, "dup"), ("label", "b"), ("this", "a")],
    //                     ),
    //                 ]),
    //                 None,
    //                 None,
    //                 None,
    //             ],
    //             &ctx,
    //         )
    //         .unwrap_err();
    //
    //     assert!(
    //         err.to_string()
    //             .contains("vector cannot contain metrics with the same labelset"),
    //         "unexpected error: {}",
    //         err
    //     );
    // }

    #[test]
    fn should_preserve_metric_name_when_label_replace_writes_name_label() {
        let func = resolve_function("label_replace").unwrap();
        let raw_args = box_exprs(label_replace_raw_args(
            "__name__", "rate_$1", "__name__", "(.+)",
        ));
        let ctx = FunctionCallContext {
            eval_timestamp_ms: 1000,
            raw_args: &raw_args,
        };
        let mut sample =
            create_sample_with_labels(1.0, &[(METRIC_NAME, "metric_total"), ("env", "1")]);
        sample.drop_name = true;

        let result = func
            .apply_call(
                vec![
                    PromQLArg::InstantVector(vec![sample]),
                    "".into(),
                    "".into(),
                    "".into(),
                    "".into(),
                ],
                &ctx,
            )
            .unwrap();

        let ExprResult::InstantVector(result) = result else {
            panic!("expected InstantVector");
        };

        assert_eq!(result[0].labels.get(METRIC_NAME), Some("rate_metric_total"));
        assert!(!result[0].drop_name);
    }

    // #[test]
    // fn should_preserve_metric_name_when_label_join_writes_name_label() {
    //     let registry = FunctionRegistry::new();
    //     let func = registry.get("label_join").unwrap();
    //     let raw_args = box_exprs(label_join_raw_args("__name__", "_", &["__name__", "env"]));
    //     let ctx = FunctionCallContext {
    //         eval_timestamp_ms: 1000,
    //         raw_args: &raw_args,
    //     };
    //     let mut sample =
    //         create_sample_with_labels(1.0, &[(METRIC_NAME, "metric_total"), ("env", "1")]);
    //     sample.drop_name = true;
    //
    //     let result = func
    //         .apply_call(
    //             vec![
    //                 PromQLArg::InstantVector(vec![sample]),
    //                 None,
    //                 None,
    //                 None,
    //                 None,
    //             ],
    //             &ctx,
    //         )
    //         .unwrap();
    //
    //     assert_eq!(result[0].labels.get(METRIC_NAME), Some("metric_total_1"));
    //     assert!(!result[0].drop_name);
    // }

    #[test]
    fn should_include_actual_argument_count_in_clamp_errors() {
        let clamp = resolve_function("clamp").unwrap();

        let err = clamp.apply_args(vec![], 1000).unwrap_err();
        assert!(
            err.to_string()
                .contains("clamp requires exactly 3 argument(s), got 0"),
            "unexpected error: {}",
            err
        );

        let err = clamp
            .apply_args(
                vec![
                    PromQLArg::InstantVector(vec![create_sample(1.0)]),
                    PromQLArg::Scalar(0.0),
                    PromQLArg::Scalar(1.0),
                    PromQLArg::Scalar(2.0),
                ],
                1000,
            )
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("clamp requires exactly 3 argument(s), got 4"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn should_include_actual_argument_count_in_clamp_min_errors() {
        let clamp_min = resolve_function("clamp_min").unwrap();

        let err = clamp_min.apply_args(vec![], 1000).unwrap_err();
        assert!(
            err.to_string()
                .contains("clamp_min requires exactly 2 argument(s), got 0"),
            "unexpected error: {}",
            err
        );

        let err = clamp_min
            .apply_args(
                vec![
                    PromQLArg::InstantVector(vec![create_sample(1.0)]),
                    PromQLArg::Scalar(0.0),
                    PromQLArg::Scalar(1.0),
                ],
                1000,
            )
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("clamp_min requires exactly 2 argument(s), got 3"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn should_include_actual_argument_count_in_clamp_max_errors() {
        let clamp_max = resolve_function("clamp_max").unwrap();

        let err = clamp_max.apply_args(vec![], 1000).unwrap_err();
        assert!(
            err.to_string()
                .contains("clamp_max requires exactly 2 argument(s), got 0"),
            "unexpected error: {}",
            err
        );

        let err = clamp_max
            .apply_args(
                vec![
                    PromQLArg::InstantVector(vec![create_sample(1.0)]),
                    PromQLArg::Scalar(0.0),
                    PromQLArg::Scalar(1.0),
                ],
                1000,
            )
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("clamp_max requires exactly 2 argument(s), got 3"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn should_apply_clamp_functions() {
        let samples = vec![
            create_sample(-50.0),
            create_sample(0.0),
            create_sample(100.0),
        ];

        let clamped = call_apply_args(
            "clamp",
            vec![
                PromQLArg::InstantVector(samples.clone()),
                PromQLArg::Scalar(-25.0),
                PromQLArg::Scalar(75.0),
            ],
            1000,
        );

        assert_eq!(clamped[0].value, -25.0);
        assert_eq!(clamped[1].value, 0.0);
        assert_eq!(clamped[2].value, 75.0);

        let min_clamped = call_apply_args(
            "clamp_min",
            vec![
                PromQLArg::InstantVector(samples.clone()),
                PromQLArg::Scalar(-25.0),
            ],
            1000,
        );

        assert_eq!(min_clamped[0].value, -25.0);
        assert_eq!(min_clamped[1].value, 0.0);
        assert_eq!(min_clamped[2].value, 100.0);

        let max_clamped = call_apply_args(
            "clamp_max",
            vec![PromQLArg::InstantVector(samples), PromQLArg::Scalar(75.0)],
            1000,
        );
        assert_eq!(max_clamped[0].value, -50.0);
        assert_eq!(max_clamped[1].value, 0.0);
        assert_eq!(max_clamped[2].value, 75.0);
    }

    #[test]
    fn should_return_empty_for_clamp_when_min_exceeds_max() {
        let samples = vec![
            create_sample(-50.0),
            create_sample(0.0),
            create_sample(100.0),
        ];

        let result = call_apply_args(
            "clamp",
            vec![
                PromQLArg::InstantVector(samples),
                PromQLArg::Scalar(5.0),
                PromQLArg::Scalar(-5.0),
            ],
            1000,
        );

        assert!(result.is_empty());
    }

    #[test]
    fn should_propagate_nan_when_clamp_bounds_or_samples_are_nan() {
        let bound_samples = vec![
            create_sample(-50.0),
            create_sample(0.0),
            create_sample(100.0),
        ];

        let min_nan_result = call_apply_args(
            "clamp",
            vec![
                PromQLArg::InstantVector(bound_samples.clone()),
                PromQLArg::Scalar(f64::NAN),
                PromQLArg::Scalar(100.0),
            ],
            1000,
        );

        assert_eq!(min_nan_result.len(), 3);
        assert!(min_nan_result[0].value.is_nan());
        assert!(min_nan_result[1].value.is_nan());
        assert!(min_nan_result[2].value.is_nan());

        let max_nan_result = call_apply_args(
            "clamp",
            vec![
                PromQLArg::InstantVector(bound_samples),
                PromQLArg::Scalar(0.0),
                PromQLArg::Scalar(f64::NAN),
            ],
            1000,
        );

        assert_eq!(max_nan_result.len(), 3);
        assert!(max_nan_result[0].value.is_nan());
        assert!(max_nan_result[1].value.is_nan());
        assert!(max_nan_result[2].value.is_nan());

        let sample_nan_result = call_apply_args(
            "clamp",
            vec![
                PromQLArg::InstantVector(vec![
                    create_sample(-50.0),
                    create_sample(f64::NAN),
                    create_sample(100.0),
                ]),
                PromQLArg::Scalar(-25.0),
                PromQLArg::Scalar(75.0),
            ],
            1000,
        );

        assert_eq!(sample_nan_result.len(), 3);
        assert_eq!(sample_nan_result[0].value, -25.0);
        assert!(sample_nan_result[1].value.is_nan());
        assert_eq!(sample_nan_result[2].value, 75.0);
    }

    #[test]
    fn should_propagate_nan_when_clamp_min_bound_or_samples_are_nan() {
        let bound_samples = vec![
            create_sample(-50.0),
            create_sample(0.0),
            create_sample(100.0),
        ];

        let min_nan_result = call_apply_args(
            "clamp_min",
            vec![
                PromQLArg::InstantVector(bound_samples),
                PromQLArg::Scalar(f64::NAN),
            ],
            1000,
        );

        assert_eq!(min_nan_result.len(), 3);
        assert!(min_nan_result[0].value.is_nan());
        assert!(min_nan_result[1].value.is_nan());
        assert!(min_nan_result[2].value.is_nan());

        let sample_nan_result = call_apply_args(
            "clamp_min",
            vec![
                PromQLArg::InstantVector(vec![
                    create_sample(-50.0),
                    create_sample(f64::NAN),
                    create_sample(100.0),
                ]),
                PromQLArg::Scalar(-25.0),
            ],
            1000,
        );

        assert_eq!(sample_nan_result.len(), 3);
        assert_eq!(sample_nan_result[0].value, -25.0);
        assert!(sample_nan_result[1].value.is_nan());
        assert_eq!(sample_nan_result[2].value, 100.0);
    }

    #[test]
    fn should_propagate_nan_when_clamp_max_bound_or_samples_are_nan() {
        let bound_samples = vec![
            create_sample(-50.0),
            create_sample(0.0),
            create_sample(100.0),
        ];

        let max_nan_result = call_apply_args(
            "clamp_max",
            vec![
                PromQLArg::InstantVector(bound_samples),
                PromQLArg::Scalar(f64::NAN),
            ],
            1000,
        );

        assert_eq!(max_nan_result.len(), 3);
        assert!(max_nan_result[0].value.is_nan());
        assert!(max_nan_result[1].value.is_nan());
        assert!(max_nan_result[2].value.is_nan());

        let sample_nan_result = call_apply_args(
            "clamp_max",
            vec![
                PromQLArg::InstantVector(vec![
                    create_sample(-50.0),
                    create_sample(f64::NAN),
                    create_sample(100.0),
                ]),
                PromQLArg::Scalar(75.0),
            ],
            1000,
        );

        assert_eq!(sample_nan_result.len(), 3);
        assert_eq!(sample_nan_result[0].value, -50.0);
        assert!(sample_nan_result[1].value.is_nan());
        assert_eq!(sample_nan_result[2].value, 75.0);
    }

    #[test]
    fn should_apply_absent_function() {
        let eval_timestamp_ms = 5000i64;

        // Empty input should return one sample with value 1.0 at eval timestamp
        let result = call_apply(
            "absent",
            PromQLArg::InstantVector(vec![]),
            eval_timestamp_ms,
        );

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 1.0);
        assert_eq!(result[0].timestamp_ms, eval_timestamp_ms);

        // Non-empty input should return empty
        let result = call_apply(
            "absent",
            PromQLArg::InstantVector(vec![create_sample(42.0)]),
            eval_timestamp_ms,
        );

        assert!(result.is_empty());
    }

    #[test]
    fn should_apply_scalar_function() {
        let func = resolve_function("scalar").unwrap();

        // A single element should be returned as a scalar-encoded sample.
        let result = func
            .apply(PromQLArg::InstantVector(vec![create_sample(42.0)]), 1000)
            .unwrap();
        let ExprResult::Scalar(f) = result else {
            panic!("expected Scalar result")
        };
        assert_eq!(f, 42.0);

        // Zero or multiple elements should return NaN encoded as a scalar sample.
        let result = func.apply(PromQLArg::InstantVector(vec![]), 1000).unwrap();
        let ExprResult::Scalar(_value) = result else {
            panic!("expected Scalar result")
        };

        let result = func
            .apply(
                PromQLArg::InstantVector(vec![create_sample(1.0), create_sample(2.0)]),
                1000,
            )
            .unwrap();

        let ExprResult::Scalar(value) = result else {
            panic!("expected Scalar result")
        };

        assert!(value.is_nan());
    }

    #[test]
    fn should_apply_timestamp_function() {
        let samples = vec![EvalSample {
            timestamp_ms: 10_000,
            value: 123.0,
            labels: Labels::from_pairs(&[(METRIC_NAME, "metric"), ("job", "api")]),
            drop_name: false,
        }];

        let result = call_apply("timestamp", PromQLArg::InstantVector(samples), 600_000);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 10.0);
        assert_eq!(result[0].timestamp_ms, 600_000);
        assert_eq!(result[0].labels.get(METRIC_NAME), Some("metric"));
        assert_eq!(result[0].labels.get("job"), Some("api"));
        assert!(result[0].drop_name);
    }

    #[test]
    fn should_apply_minute_function() {
        let result = call_apply(
            "minute",
            PromQLArg::InstantVector(vec![create_sample_with_labels(
                1136239445.0,
                &[("__name__", "metric"), ("job", "api")],
            )]),
            600_000,
        );

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 4.0);
        assert_eq!(result[0].timestamp_ms, 600_000);
        assert_eq!(result[0].labels.get(METRIC_NAME), Some("metric"));
        assert_eq!(result[0].labels.get("job"), Some("api"));
        assert!(result[0].drop_name);
    }

    #[test]
    fn should_apply_year_function_without_arguments() {
        let result = call_apply_args("year", vec![], 0);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 1970.0);
        assert_eq!(result[0].timestamp_ms, 0);
        assert_eq!(result[0].labels, Labels::default());
        assert!(!result[0].drop_name);
    }

    #[test]
    fn should_apply_days_in_month_function() {
        let result = call_apply(
            "days_in_month",
            PromQLArg::InstantVector(vec![create_sample(1454284800.0)]),
            1000,
        );

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 29.0);
        assert_eq!(result[0].timestamp_ms, 1000);
        assert!(result[0].drop_name);
    }

    #[test]
    fn should_truncate_date_time_function_inputs_to_whole_seconds() {
        let result = call_apply(
            "year",
            PromQLArg::InstantVector(vec![create_sample(-0.9)]),
            1000,
        );

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 1970.0);
        assert_eq!(result[0].timestamp_ms, 1000);
        assert!(result[0].drop_name);
    }

    #[test]
    fn should_truncate_negative_eval_time_to_whole_seconds() {
        let result = call_apply_args("year", vec![], -1);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 1970.0);
        assert_eq!(result[0].timestamp_ms, -1);
        assert!(!result[0].drop_name);
    }

    fn create_eval_samples<L: Into<Labels>>(values: Vec<(i64, f64)>, labels: L) -> EvalSamples {
        let values = values
            .into_iter()
            .map(|(t, v)| Sample::new(t, v))
            .collect::<Vec<_>>();
        EvalSamples {
            values,
            labels: labels.into(),
            range_ms: 0,
            range_end_ms: 0,
            drop_name: false,
        }
    }

    #[test]
    fn should_apply_rate_function() {
        let mut labels: HashMap<String, String> = Default::default();
        labels.insert("job".to_string(), "test".to_string());

        // Create sample series with increasing counter values
        let samples = vec![create_eval_samples(
            vec![
                (1000, 100.0), // t=1s, value=100
                (2000, 110.0), // t=2s, value=110
                (3000, 125.0), // t=3s, value=125
            ],
            labels.clone(),
        )];

        let func = get_range_function("rate").unwrap();

        // 2s range, samples sit exactly at the window edges.
        let result = func
            .apply_with_range(samples, 3000, Duration::from_millis(2000))
            .unwrap()
            .expect_instant_vector("expected an instant vector");

        assert_eq!(result.len(), 1);
        // Rate = (125 - 100) / (3000 - 1000) * 1000 = 25 / 2 = 12.5 per second
        assert_eq!(result[0].value, 12.5);
        assert_eq!(result[0].timestamp_ms, 3000);
        assert_eq!(result[0].labels, labels.into());
    }

    #[test]
    fn should_apply_sum_over_time_function() {
        // given
        let samples = vec![create_eval_samples(
            vec![(1000, 1.0), (2000, 2.0), (3000, 3.0)],
            Labels::default(),
        )];

        // when
        let result = call_range_function("sum_over_time", samples, 2000);

        // then
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 6.0); // 1 + 2 + 3
    }

    #[test]
    fn should_apply_avg_over_time_function() {
        // given
        let samples = vec![create_eval_samples(
            vec![(1000, 10.0), (2000, 20.0), (3000, 30.0)],
            Labels::default(),
        )];

        // when
        let result = call_range_function("avg_over_time", samples, 3000);

        // then
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 20.0); // (10 + 20 + 30) / 3
    }

    #[test]
    fn should_apply_min_over_time_function() {
        // given
        let samples = vec![create_eval_samples(
            vec![(1000, 10.0), (2000, 5.0), (3000, 30.0)],
            Labels::default(),
        )];

        // when
        let result = call_range_function("min_over_time", samples, 3000);

        // then
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 5.0);
    }

    #[test]
    fn should_apply_max_over_time_function() {
        // given
        let samples = vec![create_eval_samples(
            vec![(1000, 10.0), (2000, 50.0), (3000, 30.0)],
            Labels::default(),
        )];

        // when
        let result = call_range_function("max_over_time", samples, 3000);

        // then
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 50.0);
    }

    #[test]
    fn should_apply_count_over_time_function() {
        // given
        let samples = vec![create_eval_samples(
            vec![(1000, 10.0), (2000, 20.0), (3000, 30.0), (4000, 40.0)],
            Labels::default(),
        )];

        // when
        let result = call_range_function("count_over_time", samples, 4000);

        // then
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 4.0);
    }

    #[test]
    fn should_apply_stddev_over_time_function() {
        // given

        // Values: [10, 20, 30, 40]
        // Mean: 25
        // Variance: ((10-25)^2 + (20-25)^2 + (30-25)^2 + (40-25)^2) / 4 = (225 + 25 + 25 + 225) / 4 = 125
        // Stddev: sqrt(125) ≈ 11.180339887498949
        let samples = vec![create_eval_samples(
            vec![(1000, 10.0), (2000, 20.0), (3000, 30.0), (4000, 40.0)],
            Labels::default(),
        )];

        // when
        let result = call_range_function("stddev_over_time", samples, 4000);

        // then
        assert_eq!(result.len(), 1);
        assert!((result[0].value - 11.180339887498949).abs() < 1e-10);
    }

    #[test]
    fn should_apply_stdvar_over_time_function() {
        // given

        // Values: [10, 20, 30, 40]
        // Mean: 25
        // Variance: 125
        let samples = vec![create_eval_samples(
            vec![(1000, 10.0), (2000, 20.0), (3000, 30.0), (4000, 40.0)],
            Labels::default(),
        )];

        // when
        let result = call_range_function("stdvar_over_time", samples, 4000);

        // then
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 125.0);
    }

    #[test]
    fn should_return_zero_for_stddev_with_single_value() {
        // given
        let samples = vec![create_eval_samples(vec![(1000, 42.0)], Labels::default())];

        // when
        let result = call_range_function("stddev_over_time", samples, 1000);

        // then
        assert_eq!(result.len(), 1);
        // Single value has variance 0, stddev 0
        assert_eq!(result[0].value, 0.0);
    }

    #[test]
    fn should_skip_empty_series_for_stddev() {
        // given
        let samples = vec![create_eval_samples(vec![], Labels::default())];

        // when
        let result = call_range_function("stddev_over_time", samples, 1000);

        // then
        // Empty series are skipped (Prometheus behavior)
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn should_skip_empty_series_for_stdvar() {
        // given
        let samples = vec![create_eval_samples(vec![], Labels::default())];

        // when
        let result = call_range_function("stdvar_over_time", samples, 1000);

        // then
        // Empty series are skipped (Prometheus behavior)
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn variance_kahan_empty_returns_nan() {
        assert!(variance_kahan(&[]).is_nan());
    }

    #[test]
    fn variance_kahan_single_value_returns_zero() {
        let result = variance_kahan(&test_samples(&[42.0]));
        assert!(almost_equal(result, 0.0, 1e-6));
    }

    #[rstest]
    #[case(&[10.0, 20.0, 30.0, 40.0], 125.0)]
    #[case(&[5.0, 5.0, 5.0, 5.0], 0.0)]
    #[case(&[1.0, 2.0], 0.25)]
    #[case(&[1.0, 2.0, 3.0, 4.0, 5.0], 2.0)]
    fn variance_kahan_fixed_vectors(#[case] values: &[f64], #[case] expected: f64) {
        let result = variance_kahan(&test_samples(values));
        assert!(
            almost_equal(result, expected, 1e-6),
            "Expected {}, got {}",
            expected,
            result
        );
    }

    #[test]
    fn variance_kahan_numerical_stability_stress() {
        // Large base + small deltas: base=1e10, values [base+0, base+1, base+2, base+3]
        // Expected from delta-space variance ([0,1,2,3]) = 1.25
        let base = 1e10;
        let samples = test_samples(&[base, base + 1.0, base + 2.0, base + 3.0]);
        let result = variance_kahan(&samples);
        assert!(
            almost_equal(result, 1.25, 1e-6),
            "Expected 1.25, got {}",
            result
        );
    }

    #[test]
    fn variance_kahan_vs_two_pass_oracle() {
        // Test against independent two-pass algorithm (inlined to prevent misuse)
        let samples = test_samples(&[10.0, 20.0, 30.0, 40.0]);

        let welford_result = variance_kahan(&samples);

        // Two-pass oracle (inlined)
        let mean = samples.iter().map(|s| s.value).sum::<f64>() / samples.len() as f64;
        let two_pass_result = samples
            .iter()
            .map(|s| (s.value - mean).powi(2))
            .sum::<f64>()
            / samples.len() as f64;

        assert!(
            almost_equal(welford_result, two_pass_result, 1e-6),
            "Welford: {}, Two-pass: {}",
            welford_result,
            two_pass_result
        );
    }

    #[test]
    fn variance_kahan_nan_propagation() {
        assert!(variance_kahan(&test_samples(&[1.0, f64::NAN, 3.0])).is_nan());
    }

    #[test]
    fn should_handle_constant_values_in_stddev() {
        // given
        let _func = get_range_function("stddev_over_time").unwrap();

        let samples = vec![create_eval_samples(
            vec![(1000, 5.0), (2000, 5.0), (3000, 5.0), (4000, 5.0)],
            Labels::default(),
        )];

        // when
        let result = call_range_function("stddev_over_time", samples, 4000);

        // then
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 0.0); // All values same = zero variance
    }

    #[test]
    fn should_handle_large_magnitude_small_variance_in_stddev() {
        // given

        // Large magnitude values (1e10) with small variance
        // At 1e16, floating point precision limits prevent accurate small variance calculation
        // Values: [1e10, 1e10 + 1, 1e10 + 2, 1e10 + 3]
        // Mean: 1e10 + 1.5
        // Variance: ((0-1.5)^2 + (1-1.5)^2 + (2-1.5)^2 + (3-1.5)^2) / 4 = 1.25
        // Stddev: sqrt(1.25) ≈ 1.118033988749895
        let base = 1e10;
        let samples = vec![create_eval_samples(
            vec![
                (1000, base),
                (2000, base + 1.0),
                (3000, base + 2.0),
                (4000, base + 3.0),
            ],
            Labels::default(),
        )];

        // when
        let result = call_range_function("stddev_over_time", samples, 4000);

        // then
        assert_eq!(result.len(), 1);
        // Kahan summation should maintain reasonable precision
        let expected_stddev = 1.118033988749895;
        let rel_error = ((result[0].value - expected_stddev) / expected_stddev).abs();
        assert!(
            rel_error < 1e-6,
            "stddev should be reasonably accurate for large magnitude with small variance: computed={}, expected={}, rel_error={}",
            result[0].value,
            expected_stddev,
            rel_error
        );
    }

    #[test]
    fn should_propagate_nan_in_stddev() {
        // given
        let samples = vec![create_eval_samples(
            vec![(1000, 10.0), (2000, f64::NAN), (3000, 30.0)],
            Labels::default(),
        )];

        // when
        let result = call_range_function("stddev_over_time", samples, 3000);

        // then
        assert_eq!(result.len(), 1);
        assert!(
            result[0].value.is_nan(),
            "NaN should propagate through stddev calculation"
        );
    }

    #[test]
    fn should_propagate_nan_in_stdvar() {
        // given
        let samples = vec![create_eval_samples(
            vec![(1000, 10.0), (2000, f64::NAN), (3000, 30.0)],
            Labels::default(),
        )];

        // when
        let result = call_range_function("stdvar_over_time", samples, 3000);

        // then
        assert_eq!(result.len(), 1);
        assert!(
            result[0].value.is_nan(),
            "NaN should propagate through stdvar calculation"
        );
    }

    #[test]
    fn should_handle_counter_reset_in_rate() {
        let func = get_range_function("rate").unwrap();
        let labels = Labels::default();

        // Create sample series with counter reset (value goes down)
        let samples = vec![create_eval_samples(
            vec![
                (1000, 100.0), // t=1s, value=100
                (2000, 50.0),  // t=2s, value=50 (counter reset)
            ],
            labels,
        )];

        // 1s range, samples sit exactly at the window edges.
        // increase = (50 - 100) + 100 = 50 (counter reset correction)
        // rate = 50.0 per second
        let result = func
            .apply_with_range(samples, 2000, Duration::from_millis(1000))
            .unwrap()
            .expect_instant_vector("expected instant vector result from rate function");

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 50.0);
    }

    #[test]
    fn should_handle_multiple_counter_resets_in_rate() {
        // given
        let func = get_range_function("rate").unwrap();

        // Sequence: [1, 5, 2, 4, 1, 3, 6] with 1s intervals and two resets
        let samples = vec![create_eval_samples(
            vec![
                (1000, 1.0),
                (2000, 5.0),
                (3000, 2.0),
                (4000, 4.0),
                (5000, 1.0),
                (6000, 3.0),
                (7000, 6.0),
            ],
            HashMap::new(),
        )];

        // when
        // 6s range, samples sit exactly at the window edges.
        // increase = (6 - 1) + 5 + 4 = 14 so rate = 14.0 / 6
        let result = func
            .apply_with_range(samples, 7000, Duration::from_millis(6000))
            .unwrap()
            .expect_instant_vector("instant vector expected");

        // then
        assert_eq!(result.len(), 1);
        let expected = 14.0 / 6.0;
        assert!(
            (result[0].value - expected).abs() < 1e-10,
            "expected {}, got {}",
            expected,
            result[0].value
        );
    }

    #[test]
    fn should_extrapolate_rate_to_window_boundaries() {
        // given
        let func = get_range_function("rate").unwrap();

        let samples = vec![create_eval_samples(
            vec![(1000, 10.0), (2000, 20.0), (3000, 30.0), (4000, 40.0)],
            HashMap::new(),
        )];

        // when
        // Window: [0s, 5s], eval at 5s
        let result = func
            .apply_with_range(samples, 5000, Duration::from_secs(5))
            .unwrap()
            .expect_instant_vector("instant vector expected");

        // then
        assert_eq!(result.len(), 1);

        // Prometheus extrapolation:
        // sampled interval = 3s, avg_duration_between_samples = 1s, threshold = 1.1s
        // duration_to_start = (1 - 0) / 1 = 1.0s (< 1.1, extrapolate to boundary)
        // duration_to_end = (5 - 4) / 1 = 1.0s (< 1.1, extrapolate to boundary)
        // Counter zero-point: duration_to_zero = 3.0 * (10.0 / (40.0 - 10.0)) = 1.0s
        // duration_to_zero (1.0) == duration_to_start (1.0), so no change to duration_to_start
        // factor = (3.0 + 1.0 + 1.0) / 3.0 / 5.0 = 5.0 / 3.0 / 5.0 = 1/3
        // result = 30.0 * (1/3) = 10.0
        let expected = 10.0;
        assert!(
            (result[0].value - expected).abs() < 1e-10,
            "expected {}, got {}",
            expected,
            result[0].value
        );
    }

    #[test]
    fn should_limit_extrapolation_when_samples_far_from_boundary() {
        // given
        let func = get_range_function("rate").unwrap();

        let samples = vec![create_eval_samples(
            vec![(4000, 10.0), (5000, 20.0), (6000, 30.0)],
            HashMap::new(),
        )];

        // when
        // Window: [0s, 10s], eval at 10s
        let result = func
            .apply_with_range(samples, 10000, Duration::from_secs(10))
            .unwrap()
            .expect_instant_vector("instant vector expected");

        // then
        assert_eq!(result.len(), 1);

        // sampled interval = 2s, avg_duration_between_samples = 1s
        // duration_to_start = 4.0s (> 1.1s, use avg_duration_between_samples/2 = 0.5s)
        // duration_to_end = 4.0s (> 1.1s, similarly use 0.5s)
        // Counter zero-point: duration_to_zero = 2.0 * (10.0 / (30.0 - 10.0)) = 1.0s
        // duration_to_zero (1.0) > durationToStart (0.5), so no change to duration_to_start
        // factor = (2.0 + 0.5 + 0.5) / 2.0 / 10.0 = 3.0 / 2.0 / 10.0 = 0.15
        // result = 20.0 * 0.15 = 3.0
        let expected = 3.0;
        assert!(
            (result[0].value - expected).abs() < 1e-10,
            "expected {}, got {}",
            expected,
            result[0].value
        );
    }

    #[test]
    fn should_extrapolate_rate_with_counter_reset() {
        // given
        let func = get_range_function("rate").unwrap();

        let samples = vec![create_eval_samples(
            vec![
                (1000, 10.0),
                (2000, 20.0),
                (3000, 5.0), // counter reset
                (4000, 15.0),
            ],
            HashMap::new(),
        )];

        // when
        // Window: [0s, 5s], eval at 5s
        let result = func
            .apply_with_range(samples, 5000, Duration::from_secs(5))
            .unwrap()
            .expect_instant_vector("instant vector expected");

        // then
        assert_eq!(result.len(), 1);

        // Counter reset correction: increase = (15 - 10) + 20 = 25
        // sampled interval = 3s, avg_duration_between_samples = 1s, threshold = 1.1s
        // duration_to_start = 1.0s (< 1.1, extrapolate to boundary)
        // duration_to_end = 1.0s (< 1.1, extrapolate to boundary)
        // Counter zero-point: duration_to_zero = 3.0 * (10.0 / 25.0) = 1.2s
        // duration_to_zero (1.2) > duration_to_start (1.0), so no change to duration_to_start
        // factor = (3.0 + 1.0 + 1.0) / 3.0 / 5.0 = 5/3/5 = 1/3
        // result = 25.0 / 3
        let expected = 25.0 / 3.0;
        assert!(
            (result[0].value - expected).abs() < 1e-10,
            "expected {}, got {}",
            expected,
            result[0].value
        );
    }

    #[test]
    fn should_skip_series_with_insufficient_samples() {
        // Create sample series with only one point
        let samples = vec![create_eval_samples(vec![(1000, 100.0)], Labels::default())];

        let func = get_range_function("rate").unwrap();
        let result = func
            .apply_with_range(samples, 1000, Duration::from_millis(1000))
            .unwrap()
            .expect_instant_vector("instant vector expected");

        // Should return empty result for insufficient samples
        assert!(result.is_empty());
    }

    #[test]
    fn should_handle_catastrophic_cancellation_in_sum() {
        // Test Kahan summation handles catastrophic cancellation
        let values = vec![
            Sample {
                timestamp: 0,
                value: 1e16,
            },
            Sample {
                timestamp: 1,
                value: 1.0,
            },
            Sample {
                timestamp: 2,
                value: -1e16,
            },
        ];

        let naive: f64 = values.iter().map(|s| s.value).sum();

        let mut sum = 0.0;
        let mut c = 0.0;
        for sample in &values {
            (sum, c) = kahan_inc(sample.value, sum, c);
        }
        let kahan = sum + c;

        // Naive sum loses precision due to catastrophic cancellation
        // Kahan summation preserves the 1.0
        assert!((kahan - 1.0).abs() < 1e-9, "kahan={}, expected=1.0", kahan);
        assert!(
            (naive - 1.0).abs() > 1e-9,
            "naive={}, should lose precision",
            naive
        );
    }

    #[test]
    fn should_match_prometheus_kahan_bits() {
        // Bitwise exact match with Prometheus Go implementation.
        //
        // Generated using the following Go harness:
        //
        // package main
        //
        // import (
        //     "fmt"
        //     "math"
        //     "github.com/prometheus/prometheus/util/kahansum"
        // )
        //
        // func main() {
        //     values := []float64{1e16, 1.0, -1e16}
        //
        //     sum, c := 0.0, 0.0
        //     for _, v := range values {
        //         sum, c = kahansum.Inc(v, sum, c)
        //     }
        //
        //     result := sum + c
        //     fmt.Println(math.Float64bits(result))
        // }
        //
        // Output:
        // 4607182418800017408
        //
        // This locks Rust behavior to Prometheus' exact IEEE-754 bit pattern.
        // Any future compiler or refactor drift will be caught immediately.
        let values = vec![
            Sample {
                timestamp: 0,
                value: 1e16,
            },
            Sample {
                timestamp: 1,
                value: 1.0,
            },
            Sample {
                timestamp: 2,
                value: -1e16,
            },
        ];

        let mut sum = 0.0;
        let mut c = 0.0;
        for sample in &values {
            (sum, c) = kahan_inc(sample.value, sum, c);
        }
        let result = sum + c;

        // Expected bits generated from Go harness
        let expected_bits: u64 = 4607182418800017408;

        assert_eq!(
            result.to_bits(),
            expected_bits,
            "result={}, bits={}, expected_bits={}",
            result,
            result.to_bits(),
            expected_bits
        );
    }

    #[rstest]
    #[case(vec![1e16, 1.0, -1e16], 1.0, "catastrophic cancellation")]
    #[case(vec![1e10, 1.0, 1.0, 1.0, 1.0, 1.0, -1e10], 5.0, "small values lost in large sum")]
    #[case(vec![1e8, 1.0, -1e8, 1.0, 1e8, 1.0, -1e8], 3.0, "alternating magnitudes")]
    #[case(vec![1.0, 1e100, 1.0, -1e100], 2.0, "Neumaier improvement case")]
    #[case(vec![0.1; 10], 1.0, "repeated small values")]
    #[case(vec![1e10, 1e5, 1e0, 1e-5, 1e-10], 1e10 + 1e5 + 1.0 + 1e-5 + 1e-10, "decreasing magnitude"
    )]
    #[case(vec![1e-10, 1e-5, 1e0, 1e5, 1e10], 1e10 + 1e5 + 1.0 + 1e-5 + 1e-10, "increasing magnitude"
    )]
    #[case(vec![1e16, -1e16, 1e16, -1e16, 1.0], 1.0, "near-zero with large intermediates")]
    #[case(vec![1e-100; 1000], 1e-97, "very small repeated values")]
    #[case(vec![1.0, -2.0, 3.0, -4.0, 5.0], 3.0, "mixed signs")]
    fn should_handle_kahan_edge_cases(
        #[case] values: Vec<f64>,
        #[case] expected: f64,
        #[case] description: &str,
    ) {
        let mut sum = 0.0;
        let mut c = 0.0;
        for &val in &values {
            (sum, c) = kahan_inc(val, sum, c);
        }
        let result = sum + c;

        // Use relative error for large values, absolute error for small values
        let tolerance = if expected.abs() > 1.0 {
            expected.abs() * 1e-10
        } else {
            1e-10
        };

        assert!(
            (result - expected).abs() <= tolerance,
            "Failed case '{}': expected {}, got {}, error {}",
            description,
            expected,
            result,
            (result - expected).abs()
        );
    }

    #[test]
    fn should_handle_nan_in_max_over_time() {
        // Test that NaN is replaced by subsequent values (Prometheus behavior)
        let samples = vec![create_eval_samples(
            vec![(1000, f64::NAN), (2000, 5.0)],
            Labels::default(),
        )];

        let result = call_range_function("max_over_time", samples, 2000);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 5.0, "NaN should be replaced by 5.0");
    }

    #[test]
    fn should_handle_nan_in_min_over_time() {
        // Test that NaN is replaced by subsequent values (Prometheus behavior)

        let samples = vec![create_eval_samples(
            vec![(1000, f64::NAN), (2000, 5.0)],
            Labels::default(),
        )];

        let result = call_range_function("min_over_time", samples, 2000);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 5.0, "NaN should be replaced by 5.0");
    }

    #[test]
    fn should_match_prometheus_all_nan_max() {
        // Test that all-NaN returns NaN (not -inf from fold)

        let samples = vec![create_eval_samples(
            vec![(1000, f64::NAN), (2000, f64::NAN)],
            Labels::default(),
        )];

        let result = call_range_function("max_over_time", samples, 2000);

        assert_eq!(result.len(), 1);
        assert!(
            result[0].value.is_nan(),
            "All-NaN should return NaN, got {}",
            result[0].value
        );
    }

    #[test]
    fn should_match_prometheus_all_nan_min() {
        // Test that all-NaN returns NaN (not +inf from fold)
        let samples = vec![create_eval_samples(
            vec![(1000, f64::NAN), (2000, f64::NAN)],
            Labels::default(),
        )];

        let result = call_range_function("min_over_time", samples, 2000);

        assert_eq!(result.len(), 1);
        assert!(
            result[0].value.is_nan(),
            "All-NaN should return NaN, got {}",
            result[0].value
        );
    }

    #[test]
    fn should_apply_rate_zero_point_counter_guard() {
        // given
        let func = get_range_function("rate").unwrap();

        let samples = vec![create_eval_samples(
            vec![(1000, 1.0), (4000, 10.0)],
            HashMap::new(),
        )];

        // when
        // Window: [0s, 5s], eval at 5s
        let result = func
            .apply_with_range(samples, 5000, Duration::from_secs(5))
            .unwrap()
            .expect_instant_vector("instant_vector_expected");

        // then
        assert_eq!(result.len(), 1);

        // increase = 9.0
        // sampled interval = 3.0s, avg_duration_between_samples = 3s, threshold = 3.3s
        // duration_to_start = 1.0s (< 3.3s, extrapolate to boundary)
        // duration_to_zero = 3.0 * (1.0 / (10.0 - 1.0)) = 1/3
        // duration_to_zero (1/3) < duration_to_start (1.0), so set duration_to_start = 1/3
        // duration_to_end = 1.0s (< 3.3 so unchanged)
        // factor = (3.0 + 1/3 + 1.0) / 3.0 / 5.0
        let expected = 9.0 * (3.0 + (1.0 / 3.0) + 1.0) / 3.0 / 5.0;
        assert!(
            (result[0].value - expected).abs() < 1e-10,
            "expected {}, got {}",
            expected,
            result[0].value
        );
    }

    #[test]
    fn should_skip_zero_interval_series_in_rate() {
        // given
        let func = get_range_function("rate").unwrap();

        // Two samples at the same timestamp (sampled_interval = 0), so series skipped
        let samples = vec![create_eval_samples(
            vec![(1000, 10.0), (1000, 20.0)],
            HashMap::new(),
        )];

        // when
        let result = func
            .apply_with_range(samples, 2000, Duration::from_millis(2000))
            .unwrap()
            .expect_instant_vector("instant_vector_expected");

        // then
        assert!(result.is_empty());
    }

    // Property-based tests using proptest
    mod proptests {
        use super::*;
        use proptest::prelude::*;

        /// Generate finite f64 values (no NaN, no infinity)
        fn finite_f64() -> impl Strategy<Value = f64> {
            prop::num::f64::NORMAL
        }

        proptest! {
            /// Test that min_over_time returns the actual minimum
            #[test]
            fn min_over_time_returns_minimum(
                values in prop::collection::vec(finite_f64(), 1..100)
            ) {
                let samples = vec![create_eval_samples(
                    values.iter().enumerate().map(|(i, &v)| ((i as i64) * 1000, v)).collect(),
                    Labels::default(),
                )];

                let result = call_range_function("min_over_time", samples, 0);

                let computed_min = result[0].value;
                let expected_min = values.iter().copied().fold(f64::INFINITY, f64::min);

                assert_eq!(computed_min, expected_min, "min_over_time should return exact minimum");
            }

            /// Test that max_over_time returns the actual maximum
            #[test]
            fn max_over_time_returns_maximum(
                values in prop::collection::vec(finite_f64(), 1..100)
            ) {
                let samples = vec![create_eval_samples(
                    values.iter().enumerate().map(|(i, &v)| ((i as i64) * 1000, v)).collect(),
                    Labels::default(),
                )];

                let result = call_range_function("max_over_time", samples, 0);

                let computed_max = result[0].value;
                let expected_max = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);

                assert_eq!(computed_max, expected_max, "max_over_time should return exact maximum");
            }

            /// Test that count_over_time returns the correct count
            #[test]
            fn count_over_time_returns_count(
                values in prop::collection::vec(finite_f64(), 1..100)
            ) {
                let samples = vec![create_eval_samples(
                    values.iter().enumerate().map(|(i, &v)| ((i as i64) * 1000, v)).collect(),
                    Labels::default(),
                )];


                let result = call_range_function("count_over_time", samples, 0);

                let computed_count = result[0].value;
                let expected_count = values.len() as f64;

                assert_eq!(computed_count, expected_count, "count_over_time should return exact count");
            }

            /// Test that stddev_over_time computes correct standard deviation
            #[test]
            fn stddev_over_time_computes_correctly(
                values in prop::collection::vec(finite_f64(), 2..100)
            ) {
                let eval_samples = vec![create_eval_samples(
                    values.iter().enumerate().map(|(i, &v)| ((i as i64) * 1000, v)).collect(),
                    Labels::default(),
                )];

                let result = call_range_function("stddev_over_time", eval_samples, 0);
                let computed_stddev = result[0].value;

                // Independent two-pass algorithm (stable baseline)
                let mean = values.iter().sum::<f64>() / values.len() as f64;
                let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
                let expected_stddev = variance.sqrt();

                // Skip if overflow occurred
                if computed_stddev.is_infinite() || expected_stddev.is_infinite() {
                    return Ok(());
                }

                // Allow small relative error due to numerical differences
                let rel_error = ((computed_stddev - expected_stddev) / expected_stddev.max(1e-10)).abs();
                prop_assert!(
                    rel_error < 1e-10 || (computed_stddev - expected_stddev).abs() < 1e-10,
                    "stddev_over_time error too large: computed={}, expected={}, rel_error={}",
                    computed_stddev, expected_stddev, rel_error
                );
            }

            /// Test that stdvar_over_time computes correct variance
            #[test]
            fn stdvar_over_time_computes_correctly(
                values in prop::collection::vec(finite_f64(), 2..100)
            ) {
                let eval_samples = vec![create_eval_samples(
                    values.iter().enumerate().map(|(i, &v)| ((i as i64) * 1000, v)).collect(),
                    Labels::default(),
                )];

                let result = call_range_function("stdvar_over_time", eval_samples, 0);
                let computed_variance = result[0].value;

                // Independent two-pass algorithm (stable baseline)
                let mean = values.iter().sum::<f64>() / values.len() as f64;
                let expected_variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;

                // Skip if overflow occurred
                if computed_variance.is_infinite() || expected_variance.is_infinite() {
                    return Ok(());
                }

                // Allow small relative error due to numerical differences
                let rel_error = ((computed_variance - expected_variance) / expected_variance.max(1e-10)).abs();
                prop_assert!(
                    rel_error < 1e-10 || (computed_variance - expected_variance).abs() < 1e-10,
                    "stdvar_over_time error too large: computed={}, expected={}, rel_error={}",
                    computed_variance, expected_variance, rel_error
                );
            }

            /// Test stddev with extremely close values (catastrophic cancellation scenario)
            /// Values are base ± small_delta where base >> small_delta
            #[test]
            fn stddev_handles_extremely_close_values(
                base in 1e10_f64..1e14_f64,  // Limit base to avoid extreme precision loss
                small_deltas in prop::collection::vec(-10.0_f64..10.0_f64, 3..20)  // At least 3 values
            ) {
                // Create values: base + delta for each delta
                let values: Vec<f64> = small_deltas.iter().map(|&d| base + d).collect();

                let samples = vec![create_eval_samples(
                    values.iter().enumerate().map(|(i, &v)| ((i as i64) * 1000, v)).collect(),
                    Labels::default(),
                )];

                let result = call_range_function("stddev_over_time", samples, 0);

                let computed_stddev = result[0].value;

                // Compute expected stddev from the deltas (more numerically stable)
                let delta_mean = small_deltas.iter().sum::<f64>() / small_deltas.len() as f64;
                let delta_variance = small_deltas.iter().map(|d| (d - delta_mean).powi(2)).sum::<f64>() / small_deltas.len() as f64;
                let expected_stddev = delta_variance.sqrt();

                // Skip if overflow occurred or variance is too small
                if computed_stddev.is_infinite() || expected_stddev.is_infinite() || expected_stddev < 1e-10 {
                    return Ok(());
                }

                // For catastrophic cancellation scenarios, Welford's algorithm should maintain
                // reasonable accuracy. We expect relative error < 1% for well-conditioned cases.
                // The condition number is roughly base/stddev, so we scale tolerance accordingly.
                let condition_number = base / expected_stddev.max(1.0);
                let tolerance = if condition_number > 1e12 {
                    0.1  // Very ill-conditioned: 10% tolerance
                } else if condition_number > 1e10 {
                    0.01  // Ill-conditioned: 1% tolerance
                } else {
                    0.001  // Well-conditioned: 0.1% tolerance
                };

                let rel_error = ((computed_stddev - expected_stddev) / expected_stddev).abs();
                prop_assert!(
                    rel_error < tolerance,
                    "stddev_over_time failed for extremely close values: base={}, computed={}, expected={}, rel_error={}, tolerance={}, condition_number={}",
                    base, computed_stddev, expected_stddev, rel_error, tolerance, condition_number
                );
            }
        }
    }
}
