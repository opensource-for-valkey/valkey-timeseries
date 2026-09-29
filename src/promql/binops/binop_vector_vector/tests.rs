use super::*;
use promql_parser::parser::token::{T_ADD, T_DIV, T_GTR, T_NEQ, TokenType};
use promql_parser::parser::{BinModifier, BinaryExpr, Expr, NumberLiteral, VectorMatchFillValues};

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
            BinModifier::default().with_fill_values(VectorMatchFillValues::default().with_rhs(0.0)),
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
            BinModifier::default().with_fill_values(VectorMatchFillValues::default().with_lhs(1.0)),
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
    let staging =
        find_sample(&result, "staging").expect("staging sample missing (fill_left should emit it)");
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
            BinModifier::default().with_fill_values(VectorMatchFillValues::default().with_rhs(0.0)),
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
            BinModifier::default().with_fill_values(VectorMatchFillValues::default().with_rhs(5.0)),
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
            BinModifier::default().with_fill_values(VectorMatchFillValues::default().with_rhs(0.0)),
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
    let staging = find_sample(&result, "staging").expect("staging should be emitted via NaN fill");
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
            BinModifier::default().with_fill_values(VectorMatchFillValues::default().with_rhs(0.0)),
        ),
    );

    let result = eval_binop_vector_vector(&expr, lhs, rhs)
        .unwrap()
        .into_instant_vector()
        .unwrap();

    let prod = find_sample(&result, "prod").expect("prod should be emitted");
    assert_eq!(prod.value, f64::INFINITY, "10 / fill(0) should be +Inf");
}

// ── bool modifier, no other modifier ─────────────────────────────────────────────

#[test]
fn test_bool_without_matching_omits_unmatched_lhs() {
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
fn test_bool_without_matching_true_and_false() {
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

// ── no-modifier cardinality ───────────────────────────────────────────────
//
// Without a modifier, one-to-one is enforced exactly as with `on` or
// `ignoring`. The realistic trigger is a bare selector matching several
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
fn test_no_modifier_duplicate_rhs_errors() {
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
fn test_no_modifier_duplicate_lhs_errors() {
    let lhs = vec![
        sample(1000, 1.0, &[("__name__", "a"), ("env", "prod")]),
        sample(1000, 2.0, &[("__name__", "b"), ("env", "prod")]),
    ];
    let rhs = vec![sample(1000, 3.0, &[("__name__", "c"), ("env", "prod")])];
    assert_many_side_duplicate(eval_binop_vector_vector(&make_expr(T_ADD, None), lhs, rhs));
}

#[test]
fn test_no_modifier_duplicate_errors_even_when_comparison_is_false() {
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
fn test_no_modifier_unmatched_duplicates_are_ignored() {
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
                Some(group_left().with_fill_values(VectorMatchFillValues::default().with_lhs(0.0))),
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
                    group_right().with_fill_values(VectorMatchFillValues::default().with_rhs(0.0)),
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

    let err = eval_binop_vector_vector(&expr, lhs, rhs)
        .expect_err("ambiguous one-to-one match must error even though both sides compare true");
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

    let err = eval_binop_vector_vector(&expr, lhs, rhs)
        .expect_err("ambiguous one-to-one match must error even though both sides compare false");
    assert!(
        err.to_string()
            .contains("many-to-one matching must be explicit"),
        "unexpected error: {err}"
    );
}

// ── label projection, grouping copies, set operators ─────────────────────

fn label_list(names: &[&str]) -> promql_parser::label::Labels {
    promql_parser::label::Labels::new(names.to_vec())
}

fn on(names: &[&str]) -> BinModifier {
    BinModifier::default().with_matching(Some(LabelModifier::Include(label_list(names))))
}

fn ignoring(names: &[&str]) -> BinModifier {
    BinModifier::default().with_matching(Some(LabelModifier::Exclude(label_list(names))))
}

fn group_left(modifier: BinModifier, names: &[&str]) -> BinModifier {
    modifier.with_card(VectorMatchCardinality::ManyToOne(label_list(names)))
}

fn group_right(modifier: BinModifier, names: &[&str]) -> BinModifier {
    modifier.with_card(VectorMatchCardinality::OneToMany(label_list(names)))
}

fn eval(
    op: u16,
    modifier: Option<BinModifier>,
    lhs: Vec<EvalSample>,
    rhs: Vec<EvalSample>,
) -> EvalResult<Vec<EvalSample>> {
    eval_binop_vector_vector(&make_expr(op, modifier), lhs, rhs)
        .map(|r| r.into_instant_vector().expect("an instant vector"))
}

/// Result label sets, sorted, so a test can compare the whole result.
fn all_labels(result: &[EvalSample]) -> Vec<Vec<(String, String)>> {
    let mut all: Vec<_> = result.iter().map(exact_labels).collect();
    all.sort();
    all
}

#[test]
fn test_one_to_one_on_keeps_only_the_listed_labels() {
    use promql_parser::parser::token::T_SUB;
    let lhs = vec![sample(
        1000,
        10.0,
        &[("__name__", "a"), ("env", "prod"), ("host", "h1")],
    )];
    let rhs = vec![sample(
        1000,
        3.0,
        &[("__name__", "b"), ("env", "prod"), ("zone", "z")],
    )];
    let result = eval(T_SUB, Some(on(&["env"])), lhs, rhs).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].value, 7.0);
    assert_eq!(exact_labels(&result[0]), pairs(&[("env", "prod")]));
}

#[test]
fn test_one_to_one_ignoring_removes_the_listed_labels_and_the_name() {
    use promql_parser::parser::token::T_SUB;
    let lhs = vec![sample(
        1000,
        10.0,
        &[("__name__", "a"), ("env", "prod"), ("host", "h1")],
    )];
    let rhs = vec![sample(
        1000,
        3.0,
        &[("__name__", "b"), ("env", "prod"), ("host", "h2")],
    )];
    let result = eval(T_SUB, Some(ignoring(&["host"])), lhs, rhs).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].value, 7.0);
    assert_eq!(exact_labels(&result[0]), pairs(&[("env", "prod")]));
}

/// `group_left(x)` sets `x` from the "one" side and keeps every other label
/// of the "many" side.
#[test]
fn test_group_left_copies_a_listed_label_from_the_one_side() {
    use promql_parser::parser::token::T_MUL;
    let lhs = vec![sample(1000, 2.0, &[("env", "prod"), ("host", "h1")])];
    let rhs = vec![sample(
        1000,
        3.0,
        &[("env", "prod"), ("x", "1"), ("y", "2")],
    )];
    let modifier = group_left(on(&["env"]), &["x"]);
    let result = eval(T_MUL, Some(modifier), lhs, rhs).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].value, 6.0);
    assert_eq!(
        exact_labels(&result[0]),
        pairs(&[("env", "prod"), ("host", "h1"), ("x", "1")])
    );
}

/// A listed label the "one" side lacks is deleted from the result, as
/// Prometheus' `resultMetric` does, rather than kept from the "many" side.
#[test]
fn test_group_left_deletes_a_listed_label_the_one_side_lacks() {
    use promql_parser::parser::token::T_MUL;
    let lhs = vec![sample(
        1000,
        2.0,
        &[("env", "prod"), ("host", "h1"), ("x", "old")],
    )];
    let rhs = vec![sample(1000, 3.0, &[("env", "prod")])];
    let modifier = group_left(on(&["env"]), &["x"]);
    let result = eval(T_MUL, Some(modifier), lhs, rhs).unwrap();
    assert_eq!(
        exact_labels(&result[0]),
        pairs(&[("env", "prod"), ("host", "h1")])
    );
}

/// Two "many" series that `group_left(x)` makes identical collide only if a
/// comparison keeps both: the uniqueness check runs after the filter.
#[test]
fn test_grouped_uniqueness_is_checked_after_comparison_filtering() {
    let many = |a: f64, b: f64| {
        vec![
            sample(1000, a, &[("env", "prod"), ("x", "a")]),
            sample(1000, b, &[("env", "prod"), ("x", "b")]),
        ]
    };
    let one = || vec![sample(1000, 3.0, &[("env", "prod"), ("x", "c")])];
    let modifier = || Some(group_left(on(&["env"]), &["x"]));

    // 5 > 3 passes and 1 > 3 does not: one result, no collision.
    let result = eval(T_GTR, modifier(), many(5.0, 1.0), one()).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].value, 5.0);
    assert_eq!(
        exact_labels(&result[0]),
        pairs(&[("env", "prod"), ("x", "c")])
    );

    // Both pass: two results labelled {env="prod", x="c"}.
    let err = eval(T_GTR, modifier(), many(5.0, 4.0), one()).unwrap_err();
    assert!(
        err.to_string()
            .contains("grouping labels must ensure unique matches"),
        "{err}"
    );
}

/// Under `group_right` the fills still follow the cardinality: `fill_left`
/// stands in for a missing "many" operand (here the right) and `fill_right`
/// for a missing "one" operand (the left). The operator still sees the
/// operands in written order.
#[test]
fn test_group_right_fill_values_follow_the_sides() {
    use promql_parser::parser::token::T_SUB;
    let lhs = vec![
        sample(1000, 10.0, &[("i", "1")]),
        sample(1000, 20.0, &[("i", "2")]),
    ];
    let rhs = vec![
        sample(1000, 1.0, &[("c", "a"), ("i", "1")]),
        sample(1000, 3.0, &[("c", "b"), ("i", "3")]),
    ];
    let modifier = group_right(on(&["i"]), &[]).with_fill_values(
        VectorMatchFillValues::default()
            .with_lhs(100.0)
            .with_rhs(1000.0),
    );
    let result = eval(T_SUB, Some(modifier), lhs, rhs).unwrap();
    let mut got: Vec<_> = result.iter().map(|s| (exact_labels(s), s.value)).collect();
    got.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        got,
        vec![
            // Matched: 10 - 1.
            (pairs(&[("c", "a"), ("i", "1")]), 9.0),
            // No left series for i="3": fill_right(1000) - 3.
            (pairs(&[("c", "b"), ("i", "3")]), 997.0),
            // No right series for i="2": 20 - fill_left(100), labelled with
            // the match labels.
            (pairs(&[("i", "2")]), -80.0),
        ]
    );
}

#[test]
fn test_and_on_keeps_left_series_with_a_partner() {
    use promql_parser::parser::token::T_LAND;
    let lhs = vec![
        sample(1000, 1.0, &[("env", "prod"), ("host", "h1")]),
        sample(1000, 2.0, &[("env", "dev"), ("host", "h2")]),
    ];
    let rhs = vec![sample(1000, 9.0, &[("env", "prod"), ("zone", "z")])];
    let result = eval(T_LAND, Some(on(&["env"])), lhs, rhs).unwrap();
    assert_eq!(
        all_labels(&result),
        vec![pairs(&[("env", "prod"), ("host", "h1")])]
    );
    assert_eq!(result[0].value, 1.0, "values come from the left");
}

#[test]
fn test_unless_ignoring_keeps_left_series_without_a_partner() {
    use promql_parser::parser::token::T_LUNLESS;
    let lhs = vec![
        sample(1000, 1.0, &[("env", "prod"), ("host", "h1")]),
        sample(1000, 2.0, &[("env", "dev"), ("host", "h2")]),
    ];
    let rhs = vec![sample(1000, 9.0, &[("env", "prod"), ("host", "h9")])];
    let result = eval(T_LUNLESS, Some(ignoring(&["host"])), lhs, rhs).unwrap();
    assert_eq!(
        all_labels(&result),
        vec![pairs(&[("env", "dev"), ("host", "h2")])]
    );
}

#[test]
fn test_or_on_adds_right_series_whose_key_the_left_lacks() {
    use promql_parser::parser::token::T_LOR;
    let lhs = vec![sample(1000, 1.0, &[("env", "prod"), ("host", "h1")])];
    let rhs = vec![
        sample(1000, 2.0, &[("env", "prod"), ("host", "h2")]),
        sample(1000, 3.0, &[("env", "dev"), ("host", "h3")]),
    ];
    let result = eval(T_LOR, Some(on(&["env"])), lhs, rhs).unwrap();
    assert_eq!(
        all_labels(&result),
        vec![
            pairs(&[("env", "dev"), ("host", "h3")]),
            pairs(&[("env", "prod"), ("host", "h1")]),
        ]
    );
}

/// An empty operand matches nothing, so a pending `__name__` drop on the
/// other stays pending even under `on(__name__)`: it is applied at the end
/// of evaluation, where an enclosing `by (__name__)` can still see it.
#[test]
fn test_empty_operand_leaves_pending_name_drops_pending() {
    use promql_parser::parser::token::T_LOR;
    let mut pending = sample(1000, 1.0, &[("__name__", "rate_src"), ("env", "prod")]);
    pending.drop_name = true;
    let result = eval(T_LOR, Some(on(&["__name__", "env"])), vec![], vec![pending]).unwrap();
    assert_eq!(result.len(), 1);
    assert!(result[0].drop_name);
    assert_eq!(result[0].labels.get("__name__"), Some("rate_src"));
}
