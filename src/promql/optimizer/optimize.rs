// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// https://github.com/apache/arrow-datafusion/tree/main/datafusion/optimizer/src
// https://github.com/apache/arrow-datafusion/blob/e222bd627b6e7974133364fed4600d74b4da6811/datafusion/optimizer/src/utils.rs

use crate::parser::ParseResult;
use crate::promql::optimizer::const_folding::fold_constants;
use crate::promql::optimizer::pushdown::pushdown_filters_in_place;
use crate::promql::optimizer::utils::{
    expr_contains, is_null, is_number, is_one, is_op_with, is_zero,
};
use promql_parser::label::{Matcher, Matchers};
use promql_parser::parser::token::{T_ADD, T_DIV, T_LAND, T_LOR, T_MUL, TokenType};
use promql_parser::parser::{BinaryExpr, Expr, ParenExpr};
// https://prometheus.io/docs/prometheus/latest/querying/operators
// Expression optimization API

/// Optimizes this [`Expr`]`s as much as possible, evaluating
/// constants and applying simplifications.
///
/// The types of the expression must match what operators expect,
/// or else an error may occur trying to evaluate.
///
/// # Example:
///
/// `b > 2 AND b > 2`
///
/// can be written to
///
/// `b > 2`
///
pub fn optimize_expr(expr: Expr) -> ParseResult<Expr> {
    const MAX_OPTIMIZATION_PASSES: usize = 4;

    let mut expr = expr;
    for _ in 0..MAX_OPTIMIZATION_PASSES {
        let previous = expr.clone();
        expr = optimize_internal(fold_constants(expr));
        if expr == previous {
            break;
        }
    }

    // push down filters
    pushdown_filters_in_place(&mut expr);
    Ok(expr)
}

/// Apply algebraic optimization rules to a binary expression.
///
/// This covers constant folding (both sides are number literals) and
/// operator-specific rewrites such as `A + 0 → A`, `A + A → A * 2`,
/// `A OR (A AND B) → A`, `A * 1 → A`, `A / 1 → A`, etc.
fn optimize_binary(binary: BinaryExpr) -> Expr {
    let BinaryExpr {
        lhs,
        rhs,
        op,
        modifier,
    } = binary;

    let left = optimize_internal(*lhs);
    let right = optimize_internal(*rhs);
    match op.id() {
        //
        // Rules for Add
        //

        // A + 0 --> A
        T_ADD if is_zero(&right) => left,

        // 0 + A --> A
        T_ADD if is_zero(&left) => right,

        // A + A --> A * 2
        // Our use case envisions that this expression involving metric selectors
        // will need to make network calls to evaluate. If both sides are the same
        // we can optimize by multiplying by 2 and only making one network call.
        // Guard: only safe when no matching modifier is present; a modifier such as
        // `on(label)` or `group_left()` is only meaningful for vector–vector ops and
        // cannot be mapped onto the resulting vector–scalar multiplication.
        T_ADD
            if left == right
                && modifier.is_none()
                && matches!(
                    &left,
                    Expr::VectorSelector(_) | Expr::Subquery(_) | Expr::Aggregate(_)
                ) =>
        {
            let two = Expr::from(2.0);
            Expr::Binary(BinaryExpr {
                rhs: Box::new(two),
                lhs: Box::new(left),
                op: TokenType::new(T_MUL),
                modifier,
            })
        }

        // Rules for OR
        //
        // (..A..) OR A --> (..A..)
        T_LOR if expr_contains(&left, &right, TokenType::new(T_LOR)) => left,
        // A OR (A AND B) --> A
        T_LOR if is_op_with(TokenType::new(T_LAND), &right, &left) => left,
        // (A AND B) OR A --> A
        T_LOR if is_op_with(TokenType::new(T_LAND), &left, &right) => right,

        //
        // Rules for AND
        //
        // (..A..) AND A --> ..A..  (unwrap a single-level Paren if present)
        T_LAND if expr_contains(&left, &right, TokenType::new(T_LAND)) => {
            // if both sides are identical (e.g., `(A) AND (A)`), preserve the original
            // wrapper by returning lhs as-is. Otherwise, unwrap a single-level Paren.
            if left == right {
                left
            } else {
                match left {
                    Expr::Paren(p) => *p.expr,
                    other => other,
                }
            }
        }
        // A AND (..A..) --> A..
        T_LAND if expr_contains(&right, &left, TokenType::new(T_LAND)) => {
            if left == right {
                right
            } else {
                match right {
                    Expr::Paren(p) => *p.expr,
                    other => other,
                }
            }
        }
        // A AND (A OR B) --> A
        T_LAND if is_op_with(TokenType::new(T_LOR), &right, &left) => {
            if left == right {
                left
            } else {
                match left {
                    Expr::Paren(p) => *p.expr,
                    other => other,
                }
            }
        }
        // (A OR B) AND A --> A
        T_LAND if is_op_with(TokenType::new(T_LOR), &left, &right) => {
            if left == right {
                right
            } else {
                match right {
                    Expr::Paren(p) => *p.expr,
                    other => other,
                }
            }
        }

        //
        // Rules for Mul
        //
        // A * 1 --> A
        T_MUL if is_one(&right) => left,
        // 1 * A --> A
        T_MUL if is_one(&left) => right,

        //
        // Rules for Div
        //
        // A / 1 --> A
        T_DIV if is_one(&right) => left,
        // A / A --> NAN if A.is_nan() else 1.0. The NaN comparison can be valid for
        // NumberLiteral, but not for VectorSelector, Subquery, Aggregation, etc.
        T_DIV if left == right && is_number(&left) => {
            if is_null(&right) {
                Expr::from(f64::NAN)
            } else {
                Expr::from(1.0)
            }
        }
        //
        // Rules for Mod
        //
        // no additional rewrites possible
        _ => Expr::Binary(BinaryExpr {
            lhs: Box::new(left),
            rhs: Box::new(right),
            op,
            modifier,
        }),
    }
}

/// Simplifies [`Expr`]s by applying algebraic transformation rules
///
/// Example transformations that are applied:
/// * `expr == bool 1` and `expr != false` to `expr` when `expr` is of boolean type
/// * `expr != true` to `!expr` when `expr` is of boolean type
/// * `1 == bool 1` to `1`
/// * `0 == bool 1` to `0`
/// * `expr == NaN` and `expr != NaN` to `NaN`
fn optimize_internal(expr: Expr) -> Expr {
    match expr {
        Expr::Paren(p) => {
            // Determine whether the inner expression was originally a Binary
            // so we can decide whether to preserve the Paren after optimization.
            let was_binary = matches!(&*p.expr, Expr::Binary(_));
            let inner = optimize_internal(*p.expr);
            // Strip the Paren wrapper only when the inner was a Binary that
            // simplified to a leaf (e.g., `(A + 0)` → `A`). In all other
            // cases—unchanged Binaries, VectorSelectors with `@` modifiers,
            // Aggregates, Calls, etc.—preserve the Paren so that simplification
            // rules can inspect and unwrap it, and so that operator precedence
            // is preserved in the output.
            if was_binary && !matches!(&inner, Expr::Binary(_)) {
                inner
            } else {
                match inner {
                    Expr::Paren(_) => inner,
                    _ => Expr::Paren(ParenExpr {
                        expr: Box::new(inner),
                    }),
                }
            }
        }
        Expr::Subquery(mut sq) => {
            *sq.expr = optimize_internal(*sq.expr);
            Expr::Subquery(sq)
        }
        Expr::Aggregate(mut agg) => {
            *agg.expr = optimize_internal(*agg.expr);
            if let Some(param) = agg.param {
                let mut param = param;
                *param = optimize_internal(*param);
                agg.param = Some(param);
            }
            Expr::Aggregate(agg)
        }
        Expr::Call(mut call) => {
            call.args.args = call
                .args
                .args
                .into_iter()
                .map(|mut arg| {
                    *arg = optimize_internal(*arg);
                    arg
                })
                .collect();
            Expr::Call(call)
        }
        Expr::Unary(mut unary) => {
            *unary.expr = optimize_internal(*unary.expr);
            Expr::Unary(unary)
        }
        Expr::VectorSelector(mut vs) => {
            // Simplify any label matchers within the vector selector
            vs.matchers = dedupe_matchers(vs.matchers);
            Expr::VectorSelector(vs)
        }
        Expr::MatrixSelector(mut mst) => {
            mst.vs.matchers = dedupe_matchers(mst.vs.matchers);
            Expr::MatrixSelector(mst)
        }
        Expr::Binary(binary) => optimize_binary(binary),
        expr => expr,
    }
}

/// Remove duplicate `(op, name, value)` matchers from a parser
/// [`Matchers`] while preserving first-occurrence order.
fn dedupe_matchers(matchers: Matchers) -> Matchers {
    let Matchers {
        matchers: mut existing,
        or_matchers,
    } = matchers;

    // In-place stable dedupe to avoid allocating a second matcher buffer.
    // Keep the first occurrence of each structural matcher.
    // NOTE: this is O(n^2) in the number of matchers, but the number of matchers is expected to be small.
    let mut unique_len = 0;
    for idx in 0..existing.len() {
        if existing[..unique_len]
            .iter()
            .any(|seen| matcher_eq(seen, &existing[idx]))
        {
            continue;
        }

        if idx != unique_len {
            existing.swap(unique_len, idx);
        }
        unique_len += 1;
    }

    existing.truncate(unique_len);

    Matchers {
        matchers: existing,
        or_matchers,
    }
}

/// Structural matcher equality. `MatchOp` already derives `PartialEq` in
/// promql-parser 0.8 (regex compares on source string), so `Matcher`'s
/// derived `PartialEq` is a complete structural check. Helper kept as a
/// one-liner for readability / to isolate the dependency on the derived
/// impl in case promql-parser semantics shift.
fn matcher_eq(a: &Matcher, b: &Matcher) -> bool {
    a == b
}

#[cfg(test)]
mod tests {
    use super::*;
    use promql_parser::label::MatchOp;

    fn parse(expr: &str) -> Expr {
        promql_parser::parser::parse(expr).unwrap()
    }
    fn assert_expr_eq(expected: &Expr, actual: &Expr) {
        assert_eq!(
            expected, actual,
            "expected: \n{}\n but got: \n{}",
            expected, actual
        );
    }

    fn assert_string_expr_eq(expr: &str, expected: Expr) {
        let expr = parse(expr);
        let actual = simplify(expr);
        assert_eq!(
            expected, actual,
            "expected: \n{}\n but got: \n{}",
            expected, actual
        );
    }

    fn assert_string_simplify(expr: &str, expected: &str) {
        let expr = parse(expr);
        let expected_expr = parse(expected);
        let actual = simplify(expr);
        assert_eq!(
            expected_expr, actual,
            "expected: \n{}\n but got: \n{}",
            expected, actual
        );
    }

    // ------------------------------
    // --- ExprSimplifier tests -----
    // ------------------------------
    #[test]
    fn api_basic() {
        assert_string_simplify("1.0 + 2.0", "3.0");
    }

    #[test]
    fn test_dedupe_matchers_keeps_first_occurrence_order() {
        let a = Matcher::new(MatchOp::Equal, "job", "api");
        let b = Matcher::new(MatchOp::Equal, "env", "prod");
        let c = Matcher::new(MatchOp::NotEqual, "zone", "us-east");

        let matchers = Matchers {
            matchers: vec![a.clone(), b.clone(), a.clone(), c.clone(), b.clone()],
            or_matchers: vec![],
        };

        let deduped = dedupe_matchers(matchers);

        assert_eq!(deduped.matchers, vec![a, b, c]);
        assert!(deduped.or_matchers.is_empty());
    }

    #[test]
    fn test_dedupe_matchers_preserves_or_matchers() {
        let and_matcher = Matcher::new(MatchOp::Equal, "job", "api");
        let or_left = Matcher::new(MatchOp::Equal, "instance", "a");
        let or_right = Matcher::new(MatchOp::Equal, "instance", "b");

        let or_matchers = vec![vec![or_left.clone()], vec![or_right.clone()]];
        let matchers = Matchers {
            matchers: vec![and_matcher.clone(), and_matcher.clone()],
            or_matchers: or_matchers.clone(),
        };

        let deduped = dedupe_matchers(matchers);

        assert_eq!(deduped.matchers, vec![and_matcher]);
        assert_eq!(deduped.or_matchers, or_matchers);
    }

    #[test]
    fn test_dedupe_matchers_in_expr_vector_selector() {
        assert_string_simplify(
            "http_requests_total{job=\"api\",job=\"api\",env=\"prod\"}",
            "http_requests_total{job=\"api\",env=\"prod\"}",
        );
    }

    #[test]
    fn test_dedupe_matchers_in_expr_matrix_selector_nested_call() {
        assert_string_simplify(
            "sum(rate(http_requests_total{job=\"api\",job=\"api\"}[5m]))",
            "sum(rate(http_requests_total{job=\"api\"}[5m]))",
        );
    }

    #[test]
    fn simplify_and_constant_prop() {
        // should be able to simplify to false
        // (6 * (1 - 2)) > 0
        assert_string_simplify("(6.0 * (1.0 - 2.0)) > bool 0.0", "0.0");
    }

    // ------------------------------
    // --- Simplifier tests -----
    // ------------------------------

    #[test]
    fn test_simplify_or_same() {
        assert_string_simplify("c2 or c2", "c2");
    }

    #[test]
    fn test_simplify_and_same() {
        assert_string_simplify("c2 and c2", "c2");
    }

    #[test]
    fn test_simplify_selector_plus_selector_same() {
        let expr = "c2 + c2";
        let expected = "c2 * 2.0";
        assert_string_simplify(expr, expected);
    }

    #[test]
    fn test_simplify_mul_by_one() {
        assert_string_simplify("c2 * 1.0", "c2");
        assert_string_simplify("1.0 * c2", "c2");

        assert_string_simplify("45.0 * 1.0", "45.0");
        assert_string_simplify("1.0 * 89.0", "89.0");
    }

    #[test]
    fn test_simplify_mul_by_nan() {
        // Scalar * NaN / NaN * Scalar are handled by const_simplify (both NumberLiterals)
        assert_string_simplify("5.0 * NaN", "NaN");
        assert_string_simplify("NaN * 5.0", "NaN");
        // Vector * NaN must NOT fold — it would change the expression type from
        // instant vector to scalar, breaking downstream aggregations.
        assert_string_simplify("c2 * NaN", "c2 * NaN");
        assert_string_simplify("NaN * c2", "NaN * c2");
    }

    #[test]
    fn test_simplify_add_zero() {
        // 0 + A --> A, where A is numeric
        assert_string_simplify("0.0 + 5.0", "5.0");

        // 0 + A --> A
        // Only simplify when A is numeric
        assert_string_simplify("0.0 + c2", "c2");

        // A + 0 --> A if A
        // Only simplify when A is numeric
        assert_string_simplify("foo + 0.0", "foo");
    }

    #[test]
    fn test_simplify_mul_by_zero() {
        // 0 * A --> 0
        {
            // should remain unchanged if A is not numeric
            assert_string_simplify("0.0 * c2", "0.0 * c2");

            // should return 0.0 if it is numeric
            assert_string_simplify("0.0 * 12.5", "0.0");
        }

        // A * 0 --> 0
        {
            // should remain unchanged for non-numeric A
            let expr = "foo * 0.0";
            assert_string_simplify(expr, expr);

            let expr = "0.0 * 65.4";
            assert_string_simplify(expr, "0.0");
        }
    }

    #[test]
    fn test_simplify_div_by_one() {
        // A / 1 = A
        // should remain unchanged for non-numeric A
        assert_string_simplify("c2 / 1.0", "c2");

        // return A for numeric A
        assert_string_simplify("42.0 / 1.0", "42.0");
    }

    #[test]
    fn test_simplify_div_nan() {
        // Scalar ÷ Scalar NaN cases are handled by const_simplify (both NumberLiterals)
        assert_string_simplify("NaN / NaN", "NaN");
        // Vector / NaN and NaN / Vector must NOT fold — type change from instant
        // vector to scalar would break downstream operators such as sum() or rate().
        assert_string_simplify("c1 / NaN", "c1 / NaN");
        assert_string_simplify("NaN / c2", "NaN / c2");
    }

    #[test]
    fn test_simplify_div_zero_by_zero() {
        // 0 / 0 -> NAN
        let expr = "0.0 / 0.0";
        assert_string_simplify(expr, "NaN");
    }

    #[test]
    fn test_simplify_div_by_zero() {
        // Scalar / 0 is folded by the NumberLiteral early-return (apply_binary_op returns NaN)
        assert_string_simplify("5.0 / 0.0", "NaN");
        // Vector / 0 must NOT fold — replacing an instant vector with scalar NaN
        // changes the result type and breaks any aggregation that wraps this expression.
        assert_string_simplify("c2 / 0.0", "c2 / 0.0");
    }

    #[test]
    fn test_simplify_mod_by_nan() {
        // Scalar % NaN is handled by const_simplify (both NumberLiterals)
        assert_string_simplify("5.0 % NaN", "NaN");
        assert_string_simplify("NaN % 5.0", "NaN");
        // Vector % NaN and NaN % Vector must NOT fold — same type-change issue as MUL/DIV.
        assert_string_simplify("c2 % NaN", "c2 % NaN");
        assert_string_simplify("NaN % c2", "NaN % c2");
    }

    #[test]
    fn test_simplify_mod_by_one() {
        // test with number
        assert_string_simplify("789.0 % 1.0", "0.0");
    }

    #[test]
    fn test_simplify_simple_and() {
        // (c > 5) AND (c > 5)
        assert_string_simplify("(c > 5) AND (c > 5)", "(c > 5)");
    }

    #[test]
    fn test_simplify_composed_and() {
        // The test expects `c2` on both sides of the AND when simplifying
        let expr = "((c2 > 5) AND (c1 < 6)) AND (c2 > 5)";
        // Expect parentheses preserved on the inner operands for this test
        let expected = "(c2 > 5) AND (c1 < 6)";

        assert_string_simplify(expr, expected);
    }

    #[test]
    fn test_simplify_or_and() {
        // (c2 > 5) OR ((c1 < 6) AND (c2 > 5)) --> c2 > 5
        assert_string_simplify("(c2 > 5) OR ((c1 < 6) AND (c2 > 5))", "(c2 > 5.0)");
        assert_string_simplify("((c1 < 6.0) AND (c2 > 5.0)) OR (c2 > 5)", "(c2 > 5.0)");
    }

    #[test]
    fn test_simplify_and_or() {
        // (c2 > 5) AND ((c1 < 6) OR (c2 > 5)) --> c2 > 5
        assert_string_simplify("(c2 > 5) AND ((c1 < 6) OR (c2 > 5))", "c2 > 5.0");

        // ((c1 < 6) OR (c2 > 5)) AND (c2 > 5) --> c2 > 5
        assert_string_simplify("((c1 < 6) OR (c2 > 5)) AND (c2 > 5)", "c2 > 5.0");
    }

    #[test]
    fn test_simplify_div_nan_by_nan() {
        assert_string_simplify("NaN / NaN", "NaN");
    }

    #[test]
    fn test_simplify_simplify_arithmetic_expr() {
        assert_string_simplify("1.0 + 1.0", "2.0");
        assert_string_simplify("1.0 == bool 1.0", "1.0");
    }

    #[test]
    fn test_simplify_nested_variants() {
        assert_string_simplify("abs(c1 + 0.0)", "abs(c1)");
        assert_string_simplify("sum(c1 + 0.0)", "sum(c1)");
        assert_string_simplify("-(c1 + 0.0)", "-c1");
    }

    // ------------------------------
    // ----- Simplifier tests -------
    // ------------------------------

    fn try_simplify(expr: Expr) -> ParseResult<Expr> {
        optimize_expr(expr)
    }

    fn simplify(expr: Expr) -> Expr {
        try_simplify(expr).unwrap()
    }

    #[test]
    fn simplify_expr_nan_comparison() {
        // scalar == bool NAN is always false
        assert_string_simplify("1.0 == bool NaN", "0.0");

        assert_string_simplify("NaN == bool NaN", "0.0");

        // NAN != NAN is always 0
        assert_string_simplify("NaN != bool NaN", "1.0");

        // scalar != NAN is always 1
        assert_string_simplify("1.0 != bool NaN", "1.0");
    }

    #[test]
    fn simplify_expr_eq() {
        // true == true -> true
        assert_string_simplify("1.0 == bool 1.0", "1.0");

        // true == false -> false
        assert_string_simplify("1.0 == bool 0.0", "0.0");
    }

    #[test]
    fn simplify_expr_eq_skip_non_boolean_type() {
        // don't fold c1 == foo
        let actual = "c1 == foo";
        assert_string_simplify(actual, actual);
    }

    #[test]
    fn simplify_expr_not_eq() {
        // test constant
        assert_string_simplify("1.0 != bool 1.0", "0.0");
        assert_string_simplify("1.0 != bool 0.0", "1.0");
    }

    #[test]
    fn simplify_expr_not_eq_skip_non_boolean_type() {
        let actual = "c1 != foo";
        let expected = "c1 != foo";
        assert_string_simplify(expected, actual);
    }
}
