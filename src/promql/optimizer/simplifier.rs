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
use crate::promql::binops::apply_binary_op;
use crate::promql::functions::PromqlFunctionKind;
use crate::promql::functions::resolve_function;
use crate::promql::optimizer::const_evaluator::const_simplify;
use crate::promql::optimizer::pushdown::optimize_in_place;
use crate::promql::optimizer::utils::{
    expr_contains, is_null, is_number, is_one, is_op_with, is_zero,
};
use promql_parser::parser::token::{T_ADD, T_DIV, T_LAND, T_LOR, T_MOD, T_MUL, TokenType};
use promql_parser::parser::{BinaryExpr, Expr};
use std::ops::Deref;
// https://prometheus.io/docs/prometheus/latest/querying/operators
// Expression simplification API

/// Simplifies this [`Expr`]`s as much as possible, evaluating
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
    let expr = const_simplify(expr);

    let mut expr = simplify_internal(expr);
    // push down filters
    optimize_in_place(&mut expr);
    Ok(expr)
}

/// Simplifies [`Expr`]s by applying algebraic transformation rules
///
/// Example transformations that are applied:
/// * `expr == bool 1` and `expr != false` to `expr` when `expr` is of boolean type
/// * `expr != true` to `!expr` when `expr` is of boolean type
/// * `1 == bool 1` to `1`
/// * `0 == bool 1` to `0`
/// * `expr == NaN` and `expr != NaN` to `NaN`
fn simplify_internal(expr: Expr) -> Expr {
    match expr {
        Expr::Binary(BinaryExpr {
            lhs,
            rhs,
            op,
            modifier,
        }) => {
            if let Expr::NumberLiteral(left) = &*lhs
                && let Expr::NumberLiteral(right) = &*rhs
            {
                // properly constructed expressions (from the parser) should not panic
                let n = apply_binary_op(op, left.val, right.val).expect("binary operation failed");
                let return_bool = matches!(modifier, Some(m) if m.return_bool);
                if return_bool {
                    return Expr::from(if n.is_nan() || n == 0.0 { 0.0 } else { 1.0 });
                }
                return Expr::from(n);
            };
            match op.id() {
                //
                // Rules for Add
                //

                // A + 0 --> A
                T_ADD if is_zero(&rhs) => *lhs,

                // 0 + A --> A
                T_ADD if is_zero(&lhs) => *rhs,

                // A + A --> A * 2
                // Our use case envisions that this expression involving metric selectors
                // will need to make network calls to evaluate. If both sides are the same
                // we can optimize by multiplying by 2 and only making one network call.
                T_ADD
                    if *lhs == *rhs
                        && matches!(
                            lhs.deref(),
                            Expr::VectorSelector(_) | Expr::Subquery(_) | Expr::Aggregate(_)
                        ) =>
                {
                    let two = Expr::from(2.0);
                    Expr::Binary(BinaryExpr {
                        rhs: Box::new(two),
                        lhs,
                        op: TokenType::new(T_MUL),
                        modifier,
                    })
                }

                // Rules for OR
                //
                // (..A..) OR A --> (..A..)
                T_LOR if expr_contains(&lhs, &rhs, TokenType::new(T_LOR)) => *lhs,
                // A OR (..A..) --> (..A..)
                T_LOR if expr_contains(&rhs, &lhs, TokenType::new(T_LOR)) => *rhs,
                // A OR (A AND B) --> A
                T_LOR if is_op_with(TokenType::new(T_LAND), &rhs, &lhs) => *lhs,
                // (A AND B) OR A --> A
                T_LOR if is_op_with(TokenType::new(T_LAND), &lhs, &rhs) => *rhs,

                //
                // Rules for AND
                //
                // (..A..) AND A --> ..A..  (unwrap a single-level Paren if present)
                T_LAND if expr_contains(&lhs, &rhs, TokenType::new(T_LAND)) => {
                    // if both sides are identical (e.g., `(A) AND (A)`), preserve the original
                    // wrapper by returning lhs as-is. Otherwise unwrap a single-level Paren.
                    if *lhs == *rhs {
                        *lhs
                    } else {
                        match *lhs {
                            Expr::Paren(p) => *p.expr,
                            other => other,
                        }
                    }
                }
                // A AND (..A..) --> ..A..
                T_LAND if expr_contains(&rhs, &lhs, TokenType::new(T_LAND)) => {
                    if *lhs == *rhs {
                        *rhs
                    } else {
                        match *rhs {
                            Expr::Paren(p) => *p.expr,
                            other => other,
                        }
                    }
                }
                // A AND (A OR B) --> A
                T_LAND if is_op_with(TokenType::new(T_LOR), &rhs, &lhs) => {
                    if *lhs == *rhs {
                        *lhs
                    } else {
                        match *lhs {
                            Expr::Paren(p) => *p.expr,
                            other => other,
                        }
                    }
                }
                // (A OR B) AND A --> A
                T_LAND if is_op_with(TokenType::new(T_LOR), &lhs, &rhs) => {
                    if *lhs == *rhs {
                        *rhs
                    } else {
                        match *rhs {
                            Expr::Paren(p) => *p.expr,
                            other => other,
                        }
                    }
                }

                //
                // Rules for Mul
                //
                // A * 1 --> A
                T_MUL if is_one(&rhs) => *lhs,
                // 1 * A --> A
                T_MUL if is_one(&lhs) => *rhs,
                // A * NaN --> NaN
                T_MUL if is_null(&rhs) => *rhs,
                // NaN * A --> NaN
                T_MUL if is_null(&lhs) => *lhs,

                //
                // Rules for Div
                //
                // A / 1 --> A
                T_DIV if is_one(&rhs) => *lhs,
                // NaN / A --> NaN
                T_DIV if is_null(&lhs) => *lhs,
                // A / NaN --> NaN
                T_DIV if is_null(&rhs) => *rhs,
                // A / A --> NAN if A.is_nan() else 1.0. The NaN comparison can be valid for
                // NumberLiteral, but not for VectorSelector, Subquery, Aggregation, etc.
                T_DIV if lhs == rhs && is_number(&lhs) => {
                    if is_null(&rhs) {
                        Expr::from(f64::NAN)
                    } else {
                        Expr::from(1.0)
                    }
                }
                // A / 0 -> NaN
                T_DIV if is_zero(&rhs) => {
                    // if we have an instant vector or sample, check if we need to maintain
                    // the label set
                    let should_keep_metric_names = {
                        if let Expr::Call(fe) = &lhs.as_ref() {
                            if let Some(func) = resolve_function(fe.func.name) {
                                use PromqlFunctionKind::*;
                                let kind = func.kind();
                                matches!(kind, LabelReplace | LabelJoin)
                            } else {
                                false
                            }
                        } else {
                            false
                        }
                    };
                    if should_keep_metric_names {
                        return Expr::Binary(BinaryExpr {
                            lhs,
                            rhs,
                            op,
                            modifier,
                        });
                    }

                    Expr::from(f64::NAN)
                }
                //
                // Rules for Mod
                //
                // A % NaN --> NaN
                T_MOD if is_null(&rhs) => *rhs,
                // NaN % A --> NaN
                T_MOD if is_null(&lhs) => *lhs,
                // no additional rewrites possible
                _ => Expr::Binary(BinaryExpr {
                    lhs,
                    rhs,
                    op,
                    modifier,
                }),
            }
        }
        expr => expr,
    }
}

// see https://prometheus.io/docs/prometheus/latest/querying/operators/

#[cfg(test)]
mod tests {
    use super::*;

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
        // A * NAN --> NAN
        assert_string_simplify("c2 * NaN", "NaN");
        // NAN * A --> NAN
        assert_string_simplify("NaN * c2", "NaN");
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
        // A / NAN --> NAN
        assert_string_simplify("c1 / NaN", "NaN");
        // NAN / A --> NAN
        assert_string_simplify("NaN / c2", "NaN");
        // NAN / NAN --> NAN
        assert_string_simplify("NaN / NaN", "NaN");
    }

    #[test]
    fn test_simplify_div_zero_by_zero() {
        // 0 / 0 -> NAN
        let expr = "0.0 / 0.0";
        assert_string_simplify(expr, "NaN");
    }

    #[test]
    fn test_simplify_div_by_zero() {
        // A / 0 -> NaN
        assert_string_simplify("c2 / 0.0", "NaN");
    }

    #[test]
    fn test_simplify_mod_by_nan() {
        // A % NaN --> NaN
        assert_string_simplify("c2 % NaN", "NaN");
        // NaN % A --> NaN
        assert_string_simplify("NaN % c2", "NaN");
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

    // TODO: BinaryExpr
}
