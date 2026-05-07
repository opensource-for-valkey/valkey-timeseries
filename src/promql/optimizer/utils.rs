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

//! Utility functions for expression simplification
use promql_parser::parser::token::TokenType;
use promql_parser::parser::{BinaryExpr, Expr, NumberLiteral};

/// Returns true if `needle` is found in a chain of `search_op`
/// expressions, such as `(A AND B) AND C`
pub(super) fn expr_contains(expr: &Expr, needle: &Expr, search_op: TokenType) -> bool {
    // unwrap parentheses on either side so comparisons succeed whether either side is paren-wrapped
    match (expr, needle) {
        (Expr::Paren(pe), _) => expr_contains(&pe.expr, needle, search_op),
        (_, Expr::Paren(pn)) => expr_contains(expr, &pn.expr, search_op),
        (Expr::Binary(BinaryExpr { lhs, op, rhs, .. }), _) if *op == search_op => {
            expr_contains(lhs, needle, search_op) || expr_contains(rhs, needle, search_op)
        }
        _ => expr == needle,
    }
}

pub(super) fn is_number(s: &Expr) -> bool {
    matches!(s, Expr::NumberLiteral(_))
}

pub(super) fn is_number_value(s: &Expr, num: f64) -> bool {
    match s {
        Expr::NumberLiteral(NumberLiteral { val, .. }) => {
            val.total_cmp(&num) == std::cmp::Ordering::Equal
        }
        _ => false,
    }
}

pub(super) fn is_zero(s: &Expr) -> bool {
    is_number_value(s, 0.0)
}

pub(super) fn is_one(s: &Expr) -> bool {
    is_number_value(s, 1.0)
}

pub(super) fn is_null(expr: &Expr) -> bool {
    match expr {
        Expr::NumberLiteral(NumberLiteral { val, .. }) => val.is_nan(),
        _ => false,
    }
}

/// returns true if `haystack` looks like `(needle OP X)` or `(X OP needle)`
pub(super) fn is_op_with(target_op: TokenType, haystack: &Expr, needle: &Expr) -> bool {
    // unwrap needle if it's paren-wrapped so comparisons succeed
    if let Expr::Paren(pn) = needle {
        return is_op_with(target_op, haystack, &pn.expr);
    }

    match haystack {
        // unwrap parentheses and inspect inner expression
        Expr::Paren(p) => is_op_with(target_op, &p.expr, needle),
        Expr::Binary(BinaryExpr { lhs, op, rhs, .. }) if op == &target_op => {
            // compare needle with lhs/rhs after unwrapping any Paren wrapper on them
            let lhs_un = match lhs.as_ref() {
                Expr::Paren(p) => p.expr.as_ref(),
                other => other,
            };
            let rhs_un = match rhs.as_ref() {
                Expr::Paren(p) => p.expr.as_ref(),
                other => other,
            };
            if needle == lhs_un || needle == rhs_un {
                return true;
            }
            false
        }
        _ => false,
    }
}
