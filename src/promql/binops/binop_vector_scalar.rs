use super::binary_op_fn;
use super::labels::changes_metric_schema;
use crate::promql::{EvalResult, EvalSample, ExprResult};
use promql_parser::parser::BinaryExpr;

/// `vector <op> scalar`: the scalar is the right-hand side.
pub(super) fn eval_binop_vector_scalar(
    expr: &BinaryExpr,
    vector: Vec<EvalSample>,
    scalar: f64,
) -> EvalResult<ExprResult> {
    let apply = binary_op_fn(expr.op)?;
    Ok(eval_vector(expr, vector, |value| apply(value, scalar)))
}

/// `scalar <op> vector`: the scalar is the left-hand side.
pub(super) fn eval_binop_scalar_vector(
    expr: &BinaryExpr,
    scalar: f64,
    vector: Vec<EvalSample>,
) -> EvalResult<ExprResult> {
    let apply = binary_op_fn(expr.op)?;
    Ok(eval_vector(expr, vector, |value| apply(scalar, value)))
}

/// Shared body for both operand orders. `apply` is generic rather than a
/// runtime flag so each caller gets its own monomorphized per-sample loop with
/// the operand order already resolved, as `binop_range_scalar::eval_range` does
/// for range vectors.
fn eval_vector<F>(expr: &BinaryExpr, vector: Vec<EvalSample>, apply: F) -> ExprResult
where
    F: Fn(f64) -> f64,
{
    // With `bool` modifier, comparison ops return 0/1 for all pairs instead of filtering
    let return_bool = expr.return_bool();
    let mut result = vector;

    if expr.op.is_comparison_operator() && !return_bool {
        // Filtering path: comparisons without `bool` drop samples that compare false.
        // `drop_name` is left alone here: a comparison operator is never one of the
        // schema-changing arithmetic ops, so `changes_metric_schema` is false, and
        // `return_bool` is false by the branch condition.
        // A passing sample keeps its original value. Only `bool` comparisons
        // rewrite values to 0/1.
        result.retain(|sample| apply(sample.value) != 0.0);
    } else {
        // Every sample survives, so walk in place rather than paying for `retain_mut`.
        let should_drop_name = changes_metric_schema(expr.op) || return_bool;
        for sample in &mut result {
            sample.value = apply(sample.value);
            sample.drop_name |= should_drop_name;
        }
    }

    ExprResult::InstantVector(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::promql::exec::types::EvalLabels;
    use promql_parser::parser::token::{T_GTR, TokenType};
    use promql_parser::parser::{Expr, NumberLiteral};

    fn sample(value: f64) -> EvalSample {
        let mut labels = EvalLabels::empty();
        labels.set("__name__", "m".to_string());
        EvalSample {
            timestamp_ms: 0,
            value,
            labels,
            drop_name: false,
        }
    }

    /// Prometheus `shouldDropMetricName` covers `%`, `^` and `atan2` as well as
    /// `+ - * /`, so `metric ^ 2` yields a nameless series. The promqltest case
    /// for this (`node_cpu ^ 2` in operators.test) cannot catch a regression,
    /// because a `{...}` expectation matches with or without `__name__`.
    #[test]
    fn mod_pow_atan2_drop_metric_name() {
        use promql_parser::parser::token::{T_ATAN2, T_MOD, T_POW};

        for op in [T_MOD, T_POW, T_ATAN2] {
            let expr = BinaryExpr {
                op: TokenType::new(op),
                lhs: Box::new(Expr::NumberLiteral(NumberLiteral { val: 0.0 })),
                rhs: Box::new(Expr::NumberLiteral(NumberLiteral { val: 0.0 })),
                modifier: None,
            };
            let out = match eval_binop_vector_scalar(&expr, vec![sample(8.0)], 3.0) {
                Ok(ExprResult::InstantVector(v)) => v,
                other => panic!("expected InstantVector, got {other:?}"),
            };
            assert!(
                out[0].drop_name,
                "op {op:?} must mark __name__ for dropping"
            );
        }
    }

    /// The two entry points differ only in operand order, which the shared
    /// body must not lose: `10 - m` is not `m - 10`, and `10 > m` keeps the
    /// samples `m > 10` drops.
    #[test]
    fn operand_order_follows_the_entry_point() {
        use promql_parser::parser::token::T_SUB;

        let sub = BinaryExpr {
            op: TokenType::new(T_SUB),
            lhs: Box::new(Expr::NumberLiteral(NumberLiteral { val: 0.0 })),
            rhs: Box::new(Expr::NumberLiteral(NumberLiteral { val: 0.0 })),
            modifier: None,
        };
        let values = |result: EvalResult<ExprResult>| -> Vec<f64> {
            match result {
                Ok(ExprResult::InstantVector(v)) => v.iter().map(|s| s.value).collect(),
                other => panic!("expected InstantVector, got {other:?}"),
            }
        };
        assert_eq!(
            values(eval_binop_vector_scalar(&sub, vec![sample(3.0)], 10.0)),
            vec![-7.0]
        );
        assert_eq!(
            values(eval_binop_scalar_vector(&sub, 10.0, vec![sample(3.0)])),
            vec![7.0]
        );

        let gtr = BinaryExpr {
            op: TokenType::new(T_GTR),
            lhs: Box::new(Expr::NumberLiteral(NumberLiteral { val: 0.0 })),
            rhs: Box::new(Expr::NumberLiteral(NumberLiteral { val: 0.0 })),
            modifier: None,
        };
        let input = || vec![sample(1.0), sample(5.0)];
        assert_eq!(
            values(eval_binop_vector_scalar(&gtr, input(), 3.0)),
            vec![5.0]
        );
        assert_eq!(
            values(eval_binop_scalar_vector(&gtr, 3.0, input())),
            vec![1.0]
        );
    }

    /// Prometheus semantics: a filtering comparison keeps the *original* sample
    /// value for every sample that passes; only `bool` rewrites values to 0/1.
    #[test]
    fn filtering_comparison_keeps_original_values() {
        let gtr = BinaryExpr {
            op: TokenType::new(T_GTR),
            lhs: Box::new(Expr::NumberLiteral(NumberLiteral { val: 0.0 })),
            rhs: Box::new(Expr::NumberLiteral(NumberLiteral { val: 0.0 })),
            modifier: None,
        };
        let input = vec![sample(1.0), sample(5.0), sample(2.0), sample(9.0)];
        let out = match eval_binop_vector_scalar(&gtr, input, 3.0) {
            Ok(ExprResult::InstantVector(v)) => v,
            other => panic!("expected InstantVector, got {other:?}"),
        };
        let values: Vec<f64> = out.iter().map(|s| s.value).collect();
        assert_eq!(values, vec![5.0, 9.0]);
        assert!(out.iter().all(|s| !s.drop_name));
    }
}
