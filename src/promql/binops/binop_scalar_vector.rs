use super::apply_binary_op;
use super::labels::changes_metric_schema;
use crate::promql::{EvalResult, EvalSample, ExprResult};
use promql_parser::parser::BinaryExpr;

/// Mirrors [`super::binop_vector_scalar::eval_binop_vector_scalar`] with the
/// operand order swapped: the scalar is the left-hand side.
pub(super) fn eval_binop_scalar_vector(
    expr: &BinaryExpr,
    scalar: f64,
    vector: Vec<EvalSample>,
) -> EvalResult<ExprResult> {
    // With `bool` modifier, comparison ops return 0/1 for all pairs instead of filtering
    let return_bool = expr.return_bool();
    let mut result = vector;

    if expr.op.is_comparison_operator() && !return_bool {
        // Filtering path: comparisons without `bool` drop samples that compare false.
        // `drop_name` is left alone here: a comparison operator is never one of the
        // schema-changing arithmetic ops, so `changes_metric_schema` is false, and
        // `return_bool` is false by the branch condition.
        let mut first_err = None;
        result.retain_mut(
            |sample| match apply_binary_op(expr.op, scalar, sample.value) {
                Ok(0.0) => false,
                // The sample passed: keep its original value. Only `bool`
                // comparisons rewrite values to 0/1.
                Ok(_) => true,
                Err(e) => {
                    first_err.get_or_insert(e);
                    false
                }
            },
        );
        if let Some(e) = first_err {
            return Err(e);
        }
    } else {
        // Every sample survives, so walk in place rather than paying for `retain_mut`.
        let should_drop_name = changes_metric_schema(expr.op) || return_bool;
        for sample in &mut result {
            sample.value = apply_binary_op(expr.op, scalar, sample.value)?;
            sample.drop_name |= should_drop_name;
        }
    }

    Ok(ExprResult::InstantVector(result))
}
