use super::binary_op_fn;
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
    let apply = binary_op_fn(expr.op)?;
    let mut result = vector;

    if expr.op.is_comparison_operator() && !return_bool {
        // Filtering path: comparisons without `bool` drop samples that compare false.
        // `drop_name` is left alone here: a comparison operator is never one of the
        // schema-changing arithmetic ops, so `changes_metric_schema` is false, and
        // `return_bool` is false by the branch condition.
        // A passing sample keeps its original value. Only `bool` comparisons
        // rewrite values to 0/1.
        result.retain(|sample| apply(scalar, sample.value) != 0.0);
    } else {
        // Every sample survives, so walk in place rather than paying for `retain_mut`.
        let should_drop_name = changes_metric_schema(expr.op) || return_bool;
        for sample in &mut result {
            sample.value = apply(scalar, sample.value);
            sample.drop_name |= should_drop_name;
        }
    }

    Ok(ExprResult::InstantVector(result))
}
