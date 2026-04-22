use super::apply_binary_op;
use super::labels::changes_metric_schema;
use crate::promql::{EvalResult, EvalSample, ExprResult};
use promql_parser::parser::BinaryExpr;

pub(super) fn eval_binop_vector_scalar(
    expr: &BinaryExpr,
    vector: Vec<EvalSample>,
    scalar: f64,
) -> EvalResult<ExprResult> {
    // With `bool` modifier, comparison ops return 0/1 for all pairs instead of filtering
    let return_bool = expr.return_bool();
    let is_comparison = expr.op.is_comparison_operator();
    let mut result = vector;
    result.retain_mut(|sample| {
        match apply_binary_op(expr.op, sample.value, scalar) {
            Ok(value) => {
                // For comparison ops without bool, filter out false results
                if is_comparison && !return_bool && value == 0.0 {
                    false
                } else {
                    sample.value = value;
                    sample.drop_name |= changes_metric_schema(expr.op) || return_bool;
                    true
                }
            }
            Err(_) => false,
        }
    });
    Ok(ExprResult::InstantVector(result))
}
