use crate::promql::{EvalResult, EvaluationError, ExprResult};
use promql_parser::parser::BinaryExpr;
use promql_parser::parser::token::{
    T_ADD, T_DIV, T_EQLC, T_GTE, T_GTR, T_LSS, T_LTE, T_MUL, T_NEQ, T_SUB, T_MOD,
    TokenType,
};

mod binop_range_scalar;
mod binop_scalar_vector;
mod binop_string_string;
mod binop_vector_scalar;
mod binop_vector_vector;
mod labels;

pub(crate) fn eval_binary_expr(
    expr: &BinaryExpr,
    lhs: ExprResult,
    rhs: ExprResult,
) -> EvalResult<ExprResult> {
    match (lhs, rhs) {
        // Vector-Scalar operations: apply scalar to each vector element
        (ExprResult::InstantVector(vector), ExprResult::Scalar(scalar)) => {
            binop_vector_scalar::eval_binop_vector_scalar(expr, vector, scalar)
        }
        // Scalar-Vector operations: apply scalar to each vector element
        (ExprResult::Scalar(scalar), ExprResult::InstantVector(vector)) => {
            binop_scalar_vector::eval_binop_scalar_vector(expr, scalar, vector)
        }
        (ExprResult::InstantVector(left_vector), ExprResult::InstantVector(right_vector)) => {
            binop_vector_vector::eval_binop_vector_vector(expr, left_vector, right_vector)
        }
        // Scalar-Scalar operations
        (ExprResult::Scalar(left), ExprResult::Scalar(right)) => {
            let result_value = apply_binary_op(expr.op, left, right)?;
            Ok(ExprResult::Scalar(result_value))
        }
        // RangeVector - Scalar or Scalar - Range operations
        (ExprResult::RangeVector(range), ExprResult::Scalar(scalar)) => {
            binop_range_scalar::eval_binop_range_scalar(expr, range, scalar)
        }
        (ExprResult::Scalar(scalar), ExprResult::RangeVector(range)) => {
            binop_range_scalar::eval_binop_scalar_range(expr, scalar, range)
        }
        (ExprResult::String(left), ExprResult::String(right)) => {
            binop_string_string::eval_binop_string_string(expr.op, &left, &right)
        }
        // Other RangeVector combinations are not supported yet
        (ExprResult::RangeVector(_), _) | (_, ExprResult::RangeVector(_)) => {
            Err(EvaluationError::InternalError(
                "Binary operations with range vectors (except scalar combinations) not yet supported".to_string(),
            ))
        }
        _ => {
            Err(EvaluationError::InternalError(
                "Unsupported binary operation".to_string(),
            ))
        }
    }
}
pub(crate) fn apply_binary_op(op: TokenType, left: f64, right: f64) -> EvalResult<f64> {
    // Use the token constants with TokenType::new() for clean comparison
    match op.id() {
        T_ADD => Ok(left + right),
        T_SUB => Ok(left - right),
        T_MUL => Ok(left * right),
        T_DIV => {
            if right == 0.0 {
                Ok(f64::NAN) // Division by zero results in NaN in PromQL
            } else {
                Ok(left / right)
            }
        }
        T_MOD => {
            // Modulo by zero results in NaN in PromQL
            if right == 0.0 {
                Ok(f64::NAN)
            } else {
                Ok(left % right)
            }
        }
        T_NEQ => Ok(if left != right { 1.0 } else { 0.0 }),
        T_LSS => Ok(if left < right { 1.0 } else { 0.0 }),
        T_GTR => Ok(if left > right { 1.0 } else { 0.0 }),
        T_LTE => Ok(if left <= right { 1.0 } else { 0.0 }),
        T_GTE => Ok(if left >= right { 1.0 } else { 0.0 }),
        T_EQLC => Ok(if left == right { 1.0 } else { 0.0 }),
        _ => Err(EvaluationError::InternalError(format!(
            "Binary operator not yet implemented: {:?}",
            op
        ))),
    }
}
