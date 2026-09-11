use crate::promql::{EvalResult, EvaluationError, ExprResult};
use promql_parser::parser::BinaryExpr;
use promql_parser::parser::token::{
    T_ADD, T_ATAN2, T_DIV, T_EQLC, T_GTE, T_GTR, T_LSS, T_LTE, T_MOD, T_MUL, T_NEQ, T_POW, T_SUB,
    TokenType,
};

mod binop_range_scalar;
mod binop_scalar_vector;
mod binop_string_string;
mod binop_vector_scalar;
mod binop_vector_vector;
mod labels;

pub(in crate::promql) use labels::*;

/// Re-exported for the external Criterion benchmark crates (`benches/fast_path.rs`),
/// which reach these through `promql::binops`.
#[cfg(feature = "bench")]
pub use binop_vector_vector::{bench_eval_aligned, bench_eval_unaligned, bench_eval_with_fill};

#[cfg(feature = "bench")]
pub use binop_vector_scalar::{BenchOp, LabelMode, VectorScalarCase, VectorScalarInput};

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

/// The scalar function behind a binary operator, resolved once.
///
/// Per-sample loops resolve the operator up front through this and then call
/// the function, so the one failure mode — a token that is not an arithmetic
/// or comparison operator, which the parser never produces for a vector
/// operation — is reported once, before any sample is looked at, instead of
/// being handled (differently) at every call site.
pub(crate) fn binary_op_fn(op: TokenType) -> EvalResult<fn(f64, f64) -> f64> {
    Ok(match op.id() {
        T_ADD => |l, r| l + r,
        T_SUB => |l, r| l - r,
        T_MUL => |l, r| l * r,
        // PromQL arithmetic is IEEE 754 float arithmetic, so division by zero is
        // signed infinity (`1 / 0` is `+Inf`) and only `0 / 0` is NaN. Modulo by
        // zero is NaN, which is what `%` already yields.
        T_DIV => |l, r| l / r,
        T_MOD => |l, r| l % r,
        T_NEQ => |l, r| if l != r { 1.0 } else { 0.0 },
        T_LSS => |l, r| if l < r { 1.0 } else { 0.0 },
        T_GTR => |l, r| if l > r { 1.0 } else { 0.0 },
        T_LTE => |l, r| if l <= r { 1.0 } else { 0.0 },
        T_GTE => |l, r| if l >= r { 1.0 } else { 0.0 },
        T_EQLC => |l, r| if l == r { 1.0 } else { 0.0 },
        T_POW => f64::powf,
        T_ATAN2 => f64::atan2,
        _ => {
            return Err(EvaluationError::InternalError(format!(
                "Binary operator not yet implemented: {:?}",
                op
            )));
        }
    })
}

pub(crate) fn apply_binary_op(op: TokenType, left: f64, right: f64) -> EvalResult<f64> {
    Ok(binary_op_fn(op)?(left, right))
}
