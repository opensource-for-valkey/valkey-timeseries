use crate::promql::{EvalResult, EvaluationError, ExprResult};
use promql_parser::parser::token::{T_ADD, T_EQLC, T_GTE, T_GTR, T_LSS, T_LTE, T_NEQ, TokenType};

pub(crate) fn eval_binop_string_string(
    op: TokenType,
    left: &str,
    right: &str,
) -> EvalResult<ExprResult> {
    match op.id() {
        T_ADD => match (left.is_empty(), right.is_empty()) {
            (false, false) => {
                let mut res = String::with_capacity(left.len() + right.len());
                res += left;
                res += right;
                Ok(ExprResult::String(res))
            }
            (true, false) => Ok(ExprResult::String(right.to_string())),
            (false, true) => Ok(ExprResult::String(left.to_string())),
            (true, true) => Ok(ExprResult::String("".to_string())),
        },
        _ => {
            let value = compare(op, left, right)?;
            Ok(ExprResult::Scalar(value))
        }
    }
}

fn compare(op: TokenType, left: &str, right: &str) -> EvalResult<f64> {
    match op.id() {
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
