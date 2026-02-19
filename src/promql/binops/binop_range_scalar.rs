use super::apply_binary_op;
use super::labels::changes_metric_schema;
use crate::promql::{EvalResult, EvalSamples, ExprResult};
use promql_parser::parser::BinaryExpr;
use promql_parser::parser::token::TokenType;

/// Evaluate binary operations where the left side is a range vector and the
/// right side is a scalar. The operation is applied pointwise to every sample
/// in every range series. Comparison/filter semantics mirror
/// `binop_vector_scalar` behavior: for non-`bool` comparisons false results
/// are removed (sample dropped); for `bool` comparisons the per-sample 0/1 is
/// kept and `__name__` is removed.
pub(super) fn eval_binop_range_scalar(
    expr: &BinaryExpr,
    range: Vec<EvalSamples>,
    scalar: f64,
) -> EvalResult<ExprResult> {
    let return_bool = expr.return_bool();
    let is_comparison = expr.op.is_comparison_operator();

    let mut result = range;
    // scalar_left = false because samples are the left operand
    result.retain_mut(|series| {
        evaluate_series(series, expr.op, scalar, is_comparison, return_bool, false)
    });

    Ok(ExprResult::RangeVector(result))
}

/// Evaluate binary operations where the left side is a scalar and the right
/// side is a range vector. Mirrors `eval_binop_range_scalar` with operand
/// order swapped.
pub(super) fn eval_binop_scalar_range(
    expr: &BinaryExpr,
    scalar: f64,
    range: Vec<EvalSamples>,
) -> EvalResult<ExprResult> {
    let return_bool = expr.return_bool();
    let is_comparison = expr.op.is_comparison_operator();

    let mut result = range;
    // scalar is left operand in this variant
    result.retain_mut(|series| {
        evaluate_series(series, expr.op, scalar, is_comparison, return_bool, true)
    });

    Ok(ExprResult::RangeVector(result))
}

fn evaluate_series(
    series: &mut EvalSamples,
    op: TokenType,
    scalar: f64,
    is_comparison: bool,
    return_bool: bool,
    scalar_left: bool,
) -> bool {
    series.values.retain_mut(|sample| {
        let res = if scalar_left {
            apply_binary_op(op, scalar, sample.value)
        } else {
            apply_binary_op(op, sample.value, scalar)
        };

        match res {
            Ok(value) => {
                // For comparison ops without bool, filter out false results
                if is_comparison && !return_bool && value == 0.0 {
                    false
                } else {
                    sample.value = value;
                    true
                }
            }
            Err(_) => false,
        }
    });

    if series.is_empty() {
        return false;
    }

    series.drop_name |= changes_metric_schema(op) || return_bool;

    true
}
