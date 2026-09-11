use super::apply_binary_op;
use super::labels::changes_metric_schema;
use crate::promql::{EvalResult, EvalSamples, ExprResult};
use promql_parser::parser::BinaryExpr;

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
    // Samples are the left operand.
    eval_range(expr, range, |value| apply_binary_op(expr.op, value, scalar))
}

/// Evaluate binary operations where the left side is a scalar and the right
/// side is a range vector. Mirrors `eval_binop_range_scalar` with operand
/// order swapped.
pub(super) fn eval_binop_scalar_range(
    expr: &BinaryExpr,
    scalar: f64,
    range: Vec<EvalSamples>,
) -> EvalResult<ExprResult> {
    // Scalar is the left operand.
    eval_range(expr, range, |value| apply_binary_op(expr.op, scalar, value))
}

/// Shared body for both operand orders. `apply` is generic rather than a
/// runtime `scalar_left` flag so each caller gets its own monomorphized
/// per-sample loop with the operand order already resolved.
///
/// Series that end up empty are removed from the result. Series that were
/// already empty on input are removed too, matching prior behavior.
fn eval_range<F>(expr: &BinaryExpr, range: Vec<EvalSamples>, apply: F) -> EvalResult<ExprResult>
where
    F: Fn(f64) -> EvalResult<f64>,
{
    // With `bool` modifier, comparison ops return 0/1 for all pairs instead of filtering
    let return_bool = expr.return_bool();
    let mut result = range;

    if expr.op.is_comparison_operator() && !return_bool {
        // Filtering path: comparisons without `bool` drop samples that compare false.
        // `drop_name` is left alone here: a comparison operator is never one of the
        // schema-changing arithmetic ops, so `changes_metric_schema` is false, and
        // `return_bool` is false by the branch condition.
        let mut first_err = None;
        result.retain_mut(|series| {
            series
                .values
                .to_mut()
                .retain_mut(|sample| match apply(sample.value) {
                    Ok(0.0) => false,
                    // The sample passed: keep its original value. Only `bool`
                    // comparisons rewrite values to 0/1.
                    Ok(_) => true,
                    Err(e) => {
                        first_err.get_or_insert(e);
                        false
                    }
                });
            !series.is_empty()
        });
        if let Some(e) = first_err {
            return Err(e);
        }
    } else {
        // Every sample survives, so walk each series in place rather than paying
        // for a per-sample `retain_mut`. The outer `retain_mut` is per series and
        // only there to drop series that were empty on input.
        let should_drop_name = changes_metric_schema(expr.op) || return_bool;
        let mut first_err = None;
        result.retain_mut(|series| {
            if series.is_empty() {
                return false;
            }
            for sample in series.values.to_mut() {
                match apply(sample.value) {
                    Ok(value) => sample.value = value,
                    Err(e) => {
                        first_err.get_or_insert(e);
                        return false;
                    }
                }
            }
            series.drop_name |= should_drop_name;
            true
        });
        if let Some(e) = first_err {
            return Err(e);
        }
    }

    Ok(ExprResult::RangeVector(result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Sample;
    use crate::promql::exec::types::EvalLabels;
    use promql_parser::parser::token::{T_ADD, T_GTR, T_LAND, T_SUB, TokenType};
    use promql_parser::parser::{BinModifier, Expr, NumberLiteral};

    fn expr(op: TokenType, return_bool: bool) -> BinaryExpr {
        BinaryExpr {
            op,
            lhs: Box::new(Expr::NumberLiteral(NumberLiteral { val: 0.0 })),
            rhs: Box::new(Expr::NumberLiteral(NumberLiteral { val: 0.0 })),
            modifier: return_bool.then(|| BinModifier::default().with_return_bool(true)),
        }
    }

    fn series(id: &str, values: &[f64]) -> EvalSamples {
        let mut labels = EvalLabels::empty();
        labels.set("__name__", "m".to_string());
        labels.set("id", id.to_string());
        EvalSamples {
            values: values
                .iter()
                .enumerate()
                .map(|(i, &v)| Sample::new(i as i64 * 1000, v))
                .collect(),
            labels,
            drop_name: false,
            range_ms: 0,
            range_end_ms: 0,
        }
    }

    fn values(s: &EvalSamples) -> Vec<f64> {
        s.values.iter().map(|x| x.value).collect()
    }

    fn unwrap_range(r: EvalResult<ExprResult>) -> Vec<EvalSamples> {
        match r.expect("evaluation should succeed") {
            ExprResult::RangeVector(v) => v,
            other => panic!("expected RangeVector, got {other:?}"),
        }
    }

    #[test]
    fn arithmetic_applies_to_every_sample_and_drops_name() {
        let input = vec![series("a", &[1.0, 2.0]), series("b", &[10.0])];
        let out = unwrap_range(eval_binop_range_scalar(
            &expr(TokenType::new(T_ADD), false),
            input,
            5.0,
        ));
        assert_eq!(out.len(), 2);
        assert_eq!(values(&out[0]), vec![6.0, 7.0]);
        assert_eq!(values(&out[1]), vec![15.0]);
        assert!(
            out.iter().all(|s| s.drop_name),
            "arithmetic changes the metric schema"
        );
    }

    /// `sub` is order-sensitive, so it proves each entry point resolves the
    /// operand order correctly through the shared closure.
    #[test]
    fn operand_order_is_respected() {
        let sub = expr(TokenType::new(T_SUB), false);

        let out = unwrap_range(eval_binop_range_scalar(
            &sub,
            vec![series("a", &[10.0])],
            3.0,
        ));
        assert_eq!(values(&out[0]), vec![7.0], "range - scalar");

        let out = unwrap_range(eval_binop_scalar_range(
            &sub,
            3.0,
            vec![series("a", &[10.0])],
        ));
        assert_eq!(values(&out[0]), vec![-7.0], "scalar - range");
    }

    #[test]
    fn comparison_filters_samples_and_drops_empty_series() {
        let gtr = expr(TokenType::new(T_GTR), false);
        let input = vec![
            series("mixed", &[1.0, 5.0, 2.0, 9.0]),
            series("none", &[1.0, 2.0]),
            series("all", &[7.0, 8.0]),
        ];
        let out = unwrap_range(eval_binop_range_scalar(&gtr, input, 3.0));

        assert_eq!(out.len(), 2, "series with no passing samples is removed");
        assert_eq!(values(&out[0]), vec![5.0, 9.0]);
        assert_eq!(values(&out[1]), vec![7.0, 8.0]);
        // Filtering keeps the original values, not 0/1.
        assert!(
            out.iter().all(|s| !s.drop_name),
            "plain comparison keeps __name__"
        );
    }

    #[test]
    fn comparison_with_bool_keeps_everything_as_zero_one() {
        let gtr_bool = expr(TokenType::new(T_GTR), true);
        let out = unwrap_range(eval_binop_range_scalar(
            &gtr_bool,
            vec![series("a", &[1.0, 5.0, 2.0])],
            3.0,
        ));
        assert_eq!(out.len(), 1);
        assert_eq!(values(&out[0]), vec![0.0, 1.0, 0.0]);
        assert!(out[0].drop_name, "`bool` comparisons drop __name__");
    }

    #[test]
    fn scalar_on_left_comparison_uses_swapped_order() {
        // `3 > v` passes where v < 3.
        let gtr = expr(TokenType::new(T_GTR), false);
        let out = unwrap_range(eval_binop_scalar_range(
            &gtr,
            3.0,
            vec![series("a", &[1.0, 5.0, 2.0])],
        ));
        assert_eq!(values(&out[0]), vec![1.0, 2.0]);
    }

    #[test]
    fn series_empty_on_input_are_removed() {
        let input = vec![series("empty", &[]), series("a", &[1.0])];
        let out = unwrap_range(eval_binop_range_scalar(
            &expr(TokenType::new(T_ADD), false),
            input,
            1.0,
        ));
        assert_eq!(out.len(), 1);
        assert_eq!(values(&out[0]), vec![2.0]);
    }

    /// An operator `apply_binary_op` can't evaluate must surface as an error
    /// rather than an empty result, on both the filtering and plain paths.
    #[test]
    fn unsupported_operator_is_an_error_not_an_empty_result() {
        let and = expr(TokenType::new(T_LAND), false);
        assert!(
            !and.op.is_comparison_operator(),
            "test relies on the non-filtering path"
        );

        let res = eval_binop_range_scalar(&and, vec![series("a", &[1.0])], 1.0);
        assert!(res.is_err(), "range and scalar");

        let res = eval_binop_scalar_range(&and, 1.0, vec![series("a", &[1.0])]);
        assert!(res.is_err(), "scalar and range");
    }
}
