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
    let mut result = vector;

    if expr.op.is_comparison_operator() && !return_bool {
        // Filtering path: comparisons without `bool` drop samples that compare false.
        // `drop_name` is left alone here: a comparison operator is never one of the
        // schema-changing arithmetic ops, so `changes_metric_schema` is false, and
        // `return_bool` is false by the branch condition.
        let mut first_err = None;
        result.retain_mut(
            |sample| match apply_binary_op(expr.op, sample.value, scalar) {
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
            sample.value = apply_binary_op(expr.op, sample.value, scalar)?;
            sample.drop_name |= should_drop_name;
        }
    }

    Ok(ExprResult::InstantVector(result))
}

// ------------------------- Benchmark helpers -------------------------------
// Compiled only under the `bench` feature, for the external Criterion crate
// (`benches/fast_path.rs`). `run_legacy` preserves the pre-split `retain_mut`
// implementation so both variants are measured in a single run, with no
// rebuild skew between them.

#[cfg(feature = "bench")]
mod bench_support {
    use super::eval_binop_vector_scalar;
    use crate::labels::Label;
    use crate::promql::binops::apply_binary_op;
    use crate::promql::binops::binop_scalar_vector::eval_binop_scalar_vector;
    use crate::promql::binops::labels::changes_metric_schema;
    use crate::promql::exec::types::EvalLabels;
    use crate::promql::{EvalResult, EvalSample, ExprResult};
    use promql_parser::parser::token::{T_ADD, T_GTR, TokenType};
    use promql_parser::parser::{BinModifier, BinaryExpr, Expr, NumberLiteral};

    /// The pre-split loop structure: one `retain_mut` for every operator, with
    /// the comparison/`bool` flags tested per element. Filtering semantics are
    /// the corrected ones (passing samples keep their value) so the A/B
    /// isolates loop structure rather than the bug the split fixed.
    fn legacy_eval_binop_vector_scalar(
        expr: &BinaryExpr,
        vector: Vec<EvalSample>,
        scalar: f64,
    ) -> EvalResult<ExprResult> {
        let return_bool = expr.return_bool();
        let is_comparison = expr.op.is_comparison_operator();
        let should_drop_name = changes_metric_schema(expr.op) || return_bool;

        let mut result = vector;
        result.retain_mut(
            |sample| match apply_binary_op(expr.op, sample.value, scalar) {
                Ok(value) => {
                    if is_comparison && !return_bool {
                        value != 0.0
                    } else {
                        sample.value = value;
                        sample.drop_name |= should_drop_name;
                        true
                    }
                }
                Err(_) => false,
            },
        );
        Ok(ExprResult::InstantVector(result))
    }

    /// Which operator shape to exercise.
    #[derive(Clone, Copy)]
    pub enum BenchOp {
        /// `v + 1` — arithmetic, nothing is filtered.
        Add,
        /// `v > s` — comparison without `bool`, filters.
        Greater,
        /// `v > bool s` — comparison with `bool`, nothing is filtered.
        GreaterBool,
    }

    /// How each sample's labels are held. Production instant queries build a
    /// fresh sole-owner `Shared` Arc per sample (`EvalLabels::from(Labels)`);
    /// preloaded range queries clone a series Arc per step (`SharedRetained`).
    #[derive(Clone, Copy)]
    pub enum LabelMode {
        /// `EvalLabels::Owned(Vec<Label>)`: drop frees the Vec and every String.
        Owned,
        /// `EvalLabels::Shared` with refcount 1: drop frees the slab and every String.
        SharedSole,
        /// `EvalLabels::Shared` with a reference retained elsewhere: drop is an
        /// atomic decrement only.
        SharedRetained,
    }

    /// A prepared vector-scalar operation. Building the input vector is
    /// expensive relative to the loop under test, so it happens once here and
    /// is cloned per iteration inside Criterion's untimed `setup`.
    pub struct VectorScalarCase {
        expr: BinaryExpr,
        scalar: f64,
        vector: Vec<EvalSample>,
        scalar_left: bool,
        label_mode: LabelMode,
        /// Keeps every sample's Arc alive for `LabelMode::SharedRetained`, so
        /// the clone handed to each iteration is never the last reference.
        _retained: Vec<EvalLabels>,
    }

    /// Opaque owned input for one timed iteration.
    pub struct VectorScalarInput(Vec<EvalSample>);

    impl VectorScalarInput {
        pub fn len(&self) -> usize {
            self.0.len()
        }

        pub fn is_empty(&self) -> bool {
            self.0.is_empty()
        }

        /// `(timestamp, value, drop_name)` per sample, for equivalence checks.
        pub fn samples(&self) -> Vec<(i64, u64, bool)> {
            self.0
                .iter()
                .map(|s| (s.timestamp_ms, s.value.to_bits(), s.drop_name))
                .collect()
        }
    }

    impl VectorScalarCase {
        /// `n` samples; `pass_ratio` is the fraction that survive a filtering
        /// comparison (ignored for non-filtering ops). `scalar_left` selects
        /// `eval_binop_scalar_vector` over `eval_binop_vector_scalar`.
        pub fn new(op: BenchOp, n: usize, pass_ratio: f64, scalar_left: bool) -> Self {
            Self::with_labels(op, n, pass_ratio, scalar_left, LabelMode::Owned)
        }

        pub fn with_labels(
            op: BenchOp,
            n: usize,
            pass_ratio: f64,
            scalar_left: bool,
            label_mode: LabelMode,
        ) -> Self {
            let (token, return_bool) = match op {
                BenchOp::Add => (T_ADD, false),
                BenchOp::Greater => (T_GTR, false),
                BenchOp::GreaterBool => (T_GTR, true),
            };

            let expr = BinaryExpr {
                op: TokenType::new(token),
                lhs: Box::new(Expr::NumberLiteral(NumberLiteral { val: 0.0 })),
                rhs: Box::new(Expr::NumberLiteral(NumberLiteral { val: 0.0 })),
                modifier: return_bool.then(|| BinModifier::default().with_return_bool(true)),
            };

            // Values run 0..n; the threshold puts `pass_ratio` of them above it.
            // Interleave rather than partition so the filtered-out samples are
            // spread through the vector, as they would be in a real result.
            let threshold = (1.0 - pass_ratio) * n as f64;
            let mut vector = Vec::with_capacity(n);
            let mut retained = Vec::new();
            for i in 0..n {
                // Sorted by name, as storage guarantees: "__name__" < "id".
                let raw = vec![
                    Label::new("__name__".to_string(), "bench_metric".to_string()),
                    Label::new("id".to_string(), i.to_string()),
                ];
                let labels = match label_mode {
                    LabelMode::Owned => EvalLabels::owned(raw),
                    LabelMode::SharedSole => EvalLabels::shared(raw),
                    LabelMode::SharedRetained => {
                        let shared = EvalLabels::shared(raw);
                        retained.push(shared.clone());
                        shared
                    }
                };

                // Rotate the value so passing/failing samples alternate.
                let value = ((i * 7919) % n.max(1)) as f64;
                vector.push(EvalSample {
                    timestamp_ms: 1,
                    value,
                    labels,
                    drop_name: false,
                });
            }

            Self {
                expr,
                scalar: threshold,
                vector,
                scalar_left,
                label_mode,
                _retained: retained,
            }
        }

        /// Untimed per-iteration setup: a fresh copy of the input.
        ///
        /// `Vec::clone` refcount-bumps `Shared` labels, which would leave this
        /// case holding a second reference and turn `SharedSole` into
        /// `SharedRetained`. For `SharedSole` each sample instead gets a brand
        /// new Arc, so the timed drop really is the last reference.
        pub fn input(&self) -> VectorScalarInput {
            let vector = match self.label_mode {
                LabelMode::SharedSole => self
                    .vector
                    .iter()
                    .map(|s| EvalSample {
                        timestamp_ms: s.timestamp_ms,
                        value: s.value,
                        labels: EvalLabels::shared(s.labels.to_label_vec()),
                        drop_name: s.drop_name,
                    })
                    .collect(),
                LabelMode::Owned | LabelMode::SharedRetained => self.vector.clone(),
            };
            VectorScalarInput(vector)
        }

        /// The current implementation (branch split out of the loop).
        ///
        /// Returns the result vector rather than its length: freeing ~3 heap
        /// allocations per sample costs far more than the loop under test, so
        /// the output must be handed back to Criterion to drop untimed.
        pub fn run(&self, input: VectorScalarInput) -> VectorScalarInput {
            let res = if self.scalar_left {
                // Mirror the operand order so the comparison keeps the same
                // pass ratio: `scalar > v` passes where `v < scalar`.
                eval_binop_scalar_vector(&self.expr, self.scalar, input.0)
            } else {
                eval_binop_vector_scalar(&self.expr, input.0, self.scalar)
            };
            match res {
                Ok(ExprResult::InstantVector(v)) => VectorScalarInput(v),
                _ => VectorScalarInput(Vec::new()),
            }
        }

        /// The pre-split implementation, for A/B comparison.
        pub fn run_legacy(&self, input: VectorScalarInput) -> VectorScalarInput {
            match legacy_eval_binop_vector_scalar(&self.expr, input.0, self.scalar) {
                Ok(ExprResult::InstantVector(v)) => VectorScalarInput(v),
                _ => VectorScalarInput(Vec::new()),
            }
        }
    }
}

#[cfg(feature = "bench")]
pub use bench_support::{BenchOp, LabelMode, VectorScalarCase, VectorScalarInput};

#[cfg(all(test, feature = "bench"))]
mod bench_support_tests {
    use super::bench_support::{BenchOp, VectorScalarCase};

    /// The split implementation must agree with the pre-split one it replaced,
    /// otherwise the A/B benchmark is comparing two different computations.
    #[test]
    fn split_matches_legacy() {
        for op in [BenchOp::Add, BenchOp::GreaterBool, BenchOp::Greater] {
            for pass_ratio in [1.0, 0.9, 0.5, 0.1] {
                let case = VectorScalarCase::new(op, 1000, pass_ratio, false);
                assert_eq!(
                    case.run(case.input()).samples(),
                    case.run_legacy(case.input()).samples(),
                    "split and legacy diverged at pass_ratio {pass_ratio}"
                );
            }
        }
    }

    /// Guards the benchmark inputs themselves: a filtering case that silently
    /// kept every sample would make the filtering measurements meaningless.
    #[test]
    fn filtering_cases_filter_as_configured() {
        let n = 1000;
        for (pass_ratio, expected) in [(0.9, 900), (0.5, 500), (0.1, 100)] {
            let case = VectorScalarCase::new(BenchOp::Greater, n, pass_ratio, false);
            let kept = case.run(case.input()).len();
            assert!(
                kept.abs_diff(expected) <= 1,
                "pass_ratio {pass_ratio} kept {kept}, expected ~{expected}"
            );
        }

        // Non-filtering ops must keep everything.
        for op in [BenchOp::Add, BenchOp::GreaterBool] {
            let case = VectorScalarCase::new(op, n, 0.5, false);
            assert_eq!(case.run(case.input()).len(), n);
        }
    }
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
