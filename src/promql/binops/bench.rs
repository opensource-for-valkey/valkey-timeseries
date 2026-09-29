//! Vector-vector benchmark cases. Compiled only under the `bench` feature, for the external Criterion crate
//! (`benches/fast_path.rs`). Follows `binop_vector_scalar::VectorScalarCase`:
//! the case is built once, each iteration gets a fresh input from `input()`
//! inside Criterion's untimed setup, and `run()` returns its result so the
//! output is freed untimed too.

use super::binop_vector_vector::eval_binop_vector_vector;
use crate::labels::Label;
use crate::promql::exec::types::EvalLabels;
use crate::promql::{EvalSample, ExprResult};
use promql_parser::parser::token::{T_ADD, TokenType};
use promql_parser::parser::{BinModifier, BinaryExpr, Expr, NumberLiteral, VectorMatchFillValues};

/// Which shape of `a + b` to measure.
///
/// The old helpers had an "unaligned" shape that reversed one operand.
/// The join is keyed on the match key, so operand order does not select a
/// different path and it measured exactly the same thing as "aligned".
#[derive(Clone, Copy)]
pub enum VectorVectorShape {
    /// Every key has a partner: all matched.
    Aligned,
    /// Half the keys on each side have a partner. Exercises the
    /// probe-miss path, which the aligned shape never reaches.
    HalfOverlap,
    /// Half overlap under `fill_left`/`fill_right`, which forces the
    /// general join and makes both fill branches emit.
    HalfOverlapWithFill,
}

/// A prepared vector-vector operation.
pub struct VectorVectorCase {
    expr: BinaryExpr,
    n: usize,
    rhs_id_offset: usize,
}

impl VectorVectorCase {
    pub fn new(shape: VectorVectorShape, n: usize) -> Self {
        let fill = matches!(shape, VectorVectorShape::HalfOverlapWithFill);
        let expr = BinaryExpr {
            op: TokenType::new(T_ADD),
            lhs: Box::new(Expr::NumberLiteral(NumberLiteral { val: 0.0 })),
            rhs: Box::new(Expr::NumberLiteral(NumberLiteral { val: 0.0 })),
            modifier: fill.then(|| {
                BinModifier::default().with_fill_values(VectorMatchFillValues::new(0.0, 0.0))
            }),
        };
        let rhs_id_offset = match shape {
            VectorVectorShape::Aligned => 0,
            VectorVectorShape::HalfOverlap | VectorVectorShape::HalfOverlapWithFill => n / 2,
        };
        Self {
            expr,
            n,
            rhs_id_offset,
        }
    }

    /// Untimed per-iteration setup: fresh operands.
    ///
    /// Built new each time rather than cloned, so every label set is a
    /// sole-owner `Shared` Arc — what an instant selector hands over — and
    /// the drop inside the join is a real free, not a refcount decrement.
    pub fn input(&self) -> (Vec<EvalSample>, Vec<EvalSample>) {
        (series(self.n, 0), series(self.n, self.rhs_id_offset))
    }

    /// Returns the result vector so Criterion frees it untimed; at a few
    /// heap allocations per sample the teardown would otherwise dwarf the
    /// join.
    pub fn run(&self, (left, right): (Vec<EvalSample>, Vec<EvalSample>)) -> Vec<EvalSample> {
        match eval_binop_vector_vector(&self.expr, left, right) {
            Ok(ExprResult::InstantVector(v)) => v,
            Ok(_) => unreachable!("vector-vector always yields an instant vector"),
            Err(e) => panic!("{e}"),
        }
    }
}

/// `requests * on(job) group_left(owner) info`: every result copies
/// `owner` from its "one"-side series. Both operands carry interned
/// labels, as a selector's results do, so this measures the path
/// production takes (the shapes above use `Shared` labels).
pub struct GroupLeftCase {
    expr: BinaryExpr,
    n: usize,
    groups: usize,
}

impl GroupLeftCase {
    pub fn new(n: usize) -> Self {
        use promql_parser::label::Labels as ModifierLabels;
        use promql_parser::parser::token::T_MUL;
        use promql_parser::parser::{LabelModifier, VectorMatchCardinality};
        let modifier = BinModifier::default()
            .with_matching(Some(LabelModifier::Include(ModifierLabels::new(vec![
                "job",
            ]))))
            .with_card(VectorMatchCardinality::ManyToOne(ModifierLabels::new(
                vec!["owner"],
            )));
        Self {
            expr: BinaryExpr {
                op: TokenType::new(T_MUL),
                lhs: Box::new(Expr::NumberLiteral(NumberLiteral { val: 0.0 })),
                rhs: Box::new(Expr::NumberLiteral(NumberLiteral { val: 0.0 })),
                modifier: Some(modifier),
            },
            n,
            groups: (n / 10).max(1),
        }
    }

    /// Untimed per-iteration setup: `n` request series over `n / 10` jobs,
    /// and one info series per job.
    pub fn input(&self) -> (Vec<EvalSample>, Vec<EvalSample>) {
        use crate::labels::MetricName;
        let interned = |pairs: &[(&str, &str)], value: f64| EvalSample {
            timestamp_ms: 1,
            value,
            labels: EvalLabels::interned(&MetricName::from_pairs(pairs.iter().copied())),
            drop_name: false,
        };
        let many = (0..self.n)
            .map(|i| {
                let (job, instance) = (format!("job{}", i % self.groups), i.to_string());
                interned(
                    &[
                        ("__name__", "requests"),
                        ("instance", &instance),
                        ("job", &job),
                        ("method", "GET"),
                    ],
                    i as f64,
                )
            })
            .collect();
        let one = (0..self.groups)
            .map(|g| {
                let (job, owner) = (format!("job{g}"), format!("team-{g}"));
                interned(
                    &[
                        ("__name__", "info"),
                        ("job", &job),
                        ("owner", &owner),
                        ("version", "v1"),
                    ],
                    1.0,
                )
            })
            .collect();
        (many, one)
    }

    pub fn run(&self, (left, right): (Vec<EvalSample>, Vec<EvalSample>)) -> Vec<EvalSample> {
        match eval_binop_vector_vector(&self.expr, left, right) {
            Ok(ExprResult::InstantVector(v)) => v,
            Ok(_) => unreachable!("vector-vector always yields an instant vector"),
            Err(e) => panic!("{e}"),
        }
    }
}

/// `a + on(l) b` against `a + on(l) group_right b` over the same operands:
/// `n` series a side, one partner each, so both produce `n` results. Either
/// interned labels (a local selector's output) or `Shared` ones (what a
/// cluster range read decodes into).
///
/// Plain interned inputs are each the sole owner of their label set, so the
/// join frees every set it consumes. [`Self::new_indexed`] keeps the sets
/// alive in the case, as the series index does in production, where dropping
/// one is a reference-count decrement.
pub struct OnMatchCase {
    expr: BinaryExpr,
    n: usize,
    interned: bool,
    indexed: Option<(
        Vec<crate::labels::MetricName>,
        Vec<crate::labels::MetricName>,
    )>,
}

impl OnMatchCase {
    pub fn new(group_right: bool, interned: bool, n: usize) -> Self {
        use promql_parser::label::Labels as ModifierLabels;
        use promql_parser::parser::{LabelModifier, VectorMatchCardinality};
        let mut modifier = BinModifier::default()
            .with_matching(Some(LabelModifier::Include(ModifierLabels::new(vec!["l"]))));
        if group_right {
            modifier = modifier.with_card(VectorMatchCardinality::OneToMany(ModifierLabels::new(
                Vec::<&str>::new(),
            )));
        }
        Self {
            expr: BinaryExpr {
                op: TokenType::new(T_ADD),
                lhs: Box::new(Expr::NumberLiteral(NumberLiteral { val: 0.0 })),
                rhs: Box::new(Expr::NumberLiteral(NumberLiteral { val: 0.0 })),
                modifier: Some(modifier),
            },
            n,
            interned,
            indexed: None,
        }
    }

    /// Interned inputs whose label sets the case also holds.
    pub fn new_indexed(group_right: bool, n: usize) -> Self {
        use crate::labels::MetricName;
        let mut case = Self::new(group_right, true, n);
        let names = |name: &str| -> Vec<MetricName> {
            (0..n)
                .map(|i| {
                    let (l, inst) = (i.to_string(), format!("10.0.0.{}:9100", i % 50));
                    MetricName::from_pairs([
                        ("__name__", name),
                        ("instance", inst.as_str()),
                        ("job", "api"),
                        ("l", l.as_str()),
                    ])
                })
                .collect()
        };
        case.indexed = Some((names("a"), names("b")));
        case
    }

    pub fn input(&self) -> (Vec<EvalSample>, Vec<EvalSample>) {
        use crate::labels::MetricName;
        if let Some((a, b)) = &self.indexed {
            let side = |names: &[MetricName]| -> Vec<EvalSample> {
                names
                    .iter()
                    .enumerate()
                    .map(|(i, name)| EvalSample {
                        timestamp_ms: 1,
                        value: i as f64,
                        labels: EvalLabels::interned(name),
                        drop_name: false,
                    })
                    .collect()
            };
            return (side(a), side(b));
        }
        let make = |pairs: &[(&str, &str)], value: f64| EvalSample {
            timestamp_ms: 1,
            value,
            labels: if self.interned {
                EvalLabels::interned(&MetricName::from_pairs(pairs.iter().copied()))
            } else {
                let mut raw: Vec<Label> = pairs.iter().map(|(n, v)| Label::new(*n, *v)).collect();
                raw.sort();
                EvalLabels::shared(raw)
            },
            drop_name: false,
        };
        let side = |name: &str| -> Vec<EvalSample> {
            (0..self.n)
                .map(|i| {
                    let (l, inst) = (i.to_string(), format!("10.0.0.{}:9100", i % 50));
                    make(
                        &[
                            ("__name__", name),
                            ("instance", &inst),
                            ("job", "api"),
                            ("l", &l),
                        ],
                        i as f64,
                    )
                })
                .collect()
        };
        (side("a"), side("b"))
    }

    pub fn run(&self, (left, right): (Vec<EvalSample>, Vec<EvalSample>)) -> Vec<EvalSample> {
        match eval_binop_vector_vector(&self.expr, left, right) {
            Ok(ExprResult::InstantVector(v)) => v,
            Ok(_) => unreachable!("vector-vector always yields an instant vector"),
            Err(e) => panic!("{e}"),
        }
    }
}

/// `n` series shaped like selector output: `__name__` plus a unique `id`
/// and two shared labels, held as a sole-owner `Shared` Arc.
fn series(n: usize, id_offset: usize) -> Vec<EvalSample> {
    (0..n)
        .map(|i| {
            let id = i + id_offset;
            let mut raw = vec![
                Label::new("__name__".to_string(), "http_requests_total".to_string()),
                Label::new("id".to_string(), id.to_string()),
                Label::new("instance".to_string(), format!("10.0.0.{}:9100", id % 50)),
                Label::new("job".to_string(), "api".to_string()),
            ];
            raw.sort();
            EvalSample {
                timestamp_ms: 1,
                value: id as f64,
                labels: EvalLabels::shared(raw),
                drop_name: false,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Guards the benchmark inputs: each shape must reach the path it
    /// claims to measure.
    #[test]
    fn shapes_match_as_configured() {
        let n = 1000;
        let aligned = VectorVectorCase::new(VectorVectorShape::Aligned, n);
        assert_eq!(aligned.run(aligned.input()).len(), n);

        let half = VectorVectorCase::new(VectorVectorShape::HalfOverlap, n);
        assert_eq!(half.run(half.input()).len(), n / 2);

        // Fill emits for every unmatched key on both sides, so all keys
        // from both operands come out: n/2 matched + n/2 + n/2 filled.
        let filled = VectorVectorCase::new(VectorVectorShape::HalfOverlapWithFill, n);
        assert_eq!(filled.run(filled.input()).len(), n + n / 2);
    }
}
