//! Binary-operator benchmark cases. Compiled only under the `bench` feature,
//! for the external Criterion crate (`benches/fast_path.rs`). Each case is
//! built once, each iteration gets a fresh input from `input()` inside
//! Criterion's untimed setup, and `run()` returns its result so the output is
//! freed untimed too.

use super::binop_scalar_vector::eval_binop_scalar_vector;
use super::binop_vector_scalar::eval_binop_vector_scalar;
use super::binop_vector_vector::eval_binop_vector_vector;
use crate::labels::Label;
use crate::promql::exec::types::EvalLabels;
use crate::promql::{EvalSample, ExprResult};
use promql_parser::parser::token::{T_ADD, T_GTR, TokenType};
use promql_parser::parser::{BinModifier, BinaryExpr, Expr, NumberLiteral, VectorMatchFillValues};

// ------------------------------ vector-scalar ------------------------------

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

    /// Runs the operation under test.
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
}

// ------------------------------ vector-vector ------------------------------

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
