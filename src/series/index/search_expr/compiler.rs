// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Ported from Prometheus `util/searchexpr/compiler.go`.

use super::{Expr, MAX_EXPRESSION_NESTING_DEPTH, MAX_EXPRESSION_TERMS, SearchExprError, invalid};
use crate::series::index::FuzzyFilter;

/// The deepest parsed AST has one term below 31 binary nodes and 16 `NOT` nodes.
pub(super) const MAX_COMPILED_EXPRESSION_DEPTH: usize =
    MAX_EXPRESSION_TERMS + MAX_EXPRESSION_NESTING_DEPTH;

/// The error a term filter factory may return.
pub type TermFilterError = Box<dyn std::error::Error + Send + Sync>;

/// Validates `expr` and converts it into a [`FuzzyFilter`].
///
/// `new_term_filter(term, negated)` returns the filter for one term. The filter must match the
/// term itself, because the compiled filter applies the negation; `negated` is true when the
/// term is under an odd number of `NOT`s. Term filters must score in `[0, 1]`. Terms are
/// compiled left to right.
///
/// The score is the highest mean of the positive term scores on any accepting path. Negated
/// terms do not add to a path's score.
pub fn compile<F, E>(expr: &Expr, mut new_term_filter: F) -> Result<CompiledFilter, SearchExprError>
where
    F: FnMut(&str, bool) -> Result<Box<dyn FuzzyFilter>, E>,
    E: Into<TermFilterError>,
{
    validate(expr)?;
    let root = compile_node(expr, false, false, &mut new_term_filter)?;
    Ok(CompiledFilter {
        root: root.into_scalar(),
    })
}

/// Returns an error if [`compile`] cannot compile `expr`. Every accepting path must require a
/// positive term, and the [`parse`](super::parse) limits apply.
pub fn validate(expr: &Expr) -> Result<(), SearchExprError> {
    let anchored = validate_expression(expr, false, 1, &mut 0)?;
    if !anchored {
        return Err(invalid("every accepting path must require a positive term"));
    }
    Ok(())
}

/// `sum` and `count` give the mean of the positive term scores.
#[derive(Debug, Default, Clone, Copy)]
struct EvalResult {
    accepted: bool,
    sum: f64,
    count: usize,
}

impl EvalResult {
    fn mean(self) -> f64 {
        self.sum / self.count as f64
    }

    /// Relies on no term scoring more than 1.0. An accepted result without positive terms is
    /// perfect too: a scalar OR beneath an AND has operands with equal counts, so the other
    /// operand cannot score higher.
    fn perfect(self) -> bool {
        self.sum >= self.count as f64
    }
}

/// A compiled search expression. Safe for concurrent use: evaluation keeps its state on the
/// stack.
pub struct CompiledFilter {
    pub(super) root: Scalar,
}

impl FuzzyFilter for CompiledFilter {
    fn accept(&self, value: &str) -> (bool, f64) {
        let result = self.root.evaluate(value);
        if !result.accepted {
            return (false, 0.0);
        }
        // `validate` guarantees that an accepted root has a positive term.
        (true, result.mean())
    }
}

/// Scores a subtree whose best accepting path its parent can choose by mean alone: every
/// accepting path has the same positive-term count, or only ORs enclose the subtree.
pub(super) enum Scalar {
    Term {
        filter: Box<dyn FuzzyFilter>,
        negated: bool,
    },
    And(Box<Scalar>, Box<Scalar>),
    Or(Box<Scalar>, Box<Scalar>),
    /// A count-varying AND that only ORs enclose. Those ORs compare means, so a single term
    /// with the best mean stands in for its paths.
    BestAlternative(Box<Alternatives>),
}

impl Scalar {
    fn evaluate(&self, value: &str) -> EvalResult {
        match self {
            Scalar::Term { filter, negated } => {
                let (accepted, score) = filter.accept(value);
                if *negated {
                    EvalResult {
                        accepted: !accepted,
                        ..EvalResult::default()
                    }
                } else if accepted {
                    EvalResult {
                        accepted: true,
                        sum: score,
                        count: 1,
                    }
                } else {
                    EvalResult::default()
                }
            }
            Scalar::And(left, right) => {
                let left = left.evaluate(value);
                if !left.accepted {
                    return EvalResult::default();
                }
                let right = right.evaluate(value);
                if !right.accepted {
                    return EvalResult::default();
                }
                EvalResult {
                    accepted: true,
                    sum: left.sum + right.sum,
                    count: left.count + right.count,
                }
            }
            Scalar::Or(left, right) => {
                let left = left.evaluate(value);
                if left.accepted && left.perfect() {
                    return left;
                }
                let right = right.evaluate(value);
                if !right.accepted || (left.accepted && left.mean() > right.mean()) {
                    return left;
                }
                right
            }
            Scalar::BestAlternative(and) => {
                let mut paths = ScoreAlternatives::default();
                and.evaluate(value, &mut paths);
                // `validate` requires a positive term on each of this AND's paths, so count
                // zero is never present.
                let best = paths
                    .iter()
                    .map(|(count, sum)| sum / count as f64)
                    .fold(0.0, f64::max);
                EvalResult {
                    accepted: paths.counts != 0,
                    sum: best,
                    count: 1,
                }
            }
        }
    }
}

/// Retains the highest sum for each attainable positive-term count. Bit `n` of `counts` marks
/// `sums[n]` as valid; other sums are stale. Bit zero is an accepting path without positive
/// terms, and no bits means rejection.
#[derive(Clone)]
struct ScoreAlternatives {
    counts: u64,
    sums: [f64; MAX_EXPRESSION_TERMS + 1],
}

// Every attainable count must fit in the `counts` bitmask.
const _: () = assert!(MAX_EXPRESSION_TERMS < u64::BITS as usize);

impl Default for ScoreAlternatives {
    fn default() -> Self {
        Self {
            counts: 0,
            sums: [0.0; MAX_EXPRESSION_TERMS + 1],
        }
    }
}

/// The highest count in a non-empty bitmask.
fn highest_count(counts: u64) -> usize {
    (u64::BITS - 1 - counts.leading_zeros()) as usize
}

impl ScoreAlternatives {
    /// The valid `(count, sum)` pairs, lowest count first.
    fn iter(&self) -> impl Iterator<Item = (usize, f64)> + '_ {
        let mut counts = self.counts;
        std::iter::from_fn(move || {
            if counts == 0 {
                return None;
            }
            let count = counts.trailing_zeros() as usize;
            counts &= counts - 1;
            Some((count, self.sums[count]))
        })
    }

    fn add(&mut self, count: usize, sum: f64) {
        let bit = 1u64 << count;
        if self.counts & bit == 0 || sum > self.sums[count] {
            self.sums[count] = sum;
            self.counts |= bit;
        }
    }

    /// Adds the paths of an OR operand.
    fn merge(&mut self, other: &ScoreAlternatives) {
        for (count, sum) in other.iter() {
            self.add(count, sum);
        }
    }

    /// Adds a fixed-count AND operand to every path.
    fn shift(&mut self, count: usize, sum: f64) {
        // Move the highest counts first, so no sum is overwritten before it moves.
        let mut counts = self.counts;
        while counts != 0 {
            let from = highest_count(counts);
            counts &= !(1u64 << from);
            self.sums[from + count] = self.sums[from] + sum;
        }
        self.counts <<= count;
    }

    /// Pairs every path with every path of another AND operand. For any resulting count,
    /// smaller sums cannot beat the largest after adding surrounding terms.
    fn combine(&mut self, other: &ScoreAlternatives) {
        let mut pending = self.counts;
        self.counts = 0;
        // Pairs write at or above the count being read. Reading the highest counts first
        // therefore reads every original sum before a pair overwrites it.
        while pending != 0 {
            let count = highest_count(pending);
            pending &= !(1u64 << count);
            let sum = self.sums[count];
            for (other_count, other_sum) in other.iter() {
                self.add(count + other_count, sum + other_sum);
            }
        }
    }
}

/// Scores a subtree beneath an AND whose accepting paths can have different positive-term
/// counts. Its parent needs every count, because surrounding terms decide which count gives
/// the best final mean.
pub(super) enum Alternatives {
    /// A fixed-count operand.
    Fixed(Scalar),
    And(Box<Alternatives>, Box<Alternatives>),
    Or(Box<Alternatives>, Box<Alternatives>),
}

impl Alternatives {
    /// Replaces `out` with the subtree's paths.
    fn evaluate(&self, value: &str, out: &mut ScoreAlternatives) {
        match self {
            Alternatives::Fixed(_) => {
                out.counts = 0;
                self.add_to(value, out);
            }
            Alternatives::And(left, right) => {
                left.evaluate(value, out);
                if out.counts != 0 {
                    right.pair_with(value, out);
                }
            }
            Alternatives::Or(left, right) => {
                left.evaluate(value, out);
                right.add_to(value, out);
            }
        }
    }

    /// Adds the subtree's paths to `out`, as an OR does.
    fn add_to(&self, value: &str, out: &mut ScoreAlternatives) {
        match self {
            Alternatives::Fixed(scalar) => {
                let result = scalar.evaluate(value);
                if result.accepted {
                    out.add(result.count, result.sum);
                }
            }
            Alternatives::And(..) => {
                let mut paths = ScoreAlternatives::default();
                self.evaluate(value, &mut paths);
                out.merge(&paths);
            }
            Alternatives::Or(left, right) => {
                left.add_to(value, out);
                right.add_to(value, out);
            }
        }
    }

    /// Pairs `out`'s paths with the subtree's paths, as an AND does. `out` must not be empty.
    fn pair_with(&self, value: &str, out: &mut ScoreAlternatives) {
        match self {
            Alternatives::Fixed(scalar) => {
                let result = scalar.evaluate(value);
                if !result.accepted {
                    out.counts = 0;
                    return;
                }
                out.shift(result.count, result.sum);
            }
            Alternatives::And(left, right) => {
                // Pairing is associative, so pair `out` with each operand in turn.
                left.pair_with(value, out);
                if out.counts != 0 {
                    right.pair_with(value, out);
                }
            }
            Alternatives::Or(..) => {
                let mut paths = ScoreAlternatives::default();
                self.evaluate(value, &mut paths);
                out.combine(&paths);
            }
        }
    }
}

/// Reports whether every accepting path has a positive term.
fn validate_expression(
    expr: &Expr,
    negated: bool,
    depth: usize,
    term_count: &mut usize,
) -> Result<bool, SearchExprError> {
    if depth > MAX_COMPILED_EXPRESSION_DEPTH {
        return Err(invalid(format!(
            "AST depth exceeds maximum of {MAX_COMPILED_EXPRESSION_DEPTH}"
        )));
    }
    let mut children = |left: &Expr, right: &Expr| -> Result<(bool, bool), SearchExprError> {
        let left = validate_expression(left, negated, depth + 1, term_count)?;
        let right = validate_expression(right, negated, depth + 1, term_count)?;
        Ok((left, right))
    };

    match expr {
        Expr::Term(value) => {
            if value.is_empty() {
                return Err(invalid("cannot compile an empty term"));
            }
            *term_count += 1;
            if *term_count > MAX_EXPRESSION_TERMS {
                return Err(invalid(format!(
                    "term count exceeds maximum of {MAX_EXPRESSION_TERMS}"
                )));
            }
            Ok(!negated)
        }
        Expr::And(left, right) => {
            let (left, right) = children(left, right)?;
            Ok(if negated {
                left && right
            } else {
                left || right
            })
        }
        Expr::Or(left, right) => {
            let (left, right) = children(left, right)?;
            Ok(if negated {
                left || right
            } else {
                left && right
            })
        }
        Expr::Not(expr) => validate_expression(expr, !negated, depth + 1, term_count),
    }
}

/// A compiled subtree with the range of positive-term counts on its accepting paths. It is
/// conservative: some paths may never accept.
struct CompiledNode {
    eval: NodeEval,
    min_count: usize,
    max_count: usize,
}

enum NodeEval {
    Scalar(Scalar),
    /// Set only for a count-varying subtree beneath an AND.
    Alternatives(Alternatives),
}

impl CompiledNode {
    fn into_alternatives(self) -> Alternatives {
        match self.eval {
            NodeEval::Scalar(scalar) => Alternatives::Fixed(scalar),
            NodeEval::Alternatives(alternatives) => alternatives,
        }
    }

    /// Only a subtree beneath an AND keeps alternatives, and a fixed-count one never does,
    /// so callers only reach the first arm. The second reduces the paths to their best mean,
    /// which is right for any subtree that only ORs enclose.
    fn into_scalar(self) -> Scalar {
        match self.eval {
            NodeEval::Scalar(scalar) => scalar,
            NodeEval::Alternatives(alternatives) => Scalar::BestAlternative(Box::new(alternatives)),
        }
    }
}

/// Expects an expression that `validate` accepts. `in_and` is true beneath an effective AND,
/// after the De Morgan rewrite.
fn compile_node<F, E>(
    expr: &Expr,
    negated: bool,
    in_and: bool,
    new_term_filter: &mut F,
) -> Result<CompiledNode, SearchExprError>
where
    F: FnMut(&str, bool) -> Result<Box<dyn FuzzyFilter>, E>,
    E: Into<TermFilterError>,
{
    match expr {
        Expr::Term(term) => {
            let filter =
                new_term_filter(term, negated).map_err(|source| SearchExprError::TermFilter {
                    term: term.clone(),
                    source: source.into(),
                })?;
            let count = usize::from(!negated);
            Ok(CompiledNode {
                eval: NodeEval::Scalar(Scalar::Term { filter, negated }),
                min_count: count,
                max_count: count,
            })
        }
        Expr::And(left, right) if negated => {
            compile_disjunction(left, right, negated, in_and, new_term_filter)
        }
        Expr::And(left, right) => {
            compile_conjunction(left, right, negated, in_and, new_term_filter)
        }
        Expr::Or(left, right) if negated => {
            compile_conjunction(left, right, negated, in_and, new_term_filter)
        }
        Expr::Or(left, right) => compile_disjunction(left, right, negated, in_and, new_term_filter),
        Expr::Not(expr) => compile_node(expr, !negated, in_and, new_term_filter),
    }
}

fn compile_conjunction<F, E>(
    left: &Expr,
    right: &Expr,
    negated: bool,
    in_and: bool,
    new_term_filter: &mut F,
) -> Result<CompiledNode, SearchExprError>
where
    F: FnMut(&str, bool) -> Result<Box<dyn FuzzyFilter>, E>,
    E: Into<TermFilterError>,
{
    let left = compile_node(left, negated, true, new_term_filter)?;
    let right = compile_node(right, negated, true, new_term_filter)?;
    let min_count = left.min_count + right.min_count;
    let max_count = left.max_count + right.max_count;
    let eval = if min_count == max_count {
        // Every path has the same count, so the highest sum has the best mean.
        NodeEval::Scalar(Scalar::And(
            Box::new(left.into_scalar()),
            Box::new(right.into_scalar()),
        ))
    } else {
        let and = Alternatives::And(
            Box::new(left.into_alternatives()),
            Box::new(right.into_alternatives()),
        );
        if in_and {
            NodeEval::Alternatives(and)
        } else {
            // The topmost AND reduces its paths to a mean for the ORs that enclose it.
            NodeEval::Scalar(Scalar::BestAlternative(Box::new(and)))
        }
    };
    Ok(CompiledNode {
        eval,
        min_count,
        max_count,
    })
}

fn compile_disjunction<F, E>(
    left: &Expr,
    right: &Expr,
    negated: bool,
    in_and: bool,
    new_term_filter: &mut F,
) -> Result<CompiledNode, SearchExprError>
where
    F: FnMut(&str, bool) -> Result<Box<dyn FuzzyFilter>, E>,
    E: Into<TermFilterError>,
{
    let left = compile_node(left, negated, in_and, new_term_filter)?;
    let right = compile_node(right, negated, in_and, new_term_filter)?;
    let min_count = left.min_count.min(right.min_count);
    let max_count = left.max_count.max(right.max_count);
    // Without an enclosing AND, the operand with the higher mean is the best path. Beneath
    // one, operands with equal counts compare the same way.
    let eval = if !in_and || min_count == max_count {
        NodeEval::Scalar(Scalar::Or(
            Box::new(left.into_scalar()),
            Box::new(right.into_scalar()),
        ))
    } else {
        NodeEval::Alternatives(Alternatives::Or(
            Box::new(left.into_alternatives()),
            Box::new(right.into_alternatives()),
        ))
    };
    Ok(CompiledNode {
        eval,
        min_count,
        max_count,
    })
}
