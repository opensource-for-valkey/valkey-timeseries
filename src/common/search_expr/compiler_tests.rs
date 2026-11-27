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

// Ported from Prometheus `util/searchexpr/compiler_test.go`. Its nil, unknown-node and
// pointer-node cases have no counterpart: `Expr` is a closed enum of owned nodes.

use super::compiler::{CompiledFilter, MAX_COMPILED_EXPRESSION_DEPTH, Scalar, validate};
use super::parser::parse;
use super::{
    Expr, MAX_EXPRESSION_NESTING_DEPTH, MAX_EXPRESSION_TERMS, SearchExprError, TermFilterError,
    compile,
};
use crate::common::strings::filter::StringFilter;
use crate::series::index::{
    LabelSearchResult, SearchHints, SearchResultOrdering, apply_search_hints,
};
use proptest::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

const EPSILON: f64 = 1e-9;

/// Per-term scores for a table case.
type Scores = &'static [(&'static str, f64)];

struct ContainsFilter {
    term: String,
    score: f64,
}

impl StringFilter for ContainsFilter {
    fn accept(&self, value: &str) -> (bool, f64) {
        if value.contains(&self.term) {
            (true, self.score)
        } else {
            (false, 0.0)
        }
    }
}

type Calls = Arc<Mutex<HashMap<String, usize>>>;

/// Records each call, then answers with a fixed verdict.
struct CountingFilter {
    accepted: bool,
    score: f64,
    calls: Calls,
    term: String,
}

impl StringFilter for CountingFilter {
    fn accept(&self, _value: &str) -> (bool, f64) {
        *self
            .calls
            .lock()
            .unwrap()
            .entry(self.term.clone())
            .or_default() += 1;
        (self.accepted, self.score)
    }
}

type Factory<'a> =
    Box<dyn FnMut(&str, bool) -> Result<Box<dyn StringFilter>, TermFilterError> + 'a>;

/// Term filters that match by substring and score from `scores`, where a missing term
/// scores 0.
fn contains_factory(scores: &HashMap<String, f64>) -> Factory<'_> {
    Box::new(move |term, _| {
        Ok(Box::new(ContainsFilter {
            term: term.to_string(),
            score: scores.get(term).copied().unwrap_or(0.0),
        }))
    })
}

fn score_table(scores: &[(&str, f64)]) -> HashMap<String, f64> {
    scores.iter().map(|&(t, s)| (t.to_string(), s)).collect()
}

#[track_caller]
fn compile_contains(input: &str, scores: &[(&str, f64)]) -> CompiledFilter {
    let expr = parse(input).unwrap_or_else(|err| panic!("parse({input:?}): {err}"));
    compile(&expr, contains_factory(&score_table(scores)))
        .unwrap_or_else(|err| panic!("compile({input:?}): {err}"))
}

/// Whether the root AND keeps score alternatives, so a case meant for one evaluation path
/// cannot silently test the other.
#[track_caller]
fn assert_alternatives(filter: &CompiledFilter, want: bool) {
    let got = matches!(filter.root, Scalar::BestAlternative(_));
    assert_eq!(got, want, "root keeps score alternatives");
}

#[track_caller]
fn assert_close(got: f64, want: f64, context: &str) {
    assert!(
        (got - want).abs() <= EPSILON,
        "{context}: score {got}, want {want}"
    );
}

#[test]
fn compile_not_expressions() {
    let scores = [("foo", 0.8), ("old", 0.6), ("stale", 0.5)];
    for (name, expression, value, want_accept, want_score) in [
        (
            "positive AND negative keeps positive score",
            "foo AND NOT old",
            "foo_new",
            true,
            0.8,
        ),
        (
            "positive AND negative rejects excluded value",
            "foo AND NOT old",
            "foo_old",
            false,
            0.0,
        ),
        (
            "double negation keeps positive score",
            "NOT NOT foo",
            "foo_new",
            true,
            0.8,
        ),
        (
            "double negation rejects a non-match",
            "NOT NOT foo",
            "brand_new",
            false,
            0.0,
        ),
        (
            "nested negation keeps positive branch score",
            "NOT (NOT foo OR old)",
            "foo_new",
            true,
            0.8,
        ),
        (
            "nested negation rejects excluded value",
            "NOT (NOT foo OR old)",
            "foo_old",
            false,
            0.0,
        ),
        (
            "negated AND accepts value missing one term",
            "foo AND NOT (old AND stale)",
            "foo_old",
            true,
            0.8,
        ),
        (
            "negated AND rejects value with both terms",
            "foo AND NOT (old AND stale)",
            "foo_old_stale",
            false,
            0.0,
        ),
    ] {
        let filter = compile_contains(expression, &scores);
        let (accepted, score) = filter.accept(value);
        assert_eq!(accepted, want_accept, "{name}");
        assert_close(score, want_score, name);
    }
}

#[test]
fn compile_scores_mean_of_positive_terms() {
    let default_scores: &[(&str, f64)] = &[
        ("foo", 0.8),
        ("bar", 0.4),
        ("baz", 0.7),
        ("low", 0.2),
        ("high", 0.8),
    ];
    let even: &[(&str, f64)] = &[("a", 0.75), ("b", 0.75), ("c", 0.75), ("d", 0.25)];
    let perfect: &[(&str, f64)] = &[("a", 1.0), ("b", 1.0), ("c", 1.0), ("d", 0.25)];
    let strong: &[(&str, f64)] = &[("a", 0.75), ("b", 0.75), ("c", 0.75), ("d", 1.0)];
    let cases: &[(&str, &str, &str, f64, Scores)] = &[
        (
            "AND is the mean of both terms",
            "foo AND bar OR baz",
            "foo_bar",
            0.6,
            default_scores,
        ),
        (
            "OR picks the operand with the higher mean",
            "foo AND bar OR baz",
            "foo_bar_baz",
            0.7,
            default_scores,
        ),
        (
            "OR with one accepted operand",
            "foo AND bar OR baz",
            "baz",
            0.7,
            default_scores,
        ),
        (
            "AND does not use the lowest term",
            "low AND high",
            "low_high",
            0.5,
            default_scores,
        ),
        (
            "OR keeps the left operand when it is higher",
            "high OR low",
            "high_low",
            0.8,
            default_scores,
        ),
        (
            "nested AND includes every term",
            "foo AND (bar AND high)",
            "foo_bar_high",
            (0.8 + 0.4 + 0.8) / 3.0,
            default_scores,
        ),
        (
            "unscored subtree adds no term",
            "(NOT old AND NOT stale) AND foo",
            "foo",
            0.8,
            default_scores,
        ),
        (
            "unscored OR subtree adds no term",
            "(NOT old OR NOT stale) AND foo",
            "foo_old",
            0.8,
            default_scores,
        ),
        (
            "OR takes a positive operand that raises the mean",
            "bar AND (foo OR NOT old)",
            "bar_foo",
            0.6,
            default_scores,
        ),
        // Equal OR means can carry different term counts into a surrounding AND. Both
        // operand orders must retain the best complete-path mean.
        (
            "OR keeps both counts under AND",
            "(a OR (b AND c)) AND d",
            "a_b_c_d",
            1.75 / 3.0,
            even,
        ),
        (
            "reversed OR keeps both counts under AND",
            "((b AND c) OR a) AND d",
            "a_b_c_d",
            1.75 / 3.0,
            even,
        ),
        (
            "De Morgan rewrite keeps both counts under AND",
            "NOT (NOT (a OR (b AND c)) OR NOT d)",
            "a_b_c_d",
            1.75 / 3.0,
            even,
        ),
        (
            "perfect OR keeps both counts under AND",
            "(a OR (b AND c)) AND d",
            "a_b_c_d",
            0.75,
            perfect,
        ),
        (
            "reversed perfect OR keeps both counts under AND",
            "((b AND c) OR a) AND d",
            "a_b_c_d",
            0.75,
            perfect,
        ),
        // Preferring the larger count on ties also fails: a strong surrounding term makes
        // the smaller branch preferable instead.
        (
            "strong conjunct prefers fewer terms",
            "(a OR (b AND c)) AND d",
            "a_b_c_d",
            0.875,
            strong,
        ),
        // A locally better mean can produce a worse complete-path mean even without a tie,
        // so tie-breaking alone cannot fix score composition.
        (
            "lower local mean wins with more terms",
            "(a OR (b AND c)) AND d",
            "a_b_c_d",
            1.7 / 3.0,
            &[("a", 0.9), ("b", 0.8), ("c", 0.8), ("d", 0.1)],
        ),
        (
            "before improving OR terms",
            "(a OR (b AND c)) AND d",
            "a_b_c_d",
            0.75,
            &[("a", 0.5), ("b", 0.49), ("c", 0.49), ("d", 1.0)],
        ),
        (
            "improving OR terms cannot lower score",
            "(a OR (b AND c)) AND d",
            "a_b_c_d",
            0.75,
            &[("a", 0.5), ("b", 0.51), ("c", 0.51), ("d", 1.0)],
        ),
        // An exclusion adds no term, so also accepting a weak positive operand cannot lower
        // the score below that of a value without it.
        (
            "OR takes an exclusion over a weaker positive operand",
            "bar AND (foo OR NOT old)",
            "bar_foo",
            0.9,
            &[("bar", 0.9), ("foo", 0.1)],
        ),
    ];
    for &(name, expression, value, want_score, scores) in cases {
        let filter = compile_contains(expression, scores);
        let (accepted, score) = filter.accept(value);
        assert!(accepted, "{name}");
        assert_close(score, want_score, name);
    }
}

#[test]
fn compile_scores_at_term_limit() {
    // Ten independent OR choices have 1024 accepting paths but only eleven possible counts.
    // The evaluator must retain those counts within the limit.
    let expression = "(a OR (b AND c)) AND ".repeat(10) + "d AND e";
    for (name, branch, conjunct, want_score) in [
        ("weak conjuncts prefer more terms", 0.75, 0.25, 15.5 / 22.0),
        ("strong conjuncts prefer fewer terms", 0.75, 1.0, 9.5 / 12.0),
        (
            "perfect branches retain their counts",
            1.0,
            0.25,
            20.5 / 22.0,
        ),
    ] {
        let filter = compile_contains(
            &expression,
            &[
                ("a", branch),
                ("b", branch),
                ("c", branch),
                ("d", conjunct),
                ("e", conjunct),
            ],
        );
        let (accepted, score) = filter.accept("a_b_c_d_e");
        assert!(accepted, "{name}");
        assert_close(score, want_score, name);
    }
}

#[test]
fn compile_equivalent_expressions_rank_identically() {
    // The other candidate lies between the two scores an operand-order-dependent evaluation
    // would produce, so a limit of one exposes any difference.
    for expression in [
        "((a OR (b AND c)) AND d) OR other",
        "(((b AND c) OR a) AND d) OR other",
    ] {
        let filter = compile_contains(
            expression,
            &[
                ("a", 0.75),
                ("b", 0.75),
                ("c", 0.75),
                ("d", 0.25),
                ("other", 0.55),
            ],
        );
        let candidates = ["a_b_c_d", "other"]
            .into_iter()
            .filter_map(|value| {
                let (accepted, score) = filter.accept(value);
                accepted.then(|| LabelSearchResult {
                    value: value.to_string(),
                    score,
                    cardinality: 0,
                })
            })
            .collect();
        let hints = SearchHints {
            limit: 1,
            order_by: SearchResultOrdering::ScoreDesc,
            ..SearchHints::default()
        };
        let results = apply_search_hints(candidates, &hints).results;
        assert_eq!(
            results,
            vec![LabelSearchResult {
                value: "a_b_c_d".to_string(),
                score: 1.75 / 3.0,
                cardinality: 0,
            }],
            "{expression}"
        );
    }
}

#[test]
fn compile_accept_is_safe_for_concurrent_use() {
    let cases = [
        ("foo_only", true, 0.9),
        ("bar_only", true, 0.3),
        ("foo_bar", true, 0.9),
        ("neither", false, 0.0),
    ];
    // Exercise both evaluation paths; score alternatives must stay request-local.
    for (expression, alternatives) in [
        ("foo OR bar", false),
        ("(foo OR (bar AND bar)) AND NOT old", true),
    ] {
        let filter = compile_contains(expression, &[("foo", 0.9), ("bar", 0.3)]);
        assert_alternatives(&filter, alternatives);
        std::thread::scope(|scope| {
            for _ in 0..16 {
                scope.spawn(|| {
                    for _ in 0..50 {
                        for (value, want_accept, want_score) in cases {
                            let (accepted, score) = filter.accept(value);
                            assert_eq!(accepted, want_accept, "{value}");
                            assert_close(score, want_score, value);
                        }
                    }
                });
            }
        });
    }
}

fn counting_factory(
    calls: &Calls,
    accept: impl Fn(&str) -> bool,
    score: impl Fn(&str) -> f64,
) -> impl FnMut(&str, bool) -> Result<Box<dyn StringFilter>, TermFilterError> {
    move |term, _| {
        Ok(Box::new(CountingFilter {
            accepted: accept(term),
            score: score(term),
            calls: Arc::clone(calls),
            term: term.to_string(),
        }))
    }
}

fn call_counts(terms: &str) -> HashMap<String, usize> {
    let mut counts = HashMap::new();
    for term in terms.split_whitespace() {
        *counts.entry(term.to_string()).or_default() += 1;
    }
    counts
}

#[test]
fn compile_and_stops_at_first_rejection() {
    // Both the scalar and the alternative-score evaluators must stop on rejection.
    for (expression, alternatives) in [
        ("first AND later", false),
        ("first AND (later OR (a AND b))", true),
    ] {
        let calls = Calls::default();
        let expr = parse(expression).unwrap();
        let filter = compile(&expr, counting_factory(&calls, |t| t != "first", |_| 0.8)).unwrap();
        assert_alternatives(&filter, alternatives);

        assert_eq!(filter.accept("anything"), (false, 0.0), "{expression}");
        assert_eq!(*calls.lock().unwrap(), call_counts("first"), "{expression}");
    }
}

#[test]
fn compile_or_stops_only_at_perfect_positive_match() {
    let cases: &[(&str, &str, Scores, &str, f64, &str)] = &[
        (
            "scalar perfect match",
            "perfect OR later",
            &[],
            "",
            1.0,
            "perfect",
        ),
        // The unused subtree keeps score alternatives. A perfect root match must still skip
        // all of it.
        (
            "perfect root match",
            "perfect OR (anchor AND (a OR (b AND c)))",
            &[],
            "",
            1.0,
            "perfect",
        ),
        (
            "chained root ORs",
            "perfect OR later OR (anchor AND (a OR (b AND c)))",
            &[],
            "",
            1.0,
            "perfect",
        ),
        (
            "De Morgan root OR",
            "NOT (NOT perfect AND NOT (anchor AND (a OR (b AND c))))",
            &[],
            "",
            1.0,
            "perfect",
        ),
        // The perfect alternative need not have the smallest positive count.
        (
            "perfect path among several counts",
            "(a AND (b OR (c AND d))) OR later",
            &[("b", 0.5)],
            "",
            1.0,
            "a b c d",
        ),
        // An enclosing AND needs the larger-count perfect path to maximize its final mean.
        // OR must preserve that context through either child.
        (
            "perfect OR under AND",
            "anchor AND (perfect OR (b AND c))",
            &[("anchor", 0.25)],
            "",
            0.75,
            "anchor perfect b c",
        ),
        (
            "left nested OR under AND",
            "anchor AND ((perfect OR (b AND c)) OR later)",
            &[("anchor", 0.25)],
            "",
            0.75,
            "anchor perfect b c later",
        ),
        (
            "right nested OR under AND",
            "anchor AND (perfect OR (later OR (b AND c)))",
            &[("anchor", 0.25)],
            "",
            0.75,
            "anchor perfect later b c",
        ),
        (
            "De Morgan enclosing AND",
            "NOT (NOT anchor OR NOT (perfect OR (b AND c)))",
            &[("anchor", 0.25)],
            "",
            0.75,
            "anchor perfect b c",
        ),
        // Rejection and non-perfect scores leave room for the other operand.
        (
            "rejected left operand",
            "perfect OR (anchor AND (a OR (b AND c)))",
            &[],
            "perfect",
            1.0,
            "perfect anchor a b c",
        ),
        (
            "zero left score",
            "perfect OR (anchor AND (a OR (b AND c)))",
            &[("perfect", 0.0)],
            "",
            1.0,
            "perfect anchor a b c",
        ),
        (
            "non-perfect left score",
            "perfect OR (anchor AND (a OR (b AND c)))",
            &[("perfect", 0.5)],
            "",
            1.0,
            "perfect anchor a b c",
        ),
        // An accepted exclusion adds no term, but must not hide a later positive operand
        // that raises the mean.
        (
            "accepted exclusion under AND",
            "anchor AND (NOT old OR later)",
            &[("anchor", 0.5)],
            "old",
            0.75,
            "anchor old later",
        ),
    ];
    for &(name, expression, scores, rejected, want_score, want_calls) in cases {
        let calls = Calls::default();
        let scores = score_table(scores);
        let expr = parse(expression).unwrap();
        let filter = compile(
            &expr,
            counting_factory(
                &calls,
                |t| t != rejected,
                |t| scores.get(t).copied().unwrap_or(1.0),
            ),
        )
        .unwrap();

        let (accepted, score) = filter.accept("anything");
        assert!(accepted, "{name}");
        assert_eq!(score, want_score, "{name}");
        assert_eq!(*calls.lock().unwrap(), call_counts(want_calls), "{name}");
    }
}

#[test]
fn compile_reports_effective_term_polarity() {
    let expr = parse("anchor AND NOT old AND NOT NOT restored AND NOT (gone OR NOT back)").unwrap();
    let mut polarities = HashMap::new();
    compile(&expr, |term, negated| {
        polarities.insert(term.to_string(), negated);
        Ok::<_, TermFilterError>(Box::new(ContainsFilter {
            term: term.to_string(),
            score: 1.0,
        }) as Box<dyn StringFilter>)
    })
    .unwrap();
    let want: HashMap<String, bool> = [
        ("anchor", false),
        ("old", true),
        ("restored", false),
        ("gone", true),
        ("back", false),
    ]
    .into_iter()
    .map(|(t, n)| (t.to_string(), n))
    .collect();
    assert_eq!(polarities, want);
}

#[derive(Debug, thiserror::Error)]
#[error("factory failed")]
struct FactoryFailed;

fn failing_on_bar(term: &str, _: bool) -> Result<Box<dyn StringFilter>, FactoryFailed> {
    if term == "bar" {
        return Err(FactoryFailed);
    }
    Ok(Box::new(ContainsFilter {
        term: term.to_string(),
        score: 0.0,
    }))
}

#[test]
fn compile_rejects_invalid_inputs() {
    let no_scores = HashMap::new();
    let err = compile(&Expr::term(""), contains_factory(&no_scores)).err();
    assert_eq!(
        err.map(|e| e.to_string()).as_deref(),
        Some("search expression: cannot compile an empty term")
    );

    for expr in [
        Expr::and(Expr::term("foo"), Expr::term("bar")),
        // The left operand compiles first.
        Expr::or(Expr::term("bar"), Expr::term("foo")),
    ] {
        let err = compile(&expr, failing_on_bar).err();
        assert_eq!(
            err.map(|e| e.to_string()).as_deref(),
            Some(r#"search expression: compile term "bar": factory failed"#),
            "{expr:?}"
        );
    }
}

#[test]
fn compile_wraps_factory_error() {
    let Err(err) = compile(&Expr::term("bar"), failing_on_bar) else {
        panic!("compile succeeded");
    };
    assert!(matches!(&err, SearchExprError::TermFilter { term, .. } if term == "bar"));
    let source = std::error::Error::source(&err).expect("error has a source");
    assert!(source.downcast_ref::<FactoryFailed>().is_some());
}

const NO_POSITIVE_TERM: &str =
    "search expression: every accepting path must require a positive term";

#[test]
fn validate_rejects_expressions_without_positive_term() {
    let no_scores = HashMap::new();
    for expr in [
        Expr::not(Expr::term("old")),
        Expr::or(Expr::term("foo"), Expr::not(Expr::term("old"))),
        Expr::not(Expr::or(Expr::term("foo"), Expr::term("old"))),
        Expr::not(Expr::and(Expr::term("foo"), Expr::term("old"))),
        Expr::or(
            Expr::and(Expr::term("foo"), Expr::not(Expr::term("old"))),
            Expr::not(Expr::term("stale")),
        ),
    ] {
        assert_eq!(
            validate(&expr).unwrap_err().to_string(),
            NO_POSITIVE_TERM,
            "{expr:?}"
        );
        let err = compile(&expr, contains_factory(&no_scores)).err();
        assert_eq!(
            err.map(|e| e.to_string()).as_deref(),
            Some(NO_POSITIVE_TERM),
            "{expr:?}"
        );
    }
}

#[test]
fn validate_accepts_expressions_with_positive_term() {
    for input in [
        "foo",
        "NOT NOT foo",
        "foo AND NOT old",
        "NOT (NOT foo OR old)",
        "NOT (old AND NOT foo) AND bar",
        "(NOT old OR NOT stale) AND foo",
    ] {
        let expr = parse(input).unwrap();
        if let Err(err) = validate(&expr) {
            panic!("validate({input:?}): {err}");
        }
    }
}

#[test]
fn validate_rejects_ast_outside_resource_limits() {
    let mut too_many_terms = Expr::term("term");
    for _ in 0..MAX_EXPRESSION_TERMS {
        too_many_terms = Expr::and(too_many_terms, Expr::term("term"));
    }
    assert_eq!(
        validate(&too_many_terms).unwrap_err().to_string(),
        "search expression: term count exceeds maximum of 32"
    );

    let mut too_deep = Expr::term("foo");
    for _ in 0..MAX_COMPILED_EXPRESSION_DEPTH {
        too_deep = Expr::not(too_deep);
    }
    assert_eq!(
        validate(&too_deep).unwrap_err().to_string(),
        "search expression: AST depth exceeds maximum of 48"
    );
}

fn expr_depth(expr: &Expr) -> usize {
    match expr {
        Expr::Term(_) => 1,
        Expr::And(left, right) | Expr::Or(left, right) => {
            1 + expr_depth(left).max(expr_depth(right))
        }
        Expr::Not(expr) => 1 + expr_depth(expr),
    }
}

#[test]
fn compile_accepts_deepest_parsed_expression() {
    let input = "NOT ".repeat(MAX_EXPRESSION_NESTING_DEPTH)
        + &vec!["term"; MAX_EXPRESSION_TERMS].join(" AND ");
    let expr = parse(&input).unwrap();
    assert_eq!(expr_depth(&expr), MAX_COMPILED_EXPRESSION_DEPTH);

    let filter = compile(&expr, contains_factory(&score_table(&[("term", 1.0)]))).unwrap();
    assert!(!filter.accept("other").0);
}

/// Evaluates `expr` without the De Morgan rewrite.
fn evaluate_directly(expr: &Expr, value: &str) -> bool {
    match expr {
        Expr::Term(term) => value.contains(term.as_str()),
        Expr::And(left, right) => evaluate_directly(left, value) && evaluate_directly(right, value),
        Expr::Or(left, right) => evaluate_directly(left, value) || evaluate_directly(right, value),
        Expr::Not(expr) => !evaluate_directly(expr, value),
    }
}

/// Enumerates leaf scores instead of pruning by term count: an independent score oracle for
/// expressions with few terms.
fn scored_accepting_paths(
    expr: &Expr,
    value: &str,
    negated: bool,
    scores: &HashMap<String, f64>,
) -> Vec<Vec<f64>> {
    let (left, right, conjunction) = match expr {
        Expr::Term(term) => {
            let accepted = value.contains(term.as_str());
            return match (negated, accepted) {
                (true, false) => vec![vec![]],
                (false, true) => vec![vec![scores.get(term).copied().unwrap_or(0.0)]],
                _ => vec![],
            };
        }
        Expr::Not(expr) => return scored_accepting_paths(expr, value, !negated, scores),
        Expr::And(left, right) => (left, right, !negated),
        Expr::Or(left, right) => (left, right, negated),
    };

    let left_paths = scored_accepting_paths(left, value, negated, scores);
    let right_paths = scored_accepting_paths(right, value, negated, scores);
    if !conjunction {
        return [left_paths, right_paths].concat();
    }
    let mut paths = Vec::new();
    for l in &left_paths {
        for r in &right_paths {
            paths.push([l.as_slice(), r.as_slice()].concat());
        }
    }
    paths
}

const ORACLE_VALUES: [&str; 8] = [
    "",
    "foo_new_metric",
    "foo_old",
    "old metric",
    "AND",
    "f",
    "a_b_c_d",
    "foo_bar_baz",
];

/// Checks acceptance against direct evaluation, then scores against the exhaustive path
/// oracle.
fn check_against_oracles(expr: &Expr) {
    if validate(expr).is_err() {
        assert!(compile(expr, contains_factory(&HashMap::new())).is_err());
        return;
    }

    let mut scores = HashMap::new();
    let mut term_count = 0;
    let filter = compile(expr, |term, _| {
        term_count += 1;
        // Vary scores deterministically, including zero and perfect matches.
        let score = ((term.len() + usize::from(term.as_bytes()[0])) % 9) as f64 / 8.0;
        scores.insert(term.to_string(), score);
        Ok::<_, TermFilterError>(Box::new(ContainsFilter {
            term: term.to_string(),
            score: 0.5,
        }) as Box<dyn StringFilter>)
    })
    .unwrap();
    for value in ORACLE_VALUES {
        let (accepted, score) = filter.accept(value);
        assert_eq!(
            accepted,
            evaluate_directly(expr, value),
            "{expr:?} on {value:?}"
        );
        // Every positive term scores 0.5, so every mean is 0.5.
        let want = if accepted { 0.5 } else { 0.0 };
        assert_close(score, want, value);
    }

    // Exhaustive paths can grow exponentially; the compiled evaluator stays bounded by the
    // term count. Keep the oracle's inputs small.
    if term_count > 8 {
        return;
    }
    let filter = compile(expr, contains_factory(&scores)).unwrap();
    for value in ORACLE_VALUES {
        let paths = scored_accepting_paths(expr, value, false, &scores);
        let mut want_score: f64 = 0.0;
        for path in &paths {
            assert!(
                !path.is_empty(),
                "validated accepting paths require a positive term"
            );
            want_score = want_score.max(path.iter().sum::<f64>() / path.len() as f64);
        }
        let (accepted, score) = filter.accept(value);
        assert_eq!(accepted, !paths.is_empty(), "{expr:?} on {value:?}");
        assert_close(score, want_score, value);
    }
}

#[test]
fn compile_seed_corpus_matches_oracles() {
    for input in [
        "foo",
        "NOT foo",
        "foo AND NOT old",
        "NOT NOT foo",
        "NOT (foo OR old)",
        "NOT (NOT foo OR old)",
        "(NOT o OR NOT n) AND f",
        r#""AND" OR NOT "old metric""#,
        "(a OR (b AND c)) AND d",
        "((b AND c) OR a) AND d",
        "NOT (NOT (a OR (b AND c)) OR NOT d)",
    ] {
        check_against_oracles(&parse(input).unwrap());
    }
}

/// ASTs over terms that occur in [`ORACLE_VALUES`], so acceptance varies by value.
fn arbitrary_expr() -> impl Strategy<Value = Expr> {
    let leaf = prop::sample::select(vec![
        "foo",
        "old",
        "new",
        "metric",
        "f",
        "o",
        "n",
        "a",
        "b",
        "c",
        "d",
        "bar",
        "baz",
        "old metric",
        "AND",
        "_",
    ])
    .prop_map(Expr::term);
    leaf.prop_recursive(6, 24, 2, |inner| {
        prop_oneof![
            (inner.clone(), inner.clone()).prop_map(|(l, r)| Expr::and(l, r)),
            (inner.clone(), inner.clone()).prop_map(|(l, r)| Expr::or(l, r)),
            inner.prop_map(Expr::not),
        ]
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2048))]

    #[test]
    fn compiled_filters_match_oracles(expr in arbitrary_expr()) {
        check_against_oracles(&expr);
    }
}
