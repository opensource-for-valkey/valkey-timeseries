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

// Ported from Prometheus `web/api/v1/search.go` (buildSearchFilter, buildSearchExprFilter,
// newSearchTermFilter, wrapSearchFilter, filterChainHasExpensiveScoring) at 03bfb64967.

//! Build the filter of a label search from its terms or its search expression.
//!
//! `fuzz_threshold` is in `[0.0, 1.0]`; upstream takes a percentage. Upstream knows two
//! algorithms and treats anything but subsequence as Jaro-Winkler; [`FuzzyAlgorithm::NoOp`]
//! builds the substring-only term filter that Jaro-Winkler builds at a zero threshold.

use super::search_expr::{Expr, SearchExprError, compile};
use super::{
    CaseFoldingFilter, ContainsFilter, JaroWinklerFilter, MemoizingFilter, OrFilter,
    OrSearchesFilter, SubsequenceFilter, SubstringFilter,
};
use crate::series::index::{FuzzyAlgorithm, StringFilter};
use std::convert::Infallible;

/// Builds the filter of a search for `searches`, accepting values that match any of them.
/// Empty terms are skipped; with no other terms there is no filter.
///
/// For a case-insensitive search the terms are lowercased here and the filter is wrapped in a
/// [`CaseFoldingFilter`], so each value is lowercased once. When a term filter scores at a
/// non-trivial cost (subsequence, or Jaro-Winkler above a zero threshold) the filter is wrapped
/// in a [`MemoizingFilter`], so a value that reaches it more than once in a search is scored
/// once. Substring-only filters skip the cache: substring scoring is already linear in the
/// value, and a lookup would only add to it.
pub fn build_search_filter<S: AsRef<str>>(
    searches: &[S],
    fuzz_threshold: f64,
    algorithm: FuzzyAlgorithm,
    case_sensitive: bool,
) -> Option<Box<dyn StringFilter>> {
    let mut filters: Vec<Box<dyn StringFilter>> = searches
        .iter()
        .map(AsRef::as_ref)
        .filter(|term| !term.is_empty())
        .map(|term| {
            new_search_term_filter(fold_case(term, case_sensitive), fuzz_threshold, algorithm)
        })
        .collect();
    let combined = match filters.len() {
        0 => return None,
        1 => filters.pop().expect("one filter"),
        _ => Box::new(OrSearchesFilter::new(filters)),
    };
    Some(wrap_search_filter(
        combined,
        fuzz_threshold,
        algorithm,
        case_sensitive,
    ))
}

/// Compiles a search expression into the filter of a search. Positive terms get the filter of
/// a [`build_search_filter`] term. Negated terms match literally, because a fuzzy match would
/// also exclude values that do not contain the term; their filter computes no score, since
/// [`compile`] ignores it. The case folding and caching of [`build_search_filter`] apply.
pub fn build_search_expr_filter(
    expr: &Expr,
    fuzz_threshold: f64,
    algorithm: FuzzyAlgorithm,
    case_sensitive: bool,
) -> Result<Box<dyn StringFilter>, SearchExprError> {
    let filter = compile(expr, |term, negated| {
        let term = fold_case(term, case_sensitive);
        let filter: Box<dyn StringFilter> = if negated {
            Box::new(ContainsFilter::new(term))
        } else {
            new_search_term_filter(term, fuzz_threshold, algorithm)
        };
        Ok::<_, Infallible>(filter)
    })?;
    Ok(wrap_search_filter(
        Box::new(filter),
        fuzz_threshold,
        algorithm,
        case_sensitive,
    ))
}

fn fold_case(term: &str, case_sensitive: bool) -> String {
    if case_sensitive {
        term.to_owned()
    } else {
        term.to_lowercase()
    }
}

/// The filter of one search term, which must already be lowercased for a case-insensitive
/// search.
fn new_search_term_filter(
    term: String,
    fuzz_threshold: f64,
    algorithm: FuzzyAlgorithm,
) -> Box<dyn StringFilter> {
    match algorithm {
        FuzzyAlgorithm::Subsequence => Box::new(SubsequenceFilter::new(term, fuzz_threshold)),
        // A substring match, or failing that a Jaro-Winkler one above a non-zero threshold.
        FuzzyAlgorithm::JaroWinkler if fuzz_threshold > 0.0 => {
            let fuzzy = JaroWinklerFilter::new(term.clone(), fuzz_threshold);
            Box::new(OrFilter::new(
                Some(Box::new(SubstringFilter::new(term))),
                Some(Box::new(fuzzy)),
            ))
        }
        FuzzyAlgorithm::JaroWinkler | FuzzyAlgorithm::NoOp => Box::new(SubstringFilter::new(term)),
    }
}

/// Adds the case folding and caching wrappers that [`build_search_filter`] documents.
fn wrap_search_filter(
    filter: Box<dyn StringFilter>,
    fuzz_threshold: f64,
    algorithm: FuzzyAlgorithm,
    case_sensitive: bool,
) -> Box<dyn StringFilter> {
    let filter: Box<dyn StringFilter> = if case_sensitive {
        filter
    } else {
        Box::new(CaseFoldingFilter::new(filter))
    };
    if has_expensive_scoring(fuzz_threshold, algorithm) {
        Box::new(MemoizingFilter::new(filter))
    } else {
        filter
    }
}

/// Whether the term filters [`new_search_term_filter`] builds score at a cost that a cache
/// repays.
fn has_expensive_scoring(fuzz_threshold: f64, algorithm: FuzzyAlgorithm) -> bool {
    match algorithm {
        FuzzyAlgorithm::Subsequence => true,
        // Jaro-Winkler only runs above a zero threshold; below it the filter is substring-only.
        FuzzyAlgorithm::JaroWinkler => fuzz_threshold > 0.0,
        FuzzyAlgorithm::NoOp => false,
    }
}

#[cfg(test)]
mod tests {
    use super::super::search_expr::parse;
    use super::*;

    const JW: FuzzyAlgorithm = FuzzyAlgorithm::JaroWinkler;
    const SUBSEQ: FuzzyAlgorithm = FuzzyAlgorithm::Subsequence;

    fn search(terms: &[&str], threshold: f64, algorithm: FuzzyAlgorithm) -> Box<dyn StringFilter> {
        build_search_filter(terms, threshold, algorithm, true).expect("a filter")
    }

    fn expr(
        input: &str,
        threshold: f64,
        algorithm: FuzzyAlgorithm,
        case_sensitive: bool,
    ) -> Box<dyn StringFilter> {
        let expr = parse(input).unwrap_or_else(|err| panic!("parse({input:?}): {err}"));
        build_search_expr_filter(&expr, threshold, algorithm, case_sensitive)
            .unwrap_or_else(|err| panic!("build({input:?}): {err}"))
    }

    #[test]
    fn no_usable_terms_build_no_filter() {
        let none: &[&str] = &[];
        assert!(build_search_filter(none, 0.0, JW, true).is_none());
        assert!(build_search_filter(&["", ""], 0.5, SUBSEQ, false).is_none());
    }

    #[test]
    fn empty_terms_are_skipped() {
        let filter = search(&["", "cpu"], 0.0, JW);
        assert_eq!(filter.accept("cpu"), (true, 1.0));
        assert_eq!(filter.accept("memory"), (false, 0.0));
    }

    #[test]
    fn one_term_scores_as_its_term_filter() {
        let filter = search(&["cpu"], 0.0, JW);
        let term = SubstringFilter::new("cpu");
        for value in ["cpu", "node_cpu", "memory"] {
            assert_eq!(filter.accept(value), term.accept(value), "{value}");
        }
    }

    #[test]
    fn terms_combine_with_or_and_the_best_score() {
        let filter = search(&["cpu", "node"], 0.0, JW);
        assert_eq!(filter.accept("node_cpu"), (true, 1.0));
        assert_eq!(
            filter.accept("xx_cpu"),
            SubstringFilter::new("cpu").accept("xx_cpu")
        );
        assert_eq!(filter.accept("memory"), (false, 0.0));
    }

    #[test]
    fn zero_threshold_jaro_winkler_is_substring_only() {
        let filter = search(&["metric"], 0.0, JW);
        assert_eq!(filter.accept("metirc"), (false, 0.0));
    }

    #[test]
    fn jaro_winkler_falls_back_from_a_substring_match() {
        let filter = search(&["metric"], 0.8, JW);
        assert_eq!(
            filter.accept("my_metric"),
            SubstringFilter::new("metric").accept("my_metric")
        );
        let fuzzy = JaroWinklerFilter::new("metric", 0.8).accept("metirc");
        assert!(fuzzy.0);
        assert_eq!(filter.accept("metirc"), fuzzy);
        // Twice, so the second answer comes from the cache.
        assert_eq!(filter.accept("metirc"), fuzzy);
    }

    #[test]
    fn subsequence_uses_the_subsequence_filter() {
        let filter = search(&["nde"], 0.0, SUBSEQ);
        let term = SubsequenceFilter::new("nde", 0.0);
        for value in ["nde_x", "node", "disk"] {
            assert_eq!(filter.accept(value), term.accept(value), "{value}");
        }
    }

    #[test]
    fn noop_is_substring_only_at_any_threshold() {
        let filter = search(&["metric"], 0.8, FuzzyAlgorithm::NoOp);
        assert_eq!(filter.accept("metirc"), (false, 0.0));
        assert_eq!(filter.accept("metric"), (true, 1.0));
    }

    #[test]
    fn case_insensitive_search_folds_terms_and_values() {
        let filter = build_search_filter(&["CPU"], 0.0, JW, false).unwrap();
        assert_eq!(
            filter.accept("Node_Cpu"),
            SubstringFilter::new("cpu").accept("node_cpu")
        );
        assert_eq!(filter.accept("CPU"), (true, 1.0));

        let sensitive = search(&["CPU"], 0.0, JW);
        assert_eq!(sensitive.accept("cpu"), (false, 0.0));
    }

    #[test]
    fn expensive_scoring_follows_the_term_filters() {
        assert!(has_expensive_scoring(0.0, SUBSEQ));
        assert!(has_expensive_scoring(0.5, JW));
        assert!(!has_expensive_scoring(0.0, JW));
        assert!(!has_expensive_scoring(0.5, FuzzyAlgorithm::NoOp));
    }

    #[test]
    fn expression_terms_use_the_search_term_filter() {
        let filter = expr("cpu AND node", 0.0, JW, true);
        assert_eq!(filter.accept("node_cpu"), {
            let cpu = SubstringFilter::new("cpu").accept("node_cpu").1;
            (true, (1.0 + cpu) / 2.0)
        });
        assert_eq!(filter.accept("cpu"), (false, 0.0));
    }

    #[test]
    fn negated_expression_terms_match_literally() {
        // A fuzzy "memry" would also exclude "cpu_memory"; the literal one does not.
        let filter = expr("cpu AND NOT memry", 0.8, JW, true);
        assert!(filter.accept("cpu_memory").0);
        assert!(!filter.accept("cpu_memry").0);

        let subsequence = expr("cpu AND NOT mry", 0.0, SUBSEQ, true);
        assert!(
            subsequence.accept("cpu_memory").0,
            "mry is a subsequence, not a substring"
        );
        assert!(!subsequence.accept("cpu_mry").0);
    }

    #[test]
    fn case_insensitive_expression_folds_every_term() {
        let filter = expr("CPU AND NOT Idle", 0.0, JW, false);
        assert_eq!(
            filter.accept("Node_cpu"),
            SubstringFilter::new("cpu").accept("node_cpu")
        );
        assert!(!filter.accept("cpu_IDLE").0);
    }

    #[test]
    fn invalid_expression_is_refused() {
        let expr = parse("NOT cpu").unwrap();
        assert!(build_search_expr_filter(&expr, 0.0, JW, true).is_err());
    }
}
