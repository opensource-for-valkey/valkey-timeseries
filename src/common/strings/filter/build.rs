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
// newSearchTermFilter, wrapSearchFilter) at 03bfb64967.

//! Build the filter of a label search from its terms or its search expression.
//!
//! `fuzz_threshold` is a whole percentage in `[0, 100]`, as upstream takes it. Each term filter
//! normalizes it to `[0.0, 1.0]`, the scale of the scores it returns. Upstream knows two
//! algorithms and treats anything but subsequence as Jaro-Winkler; [`FuzzyAlgorithm::NoOp`]
//! builds the substring-only term filter that Jaro-Winkler builds at a zero threshold.
//!
//! Upstream also wraps filters that score at a non-trivial cost in a memo cache, because a
//! value reaches its filter once per TSDB block. Here a search sees each distinct value about
//! once, so every lookup would miss: the builders leave the cache out.

use super::combining::{OrFilter, OrSearchesFilter};
use super::term::{ContainsFilter, JaroWinklerFilter, SubsequenceFilter, SubstringFilter};
use super::types::StringFilter;
use super::wrapping::CaseFoldingFilter;
use crate::common::search_expr::{Expr, SearchExprError, compile};
use crate::common::strings::filter::FuzzyAlgorithm;
use std::convert::Infallible;

/// Builds the filter of a search for `searches`, accepting values that match any of them.
/// Empty terms are skipped; with no other terms there is no filter.
///
/// For a case-insensitive search the terms are lowercased here and the filter is wrapped in a
/// [`CaseFoldingFilter`], so each value is lowercased once.
pub fn build_search_filter<S: AsRef<str>>(
    searches: &[S],
    fuzz_threshold: u8,
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
    Some(wrap_search_filter(combined, case_sensitive))
}

/// Compiles a search expression into the filter of a search. Positive terms get the filter of
/// a [`build_search_filter`] term. Negated terms match literally, because a fuzzy match would
/// also exclude values that do not contain the term; their filter computes no score, since
/// [`compile`] ignores it. The case folding of [`build_search_filter`] applies.
pub fn build_search_expr_filter(
    expr: &Expr,
    fuzz_threshold: u8,
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
    Ok(wrap_search_filter(Box::new(filter), case_sensitive))
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
    fuzz_threshold: u8,
    algorithm: FuzzyAlgorithm,
) -> Box<dyn StringFilter> {
    debug_assert!(
        fuzz_threshold <= 100,
        "fuzz_threshold {fuzz_threshold} is not a percentage"
    );
    let threshold = f64::from(fuzz_threshold) / 100.0;
    match algorithm {
        FuzzyAlgorithm::Subsequence => Box::new(SubsequenceFilter::new(term, threshold)),
        // A substring match, or failing that a Jaro-Winkler one above a non-zero threshold.
        FuzzyAlgorithm::JaroWinkler if fuzz_threshold > 0 => {
            let fuzzy = JaroWinklerFilter::new(term.clone(), threshold);
            Box::new(OrFilter::new(
                Some(Box::new(SubstringFilter::new(term))),
                Some(Box::new(fuzzy)),
            ))
        }
        FuzzyAlgorithm::JaroWinkler | FuzzyAlgorithm::NoOp => Box::new(SubstringFilter::new(term)),
    }
}

/// Adds the case folding that [`build_search_filter`] documents.
fn wrap_search_filter(
    filter: Box<dyn StringFilter>,
    case_sensitive: bool,
) -> Box<dyn StringFilter> {
    if case_sensitive {
        filter
    } else {
        Box::new(CaseFoldingFilter::new(filter))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::search_expr::parse;

    const JW: FuzzyAlgorithm = FuzzyAlgorithm::JaroWinkler;
    const SUBSEQ: FuzzyAlgorithm = FuzzyAlgorithm::Subsequence;

    fn search(terms: &[&str], threshold: u8, algorithm: FuzzyAlgorithm) -> Box<dyn StringFilter> {
        build_search_filter(terms, threshold, algorithm, true).expect("a filter")
    }

    fn expr(
        input: &str,
        threshold: u8,
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
        assert!(build_search_filter(none, 0, JW, true).is_none());
        assert!(build_search_filter(&["", ""], 50, SUBSEQ, false).is_none());
    }

    #[test]
    fn empty_terms_are_skipped() {
        let filter = search(&["", "cpu"], 0, JW);
        assert_eq!(filter.accept("cpu"), (true, 1.0));
        assert_eq!(filter.accept("memory"), (false, 0.0));
    }

    #[test]
    fn one_term_scores_as_its_term_filter() {
        let filter = search(&["cpu"], 0, JW);
        let term = SubstringFilter::new("cpu");
        for value in ["cpu", "node_cpu", "memory"] {
            assert_eq!(filter.accept(value), term.accept(value), "{value}");
        }
    }

    #[test]
    fn terms_combine_with_or_and_the_best_score() {
        let filter = search(&["cpu", "node"], 0, JW);
        assert_eq!(filter.accept("node_cpu"), (true, 1.0));
        assert_eq!(
            filter.accept("xx_cpu"),
            SubstringFilter::new("cpu").accept("xx_cpu")
        );
        assert_eq!(filter.accept("memory"), (false, 0.0));
    }

    #[test]
    fn zero_threshold_jaro_winkler_is_substring_only() {
        let filter = search(&["metric"], 0, JW);
        assert_eq!(filter.accept("metirc"), (false, 0.0));
    }

    #[test]
    fn jaro_winkler_falls_back_from_a_substring_match() {
        let filter = search(&["metric"], 80, JW);
        assert_eq!(
            filter.accept("my_metric"),
            SubstringFilter::new("metric").accept("my_metric")
        );
        let fuzzy = JaroWinklerFilter::new("metric", 0.8).accept("metirc");
        assert!(fuzzy.0);
        assert_eq!(filter.accept("metirc"), fuzzy);
    }

    #[test]
    fn percentage_thresholds_are_normalized_for_the_term_filters() {
        // `metirc` scores about 0.96 against `metric`: 96 % accepts it, 97 % does not.
        let score = JaroWinklerFilter::new("metric", 0.0).accept("metirc").1;
        let pct = (score * 100.0).floor() as u8;
        assert!(search(&["metric"], pct, JW).accept("metirc").0);
        assert!(!search(&["metric"], pct + 1, JW).accept("metirc").0);
        // Scores stay in [0.0, 1.0], whatever the threshold.
        assert_eq!(search(&["metric"], pct, JW).accept("metirc").1, score);

        for pct in [0, 30, 100] {
            let term = SubsequenceFilter::new("nde", f64::from(pct) / 100.0);
            let filter = search(&["nde"], pct, SUBSEQ);
            for value in ["nde_x", "node", "nxdxe", "disk"] {
                assert_eq!(filter.accept(value), term.accept(value), "{pct} {value}");
            }
        }
    }

    #[test]
    fn subsequence_uses_the_subsequence_filter() {
        let filter = search(&["nde"], 0, SUBSEQ);
        let term = SubsequenceFilter::new("nde", 0.0);
        for value in ["nde_x", "node", "disk"] {
            assert_eq!(filter.accept(value), term.accept(value), "{value}");
        }
    }

    #[test]
    fn noop_is_substring_only_at_any_threshold() {
        let filter = search(&["metric"], 80, FuzzyAlgorithm::NoOp);
        assert_eq!(filter.accept("metirc"), (false, 0.0));
        assert_eq!(filter.accept("metric"), (true, 1.0));
    }

    #[test]
    fn case_insensitive_search_folds_terms_and_values() {
        let filter = build_search_filter(&["CPU"], 0, JW, false).unwrap();
        assert_eq!(
            filter.accept("Node_Cpu"),
            SubstringFilter::new("cpu").accept("node_cpu")
        );
        assert_eq!(filter.accept("CPU"), (true, 1.0));

        let sensitive = search(&["CPU"], 0, JW);
        assert_eq!(sensitive.accept("cpu"), (false, 0.0));
    }

    #[test]
    fn expression_terms_use_the_search_term_filter() {
        let filter = expr("cpu AND node", 0, JW, true);
        assert_eq!(filter.accept("node_cpu"), {
            let cpu = SubstringFilter::new("cpu").accept("node_cpu").1;
            (true, (1.0 + cpu) / 2.0)
        });
        assert_eq!(filter.accept("cpu"), (false, 0.0));
    }

    #[test]
    fn negated_expression_terms_match_literally() {
        // A fuzzy "memry" would also exclude "cpu_memory"; the literal one does not.
        let filter = expr("cpu AND NOT memry", 80, JW, true);
        assert!(filter.accept("cpu_memory").0);
        assert!(!filter.accept("cpu_memry").0);

        let subsequence = expr("cpu AND NOT mry", 0, SUBSEQ, true);
        assert!(
            subsequence.accept("cpu_memory").0,
            "mry is a subsequence, not a substring"
        );
        assert!(!subsequence.accept("cpu_mry").0);
    }

    #[test]
    fn case_insensitive_expression_folds_every_term() {
        let filter = expr("CPU AND NOT Idle", 0, JW, false);
        assert_eq!(
            filter.accept("Node_cpu"),
            SubstringFilter::new("cpu").accept("node_cpu")
        );
        assert!(!filter.accept("cpu_IDLE").0);
    }

    #[test]
    fn invalid_expression_is_refused() {
        let expr = parse("NOT cpu").unwrap();
        assert!(build_search_expr_filter(&expr, 0, JW, true).is_err());
    }
}
