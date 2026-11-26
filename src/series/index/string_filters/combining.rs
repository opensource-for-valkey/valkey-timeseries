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

// Ported from Prometheus `web/api/v1/search_filters.go` (ChainFilter, orSearchesFilter, orFilter).

//! Filters that combine other filters.

use crate::series::index::StringFilter;

/// Accepts a value only if every filter accepts it, scoring it with the best (highest) of their
/// scores so that the ranking reflects the strongest matching dimension. With no filters it
/// accepts everything with a score of 1.0.
#[derive(Default)]
pub struct ChainFilter {
    filters: Vec<Box<dyn StringFilter>>,
}

impl ChainFilter {
    pub fn new(filters: Vec<Box<dyn StringFilter>>) -> Self {
        Self { filters }
    }
}

impl StringFilter for ChainFilter {
    fn accept(&self, value: &str) -> (bool, f64) {
        if self.filters.is_empty() {
            return (true, 1.0);
        }
        let mut max_score = 0.0_f64;
        for filter in &self.filters {
            let (accepted, score) = filter.accept(value);
            if !accepted {
                return (false, 0.0);
            }
            if score > max_score {
                max_score = score;
            }
        }
        (true, max_score)
    }
}

/// Accepts a value if any filter accepts it, scoring it with the best score among the filters
/// that accept it; a rejecting filter's score is ignored. Stops at the first perfect (1.0)
/// score. Upstream uses it to OR the per-term filters of a multi-term search.
///
/// Unlike [`ChainFilter`], with no filters it accepts nothing.
#[derive(Default)]
pub struct OrSearchesFilter {
    filters: Vec<Box<dyn StringFilter>>,
}

impl OrSearchesFilter {
    pub fn new(filters: Vec<Box<dyn StringFilter>>) -> Self {
        Self { filters }
    }
}

impl StringFilter for OrSearchesFilter {
    fn accept(&self, value: &str) -> (bool, f64) {
        let mut max_score = 0.0_f64;
        let mut accepted = false;
        for filter in &self.filters {
            let (ok, score) = filter.accept(value);
            if !ok {
                continue;
            }
            accepted = true;
            if score > max_score {
                max_score = score;
            }
            if max_score >= 1.0 {
                return (true, max_score);
            }
        }
        (accepted, max_score)
    }
}

/// Tries `primary` first and falls back to `fallback` when it rejects the value. Upstream pairs
/// a substring filter with a fuzzy one this way.
///
/// The fallback's verdict is final, score included: a value both reject scores what the
/// fallback gave it. Without either filter it accepts everything with a score of 1.0; without
/// only the fallback, a value the primary rejects scores 0.0.
#[derive(Default)]
pub struct OrFilter {
    primary: Option<Box<dyn StringFilter>>,
    fallback: Option<Box<dyn StringFilter>>,
}

impl OrFilter {
    pub fn new(
        primary: Option<Box<dyn StringFilter>>,
        fallback: Option<Box<dyn StringFilter>>,
    ) -> Self {
        Self { primary, fallback }
    }
}

impl StringFilter for OrFilter {
    fn accept(&self, value: &str) -> (bool, f64) {
        if self.primary.is_none() && self.fallback.is_none() {
            return (true, 1.0);
        }
        if let Some(primary) = &self.primary {
            let (accepted, score) = primary.accept(value);
            if accepted {
                return (true, score);
            }
        }
        match &self.fallback {
            Some(fallback) => fallback.accept(value),
            None => (false, 0.0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Accepts values containing `query`, scoring each with `score`, and counts its calls.
    struct Scoring {
        query: &'static str,
        score: f64,
        /// The score of a value it rejects.
        reject_score: f64,
        calls: Arc<AtomicUsize>,
    }

    fn scoring(query: &'static str, score: f64) -> Box<dyn StringFilter> {
        Box::new(Scoring {
            query,
            score,
            reject_score: 0.0,
            calls: Arc::default(),
        })
    }

    fn counted(query: &'static str, score: f64) -> (Box<dyn StringFilter>, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let filter = Box::new(Scoring {
            query,
            score,
            reject_score: 0.0,
            calls: calls.clone(),
        });
        (filter, calls)
    }

    impl StringFilter for Scoring {
        fn accept(&self, value: &str) -> (bool, f64) {
            self.calls.fetch_add(1, Ordering::Relaxed);
            if value.contains(self.query) {
                (true, self.score)
            } else {
                (false, self.reject_score)
            }
        }
    }

    #[test]
    fn empty_chain_accepts_everything() {
        assert_eq!(ChainFilter::default().accept("anything"), (true, 1.0));
        assert_eq!(ChainFilter::new(Vec::new()).accept(""), (true, 1.0));
    }

    #[test]
    fn chain_requires_every_filter_and_scores_the_best() {
        let chain = ChainFilter::new(vec![
            scoring("node", 0.4),
            scoring("cpu", 0.9),
            scoring("_", 0.6),
        ]);
        assert_eq!(chain.accept("node_cpu"), (true, 0.9));
        assert_eq!(chain.accept("node_memory"), (false, 0.0));
        assert_eq!(chain.accept("nodecpu"), (false, 0.0));
    }

    #[test]
    fn chain_stops_at_the_first_rejection() {
        let (last, calls) = counted("cpu", 1.0);
        let chain = ChainFilter::new(vec![scoring("node", 1.0), last]);
        assert_eq!(chain.accept("disk_cpu"), (false, 0.0));
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn chain_score_is_zero_when_every_filter_scores_zero() {
        let chain = ChainFilter::new(vec![scoring("a", 0.0), scoring("b", 0.0)]);
        assert_eq!(chain.accept("ab"), (true, 0.0));
    }

    #[test]
    fn empty_or_searches_accepts_nothing() {
        assert_eq!(OrSearchesFilter::default().accept("anything"), (false, 0.0));
    }

    #[test]
    fn or_searches_scores_the_best_accepting_filter() {
        let or = OrSearchesFilter::new(vec![
            scoring("node", 0.4),
            scoring("cpu", 0.9),
            scoring("disk", 0.6),
        ]);
        assert_eq!(or.accept("node_cpu"), (true, 0.9));
        assert_eq!(or.accept("node_memory"), (true, 0.4));
        assert_eq!(or.accept("network"), (false, 0.0));
    }

    #[test]
    fn or_searches_ignores_the_scores_of_rejecting_filters() {
        let rejecting = Box::new(Scoring {
            query: "cpu",
            score: 1.0,
            reject_score: 0.9,
            calls: Arc::default(),
        });
        let or = OrSearchesFilter::new(vec![rejecting, scoring("mem", 0.2)]);
        assert_eq!(or.accept("memory"), (true, 0.2));
        assert_eq!(or.accept("disk"), (false, 0.0));
    }

    #[test]
    fn or_searches_stops_at_a_perfect_score() {
        let (after, calls) = counted("cpu", 0.5);
        let or = OrSearchesFilter::new(vec![scoring("node", 0.3), scoring("cpu", 1.0), after]);
        assert_eq!(or.accept("node_cpu"), (true, 1.0));
        assert_eq!(calls.load(Ordering::Relaxed), 0);

        // Below a perfect score every filter is tried.
        assert_eq!(or.accept("node"), (true, 0.3));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn or_searches_accepts_with_a_zero_score() {
        let or = OrSearchesFilter::new(vec![scoring("a", 0.0)]);
        assert_eq!(or.accept("a"), (true, 0.0));
    }

    #[test]
    fn empty_or_accepts_everything() {
        assert_eq!(OrFilter::default().accept("anything"), (true, 1.0));
    }

    #[test]
    fn or_prefers_the_primary() {
        let (fallback, calls) = counted("cpu", 0.3);
        let or = OrFilter::new(Some(scoring("cpu", 0.8)), Some(fallback));
        assert_eq!(or.accept("node_cpu"), (true, 0.8));
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn or_falls_back_when_the_primary_rejects() {
        let or = OrFilter::new(Some(scoring("cpu", 0.8)), Some(scoring("mem", 0.3)));
        assert_eq!(or.accept("node_memory"), (true, 0.3));
    }

    #[test]
    fn or_returns_the_fallback_score_of_a_rejected_value() {
        let fallback = Box::new(Scoring {
            query: "mem",
            score: 1.0,
            reject_score: 0.42,
            calls: Arc::default(),
        });
        let or = OrFilter::new(Some(scoring("cpu", 1.0)), Some(fallback));
        assert_eq!(or.accept("disk"), (false, 0.42));
    }

    #[test]
    fn or_with_one_filter_uses_it_alone() {
        let primary_only = OrFilter::new(Some(scoring("cpu", 0.7)), None);
        assert_eq!(primary_only.accept("cpu"), (true, 0.7));
        assert_eq!(primary_only.accept("disk"), (false, 0.0));

        let fallback_only = OrFilter::new(None, Some(scoring("cpu", 0.7)));
        assert_eq!(fallback_only.accept("cpu"), (true, 0.7));
        assert_eq!(fallback_only.accept("disk"), (false, 0.0));
    }
}
