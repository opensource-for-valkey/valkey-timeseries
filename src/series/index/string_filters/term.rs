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

// Ported from Prometheus `web/api/v1/search_filters.go` (SubstringFilter, containsFilter,
// FuzzyFilter, SubsequenceFilter).

//! Filters that match one search term. Each matches the term and the value in their literal
//! case; see the [module docs](super) for case-insensitive search.

use crate::common::strings::{JaroWinklerMatcher, SubsequenceMatcher};
use crate::series::index::StringFilter;

/// Accepts values containing the query. A prefix match scores 1.0; any other match scores in
/// `[0.1, 1.0)`, higher the earlier it starts. An empty query accepts everything with 1.0.
pub struct SubstringFilter {
    query: String,
}

impl SubstringFilter {
    pub fn new(query: impl Into<String>) -> Self {
        Self {
            query: query.into(),
        }
    }
}

impl StringFilter for SubstringFilter {
    /// The match position is counted in characters, not bytes, so a match at character 1 of a
    /// multi-byte value scores the same as one at byte 1 of an ASCII value. ASCII values skip
    /// the character count.
    fn accept(&self, value: &str) -> (bool, f64) {
        if self.query.is_empty() {
            return (true, 1.0);
        }
        let Some(idx) = value.find(self.query.as_str()) else {
            return (false, 0.0);
        };
        if idx == 0 {
            return (true, 1.0);
        }
        // The whole value must be ASCII: a multi-byte character after the match would make
        // the byte length overcount characters and inflate the score.
        let (pos, max_pos) = if self.query.is_ascii() && value.is_ascii() {
            (idx, value.len() - self.query.len())
        } else {
            let pos = value[..idx].chars().count();
            let max_pos = value.chars().count() - self.query.chars().count();
            (pos, max_pos)
        };
        if max_pos == 0 {
            // Unreachable: equal character counts force a match at 0, handled above.
            return (true, 1.0);
        }
        (true, 1.0 - 0.9 * pos as f64 / max_pos as f64)
    }
}

/// Accepts values containing the query, each with a score of 1.0. Use it only where the score
/// is ignored.
pub struct ContainsFilter {
    query: String,
}

impl ContainsFilter {
    pub fn new(query: impl Into<String>) -> Self {
        Self {
            query: query.into(),
        }
    }
}

impl StringFilter for ContainsFilter {
    fn accept(&self, value: &str) -> (bool, f64) {
        if value.contains(self.query.as_str()) {
            (true, 1.0)
        } else {
            (false, 0.0)
        }
    }
}

/// Accepts values whose Jaro-Winkler similarity to the query is at least `threshold`, scoring
/// them with it. A threshold of 1.0 requires an exact match.
pub struct JaroWinklerFilter {
    matcher: JaroWinklerMatcher,
    threshold: f64,
}

impl JaroWinklerFilter {
    pub fn new(query: impl Into<String>, threshold: f64) -> Self {
        Self {
            matcher: JaroWinklerMatcher::new(query.into()),
            threshold,
        }
    }
}

impl StringFilter for JaroWinklerFilter {
    fn accept(&self, value: &str) -> (bool, f64) {
        let score = self.matcher.score(value);
        (score >= self.threshold, score)
    }
}

/// Accepts values that contain every query character in order, scoring consecutive runs above
/// gaps ([`SubsequenceMatcher`]), when the score is at least `threshold`. A prefix match scores
/// 1.0, as with [`SubstringFilter`]. A value that is not a subsequence scores 0 and is rejected
/// whatever the threshold, so a threshold of 0.0 accepts any subsequence.
pub struct SubsequenceFilter {
    query: String,
    matcher: SubsequenceMatcher,
    threshold: f64,
}

impl SubsequenceFilter {
    pub fn new(query: impl Into<String>, threshold: f64) -> Self {
        let query = query.into();
        Self {
            matcher: SubsequenceMatcher::new(&query),
            query,
            threshold,
        }
    }
}

impl StringFilter for SubsequenceFilter {
    fn accept(&self, value: &str) -> (bool, f64) {
        if value.starts_with(self.query.as_str()) {
            return (true, 1.0);
        }
        let score = self.matcher.score(value);
        (score > 0.0 && score >= self.threshold, score)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_close(actual: (bool, f64), expected: (bool, f64)) {
        assert_eq!(actual.0, expected.0, "accepted of {actual:?}");
        assert!(
            (actual.1 - expected.1).abs() < 1e-12,
            "score {} != {}",
            actual.1,
            expected.1
        );
    }

    #[test]
    fn substring_scores_by_position() {
        let filter = SubstringFilter::new("cpu");
        assert_eq!(filter.accept("cpu_seconds"), (true, 1.0));
        assert_eq!(filter.accept("cpu"), (true, 1.0));
        // max_pos = 9 - 3 = 6.
        assert_close(filter.accept("node_cpu_"), (true, 1.0 - 0.9 * 5.0 / 6.0));
        assert_close(filter.accept("xxxxxxcpu"), (true, 0.1));
        assert_eq!(filter.accept("memory"), (false, 0.0));
        assert_eq!(filter.accept("CPU"), (false, 0.0));
    }

    #[test]
    fn substring_scores_earlier_matches_higher() {
        let filter = SubstringFilter::new("ab");
        let early = filter.accept("xabxxxxx").1;
        let late = filter.accept("xxxxxabx").1;
        assert!(early > late && late >= 0.1, "{early} vs {late}");
    }

    #[test]
    fn substring_counts_characters_not_bytes() {
        let filter = SubstringFilter::new("b");
        // Character offset 1 of 2 in both; the multi-byte value must not score lower.
        assert_eq!(filter.accept("ab_"), filter.accept("äb_"));
        // A multi-byte character after the match must not inflate the score.
        assert_eq!(filter.accept("ab_"), filter.accept("abä"));
        let query = SubstringFilter::new("ü");
        assert_close(query.accept("aü_"), (true, 1.0 - 0.9 * 1.0 / 2.0));
    }

    #[test]
    fn empty_substring_accepts_everything() {
        let filter = SubstringFilter::new("");
        assert_eq!(filter.accept(""), (true, 1.0));
        assert_eq!(filter.accept("anything"), (true, 1.0));
    }

    #[test]
    fn contains_scores_every_match_one() {
        let filter = ContainsFilter::new("cpu");
        assert_eq!(filter.accept("node_cpu_seconds"), (true, 1.0));
        assert_eq!(filter.accept("memory"), (false, 0.0));
    }

    #[test]
    fn jaro_winkler_applies_the_threshold() {
        let filter = JaroWinklerFilter::new("metric", 0.9);
        assert_eq!(filter.accept("metric"), (true, 1.0));
        let (accepted, score) = filter.accept("metirc");
        assert_eq!(accepted, score >= 0.9);
        let (accepted, score) = filter.accept("zzz");
        assert!(!accepted && score < 0.9);

        let exact = JaroWinklerFilter::new("metric", 1.0);
        assert!(!exact.accept("metirc").0);
    }

    #[test]
    fn subsequence_scores_a_prefix_one() {
        let filter = SubsequenceFilter::new("nod", 0.99);
        assert_eq!(filter.accept("node_cpu"), (true, 1.0));
    }

    #[test]
    fn subsequence_rejects_a_non_subsequence_at_zero_threshold() {
        let filter = SubsequenceFilter::new("ndx", 0.0);
        assert_eq!(filter.accept("node"), (false, 0.0));
        let (accepted, score) = filter.accept("node_x");
        assert!(accepted && score > 0.0 && score < 1.0, "{score}");
    }

    #[test]
    fn subsequence_applies_the_threshold() {
        let filter = SubsequenceFilter::new("nde", 0.0);
        let (accepted, score) = filter.accept("node");
        assert!(accepted);
        let strict = SubsequenceFilter::new("nde", score + 0.01);
        assert_eq!(strict.accept("node"), (false, score));
    }
}
