use crate::common::strings::{JaroWinklerMatcher, SubsequenceMatcher};
use std::fmt::Display;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum FuzzyAlgorithm {
    #[default]
    JaroWinkler,
    Subsequence,
    NoOp,
}

impl TryFrom<&str> for FuzzyAlgorithm {
    type Error = String;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let algo = hashify::map_ignore_case!(
            value.as_bytes(),
            FuzzyAlgorithm,
            "jarowinkler" => FuzzyAlgorithm::JaroWinkler,
            "jaro-winkler" => FuzzyAlgorithm::JaroWinkler,
            "subsequence" => FuzzyAlgorithm::Subsequence,
            "noop" => FuzzyAlgorithm::NoOp,
        )
        .copied();
        if let Some(algo) = algo {
            Ok(algo)
        } else {
            Err(format!("Unknown fuzzy algorithm: {}", value))
        }
    }
}

impl Display for FuzzyAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FuzzyAlgorithm::JaroWinkler => write!(f, "jarowinkler"),
            FuzzyAlgorithm::Subsequence => write!(f, "subsequence"),
            FuzzyAlgorithm::NoOp => write!(f, "noop"),
        }
    }
}

/// Filter determines whether a value should be included in results.
pub trait FuzzyFilter: Send + Sync {
    /// Returns (accepted, score) where score is used for relevance ranking.
    /// Score should be in range [0.0, 1.0] where 1.0 is perfect match.
    fn accept(&self, value: &str) -> (bool, f64);
}

#[derive(Default)]
pub struct NoOpSearchMatcher;

impl FuzzyFilter for NoOpSearchMatcher {
    fn accept(&self, _value: &str) -> (bool, f64) {
        (true, 1.0)
    }
}

pub enum SimilarityMatcher {
    JaroWinkler(JaroWinklerMatcher),
    Subsequence(SubsequenceMatcher),
    NoOp(NoOpSearchMatcher),
}

impl Default for SimilarityMatcher {
    fn default() -> Self {
        SimilarityMatcher::NoOp(NoOpSearchMatcher)
    }
}

impl SimilarityMatcher {
    pub fn new(pattern: &str, algorithm: FuzzyAlgorithm) -> Self {
        match algorithm {
            FuzzyAlgorithm::JaroWinkler => {
                SimilarityMatcher::JaroWinkler(JaroWinklerMatcher::new(pattern.to_string()))
            }
            FuzzyAlgorithm::Subsequence => {
                SimilarityMatcher::Subsequence(SubsequenceMatcher::new(pattern))
            }
            FuzzyAlgorithm::NoOp => SimilarityMatcher::NoOp(NoOpSearchMatcher),
        }
    }

    pub fn score(&self, value: &str) -> f64 {
        match self {
            SimilarityMatcher::JaroWinkler(m) => m.score(value),
            SimilarityMatcher::Subsequence(m) => m.score(value),
            SimilarityMatcher::NoOp(_) => 1.0,
        }
    }
}

#[derive(Default)]
pub struct SimilarityFilter {
    matcher: SimilarityMatcher,
    threshold: f64,
    case_sensitive: bool,
}

impl SimilarityFilter {
    #[cfg(test)]
    pub fn new(pattern: &str, algorithm: FuzzyAlgorithm, threshold: f64) -> Self {
        Self::new_with_case_sensitivity(pattern, algorithm, threshold, true)
    }

    pub fn new_with_case_sensitivity(
        pattern: &str,
        algorithm: FuzzyAlgorithm,
        threshold: f64,
        case_sensitive: bool,
    ) -> Self {
        let normalized_pattern = if case_sensitive {
            pattern.to_string()
        } else {
            pattern.to_lowercase()
        };

        Self {
            matcher: SimilarityMatcher::new(&normalized_pattern, algorithm),
            threshold,
            case_sensitive,
        }
    }
}

impl FuzzyFilter for SimilarityFilter {
    fn accept(&self, value: &str) -> (bool, f64) {
        let normalized_value;
        let candidate = if self.case_sensitive {
            value
        } else {
            normalized_value = value.to_lowercase();
            &normalized_value
        };

        let score = self.matcher.score(candidate);
        (score >= self.threshold, score)
    }
}

pub struct LabelNameSearchFilter {
    /// One `SimilarityFilter` per term; empty when `fuzz_threshold == 0`.
    similarity_filters: Vec<SimilarityFilter>,
    /// Terms for substring matching when `fuzz_threshold == 0`.
    substring_terms: Option<Vec<String>>,
    /// Retained for candidate normalization in the substring check.
    case_sensitive: bool,
}

impl LabelNameSearchFilter {
    pub(crate) fn new(
        terms: Vec<String>,
        algorithm: FuzzyAlgorithm,
        fuzz_threshold: f64,
        case_sensitive: bool,
    ) -> Self {
        let normalized_terms: Vec<String> = if case_sensitive {
            terms
        } else {
            terms.into_iter().map(|t| t.to_lowercase()).collect()
        };

        let similarity_filters = if fuzz_threshold == 0.0 {
            Vec::new()
        } else {
            normalized_terms
                .iter()
                .map(|term| {
                    SimilarityFilter::new_with_case_sensitivity(
                        term,
                        algorithm,
                        fuzz_threshold,
                        case_sensitive,
                    )
                })
                .collect()
        };

        Self {
            similarity_filters,
            substring_terms: if fuzz_threshold == 0.0 {
                Some(normalized_terms)
            } else {
                None
            },
            case_sensitive,
        }
    }
}

impl FuzzyFilter for LabelNameSearchFilter {
    fn accept(&self, value: &str) -> (bool, f64) {
        // If no fuzzy filters were created (FUZZY_THRESHOLD == 0.0)
        // perform a simple substring/prefix containment check using
        // the pre-normalized `terms`. This preserves the intended
        // behaviour of SEARCH when no fuzzy threshold is provided.
        if let Some(terms) = &self.substring_terms {
            let normalized_value = if self.case_sensitive {
                value.to_string()
            } else {
                value.to_lowercase()
            };
            for term in terms {
                if normalized_value.contains(term) {
                    return (true, 1.0);
                }
            }
            return (false, 0.0);
        }

        let mut accepted = false;
        let mut best_score: f64 = 0.0;

        for filter in self.similarity_filters.iter() {
            let (sim_accepted, score) = filter.accept(value);
            best_score = best_score.max(score);
            if sim_accepted {
                accepted = true;
            }
        }

        (accepted, best_score)
    }
}
