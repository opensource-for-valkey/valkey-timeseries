use std::fmt::Display;

/// Filter determines whether a value should be included in results.
pub trait StringFilter: Send + Sync {
    /// Returns (accepted, score) where score is used for relevance ranking.
    /// Score should be in range [0.0, 1.0] where 1.0 is perfect match.
    fn accept(&self, value: &str) -> (bool, f64);
}

impl<T: StringFilter + ?Sized> StringFilter for Box<T> {
    fn accept(&self, value: &str) -> (bool, f64) {
        (**self).accept(value)
    }
}

#[derive(Default)]
pub struct NoOpSearchMatcher;

impl StringFilter for NoOpSearchMatcher {
    fn accept(&self, _value: &str) -> (bool, f64) {
        (true, 1.0)
    }
}

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
