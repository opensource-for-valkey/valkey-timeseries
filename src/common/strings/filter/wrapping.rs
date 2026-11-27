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

// Ported from Prometheus `web/api/v1/search_filters.go` (caseFoldingFilter).

//! Filters that wrap one inner filter and change how values reach it.
use super::StringFilter;
use std::borrow::Cow;

/// Case folds `value`, borrowing it when it holds no uppercase character.
pub(super) fn case_fold(value: &str) -> Cow<'_, str> {
    if value.chars().any(char::is_uppercase) {
        Cow::Owned(value.to_lowercase())
    } else {
        Cow::Borrowed(value)
    }
}

/// Lowercases the value once before delegating, so a chain of case-insensitive filters does not
/// each repeat the case fold. The inner filter must have been built from a lowercased query.
pub struct CaseFoldingFilter<F = Box<dyn StringFilter>> {
    inner: F,
}

impl<F: StringFilter> CaseFoldingFilter<F> {
    pub fn new(inner: F) -> Self {
        Self { inner }
    }
}

impl<F: StringFilter> StringFilter for CaseFoldingFilter<F> {
    fn accept(&self, value: &str) -> (bool, f64) {
        self.inner.accept(&case_fold(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Accepts values containing `query`, scoring 1.0.
    struct Counting {
        query: &'static str,
    }

    impl Counting {
        fn new(query: &'static str) -> Self {
            Self { query }
        }
    }

    impl StringFilter for Counting {
        fn accept(&self, value: &str) -> (bool, f64) {
            if value.contains(self.query) {
                (true, 1.0)
            } else {
                (false, 0.0)
            }
        }
    }

    /// Records the last value it saw.
    #[derive(Default)]
    struct Recording(Mutex<String>);

    impl StringFilter for Recording {
        fn accept(&self, value: &str) -> (bool, f64) {
            *self.0.lock().unwrap() = value.to_owned();
            (true, 1.0)
        }
    }

    #[test]
    fn case_folding_lowercases_before_delegating() {
        let filter = CaseFoldingFilter::new(Counting::new("cpu"));
        assert_eq!(filter.accept("Node_CPU_seconds"), (true, 1.0));
        assert_eq!(filter.accept("cpu"), (true, 1.0));
        assert_eq!(filter.accept("MEMORY"), (false, 0.0));
    }

    #[test]
    fn case_folding_folds_non_ascii() {
        let filter = CaseFoldingFilter::new(Recording::default());
        filter.accept("ÄRGER");
        assert_eq!(*filter.inner.0.lock().unwrap(), "ärger");
    }

    #[test]
    fn case_folding_does_not_lowercase_the_query() {
        // The inner filter's query must already be lowercase.
        let filter = CaseFoldingFilter::new(Counting::new("CPU"));
        assert_eq!(filter.accept("CPU"), (false, 0.0));
    }
}
