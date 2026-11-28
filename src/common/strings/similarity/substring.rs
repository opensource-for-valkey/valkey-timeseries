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

// Scoring ported from Prometheus `web/api/v1/search_filters.go` (SubstringFilter).

/// Lowest score a match can get: a match at the last possible position.
pub const SUBSTRING_MIN_MATCH_SCORE: f64 = 0.1;

/// Scores how early a fixed pattern occurs in candidate strings. A prefix match scores 1.0;
/// any other match scores in `[0.1, 1.0)`, higher the earlier it starts; no match scores 0.0.
/// An empty pattern matches everything with 1.0.
pub struct SubstringMatcher {
    pattern: String,
    pattern_ascii: bool,
    /// Character count, so a non-ASCII value need not recount the pattern.
    pattern_chars: usize,
}

impl SubstringMatcher {
    pub fn new(pattern: impl Into<String>) -> Self {
        let pattern = pattern.into();
        let pattern_ascii = pattern.is_ascii();
        let pattern_chars = if pattern_ascii {
            pattern.len()
        } else {
            pattern.chars().count()
        };
        Self {
            pattern,
            pattern_ascii,
            pattern_chars,
        }
    }

    /// The match position is counted in characters, not bytes, so a match at character 1 of a
    /// multi-byte value scores the same as one at byte 1 of an ASCII value. ASCII values skip
    /// the character count.
    pub fn score(&self, value: &str) -> f64 {
        if self.pattern.is_empty() {
            return 1.0;
        }
        let Some(idx) = value.find(self.pattern.as_str()) else {
            return 0.0;
        };
        if idx == 0 {
            return 1.0;
        }
        // The whole value must be ASCII: a multi-byte character after the match would make
        // the byte length overcount characters and inflate the score.
        let (pos, max_pos) = if self.pattern_ascii && value.is_ascii() {
            (idx, value.len() - self.pattern.len())
        } else {
            let pos = value[..idx].chars().count();
            let max_pos = value.chars().count() - self.pattern_chars;
            (pos, max_pos)
        };
        if max_pos == 0 {
            // Unreachable: equal character counts force a match at 0, handled above.
            return 1.0;
        }
        1.0 - (1.0 - SUBSTRING_MIN_MATCH_SCORE) * pos as f64 / max_pos as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EPSILON: f64 = 1e-9;

    fn assert_score(pattern: &str, value: &str, want: f64) {
        let got = SubstringMatcher::new(pattern).score(value);
        assert!(
            (got - want).abs() < EPSILON,
            "pattern={pattern:?} value={value:?}: expected {want}, got {got}"
        );
    }

    #[test]
    fn prefix_and_exact_matches_score_one() {
        assert_score("cpu", "cpu", 1.0);
        assert_score("cpu", "cpu_seconds", 1.0);
    }

    #[test]
    fn later_matches_score_lower() {
        // pos 2, max_pos = 9 - 3 = 6.
        assert_score("cpu", "a_cpu_xyz", 1.0 - 0.9 * 2.0 / 6.0);
        // A suffix match is the last possible position: the minimum score.
        assert_score("ab", "xxab", SUBSTRING_MIN_MATCH_SCORE);
    }

    #[test]
    fn no_match_scores_zero() {
        assert_score("gpu", "node_cpu", 0.0);
        assert_score("longer", "short", 0.0);
        assert_score("a", "", 0.0);
    }

    #[test]
    fn empty_pattern_matches_everything() {
        assert_score("", "anything", 1.0);
        assert_score("", "", 1.0);
    }

    #[test]
    fn counts_characters_not_bytes() {
        // "éb": 'b' at char 1 of 2 (byte 2 of 3) — same as "ab".
        assert_score("b", "éb", SUBSTRING_MIN_MATCH_SCORE);
        assert_score("b", "ab", SUBSTRING_MIN_MATCH_SCORE);
        // An ASCII match followed by a multi-byte char: pos 1, max_pos 2.
        assert_score("b", "abé", 1.0 - 0.9 * 1.0 / 2.0);
        // A non-ASCII pattern.
        assert_score("ü", "aüb", 1.0 - 0.9 * 1.0 / 2.0);
    }
}
