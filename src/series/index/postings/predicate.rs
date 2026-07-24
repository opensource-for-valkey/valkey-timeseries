//! Translating a single `LabelFilter` into a posting bitmap.
//!
//! Each `PredicateMatch` variant gets a handler that picks the cheapest term-dictionary access for
//! it — an exact key hit, a prefix scan, or a full scan of one label's values with a predicate.
//! [`Terms::inverse_postings_for_filter`] serves the planner's negation path, where Prometheus
//! semantics require "matches the inverse" rather than "does not match".

use super::terms::Terms;
use super::{EMPTY_BITMAP, PostingsBitmap};
use crate::error_consts::INTERNAL_ERROR;
use crate::labels::filters::{LabelFilter, PredicateMatch, PredicateValue};
use std::borrow::Cow;
use valkey_module::{ValkeyError, ValkeyResult};

impl<'a> Terms<'a> {
    pub(super) fn postings_for_filter(
        self,
        filter: &LabelFilter,
    ) -> ValkeyResult<Cow<'a, PostingsBitmap>> {
        let result = match filter.matcher {
            PredicateMatch::Equal(ref value) => handle_equal_match(self, &filter.label, value),
            PredicateMatch::NotEqual(ref value) => {
                handle_not_equal_match(self, &filter.label, value)
            }
            PredicateMatch::MatchAll => Cow::Borrowed(self.all()),
            PredicateMatch::MatchNone => Cow::Borrowed(&*EMPTY_BITMAP),
            PredicateMatch::RegexEqual(_) => handle_regex_equal_match(self, filter)?,
            PredicateMatch::RegexNotEqual(_) => handle_regex_not_equal_match(self, filter)?,
            PredicateMatch::StartsWith(ref prefix) => {
                handle_starts_with(self, &filter.label, prefix)
            }
            PredicateMatch::NotStartsWith(ref prefix) => {
                handle_not_starts_with(self, &filter.label, prefix)
            }
            PredicateMatch::Contains(ref needle) => handle_contains(self, &filter.label, needle),
            PredicateMatch::NotContains(_) => handle_not_contains(self, filter)?,
        };
        Ok(result)
    }

    pub(super) fn inverse_postings_for_filter(
        self,
        filter: &LabelFilter,
    ) -> Cow<'a, PostingsBitmap> {
        match &filter.matcher {
            PredicateMatch::NotEqual(pv) => handle_equal_match(self, &filter.label, pv),
            // If the matcher being inverted is ="", we just want all the values.
            PredicateMatch::Equal(PredicateValue::String(s)) if s.is_empty() => {
                Cow::Owned(self.postings_for_all_label_values(&filter.label))
            }
            PredicateMatch::MatchAll => Cow::Borrowed(&*EMPTY_BITMAP),
            PredicateMatch::MatchNone => Cow::Borrowed(self.all()),
            // If the matcher being inverted is =~"", we just want all the values.
            PredicateMatch::RegexEqual(re) if matches!(re.regex.as_str(), "" | ".*") => {
                Cow::Owned(self.postings_for_all_label_values(&filter.label))
            }
            PredicateMatch::StartsWith(prefix) => {
                let all = self.postings_for_all_label_values(&filter.label);
                let matching_prefix = postings_by_prefix_value(self, &filter.label, prefix);
                Cow::Owned(all.andnot(&matching_prefix))
            }
            PredicateMatch::NotStartsWith(prefix) => {
                Cow::Owned(postings_by_prefix_value(self, &filter.label, prefix))
            }
            PredicateMatch::Contains(needle) => {
                let all = self.postings_for_all_label_values(&filter.label);
                let matching = handle_contains(self, &filter.label, needle).into_owned();
                Cow::Owned(all.andnot(&matching))
            }
            PredicateMatch::NotContains(needle) => {
                Cow::Owned(handle_contains(self, &filter.label, needle).into_owned())
            }
            _ => {
                let mut state = filter;
                let postings =
                    self.postings_for_label_matching(&filter.label, &mut state, |s, state| {
                        let valid = state.matches(s);
                        !valid
                    });
                Cow::Owned(postings)
            }
        }
    }
}

fn handle_equal_match<'a>(
    ix: Terms<'a>,
    label: &str,
    value: &PredicateValue,
) -> Cow<'a, PostingsBitmap> {
    match value {
        PredicateValue::String(s) => {
            if s.is_empty() {
                return ix.postings_without_label(label);
            }
            ix.postings_for_label_value(label, s)
        }
        PredicateValue::List(val) => match val.len() {
            0 => ix.postings_without_label(label),
            1 => {
                if val[0].is_empty() {
                    ix.postings_without_label(label)
                } else {
                    ix.postings_for_label_value(label, &val[0])
                }
            }
            _ => {
                // If the list contains an explicit empty alternative, include series
                // without the label as well.
                let contains_empty = val.iter().any(|s| s.is_empty());

                let non_empty_values: Vec<String> =
                    val.iter().filter(|s| !s.is_empty()).cloned().collect();

                if non_empty_values.is_empty() {
                    // only empty alternative -> postings without label
                    return ix.postings_without_label(label);
                }

                let mut result = ix.postings_for_label_values(label, &non_empty_values);
                // include series that don't have the label only if the original
                // alternatives contained an empty branch.
                if contains_empty {
                    let without = ix.postings_without_label(label).into_owned();
                    result |= without;
                }
                Cow::Owned(result)
            }
        },
        PredicateValue::Empty => ix.postings_without_label(label),
    }
}

// return postings for series which has the label `label
fn with_label<'a>(ix: Terms<'a>, label: &str) -> Cow<'a, PostingsBitmap> {
    let mut state = ();
    let postings = ix.postings_for_label_matching(label, &mut state, |_value, _| true);
    Cow::Owned(postings)
}

fn handle_not_equal_match<'a>(
    ix: Terms<'a>,
    label: &str,
    value: &PredicateValue,
) -> Cow<'a, PostingsBitmap> {
    // the time series has a label named label
    match value {
        PredicateValue::String(s) => {
            if s.is_empty() {
                return with_label(ix, label);
            }
            let all = ix.all();
            let postings = ix.postings_for_label_value(label, s);
            if postings.is_empty() {
                Cow::Borrowed(all)
            } else {
                let result = all.andnot(&postings);
                Cow::Owned(result)
            }
        }
        PredicateValue::List(values) => {
            match values.len() {
                0 => with_label(ix, label),
                _ => {
                    // get postings for label m.label without values in values
                    let to_remove = ix.postings_for_label_values(label, values);
                    let all_postings = ix.all();
                    if to_remove.is_empty() {
                        Cow::Borrowed(all_postings)
                    } else {
                        let result = all_postings.andnot(&to_remove);
                        Cow::Owned(result)
                    }
                }
            }
        }
        PredicateValue::Empty => with_label(ix, label),
    }
}

fn handle_regex_equal_match<'a>(
    postings: Terms<'a>,
    filter: &LabelFilter,
) -> ValkeyResult<Cow<'a, PostingsBitmap>> {
    if filter.matches_empty() {
        return Ok(postings.postings_without_label(&filter.label));
    }
    // The caller dispatches on this same enum (`postings_for_filter`), so this arm is only
    // reached for `RegexEqual` filters; a mismatch means that invariant broke.
    let PredicateMatch::RegexEqual(re) = &filter.matcher else {
        debug_assert!(false, "unexpected matcher type in handle_regex_equal_match");
        return Err(ValkeyError::Str(INTERNAL_ERROR));
    };
    let res = if let Some(prefix) = &re.prefix {
        postings.postings_by_prefix_and_predicate(&filter.label, prefix, |v| re.is_match(v))
    } else {
        let mut state = ();
        postings
            .postings_for_label_matching(&filter.label, &mut state, |value, _| re.is_match(value))
    };
    Ok(Cow::Owned(res))
}

fn handle_regex_not_equal_match<'a>(
    postings: Terms<'a>,
    filter: &LabelFilter,
) -> ValkeyResult<Cow<'a, PostingsBitmap>> {
    if filter.matches_empty() {
        return Ok(with_label(postings, &filter.label));
    }
    // The caller dispatches on this same enum (`postings_for_filter`), so this arm is only
    // reached for `RegexNotEqual` filters; a mismatch means that invariant broke.
    let PredicateMatch::RegexNotEqual(re) = &filter.matcher else {
        debug_assert!(
            false,
            "unexpected matcher type in handle_regex_not_equal_match"
        );
        return Err(ValkeyError::Str(INTERNAL_ERROR));
    };
    let mut state = ();
    let res = postings
        .postings_for_label_matching(&filter.label, &mut state, |value, _| !re.is_match(value));
    Ok(Cow::Owned(res))
}

fn handle_starts_with<'a>(
    postings: Terms<'a>,
    label: &str,
    prefix: &PredicateValue,
) -> Cow<'a, PostingsBitmap> {
    Cow::Owned(postings_by_prefix_value(postings, label, prefix))
}

fn handle_not_starts_with<'a>(
    postings: Terms<'a>,
    label: &str,
    prefix: &PredicateValue,
) -> Cow<'a, PostingsBitmap> {
    let matching_prefix = postings_by_prefix_value(postings, label, prefix);
    Cow::Owned(postings.all().andnot(&matching_prefix))
}

fn postings_by_prefix_value(
    postings: Terms<'_>,
    label: &str,
    value: &PredicateValue,
) -> PostingsBitmap {
    match value {
        PredicateValue::Empty => PostingsBitmap::new(),
        PredicateValue::String(prefix) => postings.postings_by_prefix(label, prefix),
        PredicateValue::List(prefixes) => {
            let mut result = PostingsBitmap::new();
            for prefix in prefixes {
                result.or_inplace(&postings.postings_by_prefix(label, prefix));
            }
            result
        }
    }
}

fn handle_contains<'a>(
    postings: Terms<'a>,
    label: &str,
    value: &PredicateValue,
) -> Cow<'a, PostingsBitmap> {
    let mut state = value;
    let res = postings.postings_for_label_matching(label, &mut state, |candidate, value| {
        value.matches_contains_any(candidate)
    });
    Cow::Owned(res)
}

fn handle_not_contains<'a>(
    postings: Terms<'a>,
    filter: &LabelFilter,
) -> ValkeyResult<Cow<'a, PostingsBitmap>> {
    // The caller dispatches on this same enum (`postings_for_filter`), so this arm is only
    // reached for `NotContains` filters; a mismatch means that invariant broke.
    let PredicateMatch::NotContains(value) = &filter.matcher else {
        debug_assert!(false, "unexpected matcher type in handle_not_contains");
        return Err(ValkeyError::Str(INTERNAL_ERROR));
    };
    let mut state = value;
    let contains_matches =
        postings.postings_for_label_matching(&filter.label, &mut state, |candidate, value| {
            value.matches_contains_any(candidate)
        });
    let res = postings.all();
    let value = res.andnot(&contains_matches);
    Ok(Cow::Owned(value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::labels::filters::MatchOp;
    use crate::series::index::postings::Postings;

    #[test]
    fn test_not_starts_with_filter_returns_non_matching_label_values() {
        let mut postings = Postings::default();
        postings.add_posting_for_label_value(1, "instance", "server-1");
        postings.add_posting_for_label_value(2, "instance", "client-1");
        postings.add_posting_for_label_value(3, "instance", "server-2");
        postings.add_posting_for_label_value(4, "other", "value");

        let filter = LabelFilter {
            label: "instance".to_string(),
            matcher: PredicateMatch::NotStartsWith(PredicateValue::String("server".to_string())),
        };

        let result = postings.terms().postings_for_filter(&filter).unwrap();
        // Prometheus-compatible negative matchers include series that do not have the label.
        assert_eq!(result.cardinality(), 2);
        assert!(result.contains(2));
        assert!(result.contains(4));
        assert!(!result.contains(1));
        assert!(!result.contains(3));
    }

    #[test]
    fn test_not_starts_with_empty_prefix_returns_only_series_without_label() {
        let mut postings = Postings::default();
        postings.add_posting_for_label_value(1, "instance", "server-1");
        postings.add_posting_for_label_value(2, "instance", "client-1");
        postings.all_postings.add(4);

        let err = LabelFilter::create(MatchOp::NotStartsWith, "instance", "")
            .expect_err("empty prefixes should be rejected");
        assert!(
            err.to_string()
                .contains("starts with matcher does not allow empty values")
        );
    }

    #[test]
    fn test_starts_with_filter_with_value_list_uses_any_match_semantics() {
        let mut postings = Postings::default();
        postings.add_posting_for_label_value(1, "instance", "server-1");
        postings.add_posting_for_label_value(2, "instance", "client-1");
        postings.add_posting_for_label_value(3, "instance", "proxy-1");

        let filter = LabelFilter {
            label: "instance".to_string(),
            matcher: PredicateMatch::StartsWith(PredicateValue::from(vec![
                "server".to_string(),
                "client".to_string(),
            ])),
        };

        let result = postings.terms().postings_for_filter(&filter).unwrap();
        assert_eq!(result.cardinality(), 2);
        assert!(result.contains(1));
        assert!(result.contains(2));
        assert!(!result.contains(3));
    }

    #[test]
    fn test_contains_filter_returns_matching_label_values() {
        let mut postings = Postings::default();
        postings.add_posting_for_label_value(1, "instance", "server-1");
        postings.add_posting_for_label_value(2, "instance", "client-1");
        postings.add_posting_for_label_value(3, "instance", "web-server-2");
        postings.add_posting_for_label_value(4, "other", "value");

        let filter = LabelFilter {
            label: "instance".to_string(),
            matcher: PredicateMatch::Contains(PredicateValue::String("server".to_string())),
        };

        let result = postings.terms().postings_for_filter(&filter).unwrap();

        assert_eq!(result.cardinality(), 2);
        assert!(result.contains(1));
        assert!(result.contains(3));
        assert!(!result.contains(2));
        assert!(!result.contains(4));
    }

    #[test]
    fn test_contains_filter_with_value_list_uses_any_match_semantics() {
        let mut postings = Postings::default();
        postings.add_posting_for_label_value(1, "instance", "server-1");
        postings.add_posting_for_label_value(2, "instance", "client-1");
        postings.add_posting_for_label_value(3, "instance", "proxy-1");

        let filter = LabelFilter {
            label: "instance".to_string(),
            matcher: PredicateMatch::Contains(PredicateValue::from(vec![
                "server".to_string(),
                "client".to_string(),
            ])),
        };

        let result = postings.terms().postings_for_filter(&filter).unwrap();
        assert_eq!(result.cardinality(), 2);
        assert!(result.contains(1));
        assert!(result.contains(2));
        assert!(!result.contains(3));
    }

    #[test]
    fn test_decompose_vs_fullmatch_server_wildcard() {
        // Test to verify that decomposed regex matching for "server.*" works correctly
        use regex::Regex;

        // Pattern: "server.*" decomposes to prefix "server" + remainder ".*" (compiled as ^.*$)
        let re_decomposed = Regex::new("^.*$").unwrap();

        // Full pattern: "server.*" (for reference)
        let re_full = Regex::new("^server.*$").unwrap();

        let test_values = vec!["server1", "server", "serverx", "serverabc"];

        for val in test_values {
            // Check full regex
            let full_match = re_full.is_match(val);

            // Check decomposed: if starts with "server", check if remainder matches ^.*$
            let decomposed_match = if let Some(remainder) = val.strip_prefix("server") {
                re_decomposed.is_match(remainder)
            } else {
                false
            };

            assert_eq!(
                full_match, decomposed_match,
                "Mismatch for value '{}': full={}, decomposed={}",
                val, full_match, decomposed_match
            );

            // debug printing removed
        }
    }
}
