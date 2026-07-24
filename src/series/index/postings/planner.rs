//! Boolean set algebra: assembling a list of filters or selectors into a single bitmap.
//!
//! Follows Prometheus's `PostingsForMatchers` strategy. Matchers are split into intersecting and
//! subtracting sets, ordered so the intersecting ones run first (keeping the base of subtraction
//! small and stable), and cheap matchers run before expensive ones. Degenerate regexes (`.*`,
//! `.+`) are short-circuited before touching the index at all.

use super::terms::Terms;
use super::{EMPTY_BITMAP, Postings, PostingsBitmap};
use crate::error_consts::MISSING_FILTER;
use crate::labels::filters::{FilterList, LabelFilter, MatchOp, PredicateMatch, SeriesSelector};
use smallvec::SmallVec;
use std::borrow::Cow;
use std::cmp::Ordering;
use valkey_module::{ValkeyError, ValkeyResult};

impl<'a> Terms<'a> {
    /// `postings_for_label_filters` assembles a single postings iterator against the index
    /// based on the given matchers.
    pub(super) fn postings_for_label_filters(
        self,
        filters: &[LabelFilter],
    ) -> ValkeyResult<Cow<'a, PostingsBitmap>> {
        if filters.is_empty() {
            return Ok(Cow::Borrowed(self.all()));
        }
        if filters.len() == 1 {
            let filter = &filters[0];
            // follow Prometheus here: if we have an empty matcher and label, return all postings.
            if filter.label.is_empty() && filter.matcher.is_empty() {
                return Ok(Cow::Borrowed(self.all()));
            }
            // shortcut the handling of simple equality matchers
            if !filter.is_negative_matcher() && !filter.matches_empty() {
                let it = self.postings_for_filter(filter)?;
                if it.is_empty() {
                    return Ok(Cow::Borrowed(&*EMPTY_BITMAP));
                }
                return Ok(it);
            }
        }

        let mut its: SmallVec<[_; 4]> = SmallVec::new();
        let mut not_its: SmallVec<[Cow<PostingsBitmap>; 4]> = SmallVec::new();

        let mut sorted_matchers: SmallVec<[(&LabelFilter, bool, bool); 4]> = SmallVec::new();

        let mut has_subtracting_matchers = false;
        let mut has_intersecting_matchers = false;
        for m in filters {
            let matches_empty = m.matches("");

            let is_subtracting = matches_empty || m.is_negative_matcher();

            if is_subtracting {
                has_subtracting_matchers = true;
            } else {
                has_intersecting_matchers = true;
            }

            sorted_matchers.push((m, matches_empty, is_subtracting))
        }

        if has_subtracting_matchers && !has_intersecting_matchers {
            // If there's nothing to subtract from, add in everything and remove the not_its later.
            // We prefer to get all_postings so that the base of subtraction (i.e., all_postings)
            // doesn't include series that may be added to the index reader during this function call.
            its.push(Cow::Borrowed(self.all()));
        };

        // Sort matchers to have the intersecting matchers first.
        // This way the base for subtraction is smaller, and there is no chance that the set we subtract
        // from contains postings of series that didn't exist when we constructed the set we subtract by.
        sorted_matchers.sort_by(|i, j| -> Ordering {
            let is_i_subtracting = i.2;
            let is_j_subtracting = j.2;
            if !is_i_subtracting && is_j_subtracting {
                return Ordering::Less;
            }
            // sort by match cost
            let cost_i = i.0.cost();
            let cost_j = j.0.cost();
            cost_i.cmp(&cost_j)
        });

        for (filter, matches_empty, _is_subtracting) in sorted_matchers {
            //let value = &m.value;
            let name = &filter.label;

            if name.is_empty() && matches_empty {
                // We already handled the case at the top of the function,
                // and it is unexpected to get all postings again here.
                return Err(ValkeyError::Str(MISSING_FILTER));
            }

            if matches!(filter.matcher, PredicateMatch::MatchAll) {
                continue;
            }

            if matches!(filter.matcher, PredicateMatch::MatchNone) {
                return Ok(Cow::Borrowed(&*EMPTY_BITMAP));
            }

            let typ = filter.op();
            let regex_value = filter.regex_text().unwrap_or("");

            match (typ, regex_value) {
                // .* regexp matches any string: do nothing
                (MatchOp::RegexEqual, ".*") => continue,

                // .* regexp does not match any string: return empty
                (MatchOp::RegexNotEqual, ".*") => {
                    return Ok(Cow::Borrowed(&*EMPTY_BITMAP));
                }

                // .+ regexp matches any non-empty string
                (MatchOp::RegexEqual, ".+") => {
                    // .+ regexp matches any non-empty string: get postings for all label values.
                    let it = self.postings_for_all_label_values(&filter.label);
                    its.push(Cow::Owned(it));
                }

                // .+ regexp does not match any non-empty string
                (MatchOp::RegexNotEqual, ".+") => {
                    let it = self.postings_for_all_label_values(&filter.label);
                    not_its.push(Cow::Owned(it));
                }
                // See which label must be non-empty.
                // Optimization for a case like {l=~".", l!="1"}.
                _ if !matches_empty => {
                    // If this matcher must be non-empty, we can be smarter.
                    let is_not = matches!(
                        typ,
                        MatchOp::NotEqual
                            | MatchOp::RegexNotEqual
                            | MatchOp::NotContains
                            | MatchOp::NotStartsWith
                    );
                    match (is_not, matches_empty) {
                        // l!="foo"
                        (true, true) => {
                            // If the label can't be empty and is a Not and the inner matcher
                            // doesn't match empty, then subtract it out at the end.
                            let inverse = filter.clone().inverse();
                            let it = self.postings_for_filter(&inverse)?;
                            not_its.push(it);
                        }
                        // l!=""
                        (true, false) => {
                            // If the label can't be empty and is a Not, but the inner matcher can
                            // be empty, we need to use inverse_postings_for_filter.
                            let inverse = filter.clone().inverse();
                            let it = self.inverse_postings_for_filter(&inverse);
                            if it.is_empty() {
                                return Ok(Cow::Borrowed(&*EMPTY_BITMAP));
                            }
                            its.push(it);
                        }
                        // l="a", l=~"a|b", etc.
                        _ => {
                            // Non-Not matcher, use normal postings_for_filter.
                            let it = self.postings_for_filter(filter)?;
                            if it.is_empty() {
                                return Ok(Cow::Borrowed(&*EMPTY_BITMAP));
                            }
                            its.push(it);
                        }
                    }
                }
                _ => {
                    // l=""
                    // If the matchers for a label name selects an empty value, it selects all
                    // the series which also don't have the label name. See:
                    // https://github.com/prometheus/prometheus/issues/3575 and
                    // https://github.com/prometheus/prometheus/pull/3578#issuecomment-351653555
                    let it = self.inverse_postings_for_filter(filter);

                    not_its.push(it)
                }
            }
        }

        // optimization: if we have a single iterator and no not_its, return it directly, saving a clone.
        if its.len() == 1 && not_its.is_empty() {
            let single = its
                .pop()
                .expect("unexpected out of bounds error running matchers");

            return Ok(single);
        }

        let mut result = if its.is_empty() {
            self.all().clone()
        } else {
            // sort by cardinality first to reduce the amount of work
            its.sort_by_key(|a| a.cardinality());
            intersection(its)
        };

        for not in not_its {
            result.andnot_inplace(&not)
        }

        Ok(Cow::Owned(result))
    }

    pub(super) fn postings_for_selector(
        self,
        selector: &SeriesSelector,
    ) -> ValkeyResult<Cow<'a, PostingsBitmap>> {
        match &selector {
            SeriesSelector::And(filters) => self.postings_for_label_filters(filters),
            SeriesSelector::Or(filters) => self.process_or_matchers(filters),
        }
    }

    pub(super) fn postings_for_selectors(
        self,
        selectors: &[SeriesSelector],
    ) -> ValkeyResult<Cow<'a, PostingsBitmap>> {
        match selectors {
            [] => Ok(Cow::Borrowed(&*EMPTY_BITMAP)),
            [selector] => {
                let result = self.postings_for_selector(selector)?;
                Ok(self.mask_cow(result))
            }
            _ => {
                let first = self.postings_for_selector(&selectors[0])?;

                let mut result = first.into_owned();
                for selector in &selectors[1..] {
                    let bitmap = self.postings_for_selector(selector)?;
                    result.and_inplace(&bitmap);
                }

                self.mask(&mut result);

                Ok(Cow::Owned(result))
            }
        }
    }

    fn process_or_matchers(self, filters: &[FilterList]) -> ValkeyResult<Cow<'a, PostingsBitmap>> {
        match filters {
            [] => Ok(Cow::Borrowed(self.all())),
            [filters] => self.postings_for_label_filters(filters),
            _ => {
                let mut result = PostingsBitmap::new();
                // maybe chili here to run in parallel
                for matchers in filters {
                    let postings = self.postings_for_label_filters(matchers)?;
                    result.or_inplace(&postings);
                }
                Ok(Cow::Owned(result))
            }
        }
    }
}

impl Postings {
    pub fn postings_for_selector<'a>(
        &'a self,
        selector: &SeriesSelector,
    ) -> ValkeyResult<Cow<'a, PostingsBitmap>> {
        self.terms().postings_for_selector(selector)
    }

    pub fn postings_for_selectors<'a>(
        &'a self,
        selectors: &[SeriesSelector],
    ) -> ValkeyResult<Cow<'a, PostingsBitmap>> {
        self.terms().postings_for_selectors(selectors)
    }
}

fn intersection<'a, I>(its: I) -> PostingsBitmap
where
    I: IntoIterator<Item = Cow<'a, PostingsBitmap>>,
{
    let mut its = its.into_iter();
    if let Some(it) = its.next() {
        let mut result = it.into_owned();

        for it in its {
            if it.is_empty() {
                result.clear();
                return result;
            }

            result.and_inplace(&it);
        }

        result
    } else {
        PostingsBitmap::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::labels::filters::PredicateValue;

    #[test]
    fn test_equal_list_includes_empty_alternative() {
        // Ensure that an equality list containing an explicit empty alternative
        // includes series that do not have the label (postings_without_label).
        let mut postings = Postings::default();

        // Series 1 has label "i" with value "x"
        postings.add_posting_for_label_value(1, "i", "x");

        // Series 2 exists in all_postings but does not have label "i"
        postings.all_postings.add(2);

        // Create a LabelFilter for i in ("x", "")
        let lf = LabelFilter {
            label: "i".to_string(),
            matcher: PredicateMatch::Equal(PredicateValue::from(vec![
                "x".to_string(),
                "".to_string(),
            ])),
        };

        let res = postings.terms().postings_for_label_filters(&[lf]).unwrap();
        let res = res.into_owned();

        // Both series 1 (has value x) and 2 (no label) should be present.
        assert!(res.contains(1));
        assert!(res.contains(2));
    }

    #[test]
    fn test_postings_equal_list_matcher() {
        // Test case for decomposed regex node[12] -> Equal(List(["node1", "node2"]))
        let mut postings = Postings::default();

        // Add test series with various node labels
        postings.add_posting_for_label_value(1, "node", "node1");
        postings.add_posting_for_label_value(2, "node", "node2");
        postings.add_posting_for_label_value(3, "node", "node3");
        postings.add_posting_for_label_value(4, "node", "node1");

        // Create a label filter for node = ("node1", "node2")
        // This is what parse_regex_matcher("node[12]", true) should now produce
        let lf = LabelFilter {
            label: "node".to_string(),
            matcher: PredicateMatch::Equal(PredicateValue::from(vec![
                "node1".to_string(),
                "node2".to_string(),
            ])),
        };

        // Query using this filter
        let result = postings.terms().postings_for_label_filters(&[lf]).unwrap();
        let result = result.into_owned();

        // Should match series 1, 2, and 4 (all with node1 or node2)
        assert_eq!(result.cardinality(), 3);
        assert!(result.contains(1));
        assert!(result.contains(2));
        assert!(result.contains(4));
        assert!(!result.contains(3)); // node3 should not be matched
    }

    #[test]
    fn test_match_none_filter_returns_empty_postings() {
        let mut postings = Postings::default();
        postings.add_posting_for_label_value(1, "instance", "server-1");
        postings.add_posting_for_label_value(2, "instance", "client-1");

        let filter = LabelFilter {
            label: "instance".to_string(),
            matcher: PredicateMatch::MatchNone,
        };

        let result = postings
            .terms()
            .postings_for_label_filters(&[filter])
            .unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_not_starts_with_filter_with_value_list_requires_all_absent() {
        let mut postings = Postings::default();
        postings.add_posting_for_label_value(1, "instance", "server-1");
        postings.add_posting_for_label_value(2, "instance", "client-1");
        postings.add_posting_for_label_value(3, "instance", "proxy-1");
        postings.all_postings.add(4);

        let filter = LabelFilter {
            label: "instance".to_string(),
            matcher: PredicateMatch::NotStartsWith(PredicateValue::from(vec![
                "server".to_string(),
                "client".to_string(),
            ])),
        };

        let result = postings
            .terms()
            .postings_for_label_filters(&[filter])
            .unwrap();
        let result = result.into_owned();
        assert_eq!(result.cardinality(), 2);
        assert!(result.contains(3));
        assert!(result.contains(4));
        assert!(!result.contains(1));
        assert!(!result.contains(2));
    }

    #[test]
    fn test_not_contains_filter_returns_non_matching_and_missing_labels() {
        let mut postings = Postings::default();
        postings.add_posting_for_label_value(1, "instance", "server-1");
        postings.add_posting_for_label_value(2, "instance", "client-1");
        postings.add_posting_for_label_value(3, "instance", "web-server-2");
        postings.add_posting_for_label_value(4, "other", "value");
        postings.all_postings.add(4);

        let filter = LabelFilter {
            label: "instance".to_string(),
            matcher: PredicateMatch::NotContains(PredicateValue::String("server".to_string())),
        };

        let result = postings
            .terms()
            .postings_for_label_filters(&[filter])
            .unwrap();
        let result = result.into_owned();

        assert_eq!(result.cardinality(), 2);
        assert!(result.contains(2));
        assert!(result.contains(4));
        assert!(!result.contains(1));
        assert!(!result.contains(3));
    }

    #[test]
    fn test_not_contains_filter_with_value_list_requires_all_absent() {
        let mut postings = Postings::default();
        postings.add_posting_for_label_value(1, "instance", "server-1");
        postings.add_posting_for_label_value(2, "instance", "client-1");
        postings.add_posting_for_label_value(3, "instance", "proxy-1");
        postings.all_postings.add(4);

        let filter = LabelFilter {
            label: "instance".to_string(),
            matcher: PredicateMatch::NotContains(PredicateValue::from(vec![
                "server".to_string(),
                "client".to_string(),
            ])),
        };

        let result = postings
            .terms()
            .postings_for_label_filters(&[filter])
            .unwrap();
        let result = result.into_owned();
        assert_eq!(result.cardinality(), 2);
        assert!(result.contains(3));
        assert!(result.contains(4));
        assert!(!result.contains(1));
        assert!(!result.contains(2));
    }
}
