//! Boolean set algebra: assembling a list of filters or selectors into a single bitmap.
//!
//! Follows Prometheus's `PostingsForMatchers` strategy. Matchers are split into intersecting and
//! subtracting sets, ordered so the intersecting ones run first (keeping the base of subtraction
//! small and stable), and cheap matchers run before expensive ones. Degenerate matchers
//! (`.*`, and the presence checks `l!=""` / `l=""` however they are spelled) are answered
//! without running a predicate over label values.

use super::terms::{Terms, union_cow};
use super::{EMPTY_BITMAP, Postings, PostingsBitmap};
use crate::labels::filters::{FilterList, LabelFilter, PredicateMatch, SeriesSelector};
use smallvec::SmallVec;
use std::borrow::Cow;
use valkey_module::ValkeyResult;

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

        // A filter that accepts "" also selects the series without its label, so it can only
        // remove series (`not_its`, or nothing for `.*`). One that rejects "" needs the label
        // present and intersects (`its`), even when negative (`l!=""`, `l!~"a*"`). Intersecting
        // filters run first, cheapest first, so an empty one returns before any subtracting work.
        let mut sorted_matchers: SmallVec<[(&LabelFilter, bool); 4]> =
            filters.iter().map(|f| (f, f.matches_empty())).collect();
        sorted_matchers.sort_by_key(|&(filter, matches_empty)| (matches_empty, filter.cost()));

        for (filter, matches_empty) in sorted_matchers {
            // `.*` and its spellings were turned into MatchAll / MatchNone when the pattern was
            // parsed; `l!=""`, `l=~".+"` and theirs into HasLabel, and `l=""`, `l!~".+"` and
            // theirs into LacksLabel.
            match &filter.matcher {
                PredicateMatch::MatchAll => {
                    continue;
                }
                PredicateMatch::MatchNone => {
                    return Ok(Cow::Borrowed(&*EMPTY_BITMAP));
                }
                // Every series carrying the label.
                PredicateMatch::HasLabel => {
                    let it = self.postings_for_all_label_values(&filter.label);
                    if it.is_empty() {
                        return Ok(Cow::Borrowed(&*EMPTY_BITMAP));
                    }
                    its.push(it);
                }
                // Every series without the label: subtract the ones carrying it.
                PredicateMatch::LacksLabel => {
                    not_its.push(self.postings_for_all_label_values(&filter.label));
                }
                // See which label must be non-empty.
                // Optimization for a case like {l=~".", l!="1"}.
                _ if !matches_empty => {
                    let it = if filter.is_negative_matcher() {
                        // l!~"a*": the inner matcher matches empty, so the label must be present
                        // with a value the inner matcher rejects.
                        let inverse = filter.clone().inverse();
                        self.inverse_postings_for_filter(&inverse)
                    } else {
                        // l="a", l=~"a|b", etc.
                        self.postings_for_filter(filter)?
                    };
                    if it.is_empty() {
                        return Ok(Cow::Borrowed(&*EMPTY_BITMAP));
                    }
                    its.push(it);
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

        let base = match its.len() {
            // No intersecting filter: subtract from every series.
            0 => Cow::Borrowed(self.all()),
            // a single intersecting set stays borrowed if nothing is subtracted from it
            1 => its
                .pop()
                .expect("index planner: intersecting set must have at least one element"),
            _ => {
                // sort by cardinality first to reduce the amount of work
                its.sort_by_cached_key(|a| a.cardinality());
                Cow::Owned(intersection(its))
            }
        };

        let mut not_its = not_its.into_iter().filter(|not| !not.is_empty());
        let Some(first_not) = not_its.next() else {
            return Ok(base);
        };
        if base.is_empty() {
            return Ok(base);
        }
        // `andnot` on a borrowed base allocates only the difference, not a full clone.
        let mut result = match base {
            Cow::Borrowed(base) => base.andnot(&first_not),
            Cow::Owned(mut base) => {
                base.andnot_inplace(&first_not);
                base
            }
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
            // A union of no groups is empty. The parser never builds one; a fan-out payload can,
            // and it must not widen to every series.
            [] => Ok(Cow::Borrowed(&*EMPTY_BITMAP)),
            [filters] => self.postings_for_label_filters(filters),
            _ => filters
                .iter()
                .try_fold(Cow::Borrowed(&*EMPTY_BITMAP), |acc, matchers| {
                    Ok(union_cow(acc, self.postings_for_label_filters(matchers)?))
                }),
        }
    }
}

impl Postings {
    /// The postings matching every filter in `filters` (an AND-list), stale ids removed.
    /// An empty list matches every series.
    pub fn postings_for_label_filters<'a>(
        &'a self,
        filters: &[LabelFilter],
    ) -> ValkeyResult<Cow<'a, PostingsBitmap>> {
        let terms = self.terms();
        let result = terms.postings_for_label_filters(filters)?;
        Ok(terms.mask_cow(result))
    }

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
    let Some(first) = its.next() else {
        return PostingsBitmap::new();
    };
    let Some(second) = its.next() else {
        return first.into_owned();
    };
    // A borrowed `first` gets a fresh bitmap sized to the intersection rather than a full clone;
    // an owned one is narrowed in place.
    let mut result = match first {
        Cow::Borrowed(first) => first.and(&second),
        Cow::Owned(mut first) => {
            first.and_inplace(&second);
            first
        }
    };
    for it in its {
        if result.is_empty() {
            break;
        }
        result.and_inplace(&it);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::labels::filters::{MatchOp, PredicateValue};

    fn filter(label: &str, matcher: PredicateMatch) -> LabelFilter {
        LabelFilter {
            label: label.to_string(),
            matcher,
        }
    }

    #[test]
    fn subtracting_an_absent_value_keeps_the_base_borrowed() {
        let mut postings = Postings::default();
        postings.add_posting_for_label_value(1, "job", "api");
        postings.add_posting_for_label_value(2, "job", "api");
        postings.add_posting_for_label_value(2, "env", "prod");

        let filters = [
            filter("job", PredicateMatch::Equal(PredicateValue::from("api"))),
            filter("env", PredicateMatch::NotEqual(PredicateValue::from("dev"))),
        ];
        let result = postings
            .terms()
            .postings_for_label_filters(&filters)
            .unwrap();
        assert!(matches!(result, Cow::Borrowed(_)));
        assert_eq!(result.iter().collect::<Vec<_>>(), vec![1, 2]);

        let filters = [
            filter("job", PredicateMatch::Equal(PredicateValue::from("api"))),
            filter(
                "env",
                PredicateMatch::NotEqual(PredicateValue::from("prod")),
            ),
        ];
        let result = postings
            .terms()
            .postings_for_label_filters(&filters)
            .unwrap();
        assert_eq!(result.iter().collect::<Vec<_>>(), vec![1]);
    }

    #[test]
    fn or_selector_with_one_non_empty_branch_stays_borrowed() {
        let mut postings = Postings::default();
        postings.add_posting_for_label_value(1, "job", "api");
        postings.add_posting_for_label_value(2, "job", "web");

        let branch = |value: &str| {
            FilterList::from(vec![filter(
                "job",
                PredicateMatch::Equal(PredicateValue::from(value)),
            )])
        };
        let selector = SeriesSelector::Or(vec![branch("api"), branch("missing")]);
        let result = postings.postings_for_selector(&selector).unwrap();
        assert!(matches!(result, Cow::Borrowed(_)));
        assert_eq!(result.iter().collect::<Vec<_>>(), vec![1]);

        let selector = SeriesSelector::Or(vec![branch("api"), branch("web")]);
        let result = postings.postings_for_selector(&selector).unwrap();
        assert_eq!(result.iter().collect::<Vec<_>>(), vec![1, 2]);
    }

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

    fn not_equal_empty_fixture() -> Postings {
        let mut postings = Postings::default();
        postings.add_posting_for_label_value(1, "i", "x");
        postings.add_posting_for_label_value(1, "n", "1");
        postings.add_posting_for_label_value(2, "i", "y");
        postings.add_posting_for_label_value(3, "n", "1");
        postings
    }

    #[test]
    fn test_not_equal_empty_selects_series_with_label() {
        let postings = not_equal_empty_fixture();

        // `PredicateValue::Empty` is what the RTS `i!=` form and fanout decoding produce.
        for value in [PredicateValue::Empty, PredicateValue::String(String::new())] {
            let filter = LabelFilter {
                label: "i".to_string(),
                matcher: PredicateMatch::NotEqual(value.clone()),
            };
            let result = postings
                .terms()
                .postings_for_label_filters(&[filter])
                .unwrap();
            assert_eq!(result.iter().collect::<Vec<_>>(), vec![1, 2], "{value:?}");
        }
    }

    #[test]
    fn test_not_equal_empty_intersects_with_other_matchers() {
        let postings = not_equal_empty_fixture();
        let filters = [
            LabelFilter::equals("n".to_string(), "1"),
            LabelFilter {
                label: "i".to_string(),
                matcher: PredicateMatch::NotEqual(PredicateValue::Empty),
            },
        ];
        let result = postings
            .terms()
            .postings_for_label_filters(&filters)
            .unwrap();
        assert_eq!(result.iter().collect::<Vec<_>>(), vec![1]);
    }

    #[test]
    fn test_not_equal_empty_on_absent_label_is_empty() {
        let postings = not_equal_empty_fixture();
        let filters = [
            LabelFilter::equals("n".to_string(), "1"),
            LabelFilter::not_equals("missing".to_string(), ""),
        ];
        let result = postings
            .terms()
            .postings_for_label_filters(&filters)
            .unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_negative_regex_matching_empty_requires_label() {
        let mut postings = Postings::default();
        postings.add_posting_for_label_value(1, "i", "x");
        postings.add_posting_for_label_value(2, "i", "xx");
        postings.add_posting_for_label_value(3, "i", "y");
        postings.add_posting_for_label_value(4, "n", "1");

        // `x*` matches "", so `i!~"x*"` excludes series without `i`.
        let filter = LabelFilter::create(MatchOp::RegexNotEqual, "i", "x*").unwrap();
        let result = postings
            .terms()
            .postings_for_label_filters(&[filter])
            .unwrap();
        assert_eq!(result.iter().collect::<Vec<_>>(), vec![3]);
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

    #[test]
    fn test_postings_regex_with_empty_alternative() {
        // `foo|` decomposes to the literal list ["foo", ""]: it matches `l=foo`
        // and the series without `l`; its negation matches any other value.
        let mut postings = Postings::default();
        postings.add_posting_for_label_value(1, "l", "foo");
        postings.add_posting_for_label_value(1, "n", "1");
        postings.add_posting_for_label_value(2, "l", "bar");
        postings.add_posting_for_label_value(2, "n", "1");
        postings.add_posting_for_label_value(3, "n", "1");
        let n = LabelFilter::create(MatchOp::Equal, "n", "1").unwrap();

        for (op, expected) in [
            (MatchOp::RegexEqual, vec![1, 3]),
            (MatchOp::RegexNotEqual, vec![2]),
        ] {
            let filter = LabelFilter::create(op, "l", "foo|").unwrap();
            for filters in [vec![filter.clone()], vec![n.clone(), filter.clone()]] {
                let got: Vec<u64> = postings
                    .terms()
                    .postings_for_label_filters(&filters)
                    .unwrap()
                    .iter()
                    .collect();
                assert_eq!(got, expected, "{op:?} with {} filter(s)", filters.len());
            }
        }
    }

    /// `l!=""` has several spellings (`PredicateValue::Empty` from the RTS `l!=` form,
    /// `String("")`, a list holding only `""`). All of them must select the series that carry
    /// the label, through the same planner arm.
    #[test]
    fn not_equal_absent_selects_series_with_the_label_in_every_spelling() {
        let mut postings = Postings::default();
        postings.add_posting_for_label_value(1, "env", "prod");
        postings.add_posting_for_label_value(2, "env", "dev");
        postings.add_posting_for_label_value(3, "job", "api");
        for operand in [
            PredicateValue::Empty,
            PredicateValue::from(""),
            PredicateValue::from(vec![String::new()]),
            PredicateValue::from(vec![String::new(), String::new()]),
        ] {
            let filters = [
                filter("env", PredicateMatch::NotEqual(operand.clone())),
                filter("env", PredicateMatch::NotEqual(PredicateValue::from("dev"))),
            ];
            let result = postings.postings_for_label_filters(&filters).unwrap();
            assert_eq!(result.iter().collect::<Vec<_>>(), vec![1], "{operand:?}");
        }
    }

    /// A union of no groups is empty. It used to return every series, so a fan-out payload
    /// carrying `Or([])` made a shard answer with its whole keyspace.
    #[test]
    fn an_or_of_no_groups_matches_nothing() {
        let mut postings = Postings::default();
        postings.add_posting_for_label_value(1, "job", "api");
        postings.add_posting_for_label_value(2, "job", "web");
        let result = postings
            .postings_for_selector(&SeriesSelector::Or(vec![]))
            .unwrap();
        assert!(result.is_empty());
        // An AND of no filters, by contrast, matches every series.
        let all = postings
            .postings_for_selector(&SeriesSelector::And(FilterList::default()))
            .unwrap();
        assert_eq!(all.iter().collect::<Vec<_>>(), vec![1, 2]);
    }

    /// Every spelling of `.+` takes the label-values path, not only the text `.+`.
    #[test]
    fn any_non_empty_regex_selects_by_label_presence_in_any_spelling() {
        let mut postings = Postings::default();
        postings.add_posting_for_label_value(1, "env", "prod");
        postings.add_posting_for_label_value(2, "env", "dev");
        postings.add_posting_for_label_value(3, "job", "api");
        let ids = |op, pattern: &str| {
            let filters = [
                LabelFilter::create(op, "env", pattern).unwrap(),
                LabelFilter::create(MatchOp::RegexEqual, "job", ".*").unwrap(),
            ];
            let result = postings.postings_for_label_filters(&filters).unwrap();
            result.iter().collect::<Vec<_>>()
        };
        for pattern in [".+", "^.+$", "(.+)", ".+.*"] {
            assert_eq!(ids(MatchOp::RegexEqual, pattern), vec![1, 2], "{pattern}");
            assert_eq!(ids(MatchOp::RegexNotEqual, pattern), vec![3], "{pattern}");
        }
    }

    /// `HasLabel` and `LacksLabel` partition the series by the label, alone and beside other
    /// matchers, in either order, and agree with the same filter written as `!=""` / `=""`
    /// built without normalizing.
    #[test]
    fn presence_checks_partition_series_by_label() {
        let mut postings = Postings::default();
        postings.add_posting_for_label_value(1, "env", "prod");
        postings.add_posting_for_label_value(1, "job", "api");
        postings.add_posting_for_label_value(2, "env", "dev");
        postings.add_posting_for_label_value(3, "job", "api");
        postings.add_posting_for_label_value(4, "job", "web");
        let ids = |filters: &[LabelFilter]| {
            let result = postings.postings_for_label_filters(filters).unwrap();
            result.iter().collect::<Vec<_>>()
        };
        let has = filter("env", PredicateMatch::HasLabel);
        let lacks = filter("env", PredicateMatch::LacksLabel);
        let has_raw = filter("env", PredicateMatch::NotEqual(PredicateValue::from("")));
        let lacks_raw = filter("env", PredicateMatch::Equal(PredicateValue::from("")));
        let job_api = filter("job", PredicateMatch::Equal(PredicateValue::from("api")));
        let missing = |matcher| filter("absent", matcher);
        let none: Vec<u64> = vec![];

        assert_eq!(ids(std::slice::from_ref(&has)), vec![1, 2]);
        assert_eq!(ids(std::slice::from_ref(&lacks)), vec![3, 4]);
        assert_eq!(ids(&[has.clone(), job_api.clone()]), vec![1]);
        assert_eq!(ids(&[job_api.clone(), has.clone()]), vec![1]);
        assert_eq!(ids(&[lacks.clone(), job_api.clone()]), vec![3]);
        assert_eq!(ids(&[has.clone(), lacks.clone()]), none);
        assert_eq!(ids(&[missing(PredicateMatch::HasLabel)]), none);
        assert_eq!(
            ids(&[missing(PredicateMatch::LacksLabel)]),
            vec![1, 2, 3, 4]
        );
        for (normalized, raw) in [(&has, &has_raw), (&lacks, &lacks_raw)] {
            assert_eq!(
                ids(std::slice::from_ref(normalized)),
                ids(std::slice::from_ref(raw))
            );
            assert_eq!(
                ids(&[normalized.clone(), job_api.clone()]),
                ids(&[raw.clone(), job_api.clone()])
            );
        }
    }

    /// The parser and the fan-out decoder reject an empty label name, so the planner needs no
    /// guard for one: built directly, it reads as a label no series has.
    #[test]
    fn an_empty_label_name_reads_as_a_label_no_series_has() {
        let mut postings = Postings::default();
        postings.add_posting_for_label_value(1, "job", "a");
        postings.add_posting_for_label_value(2, "job", "b");
        let job_a = filter("job", PredicateMatch::Equal(PredicateValue::from("a")));
        let cases = [
            // Accepts "": removes nothing.
            (PredicateMatch::Equal(PredicateValue::from("")), vec![1]),
            (PredicateMatch::NotEqual(PredicateValue::from("x")), vec![1]),
            (PredicateMatch::MatchAll, vec![1]),
            (PredicateMatch::LacksLabel, vec![1]),
            // Rejects "": matches nothing.
            (PredicateMatch::HasLabel, vec![]),
            (PredicateMatch::Equal(PredicateValue::from("x")), vec![]),
            (PredicateMatch::NotEqual(PredicateValue::from("")), vec![]),
        ];
        for (matcher, want) in cases {
            let filters = [job_a.clone(), filter("", matcher.clone())];
            let got = postings.postings_for_label_filters(&filters).unwrap();
            assert_eq!(got.iter().collect::<Vec<_>>(), want, "{matcher:?}");
        }
    }
}
