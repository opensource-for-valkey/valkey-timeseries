//! What the series index knows about the labels of the series a selector
//! matches — the input to the data-derived filter push-down
//! ([`super::derived_filters`]).
//!
//! A profile answers two questions about a selector without reading a sample:
//! which label filters *every* one of its series satisfies (so they can be
//! pushed into the other operand of a binary operation), and whether a given
//! filter would exclude any of its series at all (so a filter that prunes
//! nothing is never added). Both are per label: how many series carry it, and
//! the distinct values they carry, capped at [`MAX_PUSHDOWN_VALUES`] and
//! [`MAX_PUSHDOWN_BYTES`] beyond which the label is only known to be
//! high-cardinality.
//!
//! Label values are unbounded at write time, so the byte caps are what bound
//! a derived filter's cost: its values are copied into the profile (and
//! shipped by every shard), and the regex its PromQL matcher carries costs
//! ~250 ns and ~260 bytes of transient heap per byte of pattern to compile,
//! ~27 bytes of it retained for the query's lifetime.

use crate::labels::compile_literal_set;
use ahash::AHashMap;
use promql_parser::label::{METRIC_NAME, MatchOp, Matcher};
use regex::{Regex, escape};
use std::collections::BTreeSet;

/// The most distinct values a derived filter enumerates. The index resolves
/// the filter by one lookup per value, and past this the values are unlikely
/// to be selective anyway.
pub const MAX_PUSHDOWN_VALUES: usize = 60;

/// The most bytes of distinct values a label's profile — and so one derived
/// filter — holds: 60 values of up to ~68 bytes. At this size the filter's
/// regex compiles in ~1 ms with ~1 MiB of transient heap, ~110 KiB retained.
pub const MAX_PUSHDOWN_BYTES: usize = 4 * 1024;

/// The most alternation text the derived filters of one selector compile,
/// together — ~4 ms, whatever number of labels its series share. Equality
/// filters compile nothing and are not counted.
pub const MAX_DERIVED_BYTES_PER_SELECTOR: usize = 16 * 1024;

/// The most series a profile is built from. Past this the selector is left
/// alone (its profile is unavailable) rather than walked at length under the
/// module lock.
pub const MAX_PROFILED_SERIES: usize = 50_000;

/// How many series a profile may be built from for a query with these
/// options: the query's own series limit when it is tighter than
/// [`MAX_PROFILED_SERIES`]. A selector past the limit is left as written.
pub fn profiled_series_cap(options: &crate::promql::engine::QueryOptions) -> usize {
    if options.max_series > 0 {
        options.max_series.min(MAX_PROFILED_SERIES)
    } else {
        MAX_PROFILED_SERIES
    }
}

/// One label over a selector's series.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LabelValueProfile {
    pub name: String,
    /// How many of the selector's series carry the label.
    pub carried_by: u64,
    /// The distinct values, sorted; complete unless `overflow`.
    pub values: Vec<String>,
    /// More than [`MAX_PUSHDOWN_VALUES`] distinct values, or more than
    /// [`MAX_PUSHDOWN_BYTES`] of them, were seen. `values` is then empty: a
    /// partial set is never used.
    pub overflow: bool,
}

/// The labels of the series a selector matches.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LabelProfile {
    /// How many series the selector matched.
    pub series: u64,
    /// One entry per label name any of them carries, `__name__` excluded,
    /// sorted by name.
    pub labels: Vec<LabelValueProfile>,
}

impl LabelProfile {
    /// The filters every series of the profile satisfies: one per label
    /// carried by all of them with a known, bounded value set — `name="v"`
    /// for a single value, `name=~"v1|v2|…"` otherwise.
    pub fn common_filters(&self) -> Vec<Matcher> {
        self.common_filters_where(|_| true)
    }

    /// [`Self::common_filters`], limited to the labels `wanted` accepts —
    /// which is asked before a filter's regex is compiled, the one costly
    /// step. Within [`MAX_DERIVED_BYTES_PER_SELECTOR`], by label name.
    pub fn common_filters_where(
        &self,
        wanted: impl Fn(&LabelValueProfile) -> bool,
    ) -> Vec<Matcher> {
        if self.series == 0 {
            return Vec::new();
        }
        let mut budget = MAX_DERIVED_BYTES_PER_SELECTOR;
        self.labels
            .iter()
            .filter(|label| label.carried_by == self.series && !label.overflow)
            .filter(|label| wanted(label))
            .filter_map(|label| {
                derived_filter(
                    &label.name,
                    label.values.iter().map(String::as_str),
                    &mut budget,
                )
            })
            .collect()
    }

    /// Whether every series of the profile matches `matcher` — in which case
    /// adding it to the selector would prune nothing. A label the profile
    /// cannot vouch for (overflowed values) answers `false`: the filter is
    /// kept, as it would be without a profile.
    pub fn satisfied_by_all(&self, matcher: &Matcher) -> bool {
        if self.series == 0 {
            return true;
        }
        let missing_matches = || matcher.is_match("");
        match self.label(&matcher.name) {
            // No series carries the label: for all of them its value is "".
            None => missing_matches(),
            Some(label) => {
                if label.overflow {
                    return false;
                }
                let all_carriers_match = label.values.iter().all(|value| matcher.is_match(value));
                let every_series_carries = label.carried_by == self.series;
                all_carriers_match && (every_series_carries || missing_matches())
            }
        }
    }

    /// Whether the filter `filter.name ∈ filter.values` — another profile's
    /// label, as [`Self::common_filters`] would derive it — could exclude
    /// any series of this one: `!satisfied_by_all` for that filter, answered
    /// from the value sets alone so the question costs no regex. Errs towards
    /// `true` where unsure (an overflowed label, an empty value), which only
    /// means the filter is built and `satisfied_by_all` decides.
    pub fn could_be_pruned_by(&self, filter: &LabelValueProfile) -> bool {
        if self.series == 0 {
            return false;
        }
        match self.label(&filter.name) {
            None => true,
            Some(label) => {
                label.overflow
                    || label.carried_by < self.series
                    || label
                        .values
                        .iter()
                        .any(|value| filter.values.binary_search(value).is_err())
            }
        }
    }

    fn label(&self, name: &str) -> Option<&LabelValueProfile> {
        self.labels
            .binary_search_by(|label| label.name.as_str().cmp(name))
            .ok()
            .map(|index| &self.labels[index])
    }
}

/// Accumulates a [`LabelProfile`] one series — or one shard's profile — at a
/// time.
#[derive(Default)]
pub struct LabelProfileBuilder {
    series: u64,
    labels: AHashMap<String, LabelAccumulator>,
}

#[derive(Default)]
struct LabelAccumulator {
    carried_by: u64,
    values: BTreeSet<String>,
    /// The summed length of `values`.
    bytes: usize,
    overflow: bool,
}

impl LabelAccumulator {
    fn add_value(&mut self, value: &str) {
        if self.overflow || self.values.contains(value) {
            return;
        }
        if self.values.len() >= MAX_PUSHDOWN_VALUES || self.bytes + value.len() > MAX_PUSHDOWN_BYTES
        {
            self.set_overflow();
            return;
        }
        self.bytes += value.len();
        self.values.insert(value.to_string());
    }

    fn set_overflow(&mut self) {
        self.overflow = true;
        self.values = BTreeSet::new();
        self.bytes = 0;
    }
}

impl LabelProfileBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// How many series have been added so far.
    pub fn series(&self) -> u64 {
        self.series
    }

    /// Count one series with the given labels. `__name__` is ignored.
    pub fn add_series<'a>(&mut self, labels: impl IntoIterator<Item = (&'a str, &'a str)>) {
        self.series += 1;
        for (name, value) in labels {
            if name == METRIC_NAME {
                continue;
            }
            let label = match self.labels.get_mut(name) {
                Some(label) => label,
                None => self.labels.entry(name.to_string()).or_default(),
            };
            label.carried_by += 1;
            label.add_value(value);
        }
    }

    /// Fold in another profile — a shard's, over series disjoint from those
    /// already counted, so the counts add.
    pub fn merge(&mut self, other: LabelProfile) {
        self.series += other.series;
        for entry in other.labels {
            let label = self.labels.entry(entry.name).or_default();
            label.carried_by += entry.carried_by;
            if entry.overflow {
                label.set_overflow();
            }
            for value in &entry.values {
                label.add_value(value);
            }
        }
    }

    pub fn finish(self) -> LabelProfile {
        let mut labels: Vec<LabelValueProfile> = self
            .labels
            .into_iter()
            .map(|(name, acc)| LabelValueProfile {
                name,
                carried_by: acc.carried_by,
                values: acc.values.into_iter().collect(),
                overflow: acc.overflow,
            })
            .collect();
        labels.sort_by(|a, b| a.name.cmp(&b.name));
        LabelProfile {
            series: self.series,
            labels,
        }
    }
}

/// The filter `name ∈ values`: `name="v"` for one value, `name=~"v1|v2|…"`
/// otherwise, which the index resolves by lookup (see
/// [`crate::labels::literal_alternatives`]). `None` — no filter, the selector
/// left as written — for more than [`MAX_PUSHDOWN_VALUES`] or
/// [`MAX_PUSHDOWN_BYTES`] of values, or an alternation longer than what is
/// left of `budget`, which it is charged against.
pub fn derived_filter<'a>(
    name: &str,
    values: impl IntoIterator<Item = &'a str>,
    budget: &mut usize,
) -> Option<Matcher> {
    let values: Vec<&str> = values.into_iter().collect();
    let bytes: usize = values.iter().map(|v| v.len()).sum();
    if values.len() > MAX_PUSHDOWN_VALUES || bytes > MAX_PUSHDOWN_BYTES {
        return None;
    }
    match values.as_slice() {
        [] => None,
        [value] => Some(Matcher::new(MatchOp::Equal, name, value)),
        _ => {
            let alternation = join_regexp_values(values);
            if alternation.len() > *budget {
                return None;
            }
            let regex = compile_literal_set(&alternation).ok()?;
            *budget -= alternation.len();
            Some(Matcher::new(MatchOp::Re(regex), name, &alternation))
        }
    }
}

/// A `name=~"alternation"` matcher. PromQL regexes are fully anchored, and
/// the compiled form must agree with the text the index will parse from
/// `value`, or `is_match` would say `a|b` matches `ab`.
pub fn regex_matcher(name: &str, alternation: String) -> Matcher {
    // Escaped literals joined by `|`: a valid regex by construction.
    let regex = Regex::new(&format!("^(?:{alternation})$")).unwrap();
    Matcher::new(MatchOp::Re(regex), name, &alternation)
}

/// The values as a regex alternation, sorted so the same set always yields
/// the same matcher — the selector it lands in is a cache and fanout key.
pub fn join_regexp_values<'a>(values: impl IntoIterator<Item = &'a str>) -> String {
    let mut values: Vec<&str> = values.into_iter().collect();
    values.sort_unstable();
    let mut res = String::with_capacity(values.iter().map(|v| v.len() + 3).sum());
    for (i, &s) in values.iter().enumerate() {
        if i > 0 {
            res.push('|');
        }
        res.push_str(&escape(s));
    }
    res
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile(series: &[&[(&str, &str)]]) -> LabelProfile {
        let mut builder = LabelProfileBuilder::new();
        for labels in series {
            builder.add_series(labels.iter().copied());
        }
        builder.finish()
    }

    fn rendered(filters: &[Matcher]) -> Vec<String> {
        filters.iter().map(|m| m.to_string()).collect()
    }

    #[test]
    fn common_filters_come_from_labels_every_series_carries() {
        let p = profile(&[
            &[("__name__", "cpu"), ("region", "us"), ("host", "a")],
            &[("__name__", "cpu"), ("region", "us"), ("host", "b")],
            &[
                ("__name__", "cpu"),
                ("region", "eu"),
                ("host", "c"),
                ("rack", "1"),
            ],
        ]);
        assert_eq!(p.series, 3);
        assert_eq!(
            rendered(&p.common_filters()),
            vec![r#"host=~"a|b|c""#, r#"region=~"eu|us""#]
        );
    }

    #[test]
    fn a_single_value_is_an_equality_and_name_is_never_derived() {
        let p = profile(&[
            &[("__name__", "a"), ("job", "api")],
            &[("__name__", "b"), ("job", "api")],
        ]);
        assert_eq!(rendered(&p.common_filters()), vec![r#"job="api""#]);
    }

    #[test]
    fn overflowing_values_yield_no_filter() {
        let series: Vec<Vec<(&str, &str)>> = (0..=MAX_PUSHDOWN_VALUES)
            .map(|i| vec![("host", Box::leak(format!("h{i}").into_boxed_str()) as &str)])
            .collect();
        let refs: Vec<&[(&str, &str)]> = series.iter().map(|s| s.as_slice()).collect();
        let p = profile(&refs);
        assert!(p.labels[0].overflow);
        assert!(p.labels[0].values.is_empty());
        assert!(p.common_filters().is_empty());
    }

    #[test]
    fn empty_profile_derives_nothing_and_is_satisfied_by_everything() {
        let p = profile(&[]);
        assert!(p.common_filters().is_empty());
        assert!(p.satisfied_by_all(&Matcher::new(MatchOp::Equal, "x", "1")));
    }

    #[test]
    fn satisfied_by_all_tells_a_pruning_filter_from_a_redundant_one() {
        let p = profile(&[
            &[("region", "us"), ("host", "a")],
            &[("region", "us"), ("host", "b"), ("rack", "1")],
        ]);
        let eq = |n: &str, v: &str| Matcher::new(MatchOp::Equal, n, v);
        let re = |n: &str, v: &str| regex_matcher(n, v.to_string());

        // Every series has region="us": redundant.
        assert!(p.satisfied_by_all(&eq("region", "us")));
        assert!(p.satisfied_by_all(&re("region", "us|eu")));
        // Excludes region=us entirely: prunes.
        assert!(!p.satisfied_by_all(&eq("region", "eu")));
        // host has two values: an equality on one prunes the other.
        assert!(!p.satisfied_by_all(&eq("host", "a")));
        assert!(p.satisfied_by_all(&re("host", "a|b")));
        // Anchored: `a|b` does not vouch for a value that merely contains one.
        let ab = profile(&[&[("host", "ab")]]);
        assert!(!ab.satisfied_by_all(&re("host", "a|b")));
        // rack is carried by one series only; the other has rack="" which
        // `rack="1"` excludes but `rack!="2"` accepts.
        assert!(!p.satisfied_by_all(&eq("rack", "1")));
        assert!(p.satisfied_by_all(&Matcher::new(MatchOp::NotEqual, "rack", "2")));
        // A label no series carries: only a filter matching "" is redundant.
        assert!(!p.satisfied_by_all(&eq("zone", "z")));
        assert!(p.satisfied_by_all(&eq("zone", "")));
    }

    #[test]
    fn an_overflowed_label_is_never_vouched_for() {
        let series: Vec<Vec<(&str, &str)>> = (0..=MAX_PUSHDOWN_VALUES)
            .map(|i| vec![("host", Box::leak(format!("h{i}").into_boxed_str()) as &str)])
            .collect();
        let refs: Vec<&[(&str, &str)]> = series.iter().map(|s| s.as_slice()).collect();
        let p = profile(&refs);
        assert!(!p.satisfied_by_all(&regex_matcher("host", "h.*".to_string())));
    }

    #[test]
    fn merge_adds_counts_and_unions_values() {
        let shard_a = profile(&[&[("region", "us"), ("host", "a")]]);
        let shard_b = profile(&[&[("region", "eu"), ("host", "b")], &[("host", "c")]]);
        let mut builder = LabelProfileBuilder::new();
        builder.merge(shard_a);
        builder.merge(shard_b);
        let merged = builder.finish();
        assert_eq!(merged.series, 3);
        assert_eq!(
            merged.labels,
            vec![
                LabelValueProfile {
                    name: "host".into(),
                    carried_by: 3,
                    values: vec!["a".into(), "b".into(), "c".into()],
                    overflow: false,
                },
                LabelValueProfile {
                    name: "region".into(),
                    carried_by: 2,
                    values: vec!["eu".into(), "us".into()],
                    overflow: false,
                },
            ]
        );
        // region is not carried by every series once the shards are combined.
        assert_eq!(rendered(&merged.common_filters()), vec![r#"host=~"a|b|c""#]);
    }

    #[test]
    fn merge_overflows_when_the_union_does() {
        let mut builder = LabelProfileBuilder::new();
        for shard in 0..2 {
            let series: Vec<Vec<(&str, &str)>> = (0..MAX_PUSHDOWN_VALUES)
                .map(|i| {
                    vec![(
                        "host",
                        Box::leak(format!("s{shard}-h{i}").into_boxed_str()) as &str,
                    )]
                })
                .collect();
            let refs: Vec<&[(&str, &str)]> = series.iter().map(|s| s.as_slice()).collect();
            let p = profile(&refs);
            assert!(!p.labels[0].overflow);
            builder.merge(p);
        }
        let merged = builder.finish();
        assert!(merged.labels[0].overflow);
    }

    #[test]
    fn regexp_values_are_sorted_and_escaped() {
        assert_eq!(join_regexp_values(["b", "a.c", "a"]), r"a|a\.c|b");
        assert_eq!(join_regexp_values(["x"]), "x");
    }

    fn single_label(name: &str, values: Vec<String>) -> LabelValueProfile {
        LabelValueProfile {
            name: name.into(),
            carried_by: 60,
            values,
            overflow: false,
        }
    }

    /// Sixty hex ids used to exceed the general regex size limit, and the
    /// filter was dropped; within the byte budget it is now derived.
    #[test]
    fn sixty_values_within_the_byte_budget_are_one_filter() {
        let mut values: Vec<String> = (0_u64..60)
            .map(|i| format!("{:016x}", i.wrapping_mul(0x9e3779b97f4a7c15)))
            .collect();
        values.sort();
        let profile = LabelProfile {
            series: 60,
            labels: vec![single_label("instance", values.clone())],
        };
        let filters = profile.common_filters();
        assert_eq!(filters.len(), 1);
        assert!(values.iter().all(|v| filters[0].is_match(v)));
        assert!(!filters[0].is_match("0000000000000000x"));
    }

    #[test]
    fn values_past_the_byte_budget_overflow_the_label() {
        let big = "x".repeat(MAX_PUSHDOWN_BYTES / 2 + 1);
        let a = format!("a{big}");
        let b = format!("b{big}");
        let p = profile(&[&[("host", &a)], &[("host", &b)]]);
        assert!(p.labels[0].overflow);
        assert!(p.labels[0].values.is_empty());
        assert!(p.common_filters().is_empty());

        // One value past the budget on its own is never copied either.
        let huge = "y".repeat(MAX_PUSHDOWN_BYTES + 1);
        let p = profile(&[&[("host", &huge)]]);
        assert!(p.labels[0].overflow && p.labels[0].values.is_empty());
    }

    #[test]
    fn an_overflowed_shard_clears_the_merged_values() {
        let mut builder = LabelProfileBuilder::new();
        builder.merge(profile(&[&[("host", "a")]]));
        builder.merge(LabelProfile {
            series: 1,
            labels: vec![LabelValueProfile {
                name: "host".into(),
                carried_by: 1,
                values: vec![],
                overflow: true,
            }],
        });
        builder.add_series([("host", "b")]);
        let merged = builder.finish();
        assert!(merged.labels[0].overflow);
        assert!(merged.labels[0].values.is_empty());
    }

    #[test]
    fn a_selector_compiles_at_most_its_derived_byte_budget() {
        // Each label's alternation is ~3.6 KiB: only four fit in 16 KiB.
        let labels: Vec<LabelValueProfile> = (0..10)
            .map(|l| {
                let mut values: Vec<String> = (0..60)
                    .map(|i| format!("{l}-{i:02}-{}", "v".repeat(52)))
                    .collect();
                values.sort();
                single_label(&format!("l{l:02}"), values)
            })
            .collect();
        let profile = LabelProfile { series: 60, labels };
        let filters = profile.common_filters();
        let compiled: usize = filters.iter().map(|m| m.value.len()).sum();
        assert_eq!(filters.len(), 4);
        assert!(compiled <= MAX_DERIVED_BYTES_PER_SELECTOR);

        // Equalities cost nothing to compile and are never crowded out.
        let mut labels = profile.labels.clone();
        labels.push(single_label("zone", vec!["z1".into()]));
        let filters = LabelProfile { series: 60, labels }.common_filters();
        assert_eq!(filters.last().unwrap().to_string(), r#"zone="z1""#);
    }

    #[test]
    fn common_filters_where_asks_before_compiling() {
        let p = profile(&[
            &[("region", "us"), ("host", "a")],
            &[("region", "eu"), ("host", "b")],
        ]);
        let asked = std::cell::RefCell::new(Vec::new());
        let filters = p.common_filters_where(|label| {
            asked.borrow_mut().push(label.name.clone());
            label.name == "region"
        });
        assert_eq!(*asked.borrow(), vec!["host", "region"]);
        assert_eq!(rendered(&filters), vec![r#"region=~"eu|us""#]);
    }

    /// `could_be_pruned_by` must never answer `false` where
    /// `satisfied_by_all` of the built filter would: a `false` skips the
    /// filter without building it.
    #[test]
    fn could_be_pruned_by_agrees_with_satisfied_by_all() {
        let source = profile(&[&[("host", "a")], &[("host", "b")]]);
        let host = &source.labels[0];
        let filter = source.common_filters().pop().unwrap();

        let targets = [
            profile(&[&[("host", "a")], &[("host", "b")]]),
            profile(&[&[("host", "a")]]),
            profile(&[&[("host", "a")], &[("host", "c")]]),
            profile(&[&[("host", "a")], &[("rack", "1")]]),
            profile(&[&[("rack", "1")]]),
            profile(&[]),
        ];
        let expected = [false, false, true, true, true, false];
        for (target, expected) in targets.iter().zip(expected) {
            assert_eq!(target.could_be_pruned_by(host), expected, "{target:?}");
            assert_eq!(!target.satisfied_by_all(&filter), expected, "{target:?}");
        }

        let series: Vec<Vec<(&str, &str)>> = (0..=MAX_PUSHDOWN_VALUES)
            .map(|i| vec![("host", Box::leak(format!("h{i}").into_boxed_str()) as &str)])
            .collect();
        let refs: Vec<&[(&str, &str)]> = series.iter().map(|s| s.as_slice()).collect();
        assert!(profile(&refs).could_be_pruned_by(host));
    }
}
