//! Selectors as text the selector parser reads back.
//!
//! Parsing optimizes: a regex can become a prefix or substring matcher (`Contains`), a list of
//! literals, or `MatchAll`. Some of those have no syntax of their own, so rendering writes each
//! matcher in a form the grammar has — `Contains` as the regex it came from, `.*needle.*` — and
//! quotes any name that is not an identifier. Parsing the output gives an equivalent selector;
//! it may differ in representation (filter order, `""` for the RTS `l=` form), not in what it
//! matches.

use crate::common::constants::METRIC_NAME_LABEL;
use crate::labels::filters::{
    FilterList, LabelFilter, PredicateMatch, PredicateValue, SeriesSelector,
};
use crate::parser::utils::{is_bare_name, quote_string, write_name};
use std::fmt::{self, Write};

const MATCH_ALL: &str = ".*";

/// One matcher, `name<op>value`.
pub(crate) fn write_filter(f: &mut impl Write, filter: &LabelFilter) -> fmt::Result {
    write_name(f, &filter.label)?;
    match &filter.matcher {
        PredicateMatch::Equal(value) => write_op_value(f, "=", value),
        PredicateMatch::NotEqual(value) => write_op_value(f, "!=", value),
        PredicateMatch::MatchAll => write!(f, "=~{}", quote_string(MATCH_ALL)),
        PredicateMatch::MatchNone => write!(f, "!~{}", quote_string(MATCH_ALL)),
        PredicateMatch::HasLabel => write!(f, "!={}", quote_string("")),
        PredicateMatch::LacksLabel => write!(f, "={}", quote_string("")),
        PredicateMatch::RegexEqual(re) => write!(f, "=~{}", quote_string(re.value())),
        PredicateMatch::RegexNotEqual(re) => write!(f, "!~{}", quote_string(re.value())),
        PredicateMatch::StartsWith(value) => write_op_value(f, "^=", value),
        PredicateMatch::NotStartsWith(value) => write_op_value(f, "^~", value),
        // There is no substring operator: write the regex the matcher was decomposed from.
        PredicateMatch::Contains(value) => {
            write!(f, "=~{}", quote_string(&contains_regex(value)))
        }
        PredicateMatch::NotContains(value) => {
            write!(f, "!~{}", quote_string(&contains_regex(value)))
        }
    }
}

fn write_op_value(f: &mut impl Write, op: &str, value: &PredicateValue) -> fmt::Result {
    f.write_str(op)?;
    match value {
        PredicateValue::Empty => f.write_str(&quote_string("")),
        PredicateValue::String(s) => f.write_str(&quote_string(s)),
        PredicateValue::List(list) => match list.as_slice() {
            [] => f.write_str(&quote_string("")),
            [one] => f.write_str(&quote_string(one)),
            many => {
                f.write_str("(")?;
                for (i, v) in many.iter().enumerate() {
                    if i > 0 {
                        f.write_str(",")?;
                    }
                    f.write_str(&quote_string(v))?;
                }
                f.write_str(")")
            }
        },
    }
}

fn needles(value: &PredicateValue) -> Vec<&str> {
    match value {
        PredicateValue::Empty => vec![""],
        PredicateValue::String(s) => vec![s.as_str()],
        PredicateValue::List(list) => list.iter().map(String::as_str).collect(),
    }
}

/// A regex matching exactly the literals in `values`: `a`, or `a|b`.
fn literals_regex(values: &[&str]) -> String {
    let escaped: Vec<String> = values.iter().map(|v| regex::escape(v)).collect();
    escaped.join("|")
}

/// The regex a `Contains` operand was decomposed from: `.*foo.*`, or `.*(?:a|b).*`.
fn contains_regex(value: &PredicateValue) -> String {
    let needles = needles(value);
    match needles.as_slice() {
        [one] => format!("{MATCH_ALL}{}{MATCH_ALL}", regex::escape(one)),
        many => format!("{MATCH_ALL}(?:{}){MATCH_ALL}", literals_regex(many)),
    }
}

/// The metric names a group's `__name__` equalities allow: their intersection, in the order of
/// the first one (`[""]` for the absent name). `None` when there is no such equality, and an
/// empty list when they contradict each other, so the group matches nothing.
fn metric_names(group: &FilterList) -> Option<Vec<&str>> {
    let mut names: Option<Vec<&str>> = None;
    for filter in group.iter().filter(|f| f.label == METRIC_NAME_LABEL) {
        let allowed = match &filter.matcher {
            PredicateMatch::LacksLabel => vec![""],
            PredicateMatch::Equal(value) if value.is_absent() => vec![""],
            PredicateMatch::Equal(value) => needles(value),
            _ => continue,
        };
        names = Some(match names {
            None => allowed,
            Some(kept) => kept.into_iter().filter(|n| allowed.contains(n)).collect(),
        });
    }
    names
}

/// The group's metric name, if it is a single identifier that can go before the braces. Not
/// when the group has another `__name__` matcher: the parser rejects any `__name__` matcher in
/// braces that follow a metric name (`cpu{__name__^="c"}`), so the name stays inside them.
fn hoistable_name(group: &FilterList) -> Option<&str> {
    let other_name_matcher = group
        .iter()
        .any(|f| f.label == METRIC_NAME_LABEL && !is_name_equality(f));
    if other_name_matcher {
        return None;
    }
    match metric_names(group)?.as_slice() {
        [name] if is_bare_name(name) => Some(name),
        _ => None,
    }
}

/// The filters of one group, comma-separated, without braces.
///
/// The parser takes one metric-name equality per group, so the group's `__name__` equalities
/// are written as one: the intersection of the names they allow ([`metric_names`]). A single
/// name is written as the metric-name shorthand (`cpu`, or `"my metric"`), or left out when
/// `hoisted` before the braces; several as `__name__=("a","b")`. Equalities that contradict
/// each other make the group match nothing, which is written as such.
fn write_group_items(f: &mut impl Write, group: &FilterList, hoisted: bool) -> fmt::Result {
    let mut first = true;
    match metric_names(group).as_deref() {
        None => {}
        Some([]) => return write!(f, "{METRIC_NAME_LABEL}!~{}", quote_string(MATCH_ALL)),
        Some([name]) if !name.is_empty() => {
            if !hoisted {
                separator(f, &mut first)?;
                write_name(f, name)?;
            }
        }
        Some(names) => {
            separator(f, &mut first)?;
            write_name(f, METRIC_NAME_LABEL)?;
            let value = match names {
                [one] => PredicateValue::from(*one),
                many => {
                    PredicateValue::from(many.iter().map(|n| n.to_string()).collect::<Vec<_>>())
                }
            };
            write_op_value(f, "=", &value)?;
        }
    }
    for filter in group.iter() {
        if is_name_equality(filter) {
            continue;
        }
        separator(f, &mut first)?;
        write_filter(f, filter)?;
    }
    if first {
        // An empty group matches every series. The braces form drops an empty group between
        // `or`s, so spell it as a matcher that also matches everything.
        write_name(f, METRIC_NAME_LABEL)?;
        write!(f, "=~{}", quote_string(MATCH_ALL))?;
    }
    Ok(())
}

/// Whether the group has a filter besides its `__name__` equalities.
fn has_other_filters(group: &FilterList) -> bool {
    group.iter().any(|f| !is_name_equality(f))
}

/// A `__name__` equality, which [`metric_names`] folds into the group's name: `__name__="x"`,
/// or the absent name `__name__=""`.
fn is_name_equality(filter: &LabelFilter) -> bool {
    filter.label == METRIC_NAME_LABEL
        && matches!(
            filter.matcher,
            PredicateMatch::Equal(_) | PredicateMatch::LacksLabel
        )
}

fn separator(f: &mut impl Write, first: &mut bool) -> fmt::Result {
    if !*first {
        f.write_str(",")?;
    }
    *first = false;
    Ok(())
}

/// One AND group as a selector: `cpu{…}`, `cpu`, or `{…}`.
pub(crate) fn write_and(f: &mut impl Write, group: &FilterList) -> fmt::Result {
    if group.is_empty() {
        return f.write_str("{}");
    }
    if let Some(name) = hoistable_name(group) {
        f.write_str(name)?;
        if !has_other_filters(group) {
            return Ok(());
        }
        f.write_str("{")?;
        write_group_items(f, group, true)?;
        return f.write_str("}");
    }
    f.write_str("{")?;
    write_group_items(f, group, false)?;
    f.write_str("}")
}

/// A selector: an `And` as [`write_and`]; an `Or` as `{g1 or g2 …}`, or `cpu{g1 or g2 …}` when
/// every group names the same metric and still has another filter.
pub(crate) fn write_selector(f: &mut impl Write, selector: &SeriesSelector) -> fmt::Result {
    let groups = match selector {
        SeriesSelector::And(group) => return write_and(f, group),
        SeriesSelector::Or(groups) => groups,
    };
    if groups.is_empty() {
        // A union of no groups matches nothing.
        return write!(f, "{{{METRIC_NAME_LABEL}!~{}}}", quote_string(MATCH_ALL));
    }
    let common = hoistable_name(&groups[0]).filter(|name| {
        groups
            .iter()
            .all(|g| has_other_filters(g) && hoistable_name(g) == Some(*name))
    });
    if let Some(name) = common {
        f.write_str(name)?;
    }
    f.write_str("{")?;
    for (i, group) in groups.iter().enumerate() {
        if i > 0 {
            f.write_str(" or ")?;
        }
        write_group_items(f, group, common.is_some())?;
    }
    f.write_str("}")
}

#[cfg(test)]
mod tests {
    use crate::labels::filters::{
        FilterList, LabelFilter, PredicateMatch, PredicateValue, SeriesSelector,
    };

    /// Whether `selector` matches a series with `labels`, a missing label reading as `""` (the
    /// planner's absent-label rule).
    fn selects(selector: &SeriesSelector, labels: &[(&str, &str)]) -> bool {
        let value = |name: &str| {
            labels
                .iter()
                .find(|(n, _)| *n == name)
                .map_or("", |(_, v)| *v)
        };
        selector
            .groups()
            .any(|group| group.iter().all(|f| f.matches(value(&f.label))))
    }

    fn sample_series() -> Vec<Vec<(&'static str, &'static str)>> {
        let names = ["cpu", "mem", "my metric", "or", "9x"];
        let jobs = ["", "api", "api-server", "web", "x.y", "foo{1}"];
        let others = [("a b", "v"), ("or", "1"), ("env", "prod")];
        let mut out = vec![vec![]];
        for name in names {
            for job in jobs {
                let mut base = vec![("__name__", name)];
                if !job.is_empty() {
                    base.push(("job", job));
                }
                out.push(base.clone());
                for other in others {
                    let mut with = base.clone();
                    with.push(other);
                    out.push(with);
                }
            }
        }
        out
    }

    /// Rendering, then parsing, gives a selector that matches exactly the same series.
    fn assert_round_trips(selector: &SeriesSelector) -> String {
        let text = selector.to_string();
        let parsed = SeriesSelector::parse(&text).unwrap_or_else(|e| {
            panic!("{selector:?} rendered as {text:?}, which does not parse: {e}")
        });
        for labels in sample_series() {
            assert_eq!(
                selects(selector, &labels),
                selects(&parsed, &labels),
                "{text:?} on {labels:?}"
            );
        }
        text
    }

    #[test]
    fn test_parsed_selectors_render_to_equivalent_text() {
        for input in [
            "cpu",
            r#"cpu{job="api"}"#,
            r#"{job=~".*api.*"}"#,
            r#"{job!~".*api.*"}"#,
            r#"{job=~".*(api|web).*"}"#,
            r#"{job=~"api.*"}"#,
            r#"{job^="api"}"#,
            r#"{job^~("api","web")}"#,
            r#"{job=("api","web")}"#,
            r#"{job=~"api|web|"}"#,
            r#"{job!=""}"#,
            r#"{job=""}"#,
            r#"{job=~".*"}"#,
            r#"{job!~".*"}"#,
            r#"{job=~"x\\.y"}"#,
            r#"{job=~".*\\{1\\}.*"}"#,
            r#"{"a b"="v"}"#,
            r#"{"or"="1"}"#,
            r#"{"my metric"}"#,
            r#"{"my metric", job="api"}"#,
            r#"{__name__="9x"}"#,
            r#"{__name__=~"c.*", __name__="cpu"}"#,
            r#"cpu{job="api" or job="web"}"#,
            r#"{__name__="cpu", job="api" or env="prod"}"#,
            r#"{job="api"} or {env="prod"}"#,
            r#"{}"#,
        ] {
            let selector = SeriesSelector::parse(input).unwrap_or_else(|e| panic!("{input}: {e}"));
            assert_round_trips(&selector);
        }
    }

    /// Selectors no parse produces, but other code (fan-out decoding, the planner) builds.
    #[test]
    fn test_built_selectors_render_to_equivalent_text() {
        let filter = |label: &str, matcher| LabelFilter {
            label: label.to_string(),
            matcher,
        };
        let eq =
            |label: &str, v: &str| filter(label, PredicateMatch::Equal(PredicateValue::from(v)));
        let cases = [
            // Matches nothing.
            SeriesSelector::Or(vec![]),
            // An empty group among others matches everything.
            SeriesSelector::Or(vec![
                FilterList::from(vec![eq("job", "api")]),
                FilterList::default(),
            ]),
            // Two metric-name equalities (contradictory) in one group.
            SeriesSelector::with_filters(vec![eq("__name__", "cpu"), eq("__name__", "mem")]),
            SeriesSelector::with_filters(vec![eq("__name__", "cpu"), eq("__name__", "cpu")]),
            // The RTS `l=` / `l!=` forms, built directly and as the presence checks they parse to.
            SeriesSelector::with_filters(vec![filter(
                "job",
                PredicateMatch::NotEqual(PredicateValue::Empty),
            )]),
            SeriesSelector::with_filters(vec![filter(
                "job",
                PredicateMatch::Equal(PredicateValue::Empty),
            )]),
            SeriesSelector::with_filters(vec![
                eq("job", "api"),
                filter("env", PredicateMatch::HasLabel),
                filter("region", PredicateMatch::LacksLabel),
            ]),
            // The absent metric name, alone and beside a name.
            SeriesSelector::with_filters(vec![
                filter("__name__", PredicateMatch::LacksLabel),
                eq("job", "api"),
            ]),
            SeriesSelector::with_filters(vec![
                eq("__name__", "cpu"),
                filter("__name__", PredicateMatch::LacksLabel),
            ]),
            // Substring lists, with characters that need escaping.
            SeriesSelector::with_filters(vec![filter(
                "job",
                PredicateMatch::Contains(PredicateValue::from(vec![
                    "x.y".to_string(),
                    "{1}".to_string(),
                ])),
            )]),
            SeriesSelector::with_filters(vec![filter(
                "job",
                PredicateMatch::NotContains(PredicateValue::from("-ser")),
            )]),
            // Names that are not identifiers, as label and as metric.
            SeriesSelector::with_filters(vec![
                eq("__name__", "my metric"),
                eq("a b", "v"),
                eq("or", "1"),
            ]),
        ];
        for selector in cases {
            assert_round_trips(&selector);
        }
    }

    #[test]
    fn test_rendered_text() {
        let render = |input: &str| SeriesSelector::parse(input).unwrap().to_string();
        // Substring matchers render as the regex they came from; there is no `*=` operator.
        assert_eq!(render(r#"{job=~".*api.*"}"#), r#"{job=~".*api.*"}"#);
        assert_eq!(
            render(r#"{job!~".*(api|web).*"}"#),
            r#"{job!~".*(?:api|web).*"}"#
        );
        // Names that are not identifiers are quoted; a bare metric name goes before the braces.
        assert_eq!(render(r#"{"a b"="v"}"#), r#"{"a b"="v"}"#);
        assert_eq!(render(r#"{"or"="1"}"#), r#"{"or"="1"}"#);
        assert_eq!(
            render(r#"{"my metric", job="api"}"#),
            r#"{"my metric",job="api"}"#
        );
        assert_eq!(render(r#"cpu{job="api"}"#), r#"cpu{job="api"}"#);
        assert_eq!(render("cpu"), "cpu");
        let twice = SeriesSelector::with_filters(vec![
            LabelFilter::equals("__name__".into(), "cpu"),
            LabelFilter::equals("__name__".into(), "cpu"),
        ]);
        assert_eq!(twice.to_string(), "cpu");
        assert_eq!(
            render(r#"cpu{job="api" or job="web"}"#),
            r#"cpu{job="api" or job="web"}"#
        );
    }
}
