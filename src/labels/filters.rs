use crate::common::constants::METRIC_NAME_LABEL;
use crate::labels::parse_series_selector;
use crate::labels::regex::CompiledRegex;
use crate::labels::regex::compile_matcher;
use crate::labels::regex_cache::HeapSize;
use crate::labels::regex_utils::{RegexHints, parse_regex_matcher, regex_hints_for_pattern};
use crate::labels::render;
use crate::parser::ParseError;
use crate::parser::utils::quote_string;
use smallvec::SmallVec;
use std::cmp::PartialEq;
use std::default::Default;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::ops::{Deref, DerefMut};

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum MatchOp {
    Equal,
    NotEqual,
    RegexEqual,
    RegexNotEqual,
    StartsWith, // ^= operator for prefix matching
    NotStartsWith,
    Contains,
    NotContains,
}

impl MatchOp {
    pub fn is_regex(&self) -> bool {
        matches!(self, MatchOp::RegexEqual | MatchOp::RegexNotEqual)
    }
}

impl Display for MatchOp {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            MatchOp::Equal => write!(f, "="),
            MatchOp::NotEqual => write!(f, "!="),
            MatchOp::RegexEqual => write!(f, "=~"),
            MatchOp::RegexNotEqual => write!(f, "!~"),
            MatchOp::StartsWith => write!(f, "^="),
            MatchOp::NotStartsWith => write!(f, "^~"),
            MatchOp::Contains => write!(f, "*="),
            MatchOp::NotContains => write!(f, "!*="),
        }
    }
}

pub type ValueList = SmallVec<[String; 4]>;

#[derive(Clone, Debug)]
pub enum PredicateValue {
    Empty,
    List(ValueList),
    String(String),
}

impl Display for PredicateValue {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            PredicateValue::Empty => write!(f, "\"\""),
            PredicateValue::String(s) => write!(f, "{}", quote_string(s)),
            PredicateValue::List(values) => {
                write!(f, "(")?;
                for (i, value) in values.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", quote_string(value))?;
                }
                write!(f, ")")
            }
        }
    }
}

impl PredicateValue {
    pub fn matches(&self, value: &str) -> bool {
        match self {
            PredicateValue::Empty => value.is_empty(),
            PredicateValue::String(s) => s == value,
            // An empty list stands for "label absent", so it matches only the
            // empty value. A non-empty list matches the empty value only when it
            // holds an empty alternative, e.g. `l=~"foo|"`.
            PredicateValue::List(list) => {
                (value.is_empty() && list.is_empty()) || list.iter().any(|x| x == value)
            }
        }
    }

    pub fn text(&self) -> Option<&str> {
        match self {
            PredicateValue::Empty => None,
            PredicateValue::String(s) => Some(s),
            PredicateValue::List(list) => {
                if list.len() == 1 {
                    return list.first().map(|x| x.as_str());
                }
                None
            }
        }
    }

    /// True when this value matches only a missing label: `Empty` (the RTS `l=` form),
    /// `String("")`, or a list whose alternatives are all `""` (including none). These are three
    /// spellings of one thing; code that asks "is this the absent value?" asks this, not which
    /// variant it is.
    pub fn is_absent(&self) -> bool {
        match self {
            PredicateValue::Empty => true,
            PredicateValue::String(value) => value.is_empty(),
            PredicateValue::List(list) => list.iter().all(String::is_empty),
        }
    }

    /// True when every alternative is a value a series must actually carry: at least one, and
    /// none empty. The operand rule for the prefix and substring matchers, and the case the
    /// planner can answer from the index alone. A list mixing `""` with values is neither this
    /// nor [`Self::is_absent`].
    pub fn requires_present(&self) -> bool {
        match self {
            PredicateValue::Empty => false,
            PredicateValue::String(value) => !value.is_empty(),
            PredicateValue::List(list) => !list.is_empty() && list.iter().all(|v| !v.is_empty()),
        }
    }

    /// Whether `value` contains any of the needles. Construction rejects empty needles and empty
    /// lists ([`PredicateMatch::validate`]); without them this would be true (an empty needle) or
    /// false (no needles), and `NotContains` is its exact complement either way.
    pub fn matches_contains_any(&self, value: &str) -> bool {
        match self {
            PredicateValue::Empty => false,
            PredicateValue::String(needle) => value.contains(needle.as_str()),
            PredicateValue::List(list) => list.iter().any(|needle| value.contains(needle.as_str())),
        }
    }

    /// Whether `value` starts with any of the prefixes; see [`Self::matches_contains_any`].
    pub fn matches_starts_with_any(&self, value: &str) -> bool {
        match self {
            PredicateValue::Empty => false,
            PredicateValue::String(prefix) => value.starts_with(prefix.as_str()),
            PredicateValue::List(list) => {
                list.iter().any(|prefix| value.starts_with(prefix.as_str()))
            }
        }
    }

    fn cost(&self) -> usize {
        match self {
            PredicateValue::Empty => 0,
            PredicateValue::String(_) => 1,
            PredicateValue::List(list) => list.len(),
        }
    }
}

/// A prefix or substring operand must be needles a value can contain: at least one, none
/// empty. An empty needle would match every value and no needles would match none, neither of
/// which anyone writes on purpose.
fn require_needles(value: &PredicateValue, matcher: &str) -> Result<(), ParseError> {
    if value.requires_present() {
        Ok(())
    } else {
        Err(ParseError::General(format!(
            "{matcher} matcher does not allow empty values"
        )))
    }
}

pub(crate) fn validate_contains_value(value: &PredicateValue) -> Result<(), ParseError> {
    require_needles(value, "contains")
}

pub(crate) fn validate_starts_with_value(value: &PredicateValue) -> Result<(), ParseError> {
    require_needles(value, "starts with")
}

impl From<Vec<String>> for PredicateValue {
    fn from(value: Vec<String>) -> Self {
        PredicateValue::List(value.into())
    }
}

impl From<&str> for PredicateValue {
    fn from(value: &str) -> Self {
        PredicateValue::String(value.to_string())
    }
}

impl From<String> for PredicateValue {
    fn from(value: String) -> Self {
        PredicateValue::String(value)
    }
}

impl PartialEq for PredicateValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (PredicateValue::Empty, PredicateValue::Empty) => true,
            (PredicateValue::String(s1), PredicateValue::String(s2)) => s1 == s2,
            (PredicateValue::List(v1), PredicateValue::List(v2)) => v1 == v2,
            _ => false,
        }
    }
}

/// A compiled regex plus the prefilters derived from it. The fields are private because the
/// prefilters must agree with the regex: the postings planner trusts `prefix` to narrow its
/// scan, so a prefix the regex does not imply would drop matching series. Build one with
/// [`Self::create`], [`Self::from_parts`] or [`Self::with_hints`].
#[derive(Clone, Debug)]
pub struct RegexMatcher {
    regex: CompiledRegex,
    prefix: Option<String>,
    suffix: Option<String>,
    required_literals: SmallVec<[String; 2]>,
    /// The pattern matches exactly the non-empty values (see [`RegexHints`]).
    any_non_empty: bool,
    /// The pattern as the user wrote it; the fan-out codec sends this, and a shard recompiles it.
    value: String,
}

impl RegexMatcher {
    fn new(regex: CompiledRegex, value: String) -> Self {
        Self::from_parts(regex, value, None)
    }

    /// Build a matcher for `value`, deriving the prefilter hints from it.
    /// This parses `value` once; callers that already hold its parsed form use
    /// [`Self::with_hints`].
    pub(crate) fn from_parts(regex: CompiledRegex, value: String, prefix: Option<String>) -> Self {
        let hints = regex_hints_for_pattern(&value, prefix.as_deref());
        Self::with_hints(regex, value, prefix, hints)
    }

    /// Build a matcher from hints the caller already derived from `value`.
    pub(crate) fn with_hints(
        regex: CompiledRegex,
        value: String,
        prefix: Option<String>,
        hints: RegexHints,
    ) -> Self {
        Self {
            regex,
            value,
            prefix,
            suffix: hints.suffix,
            required_literals: hints.required_literals.into(),
            any_non_empty: hints.matches_any_non_empty,
        }
    }

    /// True when the pattern matches exactly the non-empty values: `.+`, `^.+$`, `(.+)`,
    /// `.+.*` and so on. Recognized on the parsed pattern, not its text.
    pub fn matches_any_non_empty(&self) -> bool {
        self.any_non_empty
    }

    /// The pattern as written.
    pub fn value(&self) -> &str {
        &self.value
    }

    /// A literal every matching value starts with, if the decomposition found one.
    pub fn prefix(&self) -> Option<&str> {
        self.prefix.as_deref()
    }

    /// A literal every matching value ends with, if one was found.
    pub fn suffix(&self) -> Option<&str> {
        self.suffix.as_deref()
    }

    /// Literals every matching value contains, in order, after the prefix.
    pub fn required_literals(&self) -> &[String] {
        &self.required_literals
    }

    pub fn create(value: &str) -> Result<Self, ParseError> {
        let regex = compile_matcher(value)
            .map_err(|err| ParseError::InvalidRegex(format!("`{value}`: {err}")))?;
        Ok(Self::new(regex, value.to_string()))
    }

    pub fn is_match(&self, other: &str) -> bool {
        if self.any_non_empty {
            return !other.is_empty();
        }
        if other.is_empty() {
            if self
                .prefix
                .as_ref()
                .is_some_and(|prefix| !prefix.is_empty())
            {
                return false;
            }
            return self.regex.is_match("");
        }
        // Fast-path: if a literal prefix is known, reject immediately without
        // running the full regex engine. The compiled regex already encodes the
        // prefix, so we pass the full string to it unchanged.
        if let Some(prefix) = &self.prefix
            && !other.starts_with(prefix.as_str())
        {
            return false;
        }
        let remainder = self
            .prefix
            .as_ref()
            .and_then(|prefix| other.strip_prefix(prefix))
            .unwrap_or(other);
        if let Some(suffix) = &self.suffix
            && !remainder.ends_with(suffix)
        {
            return false;
        }
        if !self.matches_required_literals(remainder) {
            return false;
        }
        self.regex.is_match(other)
    }

    pub(crate) fn cost(&self) -> usize {
        match (
            self.prefix.is_some(),
            self.suffix.is_some(),
            self.required_literals.is_empty(),
        ) {
            (true, true, false) => 12,
            (true, true, true) => 14,
            (true, false, false) | (false, true, false) => 16,
            (true, false, true) | (false, true, true) => 18,
            (false, false, false) => 30,
            (false, false, true) => 50,
        }
    }

    fn matches_required_literals(&self, mut haystack: &str) -> bool {
        for literal in &self.required_literals {
            if literal.is_empty() {
                continue;
            }
            let Some(idx) = haystack.find(literal.as_str()) else {
                return false;
            };
            haystack = &haystack[idx + literal.len()..];
        }
        true
    }
}

impl TryFrom<&str> for RegexMatcher {
    type Error = ParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::create(value)
    }
}

impl Eq for RegexMatcher {}

impl PartialEq for RegexMatcher {
    fn eq(&self, other: &Self) -> bool {
        self.regex.as_str() == other.regex.as_str()
            && self.prefix == other.prefix
            && self.suffix == other.suffix
            && self.required_literals == other.required_literals
            && self.any_non_empty == other.any_non_empty
            && self.value == other.value
    }
}

/// What a decomposed matcher keeps alive, for the matcher caches in `regex_utils`. The regex
/// dominates; strings are charged their capacity.
impl HeapSize for PredicateValue {
    fn heap_size(&self) -> usize {
        match self {
            PredicateValue::Empty => 0,
            PredicateValue::String(value) => value.capacity(),
            PredicateValue::List(values) => {
                let spilled = if values.spilled() {
                    values.capacity() * size_of::<String>()
                } else {
                    0
                };
                spilled + values.iter().map(String::capacity).sum::<usize>()
            }
        }
    }
}

impl HeapSize for RegexMatcher {
    fn heap_size(&self) -> usize {
        let strings = self.value.capacity()
            + self.prefix.as_ref().map_or(0, String::capacity)
            + self.suffix.as_ref().map_or(0, String::capacity)
            + self
                .required_literals
                .iter()
                .map(String::capacity)
                .sum::<usize>();
        // Boxed inside `PredicateMatch`.
        size_of::<Self>() + self.regex.heap_size() + strings
    }
}

impl HeapSize for PredicateMatch {
    fn heap_size(&self) -> usize {
        match self {
            PredicateMatch::MatchAll | PredicateMatch::MatchNone => 0,
            PredicateMatch::RegexEqual(re) | PredicateMatch::RegexNotEqual(re) => re.heap_size(),
            PredicateMatch::Equal(value)
            | PredicateMatch::NotEqual(value)
            | PredicateMatch::StartsWith(value)
            | PredicateMatch::NotStartsWith(value)
            | PredicateMatch::Contains(value)
            | PredicateMatch::NotContains(value) => value.heap_size(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum PredicateMatch {
    Equal(PredicateValue),
    NotEqual(PredicateValue),
    MatchAll,
    MatchNone,
    // Boxed: a `RegexMatcher` is 168 bytes and most matchers are not regexes, so holding it
    // inline made every `LabelFilter` 200 bytes and every `SeriesSelector` over 1 KiB.
    RegexEqual(Box<RegexMatcher>),
    RegexNotEqual(Box<RegexMatcher>),
    StartsWith(PredicateValue),
    NotStartsWith(PredicateValue),
    Contains(PredicateValue),
    NotContains(PredicateValue),
}

impl PredicateMatch {
    pub fn op(&self) -> MatchOp {
        match self {
            PredicateMatch::Equal(_) => MatchOp::Equal,
            PredicateMatch::NotEqual(_) => MatchOp::NotEqual,
            // `l=~".*"` and `l!~".*"`, which is how they are written and rendered.
            PredicateMatch::MatchAll => MatchOp::RegexEqual,
            PredicateMatch::MatchNone => MatchOp::RegexNotEqual,
            PredicateMatch::RegexEqual(_) => MatchOp::RegexEqual,
            PredicateMatch::RegexNotEqual(_) => MatchOp::RegexNotEqual,
            PredicateMatch::StartsWith(_) => MatchOp::StartsWith,
            PredicateMatch::NotStartsWith(_) => MatchOp::NotStartsWith,
            PredicateMatch::Contains(_) => MatchOp::Contains,
            PredicateMatch::NotContains(_) => MatchOp::NotContains,
        }
    }

    pub fn matches_empty(&self) -> bool {
        self.matches("")
    }

    pub fn matches(&self, other: &str) -> bool {
        match self {
            PredicateMatch::Equal(value) => value.matches(other),
            PredicateMatch::NotEqual(value) => !value.matches(other),
            PredicateMatch::MatchAll => true,
            PredicateMatch::MatchNone => false,
            PredicateMatch::RegexEqual(re) => re.is_match(other),
            PredicateMatch::RegexNotEqual(re) => !re.is_match(other),
            PredicateMatch::StartsWith(value) => value.matches_starts_with_any(other),
            PredicateMatch::NotStartsWith(value) => !value.matches_starts_with_any(other),
            PredicateMatch::Contains(value) => value.matches_contains_any(other),
            PredicateMatch::NotContains(value) => !value.matches_contains_any(other),
        }
    }

    pub fn inverse(self) -> Self {
        match self {
            PredicateMatch::Equal(value) => PredicateMatch::NotEqual(value),
            PredicateMatch::NotEqual(value) => PredicateMatch::Equal(value),
            PredicateMatch::MatchAll => PredicateMatch::MatchNone,
            PredicateMatch::MatchNone => PredicateMatch::MatchAll,
            PredicateMatch::RegexEqual(re) => PredicateMatch::RegexNotEqual(re),
            PredicateMatch::RegexNotEqual(re) => PredicateMatch::RegexEqual(re),
            PredicateMatch::StartsWith(p) => PredicateMatch::NotStartsWith(p),
            PredicateMatch::NotStartsWith(p) => PredicateMatch::StartsWith(p),
            PredicateMatch::Contains(p) => PredicateMatch::NotContains(p),
            PredicateMatch::NotContains(p) => PredicateMatch::Contains(p),
        }
    }

    /// Reject operands no matcher may hold: an empty needle, or no needles, for the prefix and
    /// substring matchers. [`LabelFilter::create_with_value`] and the fan-out decoder both apply
    /// this, so a constructed matcher's match functions can trust their operands.
    pub(crate) fn validate(&self) -> Result<(), ParseError> {
        match self {
            PredicateMatch::StartsWith(value) | PredicateMatch::NotStartsWith(value) => {
                validate_starts_with_value(value)
            }
            PredicateMatch::Contains(value) | PredicateMatch::NotContains(value) => {
                validate_contains_value(value)
            }
            PredicateMatch::Equal(_)
            | PredicateMatch::NotEqual(_)
            | PredicateMatch::MatchAll
            | PredicateMatch::MatchNone
            | PredicateMatch::RegexEqual(_)
            | PredicateMatch::RegexNotEqual(_) => Ok(()),
        }
    }

    pub(crate) fn cost(&self) -> usize {
        match &self {
            PredicateMatch::Equal(value) => value.cost(),
            PredicateMatch::NotEqual(value) => value.cost(),
            PredicateMatch::MatchAll => 0,
            PredicateMatch::MatchNone => 0,
            PredicateMatch::RegexEqual(re) => re.cost(),
            PredicateMatch::RegexNotEqual(re) => re.cost() + 5,
            PredicateMatch::StartsWith(value) => 10 + value.cost(),
            PredicateMatch::NotStartsWith(value) => 10 + value.cost(),
            PredicateMatch::Contains(value) => 20 + value.cost(),
            PredicateMatch::NotContains(value) => 25 + value.cost(),
        }
    }
}

impl Eq for PredicateMatch {}

impl PartialEq for PredicateMatch {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (PredicateMatch::Equal(v1), PredicateMatch::Equal(v2)) => v1 == v2,
            (PredicateMatch::NotEqual(v1), PredicateMatch::NotEqual(v2)) => v1 == v2,
            (PredicateMatch::MatchAll, PredicateMatch::MatchAll) => true,
            (PredicateMatch::MatchNone, PredicateMatch::MatchNone) => true,
            (PredicateMatch::RegexEqual(re1), PredicateMatch::RegexEqual(re2)) => re1 == re2,
            (PredicateMatch::RegexNotEqual(re1), PredicateMatch::RegexNotEqual(re2)) => re1 == re2,
            (PredicateMatch::StartsWith(p1), PredicateMatch::StartsWith(p2)) => p1 == p2,
            (PredicateMatch::NotStartsWith(p1), PredicateMatch::NotStartsWith(p2)) => p1 == p2,
            (PredicateMatch::Contains(v1), PredicateMatch::Contains(v2)) => v1 == v2,
            (PredicateMatch::NotContains(v1), PredicateMatch::NotContains(v2)) => v1 == v2,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabelFilter {
    pub label: String,
    pub matcher: PredicateMatch,
}

impl LabelFilter {
    pub fn create<N, V>(match_op: MatchOp, label: N, value: V) -> Result<Self, ParseError>
    where
        N: Into<String>,
        V: Into<String>,
    {
        Self::create_with_value(match_op, label, PredicateValue::String(value.into()))
    }

    pub fn create_with_value<N>(
        match_op: MatchOp,
        label: N,
        value: PredicateValue,
    ) -> Result<Self, ParseError>
    where
        N: Into<String>,
    {
        let regex_text = |value: &PredicateValue| {
            value.text().map(str::to_owned).ok_or_else(|| {
                ParseError::General("regex matcher requires a single value".to_string())
            })
        };
        let matcher = match match_op {
            MatchOp::Equal => PredicateMatch::Equal(value),
            MatchOp::NotEqual => PredicateMatch::NotEqual(value),
            MatchOp::RegexEqual => parse_regex_matcher(&regex_text(&value)?, true)?,
            MatchOp::RegexNotEqual => parse_regex_matcher(&regex_text(&value)?, false)?,
            MatchOp::StartsWith => PredicateMatch::StartsWith(value),
            MatchOp::NotStartsWith => PredicateMatch::NotStartsWith(value),
            MatchOp::Contains => PredicateMatch::Contains(value),
            MatchOp::NotContains => PredicateMatch::NotContains(value),
        };
        matcher.validate()?;
        Ok(Self {
            label: label.into(),
            matcher,
        })
    }

    /// Whether the label name, a value, or a regex as written holds a NUL byte. No stored label
    /// holds one (DIV-0068), so the selector parser refuses such a matcher.
    pub fn contains_nul(&self) -> bool {
        fn value_has_nul(value: &PredicateValue) -> bool {
            match value {
                PredicateValue::Empty => false,
                PredicateValue::String(s) => s.contains('\0'),
                PredicateValue::List(list) => list.iter().any(|s| s.contains('\0')),
            }
        }
        self.label.contains('\0')
            || match &self.matcher {
                PredicateMatch::Equal(v)
                | PredicateMatch::NotEqual(v)
                | PredicateMatch::StartsWith(v)
                | PredicateMatch::NotStartsWith(v)
                | PredicateMatch::Contains(v)
                | PredicateMatch::NotContains(v) => value_has_nul(v),
                PredicateMatch::RegexEqual(r) | PredicateMatch::RegexNotEqual(r) => {
                    r.value.contains('\0')
                }
                PredicateMatch::MatchAll | PredicateMatch::MatchNone => false,
            }
    }

    pub fn equals(label: String, value: &str) -> Self {
        Self {
            label,
            matcher: PredicateMatch::Equal(value.into()),
        }
    }

    pub fn not_equals(label: String, value: &str) -> Self {
        Self {
            label,
            matcher: PredicateMatch::NotEqual(value.into()),
        }
    }

    pub fn op(&self) -> MatchOp {
        self.matcher.op()
    }

    pub fn inverse(self) -> Self {
        Self {
            label: self.label,
            matcher: self.matcher.inverse(),
        }
    }

    pub fn matches_empty(&self) -> bool {
        self.matches("")
    }

    #[inline]
    /// A matcher written as a negation (`!=`, `!~`, `^~`, `!*=`), whose matches are found by
    /// subtracting from a base set. `MatchNone` (`l!~".*"`) is not one, though it is written with
    /// `!~`: it matches nothing, so the planner answers it directly and `is_bounded` counts it as
    /// bounding the selector.
    pub fn is_negative_matcher(&self) -> bool {
        matches!(
            self.matcher,
            PredicateMatch::NotEqual(_)
                | PredicateMatch::RegexNotEqual(_)
                | PredicateMatch::NotStartsWith(_)
                | PredicateMatch::NotContains(_)
        )
    }

    #[inline]
    pub fn is_metric_name_filter(&self) -> bool {
        self.label == METRIC_NAME_LABEL && self.op() == MatchOp::Equal
    }

    pub fn matches(&self, value: &str) -> bool {
        self.matcher.matches(value)
    }

    pub(crate) fn cost(&self) -> usize {
        self.matcher.cost()
    }
}

/// The matcher as the selector parser reads it back; see [`crate::labels::render`].
impl Display for LabelFilter {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        render::write_filter(f, self)
    }
}

/// The metric name every series matching `filters` (one AND group) must have, if the group
/// pins a single one: an equality on `__name__` with one non-empty value, which every other
/// `__name__` matcher in the group also accepts.
///
/// `None` when the group leaves the name open (no equality, or one listing several names), and
/// when its `__name__` matchers contradict each other (`{__name__=~"m.*",__name__="cpu"}`):
/// that group matches nothing, so no name describes what it matches.
fn group_metric_name(filters: &[LabelFilter]) -> Option<&str> {
    let mut name_filters = filters.iter().filter(|f| f.label == METRIC_NAME_LABEL);
    let name = name_filters.clone().find_map(|f| match &f.matcher {
        PredicateMatch::Equal(value) => value.text().filter(|name| !name.is_empty()),
        _ => None,
    })?;
    name_filters.all(|f| f.matches(name)).then_some(name)
}

/// FilterList is a small vector of LabelFilter, used for AND combinations of label filters.
/// We try to minimize allocations by reserving stack space for 3 LabelFilter, which seems reasonable for the common
/// case. e.g. http_requests_total{job="api",method="GET"}
#[derive(Debug, Default, Clone, PartialEq)]
pub struct FilterList(SmallVec<[LabelFilter; 3]>);

impl FilterList {
    pub fn new(matchers: Vec<LabelFilter>) -> Self {
        let inner = SmallVec::from_vec(matchers);
        Self(inner)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// True when the group is exactly one metric-name equality: it selects every series with
    /// that name and constrains nothing else.
    pub fn is_only_metric_name(&self) -> bool {
        self.0.len() == 1 && group_metric_name(&self.0).is_some()
    }

    /// See [`SeriesSelector::get_metric_name`].
    pub fn get_metric_name(&self) -> Option<&str> {
        group_metric_name(&self.0)
    }

    pub fn push(&mut self, matcher: LabelFilter) {
        self.0.push(matcher);
    }

    pub fn insert(&mut self, index: usize, matcher: LabelFilter) {
        self.0.insert(index, matcher);
    }

    pub fn iter(&self) -> impl Iterator<Item = &LabelFilter> {
        self.0.iter()
    }
}

/// The group as an AND selector the parser reads back; see [`crate::labels::render`].
impl Display for FilterList {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        render::write_and(f, self)
    }
}

impl Deref for FilterList {
    type Target = [LabelFilter];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for FilterList {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<Vec<LabelFilter>> for FilterList {
    fn from(value: Vec<LabelFilter>) -> Self {
        Self::new(value)
    }
}

/// The groups of an OR selector, each an AND of filters. A plain `Vec`: OR selectors are rare,
/// and inline storage for two `FilterList`s made every `SeriesSelector`, OR or not, that big.
pub type OrFiltersList = Vec<FilterList>;

/// See [`SeriesSelector::is_bounded`]. An empty group matches every series, so it is unbounded.
fn filter_group_is_bounded(filters: &FilterList) -> bool {
    filters
        .iter()
        .any(|f| !f.is_negative_matcher() && !f.matches_empty())
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum SeriesSelector {
    Or(OrFiltersList),
    And(FilterList),
}

impl SeriesSelector {
    pub fn parse(selector: &str) -> Result<Self, ParseError> {
        parse_series_selector(selector)
    }

    pub fn with_filters(matchers: Vec<LabelFilter>) -> Self {
        SeriesSelector::And(matchers.into())
    }

    /// The AND groups this selector is the union of. Every selector reads as an OR over its
    /// groups: `And(g)` is the one group `g`, and `Or(gs)` is `gs`. An empty group matches every
    /// series, and no groups at all (`Or([])`) match none.
    pub fn groups(&self) -> std::slice::Iter<'_, FilterList> {
        match self {
            SeriesSelector::And(group) => std::slice::from_ref(group).iter(),
            SeriesSelector::Or(groups) => groups.iter(),
        }
    }

    /// True if this selector on its own restricts the search to a bounded set of series,
    /// rather than requiring a full keyspace scan.
    ///
    /// A filter group is bounded when it holds at least one matcher that cannot be satisfied
    /// by a missing label — i.e. one that is neither negative (`l!=v`, `l!~re`) nor
    /// empty-matching (`l=""`, `l=~".*"`). Groups are unioned, so the selector is bounded only
    /// when *every* group is (and there is at least one); within a group filters are
    /// intersected, so one bounded matcher suffices.
    pub fn is_bounded(&self) -> bool {
        let mut groups = self.groups().peekable();
        groups.peek().is_some() && groups.all(filter_group_is_bounded)
    }

    /// True when the selector is exactly "every series named X": each group is a single
    /// equality on the same metric name.
    pub fn is_only_metric_name(&self) -> bool {
        self.get_metric_name().is_some() && self.groups().all(|group| group.len() == 1)
    }

    /// The metric name every series this selector matches must have, if there is exactly one.
    ///
    /// A name is returned only when it is a constraint, safe to plan with: every OR branch
    /// must pin the same name (see [`group_metric_name`]). `{__name__="cpu" or job="api"}` has
    /// none, because its second branch matches any metric.
    pub fn get_metric_name(&self) -> Option<&str> {
        let mut names = self.groups().map(FilterList::get_metric_name);
        let first = names.next()??;
        names.all(|name| name == Some(first)).then_some(first)
    }

    /// The selector matching what either `self` or `other` matches: their groups, in order, as
    /// one `Or`. (So two `And` selectors become an `Or` of two groups.) This is `a or b`.
    pub fn or_with(self, other: SeriesSelector) -> Self {
        match (self, other) {
            // Both are And selectors - create Or with both
            (SeriesSelector::And(left), SeriesSelector::And(right)) => {
                SeriesSelector::Or(vec![left, right])
            }
            // Left is And, right is Or - add left to right's Or list
            (SeriesSelector::And(left), SeriesSelector::Or(mut right)) => {
                right.insert(0, left);
                SeriesSelector::Or(right)
            }
            // Left is Or, right is And - add right to left's Or list
            (SeriesSelector::Or(mut left), SeriesSelector::And(right)) => {
                left.push(right);
                SeriesSelector::Or(left)
            }
            // Both are Or - combine the Or lists
            (SeriesSelector::Or(mut left), SeriesSelector::Or(mut right)) => {
                left.append(&mut right);
                SeriesSelector::Or(left)
            }
        }
    }
}

/// The selector as text the parser reads back as an equivalent selector; see
/// [`crate::labels::render`].
impl Display for SeriesSelector {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        render::write_selector(f, self)
    }
}

impl Default for SeriesSelector {
    fn default() -> Self {
        SeriesSelector::And(FilterList::default())
    }
}

impl From<Vec<LabelFilter>> for SeriesSelector {
    fn from(value: Vec<LabelFilter>) -> Self {
        SeriesSelector::And(FilterList::new(value))
    }
}

#[cfg(test)]
mod tests {
    use crate::labels::filters::{
        LabelFilter, MatchOp, PredicateMatch, PredicateValue, RegexMatcher,
        validate_contains_value, validate_starts_with_value,
    };

    #[test]
    fn test_match_anchoring() {
        // Test anchoring behavior - should match full string only
        let matcher = RegexMatcher::create("abc.*").unwrap();
        assert!(matcher.is_match("abc123"));
        assert!(!matcher.is_match("xabc123"));

        let matcher = RegexMatcher::create(".*xyz$").unwrap();
        assert!(matcher.is_match("123xyz"));
        assert!(!matcher.is_match("123xyzx"));

        let matcher = RegexMatcher::create("abc").unwrap();

        assert!(matcher.is_match("abc"));
        assert!(!matcher.is_match("xabc"));
        assert!(!matcher.is_match("abcx"));
    }

    #[test]
    fn test_regex_filter_rejects_trailing_backslash() {
        for op in [MatchOp::RegexEqual, MatchOp::RegexNotEqual] {
            for pattern in [r"foo\", r"foo\\\", r"\"] {
                assert!(
                    LabelFilter::create(op, "l", pattern).is_err(),
                    "{op} {pattern:?} should be an invalid regex"
                );
            }
            // An escaped backslash is a literal, not a dangling escape.
            assert!(LabelFilter::create(op, "l", r"foo\\").is_ok());
        }
    }

    #[test]
    fn test_regex_matcher_empty() {
        let matcher = RegexMatcher::create(".*").unwrap();
        assert!(matcher.is_match(""));
        assert!(matcher.is_match("anything"));

        let matcher = RegexMatcher::create("^$").unwrap();
        assert!(matcher.is_match(""));
        assert!(!matcher.is_match("not empty"));

        let matcher = RegexMatcher::create("^.*$").unwrap();
        assert!(matcher.is_match(""));
        assert!(matcher.is_match("anything"));

        let matcher = RegexMatcher::create("a*").unwrap();
        assert!(matcher.is_match(""));
        assert!(matcher.is_match("aaa"));
        assert!(!matcher.is_match("b"));
    }

    #[test]
    fn test_regex_matcher_non_empty() {
        let matcher = RegexMatcher::create("a+").unwrap();
        assert!(!matcher.is_match(""));
        assert!(matcher.is_match("a"));
        assert!(matcher.is_match("aaa"));
        assert!(!matcher.is_match("b"));

        let matcher = RegexMatcher::create("a{2,}").unwrap();
        assert!(!matcher.is_match(""));
        assert!(!matcher.is_match("a"));
        assert!(matcher.is_match("aa"));
        assert!(matcher.is_match("aaa"));
        assert!(!matcher.is_match("b"));

        let matcher = RegexMatcher::create("staging|prod").unwrap();
        assert!(!matcher.is_match(""));
        assert!(matcher.is_match("staging"));
        assert!(matcher.is_match("prod"));
        assert!(!matcher.is_match("dev"));
    }

    #[test]
    fn test_regex_matcher_with_prefix_does_not_match_empty_input() {
        let mut matcher = RegexMatcher::create(".*").unwrap();
        matcher.prefix = Some("server".to_string());

        assert!(!matcher.is_match(""));
        assert!(matcher.is_match("server"));
        assert!(matcher.is_match("server-1"));
    }

    #[test]
    fn test_regex_matcher_equality_includes_prefix_and_value() {
        let mut left = RegexMatcher::create("server.*").unwrap();
        left.prefix = Some("server".to_string());

        let mut equal = RegexMatcher::create("server.*").unwrap();
        equal.prefix = Some("server".to_string());
        assert_eq!(left, equal);

        let mut right = RegexMatcher::create("server.*").unwrap();
        right.prefix = Some("client".to_string());

        assert_ne!(left, right);

        let mut right = RegexMatcher::create("server.*").unwrap();
        right.prefix = Some("server".to_string());
        right.value = "server.+".to_string();

        assert_ne!(left, right);
    }

    #[test]
    fn test_regex_matcher_prefilters_ordered_literals_after_prefix() {
        let filter =
            LabelFilter::create(MatchOp::RegexEqual, "instance", "^server.*db.*prod$").unwrap();

        let PredicateMatch::RegexEqual(matcher) = filter.matcher else {
            panic!("expected regex matcher");
        };

        assert_eq!(matcher.prefix.as_deref(), Some("server"));
        assert_eq!(matcher.suffix.as_deref(), Some("prod"));
        assert_eq!(matcher.required_literals.as_slice(), ["db", "prod"]);
        assert!(matcher.is_match("server-east-db-primary-prod"));
        assert!(!matcher.is_match("server-east-prod-primary-db"));
    }

    #[test]
    fn test_regex_matcher_prefilters_ordered_literals_without_prefix() {
        let matcher = RegexMatcher::create(".*foo.*bar.*").unwrap();

        assert_eq!(matcher.suffix, None);
        assert_eq!(matcher.required_literals.as_slice(), ["foo", "bar"]);
        assert!(matcher.is_match("aaafoo123barzzz"));
        assert!(!matcher.is_match("aaabar123foozzz"));
    }

    #[test]
    fn test_regex_matcher_prefilters_anchored_suffix() {
        let filter = LabelFilter::create(MatchOp::RegexEqual, "instance", "^.*bar$").unwrap();

        let PredicateMatch::RegexEqual(matcher) = filter.matcher else {
            panic!("expected regex matcher");
        };

        assert_eq!(matcher.prefix, None);
        assert_eq!(matcher.suffix.as_deref(), Some("bar"));
        assert!(matcher.is_match("foo-bar"));
        assert!(!matcher.is_match("bar-foo"));
    }

    #[test]
    fn test_regex_matcher_prefilters_suffix_with_dot_plus_middle() {
        let filter = LabelFilter::create(MatchOp::RegexEqual, "instance", "^server.+db$").unwrap();

        let PredicateMatch::RegexEqual(matcher) = filter.matcher else {
            panic!("expected regex matcher");
        };

        assert_eq!(matcher.prefix.as_deref(), Some("server"));
        assert_eq!(matcher.suffix.as_deref(), Some("db"));
        assert!(matcher.is_match("server-east-db"));
        assert!(!matcher.is_match("serverdb"));
        assert!(!matcher.is_match("server-east-cache"));
    }

    #[test]
    fn test_contains_matcher_matches_substrings() {
        let matcher = PredicateMatch::Contains(PredicateValue::String("erv".to_string()));

        assert!(matcher.matches("server-1"));
        assert!(!matcher.matches("client-1"));
    }

    #[test]
    fn test_contains_matcher_matches_any_value_from_list() {
        let matcher = PredicateMatch::Contains(PredicateValue::from(vec![
            "erv".to_string(),
            "cli".to_string(),
        ]));

        assert!(matcher.matches("server-1"));
        assert!(matcher.matches("client-1"));
        assert!(!matcher.matches("proxy-1"));
    }

    #[test]
    fn test_contains_matcher_inverse_round_trip() {
        let matcher = PredicateMatch::Contains(PredicateValue::String("prod".to_string()));
        let inverted = matcher.clone().inverse();

        assert_eq!(
            inverted,
            PredicateMatch::NotContains(PredicateValue::String("prod".to_string()))
        );
        assert_eq!(inverted.inverse(), matcher);
    }

    #[test]
    fn test_label_filter_create_contains() {
        let filter = LabelFilter::create(MatchOp::Contains, "instance", "server").unwrap();

        assert_eq!(filter.op(), MatchOp::Contains);
        assert!(filter.matches("server-1"));
        assert!(!filter.matches("client-1"));
    }

    #[test]
    fn test_contains_matcher_rejects_empty_values() {
        assert!(LabelFilter::create(MatchOp::Contains, "instance", "").is_err());
        assert!(validate_contains_value(&PredicateValue::Empty).is_err());
        assert!(validate_contains_value(&PredicateValue::from(vec!["".to_string()])).is_err());
    }

    #[test]
    fn test_match_all_matches_any_input() {
        let matcher = PredicateMatch::MatchAll;

        assert!(matcher.matches(""));
        assert!(matcher.matches("server-1"));
        assert!(matcher.matches("client-1"));
        assert!(matcher.matches_empty());
    }

    #[test]
    fn test_match_all_inverse_rejects_everything() {
        let inverted = PredicateMatch::MatchAll.inverse();

        assert!(matches!(inverted, PredicateMatch::MatchNone));

        assert!(!inverted.matches(""));
        assert!(!inverted.matches("server-1"));
        assert!(!inverted.matches("client-1"));
    }

    #[test]
    fn test_match_none_inverse_matches_everything() {
        let inverted = PredicateMatch::MatchNone.inverse();

        assert!(matches!(inverted, PredicateMatch::MatchAll));
        assert!(inverted.matches(""));
        assert!(inverted.matches("server-1"));
        assert!(inverted.matches("client-1"));
    }

    #[test]
    fn test_label_filter_display_for_match_all() {
        let filter = LabelFilter {
            label: "instance".to_string(),
            matcher: PredicateMatch::MatchAll,
        };

        assert_eq!(filter.to_string(), r#"instance=~".*""#);
    }

    #[test]
    fn test_label_filter_display_for_match_none() {
        let filter = LabelFilter {
            label: "instance".to_string(),
            matcher: PredicateMatch::MatchNone,
        };

        assert_eq!(filter.to_string(), r#"instance!~".*""#);
    }

    #[test]
    fn test_starts_with_matcher_matches_any_prefix_from_list() {
        let matcher = PredicateMatch::StartsWith(PredicateValue::from(vec![
            "server".to_string(),
            "client".to_string(),
        ]));

        assert!(matcher.matches("server-1"));
        assert!(matcher.matches("client-1"));
        assert!(!matcher.matches("proxy-1"));
    }

    #[test]
    fn test_not_starts_with_matcher_requires_all_prefixes_absent() {
        let matcher = PredicateMatch::NotStartsWith(PredicateValue::from(vec![
            "server".to_string(),
            "client".to_string(),
        ]));

        assert!(!matcher.matches("server-1"));
        assert!(!matcher.matches("client-1"));
        assert!(matcher.matches("proxy-1"));
    }

    #[test]
    fn test_label_filter_create_starts_with_list() {
        let filter = LabelFilter::create_with_value(
            MatchOp::StartsWith,
            "instance",
            PredicateValue::from(vec!["server".to_string(), "client".to_string()]),
        )
        .unwrap();

        assert_eq!(filter.op(), MatchOp::StartsWith);
        assert!(filter.matches("server-1"));
        assert!(filter.matches("client-1"));
        assert!(!filter.matches("proxy-1"));
    }

    #[test]
    fn test_starts_with_matcher_rejects_empty_values() {
        assert!(LabelFilter::create(MatchOp::StartsWith, "instance", "").is_err());
        assert!(validate_starts_with_value(&PredicateValue::Empty).is_err());
        assert!(validate_starts_with_value(&PredicateValue::from(vec!["".to_string()])).is_err());
        assert!(
            validate_starts_with_value(&PredicateValue::from(vec![
                "server".to_string(),
                "".to_string(),
            ]))
            .is_err()
        );
    }

    #[test]
    fn test_predicate_value_list_matches_empty_alternative() {
        let with_empty = PredicateValue::from(vec!["foo".to_string(), String::new()]);
        assert!(with_empty.matches(""));
        assert!(with_empty.matches("foo"));
        assert!(!with_empty.matches("bar"));

        let without_empty = PredicateValue::from(vec!["foo".to_string(), "bar".to_string()]);
        assert!(!without_empty.matches(""));

        assert!(PredicateValue::from(Vec::<String>::new()).matches(""));

        let filter = LabelFilter::create(MatchOp::RegexEqual, "l", "foo|").unwrap();
        assert!(filter.matches_empty());
        let filter = LabelFilter::create(MatchOp::RegexNotEqual, "l", "foo|").unwrap();
        assert!(!filter.matches_empty());
    }

    #[test]
    fn test_metric_name_is_reported_only_when_it_constrains_every_match() {
        use crate::labels::filters::SeriesSelector;
        let name = |selector: &str| {
            let parsed = SeriesSelector::parse(selector).unwrap();
            (
                parsed.get_metric_name().map(str::to_string),
                parsed.is_only_metric_name(),
            )
        };
        let some = |n: &str, only: bool| (Some(n.to_string()), only);
        let none = (None, false);

        assert_eq!(name("cpu"), some("cpu", true));
        assert_eq!(name(r#"cpu{job="a"}"#), some("cpu", false));
        assert_eq!(name(r#"{__name__="cpu"}"#), some("cpu", true));
        // Every branch pins the same name.
        assert_eq!(
            name(r#"{__name__="cpu" or __name__="cpu"}"#),
            some("cpu", true)
        );
        assert_eq!(
            name(r#"{__name__="cpu" or __name__="cpu",job="a"}"#),
            some("cpu", false)
        );
        // A branch that leaves the name open, or names another, gives no name.
        assert_eq!(name(r#"{__name__="cpu" or job="api"}"#), none);
        assert_eq!(name(r#"{__name__="cpu" or __name__="mem"}"#), none);
        // A list of names is not one name.
        assert_eq!(name(r#"{__name__=("cpu","mem")}"#), none);
        // Other `__name__` matchers must accept the name; a contradiction matches nothing.
        assert_eq!(
            name(r#"{__name__=~"c.*",__name__="cpu"}"#),
            some("cpu", false)
        );
        assert_eq!(name(r#"{__name__=~"m.*",__name__="cpu"}"#), none);
        assert_eq!(name(r#"{__name__!="cpu",__name__="cpu"}"#), none);
        assert_eq!(name(r#"{job="a"}"#), none);
    }

    #[test]
    fn test_absent_and_present_operands() {
        let list = |items: &[&str]| {
            PredicateValue::from(items.iter().map(|s| s.to_string()).collect::<Vec<_>>())
        };
        // (operand, is_absent, requires_present)
        let cases = [
            (PredicateValue::Empty, true, false),
            (PredicateValue::from(""), true, false),
            (list(&[]), true, false),
            (list(&["", ""]), true, false),
            (PredicateValue::from("a"), false, true),
            (list(&["a", "b"]), false, true),
            // A mix is neither: it matches the absent value and some present ones.
            (list(&["a", ""]), false, false),
        ];
        for (operand, absent, present) in cases {
            assert_eq!(operand.is_absent(), absent, "{operand:?}");
            assert_eq!(operand.requires_present(), present, "{operand:?}");
            // An absent operand is exactly one that matches nothing but "".
            if absent {
                assert!(operand.matches(""));
                assert!(!operand.matches("a"));
            }
        }
    }

    /// The negative prefix and substring matchers are the exact complements of the positive
    /// ones, for any operand. They used to re-check the operand and return false for an empty
    /// needle or an empty list, so `!*=` and `^~` rejected values `*=` and `^=` also rejected.
    #[test]
    fn test_negative_needle_matchers_are_complements() {
        let operands = [
            PredicateValue::Empty,
            PredicateValue::from(""),
            PredicateValue::from(vec![]),
            PredicateValue::from(vec![String::new()]),
            PredicateValue::from(vec!["ser".to_string(), String::new()]),
            PredicateValue::from("ser"),
            PredicateValue::from(vec!["ser".to_string(), "cli".to_string()]),
        ];
        for operand in operands {
            for (positive, negative) in [
                (
                    PredicateMatch::StartsWith(operand.clone()),
                    PredicateMatch::NotStartsWith(operand.clone()),
                ),
                (
                    PredicateMatch::Contains(operand.clone()),
                    PredicateMatch::NotContains(operand.clone()),
                ),
            ] {
                for value in ["", "server", "client", "a-ser", "proxy"] {
                    assert_ne!(
                        positive.matches(value),
                        negative.matches(value),
                        "{positive:?} / {negative:?} on {value:?}"
                    );
                }
                assert_eq!(positive.validate().is_ok(), operand.requires_present());
            }
        }
    }

    #[test]
    fn test_selector_groups_and_or_with() {
        use crate::labels::filters::{FilterList, SeriesSelector};
        let group =
            |label: &str| FilterList::from(vec![LabelFilter::equals(label.to_string(), "1")]);
        let labels = |selector: &SeriesSelector| {
            selector
                .groups()
                .map(|g| g.iter().map(|f| f.label.clone()).collect::<Vec<_>>())
                .collect::<Vec<_>>()
        };

        let a = SeriesSelector::And(group("a"));
        assert_eq!(labels(&a), [["a"]]);
        let empty_or = SeriesSelector::Or(vec![]);
        assert_eq!(empty_or.groups().count(), 0);

        // `or_with` keeps every group, in order; two ANDs become an OR of two.
        let ab = a.clone().or_with(SeriesSelector::And(group("b")));
        assert!(matches!(ab, SeriesSelector::Or(_)));
        assert_eq!(labels(&ab), [["a"], ["b"]]);
        let cd = SeriesSelector::Or(vec![group("c"), group("d")]);
        assert_eq!(
            labels(&ab.clone().or_with(cd.clone())),
            [["a"], ["b"], ["c"], ["d"]]
        );
        assert_eq!(labels(&cd.or_with(a)), [["c"], ["d"], ["a"]]);

        // An empty group matches everything and no groups match nothing; neither is bounded.
        assert!(!SeriesSelector::And(FilterList::default()).is_bounded());
        assert!(!empty_or.is_bounded());
        assert!(ab.is_bounded());
    }
}
