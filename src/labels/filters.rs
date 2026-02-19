use crate::common::constants::METRIC_NAME_LABEL;
use crate::labels::parse_series_selector;
use crate::labels::regex::parse_regex_anchored;
use crate::parser::ParseError;
use crate::parser::lex::Token;
use enquote::enquote;
use regex::Regex;
use smallvec::SmallVec;
use std::cmp::PartialEq;
use std::default::Default;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::ops::{Deref, DerefMut};

const EMPTY_TEXT: &str = "";

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum MatchOp {
    Equal,
    NotEqual,
    RegexEqual,
    RegexNotEqual,
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
        }
    }
}

impl TryFrom<Token> for MatchOp {
    type Error = ParseError;

    fn try_from(value: Token) -> Result<Self, Self::Error> {
        match value {
            Token::Equal => Ok(MatchOp::Equal),
            Token::OpNotEqual => Ok(MatchOp::NotEqual),
            Token::RegexEqual => Ok(MatchOp::RegexEqual),
            Token::RegexNotEqual => Ok(MatchOp::RegexNotEqual),
            _ => Err(ParseError::InvalidMatchOperator(value.to_string())),
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
            PredicateValue::String(s) => write!(f, "{}", enquote('"', s)),
            PredicateValue::List(values) => {
                write!(f, "(")?;
                for (i, value) in values.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", enquote('"', value))?;
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
            PredicateValue::List(list) => {
                if value.is_empty() {
                    return list.is_empty();
                }
                list.iter().any(|x| x.as_str() == value)
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

    pub fn is_empty(&self) -> bool {
        match self {
            PredicateValue::List(list) => list.is_empty(),
            PredicateValue::String(value) => value.is_empty(),
            PredicateValue::Empty => true,
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

impl Hash for PredicateValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            PredicateValue::Empty => state.write_u8(1),
            PredicateValue::String(s) => {
                state.write_u8(2);
                s.hash(state);
            }
            PredicateValue::List(values) => {
                state.write_u8(3);
                for value in values {
                    value.hash(state);
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct RegexMatcher {
    pub regex: Regex,
    pub prefix: Option<String>,
    pub suffix: Option<String>,
    pub required_literals: SmallVec<[String; 2]>,
    pub value: String, // original (possibly unanchored) string
}

impl Hash for RegexMatcher {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.regex.as_str().hash(state);
        self.value.hash(state);
    }
}

impl RegexMatcher {
    pub(crate) fn new(regex: Regex, value: String) -> Self {
        Self { regex, value }
    }

    pub fn create(value: &str) -> Result<Self, ParseError> {
        let (regex, unanchored) = parse_regex_anchored(value)?;
        Ok(Self::new(regex, unanchored.to_string()))
    }

    pub fn is_match(&self, other: &str) -> bool {
        if other.is_empty() {
            return is_empty_regex_matcher(&self.regex);
        }
        self.regex.is_match(other)
    }
}

impl TryFrom<&str> for RegexMatcher {
    type Error = ParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::create(value)
    }
}

impl Display for RegexMatcher {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", enquote('"', &self.value))
    }
}

impl Eq for RegexMatcher {}

impl PartialEq for RegexMatcher {
    fn eq(&self, other: &Self) -> bool {
        self.regex.as_str() == other.regex.as_str()
    }
}

#[derive(Clone, Debug)]
pub enum PredicateMatch {
    Equal(PredicateValue),
    NotEqual(PredicateValue),
    RegexEqual(RegexMatcher),
    RegexNotEqual(RegexMatcher),
}

impl PredicateMatch {
    pub fn op(&self) -> MatchOp {
        match self {
            PredicateMatch::Equal(_) => MatchOp::Equal,
            PredicateMatch::NotEqual(_) => MatchOp::NotEqual,
            PredicateMatch::RegexEqual(_) => MatchOp::RegexEqual,
            PredicateMatch::RegexNotEqual(_) => MatchOp::RegexNotEqual,
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            PredicateMatch::Equal(value) | PredicateMatch::NotEqual(value) => value.is_empty(),
            PredicateMatch::RegexEqual(re) | PredicateMatch::RegexNotEqual(re) => {
                re.regex.as_str().is_empty()
            }
        }
    }

    pub fn matches_empty(&self) -> bool {
        self.matches("")
    }

    pub fn matches(&self, other: &str) -> bool {
        match self {
            PredicateMatch::Equal(value) => value.matches(other),
            PredicateMatch::NotEqual(value) => !value.matches(other),
            PredicateMatch::RegexEqual(re) => re.is_match(other),
            PredicateMatch::RegexNotEqual(re) => !re.is_match(other),
        }
    }

    pub fn inverse(self) -> Self {
        match self {
            PredicateMatch::Equal(value) => PredicateMatch::NotEqual(value),
            PredicateMatch::NotEqual(value) => PredicateMatch::Equal(value),
            PredicateMatch::RegexEqual(re) => PredicateMatch::RegexNotEqual(re),
            PredicateMatch::RegexNotEqual(re) => PredicateMatch::RegexEqual(re),
        }
    }

    pub(crate) fn cost(&self) -> usize {
        match &self {
            PredicateMatch::Equal(value) => value.cost(),
            PredicateMatch::NotEqual(value) => value.cost(),
            PredicateMatch::RegexEqual(_) => 50,
            PredicateMatch::RegexNotEqual(_) => 55,
        }
    }

    fn text(&self) -> Option<&str> {
        match self {
            PredicateMatch::Equal(value) | PredicateMatch::NotEqual(value) => value.text(),
            PredicateMatch::RegexEqual(re) | PredicateMatch::RegexNotEqual(re) => Some(&re.value),
        }
    }
}

impl Display for PredicateMatch {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let value = match self {
            PredicateMatch::Equal(v) | PredicateMatch::NotEqual(v) => v as &dyn Display,
            PredicateMatch::RegexEqual(re) | PredicateMatch::RegexNotEqual(re) => {
                re as &dyn Display
            }
        };

        write!(f, "{value}")
    }
}

impl Hash for PredicateMatch {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            PredicateMatch::Equal(value) => {
                state.write_u8(1);
                value.hash(state);
            }
            PredicateMatch::NotEqual(value) => {
                state.write_u8(2);
                value.hash(state);
            }
            PredicateMatch::RegexEqual(re) => {
                state.write_u8(3);
                re.hash(state);
            }
            PredicateMatch::RegexNotEqual(re) => {
                state.write_u8(4);
                re.hash(state);
            }
        }
    }
}

impl Eq for PredicateMatch {}

impl PartialEq for PredicateMatch {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (PredicateMatch::Equal(v1), PredicateMatch::Equal(v2)) => v1 == v2,
            (PredicateMatch::NotEqual(v1), PredicateMatch::NotEqual(v2)) => v1 == v2,
            (PredicateMatch::RegexEqual(re1), PredicateMatch::RegexEqual(re2)) => re1 == re2,
            (PredicateMatch::RegexNotEqual(re1), PredicateMatch::RegexNotEqual(re2)) => re1 == re2,
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
        let label = label.into();
        let value = value.into();

        match match_op {
            MatchOp::Equal => Ok(Self::equals(label, &value)),
            MatchOp::NotEqual => Ok(Self::not_equals(label, &value)),
            MatchOp::RegexEqual => {
                let re = RegexMatcher::create(&value)?;
                Ok(Self {
                    label,
                    matcher: PredicateMatch::RegexEqual(re),
                })
            }
            MatchOp::RegexNotEqual => {
                let re = RegexMatcher::create(&value)?;
                Ok(Self {
                    label,
                    matcher: PredicateMatch::RegexNotEqual(re),
                })
            }
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

    pub fn regex_text(&self) -> Option<&str> {
        match &self.matcher {
            PredicateMatch::RegexEqual(re) | PredicateMatch::RegexNotEqual(re) => Some(&re.value),
            _ => None,
        }
    }

    pub fn matches_empty(&self) -> bool {
        self.matches("")
    }

    #[inline]
    pub fn is_negative_matcher(&self) -> bool {
        matches!(self.op(), MatchOp::NotEqual | MatchOp::RegexNotEqual)
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

    fn text(&self) -> Option<&str> {
        self.matcher.text()
    }
}

impl Display for LabelFilter {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}{}", self.label, self.op())?;
        match &self.matcher {
            PredicateMatch::Equal(value) | PredicateMatch::NotEqual(value) => {
                write!(f, "{value}")
            }
            PredicateMatch::RegexEqual(re) | PredicateMatch::RegexNotEqual(re) => {
                write!(f, "{}", enquote('"', &re.value))
            }
        }
    }
}

impl Hash for LabelFilter {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.label.hash(state);
        self.matcher.hash(state);
    }
}

fn is_empty_regex_matcher(re: &Regex) -> bool {
    // cheap check
    let value = re.as_str();
    let matches_empty = match value.len() {
        0 => true,
        1 => value == "^" || value == "$",
        2 => value == ".*" || value == "^$" || value == "?:",
        3 => value == "^.*" || value == ".*$",
        4 => value == "^.*$" || value == "^?:$",
        _ => false,
    };
    matches_empty || re.is_match("")
}

fn get_metric_name(filters: &[LabelFilter]) -> Option<&str> {
    filters
        .iter()
        .find(|m| m.is_metric_name_filter())
        .and_then(|m| m.text())
}

/// FilterList is a small vector of LabelFilter, used for AND combinations of label filters.
/// We try to minimize allocations by reserving stack space for 3 LabelFilter, which seems reasonable for the common
/// case. e.g. http_requests_total{job="api",method="GET"}
#[derive(Debug, Default, Clone, Hash, PartialEq)]
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

    pub fn is_only_metric_name(&self) -> bool {
        if self.0.len() == 1 {
            let first = &self.0[0];
            if first.is_metric_name_filter() {
                return true;
            }
        }
        false
    }

    pub fn get_metric_name(&self) -> Option<&str> {
        get_metric_name(&self.0)
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

impl Display for FilterList {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        join_matchers(f, &self.0)
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

impl From<SmallVec<LabelFilter, 3>> for FilterList {
    fn from(value: SmallVec<LabelFilter, 3>) -> Self {
        Self(value)
    }
}

/// OrFiltersList is a small vector of FilterList, used for OR combinations of AND filters.
pub type OrFiltersList = SmallVec<[FilterList; 2]>;

/// See [`SeriesSelector::is_bounded`]. An empty group matches every series, so it is unbounded.
fn filter_group_is_bounded(filters: &FilterList) -> bool {
    filters
        .iter()
        .any(|f| !f.is_negative_matcher() && !f.matches_empty())
}

#[derive(Debug, Clone, Hash, PartialEq)]
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

    /// True if this selector on its own restricts the search to a bounded set of series,
    /// rather than requiring a full keyspace scan.
    ///
    /// A filter group is bounded when it holds at least one matcher that cannot be satisfied
    /// by a missing label — i.e. one that is neither negative (`l!=v`, `l!~re`) nor
    /// empty-matching (`l=""`, `l=~".*"`). `Or` branches are unioned, so an `Or` selector is
    /// bounded only when *every* branch is; `And` filters are intersected, so one bounded
    /// matcher anywhere in the group suffices.
    pub fn is_bounded(&self) -> bool {
        match self {
            SeriesSelector::And(filters) => filter_group_is_bounded(filters),
            SeriesSelector::Or(groups) => {
                !groups.is_empty() && groups.iter().all(filter_group_is_bounded)
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            SeriesSelector::Or(or_matchers) => or_matchers.len(),
            SeriesSelector::And(and_matchers) => and_matchers.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            SeriesSelector::Or(or_matchers) => or_matchers.is_empty(),
            SeriesSelector::And(and_matchers) => and_matchers.is_empty(),
        }
    }

    pub fn is_only_metric_name(&self) -> bool {
        match self {
            SeriesSelector::Or(_) => false,
            SeriesSelector::And(and_matchers) => {
                if and_matchers.len() == 1 {
                    let first = &and_matchers.0[0];
                    if first.is_metric_name_filter() {
                        return true;
                    }
                }
                false
            }
        }
    }

    pub fn get_metric_name(&self) -> Option<&str> {
        match self {
            SeriesSelector::Or(or_matchers) => {
                let mut name: Option<&str> = None;
                for and_matchers in or_matchers.iter() {
                    if let Some(current_name) = get_metric_name(and_matchers) {
                        if name.is_some() && name != Some(current_name) {
                            // Multiple different names found
                            return None;
                        }
                        name = Some(current_name);
                    }
                }
                name
            }
            SeriesSelector::And(and_matchers) => get_metric_name(and_matchers),
        }
    }

    /// Merges two SeriesSelector instances into one.
    ///
    /// The merge logic follows these rules:
    /// - And + And: Creates an Or selector with both filter lists
    /// - And + Or: Adds the And filters to the Or list
    /// - Or + And: Adds the And filters to the Or list
    /// - Or + Or: Combines both Or lists
    pub fn merge_with(self, other: SeriesSelector) -> Self {
        match (self, other) {
            // Both are And selectors - create Or with both
            (SeriesSelector::And(left), SeriesSelector::And(right)) => {
                let mut or_list = OrFiltersList::new();
                or_list.push(left);
                or_list.push(right);
                SeriesSelector::Or(or_list)
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

impl Display for SeriesSelector {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match &self {
            SeriesSelector::Or(or_matchers) => {
                for (i, matchers) in or_matchers.iter().enumerate() {
                    if i > 0 {
                        write!(f, " or ")?;
                    }
                    write!(f, "{matchers}")?;
                }
                Ok(())
            }
            SeriesSelector::And(and_matchers) => write!(f, "{and_matchers}"),
        }
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
impl From<Vec<Vec<LabelFilter>>> for SeriesSelector {
    fn from(value: Vec<Vec<LabelFilter>>) -> Self {
        if value.len() == 1 {
            let first = value.into_iter().next().expect("value is not empty");
            return SeriesSelector::And(FilterList::new(first));
        }
        let and_matchers: Vec<FilterList> = value.into_iter().map(FilterList::new).collect();
        SeriesSelector::Or(and_matchers.into())
    }
}

fn join_matchers(f: &mut Formatter<'_>, v: &[LabelFilter]) -> fmt::Result {
    // Find the metric name matcher (single __name__=value filter)
    let metric_name_info = find_metric_name_matcher(v);

    // If there's only a metric name filter, output it directly
    if let Some((_, name)) = metric_name_info
        && v.len() == 1
    {
        return write!(f, "{name}");
    }

    // Write metric name (if any) followed by labels in braces
    let name = metric_name_info.map(|(_, n)| n).unwrap_or("");
    write!(f, "{name}{{")?;

    let mut first = true;
    for (i, matcher) in v.iter().enumerate() {
        // Skip the metric name matcher since we already wrote it
        if metric_name_info.map(|(idx, _)| idx == i).unwrap_or(false) {
            continue;
        }

        if !first {
            write!(f, ",")?;
        }
        write!(f, "{matcher}")?;
        first = false;
    }

    write!(f, "}}")
}

/// Finds the single __name__=value matcher in the filter list.
/// Returns None if there are zero or multiple __name__ matchers.
fn find_metric_name_matcher(filters: &[LabelFilter]) -> Option<(usize, &str)> {
    let mut result: Option<(usize, &str)> = None;

    for (i, matcher) in filters.iter().enumerate() {
        if matcher.label == METRIC_NAME_LABEL {
            // Multiple __name__ matchers found - invalid
            if result.is_some() {
                return None;
            }

            // Only Equal operations define a metric name
            if matcher.matcher.op() == MatchOp::Equal
                && let Some(name) = matcher.text()
            {
                result = Some((i, name));
            }
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use crate::labels::filters::RegexMatcher;

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
}
