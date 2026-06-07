use crate::common::constants::METRIC_NAME_LABEL;
use crate::labels::parse_series_selector;
use crate::labels::regex::parse_regex_anchored;
use crate::labels::regex_utils::{
    extract_ordered_required_literals, extract_stable_suffix_hint, parse_regex_matcher,
};
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
const MATCH_ALL_REGEX_TEXT: &str = ".*";

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
    MatchAll,
    MatchNone,
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
            MatchOp::MatchAll => write!(f, "*="),
            MatchOp::MatchNone => write!(f, "!~"), // TODO
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
            Token::StartsWith => Ok(MatchOp::StartsWith),
            Token::NotStartsWith => Ok(MatchOp::NotStartsWith),
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

    pub fn matches_contains_any(&self, value: &str) -> bool {
        match self {
            PredicateValue::Empty => false,
            PredicateValue::String(s) => !s.is_empty() && value.contains(s),
            PredicateValue::List(list) => {
                !list.is_empty()
                    && list.iter().all(|needle| !needle.is_empty())
                    && list.iter().any(|needle| value.contains(needle.as_str()))
            }
        }
    }

    pub fn matches_not_contains_all(&self, value: &str) -> bool {
        match self {
            PredicateValue::Empty => false,
            PredicateValue::String(s) => !s.is_empty() && !value.contains(s),
            PredicateValue::List(list) => {
                !list.is_empty()
                    && list.iter().all(|needle| !needle.is_empty())
                    && list.iter().all(|needle| !value.contains(needle.as_str()))
            }
        }
    }

    pub fn matches_starts_with_any(&self, value: &str) -> bool {
        match self {
            PredicateValue::Empty => false,
            PredicateValue::String(prefix) => !prefix.is_empty() && value.starts_with(prefix),
            PredicateValue::List(list) => {
                !list.is_empty()
                    && list.iter().all(|prefix| !prefix.is_empty())
                    && list.iter().any(|prefix| value.starts_with(prefix.as_str()))
            }
        }
    }

    pub fn matches_not_starts_with_all(&self, value: &str) -> bool {
        match self {
            PredicateValue::Empty => false,
            PredicateValue::String(prefix) => !prefix.is_empty() && !value.starts_with(prefix),
            PredicateValue::List(list) => {
                !list.is_empty()
                    && list.iter().all(|prefix| !prefix.is_empty())
                    && list
                        .iter()
                        .all(|prefix| !value.starts_with(prefix.as_str()))
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

pub(crate) fn validate_contains_value(value: &PredicateValue) -> Result<(), ParseError> {
    let is_valid = match value {
        PredicateValue::Empty => false,
        PredicateValue::String(s) => !s.is_empty(),
        PredicateValue::List(list) => {
            !list.is_empty() && list.iter().all(|needle| !needle.is_empty())
        }
    };

    if is_valid {
        Ok(())
    } else {
        Err(ParseError::General(
            "contains matcher does not allow empty values".to_string(),
        ))
    }
}

pub(crate) fn validate_starts_with_value(value: &PredicateValue) -> Result<(), ParseError> {
    let is_valid = match value {
        PredicateValue::Empty => false,
        PredicateValue::String(prefix) => !prefix.is_empty(),
        PredicateValue::List(list) => {
            !list.is_empty() && list.iter().all(|prefix| !prefix.is_empty())
        }
    };

    if is_valid {
        Ok(())
    } else {
        Err(ParseError::General(
            "starts with matcher does not allow empty values".to_string(),
        ))
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
        self.prefix.hash(state);
        self.suffix.hash(state);
        self.required_literals.hash(state);
        self.value.hash(state);
    }
}

impl RegexMatcher {
    pub(crate) fn new(regex: Regex, value: String) -> Self {
        Self::from_parts(regex, value, None)
    }

    pub(crate) fn from_parts(regex: Regex, value: String, prefix: Option<String>) -> Self {
        let required_literals = extract_ordered_required_literals(&value, prefix.as_deref()).into();
        let suffix = extract_stable_suffix_hint(&value, prefix.as_deref());
        Self {
            regex,
            value,
            prefix,
            suffix,
            required_literals,
        }
    }

    pub fn create(value: &str) -> Result<Self, ParseError> {
        let (regex, unanchored) = parse_regex_anchored(value)?;
        Ok(Self::new(regex, unanchored.to_string()))
    }

    pub fn is_match(&self, other: &str) -> bool {
        if other.is_empty() {
            if self
                .prefix
                .as_ref()
                .is_some_and(|prefix| !prefix.is_empty())
            {
                return false;
            }
            return is_empty_regex_matcher(&self.regex);
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

impl Display for RegexMatcher {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", enquote('"', &self.value))
    }
}

impl Eq for RegexMatcher {}

impl PartialEq for RegexMatcher {
    fn eq(&self, other: &Self) -> bool {
        self.regex.as_str() == other.regex.as_str()
            && self.prefix == other.prefix
            && self.suffix == other.suffix
            && self.required_literals == other.required_literals
            && self.value == other.value
    }
}

#[derive(Clone, Debug)]
pub enum PredicateMatch {
    Equal(PredicateValue),
    NotEqual(PredicateValue),
    MatchAll,
    MatchNone,
    RegexEqual(RegexMatcher),
    RegexNotEqual(RegexMatcher),
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
            PredicateMatch::MatchAll => MatchOp::RegexEqual,
            PredicateMatch::MatchNone => MatchOp::MatchNone,
            PredicateMatch::RegexEqual(_) => MatchOp::RegexEqual,
            PredicateMatch::RegexNotEqual(_) => MatchOp::RegexNotEqual,
            PredicateMatch::StartsWith(_) => MatchOp::StartsWith,
            PredicateMatch::NotStartsWith(_) => MatchOp::NotStartsWith,
            PredicateMatch::Contains(_) => MatchOp::Contains,
            PredicateMatch::NotContains(_) => MatchOp::NotContains,
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            PredicateMatch::Equal(value) | PredicateMatch::NotEqual(value) => value.is_empty(),
            PredicateMatch::MatchAll => false,
            PredicateMatch::MatchNone => false,
            PredicateMatch::RegexEqual(re) | PredicateMatch::RegexNotEqual(re) => {
                re.regex.as_str().is_empty()
            }
            PredicateMatch::StartsWith(value) => value.is_empty(),
            PredicateMatch::NotStartsWith(value) => value.is_empty(),
            PredicateMatch::Contains(value) => value.is_empty(),
            PredicateMatch::NotContains(value) => value.is_empty(),
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
            PredicateMatch::NotStartsWith(value) => value.matches_not_starts_with_all(other),
            PredicateMatch::Contains(value) => value.matches_contains_any(other),
            PredicateMatch::NotContains(value) => value.matches_not_contains_all(other),
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

    fn text(&self) -> Option<&str> {
        match self {
            PredicateMatch::Equal(value) | PredicateMatch::NotEqual(value) => value.text(),
            PredicateMatch::MatchAll => Some(MATCH_ALL_REGEX_TEXT),
            PredicateMatch::MatchNone => Some(MATCH_ALL_REGEX_TEXT),
            PredicateMatch::RegexEqual(re) | PredicateMatch::RegexNotEqual(re) => Some(&re.value),
            PredicateMatch::StartsWith(value) => value.text(),
            PredicateMatch::NotStartsWith(value) => value.text(),
            PredicateMatch::Contains(value) => value.text(),
            PredicateMatch::NotContains(value) => value.text(),
        }
    }
}

impl Display for PredicateMatch {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let value = match self {
            PredicateMatch::Equal(v) | PredicateMatch::NotEqual(v) => v as &dyn Display,
            PredicateMatch::MatchAll => &MATCH_ALL_REGEX_TEXT as &dyn Display,
            PredicateMatch::MatchNone => {
                return write!(f, "!{MATCH_ALL_REGEX_TEXT}");
            }
            PredicateMatch::RegexEqual(re) | PredicateMatch::RegexNotEqual(re) => {
                re as &dyn Display
            }
            PredicateMatch::StartsWith(value) => value as &dyn Display,
            PredicateMatch::NotStartsWith(value) => value as &dyn Display,
            PredicateMatch::Contains(value) => value as &dyn Display,
            PredicateMatch::NotContains(value) => value as &dyn Display,
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
            PredicateMatch::MatchAll => {
                state.write_u8(3);
            }
            PredicateMatch::MatchNone => {
                state.write_u8(10);
            }
            PredicateMatch::RegexEqual(re) => {
                state.write_u8(4);
                re.hash(state);
            }
            PredicateMatch::RegexNotEqual(re) => {
                state.write_u8(5);
                re.hash(state);
            }
            PredicateMatch::StartsWith(p) => {
                state.write_u8(6);
                p.hash(state);
            }
            PredicateMatch::NotStartsWith(p) => {
                state.write_u8(7);
                p.hash(state);
            }
            PredicateMatch::Contains(value) => {
                state.write_u8(8);
                value.hash(state);
            }
            PredicateMatch::NotContains(value) => {
                state.write_u8(9);
                value.hash(state);
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
        let label = label.into();

        match match_op {
            MatchOp::Equal => Ok(Self {
                label,
                matcher: PredicateMatch::Equal(value),
            }),
            MatchOp::NotEqual => Ok(Self {
                label,
                matcher: PredicateMatch::NotEqual(value),
            }),
            MatchOp::RegexEqual => {
                let Some(value) = value.text() else {
                    return Err(ParseError::General(
                        "regex matcher requires a single value".to_string(),
                    ));
                };
                let matcher = parse_regex_matcher(value, true)?;
                Ok(Self { label, matcher })
            }
            MatchOp::RegexNotEqual => {
                let Some(value) = value.text() else {
                    return Err(ParseError::General(
                        "regex matcher requires a single value".to_string(),
                    ));
                };
                let matcher = parse_regex_matcher(value, false)?;
                Ok(Self { label, matcher })
            }
            MatchOp::StartsWith => {
                validate_starts_with_value(&value)?;
                Ok(Self {
                    label,
                    matcher: PredicateMatch::StartsWith(value),
                })
            }
            MatchOp::NotStartsWith => {
                validate_starts_with_value(&value)?;
                Ok(Self {
                    label,
                    matcher: PredicateMatch::NotStartsWith(value),
                })
            }
            MatchOp::Contains => {
                validate_contains_value(&value)?;
                Ok(Self {
                    label,
                    matcher: PredicateMatch::Contains(value),
                })
            }
            MatchOp::NotContains => {
                validate_contains_value(&value)?;
                Ok(Self {
                    label,
                    matcher: PredicateMatch::NotContains(value),
                })
            }
            MatchOp::MatchAll => Ok(Self {
                label,
                matcher: PredicateMatch::MatchAll,
            }),
            MatchOp::MatchNone => Ok(Self {
                label,
                matcher: PredicateMatch::MatchNone,
            }),
        }
    }

    pub fn equals(label: String, value: &str) -> Self {
        Self {
            label,
            matcher: PredicateMatch::Equal(value.into()),
        }
    }

    pub fn prefix(label: String, value: String) -> Self {
        Self {
            label,
            matcher: PredicateMatch::StartsWith(PredicateValue::String(value)),
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
            PredicateMatch::MatchAll => Some(MATCH_ALL_REGEX_TEXT),
            PredicateMatch::MatchNone => Some(MATCH_ALL_REGEX_TEXT),
            _ => None,
        }
    }

    pub fn matches_empty(&self) -> bool {
        self.matches("")
    }

    #[inline]
    pub fn is_negative_matcher(&self) -> bool {
        matches!(
            self.op(),
            MatchOp::NotEqual
                | MatchOp::RegexNotEqual
                | MatchOp::NotStartsWith
                | MatchOp::NotContains
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
            PredicateMatch::MatchAll => write!(f, "{}", enquote('"', MATCH_ALL_REGEX_TEXT)),
            PredicateMatch::MatchNone => write!(f, "{}", enquote('"', MATCH_ALL_REGEX_TEXT)),
            PredicateMatch::RegexEqual(re) | PredicateMatch::RegexNotEqual(re) => {
                write!(f, "{}", enquote('"', &re.value))
            }
            PredicateMatch::StartsWith(value) | PredicateMatch::NotStartsWith(value) => {
                write!(f, "{value}")
            }
            PredicateMatch::Contains(value) | PredicateMatch::NotContains(value) => {
                write!(f, "{value}")
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
}