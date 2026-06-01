//! Optimize matches against the postings index by decomposing regexes into simpler components when possible.
//! For example, `^prod.*` can be decomposed into a simple prefix match for `prod`, and `foo|bar|baz` can be
//! decomposed into a set of literal alternatives. More complex patterns that cannot be simplified will be
//! returned as-is for full regex matching.
use crate::labels::filters::{
    PredicateMatch, PredicateValue, RegexMatcher, validate_contains_value,
    validate_starts_with_value,
};
use crate::labels::regex::build_with_repeat_fallback;
use crate::parser::{ParseError, ParseResult};
use regex::Error as RegexError;
use regex::Regex;
use regex_syntax::hir::Class::{Bytes, Unicode};
use regex_syntax::hir::{Class, Hir, HirKind, Look};
use regex_syntax::parse as parse_regex;

// Beyond this, it's better to use regexp.
const MAX_OR_VALUES: usize = 16;

pub(crate) fn parse_regex_matcher(expr: &str, is_equal: bool) -> ParseResult<PredicateMatch> {
    if is_match_all_regex_pattern(expr) {
        return Ok(if is_equal {
            PredicateMatch::MatchAll
        } else {
            PredicateMatch::MatchNone
        });
    }

    let Ok(decomposed) = decompose_regex(expr) else {
        return Err(ParseError::InvalidRegex(expr.to_string()));
    };

    Ok(decomposed_regex_to_predicate_match(
        expr, decomposed, is_equal,
    ))
}

fn decomposed_regex_to_predicate_match(
    expr: &str,
    decomposed: RegexDecomposition,
    is_equal: bool,
) -> PredicateMatch {
    match decomposed {
        RegexDecomposition::PrefixWithRegex(prefix, remainder) => {
            // `remainder` now matches the entire string (prefix included).
            // Store the compiled pattern string as `value` for display/serialization.
            let matcher = RegexMatcher::from_parts(remainder, expr.to_string(), Some(prefix));
            if is_equal {
                PredicateMatch::RegexEqual(matcher)
            } else {
                PredicateMatch::RegexNotEqual(matcher)
            }
        }
        RegexDecomposition::Regex(regex) => {
            let matcher = RegexMatcher::from_parts(regex, expr.to_string(), None);
            if is_equal {
                PredicateMatch::RegexEqual(matcher)
            } else {
                PredicateMatch::RegexNotEqual(matcher)
            }
        }
        RegexDecomposition::Prefix(prefix) => {
            if is_equal {
                PredicateMatch::StartsWith(prefix)
            } else {
                PredicateMatch::NotStartsWith(prefix)
            }
        }
        RegexDecomposition::Contains(needle) => {
            validate_contains_value(&needle)
                .expect("contains decomposition must reject empty values");
            if is_equal {
                PredicateMatch::Contains(needle)
            } else {
                PredicateMatch::NotContains(needle)
            }
        }
        RegexDecomposition::Literals(mut lits) => {
            if lits.len() == 1 {
                let literal = lits.pop().unwrap();
                if is_equal {
                    PredicateMatch::Equal(PredicateValue::String(literal))
                } else {
                    PredicateMatch::NotEqual(PredicateValue::String(literal))
                }
            } else {
                let value = PredicateValue::from(lits);
                if is_equal {
                    PredicateMatch::Equal(value)
                } else {
                    PredicateMatch::NotEqual(value)
                }
            }
        }
    }
}

/// remove_start_end_anchors removes '^' at the start of expr and '$' at the end of the expr.
fn remove_start_end_anchors(expr: &str) -> &str {
    let mut cursor = expr;
    while let Some(t) = cursor.strip_prefix('^') {
        cursor = t;
    }
    while cursor.ends_with('$') && !cursor.ends_with("\\$") {
        cursor = &cursor[..cursor.len() - 1];
    }
    cursor
}

pub fn is_match_all_regex_pattern(expr: &str) -> bool {
    remove_start_end_anchors(expr) == ".*"
}

fn get_or_values(sre: &Hir, dest: &mut Vec<String>) -> bool {
    use HirKind::*;

    if dest.len() > MAX_OR_VALUES {
        return false;
    }

    match sre.kind() {
        Class(clazz) => {
            // Expand simple character classes into literal single-character strings when
            // the total number of alternatives is small. This handles cases like `1|2`
            // which the parser may represent as a character class `{'1'..='2'}`.
            match clazz {
                Unicode(uni) => {
                    // Count total characters and bail out if too many.
                    let mut total = 0usize;
                    for r in uni.ranges() {
                        let start = r.start() as u32;
                        let end = r.end() as u32;
                        total = total.saturating_add((end - start + 1) as usize);
                        if total > MAX_OR_VALUES {
                            return false;
                        }
                    }
                    for r in uni.ranges() {
                        let start = r.start() as u32;
                        let end = r.end() as u32;
                        for cp in start..=end {
                            if let Some(ch) = std::char::from_u32(cp) {
                                dest.push(ch.to_string());
                            } else {
                                return false;
                            }
                        }
                    }
                    true
                }
                Bytes(_) => false, // don't attempt to expand arbitrary byte classes
            }
        }
        Empty => {
            dest.push("".to_string());
            true
        }
        Capture(cap) => get_or_values(cap.sub.as_ref(), dest),
        Literal(literal) => {
            if let Ok(s) = String::from_utf8(literal.0.to_vec()) {
                dest.push(s);
                true
            } else {
                false
            }
        }
        Alternation(alt) => {
            dest.reserve(alt.len());
            for sub in alt.iter() {
                let start_count = dest.len();
                if let Some(literal) = get_literal(sub) {
                    dest.push(literal);
                } else if !get_or_values(sub, dest) {
                    return false;
                }
                if dest.len() - start_count > MAX_OR_VALUES {
                    return false;
                }
            }
            true
        }
        Concat(concat) => {
            let mut prefixes = Vec::with_capacity(MAX_OR_VALUES);
            if !get_or_values(&concat[0], &mut prefixes) {
                return false;
            }
            let subs = Vec::from(&concat[1..]);
            let concat = Hir::concat(subs);
            let prefix_count = prefixes.len();
            if !get_or_values(&concat, &mut prefixes) {
                return false;
            }
            let suffix_count = prefixes.len() - prefix_count;
            let additional_capacity = prefix_count * suffix_count;
            if additional_capacity > MAX_OR_VALUES {
                // It is cheaper to use regexp here.
                return false;
            }
            dest.reserve(additional_capacity);
            let (pre, suffixes) = prefixes.split_at(prefix_count);
            for prefix in pre.iter() {
                for suffix in suffixes.iter() {
                    if dest.len() > MAX_OR_VALUES {
                        return false;
                    }
                    dest.push(format!("{prefix}{suffix}"));
                }
            }
            true
        }
        // Previously we listed simple `?` repetitions here into two literal
        // alternatives (empty + literal). That caused certain patterns like
        // `ab?c` to become literal lists. Prefer returning `false` here and let
        // the main decomposition logic handle `?` by producing a
        // `PrefixWithRegex` (so `ab?c` becomes prefix `a` with the remainder
        // `b?c`). This preserves semantics while allowing prefix-based
        // optimizations elsewhere.
        // (Fall through to default -> false)
        _ => false,
    }
}

fn hir_to_string(sre: &Hir) -> String {
    match sre.kind() {
        HirKind::Literal(lit) => String::from_utf8(lit.0.to_vec()).unwrap_or_default(),
        HirKind::Concat(concat) => {
            let mut s = String::new();
            for hir in concat.iter() {
                s.push_str(&hir_to_string(hir));
            }
            s
        }
        HirKind::Alternation(alternate) => alternate
            .iter()
            .map(hir_to_string)
            .collect::<Vec<_>>()
            .join("|"),
        HirKind::Repetition(_) => {
            if is_dot_star(sre) {
                ".*".to_string()
            } else if is_dot_plus(sre) {
                ".+".to_string()
            } else {
                sre.to_string()
            }
        }
        _ => sre.to_string(),
    }
}

fn get_literal(sre: &Hir) -> Option<String> {
    match sre.kind() {
        HirKind::Capture(cap) => get_literal(cap.sub.as_ref()),
        HirKind::Literal(lit) => {
            let s = String::from_utf8(lit.0.to_vec()).unwrap_or_default();
            Some(s)
        }
        _ => None,
    }
}

fn get_repetition(sre: &Hir) -> Option<&regex_syntax::hir::Repetition> {
    match sre.kind() {
        HirKind::Capture(cap) => get_repetition(cap.sub.as_ref()),
        HirKind::Repetition(repetition) => Some(repetition),
        _ => None,
    }
}

fn is_dot_star(sre: &Hir) -> bool {
    match sre.kind() {
        HirKind::Capture(cap) => is_dot_star(cap.sub.as_ref()),
        HirKind::Alternation(alternate) => alternate.iter().any(is_dot_star),
        HirKind::Repetition(repetition) => {
            if let HirKind::Class(clazz) = repetition.sub.kind() {
                repetition.min == 0
                    && repetition.max.is_none()
                    && repetition.greedy
                    && is_empty_class(clazz)
            } else {
                false
            }
        }
        _ => false,
    }
}

fn is_dot_plus(sre: &Hir) -> bool {
    match sre.kind() {
        HirKind::Capture(cap) => is_dot_plus(cap.sub.as_ref()),
        HirKind::Repetition(repetition) => {
            if let HirKind::Class(clazz) = repetition.sub.kind() {
                repetition.min == 1
                    && repetition.max.is_none()
                    && repetition.greedy
                    && is_empty_class(clazz)
            } else {
                false
            }
        }
        _ => false,
    }
}

fn is_empty_class(class: &Class) -> bool {
    if class.is_empty() {
        return true;
    }
    match class {
        Unicode(uni) => {
            let ranges = uni.ranges();
            if ranges.len() == 2 {
                let first = ranges.first().unwrap();
                let last = ranges.last().unwrap();
                if first.start() == '\0' && last.end() == '\u{10ffff}' {
                    return true;
                }
            }
        }
        Bytes(bytes) => {
            let ranges = bytes.ranges();
            if ranges.len() == 2 {
                let first = ranges.first().unwrap();
                let last = ranges.last().unwrap();
                if first.start() == 0 && last.end() == 255 {
                    return true;
                }
            }
        }
    }
    false
}

fn build_hir(pattern: &str) -> Result<Hir, RegexError> {
    parse_regex(pattern).map_err(|err| RegexError::Syntax(err.to_string()))
}

/// Result of decomposing a regex for postings-index optimization.
#[derive(Debug, Clone)]
pub enum RegexDecomposition {
    /// The regex is a simple alternation of literals, e.g. `foo|bar|baz`.
    Literals(Vec<String>),
    /// The regex is an anchored prefix followed by a wildcard, e.g. `^prod.*`.
    Prefix(PredicateValue),
    /// The regex is exactly `.*literal.*` or `.*(a|b|c).*` after anchor normalization.
    Contains(PredicateValue),
    /// The regex is an anchored literal prefix followed by a (non-.*) remainder.
    PrefixWithRegex(String, Regex),
    /// The regex could not be simplified
    Regex(Regex),
}

impl PartialEq for RegexDecomposition {
    fn eq(&self, other: &Self) -> bool {
        use RegexDecomposition::*;
        match (self, other) {
            (Literals(a), Literals(b)) => a == b,
            (Prefix(a), Prefix(b)) => a == b,
            (Contains(a), Contains(b)) => a == b,
            (PrefixWithRegex(a_pref, a_re), PrefixWithRegex(b_pref, b_re)) => {
                a_pref == b_pref && a_re.as_str() == b_re.as_str()
            }
            // Compare two Regex variants by their pattern text. Compiled Regex
            // does not implement PartialEq, so compare the original pattern
            // strings exposed via `as_str()` which is what tests expect.
            (Regex(a), Regex(b)) => a.as_str() == b.as_str(),
            _ => false,
        }
    }
}

impl Eq for RegexDecomposition {}

/// Attempt to decompose `expr` into a form that can be resolved directly against
/// the inverted postings index without running the full regex engine.
///
/// Recognized patterns:
/// - **Simple alternations** (`foo|bar|baz`) -> `RegexDecomposition::Literals`
/// - **Anchored prefix** (`^prod.*` / `^prod.+`) -> `RegexDecomposition::Prefix`
/// - **Exact contains** (`.*foo.*`) -> `RegexDecomposition::Contains`
/// - **Plain literal** (`prod`) -> `RegexDecomposition::Literals` (single element)
/// - **Anchored prefix with a non-wildcard remainder** (`^foo.+bar`, `^foo.*bar`, `^foo.?bar`, `^foo.bar`) -> `RegexDecomposition::PrefixWithRegex(prefix, regex)` where `regex` is the compiled regex for the string.
///
/// Returns `None` for anything more complex.
pub fn decompose_regex(expr: &str) -> Result<RegexDecomposition, RegexError> {
    // empty handled below
    if expr.is_empty() {
        return Ok(RegexDecomposition::Literals(vec!["".to_string()]));
    }

    let sre = build_hir(expr)?;
    // For Prometheus-compatible filters, we assume matchers are anchored.
    // Treat the parsed HIR as if it had start/end anchors, so decomposition
    // can be more aggressive about extracting prefixes.
    let (inner, _orig_anchor_start, _orig_anchor_end) = strip_anchors_hir(&sre);
    let anchor_start = true;
    let _anchor_end = true;

    // Anchored repetition prefix: `^a{2,3}` or `^a{2,3}rest`.
    if anchor_start {
        if let Some(prefix) = extract_anchored_prefix_value(&inner) {
            return Ok(RegexDecomposition::Prefix(prefix));
        }

        // Concat whose first element is a repetition.
        if let HirKind::Concat(subs) = inner.kind()
            && let Some(rep) = subs.first().and_then(|s| get_repetition(s))
            && let Some((prefix, mut rem)) = repetition_to_prefix_rem(rep)
        {
            if subs.len() > 1 {
                rem.push_str(&hir_to_string(&Hir::concat(Vec::from(&subs[1..]))));
            }
            return finish_prefix_rem(prefix, rem);
        }
        // Standalone repetition, e.g. `^a{2,3}`.
        if let Some(rep) = get_repetition(&inner)
            && let Some((prefix, rem)) = repetition_to_prefix_rem(rep)
        {
            return finish_prefix_rem(prefix, rem);
        }

        // literal prefix followed by a non-wildcard remainder or pure wildcard.
        if let Some((prefix, maybe_remainder)) = extract_anchored_prefix_or_remainder(&inner)
            && !prefix.is_empty()
        {
            if let Some(remainder) = maybe_remainder {
                // If the remainder can be expanded into a small set of literal
                // alternatives (e.g., a character class), list them and
                // return a flat literal list prefixed with `prefix`.
                let mut or_values = Vec::new();
                if get_or_values(&remainder, &mut or_values) && !or_values.is_empty() {
                    let lits = or_values
                        .into_iter()
                        .map(|s| format!("{}{}", prefix, s))
                        .collect();
                    return Ok(RegexDecomposition::Literals(lits));
                }

                // Compile a regex that matches the full string by combining the
                // literal `prefix` with the remainder HIR converted to a
                // pattern string. Passing `expr` here would recompile the
                // original expression (which already contains the prefix) and
                // cause duplication when `compile_prefixed_regex` combines the
                // prefix again. Convert the `remainder` HIR to a pattern and
                // compile that instead, so the resulting compiled regex
                // represents `prefix + remainder` as intended.
                if let Ok(compiled) = compile_prefixed_regex(&hir_to_string(&remainder), prefix) {
                    return Ok(compiled);
                }
                // Fall back to compiling the full expr with the same options
                // and the same anchoring wrapper used elsewhere.
                return Ok(RegexDecomposition::Regex(compile_regex(expr)?));
            }
            return Ok(RegexDecomposition::Prefix(PredicateValue::String(prefix)));
        }
    }

    // Concat with a leading literal prefix followed by an optional (`?`) or,
    // for anchored patterns, a wildcard (`.*` / `.+`) then further content.
    if let HirKind::Concat(subs) = inner.kind()
        && subs.len() >= 2
    {
        let mut prefix = String::new();
        let mut idx = 0;
        for (i, sub) in subs.iter().enumerate() {
            if let Some(lit) = get_literal(sub) {
                prefix.push_str(&lit);
                idx = i + 1;
            } else {
                break;
            }
        }
        if !prefix.is_empty() && idx < subs.len() {
            let next = &subs[idx];
            let is_optional = get_repetition(next).is_some_and(|r| r.min == 0 && r.max == Some(1));
            let is_wildcard = anchor_start && (is_dot_star(next) || is_dot_plus(next));
            if is_optional || is_wildcard {
                let regex = compile_regex(expr)?;
                return Ok(RegexDecomposition::PrefixWithRegex(prefix, regex));
            }
        }
    }

    if let Some(needle) = extract_contains_value(&inner) {
        return Ok(RegexDecomposition::Contains(needle));
    }

    // Flat list of literal alternatives.
    let mut or_values = Vec::new();
    if get_or_values(&inner, &mut or_values) && !or_values.is_empty() {
        return Ok(RegexDecomposition::Literals(or_values));
    }

    Ok(RegexDecomposition::Regex(compile_regex(expr)?))
}

pub fn compile_regex(expr: &str) -> Result<Regex, RegexError> {
    let anchored = format!("^(?:{})$", expr);
    build_with_repeat_fallback(&anchored)
}

pub(crate) fn compile_prefixed_regex(
    pattern: &str,
    prefix: String,
) -> Result<RegexDecomposition, RegexError> {
    // The `pattern` argument represents the remainder of the regex after the
    // literal `prefix`. To produce a Regex that matches the full string
    // (including the prefix), prefix the (escaped) literal prefix to the
    // remainder pattern and compile the resulting expression.
    let combined = format!("{}{}", regex::escape(&prefix), pattern);
    let compiled = compile_regex(&combined)?;
    Ok(RegexDecomposition::PrefixWithRegex(prefix, compiled))
}

/// Build a `(prefix, remainder_pattern)` pair from a literal-repetition node with `min >= 1`.
/// Returns `None` if the repetition sub is not a plain literal or `min == 0`.
fn repetition_to_prefix_rem(rep: &regex_syntax::hir::Repetition) -> Option<(String, String)> {
    let lit = get_literal(rep.sub.as_ref())?;
    if rep.min == 0 {
        return None;
    }
    let prefix = lit.repeat(rep.min as usize);
    let escaped = regex::escape(&lit);
    let rem = match rep.max {
        Some(max) => {
            let n = max.saturating_sub(rep.min);
            if n > 0 {
                format!("(?:{escaped}){{0,{n}}}")
            } else {
                String::new()
            }
        }
        None => format!("(?:{escaped}){{0,}}"),
    };
    Some((prefix, rem))
}

/// Turn a `(prefix, remainder_pattern)` pair into a `RegexDecomposition`.
/// An empty remainder means a pure prefix match; otherwise compile and wrap.
fn finish_prefix_rem(prefix: String, rem: String) -> Result<RegexDecomposition, RegexError> {
    if rem.is_empty() {
        Ok(RegexDecomposition::Prefix(PredicateValue::String(prefix)))
    } else {
        compile_prefixed_regex(&rem, prefix)
    }
}

/// Strip leading `Look(Start)` and trailing `Look(End)` from a concat HIR,
/// returning the inner node and whether each anchor was present.
fn strip_anchors_hir(sre: &Hir) -> (Hir, bool, bool) {
    if let HirKind::Concat(subs) = sre.kind() {
        let mut slice = &subs[..];
        let mut start = false;
        let mut end = false;
        if !slice.is_empty()
            && let HirKind::Look(look) = slice[0].kind()
            && matches!(look, Look::Start | Look::StartLF | Look::StartCRLF)
        {
            slice = &slice[1..];
            start = true;
        }
        if !slice.is_empty()
            && let HirKind::Look(look) = slice[slice.len() - 1].kind()
            && matches!(look, Look::End | Look::EndLF | Look::EndCRLF)
        {
            slice = &slice[..slice.len() - 1];
            end = true;
        }
        if slice.len() == 1 {
            return (slice[0].clone(), start, end);
        }
        if start || end {
            // Rebuild a concat node from the trimmed slice.
            return (Hir::concat(Vec::from(slice)), start, end);
        }
    }
    (sre.clone(), false, false)
}

/// If `sre` is a concat starting with a `Literal`, return the literal prefix and optionally
/// the remainder HIR when the remainder is not a simple `.*` wildcard. The return is
/// `(prefix, Option<remainder>)`. If the remainder is `.*`, it returns `None` and
/// the caller should treat it as a pure prefix match.
fn extract_anchored_prefix_or_remainder(sre: &Hir) -> Option<(String, Option<Hir>)> {
    match sre.kind() {
        HirKind::Concat(subs) => {
            if subs.is_empty() {
                return None;
            }

            // Collect contiguous leading literal-like nodes (allowing Capture(Literal)).
            let mut prefix = String::new();
            let mut idx = 0usize;
            for (i, sub) in subs.iter().enumerate() {
                if let Some(lit) = get_literal(sub) {
                    prefix.push_str(&lit);
                    idx = i + 1;
                } else {
                    break;
                }
            }

            if prefix.is_empty() {
                return None;
            }

            // No remainder -> pure prefix
            if idx == subs.len() {
                return Some((prefix, None));
            }

            // Build remainder HIR from the tail.
            let tail = Vec::from(&subs[idx..]);
            let remainder = Hir::concat(tail);

            // If remainder is simply .*, treat it as pure prefix optimization.
            if is_dot_star(&remainder) {
                return Some((prefix, None));
            }

            Some((prefix, Some(remainder)))
        }
        _ => None,
    }
}

fn extract_anchored_prefix_value(sre: &Hir) -> Option<PredicateValue> {
    let HirKind::Concat(subs) = sre.kind() else {
        return None;
    };

    let (last, prefix_parts) = subs.split_last()?;
    if !is_dot_star(last) || prefix_parts.is_empty() {
        return None;
    }

    let prefix_hir = if prefix_parts.len() == 1 {
        prefix_parts[0].clone()
    } else {
        Hir::concat(prefix_parts.to_vec())
    };

    if let Some(literal) = get_literal(&prefix_hir) {
        let value = PredicateValue::String(literal);
        return validate_starts_with_value(&value).ok().map(|_| value);
    }

    let mut values = Vec::new();
    if !get_or_values(&prefix_hir, &mut values) || values.is_empty() {
        return None;
    }

    let value = if values.len() == 1 {
        PredicateValue::String(values.pop().unwrap())
    } else {
        PredicateValue::from(values)
    };
    validate_starts_with_value(&value).ok().map(|_| value)
}

pub(crate) fn extract_ordered_required_literals(expr: &str, prefix: Option<&str>) -> Vec<String> {
    let Ok(sre) = build_hir(expr) else {
        return Vec::new();
    };

    let (inner, _, _) = strip_anchors_hir(&sre);
    match prefix {
        Some(prefix) => {
            let Some((actual_prefix, maybe_remainder)) =
                extract_anchored_prefix_or_remainder(&inner)
            else {
                return Vec::new();
            };
            if actual_prefix != prefix {
                return Vec::new();
            }
            maybe_remainder
                .and_then(|remainder| extract_literal_sequence(&remainder))
                .unwrap_or_default()
        }
        None => extract_literal_sequence(&inner).unwrap_or_default(),
    }
}

pub(crate) fn extract_stable_suffix_hint(expr: &str, prefix: Option<&str>) -> Option<String> {
    let Ok(sre) = build_hir(expr) else {
        return None;
    };

    let (inner, _, _) = strip_anchors_hir(&sre);
    match prefix {
        Some(prefix) => {
            let (actual_prefix, maybe_remainder) = extract_anchored_prefix_or_remainder(&inner)?;
            if actual_prefix != prefix {
                return None;
            }
            maybe_remainder.and_then(|remainder| extract_trailing_literal_sequence(&remainder))
        }
        None => extract_trailing_literal_sequence(&inner),
    }
}

fn extract_contains_value(sre: &Hir) -> Option<PredicateValue> {
    let HirKind::Concat(subs) = sre.kind() else {
        return None;
    };

    if subs.len() < 3 || !is_dot_star(&subs[0]) || !is_dot_star(subs.last()?) {
        return None;
    }

    let middle = Hir::concat(Vec::from(&subs[1..subs.len() - 1]));

    if let Some(literal) = get_literal(&middle) {
        let value = PredicateValue::String(literal);
        return validate_contains_value(&value).ok().map(|_| value);
    }

    let mut values = Vec::new();
    if !get_or_values(&middle, &mut values) || values.is_empty() {
        return None;
    }

    let value = if values.len() == 1 {
        PredicateValue::String(values.pop().unwrap())
    } else {
        PredicateValue::from(values)
    };
    validate_contains_value(&value).ok().map(|_| value)
}

fn extract_literal_sequence(sre: &Hir) -> Option<Vec<String>> {
    if let Some(literal) = get_literal(sre) {
        return Some(vec![literal]);
    }
    if is_dot_star(sre) || is_dot_plus(sre) {
        return Some(Vec::new());
    }

    let HirKind::Concat(subs) = sre.kind() else {
        return None;
    };

    let mut literals = Vec::new();
    let mut current = String::new();
    for sub in subs {
        if let Some(literal) = get_literal(sub) {
            current.push_str(&literal);
            continue;
        }
        if is_dot_star(sub) || is_dot_plus(sub) {
            if !current.is_empty() {
                literals.push(std::mem::take(&mut current));
            }
            continue;
        }
        return None;
    }
    if !current.is_empty() {
        literals.push(current);
    }
    Some(literals)
}

fn extract_trailing_literal_sequence(sre: &Hir) -> Option<String> {
    if let Some(literal) = get_literal(sre) {
        return (!literal.is_empty()).then_some(literal);
    }

    let HirKind::Concat(subs) = sre.kind() else {
        return None;
    };

    let mut suffix_parts = Vec::new();
    for sub in subs.iter().rev() {
        let Some(literal) = get_literal(sub) else {
            break;
        };
        if literal.is_empty() {
            continue;
        }
        suffix_parts.push(literal);
    }

    if suffix_parts.is_empty() {
        return None;
    }

    suffix_parts.reverse();
    Some(suffix_parts.concat())
}

#[cfg(test)]
mod test {
    use super::{RegexDecomposition, decompose_regex, remove_start_end_anchors};
    use crate::labels::filters::{PredicateMatch, PredicateValue};

    #[test]
    fn test_is_dot_star() {
        fn check(s: &str, expected: bool) {
            let sre = super::build_hir(s).unwrap();
            let got = super::is_dot_star(&sre);
            assert_eq!(
                got, expected,
                "unexpected is_dot_star for s={:?}; got {:?}; want {:?}",
                s, got, expected
            );
        }

        check(".*", true);
        check(".+", false);
        check("foo.*", false);
        check(".*foo", false);
        check("foo.*bar", false);
        check(".*foo.*", false);
        check(".*foo.*bar", false);
        check(".*foo.*bar.*", false);
        check(".*foo.*bar.*baz", false);
        check(".*foo.*bar.*baz.*", false);
        check(".*foo.*bar.*baz.*qux.*", false);
        check(".*foo.*bar.*baz.*qux.*quux.*quuz.*corge.*grault", false);
        check(".*foo.*bar.*baz.*qux.*quux.*quuz.*corge.*grault.*", false);
    }

    #[test]
    fn test_is_dot_plus() {
        fn check(s: &str, expected: bool) {
            let sre = super::build_hir(s).unwrap();
            let got = super::is_dot_plus(&sre);
            assert_eq!(
                got, expected,
                "unexpected is_dot_plus for s={:?}; got {:?}; want {:?}",
                s, got, expected
            );
        }

        check(".*", false);
        check(".+", true);
        check("foo.*", false);
        check(".*foo.*bar.*baz.*qux.*quux.*quuz.*corge.*grault", false);
        check(".*foo.*bar.*baz.*qux.*quux.*quuz.*corge.*grault.*", false);
    }

    #[test]
    fn test_remove_start_end_anchors() {
        fn f(s: &str, result_expected: &str) {
            let result = remove_start_end_anchors(s);
            assert_eq!(
                result, result_expected,
                "unexpected result for remove_start_end_anchors({s}); got {result}; want {}",
                result_expected
            );
        }

        f("", "");
        f("a", "a");
        f("^^abc", "abc");
        f("a^b$c", "a^b$c");
        f("$$abc^", "$$abc^");
        f("^abc|de$", "abc|de");
        f("abc\\$", "abc\\$");
        f("^abc\\$$$", "abc\\$");
        f("^a\\$b\\$$", "a\\$b\\$")
    }

    #[test]
    fn test_regex_failure() {
        let s = "a(";
        let got = super::build_hir(s);
        assert!(got.is_err());
    }

    #[test]
    fn test_decompose_regex_literals() {
        assert_eq!(
            decompose_regex("foo|bar|baz").unwrap(),
            RegexDecomposition::Literals(vec!["foo".into(), "bar".into(), "baz".into()])
        );
    }

    #[test]
    fn test_decompose_regex_numeric_alternation() {
        // Ensure patterns like "1|2" (often parsed as a char class) are
        // decomposed into literal alternatives ["1","2"].
        assert_eq!(
            decompose_regex("1|2").unwrap(),
            RegexDecomposition::Literals(vec!["1".into(), "2".into()])
        );
    }

    #[test]
    fn test_decompose_regex_single_literal() {
        assert_eq!(
            decompose_regex("prod").unwrap(),
            RegexDecomposition::Literals(vec!["prod".into()])
        );
    }

    #[test]
    fn test_decompose_regex_anchored_prefix_dotstar() {
        assert_eq!(
            decompose_regex("^prod.*").unwrap(),
            RegexDecomposition::Prefix(PredicateValue::String("prod".into()))
        );
    }

    #[test]
    fn test_decompose_regex_anchored_prefix_dotplus() {
        let got = decompose_regex("^prod.+").unwrap();
        match got {
            RegexDecomposition::PrefixWithRegex(pref, re) => {
                assert_eq!(pref, "prod");
                // regex matches the full string; at least one char after prefix is required
                assert!(re.is_match("prodx"));
                assert!(re.is_match("prodxy"));
                assert!(!re.is_match("prod")); // .+ requires at least one char after prefix
            }
            other => panic!("unexpected decomposition: {:?}", other),
        }
    }

    #[test]
    fn test_decompose_regex_complex_returns_regex() {
        // Patterns that cannot be decomposed should return RegexDecompostion::Regex.
        // Note: `foo.*bar` is now decomposable as PrefixWithRegex(foo, .*bar)
        let got1 = decompose_regex("[a-z]+").unwrap();
        match got1 {
            RegexDecomposition::Regex(_re) => {}
            other => panic!("pattern '[a-z]+' unexpectedly decomposed: {:?}", other),
        }
        let got2 = decompose_regex(".*foo.*bar.*").unwrap();
        match got2 {
            RegexDecomposition::Regex(_re) => {}
            other => panic!(
                "pattern '.*foo.*bar.*' unexpectedly decomposed: {:?}",
                other
            ),
        }
    }

    #[test]
    fn test_decompose_regex_contains() {
        assert_eq!(
            decompose_regex(".*foo.*").unwrap(),
            RegexDecomposition::Contains(PredicateValue::String("foo".into()))
        );
        assert_eq!(
            decompose_regex("^.*foo.*$").unwrap(),
            RegexDecomposition::Contains(PredicateValue::String("foo".into()))
        );
    }

    #[test]
    fn test_decompose_regex_contains_with_captured_literal() {
        assert_eq!(
            decompose_regex(".*foo(bar)baz.*").unwrap(),
            RegexDecomposition::Contains(PredicateValue::String("foobarbaz".into()))
        );
    }

    #[test]
    fn test_decompose_regex_contains_with_literal_alternation() {
        assert_eq!(
            decompose_regex(".*(foo|bar|baz).*").unwrap(),
            RegexDecomposition::Contains(PredicateValue::from(vec![
                "foo".to_string(),
                "bar".to_string(),
                "baz".to_string(),
            ]))
        );
    }

    #[test]
    fn test_decompose_regex_contains_rejects_empty_alternation() {
        match decompose_regex(".*(foo||bar).*").unwrap() {
            RegexDecomposition::Contains(_) => {
                panic!("contains decomposition should reject empty alternatives")
            }
            RegexDecomposition::Regex(_) => {}
            other => panic!("unexpected decomposition: {other:?}"),
        }
    }

    #[test]
    fn test_parse_regex_matcher_contains() {
        use crate::labels::filters::PredicateMatch;

        let got = super::parse_regex_matcher(".*server.*", true).unwrap();
        assert_eq!(
            got,
            PredicateMatch::Contains(PredicateValue::String("server".to_string()))
        );

        let got = super::parse_regex_matcher(".*server.*", false).unwrap();
        assert_eq!(
            got,
            PredicateMatch::NotContains(PredicateValue::String("server".to_string()))
        );
    }

    #[test]
    fn test_parse_regex_matcher_contains_list() {
        let got = super::parse_regex_matcher(".*(server|client).*", true).unwrap();
        assert_eq!(
            got,
            PredicateMatch::Contains(PredicateValue::from(vec![
                "server".to_string(),
                "client".to_string(),
            ]))
        );
    }

    #[test]
    fn test_parse_regex_matcher_match_all() {
        assert_eq!(
            super::parse_regex_matcher(".*", true).unwrap(),
            PredicateMatch::MatchAll
        );
        assert_eq!(
            super::parse_regex_matcher("^.*$", true).unwrap(),
            PredicateMatch::MatchAll
        );
        assert_eq!(
            super::parse_regex_matcher(".*", false).unwrap(),
            PredicateMatch::MatchNone
        );
        assert_eq!(
            super::parse_regex_matcher("^.*$", false).unwrap(),
            PredicateMatch::MatchNone
        );
    }

    #[test]
    fn test_extract_ordered_required_literals_prefixed_regex() {
        let literals =
            super::extract_ordered_required_literals("^server.*db.*prod$", Some("server"));
        assert_eq!(literals, vec!["db".to_string(), "prod".to_string()]);
    }

    #[test]
    fn test_extract_ordered_required_literals_generic_regex() {
        let literals = super::extract_ordered_required_literals(".*foo.*bar.*", None);
        assert_eq!(literals, vec!["foo".to_string(), "bar".to_string()]);
    }

    #[test]
    fn test_extract_stable_suffix_hint_prefixed_regex() {
        let suffix = super::extract_stable_suffix_hint("^server.*db.*prod$", Some("server"));
        assert_eq!(suffix.as_deref(), Some("prod"));
    }

    #[test]
    fn test_extract_stable_suffix_hint_without_prefix() {
        let suffix = super::extract_stable_suffix_hint("^.*bar$", None);
        assert_eq!(suffix.as_deref(), Some("bar"));
    }

    #[test]
    fn test_extract_stable_suffix_hint_ignores_wildcard_only_tail() {
        let suffix = super::extract_stable_suffix_hint("^server.*$", Some("server"));
        assert_eq!(suffix, None);
    }

    #[test]
    fn test_decompose_regex_empty() {
        assert_eq!(
            decompose_regex("").unwrap(),
            RegexDecomposition::Literals(vec!["".into()])
        );
    }

    #[test]
    fn test_decompose_regex_prefix_with_alternation_expands_to_literals() {
        // Pattern with simple alternation (foo|bar) should expand to literals
        let got = decompose_regex("^prod(foo|bar)").unwrap();
        match got {
            RegexDecomposition::Literals(literals) => {
                assert_eq!(literals.len(), 2);
                assert!(literals.contains(&"prodfoo".to_string()));
                assert!(literals.contains(&"prodbar".to_string()));
            }
            _ => panic!("pattern ^prod(foo|bar) should expand to Literals"),
        }
    }

    #[test]
    fn test_decompose_regex_prefix_with_non_capturing_group() {
        // Non-capturing group with alternation should also expand to literals
        let got = decompose_regex("^prod(?:foo|bar)").unwrap();
        match got {
            RegexDecomposition::Literals(literals) => {
                assert_eq!(literals.len(), 2);
                assert!(literals.contains(&"prodfoo".to_string()));
                assert!(literals.contains(&"prodbar".to_string()));
            }
            _ => panic!(
                "pattern ^prod(?:foo|bar) should expand to Literals, got {:?}",
                got
            ),
        }
    }

    #[test]
    fn test_decompose_regex_prefix_with_charclass_plus() {
        let got = decompose_regex("^prod[a-z]+").unwrap();
        match got {
            RegexDecomposition::PrefixWithRegex(pref, re) => {
                assert_eq!(pref, "prod");
                assert!(re.is_match("proda"));
                assert!(re.is_match("prodabc"));
                assert!(!re.is_match("prod"));
            }
            other => panic!("unexpected decomposition: {:?}", other),
        }
    }

    #[test]
    fn test_decompose_regex_prefix_with_trailing_literal() {
        // Pattern with alternation and trailing literal should expand to all combinations
        let got = decompose_regex("^prod(foo|bar)baz").unwrap();
        match got {
            RegexDecomposition::Literals(literals) => {
                assert_eq!(literals.len(), 2);
                assert!(literals.contains(&"prodfoobaz".to_string()));
                assert!(literals.contains(&"prodbarbaz".to_string()));
            }
            _ => panic!(
                "pattern ^prod(foo|bar)baz should expand to Literals, got {:?}",
                got
            ),
        }
    }

    #[test]
    fn test_decompose_regex_negative_cases() {
        // Patterns that are too complex for decomposition should return RegexDecomposition::Regex.
        // With Prometheus-compatible anchored matching, repetition
        // prefixes like `a{2,3}` may be treated as a fixed prefix and thus
        // decomposed. We still expect other complex constructs to return RegexDecomposition::Regex.
        let cases = vec!["\\w+", "(?=foo)bar"];
        for c in cases {
            // If the pattern fails to parse, decompose_regex should return Err.
            match super::build_hir(c) {
                Ok(_) => {
                    // The important property here is that complex patterns should
                    // not be decomposed into simpler forms; they should be
                    // returned as RegexDecomposition::Regex. Compiling the actual
                    // Regex object can fail if the pattern exceeds the compiled-size
                    // limit (e.g. `\w+`'s Unicode-aware character class) -- that is
                    // also an acceptable outcome, since the limit must be enforced
                    // consistently rather than silently bypassed.
                    match decompose_regex(c) {
                        Ok(RegexDecomposition::Regex(_)) => {}
                        Ok(other) => {
                            panic!("pattern {} unexpectedly decomposed to {:?}", c, other)
                        }
                        Err(_) => {}
                    }
                }
                Err(_) => assert!(
                    decompose_regex(c).is_err(),
                    "pattern {} should error on parse",
                    c
                ),
            }
        }
    }

    #[test]
    fn test_decompose_regex_optional_question() {
        // `ab?c` should be represented as a prefix `a` with remainder `b?c`.
        let got = decompose_regex("ab?c").unwrap();
        match got {
            RegexDecomposition::PrefixWithRegex(pref, re) => {
                assert_eq!(pref, "a");
                // regex matches the full string including the prefix
                assert!(re.is_match("ac"));
                assert!(re.is_match("abc"));
                assert!(!re.is_match("axc"));
            }
            other => panic!("unexpected decomposition: {:?}", other),
        }
    }

    #[test]
    fn test_decompose_regex_empty_alternate() {
        assert_eq!(
            decompose_regex("c||d").unwrap(),
            RegexDecomposition::Literals(vec!["c".into(), "".into(), "d".into()])
        );
    }

    #[test]
    fn test_decompose_regex_repetition_prefix() {
        // ^a{2,3} should yield prefix "aa" and PrefixWithRegex when followed by other tokens
        let p = decompose_regex("^a{2,3}").unwrap();
        match p {
            RegexDecomposition::PrefixWithRegex(pref, re) => {
                assert_eq!(pref, "aa");
                // regex matches full string; "aa" or "aaa" are valid
                assert!(re.is_match("aa"));
                assert!(re.is_match("aaa"));
            }
            other => panic!("unexpected decomposition: {:?}", other),
        }

        let p2 = decompose_regex("^a{2,3}b").unwrap();
        match p2 {
            RegexDecomposition::PrefixWithRegex(pref, re) => {
                assert_eq!(pref, "aa");
                // regex matches full string; "aab" (prefix + zero optional 'a' + 'b')
                // or "aaab" (prefix + one optional 'a' + 'b')
                assert!(re.is_match("aab"));
                assert!(re.is_match("aaab"));
                assert!(!re.is_match("aa")); // missing 'b'
                assert!(!re.is_match("aaa")); // missing 'b'
            }
            other => panic!("unexpected decomposition: {:?}", other),
        }
    }

    #[test]
    fn test_decompose_regex_optional_after_multiple_literals() {
        // Pattern: `abc?d` -> prefix `ab`, remainder `c?d`
        let got = decompose_regex("abc?d").unwrap();
        match got {
            RegexDecomposition::PrefixWithRegex(pref, re) => {
                assert_eq!(pref, "ab");
                // regex matches full string including prefix
                assert!(re.is_match("abcd"));
                assert!(re.is_match("abd"));
                assert!(!re.is_match("abacd"));
                assert!(!re.is_match("abad"));
            }
            other => panic!("unexpected decomposition for 'abc?d': {:?}", other),
        }
    }

    #[test]
    fn test_decompose_regex_multiple_optionals() {
        // Pattern: `abc?d?e` -> prefix `ab`, remainder `c?d?e`
        let got = decompose_regex("abc?d?e").unwrap();
        match got {
            RegexDecomposition::PrefixWithRegex(pref, re) => {
                assert_eq!(pref, "ab");
                // regex matches full string including prefix
                assert!(re.is_match("abcde"));
                assert!(re.is_match("abde"));
                assert!(re.is_match("abce"));
                assert!(re.is_match("abe"));
                assert!(!re.is_match("abcd")); // missing 'e'
                assert!(!re.is_match("ababc")); // wrong pattern
            }
            other => panic!("unexpected decomposition for 'abc?d?e': {:?}", other),
        }
    }

    #[test]
    fn test_decompose_regex_optional_with_trailing_literal() {
        // Pattern: `abcd?ef` -> prefix `abc`, remainder `d?ef`
        let got = decompose_regex("abcd?ef").unwrap();
        match got {
            RegexDecomposition::PrefixWithRegex(pref, re) => {
                assert_eq!(pref, "abc");
                // regex matches full string including prefix
                assert!(re.is_match("abcdef"));
                assert!(re.is_match("abcef"));
                assert!(!re.is_match("abce")); // missing 'f'
                assert!(!re.is_match("abcdf")); // missing 'e'
            }
            other => panic!("unexpected decomposition for 'abcd?ef': {:?}", other),
        }
    }

    #[test]
    fn test_decompose_regex_no_leading_literals() {
        // Pattern: `a?bc` -> no fixed prefix, should fall through and not decompose to PrefixWithRegex
        // (but might match as literals if `a?bc` can be enumerated)
        let got = decompose_regex("a?bc").unwrap();
        // Since `a?bc` is not anchored and starts with optional, it shouldn't decompose to PrefixWithRegex
        // It might return None or Literals depending on enumeration; we just verify it's not a bad decomposition.
        match got {
            RegexDecomposition::PrefixWithRegex(_, _) => {
                panic!("unexpected PrefixWithRegex for 'a?bc' (no leading fixed prefix)")
            }
            _ => {
                // Ok: either None, Literals, or Prefix is acceptable
            }
        }
    }

    #[test]
    fn test_decompose_regex_prefix_dotstar_suffix() {
        // Pattern: `^foo.*bar$` -> prefix `foo`, remainder `.*bar`
        let got = decompose_regex("^foo.*bar$").unwrap();
        match got {
            RegexDecomposition::PrefixWithRegex(pref, re) => {
                assert_eq!(pref, "foo");
                // regex matches full string including prefix
                assert!(re.is_match("foobar"));
                assert!(re.is_match("fooxbar"));
                assert!(re.is_match("fooxybar"));
                assert!(!re.is_match("foobaz"));
            }
            other => panic!("unexpected decomposition for '^foo.*bar$': {:?}", other),
        }
    }

    #[test]
    fn test_decompose_regex_prefix_dotplus_suffix() {
        // Pattern: `^foo.+bar$` -> prefix `foo`, remainder `.+bar`
        let got = decompose_regex("^foo.+bar$").unwrap();
        match got {
            RegexDecomposition::PrefixWithRegex(pref, re) => {
                assert_eq!(pref, "foo");
                // regex matches full string including prefix
                assert!(re.is_match("fooxbar"));
                assert!(re.is_match("fooxybar"));
                assert!(!re.is_match("foobar")); // .+ requires at least one char between prefix and "bar"
                assert!(!re.is_match("foobaz"));
            }
            other => panic!("unexpected decomposition for '^foo.+bar$': {:?}", other),
        }
    }

    #[test]
    fn test_decompose_regex_prefix_dotstar_complex_suffix() {
        // Pattern: `^prod.*[0-9]+$` -> prefix `prod`, remainder `.*[0-9]+`
        let got = decompose_regex("^prod.*[0-9]+$").unwrap();
        match got {
            RegexDecomposition::PrefixWithRegex(pref, re) => {
                assert_eq!(pref, "prod");
                // regex matches full string including prefix
                assert!(re.is_match("prod123"));
                assert!(re.is_match("prodx123"));
                assert!(re.is_match("prodxyz789"));
                assert!(!re.is_match("prodx"));
                assert!(!re.is_match("prodxyz"));
            }
            other => panic!("unexpected decomposition for '^prod.*[0-9]+$': {:?}", other),
        }
    }

    #[test]
    fn test_decompose_regex_multichar_prefix_wildcard_suffix() {
        // Pattern: `^production.+metrics$` -> prefix `production`, remainder `.+metrics`
        let got = decompose_regex("^production.+metrics$").unwrap();
        match got {
            RegexDecomposition::PrefixWithRegex(pref, re) => {
                assert_eq!(pref, "production");
                // regex matches full string including prefix
                assert!(re.is_match("productionxmetrics"));
                assert!(re.is_match("production_metrics"));
                assert!(!re.is_match("productionmetrics")); // .+ requires at least one char
            }
            other => panic!(
                "unexpected decomposition for 'production.+metrics': {:?}",
                other
            ),
        }
    }

    #[test]
    fn test_parse_regex_matcher_charclass_node12() {
        use crate::labels::filters::{PredicateMatch, PredicateValue};

        let got = super::parse_regex_matcher("node[12]", true).unwrap();

        let expected = PredicateMatch::Equal(PredicateValue::from(vec![
            "node1".to_string(),
            "node2".to_string(),
        ]));

        assert_eq!(got, expected);
    }

    #[test]
    fn test_decompose_regex_prefix_wildcard_only() {
        // Pattern: `^server.*$` -> prefix `server` (wildcard only, no suffix)
        // When the remainder is just .* or .+, it's optimized to Prefix
        let got = decompose_regex("^server.*$").unwrap();
        match got {
            RegexDecomposition::Prefix(PredicateValue::String(pref)) => {
                assert_eq!(pref, "server");
            }
            other => panic!("unexpected decomposition for '^server.*$': {:?}", other),
        }
    }

    #[test]
    fn test_decompose_regex_multi_node_prefix() {
        // Pattern where the literal prefix is split across multiple HIR nodes,
        // e.g., a capture in the middle: `^pro(d)uction.*$` -> prefix `production`.
        let got = decompose_regex("^pro(d)uction.*$").unwrap();
        match got {
            RegexDecomposition::Prefix(PredicateValue::String(pref)) => {
                assert_eq!(pref, "production");
            }
            other => panic!(
                "unexpected decomposition for '^pro(d)uction.*$': {:?}",
                other
            ),
        }
    }

    #[test]
    fn test_decompose_regex_prefix_alternation_to_prefix_list() {
        assert_eq!(
            decompose_regex("(A|B|C).*").unwrap(),
            RegexDecomposition::Prefix(PredicateValue::from(vec![
                "A".to_string(),
                "B".to_string(),
                "C".to_string(),
            ]))
        );
    }

    #[test]
    fn test_parse_regex_matcher_prefix_alternation() {
        let got = super::parse_regex_matcher("(A|B|C).+", true).unwrap();
        match got {
            PredicateMatch::RegexEqual(re) => {
                assert!(re.is_match("A1"));
                assert!(re.is_match("B1"));
                assert!(!re.is_match("A"));
            }
            other => panic!("expected full regex for .+ suffix, got {other:?}"),
        }

        let got = super::parse_regex_matcher("(A|B|C).*", true).unwrap();
        assert_eq!(
            got,
            PredicateMatch::StartsWith(PredicateValue::from(vec![
                "A".to_string(),
                "B".to_string(),
                "C".to_string(),
            ]))
        );
    }

    #[test]
    fn test_decompose_regex_prefix_alternation_rejects_empty_prefixes() {
        match decompose_regex("(A||B).*").unwrap() {
            RegexDecomposition::Prefix(_) => {
                panic!("prefix decomposition should reject empty alternatives")
            }
            RegexDecomposition::Regex(_) => {}
            other => panic!("unexpected decomposition: {other:?}"),
        }
    }
}
