//! Optimize matches against the postings index by decomposing regexes into simpler components when possible.
//! For example, `^prod.*` can be decomposed into a simple prefix match for `prod`, and `foo|bar|baz` can be
//! decomposed into a set of literal alternatives. More complex patterns that cannot be simplified will be
//! returned as-is for full regex matching.
use crate::labels::filters::{
    PredicateMatch, PredicateValue, RegexMatcher, validate_contains_value,
    validate_starts_with_value,
};
use crate::labels::regex::{RegexError, compile_matcher, compile_rust_syntax, parse_matcher_hir};
use crate::labels::regex_cache::{PatternCache, REGEX_CACHE_CAPACITY, REGEX_CACHE_MAX_PATTERN_LEN};
use crate::parser::{ParseError, ParseResult};
use regex::Regex;
use regex_syntax::hir::Class::{Bytes, Unicode};
use regex_syntax::hir::{Class, Hir, HirKind, Look};
use std::sync::LazyLock;

// The most strings decomposition will build out of a character class, a concatenation's
// product, or one non-literal branch of an alternation; beyond this, the regex is cheaper.
// Flat lists of literal alternatives are not capped; see `get_or_values`.
const MAX_OR_VALUES: usize = 16;
// Prefix extraction is an index optimization, not a reason to materialize an
// unbounded string from an otherwise compact regex repetition. Larger patterns
// fall back to the regular compiler, whose size limit applies.
const MAX_REPETITION_PREFIX_BYTES: u64 = 4 * 1024;

/// Decomposed `=~` and `!~` matchers by pattern. A matcher depends only on its pattern and
/// operator, and building one costs a regex-syntax parse (1–5 µs) plus, when no literal, prefix
/// or list form applies, a compile; a hit is one clone. Bounded like the regex cache, whose
/// rules (only `is_match` is called, failures are not cached) apply here too. A matcher is
/// stored on the second sighting of its pattern: a miss that only decomposes is cheap enough
/// that storing and evicting one-off patterns would cost a quarter of it.
static EQUAL_MATCHERS: LazyLock<PatternCache<PredicateMatch>> = LazyLock::new(|| {
    PatternCache::admitting_on_second_sighting(REGEX_CACHE_CAPACITY, REGEX_CACHE_MAX_PATTERN_LEN)
});
static NOT_EQUAL_MATCHERS: LazyLock<PatternCache<PredicateMatch>> = LazyLock::new(|| {
    PatternCache::admitting_on_second_sighting(REGEX_CACHE_CAPACITY, REGEX_CACHE_MAX_PATTERN_LEN)
});

/// The matcher for `=~ expr` (or `!~ expr` when `is_equal` is false), decomposed into a
/// literal, prefix, list or contains match where the regex allows it.
pub(crate) fn parse_regex_matcher(expr: &str, is_equal: bool) -> ParseResult<PredicateMatch> {
    if is_match_all_regex_pattern(expr) {
        return Ok(if is_equal {
            PredicateMatch::MatchAll
        } else {
            PredicateMatch::MatchNone
        });
    }
    let cache = if is_equal {
        &EQUAL_MATCHERS
    } else {
        &NOT_EQUAL_MATCHERS
    };
    cache.get_or_compile(expr, |expr| decompose_matcher(expr, is_equal))
}

fn decompose_matcher(expr: &str, is_equal: bool) -> ParseResult<PredicateMatch> {
    let invalid = |err: RegexError| ParseError::InvalidRegex(format!("`{expr}`: {err}"));
    // Parse once: the decomposition and the matcher's prefilter hints both
    // read this tree.
    let inner = parse_anchored_hir(expr).map_err(invalid)?;
    let decomposed = decompose_parsed(expr, &inner).map_err(invalid)?;
    Ok(decomposed_regex_to_predicate_match(
        expr, &inner, decomposed, is_equal,
    ))
}

fn regex_predicate(
    expr: &str,
    inner: &Hir,
    regex: Regex,
    prefix: Option<String>,
    is_equal: bool,
) -> PredicateMatch {
    let hints = regex_hints(inner, prefix.as_deref());
    // `regex` matches the entire string, prefix included; `expr` is kept as
    // `value` for display and serialization.
    let matcher = Box::new(RegexMatcher::with_hints(
        regex,
        expr.to_string(),
        prefix,
        hints,
    ));
    if is_equal {
        PredicateMatch::RegexEqual(matcher)
    } else {
        PredicateMatch::RegexNotEqual(matcher)
    }
}

fn decomposed_regex_to_predicate_match(
    expr: &str,
    inner: &Hir,
    decomposed: RegexDecomposition,
    is_equal: bool,
) -> PredicateMatch {
    match decomposed {
        RegexDecomposition::PrefixWithRegex(prefix, regex) => {
            regex_predicate(expr, inner, regex, Some(prefix), is_equal)
        }
        RegexDecomposition::Regex(regex) => regex_predicate(expr, inner, regex, None, is_equal),
        RegexDecomposition::Prefix(prefix) => {
            if is_equal {
                PredicateMatch::StartsWith(prefix)
            } else {
                PredicateMatch::NotStartsWith(prefix)
            }
        }
        RegexDecomposition::Contains(needle) => {
            // `extract_contains_value`, the only producer, validates before
            // returning. A panic here would abort the server, so only debug
            // builds check it again.
            debug_assert!(
                validate_contains_value(&needle).is_ok(),
                "contains decomposition must reject empty values"
            );
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

pub(crate) fn is_match_all_regex_pattern(expr: &str) -> bool {
    remove_start_end_anchors(expr) == ".*"
}

/// The finite set of strings `sre` matches, when it is small enough to look up value by value
/// instead of running a regex; `None` otherwise.
///
/// [`MAX_OR_VALUES`] bounds every set this function has to *build*: a character class, a
/// concatenation's product, and each non-literal branch of an alternation. A literal branch
/// costs one value, and a flat list of literal alternatives (`a|b|c|…`, as dashboards generate
/// for multi-value variables) is not capped: it is a direct postings lookup per value, and its
/// size is the size of the pattern.
///
/// Work is linear in the size of `sre`. Each node is visited once, without building new trees,
/// and recursion follows only the nesting the parser allows (`regex_syntax`'s nest limit), not
/// the length of a concatenation. A concatenation is folded from the left and stops at the
/// first child that would push the product over the budget, so `[ab]` repeated thousands of
/// times gives up after five terms.
fn get_or_values(sre: &Hir) -> Option<Vec<String>> {
    use HirKind::*;

    match sre.kind() {
        // Expand a small character class into single-character strings. This handles cases
        // like `1|2`, which the parser represents as the class `[12]`.
        Class(Unicode(uni)) => {
            let mut total = 0usize;
            for r in uni.ranges() {
                total = total.saturating_add((r.end() as u32 - r.start() as u32 + 1) as usize);
                if total > MAX_OR_VALUES {
                    return None;
                }
            }
            let mut values = Vec::with_capacity(total);
            for r in uni.ranges() {
                for cp in r.start() as u32..=r.end() as u32 {
                    values.push(std::char::from_u32(cp)?.to_string());
                }
            }
            Some(values)
        }
        // Don't attempt to expand arbitrary byte classes.
        Class(Bytes(_)) => None,
        Empty => Some(vec![String::new()]),
        Capture(cap) => get_or_values(cap.sub.as_ref()),
        Literal(literal) => String::from_utf8(literal.0.to_vec()).ok().map(|s| vec![s]),
        Alternation(alt) => {
            let mut values = Vec::with_capacity(alt.len());
            for sub in alt.iter() {
                if let Some(literal) = get_literal(sub) {
                    values.push(literal);
                    continue;
                }
                let branch = get_or_values(sub)?;
                if branch.len() > MAX_OR_VALUES {
                    return None;
                }
                values.extend(branch);
            }
            Some(values)
        }
        Concat(subs) => {
            let mut product = vec![String::new()];
            for sub in subs {
                let suffixes = get_or_values(sub)?;
                if product.len().saturating_mul(suffixes.len()) > MAX_OR_VALUES {
                    // It is cheaper to use the regex here.
                    return None;
                }
                if let [suffix] = suffixes.as_slice() {
                    // The common step: extend in place rather than copy every prefix.
                    for prefix in &mut product {
                        prefix.push_str(suffix);
                    }
                } else {
                    product = product
                        .iter()
                        .flat_map(|prefix| suffixes.iter().map(move |s| format!("{prefix}{s}")))
                        .collect();
                }
            }
            Some(product)
        }
        // Repetitions are not expanded, `?` included: listing `b?` as `""` and `"b"` turned
        // `ab?c` into a literal list, where the decomposition proper keeps prefix `a` with the
        // remainder `b?c`.
        _ => None,
    }
}

/// [`get_or_values`], treating an empty set (a class that matches nothing) as "not
/// decomposable" the way every caller wants it.
fn non_empty_or_values(sre: &Hir) -> Option<Vec<String>> {
    get_or_values(sre).filter(|values| !values.is_empty())
}

fn get_literal(sre: &Hir) -> Option<String> {
    match sre.kind() {
        HirKind::Capture(cap) => get_literal(cap.sub.as_ref()),
        HirKind::Literal(lit) => String::from_utf8(lit.0.to_vec()).ok(),
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
                    && is_any_char_class(clazz)
            } else {
                false
            }
        }
        _ => false,
    }
}

// Unlike `is_dot_star`, this does not look through alternations: `x|.+` is
// only `.+` when `x` cannot match the empty string.
fn is_dot_plus(sre: &Hir) -> bool {
    match sre.kind() {
        HirKind::Capture(cap) => is_dot_plus(cap.sub.as_ref()),
        HirKind::Repetition(repetition) => {
            if let HirKind::Class(clazz) = repetition.sub.kind() {
                repetition.min == 1
                    && repetition.max.is_none()
                    && repetition.greedy
                    && is_any_char_class(clazz)
            } else {
                false
            }
        }
        _ => false,
    }
}

/// Whether `class` is `.`, i.e. matches every character.
///
/// `build_hir` parses with `dot_matches_new_line`, so `.` is a single range
/// spanning all of Unicode (or all bytes). A multi-range class such as `[^a]`
/// is never `.`, and neither is the empty class, which matches nothing.
fn is_any_char_class(class: &Class) -> bool {
    match class {
        Unicode(uni) => matches!(
            uni.ranges(),
            [r] if r.start() == '\0' && r.end() == '\u{10ffff}'
        ),
        Bytes(bytes) => matches!(bytes.ranges(), [r] if r.start() == 0 && r.end() == 255),
    }
}

/// Parse `pattern` the way `compile_regex` compiles it (Go syntax, `.` matching a newline), so
/// the decomposition and the compiled regex agree on what it means.
fn build_hir(pattern: &str) -> Result<Hir, RegexError> {
    parse_matcher_hir(pattern)
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

/// Parse and decompose `expr` in one call; see [`decompose_parsed`].
#[cfg(test)]
fn decompose_regex(expr: &str) -> Result<RegexDecomposition, RegexError> {
    decompose_parsed(expr, &parse_anchored_hir(expr)?)
}

/// Parse `expr` and strip the redundant outer anchors.
fn parse_anchored_hir(expr: &str) -> Result<Hir, RegexError> {
    Ok(strip_anchors_hir(&build_hir(expr)?))
}

/// Attempt to decompose `expr`, already parsed as `inner` (see
/// [`parse_anchored_hir`]), into a form that can be resolved directly against
/// the inverted postings index without running the full regex engine.
///
/// Matchers are fully anchored (Prometheus semantics), so leading `^` and
/// trailing `$` are stripped before decomposition.
///
/// Recognized patterns:
/// - **Simple alternations** (`foo|bar|baz`) -> `RegexDecomposition::Literals`
/// - **Anchored prefix** (`^prod.*`) -> `RegexDecomposition::Prefix`
/// - **Exact contains** (`.*foo.*`) -> `RegexDecomposition::Contains`
/// - **Plain literal** (`prod`) -> `RegexDecomposition::Literals` (single element)
/// - **Literal prefix with a non-wildcard remainder** (`^foo.+bar`, `^foo.*bar`, `^foo.?bar`,
///   `^foo.bar`) -> `RegexDecomposition::PrefixWithRegex(prefix, regex)`, where `regex`
///   matches the whole string, prefix included.
///
/// Anything else is compiled from `expr` as `RegexDecomposition::Regex`.
/// Returns `Err` if the compiled regex exceeds the size limit.
fn decompose_parsed(expr: &str, inner: &Hir) -> Result<RegexDecomposition, RegexError> {
    // Prefix alternatives followed by `.*`, e.g. `^prod.*` or `(a|b).*`.
    if let Some(prefix) = extract_anchored_prefix_value(inner) {
        return Ok(RegexDecomposition::Prefix(prefix));
    }

    // Concat whose first element is a repetition, e.g. `^a{2,3}rest`.
    if let HirKind::Concat(subs) = inner.kind()
        && let Some(rep) = subs.first().and_then(|s| get_repetition(s))
        && let Some((prefix, mut rem)) = repetition_to_prefix_rem(rep)
    {
        if subs.len() > 1 {
            rem.push_str(&Hir::concat(Vec::from(&subs[1..])).to_string());
        }
        return finish_prefix_rem(prefix, rem);
    }
    // Standalone repetition, e.g. `^a{2,3}`.
    if let Some(rep) = get_repetition(inner)
        && let Some((prefix, rem)) = repetition_to_prefix_rem(rep)
    {
        return finish_prefix_rem(prefix, rem);
    }

    // Literal prefix followed by nothing, by `.*`, or by some other remainder.
    if let Some((prefix, tail)) = extract_anchored_prefix_or_remainder(inner) {
        let remainder = match tail {
            PrefixTail::Exact => return Ok(RegexDecomposition::Literals(vec![prefix])),
            PrefixTail::Anything => {
                return Ok(RegexDecomposition::Prefix(PredicateValue::String(prefix)));
            }
            PrefixTail::Rest(remainder) => remainder,
        };
        // A remainder that expands into a few literals (e.g. a character
        // class) becomes a flat literal list carrying the prefix.
        if let Some(or_values) = non_empty_or_values(&remainder) {
            let lits = or_values
                .into_iter()
                .map(|s| format!("{prefix}{s}"))
                .collect();
            return Ok(RegexDecomposition::Literals(lits));
        }
        // `Hir`'s `Display` escapes literals and groups alternations, so the
        // rendered remainder means exactly what it meant inside `expr`.
        if let Ok(compiled) = compile_prefixed_regex(&remainder.to_string(), prefix) {
            return Ok(compiled);
        }
        return Ok(RegexDecomposition::Regex(compile_regex(expr)?));
    }

    if let Some(needle) = extract_contains_value(inner) {
        return Ok(RegexDecomposition::Contains(needle));
    }

    // Flat list of literal alternatives.
    if let Some(or_values) = non_empty_or_values(inner) {
        return Ok(RegexDecomposition::Literals(or_values));
    }

    Ok(RegexDecomposition::Regex(compile_regex(expr)?))
}

/// Compile a user's pattern (Go syntax), anchored at both ends, through the process-wide
/// cache of compiled matchers (`regex_cache`).
pub(crate) fn compile_regex(expr: &str) -> Result<Regex, RegexError> {
    compile_matcher(expr)
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
    // `pattern` is a printed `Hir` or built here, so it is Rust syntax already.
    let compiled = compile_rust_syntax(&combined)?;
    Ok(RegexDecomposition::PrefixWithRegex(prefix, compiled))
}

/// Build a `(prefix, remainder_pattern)` pair from a literal-repetition node with `min >= 1`.
/// Returns `None` if the repetition sub is not a plain literal or `min == 0`.
fn repetition_to_prefix_rem(rep: &regex_syntax::hir::Repetition) -> Option<(String, String)> {
    let lit = get_literal(rep.sub.as_ref())?;
    if rep.min == 0 {
        return None;
    }
    let prefix_len = u64::from(rep.min).checked_mul(lit.len() as u64)?;
    if prefix_len > MAX_REPETITION_PREFIX_BYTES {
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
/// An empty remainder means nothing may follow the prefix (e.g. `a{2}`), so it
/// is an exact literal; otherwise compile and wrap.
fn finish_prefix_rem(prefix: String, rem: String) -> Result<RegexDecomposition, RegexError> {
    if rem.is_empty() {
        Ok(RegexDecomposition::Literals(vec![prefix]))
    } else {
        compile_prefixed_regex(&rem, prefix)
    }
}

/// Strip a leading `Look(Start)` and a trailing `Look(End)` from a concat HIR.
/// Matchers are fully anchored, so explicit anchors at the ends are redundant.
fn strip_anchors_hir(sre: &Hir) -> Hir {
    let HirKind::Concat(subs) = sre.kind() else {
        return sre.clone();
    };
    let mut slice = &subs[..];
    if let Some(HirKind::Look(Look::Start | Look::StartLF | Look::StartCRLF)) =
        slice.first().map(Hir::kind)
    {
        slice = &slice[1..];
    }
    if let Some(HirKind::Look(Look::End | Look::EndLF | Look::EndCRLF)) =
        slice.last().map(Hir::kind)
    {
        slice = &slice[..slice.len() - 1];
    }
    match slice {
        [single] => single.clone(),
        _ if slice.len() == subs.len() => sre.clone(),
        _ => Hir::concat(Vec::from(slice)),
    }
}

/// What follows the literal prefix found by [`extract_anchored_prefix_or_remainder`].
enum PrefixTail {
    /// Nothing: the pattern is exactly the prefix, e.g. `(foo)(bar)`.
    Exact,
    /// `.*`: any value starting with the prefix.
    Anything,
    /// Any other remainder.
    Rest(Hir),
}

/// If `sre` is a concat starting with literals (captures included), return the
/// literal prefix and what follows it.
fn extract_anchored_prefix_or_remainder(sre: &Hir) -> Option<(String, PrefixTail)> {
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

            if idx == subs.len() {
                return Some((prefix, PrefixTail::Exact));
            }

            // Build remainder HIR from the tail.
            let tail = Vec::from(&subs[idx..]);
            let remainder = Hir::concat(tail);

            if is_dot_star(&remainder) {
                return Some((prefix, PrefixTail::Anything));
            }

            Some((prefix, PrefixTail::Rest(remainder)))
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

    let mut values = non_empty_or_values(&prefix_hir)?;

    let value = if values.len() == 1 {
        PredicateValue::String(values.pop().unwrap())
    } else {
        PredicateValue::from(values)
    };
    validate_starts_with_value(&value).ok().map(|_| value)
}

/// Cheap prefilters `RegexMatcher::is_match` runs before the full regex.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct RegexHints {
    /// Literals the value must contain, in order, after the prefix.
    pub(crate) required_literals: Vec<String>,
    /// A literal the value must end with.
    pub(crate) suffix: Option<String>,
}

/// [`regex_hints`] for a pattern that has not been parsed yet. A pattern that
/// does not parse gets no hints.
pub(crate) fn regex_hints_for_pattern(expr: &str, prefix: Option<&str>) -> RegexHints {
    parse_anchored_hir(expr)
        .map(|inner| regex_hints(&inner, prefix))
        .unwrap_or_default()
}

/// Derive the prefilter hints from `inner`, the anchor-stripped pattern.
///
/// With a `prefix`, the hints describe the remainder after it, and only when
/// `inner` starts with exactly that literal prefix; otherwise there are none.
fn regex_hints(inner: &Hir, prefix: Option<&str>) -> RegexHints {
    let Some(prefix) = prefix else {
        return hints_from(inner);
    };
    match extract_anchored_prefix_or_remainder(inner) {
        Some((actual, PrefixTail::Rest(remainder))) if actual == prefix => hints_from(&remainder),
        _ => RegexHints::default(),
    }
}

fn hints_from(sre: &Hir) -> RegexHints {
    RegexHints {
        required_literals: extract_literal_sequence(sre).unwrap_or_default(),
        suffix: extract_trailing_literal_sequence(sre),
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

    let mut values = non_empty_or_values(&middle)?;

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
    use super::{
        RegexDecomposition, decompose_regex, parse_regex_matcher, remove_start_end_anchors,
    };
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
    fn test_regex_hints_required_literals_prefixed_regex() {
        let hints = super::regex_hints_for_pattern("^server.*db.*prod$", Some("server"));
        assert_eq!(
            hints.required_literals,
            vec!["db".to_string(), "prod".to_string()]
        );
    }

    #[test]
    fn test_regex_hints_required_literals_generic_regex() {
        let hints = super::regex_hints_for_pattern(".*foo.*bar.*", None);
        assert_eq!(
            hints.required_literals,
            vec!["foo".to_string(), "bar".to_string()]
        );
    }

    #[test]
    fn test_regex_hints_suffix_prefixed_regex() {
        let suffix = super::regex_hints_for_pattern("^server.*db.*prod$", Some("server")).suffix;
        assert_eq!(suffix.as_deref(), Some("prod"));
    }

    #[test]
    fn test_regex_hints_suffix_without_prefix() {
        let suffix = super::regex_hints_for_pattern("^.*bar$", None).suffix;
        assert_eq!(suffix.as_deref(), Some("bar"));
    }

    #[test]
    fn test_regex_hints_suffix_ignores_wildcard_only_tail() {
        let suffix = super::regex_hints_for_pattern("^server.*$", Some("server")).suffix;
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
    fn test_oversized_repetition_prefix_falls_back_to_limited_compilation() {
        // Decomposition must not expand this compact syntax into a multi-gigabyte
        // prefix. The normal compiler rejects it under its configured size limit.
        assert!(decompose_regex("^(abcde){900000000}").is_err());
        assert!(parse_regex_matcher("a{999999999}", true).is_err());
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

    /// Every decomposition must accept exactly what the whole pattern accepts
    /// when compiled by `compile_regex`, without decomposition.
    #[test]
    fn test_decomposition_agrees_with_direct_regex() {
        let patterns = [
            // Remainders with escaped metacharacters or grouped alternations.
            r"^foo.+a\.b",
            r"^foo.+a\.b(x|y)",
            r"^foo.+a\+b$",
            r"^foo.+(?:a|bc)",
            r"^a{2,3}x\.y",
            r"^a{2,3}(?:x|yz)",
            r"^x(?:a|b)c.+d",
            r"^foo.+(?i)bar",
            // Negated and empty classes are not `.`.
            r"^prod[^a]*",
            r"^prod[^\n]*",
            r"^prod[^a-m]*",
            r"^prod.*[^0]*",
            r"^prod[^\x00-\x{10FFFF}]*",
            r".*[^a]*foo.*",
            // `.` matches newlines, as in the compiled regex.
            r"^prod.*",
            r"^prod.+",
            r"prod(?s:.)*",
            r".*foo.*",
            r".*(foo|bar).*",
            // Shapes already covered by the structural tests above.
            r"^foo.*bar$",
            r"^prod.*[0-9]+$",
            r"(A|B|C).*",
            r"(A|B|C).+",
            r"node[12]",
            r"ab?c",
            r"abc?d?e",
            r"^server.*db.*prod$",
            r"foo|bar|baz",
            r"a\.b",
            r"foo\$",
            // Exact repetitions and capture-split literals have no tail.
            r"a{2}",
            r"^a{2}$",
            r"x{3}",
            r"(?:ab){2}",
            r"a{2}b",
            r"a{2,}",
            r"(foo)(bar)",
            r"foo(bar)",
            r"(foo)bar",
            r"^pro(d)uction$",
            r"(foo)(bar).*",
            // Go-style braces that are not a repeat are literals.
            r"abc{def}",
            r"x{,2}",
            r"a{}",
            r"{",
            r"^foo.+{bar}$",
            r"node{1,2}x{y}",
            r".*{a}.*",
        ];
        let values = [
            "prod",
            "proda",
            "prodb",
            "prodn",
            "prod\nx",
            "prod0",
            "prodx123",
            "fooXaZb",
            "fooXa.b",
            "fooXaZbx",
            "fooXa.bx",
            "fooXbc",
            "fooXa",
            "fooXaab",
            "fooXa+b",
            "aaxZy",
            "aax.y",
            "aaax.y",
            "aayz",
            "aaayz",
            "xacZd",
            "xbcZd",
            "fooXBAR",
            "foobar",
            "fooxbar",
            "Xfoo",
            "a\nfoo\nb",
            "A",
            "A1",
            "B\n",
            "node1",
            "node3",
            "ac",
            "abc",
            "abde",
            "serverXdbYprod",
            "baz",
            "a.b",
            "aXb",
            "foo$",
            "a",
            "aa",
            "aaa",
            "aab",
            "aabx",
            "xxx",
            "xxxx",
            "abab",
            "ababx",
            "foobarbaz",
            "foobarx",
            "production",
            "productionx",
            "abc{def}",
            "abcdef",
            "x{,2}",
            "xx",
            "a{}",
            "{",
            "fooX{bar}",
            "foo{bar}",
            "nodex{y}",
            "nodeex{y}",
            "nodeeex{y}",
            "Q{a}Q",
        ];

        for pattern in patterns {
            let direct = super::compile_regex(pattern)
                .unwrap_or_else(|e| panic!("reference regex {pattern:?} failed: {e}"));
            let equal = parse_regex_matcher(pattern, true).unwrap();
            let not_equal = parse_regex_matcher(pattern, false).unwrap();
            for value in values {
                let want = direct.is_match(value);
                assert_eq!(
                    equal.matches(value),
                    want,
                    "{pattern:?} =~ {value:?}: decomposed as {equal:?}"
                );
                assert_eq!(
                    not_equal.matches(value),
                    !want,
                    "{pattern:?} !~ {value:?}: decomposed as {not_equal:?}"
                );
            }
        }
    }

    #[test]
    fn test_negated_class_is_not_a_wildcard() {
        for pattern in [r"^prod[^a]*", r"^prod[^\n]*", r"^prod[^\x00-\x{10FFFF}]*"] {
            assert!(
                !matches!(
                    decompose_regex(pattern).unwrap(),
                    RegexDecomposition::Prefix(_)
                ),
                "{pattern:?} must not decompose to a bare prefix"
            );
        }
        assert!(!super::is_dot_star(&super::build_hir("[^a]*").unwrap()));
        assert!(!super::is_dot_plus(&super::build_hir("[^\\n]+").unwrap()));
    }

    #[test]
    fn test_dotall_dot_is_a_wildcard() {
        assert!(super::is_dot_star(&super::build_hir("(?s:.)*").unwrap()));
        assert_eq!(
            decompose_regex("^prod(?s:.)*").unwrap(),
            RegexDecomposition::Prefix(PredicateValue::String("prod".into()))
        );
    }

    #[test]
    fn test_prefix_remainder_keeps_escapes_and_grouping() {
        let RegexDecomposition::PrefixWithRegex(prefix, re) =
            decompose_regex(r"^foo.+a\.b(x|y)").unwrap()
        else {
            panic!("expected PrefixWithRegex");
        };
        assert_eq!(prefix, "foo");
        assert!(re.is_match("fooXa.bx"));
        assert!(!re.is_match("fooXaZbx"));

        let RegexDecomposition::PrefixWithRegex(prefix, re) =
            decompose_regex(r"^foo.+(?:a|bc)").unwrap()
        else {
            panic!("expected PrefixWithRegex");
        };
        assert_eq!(prefix, "foo");
        assert!(re.is_match("fooXbc"));
        assert!(!re.is_match("bc"));
    }

    /// The hints `parse_regex_matcher` derives from its parsed tree must equal
    /// the ones `RegexMatcher::from_parts` derives from the pattern text, which
    /// is how a fan-out shard rebuilds the matcher.
    #[test]
    fn test_parsed_hints_match_from_parts() {
        use crate::labels::filters::RegexMatcher;

        let patterns = [
            r"^foo.+a\.b",
            r"^foo.+a\.b(x|y)",
            r"^foo.+a\+b$",
            r"^foo.+(?:a|bc)",
            r"^a{2,3}x\.y",
            r"^a{3}b.*c",
            r"^x(?:a|b)c.+d",
            r"^prod[^a]*",
            r"^prod.*[^0]*",
            r"^prod.+",
            r"^foo.*bar$",
            r"^prod.*[0-9]+$",
            r"(A|B|C).+",
            r"ab?c",
            r"abc?d?e",
            r"^server.*db.*prod$",
            r".*foo.*bar.*",
            r"^.*bar$",
            r"(?i)prod.*server",
            r"^host-[a-z0-9]{8}-[a-z0-9]{4}$",
            r"pre(fix).+mid.*end",
            r"^a.+b.+c$",
            r"x.*(?:y|z)w",
            r"^(?:abc).+def",
            r"^foo$|^bar.+",
            r"\d+abc",
        ];
        let mut regex_matchers = 0;
        for pattern in patterns {
            for is_equal in [true, false] {
                let (PredicateMatch::RegexEqual(parsed) | PredicateMatch::RegexNotEqual(parsed)) =
                    parse_regex_matcher(pattern, is_equal).unwrap()
                else {
                    continue;
                };
                regex_matchers += 1;
                let rebuilt = RegexMatcher::from_parts(
                    super::compile_regex(parsed.value()).unwrap(),
                    parsed.value().to_string(),
                    parsed.prefix().map(str::to_string),
                );
                assert_eq!(parsed.suffix(), rebuilt.suffix(), "suffix for {pattern:?}");
                assert_eq!(
                    parsed.required_literals(),
                    rebuilt.required_literals(),
                    "required literals for {pattern:?}"
                );
            }
        }
        assert!(
            regex_matchers >= 40,
            "only {regex_matchers} patterns stayed regexes"
        );
    }

    #[test]
    fn test_decompose_regex_exact_patterns_are_literals() {
        for (pattern, literal) in [
            ("a{2}", "aa"),
            ("^x{3}$", "xxx"),
            ("(?:ab){2}", "abab"),
            ("(foo)(bar)", "foobar"),
            ("foo(bar)", "foobar"),
            ("^pro(d)uction$", "production"),
        ] {
            assert_eq!(
                decompose_regex(pattern).unwrap(),
                RegexDecomposition::Literals(vec![literal.into()]),
                "{pattern:?}"
            );
        }
        // A trailing `.*` still makes it a prefix.
        assert_eq!(
            decompose_regex("(foo)(bar).*").unwrap(),
            RegexDecomposition::Prefix(PredicateValue::String("foobar".into()))
        );
    }

    #[test]
    fn test_parse_regex_matcher_accepts_go_style_literal_braces() {
        assert_eq!(
            parse_regex_matcher("abc{def}", true).unwrap(),
            PredicateMatch::Equal(PredicateValue::String("abc{def}".into()))
        );
        assert_eq!(
            parse_regex_matcher(".*{a}.*", true).unwrap(),
            PredicateMatch::Contains(PredicateValue::String("{a}".into()))
        );
        // A valid repeat next to literal braces keeps its meaning.
        let PredicateMatch::RegexEqual(matcher) =
            parse_regex_matcher("node{1,2}x{y}", true).unwrap()
        else {
            panic!("expected a regex matcher");
        };
        assert!(matcher.is_match("nodeex{y}"));
        assert!(!matcher.is_match("nodeeex{y}"));
        // Patterns that are invalid for another reason still fail.
        assert!(parse_regex_matcher("a{b}(", true).is_err());
    }

    #[test]
    fn cached_matchers_equal_freshly_decomposed_ones() {
        // Literal, list, prefix, contains, prefix + regex, plain regex, and an invalid pattern,
        // each through the cache three times for both operators. The matcher caches admit a
        // pattern on its second sighting, so on a cold cache the first request only records
        // it, the second stores it, and the third is a hit; a warm one hits sooner.
        for pattern in [
            "api",
            "prod|staging|dev",
            "api.*",
            ".*server.*",
            "api-[0-9]+",
            "^host-(a|b)[0-9]{2}$",
            "a{999999999}",
        ] {
            for is_equal in [true, false] {
                let fresh = super::decompose_matcher(pattern, is_equal);
                for _ in 0..3 {
                    let cached = parse_regex_matcher(pattern, is_equal);
                    match (&fresh, &cached) {
                        (Ok(f), Ok(c)) => assert_eq!(f, c, "{pattern} {is_equal}"),
                        (Err(_), Err(_)) => {}
                        _ => panic!("{pattern} {is_equal}: {fresh:?} vs {cached:?}"),
                    }
                }
            }
        }
    }

    fn or_values(pattern: &str) -> Option<Vec<String>> {
        super::get_or_values(&super::build_hir(pattern).unwrap())
    }

    #[test]
    fn test_or_values_concat_product_order_and_budget() {
        assert_eq!(
            or_values("[ab][cd]").unwrap(),
            ["ac", "ad", "bc", "bd"].map(String::from)
        );
        assert_eq!(
            or_values("x[ab]y(c|d)z").unwrap(),
            ["xaycz", "xaydz", "xbycz", "xbydz"].map(String::from)
        );
        // 2^4 = 16 is within the budget, 2^5 is not.
        assert_eq!(or_values(&"[ab]".repeat(4)).unwrap().len(), 16);
        assert_eq!(or_values(&"[ab]".repeat(5)), None);
        // A class that matches nothing yields no values to look up, so the pattern is not
        // turned into a (vacuous) literal list; it stays a regex that matches nothing.
        let empty_class = r"[ab][^\x00-\x{10FFFF}]";
        assert!(!matches!(or_values(empty_class), Some(values) if !values.is_empty()));
        assert!(!matches!(
            decompose_regex(empty_class).unwrap(),
            RegexDecomposition::Literals(_)
        ));
    }

    #[test]
    fn test_or_values_alternation_budget_is_per_built_branch() {
        // Literal branches are not capped: a long list stays a list of lookups.
        let words: Vec<String> = (0..40).map(|i| format!("host{i}")).collect();
        assert_eq!(or_values(&words.join("|")).unwrap(), words);
        // Earlier literal branches no longer count against a later non-literal one.
        let pattern = format!("{}|x[yz]", words.join("|"));
        assert_eq!(or_values(&pattern).unwrap().len(), 42);
        // A branch that has to be built is held to the budget.
        assert_eq!(or_values(&format!("a|{}", "[ab]".repeat(5))), None);
    }

    /// LABELS-01: decomposing a long flat concatenation used to recurse once per term and
    /// clone the remaining terms at every level -- quadratic work, and a stack overflow at a
    /// few thousand terms (16 KB of pattern). A stack overflow aborts the whole test binary,
    /// so the patterns run in a child process and the parent checks that it exited normally.
    #[test]
    fn test_long_flat_concat_decomposes_without_deep_recursion() {
        const CHILD: &str = "VALKEY_TS_TEST_LONG_CONCAT_CHILD";
        const NAME: &str = "test_long_flat_concat_decomposes_without_deep_recursion";

        if std::env::var_os(CHILD).is_some() {
            let terms = 8192;
            let classes = "[ab]".repeat(terms);
            let captures = "(a)".repeat(terms);
            let mixed = "a[bc]".repeat(terms / 2);
            for pattern in [
                // Plain product: gives up after five terms, then the regex compiler's size
                // limit rejects the pattern.
                classes.clone(),
                // Leading literal: reaches decomposition through the prefix remainder.
                format!("x{classes}"),
                // Contains: reaches it through the middle of `.*…*.`.
                format!(".*{classes}.*"),
                // Prefix: through the part before a trailing `.*`.
                format!("{classes}.*"),
                // One value per term: the product never grows, every term is visited.
                captures,
                mixed,
            ] {
                for is_equal in [true, false] {
                    // Accepting or rejecting is not the point; returning is.
                    let _ = parse_regex_matcher(&pattern, is_equal);
                }
            }
            return;
        }

        let module = module_path!();
        let test = format!(
            "{}::{NAME}",
            module.split_once("::").map_or(module, |(_, m)| m)
        );
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([test.as_str(), "--exact", "--test-threads=1", "--nocapture"])
            .env(CHILD, "1")
            .output()
            .expect("spawn the test binary");
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success(),
            "child exited with {:?}\nstdout:\n{stdout}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
        // Guard against a filter that matched nothing and "passed" vacuously.
        assert!(stdout.contains("1 passed"), "child ran no test:\n{stdout}");
    }
}
