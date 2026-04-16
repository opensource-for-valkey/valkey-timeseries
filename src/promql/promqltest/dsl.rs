use crate::common::Sample;
use crate::labels::Label;
use crate::parser::number::parse_number;
use crate::promql::{InstantSample, Labels, QueryValue, RangeSample};
use lazy_static::lazy_static;
use promql_parser::util::unquote_string;
use regex::Regex;
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
// ============================================================================
// Command Types
// ============================================================================

#[derive(Debug, Clone)]
pub struct LoadCmd {
    pub interval: Duration,
    pub series: Vec<SeriesLoad>,
}

#[derive(Debug, Clone)]
pub struct EvalInstantCmd {
    pub time: SystemTime,
    pub query: String,
    pub expected: QueryValue,
    pub expect_ordered: bool,
    pub expect_fail: bool,
    pub line_number: usize,
}

#[derive(Debug, Clone)]
pub struct EvalRangeCmd {
    pub start: SystemTime,
    pub end: SystemTime,
    pub step: Duration,
    pub query: String,
    pub expected: QueryValue,
    pub expect_fail: bool,
    pub line_number: usize,
}

#[derive(Debug, Clone)]
pub struct ClearCmd;

#[derive(Debug, Clone)]
pub struct IgnoreCmd;

#[derive(Debug, Clone)]
pub struct ResumeCmd;

#[derive(Debug, Clone)]
pub enum Command {
    Load(LoadCmd),
    EvalInstant(EvalInstantCmd),
    EvalRange(EvalRangeCmd),
    Clear(ClearCmd),
    Ignore(IgnoreCmd),
    Resume(ResumeCmd),
}

// ============================================================================
// Data Structures
// ============================================================================

#[derive(Debug, Clone)]
pub struct SeriesLoad {
    pub labels: HashMap<String, String>, // includes __name__
    pub values: Vec<(i64, f64)>,         // (step_index, value)
}

// ============================================================================
// Parser
// ============================================================================

lazy_static! {
    static ref PAT_EVAL_RANGE: Regex = Regex::new(
        r"^eval(?:_(?:fail|warn|info))?\s+range\s+from\s+(.+)\s+to\s+(.+)\s+step\s+(.+?)\s+(.+)$"
    )
    .unwrap();
}

struct Parser;

impl Parser {
    /// Parse the entire test file into commands
    pub fn parse_file(input: &str) -> Result<Vec<Command>, String> {
        let mut commands = Vec::new();
        let lines: Vec<&str> = input.lines().collect();
        let mut line_idx = 0;
        let mut ignoring = false;

        while line_idx < lines.len() {
            let line = lines[line_idx].trim();
            line_idx += 1;

            // Skip empty lines and comments
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            if let Some(control_cmd) = Self::parse_control_command(line, &mut ignoring) {
                commands.push(control_cmd);
                continue;
            }

            // Skip parsing other commands when ignoring
            if ignoring {
                continue;
            }

            let cmd = Self::parse_non_control_command(line, &lines, &mut line_idx)?
                .ok_or_else(|| format!("Unknown directive at line {}: {}", line_idx, line))?;
            commands.push(cmd);
        }

        Ok(commands)
    }

    fn parse_control_command(line: &str, ignoring: &mut bool) -> Option<Command> {
        match line {
            "ignore" => {
                *ignoring = true;
                Some(Command::Ignore(IgnoreCmd))
            }
            "resume" => {
                *ignoring = false;
                Some(Command::Resume(ResumeCmd))
            }
            "clear" => Some(Command::Clear(ClearCmd)),
            _ => None,
        }
    }

    fn parse_non_control_command(
        line: &str,
        lines: &[&str],
        line_idx: &mut usize,
    ) -> Result<Option<Command>, String> {
        if let Some(cmd) = Self::try_parse_load(line, lines, line_idx)? {
            return Ok(Some(cmd));
        }
        if let Some(cmd) = Self::try_parse_eval_instant(line, lines, line_idx)? {
            return Ok(Some(cmd));
        }
        if let Some(cmd) = Self::try_parse_eval_range(line, lines, line_idx)? {
            return Ok(Some(cmd));
        }
        Ok(None)
    }

    /// Try to parse the "load" command (with indented series lines)
    fn try_parse_load(
        line: &str,
        lines: &[&str],
        line_idx: &mut usize,
    ) -> Result<Option<Command>, String> {
        let rest = match line.strip_prefix("load ") {
            Some(r) => r,
            None => return Ok(None),
        };

        let interval = parse_duration(rest)?;
        let mut series = Vec::new();

        // Collect indented series lines
        while *line_idx < lines.len() {
            let next_line = lines[*line_idx];
            if !next_line.starts_with(' ') && !next_line.starts_with('\t') {
                break;
            }

            let trimmed = next_line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                *line_idx += 1;
                continue;
            }

            series.push(parse_series(trimmed)?);
            *line_idx += 1;
        }

        Ok(Some(Command::Load(LoadCmd { interval, series })))
    }

    /// Try to parse "eval instant at" command (with indented expected results)
    fn try_parse_eval_instant(
        line: &str,
        lines: &[&str],
        line_idx: &mut usize,
    ) -> Result<Option<Command>, String> {
        let rest = match line.strip_prefix("eval instant at ") {
            Some(r) => r,
            None => return Ok(None),
        };

        let line_number = *line_idx;
        // Parse time and query from same line or next line
        let (time_str, query) = Self::parse_time_and_query(rest, lines, line_idx)?;
        let time = parse_time(&time_str)?;

        let (expected, expect_ordered) = parse_expectations(lines, line_idx)?;

        Ok(Some(Command::EvalInstant(EvalInstantCmd {
            time,
            query,
            expected,
            expect_ordered,
            expect_fail: false,
            line_number
        })))
    }

    fn try_parse_eval_range(
        line: &str,
        lines: &[&str],
        line_idx: &mut usize,
    ) -> Result<Option<Command>, String> {
        if line.strip_prefix("eval range from ").is_none() {
            return Ok(None);
        };
        // Not implemented yet, but this is where we'd parse "eval range from <start> to <end> step <step> <query>"
        let caps = PAT_EVAL_RANGE
            .captures(line)
            .ok_or("invalid range vector definition")?;

        let line_number = *line_idx;

        let from_str = caps.get(1).unwrap().as_str();
        let to_str = caps.get(2).unwrap().as_str();
        let step_str = caps.get(3).unwrap().as_str();
        let query = caps.get(4).unwrap().as_str();
        let from =
            parse_duration(from_str).map_err(|e| format!("Invalid 'from' duration: {}", e))?;
        let to = parse_duration(to_str).map_err(|e| format!("Invalid 'to' duration: {}", e))?;
        let step =
            parse_duration(step_str).map_err(|e| format!("Invalid 'step' duration: {}", e))?;
        // Anchor range query times to UNIX_EPOCH like `parse_time` does for
        // instant evals. Tests and loaded series use UNIX_EPOCH-based times,
        // so using SystemTime::now() would make ranges miss the loaded data.
        let start = UNIX_EPOCH + from;
        let end = UNIX_EPOCH + to;

        let (expected, _expect_ordered) = parse_expectations(lines, line_idx)?;

        let cmd = EvalRangeCmd {
            start,
            end,
            step,
            query: query.to_string(),
            expected,
            expect_fail: false, // todo
            line_number
        };
        Ok(Some(Command::EvalRange(cmd)))
    }

    /// Parse time and query from "eval instant at" line
    /// Format: "eval instant at <time> <query>"
    /// Query can span to the next line if on the same line it's empty
    fn parse_time_and_query(
        rest: &str,
        lines: &[&str],
        line_idx: &mut usize,
    ) -> Result<(String, String), String> {
        // Robustly split the first token (time) from the rest of the line.
        // We locate the first whitespace index and then trim only the leading
        // whitespace from the remainder so sequences of tabs/spaces are handled
        // correctly and the remainder preserves internal spacing of the query.
        let time_str;
        let remainder;

        if let Some(idx) = rest
            .char_indices()
            .find_map(|(i, c)| if c.is_whitespace() { Some(i) } else { None })
        {
            time_str = &rest[..idx];
            remainder = &rest[idx..];
        } else {
            // No whitespace: entire rest is the time, query must be on next line
            time_str = rest;
            remainder = "";
        }

        let query = if !remainder.trim_start().is_empty() {
            remainder.trim_start().to_string()
        } else {
            // Query on next line
            if *line_idx < lines.len() {
                let query_line = lines[*line_idx];
                *line_idx += 1;
                query_line.trim().to_string()
            } else {
                return Err("Missing query after 'eval instant at'".to_string());
            }
        };

        Ok((time_str.to_string(), query))
    }
}

// ============================================================================
// Parsing Helpers (Series, Metrics, Values, Durations)
// ============================================================================

fn parse_series(line: &str) -> Result<SeriesLoad, String> {
    let mut chars = line.chars().peekable();
    let mut metric_part = String::new();

    // Read metric name (until { or whitespace)
    while let Some(&c) = chars.peek() {
        if c == '{' || c.is_whitespace() {
            break;
        }
        metric_part.push(chars.next().unwrap());
    }

    // Read label set if present
    if chars.peek() == Some(&'{') {
        let mut brace_depth = 0;
        for c in chars.by_ref() {
            metric_part.push(c);
            if c == '{' {
                brace_depth += 1;
            } else if c == '}' {
                brace_depth -= 1;
                if brace_depth == 0 {
                    break;
                }
            }
        }

        if brace_depth != 0 {
            return Err(format!("Unbalanced {{ }} in series: {}", line));
        }
    }

    let value_parts: String = chars.collect::<String>().trim().to_string();
    if value_parts.is_empty() {
        return Err(format!("Series missing values: {}", line));
    }

    let (metric, labels) = parse_metric(metric_part.trim())?;
    let values = parse_multiple_value_exprs(&value_parts)?;

    let mut all_labels = labels;
    if !metric.is_empty() {
        all_labels.insert("__name__".to_string(), metric);
    }

    Ok(SeriesLoad {
        labels: all_labels,
        values,
    })
}

fn parse_metric(s: &str) -> Result<(String, HashMap<String, String>), String> {
    if let Some((m, rest)) = s.split_once('{') {
        let rest = rest
            .strip_suffix('}')
            .ok_or_else(|| format!("Missing closing }} in metric: '{}'", s))?;
        let labels = parse_labels(rest, s)?;
        Ok((m.to_string(), labels))
    } else if s.starts_with('{') {
        // Label-only (no metric name, used in expected results)
        let rest = s
            .strip_prefix('{')
            .unwrap()
            .strip_suffix('}')
            .ok_or_else(|| format!("Missing closing }} in labels: '{}'", s))?;
        let labels = parse_labels(rest, s)?;
        Ok((String::new(), labels))
    } else {
        // Metric name only (no labels)
        Ok((s.to_string(), HashMap::new()))
    }
}

fn parse_labels(labels_str: &str, context: &str) -> Result<HashMap<String, String>, String> {
    let mut labels = HashMap::new();
    for kv in labels_str.split(',') {
        let kv = kv.trim();
        if kv.is_empty() {
            continue;
        }
        let (k, v) = kv
            .split_once('=')
            .ok_or_else(|| format!("Invalid label '{kv}' (missing =) in: '{context}'"))?;
        labels.insert(k.to_string(), v.trim_matches('"').to_string());
    }
    Ok(labels)
}

fn parse_multiple_value_exprs(s: &str) -> Result<Vec<(i64, f64)>, String> {
    let mut all = Vec::new();
    let mut base_step = 0i64;

    // IMPORTANT (test-driver design decision):
    //
    // We treat multiple value expressions as sequential blocks to guarantee
    // monotonically increasing step indices at parse time.
    //
    // Example: "0+10x3 100+20x2" produces steps [0,1,2,3,4,5,6]
    // (x3 => 4 samples, x2 => 3 samples) instead of [0,1,2,3,0,1,2]
    //
    // Why this matters:
    // 1. Predictable behavior: Test authors see sequential timestamps
    // 2. Avoids confusion: Overlapping steps would be non-obvious
    // 3. Safety: Prevents accidental backwards timestamps
    //
    // Our runner also sorts/deduplicates before ingestion, so overlaps would
    // work. But we enforce sequential blocks for clarity and predictability.
    //
    // This is a test-driver constraint, not a PromQL semantic requirement.
    for part in s.split_whitespace() {
        let values = parse_values(part)?;
        for (step, value) in values {
            all.push((step + base_step, value));
        }
        base_step = all.len() as i64;
    }

    Ok(all)
}

fn parse_values(s: &str) -> Result<Vec<(i64, f64)>, String> {
    let s = s.trim();

    // Check for expansion syntax: "start+step x count"
    // Prometheus promqltest semantics are inclusive:
    // "0+10x5" => 6 samples: [0, 10, 20, 30, 40, 50].
    // Example: "0+10x100" => [0, 10, 20, ..., 1000].
    if s.contains('+') && s.contains('x') {
        let (lhs, count_str) = s
            .split_once('x')
            .ok_or_else(|| format!("Invalid expansion syntax: {s}"))?;
        let (start_str, step_str) = lhs
            .split_once('+')
            .ok_or_else(|| format!("Invalid expansion syntax: {s}"))?;

        let start: f64 = start_str
            .parse()
            .map_err(|_| format!("Invalid start value: {start_str}"))?;
        let step: f64 = step_str
            .parse()
            .map_err(|_| format!("Invalid step value: {step_str}"))?;
        let count: usize = count_str
            .parse()
            .map_err(|_| format!("Invalid count: {count_str}"))?;

        Ok((0..=count)
            .map(|i| (i as i64, start + step * i as f64))
            .collect())
    } else {
        // Space-separated individual values
        s.split_whitespace()
            .enumerate()
            .filter_map(|(i, v)| if v == "_" { None } else { Some((i as i64, v)) })
            .map(|(i, v)| {
                v.parse::<f64>()
                    .map(|f| (i, f))
                    .map_err(|_| format!("Invalid value '{}'", v))
            })
            .collect()
    }
}

fn parse_expected(line: &str) -> Result<(RangeSample, bool), String> {
    // Parse metric/labels prefix and trailing sample value expression.
    let mut chars = line.chars().peekable();
    let mut metric_part = String::new();

    // Read metric name if present (until { or whitespace)
    while let Some(&c) = chars.peek() {
        if c == '{' || c.is_whitespace() {
            break;
        }
        metric_part.push(chars.next().unwrap());
    }

    // Read full label set if present
    if chars.peek() == Some(&'{') {
        let mut brace_depth = 0;
        for c in chars.by_ref() {
            metric_part.push(c);
            if c == '{' {
                brace_depth += 1;
            } else if c == '}' {
                brace_depth -= 1;
                if brace_depth == 0 {
                    break;
                }
            }
        }

        if brace_depth != 0 {
            return Err(format!("Unbalanced {{ }} in expected: {}", line));
        }
    }

    let value_expr: String = chars.collect::<String>().trim().to_string();
    if value_expr.is_empty() {
        return Err(format!("Missing value in expected: {}", line));
    }

    let (metric_name, label_map) = parse_metric(metric_part.trim())?;

    let labels = build_sorted_labels(metric_name, label_map);
    let samples = parse_expected_samples(&value_expr, line)?;
    let had_braces = line.trim_start().starts_with('{');

    Ok((
        RangeSample {
            labels: Labels::new(labels),
            samples,
        },
        had_braces,
    ))
}

fn build_sorted_labels(metric_name: String, label_map: HashMap<String, String>) -> Vec<Label> {
    let mut labels: Vec<Label> = label_map
        .into_iter()
        .map(|(k, v)| Label::new(k, v))
        .collect();
    if !metric_name.is_empty() {
        labels.push(Label::metric_name(metric_name));
    }
    labels.sort();
    labels
}

fn parse_expected_samples(value_expr: &str, line: &str) -> Result<Vec<Sample>, String> {
    if value_expr.contains('+')
        || value_expr.contains('x')
        || value_expr.contains(char::is_whitespace)
    {
        let parsed = parse_multiple_value_exprs(value_expr)
            .map_err(|e| format!("Invalid value '{}' in expected: {}", value_expr, e))?;
        return Ok(parsed
            .into_iter()
            .map(|(step, value)| Sample::new(step, value))
            .collect());
    }

    let value = value_expr
        .parse::<f64>()
        .map_err(|_| format!("Invalid value '{}' in expected: {}", value_expr, line))?;
    Ok(vec![Sample::new(0, value)])
}

enum ExpectDirective {
    Ordered,
    String(QueryValue),
    Ignored,
}

fn parse_expect_directive(trimmed_line: &str) -> Result<Option<ExpectDirective>, String> {
    if !trimmed_line.starts_with("expect ") {
        return Ok(None);
    }

    if trimmed_line == "expect ordered" {
        return Ok(Some(ExpectDirective::Ordered));
    }

    if let Some(raw) = trimmed_line.strip_prefix("expect string ") {
        let raw = raw.trim();
        let value = unquote_string(raw)
            .map_err(|e| format!("Invalid expected string value '{}': {}", raw, e))?;
        return Ok(Some(ExpectDirective::String(QueryValue::String(value))));
    }

    Ok(Some(ExpectDirective::Ignored))
}

fn parse_bare_scalar_expectation(trimmed_line: &str) -> Option<QueryValue> {
    parse_number(trimmed_line)
        .map(|value| QueryValue::Scalar {
            timestamp_ms: 0,
            value,
        })
        .ok()
}

fn consume_remaining_indented_block(lines: &[&str], line_idx: &mut usize) {
    while *line_idx < lines.len() {
        let next_line = lines[*line_idx];
        if !next_line.starts_with(' ') && !next_line.starts_with('\t') {
            break;
        }
        *line_idx += 1;
    }
}

fn build_expected_query_value(
    expected_ranges: Vec<RangeSample>,
    expected_value: Option<QueryValue>,
    had_label_only_syntax: Vec<bool>,
) -> QueryValue {
    if let Some(qv) = expected_value {
        return qv;
    }

    if expected_ranges.is_empty() {
        return QueryValue::Matrix(vec![]);
    }

    let is_single_implicit_scalar = expected_ranges.len() == 1
        && expected_ranges[0].labels.is_empty()
        && expected_ranges[0].samples.len() == 1
        && had_label_only_syntax.first() != Some(&true);

    if is_single_implicit_scalar {
        let s = &expected_ranges[0].samples[0];
        return QueryValue::Scalar {
            timestamp_ms: s.timestamp,
            value: s.value,
        };
    }

    if expected_ranges.iter().all(|r| r.samples.len() == 1) {
        let vec_samples: Vec<InstantSample> = expected_ranges
            .into_iter()
            .map(|r| {
                let s = r.samples.into_iter().next().unwrap();
                InstantSample {
                    labels: r.labels,
                    timestamp_ms: s.timestamp,
                    value: s.value,
                }
            })
            .collect();
        return QueryValue::Vector(vec_samples);
    }

    QueryValue::Matrix(expected_ranges)
}

fn parse_expectations(lines: &[&str], line_idx: &mut usize) -> Result<(QueryValue, bool), String> {
    let mut expected_ranges: Vec<RangeSample> = Vec::new();
    let mut expect_ordered = false;
    let mut expected_value: Option<QueryValue> = None;
    let mut had_label_only_syntax: Vec<bool> = Vec::new();

    // Collect indented expected result lines
    while *line_idx < lines.len() {
        let next_line = lines[*line_idx];
        if !next_line.starts_with(' ') && !next_line.starts_with('\t') {
            break;
        }

        let trimmed = next_line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            *line_idx += 1;
            continue;
        }

        if let Some(directive) = parse_expect_directive(trimmed)? {
            match directive {
                ExpectDirective::Ordered => {
                    expect_ordered = true;
                    *line_idx += 1;
                    continue;
                }
                ExpectDirective::String(value) => {
                    expected_value = Some(value);
                    *line_idx += 1;
                    consume_remaining_indented_block(lines, line_idx);
                    break;
                }
                ExpectDirective::Ignored => {
                    *line_idx += 1;
                    continue;
                }
            }
        }

        if let Some(scalar_expectation) = parse_bare_scalar_expectation(trimmed) {
            expected_value = Some(scalar_expectation);

            if !expected_ranges.is_empty() {
                return Err(
                    "Cannot mix scalar expectation with range/vector expectations".to_string(),
                );
            }
            // Consume the scalar expectation line so the parser does not
            // treat it as a top-level directive on the next iteration.
            *line_idx += 1;
            break;
        }

        let (range_sample, had_braces) = parse_expected(trimmed)?;
        expected_ranges.push(range_sample);
        had_label_only_syntax.push(had_braces);
        *line_idx += 1;
    }

    let expected =
        build_expected_query_value(expected_ranges, expected_value, had_label_only_syntax);

    Ok((expected, expect_ordered))
}

pub fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();

    // Try using promql_parser's duration parser which handles compound durations like "5m59s"
    if let Ok(duration) = promql_parser::util::parse_duration(s) {
        return Ok(duration);
    }

    // Fall back to suffix-based parsing (strict: requires units)
    if let Some(ms) = s.strip_suffix("ms") {
        ms.parse::<u64>()
            .map(Duration::from_millis)
            .map_err(|e| format!("Invalid duration {s}: {}", e))
    } else if let Some(h) = s.strip_suffix('h') {
        h.parse::<u64>()
            .map(|v| Duration::from_secs(v * 3600))
            .map_err(|e| format!("Invalid duration {s}: {}", e))
    } else if let Some(m) = s.strip_suffix('m') {
        m.parse::<u64>()
            .map(|v| Duration::from_secs(v * 60))
            .map_err(|e| format!("Invalid duration {s}: {}", e))
    } else if let Some(sec) = s.strip_suffix('s') {
        sec.parse::<u64>()
            .map(Duration::from_secs)
            .map_err(|e| format!("Invalid duration {s}: {}", e))
    } else {
        // Allow unitless numbers as seconds (e.g., "0", "100", "100.5")
        if let Ok(secs) = s.parse::<f64>() {
            return Ok(Duration::from_secs_f64(secs));
        }
        Err(format!("Invalid duration {s}: missing unit (ms, s, m, h)"))
    }
}

fn parse_time(s: &str) -> Result<SystemTime, String> {
    // Matches durations with or without units
    // "10s", "5m", "100", "100.5" all valid

    // Try parsing as duration first (e.g., "10s", "5m")
    if let Ok(duration) = parse_duration(s) {
        return Ok(UNIX_EPOCH + duration);
    }

    // Fall back to unitless seconds (e.g., "100", "100.5")
    // This matches @ 100, @ 100s, and @ 1m40s are all equivalent
    match s.parse::<f64>() {
        Ok(secs) => Ok(UNIX_EPOCH + Duration::from_secs_f64(secs)),
        Err(_) => Err(format!(
            "Invalid time '{}': expected duration (10s, 5m) or seconds (100, 100.5)",
            s
        )),
    }
}

// ============================================================================
// Public API
// ============================================================================

pub fn parse_test_file(input: &str) -> Result<Vec<Command>, String> {
    Parser::parse_file(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::promql::QueryValue;

    #[test]
    fn should_parse_clear_command() {
        // given
        let input = "clear";

        // when
        let cmds = Parser::parse_file(input).unwrap();

        // then
        assert_eq!(cmds.len(), 1);
        assert!(matches!(cmds[0], Command::Clear(_)));
    }

    #[test]
    fn should_parse_ignore_command() {
        // given
        let input = "ignore";

        // when
        let cmds = Parser::parse_file(input).unwrap();

        // then
        assert_eq!(cmds.len(), 1);
        assert!(matches!(cmds[0], Command::Ignore(_)));
    }

    #[test]
    fn should_parse_resume_command() {
        // given
        let input = "resume";

        // when
        let cmds = Parser::parse_file(input).unwrap();

        // then
        assert_eq!(cmds.len(), 1);
        assert!(matches!(cmds[0], Command::Resume(_)));
    }

    #[test]
    fn should_parse_load_command() {
        // given
        let input = "load 10s\n  metric{job=\"1\"} 1 2 3";

        // when
        let cmds = Parser::parse_file(input).unwrap();

        // then
        assert_eq!(cmds.len(), 1);
        match &cmds[0] {
            Command::Load(cmd) => {
                assert_eq!(cmd.interval, Duration::from_secs(10));
                assert_eq!(cmd.series.len(), 1);
            }
            _ => panic!("Expected Load command"),
        }
    }

    #[test]
    fn should_parse_eval_instant_command() {
        // given
        let input = "eval instant at 10s metric\n  {job=\"1\"} 5";

        // when
        let cmds = Parser::parse_file(input).unwrap();

        // then
        assert_eq!(cmds.len(), 1);
        match &cmds[0] {
            Command::EvalInstant(cmd) => {
                assert_eq!(cmd.query, "metric");
                match &cmd.expected {
                    QueryValue::Vector(v) => assert_eq!(v.len(), 1),
                    QueryValue::Matrix(m) => assert_eq!(m.len(), 1),
                    _ => panic!("Unexpected expected type"),
                }
                assert!(!cmd.expect_ordered);
            }
            _ => panic!("Expected EvalInstant command"),
        }
    }

    #[test]
    fn should_parse_expect_ordered_directive() {
        // given
        let input = "eval instant at 10s metric\n  expect ordered\n  {job=\"1\"} 5";

        // when
        let cmds = Parser::parse_file(input).unwrap();

        // then
        assert_eq!(cmds.len(), 1);
        match &cmds[0] {
            Command::EvalInstant(cmd) => {
                assert_eq!(cmd.query, "metric");
                assert!(cmd.expect_ordered);
                match &cmd.expected {
                    QueryValue::Vector(v) => assert_eq!(v.len(), 1),
                    QueryValue::Matrix(m) => assert_eq!(m.len(), 1),
                    _ => panic!("Unexpected expected type"),
                }
            }
            _ => panic!("Expected EvalInstant command"),
        }
    }

    #[test]
    fn should_parse_expansion_syntax() {
        // given
        let input = "0+10x5";

        // when
        let vals = parse_values(input).unwrap();

        // then
        assert_eq!(
            vals,
            vec![
                (0, 0.0),
                (1, 10.0),
                (2, 20.0),
                (3, 30.0),
                (4, 40.0),
                (5, 50.0),
            ]
        );
    }

    #[test]
    fn should_use_absolute_step_indices_for_multiple_expressions() {
        // given
        let input = "0+10x3 100+20x2";

        // when
        let vals = parse_multiple_value_exprs(input).unwrap();

        // then
        assert_eq!(
            vals,
            vec![
                (0, 0.0),
                (1, 10.0),
                (2, 20.0),
                (3, 30.0),
                (4, 100.0),
                (5, 120.0),
                (6, 140.0),
            ]
        );
    }

    #[test]
    fn should_reject_invalid_values() {
        // given
        let input = "1 2 invalid 4";

        // when
        let result = parse_values(input);

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid value 'invalid'"));
    }

    #[test]
    fn should_use_absolute_step_offsets_for_multiple_expressions() {
        // given
        let input = "0+10x3 100+5x3";

        // when
        let vals = parse_multiple_value_exprs(input).unwrap();

        // then
        assert_eq!(
            vals,
            vec![
                (0, 0.0),
                (1, 10.0),
                (2, 20.0),
                (3, 30.0),
                (4, 100.0),
                (5, 105.0),
                (6, 110.0),
                (7, 115.0),
            ]
        );
    }

    #[test]
    fn should_produce_sequential_steps_to_preserve_monotonic_timestamps() {
        // given - this demonstrates why we use sequential blocks
        let input = "0+10x3 0+20x2"; // Second expression also starts at step 0

        // when
        let vals = parse_multiple_value_exprs(input).unwrap();

        // then - we produce sequential steps [0,1,2,3,4,5,6]
        // not overlapping [0,1,2,3,0,1,2]
        // This guarantees strictly increasing timestamps when steps are converted
        // to wall-clock time, preventing Gorilla/tsz encoder panics.
        assert_eq!(
            vals,
            vec![
                (0, 0.0),  // First expression
                (1, 10.0), // First expression
                (2, 20.0), // First expression
                (3, 30.0), // First expression
                (4, 0.0),  // Second expression (step 4, not 0)
                (5, 20.0), // Second expression (step 5, not 1)
                (6, 40.0), // Second expression (step 6, not 2)
            ]
        );

        // Without sequential blocks, this would produce:
        // [(0, 0.0), (1, 10.0), (2, 20.0), (3, 30.0), (0, 0.0), (1, 20.0), (2, 40.0)]
        // which has backwards timestamps after sorting
    }

    #[test]
    fn should_parse_duration_with_units() {
        // given / when / then
        assert_eq!(parse_duration("10s").unwrap(), Duration::from_secs(10));
        assert_eq!(parse_duration("1m").unwrap(), Duration::from_secs(60));
        assert_eq!(parse_duration("1h").unwrap(), Duration::from_secs(3600));
        assert_eq!(
            parse_duration("1000ms").unwrap(),
            Duration::from_millis(1000)
        );
    }

    #[test]
    fn should_parse_time_with_and_without_units() {
        // given
        let with_unit = "10s";
        let without_unit = "10";

        // when
        let t1 = parse_time(with_unit).unwrap();
        let t2 = parse_time(without_unit).unwrap();

        // then
        assert_eq!(t1, t2);
    }

    #[test]
    fn should_parse_metric_with_labels() {
        // given
        let input = "load 5m\n  metric{job=\"test\",instance=\"localhost\"} 1 2 3";

        // when
        let cmds = Parser::parse_file(input).unwrap();

        // then
        match &cmds[0] {
            Command::Load(cmd) => {
                assert_eq!(
                    cmd.series[0].labels.get("__name__"),
                    Some(&"metric".to_string())
                );
                assert_eq!(cmd.series[0].labels.get("job"), Some(&"test".to_string()));
                assert_eq!(
                    cmd.series[0].labels.get("instance"),
                    Some(&"localhost".to_string())
                );
            }
            _ => panic!("Expected Load command"),
        }
    }

    #[test]
    fn should_parse_label_only_selector() {
        // given
        let input = "eval instant at 10s {job=\"test\"}\n  {job=\"test\"} 5";

        // when
        let cmds = Parser::parse_file(input).unwrap();

        // then
        match &cmds[0] {
            Command::EvalInstant(cmd) => {
                assert_eq!(cmd.query, "{job=\"test\"}");
                match &cmd.expected {
                    QueryValue::Vector(v) => assert_eq!(v[0].labels.get("job"), Some("test")),
                    QueryValue::Matrix(m) => assert_eq!(m[0].labels.get("job"), Some("test")),
                    _ => panic!("Unexpected expected type"),
                }
            }
            _ => panic!("Expected EvalInstant command"),
        }
    }

    #[test]
    fn should_reject_unbalanced_braces() {
        // given
        let input = "load 5m\n  metric{job=\"test\" 1 2 3";

        // when
        let result = Parser::parse_file(input);

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unbalanced"));
    }

    #[test]
    fn should_handle_multiple_tabs_between_time_and_query() {
        // given
        let input = "eval instant at 10s\t\tmetric\n  {job=\"test\"} 5";

        // when
        let cmds = Parser::parse_file(input).unwrap();

        // then
        match &cmds[0] {
            Command::EvalInstant(cmd) => {
                assert_eq!(cmd.query, "metric");
                assert_eq!(cmd.time, UNIX_EPOCH + Duration::from_secs(10));
            }
            _ => panic!("Expected EvalInstant command"),
        }
    }

    #[test]
    fn should_trim_leading_whitespace_from_query() {
        // given
        let input = "eval instant at 10s \t  metric{job=\"test\"}\n  {job=\"test\"} 5";

        // when
        let cmds = Parser::parse_file(input).unwrap();

        // then
        match &cmds[0] {
            Command::EvalInstant(cmd) => {
                assert_eq!(cmd.query, "metric{job=\"test\"}");
            }
            _ => panic!("Expected EvalInstant command"),
        }
    }
}
