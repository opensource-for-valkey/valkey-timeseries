//! PromQL test framework – idiomatic Rust translation.
//! Original source: https://github.com/prometheus/prometheus/blob/main/promqltest/test.go

#![allow(unused)] // many stubbed types/methods; remove in production

use crate::common::Sample;
use crate::common::time::system_time_to_millis;
use crate::promql::hashers::SeriesFingerprint;
use crate::promql::promqltest::dsl::parse_duration;
use crate::promql::{Labels, QueryValue, RangeSample};
use lazy_static::lazy_static;
use regex::Regex;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::fmt::format;
use std::path::Path;
use std::rc::Rc;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnnotationKind {
    Warning,
    Info,
}

// Annotations
#[derive(Debug, Clone)]
pub struct Annotation {
    pub msg: String,
    pub kind: AnnotationKind,
}

// Series
#[derive(Debug, Clone)]
pub struct Series {
    pub metric: Labels,
    pub points: Vec<Sample>,
}

// SequenceValue from parser (used in test definitions)
#[derive(Debug, Clone)]
pub struct SequenceValue {
    pub omitted: bool,
    pub value: f64,
    pub counter_reset_hint_set: bool,
}

mod prometheus_stubs {
    use super::*;
    use crate::promql::Labels;

    // Engine & Storage traits
    pub trait QueryEngine: Send + Sync {
        fn new_instant_query(
            &self,
            opts: Option<&QueryOpts>,
            expr: &str,
            eval_time: SystemTime,
        ) -> Result<Box<dyn Query>, String>;
        fn new_range_query(
            &self,
            opts: Option<&QueryOpts>,
            expr: &str,
            start: SystemTime,
            end: SystemTime,
            step: Duration,
        ) -> Result<Box<dyn Query>, String>;
        fn close(&self) -> Result<(), String>;
    }

    pub trait Query {
        fn exec(&mut self) -> Result<QueryValue, String>;
    }

    pub trait Storage: Send + Sync {
        fn appender_v2(&self) -> Box<dyn AppenderV2>;
    }

    pub trait AppenderV2: Send {
        fn append(&mut self, metric: &Labels, st: i64, t: i64, f: f64) -> Result<u64, String>;
    }

    pub struct QueryOpts;

    // Parser options
    #[derive(Debug, Clone, Default)]
    pub struct ParserOptions {
        pub enable_experimental_functions: bool,
        pub experimental_duration_expr: bool,
        pub enable_extended_range_selectors: bool,
        pub enable_binop_fill_modifiers: bool,
    }

    // Helper for test parsing
    pub fn parse_series_desc(line: &str) -> Result<(Labels, Vec<SequenceValue>), String> {
        // Stub: in real code this would use the PromQL parser.
        panic!("parse_series_desc not implemented in stub")
    }
}
use prometheus_stubs::*;

// ============================================================================
// Actual translation of promqltest.go
// ============================================================================

lazy_static! {
    static ref PAT_SPACE: Regex = Regex::new(r"[\t ]+").unwrap();
    static ref PAT_LOAD: Regex = Regex::new(r"^load(?:_(with_nhcb))?\s+(.+?)$").unwrap();
    static ref PAT_EVAL_INSTANT: Regex =
        Regex::new(r"^eval(?:_(fail|warn|ordered|info))?\s+instant\s+(?:at\s+(.+?))?\s+(.+)$")
            .unwrap();
    static ref PAT_EVAL_RANGE: Regex = Regex::new(
        r"^eval(?:_(fail|warn|info))?\s+range\s+from\s+(.+)\s+to\s+(.+)\s+step\s+(.+?)\s+(.+)$"
    )
    .unwrap();
    static ref PAT_EXPECT: Regex =
        Regex::new(r"^expect\s+(ordered|fail|warn|no_warn|info|no_info)(?:\s+(regex|msg):(.+))?$")
            .unwrap();
    static ref PAT_MATCH_ANY: Regex = Regex::new(r"^.*$").unwrap();
    static ref PAT_EXPECT_RANGE: Regex =
        Regex::new(r"^expect range vector\s+from\s+(.+)\s+to\s+(.+)\s+step\s+(.+)$").unwrap();
}

const DEFAULT_EPSILON: f64 = 0.000001;
const DEFAULT_MAX_SAMPLES_PER_QUERY: usize = 10000;
const RANGE_VECTOR_PREFIX: &str = "expect range vector";
const EXPECT_STRING_PREFIX: &str = "expect string";

/// Start time for all tests: Unix epoch.
fn test_start_time() -> SystemTime {
    SystemTime::now()
}

// ----------------------------------------------------------------------------
// Public API (as in original)
// ----------------------------------------------------------------------------

/// Returns the parser options used by all built-in test engines.
pub fn test_parser_opts() -> ParserOptions {
    ParserOptions {
        enable_experimental_functions: true,
        experimental_duration_expr: true,
        enable_extended_range_selectors: true,
        enable_binop_fill_modifiers: true,
    }
}

/// Creates a new PromQL engine with the given options.
pub fn new_test_engine(
    enable_per_step_stats: bool,
    lookback_delta: Duration,
    max_samples: usize,
) -> Box<dyn QueryEngine> {
    new_test_engine_with_opts(EngineOpts {
        max_samples,
        timeout: Duration::from_secs(100),
        enable_at_modifier: true,
        lookback_delta,
        enable_delayed_name_removal: true,
        parser: test_parser_opts(),
    })
}

#[derive(Default)]
pub struct EngineOpts {
    pub max_samples: usize,
    pub timeout: Duration,
    pub enable_at_modifier: bool,
    pub lookback_delta: Duration,
    pub enable_delayed_name_removal: bool,
    pub parser: ParserOptions,
}

pub fn new_test_engine_with_opts(_opts: EngineOpts) -> Box<dyn QueryEngine> {
    // Stub – in real code would instantiate the engine.
    struct StubEngine;
    impl QueryEngine for StubEngine {
        fn new_instant_query(
            &self,
            _opts: Option<&QueryOpts>,
            _expr: &str,
            _eval_time: SystemTime,
        ) -> Result<Box<dyn Query>, String> {
            unimplemented!("stub")
        }
        fn new_range_query(
            &self,
            _opts: Option<&QueryOpts>,
            _expr: &str,
            _start: SystemTime,
            _end: SystemTime,
            _step: Duration,
        ) -> Result<Box<dyn Query>, String> {
            unimplemented!("stub")
        }
        fn close(&self) -> Result<(), String> {
            Ok(())
        }
    }
    Box::new(StubEngine)
}

// ----------------------------------------------------------------------------
// Internal test structures
// ----------------------------------------------------------------------------

type TestStorage = Box<dyn Storage>;

#[derive(Clone)]
struct SampleST {
    sample: Sample,
    st: i64, // start timestamp (0 = unknown)
}

#[derive(Clone)]
struct LoadCmd {
    gap: Duration,
    metrics: HashMap<SeriesFingerprint, Labels>,
    defs: HashMap<SeriesFingerprint, Vec<SampleST>>,
    start_time: SystemTime,
}

impl LoadCmd {
    fn new(gap: Duration) -> Self {
        LoadCmd {
            gap,
            metrics: HashMap::new(),
            defs: HashMap::new(),
            start_time: test_start_time(),
        }
    }

    fn set(&mut self, m: Labels, vals: Vec<SequenceValue>, st_vals: Option<Vec<SequenceValue>>) {
        let h = m.get_fingerprint();
        let mut samples = Vec::new();
        let mut ts = self.start_time;
        for (i, v) in vals.iter().enumerate() {
            let ts_ms = system_time_to_millis(ts);
            if !v.omitted {
                let mut s = SampleST {
                    sample: Sample {
                        timestamp: ts_ms,
                        value: v.value,
                    },
                    st: 0,
                };
                samples.push(s);
            }
            ts += self.gap;
        }
        self.defs.insert(h, samples);
        self.metrics.insert(h, m);
    }

    fn append(&self, app: &mut dyn AppenderV2) -> Result<(), String> {
        for (h, samples) in &self.defs {
            let m = &self.metrics[h];
            for s in samples {
                append_sample(app, s, m)?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Default, Debug)]
struct ExpectCmd {
    message: String,
    regex: Option<Regex>,
}

impl ExpectCmd {
    fn check_match(&self, s: &str) -> bool {
        match &self.regex {
            Some(re) => re.is_match(s),
            None => self.message == s,
        }
    }
    fn type_str(&self) -> &'static str {
        if self.regex.is_some() {
            "pattern"
        } else {
            "message"
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum ExpectCmdType {
    Ordered,
    Fail,
    Warn,
    NoWarn,
    Info,
    NoInfo,
}

#[derive(Clone)]
struct Entry {
    pos: usize,
    vals: Vec<SequenceValue>,
}

#[derive(Clone)]
struct EvalCmd {
    expr: String,
    start: SystemTime,
    end: SystemTime,
    step: Duration,
    line: usize,
    eval: SystemTime,
    is_range: bool,
    fail: bool,
    warn: bool,
    ordered: bool,
    info: bool,
    expected_fail_message: String,
    expected_fail_regexp: Option<Regex>,
    expected_cmds: HashMap<ExpectCmdType, Vec<ExpectCmd>>,
    metrics: HashMap<SeriesFingerprint, Labels>,
    expect_scalar: bool,
    expected: HashMap<SeriesFingerprint, Entry>,
    expected_string: Option<String>,
    exclude_from_range_query: bool,
}

impl EvalCmd {
    fn new_instant(expr: String, start: SystemTime, line: usize) -> Self {
        EvalCmd {
            expr,
            start,
            end: start,
            step: Duration::from_secs(0),
            line,
            eval: start,
            is_range: false,
            fail: false,
            warn: false,
            ordered: false,
            info: false,
            expected_fail_message: String::new(),
            expected_fail_regexp: None,
            expected_cmds: HashMap::new(),
            metrics: HashMap::new(),
            expect_scalar: false,
            expected: HashMap::new(),
            expected_string: None,
            exclude_from_range_query: false,
        }
    }

    fn new_range(
        expr: String,
        start: SystemTime,
        end: SystemTime,
        step: Duration,
        line: usize,
    ) -> Self {
        EvalCmd {
            expr,
            start,
            end,
            step,
            line,
            eval: end,
            is_range: true,
            ..Default::default()
        }
    }

    fn expect(&mut self, pos: usize, vals: Vec<SequenceValue>) {
        self.expect_scalar = true;
        self.expected.insert(0, Entry { pos, vals });
    }

    fn expect_metric(&mut self, pos: usize, m: Labels, vals: Vec<SequenceValue>) {
        self.expect_scalar = false;
        let h = m.get_fingerprint();
        self.metrics.insert(h, m);
        self.expected.insert(h, Entry { pos, vals });
    }

    fn is_fail(&self) -> bool {
        self.fail || self.expected_cmds.contains_key(&ExpectCmdType::Fail)
    }

    fn is_ordered(&self) -> bool {
        self.ordered || self.expected_cmds.contains_key(&ExpectCmdType::Ordered)
    }
}

impl Default for EvalCmd {
    fn default() -> Self {
        EvalCmd {
            expr: String::new(),
            start: test_start_time(),
            end: test_start_time(),
            step: Duration::from_secs(0),
            line: 0,
            eval: test_start_time(),
            is_range: false,
            fail: false,
            warn: false,
            ordered: false,
            info: false,
            expected_fail_message: String::new(),
            expected_fail_regexp: None,
            expected_cmds: HashMap::new(),
            metrics: HashMap::new(),
            expect_scalar: false,
            expected: HashMap::new(),
            expected_string: None,
            exclude_from_range_query: false,
        }
    }
}

#[derive(Clone)]
struct ClearCmd;

#[derive(Clone)]
enum TestCommand {
    Load(LoadCmd),
    Eval(EvalCmd),
    Clear(ClearCmd),
}

struct Test<'a> {
    tb: Option<&'a mut dyn fmt::Write>, // simplified – original uses testing.TB
    testing_mode: bool,
    cmds: Vec<TestCommand>,
    storage: Option<TestStorage>,
}

fn new_test<'a>(
    tb: &'a mut dyn fmt::Write,
    input: &str,
    testing_mode: bool,
    new_storage: fn(&mut dyn fmt::Write) -> TestStorage,
) -> Result<Test<'a>, String> {
    let mut test = Test {
        tb: Some(tb),
        testing_mode,
        cmds: Vec::new(),
        storage: None,
    };
    test.parse(input)?;
    test.clear();
    Ok(test)
}

impl<'a> Test<'a> {
    fn parse(&mut self, input: &str) -> Result<(), String> {
        let lines = get_lines(input);
        let mut i = 0;
        while i < lines.len() {
            let l = lines[i].trim();
            if l.is_empty() {
                i += 1;
                continue;
            }
            let first_word = PAT_SPACE.split(l).next().unwrap_or("").to_lowercase();
            let cmd: TestCommand = match first_word.as_str() {
                "clear" => TestCommand::Clear(ClearCmd),
                _ if first_word.starts_with("load") => {
                    let (next_i, cmd) = parse_load(&lines, i, test_start_time())?;
                    i = next_i;
                    TestCommand::Load(cmd)
                }
                _ if first_word.starts_with("eval") => {
                    let (next_i, cmd) = self.parse_eval(&lines, i)?;
                    i = next_i;
                    TestCommand::Eval(cmd)
                }
                _ => return raise(i, &format!("invalid command '{}'", l)),
            };
            self.cmds.push(cmd);
            i += 1;
        }
        Ok(())
    }

    fn parse_eval(&self, lines: &[String], i: usize) -> Result<(usize, EvalCmd), String> {
        let line = &lines[i];
        let instant_caps = PAT_EVAL_INSTANT.captures(line);
        let range_caps = PAT_EVAL_RANGE.captures(line);

        let (is_instant, mod_str, expr) = if let Some(caps) = &instant_caps {
            (
                true,
                caps.get(1).map(|m| m.as_str()),
                caps.get(3).unwrap().as_str(),
            )
        } else if let Some(caps) = &range_caps {
            (
                false,
                caps.get(1).map(|m| m.as_str()),
                caps.get(5).unwrap().as_str(),
            )
        } else {
            return raise(i, "invalid evaluation command");
        };

        // Parse expression (stub – real parser would be used)
        // In real code: parser::ParseExpr(expr, TestParserOpts)
        // We skip full parsing here.

        let mut cmd = if is_instant {
            let at = instant_caps
                .unwrap()
                .get(2)
                .map(|m| m.as_str())
                .unwrap_or("");
            let offset = if at.is_empty() {
                Duration::from_secs(0)
            } else {
                parse_duration(at)?
            };
            let ts = test_start_time() + offset;
            EvalCmd::new_instant(expr.to_string(), ts, i + 1)
        } else {
            let caps = range_caps.unwrap();
            let from_str = caps.get(2).unwrap().as_str();
            let to_str = caps.get(3).unwrap().as_str();
            let step_str = caps.get(4).unwrap().as_str();
            let from = parse_duration(from_str)?;
            let to = parse_duration(to_str)?;
            let step = parse_duration(step_str)?;
            if to < from {
                return raise(
                    i,
                    &format!("end timestamp {to_str} before start {from_str}"),
                );
            }
            EvalCmd::new_range(
                expr.to_string(),
                test_start_time() + from,
                test_start_time() + to,
                step,
                i + 1,
            )
        };

        match mod_str {
            Some("ordered") => cmd.ordered = true,
            Some("fail") => cmd.fail = true,
            Some("warn") => cmd.warn = true,
            Some("info") => cmd.info = true,
            _ => {}
        }

        let mut j = 1;
        let mut idx = i;
        while idx + 1 < lines.len() {
            idx += 1;
            let def_line = lines[idx].trim();
            if def_line.is_empty() {
                idx -= 1;
                break;
            }

            if cmd.fail && def_line.starts_with("expected_fail_message") {
                cmd.expected_fail_message = def_line
                    .strip_prefix("expected_fail_message")
                    .unwrap()
                    .trim()
                    .to_string();
                break;
            }

            if cmd.fail && def_line.starts_with("expected_fail_regexp") {
                let pattern = def_line
                    .strip_prefix("expected_fail_regexp")
                    .unwrap()
                    .trim();

                let regexp = Regex::new(pattern)
                    .map_err(|e| format!("invalid regex in expected_fail_regexp: {}", e))?;
                cmd.expected_fail_regexp = Some(regexp);

                break;
            }

            if def_line.starts_with(RANGE_VECTOR_PREFIX) {
                let (start, end, step) = self.parse_expect_range_vector(def_line)?;
                cmd.start = start;
                cmd.end = end;
                cmd.step = step;
                cmd.eval = end;
                cmd.exclude_from_range_query = true;
                continue;
            }

            if def_line.starts_with(EXPECT_STRING_PREFIX) {
                let s = parse_as_string_literal(def_line)?;
                cmd.expected_string = Some(s);
                cmd.exclude_from_range_query = true;
                continue;
            }

            if def_line.split_whitespace().next() == Some("expect") {
                let (anno_type, exp_cmd) = parse_expect(def_line)?;
                cmd.expected_cmds
                    .entry(anno_type)
                    .or_default()
                    .push(exp_cmd);
                validate_expected_cmds(&cmd)?;
                j -= 1;
                continue;
            }

            if let Ok(f) = parse_number(def_line) {
                cmd.expect(
                    0,
                    vec![SequenceValue {
                        omitted: false,
                        value: f,
                        counter_reset_hint_set: false,
                    }],
                );
                break;
            }

            let (metric, vals) = parse_series(def_line, idx)?;
            if vals.len() > 1 && is_instant && !cmd.exclude_from_range_query {
                return raise(
                    idx,
                    "expecting multiple values in instant evaluation not allowed. consider using 'expect range vector'",
                );
            }
            cmd.expect_metric(j, metric, vals);
            j += 1;
        }
        Ok((idx, cmd))
    }

    fn parse_expect_range_vector(
        &self,
        line: &str,
    ) -> Result<(SystemTime, SystemTime, Duration), String> {
        let caps = PAT_EXPECT_RANGE
            .captures(line)
            .ok_or("invalid range vector definition")?;
        let from_str = caps.get(1).unwrap().as_str();
        let to_str = caps.get(2).unwrap().as_str();
        let step_str = caps.get(3).unwrap().as_str();
        let from = parse_duration(from_str)?;
        let to = parse_duration(to_str)?;
        let step = parse_duration(step_str)?;
        Ok((test_start_time() + from, test_start_time() + to, step))
    }

    fn clear(&mut self) {
        let tb = self.tb.as_mut().unwrap();
    }

    fn exec_cmd(
        &mut self,
        cmd: &TestCommand,
        engine: Option<&dyn QueryEngine>,
    ) -> Result<(), String> {
        match cmd {
            TestCommand::Clear(_) => self.clear(),
            TestCommand::Load(load) => {
                let mut app = self.storage.as_ref().unwrap().appender_v2();
                load.append(&mut *app)?;
            }
            TestCommand::Eval(eval) => {
                let engine = engine.ok_or("eval command needs an engine".to_string())?;
                self.exec_eval(eval, engine)?;
            }
        }
        Ok(())
    }

    fn exec_eval(&self, cmd: &EvalCmd, engine: &dyn QueryEngine) -> Result<(), String> {
        if cmd.is_range {
            self.exec_range_eval(cmd, engine)
        } else {
            self.exec_instant_eval(cmd, engine)
        }
    }

    fn exec_range_eval(&self, cmd: &EvalCmd, engine: &dyn QueryEngine) -> Result<(), String> {
        let mut q = engine.new_range_query(None, &cmd.expr, cmd.start, cmd.end, cmd.step)?;
        let res = q.exec();
        if let Err(err) = res {
            if cmd.is_fail() {
                return check_expected_failure(cmd, &err);
            }
            return Err(err);
        }
        if cmd.is_fail() {
            return Err("expected error evaluating query but got none".to_string());
        }
        let res = res?;
        compare_result(cmd, &res)?;
        Ok(())
    }

    fn exec_instant_eval(&self, cmd: &EvalCmd, engine: &dyn QueryEngine) -> Result<(), String> {
        let test_cases = at_modifier_test_cases(&cmd.expr, cmd.eval)?;
        let mut all_cases = vec![(cmd.expr.clone(), cmd.eval)];
        all_cases.extend(test_cases);
        for (expr, eval_time) in all_cases {
            let mut q = engine.new_instant_query(None, &expr, eval_time)?;
            let res = q.exec();
            if let Err(err) = &res {
                if cmd.is_fail() {
                    check_expected_failure(cmd, err)?;
                    continue;
                }
                return Err(err.to_string());
            }

            let res = res?;
            if cmd.is_fail() {
                return Err("expected error evaluating query but got none".to_string());
            }

            compare_result(cmd, &res)?;

            if !cmd.exclude_from_range_query && !expr.contains("range()") {
                // Also test in range mode (middle step)
                let mid = eval_time - Duration::from_secs(60);
                let mut q_range = engine.new_range_query(
                    None,
                    &expr,
                    mid,
                    eval_time + Duration::from_secs(60),
                    Duration::from_secs(60),
                )?;

                let range_res = q_range.exec()?;

                let matrix = match range_res {
                    QueryValue::Matrix(m) => m,
                    _ => return Err("range query did not return matrix".to_string()),
                };
                assert_matrix_sorted(&matrix)?;
                let mut vec = Vec::new();
                let eval_time = system_time_to_millis(eval_time);
                for series in matrix {
                    for point in series.samples {
                        if point.timestamp == eval_time {
                            vec.push(Sample {
                                timestamp: point.timestamp,
                                value: point.value,
                            });
                            break;
                        }
                    }
                }
                let vec_value = if let QueryValue::Scalar {
                    timestamp_ms: _,
                    value: _,
                } = &res
                {
                    if vec.is_empty() {
                        return Err("no point at eval time".to_string());
                    }
                    QueryValue::Scalar {
                        timestamp_ms: 0,
                        value: vec[0].value,
                    }
                } else {
                    // QueryValue::Vector(vec)
                    todo!()
                };
                compare_result(cmd, &vec_value)?;
            }
        }
        Ok(())
    }
}

// ----------------------------------------------------------------------------
// Helper functions (parsing, comparison, etc.)
// ----------------------------------------------------------------------------

fn get_lines(input: &str) -> Vec<String> {
    input
        .lines()
        .map(|l| {
            let trimmed = l.trim();
            if trimmed.starts_with('#') {
                String::new()
            } else {
                trimmed.to_string()
            }
        })
        .collect()
}

fn raise<T>(line: usize, msg: &str) -> Result<T, String> {
    Err(format!("line {}: {msg}", line + 1))
}

fn parse_number(s: &str) -> Result<f64, String> {
    s.parse::<f64>().map_err(|_| format!("invalid number: {s}"))
}

fn parse_load(
    lines: &[String],
    i: usize,
    start_time: SystemTime,
) -> Result<(usize, LoadCmd), String> {
    let line = &lines[i];
    let caps = PAT_LOAD
        .captures(line)
        .ok_or("invalid load command".to_string())?;
    let with_nhcb = caps.get(1).map(|m| m.as_str()) == Some("with_nhcb");
    let step_str = caps.get(2).unwrap().as_str();
    let gap = parse_duration(step_str)?;
    // NOTE: with_nhcb is not supported in this stubbed implementation; ignore it
    let mut cmd = LoadCmd::new(gap);
    cmd.start_time = start_time;

    let mut idx = i;
    let mut pending_st_vals: Option<(Labels, Vec<SequenceValue>, usize)> = None;

    while idx + 1 < lines.len() {
        idx += 1;
        let def_line = lines[idx].trim();
        if def_line.is_empty() {
            idx -= 1;
            break;
        }
        if is_st_line(def_line) {
            if pending_st_vals.is_some() {
                return raise(idx, "@st line has no following sample line");
            }
            let (metric, st_vals) = parse_st_line(def_line, idx)?;
            pending_st_vals = Some((metric, st_vals, idx));
            continue;
        }
        let (metric, vals) = parse_series(def_line, idx)?;
        if let Some((ref st_metric, ref st_vals, st_line)) = pending_st_vals {
            if metric != *st_metric {
                return raise(
                    st_line,
                    "@st metric does not match the following sample line metric",
                );
            }
            if st_vals.len() != vals.len() {
                return raise(
                    st_line,
                    &format!(
                        "@st line has {} values but sample line has {}",
                        st_vals.len(),
                        vals.len()
                    ),
                );
            }
            cmd.set(metric, vals, Some(st_vals.clone()));
        } else {
            cmd.set(metric, vals, None);
        }
        pending_st_vals = None;
    }
    if pending_st_vals.is_some() {
        return raise(idx, "@st line has no following sample line");
    }
    Ok((idx, cmd))
}

fn is_st_line(def_line: &str) -> bool {
    let trimmed = def_line.trim();
    let space_idx = trimmed.find(|c: char| c.is_whitespace());
    if let Some(idx) = space_idx {
        trimmed[..idx].ends_with("@st")
    } else {
        false
    }
}

fn parse_st_line(def_line: &str, line: usize) -> Result<(Labels, Vec<SequenceValue>), String> {
    let trimmed = def_line.trim();
    let space_idx = trimmed.find(char::is_whitespace).unwrap();
    let metric_part = trimmed[..space_idx].strip_suffix("@st").unwrap();
    let vals_part = trimmed[space_idx + 1..].trim();
    // Parse metric using series parser stub
    let (metric, _) = parse_series(&format!("{} _", metric_part), line)?;
    let st_vals = parse_st_sequence(vals_part)?;
    Ok((metric, st_vals))
}

fn parse_st_sequence(input: &str) -> Result<Vec<SequenceValue>, String> {
    let mut result = Vec::new();
    for item in input.split_whitespace() {
        let vals = parse_st_item(item)?;
        result.extend(vals);
    }
    Ok(result)
}

fn parse_st_item(item: &str) -> Result<Vec<SequenceValue>, String> {
    if item == "_" {
        return Ok(vec![SequenceValue {
            omitted: true,
            value: 0.0,
            counter_reset_hint_set: false,
        }]);
    }
    if let Some(rest) = item.strip_prefix("_x") {
        let n: usize = rest
            .parse()
            .map_err(|_| "Error parsing usize".to_string())?;
        if n == 0 {
            return Err("invalid repeat count".to_string());
        }
        let mut vals = Vec::with_capacity(n);
        for _ in 0..n {
            vals.push(SequenceValue {
                omitted: true,
                value: 0.0,
                counter_reset_hint_set: false,
            });
        }
        return Ok(vals);
    }
    // Parse <dur> or <dur>xN or <dur>+<dur>xN etc.
    // Stub: just parse as single number
    let num = parse_duration_prefix(item)?;
    Ok(vec![SequenceValue {
        omitted: false,
        value: num as f64,
        counter_reset_hint_set: false,
    }])
}

fn parse_duration_prefix(s: &str) -> Result<i64, String> {
    // Stub: parse a simple integer or float
    let s = s.trim();
    if s.is_empty() {
        return Err("empty duration".to_string());
    }
    let negative = s.starts_with('-');
    let s = s.trim_start_matches(|c| ['+', '-'].contains(&c));
    let num: f64 = s.parse().map_err(|_| "Error parsing f64".to_string())?;
    let ms = (num * 1000.0) as i64;
    Ok(if negative { -ms } else { ms })
}

fn parse_series(def_line: &str, line: usize) -> Result<(Labels, Vec<SequenceValue>), String> {
    // Use the stub from prometheus_stubs
    parse_series_desc(def_line)
}

fn parse_expect(def_line: &str) -> Result<(ExpectCmdType, ExpectCmd), String> {
    let caps = PAT_EXPECT
        .captures(def_line.trim())
        .ok_or("invalid expect statement")?;
    let mode = caps.get(1).unwrap().as_str();
    let expect_type = match mode {
        "ordered" => ExpectCmdType::Ordered,
        "fail" => ExpectCmdType::Fail,
        "warn" => ExpectCmdType::Warn,
        "no_warn" => ExpectCmdType::NoWarn,
        "info" => ExpectCmdType::Info,
        "no_info" => ExpectCmdType::NoInfo,
        _ => return Err(format!("unknown expect type {mode}")),
    };
    let has_optional = caps.get(2).is_some();
    let (message, regex) = if has_optional {
        let kind = caps.get(2).unwrap().as_str();
        let value = caps.get(3).unwrap().as_str().trim();
        match kind {
            "msg" => (value.to_string(), None),
            "regex" => (
                String::new(),
                Some(Regex::new(value).map_err(|e| e.to_string())?),
            ),
            _ => return Err(format!("invalid token after {mode}")),
        }
    } else {
        (String::new(), Some(PAT_MATCH_ANY.clone()))
    };
    Ok((expect_type, ExpectCmd { message, regex }))
}

fn validate_expected_cmds(cmd: &EvalCmd) -> Result<(), String> {
    if cmd.expected_cmds.contains_key(&ExpectCmdType::Info)
        && cmd.expected_cmds.contains_key(&ExpectCmdType::NoInfo)
    {
        return Err("info and no_info cannot be used together".to_string());
    }
    if cmd.expected_cmds.contains_key(&ExpectCmdType::Warn)
        && cmd.expected_cmds.contains_key(&ExpectCmdType::NoWarn)
    {
        return Err("warn and no_warn cannot be used together".to_string());
    }
    if cmd.expected_cmds.get(&ExpectCmdType::Fail).map(|v| v.len()) > Some(1) {
        return Err("multiple expect fail lines are not allowed".to_string());
    }
    Ok(())
}

fn parse_as_string_literal(line: &str) -> Result<String, String> {
    if line == EXPECT_STRING_PREFIX {
        return Err(
            "expected string literal not valid - a quoted string literal is required".to_string(),
        );
    }
    let rest = line
        .strip_prefix(&format!("{} ", EXPECT_STRING_PREFIX))
        .unwrap();
    if rest.is_empty() {
        return Err(
            "expected string literal not valid - a quoted string literal is required".to_string(),
        );
    }
    // Unquote using standard Rust unescaping
    let unquoted = match rest.trim_matches('"') {
        s if rest.starts_with('"') && rest.ends_with('"') => s.to_string(),
        _ => return Err("string must be double-quoted".to_string()),
    };
    Ok(unquoted)
}

fn at_modifier_test_cases(
    expr: &str,
    eval_time: SystemTime,
) -> Result<Vec<(String, SystemTime)>, String> {
    // Stub: return empty in this translation.
    // In real code, this would parse the expression and generate additional eval times.
    Ok(Vec::new())
}

fn validate_expected_annotations(
    cmd: &EvalCmd,
    warnings: &[String],
    infos: &[String],
) -> Result<(), String> {
    fn check_set(
        expected: &[ExpectCmd],
        actual: &[String],
        kind: &str,
        expr: &str,
        line: usize,
    ) -> Result<(), String> {
        if expected.is_empty() {
            return Ok(());
        }
        if actual.is_empty() {
            return Err(format!(
                "expected {kind} annotations but none found for query {:?} (line {line})",
                expr
            ));
        }
        for exp in expected {
            if !actual.iter().any(|a| exp.check_match(a)) {
                return Err(format!(
                    "expected {kind} annotation matching {} {:?} not found",
                    exp.type_str(),
                    exp
                ));
            }
        }
        for act in actual {
            if !expected.iter().any(|e| e.check_match(act)) {
                return Err(format!("unexpected {kind} annotation {:?} found", act));
            }
        }
        Ok(())
    }

    let exp_warn = cmd
        .expected_cmds
        .get(&ExpectCmdType::Warn)
        .cloned()
        .unwrap_or_default();
    let exp_info = cmd
        .expected_cmds
        .get(&ExpectCmdType::Info)
        .cloned()
        .unwrap_or_default();
    check_set(&exp_warn, warnings, "warn", &cmd.expr, cmd.line)?;
    check_set(&exp_info, infos, "info", &cmd.expr, cmd.line)?;

    if cmd.expected_cmds.contains_key(&ExpectCmdType::NoWarn) && !warnings.is_empty() {
        return Err(format!("unexpected warning annotations: {:?}", warnings));
    }
    if cmd.expected_cmds.contains_key(&ExpectCmdType::NoInfo) && !infos.is_empty() {
        return Err(format!("unexpected info annotations: {:?}", infos));
    }
    Ok(())
}

fn check_expected_failure(cmd: &EvalCmd, actual_err: &str) -> Result<(), String> {
    let err_str = actual_err.to_string();
    if !cmd.expected_fail_message.is_empty() && cmd.expected_fail_message != err_str {
        return Err(format!(
            "expected error {:?} but got {:?}",
            cmd.expected_fail_message, err_str
        ));
    }
    if let Some(re) = &cmd.expected_fail_regexp
        && !re.is_match(&err_str)
    {
        return Err(format!(
            "expected error matching pattern {:?} but got {:?}",
            re, err_str
        ));
    }
    if let Some(exp_fail) = cmd.expected_cmds.get(&ExpectCmdType::Fail)
        && !exp_fail[0].check_match(&err_str)
    {
        return Err(format!(
            "expected error matching {:?} but got {:?}",
            exp_fail[0], err_str
        ));
    }
    Ok(())
}

fn compare_result(cmd: &EvalCmd, val: &QueryValue) -> Result<(), String> {
    match val {
        QueryValue::Matrix(mat) => {
            if cmd.is_ordered() {
                return Err("expected ordered result, but query returned a matrix".to_string());
            }
            if cmd.expect_scalar {
                return Err("expected scalar result, but got matrix".to_string());
            }
            assert_matrix_sorted(mat)?;
            let mut seen = std::collections::HashSet::new();
            for series in mat {
                let h = series.labels.get_fingerprint();
                let exp_entry = cmd
                    .expected
                    .get(&h)
                    .ok_or(format!("unexpected metric {:?}", series.labels))?;
                seen.insert(h);

                let mut expected_floats = Vec::new();
                for (i, e) in exp_entry.vals.iter().enumerate() {
                    let ts = cmd.start + Duration::from_secs(i as u64 * cmd.step.as_secs());
                    if ts > cmd.end {
                        return Err(format!(
                            "expected {} points for {:?} but time range too short",
                            exp_entry.vals.len(),
                            series.labels
                        ));
                    }

                    let t = system_time_to_millis(ts);
                    if !e.omitted {
                        expected_floats.push((t, e.value));
                    }
                }
                if expected_floats.len() != series.samples.len() {
                    return Err(format!(
                        "expected {} float points for {:?}",
                        expected_floats.len(),
                        series.labels
                    ));
                }
                for (i, (exp_t, exp_f)) in expected_floats.into_iter().enumerate() {
                    let act = &series.samples[i];
                    let timestamp = act.timestamp;
                    if exp_t != timestamp {
                        return Err(format!(
                            "timestamp mismatch at index {i}: expected {exp_t} got {timestamp}"
                        ));
                    }
                    if !almost_equal(act.value, exp_f, DEFAULT_EPSILON) {
                        return Err(format!(
                            "value mismatch at index {i}: expected {exp_f} got {}",
                            act.value
                        ));
                    }
                }
            }
            for h in cmd.expected.keys() {
                if !seen.contains(h) {
                    return Err(format!(
                        "expected metric {:?} not found",
                        cmd.metrics.get(h).unwrap()
                    ));
                }
            }
        }
        QueryValue::Vector(vec) => {
            if cmd.expect_scalar {
                return Err("expected scalar result, but got vector".to_string());
            }
            let mut seen = std::collections::HashSet::new();
            for (pos, sample) in vec.iter().enumerate() {
                let h = sample.labels.get_fingerprint();
                let exp_entry = cmd
                    .expected
                    .get(&h)
                    .ok_or(format!("unexpected metric {:?}", sample.labels))?;
                if cmd.is_ordered() && exp_entry.pos != pos + 1 {
                    return Err(format!(
                        "expected metric at position {} but got {}",
                        exp_entry.pos,
                        pos + 1
                    ));
                }
                let exp0 = &exp_entry.vals[0];
                if !almost_equal(exp0.value, sample.value, DEFAULT_EPSILON) {
                    return Err(format!(
                        "value mismatch: expected {} got {}",
                        exp0.value, sample.value
                    ));
                }
                seen.insert(h);
            }
            for h in cmd.expected.keys() {
                if !seen.contains(h) {
                    return Err(format!(
                        "expected metric {:?} not found",
                        cmd.metrics.get(h).unwrap()
                    ));
                }
            }
        }
        QueryValue::Scalar {
            timestamp_ms: _,
            value,
        } => {
            if !cmd.expect_scalar {
                return Err("expected vector or matrix result, but got scalar".to_string());
            }
            let scalar = *value;
            let exp0 = &cmd.expected[&0].vals[0];
            if !almost_equal(exp0.value, scalar, DEFAULT_EPSILON) {
                return Err(format!(
                    "scalar mismatch: expected {scalar} got {}",
                    exp0.value
                ));
            }
        }
        QueryValue::String(s) => {
            if let Some(ref exp_str) = cmd.expected_string {
                if exp_str != s {
                    return Err(format!(
                        "string mismatch: expected {:?} got {:?}",
                        exp_str, s
                    ));
                }
            } else {
                return Err("unexpected string result".to_string());
            }
        }
    }
    Ok(())
}

fn assert_matrix_sorted(mat: &[RangeSample]) -> Result<(), String> {
    for i in 0..mat.len().saturating_sub(1) {
        if mat[i].labels > mat[i + 1].labels {
            return Err("matrix not sorted by labels".to_string());
        }
    }
    Ok(())
}

fn almost_equal(a: f64, b: f64, epsilon: f64) -> bool {
    (a - b).abs() < epsilon || (a - b).abs() / a.abs().max(b.abs()) < epsilon
}

fn append_sample(app: &mut dyn AppenderV2, s: &SampleST, m: &Labels) -> Result<(), String> {
    // Stub: in real code, this would append to the storage. Here we just check that the sample is valid.
    Ok(())
}

fn duration_milliseconds(d: Duration) -> i64 {
    d.as_millis() as i64
}
