//! Timed execution: paired trials of one case against both engines.
//!
//! Per trial and engine: create the series (engine-specific setup stream),
//! preload if the case reads, open the case's connections, warm up, replay the
//! workload with the configured pipeline depth while validating every reply,
//! check the resulting state for writes, then delete only the keys we created.
//! Closed-loop throughout: each connection has at most `pipeline` requests in
//! flight and never sends the next batch before the previous one is drained.
//!
//! Latency is measured per command from the submission of its batch to the
//! full consumption of its reply, on a monotonic clock. Under pipelining that
//! is a per-command figure only because every reply is matched to the batch it
//! belongs to; the report labels it as closed-loop pipelined latency.
//! Nothing is retried; an error or timeout is counted and, for timeouts and
//! transport failures, ends the trial as incomplete.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use hdrhistogram::Histogram;
use redis::{Connection, Value};

use crate::engine::{as_text, pairs, url_with_protocol};
use crate::fixture::Fixture;
use crate::manifest::now_rfc3339;
use crate::results::{
    CaseResults, LatencySummary, MemoryPair, MemoryResult, MemorySnapshot, TrialPair, TrialResult,
};
use crate::scenario::{Case, CaseKind, Protocol, Scenario};
use crate::trace::{CaseTrace, Engine, Expect, Frame};

/// Per-request read timeout on the hot path. A reply slower than this is a
/// timeout, which ends the trial as incomplete.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// Control-connection batching for setup, preload, verification and cleanup.
const CONTROL_BATCH: usize = 256;
const DELETE_BATCH: usize = 512;
const MEMORY_SAMPLE_KEYS: usize = 100;
const MAX_ERROR_SAMPLES: usize = 5;

#[derive(Debug, Clone)]
pub struct Target {
    pub engine: Engine,
    pub url: String,
    pub protocol: Protocol,
    /// From preflight: the fresh process before any key existed.
    pub empty_process: MemorySnapshot,
}

pub struct Progress<'a>(pub &'a dyn Fn(&str));

// -------- reply validation --------

fn as_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Double(d) => Some(*d),
        Value::Int(i) => Some(*i as f64),
        Value::BulkString(b) => std::str::from_utf8(b).ok()?.parse().ok(),
        Value::SimpleString(s) => s.parse().ok(),
        _ => None,
    }
}

fn as_i64(v: &Value) -> Option<i64> {
    match v {
        Value::Int(i) => Some(*i),
        Value::BulkString(b) => std::str::from_utf8(b).ok()?.parse().ok(),
        _ => None,
    }
}

fn sample_pair(v: &Value) -> Option<(i64, f64)> {
    let Value::Array(pair) = v else { return None };
    if pair.len() != 2 {
        return None;
    }
    Some((as_i64(&pair[0])?, as_f64(&pair[1])?))
}

/// Number of samples the reply carries, or why it is wrong.
pub fn check_reply(expect: &Expect, v: &Value) -> Result<u64, String> {
    if let Value::ServerError(e) = v {
        return Err(format!("server error: {e}"));
    }
    match expect {
        Expect::Ok => match v {
            Value::Okay => Ok(0),
            Value::SimpleString(s) if s == "OK" => Ok(0),
            other => Err(format!("expected OK, got {other:?}")),
        },
        Expect::Int(want) => match as_i64(v) {
            Some(got) if got == *want => Ok(1),
            _ => Err(format!("expected integer {want}, got {v:?}")),
        },
        Expect::Ints(want) => {
            let Value::Array(items) = v else {
                return Err(format!("expected {} integers, got {v:?}", want.len()));
            };
            if items.len() != want.len() {
                return Err(format!(
                    "expected {} entries, got {}",
                    want.len(),
                    items.len()
                ));
            }
            for (i, (w, got)) in want.iter().zip(items).enumerate() {
                if let Value::ServerError(e) = got {
                    return Err(format!("entry {i}: server error: {e}"));
                }
                if as_i64(got) != Some(*w) {
                    return Err(format!("entry {i}: expected {w}, got {got:?}"));
                }
            }
            Ok(want.len() as u64)
        }
        Expect::Sample { timestamp, value } => match sample_pair(v) {
            Some((ts, val)) if ts == *timestamp && val == *value => Ok(1),
            Some((ts, val)) => Err(format!(
                "expected [{timestamp}, {value}], got [{ts}, {val}]"
            )),
            None => Err(format!("expected a [timestamp, value] pair, got {v:?}")),
        },
        Expect::Samples { count, from, to } => {
            let Value::Array(items) = v else {
                return Err(format!("expected {count} samples, got {v:?}"));
            };
            if items.len() != *count {
                return Err(format!("expected {count} samples, got {}", items.len()));
            }
            for (i, item) in items.iter().enumerate() {
                match sample_pair(item) {
                    Some((ts, _)) if ts >= *from && ts <= *to => {}
                    Some((ts, _)) => {
                        return Err(format!("sample {i}: timestamp {ts} outside [{from}, {to}]"));
                    }
                    None => return Err(format!("sample {i}: malformed entry {item:?}")),
                }
            }
            Ok(*count as u64)
        }
    }
}

// -------- transport --------

fn connect(target: &Target) -> Result<Connection> {
    let client = redis::Client::open(url_with_protocol(&target.url, target.protocol).as_str())?;
    let con = client.get_connection_with_timeout(Duration::from_secs(5))?;
    con.set_read_timeout(Some(REQUEST_TIMEOUT))?;
    con.set_write_timeout(Some(REQUEST_TIMEOUT))?;
    Ok(con)
}

/// Send `frames` in batches over one connection, validating every reply.
/// Returns the samples carried by the replies.
fn replay_validated(con: &mut Connection, frames: &[Frame], what: &str) -> Result<u64> {
    let mut samples = 0u64;
    let mut buf = Vec::new();
    for batch in frames.chunks(CONTROL_BATCH) {
        buf.clear();
        for f in batch {
            buf.extend_from_slice(&f.bytes);
        }
        con.send_packed_command(&buf)
            .with_context(|| format!("{what}: sending batch"))?;
        for (i, f) in batch.iter().enumerate() {
            let v = con
                .recv_response()
                .with_context(|| format!("{what}: reading reply {i}"))?;
            samples += check_reply(&f.expect, &v)
                .map_err(|e| anyhow!("{what}: frame {i} of batch: {e}"))?;
        }
    }
    Ok(samples)
}

fn info_map(con: &mut Connection, section: &str) -> Result<BTreeMap<String, String>> {
    let v: Value = redis::cmd("INFO").arg(section).query(con)?;
    let text = as_text(&v)?;
    Ok(text
        .lines()
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        .collect())
}

fn server_counters(con: &mut Connection) -> Result<BTreeMap<String, f64>> {
    let mut out = BTreeMap::new();
    let stats = info_map(con, "stats")?;
    let cpu = info_map(con, "cpu")?;
    for (k, m) in [
        ("total_net_input_bytes", &stats),
        ("total_net_output_bytes", &stats),
        ("total_commands_processed", &stats),
        ("used_cpu_sys", &cpu),
        ("used_cpu_user", &cpu),
    ] {
        if let Some(v) = m.get(k).and_then(|v| v.parse::<f64>().ok()) {
            out.insert(k.to_string(), v);
        }
    }
    Ok(out)
}

fn deltas(before: &BTreeMap<String, f64>, after: &BTreeMap<String, f64>) -> BTreeMap<String, f64> {
    after
        .iter()
        .filter_map(|(k, a)| before.get(k).map(|b| (k.clone(), a - b)))
        .collect()
}

pub fn memory_snapshot(con: &mut Connection) -> Result<MemorySnapshot> {
    let m = info_map(con, "memory")?;
    let get_u64 = |k: &str| m.get(k).and_then(|v| v.parse::<u64>().ok());
    let dbsize: i64 = redis::cmd("DBSIZE").query(con)?;
    Ok(MemorySnapshot {
        used_memory: get_u64("used_memory").unwrap_or(0),
        used_memory_rss: get_u64("used_memory_rss").unwrap_or(0),
        allocator_allocated: get_u64("allocator_allocated"),
        allocator_active: get_u64("allocator_active"),
        mem_fragmentation_ratio: m
            .get("mem_fragmentation_ratio")
            .and_then(|v| v.parse().ok()),
        dbsize: dbsize.max(0) as u64,
    })
}

/// `DEL` every fixture key in batches (synchronous, so memory settles), then
/// confirm none is left. Only keys named by the fixture are ever touched.
fn delete_fixture_keys(con: &mut Connection, fixture: &Fixture, key_prefix: &str) -> Result<()> {
    for batch in fixture.series.chunks(DELETE_BATCH) {
        let mut cmd = redis::cmd("DEL");
        for s in batch {
            cmd.arg(&s.key);
        }
        let _: i64 = cmd.query(con).context("deleting fixture keys")?;
    }
    let pattern = format!("{key_prefix}:*");
    let mut cursor = "0".to_string();
    let mut left = 0u64;
    loop {
        let v: Value = redis::cmd("SCAN")
            .arg(&cursor)
            .arg("MATCH")
            .arg(&pattern)
            .arg("COUNT")
            .arg(1000)
            .query(con)?;
        let Value::Array(parts) = v else {
            bail!("SCAN: malformed reply")
        };
        cursor = as_text(&parts[0])?;
        if let Value::Array(keys) = &parts[1] {
            left += keys.len() as u64;
        }
        if cursor == "0" {
            break;
        }
    }
    if left != 0 {
        bail!("{left} key(s) matching {pattern:?} remain after cleanup");
    }
    Ok(())
}

/// Write-case state check: every series holds exactly the fixture's samples.
fn verify_written_state(con: &mut Connection, fixture: &Fixture) -> Result<()> {
    let mut buf = Vec::new();
    for batch in fixture.series.chunks(CONTROL_BATCH) {
        buf.clear();
        for s in batch {
            buf.extend_from_slice(&crate::trace::encode(&[b"TS.INFO", s.key.as_bytes()]));
        }
        con.send_packed_command(&buf)?;
        for s in batch {
            let v = con.recv_response()?;
            let info = pairs(&v).with_context(|| format!("TS.INFO {}", s.key))?;
            let total: u64 = info
                .get("totalSamples")
                .and_then(|v| v.parse().ok())
                .with_context(|| format!("TS.INFO {}: no totalSamples", s.key))?;
            let first: i64 = info
                .get("firstTimestamp")
                .and_then(|v| v.parse().ok())
                .unwrap_or(i64::MIN);
            let last: i64 = info
                .get("lastTimestamp")
                .and_then(|v| v.parse().ok())
                .unwrap_or(i64::MIN);
            let want_first = s.samples.first().map(|x| x.timestamp).unwrap_or(0);
            let want_last = s.samples.last().map(|x| x.timestamp).unwrap_or(0);
            if total != s.samples.len() as u64 || first != want_first || last != want_last {
                bail!(
                    "{}: totalSamples {total} [{first}..{last}], expected {} [{want_first}..{want_last}]",
                    s.key,
                    s.samples.len()
                );
            }
        }
    }
    Ok(())
}

// -------- workers --------

#[derive(Clone, Copy)]
enum Mode {
    /// Replay the stream exactly once (write trials).
    Once,
    /// Loop over the stream; count only after `warmup_until`, stop at `deadline`.
    Loop {
        warmup_until: Instant,
        deadline: Instant,
    },
}

struct WorkerOut {
    hist: Histogram<u64>,
    requests: u64,
    samples: u64,
    errors: u64,
    timeouts: u64,
    error_samples: Vec<String>,
    first_submit: Option<Instant>,
    last_reply: Option<Instant>,
    cycles: f64,
    complete: bool,
    reason: Option<String>,
}

fn new_histogram() -> Histogram<u64> {
    Histogram::<u64>::new_with_bounds(1, 120_000_000_000, 3).expect("histogram bounds")
}

fn worker(
    mut con: Connection,
    trace: Arc<CaseTrace>,
    conn: usize,
    pipeline: usize,
    mode: Mode,
) -> WorkerOut {
    let frames = &trace.workload[conn].frames;
    let mut out = WorkerOut {
        hist: new_histogram(),
        requests: 0,
        samples: 0,
        errors: 0,
        timeouts: 0,
        error_samples: Vec::new(),
        first_submit: None,
        last_reply: None,
        cycles: 0.0,
        complete: true,
        reason: None,
    };
    if frames.is_empty() {
        return out;
    }
    let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
    let mut pos = 0usize;
    let mut wraps = 0u64;
    'outer: loop {
        let timed = match mode {
            Mode::Once => {
                if pos >= frames.len() {
                    break;
                }
                true
            }
            Mode::Loop {
                warmup_until,
                deadline,
            } => {
                let now = Instant::now();
                if now >= deadline {
                    break;
                }
                if pos >= frames.len() {
                    pos = 0;
                    wraps += 1;
                }
                now >= warmup_until
            }
        };
        let end = (pos + pipeline).min(frames.len());
        buf.clear();
        for f in &frames[pos..end] {
            buf.extend_from_slice(&f.bytes);
        }
        let submit = Instant::now();
        if let Err(e) = con.send_packed_command(&buf) {
            out.complete = false;
            out.reason = Some(format!("send failed: {e}"));
            if e.is_timeout() {
                out.timeouts += 1;
            }
            break;
        }
        if timed && out.first_submit.is_none() {
            out.first_submit = Some(submit);
        }
        for f in &frames[pos..end] {
            match con.recv_response() {
                Ok(v) => {
                    let now = Instant::now();
                    match check_reply(&f.expect, &v) {
                        Ok(n) => {
                            if timed {
                                out.samples += n;
                            }
                        }
                        Err(e) => {
                            if timed {
                                out.errors += 1;
                                if out.error_samples.len() < MAX_ERROR_SAMPLES {
                                    out.error_samples.push(e);
                                }
                            }
                        }
                    }
                    if timed {
                        out.requests += 1;
                        let _ = out.hist.record((now - submit).as_nanos() as u64);
                        out.last_reply = Some(now);
                    }
                }
                Err(e) => {
                    if e.is_timeout() {
                        out.timeouts += 1;
                        out.reason = Some("reply timed out".to_string());
                    } else {
                        out.reason = Some(format!("transport error: {e}"));
                    }
                    out.complete = false;
                    break 'outer;
                }
            }
        }
        pos = end;
    }
    out.cycles = wraps as f64 + pos as f64 / frames.len() as f64;
    out
}

fn summarize(hist: &Histogram<u64>) -> LatencySummary {
    let us = |v: u64| v as f64 / 1000.0;
    LatencySummary {
        observations: hist.len(),
        p50_us: us(hist.value_at_quantile(0.50)),
        p95_us: us(hist.value_at_quantile(0.95)),
        p99_us: us(hist.value_at_quantile(0.99)),
        p999_us: us(hist.value_at_quantile(0.999)),
        max_us: us(hist.max()),
        mean_us: hist.mean() / 1000.0,
    }
}

// -------- trials --------

struct TrialCtx<'a> {
    scenario: &'a Scenario,
    fixture: &'a Fixture,
    case: &'a Case,
    trace: Arc<CaseTrace>,
    progress: &'a Progress<'a>,
}

fn incomplete(engine: Engine, reason: String) -> TrialResult {
    TrialResult {
        engine,
        started_at: now_rfc3339(),
        duration_s: 0.0,
        requests: 0,
        samples: 0,
        errors: 0,
        timeouts: 0,
        error_samples: Vec::new(),
        complete: false,
        incomplete_reason: Some(reason),
        latency: LatencySummary::default(),
        server_deltas: BTreeMap::new(),
        cycles_completed: 0.0,
        state_verified: None,
        under_calibrated: false,
    }
}

/// One engine's side of a paired trial. Never propagates an error: whatever
/// went wrong becomes an incomplete result (after a best-effort cleanup), so
/// the run continues and the report shows the failure.
fn run_trial(ctx: &TrialCtx<'_>, target: &Target) -> TrialResult {
    let engine = target.engine;
    let log = |m: &str| (ctx.progress.0)(&format!("    {} {m}", engine.name()));
    let mut control = match connect(target) {
        Ok(c) => c,
        Err(e) => return incomplete(engine, format!("control connection: {e:#}")),
    };
    let result = run_trial_inner(ctx, target, &mut control, &log);
    // Cleanup runs whatever happened; a failure here is worse than the trial's.
    if let Err(e) = delete_fixture_keys(&mut control, ctx.fixture, &ctx.scenario.fixture.key_prefix)
    {
        return incomplete(engine, format!("cleanup failed: {e:#}"));
    }
    match result {
        Ok(r) => r,
        Err(e) => incomplete(engine, format!("{e:#}")),
    }
}

fn run_trial_inner(
    ctx: &TrialCtx<'_>,
    target: &Target,
    control: &mut Connection,
    log: &dyn Fn(&str),
) -> Result<TrialResult> {
    let engine = target.engine;
    let started_at = now_rfc3339();

    // Leftovers from an earlier failed trial would corrupt this one.
    delete_fixture_keys(control, ctx.fixture, &ctx.scenario.fixture.key_prefix)
        .context("pre-trial cleanup")?;

    let t0 = Instant::now();
    replay_validated(control, &ctx.trace.setup(engine).frames, "setup")?;
    if !ctx.trace.preload.frames.is_empty() {
        replay_validated(control, &ctx.trace.preload.frames, "preload")?;
    }
    log(&format!(
        "setup {} series{} in {:.1} s",
        ctx.trace.setup(engine).frames.len(),
        if ctx.trace.preloaded_samples > 0 {
            format!(" + preload {} samples", ctx.trace.preloaded_samples)
        } else {
            String::new()
        },
        t0.elapsed().as_secs_f64()
    ));

    let connections = ctx.case.connections as usize;
    let pipeline = ctx.case.pipeline as usize;
    let mut cons = Vec::with_capacity(connections);
    for _ in 0..connections {
        cons.push(connect(target).context("opening workload connection")?);
    }

    let mode = if ctx.case.kind.is_read() {
        let now = Instant::now();
        let warmup_until = now + Duration::from_secs(ctx.scenario.warmup_seconds);
        Mode::Loop {
            warmup_until,
            deadline: warmup_until + Duration::from_secs(ctx.scenario.read_duration_seconds),
        }
    } else {
        // Write trials replay a fixed trace on precreated series; the setup
        // phase has already exercised the connections and the write path on
        // this engine. There is no disposable-data warm-up yet.
        for c in cons.iter_mut() {
            let pong: String = redis::cmd("PING").query(c)?;
            if pong != "PONG" {
                bail!("PING before the timed phase returned {pong:?}");
            }
        }
        Mode::Once
    };

    let before = server_counters(control)?;
    let handles: Vec<_> = cons
        .into_iter()
        .enumerate()
        .map(|(i, con)| {
            let trace = Arc::clone(&ctx.trace);
            thread::spawn(move || worker(con, trace, i, pipeline, mode))
        })
        .collect();
    let mut outs = Vec::with_capacity(connections);
    for h in handles {
        outs.push(h.join().map_err(|_| anyhow!("worker thread panicked"))?);
    }
    let after = server_counters(control)?;

    let mut hist = new_histogram();
    let mut requests = 0;
    let mut samples = 0;
    let mut errors = 0;
    let mut timeouts = 0;
    let mut error_samples = Vec::new();
    let mut first = None::<Instant>;
    let mut last = None::<Instant>;
    let mut complete = true;
    let mut reason = None;
    let mut cycles = 0.0;
    for o in &outs {
        hist.add(&o.hist).context("merging histograms")?;
        requests += o.requests;
        samples += o.samples;
        errors += o.errors;
        timeouts += o.timeouts;
        for e in &o.error_samples {
            if error_samples.len() < MAX_ERROR_SAMPLES {
                error_samples.push(e.clone());
            }
        }
        first = match (first, o.first_submit) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        last = match (last, o.last_reply) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        };
        if !o.complete {
            complete = false;
            reason = reason.or_else(|| o.reason.clone());
        }
        cycles += o.cycles;
    }
    let duration_s = match (first, last) {
        (Some(a), Some(b)) => (b - a).as_secs_f64(),
        _ => 0.0,
    };
    let cycles_completed = if connections > 0 {
        cycles / connections as f64
    } else {
        0.0
    };

    let state_verified = if ctx.case.kind.is_write() {
        match verify_written_state(control, ctx.fixture) {
            Ok(()) => Some(true),
            Err(e) => {
                complete = false;
                reason = reason.or(Some(format!("state check failed: {e:#}")));
                Some(false)
            }
        }
    } else {
        None
    };

    let under_calibrated =
        ctx.case.kind.is_write() && duration_s < ctx.scenario.min_write_seconds as f64;

    log(&format!(
        "{} requests, {} samples, {} errors, {} timeouts in {:.2} s (p50 {:.0} µs, p99 {:.0} µs){}",
        requests,
        samples,
        errors,
        timeouts,
        duration_s,
        hist.value_at_quantile(0.5) as f64 / 1000.0,
        hist.value_at_quantile(0.99) as f64 / 1000.0,
        if complete { "" } else { " — INCOMPLETE" }
    ));

    Ok(TrialResult {
        engine,
        started_at,
        duration_s,
        requests,
        samples,
        errors,
        timeouts,
        error_samples,
        complete,
        incomplete_reason: reason,
        latency: summarize(&hist),
        server_deltas: deltas(&before, &after),
        cycles_completed,
        state_verified,
        under_calibrated,
    })
}

// -------- memory --------

fn run_memory(ctx: &TrialCtx<'_>, target: &Target, fresh_process: bool) -> MemoryResult {
    let engine = target.engine;
    let log = |m: &str| (ctx.progress.0)(&format!("    {} {m}", engine.name()));
    let empty = MemorySnapshot {
        used_memory: 0,
        used_memory_rss: 0,
        allocator_allocated: None,
        allocator_active: None,
        mem_fragmentation_ratio: None,
        dbsize: 0,
    };
    let fail = |reason: String| MemoryResult {
        engine,
        empty_process: target.empty_process.clone(),
        before: empty.clone(),
        created_empty: empty.clone(),
        loaded: empty.clone(),
        series: 0,
        samples: 0,
        ts_info_memory_sampled: 0,
        memory_usage_sampled: 0,
        sampled_keys: 0,
        settle_seconds: ctx.scenario.settle_seconds,
        fresh_process,
        complete: false,
        incomplete_reason: Some(reason),
    };
    let mut control = match connect(target) {
        Ok(c) => c,
        Err(e) => return fail(format!("control connection: {e:#}")),
    };
    let result = run_memory_inner(ctx, target, &mut control, fresh_process, &log);
    if let Err(e) = delete_fixture_keys(&mut control, ctx.fixture, &ctx.scenario.fixture.key_prefix)
    {
        return fail(format!("cleanup failed: {e:#}"));
    }
    match result {
        Ok(r) => r,
        Err(e) => fail(format!("{e:#}")),
    }
}

fn run_memory_inner(
    ctx: &TrialCtx<'_>,
    target: &Target,
    control: &mut Connection,
    fresh_process: bool,
    log: &dyn Fn(&str),
) -> Result<MemoryResult> {
    let settle = Duration::from_secs(ctx.scenario.settle_seconds);
    delete_fixture_keys(control, ctx.fixture, &ctx.scenario.fixture.key_prefix)?;
    thread::sleep(settle);
    let before = memory_snapshot(control)?;

    replay_validated(control, &ctx.trace.setup(target.engine).frames, "setup")?;
    thread::sleep(settle);
    let created_empty = memory_snapshot(control)?;

    replay_validated(control, &ctx.trace.preload.frames, "preload")?;
    thread::sleep(settle);
    let loaded = memory_snapshot(control)?;

    // Per-key diagnostics on an evenly spaced sample of keys.
    let n = ctx.fixture.series.len();
    let step = n.div_ceil(MEMORY_SAMPLE_KEYS).max(1);
    let mut ts_info_memory = 0u64;
    let mut memory_usage = 0u64;
    let mut sampled = 0u64;
    for s in ctx.fixture.series.iter().step_by(step) {
        let v: Value = redis::cmd("TS.INFO").arg(&s.key).query(control)?;
        let info = pairs(&v)?;
        ts_info_memory += info
            .get("memoryUsage")
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        let mu: Value = redis::cmd("MEMORY")
            .arg("USAGE")
            .arg(&s.key)
            .query(control)?;
        memory_usage += as_i64(&mu).unwrap_or(0).max(0) as u64;
        sampled += 1;
    }

    let series = n as u64;
    let samples = ctx.trace.preloaded_samples;
    log(&format!(
        "used_memory {} -> {} (created) -> {} (loaded); {:.1} B/sample, rss {}",
        before.used_memory,
        created_empty.used_memory,
        loaded.used_memory,
        (loaded.used_memory as f64 - created_empty.used_memory as f64) / samples.max(1) as f64,
        loaded.used_memory_rss
    ));

    Ok(MemoryResult {
        engine: target.engine,
        empty_process: target.empty_process.clone(),
        before,
        created_empty,
        loaded,
        series,
        samples,
        ts_info_memory_sampled: ts_info_memory,
        memory_usage_sampled: memory_usage,
        sampled_keys: sampled,
        settle_seconds: ctx.scenario.settle_seconds,
        fresh_process,
        complete: true,
        incomplete_reason: None,
    })
}

// -------- case --------

fn order_for(trial: u32) -> [Engine; 2] {
    if trial.is_multiple_of(2) {
        [Engine::Subject, Engine::Reference]
    } else {
        [Engine::Reference, Engine::Subject]
    }
}

/// Run every trial of one case, alternating AB/BA order.
pub fn run_case(
    scenario: &Scenario,
    fixture: &Fixture,
    case: &Case,
    trace: CaseTrace,
    targets: &[Target; 2],
    // Whether each engine's process has served no earlier trial in this run.
    fresh: [bool; 2],
    progress: &Progress<'_>,
) -> CaseResults {
    let ctx = TrialCtx {
        scenario,
        fixture,
        case,
        trace: Arc::new(trace),
        progress,
    };
    let target_for = |e: Engine| {
        targets
            .iter()
            .find(|t| t.engine == e)
            .expect("both targets")
    };
    let fresh_for = |e: Engine| {
        fresh[targets
            .iter()
            .position(|t| t.engine == e)
            .expect("both targets")]
    };
    let latency_definition = if case.pipeline > 1 {
        format!(
            "closed-loop, per command, pipeline depth {}: batch submission to full consumption of the command's reply",
            case.pipeline
        )
    } else {
        "closed-loop, per command: submission to full consumption of the reply".to_string()
    };
    let mut results = CaseResults {
        case_id: case.id.clone(),
        family: family(&case.kind).to_string(),
        connections: case.connections,
        pipeline: case.pipeline,
        latency_definition,
        trials: Vec::new(),
        memory: Vec::new(),
    };

    for trial in 0..scenario.trials {
        let order = order_for(trial);
        (progress.0)(&format!(
            "  trial {}/{} order {} then {}",
            trial + 1,
            scenario.trials,
            order[0].name(),
            order[1].name()
        ));
        if matches!(case.kind, CaseKind::Memory {}) {
            let mut got: BTreeMap<&str, MemoryResult> = BTreeMap::new();
            for e in order {
                got.insert(e.name(), run_memory(&ctx, target_for(e), fresh_for(e)));
            }
            results.memory.push(MemoryPair {
                trial,
                order,
                subject: got.remove("subject").expect("subject ran"),
                reference: got.remove("reference").expect("reference ran"),
            });
        } else {
            let mut got: BTreeMap<&str, TrialResult> = BTreeMap::new();
            for e in order {
                got.insert(e.name(), run_trial(&ctx, target_for(e)));
            }
            results.trials.push(TrialPair {
                trial,
                order,
                subject: got.remove("subject").expect("subject ran"),
                reference: got.remove("reference").expect("reference ran"),
            });
        }
    }
    results
}

pub fn family(kind: &CaseKind) -> &'static str {
    match kind {
        CaseKind::Add {} => "add",
        CaseKind::Madd { .. } => "madd",
        CaseKind::Get { .. } => "get",
        CaseKind::Range { .. } => "range",
        CaseKind::Memory {} => "memory",
    }
}

/// A snapshot from preflight's recorded `INFO memory` fields (strings).
pub fn snapshot_from_info(m: &BTreeMap<String, String>, dbsize: u64) -> MemorySnapshot {
    let get_u64 = |k: &str| m.get(k).and_then(|v| v.parse::<u64>().ok());
    MemorySnapshot {
        used_memory: get_u64("used_memory").unwrap_or(0),
        used_memory_rss: get_u64("used_memory_rss").unwrap_or(0),
        allocator_allocated: get_u64("allocator_allocated"),
        allocator_active: get_u64("allocator_active"),
        mem_fragmentation_ratio: m
            .get("mem_fragmentation_ratio")
            .and_then(|v| v.parse().ok()),
        dbsize,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bulk(s: &str) -> Value {
        Value::BulkString(s.as_bytes().to_vec())
    }

    #[test]
    fn ok_and_int_replies() {
        assert_eq!(check_reply(&Expect::Ok, &Value::Okay), Ok(0));
        assert!(check_reply(&Expect::Ok, &Value::Int(1)).is_err());
        assert_eq!(check_reply(&Expect::Int(7), &Value::Int(7)), Ok(1));
        assert!(check_reply(&Expect::Int(7), &Value::Int(8)).is_err());
    }

    #[test]
    fn madd_replies_are_checked_per_entry() {
        let want = Expect::Ints(vec![1, 2, 3]);
        assert_eq!(
            check_reply(
                &want,
                &Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)])
            ),
            Ok(3)
        );
        let short = Value::Array(vec![Value::Int(1), Value::Int(2)]);
        assert!(
            check_reply(&want, &short)
                .unwrap_err()
                .contains("expected 3 entries")
        );
        let wrong = Value::Array(vec![Value::Int(1), Value::Int(9), Value::Int(3)]);
        assert!(check_reply(&want, &wrong).unwrap_err().contains("entry 1"));
    }

    #[test]
    fn get_replies_in_both_protocols() {
        let want = Expect::Sample {
            timestamp: 5,
            value: 1.5,
        };
        let resp2 = Value::Array(vec![Value::Int(5), bulk("1.5")]);
        let resp3 = Value::Array(vec![Value::Int(5), Value::Double(1.5)]);
        assert_eq!(check_reply(&want, &resp2), Ok(1));
        assert_eq!(check_reply(&want, &resp3), Ok(1));
        assert!(check_reply(&want, &Value::Array(vec![])).is_err());
        assert!(
            check_reply(&want, &Value::Array(vec![Value::Int(5), bulk("1.6")]))
                .unwrap_err()
                .contains("expected [5, 1.5]")
        );
    }

    #[test]
    fn range_replies_check_count_and_window() {
        let want = Expect::Samples {
            count: 2,
            from: 10,
            to: 20,
        };
        let ok = Value::Array(vec![
            Value::Array(vec![Value::Int(10), bulk("1")]),
            Value::Array(vec![Value::Int(20), bulk("2")]),
        ]);
        assert_eq!(check_reply(&want, &ok), Ok(2));
        let outside = Value::Array(vec![
            Value::Array(vec![Value::Int(10), bulk("1")]),
            Value::Array(vec![Value::Int(21), bulk("2")]),
        ]);
        assert!(
            check_reply(&want, &outside)
                .unwrap_err()
                .contains("outside")
        );
        let fewer = Value::Array(vec![Value::Array(vec![Value::Int(10), bulk("1")])]);
        assert!(
            check_reply(&want, &fewer)
                .unwrap_err()
                .contains("expected 2 samples, got 1")
        );
    }

    #[test]
    fn trial_order_alternates() {
        assert_eq!(order_for(0), [Engine::Subject, Engine::Reference]);
        assert_eq!(order_for(1), [Engine::Reference, Engine::Subject]);
        assert_eq!(order_for(2), [Engine::Subject, Engine::Reference]);
    }

    #[test]
    fn counter_deltas_ignore_missing_keys() {
        let mut a = BTreeMap::new();
        a.insert("x".to_string(), 1.0);
        a.insert("y".to_string(), 5.0);
        let mut b = BTreeMap::new();
        b.insert("x".to_string(), 4.0);
        b.insert("z".to_string(), 1.0);
        let d = deltas(&a, &b);
        assert_eq!(d.len(), 1);
        assert_eq!(d["x"], 3.0);
    }
}
