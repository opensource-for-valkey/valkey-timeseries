//! CSV and Markdown from a run directory's raw data (`results.json` +
//! `manifest.json`). Pure: no server is touched, so `server_bench report`
//! regenerates everything from disk.
//!
//! Ratio conventions (plan, rule 8): throughput ratio is subject/reference,
//! latency and memory ratios are reference/subject, so a value above 1 favours
//! the subject. Ratios are ratios of per-engine medians over *valid* paired
//! trials; the confidence interval is a paired bootstrap over those pairs and
//! is only printed with three or more of them. Percentiles are never averaged.

use std::fmt::Write as _;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde_json::Value as Json;

use crate::results::{CaseResults, MemoryPair, RunResults, TrialPair, TrialResult};

const BOOTSTRAP_ROUNDS: usize = 2000;
const MIN_PAIRS_FOR_CI: usize = 3;

pub fn write_reports(run_dir: &Path) -> Result<()> {
    let results: RunResults = serde_json::from_str(
        &fs::read_to_string(run_dir.join("results.json")).context("reading results.json")?,
    )
    .context("parsing results.json")?;
    let manifest: Json = serde_json::from_str(
        &fs::read_to_string(run_dir.join("manifest.json")).context("reading manifest.json")?,
    )
    .context("parsing manifest.json")?;

    fs::write(run_dir.join("results.csv"), csv(&results))?;
    fs::write(run_dir.join("report.md"), markdown(&results, &manifest))?;
    Ok(())
}

// -------- statistics --------

fn median(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut v = values.to_vec();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = v.len();
    Some(if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2.0
    })
}

fn spread(values: &[f64]) -> Option<(f64, f64)> {
    let min = values.iter().cloned().fold(f64::INFINITY, f64::min);
    let max = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    if values.is_empty() {
        None
    } else {
        Some((min, max))
    }
}

/// Deterministic xorshift64*, so a report regenerated from the same data
/// prints the same interval.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

/// 95 % paired bootstrap interval for `median(a) / median(b)` over paired
/// observations, resampling pairs with replacement.
pub fn bootstrap_ratio_ci(pairs: &[(f64, f64)]) -> Option<(f64, f64)> {
    if pairs.len() < MIN_PAIRS_FOR_CI {
        return None;
    }
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let mut ratios = Vec::with_capacity(BOOTSTRAP_ROUNDS);
    let mut a = Vec::with_capacity(pairs.len());
    let mut b = Vec::with_capacity(pairs.len());
    for _ in 0..BOOTSTRAP_ROUNDS {
        a.clear();
        b.clear();
        for _ in 0..pairs.len() {
            let (x, y) = pairs[rng.below(pairs.len())];
            a.push(x);
            b.push(y);
        }
        let (ma, mb) = (median(&a)?, median(&b)?);
        if mb > 0.0 {
            ratios.push(ma / mb);
        }
    }
    if ratios.is_empty() {
        return None;
    }
    ratios.sort_by(|x, y| x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal));
    let lo = ratios[((ratios.len() - 1) as f64 * 0.025).round() as usize];
    let hi = ratios[((ratios.len() - 1) as f64 * 0.975).round() as usize];
    Some((lo, hi))
}

// -------- derived per-trial figures --------

fn throughput(t: &TrialResult) -> f64 {
    if t.duration_s > 0.0 {
        t.requests as f64 / t.duration_s
    } else {
        0.0
    }
}

fn samples_per_s(t: &TrialResult) -> f64 {
    if t.duration_s > 0.0 {
        t.samples as f64 / t.duration_s
    } else {
        0.0
    }
}

fn trial_valid(t: &TrialResult) -> bool {
    t.complete
        && t.errors == 0
        && t.timeouts == 0
        && t.state_verified != Some(false)
        && t.requests > 0
}

/// Why a pair yields no ratio, if it does not.
fn pair_problem(p: &TrialPair) -> Option<String> {
    let mut problems = Vec::new();
    for t in [&p.subject, &p.reference] {
        let name = t.engine.name();
        if !t.complete {
            problems.push(format!(
                "{name} incomplete ({})",
                t.incomplete_reason.as_deref().unwrap_or("unknown")
            ));
        }
        if t.errors > 0 {
            problems.push(format!(
                "{name} {} error(s): {}",
                t.errors,
                t.error_samples.first().map(String::as_str).unwrap_or("")
            ));
        }
        if t.timeouts > 0 {
            problems.push(format!("{name} {} timeout(s)", t.timeouts));
        }
        if t.state_verified == Some(false) {
            problems.push(format!("{name} state check failed"));
        }
        if t.complete && t.requests == 0 {
            problems.push(format!("{name} made no timed requests"));
        }
    }
    if problems.is_empty() {
        None
    } else {
        Some(problems.join("; "))
    }
}

struct Summary {
    valid_pairs: usize,
    total_pairs: usize,
    subject_tput: Vec<f64>,
    reference_tput: Vec<f64>,
    subject_sps: Vec<f64>,
    reference_sps: Vec<f64>,
    subject_p: [Vec<f64>; 3],
    reference_p: [Vec<f64>; 3],
    subject_obs: u64,
    reference_obs: u64,
    under_calibrated: bool,
    problems: Vec<String>,
}

fn summarize(case: &CaseResults) -> Summary {
    let mut s = Summary {
        valid_pairs: 0,
        total_pairs: case.trials.len(),
        subject_tput: vec![],
        reference_tput: vec![],
        subject_sps: vec![],
        reference_sps: vec![],
        subject_p: [vec![], vec![], vec![]],
        reference_p: [vec![], vec![], vec![]],
        subject_obs: 0,
        reference_obs: 0,
        under_calibrated: false,
        problems: vec![],
    };
    for p in &case.trials {
        if let Some(problem) = pair_problem(p) {
            s.problems.push(format!("trial {}: {problem}", p.trial + 1));
            continue;
        }
        if !(trial_valid(&p.subject) && trial_valid(&p.reference)) {
            continue;
        }
        s.valid_pairs += 1;
        s.subject_tput.push(throughput(&p.subject));
        s.reference_tput.push(throughput(&p.reference));
        s.subject_sps.push(samples_per_s(&p.subject));
        s.reference_sps.push(samples_per_s(&p.reference));
        for (i, get) in [
            |t: &TrialResult| t.latency.p50_us,
            |t: &TrialResult| t.latency.p95_us,
            |t: &TrialResult| t.latency.p99_us,
        ]
        .iter()
        .enumerate()
        {
            s.subject_p[i].push(get(&p.subject));
            s.reference_p[i].push(get(&p.reference));
        }
        s.subject_obs += p.subject.latency.observations;
        s.reference_obs += p.reference.latency.observations;
        if p.subject.under_calibrated || p.reference.under_calibrated {
            s.under_calibrated = true;
        }
    }
    s
}

// -------- formatting --------

fn fmt_num(v: f64) -> String {
    if v >= 100.0 {
        let n = v.round() as i64;
        let digits = n.abs().to_string();
        let mut out = String::new();
        for (i, c) in digits.chars().enumerate() {
            if i > 0 && (digits.len() - i).is_multiple_of(3) {
                out.push(',');
            }
            out.push(c);
        }
        if n < 0 { format!("-{out}") } else { out }
    } else if v >= 10.0 {
        format!("{v:.1}")
    } else {
        format!("{v:.2}")
    }
}

fn fmt_median_spread(values: &[f64]) -> String {
    match (median(values), spread(values)) {
        (Some(m), Some((lo, hi))) if values.len() > 1 => {
            format!("{} ({}–{})", fmt_num(m), fmt_num(lo), fmt_num(hi))
        }
        (Some(m), _) => fmt_num(m),
        _ => "—".to_string(),
    }
}

fn fmt_ratio(num: &[f64], den: &[f64]) -> String {
    match (median(num), median(den)) {
        (Some(a), Some(b)) if b > 0.0 && a > 0.0 => format!("{:.2}", a / b),
        _ => "—".to_string(),
    }
}

fn fmt_ci(pairs: &[(f64, f64)]) -> String {
    match bootstrap_ratio_ci(pairs) {
        Some((lo, hi)) => format!(" [{lo:.2}, {hi:.2}]"),
        None => String::new(),
    }
}

fn json_str<'a>(v: &'a Json, path: &[&str]) -> &'a str {
    let mut cur = v;
    for p in path {
        cur = &cur[*p];
    }
    cur.as_str().unwrap_or("")
}

pub fn markdown(results: &RunResults, manifest: &Json) -> String {
    let mut md = String::new();
    let comparison = json_str(manifest, &["preflight", "comparison"]);
    let self_check = comparison == "self_check";
    let deployment = json_str(manifest, &["notes", "deployment.kind"]);
    let publishable = deployment == "containers-equal-limits" && !self_check;

    let _ = writeln!(md, "# Server benchmark report — {}", results.run_id);
    let _ = writeln!(md);
    if self_check {
        let _ = writeln!(
            md,
            "> **HARNESS SELF-CHECK.** Both engines are the same Valkey TimeSeries build. \
             These figures check the harness, not the product; ratios should sit near 1."
        );
        let _ = writeln!(md);
    }
    if !publishable {
        let _ = writeln!(
            md,
            "> **Exploratory run** (deployment `{deployment}`): not a publishable comparison \
             (plan, \"Comparable environments\")."
        );
        let _ = writeln!(md);
    }

    let _ = writeln!(md, "## Setup");
    let _ = writeln!(md);
    let _ = writeln!(md, "| | subject | reference |");
    let _ = writeln!(md, "| --- | --- | --- |");
    for (label, path) in [
        ("server", "server_name"),
        ("version", "server_version"),
        ("module", "module_name"),
        ("module version", "module_version"),
        ("os", "os"),
        ("allocator", "mem_allocator"),
        ("io threads", "io_threads_active"),
    ] {
        let s = &manifest["preflight"]["subject"][path];
        let r = &manifest["preflight"]["reference"][path];
        let _ = writeln!(md, "| {label} | {} | {} |", json_cell(s), json_cell(r));
    }
    let _ = writeln!(md);
    let sc = &manifest["scenario"]["body"];
    let _ = writeln!(
        md,
        "- scenario `{}` (schema {}), protocol {}, {} trial(s), warm-up {} s, read trial {} s",
        json_str(sc, &["name"]),
        sc["schema_version"],
        json_str(sc, &["protocol"]),
        sc["trials"],
        sc["warmup_seconds"],
        sc["read_duration_seconds"]
    );
    let fx = &manifest["fixture"]["manifest"];
    let _ = writeln!(
        md,
        "- fixture {} series × {} samples ({}, seed {}), sha256 `{}`",
        fx["shape"]["series"],
        fx["shape"]["samples_per_series"],
        json_str(fx, &["generator", "dataset_key"]),
        fx["generator"]["dataset_seed"],
        json_str(fx, &["sha256"])
    );
    let _ = writeln!(
        md,
        "- series: CHUNK_SIZE {} DUPLICATE_POLICY {} ENCODING subject={} reference={}",
        sc["series"]["chunk_size"],
        json_str(sc, &["series", "duplicate_policy"]),
        json_str(sc, &["series", "encoding", "subject"]),
        json_str(sc, &["series", "encoding", "reference"])
    );
    let _ = writeln!(
        md,
        "- source {}{}, module sha256 `{}`",
        json_str(manifest, &["source", "commit"]),
        if manifest["source"]["dirty"].as_bool() == Some(true) {
            " (dirty)"
        } else {
            ""
        },
        json_str(manifest, &["module", "sha256"])
    );
    let host = &manifest["host"];
    let _ = writeln!(
        md,
        "- host {} {} {}, {} CPUs, {} GiB; created {}",
        json_str(host, &["os"]),
        json_str(host, &["arch"]),
        json_str(host, &["cpu_model"]),
        host["cpu_count"],
        host["total_memory_bytes"].as_u64().unwrap_or(0) / (1 << 30),
        json_str(manifest, &["created_at"])
    );
    if let Some(notes) = manifest["notes"].as_object() {
        for (k, v) in notes {
            let _ = writeln!(md, "- {k}: {}", json_cell(v));
        }
    }
    let _ = writeln!(md);

    // Throughput
    let _ = writeln!(md, "## Throughput (closed-loop)");
    let _ = writeln!(md);
    let _ = writeln!(
        md,
        "Median over valid paired trials, with min–max in parentheses. Ratio = subject/reference \
         of the medians; a 95 % paired-bootstrap interval follows when there are ≥ {MIN_PAIRS_FOR_CI} valid pairs."
    );
    let _ = writeln!(md);
    let _ = writeln!(
        md,
        "| case | conns | pipe | subject cmd/s | reference cmd/s | ratio | subject samples/s | reference samples/s | ratio | valid pairs |"
    );
    let _ = writeln!(
        md,
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    );
    for case in results.cases.iter().filter(|c| c.family != "memory") {
        let s = summarize(case);
        let pairs: Vec<(f64, f64)> = s
            .subject_tput
            .iter()
            .cloned()
            .zip(s.reference_tput.iter().cloned())
            .collect();
        let _ = writeln!(
            md,
            "| {}{} | {} | {} | {} | {} | {}{} | {} | {} | {} | {}/{} |",
            case.case_id,
            if s.under_calibrated { " ⚠" } else { "" },
            case.connections,
            case.pipeline,
            fmt_median_spread(&s.subject_tput),
            fmt_median_spread(&s.reference_tput),
            fmt_ratio(&s.subject_tput, &s.reference_tput),
            fmt_ci(&pairs),
            fmt_median_spread(&s.subject_sps),
            fmt_median_spread(&s.reference_sps),
            fmt_ratio(&s.subject_sps, &s.reference_sps),
            s.valid_pairs,
            s.total_pairs
        );
    }
    let _ = writeln!(md);
    let _ = writeln!(
        md,
        "⚠ = under-calibrated write trial: the faster engine finished in under `min_write_seconds`; \
         enlarge the fixture before trusting the ratio. Samples/s counts samples written (writes) or returned (range reads)."
    );
    let _ = writeln!(md);

    // Latency
    let _ = writeln!(md, "## Latency (closed-loop, µs)");
    let _ = writeln!(md);
    let _ = writeln!(
        md,
        "Per-trial percentiles, median across valid trials (min–max). Ratio = reference/subject. \
         Pipelined cases report per-command latency from batch submission to the command's reply; \
         these are closed-loop figures and do not describe latency at a fixed arrival rate."
    );
    let _ = writeln!(md);
    let _ = writeln!(
        md,
        "| case | subject p50 | reference p50 | ratio | subject p95 | reference p95 | ratio | subject p99 | reference p99 | ratio | observations (s/r) |"
    );
    let _ = writeln!(
        md,
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    );
    for case in results.cases.iter().filter(|c| c.family != "memory") {
        let s = summarize(case);
        let _ = write!(md, "| {} ", case.case_id);
        for i in 0..3 {
            let _ = write!(
                md,
                "| {} | {} | {} ",
                fmt_median_spread(&s.subject_p[i]),
                fmt_median_spread(&s.reference_p[i]),
                fmt_ratio(&s.reference_p[i], &s.subject_p[i])
            );
        }
        let _ = writeln!(
            md,
            "| {} / {} |",
            fmt_num(s.subject_obs as f64),
            fmt_num(s.reference_obs as f64)
        );
    }
    let _ = writeln!(md);

    // Memory
    let memory_cases: Vec<&CaseResults> = results
        .cases
        .iter()
        .filter(|c| c.family == "memory")
        .collect();
    if !memory_cases.is_empty() {
        let _ = writeln!(md, "## Memory");
        let _ = writeln!(md);
        let _ = writeln!(
            md,
            "Server-accounted `used_memory` (INFO memory) at three states, and OS RSS at the loaded state. \
             Bytes/sample = (loaded − created-empty) / retained samples; bytes/series = (created-empty − before) / series. \
             Ratios are reference/subject. RSS is only comparable on fresh processes; \
             `TS.INFO memoryUsage` and `MEMORY USAGE` are per-key diagnostics summed over the sampled keys, not totals."
        );
        let _ = writeln!(md);
        let _ = writeln!(
            md,
            "| case | trial | engine | before | created-empty | loaded | bytes/sample | bytes/series (empty) | RSS loaded | frag | TS.INFO mem (sampled) | MEMORY USAGE (sampled) | fresh |"
        );
        let _ = writeln!(
            md,
            "| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |"
        );
        for case in &memory_cases {
            for p in &case.memory {
                for m in [&p.subject, &p.reference] {
                    if !m.complete {
                        let _ = writeln!(
                            md,
                            "| {} | {} | {} | incomplete: {} | | | | | | | | | |",
                            case.case_id,
                            p.trial + 1,
                            m.engine.name(),
                            m.incomplete_reason.as_deref().unwrap_or("")
                        );
                        continue;
                    }
                    let _ = writeln!(
                        md,
                        "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
                        case.case_id,
                        p.trial + 1,
                        m.engine.name(),
                        fmt_num(m.before.used_memory as f64),
                        fmt_num(m.created_empty.used_memory as f64),
                        fmt_num(m.loaded.used_memory as f64),
                        m.bytes_per_sample()
                            .map(|v| format!("{v:.2}"))
                            .unwrap_or("—".into()),
                        m.bytes_per_series_empty()
                            .map(|v| format!("{v:.0}"))
                            .unwrap_or("—".into()),
                        fmt_num(m.loaded.used_memory_rss as f64),
                        m.loaded
                            .mem_fragmentation_ratio
                            .map(|v| format!("{v:.2}"))
                            .unwrap_or("—".into()),
                        fmt_num(m.ts_info_memory_sampled as f64),
                        fmt_num(m.memory_usage_sampled as f64),
                        if m.fresh_process { "yes" } else { "no" }
                    );
                }
            }
            let _ = writeln!(md);
            let _ = writeln!(md, "{}", memory_ratios(case));
            let _ = writeln!(md);
        }
    }

    // Per-trial detail
    let _ = writeln!(md, "## Trials");
    let _ = writeln!(md);
    let _ = writeln!(
        md,
        "| case | trial | order | engine | requests | samples | duration s | cmd/s | p50 | p95 | p99 | p99.9 | max | errors | timeouts | cycles | net in | net out | cpu s | ok |"
    );
    let _ = writeln!(
        md,
        "| --- | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |"
    );
    for case in results.cases.iter().filter(|c| c.family != "memory") {
        for p in &case.trials {
            for t in [&p.subject, &p.reference] {
                let d = &t.server_deltas;
                let cpu =
                    d.get("used_cpu_sys").unwrap_or(&0.0) + d.get("used_cpu_user").unwrap_or(&0.0);
                let _ = writeln!(
                    md,
                    "| {} | {} | {}→{} | {} | {} | {} | {:.2} | {} | {} | {} | {} | {} | {} | {} | {} | {:.1} | {} | {} | {:.2} | {} |",
                    case.case_id,
                    p.trial + 1,
                    p.order[0].name(),
                    p.order[1].name(),
                    t.engine.name(),
                    fmt_num(t.requests as f64),
                    fmt_num(t.samples as f64),
                    t.duration_s,
                    fmt_num(throughput(t)),
                    fmt_num(t.latency.p50_us),
                    fmt_num(t.latency.p95_us),
                    fmt_num(t.latency.p99_us),
                    fmt_num(t.latency.p999_us),
                    fmt_num(t.latency.max_us),
                    t.errors,
                    t.timeouts,
                    t.cycles_completed,
                    fmt_num(*d.get("total_net_input_bytes").unwrap_or(&0.0)),
                    fmt_num(*d.get("total_net_output_bytes").unwrap_or(&0.0)),
                    cpu,
                    if trial_valid(t) {
                        "yes".to_string()
                    } else {
                        format!(
                            "no: {}",
                            t.incomplete_reason.clone().unwrap_or_else(|| {
                                t.error_samples
                                    .first()
                                    .cloned()
                                    .unwrap_or_else(|| "errors".into())
                            })
                        )
                    }
                );
            }
        }
    }
    let _ = writeln!(md);

    // Problems
    let problems: Vec<String> = results
        .cases
        .iter()
        .flat_map(|c| {
            summarize(c)
                .problems
                .into_iter()
                .map(move |p| format!("- {}: {p}", c.case_id))
        })
        .collect();
    let memory_problems: Vec<String> = memory_cases
        .iter()
        .flat_map(|c| {
            c.memory.iter().flat_map(move |p| {
                [&p.subject, &p.reference]
                    .into_iter()
                    .filter(|m| !m.complete)
                    .map(move |m| {
                        format!(
                            "- {}: trial {} {}: {}",
                            c.case_id,
                            p.trial + 1,
                            m.engine.name(),
                            m.incomplete_reason.as_deref().unwrap_or("incomplete")
                        )
                    })
                    .collect::<Vec<_>>()
            })
        })
        .collect();
    if !problems.is_empty() || !memory_problems.is_empty() {
        let _ = writeln!(md, "## Invalid pairs");
        let _ = writeln!(md);
        let _ = writeln!(md, "These trials produced no ratio:");
        let _ = writeln!(md);
        for p in problems.iter().chain(memory_problems.iter()) {
            let _ = writeln!(md, "{p}");
        }
        let _ = writeln!(md);
    }

    let _ = writeln!(md, "## Definitions");
    let _ = writeln!(md);
    for case in &results.cases {
        if case.family != "memory" {
            let _ = writeln!(
                md,
                "- `{}`: latency = {}",
                case.case_id, case.latency_definition
            );
        }
    }
    let _ = writeln!(
        md,
        "- Trials alternate AB/BA order. Each trial recreates the series and, for read cases, preloads the fixture, \
         then deletes only the keys it created. Every reply was validated against the fixture; an erroneous or \
         incomplete trial yields no ratio. Write trials replay a fixed trace once; read trials loop over a fixed \
         request cycle for the timed duration after an untimed warm-up."
    );
    md
}

fn memory_ratios(case: &CaseResults) -> String {
    let complete: Vec<&MemoryPair> = case
        .memory
        .iter()
        .filter(|p| p.subject.complete && p.reference.complete)
        .collect();
    if complete.is_empty() {
        return format!("`{}`: no complete memory pair.", case.case_id);
    }
    let s_bps: Vec<f64> = complete
        .iter()
        .filter_map(|p| p.subject.bytes_per_sample())
        .collect();
    let r_bps: Vec<f64> = complete
        .iter()
        .filter_map(|p| p.reference.bytes_per_sample())
        .collect();
    let s_loaded: Vec<f64> = complete
        .iter()
        .map(|p| (p.subject.loaded.used_memory - p.subject.before.used_memory) as f64)
        .collect();
    let r_loaded: Vec<f64> = complete
        .iter()
        .map(|p| (p.reference.loaded.used_memory - p.reference.before.used_memory) as f64)
        .collect();
    format!(
        "`{}`: bytes/sample subject {} vs reference {} (ratio ref/subj {}); dataset delta (loaded − before) subject {} vs reference {} bytes (ratio {}). {} complete pair(s).",
        case.case_id,
        fmt_median_spread(&s_bps),
        fmt_median_spread(&r_bps),
        fmt_ratio(&r_bps, &s_bps),
        fmt_median_spread(&s_loaded),
        fmt_median_spread(&r_loaded),
        fmt_ratio(&r_loaded, &s_loaded),
        complete.len()
    )
}

fn json_cell(v: &Json) -> String {
    match v {
        Json::String(s) => s.clone(),
        Json::Null => "—".to_string(),
        other => other.to_string(),
    }
}

pub fn csv(results: &RunResults) -> String {
    let mut out = String::new();
    out.push_str(
        "case,family,connections,pipeline,trial,order,engine,complete,requests,samples,duration_s,commands_per_s,samples_per_s,p50_us,p95_us,p99_us,p999_us,max_us,mean_us,observations,errors,timeouts,cycles,net_in_bytes,net_out_bytes,cpu_s,state_verified,under_calibrated,incomplete_reason\n",
    );
    for case in &results.cases {
        for p in &case.trials {
            for t in [&p.subject, &p.reference] {
                let d = &t.server_deltas;
                let _ = writeln!(
                    out,
                    "{},{},{},{},{},{}>{},{},{},{},{},{:.6},{:.3},{:.3},{:.1},{:.1},{:.1},{:.1},{:.1},{:.1},{},{},{},{:.3},{},{},{:.4},{},{},{}",
                    case.case_id,
                    case.family,
                    case.connections,
                    case.pipeline,
                    p.trial + 1,
                    p.order[0].name(),
                    p.order[1].name(),
                    t.engine.name(),
                    t.complete,
                    t.requests,
                    t.samples,
                    t.duration_s,
                    throughput(t),
                    samples_per_s(t),
                    t.latency.p50_us,
                    t.latency.p95_us,
                    t.latency.p99_us,
                    t.latency.p999_us,
                    t.latency.max_us,
                    t.latency.mean_us,
                    t.latency.observations,
                    t.errors,
                    t.timeouts,
                    t.cycles_completed,
                    d.get("total_net_input_bytes").unwrap_or(&0.0),
                    d.get("total_net_output_bytes").unwrap_or(&0.0),
                    d.get("used_cpu_sys").unwrap_or(&0.0) + d.get("used_cpu_user").unwrap_or(&0.0),
                    t.state_verified.map(|b| b.to_string()).unwrap_or_default(),
                    t.under_calibrated,
                    csv_escape(t.incomplete_reason.as_deref().unwrap_or(""))
                );
            }
        }
    }
    out.push_str("\nmemory_case,trial,engine,complete,before_used_memory,created_empty_used_memory,loaded_used_memory,loaded_rss,series,samples,bytes_per_sample,bytes_per_series_empty,ts_info_memory_sampled,memory_usage_sampled,sampled_keys,fresh_process,incomplete_reason\n");
    for case in &results.cases {
        for p in &case.memory {
            for m in [&p.subject, &p.reference] {
                let _ = writeln!(
                    out,
                    "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
                    case.case_id,
                    p.trial + 1,
                    m.engine.name(),
                    m.complete,
                    m.before.used_memory,
                    m.created_empty.used_memory,
                    m.loaded.used_memory,
                    m.loaded.used_memory_rss,
                    m.series,
                    m.samples,
                    m.bytes_per_sample()
                        .map(|v| format!("{v:.3}"))
                        .unwrap_or_default(),
                    m.bytes_per_series_empty()
                        .map(|v| format!("{v:.1}"))
                        .unwrap_or_default(),
                    m.ts_info_memory_sampled,
                    m.memory_usage_sampled,
                    m.sampled_keys,
                    m.fresh_process,
                    csv_escape(m.incomplete_reason.as_deref().unwrap_or(""))
                );
            }
        }
    }
    out
}

fn csv_escape(s: &str) -> String {
    if s.contains([',', '"', '\n']) {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::results::LatencySummary;
    use crate::trace::Engine;
    use std::collections::BTreeMap;

    fn trial(engine: Engine, requests: u64, duration_s: f64, p50: f64) -> TrialResult {
        TrialResult {
            engine,
            started_at: String::new(),
            duration_s,
            requests,
            samples: requests,
            errors: 0,
            timeouts: 0,
            error_samples: vec![],
            complete: true,
            incomplete_reason: None,
            latency: LatencySummary {
                observations: requests,
                p50_us: p50,
                p95_us: p50 * 2.0,
                p99_us: p50 * 3.0,
                p999_us: p50 * 4.0,
                max_us: p50 * 5.0,
                mean_us: p50,
            },
            server_deltas: BTreeMap::new(),
            cycles_completed: 1.0,
            state_verified: Some(true),
            under_calibrated: false,
        }
    }

    fn case(pairs: Vec<(TrialResult, TrialResult)>) -> CaseResults {
        CaseResults {
            case_id: "c".into(),
            family: "add".into(),
            connections: 1,
            pipeline: 1,
            latency_definition: String::new(),
            trials: pairs
                .into_iter()
                .enumerate()
                .map(|(i, (s, r))| TrialPair {
                    trial: i as u32,
                    order: [Engine::Subject, Engine::Reference],
                    subject: s,
                    reference: r,
                })
                .collect(),
            memory: vec![],
        }
    }

    #[test]
    fn median_and_spread() {
        assert_eq!(median(&[3.0, 1.0, 2.0]), Some(2.0));
        assert_eq!(median(&[4.0, 1.0, 2.0, 3.0]), Some(2.5));
        assert_eq!(median(&[]), None);
        assert_eq!(spread(&[3.0, 1.0, 2.0]), Some((1.0, 3.0)));
    }

    #[test]
    fn ratios_use_medians_and_the_right_direction() {
        let c = case(vec![
            (
                trial(Engine::Subject, 2000, 1.0, 100.0),
                trial(Engine::Reference, 1000, 1.0, 200.0),
            ),
            (
                trial(Engine::Subject, 2200, 1.0, 110.0),
                trial(Engine::Reference, 1100, 1.0, 220.0),
            ),
            (
                trial(Engine::Subject, 1800, 1.0, 90.0),
                trial(Engine::Reference, 900, 1.0, 180.0),
            ),
        ]);
        let s = summarize(&c);
        assert_eq!(s.valid_pairs, 3);
        assert_eq!(fmt_ratio(&s.subject_tput, &s.reference_tput), "2.00");
        assert_eq!(fmt_ratio(&s.reference_p[0], &s.subject_p[0]), "2.00");
        let pairs: Vec<(f64, f64)> = s
            .subject_tput
            .iter()
            .cloned()
            .zip(s.reference_tput.iter().cloned())
            .collect();
        let (lo, hi) = bootstrap_ratio_ci(&pairs).unwrap();
        assert!(lo <= 2.0 && hi >= 2.0, "{lo}..{hi}");
    }

    #[test]
    fn invalid_pairs_are_suppressed_not_averaged() {
        let mut bad = trial(Engine::Reference, 1000, 1.0, 200.0);
        bad.errors = 3;
        bad.error_samples.push("expected OK".into());
        let c = case(vec![
            (trial(Engine::Subject, 2000, 1.0, 100.0), bad),
            (
                trial(Engine::Subject, 2000, 1.0, 100.0),
                trial(Engine::Reference, 500, 1.0, 400.0),
            ),
        ]);
        let s = summarize(&c);
        assert_eq!(s.valid_pairs, 1);
        assert_eq!(s.problems.len(), 1);
        assert!(s.problems[0].contains("reference 3 error(s)"));
        assert_eq!(fmt_ratio(&s.subject_tput, &s.reference_tput), "4.00");
        assert!(
            fmt_ci(&[(1.0, 1.0)]).is_empty(),
            "no CI below the pair threshold"
        );
    }

    #[test]
    fn number_formatting() {
        assert_eq!(fmt_num(1234567.0), "1,234,567");
        assert_eq!(fmt_num(99.5), "99.5");
        assert_eq!(fmt_num(1.234), "1.23");
        assert_eq!(csv_escape("a,b"), "\"a,b\"");
    }

    #[test]
    fn bootstrap_is_deterministic() {
        let pairs = vec![(2.0, 1.0), (2.2, 1.1), (1.8, 0.9), (2.1, 1.0)];
        assert_eq!(bootstrap_ratio_ci(&pairs), bootstrap_ratio_ci(&pairs));
    }
}
