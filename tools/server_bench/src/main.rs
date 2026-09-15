//! Comparative server benchmark driver (docs/plans/rts-comparative-benchmarks-plan.md).
//!
//! Subcommands:
//!
//!   dry-run     parse a scenario, print counts, fixture size and trial budget
//!   preflight   verify the fixture, build and hash every trace, validate both
//!               engines, write the run manifest — no timed traffic
//!   run         preflight, then execute every case's paired trials, write
//!               results.json and regenerate results.csv / report.md
//!   report      regenerate results.csv / report.md from a run directory
//!
//! `tools/server_bench.sh` builds everything, exports the fixture, starts the
//! servers and invokes this binary.

mod engine;
mod executor;
mod fixture;
mod manifest;
mod preflight;
mod report;
mod results;
mod scenario;
mod trace;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};

use crate::fixture::Fixture;
use crate::manifest::{ManifestInputs, RunManifest, new_run_id};
use crate::preflight::{EngineArgs, Identity, ReferencePin};
use crate::scenario::{CaseKind, Scenario};
use crate::trace::Engine;

#[derive(Parser)]
#[command(name = "server_bench", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Print what a scenario would do without touching a server.
    DryRun {
        #[arg(long)]
        scenario: PathBuf,
        /// An exported fixture; with it, counts are exact rather than estimated.
        #[arg(long)]
        fixture: Option<PathBuf>,
    },
    /// Validate fixture, traces and both engines; write the run manifest.
    Preflight(RunArgs),
    /// Preflight and then run the trials.
    Run(RunArgs),
    /// Regenerate CSV/Markdown from a run directory's raw data.
    Report {
        #[arg(long)]
        run_dir: PathBuf,
    },
    /// Print the `benchmark_dataset` arguments that export a scenario's fixture,
    /// one per line, for the shell wrapper.
    FixtureArgs {
        #[arg(long)]
        scenario: PathBuf,
    },
    /// Put several runs side by side, grouped by equivalent scenario and
    /// resource budget (encoding and dataset sweeps).
    Compare {
        /// Run directories (each holding manifest.json and results.json).
        #[arg(long = "run-dir", required = true)]
        run_dirs: Vec<PathBuf>,
        /// Output Markdown file (default: print to stdout).
        #[arg(long)]
        out: Option<PathBuf>,
    },
}

#[derive(Args, Clone)]
struct RunArgs {
    #[arg(long)]
    scenario: PathBuf,
    #[arg(long)]
    fixture: PathBuf,
    /// Run directory (created). Manifest, preflight report and trace digests go here.
    #[arg(long)]
    out: PathBuf,
    #[arg(long)]
    subject: String,
    #[arg(long)]
    reference: String,
    /// Pinned reference versions as <server>:<module>, e.g. 8.10.0:81000.
    /// Required unless --self-check.
    #[arg(long)]
    reference_pin: Option<String>,
    /// The "reference" is a second subject build: a harness self-check, never a
    /// product comparison. Reports are marked accordingly.
    #[arg(long)]
    self_check: bool,
    /// The harness started the subject and may configure it.
    #[arg(long)]
    subject_owned: bool,
    /// The harness started the reference and may configure it.
    #[arg(long)]
    reference_owned: bool,
    /// Module binary loaded by the subject, hashed into the manifest.
    #[arg(long)]
    module_path: Option<PathBuf>,
    /// Repository root, for commit/dirty state.
    #[arg(long)]
    repo_root: Option<PathBuf>,
    /// Free-form key=value facts recorded verbatim in the manifest.
    #[arg(long = "note", value_name = "KEY=VALUE")]
    notes: Vec<String>,
    /// Override the scenario's trial count (recorded as an override).
    #[arg(long)]
    trials: Option<u32>,
    /// Override the scenario's timed read duration (recorded as an override).
    #[arg(long)]
    read_duration_seconds: Option<u64>,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match cli.command {
        Cmd::DryRun { scenario, fixture } => {
            dry_run(&scenario, fixture.as_deref()).map(|_| ExitCode::SUCCESS)
        }
        Cmd::Preflight(args) => preflight_cmd(&args).map(|_| ExitCode::SUCCESS),
        Cmd::Run(args) => run_cmd(&args),
        Cmd::Report { run_dir } => report_cmd(&run_dir),
        Cmd::FixtureArgs { scenario } => fixture_args_cmd(&scenario).map(|_| ExitCode::SUCCESS),
        Cmd::Compare { run_dirs, out } => {
            compare_cmd(&run_dirs, out.as_deref()).map(|_| ExitCode::SUCCESS)
        }
    };
    match result {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

// -------- dry-run --------

fn dry_run(scenario_path: &Path, fixture_dir: Option<&Path>) -> Result<()> {
    let scenario = Scenario::load(scenario_path)?;
    let f = &scenario.fixture;
    let total = f.total_samples();

    println!(
        "scenario  {} (schema {})",
        scenario.name, scenario.schema_version
    );
    if !scenario.description.is_empty() {
        println!("          {}", scenario.description);
    }
    println!(
        "fixture   {} series x {} samples = {} samples; {}/{}; labels {:?}",
        f.series, f.samples_per_series, total, f.workload, f.timestamp_model, f.label_cardinality
    );
    println!(
        "series    CHUNK_SIZE {} DUPLICATE_POLICY {} ENCODING subject={} reference={}",
        scenario.series.chunk_size,
        scenario.series.duplicate_policy.as_arg(),
        scenario.series.encoding.subject.as_arg(),
        scenario.series.encoding.reference.as_arg()
    );
    println!(
        "protocol  {:?}; trials {}; warm-up {} s; read trial {} s; read cycle {} requests",
        scenario.protocol,
        scenario.trials,
        scenario.warmup_seconds,
        scenario.read_duration_seconds,
        scenario.read_cycle_requests
    );
    println!("commands  {}", scenario.commands().join(" "));
    println!();
    println!("export    benchmark_dataset {}", fixture_args(f).join(" "));
    // Same line-size model as the exporter's own dry run.
    let est_bytes = total as u64 * (f.series.max(1).to_string().len() as u64 + 34)
        + f.series as u64
            * (f.key_prefix.len() as u64
                + 8
                + f.label_cardinality.len() as u64 * (f.label_value_len as u64 + 5));
    println!(
        "            estimated fixture size ~{}",
        human_bytes(est_bytes)
    );
    println!();

    let loaded = match fixture_dir {
        Some(dir) => {
            let fx =
                Fixture::load(dir).with_context(|| format!("loading fixture {}", dir.display()))?;
            Fixture::matches_spec(&fx.manifest, f)?;
            println!(
                "fixture   {} verified (sha256 {})",
                dir.display(),
                fx.manifest.sha256
            );
            Some(fx)
        }
        None => None,
    };

    println!(
        "{:<24} {:>6} {:>6} {:>12} {:>12} {:>10}  timed phase",
        "case", "conns", "pipe", "setup", "preload", "workload"
    );
    let mut budget_s = 0u64;
    for case in &scenario.cases {
        let (setup, preload, workload, bytes) = match &loaded {
            Some(fx) => {
                let d = trace::build_case(&scenario, fx, case).digest();
                (
                    d.setup_frames,
                    d.preload_frames,
                    d.workload_frames,
                    Some(d.workload_bytes),
                )
            }
            None => estimate_frames(&scenario, case),
        };
        let (phase, secs) = phase_budget(&scenario, &case.kind);
        budget_s += secs;
        println!(
            "{:<24} {:>6} {:>6} {:>12} {:>12} {:>10}  {}{}",
            case.id,
            case.connections,
            case.pipeline,
            setup,
            preload,
            workload,
            phase,
            bytes
                .map(|b| format!(" (~{} on the wire)", human_bytes(b)))
                .unwrap_or_default()
        );
    }
    println!();
    println!(
        "budget    >= {} s of timed/warm-up traffic across both engines ({} trials each), \
         excluding setup, preload, calibration and settle time",
        budget_s, scenario.trials
    );
    if loaded.is_none() {
        println!("          frame counts are estimates; pass --fixture for exact figures");
    }
    Ok(())
}

/// The exporter invocation for a scenario's fixture spec. Single source of
/// truth for the wrapper (`fixture-args`) and the dry run.
fn fixture_args(f: &scenario::FixtureSpec) -> Vec<String> {
    let labels = if f.label_cardinality.is_empty() {
        "none".to_string()
    } else {
        f.label_cardinality
            .iter()
            .map(|c| c.to_string())
            .collect::<Vec<_>>()
            .join(",")
    };
    vec![
        "--series".into(),
        f.series.to_string(),
        "--samples".into(),
        f.samples_per_series.to_string(),
        "--workload".into(),
        f.workload.clone(),
        "--ts-model".into(),
        f.timestamp_model.clone(),
        "--interval-ms".into(),
        f.interval_ms.to_string(),
        "--key-prefix".into(),
        f.key_prefix.clone(),
        "--label-cardinality".into(),
        labels,
        "--label-value-len".into(),
        f.label_value_len.to_string(),
    ]
}

fn fixture_args_cmd(scenario_path: &Path) -> Result<()> {
    let scenario = Scenario::load(scenario_path)?;
    for a in fixture_args(&scenario.fixture) {
        println!("{a}");
    }
    Ok(())
}

fn estimate_frames(
    scenario: &Scenario,
    case: &scenario::Case,
) -> (usize, usize, usize, Option<u64>) {
    let f = &scenario.fixture;
    let total = f.total_samples();
    let conns = case.connections as usize;
    let preload = total.div_ceil(128);
    match &case.kind {
        CaseKind::Add {} => (f.series, 0, total, None),
        CaseKind::Madd { batch, .. } => {
            let per_conn: usize = (0..conns)
                .map(|c| {
                    ((f.series.saturating_sub(c)).div_ceil(conns) * f.samples_per_series)
                        .div_ceil(*batch)
                })
                .sum();
            (f.series, 0, per_conn, None)
        }
        CaseKind::Memory {} => (f.series, preload, 0, None),
        _ => (f.series, preload, scenario.read_cycle_requests, None),
    }
}

/// `(description, lower bound in seconds over both engines)`.
fn phase_budget(scenario: &Scenario, kind: &CaseKind) -> (String, u64) {
    let trials = scenario.trials as u64;
    if kind.is_read() {
        let per = scenario.warmup_seconds + scenario.read_duration_seconds;
        (
            format!(
                "{} s warm-up + {} s timed per trial",
                scenario.warmup_seconds, scenario.read_duration_seconds
            ),
            trials * 2 * per,
        )
    } else if kind.is_write() {
        // Calibrated so the faster engine runs >= 30 s (plan, rule 3).
        (
            "fixed trace, calibrated to >= 30 s on the faster engine".to_string(),
            trials * 2 * 30,
        )
    } else {
        ("memory snapshots on fresh processes".to_string(), 0)
    }
}

fn human_bytes(b: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = b as f64;
    let mut i = 0;
    while v >= 1024.0 && i < UNITS.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{b} B")
    } else {
        format!("{v:.1} {}", UNITS[i])
    }
}

// -------- preflight / run --------

struct Prepared {
    run_dir: PathBuf,
    manifest: RunManifest,
    scenario: Scenario,
    fixture: Fixture,
    traces: Vec<trace::CaseTrace>,
    subject: EngineArgs,
    reference: EngineArgs,
}

fn engine_args(args: &RunArgs) -> Result<(EngineArgs, EngineArgs)> {
    let reference_identity = if args.self_check {
        if args.reference_pin.is_some() {
            bail!("--reference-pin and --self-check are mutually exclusive");
        }
        Identity::Subject
    } else {
        let Some(pin) = &args.reference_pin else {
            bail!(
                "--reference-pin <server>:<module> is required for a real comparison (or pass --self-check)"
            );
        };
        Identity::Reference(ReferencePin::parse(pin)?)
    };
    Ok((
        EngineArgs {
            role: Engine::Subject,
            url: args.subject.clone(),
            owned: args.subject_owned,
            identity: Identity::Subject,
        },
        EngineArgs {
            role: Engine::Reference,
            url: args.reference.clone(),
            owned: args.reference_owned,
            identity: reference_identity,
        },
    ))
}

fn parse_notes(notes: &[String]) -> Result<BTreeMap<String, String>> {
    let mut out = BTreeMap::new();
    for n in notes {
        let Some((k, v)) = n.split_once('=') else {
            bail!("--note expects KEY=VALUE, got {n:?}");
        };
        out.insert(k.trim().to_string(), v.to_string());
    }
    Ok(out)
}

/// Everything before timed traffic: load and verify, build and hash traces,
/// validate both engines, write the manifest.
fn prepare(args: &RunArgs, argv: &[String]) -> Result<Prepared> {
    let mut scenario = Scenario::load(&args.scenario)?;
    let (subject, reference) = engine_args(args)?;
    let mut notes = parse_notes(&args.notes)?;

    // Command-line overrides are applied to the scenario body that goes into
    // the manifest, and noted, so the manifest alone still says what ran.
    if let Some(t) = args.trials {
        notes.insert(
            "override.trials".into(),
            format!("{} -> {t}", scenario.trials),
        );
        scenario.trials = t;
    }
    if let Some(d) = args.read_duration_seconds {
        notes.insert(
            "override.read_duration_seconds".into(),
            format!("{} -> {d}", scenario.read_duration_seconds),
        );
        scenario.read_duration_seconds = d;
    }
    scenario.validate().context("scenario after overrides")?;

    let fixture = Fixture::load(&args.fixture)
        .with_context(|| format!("loading fixture {}", args.fixture.display()))?;
    Fixture::matches_spec(&fixture.manifest, &scenario.fixture)?;
    eprintln!(
        "fixture   {} verified: {} series x {} samples, sha256 {}",
        args.fixture.display(),
        fixture.manifest.shape.series,
        fixture.manifest.shape.samples_per_series,
        fixture.manifest.sha256
    );

    let traces = trace::build_all(&scenario, &fixture);
    let digests: Vec<_> = traces.iter().map(|t| t.digest()).collect();
    for d in &digests {
        eprintln!(
            "trace     {:<24} setup {:>8} preload {:>8} workload {:>10} ({} conns, {}) sha256 {}",
            d.case_id,
            d.setup_frames,
            d.preload_frames,
            d.workload_frames,
            d.connections,
            human_bytes(d.workload_bytes),
            &d.workload_sha256[..16]
        );
    }

    let run_id = new_run_id(&scenario.name);
    let run_dir = args.out.join(&run_id);
    fs::create_dir_all(run_dir.join("fixtures"))
        .with_context(|| format!("creating {}", run_dir.display()))?;
    fs::copy(
        args.fixture.join("fixture.json"),
        run_dir.join("fixtures/fixture.json"),
    )?;
    fs::copy(&args.scenario, run_dir.join("scenario.json"))?;

    let mut manifest = RunManifest::build(ManifestInputs {
        run_id: &run_id,
        args: argv,
        repo_root: args.repo_root.as_deref(),
        module_path: args.module_path.as_deref(),
        notes,
        scenario_path: &args.scenario,
        scenario: &scenario,
        fixture_dir: &args.fixture,
        fixture: &fixture.manifest,
        traces: digests.clone(),
        subject: &subject,
        reference: &reference,
    })?;
    manifest.write(&run_dir)?;
    fs::write(
        run_dir.join("traces.json"),
        serde_json::to_string_pretty(&digests)?,
    )?;
    eprintln!("run       {}", run_dir.display());

    let report = match preflight::run(&scenario, &subject, &reference) {
        Ok(r) => r,
        Err(e) => {
            manifest.status = format!("preflight_failed: {e:#}");
            manifest.write(&run_dir)?;
            return Err(e.context("preflight failed"));
        }
    };
    for w in &report.warnings {
        eprintln!("warning   {w}");
    }
    eprintln!(
        "subject   {} {} / module {} {} ({}, {})",
        report.subject.server_name,
        report.subject.server_version,
        report.subject.module_name,
        report.subject.module_version,
        report.subject.os,
        report.subject.mem_allocator
    );
    eprintln!(
        "reference {} {} / module {} {} ({}, {})",
        report.reference.server_name,
        report.reference.server_version,
        report.reference.module_name,
        report.reference.module_version,
        report.reference.os,
        report.reference.mem_allocator
    );
    eprintln!("comparison kind: {:?}", report.comparison);

    fs::write(
        run_dir.join("preflight.json"),
        serde_json::to_string_pretty(&report)?,
    )?;
    manifest.preflight = Some(report);
    manifest.status = "preflight_ok".to_string();
    manifest.write(&run_dir)?;
    Ok(Prepared {
        run_dir,
        manifest,
        scenario,
        fixture,
        traces,
        subject,
        reference,
    })
}

fn preflight_cmd(args: &RunArgs) -> Result<()> {
    let argv: Vec<String> = std::env::args().collect();
    let p = prepare(args, &argv)?;
    println!("{}", p.run_dir.display());
    Ok(())
}

fn run_cmd(args: &RunArgs) -> Result<ExitCode> {
    let argv: Vec<String> = std::env::args().collect();
    let mut p = prepare(args, &argv)?;
    let preflight = p.manifest.preflight.as_ref().expect("preflight ran");

    let target = |ea: &EngineArgs, facts: &engine::EngineFacts| executor::Target {
        engine: ea.role,
        url: ea.url.clone(),
        protocol: p.scenario.protocol,
        empty_process: executor::snapshot_from_info(&facts.baseline_memory, 0),
    };
    let targets = [
        target(&p.subject, &preflight.subject),
        target(&p.reference, &preflight.reference),
    ];

    p.manifest.status = "running".to_string();
    p.manifest.write(&p.run_dir)?;
    let progress = executor::Progress(&|m: &str| eprintln!("{m}"));
    let mut results = results::RunResults {
        results_version: results::RESULTS_VERSION,
        run_id: p.manifest.run_id.clone(),
        cases: Vec::new(),
    };
    let started = std::time::Instant::now();
    // A memory case is only on a fresh process if nothing ran before it.
    let mut fresh = [true, true];
    let traces = std::mem::take(&mut p.traces);
    for (case, trace) in p.scenario.cases.iter().zip(traces) {
        eprintln!(
            "case      {} ({} conns, pipeline {})",
            case.id, case.connections, case.pipeline
        );
        let r = executor::run_case(
            &p.scenario,
            &p.fixture,
            case,
            trace,
            &targets,
            fresh,
            &progress,
        );
        results.cases.push(r);
        fresh = [false, false];
        // Persist after every case so an interrupted run still reports.
        fs::write(
            p.run_dir.join("results.json"),
            serde_json::to_string_pretty(&results)?,
        )?;
    }
    let elapsed = started.elapsed().as_secs_f64();

    p.manifest.status = "completed".to_string();
    p.manifest.write(&p.run_dir)?;
    report::write_reports(&p.run_dir)?;
    eprintln!(
        "done      {} case(s) in {:.0} s; report at {}",
        results.cases.len(),
        elapsed,
        p.run_dir.join("report.md").display()
    );
    println!("{}", p.run_dir.display());
    Ok(ExitCode::SUCCESS)
}

fn report_cmd(run_dir: &Path) -> Result<ExitCode> {
    let manifest = run_dir.join("manifest.json");
    if !manifest.is_file() {
        bail!(
            "{} is not a run directory (no manifest.json)",
            run_dir.display()
        );
    }
    if !run_dir.join("results.json").is_file() {
        bail!(
            "{} holds no results.json (the run did not get past preflight)",
            run_dir.display()
        );
    }
    report::write_reports(run_dir)?;
    println!("{}", run_dir.join("report.md").display());
    Ok(ExitCode::SUCCESS)
}

fn compare_cmd(run_dirs: &[PathBuf], out: Option<&Path>) -> Result<()> {
    let runs: Vec<report::LoadedRun> = run_dirs
        .iter()
        .map(|d| report::load_run(d))
        .collect::<Result<_>>()?;
    let md = report::compare(&runs);
    match out {
        Some(path) => {
            fs::write(path, md).with_context(|| format!("writing {}", path.display()))?;
            println!("{}", path.display());
        }
        None => print!("{md}"),
    }
    Ok(())
}
