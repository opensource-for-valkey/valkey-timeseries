//! The run manifest: everything needed to say what was measured, by what,
//! on what. Written before any trial runs so a crash mid-run still leaves an
//! honest record, then rewritten with a final status.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde::Serialize;

use crate::engine::redact_url;
use crate::fixture::{FixtureManifest, sha256_file};
use crate::preflight::{EngineArgs, PreflightReport};
use crate::scenario::Scenario;
use crate::trace::CaseDigest;

pub const MANIFEST_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize)]
pub struct RunManifest {
    pub manifest_version: u32,
    pub run_id: String,
    pub created_at: String,
    pub status: String,
    pub driver: DriverInfo,
    pub invocation: Invocation,
    pub source: SourceInfo,
    pub module: Option<ArtifactInfo>,
    pub host: HostInfo,
    /// Free-form facts the wrapper knows and the driver cannot discover
    /// (container limits, image digests, CPU affinity, ...).
    pub notes: BTreeMap<String, String>,
    pub scenario: ScenarioInfo,
    pub fixture: FixtureInfo,
    pub traces: Vec<CaseDigest>,
    pub engines: Engines,
    pub preflight: Option<PreflightReport>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DriverInfo {
    pub name: &'static str,
    pub version: &'static str,
    /// `rustc -V` of the toolchain that built the driver, when the wrapper
    /// passed it (`SERVER_BENCH_RUSTC`).
    pub rustc: Option<String>,
    pub client_library: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct Invocation {
    pub args: Vec<String>,
    pub cwd: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SourceInfo {
    pub repo_root: Option<String>,
    pub commit: Option<String>,
    pub branch: Option<String>,
    pub dirty: Option<bool>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ArtifactInfo {
    pub path: String,
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct HostInfo {
    pub hostname: Option<String>,
    pub os: String,
    pub arch: String,
    pub kernel: Option<String>,
    pub cpu_model: Option<String>,
    pub cpu_count: Option<usize>,
    pub total_memory_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ScenarioInfo {
    pub path: String,
    pub sha256: String,
    pub name: String,
    pub schema_version: u32,
    pub body: Scenario,
}

#[derive(Debug, Clone, Serialize)]
pub struct FixtureInfo {
    pub dir: String,
    pub sha256: String,
    pub manifest: FixtureManifest,
}

#[derive(Debug, Clone, Serialize)]
pub struct Engines {
    pub subject: EngineArgs,
    pub reference: EngineArgs,
}

pub struct ManifestInputs<'a> {
    pub run_id: &'a str,
    pub args: &'a [String],
    pub repo_root: Option<&'a Path>,
    pub module_path: Option<&'a Path>,
    pub notes: BTreeMap<String, String>,
    pub scenario_path: &'a Path,
    pub scenario: &'a Scenario,
    pub fixture_dir: &'a Path,
    pub fixture: &'a FixtureManifest,
    pub traces: Vec<CaseDigest>,
    pub subject: &'a EngineArgs,
    pub reference: &'a EngineArgs,
}

impl RunManifest {
    pub fn build(inputs: ManifestInputs<'_>) -> Result<Self> {
        let (_, scenario_sha) = sha256_file(inputs.scenario_path)?;
        let module = match inputs.module_path {
            Some(p) => {
                let (bytes, sha256) =
                    sha256_file(p).with_context(|| format!("hashing module {}", p.display()))?;
                Some(ArtifactInfo {
                    path: p.display().to_string(),
                    bytes,
                    sha256,
                })
            }
            None => None,
        };
        Ok(Self {
            manifest_version: MANIFEST_VERSION,
            run_id: inputs.run_id.to_string(),
            created_at: now_rfc3339(),
            status: "created".to_string(),
            driver: DriverInfo {
                name: env!("CARGO_PKG_NAME"),
                version: env!("CARGO_PKG_VERSION"),
                rustc: std::env::var("SERVER_BENCH_RUSTC").ok(),
                client_library: "redis (redis-rs) 1.x, sync",
            },
            invocation: Invocation {
                args: inputs.args.iter().map(|a| redact_url(a)).collect(),
                cwd: std::env::current_dir()
                    .map(|p| p.display().to_string())
                    .unwrap_or_default(),
            },
            source: source_info(inputs.repo_root),
            module,
            host: host_info(),
            notes: inputs.notes,
            scenario: ScenarioInfo {
                path: inputs.scenario_path.display().to_string(),
                sha256: scenario_sha,
                name: inputs.scenario.name.clone(),
                schema_version: inputs.scenario.schema_version,
                body: inputs.scenario.clone(),
            },
            fixture: FixtureInfo {
                dir: inputs.fixture_dir.display().to_string(),
                sha256: inputs.fixture.sha256.clone(),
                manifest: inputs.fixture.clone(),
            },
            traces: inputs.traces,
            engines: Engines {
                subject: inputs.subject.clone(),
                reference: inputs.reference.clone(),
            },
            preflight: None,
        })
    }

    pub fn write(&self, run_dir: &Path) -> Result<PathBuf> {
        let path = run_dir.join("manifest.json");
        let json = serde_json::to_string_pretty(self)?;
        fs::write(&path, json).with_context(|| format!("writing {}", path.display()))?;
        Ok(path)
    }
}

fn git(repo: &Path, args: &[&str]) -> Option<String> {
    let out = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

fn source_info(repo_root: Option<&Path>) -> SourceInfo {
    let Some(repo) = repo_root else {
        return SourceInfo {
            repo_root: None,
            commit: None,
            branch: None,
            dirty: None,
        };
    };
    SourceInfo {
        repo_root: Some(repo.display().to_string()),
        commit: git(repo, &["rev-parse", "HEAD"]),
        branch: git(repo, &["rev-parse", "--abbrev-ref", "HEAD"]),
        dirty: git(repo, &["status", "--porcelain", "--untracked-files=no"]).map(|s| !s.is_empty()),
    }
}

fn sh(cmd: &str, args: &[&str]) -> Option<String> {
    let out = Command::new(cmd).args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

fn host_info() -> HostInfo {
    let os = std::env::consts::OS;
    let (cpu_model, total_memory_bytes, kernel) = if os == "linux" {
        let cpuinfo = fs::read_to_string("/proc/cpuinfo").unwrap_or_default();
        let model = cpuinfo
            .lines()
            .find(|l| l.starts_with("model name"))
            .and_then(|l| l.split_once(':'))
            .map(|(_, v)| v.trim().to_string());
        let meminfo = fs::read_to_string("/proc/meminfo").unwrap_or_default();
        let mem = meminfo
            .lines()
            .find(|l| l.starts_with("MemTotal:"))
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|kb| kb.parse::<u64>().ok())
            .map(|kb| kb * 1024);
        (model, mem, sh("uname", &["-r"]))
    } else if os == "macos" {
        (
            sh("sysctl", &["-n", "machdep.cpu.brand_string"]),
            sh("sysctl", &["-n", "hw.memsize"]).and_then(|s| s.parse().ok()),
            sh("uname", &["-r"]),
        )
    } else {
        (None, None, sh("uname", &["-r"]))
    };
    HostInfo {
        hostname: sh("hostname", &[]),
        os: os.to_string(),
        arch: std::env::consts::ARCH.to_string(),
        kernel,
        cpu_model,
        cpu_count: std::thread::available_parallelism().ok().map(|n| n.get()),
        total_memory_bytes,
    }
}

/// `YYYY-MM-DDTHH:MM:SSZ` without a date-time dependency.
pub fn now_rfc3339() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format_rfc3339(secs)
}

pub fn format_rfc3339(secs: u64) -> String {
    let days = secs / 86_400;
    let rem = secs % 86_400;
    let (h, m, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    // Howard Hinnant's civil-from-days.
    let z = days as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mo = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if mo <= 2 { y + 1 } else { y };
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{m:02}:{s:02}Z")
}

/// A run id that sorts chronologically and cannot collide across processes.
pub fn new_run_id(scenario_name: &str) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format!(
        "{}-{}-{}",
        format_rfc3339(now.as_secs()).replace([':', '-'], ""),
        scenario_name,
        std::process::id()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc3339_formatting() {
        assert_eq!(format_rfc3339(0), "1970-01-01T00:00:00Z");
        assert_eq!(format_rfc3339(951_782_400), "2000-02-29T00:00:00Z");
        assert_eq!(format_rfc3339(1_700_000_000), "2023-11-14T22:13:20Z");
        assert_eq!(format_rfc3339(4_102_444_799), "2099-12-31T23:59:59Z");
    }

    #[test]
    fn run_ids_sort_and_name_the_scenario() {
        let id = new_run_id("smoke");
        assert!(id.contains("-smoke-"), "{id}");
        assert_eq!(id.split('-').next().unwrap().len(), 16); // YYYYMMDDTHHMMSSZ
    }
}
