//! Preflight: refuse to measure anything until both engines are exactly what
//! the run claims they are.
//!
//! Checks, per engine: reachable; pinned server/module versions; no
//! replication; persistence, eviction and keyspace notifications off;
//! every command the scenario sends is known to the server; the keyspace
//! (or, for an external server, our key prefix) is empty. On the subject,
//! `ts-compatibility-mode strict` is set when we own the server and required
//! when we do not — an externally managed server is never reconfigured.
//! Effective settings are read back after any change and recorded verbatim.

use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use serde::Serialize;

use crate::engine::{Control, EngineFacts, redact_url};
use crate::scenario::{Protocol, Scenario};
use crate::trace::Engine;

/// Pinned reference versions, single-sourced from `tests/reference_server.sh`
/// and passed in by `tools/server_bench.sh`.
#[derive(Debug, Clone, Serialize)]
pub struct ReferencePin {
    pub server_version: String,
    pub module_version: i64,
}

impl ReferencePin {
    /// `8.10.0:81000`
    pub fn parse(s: &str) -> Result<Self> {
        let Some((server, module)) = s.split_once(':') else {
            bail!("reference pin must look like <server-version>:<module-version>, got {s:?}");
        };
        Ok(Self {
            server_version: server.to_string(),
            module_version: module
                .parse()
                .with_context(|| format!("module version in reference pin {s:?}"))?,
        })
    }
}

/// How the engine at the far end is expected to identify itself.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Identity {
    /// Valkey with this module loaded (`MODULE LIST` name `ts`).
    Subject,
    /// The pinned RedisTimeSeries reference.
    Reference(ReferencePin),
}

#[derive(Debug, Clone, Serialize)]
pub struct EngineArgs {
    pub role: Engine,
    #[serde(serialize_with = "serialize_redacted")]
    pub url: String,
    /// True when the harness started this server and may reconfigure it.
    pub owned: bool,
    pub identity: Identity,
}

fn serialize_redacted<S: serde::Serializer>(url: &str, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&redact_url(url))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ComparisonKind {
    /// Subject versus the pinned reference: the only kind a report may publish.
    Reference,
    /// Two subject builds: a harness self-check, never a product comparison.
    SelfCheck,
}

#[derive(Debug, Clone, Serialize)]
pub struct PreflightReport {
    pub comparison: ComparisonKind,
    pub protocol: Protocol,
    pub commands_checked: Vec<String>,
    pub subject: EngineFacts,
    pub reference: EngineFacts,
    pub warnings: Vec<String>,
}

/// Server settings that must hold for a comparable baseline. Read back and
/// compared after the subject's compatibility mode is applied.
const REQUIRED_CONFIG: &[(&str, &str)] = &[
    ("save", ""),
    ("appendonly", "no"),
    ("maxmemory-policy", "noeviction"),
    ("notify-keyspace-events", ""),
];

/// Settings recorded but not constrained.
const RECORDED_CONFIG: &[&str] = &["maxmemory", "io-threads", "hz", "lazyfree-lazy-user-del"];

const SUBJECT_MODULE: &str = "ts";
const SUBJECT_COMPAT_PARAM: &str = "ts.ts-compatibility-mode";
const REFERENCE_MODULE: &str = "timeseries";

pub fn run(
    scenario: &Scenario,
    subject: &EngineArgs,
    reference: &EngineArgs,
) -> Result<PreflightReport> {
    let comparison = match (&subject.identity, &reference.identity) {
        (Identity::Subject, Identity::Reference(_)) => ComparisonKind::Reference,
        (Identity::Subject, Identity::Subject) => ComparisonKind::SelfCheck,
        (Identity::Reference(_), _) => bail!("the subject must be a Valkey TimeSeries build"),
    };
    let commands = scenario.commands();
    let mut warnings = Vec::new();

    let subject_facts = check_engine(scenario, subject, &commands, &mut warnings)?;
    let reference_facts = check_engine(scenario, reference, &commands, &mut warnings)?;

    // `run_id` is unique per server process, so this catches the same server
    // reached through two different URLs as well.
    if subject_facts.run_id == reference_facts.run_id {
        bail!(
            "subject and reference are the same server process ({} / {})",
            redact_url(&subject.url),
            redact_url(&reference.url)
        );
    }

    Ok(PreflightReport {
        comparison,
        protocol: scenario.protocol,
        commands_checked: commands.iter().map(|c| c.to_string()).collect(),
        subject: subject_facts,
        reference: reference_facts,
        warnings,
    })
}

fn check_engine(
    scenario: &Scenario,
    args: &EngineArgs,
    commands: &[&str],
    warnings: &mut Vec<String>,
) -> Result<EngineFacts> {
    let name = args.role.name();
    let mut c = Control::connect(args.role, &args.url, scenario.protocol)?;
    c.ping()?;

    let server = c.info("server")?;
    let modules = c.modules()?;
    let (server_name, server_version) = identify_server(&server);

    let (module_name, module_version) = match &args.identity {
        Identity::Subject => {
            let Some(v) = modules.get(SUBJECT_MODULE) else {
                bail!(
                    "{name}: module {SUBJECT_MODULE:?} is not loaded (MODULE LIST: {:?})",
                    modules.keys().collect::<Vec<_>>()
                );
            };
            if server_name != "valkey" {
                bail!(
                    "{name}: expected a valkey server, INFO says {server_name:?} {server_version}"
                );
            }
            (SUBJECT_MODULE.to_string(), *v)
        }
        Identity::Reference(pin) => {
            let mut problems = Vec::new();
            let redis_version = server.get("redis_version").cloned().unwrap_or_default();
            if redis_version != pin.server_version {
                problems.push(format!(
                    "server version {redis_version:?}, pinned {:?}",
                    pin.server_version
                ));
            }
            let got = modules.get(REFERENCE_MODULE).copied();
            if got != Some(pin.module_version) {
                problems.push(format!(
                    "{REFERENCE_MODULE} module {got:?}, pinned {}",
                    pin.module_version
                ));
            }
            if !problems.is_empty() {
                bail!(
                    "{name}: reference pin mismatch: {}. Results against an unpinned reference \
                     are not comparable; see docs/plans/rts-reference-bumps.md",
                    problems.join("; ")
                );
            }
            (REFERENCE_MODULE.to_string(), pin.module_version)
        }
    };

    // Never a replica, never with replicas: replication traffic is a later project.
    let replication = c.info("replication")?;
    let role = replication.get("role").cloned().unwrap_or_default();
    let replicas = replication
        .get("connected_slaves")
        .cloned()
        .unwrap_or_else(|| "0".to_string());
    if role != "master" || replicas != "0" {
        bail!(
            "{name}: role {role:?} with {replicas} replica(s); the baseline is a standalone primary"
        );
    }

    // Subject compatibility mode: set when owned, required otherwise.
    if matches!(args.identity, Identity::Subject) {
        let current = c
            .config_get(SUBJECT_COMPAT_PARAM)?
            .get(SUBJECT_COMPAT_PARAM)
            .cloned()
            .unwrap_or_default();
        if current != "strict" {
            if args.owned {
                c.config_set(SUBJECT_COMPAT_PARAM, "strict")?;
            } else {
                bail!(
                    "{name}: {SUBJECT_COMPAT_PARAM} is {current:?} on an externally managed server; \
                     set it to strict yourself, the harness never reconfigures a server it does not own"
                );
            }
        }
        let after = c
            .config_get(SUBJECT_COMPAT_PARAM)?
            .get(SUBJECT_COMPAT_PARAM)
            .cloned()
            .unwrap_or_default();
        if after != "strict" {
            bail!("{name}: {SUBJECT_COMPAT_PARAM} reads back as {after:?} after CONFIG SET strict");
        }
    }

    // Baseline server settings, read back after any change above.
    let mut effective = BTreeMap::new();
    let mut problems = Vec::new();
    for (param, want) in REQUIRED_CONFIG {
        let got = c.config_get(param)?.get(*param).cloned();
        match got {
            Some(v) if v == *want => {
                effective.insert(param.to_string(), v);
            }
            Some(v) => {
                problems.push(format!("{param} is {v:?}, need {want:?}"));
                effective.insert(param.to_string(), v);
            }
            None => problems.push(format!("{param} is not a known parameter")),
        }
    }
    for param in RECORDED_CONFIG {
        if let Some(v) = c.config_get(param)?.get(*param) {
            effective.insert(param.to_string(), v.clone());
        }
    }
    if !problems.is_empty() {
        let hint = if args.owned {
            "the harness started this server with other flags; this is a harness bug"
        } else {
            "the harness never reconfigures a server it does not own; start it with the baseline settings"
        };
        bail!(
            "{name}: incompatible settings: {}. {hint}",
            problems.join("; ")
        );
    }

    let module_config = match args.identity {
        Identity::Subject => c.config_get("ts.*")?,
        Identity::Reference(_) => c.config_get("ts-*")?,
    };

    let unsupported = c.unsupported_commands(commands)?;
    if !unsupported.is_empty() {
        bail!(
            "{name}: unsupported command(s) {}; the scenario cannot run on this engine",
            unsupported.join(", ")
        );
    }

    // Freshness. Memory results need an exclusive, empty process; an external
    // server only has to be free of our prefix, and is flagged.
    if args.owned {
        let n = c.dbsize()?;
        if n != 0 {
            bail!("{name}: owned server already holds {n} key(s); it must start empty");
        }
    } else {
        let pattern = format!("{}:*", scenario.fixture.key_prefix);
        let n = c.count_keys(&pattern)?;
        if n != 0 {
            bail!(
                "{name}: {n} key(s) match {pattern:?} on the external server; remove them or choose another key_prefix"
            );
        }
        warnings.push(format!(
            "{name} is externally managed: whole-process memory figures are not exclusive to this run"
        ));
    }

    let memory = c.info("memory")?;
    let baseline_memory: BTreeMap<String, String> = memory
        .iter()
        .filter(|(k, _)| {
            k.starts_with("used_memory")
                || k.starts_with("allocator")
                || k.starts_with("mem_")
                || k.starts_with("rss_")
                || k.starts_with("total_system_memory")
        })
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    if let Some(v) = effective.get("io-threads")
        && v != "1"
    {
        warnings.push(format!(
            "{name}: io-threads is {v}; record both engines' threading in the report"
        ));
    }

    Ok(EngineFacts {
        role: args.role,
        url: redact_url(&args.url),
        server_name,
        server_version,
        module_name,
        module_version,
        os: server.get("os").cloned().unwrap_or_default(),
        arch_bits: server.get("arch_bits").cloned().unwrap_or_default(),
        process_id: server.get("process_id").cloned().unwrap_or_default(),
        run_id: server.get("run_id").cloned().unwrap_or_default(),
        mem_allocator: memory.get("mem_allocator").cloned().unwrap_or_default(),
        io_threads_active: server.get("io_threads_active").cloned().unwrap_or_default(),
        replication_role: role,
        connected_replicas: replicas,
        effective_config: effective,
        module_config,
        baseline_memory,
    })
}

/// `(product, version)` from `INFO server`: Valkey reports `server_name` and
/// `valkey_version`; Redis reports only `redis_version`.
fn identify_server(server: &BTreeMap<String, String>) -> (String, String) {
    if let Some(name) = server.get("server_name") {
        let version = server
            .get(&format!("{name}_version"))
            .or_else(|| server.get("redis_version"))
            .cloned()
            .unwrap_or_default();
        return (name.clone(), version);
    }
    (
        "redis".to_string(),
        server.get("redis_version").cloned().unwrap_or_default(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reference_pin_parses() {
        let p = ReferencePin::parse("8.10.0:81000").unwrap();
        assert_eq!(p.server_version, "8.10.0");
        assert_eq!(p.module_version, 81000);
        assert!(ReferencePin::parse("8.10.0").is_err());
        assert!(ReferencePin::parse("8.10.0:x").is_err());
    }

    #[test]
    fn identifies_both_products() {
        let mut m = BTreeMap::new();
        m.insert("redis_version".to_string(), "8.10.0".to_string());
        assert_eq!(
            identify_server(&m),
            ("redis".to_string(), "8.10.0".to_string())
        );

        m.insert("server_name".to_string(), "valkey".to_string());
        m.insert("valkey_version".to_string(), "9.0.4".to_string());
        m.insert("redis_version".to_string(), "7.2.4".to_string());
        assert_eq!(
            identify_server(&m),
            ("valkey".to_string(), "9.0.4".to_string())
        );
    }

    #[test]
    fn engine_args_serialize_redacted() {
        let a = EngineArgs {
            role: Engine::Subject,
            url: "redis://u:p@h:1".into(),
            owned: false,
            identity: Identity::Subject,
        };
        let json = serde_json::to_string(&a).unwrap();
        assert!(json.contains("redis://u:***@h:1"), "{json}");
        assert!(!json.contains(":p@"), "{json}");
    }
}
