//! A connection to one engine plus the facts preflight reads back from it.
//!
//! Everything here is untimed control traffic: version and module checks,
//! `CONFIG GET`, `INFO`. The timed transport (step 2) uses the same client
//! library but its own connections.

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail, ensure};
use redis::{Connection, Value};
use serde::Serialize;

use crate::scenario::Protocol;
use crate::trace::Engine;

pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
pub const CONTROL_TIMEOUT: Duration = Duration::from_secs(30);

/// Redact `user:password@` in a URL for logs and manifests.
pub fn redact_url(url: &str) -> String {
    let Some(scheme_end) = url.find("://") else {
        return url.to_string();
    };
    let rest = &url[scheme_end + 3..];
    let authority_end = rest.find('/').unwrap_or(rest.len());
    let authority = &rest[..authority_end];
    let Some(at) = authority.rfind('@') else {
        return url.to_string();
    };
    let userinfo = &authority[..at];
    let masked = match userinfo.split_once(':') {
        Some((user, _)) => format!("{user}:***"),
        None => "***".to_string(),
    };
    format!("{}{}@{}", &url[..scheme_end + 3], masked, &rest[at + 1..])
}

/// Append the protocol selector understood by the client library.
pub fn url_with_protocol(url: &str, protocol: Protocol) -> String {
    match protocol {
        Protocol::Resp2 => url.to_string(),
        Protocol::Resp3 => {
            if url.contains("protocol=") {
                url.to_string()
            } else if url.contains('?') {
                format!("{url}&protocol=resp3")
            } else {
                format!("{url}?protocol=resp3")
            }
        }
    }
}

pub struct Control {
    pub engine: Engine,
    con: Connection,
}

impl Control {
    pub fn connect(engine: Engine, url: &str, protocol: Protocol) -> Result<Self> {
        let full = url_with_protocol(url, protocol);
        let client = redis::Client::open(full.as_str())
            .with_context(|| format!("{}: invalid URL {}", engine.name(), redact_url(url)))?;
        let con = client
            .get_connection_with_timeout(CONNECT_TIMEOUT)
            .with_context(|| format!("{}: connecting to {}", engine.name(), redact_url(url)))?;
        con.set_read_timeout(Some(CONTROL_TIMEOUT))?;
        con.set_write_timeout(Some(CONTROL_TIMEOUT))?;
        Ok(Self { engine, con })
    }

    pub fn cmd(&mut self, args: &[&str]) -> Result<Value> {
        let mut c = redis::cmd(args[0]);
        for a in &args[1..] {
            c.arg(*a);
        }
        let v: Value = c
            .query(&mut self.con)
            .with_context(|| format!("{}: {}", self.engine.name(), args.join(" ")))?;
        if let Value::ServerError(e) = v {
            bail!("{}: {} -> {}", self.engine.name(), args.join(" "), e);
        }
        Ok(v)
    }

    pub fn ping(&mut self) -> Result<()> {
        match self.cmd(&["PING"])? {
            Value::SimpleString(s) if s == "PONG" => Ok(()),
            other => bail!("{}: unexpected PING reply {other:?}", self.engine.name()),
        }
    }

    /// `INFO <section>` as a flat key/value map.
    pub fn info(&mut self, section: &str) -> Result<BTreeMap<String, String>> {
        let text = as_text(&self.cmd(&["INFO", section])?)?;
        Ok(text
            .lines()
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .filter_map(|l| l.split_once(':'))
            .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
            .collect())
    }

    /// `CONFIG GET pattern` as a map (RESP2 flat array or RESP3 map).
    pub fn config_get(&mut self, pattern: &str) -> Result<BTreeMap<String, String>> {
        let v = self.cmd(&["CONFIG", "GET", pattern])?;
        pairs(&v)
    }

    pub fn config_set(&mut self, name: &str, value: &str) -> Result<()> {
        match self.cmd(&["CONFIG", "SET", name, value])? {
            Value::Okay => Ok(()),
            other => bail!(
                "{}: CONFIG SET {name} {value} -> {other:?}",
                self.engine.name()
            ),
        }
    }

    /// `MODULE LIST` as name -> version.
    pub fn modules(&mut self) -> Result<BTreeMap<String, i64>> {
        let v = self.cmd(&["MODULE", "LIST"])?;
        let entries = match v {
            Value::Array(a) => a,
            Value::Set(a) => a,
            other => bail!("{}: MODULE LIST -> {other:?}", self.engine.name()),
        };
        let mut out = BTreeMap::new();
        for e in &entries {
            let fields = pairs(e)?;
            if let (Some(name), Some(ver)) = (fields.get("name"), fields.get("ver")) {
                out.insert(name.clone(), ver.parse().unwrap_or(-1));
            }
        }
        Ok(out)
    }

    /// Which of `commands` the server does not know, per `COMMAND INFO`.
    pub fn unsupported_commands(&mut self, commands: &[&str]) -> Result<Vec<String>> {
        let mut args = vec!["COMMAND", "INFO"];
        args.extend_from_slice(commands);
        let v = self.cmd(&args)?;
        let Value::Array(entries) = v else {
            bail!("{}: COMMAND INFO -> unexpected reply", self.engine.name());
        };
        ensure!(
            entries.len() == commands.len(),
            "{}: COMMAND INFO returned {} entries for {} commands",
            self.engine.name(),
            entries.len(),
            commands.len()
        );
        Ok(commands
            .iter()
            .zip(entries.iter())
            .filter(|(_, e)| matches!(e, Value::Nil))
            .map(|(c, _)| c.to_string())
            .collect())
    }

    pub fn dbsize(&mut self) -> Result<i64> {
        match self.cmd(&["DBSIZE"])? {
            Value::Int(n) => Ok(n),
            other => bail!("{}: DBSIZE -> {other:?}", self.engine.name()),
        }
    }

    /// Number of keys matching `pattern`, via a full SCAN.
    pub fn count_keys(&mut self, pattern: &str) -> Result<u64> {
        let mut cursor = "0".to_string();
        let mut count = 0u64;
        loop {
            let v = self.cmd(&["SCAN", &cursor, "MATCH", pattern, "COUNT", "1000"])?;
            let Value::Array(parts) = v else {
                bail!("{}: SCAN -> unexpected reply", self.engine.name());
            };
            ensure!(
                parts.len() == 2,
                "{}: SCAN -> malformed reply",
                self.engine.name()
            );
            cursor = as_text(&parts[0])?;
            if let Value::Array(keys) = &parts[1] {
                count += keys.len() as u64;
            }
            if cursor == "0" {
                return Ok(count);
            }
        }
    }
}

/// Facts read from a running engine, recorded in the manifest.
#[derive(Debug, Clone, Serialize)]
pub struct EngineFacts {
    pub role: Engine,
    pub url: String,
    pub server_name: String,
    pub server_version: String,
    pub module_name: String,
    pub module_version: i64,
    pub os: String,
    pub arch_bits: String,
    pub process_id: String,
    /// `INFO server` `run_id`: unique per server process.
    pub run_id: String,
    pub mem_allocator: String,
    pub io_threads_active: String,
    pub replication_role: String,
    pub connected_replicas: String,
    /// Selected server configuration, read back after any changes.
    pub effective_config: BTreeMap<String, String>,
    /// Every module configuration parameter the server exposes.
    pub module_config: BTreeMap<String, String>,
    /// `INFO memory` on the fresh process, before any key exists.
    pub baseline_memory: BTreeMap<String, String>,
}

pub fn as_text(v: &Value) -> Result<String> {
    match v {
        Value::BulkString(b) => Ok(String::from_utf8_lossy(b).into_owned()),
        Value::SimpleString(s) => Ok(s.clone()),
        Value::VerbatimString { text, .. } => Ok(text.clone()),
        Value::Int(i) => Ok(i.to_string()),
        Value::Double(d) => Ok(d.to_string()),
        Value::Okay => Ok("OK".to_string()),
        Value::Nil => Ok(String::new()),
        other => Err(anyhow!("expected a string reply, got {other:?}")),
    }
}

/// Key/value pairs from either a RESP2 flat array or a RESP3 map. Nested
/// values (e.g. the `args` list in `MODULE LIST`) are kept as their debug
/// rendering rather than rejected.
pub fn pairs(v: &Value) -> Result<BTreeMap<String, String>> {
    fn value_text(v: &Value) -> Result<String> {
        as_text(v).or_else(|_| Ok(format!("{v:?}")))
    }
    let mut out = BTreeMap::new();
    match v {
        Value::Map(entries) => {
            for (k, v) in entries {
                out.insert(as_text(k)?, value_text(v)?);
            }
        }
        Value::Array(items) => {
            ensure!(items.len() % 2 == 0, "odd-length key/value array");
            for kv in items.chunks(2) {
                out.insert(as_text(&kv[0])?, value_text(&kv[1])?);
            }
        }
        other => bail!("expected a key/value reply, got {other:?}"),
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_passwords_only() {
        assert_eq!(
            redact_url("redis://127.0.0.1:6379"),
            "redis://127.0.0.1:6379"
        );
        assert_eq!(
            redact_url("redis://bob:s3cret@host:6379/0?protocol=resp3"),
            "redis://bob:***@host:6379/0?protocol=resp3"
        );
        assert_eq!(redact_url("redis://:pw@host"), "redis://:***@host");
        assert_eq!(redact_url("redis://tok@host"), "redis://***@host");
        // An '@' in the path is not userinfo.
        assert_eq!(redact_url("redis://host/a@b"), "redis://host/a@b");
        assert_eq!(redact_url("not a url"), "not a url");
    }

    #[test]
    fn protocol_selector_is_appended_once() {
        assert_eq!(url_with_protocol("redis://h", Protocol::Resp2), "redis://h");
        assert_eq!(
            url_with_protocol("redis://h", Protocol::Resp3),
            "redis://h?protocol=resp3"
        );
        assert_eq!(
            url_with_protocol("redis://h/1", Protocol::Resp3),
            "redis://h/1?protocol=resp3"
        );
        assert_eq!(
            url_with_protocol("redis://h?x=1", Protocol::Resp3),
            "redis://h?x=1&protocol=resp3"
        );
        assert_eq!(
            url_with_protocol("redis://h?protocol=resp2", Protocol::Resp3),
            "redis://h?protocol=resp2"
        );
    }

    #[test]
    fn pairs_accepts_both_protocol_shapes() {
        let flat = Value::Array(vec![
            Value::BulkString(b"a".to_vec()),
            Value::BulkString(b"1".to_vec()),
            Value::BulkString(b"b".to_vec()),
            Value::Int(2),
        ]);
        let map = Value::Map(vec![
            (
                Value::BulkString(b"a".to_vec()),
                Value::BulkString(b"1".to_vec()),
            ),
            (Value::BulkString(b"b".to_vec()), Value::Int(2)),
        ]);
        assert_eq!(pairs(&flat).unwrap(), pairs(&map).unwrap());
        assert_eq!(pairs(&map).unwrap()["b"], "2");
        assert!(pairs(&Value::Array(vec![Value::Int(1)])).is_err());

        // MODULE LIST entries end in `args`, an array: tolerated, not fatal.
        let module = Value::Array(vec![
            Value::BulkString(b"name".to_vec()),
            Value::BulkString(b"ts".to_vec()),
            Value::BulkString(b"args".to_vec()),
            Value::Array(vec![]),
        ]);
        assert_eq!(pairs(&module).unwrap()["name"], "ts");
    }
}
