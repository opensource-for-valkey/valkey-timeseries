//! Loads a running valkey-timeseries server with a realistic Kubernetes fleet.
//!
//! Generates a [`FleetTopology`]'s series and their samples with [`FleetMetrics`] --
//! node_exporter, cAdvisor, kube-state-metrics, JVM / Go runtimes and HTTP histograms, scraped
//! every interval over a window that ends now by default -- and writes each series as one
//! `TS.CREATE ... LABELS ...` followed by `TS.MADD` batches. Standalone servers and clusters both
//! work: the [`redis`] crate's cluster client sends every series to the primary that owns its
//! slot.
//!
//! Each worker thread generates series and sends them over its own connection, a pipeline of
//! `--pipeline` commands at a time. `--emit` writes the same commands as RESP to a file (or `-`
//! for stdout) instead, for `valkey-cli --pipe`.
//!
//! Run via `tools/fleet_loader.sh`; `--help` lists the flags.

use std::env;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use redis::cluster::{ClusterClient, ClusterConnection, cluster_pipe};
use redis::{
    Cmd, ConnectionAddr, IntoConnectionInfo, RedisConnectionInfo, RedisResult, ServerError, Value,
};
use valkey_timeseries::common::Sample;
use valkey_timeseries::tests::generators::{
    DEFAULT_FLEET_SEED, FleetMetrics, FleetPreset, FleetTopology, MAX_ROUTES_PER_SERVICE,
    ScrapeConfig, SeriesSpec,
};

struct Config {
    host: String,
    port: u16,
    user: Option<String>,
    password: Option<String>,
    topology: FleetTopology,
    preset: Option<FleetPreset>,
    scrape: ScrapeConfig,
    prefix: String,
    /// `(option, value)` pairs appended to every `TS.CREATE`.
    create_options: Vec<(&'static str, String)>,
    /// Labels added to every series. The server rejects a second series with the same label set
    /// under any key, so a second copy of one fleet needs one of these to tell it apart.
    extra_labels: Vec<(String, String)>,
    /// Samples per `TS.MADD`.
    batch: usize,
    /// Commands per pipeline: each worker sends this many, then waits for their replies.
    pipeline: usize,
    threads: usize,
    flush: bool,
    emit: Option<String>,
}

fn usage() -> ! {
    eprintln!(
        "usage: fleet_loader [-h HOST] [-p PORT] [--user USER] [--password PASSWORD]\n\
         \x20                   [--preset small|medium|large] [--seed N]\n\
         \x20                   [--clusters N] [--hosts N] [--namespaces N] [--pods N] [--routes N]\n\
         \x20                   [--duration DUR] [--interval DUR] [--end now|MS] [--jitter DUR]\n\
         \x20                   [--prefix P] [--label NAME=VALUE ...] [--retention DUR] [--encoding E]\n\
         \x20                   [--chunk-size BYTES] [--batch N] [--pipeline N] [--threads N]\n\
         \x20                   [--flush] [--emit PATH|-]\n\
         \n\
         DUR is a number with a unit (500ms, 15s, 5m, 1h, 2d); a bare number is milliseconds.\n\
         A second copy of the same fleet needs both another --prefix and a --label: the server\n\
         rejects a series whose label set already exists under any key.\n\
         --hosts must be at least 1 when --clusters is; --routes is capped at {MAX_ROUTES_PER_SERVICE}."
    );
    std::process::exit(2)
}

fn fail(msg: impl std::fmt::Display) -> ! {
    eprintln!("error: {msg}");
    std::process::exit(1)
}

fn parse_duration_ms(raw: &str) -> Option<i64> {
    let split = raw.find(|c: char| !c.is_ascii_digit()).unwrap_or(raw.len());
    let (num, unit) = raw.split_at(split);
    let n: i64 = num.parse().ok()?;
    let scale = match unit {
        "" | "ms" => 1,
        "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        _ => return None,
    };
    n.checked_mul(scale)
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after the epoch")
        .as_millis() as i64
}

fn parse_args() -> Config {
    let mut host = "127.0.0.1".to_string();
    let mut port = 6379;
    let (mut user, mut password) = (None, None);
    let mut preset = Some(FleetPreset::Small);
    let mut overrides: Vec<(String, usize)> = Vec::new();
    let mut seed = DEFAULT_FLEET_SEED;
    let (mut duration, mut interval, mut jitter) = (3_600_000, 15_000, 10);
    let mut end: Option<i64> = None;
    let mut prefix = "k8s:".to_string();
    let mut create_options = Vec::new();
    let mut extra_labels = Vec::new();
    let mut batch = 1000;
    let mut pipeline = 256;
    let mut threads =
        thread::available_parallelism().map_or(2, |n| n.get().saturating_sub(1).max(1));
    let mut flush = false;
    let mut emit = None;

    let mut args = env::args().skip(1);
    let next_value = |flag: &str, args: &mut dyn Iterator<Item = String>| -> String {
        args.next().unwrap_or_else(|| {
            eprintln!("{flag} requires a value");
            usage()
        })
    };
    fn number<T: std::str::FromStr>(flag: &str, raw: &str) -> T {
        raw.parse().unwrap_or_else(|_| {
            eprintln!("{flag} expects an integer, got '{raw}'");
            usage()
        })
    }
    fn dur(flag: &str, raw: &str) -> i64 {
        parse_duration_ms(raw).unwrap_or_else(|| {
            eprintln!("{flag} expects a duration such as 15s or 1h, got '{raw}'");
            usage()
        })
    }
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--host" => host = next_value(&arg, &mut args),
            "-p" | "--port" => port = number(&arg, &next_value(&arg, &mut args)),
            "--user" => user = Some(next_value(&arg, &mut args)),
            "-a" | "--password" => password = Some(next_value(&arg, &mut args)),
            "--preset" => {
                let raw = next_value(&arg, &mut args);
                preset = Some(FleetPreset::parse(&raw).unwrap_or_else(|| {
                    eprintln!("unknown preset '{raw}'");
                    usage()
                }));
            }
            "--seed" => {
                let raw = next_value(&arg, &mut args);
                seed = raw
                    .strip_prefix("0x")
                    .map(|h| u64::from_str_radix(h, 16))
                    .unwrap_or_else(|| raw.parse())
                    .unwrap_or_else(|_| {
                        eprintln!("--seed expects an integer, got '{raw}'");
                        usage()
                    });
            }
            "--clusters" | "--hosts" | "--namespaces" | "--pods" | "--routes" => {
                let n: usize = number(&arg, &next_value(&arg, &mut args));
                if arg == "--clusters" && n == 0 {
                    eprintln!("--clusters must be at least 1");
                    usage()
                }
                overrides.push((arg.clone(), n));
            }
            "--duration" => duration = dur(&arg, &next_value(&arg, &mut args)),
            "--interval" => interval = dur(&arg, &next_value(&arg, &mut args)),
            "--jitter" => jitter = dur(&arg, &next_value(&arg, &mut args)),
            "--end" => {
                let raw = next_value(&arg, &mut args);
                end = (raw != "now").then(|| number(&arg, &raw));
            }
            "--prefix" => prefix = next_value(&arg, &mut args),
            "--label" => {
                let raw = next_value(&arg, &mut args);
                match raw.split_once('=') {
                    Some((name, value)) if !name.is_empty() && !value.is_empty() => {
                        extra_labels.push((name.to_string(), value.to_string()))
                    }
                    _ => {
                        eprintln!("--label expects NAME=VALUE, got '{raw}'");
                        usage()
                    }
                }
            }
            "--retention" => {
                let ms = dur(&arg, &next_value(&arg, &mut args));
                create_options.push(("RETENTION", ms.to_string()));
            }
            "--encoding" => {
                create_options.push(("ENCODING", next_value(&arg, &mut args).to_uppercase()))
            }
            "--chunk-size" => {
                let bytes: u64 = number(&arg, &next_value(&arg, &mut args));
                create_options.push(("CHUNK_SIZE", bytes.to_string()));
            }
            "--batch" => batch = number::<usize>(&arg, &next_value(&arg, &mut args)).max(1),
            "--pipeline" => pipeline = number::<usize>(&arg, &next_value(&arg, &mut args)).max(1),
            "--threads" => threads = number::<usize>(&arg, &next_value(&arg, &mut args)).max(1),
            "--flush" => flush = true,
            "--emit" => emit = Some(next_value(&arg, &mut args)),
            "--help" => usage(),
            other => {
                eprintln!("unknown option '{other}'");
                usage()
            }
        }
    }

    let mut topology = FleetTopology::preset(preset.unwrap_or(FleetPreset::Small)).with_seed(seed);
    for (flag, n) in &overrides {
        match flag.as_str() {
            "--clusters" => topology.clusters = *n,
            "--hosts" => topology.hosts_per_cluster = *n,
            "--namespaces" => topology.namespaces_per_cluster = *n,
            "--pods" => topology.pods_per_cluster = *n,
            "--routes" => topology.routes_per_service = *n,
            _ => unreachable!(),
        }
    }
    if !overrides.is_empty() {
        preset = None;
    }
    if let Err(e) = topology.validate() {
        eprintln!("invalid topology: {e}");
        usage()
    }
    if interval <= 0 || duration < interval {
        eprintln!("--duration must be at least one --interval");
        usage()
    }

    // Align the window to the scrape interval, as a Prometheus server's would be, and end it at
    // the last whole interval so `now`-relative queries find fresh data.
    let end = end.unwrap_or_else(now_ms) / interval * interval;
    let samples = (duration / interval) as usize;
    let scrape = ScrapeConfig {
        start: end - samples as i64 * interval,
        interval_ms: interval,
        samples,
        jitter_ms: jitter,
    };
    Config {
        host,
        port,
        user,
        password,
        topology,
        preset,
        scrape,
        prefix,
        create_options,
        extra_labels,
        batch,
        pipeline,
        threads,
        flush,
        emit,
    }
}

// ---------------------------------------------------------------------------------------------
// Connections
// ---------------------------------------------------------------------------------------------

/// The server as redis-rs reaches it: a single node, or a cluster it routes by key slot.
enum Server {
    Standalone(redis::Client),
    Cluster(ClusterClient),
}

enum Conn {
    Standalone(Box<redis::Connection>),
    Cluster(Box<ClusterConnection>),
}

impl Server {
    /// Connect to `--host`/`--port` and find out whether it is part of a cluster. Returns the
    /// server and a description of it for the report.
    fn connect(cfg: &Config) -> RedisResult<(Self, String)> {
        let mut settings = RedisConnectionInfo::default();
        if let Some(user) = &cfg.user {
            settings = settings.set_username(user);
        }
        if let Some(password) = &cfg.password {
            settings = settings.set_password(password);
        }
        let info = ConnectionAddr::Tcp(cfg.host.clone(), cfg.port)
            .into_connection_info()?
            .set_redis_settings(settings);
        let client = redis::Client::open(info.clone())?;
        let info_cluster: String = redis::cmd("INFO")
            .arg("cluster")
            .query(&mut client.get_connection()?)?;
        let addr = format!("{}:{}", cfg.host, cfg.port);
        Ok(if info_cluster.contains("cluster_enabled:1") {
            (
                Server::Cluster(ClusterClient::new(vec![info])?),
                format!("the cluster at {addr}"),
            )
        } else {
            (Server::Standalone(client), format!("{addr} (standalone)"))
        })
    }

    fn conn(&self) -> RedisResult<Conn> {
        Ok(match self {
            Server::Standalone(client) => Conn::Standalone(Box::new(client.get_connection()?)),
            Server::Cluster(client) => Conn::Cluster(Box::new(client.get_connection()?)),
        })
    }
}

impl Conn {
    fn query(&mut self, cmd: &Cmd) -> RedisResult<Value> {
        match self {
            Conn::Standalone(c) => cmd.query(c.as_mut()),
            Conn::Cluster(c) => cmd.query(c.as_mut()),
        }
    }

    /// Run `cmds` as one pipeline, counting error replies -- including the per-sample ones
    /// nested in a `TS.MADD` reply -- rather than stopping at the first. Only a failure to talk
    /// to the server is an `Err`.
    fn run(&mut self, cmds: Vec<Cmd>, tally: &mut Tally) -> Result<(), String> {
        match self {
            Conn::Standalone(c) => {
                let mut pipe = redis::pipe();
                for cmd in cmds {
                    pipe.add_command(cmd);
                }
                let replies: Value = pipe
                    .ignore_errors()
                    .query(c.as_mut())
                    .map_err(|e| e.to_string())?;
                tally.observe(&replies);
            }
            Conn::Cluster(c) => {
                let mut pipe = cluster_pipe();
                for cmd in cmds {
                    pipe.add_command(cmd);
                }
                // A cluster pipeline can't ignore errors: one that has any fails with just the
                // top-level ones, so per-sample errors in that pipeline's other replies go
                // uncounted.
                match pipe.query::<Value>(c) {
                    Ok(replies) => tally.observe(&replies),
                    Err(e) => {
                        let text = e.to_string();
                        let errors = e.into_server_errors().ok_or(text)?;
                        errors.iter().for_each(|(_, e)| tally.error(e));
                    }
                }
            }
        }
        Ok(())
    }
}

/// Errors seen in replies.
#[derive(Default)]
struct Tally {
    errors: u64,
    examples: Vec<String>,
}

impl Tally {
    fn observe(&mut self, value: &Value) {
        match value {
            Value::ServerError(e) => self.error(e),
            Value::Array(items) => items.iter().for_each(|v| self.observe(v)),
            _ => {}
        }
    }

    fn error(&mut self, e: &ServerError) {
        self.errors += 1;
        let text = match e.details() {
            Some(details) => format!("{} {details}", e.code()),
            None => e.code().to_string(),
        };
        if self.examples.len() < 5 && !self.examples.contains(&text) {
            self.examples.push(text);
        }
    }

    fn merge(&mut self, other: Tally) {
        self.errors += other.errors;
        for e in other.examples {
            if self.examples.len() < 5 && !self.examples.contains(&e) {
                self.examples.push(e);
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Load
// ---------------------------------------------------------------------------------------------

/// The key a series is loaded under: `--prefix` followed by the generated `<metric>:ts:<n>`.
fn series_key(cfg: &Config, spec: &SeriesSpec) -> String {
    format!("{}{}", cfg.prefix, spec.key)
}

/// One `TS.CREATE` and the `TS.MADD` batches that fill it.
fn series_commands(cfg: &Config, spec: &SeriesSpec, samples: &[Sample]) -> Vec<Cmd> {
    let key = series_key(cfg, spec);
    let mut create = redis::cmd("TS.CREATE");
    create.arg(&key);
    for (option, value) in &cfg.create_options {
        create.arg(*option).arg(value);
    }
    create.arg("LABELS");
    for label in &spec.labels {
        create.arg(&label.name).arg(&label.value);
    }
    for (name, value) in &cfg.extra_labels {
        create.arg(name).arg(value);
    }
    let mut cmds = vec![create];
    for chunk in samples.chunks(cfg.batch) {
        let mut madd = redis::cmd("TS.MADD");
        for sample in chunk {
            madd.arg(&key).arg(sample.timestamp).arg(sample.value);
        }
        cmds.push(madd);
    }
    cmds
}

/// Where the commands go.
enum Sink<'a> {
    Server(&'a Server),
    /// RESP for `valkey-cli --pipe`, shared by the workers.
    Emit(Mutex<BufWriter<Box<dyn Write + Send>>>),
}

#[derive(Default)]
struct Progress {
    series: AtomicUsize,
    samples: AtomicUsize,
}

/// One worker: claim series, generate them, and send them a pipeline of about `cfg.pipeline`
/// commands at a time over its own connection.
fn worker(
    cfg: &Config,
    fleet: &FleetMetrics,
    sink: &Sink,
    next: &AtomicUsize,
    progress: &Progress,
    stop: &AtomicBool,
) -> Result<Tally, String> {
    let mut conn = match sink {
        Sink::Server(server) => Some(server.conn().map_err(|e| e.to_string())?),
        Sink::Emit(_) => None,
    };
    let mut tally = Tally::default();
    let mut cmds: Vec<Cmd> = Vec::with_capacity(cfg.pipeline + 8);
    let (mut series, mut samples) = (0, 0);
    let mut flush =
        |cmds: &mut Vec<Cmd>, series: &mut usize, samples: &mut usize| -> Result<(), String> {
            if cmds.is_empty() {
                return Ok(());
            }
            let batch = std::mem::take(cmds);
            match (&mut conn, sink) {
                (Some(conn), _) => conn.run(batch, &mut tally)?,
                (None, Sink::Emit(out)) => {
                    let mut pipe = redis::pipe();
                    for cmd in batch {
                        pipe.add_command(cmd);
                    }
                    let bytes = pipe.get_packed_pipeline();
                    out.lock()
                        .expect("emit writer poisoned")
                        .write_all(&bytes)
                        .map_err(|e| e.to_string())?;
                }
                (None, Sink::Server(_)) => unreachable!("server sinks connect first"),
            }
            progress
                .series
                .fetch_add(std::mem::take(series), Ordering::Relaxed);
            progress
                .samples
                .fetch_add(std::mem::take(samples), Ordering::Relaxed);
            Ok(())
        };
    while !stop.load(Ordering::Relaxed) {
        let i = next.fetch_add(1, Ordering::Relaxed);
        let Some(spec) = fleet.series().get(i) else {
            break;
        };
        let values = fleet.samples(spec);
        series += 1;
        samples += values.len();
        cmds.extend(series_commands(cfg, spec, &values));
        if cmds.len() >= cfg.pipeline {
            flush(&mut cmds, &mut series, &mut samples)?;
        }
    }
    flush(&mut cmds, &mut series, &mut samples)?;
    Ok(tally)
}

/// Generate and send on `cfg.threads` workers while this thread reports progress.
/// Returns the replies' errors and the number of samples sent.
fn load(cfg: &Config, fleet: &FleetMetrics, sink: &Sink) -> Result<(Tally, usize), String> {
    let next = AtomicUsize::new(0);
    let progress = Progress::default();
    let stop = AtomicBool::new(false);
    let started = Instant::now();
    let total = fleet.series().len();
    thread::scope(|scope| {
        let workers: Vec<_> = (0..cfg.threads)
            .map(|_| {
                scope.spawn(|| {
                    let result = worker(cfg, fleet, sink, &next, &progress, &stop);
                    if result.is_err() {
                        stop.store(true, Ordering::Relaxed);
                    }
                    result
                })
            })
            .collect();
        while !workers.iter().all(|w| w.is_finished()) {
            thread::sleep(Duration::from_millis(250));
            let (series, samples) = (
                progress.series.load(Ordering::Relaxed),
                progress.samples.load(Ordering::Relaxed),
            );
            let rate = samples as f64 / started.elapsed().as_secs_f64();
            eprint!(
                "\r  {} / {} series, {} samples ({}/s)   ",
                commas(series),
                commas(total),
                commas(samples),
                commas(rate as usize)
            );
        }
        eprint!("\r{:72}\r", "");
        let mut tally = Tally::default();
        for worker in workers {
            tally.merge(worker.join().expect("worker panicked")?);
        }
        if let Sink::Emit(out) = sink {
            out.lock()
                .expect("emit writer poisoned")
                .flush()
                .map_err(|e| e.to_string())?;
        }
        Ok((tally, progress.samples.load(Ordering::Relaxed)))
    })
}

fn commas(n: usize) -> String {
    let digits = n.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

/// `TS.CARD` over every series this tool writes; a cluster's TS.CARD needs a matcher.
fn series_count(conn: &mut Conn) -> RedisResult<i64> {
    let reply = conn.query(redis::cmd("TS.CARD").arg("FILTER").arg("__name__=~\".+\""))?;
    Ok(redis::from_redis_value(reply)?)
}

fn main() {
    let cfg = parse_args();
    let started = Instant::now();
    let fleet = FleetMetrics::new(&cfg.topology, cfg.scrape).unwrap_or_else(|e| fail(e));
    eprintln!(
        "Fleet: {} topology, {} series × up to {} scrapes every {}s (≤ {} samples), seed {:#x}",
        cfg.preset.map_or("custom", FleetPreset::id),
        commas(fleet.series().len()),
        cfg.scrape.samples,
        cfg.scrape.interval_ms as f64 / 1000.0,
        commas(fleet.series().len() * cfg.scrape.samples),
        cfg.topology.seed,
    );
    eprintln!(
        "Window: {} .. {} (ms since the epoch)",
        cfg.scrape.start,
        cfg.scrape.start + cfg.scrape.samples as i64 * cfg.scrape.interval_ms
    );

    let server;
    let mut admin = None;
    let (sink, target) = match &cfg.emit {
        Some(path) => {
            let out: Box<dyn Write + Send> = if path == "-" {
                Box::new(io::stdout())
            } else {
                Box::new(File::create(path).unwrap_or_else(|e| fail(format!("{path}: {e}"))))
            };
            (
                Sink::Emit(Mutex::new(BufWriter::with_capacity(1 << 20, out))),
                path.clone(),
            )
        }
        None => {
            let (connected, target) = Server::connect(&cfg).unwrap_or_else(|e| fail(e));
            server = connected;
            let mut conn = server.conn().unwrap_or_else(|e| fail(e));
            if let Err(e) = series_count(&mut conn) {
                if e.to_string()
                    .to_ascii_lowercase()
                    .contains("unknown command")
                {
                    fail(format!("valkey-timeseries is not loaded on {target}: {e}"));
                }
                eprintln!("Note: TS.CARD failed ({e}); continuing");
            }
            if cfg.flush {
                // Routed to every primary on a cluster.
                conn.query(&redis::cmd("FLUSHALL"))
                    .unwrap_or_else(|e| fail(format!("FLUSHALL on {target}: {e}")));
                eprintln!("Flushed {target}");
            } else if let Some(spec) = fleet.series().first() {
                // Loading over an earlier fleet would append its samples to whichever series
                // now has the same key, so refuse rather than mix two fleets.
                let first = series_key(&cfg, spec);
                let exists: bool = conn
                    .query(redis::cmd("EXISTS").arg(&first))
                    .and_then(|v| Ok(redis::from_redis_value(v)?))
                    .unwrap_or_else(|e| fail(e));
                if exists {
                    fail(format!(
                        "{first} already exists on {target}: pass --flush to replace it, \
                         or --prefix and --label to load a copy alongside it"
                    ));
                }
            }
            admin = Some(conn);
            (Sink::Server(&server), target)
        }
    };

    let loading = Instant::now();
    let (tally, samples) = load(&cfg, &fleet, &sink).unwrap_or_else(|e| fail(e));
    let secs = loading.elapsed().as_secs_f64();
    eprintln!(
        "Wrote {} series / {} samples to {target} in {secs:.2}s: {}/s",
        commas(fleet.series().len()),
        commas(samples),
        commas((samples as f64 / secs) as usize),
    );

    let Some(mut admin) = admin else {
        eprintln!("  load:    valkey-cli --pipe < {target}");
        eprintln!("  (total {:.2?})", started.elapsed());
        return;
    };
    if tally.errors > 0 {
        eprintln!("  {} error replies, e.g.:", commas(tally.errors as usize));
        for e in &tally.examples {
            eprintln!("    {e}");
        }
        if tally
            .examples
            .iter()
            .any(|e| e.contains("duplicate series"))
        {
            eprintln!(
                "  (the same label sets exist under other keys: add a --label to tell this copy apart)"
            );
        }
    } else {
        eprintln!("  no error replies");
    }
    if let Ok(n) = series_count(&mut admin) {
        eprintln!("  TS.CARD now reports {} series", commas(n as usize));
    }
    eprintln!("  try:");
    eprintln!("    TS.QUERYINDEX __name__=node_load1");
    eprintln!("    TS.MRANGE - + FILTER __name__=http_requests_total status_code=~\"5..\"");
    eprintln!(
        "    TS.MRANGE - + AGGREGATION avg 60000 FILTER __name__=container_memory_working_set_bytes \
         GROUPBY namespace REDUCE sum"
    );
    eprintln!("  (total {:.2?})", started.elapsed());
    if tally.errors > 0 {
        std::process::exit(1);
    }
}
