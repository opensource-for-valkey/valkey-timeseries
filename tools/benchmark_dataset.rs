//! Export a deterministic multi-series fixture for the server benchmark suite.
//!
//! `tools/server_bench/` (the load driver) is a separate Cargo workspace that
//! never links this crate, so the fixtures it replays against both engines are
//! produced here, by the same `DataGenerator` / `DatasetKey` / `dataset_seed`
//! machinery the unit tests and Criterion benches use. Nothing about the
//! workload shapes is re-implemented on the driver side (plan: "Do not
//! duplicate workload algorithms in another language").
//!
//! Output is a directory:
//!
//!   fixture.json   manifest: format version, generator inputs, shape, label
//!                  scheme, per-file byte counts and SHA-256, combined digest
//!   series.tsv     `index \t key \t label=value \t label=value ...`
//!   samples.tsv    `series_index \t timestamp_ms \t value`
//!
//! Every series gets its own seed derived from the dataset seed and its index,
//! so no two series are clones of each other, and the same inputs export
//! byte-identical files on any toolchain (the seed derivation is spelled out
//! rather than borrowed from `DefaultHasher`, whose output is not stable).
//! Values are written with Rust's shortest round-trip formatting and read back
//! before the file is accepted, so the text is exactly the float the generator
//! produced.
//!
//! Labels: label `l<i>` on series `s` has value `v<s % cardinality_i>` (zero
//! padded to `--label-value-len`), so an equality filter on `l<i>` matches an
//! exactly computable number of series. Cardinality 1 matches 100 % of series,
//! 10 matches 10 %, 100 matches 1 %: the selectivity axis of the plan.
//!
//! Run through `tools/server_bench.sh`, which supplies the required features.

use std::env;
use std::fmt::Write as _;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use sha2::{Digest, Sha256};
use valkey_timeseries::common::Sample;
use valkey_timeseries::tests::generators::{
    DataGenerator, DatasetKey, TimestampModel, ValueWorkload, dataset_seed,
};

/// Bumped whenever the file layout or the seed derivation changes; the driver
/// refuses fixtures it does not understand.
const FORMAT_VERSION: u32 = 1;

/// 2023-11-14T22:13:20Z, the same origin as `DataGenerator::dataset`.
const DEFAULT_START_TS: i64 = 1_700_000_000_000;

// -------- Configuration --------

struct Config {
    out: Option<PathBuf>,
    series: usize,
    samples: usize,
    workload: ValueWorkload,
    timestamp_model: TimestampModel,
    seed: Option<u64>,
    start_ts: i64,
    interval_ms: u64,
    key_prefix: String,
    label_cardinalities: Vec<usize>,
    label_value_len: usize,
    dry_run: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            out: None,
            series: 10,
            samples: 1000,
            workload: ValueWorkload::Drift,
            timestamp_model: TimestampModel::Regular,
            seed: None,
            start_ts: DEFAULT_START_TS,
            interval_ms: 1000,
            key_prefix: "bench".to_string(),
            label_cardinalities: vec![1, 10, 100],
            label_value_len: 8,
            dry_run: false,
        }
    }
}

fn usage() -> ! {
    eprintln!(
        "usage: benchmark_dataset --out DIR [options]

  --out DIR               fixture directory (created; must not already hold a fixture)
  --series N              number of series (default 10)
  --samples N             samples per series (default 1000)
  --workload ID           value shape: drift, drift_q2, noisy, noisy_q2, counter,
                          constant, constant_int, periodic, bursty, discrete, ...
                          (default drift)
  --ts-model ID           regular | jitter | irregular (default regular)
  --seed U64              override the derived dataset seed
  --start-ts MS           first timestamp, milliseconds (default {DEFAULT_START_TS})
  --interval-ms MS        sample spacing, milliseconds (default 1000)
  --key-prefix S          key prefix (default bench); keys are <prefix>:<zero-padded index>
  --label-cardinality L   comma-separated cardinality per label, one label each
                          (default 1,10,100); `none` for unlabeled series
  --label-value-len N     zero-pad label values to N bytes (default 8)
  --dry-run               print counts and estimated bytes; write nothing"
    );
    std::process::exit(2)
}

fn parse_workload(s: &str) -> ValueWorkload {
    ValueWorkload::all()
        .iter()
        .copied()
        .find(|w| w.id() == s)
        .unwrap_or_else(|| {
            eprintln!("error: unknown workload '{s}'");
            usage()
        })
}

fn parse_ts_model(s: &str) -> TimestampModel {
    TimestampModel::all()
        .iter()
        .copied()
        .find(|m| m.id() == s)
        .unwrap_or_else(|| {
            eprintln!("error: unknown timestamp model '{s}'");
            usage()
        })
}

fn parse_num<T: std::str::FromStr>(flag: &str, s: &str) -> T {
    s.parse().unwrap_or_else(|_| {
        eprintln!("error: {flag} expects a number, got '{s}'");
        usage()
    })
}

fn parse_args() -> Config {
    let mut cfg = Config::default();
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        let mut value = || {
            args.next().unwrap_or_else(|| {
                eprintln!("error: {arg} requires a value");
                usage()
            })
        };
        match arg.as_str() {
            "--out" => cfg.out = Some(PathBuf::from(value())),
            "--series" => cfg.series = parse_num(&arg, &value()),
            "--samples" => cfg.samples = parse_num(&arg, &value()),
            "--workload" => cfg.workload = parse_workload(&value()),
            "--ts-model" => cfg.timestamp_model = parse_ts_model(&value()),
            "--seed" => cfg.seed = Some(parse_num(&arg, &value())),
            "--start-ts" => cfg.start_ts = parse_num(&arg, &value()),
            "--interval-ms" => cfg.interval_ms = parse_num(&arg, &value()),
            "--key-prefix" => cfg.key_prefix = value(),
            "--label-cardinality" => {
                let v = value();
                cfg.label_cardinalities = if v == "none" {
                    Vec::new()
                } else {
                    v.split(',').map(|c| parse_num(&arg, c.trim())).collect()
                };
            }
            "--label-value-len" => cfg.label_value_len = parse_num(&arg, &value()),
            "--dry-run" => cfg.dry_run = true,
            "-h" | "--help" => usage(),
            other => {
                eprintln!("error: unknown option '{other}'");
                usage()
            }
        }
    }
    if cfg.series == 0 || cfg.samples == 0 {
        eprintln!("error: --series and --samples must be positive");
        usage()
    }
    if cfg.interval_ms == 0 {
        eprintln!("error: --interval-ms must be positive");
        usage()
    }
    if cfg.label_cardinalities.contains(&0) {
        eprintln!("error: label cardinalities must be positive");
        usage()
    }
    if cfg.key_prefix.is_empty() || cfg.key_prefix.contains([' ', '\t', '\n', ':']) {
        eprintln!("error: --key-prefix must be non-empty and free of whitespace and ':'");
        usage()
    }
    if cfg.out.is_none() && !cfg.dry_run {
        eprintln!("error: --out is required unless --dry-run");
        usage()
    }
    cfg
}

// -------- Determinism --------

/// Seed for series `index` of a fixture whose dataset seed is `dataset_seed`.
/// SplitMix64 finaliser over the two inputs: cheap, well mixed, and defined
/// here so the derivation can never drift with a library upgrade.
fn series_seed(dataset_seed: u64, index: u64) -> u64 {
    let mut z = dataset_seed ^ index.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn generate_series(cfg: &Config, seed: u64) -> Vec<Sample> {
    DataGenerator::builder()
        .start(cfg.start_ts)
        .interval(Duration::from_millis(cfg.interval_ms))
        .samples(cfg.samples)
        .seed(seed)
        .algorithm(cfg.workload)
        .timestamp_model(cfg.timestamp_model)
        .build()
        .generate()
}

/// Shortest text that parses back to exactly `value`. Rust's `Display` for
/// floats is already the shortest round-trip representation; the read-back
/// makes that a checked property of the file rather than an assumption.
fn float_text(value: f64) -> String {
    let text = format!("{value}");
    let back: f64 = text.parse().expect("float text must parse");
    assert!(
        back.to_bits() == value.to_bits(),
        "float text {text} did not round-trip {value:?}"
    );
    text
}

fn digits(n: usize) -> usize {
    n.max(1).to_string().len()
}

// -------- Output --------

struct Written {
    bytes: u64,
    sha256: String,
}

struct HashingWriter<W: Write> {
    inner: W,
    hasher: Sha256,
    bytes: u64,
}

impl<W: Write> HashingWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            hasher: Sha256::new(),
            bytes: 0,
        }
    }

    fn finish(mut self) -> std::io::Result<Written> {
        self.inner.flush()?;
        Ok(Written {
            bytes: self.bytes,
            sha256: hex(&self.hasher.finalize()),
        })
    }
}

impl<W: Write> Write for HashingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.hasher.update(&buf[..n]);
        self.bytes += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

fn series_key(cfg: &Config, index: usize, width: usize) -> String {
    format!("{}:{:0width$}", cfg.key_prefix, index)
}

fn label_value(index: usize, cardinality: usize, len: usize) -> String {
    format!(
        "v{:0width$}",
        index % cardinality,
        width = len.saturating_sub(1)
    )
}

fn write_series_file(cfg: &Config, path: &Path, key_width: usize) -> std::io::Result<Written> {
    let mut w = HashingWriter::new(BufWriter::new(File::create(path)?));
    for index in 0..cfg.series {
        write!(w, "{index}\t{}", series_key(cfg, index, key_width))?;
        for (li, &card) in cfg.label_cardinalities.iter().enumerate() {
            write!(
                w,
                "\tl{li}={}",
                label_value(index, card, cfg.label_value_len)
            )?;
        }
        writeln!(w)?;
    }
    w.finish()
}

fn write_samples_file(cfg: &Config, path: &Path, seed: u64) -> std::io::Result<Written> {
    let mut w = HashingWriter::new(BufWriter::new(File::create(path)?));
    for index in 0..cfg.series {
        let samples = generate_series(cfg, series_seed(seed, index as u64));
        assert_eq!(
            samples.len(),
            cfg.samples,
            "generator returned a short series"
        );
        let mut previous = i64::MIN;
        for s in &samples {
            // The driver replays these in order as monotonic per-series streams;
            // the jitter/irregular models must not have produced a step backwards.
            assert!(
                s.timestamp > previous,
                "series {index}: non-increasing timestamp {} after {previous}",
                s.timestamp
            );
            previous = s.timestamp;
            writeln!(w, "{index}\t{}\t{}", s.timestamp, float_text(s.value))?;
        }
    }
    w.finish()
}

fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// The manifest is small and its shape is fixed, so it is assembled by hand
/// rather than adding a JSON dependency to the module crate.
fn write_manifest(
    cfg: &Config,
    path: &Path,
    dataset_key: DatasetKey,
    seed: u64,
    key_width: usize,
    series: &Written,
    samples: &Written,
) -> std::io::Result<String> {
    let combined = {
        let mut h = Sha256::new();
        h.update(b"series.tsv\n");
        h.update(series.sha256.as_bytes());
        h.update(b"\nsamples.tsv\n");
        h.update(samples.sha256.as_bytes());
        h.update(b"\n");
        hex(&h.finalize())
    };

    let labels: Vec<String> = cfg
        .label_cardinalities
        .iter()
        .enumerate()
        .map(|(i, c)| {
            format!(
                "{{\"name\": \"l{i}\", \"cardinality\": {c}, \"value_len\": {}, \"expected_matches\": {}}}",
                cfg.label_value_len,
                expected_matches_json(cfg.series, *c)
            )
        })
        .collect();

    let mut m = String::new();
    let _ = writeln!(m, "{{");
    let _ = writeln!(m, "  \"format_version\": {FORMAT_VERSION},");
    let _ = writeln!(m, "  \"tool\": \"benchmark_dataset\",");
    let _ = writeln!(m, "  \"generator\": {{");
    let _ = writeln!(
        m,
        "    \"dataset_key\": {},",
        json_escape(&dataset_key.id())
    );
    let _ = writeln!(m, "    \"workload\": {},", json_escape(cfg.workload.id()));
    let _ = writeln!(
        m,
        "    \"timestamp_model\": {},",
        json_escape(cfg.timestamp_model.id())
    );
    let _ = writeln!(m, "    \"dataset_seed\": {seed},");
    let _ = writeln!(m, "    \"seed_overridden\": {},", cfg.seed.is_some());
    let _ = writeln!(
        m,
        "    \"series_seed_rule\": \"splitmix64(dataset_seed ^ index * 0x9E3779B97F4A7C15)\","
    );
    let _ = writeln!(m, "    \"start_ts\": {},", cfg.start_ts);
    let _ = writeln!(m, "    \"interval_ms\": {}", cfg.interval_ms);
    let _ = writeln!(m, "  }},");
    let _ = writeln!(m, "  \"shape\": {{");
    let _ = writeln!(m, "    \"series\": {},", cfg.series);
    let _ = writeln!(m, "    \"samples_per_series\": {},", cfg.samples);
    let _ = writeln!(m, "    \"total_samples\": {}", cfg.series * cfg.samples);
    let _ = writeln!(m, "  }},");
    let _ = writeln!(m, "  \"keys\": {{");
    let _ = writeln!(m, "    \"prefix\": {},", json_escape(&cfg.key_prefix));
    let _ = writeln!(m, "    \"index_width\": {key_width},");
    let _ = writeln!(m, "    \"rule\": \"<prefix>:<zero-padded index>\"");
    let _ = writeln!(m, "  }},");
    let _ = writeln!(m, "  \"labels\": {{");
    let _ = writeln!(
        m,
        "    \"value_rule\": \"l<i> = v<zero-padded (series_index % cardinality_i)>\","
    );
    let _ = writeln!(m, "    \"definitions\": [");
    for (i, l) in labels.iter().enumerate() {
        let sep = if i + 1 == labels.len() { "" } else { "," };
        let _ = writeln!(m, "      {l}{sep}");
    }
    let _ = writeln!(m, "    ]");
    let _ = writeln!(m, "  }},");
    let _ = writeln!(m, "  \"files\": {{");
    let _ = writeln!(
        m,
        "    \"series.tsv\": {{\"bytes\": {}, \"sha256\": \"{}\"}},",
        series.bytes, series.sha256
    );
    let _ = writeln!(
        m,
        "    \"samples.tsv\": {{\"bytes\": {}, \"sha256\": \"{}\"}}",
        samples.bytes, samples.sha256
    );
    let _ = writeln!(m, "  }},");
    let _ = writeln!(m, "  \"sha256\": \"{combined}\"");
    let _ = writeln!(m, "}}");
    fs::write(path, &m)?;
    Ok(combined)
}

/// `[count for value 0, count for value 1, ...]`: how many series carry each
/// label value, so a `TS.QUERYINDEX l<i>=v<k>` has an exact expected size.
fn expected_matches_json(series: usize, cardinality: usize) -> String {
    let counts: Vec<String> = (0..cardinality)
        .map(|k| {
            // Series s carries value k iff s % cardinality == k, i.e. s ∈ {k, k+c, ...}:
            // ceil((series - k) / cardinality) of them when k < series, else none.
            let n = if k < series {
                (series - k).div_ceil(cardinality)
            } else {
                0
            };
            n.to_string()
        })
        .collect();
    format!("[{}]", counts.join(", "))
}

// -------- Main --------

fn main() {
    let cfg = parse_args();
    let dataset_key = DatasetKey::new(cfg.workload, cfg.timestamp_model);
    let seed = cfg.seed.unwrap_or_else(|| dataset_seed(dataset_key));
    let key_width = digits(cfg.series - 1);
    let total = cfg.series * cfg.samples;

    // Rough line-size model: index + tab + 13-digit timestamp + tab + ~18-char value.
    let est_samples_bytes = total as u64 * (key_width as u64 + 2 + 13 + 18 + 1);
    let est_series_bytes = cfg.series as u64
        * (key_width as u64 * 2
            + cfg.key_prefix.len() as u64
            + 3
            + cfg.label_cardinalities.len() as u64 * (cfg.label_value_len as u64 + 5)
            + 1);

    eprintln!("fixture: {} ({})", dataset_key.id(), cfg.workload.id());
    eprintln!(
        "  seed            {seed}{}",
        if cfg.seed.is_some() {
            " (overridden)"
        } else {
            ""
        }
    );
    eprintln!("  series          {}", cfg.series);
    eprintln!("  samples/series  {}", cfg.samples);
    eprintln!("  total samples   {total}");
    eprintln!(
        "  timestamps      start {} step {} ms ({})",
        cfg.start_ts,
        cfg.interval_ms,
        cfg.timestamp_model.id()
    );
    eprintln!(
        "  labels          {}",
        if cfg.label_cardinalities.is_empty() {
            "none".to_string()
        } else {
            cfg.label_cardinalities
                .iter()
                .enumerate()
                .map(|(i, c)| format!("l{i}(card {c})"))
                .collect::<Vec<_>>()
                .join(" ")
        }
    );
    let est = est_samples_bytes + est_series_bytes;
    if est >= 1024 * 1024 {
        eprintln!("  estimated size  ~{} MiB", est / (1024 * 1024));
    } else {
        eprintln!("  estimated size  ~{} KiB", est / 1024);
    }

    if cfg.dry_run {
        println!("dry-run: nothing written");
        return;
    }

    let out = cfg.out.clone().expect("checked in parse_args");
    if out.join("fixture.json").exists() {
        eprintln!(
            "error: {} already holds a fixture; refusing to overwrite",
            out.display()
        );
        std::process::exit(1);
    }
    fs::create_dir_all(&out).unwrap_or_else(|e| {
        eprintln!("error: cannot create {}: {e}", out.display());
        std::process::exit(1);
    });

    let run = || -> std::io::Result<String> {
        let series = write_series_file(&cfg, &out.join("series.tsv"), key_width)?;
        let samples = write_samples_file(&cfg, &out.join("samples.tsv"), seed)?;
        write_manifest(
            &cfg,
            &out.join("fixture.json"),
            dataset_key,
            seed,
            key_width,
            &series,
            &samples,
        )
    };
    match run() {
        Ok(digest) => {
            eprintln!("  written to      {}", out.display());
            println!("{digest}");
        }
        Err(e) => {
            eprintln!("error: writing fixture: {e}");
            std::process::exit(1);
        }
    }
}
