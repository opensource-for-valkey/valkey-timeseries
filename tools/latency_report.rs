//! Encode/decode latency matrix across chunk encodings.
//!
//! Companion to `compression_report`: that one answers "how small", this one
//! answers "how fast". Every measurement is the median wall time of the whole
//! operation over a chunk of `--samples` samples, so the columns are directly
//! comparable across encodings.
//!
//! Run through `tools/latency_report.sh`, which supplies the required features
//! and prints where the report landed.

use std::collections::BTreeSet;
use std::env;
use std::fs::{self, File};
use std::hint::black_box;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use valkey_timeseries::common::Sample;
use valkey_timeseries::series::chunks::{ChunkEncoding, ChunkOps, TimeSeriesChunk};
use valkey_timeseries::tests::chunk_utils::encoded_size;
use valkey_timeseries::tests::generators::{
    DatasetKey, TimestampModel, ValueWorkload, dataset_seed, generate_dataset,
};

// -------- Defaults --------

const DEFAULT_SAMPLES: usize = 1000;
/// Large enough that no encoding fills up at the default sample count
/// (uncompressed needs 16 bytes/sample), so every encoding stores the same data.
const DEFAULT_CHUNK_BUDGET: usize = 1024 * 1024;
const DEFAULT_WARMUP: usize = 20;
const DEFAULT_ITERATIONS: usize = 200;
const DEFAULT_WORKLOADS: [ValueWorkload; 2] = [ValueWorkload::Drift, ValueWorkload::Noisy];

const ALL_ENCODINGS: [ChunkEncoding; 5] = [
    ChunkEncoding::Uncompressed,
    ChunkEncoding::Gorilla,
    ChunkEncoding::Xor2,
    ChunkEncoding::DeXor,
    ChunkEncoding::Chimp,
];

// -------- Configuration --------

struct Config {
    samples: usize,
    chunk_budget: usize,
    warmup: usize,
    iterations: usize,
    encodings: Vec<ChunkEncoding>,
    workloads: Vec<ValueWorkload>,
    timestamp_models: Vec<TimestampModel>,
    seed: Option<u64>,
    out_csv: PathBuf,
    out_md: PathBuf,
    quiet: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            samples: DEFAULT_SAMPLES,
            chunk_budget: DEFAULT_CHUNK_BUDGET,
            warmup: DEFAULT_WARMUP,
            iterations: DEFAULT_ITERATIONS,
            encodings: ALL_ENCODINGS.to_vec(),
            workloads: DEFAULT_WORKLOADS.to_vec(),
            timestamp_models: vec![TimestampModel::Regular],
            seed: None,
            out_csv: PathBuf::from("target/bench-reports/latency.csv"),
            out_md: PathBuf::from("target/bench-reports/latency.md"),
            quiet: false,
        }
    }
}

fn parse_encoding(name: &str) -> Option<ChunkEncoding> {
    ALL_ENCODINGS
        .iter()
        .copied()
        .find(|e| e.name().eq_ignore_ascii_case(name))
}

fn parse_workload(name: &str) -> Option<ValueWorkload> {
    ValueWorkload::all()
        .iter()
        .copied()
        .find(|w| w.id().eq_ignore_ascii_case(name))
}

fn parse_timestamp_model(name: &str) -> Option<TimestampModel> {
    [
        TimestampModel::Regular,
        TimestampModel::Jitter,
        TimestampModel::Irregular,
    ]
    .into_iter()
    .find(|m| m.id().eq_ignore_ascii_case(name))
}

/// Parses a comma-separated list, mapping each item through `parse`.
fn parse_list<T>(raw: &str, parse: impl Fn(&str) -> Option<T>, what: &str, valid: &str) -> Vec<T> {
    let mut out = Vec::new();
    for item in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        match parse(item) {
            Some(v) => out.push(v),
            None => {
                eprintln!("Unknown {what} '{item}' (expected one of: {valid})");
                std::process::exit(1);
            }
        }
    }
    if out.is_empty() {
        eprintln!("Empty {what} list");
        std::process::exit(1);
    }
    out
}

fn next_value(args: &mut impl Iterator<Item = String>, flag: &str) -> String {
    match args.next() {
        Some(v) => v,
        None => {
            eprintln!("{flag} requires a value");
            std::process::exit(1);
        }
    }
}

fn parse_usize(raw: &str, flag: &str) -> usize {
    match raw.parse::<usize>() {
        Ok(v) if v > 0 => v,
        _ => {
            eprintln!("{flag} expects a positive integer, got '{raw}'");
            std::process::exit(1);
        }
    }
}

fn parse_args() -> Config {
    let mut cfg = Config::default();
    let mut args = env::args().skip(1);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--samples" => {
                cfg.samples = parse_usize(&next_value(&mut args, "--samples"), "--samples")
            }
            "--chunk-size" => {
                cfg.chunk_budget =
                    parse_usize(&next_value(&mut args, "--chunk-size"), "--chunk-size")
            }
            "--iterations" => {
                cfg.iterations = parse_usize(&next_value(&mut args, "--iterations"), "--iterations")
            }
            "--warmup" => {
                // Zero warmup is legal, so this one is parsed directly.
                let raw = next_value(&mut args, "--warmup");
                cfg.warmup = match raw.parse::<usize>() {
                    Ok(v) => v,
                    Err(_) => {
                        eprintln!("--warmup expects an integer, got '{raw}'");
                        std::process::exit(1);
                    }
                };
            }
            "--encodings" => {
                let raw = next_value(&mut args, "--encodings");
                cfg.encodings = if raw.eq_ignore_ascii_case("all") {
                    ALL_ENCODINGS.to_vec()
                } else {
                    let valid = ALL_ENCODINGS
                        .iter()
                        .map(|e| e.name())
                        .collect::<Vec<_>>()
                        .join(", ");
                    parse_list(&raw, parse_encoding, "encoding", &valid)
                };
            }
            "--workloads" => {
                let raw = next_value(&mut args, "--workloads");
                cfg.workloads = if raw.eq_ignore_ascii_case("all") {
                    ValueWorkload::workloads().to_vec()
                } else {
                    let valid = ValueWorkload::all()
                        .iter()
                        .map(|w| w.id())
                        .collect::<Vec<_>>()
                        .join(", ");
                    parse_list(&raw, parse_workload, "workload", &valid)
                };
            }
            "--ts-models" => {
                let raw = next_value(&mut args, "--ts-models");
                cfg.timestamp_models = if raw.eq_ignore_ascii_case("all") {
                    vec![
                        TimestampModel::Regular,
                        TimestampModel::Jitter,
                        TimestampModel::Irregular,
                    ]
                } else {
                    parse_list(
                        &raw,
                        parse_timestamp_model,
                        "timestamp model",
                        "regular, jitter, irregular",
                    )
                };
            }
            "--seed" => {
                let raw = next_value(&mut args, "--seed");
                cfg.seed = match raw.parse::<u64>() {
                    Ok(v) => Some(v),
                    Err(_) => {
                        eprintln!("--seed expects a u64, got '{raw}'");
                        std::process::exit(1);
                    }
                };
            }
            "--out-csv" => cfg.out_csv = PathBuf::from(next_value(&mut args, "--out-csv")),
            "--out-md" => cfg.out_md = PathBuf::from(next_value(&mut args, "--out-md")),
            "--quiet" => cfg.quiet = true,
            other => {
                eprintln!("Unknown option '{other}'");
                std::process::exit(1);
            }
        }
    }

    cfg
}

// -------- Measurement --------

/// Median wall time of `op`, after a warmup. The median (rather than the mean)
/// keeps a stray scheduler preemption from dominating a row.
fn median_time<T>(cfg: &Config, mut op: impl FnMut() -> T) -> Duration {
    for _ in 0..cfg.warmup {
        black_box(op());
    }

    let mut times: Vec<Duration> = Vec::with_capacity(cfg.iterations);
    for _ in 0..cfg.iterations {
        let start = Instant::now();
        let out = op();
        let elapsed = start.elapsed();
        black_box(out);
        times.push(elapsed);
    }

    times.sort_unstable();
    times[times.len() / 2]
}

struct Row {
    encoding: ChunkEncoding,
    workload: ValueWorkload,
    timestamp_model: TimestampModel,
    len: usize,
    /// Samples the append path actually stored before `is_full()`. Below `len`
    /// only when `--chunk-size` is too small for the dataset.
    append_len: usize,
    bytes: usize,
    encode_bulk: Duration,
    encode_append: Duration,
    decode_iter: Duration,
    get_range: Duration,
    scan_mid_10pct: Duration,
}

fn measure(cfg: &Config, key: DatasetKey, encoding: ChunkEncoding, samples: &[Sample]) -> Row {
    let first = samples[0].timestamp;
    let last = samples[samples.len() - 1].timestamp;
    let span = last - first;
    let mid_start = first + span * 45 / 100;
    let mid_end = first + span * 55 / 100;

    let encode_bulk = median_time(cfg, || {
        let mut chunk = TimeSeriesChunk::new(encoding, cfg.chunk_budget);
        chunk.set_data(black_box(samples)).expect("set_data failed");
        chunk
    });

    let encode_append = median_time(cfg, || {
        let mut chunk = TimeSeriesChunk::new(encoding, cfg.chunk_budget);
        for sample in samples {
            if chunk.is_full() {
                break;
            }
            chunk
                .add_sample(black_box(sample))
                .expect("add_sample failed");
        }
        chunk
    });

    // `set_data` bulk-loads regardless of the budget, while `add_sample` stops
    // at `is_full()` — so with a small --chunk-size the two encode columns can
    // cover different sample counts. Measure what append actually stored.
    let mut append_chunk = TimeSeriesChunk::new(encoding, cfg.chunk_budget);
    let mut append_len = 0usize;
    for sample in samples {
        if append_chunk.is_full() {
            break;
        }
        append_chunk.add_sample(sample).expect("add_sample failed");
        append_len += 1;
    }

    let mut chunk = TimeSeriesChunk::new(encoding, cfg.chunk_budget);
    chunk.set_data(samples).expect("set_data failed");

    let decode_iter = median_time(cfg, || chunk.iter().map(|s| s.value).sum::<f64>());
    let get_range = median_time(cfg, || {
        chunk.get_range(first, last).expect("get_range failed")
    });
    let scan_mid_10pct = median_time(cfg, || chunk.range_iter(mid_start, mid_end).count());

    Row {
        encoding,
        workload: key.workload,
        timestamp_model: key.timestamp_model,
        len: chunk.len(),
        append_len,
        bytes: encoded_size(&chunk),
        encode_bulk,
        encode_append,
        decode_iter,
        get_range,
        scan_mid_10pct,
    }
}

fn micros(d: Duration) -> f64 {
    d.as_secs_f64() * 1e6
}

/// Nanoseconds per sample, the shape-independent view of a column.
fn ns_per_sample(d: Duration, len: usize) -> f64 {
    if len == 0 {
        return 0.0;
    }
    d.as_secs_f64() * 1e9 / len as f64
}

// -------- Output --------

fn ensure_dir(p: &Path) -> std::io::Result<()> {
    if let Some(parent) = p.parent() {
        fs::create_dir_all(parent)?;
    }
    Ok(())
}

fn write_csv(path: &Path, rows: &[Row]) -> std::io::Result<()> {
    ensure_dir(path)?;
    let mut w = BufWriter::new(File::create(path)?);
    writeln!(
        w,
        "encoding,workload,ts_model,len,append_len,bytes,encode_bulk_us,encode_append_us,\
         decode_iter_us,get_range_us,scan_mid_10pct_us,decode_ns_per_sample"
    )?;
    for r in rows {
        writeln!(
            w,
            "{},{},{},{},{},{},{:.3},{:.3},{:.3},{:.3},{:.3},{:.2}",
            r.encoding.name(),
            r.workload.id(),
            r.timestamp_model.id(),
            r.len,
            r.append_len,
            r.bytes,
            micros(r.encode_bulk),
            micros(r.encode_append),
            micros(r.decode_iter),
            micros(r.get_range),
            micros(r.scan_mid_10pct),
            ns_per_sample(r.decode_iter, r.len),
        )?;
    }
    Ok(())
}

fn table_header(cfg: &Config) -> String {
    format!(
        "Median of {} iterations after {} warmup runs. Times are µs for the whole \
         {}-sample operation.",
        cfg.iterations, cfg.warmup, cfg.samples
    )
}

fn write_markdown(path: &Path, cfg: &Config, rows: &[Row]) -> std::io::Result<()> {
    ensure_dir(path)?;
    let mut w = BufWriter::new(File::create(path)?);

    writeln!(w, "# Chunk encoding latency")?;
    writeln!(w)?;
    writeln!(w, "{}", table_header(cfg))?;
    writeln!(w)?;
    writeln!(
        w,
        "`encode_bulk` = `set_data`; `encode_append` = per-sample `add_sample`; \
         `decode_iter` = full scan; `get_range` = full-range materialization; \
         `scan_10%` = a 10% window centered mid-chunk."
    )?;
    writeln!(w)?;

    // One table per (workload, ts_model), rows = encodings.
    let keys: BTreeSet<(String, String)> = rows
        .iter()
        .map(|r| {
            (
                r.workload.id().to_string(),
                r.timestamp_model.id().to_string(),
            )
        })
        .collect();

    for (workload, ts_model) in keys {
        let group: Vec<&Row> = rows
            .iter()
            .filter(|r| r.workload.id() == workload && r.timestamp_model.id() == ts_model)
            .collect();

        writeln!(w, "## {workload} / {ts_model}")?;
        writeln!(w)?;
        writeln!(
            w,
            "| encoding | encode_bulk | encode_append | decode_iter | get_range | scan_10% | bytes |"
        )?;
        writeln!(w, "|---|---:|---:|---:|---:|---:|---:|")?;
        for r in group {
            writeln!(
                w,
                "| {} | {:.1} | {:.1} | {:.1} | {:.1} | {:.2} | {} |",
                r.encoding.name(),
                micros(r.encode_bulk),
                micros(r.encode_append),
                micros(r.decode_iter),
                micros(r.get_range),
                micros(r.scan_mid_10pct),
                r.bytes,
            )?;
        }
        writeln!(w)?;
    }

    Ok(())
}

fn print_table(cfg: &Config, rows: &[Row]) {
    println!("\nChunk encoding latency — {}", table_header(cfg));

    let keys: BTreeSet<(String, String)> = rows
        .iter()
        .map(|r| {
            (
                r.workload.id().to_string(),
                r.timestamp_model.id().to_string(),
            )
        })
        .collect();

    for (workload, ts_model) in keys {
        let group: Vec<&Row> = rows
            .iter()
            .filter(|r| r.workload.id() == workload && r.timestamp_model.id() == ts_model)
            .collect();

        println!("\n## {workload} / {ts_model}\n");
        println!(
            "| {:<13} | {:>11} | {:>13} | {:>11} | {:>10} | {:>9} | {:>8} |",
            "encoding",
            "encode_bulk",
            "encode_append",
            "decode_iter",
            "get_range",
            "scan_10%",
            "bytes"
        );
        println!(
            "|{:-<15}|{:->13}|{:->15}|{:->13}|{:->12}|{:->11}|{:->10}|",
            "", "", "", "", "", "", ""
        );
        for r in group {
            println!(
                "| {:<13} | {:>11.1} | {:>13.1} | {:>11.1} | {:>10.1} | {:>9.2} | {:>8} |",
                r.encoding.name(),
                micros(r.encode_bulk),
                micros(r.encode_append),
                micros(r.decode_iter),
                micros(r.get_range),
                micros(r.scan_mid_10pct),
                r.bytes,
            );
        }
    }
    println!();
}

// -------- Main --------

fn main() {
    let cfg = parse_args();

    let mut rows: Vec<Row> = Vec::new();
    for workload in &cfg.workloads {
        for ts_model in &cfg.timestamp_models {
            let key = DatasetKey::new(*workload, *ts_model);
            let seed = cfg.seed.unwrap_or_else(|| dataset_seed(key));
            let samples = generate_dataset(key, cfg.samples, seed);

            if !cfg.quiet {
                eprintln!(
                    "measuring {}/{} ({} samples, seed {seed})...",
                    workload.id(),
                    ts_model.id(),
                    samples.len()
                );
            }

            for encoding in &cfg.encodings {
                rows.push(measure(&cfg, key, *encoding, &samples));
            }
        }
    }

    // A chunk that filled before consuming the dataset makes its row
    // incomparable with the others; say so rather than reporting it silently.
    for row in &rows {
        if row.append_len != row.len {
            eprintln!(
                "warning: {} ({}/{}) append filled at {} of {} samples — \
                 encode_append is not comparable with the other columns; \
                 raise --chunk-size",
                row.encoding.name(),
                row.workload.id(),
                row.timestamp_model.id(),
                row.append_len,
                row.len,
            );
        }
        if row.len != cfg.samples {
            eprintln!(
                "warning: {} ({}/{}) stored {} of {} requested samples",
                row.encoding.name(),
                row.workload.id(),
                row.timestamp_model.id(),
                row.len,
                cfg.samples,
            );
        }
    }

    if !cfg.quiet {
        print_table(&cfg, &rows);
    }

    if let Err(e) = write_csv(&cfg.out_csv, &rows) {
        eprintln!("Failed to write CSV: {e}");
    }
    if let Err(e) = write_markdown(&cfg.out_md, &cfg, &rows) {
        eprintln!("Failed to write Markdown: {e}");
    }

    println!(
        "Latency report written to {} and {} (rows: {})",
        cfg.out_csv.display(),
        cfg.out_md.display(),
        rows.len()
    );
}
