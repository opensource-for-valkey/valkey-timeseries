//! Wire-payload cost of each chunk encoding across sample counts.
//!
//! `compression_report` answers "how small does a *full* chunk get" and
//! `latency_report` answers "how fast is a fixed-size chunk". Neither answers
//! the question the clustered fan-out path actually asks: for a response of
//! `n` samples, is it cheaper overall to ship compressed bytes than raw ones?
//!
//! Each row measures the real round trip taken by `src/commands/fanout/chunks.rs`:
//!
//!   shard:       TimeSeriesChunk::new -> set_data -> Chunk::serialize  (encode)
//!   coordinator: TimeSeriesChunk::deserialize -> iter().collect()      (decode)
//!
//! `wire_bytes` is exactly what lands in `SampleData::data`, so the deltas here
//! are the deltas on the network.
//!
//! The verdict column is a break-even link speed. Spending `dt` extra
//! microseconds of CPU to save `db` bytes pays off only while the link needs
//! longer than `dt` to move `db`, i.e. while bandwidth < db/dt. Reported in
//! Gbit/s so it can be compared against the interconnect directly: a break-even
//! of 40 Gbps means the encoding wins on any link slower than that.
//!
//! Run through `tools/wire_report.sh`, which supplies the required features.

use std::collections::BTreeMap;
use std::env;
use std::fs::{self, File};
use std::hint::black_box;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use valkey_timeseries::common::Sample;
use valkey_timeseries::series::chunks::{Chunk, ChunkEncoding, ChunkOps, TimeSeriesChunk};
use valkey_timeseries::tests::generators::{
    DatasetKey, TimestampModel, ValueWorkload, dataset_seed, generate_dataset,
};

// -------- Defaults --------

/// Sample counts to sweep. Dense below 200, where the header/threshold
/// question lives; sparser above, where the per-sample cost has converged.
const DEFAULT_SAMPLE_COUNTS: &[usize] = &[
    1, 2, 3, 5, 8, 10, 12, 16, 20, 25, 30, 40, 50, 64, 80, 100, 128, 160, 200, 256, 320, 400, 512,
    768, 1000, 2000, 4000, 8000,
];

/// Large enough that no encoding hits `is_full()` at the largest sweep point,
/// so every row stores the whole sample set.
const CHUNK_BUDGET: usize = 1024 * 1024;
const DEFAULT_WARMUP: usize = 20;
const DEFAULT_ITERATIONS: usize = 200;

/// A spread of shapes rather than the two "interesting" ones: the fan-out path
/// has no idea what it is about to ship, so the recommendation has to hold for
/// the ugly cases (noisy, irregular) as well as the friendly ones.
const DEFAULT_WORKLOADS: [ValueWorkload; 6] = [
    ValueWorkload::Drift,
    ValueWorkload::Noisy,
    ValueWorkload::Counter,
    ValueWorkload::Discrete,
    ValueWorkload::Periodic,
    ValueWorkload::DriftQuantized,
];

const ALL_ENCODINGS: [ChunkEncoding; 5] = [
    ChunkEncoding::Uncompressed,
    ChunkEncoding::Gorilla,
    ChunkEncoding::Xor2,
    ChunkEncoding::DeXor,
    ChunkEncoding::Chimp,
];

/// The baseline every delta is measured against — what the non-clustered path
/// ships today.
const BASELINE: ChunkEncoding = ChunkEncoding::Uncompressed;

// -------- Configuration --------

struct Config {
    sample_counts: Vec<usize>,
    warmup: usize,
    iterations: usize,
    encodings: Vec<ChunkEncoding>,
    workloads: Vec<ValueWorkload>,
    timestamp_models: Vec<TimestampModel>,
    seed: Option<u64>,
    /// Interconnect speed the verdict column is judged against.
    link_gbps: f64,
    out_csv: PathBuf,
    out_md: PathBuf,
    quiet: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            sample_counts: DEFAULT_SAMPLE_COUNTS.to_vec(),
            warmup: DEFAULT_WARMUP,
            iterations: DEFAULT_ITERATIONS,
            encodings: ALL_ENCODINGS.to_vec(),
            workloads: DEFAULT_WORKLOADS.to_vec(),
            timestamp_models: vec![TimestampModel::Regular, TimestampModel::Irregular],
            seed: None,
            link_gbps: 10.0,
            out_csv: PathBuf::from("target/bench-reports/wire.csv"),
            out_md: PathBuf::from("target/bench-reports/wire.md"),
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
    TimestampModel::all()
        .iter()
        .copied()
        .find(|m| m.id().eq_ignore_ascii_case(name))
}

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
            "--sample-counts" => {
                let raw = next_value(&mut args, "--sample-counts");
                cfg.sample_counts = parse_list(
                    &raw,
                    |s| s.parse::<usize>().ok().filter(|v| *v > 0),
                    "sample count",
                    "positive integers",
                );
                cfg.sample_counts.sort_unstable();
                cfg.sample_counts.dedup();
            }
            "--iterations" => {
                cfg.iterations = parse_usize(&next_value(&mut args, "--iterations"), "--iterations")
            }
            "--warmup" => {
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
                    let mut list = parse_list(&raw, parse_encoding, "encoding", &valid);
                    // Deltas are meaningless without the baseline column.
                    if !list.contains(&BASELINE) {
                        list.insert(0, BASELINE);
                    }
                    list
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
                    TimestampModel::all().to_vec()
                } else {
                    parse_list(
                        &raw,
                        parse_timestamp_model,
                        "timestamp model",
                        "regular, jitter, irregular",
                    )
                };
            }
            "--link-gbps" => {
                let raw = next_value(&mut args, "--link-gbps");
                cfg.link_gbps = match raw.parse::<f64>() {
                    Ok(v) if v > 0.0 => v,
                    _ => {
                        eprintln!("--link-gbps expects a positive number, got '{raw}'");
                        std::process::exit(1);
                    }
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

/// The shard side: build a chunk from the samples and serialize it for the wire.
fn encode(encoding: ChunkEncoding, samples: &[Sample]) -> Vec<u8> {
    let mut chunk = TimeSeriesChunk::new(encoding, CHUNK_BUDGET);
    chunk.set_data(samples).expect("set_data failed");
    let mut buf = Vec::with_capacity(chunk.len() * chunk.bytes_per_sample());
    chunk.serialize(&mut buf);
    buf
}

/// The coordinator side: parse the payload and materialize the samples.
fn decode(buf: &[u8]) -> Vec<Sample> {
    let chunk = TimeSeriesChunk::deserialize(buf).expect("deserialize failed");
    chunk.iter().collect()
}

struct Row {
    encoding: ChunkEncoding,
    workload: ValueWorkload,
    timestamp_model: TimestampModel,
    n: usize,
    wire_bytes: usize,
    encode_time: Duration,
    decode_time: Duration,
    /// Whether the round trip reproduced every sample bit-for-bit.
    lossless: bool,
}

impl Row {
    fn roundtrip_us(&self) -> f64 {
        micros(self.encode_time) + micros(self.decode_time)
    }
}

/// Bit-exact comparison, so a NaN payload (which the grouped/EMPTY path can
/// produce) has to survive as the same NaN and `-0.0` cannot pass as `0.0`.
fn samples_identical(a: &[Sample], b: &[Sample]) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b)
            .all(|(x, y)| x.timestamp == y.timestamp && x.value.to_bits() == y.value.to_bits())
}

fn measure(cfg: &Config, key: DatasetKey, encoding: ChunkEncoding, samples: &[Sample]) -> Row {
    let buf = encode(encoding, samples);
    let decoded = decode(&buf);

    let encode_time = median_time(cfg, || encode(encoding, black_box(samples)));
    let decode_time = median_time(cfg, || decode(black_box(&buf)));

    Row {
        encoding,
        workload: key.workload,
        timestamp_model: key.timestamp_model,
        n: samples.len(),
        wire_bytes: buf.len(),
        encode_time,
        decode_time,
        lossless: samples_identical(samples, &decoded),
    }
}

fn micros(d: Duration) -> f64 {
    d.as_secs_f64() * 1e6
}

/// Break-even link speed in Gbit/s: below it, the bytes saved take longer to
/// transmit than the extra CPU takes to spend, so compressing wins. `None` when
/// the encoding saves nothing (never worth it) or costs nothing (always worth
/// it) — both are degenerate and reported as such.
fn break_even_gbps(bytes_saved: f64, extra_us: f64) -> Option<f64> {
    if bytes_saved <= 0.0 {
        return None;
    }
    if extra_us <= 0.0 {
        return Some(f64::INFINITY);
    }
    // bytes/us -> MB/s -> Gbit/s
    Some(bytes_saved / extra_us * 8.0 / 1000.0)
}

// -------- Aggregation --------

/// One (encoding, n) cell aggregated over every workload / timestamp model, so
/// the recommendation is driven by the whole shape mix rather than one dataset.
#[derive(Default, Clone)]
struct Cell {
    wire_bytes: Vec<f64>,
    roundtrip_us: Vec<f64>,
    encode_us: Vec<f64>,
    decode_us: Vec<f64>,
    lossy_cases: Vec<String>,
}

fn mean(v: &[f64]) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.iter().sum::<f64>() / v.len() as f64
}

type Aggregate = BTreeMap<(usize, String), Cell>;

fn aggregate(rows: &[Row]) -> Aggregate {
    let mut agg: Aggregate = BTreeMap::new();
    for r in rows {
        let cell = agg.entry((r.n, r.encoding.name().to_string())).or_default();
        cell.wire_bytes.push(r.wire_bytes as f64);
        cell.roundtrip_us.push(r.roundtrip_us());
        cell.encode_us.push(micros(r.encode_time));
        cell.decode_us.push(micros(r.decode_time));
        if !r.lossless {
            cell.lossy_cases
                .push(format!("{}/{}", r.workload.id(), r.timestamp_model.id()));
        }
    }
    agg
}

/// A summary line: one encoding at one sample count, against the baseline.
struct Summary {
    n: usize,
    encoding: String,
    bytes: f64,
    bytes_per_sample: f64,
    /// Fraction of the baseline payload this encoding ships (lower is better).
    size_vs_baseline: f64,
    roundtrip_us: f64,
    /// Extra CPU over the baseline round trip; negative means it is also faster.
    extra_us: f64,
    bytes_saved: f64,
    break_even: Option<f64>,
    lossy_cases: Vec<String>,
}

fn summarize(cfg: &Config, agg: &Aggregate) -> Vec<Summary> {
    let mut out = Vec::new();
    for n in &cfg.sample_counts {
        let Some(base) = agg.get(&(*n, BASELINE.name().to_string())) else {
            continue;
        };
        let base_bytes = mean(&base.wire_bytes);
        let base_us = mean(&base.roundtrip_us);

        for encoding in &cfg.encodings {
            let Some(cell) = agg.get(&(*n, encoding.name().to_string())) else {
                continue;
            };
            let bytes = mean(&cell.wire_bytes);
            let us = mean(&cell.roundtrip_us);
            let bytes_saved = base_bytes - bytes;
            let extra_us = us - base_us;
            out.push(Summary {
                n: *n,
                encoding: encoding.name().to_string(),
                bytes,
                bytes_per_sample: bytes / *n as f64,
                size_vs_baseline: if base_bytes > 0.0 {
                    bytes / base_bytes
                } else {
                    f64::NAN
                },
                roundtrip_us: us,
                extra_us,
                bytes_saved,
                break_even: break_even_gbps(bytes_saved, extra_us),
                lossy_cases: cell.lossy_cases.clone(),
            });
        }
    }
    out
}

/// The smallest sample count from which an encoding beats the baseline at the
/// configured link speed, and keeps beating it at every larger count measured.
/// The "and keeps" part matters: a single crossing that later reverses is not a
/// threshold anyone should branch on.
fn stable_threshold(cfg: &Config, summaries: &[Summary], encoding: &str) -> Option<usize> {
    let mut rows: Vec<&Summary> = summaries
        .iter()
        .filter(|s| s.encoding == encoding)
        .collect();
    rows.sort_by_key(|s| s.n);

    let pays = |s: &Summary| match s.break_even {
        // Saves bytes and costs no more CPU: unconditionally better.
        Some(b) if b.is_infinite() => true,
        Some(b) => b >= cfg.link_gbps,
        None => false,
    };

    let mut candidate = None;
    for s in rows.iter().rev() {
        if pays(s) {
            candidate = Some(s.n);
        } else {
            break;
        }
    }
    candidate
}

// -------- Edge-case payloads --------

/// Payloads the fan-out path can legitimately produce but the shape generators
/// never do. The grouped/aggregated path back-fills empty buckets with NaN and
/// can emit infinities from SUM overflow, so an encoding that cannot carry
/// those bit-for-bit is unusable on the wire whatever it scores on size.
fn edge_case_payloads() -> Vec<(&'static str, Vec<Sample>)> {
    fn series(values: &[f64]) -> Vec<Sample> {
        values
            .iter()
            .enumerate()
            .map(|(i, v)| Sample {
                timestamp: i as i64 * 1000,
                value: *v,
            })
            .collect()
    }

    let mut out = vec![
        ("all_nan", series(&[f64::NAN; 32])),
        (
            "nan_interleaved",
            series(
                &(0..32)
                    .map(|i| if i % 3 == 0 { f64::NAN } else { i as f64 })
                    .collect::<Vec<_>>(),
            ),
        ),
        (
            "infinities",
            series(&[f64::INFINITY, f64::NEG_INFINITY, 0.0, f64::INFINITY]),
        ),
        ("negative_zero", series(&[0.0, -0.0, 0.0, -0.0, 1.0, -0.0])),
        (
            "extremes",
            series(&[
                f64::MIN,
                f64::MAX,
                f64::MIN_POSITIVE,
                -f64::MIN_POSITIVE,
                0.0,
            ]),
        ),
        ("subnormals", series(&[1e-310, 5e-324, -1e-310, 0.0])),
        ("single_sample", series(&[42.0])),
    ];

    // Timestamp extremes are a separate axis from value extremes: a delta-of-
    // delta timestamp encoder can overflow on a large backwards jump.
    out.push((
        "timestamp_extremes",
        vec![
            Sample {
                timestamp: 0,
                value: 1.0,
            },
            Sample {
                timestamp: 1,
                value: 2.0,
            },
            Sample {
                timestamp: i64::MAX / 2,
                value: 3.0,
            },
            Sample {
                timestamp: i64::MAX / 2 + 1,
                value: 4.0,
            },
        ],
    ));
    out.push((
        "duplicate_timestamps",
        vec![
            Sample {
                timestamp: 100,
                value: 1.0,
            },
            Sample {
                timestamp: 100,
                value: 2.0,
            },
            Sample {
                timestamp: 200,
                value: 3.0,
            },
        ],
    ));

    out
}

/// Runs every edge-case payload through every encoding's wire round trip.
/// Returns the failures as (encoding, case, detail).
fn run_edge_cases(encodings: &[ChunkEncoding]) -> Vec<(String, String, String)> {
    let mut failures = Vec::new();
    for (name, samples) in edge_case_payloads() {
        for encoding in encodings {
            // An encoder that refuses the payload outright is a failure too, so
            // catch panics rather than letting one case abort the whole run.
            let result = std::panic::catch_unwind(|| {
                let buf = encode(*encoding, &samples);
                decode(&buf)
            });
            match result {
                Ok(decoded) => {
                    if !samples_identical(&samples, &decoded) {
                        failures.push((
                            encoding.name().to_string(),
                            name.to_string(),
                            format!("{} in, {} out, values differ", samples.len(), decoded.len()),
                        ));
                    }
                }
                Err(_) => failures.push((
                    encoding.name().to_string(),
                    name.to_string(),
                    "panicked".to_string(),
                )),
            }
        }
    }
    failures
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
        "encoding,workload,ts_model,n,wire_bytes,bytes_per_sample,encode_us,decode_us,\
         roundtrip_us,lossless"
    )?;
    for r in rows {
        writeln!(
            w,
            "{},{},{},{},{},{:.4},{:.3},{:.3},{:.3},{}",
            r.encoding.name(),
            r.workload.id(),
            r.timestamp_model.id(),
            r.n,
            r.wire_bytes,
            r.wire_bytes as f64 / r.n as f64,
            micros(r.encode_time),
            micros(r.decode_time),
            r.roundtrip_us(),
            r.lossless,
        )?;
    }
    Ok(())
}

/// An encoding name, flagged when any dataset lost data at this sample count.
fn encoding_label(s: &Summary) -> String {
    if s.lossy_cases.is_empty() {
        s.encoding.clone()
    } else {
        format!("{} LOSSY", s.encoding)
    }
}

fn format_break_even(b: Option<f64>) -> String {
    match b {
        None => "never".to_string(),
        Some(v) if v.is_infinite() => "always".to_string(),
        Some(v) => format!("{v:.1}"),
    }
}

fn write_markdown(path: &Path, cfg: &Config, summaries: &[Summary]) -> std::io::Result<()> {
    ensure_dir(path)?;
    let mut w = BufWriter::new(File::create(path)?);

    writeln!(w, "# Wire payload cost by encoding and sample count")?;
    writeln!(w)?;
    writeln!(
        w,
        "Median of {} iterations after {} warmup runs, averaged over {} workload(s) x {} \
         timestamp model(s). `bytes` is the serialized `SampleData::data` payload; \
         `roundtrip` is shard-side encode plus coordinator-side decode.",
        cfg.iterations,
        cfg.warmup,
        cfg.workloads.len(),
        cfg.timestamp_models.len(),
    )?;
    writeln!(w)?;
    writeln!(
        w,
        "`break_even` is the link speed (Gbit/s) below which the bytes saved outweigh the \
         extra CPU. Compare it against the interconnect: at {:.0} Gbps an encoding pays off \
         where `break_even` >= {:.0}.",
        cfg.link_gbps, cfg.link_gbps
    )?;
    writeln!(w)?;

    for n in &cfg.sample_counts {
        let group: Vec<&Summary> = summaries.iter().filter(|s| s.n == *n).collect();
        if group.is_empty() {
            continue;
        }
        writeln!(w, "## n = {n}")?;
        writeln!(w)?;
        writeln!(
            w,
            "| encoding | bytes | bytes/sample | vs uncompressed | roundtrip us | extra us | \
             bytes saved | break_even Gbps |"
        )?;
        writeln!(w, "|---|---:|---:|---:|---:|---:|---:|---:|")?;
        for s in group {
            writeln!(
                w,
                "| {} | {:.0} | {:.2} | {:.2}x | {:.2} | {:+.2} | {:+.0} | {} |",
                encoding_label(s),
                s.bytes,
                s.bytes_per_sample,
                s.size_vs_baseline,
                s.roundtrip_us,
                s.extra_us,
                s.bytes_saved,
                format_break_even(s.break_even),
            )?;
        }
        writeln!(w)?;
    }

    writeln!(w, "## Thresholds at {:.0} Gbps", cfg.link_gbps)?;
    writeln!(w)?;
    writeln!(
        w,
        "Smallest measured sample count from which the encoding pays off and keeps paying off."
    )?;
    writeln!(w)?;
    writeln!(w, "| encoding | threshold |")?;
    writeln!(w, "|---|---:|")?;
    for encoding in &cfg.encodings {
        if *encoding == BASELINE {
            continue;
        }
        let t = stable_threshold(cfg, summaries, encoding.name())
            .map(|n| n.to_string())
            .unwrap_or_else(|| "never".to_string());
        writeln!(w, "| {} | {} |", encoding.name(), t)?;
    }

    Ok(())
}

fn print_tables(cfg: &Config, summaries: &[Summary]) {
    println!(
        "\nWire payload cost — median of {} iterations, averaged over {} workload(s) x {} \
         ts model(s), baseline {}.\n",
        cfg.iterations,
        cfg.workloads.len(),
        cfg.timestamp_models.len(),
        BASELINE.name(),
    );

    for n in &cfg.sample_counts {
        let group: Vec<&Summary> = summaries.iter().filter(|s| s.n == *n).collect();
        if group.is_empty() {
            continue;
        }
        println!("## n = {n}\n");
        println!(
            "| {:<13} | {:>8} | {:>10} | {:>7} | {:>12} | {:>9} | {:>11} | {:>10} |",
            "encoding",
            "bytes",
            "bytes/samp",
            "vs unc",
            "roundtrip us",
            "extra us",
            "bytes saved",
            "break-even"
        );
        println!(
            "|{:-<15}|{:->10}|{:->12}|{:->9}|{:->14}|{:->11}|{:->13}|{:->12}|",
            "", "", "", "", "", "", "", ""
        );
        for s in group {
            println!(
                "| {:<13} | {:>8.0} | {:>10.2} | {:>6.2}x | {:>12.2} | {:>+9.2} | {:>+11.0} | {:>10} |",
                encoding_label(s),
                s.bytes,
                s.bytes_per_sample,
                s.size_vs_baseline,
                s.roundtrip_us,
                s.extra_us,
                s.bytes_saved,
                format_break_even(s.break_even),
            );
        }
        println!();
    }

    println!("## Thresholds at {:.0} Gbps\n", cfg.link_gbps);
    for encoding in &cfg.encodings {
        if *encoding == BASELINE {
            continue;
        }
        match stable_threshold(cfg, summaries, encoding.name()) {
            Some(n) => println!("  {:<13} pays off from n >= {}", encoding.name(), n),
            None => println!(
                "  {:<13} never pays off at this link speed",
                encoding.name()
            ),
        }
    }
    println!();
}

// -------- Main --------

fn main() {
    let cfg = parse_args();

    // Correctness gate first: size and speed are irrelevant for an encoding
    // that cannot carry what the fan-out path produces.
    let edge_failures = run_edge_cases(&cfg.encodings);
    if !cfg.quiet {
        if edge_failures.is_empty() {
            eprintln!(
                "edge cases: all {} encodings round-tripped every payload bit-exactly",
                cfg.encodings.len()
            );
        } else {
            eprintln!("\nEDGE CASE FAILURES (encoding is unusable on the wire for these):");
            for (encoding, case, detail) in &edge_failures {
                eprintln!("  {encoding:<13} {case:<22} {detail}");
            }
            eprintln!();
        }
    }

    let max_n = *cfg.sample_counts.last().expect("non-empty sample counts");

    let mut rows: Vec<Row> = Vec::new();
    for workload in &cfg.workloads {
        for ts_model in &cfg.timestamp_models {
            let key = DatasetKey::new(*workload, *ts_model);
            let seed = cfg.seed.unwrap_or_else(|| dataset_seed(key));
            // Generate once at the largest count and take prefixes, so a given
            // n sees the same data in every row it appears in.
            let samples = generate_dataset(key, max_n, seed);

            if !cfg.quiet {
                eprintln!("measuring {}/{}...", workload.id(), ts_model.id());
            }

            for n in &cfg.sample_counts {
                for encoding in &cfg.encodings {
                    rows.push(measure(&cfg, key, *encoding, &samples[..*n]));
                }
            }
        }
    }

    // A lossy round trip disqualifies an encoding outright, whatever it scores
    // on size — say so loudly rather than burying it in a CSV column.
    let lossy: Vec<&Row> = rows.iter().filter(|r| !r.lossless).collect();
    if !lossy.is_empty() {
        let mut seen: BTreeMap<String, usize> = BTreeMap::new();
        for r in &lossy {
            *seen
                .entry(format!(
                    "{} ({}/{})",
                    r.encoding.name(),
                    r.workload.id(),
                    r.timestamp_model.id()
                ))
                .or_default() += 1;
        }
        eprintln!(
            "\nWARNING: lossy round trips detected — these encodings must not be used on the wire:"
        );
        for (what, count) in seen {
            eprintln!("  {what}: {count} sample count(s) failed bit-exact round trip");
        }
        eprintln!();
    }

    let agg = aggregate(&rows);
    let summaries = summarize(&cfg, &agg);

    if !cfg.quiet {
        print_tables(&cfg, &summaries);
    }

    if let Err(e) = write_csv(&cfg.out_csv, &rows) {
        eprintln!("Failed to write CSV: {e}");
    }
    if let Err(e) = write_markdown(&cfg.out_md, &cfg, &summaries) {
        eprintln!("Failed to write Markdown: {e}");
    }

    println!(
        "Wire report written to {} and {} (rows: {})",
        cfg.out_csv.display(),
        cfg.out_md.display(),
        rows.len()
    );
}
