//! Measures what label interning saves on a realistic fleet.
//!
//! Builds every series of a [`FleetTopology`] as a [`MetricName`] -- the exact structure a
//! `TS.CREATE` produces -- holds them all, and reads the live string pool back through the same
//! [`InternedString::get_stats_with_top_k`] that `TS._DEBUG STRINGPOOLSTATS` reports. The dataset
//! is then costed under the layouts interning replaced, so the saving is stated against a real
//! alternative rather than a hypothetical one.
//!
//! Run via `tools/interning_report.sh`; `--help` lists the flags.

use std::collections::{BTreeMap, HashSet};
use std::env;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::time::Instant;

use valkey_timeseries::common::string_interner::{InternedString, Stats};
use valkey_timeseries::tests::generators::{
    DEFAULT_FLEET_SEED, FleetPreset, FleetTopology, MAX_ROUTES_PER_SERVICE, SeriesSpec,
};
use valkey_timeseries::{Label, MetricName};

/// The control block in front of every heap string: the interner's header (count, length,
/// separator) and an `Arc<[u8]>`'s (strong and weak counts) are both two words.
const ARC_HEADER: usize = 2 * size_of::<usize>();
/// A `MetricName` entry: one thin pointer.
const INTERNED_SLOT: usize = size_of::<InternedString>();
/// The `Arc` counts in front of a `MetricName`'s shared label slice, once per series.
const SLICE_HEADER: usize = 2 * size_of::<usize>();
/// A `Label` in a `Vec<Label>`: two `String`s inline.
const LABEL_SLOT: usize = size_of::<Label>();

struct Config {
    topology: FleetTopology,
    preset: Option<FleetPreset>,
    top_k: usize,
    emit_commands: Option<PathBuf>,
}

fn usage() -> ! {
    eprintln!(
        "usage: interning_report [--preset small|medium|large] [--seed N] [--top K]\n\
         \x20                       [--clusters N] [--hosts N] [--namespaces N] [--pods N] [--routes N]\n\
         \x20                       [--emit-commands PATH]\n\
         \n\
         --hosts must be at least 1 when --clusters is; --routes is capped at {MAX_ROUTES_PER_SERVICE}."
    );
    std::process::exit(2)
}

fn parse_args() -> Config {
    let mut preset = Some(FleetPreset::Medium);
    let mut overrides: Vec<(String, usize)> = Vec::new();
    let mut seed = DEFAULT_FLEET_SEED;
    let mut top_k = 10;
    let mut emit_commands = None;

    let mut args = env::args().skip(1);
    let next_value = |flag: &str, args: &mut dyn Iterator<Item = String>| -> String {
        args.next().unwrap_or_else(|| {
            eprintln!("{flag} requires a value");
            usage()
        })
    };
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--preset" => {
                let raw = next_value("--preset", &mut args);
                preset = Some(FleetPreset::parse(&raw).unwrap_or_else(|| {
                    eprintln!("unknown preset '{raw}'");
                    usage()
                }));
            }
            "--seed" => {
                let raw = next_value("--seed", &mut args);
                seed = raw
                    .strip_prefix("0x")
                    .map(|h| u64::from_str_radix(h, 16))
                    .unwrap_or_else(|| raw.parse())
                    .unwrap_or_else(|_| {
                        eprintln!("--seed expects an integer, got '{raw}'");
                        usage()
                    });
            }
            "--top" => {
                let raw = next_value("--top", &mut args);
                top_k = raw.parse().unwrap_or_else(|_| {
                    eprintln!("--top expects an integer, got '{raw}'");
                    usage()
                });
            }
            "--clusters" | "--hosts" | "--namespaces" | "--pods" | "--routes" => {
                let raw = next_value(&arg, &mut args);
                let n: usize = raw.parse().unwrap_or_else(|_| {
                    eprintln!("{arg} expects an integer, got '{raw}'");
                    usage()
                });
                if arg == "--clusters" && n == 0 {
                    eprintln!("--clusters must be at least 1");
                    usage()
                }
                overrides.push((arg.clone(), n));
            }
            "--emit-commands" => {
                emit_commands = Some(PathBuf::from(next_value("--emit-commands", &mut args)));
            }
            "-h" | "--help" => usage(),
            other => {
                eprintln!("unknown option '{other}'");
                usage()
            }
        }
    }

    let mut topology = FleetTopology::preset(preset.unwrap_or(FleetPreset::Medium)).with_seed(seed);
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
    // Overrides go through the generator's own limits, so a hostless cluster or a route count
    // past the vocabulary is a usage error here rather than a hang or panic inside `generate`.
    if let Err(e) = topology.validate() {
        eprintln!("invalid topology: {e}");
        usage()
    }
    Config {
        topology,
        preset,
        top_k,
        emit_commands,
    }
}

// -------- Dataset-side accounting --------

/// What one label key costs across the fleet, under each layout.
#[derive(Default)]
struct KeyCost {
    occurrences: usize,
    unique: HashSet<String>,
    /// Σ over occurrences of the interned pair's allocation: what `Arc`-per-pair would hold.
    uninterned: usize,
    /// Σ over *unique* pairs: what the pool holds.
    pool: usize,
}

struct DatasetCost {
    series: usize,
    pairs: usize,
    unique_pairs: usize,
    duplicate_label_sets: usize,
    /// `Arc<[InternedString]>` per series + one pool allocation per unique pair.
    interned: usize,
    /// The same slots + one allocation per occurrence, no pool.
    arc_per_pair: usize,
    /// `Vec<Label>` slots + two heap `String`s per occurrence.
    string_labels: usize,
    by_key: BTreeMap<String, KeyCost>,
}

fn cost_dataset(fleet: &[SeriesSpec]) -> DatasetCost {
    let mut by_key: BTreeMap<String, KeyCost> = BTreeMap::new();
    let mut unique_pairs: HashSet<String> = HashSet::new();
    let mut label_sets: HashSet<Vec<(String, String)>> = HashSet::with_capacity(fleet.len());
    let mut pairs = 0;
    let mut slots_interned = 0;
    let mut arc_per_pair = 0;
    let mut string_labels = 0;
    let mut duplicate_label_sets = 0;

    for series in fleet {
        pairs += series.labels.len();
        slots_interned += SLICE_HEADER + series.labels.len() * INTERNED_SLOT;
        string_labels += series.labels.len() * LABEL_SLOT;
        let mut sorted: Vec<(String, String)> = Vec::with_capacity(series.labels.len());
        for label in &series.labels {
            let pair = format!("{}={}", label.name, label.value);
            let allocated = ARC_HEADER + pair.len();
            arc_per_pair += allocated;
            string_labels += label.name.len() + label.value.len();

            let cost = by_key.entry(label.name.clone()).or_default();
            cost.occurrences += 1;
            cost.uninterned += allocated;
            if cost.unique.insert(label.value.clone()) {
                cost.pool += allocated;
            }
            unique_pairs.insert(pair);
            sorted.push((label.name.clone(), label.value.clone()));
        }
        sorted.sort();
        if !label_sets.insert(sorted) {
            duplicate_label_sets += 1;
        }
    }

    let pool: usize = by_key.values().map(|c| c.pool).sum();
    DatasetCost {
        series: fleet.len(),
        pairs,
        unique_pairs: unique_pairs.len(),
        duplicate_label_sets,
        interned: slots_interned + pool,
        arc_per_pair: slots_interned + arc_per_pair,
        string_labels,
        by_key,
    }
}

// -------- Output --------

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

fn bytes(n: usize) -> String {
    const UNITS: [&str; 4] = ["B", "KiB", "MiB", "GiB"];
    let mut v = n as f64;
    let mut unit = 0;
    while v >= 1024.0 && unit < UNITS.len() - 1 {
        v /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{n} B")
    } else {
        format!("{v:.1} {}", UNITS[unit])
    }
}

fn pct(part: usize, whole: usize) -> f64 {
    if whole == 0 {
        0.0
    } else {
        part as f64 / whole as f64 * 100.0
    }
}

/// Ref-count buckets that separate the cardinality tiers, as `(order, name)`.
fn ref_bucket(refs: usize) -> (u8, &'static str) {
    match refs {
        0..=1 => (0, "1 (per-series)"),
        2..=9 => (1, "2-9"),
        10..=99 => (2, "10-99"),
        100..=999 => (3, "100-999"),
        _ => (4, "1000+ (fleet-wide)"),
    }
}

fn truncate(s: &str, width: usize) -> String {
    if s.chars().count() <= width {
        s.to_string()
    } else {
        let head: String = s.chars().take(width - 1).collect();
        format!("{head}…")
    }
}

fn print_report(cfg: &Config, dataset: &DatasetCost, stats: &Stats, build: std::time::Duration) {
    let t = &cfg.topology;
    println!("Label interning report");
    println!(
        "  fleet:   {} (seed {:#x}): {} clusters × {} hosts, {} namespaces, {} product pods/cluster, {} routes/service",
        cfg.preset.map_or("custom", FleetPreset::id),
        t.seed,
        t.clusters,
        t.hosts_per_cluster,
        t.namespaces_per_cluster,
        t.pods_per_cluster,
        t.routes_per_service
    );
    println!(
        "  series:  {}   label pairs: {} ({:.1}/series)   unique pairs: {} ({:.2}%)   built in {:.2?}",
        commas(dataset.series),
        commas(dataset.pairs),
        dataset.pairs as f64 / dataset.series as f64,
        commas(dataset.unique_pairs),
        pct(dataset.unique_pairs, dataset.pairs),
        build
    );
    if dataset.duplicate_label_sets > 0 {
        println!(
            "  WARNING: {} series share a label set with another series",
            dataset.duplicate_label_sets
        );
    }
    println!();

    let pool = &stats.total_stats;
    println!("Live pool (InternedString::get_stats, what TS._DEBUG STRINGPOOLSTATS reports)");
    println!(
        "  entries: {}   payload: {}   allocated: {} ({} B Arc header each)   {:.1} B/series",
        commas(pool.count),
        bytes(pool.bytes),
        bytes(pool.allocated),
        ARC_HEADER,
        pool.allocated as f64 / dataset.series as f64
    );
    println!(
        "  memory_saved_bytes: {}   memory_saved_pct: {:.1}%   (pool only, vs. one allocation per holder)",
        bytes(stats.memory_saved_bytes),
        stats.memory_saved_pct
    );
    // The pool-only percentage ignores the slot every holder keeps either way, which is most of
    // what labels cost once the pool is deduplicating well. Printing both stops the high number
    // from being read as the label-memory saving; the layout table below is the same comparison
    // with the per-series slice header the pool cannot see.
    println!(
        "  holders: {}   slots: {} ({} B each)   total storage: {}   storage_saved_pct: {:.1}%",
        commas(stats.holder_count),
        bytes(stats.holder_slot_bytes),
        INTERNED_SLOT,
        bytes(stats.total_storage_bytes),
        stats.storage_saved_pct
    );
    if pool.count != dataset.unique_pairs {
        println!(
            "  NOTE: pool has {} entries but the dataset has {} unique pairs -- something else is interning",
            commas(pool.count),
            commas(dataset.unique_pairs)
        );
    }
    println!();

    println!("Label storage per layout (label vectors + string payloads, chunks excluded)");
    println!(
        "  {:<44} {:>12} {:>10} {:>10}",
        "layout", "total", "per series", "saved"
    );
    let rows = [
        ("MetricName: Arc<[InternedString]> + pool", dataset.interned),
        ("Arc<[u8]> per pair, no dedup", dataset.arc_per_pair),
        (
            "Vec<Label>: String key + String value",
            dataset.string_labels,
        ),
    ];
    for (name, total) in rows {
        let saved = total.saturating_sub(dataset.interned);
        println!(
            "  {:<44} {:>12} {:>8.1} B {:>9.1}%",
            name,
            bytes(total),
            total as f64 / dataset.series as f64,
            pct(saved, total)
        );
    }
    println!();

    // Ref-count tiers, from the live pool.
    let mut tiers: BTreeMap<u8, (&str, usize, usize)> = BTreeMap::new();
    for (&refs, bucket) in &stats.by_ref_stats {
        let (order, name) = ref_bucket(refs);
        let e = tiers.entry(order).or_insert((name, 0, 0));
        e.1 += bucket.count;
        e.2 += bucket.allocated;
    }
    println!("Pool by reference count (how many series share each entry)");
    println!(
        "  {:<22} {:>10} {:>12} {:>8}",
        "refs", "entries", "pool bytes", "of pool"
    );
    for (name, count, allocated) in tiers.values() {
        println!(
            "  {:<22} {:>10} {:>12} {:>7.1}%",
            name,
            commas(*count),
            bytes(*allocated),
            pct(*allocated, pool.allocated)
        );
    }
    println!();

    // Per label key, ranked by what the pool retains for it.
    let mut keys: Vec<(&String, &KeyCost)> = dataset.by_key.iter().collect();
    keys.sort_by(|a, b| b.1.pool.cmp(&a.1.pool).then(a.0.cmp(b.0)));
    println!("By label key (ranked by pool bytes retained; 'saved' is vs. Arc-per-pair)");
    println!(
        "  {:<26} {:>10} {:>10} {:>12} {:>12} {:>7}",
        "key", "series", "unique", "pool", "uninterned", "saved"
    );
    for (key, cost) in keys.iter().take(cfg.top_k.max(12)) {
        println!(
            "  {:<26} {:>10} {:>10} {:>12} {:>12} {:>6.1}%",
            truncate(key, 26),
            commas(cost.occurrences),
            commas(cost.unique.len()),
            bytes(cost.pool),
            bytes(cost.uninterned),
            pct(cost.uninterned - cost.pool, cost.uninterned)
        );
    }
    if keys.len() > cfg.top_k.max(12) {
        println!("  … {} more keys", keys.len() - cfg.top_k.max(12));
    }
    println!();

    if cfg.top_k > 0 {
        println!("Top {} pool entries by reference count", cfg.top_k);
        for e in &stats.top_k_by_ref {
            println!(
                "  {:>10} refs  {:>4} B  {}",
                commas(e.ref_count),
                e.allocated,
                truncate(&e.value, 90)
            );
        }
        println!();
        println!("Top {} pool entries by size", cfg.top_k);
        for e in &stats.top_k_by_size {
            println!(
                "  {:>10} refs  {:>4} B  {}",
                commas(e.ref_count),
                e.allocated,
                truncate(&e.value, 90)
            );
        }
        println!();
    }
}

/// Inline-protocol `TS.CREATE` lines, one per series, for `valkey-cli --pipe`.
fn emit_commands(path: &PathBuf, fleet: &[SeriesSpec]) -> std::io::Result<()> {
    let mut out = BufWriter::new(File::create(path)?);
    for series in fleet {
        write!(out, "TS.CREATE {} LABELS", series.key)?;
        for label in &series.labels {
            if label.value.contains([' ', '\'']) {
                write!(out, " {} \"{}\"", label.name, label.value)?;
            } else {
                write!(out, " {} {}", label.name, label.value)?;
            }
        }
        writeln!(out)?;
    }
    out.flush()
}

fn main() {
    let cfg = parse_args();

    let started = Instant::now();
    let fleet = cfg.topology.try_generate().unwrap_or_else(|e| {
        eprintln!("invalid topology: {e}");
        usage()
    });
    let generated = started.elapsed();

    let pool_before = InternedString::interned_count();
    let started = Instant::now();
    // Held for the lifetime of the report so the pool reflects a populated keyspace.
    let metric_names: Vec<MetricName> = fleet.iter().map(|s| MetricName::new(&s.labels)).collect();
    let built = started.elapsed();

    let stats = InternedString::get_stats_with_top_k(cfg.top_k);
    let dataset = cost_dataset(&fleet);

    if pool_before != 0 {
        println!("NOTE: pool held {pool_before} entries before the fleet was built");
    }
    print_report(&cfg, &dataset, &stats, built);
    println!(
        "  (fleet generated in {generated:.2?}; {} MetricNames held)",
        commas(metric_names.len())
    );

    if let Some(path) = &cfg.emit_commands {
        emit_commands(path, &fleet).unwrap_or_else(|e| {
            eprintln!("failed to write {}: {e}", path.display());
            std::process::exit(1);
        });
        println!();
        println!(
            "Wrote {} TS.CREATE commands to {}",
            commas(fleet.len()),
            path.display()
        );
        println!("  load:    valkey-cli --pipe < {}", path.display());
        println!("  inspect: valkey-cli CONFIG SET ts.debug-mode yes");
        println!(
            "           valkey-cli TS._DEBUG STRINGPOOLSTATS {}",
            cfg.top_k
        );
        println!("           valkey-cli INFO ts_memory");
    }

    // Everything the report measured stays alive until here.
    drop(metric_names);
}
