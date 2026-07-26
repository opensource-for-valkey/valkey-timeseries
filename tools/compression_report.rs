use std::collections::HashMap;
use std::env;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use get_size2::GetSize;
use valkey_timeseries::series::chunks::{ChunkEncoding, ChunkOps};
use valkey_timeseries::tests::chunk_utils::{
    CHUNK_SIZE_1K, CHUNK_SIZE_4K, CHUNK_SIZE_64K, build_chunk_until_full, chunk_size_id,
    encoded_size, filled_prefix_len,
};
use valkey_timeseries::tests::generators::{
    DATASET_SAMPLES, DatasetKey, DatasetRegistry, TimestampModel, ValueWorkload,
    benchmark_dataset_keys,
};

// -------- Report structures --------

#[derive(Debug, Clone)]
struct RowKey {
    encoding: ChunkEncoding,
    workload: ValueWorkload,
    timestamp_model: TimestampModel,
    chunk_size: usize,
}

impl RowKey {
    fn id(&self) -> String {
        format!(
            "{}/{}/{}/{}",
            self.encoding.name(),
            self.workload.id(),
            self.timestamp_model.id(),
            chunk_size_id(self.chunk_size)
        )
    }
}

#[derive(Debug, Clone, Default)]
struct RowVals {
    len: usize,
    data_size: usize,
    size: usize,
    bytes_per_sample: f64,
}

impl RowVals {
    fn ratio(&self) -> f64 {
        if self.data_size == 0 {
            return f64::INFINITY;
        }
        (self.len as f64 * 16.0) / self.data_size as f64
    }
}

fn encode_matrix_chunk_sizes(key: &DatasetKey) -> Vec<usize> {
    match key.workload {
        ValueWorkload::Drift | ValueWorkload::Noisy => {
            vec![CHUNK_SIZE_1K, CHUNK_SIZE_4K, CHUNK_SIZE_64K]
        }
        _ => vec![CHUNK_SIZE_4K],
    }
}

fn encodings() -> [ChunkEncoding; 5] {
    [
        ChunkEncoding::Uncompressed,
        ChunkEncoding::Gorilla,
        ChunkEncoding::Xor2,
        ChunkEncoding::DeXor,
        ChunkEncoding::Chimp,
    ]
}

// -------- Baseline checking --------

fn load_baseline(path: &Path) -> HashMap<String, f64> {
    let mut map = HashMap::new();
    if !path.exists() {
        return map;
    }
    let Ok(text) = fs::read_to_string(path) else {
        return map;
    };
    for (i, line) in text.lines().enumerate() {
        if i == 0 {
            continue;
        } // header
        let cols: Vec<&str> = line.split(',').collect();
        if cols.len() < 9 {
            continue;
        }
        let id = format!("{}/{}/{}/{}", cols[0], cols[1], cols[2], cols[3]);
        if let Ok(r) = cols[8].parse::<f64>() {
            map.insert(id, r);
        }
    }
    map
}

// -------- Output writers --------

fn ensure_dir(p: &Path) -> std::io::Result<()> {
    if let Some(parent) = p.parent() {
        fs::create_dir_all(parent)?;
    }
    Ok(())
}

fn write_csv(
    path: &Path,
    rows: &[(RowKey, RowVals)],
    capacity4k: &HashMap<(String, ValueWorkload), usize>,
) -> std::io::Result<()> {
    ensure_dir(path)?;
    let file = File::create(path)?;
    let mut w = BufWriter::new(file);
    writeln!(
        w,
        "encoding,workload,ts_model,chunk_size,len,data_size,size,bytes_per_sample,ratio,capacity_4k"
    )?;
    for (k, v) in rows {
        let cap_key = (k.encoding.name().to_string(), k.workload);
        let cap = capacity4k.get(&cap_key).cloned().unwrap_or_default();
        writeln!(
            w,
            "{},{},{},{},{},{},{},{:.6},{:.6},{}",
            k.encoding.name(),
            k.workload.id(),
            k.timestamp_model.id(),
            chunk_size_id(k.chunk_size),
            v.len,
            v.data_size,
            v.size,
            v.bytes_per_sample,
            v.ratio(),
            cap
        )?;
    }
    Ok(())
}

fn write_markdown(path: &Path, rows: &[(RowKey, RowVals)]) -> std::io::Result<()> {
    ensure_dir(path)?;
    let file = File::create(path)?;
    let mut w = BufWriter::new(file);
    writeln!(
        w,
        "| encoding | workload | ts_model | chunk | len | data_size | size | bytes/sample | ratio |"
    )?;
    writeln!(w, "|---|---|---|---|---:|---:|---:|---:|---:|")?;
    for (k, v) in rows {
        writeln!(
            w,
            "| {} | {} | {} | {} | {} | {} | {} | {:.6} | {:.6} |",
            k.encoding.name(),
            k.workload.id(),
            k.timestamp_model.id(),
            chunk_size_id(k.chunk_size),
            v.len,
            v.data_size,
            v.size,
            v.bytes_per_sample,
            v.ratio(),
        )?;
    }
    Ok(())
}

// -------- Pivoted output (rows: workload/ts_model, columns: encoding) --------

/// The metric a pivot cell holds.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum PivotMetric {
    Ratio,
    BytesPerSample,
    Capacity,
}

impl PivotMetric {
    fn parse(s: &str) -> Option<Self> {
        match s {
            "ratio" => Some(Self::Ratio),
            "bytes-per-sample" | "bps" => Some(Self::BytesPerSample),
            "capacity" | "len" => Some(Self::Capacity),
            _ => None,
        }
    }

    fn id(self) -> &'static str {
        match self {
            Self::Ratio => "ratio",
            Self::BytesPerSample => "bytes-per-sample",
            Self::Capacity => "capacity",
        }
    }

    fn title(self) -> &'static str {
        match self {
            Self::Ratio => "Compression ratio (16 bytes/sample ÷ actual; higher is better)",
            Self::BytesPerSample => "Bytes per sample (lower is better)",
            Self::Capacity => "Samples held per chunk (higher is better)",
        }
    }

    fn value(self, v: &RowVals) -> f64 {
        match self {
            Self::Ratio => v.ratio(),
            Self::BytesPerSample => v.bytes_per_sample,
            Self::Capacity => v.len as f64,
        }
    }

    /// Whether a larger value is the better result — drives which cell is bolded.
    fn higher_is_better(self) -> bool {
        !matches!(self, Self::BytesPerSample)
    }

    fn format(self, value: f64) -> String {
        match self {
            Self::Capacity => format!("{}", value as usize),
            _ => format!("{value:.2}"),
        }
    }
}

/// Ordering index of a workload / timestamp model, so pivot rows come out in
/// declaration order rather than alphabetically.
fn workload_index(workload: ValueWorkload) -> usize {
    ValueWorkload::workloads()
        .iter()
        .position(|w| *w == workload)
        .unwrap_or(usize::MAX)
}

fn timestamp_model_index(model: TimestampModel) -> usize {
    TimestampModel::all()
        .iter()
        .position(|m| *m == model)
        .unwrap_or(usize::MAX)
}

/// A pivot row: one dataset key plus one cell per encoding (in `encodings()`
/// order), `None` where that combination was not measured.
type PivotRow = (DatasetKey, Vec<Option<RowVals>>);

/// One table per chunk size; within a table one row per workload/timestamp
/// model and one column per encoding.
fn pivot_tables(rows: &[(RowKey, RowVals)]) -> Vec<(usize, Vec<PivotRow>)> {
    let mut chunk_sizes: Vec<usize> = rows.iter().map(|(k, _)| k.chunk_size).collect();
    chunk_sizes.sort_unstable();
    chunk_sizes.dedup();

    chunk_sizes
        .into_iter()
        .map(|chunk_size| {
            let mut keys: Vec<DatasetKey> = rows
                .iter()
                .filter(|(k, _)| k.chunk_size == chunk_size)
                .map(|(k, _)| DatasetKey::new(k.workload, k.timestamp_model))
                .collect();
            keys.sort_by_key(|k| {
                (
                    workload_index(k.workload),
                    timestamp_model_index(k.timestamp_model),
                )
            });
            keys.dedup();

            let table = keys
                .into_iter()
                .map(|key| {
                    let cells = encodings()
                        .iter()
                        .map(|encoding| {
                            rows.iter()
                                .find(|(k, _)| {
                                    k.chunk_size == chunk_size
                                        && k.workload == key.workload
                                        && k.timestamp_model == key.timestamp_model
                                        && k.encoding == *encoding
                                })
                                .map(|(_, v)| v.clone())
                        })
                        .collect();
                    (key, cells)
                })
                .collect();
            (chunk_size, table)
        })
        .collect()
}

/// Index of the best cell in a row, ignoring the `uncompressed` baseline column.
fn best_cell(metric: PivotMetric, cells: &[Option<RowVals>]) -> Option<usize> {
    cells
        .iter()
        .enumerate()
        .filter(|(idx, cell)| cell.is_some() && encodings()[*idx] != ChunkEncoding::Uncompressed)
        .max_by(|(_, a), (_, b)| {
            let a = metric.value(a.as_ref().unwrap());
            let b = metric.value(b.as_ref().unwrap());
            if metric.higher_is_better() {
                a.total_cmp(&b)
            } else {
                b.total_cmp(&a)
            }
        })
        .map(|(idx, _)| idx)
}

fn write_pivot_markdown(
    path: &Path,
    rows: &[(RowKey, RowVals)],
    metric: PivotMetric,
) -> std::io::Result<()> {
    ensure_dir(path)?;
    let file = File::create(path)?;
    let mut w = BufWriter::new(file);

    writeln!(w, "# {}", metric.title())?;
    writeln!(w)?;
    writeln!(
        w,
        "Best encoding per row is **bold**; `uncompressed` is the baseline and is excluded from that comparison."
    )?;

    for (chunk_size, table) in pivot_tables(rows) {
        writeln!(w)?;
        writeln!(w, "## chunk size {}", chunk_size_id(chunk_size))?;
        writeln!(w)?;

        let mut header = String::from("| workload | ts_model |");
        let mut sep = String::from("|---|---|");
        for encoding in encodings() {
            header.push_str(&format!(" {} |", encoding.name()));
            sep.push_str("---:|");
        }
        writeln!(w, "{header}")?;
        writeln!(w, "{sep}")?;

        for (key, cells) in table {
            let best = best_cell(metric, &cells);
            let mut line = format!("| {} | {} |", key.workload.id(), key.timestamp_model.id());
            for (idx, cell) in cells.iter().enumerate() {
                match cell {
                    Some(v) => {
                        let text = metric.format(metric.value(v));
                        if Some(idx) == best {
                            line.push_str(&format!(" **{text}** |"));
                        } else {
                            line.push_str(&format!(" {text} |"));
                        }
                    }
                    None => line.push_str(" — |"),
                }
            }
            writeln!(w, "{line}")?;
        }
    }

    Ok(())
}

fn write_pivot_csv(
    path: &Path,
    rows: &[(RowKey, RowVals)],
    metric: PivotMetric,
) -> std::io::Result<()> {
    ensure_dir(path)?;
    let file = File::create(path)?;
    let mut w = BufWriter::new(file);

    let mut header = String::from("metric,chunk_size,workload,ts_model");
    for encoding in encodings() {
        header.push(',');
        header.push_str(encoding.name());
    }
    writeln!(w, "{header}")?;

    for (chunk_size, table) in pivot_tables(rows) {
        for (key, cells) in table {
            let mut line = format!(
                "{},{},{},{}",
                metric.id(),
                chunk_size_id(chunk_size),
                key.workload.id(),
                key.timestamp_model.id()
            );
            for cell in &cells {
                line.push(',');
                if let Some(v) = cell {
                    line.push_str(&format!("{:.6}", metric.value(v)));
                }
            }
            writeln!(w, "{line}")?;
        }
    }

    Ok(())
}

// -------- Main --------

fn main() {
    // Flags: --check (exit non-zero on regression beyond tolerance), --baseline <path>,
    // --by-workload [metric] (extra pivot report: rows workload/ts_model, columns encoding)
    let mut check = false;
    let mut baseline_path: Option<PathBuf> = None;
    let mut pivot_metric: Option<PivotMetric> = None;
    let mut args = env::args().skip(1).peekable();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--check" => check = true,
            "--baseline" => baseline_path = args.next().map(PathBuf::from),
            "--by-workload" => {
                // Optional metric argument; defaults to the compression ratio.
                let metric = match args.peek().map(String::as_str) {
                    Some(next) if !next.starts_with("--") => {
                        let parsed = PivotMetric::parse(next);
                        if parsed.is_none() {
                            eprintln!(
                                "Unknown --by-workload metric '{next}' (expected ratio, bytes-per-sample, or capacity)"
                            );
                            std::process::exit(1);
                        }
                        args.next();
                        parsed
                    }
                    _ => Some(PivotMetric::Ratio),
                };
                pivot_metric = metric;
            }
            _ => {}
        }
    }

    let baseline_path = baseline_path
        .unwrap_or_else(|| PathBuf::from("benches/baselines/compression_baseline.csv"));
    let baseline = load_baseline(&baseline_path);

    // Build datasets once
    let datasets = DatasetRegistry::with_samples(DATASET_SAMPLES);

    // Capacity@4k per (encoding, workload)
    let mut capacity4k: HashMap<(String, ValueWorkload), usize> = HashMap::new();

    // Collect rows
    let mut rows: Vec<(RowKey, RowVals)> = Vec::new();

    for key in benchmark_dataset_keys() {
        let data = datasets.dataset(key);
        for encoding in encodings() {
            // capacity at 4k (only compute once per (enc, workload) pair)
            let cap_key = (encoding.name().to_string(), key.workload);
            capacity4k
                .entry(cap_key)
                .or_insert_with(|| filled_prefix_len(data, encoding, CHUNK_SIZE_4K));

            for chunk_size in encode_matrix_chunk_sizes(&key) {
                // Reuse the chunk constructed during the prefix search instead of rebuilding it
                let (chunk, _needed) = build_chunk_until_full(encoding, chunk_size, data);

                let len = chunk.len();
                // Bytes the encoder actually wrote. `chunk.size()` is not
                // comparable across encodings — see `encoded_size`.
                let data_size = encoded_size(&chunk);
                // Total heap footprint, including unused buffer capacity.
                let footprint = chunk.get_size();
                let bps = if len > 0 {
                    data_size as f64 / len as f64
                } else {
                    0.0
                };
                let vals = RowVals {
                    len,
                    data_size,
                    size: footprint,
                    bytes_per_sample: bps,
                };
                let key_row = RowKey {
                    encoding,
                    workload: key.workload,
                    timestamp_model: key.timestamp_model,
                    chunk_size,
                };
                rows.push((key_row, vals));
            }
        }
    }

    // Sort rows by id for stable output
    rows.sort_by(|a, b| a.0.id().cmp(&b.0.id()));

    // Write outputs
    let out_csv = PathBuf::from("target/bench-reports/compression.csv");
    let out_md = PathBuf::from("target/bench-reports/compression.md");
    if let Err(e) = write_csv(&out_csv, &rows, &capacity4k) {
        eprintln!("Failed to write CSV: {e}");
    }
    if let Err(e) = write_markdown(&out_md, &rows) {
        eprintln!("Failed to write Markdown: {e}");
    }

    let pivot_paths = pivot_metric.map(|metric| {
        let out_csv = PathBuf::from(format!(
            "target/bench-reports/compression_by_workload_{}.csv",
            metric.id()
        ));
        let out_md = PathBuf::from(format!(
            "target/bench-reports/compression_by_workload_{}.md",
            metric.id()
        ));
        if let Err(e) = write_pivot_csv(&out_csv, &rows, metric) {
            eprintln!("Failed to write pivot CSV: {e}");
        }
        if let Err(e) = write_pivot_markdown(&out_md, &rows, metric) {
            eprintln!("Failed to write pivot Markdown: {e}");
        }
        (out_csv, out_md)
    });

    // Check against baseline if requested
    if check && !baseline.is_empty() {
        let mut failed = false;
        let tol = 0.05; // 5% relative drop tolerable? No: fail if drop > 5%
        for (k, v) in &rows {
            let id = k.id();
            if let Some(&base_ratio) = baseline.get(&id) {
                let ratio = v.ratio();
                // Fail if ratio < base_ratio * (1 - tol)
                if ratio + f64::EPSILON < base_ratio * (1.0 - tol) {
                    eprintln!(
                        "Regression: {} ratio {:.6} < baseline {:.6} (>{:.0}% drop)",
                        id,
                        ratio,
                        base_ratio,
                        tol * 100.0
                    );
                    failed = true;
                }
            }
        }
        if failed {
            std::process::exit(1);
        }
    } else if check && baseline.is_empty() {
        eprintln!(
            "--check requested but baseline file not found: {}",
            baseline_path.display()
        );
        std::process::exit(2);
    }

    println!(
        "Compression report written to {} and {} (rows: {})",
        out_csv.display(),
        out_md.display(),
        rows.len()
    );
    if let Some((pivot_csv, pivot_md)) = pivot_paths {
        println!(
            "By-workload report written to {} and {}",
            pivot_csv.display(),
            pivot_md.display()
        );
    }
}
