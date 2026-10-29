//! Fixed-sample Gorilla A/B measurements, including large absolute counters.
//!
//! Build each revision, copy the binary, then alternate runs of the binaries:
//! `cargo run --release --features enable-system-alloc,test-utils --bin gorilla_report --
//! --out target/bench-reports/gorilla.csv --samples 1000 --iterations 300`.
//! Times are local chunk microbenchmarks, not server performance measurements.

use std::fs::File;
use std::hint::black_box;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::time::Instant;

use get_size2::GetSize;
use valkey_timeseries::common::Sample;
use valkey_timeseries::series::chunks::{Chunk, ChunkOps, GorillaChunk};
use valkey_timeseries::tests::generators::{
    DatasetKey, TimestampModel, ValueWorkload, dataset_seed, generate_dataset,
};

fn median<T>(iterations: usize, mut op: impl FnMut() -> T) -> f64 {
    for _ in 0..50 {
        black_box(op());
    }
    let mut times = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let start = Instant::now();
        let result = op();
        times.push(start.elapsed().as_nanos());
        // Drop allocations after stopping the timer, consistently across revisions.
        black_box(result);
    }
    times.sort_unstable();
    times[times.len() / 2] as f64
}

fn build(samples: &[Sample]) -> GorillaChunk {
    // Every revision must process all samples, regardless of its compression ratio.
    let mut chunk = GorillaChunk::with_max_size(samples.len().saturating_mul(24).max(4096));
    chunk.set_data(samples).expect("encode");
    chunk
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut out = PathBuf::from("target/bench-reports/gorilla.csv");
    let mut count = 1000;
    let mut iterations = 300;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--help" || arg == "-h" {
            println!("gorilla_report [--out FILE] [--samples N>=2] [--iterations N>0]");
            return Ok(());
        }
        let value = args.next().ok_or("expected a flag value")?;
        match arg.as_str() {
            "--out" => out = value.into(),
            "--samples" => count = value.parse()?,
            "--iterations" => iterations = value.parse()?,
            _ => return Err(format!("unknown flag: {arg}").into()),
        }
    }
    if count < 2 || iterations == 0 {
        return Err("--samples must be >= 2 and --iterations must be positive".into());
    }
    if let Some(parent) = out.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)?;
    }
    let mut writer = BufWriter::new(File::create(out)?);
    writeln!(
        writer,
        "workload,ts_model,value_offset,seed,samples,bytes,memory_bytes,encode_bulk_ns,encode_append_ns,decode_ns,range_ns,mid_10pct_ns,tail_10pct_ns"
    )?;
    for &workload in ValueWorkload::workloads() {
        for &ts in TimestampModel::all() {
            let offsets: &[f64] = if workload == ValueWorkload::Counter {
                &[
                    0.0,
                    (1u64 << 30) as f64,
                    (1u64 << 40) as f64,
                    (1u64 << 50) as f64,
                ]
            } else {
                &[0.0]
            };
            for &offset in offsets {
                let key = DatasetKey::new(workload, ts);
                let seed = dataset_seed(key);
                let mut samples = generate_dataset(key, count, seed);
                for sample in &mut samples {
                    sample.value += offset;
                }
                let first = samples[0].timestamp;
                let last = samples[count - 1].timestamp;
                let span = last - first;
                let chunk = build(&samples);
                // Validate the entire persisted stream before timing it, including after
                // reloading and appending (the previous final sample must now decode).
                let mut serialized = Vec::new();
                chunk.serialize(&mut serialized);
                let mut restored = GorillaChunk::deserialize(&serialized).expect("deserialize");
                let extra = Sample::new(last + 1000, samples[count - 1].value + 1.0);
                restored
                    .add_sample(&extra)
                    .expect("append after deserialize");
                let decoded: Vec<_> = restored.iter().collect();
                assert_eq!(decoded.len(), count + 1);
                for (got, want) in decoded.iter().zip(samples.iter().chain([&extra])) {
                    assert_eq!(got.timestamp, want.timestamp);
                    assert_eq!(got.value.to_bits(), want.value.to_bits());
                }
                assert_eq!(restored.len(), count + 1);
                let bulk = median(iterations, || build(black_box(&samples)));
                let append = median(iterations, || {
                    let mut c = GorillaChunk::with_max_size(count.saturating_mul(24).max(4096));
                    for sample in black_box(&samples) {
                        c.add_sample(sample).expect("append");
                    }
                    c
                });
                let decode = median(iterations, || {
                    chunk.iter().for_each(|s| {
                        black_box((s.timestamp, s.value));
                    });
                });
                let range = median(iterations, || chunk.get_range(first, last).expect("range"));
                let mid = median(iterations, || {
                    chunk
                        .range_iter(first + span * 45 / 100, first + span * 55 / 100)
                        .for_each(|s| {
                            black_box((s.timestamp, s.value));
                        });
                });
                let tail = median(iterations, || {
                    chunk
                        .range_iter(first + span * 90 / 100, last)
                        .for_each(|s| {
                            black_box((s.timestamp, s.value));
                        });
                });
                writeln!(
                    writer,
                    "{},{},{offset},{seed},{count},{},{},{bulk:.0},{append:.0},{decode:.0},{range:.0},{mid:.0},{tail:.0}",
                    workload.id(),
                    ts.id(),
                    chunk.size(),
                    chunk.get_size()
                )?;
            }
        }
    }
    Ok(())
}
