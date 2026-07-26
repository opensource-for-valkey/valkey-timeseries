use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use std::time::Duration;
use valkey_timeseries::common::Timestamp;
use valkey_timeseries::series::ValueFilter;
use valkey_timeseries::series::chunks::{ChunkEncoding, ChunkOps};

mod support;

use support::{
    CHUNK_SIZE_4K, CHUNK_SIZE_64K, DatasetKey, DatasetRegistry, TimestampModel, ValueWorkload,
    build_chunk, chunk_size_id,
};

/// Workloads trimmed to the scan-relevant set
const SCAN_WORKLOADS: &[ValueWorkload] = &[
    ValueWorkload::Constant,
    ValueWorkload::Drift,
    ValueWorkload::Noisy,
    ValueWorkload::Bursty,
];

fn encodings() -> [ChunkEncoding; 3] {
    [
        ChunkEncoding::Uncompressed,
        ChunkEncoding::Gorilla,
        ChunkEncoding::Xor2,
    ]
}

/// The two chunk sizes exercised in query_scan benches.
const SCAN_CHUNK_SIZES: [usize; 2] = [CHUNK_SIZE_4K, CHUNK_SIZE_64K];

/// Compute the six (label, start, end) selectivity cases for a chunk.
///
/// Cases per plan:
/// - `full`       : [first_ts, last_ts]
/// - `head_10pct` : first 10 % of the time span
/// - `mid_10pct`  : 10 % window centred at mid-chunk (stresses seek-to-offset)
/// - `tail_10pct` : last 10 % — "recent data" dashboard shape
/// - `point`      : single-timestamp hit at mid-chunk [mid, mid]
/// - `miss`       : range entirely before first_ts (early-out path)
fn selectivity_cases(
    first_ts: Timestamp,
    last_ts: Timestamp,
) -> [(&'static str, Timestamp, Timestamp); 6] {
    let span = last_ts - first_ts;
    let tenth = (span / 10).max(1);
    let mid = first_ts + span / 2;

    [
        ("full", first_ts, last_ts),
        ("head_10pct", first_ts, first_ts + tenth),
        ("mid_10pct", mid - tenth / 2, mid + tenth / 2),
        ("tail_10pct", last_ts - tenth, last_ts),
        ("point", mid, mid),
        // range before first_ts — exercises the early-out path
        ("miss", first_ts - span - 1, first_ts - 1),
    ]
}

/// `scan` group: `range_iter` latency across workloads, encodings,
/// chunk sizes, and selectivity positions.  No throughput annotation —
/// latency is the point.
fn bench_scan(c: &mut Criterion) {
    let registry = DatasetRegistry::new();
    let mut group = c.benchmark_group("scan");
    group.measurement_time(Duration::from_secs(2));

    for &workload in SCAN_WORKLOADS {
        let key = DatasetKey::new(workload, TimestampModel::Regular);
        let dataset = registry.dataset(key);

        for encoding in encodings() {
            for chunk_size in SCAN_CHUNK_SIZES {
                let chunk = build_chunk(encoding, chunk_size, dataset);
                let first_ts = chunk.first_timestamp();
                let last_ts = chunk.last_timestamp();

                for (case, start, end) in selectivity_cases(first_ts, last_ts) {
                    let bench_id = format!(
                        "{}/{}/{}/{}/{}",
                        encoding.name(),
                        workload.id(),
                        TimestampModel::Regular.id(),
                        chunk_size_id(chunk_size),
                        case,
                    );

                    group.bench_function(BenchmarkId::from_parameter(&bench_id), |b| {
                        b.iter(|| {
                            chunk.range_iter(start, end).for_each(|s| {
                                std::hint::black_box((s.timestamp, s.value));
                            });
                        });
                    });
                }
            }
        }
    }

    group.finish();
}

/// `scan_filtered` group: one `filtered_iter` (range_iter + value-filter
/// iterator) per encoding, `drift` workload only, `mid_10pct` selectivity,
/// at both 4 KiB and 64 KiB chunk sizes.  Measures the composed-iterator
/// overhead the command layer sees.
///
/// Value filter covers ~50 % of drift samples (drift stays in [95, 105];
/// we keep the central [97, 103] band).
fn bench_filtered_scan(c: &mut Criterion) {
    let registry = DatasetRegistry::new();
    let mut group = c.benchmark_group("scan_filtered");
    group.measurement_time(Duration::from_secs(2));

    let key = DatasetKey::new(ValueWorkload::Drift, TimestampModel::Regular);
    let dataset = registry.dataset(key);
    // ~50 % selectivity on drift values (95–105 range → keep 97–103).
    let value_filter = ValueFilter {
        min: 97.0,
        max: 103.0,
    };

    for encoding in encodings() {
        for chunk_size in SCAN_CHUNK_SIZES {
            let chunk = build_chunk(encoding, chunk_size, dataset);
            let first_ts = chunk.first_timestamp();
            let last_ts = chunk.last_timestamp();
            let span = last_ts - first_ts;
            let tenth = (span / 10).max(1);
            let mid = first_ts + span / 2;
            let start = mid - tenth / 2;
            let end = mid + tenth / 2;

            let bench_id = format!(
                "{}/{}/{}/{}",
                encoding.name(),
                ValueWorkload::Drift.id(),
                chunk_size_id(chunk_size),
                "mid_10pct",
            );

            group.bench_function(BenchmarkId::from_parameter(&bench_id), |b| {
                b.iter(|| {
                    chunk
                        .filtered_iter(start, end, None, Some(value_filter))
                        .for_each(|s| {
                            std::hint::black_box((s.timestamp, s.value));
                        });
                });
            });
        }
    }

    group.finish();
}

criterion_group!(benches, bench_scan, bench_filtered_scan);
criterion_main!(benches);
