//! Multi-workload compression comparison for the ElfOnChimp codec.
//!
//! Run with:
//!
//! ```text
//! cargo test --release --features enable-system-alloc --lib \
//!     chimp::compression_tests -- --ignored --nocapture
//! ```
//!
//! Every codec sees the same samples as one continuous stream (the chunk
//! budget is set high enough that no encoding splits), so the numbers compare
//! codecs rather than chunk-restart overhead.

use super::compressor::{ChimpCompressor, ChimpDecompressor};
use crate::common::Sample;
use crate::series::chunks::{ChunkEncoding, ChunkOps};
use crate::tests::chunk_utils::encoded_size;
use crate::series::chunks::timeseries_chunk::TimeSeriesChunk;
use crate::tests::generators::{
    DATASET_SAMPLES, DatasetKey, benchmark_dataset_keys, dataset_seed, generate_dataset,
};

/// Samples per dataset. Half the benchmark matrix's [`DATASET_SAMPLES`]:
/// `Xor2Chunk` counts samples in a `u16`, so a single chunk cannot hold 64k
/// samples, and this report deliberately uses one chunk per codec.
const REPORT_SAMPLES: usize = DATASET_SAMPLES / 2;

/// Chunk budget large enough that no encoding starts a second chunk.
const SINGLE_STREAM_BUDGET: usize = 8 * 1024 * 1024;

/// Encodings compared against ElfOnChimp.
const ENCODINGS: [ChunkEncoding; 4] = [
    ChunkEncoding::Gorilla,
    ChunkEncoding::TsXor,
    ChunkEncoding::Xor2,
    ChunkEncoding::DeXor,
];

/// Bytes an uncompressed `(i64, f64)` sample takes.
const RAW_BYTES_PER_SAMPLE: f64 = 16.0;

fn elf_on_chimp_bytes(samples: &[Sample]) -> usize {
    let mut compressor = ChimpCompressor::new();
    for sample in samples {
        compressor
            .add(*sample)
            .expect("ElfOnChimp should accept the sample");
    }
    let count = compressor.count();
    let bytes = compressor.into_bytes();

    // Compression numbers are only meaningful if the stream decodes back.
    let decoded = ChimpDecompressor::new(&bytes, count)
        .collect()
        .expect("stream should decode");
    assert_eq!(decoded.len(), samples.len(), "sample count mismatch");
    for (actual, expected) in decoded.iter().zip(samples) {
        assert_eq!(actual.timestamp, expected.timestamp);
        assert_eq!(actual.value.to_bits(), expected.value.to_bits());
    }

    bytes.len()
}

fn chunk_bytes(encoding: ChunkEncoding, samples: &[Sample]) -> usize {
    let mut chunk = TimeSeriesChunk::new(encoding, SINGLE_STREAM_BUDGET);
    for sample in samples {
        chunk
            .add_sample(sample)
            .expect("sample should append to chunk");
    }
    assert_eq!(chunk.len(), samples.len(), "{encoding:?} dropped samples");
    encoded_size(&chunk)
}

#[test]
#[ignore = "reporting tool; run explicitly with --ignored --nocapture"]
fn compression_report() {
    let keys: Vec<DatasetKey> = benchmark_dataset_keys();

    println!(
        "\nElfOnChimp vs. existing encodings — {REPORT_SAMPLES} samples per dataset, \
         one continuous stream per codec.\nFigures are bytes per sample (lower is better); \
         `ratio` is {RAW_BYTES_PER_SAMPLE:.0}B/sample raw over ElfOnChimp.\n"
    );

    print!("{:<24}", "workload/ts_model");
    print!("{:>12}", "elf_chimp");
    for encoding in ENCODINGS {
        print!("{:>12}", encoding.name());
    }
    println!("{:>10}{:>14}", "ratio", "best");

    let mut elf_total = 0usize;
    let mut other_totals = [0usize; ENCODINGS.len()];
    let mut sample_total = 0usize;
    let mut elf_wins = 0usize;

    for key in &keys {
        let samples = generate_dataset(*key, REPORT_SAMPLES, dataset_seed(*key));
        let elf = elf_on_chimp_bytes(&samples);
        let others: Vec<usize> = ENCODINGS
            .iter()
            .map(|encoding| chunk_bytes(*encoding, &samples))
            .collect();

        elf_total += elf;
        sample_total += samples.len();
        for (total, bytes) in other_totals.iter_mut().zip(&others) {
            *total += *bytes;
        }

        let per_sample = |bytes: usize| bytes as f64 / samples.len() as f64;
        let (best_idx, best_bytes) = others
            .iter()
            .enumerate()
            .min_by_key(|(_, bytes)| **bytes)
            .map(|(i, bytes)| (i, *bytes))
            .expect("at least one comparison encoding");
        let best = if elf <= best_bytes {
            elf_wins += 1;
            "elf_chimp"
        } else {
            ENCODINGS[best_idx].name()
        };

        print!("{:<24}", key.id());
        print!("{:>12.3}", per_sample(elf));
        for bytes in &others {
            print!("{:>12.3}", per_sample(*bytes));
        }
        println!(
            "{:>10.2}{:>14}",
            RAW_BYTES_PER_SAMPLE / per_sample(elf),
            best
        );
    }

    let per_sample = |bytes: usize| bytes as f64 / sample_total as f64;
    print!("{:<24}", "OVERALL");
    print!("{:>12.3}", per_sample(elf_total));
    for total in &other_totals {
        print!("{:>12.3}", per_sample(*total));
    }
    println!(
        "{:>10.2}   elf best in {elf_wins}/{}",
        RAW_BYTES_PER_SAMPLE / per_sample(elf_total),
        keys.len()
    );
    println!();
}
