//! Golden Gorilla streams: the encoder's output, pinned byte for byte.
//!
//! A foreign load accepts a compressed chunk only if re-encoding its samples with the current
//! encoder reproduces the stored stream exactly (`XorChunk::verify_stream`). So any change to
//! what the encoder writes, not only to the grammar, makes every chunk written before it fail
//! `RESTORE`, `MIGRATE` and slot import. These fixtures turn such a change into a failing test.
//! See `fixtures/README.md`; the Chimp equivalent is in `chimp_chunk_tests.rs`.

use super::gorilla_encoder::GorillaEncoder;
use crate::common::Sample;
use crate::tests::generators::{
    DatasetKey, TimestampModel, ValueWorkload, dataset_seed, generate_dataset,
};

const SAMPLES: usize = 32;

const FIXTURES: [(&str, ValueWorkload, &str); 3] = [
    (
        "gorilla-drift_q2.txt",
        ValueWorkload::DriftQuantized,
        include_str!("fixtures/gorilla-drift_q2.txt"),
    ),
    (
        "gorilla-counter.txt",
        ValueWorkload::Counter,
        include_str!("fixtures/gorilla-counter.txt"),
    ),
    (
        "gorilla-noisy.txt",
        ValueWorkload::Noisy,
        include_str!("fixtures/gorilla-noisy.txt"),
    ),
];

fn dataset(workload: ValueWorkload) -> Vec<Sample> {
    let key = DatasetKey::new(workload, TimestampModel::Jitter);
    generate_dataset(key, SAMPLES, dataset_seed(key))
}

fn encode(samples: &[Sample]) -> GorillaEncoder {
    let mut encoder = GorillaEncoder::new();
    for sample in samples {
        encoder.add_sample(sample).unwrap();
    }
    encoder
}

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn from_hex(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect()
}

/// `(serialized bytes, samples)` from a fixture file.
fn parse(fixture: &str) -> (Vec<u8>, Vec<Sample>) {
    let mut lines = fixture.lines();
    let wire = from_hex(lines.next().unwrap());
    let samples = lines
        .map(|line| {
            let (ts, bits) = line.split_once(' ').unwrap();
            Sample::new(
                ts.parse().unwrap(),
                f64::from_bits(u64::from_str_radix(bits, 16).unwrap()),
            )
        })
        .collect();
    (wire, samples)
}

fn decode(encoder: &GorillaEncoder) -> Vec<Sample> {
    encoder.iter().map(Result::unwrap).collect()
}

fn assert_bit_exact(got: &[Sample], want: &[Sample]) {
    assert_eq!(got.len(), want.len(), "sample count");
    for (i, (g, w)) in got.iter().zip(want).enumerate() {
        assert_eq!(g.timestamp, w.timestamp, "timestamp {i}");
        assert_eq!(g.value.to_bits(), w.value.to_bits(), "value {i}");
    }
}

/// Regenerates the fixtures from the current encoder. Run only for a deliberate format change:
/// `cargo test --features enable-system-alloc write_gorilla_fixtures -- --ignored`.
#[test]
#[ignore]
fn write_gorilla_fixtures() {
    let dir = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/series/chunks/gorilla/fixtures"
    );
    for (name, workload, _) in FIXTURES {
        let samples = dataset(workload);
        let mut wire = Vec::new();
        encode(&samples).serialize(&mut wire);
        let mut out = to_hex(&wire);
        out.push('\n');
        for s in &samples {
            out.push_str(&format!("{} {:016x}\n", s.timestamp, s.value.to_bits()));
        }
        std::fs::write(format!("{dir}/{name}"), out).unwrap();
    }
}

/// The fixture samples are still the generator's, so the encode check below compares like
/// with like.
#[test]
fn fixture_samples_match_the_generator() {
    for (name, workload, fixture) in FIXTURES {
        let (_, samples) = parse(fixture);
        assert_eq!(samples.len(), SAMPLES, "{name}");
        assert_bit_exact(&samples, &dataset(workload));
    }
}

/// The golden streams decode bit-exactly, accept appends, and survive another
/// serialize/deserialize; a fresh encode reproduces them byte for byte.
#[test]
fn golden_streams_load_append_and_reencode() {
    for (name, _, fixture) in FIXTURES {
        let (wire, mut expected) = parse(fixture);

        // Encoder: the same samples give the same bytes.
        let mut fresh = Vec::new();
        encode(&expected).serialize(&mut fresh);
        assert_eq!(
            to_hex(&fresh),
            to_hex(&wire),
            "{name}: encoder output changed"
        );

        // Decoder: the bytes give the samples.
        let mut loaded = GorillaEncoder::deserialize(&wire).unwrap();
        assert_bit_exact(&decode(&loaded), &expected);

        // Append past the loaded state: a repeat, a small change, an integer, a large value, a
        // sign change, a NaN, and timestamp deltas that reach the wider delta-of-delta tiers.
        let last = *expected.last().unwrap();
        let extra = [
            (1_000, last.value),
            (1_000, ((last.value * 100.0).round() + 25.0) / 100.0),
            (1_013, 1_000_000.0),
            (70_000, (1u64 << 50) as f64 + 7.0),
            (1_000, (1u64 << 50) as f64 + 14.0),
            (5_000_000, -12.34),
            (1, f64::NAN),
            (1_000, 12.345),
        ];
        let mut ts = last.timestamp;
        for (delta, value) in extra {
            ts += delta;
            let sample = Sample::new(ts, value);
            loaded.add_sample(&sample).unwrap();
            expected.push(sample);
        }

        let mut saved = Vec::new();
        loaded.serialize(&mut saved);
        let resumed = GorillaEncoder::deserialize(&saved).unwrap();
        assert_bit_exact(&decode(&resumed), &expected);

        // A reloaded encoder writes what one that never stopped would have.
        let mut uninterrupted = Vec::new();
        encode(&expected).serialize(&mut uninterrupted);
        assert_eq!(
            to_hex(&saved),
            to_hex(&uninterrupted),
            "{name}: resumed stream"
        );
    }
}
