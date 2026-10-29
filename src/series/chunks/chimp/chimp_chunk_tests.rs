use super::ChimpChunk;
use crate::common::Sample;
use crate::common::encoding::{
    try_read_f64_le, try_read_signed_varint, try_read_uvarint, write_f64_le, write_signed_varint,
    write_uvarint,
};
use crate::error::TsdbError;
use crate::series::DuplicatePolicy;
use crate::series::chunks::chunk::Chunk;
use crate::series::chunks::{ChunkOps, GorillaChunk};
use crate::tests::generators::DataGenerator;
use std::time::Duration;

fn generate_samples(count: usize) -> Vec<Sample> {
    DataGenerator::builder()
        .samples(count)
        .start(1000)
        .decimal_digits(3)
        .interval(Duration::from_millis(1000))
        .build()
        .generate()
}

fn decompress(chunk: &ChimpChunk) -> Vec<Sample> {
    chunk.iter().collect()
}

fn filled_chunk(count: usize) -> (ChimpChunk, Vec<Sample>) {
    let samples = generate_samples(count);
    let mut chunk = ChimpChunk::with_max_size(16384);
    for sample in samples.iter() {
        chunk.add_sample(sample).unwrap();
    }
    (chunk, samples)
}

#[test]
fn test_chunk_compress() {
    let (chunk, data) = filled_chunk(1000);
    assert_eq!(chunk.len(), data.len());
    assert_eq!(chunk.first_timestamp(), data[0].timestamp);
    assert_eq!(chunk.last_timestamp(), data[data.len() - 1].timestamp);
    assert_eq!(chunk.last_value(), data[data.len() - 1].value);
}

#[test]
fn test_roundtrip_is_bit_exact() {
    let (chunk, data) = filled_chunk(1000);
    let decoded = decompress(&chunk);
    assert_eq!(decoded.len(), data.len());
    for (i, (expected, actual)) in data.iter().zip(decoded.iter()).enumerate() {
        assert_eq!(expected.timestamp, actual.timestamp, "timestamp {i}");
        assert_eq!(
            expected.value.to_bits(),
            actual.value.to_bits(),
            "value {i}: expected {} got {}",
            expected.value,
            actual.value
        );
    }
}

#[test]
fn test_clear() {
    let (mut chunk, data) = filled_chunk(500);
    assert_eq!(chunk.len(), data.len());

    chunk.clear();
    assert_eq!(chunk.len(), 0);
    assert_eq!(chunk.first_timestamp(), 0);
    assert_eq!(chunk.last_timestamp(), 0);

    // A cleared chunk must be usable again, which means the Chimp XOR state and
    // the ELF `last_alpha` were reset along with the bit stream.
    let more = generate_samples(50);
    for sample in more.iter() {
        chunk.add_sample(sample).unwrap();
    }
    assert_eq!(decompress(&chunk), more);
}

#[test]
fn test_upsert() {
    for chunk_size in (64..8192).step_by(512) {
        const SAMPLE_COUNT: usize = 200;
        let samples = generate_samples(SAMPLE_COUNT);
        let mut chunk = ChimpChunk::with_max_size(chunk_size);

        let sample_count = samples.len();
        for sample in samples.into_iter() {
            chunk
                .upsert_sample(sample, DuplicatePolicy::KeepLast)
                .unwrap();
        }
        assert_eq!(chunk.len(), sample_count);
    }
}

#[test]
fn test_upsert_out_of_order() {
    let samples = generate_samples(100);
    let mut chunk = ChimpChunk::with_max_size(16384);

    // Insert in reverse so every upsert has to re-encode from scratch.
    for sample in samples.iter().rev() {
        chunk
            .upsert_sample(*sample, DuplicatePolicy::KeepLast)
            .unwrap();
    }

    assert_eq!(decompress(&chunk), samples);
}

#[test]
fn test_upsert_duplicate_applies_policy() {
    let samples = generate_samples(10);
    let mut chunk = ChimpChunk::with_max_size(16384);
    for sample in &samples {
        chunk.add_sample(sample).unwrap();
    }

    let replacement = Sample {
        timestamp: samples[4].timestamp,
        value: 12345.678,
    };
    assert!(
        !chunk
            .upsert_sample(replacement, DuplicatePolicy::KeepLast)
            .unwrap(),
        "replacing an existing timestamp is an update, not an insert"
    );
    assert_eq!(chunk.len(), samples.len());

    let decoded = decompress(&chunk);
    assert_eq!(decoded.len(), samples.len());
    assert_eq!(decoded[4], replacement);
}

#[test]
fn test_split() {
    const COUNT: usize = 500;
    let (mut chunk, samples) = filled_chunk(COUNT);

    let mid = samples.len() / 2;
    let right = chunk.split().unwrap();
    assert_eq!(chunk.len(), mid);
    assert_eq!(right.len(), mid);

    let (left_samples, right_samples) = samples.split_at(mid);
    assert_eq!(decompress(&right), right_samples);
    assert_eq!(decompress(&chunk), left_samples);
}

#[test]
fn test_split_odd() {
    const COUNT: usize = 51;
    let (mut chunk, samples) = filled_chunk(COUNT);

    let mid = samples.len() / 2;
    let right = chunk.split().unwrap();
    assert_eq!(chunk.len(), mid);
    assert_eq!(right.len(), mid + 1);

    let (left_samples, right_samples) = samples.split_at(mid);
    assert_eq!(decompress(&right), right_samples);
    assert_eq!(decompress(&chunk), left_samples);
}

#[test]
fn test_iter() {
    let mut chunk = ChimpChunk::default();
    let data = generate_samples(1000);
    chunk.set_data(&data).unwrap();
    assert_eq!(chunk.iter().collect::<Vec<_>>(), data);
}

#[test]
fn test_range_iter() {
    let (chunk, samples) = filled_chunk(200);
    let start = samples[50].timestamp;
    let end = samples[149].timestamp;

    let got: Vec<_> = chunk.range_iter(start, end).collect();
    assert_eq!(got, &samples[50..150]);
}

#[test]
fn test_get_range() {
    let (chunk, samples) = filled_chunk(200);
    let got = chunk
        .get_range(samples[10].timestamp, samples[19].timestamp)
        .unwrap();
    assert_eq!(got, &samples[10..20]);
}

#[test]
fn test_remove_range() {
    let (mut chunk, samples) = filled_chunk(100);

    let mid = samples.len() / 2;
    let start_ts = samples[0].timestamp;
    let mid_ts = samples[mid].timestamp;

    // The range is inclusive, so this removes mid + 1 samples.
    assert_eq!(chunk.remove_range(start_ts, mid_ts).unwrap(), mid + 1);
    assert_eq!(chunk.iter().collect::<Vec<_>>(), &samples[mid + 1..]);

    let end_ts = samples[samples.len() - 1].timestamp;
    assert_eq!(chunk.remove_range(mid_ts, end_ts).unwrap(), mid - 1);
    assert!(chunk.is_empty());
}

#[test]
fn test_remove_range_no_overlap() {
    let (mut chunk, samples) = filled_chunk(100);

    let start_ts = samples[samples.len() - 1].timestamp + 1;
    assert_eq!(chunk.remove_range(start_ts, start_ts + 1000).unwrap(), 0);
    assert_eq!(chunk.iter().collect::<Vec<_>>(), samples);
}

#[test]
fn test_merge_samples_appends_and_interleaves() {
    let samples = generate_samples(200);
    let (head, tail) = samples.split_at(100);

    // Pure append.
    let mut chunk = ChimpChunk::with_max_size(16384);
    chunk.merge_samples(head, None).unwrap();
    chunk.merge_samples(tail, None).unwrap();
    assert_eq!(decompress(&chunk), samples);

    // Overlapping merge forces the full re-encode path.
    let mut chunk = ChimpChunk::with_max_size(16384);
    chunk.merge_samples(&samples[100..], None).unwrap();
    chunk
        .merge_samples(&samples[..100], Some(DuplicatePolicy::KeepLast))
        .unwrap();
    assert_eq!(decompress(&chunk), samples);
}

#[test]
fn test_serialize_roundtrip() {
    let (chunk, _) = filled_chunk(500);

    let mut buf = Vec::new();
    chunk.serialize(&mut buf);
    let restored = ChimpChunk::deserialize(&buf).unwrap();

    assert_eq!(restored, chunk);
    assert_eq!(decompress(&restored), decompress(&chunk));
}

/// The codec carries state across samples on both layers: Chimp's previous
/// value and rounded leading-zero count, and ELF's `last_alpha`, plus the
/// timestamp delta. If any of it is lost on the way through `serialize`,
/// samples appended afterwards decode as garbage — so a restored chunk must
/// produce the same stream as one that was never persisted.
#[test]
fn test_append_after_serialize_roundtrip() {
    for prefix_len in [1usize, 2, 3, 17, 200] {
        let samples = generate_samples(prefix_len + 100);
        let (prefix, suffix) = samples.split_at(prefix_len);

        let mut chunk = ChimpChunk::with_max_size(65536);
        for sample in prefix {
            chunk.add_sample(sample).unwrap();
        }

        let mut buf = Vec::new();
        chunk.serialize(&mut buf);
        let mut restored = ChimpChunk::deserialize(&buf).unwrap();

        for sample in suffix {
            restored.add_sample(sample).unwrap();
            chunk.add_sample(sample).unwrap();
        }

        assert_eq!(
            decompress(&restored),
            samples,
            "restored chunk diverged after appending to a {prefix_len}-sample prefix",
        );
        // The resumed encoder must also produce byte-identical output.
        assert_eq!(
            restored, chunk,
            "resumed stream differs from the unbroken one"
        );
    }
}

/// Same contract as above, but across value shapes that keep switching ELF
/// case: erasable decimals (which move `last_alpha`), values ELF stores raw,
/// and non-finite ones.
#[test]
fn test_append_after_serialize_roundtrip_mixed_cases() {
    let mut samples = Vec::new();
    for i in 0..120i64 {
        let value = match i % 4 {
            0 => 1.5 + (i % 10) as f64 * 0.1,
            1 => f64::from_bits(0x4000_0000_0000_0001 + i as u64 * 0x0003_1000_0000_0000),
            2 => 0.0,
            _ => (i as f64) * 1e-7,
        };
        samples.push(Sample {
            timestamp: 1000 + i * 1000,
            value,
        });
    }

    let (prefix, suffix) = samples.split_at(61);
    let mut chunk = ChimpChunk::with_max_size(65536);
    for sample in prefix {
        chunk.add_sample(sample).unwrap();
    }

    let mut buf = Vec::new();
    chunk.serialize(&mut buf);
    let mut restored = ChimpChunk::deserialize(&buf).unwrap();
    for sample in suffix {
        restored.add_sample(sample).unwrap();
        chunk.add_sample(sample).unwrap();
    }

    let decoded = decompress(&restored);
    assert_eq!(decoded.len(), samples.len());
    for (i, (expected, actual)) in samples.iter().zip(decoded.iter()).enumerate() {
        assert_eq!(expected.timestamp, actual.timestamp, "timestamp {i}");
        assert_eq!(
            expected.value.to_bits(),
            actual.value.to_bits(),
            "value {i}"
        );
    }
    assert_eq!(restored, chunk);
}

#[test]
fn test_deserialize_rejects_corrupt_codec_state() {
    let (chunk, _) = filled_chunk(50);
    let mut buf = Vec::new();
    chunk.serialize(&mut buf);

    // Truncation at every offset must produce an error, not a panic.
    for cut in 0..buf.len() {
        let _ = ChimpChunk::deserialize(&buf[..cut]);
    }

    // Corrupting individual header bytes must not panic either.
    for i in 0..buf.len().min(40) {
        let mut corrupt = buf.clone();
        corrupt[i] = corrupt[i].wrapping_add(0x5b);
        let _ = ChimpChunk::deserialize(&corrupt);
    }

    // Codec state the encoder could never have written is rejected on load, so a later
    // append cannot write a stream the decoder misreads.
    let header = CodecHeader::parse(&buf);
    assert_eq!(ChimpChunk::deserialize(&header.write()).unwrap(), chunk);
    assert_ne!(header.stored_lz, 64, "the fixture must have an open window");
    let with = |last_alpha, stored_lz, stored_tz| CodecHeader {
        last_alpha,
        stored_lz,
        stored_tz,
        ..header.clone()
    };
    // Each case breaks one field; the same rewrite with valid values loads.
    let alpha = header.last_alpha;
    // (32..=54 are extended windows.)
    for (a, lz, tz) in [
        (alpha, 0, 0),
        (-1, 0, 0),
        (15, 24, 39),
        (alpha, 32, 31),
        (alpha, 54, 9),
        (alpha, 64, 0),
    ] {
        assert!(
            ChimpChunk::deserialize(&with(a, lz, tz).write()).is_ok(),
            "{a} {lz} {tz}"
        );
    }
    let mut cases = vec![(-2, 0, 0), (16, 0, 0), (1 << 40, 0, 0)];
    cases.extend([-1, 1, 25, 63, 65, i64::from(i32::MAX), (1 << 32) + 8].map(|lz| (alpha, lz, 0)));
    cases.extend([-1, 64, (1 << 32) + 2].map(|tz| (alpha, 0, tz)));
    cases.extend([31, 55].map(|lz| (alpha, lz, 0)));
    cases.extend([(24, 40), (8, 56), (54, 10), (64, 1)].map(|(lz, tz)| (alpha, lz, tz)));
    for (a, lz, tz) in cases {
        let corrupt = with(a, lz, tz).write();
        assert!(
            matches!(
                ChimpChunk::deserialize(&corrupt),
                Err(TsdbError::ChunkDecoding)
            ),
            "last_alpha {a}, stored_lz {lz}, stored_tz {tz}"
        );
    }
}

/// The serialized Chimp chunk header, field by field, so a test can rewrite one.
#[derive(Debug, Clone)]
struct CodecHeader {
    max_size: u64,
    count: u64,
    first_timestamp: i64,
    last_timestamp: i64,
    last_delta: i64,
    last_value: f64,
    last_alpha: i64,
    stored_lz: i64,
    stored_tz: i64,
    stored_val: u64,
    first: u64,
    stream: Vec<u8>,
}

impl CodecHeader {
    fn parse(mut buf: &[u8]) -> Self {
        let buf = &mut buf;
        Self {
            max_size: try_read_uvarint(buf).ok().unwrap(),
            count: try_read_uvarint(buf).ok().unwrap(),
            first_timestamp: try_read_signed_varint(buf).ok().unwrap(),
            last_timestamp: try_read_signed_varint(buf).ok().unwrap(),
            last_delta: try_read_signed_varint(buf).ok().unwrap(),
            last_value: try_read_f64_le(buf).ok().unwrap(),
            last_alpha: try_read_signed_varint(buf).ok().unwrap(),
            stored_lz: try_read_signed_varint(buf).ok().unwrap(),
            stored_tz: try_read_signed_varint(buf).ok().unwrap(),
            stored_val: try_read_uvarint(buf).ok().unwrap(),
            first: try_read_uvarint(buf).ok().unwrap(),
            stream: buf.to_vec(),
        }
    }

    fn write(&self) -> Vec<u8> {
        let mut out = Vec::new();
        write_uvarint(&mut out, self.max_size);
        write_uvarint(&mut out, self.count);
        write_signed_varint(&mut out, self.first_timestamp);
        write_signed_varint(&mut out, self.last_timestamp);
        write_signed_varint(&mut out, self.last_delta);
        write_f64_le(&mut out, self.last_value);
        write_signed_varint(&mut out, self.last_alpha);
        write_signed_varint(&mut out, self.stored_lz);
        write_signed_varint(&mut out, self.stored_tz);
        write_uvarint(&mut out, self.stored_val);
        write_uvarint(&mut out, self.first);
        out.extend_from_slice(&self.stream);
        out
    }
}

#[test]
fn test_non_finite_and_edge_values() {
    let values = [
        0.0,
        -0.0,
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::MAX,
        f64::MIN,
        f64::MIN_POSITIVE,
        5e-324,
        1.0,
        -21.75,
        1e11,
        1e-20,
    ];
    let samples: Vec<Sample> = values
        .iter()
        .enumerate()
        .map(|(i, &value)| Sample {
            timestamp: 1000 + i as i64 * 10,
            value,
        })
        .collect();

    let mut chunk = ChimpChunk::with_max_size(16384);
    for sample in &samples {
        chunk.add_sample(sample).unwrap();
    }

    let decoded = decompress(&chunk);
    assert_eq!(decoded.len(), samples.len());
    for (expected, actual) in samples.iter().zip(decoded.iter()) {
        assert_eq!(expected.timestamp, actual.timestamp);
        assert_eq!(expected.value.to_bits(), actual.value.to_bits());
    }
}

#[test]
fn test_single_and_empty_chunk() {
    let empty = ChimpChunk::default();
    assert!(empty.is_empty());
    assert!(decompress(&empty).is_empty());

    let mut one = ChimpChunk::default();
    let sample = Sample {
        timestamp: 1234,
        value: 21.5,
    };
    one.add_sample(&sample).unwrap();
    assert_eq!(decompress(&one), vec![sample]);
    assert_eq!(one.first_timestamp(), 1234);
    assert_eq!(one.last_timestamp(), 1234);
}

#[test]
fn test_is_full_respects_max_size() {
    let mut chunk = ChimpChunk::with_max_size(256);
    assert!(!chunk.is_full());

    let samples = generate_samples(4096);
    let mut added = 0;
    for sample in &samples {
        if chunk.is_full() {
            break;
        }
        chunk.add_sample(sample).unwrap();
        added += 1;
    }

    assert!(chunk.is_full());
    assert!(added > 0 && added < samples.len());
    assert_eq!(decompress(&chunk), &samples[..added]);
}

/// The ELF layer exists to beat plain binary XOR coders on decimal-origin
/// data. If it stops doing that on a three-decimal-digit series, something has
/// regressed.
#[test]
fn test_beats_gorilla_on_decimal_data() {
    let samples = generate_samples(4096);

    let mut chimp = ChimpChunk::with_max_size(1 << 20);
    let mut gorilla = GorillaChunk::with_max_size(1 << 20);
    for sample in &samples {
        chimp.add_sample(sample).unwrap();
        gorilla.add_sample(sample).unwrap();
    }

    let chimp_bytes = chimp.encoder.bytes().len();
    let gorilla_bytes = gorilla.encoder.buf().len();
    assert!(
        chimp_bytes < gorilla_bytes,
        "chimp used {chimp_bytes} bytes vs gorilla's {gorilla_bytes} on 3-decimal-digit data",
    );
}

mod allocation {
    use super::*;
    use get_size2::GetSize;

    /// A sealed chunk's allocation stays within `max_size` plus a few bytes of slack, and
    /// filling it is still geometric. See the Gorilla twin of this test for the history.
    #[test]
    fn sealed_chunk_allocation_is_capped_at_max_size() {
        for max_size in [1024usize, 4096, 16384] {
            let mut chunk = ChimpChunk::with_max_size(max_size);
            let samples = generate_samples(max_size * 2);
            let mut reallocations = 0;
            let mut cap = 0;
            for sample in samples.iter() {
                if chunk.is_full() {
                    break;
                }
                chunk.add_sample(sample).unwrap();
                let c = chunk.encoder.get_heap_size();
                if c != cap {
                    reallocations += 1;
                    cap = c;
                }
            }
            assert!(
                chunk.is_full(),
                "{max_size}: not full after {} samples",
                samples.len()
            );
            let heap = chunk.encoder.get_heap_size();
            assert!(
                heap <= max_size + 32,
                "{max_size}-byte chunk holds a {heap}-byte allocation"
            );
            assert!(heap >= chunk.data_size());
            assert!(
                reallocations <= 16,
                "{max_size}-byte chunk reallocated {reallocations} times"
            );
        }
    }

    /// The cap survives a serialize/deserialize round trip and a rebuild (split).
    #[test]
    fn loaded_and_rebuilt_chunks_keep_the_cap() {
        let max_size = 1024;
        let mut chunk = ChimpChunk::with_max_size(max_size);
        for sample in generate_samples(40).iter() {
            chunk.add_sample(sample).unwrap();
        }
        let mut buf = Vec::new();
        chunk.serialize(&mut buf);
        let mut loaded = ChimpChunk::deserialize(&buf).unwrap();
        for sample in generate_samples(8000).iter().skip(40) {
            if loaded.is_full() {
                break;
            }
            loaded.add_sample(sample).unwrap();
        }
        assert!(loaded.is_full());
        assert!(loaded.encoder.get_heap_size() <= max_size + 32);

        let right = loaded.split().unwrap();
        assert!(loaded.encoder.get_heap_size() <= max_size + 32);
        assert!(right.encoder.get_heap_size() <= max_size + 32);
    }
}

/// Golden streams for the current Chimp grammar. Each fixture pins the encoder (a fresh
/// encode of its samples must reproduce its bytes) and the decoder (its bytes must decode
/// to its samples), so any change to the Chimp grammar shows up here and needs regenerated
/// fixtures (and, once the module is released, a new encoding version).
mod fixtures {
    use super::super::ChimpCompressor;
    use crate::common::Sample;
    use crate::tests::generators::{
        DatasetKey, TimestampModel, ValueWorkload, dataset_seed, generate_dataset,
    };

    const SAMPLES: usize = 32;

    const FIXTURES: [(&str, ValueWorkload, &str); 3] = [
        (
            "chimp-drift_q2.txt",
            ValueWorkload::DriftQuantized,
            include_str!("fixtures/chimp-drift_q2.txt"),
        ),
        (
            "chimp-counter.txt",
            ValueWorkload::Counter,
            include_str!("fixtures/chimp-counter.txt"),
        ),
        (
            "chimp-noisy.txt",
            ValueWorkload::Noisy,
            include_str!("fixtures/chimp-noisy.txt"),
        ),
    ];

    fn dataset(workload: ValueWorkload) -> Vec<Sample> {
        let key = DatasetKey::new(workload, TimestampModel::Jitter);
        generate_dataset(key, SAMPLES, dataset_seed(key))
    }

    fn encode(samples: &[Sample]) -> ChimpCompressor {
        let mut compressor = ChimpCompressor::new();
        for sample in samples {
            compressor.add(*sample).unwrap();
        }
        compressor
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

    fn decode(compressor: &ChimpCompressor) -> Vec<Sample> {
        compressor.iter().map(Result::unwrap).collect()
    }

    fn assert_bit_exact(got: &[Sample], want: &[Sample]) {
        assert_eq!(got.len(), want.len(), "sample count");
        for (i, (g, w)) in got.iter().zip(want).enumerate() {
            assert_eq!(g.timestamp, w.timestamp, "timestamp {i}");
            assert_eq!(g.value.to_bits(), w.value.to_bits(), "value {i}");
        }
    }

    /// Regenerates the fixtures from the current encoder. Run only for a deliberate format
    /// change:
    /// `cargo test --features enable-system-alloc write_chimp_fixtures -- --ignored`.
    #[test]
    #[ignore]
    fn write_chimp_fixtures() {
        let dir = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/series/chunks/chimp/fixtures"
        );
        for (name, workload, _) in FIXTURES {
            let samples = dataset(workload);
            let compressor = encode(&samples);
            let mut wire = Vec::new();
            compressor.serialize(&mut wire);
            let mut out = to_hex(&wire);
            out.push('\n');
            for s in &samples {
                out.push_str(&format!("{} {:016x}\n", s.timestamp, s.value.to_bits()));
            }
            std::fs::write(format!("{dir}/{name}"), out).unwrap();
            eprintln!("{name}: {:?}", compressor.stats);
        }
    }

    /// The fixture samples are still the generator's, so the encode check below compares
    /// like with like.
    #[test]
    fn fixture_samples_match_the_generator() {
        for (name, workload, fixture) in FIXTURES {
            let (_, samples) = parse(fixture);
            let generated = dataset(workload);
            assert_eq!(samples.len(), SAMPLES, "{name}");
            assert_bit_exact(&samples, &generated);
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
            let mut loaded = ChimpCompressor::deserialize(&wire).unwrap();
            assert_bit_exact(&decode(&loaded), &expected);

            // Append past the loaded state: a repeat, a quantized step, an integer, a large
            // counter, a sign change, a NaN, and timestamps that reach the wider dod tiers.
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
                loaded.add(sample).unwrap();
                expected.push(sample);
            }

            let mut saved = Vec::new();
            loaded.serialize(&mut saved);
            let resumed = ChimpCompressor::deserialize(&saved).unwrap();
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
}
