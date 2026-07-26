use super::ChimpChunk;
use crate::common::Sample;
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
    // the ELF `beta_star` were reset along with the bit stream.
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
    assert_eq!(
        chunk
            .upsert_sample(replacement, DuplicatePolicy::KeepLast)
            .unwrap(),
        samples.len()
    );

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
/// value and rounded leading-zero count, and ELF's `beta_star`, plus the
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
/// case: erasable decimals (which move `beta_star`), values ELF stores raw,
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
