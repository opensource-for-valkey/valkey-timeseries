//! Correctness cases for the Gorilla stream: what a truncated stream decodes to, every XOR window the header can
//! express, and re-encoding into a stream whose last byte is only partly written.

use super::GorillaEncoder;
use crate::common::Sample;

fn s(timestamp: i64, value: f64) -> Sample {
    Sample { timestamp, value }
}

/// Encode `samples` one at a time and record the stream's bit length after each one, so a
/// test can say exactly which samples a byte-prefix of the stream still contains.
fn encode_with_offsets(samples: &[Sample]) -> (GorillaEncoder, Vec<usize>) {
    let mut enc = GorillaEncoder::new();
    let mut ends = Vec::with_capacity(samples.len());
    for sample in samples {
        enc.add_sample(sample).unwrap();
        ends.push(enc.stream_bit_len());
    }
    (enc, ends)
}

fn assert_bit_exact(got: &[Sample], want: &[Sample]) {
    assert_eq!(got.len(), want.len(), "sample count");
    for (i, (g, w)) in got.iter().zip(want).enumerate() {
        assert_eq!(g.timestamp, w.timestamp, "timestamp at {i}");
        assert_eq!(g.value.to_bits(), w.value.to_bits(), "value bits at {i}");
    }
}

/// A stream cut at any byte boundary decodes every sample it still holds, bit-exactly,
/// and then returns `Err` on the first sample it does not: never a wrong sample.
///
/// The reader tracks loaded bits exactly, so this is the granularity at which it can see
/// a truncation; a partly written final byte is not a state it can observe (the writer
/// commits whole bytes and `num_samples` is the terminator), which is why the last sample
/// comes from the encoder's metadata rather than the stream and is exempt below.
#[test]
fn truncated_stream_errs_instead_of_decoding_a_wrong_sample() {
    // Mixed widths: 2-bit samples, full 64-bit XORs, 72-bit timestamp escapes and both
    // signs of every dod bucket, so that byte boundaries fall inside every field kind.
    let mut samples = vec![
        s(1_000, 1.0),
        s(2_000, 1.0),
        s(3_000, 1.0),
        s(4_000, f64::MAX),
        s(4_010, f64::MIN),
        s(4_010 + (1 << 40), 0.5),
        s(4_020 + (1 << 40), 0.5),
        s(4_030 + (1 << 40), 0.5),
        s(4_030 + (1 << 40) + 300, 0.75),
    ];
    let mut ts = samples.last().unwrap().timestamp;
    let mut delta = 300i64;
    for (i, dod) in [
        0i64,
        5,
        -5,
        40,
        -40,
        300,
        -300,
        3_000,
        -3_000,
        200_000,
        -200_000,
        40_000_000,
        -40_000_000,
    ]
    .iter()
    .enumerate()
    {
        delta += dod;
        ts += delta;
        samples.push(s(ts, 1.0 + i as f64 / 7.0));
    }
    let n = samples.len();
    let (enc, ends) = encode_with_offsets(&samples);
    let full = enc.buf();

    for cut in 0..=full.len() {
        let mut truncated = enc.clone();
        truncated.truncate_stream_for_test(cut);
        assert_eq!(truncated.buf(), &full[..cut]);

        // Samples 0..n-1 come from the stream; sample n-1 from metadata, reachable only
        // once everything before it decoded.
        let decodable = ends[..n - 1].iter().filter(|&&e| e <= cut * 8).count();
        let want_ok = if decodable == n - 1 { n } else { decodable };

        let mut got = Vec::new();
        let mut err = None;
        for item in truncated.iter() {
            match item {
                Ok(sample) => got.push(sample),
                Err(e) => {
                    err = Some(e);
                    break;
                }
            }
        }

        assert_bit_exact(&got, &samples[..got.len()]);
        assert_eq!(
            got.len(),
            want_ok,
            "cut at {cut} bytes ({} bits): decoded {} samples, expected {want_ok}",
            cut * 8,
            got.len()
        );
        assert_eq!(
            err.is_some(),
            want_ok < n,
            "cut at {cut} bytes: an incomplete stream must end in Err"
        );
    }
}

/// An XOR whose significant bits sit at bit `63 - lead` down to `63 - lead - sig + 1`.
fn xor_pattern(lead: u32, sig: u32) -> u64 {
    debug_assert!(lead + sig <= 64 && sig >= 1);
    let trailing = 64 - lead - sig;
    let ones = if sig == 64 {
        u64::MAX
    } else {
        (1u64 << sig) - 1
    };
    ones << trailing
}

/// Every window the 5-bit leading count and 6-bit width can express, each written as a
/// fresh window and then reused by a second sample with the same shape, round-trips
/// bit-exactly, including the 64-wide payload that the width field encodes as 0.
#[test]
fn every_xor_window_round_trips() {
    let mut samples = vec![s(0, 0.0)];
    let mut prev_bits = 0u64;
    let mut ts = 0i64;
    for lead in 0..=31u32 {
        for sig in 1..=(64 - lead) {
            let xor = xor_pattern(lead, sig);
            // Two samples per window: one that opens it and one that reuses it. The
            // reuse sample flips only the lowest significant bit so it stays inside the
            // window; a `sig == 1` window has only that bit.
            let opened = prev_bits ^ xor;
            let reused = opened ^ (1u64 << (64 - lead - sig));
            for bits in [opened, reused] {
                ts += 1;
                samples.push(s(ts, f64::from_bits(bits)));
            }
            prev_bits = reused;
        }
    }
    // Sanity: the sweep exercised every pair, and the values are not all finite.
    assert_eq!(samples.len(), 1 + 2 * (32 * 64 - (0..32).sum::<usize>()));
    assert!(samples.iter().any(|x| x.value.is_nan()));

    let mut enc = GorillaEncoder::new();
    for sample in &samples {
        enc.add_sample(sample).unwrap();
    }
    let got: Vec<Sample> = enc.iter().map(|r| r.unwrap()).collect();
    assert_bit_exact(&got, &samples);

    // The same stream through the serialized form.
    let mut buf = Vec::new();
    enc.serialize(&mut buf);
    let loaded = GorillaEncoder::deserialize(&buf).unwrap();
    let got: Vec<Sample> = loaded.iter().map(|r| r.unwrap()).collect();
    assert_bit_exact(&got, &samples);
}

/// Appending to a deserialized encoder whose last byte is partial must continue the bit
/// stream where it stopped: the bytes and the decoded samples equal a single fresh encode.
#[test]
fn append_after_deserialize_into_a_partial_byte() {
    // Prefix lengths 1..=40 so the split lands at every bit position of the last byte
    // several times over, across the first/second/nth-sample paths.
    let all: Vec<Sample> = (0..60)
        .map(|i| s(1_000 + i * 997 + (i % 3) * 13, (i as f64).sin() * 100.0))
        .collect();

    let mut partial_splits = 0;
    for prefix in 1..=40 {
        let (head, _) = encode_with_offsets(&all[..prefix]);
        if head.stream_bit_len() % 8 != 0 {
            partial_splits += 1;
        }

        let mut buf = Vec::new();
        head.serialize(&mut buf);
        let mut resumed = GorillaEncoder::deserialize(&buf).unwrap();
        for sample in &all[prefix..] {
            resumed.add_sample(sample).unwrap();
        }

        let (fresh, _) = encode_with_offsets(&all);
        assert_eq!(
            resumed.buf(),
            fresh.buf(),
            "bytes differ after split at {prefix}"
        );
        assert_eq!(resumed.stream_bit_len(), fresh.stream_bit_len());
        assert_eq!(resumed.window_state(), fresh.window_state());

        let got: Vec<Sample> = resumed.iter().map(|r| r.unwrap()).collect();
        assert_bit_exact(&got, &all);
    }
    assert!(partial_splits > 20, "the fixture must split inside a byte");
}
