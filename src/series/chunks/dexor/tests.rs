use super::*;
use proptest::prelude::*;

const MODES: [DeXorMode; 4] = [
    DeXorMode::Native,
    DeXorMode::Skippable { after: 3 },
    DeXorMode::Buffered { bits: 2 },
    DeXorMode::Buffered { bits: 4 },
];

fn configs() -> Vec<DeXorConfig> {
    MODES
        .iter()
        .flat_map(|&mode| [1u32, 8].map(|rho| DeXorConfig { mode, rho }))
        .collect()
}

/// Round-trips `values` under every mode and asserts bit-exact recovery.
#[track_caller]
fn assert_roundtrip(values: &[f64]) {
    for config in configs() {
        let bytes = DeXorEncoder::encode_slice(values, config);
        let decoded = DeXorDecoder::decode_all(&bytes, values.len(), config)
            .unwrap_or_else(|e| panic!("decode failed for {config:?}: {e}"));

        assert_eq!(
            decoded.len(),
            values.len(),
            "length mismatch for {config:?}"
        );
        for (i, (&expected, &actual)) in values.iter().zip(decoded.iter()).enumerate() {
            assert_eq!(
                expected.to_bits(),
                actual.to_bits(),
                "sample {i} mismatch for {config:?}: expected {expected} got {actual}",
            );
        }
    }
}

/// Bits per sample under the default configuration.
fn bits_per_sample(values: &[f64]) -> f64 {
    let bytes = DeXorEncoder::encode_slice(values, DeXorConfig::default());
    (bytes.len() * 8) as f64 / values.len() as f64
}

#[test]
fn empty_stream() {
    let bytes = DeXorEncoder::encode_slice(&[], DeXorConfig::default());
    assert!(bytes.is_empty());
    assert!(
        DeXorDecoder::decode_all(&bytes, 0, DeXorConfig::default())
            .unwrap()
            .is_empty()
    );
}

#[test]
fn single_value() {
    for value in [0.0, 1.0, -1.0, 3.14169, -0.001, 1e11, 1e-20] {
        assert_roundtrip(&[value]);
    }
}

#[test]
fn sensor_style_series() {
    // The case DeXOR targets: short decimal fractions drifting slowly.
    let values: Vec<f64> = (0..512).map(|i| 21.5 + (i % 40) as f64 * 0.1).collect();
    assert_roundtrip(&values);
}

#[test]
fn sensor_style_series_beats_raw_doubles() {
    let values: Vec<f64> = (0..1024).map(|i| 21.5 + (i % 40) as f64 * 0.1).collect();
    let bps = bits_per_sample(&values);
    assert!(
        bps < 32.0,
        "expected < 32 bits/sample on decimal data, got {bps}"
    );
}

#[test]
fn constant_series_is_nearly_free() {
    let values = vec![42.25; 1000];
    assert_roundtrip(&values);
    let bps = bits_per_sample(&values);
    assert!(
        bps < 4.0,
        "expected < 4 bits/sample on a constant series, got {bps}"
    );
}

#[test]
fn zero_and_signed_zero() {
    // -0.0 has no decimal representation distinct from 0.0, so it must take the
    // exception path to survive a round trip.
    assert_roundtrip(&[0.0, -0.0, 0.0, 0.0, -0.0]);
}

#[test]
fn non_finite_values() {
    assert_roundtrip(&[
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
        1.0,
        f64::NAN,
        2.5,
    ]);

    // A signalling-style NaN payload must survive verbatim too.
    let payload = f64::from_bits(0x7FF0_0000_DEAD_BEEF);
    let bytes = DeXorEncoder::encode_slice(&[payload], DeXorConfig::default());
    let decoded = DeXorDecoder::decode_all(&bytes, 1, DeXorConfig::default()).unwrap();
    assert_eq!(decoded[0].to_bits(), payload.to_bits());
}

#[test]
fn extreme_magnitudes() {
    assert_roundtrip(&[
        f64::MIN,
        f64::MAX,
        f64::MIN_POSITIVE,
        -f64::MIN_POSITIVE,
        f64::EPSILON,
        1e300,
        -1e300,
        1e-300,
        5e-324, // smallest subnormal
        1.0,
    ]);
}

#[test]
fn q_field_boundaries() {
    // q is transmitted in a biased 5-bit field spanning [-20, 11]; values just
    // inside and just outside must both round-trip.
    assert_roundtrip(&[
        1e-20, 2e-20, 1e11, 2e11, 1e-21, // below the representable q
        1e12,  // above it
        3.0,
    ]);
}

#[test]
fn alternating_signs() {
    let values: Vec<f64> = (0..256)
        .map(|i| if i % 2 == 0 { 1.25 } else { -1.25 })
        .collect();
    assert_roundtrip(&values);
}

#[test]
fn crossing_zero() {
    let values: Vec<f64> = (-100..=100).map(|i| i as f64 * 0.01).collect();
    assert_roundtrip(&values);
}

#[test]
fn many_significant_digits() {
    assert_roundtrip(&[
        0.123456789012345,
        0.123456789012346,
        1.7976931348623157e308,
        std::f64::consts::PI,
        std::f64::consts::E,
        1.0 / 3.0,
        2.0 / 3.0,
    ]);
}

#[test]
fn integer_series() {
    let values: Vec<f64> = (0..1000).map(|i| (i * 7) as f64).collect();
    assert_roundtrip(&values);
}

#[test]
fn skippable_mode_gives_up_after_threshold() {
    // Binary-origin values never take the decimal path, so `Skippable` should
    // drop the two control bits per sample and beat `Native`.
    let mut bits = 0x3FD5_5555_1234_5678u64;
    let values: Vec<f64> = (0..512)
        .map(|_| {
            bits = bits
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            f64::from_bits((bits >> 12) | 0x3FD0_0000_0000_0000)
        })
        .collect();

    assert_roundtrip(&values);

    let native = DeXorEncoder::encode_slice(&values, DeXorConfig::default()).len();
    let skippable = DeXorEncoder::encode_slice(
        &values,
        DeXorConfig {
            mode: DeXorMode::Skippable { after: 4 },
            ..DeXorConfig::default()
        },
    )
    .len();
    assert!(
        skippable < native,
        "skippable ({skippable}) should undercut native ({native}) on binary-origin data",
    );
}

#[test]
fn exception_run_recovers() {
    // Interleave decimal-friendly values with ones that must fall through, to
    // exercise the exception coder's adaptive field width in both directions.
    let mut values = Vec::new();
    for i in 0..200 {
        values.push(1.5 + (i % 10) as f64 * 0.1);
        values.push(f64::from_bits(
            0x4000_0000_0000_0001 + i as u64 * 0x1_0000_0000,
        ));
    }
    assert_roundtrip(&values);
}

#[test]
fn truncated_stream_errors_instead_of_panicking() {
    let values: Vec<f64> = (0..64).map(|i| 10.5 + i as f64 * 0.25).collect();
    let bytes = DeXorEncoder::encode_slice(&values, DeXorConfig::default());

    for cut in 0..bytes.len() {
        // Asking for far more samples than the stream holds must surface an
        // error, never a panic.
        let result =
            DeXorDecoder::decode_all(&bytes[..cut], values.len() * 4, DeXorConfig::default());
        assert!(result.is_err(), "expected EOF error at cut {cut}");
    }
}

#[test]
fn arbitrary_bytes_do_not_panic() {
    let mut seed = 0x243F_6A88_85A3_08D3u64;
    for _ in 0..2000 {
        let len = (seed % 48) as usize + 1;
        let bytes: Vec<u8> = (0..len)
            .map(|_| {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                (seed >> 33) as u8
            })
            .collect();
        for config in configs() {
            // Values may be nonsense; the contract is only that we terminate.
            let _ = DeXorDecoder::decode_all(&bytes, 64, config);
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn roundtrip_arbitrary_doubles(values in prop::collection::vec(any::<f64>(), 1..64)) {
        for config in configs() {
            let bytes = DeXorEncoder::encode_slice(&values, config);
            let decoded = DeXorDecoder::decode_all(&bytes, values.len(), config).unwrap();
            for (expected, actual) in values.iter().zip(decoded.iter()) {
                prop_assert_eq!(expected.to_bits(), actual.to_bits());
            }
        }
    }

    #[test]
    fn roundtrip_decimal_doubles(
        raw in prop::collection::vec(-10_000_000i64..10_000_000, 1..64),
        scale in 0i32..7,
    ) {
        let divisor = 10f64.powi(scale);
        let values: Vec<f64> = raw.iter().map(|&v| v as f64 / divisor).collect();
        for config in configs() {
            let bytes = DeXorEncoder::encode_slice(&values, config);
            let decoded = DeXorDecoder::decode_all(&bytes, values.len(), config).unwrap();
            for (expected, actual) in values.iter().zip(decoded.iter()) {
                prop_assert_eq!(expected.to_bits(), actual.to_bits());
            }
        }
    }
}
