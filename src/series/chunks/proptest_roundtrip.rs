//! Property-based round-trip tests for the seven chunk encodings.
//!
//! For every encoding, a randomly generated run of samples that is written into a
//! chunk must survive a `serialize` → `deserialize` round-trip unchanged: the
//! decoded chunk yields the same samples the encoder accepted and compares equal
//! to the original. This complements the example-based serialization tests in
//! `timeseries_chunk_tests.rs` with randomized coverage of timestamp deltas and
//! value magnitudes (normal, subnormal, and zero, of both signs).

use crate::common::Sample;
use crate::series::chunks::{Chunk, ChunkEncoding, ChunkOps, TimeSeriesChunk};
use proptest::prelude::*;

/// All seven encodings under test.
const CHUNK_TYPES: [ChunkEncoding; 7] = [
    ChunkEncoding::Uncompressed,
    ChunkEncoding::Gorilla,
    ChunkEncoding::TsXor,
    ChunkEncoding::Xor2,
    ChunkEncoding::Pco,
    ChunkEncoding::DeXor,
    ChunkEncoding::Chimp,
];

/// A chunk size comfortably larger than the sample runs we generate, so the
/// encodings under test rarely reject a sample for capacity. (Uncompressed still
/// caps at `MAX_UNCOMPRESSED_SAMPLES`; the round-trip logic handles a full chunk
/// by only comparing the samples that were actually accepted.)
const CHUNK_SIZE: usize = 8192;

/// Upper bound on the number of samples per generated run. Kept below
/// `MAX_UNCOMPRESSED_SAMPLES` (256) so a full run typically fits in every
/// encoding while still exercising multi-sample encode paths.
const MAX_SAMPLES: usize = 200;

/// Strategy producing a run of samples with strictly increasing timestamps and
/// finite values. Timestamps are built by prefix-summing positive gaps from a
/// random start, which guarantees strict monotonicity (required by the
/// append-only compressed encoders) without risking `i64` overflow. Values cover
/// normal, subnormal, and zero magnitudes of both signs; NaN and infinity are
/// excluded so the property is about encode/decode fidelity rather than the
/// codecs' handling of non-finite inputs.
fn samples_strategy() -> impl Strategy<Value = Vec<Sample>> {
    let value = prop::num::f64::POSITIVE
        | prop::num::f64::NEGATIVE
        | prop::num::f64::NORMAL
        | prop::num::f64::SUBNORMAL
        | prop::num::f64::ZERO;
    let gap = 1i64..1_000_000i64;
    let start = 1i64..1_000_000_000i64;
    (start, prop::collection::vec((gap, value), 0..MAX_SAMPLES)).prop_map(|(start, pairs)| {
        let mut ts = start;
        let mut samples = Vec::with_capacity(pairs.len());
        for (gap, value) in pairs {
            samples.push(Sample {
                timestamp: ts,
                value,
            });
            ts = ts.saturating_add(gap);
        }
        samples
    })
}

/// Writes `samples` into a fresh chunk of the given `encoding`, then asserts the
/// chunk survives a serialize/deserialize round-trip with its data intact.
fn assert_roundtrip(encoding: ChunkEncoding, samples: &[Sample]) -> Result<(), TestCaseError> {
    let mut chunk = TimeSeriesChunk::new(encoding, CHUNK_SIZE);

    // Feed samples until the chunk rejects one for capacity, tracking exactly
    // what was accepted so the comparison below is independent of the cap.
    let mut accepted: Vec<Sample> = Vec::with_capacity(samples.len());
    for sample in samples {
        match chunk.add_sample(sample) {
            Ok(()) => accepted.push(*sample),
            Err(_) => break,
        }
    }

    let mut buf = Vec::new();
    chunk.serialize(&mut buf);
    let restored = TimeSeriesChunk::deserialize(&buf)
        .map_err(|e| TestCaseError::fail(format!("{encoding}: deserialize failed: {e:?}")))?;

    prop_assert_eq!(chunk.get_encoding(), restored.get_encoding());
    prop_assert_eq!(chunk.len(), restored.len());

    let original = chunk
        .get_range(0, i64::MAX)
        .map_err(|e| TestCaseError::fail(format!("{encoding}: get_range failed: {e:?}")))?;
    let decoded = restored
        .get_range(0, i64::MAX)
        .map_err(|e| TestCaseError::fail(format!("{encoding}: get_range failed: {e:?}")))?;

    // The encoder must reproduce exactly the samples it accepted...
    prop_assert_eq!(&original, &accepted);
    // ...and those samples must survive the serialize/deserialize round-trip.
    prop_assert_eq!(&decoded, &accepted);
    // Structural equality is the strongest statement of round-trip fidelity: the
    // decoded chunk (including any internal append/writer state) matches the original.
    prop_assert_eq!(chunk, restored);

    Ok(())
}

proptest! {
    /// A random run of samples round-trips through serialize/deserialize for
    /// every encoding.
    #[test]
    fn chunk_encoding_serialize_roundtrip(samples in samples_strategy()) {
        for &encoding in &CHUNK_TYPES {
            assert_roundtrip(encoding, &samples)?;
        }
    }
}
