//! Load-time validation of compressed chunks: `XorChunk::check_count`, `check_header` and
//! `verify_stream`. Each forged case rewrites one header field the way a hostile payload can,
//! and must be rejected; every chunk the write path builds must be accepted.

use crate::common::Sample;
use crate::series::DuplicatePolicy;
use crate::series::chunks::{ChimpChunk, Chunk, ChunkOps, GorillaChunk, SampleCodec, XorChunk};
use crate::tests::generators::{DataGenerator, ValueWorkload};
use std::time::Duration;

/// 20 samples at 1000..=20000 with values `i * 1.5`, the shape the series-level probes used.
fn base_samples() -> Vec<Sample> {
    (1..=20)
        .map(|i| Sample::new(i * 1000, i as f64 * 1.5))
        .collect()
}

fn chunk_of<C: SampleCodec>(samples: &[Sample]) -> XorChunk<C> {
    let mut chunk = XorChunk::<C>::with_max_size(4096);
    chunk.set_data(samples).unwrap();
    chunk
}

fn accepted<C: SampleCodec>(chunk: &XorChunk<C>) -> bool {
    chunk.check_header().is_ok() && chunk.verify_stream().is_ok()
}

fn assert_accepted<C: SampleCodec>(chunk: &XorChunk<C>, what: &str) {
    if let Err(e) = chunk.check_header() {
        panic!("{} {what}: header rejected: {e}", C::ENCODING.name());
    }
    if let Err(e) = chunk.verify_stream() {
        panic!("{} {what}: stream rejected: {e}", C::ENCODING.name());
    }
}

fn honest_chunks_are_accepted<C: SampleCodec>() {
    assert_accepted(&XorChunk::<C>::with_max_size(4096), "empty");
    assert_accepted(&chunk_of::<C>(&base_samples()), "base");
    assert_accepted(
        &chunk_of::<C>(&[Sample::new(0, -0.0)]),
        "one sample at ts 0",
    );

    // Bit patterns equality must not trip over: NaN payloads, signed zeros, infinities, repeats.
    let odd = [
        Sample::new(1, f64::NAN),
        Sample::new(2, f64::from_bits(0x7ff0_0000_0000_0002)),
        Sample::new(3, -0.0),
        Sample::new(4, 0.0),
        Sample::new(5, f64::INFINITY),
        Sample::new(6, f64::NEG_INFINITY),
        Sample::new(7, 7.0),
        Sample::new(8, 7.0),
        Sample::new(9, f64::NAN),
    ];
    assert_accepted(&chunk_of::<C>(&odd), "odd values");

    for workload in [
        ValueWorkload::Uniform,
        ValueWorkload::StdNorm,
        ValueWorkload::Deriv,
        ValueWorkload::Counter,
    ] {
        let data = DataGenerator::builder()
            .start(1_700_000_000_000)
            .interval(Duration::from_millis(997))
            .algorithm(workload)
            .samples(500)
            .seed(7)
            .build()
            .generate();
        let mut chunk = chunk_of::<C>(&data);
        assert_accepted(&chunk, "generated");

        // Every mutation re-encodes from scratch; the result must still verify.
        chunk
            .upsert_sample(
                Sample::new(data[10].timestamp + 1, 1.25),
                DuplicatePolicy::KeepLast,
            )
            .unwrap();
        assert_accepted(&chunk, "after upsert");
        chunk
            .remove_range(data[100].timestamp, data[150].timestamp)
            .unwrap();
        assert_accepted(&chunk, "after remove_range");
        let right = chunk.split().unwrap();
        assert_accepted(&chunk, "split left");
        assert_accepted(&right, "split right");
        let mut appended = chunk.clone();
        appended
            .add_sample(&Sample::new(right.last_timestamp() + 5, 3.0))
            .unwrap();
        assert_accepted(&appended, "after append");
    }
}

fn forged_counts_are_rejected<C: SampleCodec>(set_count: impl Fn(&mut C, u64)) {
    let honest = chunk_of::<C>(&base_samples());

    let mut huge = honest.clone();
    set_count(&mut huge.encoder, 1 << 60);
    assert!(huge.check_count().is_err(), "2^60 samples in a few bytes");

    let mut zero = honest.clone();
    set_count(&mut zero.encoder, 0);
    assert!(zero.check_count().is_err(), "count 0 over a full stream");

    // Plausible by size, so only the decode pass can catch it.
    let mut fewer = honest.clone();
    set_count(&mut fewer.encoder, 5);
    assert!(fewer.check_header().is_ok());
    assert!(fewer.verify_stream().is_err(), "5 of 20 samples");

    let mut more = honest.clone();
    set_count(&mut more.encoder, 21);
    assert!(!accepted(&more), "21 of 20 samples");
}

fn forged_bounds_are_rejected<C: SampleCodec>(set_bounds: impl Fn(&mut C, i64, i64, f64)) {
    let honest = chunk_of::<C>(&base_samples());
    let (first, last, value) = (1000, 20000, 30.0);

    let mut swapped = honest.clone();
    set_bounds(&mut swapped.encoder, last, first, value);
    assert!(swapped.check_header().is_err(), "first > last");

    let mut negative = honest.clone();
    set_bounds(&mut negative.encoder, -101, last, value);
    assert!(negative.check_header().is_err(), "negative first timestamp");

    let mut fake_last = honest.clone();
    set_bounds(&mut fake_last.encoder, first, 99000, 12345.0);
    assert!(fake_last.check_header().is_ok());
    assert!(
        fake_last.verify_stream().is_err(),
        "a last sample never written"
    );

    let mut fake_value = honest.clone();
    set_bounds(&mut fake_value.encoder, first, last, 30.000001);
    assert!(
        fake_value.verify_stream().is_err(),
        "last value off by one ulp-ish"
    );

    let mut fake_first = honest.clone();
    set_bounds(&mut fake_first.encoder, 500, last, value);
    assert!(
        fake_first.verify_stream().is_err(),
        "first timestamp below the stream's"
    );
}

fn forged_streams_are_rejected<C: SampleCodec>(stream: impl Fn(&mut C) -> &mut Vec<u8>) {
    let honest = chunk_of::<C>(&base_samples());

    // All-zero bytes decode as samples stacked on timestamp 0.
    let mut zeros = honest.clone();
    stream(&mut zeros.encoder).iter_mut().for_each(|b| *b = 0);
    assert!(!accepted(&zeros), "zeroed stream");

    // Bytes past the last sample.
    let mut trailing = honest.clone();
    stream(&mut trailing.encoder).extend_from_slice(&[0xAB; 8]);
    assert!(!accepted(&trailing), "trailing bytes");

    // The writer leaves the free bits of the trailing byte zero, and the last sample is in the
    // header too: flipping the stream's final bit is a forgery either way.
    let mut padded = honest.clone();
    let bytes = stream(&mut padded.encoder);
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    assert!(!accepted(&padded), "final bit flipped");

    // A flipped stream bit either fails to decode or decodes to other samples. Those re-encode
    // to the flipped stream only if it is itself canonical, and then the header (last sample,
    // tail state) still has to match: count how often the pass is fooled rather than pinning
    // zero, since a self-consistent flip is a different valid chunk, not a forgery.
    let mut fooled = 0;
    let len = stream(&mut honest.clone().encoder).len();
    for byte in 0..len {
        for bit in 0..8 {
            let mut flipped = honest.clone();
            stream(&mut flipped.encoder)[byte] ^= 1 << bit;
            if accepted(&flipped) {
                fooled += 1;
            }
        }
    }
    assert!(
        fooled * 10 < len * 8,
        "{fooled} of {} flips accepted",
        len * 8
    );
}

#[test]
fn gorilla_honest_chunks_are_accepted() {
    honest_chunks_are_accepted::<crate::series::chunks::GorillaEncoder>();
}

#[test]
fn chimp_honest_chunks_are_accepted() {
    honest_chunks_are_accepted::<crate::series::chunks::ChimpCompressor>();
}

#[test]
fn gorilla_forged_counts_are_rejected() {
    forged_counts_are_rejected::<crate::series::chunks::GorillaEncoder>(|e, n| {
        e.num_samples = n as usize
    });
}

#[test]
fn chimp_forged_counts_are_rejected() {
    forged_counts_are_rejected::<crate::series::chunks::ChimpCompressor>(|e, n| {
        *e.header_mut_for_test().0 = n
    });
}

#[test]
fn gorilla_forged_bounds_are_rejected() {
    forged_bounds_are_rejected::<crate::series::chunks::GorillaEncoder>(|e, first, last, v| {
        e.first_ts = first;
        e.last_ts = last;
        e.last_value = v;
    });
}

#[test]
fn chimp_forged_bounds_are_rejected() {
    forged_bounds_are_rejected::<crate::series::chunks::ChimpCompressor>(|e, first, last, v| {
        let (_, f, l, value, _, _) = e.header_mut_for_test();
        *f = first;
        *l = last;
        *value = v;
    });
}

#[test]
fn gorilla_forged_streams_are_rejected() {
    forged_streams_are_rejected::<crate::series::chunks::GorillaEncoder>(|e| {
        &mut e.stream_mut_for_test().stream
    });
}

#[test]
fn chimp_forged_streams_are_rejected() {
    forged_streams_are_rejected::<crate::series::chunks::ChimpCompressor>(|e| {
        &mut e.stream_mut_for_test().stream
    });
}

/// The tail state only shapes the next append, so nothing a read returns exposes it: a false
/// Chimp `stored_val` made three acknowledged appends read back as NaN and garbage.
#[test]
fn forged_tail_state_is_rejected() {
    let mut chimp: ChimpChunk = chunk_of(&base_samples());
    chimp.encoder.set_stored_val_for_test(u64::MAX);
    assert!(chimp.check_header().is_ok());
    assert!(chimp.verify_stream().is_err(), "chimp stored_val");

    let mut chimp: ChimpChunk = chunk_of(&base_samples());
    *chimp.encoder.header_mut_for_test().4 += 1;
    assert!(chimp.verify_stream().is_err(), "chimp last_delta");

    let mut chimp: ChimpChunk = chunk_of(&base_samples());
    *chimp.encoder.header_mut_for_test().5 = 3;
    assert!(chimp.verify_stream().is_err(), "chimp last_alpha");

    let mut gorilla: GorillaChunk = chunk_of(&base_samples());
    gorilla.encoder.set_window_for_test(0, 0, 999);
    assert!(gorilla.verify_stream().is_err(), "gorilla window");
}

/// The free-bit count of the trailing byte is part of the header too.
#[test]
fn forged_free_bit_count_is_rejected() {
    let honest: GorillaChunk = chunk_of(&base_samples());
    let mut forged = honest.clone();
    let stream = forged.encoder.stream_mut_for_test();
    stream.count = (stream.count + 1) % 8;
    assert!(!accepted(&forged));

    let honest: ChimpChunk = chunk_of(&base_samples());
    let mut forged = honest.clone();
    let stream = forged.encoder.stream_mut_for_test();
    stream.count = (stream.count + 1) % 8;
    assert!(!accepted(&forged));
}

/// Descending or repeated timestamps decode fine from a stream built by hand, but no write path
/// stores them.
#[test]
fn unordered_streams_are_rejected() {
    let mut chimp = ChimpChunk::with_max_size(4096);
    // The Chimp codec itself takes out-of-order timestamps; only `add_sample` refuses them.
    for sample in [
        Sample::new(10, 1.0),
        Sample::new(5, 2.0),
        Sample::new(20, 3.0),
    ] {
        chimp.encoder.push(&sample).unwrap();
    }
    assert!(chimp.verify_stream().is_err(), "descending");

    let mut gorilla = GorillaChunk::with_max_size(4096);
    for sample in [
        Sample::new(10, 1.0),
        Sample::new(10, 2.0),
        Sample::new(20, 3.0),
    ] {
        gorilla.encoder.push(&sample).unwrap();
    }
    assert!(gorilla.verify_stream().is_err(), "repeated");
}
