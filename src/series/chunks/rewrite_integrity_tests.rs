//! A chunk rewrite (a merge, a range removal, defrag's capacity merge) must not turn a decode
//! error into lost samples. A read stops at the first undecodable sample and reports what it
//! decoded; a rewrite built on such a read used to re-encode the shortened chunk and report
//! success, so everything past the corrupt point was gone for good. Each one now refuses and
//! leaves the chunk as it was, like `upsert_sample` and `split` already did.

use crate::common::Sample;
use crate::series::chunks::*;
use crate::series::{DuplicatePolicy, SampleAddResult};

fn samples(n: i64) -> Vec<Sample> {
    (1..=n)
        .map(|i| Sample::new(i * 1000, i as f64 * 1.37 + (i % 7) as f64))
        .collect()
}

/// A 200-sample chunk whose stream is cut in half: its header still says 200 samples, and a
/// read stops at the cut.
fn truncated<C: SampleCodec>(stream: &impl Fn(&mut C) -> &mut Vec<u8>) -> XorChunk<C> {
    let mut chunk = XorChunk::<C>::with_max_size(8192);
    chunk.set_data(&samples(200)).unwrap();
    let bytes = stream(&mut chunk.encoder);
    let half = bytes.len() / 2;
    bytes.truncate(half);
    assert!(
        chunk.iter().count() < 200,
        "the cut must be inside the stream"
    );
    chunk
}

fn rewrites_refuse_a_chunk_they_cannot_decode<C: SampleCodec>(
    stream: impl Fn(&mut C) -> &mut Vec<u8>,
    wrap: impl Fn(XorChunk<C>) -> TimeSeriesChunk,
) {
    let name = C::ENCODING.name();
    let chunk = truncated(&stream);

    let mut merged = chunk.clone();
    let result = merged.merge_samples(
        &[Sample::new(1500, 1.0), Sample::new(2500, 2.0)],
        Some(DuplicatePolicy::KeepLast),
    );
    assert!(result.is_err(), "{name}: merge reported {result:?}");
    assert_eq!(merged, chunk, "{name}: a refused merge changed the chunk");

    let mut dest = TimeSeriesChunk::new(C::ENCODING, 65536);
    dest.set_data(&[Sample::new(0, 0.5)]).unwrap();
    let mut src = wrap(chunk.clone());
    let result = merge_by_capacity(&mut dest, &mut src, i64::MIN, None);
    assert!(
        result.is_err(),
        "{name}: capacity merge reported {result:?}"
    );
    assert_eq!(src, wrap(chunk.clone()), "{name}: the source was emptied");
    assert_eq!(
        dest.len(),
        1,
        "{name}: the destination took part of the source"
    );
}

/// A removal whose window holds no sample has nothing to rewrite: it answers without decoding
/// the chunk, so it neither re-encodes an intact chunk nor fails on one it cannot fully read.
fn a_removal_with_nothing_to_remove_rewrites_nothing<C: SampleCodec>(
    stream: impl Fn(&mut C) -> &mut Vec<u8>,
) {
    let name = C::ENCODING.name();
    let mut intact = XorChunk::<C>::with_max_size(8192);
    intact.set_data(&samples(200)).unwrap();
    let before = intact.clone();
    assert_eq!(intact.remove_range(1500, 1600).unwrap(), 0, "{name}");
    assert_eq!(intact, before, "{name}");

    // Early in the chunk, ahead of the cut: settled without reading as far as it.
    let mut cut = truncated(&stream);
    let before = cut.clone();
    assert_eq!(cut.remove_range(1500, 1600).unwrap(), 0, "{name}");
    assert_eq!(cut, before, "{name}");
    // A window that does hold samples still needs the whole chunk, and still refuses.
    assert!(cut.remove_range(2000, 3000).is_err(), "{name}");
    assert_eq!(cut, before, "{name}");
}

#[test]
fn chimp_rewrites_refuse_a_chunk_they_cannot_decode() {
    rewrites_refuse_a_chunk_they_cannot_decode::<ChimpCompressor>(
        |e| &mut e.stream_mut_for_test().stream,
        TimeSeriesChunk::Chimp,
    );
}

#[test]
fn gorilla_rewrites_refuse_a_chunk_they_cannot_decode() {
    rewrites_refuse_a_chunk_they_cannot_decode::<GorillaEncoder>(
        |e| &mut e.stream_mut_for_test().stream,
        TimeSeriesChunk::Gorilla,
    );
}

#[test]
fn chimp_removal_with_nothing_to_remove_rewrites_nothing() {
    a_removal_with_nothing_to_remove_rewrites_nothing::<ChimpCompressor>(|e| {
        &mut e.stream_mut_for_test().stream
    });
}

#[test]
fn gorilla_removal_with_nothing_to_remove_rewrites_nothing() {
    a_removal_with_nothing_to_remove_rewrites_nothing::<GorillaEncoder>(|e| {
        &mut e.stream_mut_for_test().stream
    });
}

/// One sample merged into an uncompressed chunk follows the same rules as several: it is
/// appended past a full chunk's last sample (the batch path never refused that; the one-sample
/// path returned `CapacityFull`), and an upsert reports the value it stored.
#[test]
fn a_one_sample_uncompressed_merge_follows_the_batch_rules() {
    let mut full = UncompressedChunk::with_max_size(4 * 16);
    full.set_data(&samples(4)).unwrap();
    assert!(full.is_full());
    let one = full.clone().merge_samples(&[Sample::new(9000, 1.0)], None);
    let two = full
        .clone()
        .merge_samples(&[Sample::new(9000, 1.0), Sample::new(9500, 1.0)], None);
    assert!(two.is_ok(), "{two:?}");
    assert!(one.is_ok(), "{one:?}");

    let mut chunk = UncompressedChunk::with_max_size(4096);
    chunk
        .set_data(&[Sample::new(1000, 1.0), Sample::new(3000, 2.74)])
        .unwrap();
    let result = chunk
        .merge_samples(&[Sample::new(3000, 100.0)], Some(DuplicatePolicy::Sum))
        .unwrap();
    let stored = chunk.get_range(3000, 3000).unwrap();
    assert_eq!(result, [SampleAddResult::Ok(stored[0])]);
}

/// `min_timestamp` is the floor below which defrag's capacity merge carries no sample, on the
/// path that merges the whole source as on the one that merges part of it. The whole-source
/// path used to ignore it and carry expired samples into the destination.
#[test]
fn a_capacity_merge_carries_no_sample_below_the_floor() {
    let mut dest = TimeSeriesChunk::new(ChunkEncoding::Uncompressed, 65536);
    dest.set_data(&[Sample::new(0, 0.5)]).unwrap();
    let mut src = TimeSeriesChunk::new(ChunkEncoding::Uncompressed, 4096);
    src.set_data(&samples(10)).unwrap();

    let merged = merge_by_capacity(&mut dest, &mut src, 5000, None).unwrap();
    assert_eq!(merged, Some(6));
    assert!(src.is_empty());
    let kept: Vec<i64> = dest
        .get_range(i64::MIN, i64::MAX)
        .unwrap()
        .iter()
        .map(|s| s.timestamp)
        .collect();
    assert_eq!(kept, [0, 5000, 6000, 7000, 8000, 9000, 10000]);
}
