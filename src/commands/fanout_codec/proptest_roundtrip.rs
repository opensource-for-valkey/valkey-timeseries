//! Property-based tests for the fanout wire conversions.
//!
//! Two distinct properties are covered here, and the second is the reason this
//! module exists:
//!
//! 1. **Round-trip fidelity.** A local value converted to its protobuf twin and
//!    back must compare equal to the original. This is what catches a mismapped
//!    variant pair — the `Share <=> ShareIf` / `IRate <=> Irate` rows in
//!    `map_enum!` are exactly the kind of asymmetry that a hand-written 23-arm
//!    match gets wrong silently.
//!
//! 2. **Decode totality.** For *arbitrary* wire input — including enum
//!    discriminants a peer would never legitimately send, inverted ranges, and
//!    non-finite floats — the decoding conversions must return `Ok` or `Err`,
//!    never panic. These conversions run in the fanout handlers, which are
//!    invoked from the module's C entry points, so a panic unwinds into
//!    `extern "C"` and aborts the whole process. proptest fails a test on panic,
//!    so a property that merely *calls* the conversion and discards the result
//!    is a real assertion that no input aborts the node.
//!
//! Round-trip properties generate only values a correct encoder would produce;
//! the totality properties deliberately do not.

use super::chunks::{deserialize_chunk, serialize_chunk};
use super::generated::{
    AggregationOptions as FanoutAggregationOptions, AggregationType as FanoutAggregationType,
    AggregatorConfig as FanoutAggregatorConfig, BucketAlignmentType, BucketTimestampType,
    ComparisonOperator as FanoutComparisonOperator, CompressionType as FanoutChunkEncoding,
    DateRange, Label as FanoutLabel, ReducePartialState, Sample as FanoutSample,
    ValueComparisonFilter as FanoutValueComparisonFilter,
};
use crate::aggregators::{BucketAlignment, BucketTimestamp, PartialState};
use crate::common::Sample;
use crate::common::binop::ComparisonOperator;
use crate::labels::Label;
use crate::series::TimestampRange;
use crate::series::chunks::{ChunkEncoding, samples_to_chunk_lossless};
use crate::series::request_types::{
    AggregationOptions, AggregationType, AggregatorConfig, MAX_AGGREGATIONS, ValueComparisonFilter,
};
use proptest::prelude::*;
use smallvec::SmallVec;

/// Every `AggregationType`, so the round-trip covers the whole table rather than
/// a sample of it. `map_enum!` generates exhaustive matches in both directions,
/// so a newly added variant breaks the build there; this list only needs to stay
/// in step for coverage.
const ALL_AGGREGATION_TYPES: [AggregationType; 23] = [
    AggregationType::All,
    AggregationType::Any,
    AggregationType::Avg,
    AggregationType::Count,
    AggregationType::CountAll,
    AggregationType::CountIf,
    AggregationType::CountNan,
    AggregationType::First,
    AggregationType::Increase,
    AggregationType::IRate,
    AggregationType::Last,
    AggregationType::Max,
    AggregationType::Min,
    AggregationType::None,
    AggregationType::Range,
    AggregationType::Rate,
    AggregationType::Share,
    AggregationType::StdP,
    AggregationType::StdS,
    AggregationType::Sum,
    AggregationType::SumIf,
    AggregationType::VarP,
    AggregationType::VarS,
];

const ALL_COMPARISON_OPERATORS: [ComparisonOperator; 6] = [
    ComparisonOperator::Equal,
    ComparisonOperator::NotEqual,
    ComparisonOperator::GreaterThan,
    ComparisonOperator::GreaterThanOrEqual,
    ComparisonOperator::LessThan,
    ComparisonOperator::LessThanOrEqual,
];

const ALL_BUCKET_TIMESTAMPS: [BucketTimestamp; 3] = [
    BucketTimestamp::Start,
    BucketTimestamp::End,
    BucketTimestamp::Mid,
];

const ALL_CHUNK_ENCODINGS: [ChunkEncoding; 3] = [
    ChunkEncoding::Uncompressed,
    ChunkEncoding::Gorilla,
    ChunkEncoding::Chimp,
];

// ---------------------------------------------------------------------------
// Exhaustive enum round-trips
// ---------------------------------------------------------------------------

#[test]
fn test_every_aggregation_type_round_trips() {
    for local in ALL_AGGREGATION_TYPES {
        let wire: FanoutAggregationType = local.into();
        let back = AggregationType::try_from(wire)
            .unwrap_or_else(|_| panic!("{local:?} must decode back from {wire:?}"));
        assert_eq!(local, back, "{local:?} round-tripped to {back:?}");
    }
}

#[test]
fn test_every_comparison_operator_round_trips() {
    for local in ALL_COMPARISON_OPERATORS {
        let wire: FanoutComparisonOperator = local.into();
        let back = ComparisonOperator::try_from(wire).expect("must decode back");
        assert_eq!(local, back);
    }
}

#[test]
fn test_every_bucket_timestamp_round_trips() {
    for local in ALL_BUCKET_TIMESTAMPS {
        let wire: BucketTimestampType = local.into();
        let back = BucketTimestamp::try_from(wire).expect("must decode back");
        assert_eq!(local, back);
    }
}

#[test]
fn test_every_chunk_encoding_round_trips() {
    for local in ALL_CHUNK_ENCODINGS {
        let wire: FanoutChunkEncoding = local.into();
        let back = ChunkEncoding::try_from(wire).expect("must decode back");
        assert_eq!(local, back);
    }
}

/// `BucketAlignment` is the one pair `map_enum!` does not generate, because
/// `Timestamp(i64)` carries a payload that travels in a separate wire field. The
/// enum half must still round-trip; the payload is checked by the
/// `AggregationOptions` property below.
#[test]
fn test_bucket_alignment_enum_half_round_trips() {
    let cases = [
        (BucketAlignmentType::Default, BucketAlignment::Default),
        (BucketAlignmentType::AlignStart, BucketAlignment::Start),
        (BucketAlignmentType::AlignEnd, BucketAlignment::End),
        (
            BucketAlignmentType::Timestamp,
            BucketAlignment::Timestamp(0),
        ),
    ];
    for (wire, expected) in cases {
        let back = BucketAlignment::try_from(wire).expect("must decode back");
        assert_eq!(back, expected);
    }
}

// ---------------------------------------------------------------------------
// Pinned wire numbers
// ---------------------------------------------------------------------------
//
// These are the one class of error the round-trip properties structurally cannot
// catch. Because `map_enum!` derives both directions from a single variant table,
// a wrong *row* is symmetric: swap `Max <=> Max` and `Min <=> Min` into
// `Max <=> Min` / `Min <=> Max` and every round-trip still passes while the bytes
// on the wire change meaning. Round-tripping only proves the two directions agree
// with each other, not that either agrees with the schema. Asserting the integers
// is what ties the Rust side to the `.proto` numbering.

#[test]
fn test_aggregation_type_wire_numbers_are_pinned() {
    // Must match `AGGREGATION_TYPE_*` in proto/v1/common.proto.
    let expected = [
        (AggregationType::All, 1),
        (AggregationType::Any, 2),
        (AggregationType::Avg, 3),
        (AggregationType::Count, 4),
        (AggregationType::CountAll, 5),
        (AggregationType::CountIf, 6),
        (AggregationType::CountNan, 7),
        (AggregationType::First, 8),
        (AggregationType::Increase, 9),
        (AggregationType::IRate, 10),
        (AggregationType::Last, 11),
        (AggregationType::Max, 12),
        (AggregationType::Min, 13),
        (AggregationType::None, 14),
        (AggregationType::Share, 15),
        (AggregationType::Sum, 16),
        (AggregationType::SumIf, 17),
        (AggregationType::Range, 18),
        (AggregationType::Rate, 19),
        (AggregationType::StdS, 20),
        (AggregationType::StdP, 21),
        (AggregationType::VarS, 22),
        (AggregationType::VarP, 23),
    ];
    assert_eq!(
        expected.len(),
        ALL_AGGREGATION_TYPES.len(),
        "every aggregation type needs a pinned wire number"
    );
    for (local, number) in expected {
        let wire: FanoutAggregationType = local.into();
        assert_eq!(wire as i32, number, "{local:?} must encode as {number}");
    }
    assert_eq!(FanoutAggregationType::Unspecified as i32, 0);
}

#[test]
fn test_comparison_operator_wire_numbers_are_pinned() {
    let expected = [
        (ComparisonOperator::GreaterThan, 1),
        (ComparisonOperator::GreaterThanOrEqual, 2),
        (ComparisonOperator::LessThan, 3),
        (ComparisonOperator::LessThanOrEqual, 4),
        (ComparisonOperator::Equal, 5),
        (ComparisonOperator::NotEqual, 6),
    ];
    for (local, number) in expected {
        let wire: FanoutComparisonOperator = local.into();
        assert_eq!(wire as i32, number, "{local:?} must encode as {number}");
    }
    assert_eq!(FanoutComparisonOperator::Unspecified as i32, 0);
}

#[test]
fn test_compression_and_bucket_timestamp_wire_numbers_are_pinned() {
    for (local, number) in [
        (ChunkEncoding::Uncompressed, 1),
        (ChunkEncoding::Gorilla, 2),
        (ChunkEncoding::Chimp, 3),
    ] {
        let wire: FanoutChunkEncoding = local.into();
        assert_eq!(wire as i32, number, "{local:?} must encode as {number}");
    }
    assert_eq!(FanoutChunkEncoding::Unspecified as i32, 0);

    for (local, number) in [
        (BucketTimestamp::Start, 1),
        (BucketTimestamp::End, 2),
        (BucketTimestamp::Mid, 3),
    ] {
        let wire: BucketTimestampType = local.into();
        assert_eq!(wire as i32, number, "{local:?} must encode as {number}");
    }
    assert_eq!(BucketTimestampType::Unspecified as i32, 0);
}

// ---------------------------------------------------------------------------
// Strategies
// ---------------------------------------------------------------------------

/// Values spanning normal, subnormal, and zero magnitudes of both signs, plus
/// NaN and the infinities. Non-finite values matter here: unlike the chunk
/// codecs, these conversions move a `f64` verbatim, so they must survive it.
fn any_value() -> impl Strategy<Value = f64> {
    prop_oneof![
        9 => prop::num::f64::POSITIVE
            | prop::num::f64::NEGATIVE
            | prop::num::f64::NORMAL
            | prop::num::f64::SUBNORMAL
            | prop::num::f64::ZERO,
        1 => prop::num::f64::INFINITE | prop::num::f64::QUIET_NAN,
    ]
}

fn any_comparison_operator() -> impl Strategy<Value = ComparisonOperator> {
    prop::sample::select(ALL_COMPARISON_OPERATORS.as_slice())
}

fn any_value_comparison_filter() -> impl Strategy<Value = ValueComparisonFilter> {
    (any_comparison_operator(), any_value())
        .prop_map(|(operator, value)| ValueComparisonFilter { operator, value })
}

/// A valid `AggregatorConfig`: `AggregatorConfig::new` rejects a filtered
/// aggregator without a condition, and an unfiltered one that carries a
/// condition it cannot use, so the filter is generated to match the aggregation
/// type rather than independently of it.
fn any_aggregator_config() -> impl Strategy<Value = AggregatorConfig> {
    prop::sample::select(ALL_AGGREGATION_TYPES.as_slice()).prop_flat_map(|aggregation| {
        let filter = any_value_comparison_filter();
        if aggregation.is_filtered() {
            // Condition required.
            filter.prop_map(Some).boxed()
        } else if aggregation.has_filtered_variant() {
            // Condition optional.
            prop::option::of(filter).boxed()
        } else {
            // Condition forbidden.
            Just(None).boxed()
        }
        .prop_map(move |value_filter| {
            AggregatorConfig::new(aggregation, value_filter).expect("strategy builds valid configs")
        })
    })
}

/// A valid `AggregationOptions`. Aggregation types must be distinct — the
/// decoder rejects duplicates — so the list is deduplicated after generation
/// rather than filtered, which keeps the strategy from rejecting most inputs.
fn any_aggregation_options() -> impl Strategy<Value = AggregationOptions> {
    (
        prop::collection::vec(any_aggregator_config(), 1..=MAX_AGGREGATIONS),
        1u64..1_000_000u64,
        prop::sample::select(ALL_BUCKET_TIMESTAMPS.as_slice()),
        any_bucket_alignment(),
        any::<bool>(),
    )
        .prop_map(
            |(configs, bucket_duration, timestamp_output, alignment, report_empty)| {
                let mut aggregations: SmallVec<[AggregatorConfig; 2]> = SmallVec::new();
                for config in configs {
                    if !aggregations.iter().any(|seen: &AggregatorConfig| {
                        seen.aggregation_type() == config.aggregation_type()
                    }) {
                        aggregations.push(config);
                    }
                }
                AggregationOptions {
                    aggregations,
                    bucket_duration,
                    timestamp_output,
                    alignment,
                    report_empty,
                }
            },
        )
}

/// An enum discriminant as it arrives off the wire, weighted toward the region
/// that actually exercises the decoder.
///
/// A plain `any::<i32>()` is close to useless here, and provably so: it spends
/// essentially every case on a garbage discriminant, which fails the
/// `i32 -> wire enum` step and returns early — never reaching the
/// `wire enum -> local` step behind it, which is where the interesting logic
/// (and, historically, the panic) lives. Hitting `0` by chance out of 2^32
/// values does not happen in 256 cases. So the weights put real mass on `0`
/// (`_UNSPECIFIED`, i.e. what a peer sends by omitting the field) and on the
/// defined range, while still sampling out-of-range and negative values.
fn any_wire_discriminant(max_defined: i32) -> impl Strategy<Value = i32> {
    prop_oneof![
        4 => Just(0),
        4 => 1..=max_defined,
        1 => Just(max_defined + 1),
        1 => -4..=0i32,
        1 => any::<i32>(),
    ]
}

/// Highest defined discriminant per wire enum, i.e. the count of meaningful
/// values, since `_UNSPECIFIED` occupies 0.
const AGGREGATION_TYPE_MAX: i32 = ALL_AGGREGATION_TYPES.len() as i32;
const COMPARISON_OPERATOR_MAX: i32 = ALL_COMPARISON_OPERATORS.len() as i32;
const BUCKET_TIMESTAMP_MAX: i32 = ALL_BUCKET_TIMESTAMPS.len() as i32;
const BUCKET_ALIGNMENT_MAX: i32 = 4;
const COMPRESSION_TYPE_MAX: i32 = ALL_CHUNK_ENCODINGS.len() as i32;

/// A run of samples a real encoder would accept: strictly increasing timestamps
/// and finite values. Built by prefix-summing positive gaps, which guarantees
/// monotonicity without risking `i64` overflow.
fn valid_samples() -> impl Strategy<Value = Vec<Sample>> {
    let value = prop::num::f64::POSITIVE
        | prop::num::f64::NEGATIVE
        | prop::num::f64::NORMAL
        | prop::num::f64::ZERO;
    (
        1i64..1_000_000_000i64,
        prop::collection::vec((1i64..100_000i64, value), 1..64),
    )
        .prop_map(|(start, pairs)| {
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

fn any_bucket_alignment() -> impl Strategy<Value = BucketAlignment> {
    prop_oneof![
        Just(BucketAlignment::Default),
        Just(BucketAlignment::Start),
        Just(BucketAlignment::End),
        any::<i64>().prop_map(BucketAlignment::Timestamp),
    ]
}

proptest! {
    // -----------------------------------------------------------------------
    // Round-trip fidelity
    // -----------------------------------------------------------------------

    #[test]
    fn sample_round_trips(timestamp in any::<i64>(), value in any_value()) {
        let local = Sample { timestamp, value };
        let wire: FanoutSample = local.into();
        let back: Sample = wire.into();
        // `Sample`'s `PartialEq` treats NaN as equal to NaN, which is the
        // intended semantics for "the same sample came back".
        prop_assert_eq!(local, back);
    }

    #[test]
    fn label_round_trips(name in ".*", value in ".*") {
        let local = Label { name, value };
        let wire: FanoutLabel = (&local).into();
        let back: Label = wire.into();
        prop_assert_eq!(local, back);
    }

    #[test]
    fn value_comparison_filter_round_trips(local in any_value_comparison_filter()) {
        let wire: FanoutValueComparisonFilter = local.into();
        let back = ValueComparisonFilter::try_from(wire)
            .map_err(|e| TestCaseError::fail(format!("{e:?}")))?;
        prop_assert_eq!(local.operator, back.operator);
        prop_assert!(
            local.value == back.value || (local.value.is_nan() && back.value.is_nan()),
            "value {} != {}", local.value, back.value
        );
    }

    #[test]
    fn aggregator_config_round_trips(local in any_aggregator_config()) {
        let wire: FanoutAggregatorConfig = local.into();
        let back = AggregatorConfig::try_from(wire)
            .map_err(|e| TestCaseError::fail(format!("{e:?}")))?;
        prop_assert_eq!(local.aggregation_type(), back.aggregation_type());
        prop_assert_eq!(local.filter().is_some(), back.filter().is_some());
        if let (Some(a), Some(b)) = (local.filter(), back.filter()) {
            prop_assert_eq!(a.operator, b.operator);
        }
    }

    #[test]
    fn aggregation_options_round_trips(local in any_aggregation_options()) {
        let wire: FanoutAggregationOptions = (&local).into();
        let back = AggregationOptions::try_from(wire)
            .map_err(|e| TestCaseError::fail(format!("{e:?}")))?;
        prop_assert_eq!(local.bucket_duration, back.bucket_duration);
        prop_assert_eq!(local.timestamp_output, back.timestamp_output);
        prop_assert_eq!(local.alignment, back.alignment);
        prop_assert_eq!(local.report_empty, back.report_empty);
        prop_assert_eq!(local.aggregations.len(), back.aggregations.len());
        for (a, b) in local.aggregations.iter().zip(back.aggregations.iter()) {
            prop_assert_eq!(a.aggregation_type(), b.aggregation_type());
        }
    }

    #[test]
    fn timestamp_range_round_trips(a in 0i64..i64::MAX, b in 0i64..i64::MAX) {
        let (start, end) = if a <= b { (a, b) } else { (b, a) };
        let wire = DateRange { start, end };
        let local = TimestampRange::try_from(wire)
            .map_err(|e| TestCaseError::fail(format!("{e:?}")))?;
        let back: DateRange = local.into();
        prop_assert_eq!(start, back.start);
        prop_assert_eq!(end, back.end);
    }

    #[test]
    fn partial_state_round_trips(
        count in any::<u64>(),
        acc1 in any_value(),
        acc2 in any_value(),
        acc1_c in any_value(),
        ts in any::<i64>(),
    ) {
        let local = PartialState { count, acc1, acc2, acc1_c, ts };
        let wire: ReducePartialState = local.into();
        let back: PartialState = (&wire).into();
        prop_assert_eq!(local.count, back.count);
        prop_assert_eq!(local.ts, back.ts);
        for (l, b) in [(local.acc1, back.acc1), (local.acc2, back.acc2), (local.acc1_c, back.acc1_c)] {
            prop_assert!(l == b || (l.is_nan() && b.is_nan()), "{} != {}", l, b);
        }
    }

    // -----------------------------------------------------------------------
    // Decode totality: arbitrary wire input must not abort the process
    // -----------------------------------------------------------------------

    #[test]
    fn arbitrary_aggregator_config_never_aborts(
        aggregator_type in any_wire_discriminant(AGGREGATION_TYPE_MAX),
        operator in any_wire_discriminant(COMPARISON_OPERATOR_MAX),
        value in any_value(),
        has_filter in any::<bool>(),
    ) {
        let wire = FanoutAggregatorConfig {
            aggregator_type,
            value_filter: has_filter.then_some(FanoutValueComparisonFilter { operator, value }),
        };
        // Either outcome is correct; not aborting is the property.
        let _ = AggregatorConfig::try_from(wire);
    }

    #[test]
    fn arbitrary_value_comparison_filter_never_aborts(
        operator in any_wire_discriminant(COMPARISON_OPERATOR_MAX),
        value in any_value(),
    ) {
        let _ = ValueComparisonFilter::try_from(FanoutValueComparisonFilter { operator, value });
    }

    #[test]
    fn arbitrary_aggregation_options_never_aborts(
        aggregator_types in prop::collection::vec(
            any_wire_discriminant(AGGREGATION_TYPE_MAX), 0..20
        ),
        bucket_duration in any::<u32>(),
        alignment_timestamp in any::<i64>(),
        bucket_timestamp_type in any_wire_discriminant(BUCKET_TIMESTAMP_MAX),
        bucket_alignment in any_wire_discriminant(BUCKET_ALIGNMENT_MAX),
        report_empty in any::<bool>(),
    ) {
        let wire = FanoutAggregationOptions {
            aggregators: aggregator_types
                .into_iter()
                .map(|aggregator_type| FanoutAggregatorConfig {
                    aggregator_type,
                    value_filter: None,
                })
                .collect(),
            bucket_duration,
            alignment_timestamp,
            bucket_timestamp_type,
            bucket_alignment,
            report_empty,
        };
        let _ = AggregationOptions::try_from(wire);
    }

    /// Includes inverted ranges (`start > end`), which used to be an `.expect()`.
    #[test]
    fn arbitrary_date_range_never_aborts(start in any::<i64>(), end in any::<i64>()) {
        let _ = TimestampRange::try_from(DateRange { start, end });
    }

    /// Garbage payload bytes under an arbitrary compression discriminant. This
    /// only reaches the chunk decoder, which rejects almost every random byte
    /// string — see the companion property below for the discriminant path.
    #[test]
    fn arbitrary_sample_data_bytes_never_abort(
        version in any::<u32>(),
        compression in any_wire_discriminant(COMPRESSION_TYPE_MAX),
        data in prop::collection::vec(any::<u8>(), 0..64),
    ) {
        let wire = super::generated::SampleData { version, compression, data };
        let _ = deserialize_chunk(&wire);
    }

    /// A *valid* chunk payload carrying an arbitrary compression discriminant —
    /// which is what a peer actually sends. This is the shape that mattered: the
    /// zero discriminant is `_UNSPECIFIED`, and it used to panic on every
    /// `TS.MRANGE` response decode.
    ///
    /// Feeding random bytes instead would not test this at all, because the chunk
    /// decoder rejects them before the discriminant is ever converted.
    #[test]
    fn valid_chunk_with_arbitrary_compression_never_aborts(
        samples in valid_samples(),
        compression in any_wire_discriminant(COMPRESSION_TYPE_MAX),
    ) {
        let chunk = samples_to_chunk_lossless(samples);
        let mut wire = serialize_chunk(chunk).expect("a real chunk must serialize");
        // Keep the encoder's payload, override only the declared encoding.
        wire.compression = compression;
        let _ = deserialize_chunk(&wire);
    }
}
