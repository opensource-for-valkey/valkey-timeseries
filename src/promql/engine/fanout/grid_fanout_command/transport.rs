//! How a shard's grid response travels: which series ship as raw spans and
//! which as the per-series stage's output or a group's partials, sized from
//! the wire layout in `promql.proto`.

use crate::labels::InternedLabel;
use crate::promql::EvalLabels;
use crate::promql::engine::query_reader::GridRequest;
use crate::promql::exec::aggregations::{AggregationKind, PushdownStrategy};
use crate::promql::hashers::FingerprintHashMap;
use crate::promql::model::RangeSample;
use crate::series::chunks::WIRE_COMPRESSION_MIN_SAMPLES;

/// Whether a series is smaller as its raw span than as one point per window
/// end — `step` finer than the sample cadence — in which case shipping the
/// span and letting the coordinator run the per-series stage is the cheaper
/// transfer.
///
/// This is the whole rule for a request answered in series. A request fused
/// with a reduction answers in per-`(group, step)` partials, whose size does
/// not depend on how many series fed a group, so there the unit of decision
/// is the group: see [`transport_plan`].
pub(super) fn ships_raw(window_ends: &[i64], series: &RangeSample<EvalLabels>) -> bool {
    window_ends.len() > series.samples.len()
}

/// Wire-size estimates behind the transport decision, from the message
/// layout in `promql.proto`: what a raw span and a `(group, step)` partial
/// cost on the cluster bus. They steer a heuristic, so they need to be right
/// to within the factor that separates the two forms at the boundary, not to
/// the byte.
pub(super) mod wire {
    /// A `Label` message: its own tag and length, plus a tag and a length
    /// for each of its two strings.
    pub const LABEL_OVERHEAD: u64 = 6;
    /// A raw `RangeSample` less its labels and samples: the message envelope
    /// and the `SampleData` header.
    pub const RAW_SERIES_OVERHEAD: u64 = 12;
    /// A sample in an uncompressed chunk — what a span shorter than
    /// `WIRE_COMPRESSION_MIN_SAMPLES` travels as: timestamp and value.
    pub const RAW_SAMPLE_UNCOMPRESSED: u64 = 16;
    /// A sample in a Chimp chunk on typical telemetry, the codec's own
    /// figure (see `samples_to_chunk`).
    pub const RAW_SAMPLE_COMPRESSED: u64 = 6;
    /// A `GridGroupPartial` less its labels and accumulators: the message
    /// envelope, the step as a varint, and the state's envelope and count.
    pub const PARTIAL_OVERHEAD: u64 = 16;
    /// One `double` accumulator of the state: tag and eight bytes.
    pub const ACCUMULATOR: u64 = 9;
}

/// Estimated wire bytes of `labels` as proto3 `Label` messages.
pub(super) fn labels_wire_bytes<'a>(labels: impl Iterator<Item = InternedLabel<'a>>) -> u64 {
    labels
        .map(|l| wire::LABEL_OVERHEAD + l.name.len() as u64 + l.value.len() as u64)
        .sum()
}

/// Estimated wire bytes of a series shipped raw: its labels once, then its
/// samples in whichever chunk codec their count selects.
pub(super) fn raw_wire_bytes(series: &RangeSample<EvalLabels>) -> u64 {
    let per_sample = if series.samples.len() >= WIRE_COMPRESSION_MIN_SAMPLES {
        wire::RAW_SAMPLE_COMPRESSED
    } else {
        wire::RAW_SAMPLE_UNCOMPRESSED
    };
    wire::RAW_SERIES_OVERHEAD
        + labels_wire_bytes(series.labels.iter())
        + (series.samples.len() as u64).saturating_mul(per_sample)
}

/// How many `double` accumulators a `(group, step)` partial for `kind`
/// carries on the wire: the fields [`AggregationPartial`] sets for it, since
/// proto3 omits the ones left at zero.
///
/// [`AggregationPartial`]: crate::promql::exec::partial_aggregation::AggregationPartial
pub(super) fn partial_accumulators(kind: AggregationKind) -> u64 {
    match kind {
        AggregationKind::Count | AggregationKind::Group => 0,
        AggregationKind::Min | AggregationKind::Max => 1,
        AggregationKind::Sum => 2,
        AggregationKind::Avg | AggregationKind::Stddev | AggregationKind::Stdvar => 3,
        _ => unreachable!("BUG: a fused reduction is one of the eight reductions"),
    }
}

/// The most windows one sample can land in. A sample at `t` is picked by
/// every window ending in `[t, t + backward)` — the lookback for a stepped
/// selection, the range for a rollup — and a grid at `step` has at most
/// `ceil(backward / step)` ends in that half-open span. One for a single
/// evaluation.
pub(super) fn windows_per_sample(request: &GridRequest) -> u64 {
    if request.step_ms <= 0 {
        return 1;
    }
    let backward = request.backward_ms().max(0) as u64;
    backward.div_ceil(request.step_ms as u64).max(1)
}

/// What one aggregation group of a shard's read would cost each way, from
/// labels and counts alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct GroupEstimate {
    /// One `(group, step)` partial of this group.
    pub(super) partial_bytes: u64,
    /// The members the per-series rule would ship raw, as raw spans.
    pub(super) raw_bytes: u64,
    /// Their sample count: what bounds the partials they would add.
    pub(super) raw_samples: u64,
    /// Whether a member stages under the per-series rule, so the group's
    /// partials are being shipped regardless.
    pub(super) staged: bool,
}

impl GroupEstimate {
    pub(super) fn new(partial_bytes: u64) -> Self {
        Self {
            partial_bytes,
            raw_bytes: 0,
            raw_samples: 0,
            staged: false,
        }
    }

    /// Whether the members that would travel raw are better folded into the
    /// group's partials instead.
    ///
    /// Folded, they add at most one partial per step, and at most `spread`
    /// per sample; promote when that is no more on the wire than their raw
    /// spans. A tie goes to the partials: the same bytes, and the staging and
    /// the fold stay on the shard instead of landing on the coordinator. A
    /// group that ships partials anyway (a staged member) is promoted
    /// outright — its raw members' partials land on steps it is already
    /// paying for, so they can only replace bytes, never add them.
    pub(super) fn promotes(&self, steps: u64, spread: u64) -> bool {
        if self.staged {
            return true;
        }
        let partials = steps.min(self.raw_samples.saturating_mul(spread));
        partials.saturating_mul(self.partial_bytes) <= self.raw_bytes
    }
}

/// Which of a shard's series travel raw: `true` at the index of each one.
///
/// For a request answered in series — unfused, or fused with a selecting or
/// counting operator — this is [`ships_raw`] per series. Fused with a
/// reduction, the response is per-`(group, step)` partials, whose count is
/// bounded by the grid and never grows with the series that fed a group, so
/// the decision is made per group. Per series, `sum by (job) (rate(m[5m]))`
/// over thousands of sparse series shipped every one of them raw and had the
/// coordinator stage and fold them all, one at a time, into the same handful
/// of groups the shard could have answered in a few partials per step.
///
/// So a reduction's groups are sized both ways — see
/// [`GroupEstimate::promotes`] — and a group's raw members are promoted to
/// its partials when that is the smaller form. Members that stage under the
/// per-series rule are never demoted: their span is the larger form by
/// itself. The estimate reads labels and counts only, never a sample, and
/// a read with nothing to promote costs one pass over the flags.
pub(super) fn transport_plan(
    request: &GridRequest,
    window_ends: &[i64],
    windows: &[RangeSample<EvalLabels>],
) -> Vec<bool> {
    let mut raw: Vec<bool> = windows
        .iter()
        .map(|series| ships_raw(window_ends, series))
        .collect();
    let Some(aggregation) = request
        .aggregation
        .as_ref()
        .filter(|agg| agg.strategy() == PushdownStrategy::Reduce)
    else {
        return raw;
    };
    if !raw.iter().any(|&is_raw| is_raw) {
        return raw;
    }

    let modifier = aggregation.modifier.as_ref();
    let steps = window_ends.len() as u64;
    let spread = windows_per_sample(request);
    let partial_overhead =
        wire::PARTIAL_OVERHEAD + wire::ACCUMULATOR * partial_accumulators(aggregation.kind);

    // One pass to size every group, remembering each series' group so the
    // second pass need not hash its labels again.
    let mut groups: FingerprintHashMap<GroupEstimate> = FingerprintHashMap::default();
    let mut keys = Vec::with_capacity(windows.len());
    for (series, &is_raw) in windows.iter().zip(&raw) {
        let key = series.labels.compute_grouping_key(modifier);
        keys.push(key);
        let group = groups.entry(key).or_insert_with(|| {
            GroupEstimate::new(
                partial_overhead + labels_wire_bytes(series.labels.grouping_labels(modifier)),
            )
        });
        if is_raw {
            group.raw_bytes = group.raw_bytes.saturating_add(raw_wire_bytes(series));
            group.raw_samples = group
                .raw_samples
                .saturating_add(series.samples.len() as u64);
        } else {
            group.staged = true;
        }
    }

    for (is_raw, key) in raw.iter_mut().zip(keys) {
        if *is_raw && groups[&key].promotes(steps, spread) {
            *is_raw = false;
        }
    }
    raw
}
