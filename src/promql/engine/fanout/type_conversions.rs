use crate::commands::fanout_codec::chunks::{deserialize_chunk, serialize_chunk};
use crate::common::constants::METRIC_NAME_LABEL;
use crate::labels::filters::{
    FilterList, LabelFilter, MatchOp, OrFiltersList, PredicateMatch, PredicateValue, RegexMatcher,
    SeriesSelector,
};
use crate::labels::{InternedLabel, Label, Labels, MetricName, SeriesLabel};
use crate::promql::exec::aggregations::AggregationKind;
use crate::promql::exec::partial_aggregation::AggregationPartial;
use crate::promql::exec::types::EvalLabels;
use crate::promql::generated::{
    AggregationGrouping as ProtoAggregationGrouping, AggregationKind as ProtoAggregationKind,
    AggregationPartialState as ProtoAggregationPartialState, InstantSample as ProtoInstantSample,
    Label as ProtoLabel, RangeSample as ProtoRangeSample, SeriesSelector as ProtoSeriesSelector,
};
use crate::promql::{EvalSample, RangeSample};
use crate::series::chunks::{TimeSeriesChunk, UncompressedChunk, samples_to_chunk_lossless};
use promql_parser::label::{Labels as ModifierLabels, Matcher, Matchers};
use promql_parser::parser::{LabelModifier, VectorSelector};
use valkey_module::{ValkeyError, ValkeyResult};

impl From<InternedLabel<'_>> for ProtoLabel {
    fn from(label: InternedLabel) -> Self {
        ProtoLabel {
            name: label.name().to_string(),
            value: label.value().to_string(),
        }
    }
}

impl From<MetricName> for Vec<ProtoLabel> {
    fn from(metric_name: MetricName) -> Self {
        metric_name.iter().map(ProtoLabel::from).collect()
    }
}

/// The same encoding for a materialized label set as the [`MetricName`] impl
/// above produces for the index's own representation, so a series carries the
/// same labels on the wire whichever side built it.
impl From<&Labels> for Vec<ProtoLabel> {
    fn from(labels: &Labels) -> Self {
        labels.iter().map(ProtoLabel::from).collect()
    }
}

pub(in crate::promql) fn proto_labels_to_labels(labels: Vec<ProtoLabel>) -> Labels {
    Labels::new(labels.into_iter().map(Label::from).collect())
}

pub(in crate::promql) fn metric_name_to_proto_labels(metric_name: &MetricName) -> Vec<ProtoLabel> {
    metric_name.iter().map(ProtoLabel::from).collect()
}

impl From<ProtoInstantSample> for EvalSample {
    fn from(proto: ProtoInstantSample) -> Self {
        let labels = proto
            .labels
            .into_iter()
            .map(|l| Label {
                name: l.name,
                value: l.value,
            })
            .collect();

        EvalSample {
            timestamp_ms: proto.timestamp,
            value: proto.value,
            labels: EvalLabels::shared(labels),
            drop_name: false,
        }
    }
}

/// A series as it crosses the wire: labels, and its samples still in the
/// chunk a shard packed them into. Decoding is the caller's, so a limit can
/// be checked against [`TimeSeriesChunk::len`] before any sample is
/// materialized.
pub(in crate::promql) struct WireRangeSeries {
    pub labels: EvalLabels,
    pub chunk: TimeSeriesChunk,
}

impl WireRangeSeries {
    pub fn decode(self) -> RangeSample<EvalLabels> {
        RangeSample {
            labels: self.labels,
            samples: self.chunk.iter().collect(),
        }
    }
}

impl TryFrom<ProtoRangeSample> for WireRangeSeries {
    type Error = ValkeyError;

    fn try_from(proto: ProtoRangeSample) -> Result<Self, Self::Error> {
        let chunk = match proto.data {
            Some(data) => deserialize_chunk(&data)?,
            // A series with no chunk has no samples.
            None => TimeSeriesChunk::Uncompressed(UncompressedChunk::default()),
        };
        Ok(WireRangeSeries {
            labels: proto_labels_to_eval_labels(proto.labels),
            chunk,
        })
    }
}

impl TryFrom<ProtoRangeSample> for RangeSample<EvalLabels> {
    type Error = ValkeyError;

    fn try_from(proto: ProtoRangeSample) -> Result<Self, Self::Error> {
        WireRangeSeries::try_from(proto).map(WireRangeSeries::decode)
    }
}

/// The wire form of a series' samples: packed with the chunk codec the
/// `TS.MRANGE` fan-out uses, so both push-downs ship the same bytes.
pub(in crate::promql) fn range_sample_to_proto(
    series: RangeSample<EvalLabels>,
) -> ValkeyResult<ProtoRangeSample> {
    let RangeSample { labels, samples } = series;
    Ok(ProtoRangeSample {
        labels: (&labels).into(),
        key: String::new(),
        data: Some(serialize_chunk(samples_to_chunk_lossless(samples))?),
    })
}

impl From<Matcher> for LabelFilter {
    fn from(matcher: Matcher) -> Self {
        let operator = match matcher.op {
            promql_parser::label::MatchOp::Equal => MatchOp::Equal,
            promql_parser::label::MatchOp::NotEqual => MatchOp::NotEqual,
            promql_parser::label::MatchOp::Re(_re) => MatchOp::RegexEqual,
            promql_parser::label::MatchOp::NotRe(_re) => MatchOp::RegexNotEqual,
        };
        let predicate = match operator {
            MatchOp::Equal | MatchOp::NotEqual => {
                let value = if matcher.value.is_empty() {
                    PredicateValue::Empty
                } else {
                    PredicateValue::String(matcher.value.clone())
                };
                if operator == MatchOp::Equal {
                    PredicateMatch::Equal(value)
                } else {
                    PredicateMatch::NotEqual(value)
                }
            }
            MatchOp::RegexEqual | MatchOp::RegexNotEqual => {
                // For regex matchers, an empty value doesn't make sense. We can choose to treat it as matching nothing or everything.
                // Here we will treat it as matching nothing (i.e., it won't match any series).
                if matcher.value.is_empty() {
                    panic!("Invalid regex matcher with empty value");
                }
                let regex_matcher =
                    RegexMatcher::create(&matcher.value).expect("Failed to create regex matcher");
                if operator == MatchOp::RegexEqual {
                    PredicateMatch::RegexEqual(regex_matcher)
                } else {
                    PredicateMatch::RegexNotEqual(regex_matcher)
                }
            }
            _ => unreachable!("All match operators should be covered in the match statement above"),
        };

        LabelFilter {
            label: matcher.name,
            matcher: predicate,
        }
    }
}

impl From<&Matcher> for LabelFilter {
    fn from(matcher: &Matcher) -> Self {
        let operator = match &matcher.op {
            promql_parser::label::MatchOp::Equal => MatchOp::Equal,
            promql_parser::label::MatchOp::NotEqual => MatchOp::NotEqual,
            promql_parser::label::MatchOp::Re(_re) => MatchOp::RegexEqual,
            promql_parser::label::MatchOp::NotRe(_re) => MatchOp::RegexNotEqual,
        };
        let predicate = match operator {
            MatchOp::Equal | MatchOp::NotEqual => {
                let value = if matcher.value.is_empty() {
                    PredicateValue::Empty
                } else {
                    PredicateValue::String(matcher.value.clone())
                };
                if operator == MatchOp::Equal {
                    PredicateMatch::Equal(value)
                } else {
                    PredicateMatch::NotEqual(value)
                }
            }
            MatchOp::RegexEqual | MatchOp::RegexNotEqual => {
                if matcher.value.is_empty() {
                    panic!("Invalid regex matcher with empty value");
                }
                let regex_matcher =
                    RegexMatcher::create(&matcher.value).expect("Failed to create regex matcher");
                if operator == MatchOp::RegexEqual {
                    PredicateMatch::RegexEqual(regex_matcher)
                } else {
                    PredicateMatch::RegexNotEqual(regex_matcher)
                }
            }
            _ => unreachable!("All match operators should be covered in the match statement above"),
        };

        LabelFilter {
            label: matcher.name.clone(),
            matcher: predicate,
        }
    }
}

impl From<Matchers> for SeriesSelector {
    fn from(matchers: Matchers) -> Self {
        if !matchers.matchers.is_empty() {
            let mut filters = FilterList::default();
            for filter in matchers.matchers.into_iter().map(|m| m.into()) {
                filters.push(filter);
            }
            SeriesSelector::And(filters)
        } else if !matchers.or_matchers.is_empty() {
            let mut or_list: OrFiltersList = OrFiltersList::default();
            for and_filter in matchers.or_matchers.into_iter() {
                let mut filters = FilterList::default();
                for filter in and_filter.into_iter().map(|m| m.into()) {
                    filters.push(filter);
                }
                or_list.push(filters);
            }
            SeriesSelector::Or(or_list)
        } else {
            // If there are no matchers, we can return an empty And selector (or we could define a separate variant for this case)
            SeriesSelector::And(FilterList::default())
        }
    }
}

impl From<VectorSelector> for SeriesSelector {
    fn from(vs: VectorSelector) -> Self {
        let mut selector = SeriesSelector::from(vs.matchers);
        if let Some(name) = vs.name {
            let name_filter = LabelFilter::equals(METRIC_NAME_LABEL.to_string(), &name);
            match &mut selector {
                SeriesSelector::And(filters) => {
                    filters.insert(0, name_filter);
                }
                SeriesSelector::Or(or_list) => {
                    for filters in or_list.iter_mut() {
                        filters.insert(0, name_filter.clone());
                    }
                }
            }
        }
        selector
    }
}

impl From<&VectorSelector> for SeriesSelector {
    fn from(vs: &VectorSelector) -> Self {
        // Convert from borrowed VectorSelector by reusing the existing From<&Matchers>
        // implementation to build the base selector, then prepend the __name__ filter
        let mut selector = SeriesSelector::from(&vs.matchers);
        if let Some(ref name) = vs.name {
            let name_filter = LabelFilter::equals(METRIC_NAME_LABEL.to_string(), name);
            match &mut selector {
                SeriesSelector::And(filters) => {
                    filters.insert(0, name_filter);
                }
                SeriesSelector::Or(or_list) => {
                    for filters in or_list.iter_mut() {
                        filters.insert(0, name_filter.clone());
                    }
                }
            }
        }
        selector
    }
}

impl From<&Matchers> for SeriesSelector {
    fn from(matchers: &Matchers) -> Self {
        if !matchers.matchers.is_empty() {
            let mut filters = FilterList::default();
            for filter in matchers.matchers.iter().map(LabelFilter::from) {
                filters.push(filter);
            }
            SeriesSelector::And(filters)
        } else if !matchers.or_matchers.is_empty() {
            let mut or_list: OrFiltersList = OrFiltersList::default();
            for and_filter in matchers.or_matchers.iter() {
                let mut filters = FilterList::default();
                for filter in and_filter.iter().map(LabelFilter::from) {
                    filters.push(filter);
                }
                or_list.push(filters);
            }
            SeriesSelector::Or(or_list)
        } else {
            SeriesSelector::And(FilterList::default())
        }
    }
}

// A PromQL selector reaches the wire the same way a TS.* one does: through the
// local `SeriesSelector`, then through the `filters.proto` encoding. PromQL used
// to carry its own four-operator `LabelMatcher` message; the shared one is a
// superset of it, and going through a single encoder is what keeps the two
// halves of the contract from drifting apart again.
impl From<&Matchers> for ProtoSeriesSelector {
    fn from(matchers: &Matchers) -> Self {
        (&SeriesSelector::from(matchers)).into()
    }
}

impl From<VectorSelector> for ProtoSeriesSelector {
    fn from(vs: VectorSelector) -> Self {
        (&SeriesSelector::from(vs)).into()
    }
}

impl From<&VectorSelector> for ProtoSeriesSelector {
    fn from(vs: &VectorSelector) -> Self {
        (&SeriesSelector::from(vs)).into()
    }
}

// ── Aggregation push-down ──────────────────────────────────────────────────
// Wire conversions for `AggregationFanoutCommand`: the operator, its grouping
// modifier, the mergeable partial states, and the sample type the selection
// operators ship.

impl From<AggregationKind> for ProtoAggregationKind {
    fn from(kind: AggregationKind) -> Self {
        match kind {
            AggregationKind::Sum => ProtoAggregationKind::Sum,
            AggregationKind::Avg => ProtoAggregationKind::Avg,
            AggregationKind::Min => ProtoAggregationKind::Min,
            AggregationKind::Max => ProtoAggregationKind::Max,
            AggregationKind::Count => ProtoAggregationKind::Count,
            AggregationKind::Group => ProtoAggregationKind::Group,
            AggregationKind::Stddev => ProtoAggregationKind::Stddev,
            AggregationKind::Stdvar => ProtoAggregationKind::Stdvar,
            AggregationKind::Topk => ProtoAggregationKind::Topk,
            AggregationKind::Bottomk => ProtoAggregationKind::Bottomk,
            AggregationKind::CountValues => ProtoAggregationKind::CountValues,
            AggregationKind::Limitk => ProtoAggregationKind::Limitk,
            AggregationKind::LimitRatio => ProtoAggregationKind::LimitRatio,
            // Never sent: quantile has no decomposable form, so the
            // coordinator does not push it down (`pushdown_strategy`).
            AggregationKind::Quantile => unreachable!(
                "BUG: quantile is not a push-down operator and has no wire representation"
            ),
        }
    }
}

/// `None` for `AGGREGATION_KIND_UNSPECIFIED` — the value a peer produces when it
/// omits the field. Callers treat that the same way they treat an operator they
/// do not recognize: answer `applied = false` and let the coordinator aggregate.
impl TryFrom<ProtoAggregationKind> for AggregationKind {
    type Error = ValkeyError;

    fn try_from(kind: ProtoAggregationKind) -> Result<Self, Self::Error> {
        Ok(match kind {
            ProtoAggregationKind::Sum => AggregationKind::Sum,
            ProtoAggregationKind::Avg => AggregationKind::Avg,
            ProtoAggregationKind::Min => AggregationKind::Min,
            ProtoAggregationKind::Max => AggregationKind::Max,
            ProtoAggregationKind::Count => AggregationKind::Count,
            ProtoAggregationKind::Group => AggregationKind::Group,
            ProtoAggregationKind::Stddev => AggregationKind::Stddev,
            ProtoAggregationKind::Stdvar => AggregationKind::Stdvar,
            ProtoAggregationKind::Topk => AggregationKind::Topk,
            ProtoAggregationKind::Bottomk => AggregationKind::Bottomk,
            ProtoAggregationKind::CountValues => AggregationKind::CountValues,
            ProtoAggregationKind::Limitk => AggregationKind::Limitk,
            ProtoAggregationKind::LimitRatio => AggregationKind::LimitRatio,
            ProtoAggregationKind::Unspecified => {
                return Err(ValkeyError::Str(
                    "TSDB: aggregation push-down request carries no operator",
                ));
            }
        })
    }
}

impl From<&LabelModifier> for ProtoAggregationGrouping {
    fn from(modifier: &LabelModifier) -> Self {
        match modifier {
            LabelModifier::Include(labels) => ProtoAggregationGrouping {
                without: false,
                labels: labels.labels.to_vec(),
            },
            LabelModifier::Exclude(labels) => ProtoAggregationGrouping {
                without: true,
                labels: labels.labels.to_vec(),
            },
        }
    }
}

impl From<ProtoAggregationGrouping> for LabelModifier {
    fn from(grouping: ProtoAggregationGrouping) -> Self {
        let labels = ModifierLabels::new(grouping.labels.iter().map(String::as_str).collect());
        if grouping.without {
            LabelModifier::Exclude(labels)
        } else {
            LabelModifier::Include(labels)
        }
    }
}

impl From<AggregationPartial> for ProtoAggregationPartialState {
    fn from(state: AggregationPartial) -> Self {
        ProtoAggregationPartialState {
            count: state.count,
            acc1: state.acc1,
            acc2: state.acc2,
            acc1_compensation: state.acc1_c,
        }
    }
}

impl From<ProtoAggregationPartialState> for AggregationPartial {
    fn from(state: ProtoAggregationPartialState) -> Self {
        AggregationPartial {
            count: state.count,
            acc1: state.acc1,
            acc2: state.acc2,
            acc1_c: state.acc1_compensation,
        }
    }
}

impl From<EvalSample> for ProtoInstantSample {
    fn from(sample: EvalSample) -> Self {
        ProtoInstantSample {
            labels: sample.labels.iter().map(ProtoLabel::from).collect(),
            value: sample.value,
            timestamp: sample.timestamp_ms,
            // Aggregated output does not belong to a single key.
            key: String::new(),
        }
    }
}

/// Rebuild the label set of a group from the wire.
impl From<&EvalLabels> for Vec<ProtoLabel> {
    fn from(labels: &EvalLabels) -> Self {
        labels.iter().map(ProtoLabel::from).collect()
    }
}

pub(in crate::promql) fn proto_labels_to_eval_labels(labels: Vec<ProtoLabel>) -> EvalLabels {
    // Already sorted: the sender derived them from a sorted label set.
    EvalLabels::shared(labels.into_iter().map(Label::from).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Sample;
    use crate::series::chunks::ChunkOps;
    use promql_parser::label::{MatchOp as PromMatchOp, Matcher, Matchers};
    use promql_parser::parser::VectorSelector;
    use prost::Message;

    fn series(samples: Vec<Sample>) -> RangeSample<EvalLabels> {
        RangeSample {
            labels: EvalLabels::from_pairs(&[("__name__", "cpu"), ("host", "a")]),
            samples,
        }
    }

    /// A raw series crosses the wire as a chunk and comes back sample for
    /// sample — below the compression threshold (uncompressed), above it
    /// (Chimp), with NaN, and empty.
    #[test]
    fn range_sample_round_trips_through_its_chunk() {
        let shapes: Vec<Vec<Sample>> = vec![
            Vec::new(),
            (0..5).map(|i| Sample::new(i * 1000, i as f64)).collect(),
            (0..3600)
                .map(|i| Sample::new(1_700_000_000_000 + i * 1000, (i as f64).sin() * 100.0))
                .collect(),
            vec![
                Sample::new(0, f64::NAN),
                Sample::new(1000, 1.0),
                Sample::new(2000, f64::NAN),
            ],
        ];
        for samples in shapes {
            let proto = range_sample_to_proto(series(samples.clone())).unwrap();
            let back = RangeSample::<EvalLabels>::try_from(proto).unwrap();
            assert_eq!(
                back.labels.to_string(),
                series(Vec::new()).labels.to_string()
            );
            assert_eq!(back.samples.len(), samples.len());
            for (a, b) in back.samples.iter().zip(&samples) {
                assert_eq!(a.timestamp, b.timestamp);
                assert!(a.value == b.value || (a.value.is_nan() && b.value.is_nan()));
            }
        }
    }

    /// The point of shipping chunks: an hour of 1 s telemetry is a fraction
    /// of the 18 bytes per sample the message form cost. The length is
    /// known before decoding, which is what the limit checks rely on.
    #[test]
    fn a_chunked_series_is_small_and_knows_its_length() {
        let samples: Vec<Sample> = (0..3600)
            .map(|i| {
                Sample::new(
                    1_700_000_000_000 + i * 1000,
                    40.0 + ((i % 60) as f64) * 0.25,
                )
            })
            .collect();
        let proto = range_sample_to_proto(series(samples)).unwrap();
        let bytes_per_sample = proto.encoded_len() as f64 / 3600.0;
        assert!(bytes_per_sample < 6.0, "{bytes_per_sample:.2} B/sample");
        let wire = WireRangeSeries::try_from(proto).unwrap();
        assert_eq!(wire.chunk.len(), 3600);
    }

    /// A chunk that does not decode is an error, not an empty series.
    #[test]
    fn a_corrupt_chunk_is_refused() {
        let proto = ProtoRangeSample {
            data: Some(crate::promql::generated::SampleData {
                version: 1,
                compression: 2,
                data: vec![0xFF, 0x00, 0x13],
            }),
            ..Default::default()
        };
        assert!(RangeSample::<EvalLabels>::try_from(proto).is_err());
        // No chunk at all is a series with no samples.
        let empty = RangeSample::<EvalLabels>::try_from(ProtoRangeSample::default()).unwrap();
        assert!(empty.samples.is_empty());
    }

    /// A PromQL selector now rides the shared `filters.proto` encoding rather
    /// than a four-operator message of its own. This pins the composition: what
    /// the shard decodes has to be what the coordinator meant, for the regex and
    /// negated forms as much as for plain equality.
    #[test]
    fn test_vector_selector_survives_the_shared_wire_encoding() {
        let vs = VectorSelector {
            name: Some("http_requests_total".to_string()),
            matchers: Matchers {
                matchers: vec![
                    Matcher::new(PromMatchOp::Equal, "job", "api"),
                    Matcher::new(PromMatchOp::NotEqual, "env", "dev"),
                    Matcher::new(
                        PromMatchOp::Re(RegexMatcher::create("server[0-9]+").unwrap().regex),
                        "instance",
                        "server[0-9]+",
                    ),
                ],
                or_matchers: vec![],
            },
            offset: None,
            at: None,
        };

        let expected = SeriesSelector::from(&vs);
        let wire = ProtoSeriesSelector::from(&vs);
        let decoded = SeriesSelector::try_from(&wire).expect("selector should decode");

        assert_eq!(decoded, expected);
        // The `__name__` filter the metric name expands into has to come back
        // too — dropping it would silently widen the selector to every metric.
        match &decoded {
            SeriesSelector::And(filters) => {
                assert_eq!(filters.len(), 4);
                assert_eq!(filters[0].label, METRIC_NAME_LABEL);
                assert!(filters[0].matches("http_requests_total"));
                assert!(!filters[0].matches("other_metric"));
                assert!(filters[3].matches("server42"));
                assert!(!filters[3].matches("laptop"));
            }
            other => panic!("expected SeriesSelector::And, got {other:?}"),
        }
    }

    #[test]
    fn test_vector_selector_to_series_selector_with_name() {
        let vs = VectorSelector {
            name: Some("http_requests_total".to_string()),
            matchers: Matchers {
                matchers: vec![Matcher::new(PromMatchOp::Equal, "job", "api")],
                or_matchers: vec![],
            },
            offset: None,
            at: None,
        };

        let selector = SeriesSelector::from(vs);
        match selector {
            SeriesSelector::And(filters) => {
                assert_eq!(filters.len(), 2);
                assert_eq!(filters[0].label, "__name__");
                assert_eq!(filters[0].op(), MatchOp::Equal);
                assert!(filters[0].matches("http_requests_total"));
                assert_eq!(filters[1].label, "job");
                assert_eq!(filters[1].op(), MatchOp::Equal);
                assert!(filters[1].matches("api"));
            }
            _ => panic!("Expected SeriesSelector::And"),
        }
    }

    #[test]
    fn test_vector_selector_to_series_selector_without_name() {
        let vs = VectorSelector {
            name: None,
            matchers: Matchers {
                matchers: vec![Matcher::new(PromMatchOp::Equal, "job", "api")],
                or_matchers: vec![],
            },
            offset: None,
            at: None,
        };

        let selector = SeriesSelector::from(vs);
        match selector {
            SeriesSelector::And(filters) => {
                assert_eq!(filters.len(), 1);
                assert_eq!(filters[0].label, "job");
            }
            _ => panic!("Expected SeriesSelector::And"),
        }
    }

    #[test]
    fn test_vector_selector_to_series_selector_with_or_matchers() {
        let vs = VectorSelector {
            name: Some("http_requests_total".to_string()),
            matchers: Matchers {
                matchers: vec![],
                or_matchers: vec![
                    vec![Matcher::new(PromMatchOp::Equal, "job", "api")],
                    vec![Matcher::new(PromMatchOp::Equal, "job", "worker")],
                ],
            },
            offset: None,
            at: None,
        };

        let selector = SeriesSelector::from(vs);
        match selector {
            SeriesSelector::Or(or_list) => {
                assert_eq!(or_list.len(), 2);
                for filters in or_list.iter() {
                    assert_eq!(filters.len(), 2);
                    assert_eq!(filters[0].label, "__name__");
                    assert!(filters[0].matches("http_requests_total"));
                }
                assert!(or_list[0][1].matches("api"));
                assert!(or_list[1][1].matches("worker"));
            }
            _ => panic!("Expected SeriesSelector::Or"),
        }
    }
}
