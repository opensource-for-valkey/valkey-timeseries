use crate::commands::fanout_codec::chunks::{deserialize_chunk, serialize_chunk};
use crate::common::constants::METRIC_NAME_LABEL;
use crate::labels::filters::{
    FilterList, LabelFilter, OrFiltersList, PredicateMatch, PredicateValue, RegexMatcher,
    SeriesSelector,
};
use crate::labels::{InternedLabel, Label, Labels, MetricName, SeriesLabel, literal_alternatives};
use crate::parser::parse_error::ParseError;
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
use promql_parser::label::{Labels as ModifierLabels, MatchOp as PromMatchOp, Matcher, Matchers};
use promql_parser::parser::{LabelModifier, VectorSelector};
use regex::Regex;
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
        data: Some(serialize_chunk(samples_to_chunk_lossless(samples))?),
    })
}

impl From<Matcher> for LabelFilter {
    fn from(matcher: Matcher) -> Self {
        let predicate = matcher_predicate(&matcher);
        LabelFilter {
            label: matcher.name,
            matcher: predicate,
        }
    }
}

impl From<&Matcher> for LabelFilter {
    fn from(matcher: &Matcher) -> Self {
        LabelFilter {
            label: matcher.name.clone(),
            matcher: matcher_predicate(matcher),
        }
    }
}

fn matcher_predicate(matcher: &Matcher) -> PredicateMatch {
    let value = &matcher.value;
    match &matcher.op {
        PromMatchOp::Equal => PredicateMatch::Equal(literal_value(value)),
        PromMatchOp::NotEqual => PredicateMatch::NotEqual(literal_value(value)),
        PromMatchOp::Re(parsed) => regex_predicate(value, parsed),
        PromMatchOp::NotRe(parsed) => regex_predicate(value, parsed).inverse(),
    }
}

fn literal_value(value: &str) -> PredicateValue {
    if value.is_empty() {
        PredicateValue::Empty
    } else {
        PredicateValue::String(value.to_string())
    }
}

/// The `=~` predicate for `value`. PromQL regexes are fully anchored, so one
/// that only alternates literals — every derived push-down filter
/// ([`crate::promql::engine::label_profile`]) and most hand-written `a|b`
/// selectors — is an equality over the list: resolved by one postings lookup
/// per value rather than a regex run over every value of the label.
///
/// `parsed` is the regex promql-parser compiled for the same text. It is used
/// only if the module's own compile fails, which [`validate_query_regexes`]
/// rules out for every query a command accepts; falling back keeps this
/// conversion infallible instead of panicking on a selector that got past it.
fn regex_predicate(value: &str, parsed: &Regex) -> PredicateMatch {
    try_regex_predicate(value).unwrap_or_else(|_| {
        PredicateMatch::RegexEqual(RegexMatcher::new(parsed.clone(), value.to_string()))
    })
}

/// The `=~` predicate for `value`, compiled with the module's size limits.
fn try_regex_predicate(value: &str) -> Result<PredicateMatch, ParseError> {
    // The anchored empty regex matches only the empty value, which is what
    // `=""` means: the label is absent.
    if value.is_empty() {
        return Ok(PredicateMatch::Equal(PredicateValue::Empty));
    }
    if let Some(mut values) = literal_alternatives(value) {
        let value = if values.len() == 1 {
            PredicateValue::String(values.swap_remove(0))
        } else {
            PredicateValue::from(values)
        };
        return Ok(PredicateMatch::Equal(value));
    }
    RegexMatcher::create(value).map(PredicateMatch::RegexEqual)
}

/// Rejects a query whose regex matchers the module cannot compile, before
/// the query is parsed.
///
/// promql-parser compiles each `=~`/`!~` with the regex crate's defaults (a
/// 10 MiB program), on the thread that parses the command, and carries on
/// after one fails. A heavy matcher such as `\w{2000}` takes about 100 ms to
/// build or refuse, so a 4 KiB query of them held the event loop for about
/// 30 s. Under the module's 64 KiB limit the same matcher is refused in well
/// under a millisecond, and the check stops at the first refusal; what it
/// admits is small enough that the parser's second compile stays cheap.
///
/// Uses promql-parser's own lexer, so strings and comments are tokenized
/// exactly as the parser will see them. Input the lexer rejects is left for
/// the parser to report: it fails before compiling any regex.
pub(crate) fn validate_query_regexes(query: &str) -> Result<(), String> {
    use lrpar::{Lexeme, Lexer, NonStreamingLexer, Span};
    use promql_parser::parser::token::{T_EQL_REGEX, T_NEQ_REGEX, T_STRING, TokenId};
    use promql_parser::util::unquote_string;

    let Ok(lexer) = promql_parser::parser::lexer(query) else {
        return Ok(());
    };
    // The previous token (the operator, when the current one is a regex) and
    // the one before it (the label name).
    let mut previous: Option<(TokenId, Span)> = None;
    let mut label_span: Option<Span> = None;
    for lexeme in lexer.iter().flatten() {
        let token = lexeme.tok_id();
        if token == T_STRING
            && matches!(previous, Some((T_EQL_REGEX | T_NEQ_REGEX, _)))
            // Unquoting only fails on input the parser rejects as well.
            && let Ok(pattern) = unquote_string(lexer.span_str(lexeme.span()))
            && try_regex_predicate(&pattern).is_err()
        {
            let label = label_span.map_or("", |span| lexer.span_str(span));
            let label = unquote_string(label).unwrap_or_else(|_| label.to_string());
            return Err(format!(
                "TSDB: the regex for label '{label}' is invalid or too large"
            ));
        }
        label_span = previous.map(|(_, span)| span);
        previous = Some((token, lexeme.span()));
    }
    Ok(())
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

/// One group's state from a peer's partials, or why it is corrupt.
///
/// A shard sends a partial only for a group it saw at least one sample of, so
/// a partial with no state, or with a zero count, is a corrupt reply. It used
/// to decode as the zero state: merging that into a group seen elsewhere
/// changes nothing, but a group seen only here was created with no samples and
/// finalized into a phantom series (`sum` 0, `count` 0, `group` 1, NaN for the
/// others).
pub(in crate::promql) fn decode_partial_state(
    state: Option<ProtoAggregationPartialState>,
) -> Result<AggregationPartial, &'static str> {
    match state {
        None => Err("a partial with no state"),
        Some(state) if state.count == 0 => Err("a partial that counts no samples"),
        Some(state) => Ok(state.into()),
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
            label_name_refs: Vec::new(),
            label_value_refs: Vec::new(),
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
    use crate::labels::filters::MatchOp;
    use crate::series::chunks::ChunkOps;
    use promql_parser::label::{MatchOp as PromMatchOp, Matcher, Matchers};
    use promql_parser::parser::{Expr, VectorSelector};
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

    /// The matchers of the first selector in `query`, as the parser built them.
    fn parsed_matchers(query: &str) -> Vec<Matcher> {
        match promql_parser::parser::parse(query).expect("valid query") {
            Expr::VectorSelector(vs) => vs.matchers.matchers,
            other => panic!("expected a vector selector, got {other:?}"),
        }
    }

    /// `=~""` is valid PromQL (the label is absent) and used to panic here.
    #[test]
    fn test_empty_regex_matches_only_the_absent_label() {
        let matchers = parsed_matchers(r#"up{job=~"", env!~""}"#);
        let job = LabelFilter::from(&matchers[0]);
        assert_eq!(job.matcher, PredicateMatch::Equal(PredicateValue::Empty));
        assert!(job.matches(""));
        assert!(!job.matches("api"));

        let env = LabelFilter::from(&matchers[1]);
        assert_eq!(env.matcher, PredicateMatch::NotEqual(PredicateValue::Empty));
        assert!(env.matches("prod"));
        assert!(!env.matches(""));
    }

    /// A pattern promql-parser accepts under the regex crate's 10 MiB default
    /// but the module's 64 KiB limit refuses used to panic in the conversion.
    /// A command now rejects it at parse time, and the conversion itself falls
    /// back to the parser's regex rather than panicking.
    #[test]
    fn test_regex_over_the_module_size_limit() {
        let query = r#"up{host=~"[a-z]{3000}"}"#;
        let matchers = parsed_matchers(query);
        assert!(
            RegexMatcher::create("[a-z]{3000}").is_err(),
            "must exceed the module limit"
        );

        let filter = LabelFilter::from(&matchers[0]);
        assert_eq!(filter.op(), MatchOp::RegexEqual);
        assert!(filter.matches(&"a".repeat(3000)));
        assert!(!filter.matches("a"));

        let err = validate_query_regexes(query).expect_err("rejected before parsing");
        assert_eq!(
            err,
            "TSDB: the regex for label 'host' is invalid or too large"
        );

        // Inside a range selector, a subquery, an `or` branch; negated; single-quoted,
        // backquoted and quoted-name forms; and not the first matcher.
        for query in [
            r#"rate(up{host=~"[a-z]{3000}"}[5m])"#,
            r#"max_over_time(sum(up{host=~"[a-z]{3000}"})[10m:1m])"#,
            r#"up{job="a"} or on() up{host!~"[a-z]{3000}"}"#,
            r#"up{host=~'[a-z]{3000}'}"#,
            r#"up{host=~`[a-z]{3000}`}"#,
            r#"{"host"=~"[a-z]{3000}"}"#,
            r#"up{job="a", host=~"[a-z]{3000}"}"#,
        ] {
            let err = validate_query_regexes(query).expect_err(query);
            assert!(err.contains("'host'"), "{query}: {err}");
        }
        // The same text where it is not a matcher: an ordinary string argument, a
        // comment, and a string that merely contains `=~`.
        for query in [
            r#"up{host=~"a|b", job=~""}"#,
            r#"rate(up{host=~"web-[0-9]+"}[5m])"#,
            r#"label_replace(up, "dst", "$1", "src", "[a-z]{3000}")"#,
            "up # host=~\"[a-z]{3000}\"",
            r#"label_join(up, "dst", "=~", "[a-z]{3000}")"#,
        ] {
            assert!(validate_query_regexes(query).is_ok(), "{query}");
        }
        // Everyday Unicode classes fit the 64 KiB limit (16 KiB refused them); two
        // unbounded `\w` runs still do not.
        for pattern in [r"\w+", r"\pL+", r"(?i)\w+", r"\b\w+\b", r"web_stg_\w+"] {
            let query = format!(r#"up{{host=~"{}"}}"#, pattern.replace('\\', r"\\"));
            assert!(validate_query_regexes(&query).is_ok(), "{query}");
        }
        assert!(validate_query_regexes(r#"up{host=~"\\w+-\\w+"}"#).is_err());

        // Input the lexer rejects is left for the parser to report.
        assert!(validate_query_regexes(r#"up{host=~"unterminated}"#).is_ok());
    }

    /// A 4 KiB query of heavy matchers used to hold the parsing thread for about
    /// 30 s (each one ~100 ms to build or refuse under the parser's 10 MiB
    /// default). The check refuses the first one and stops.
    #[test]
    fn test_heavy_regex_matchers_are_refused_quickly() {
        let matcher = r#"a=~"\\w{2000}""#;
        let query = format!(
            "up{{{}}}",
            vec![matcher; 4000 / (matcher.len() + 1)].join(",")
        );
        assert!(query.len() <= 4096);
        let started = std::time::Instant::now();
        assert!(validate_query_regexes(&query).is_err());
        assert!(
            started.elapsed() < std::time::Duration::from_secs(1),
            "took {:?}",
            started.elapsed()
        );
    }

    #[test]
    fn test_literal_alternation_regex_becomes_a_list_equality() {
        let re =
            |value: &str| PromMatchOp::Re(regex::Regex::new(&format!("^(?:{value})$")).unwrap());
        let not_re =
            |value: &str| PromMatchOp::NotRe(regex::Regex::new(&format!("^(?:{value})$")).unwrap());
        let list = |values: &[&str]| {
            PredicateValue::from(values.iter().map(|v| v.to_string()).collect::<Vec<_>>())
        };

        let filter = LabelFilter::from(Matcher::new(re(r"a|b\.c"), "host", r"a|b\.c"));
        assert_eq!(filter.matcher, PredicateMatch::Equal(list(&["a", "b.c"])));
        assert!(filter.matches("b.c"));
        assert!(!filter.matches("bxc"));
        assert!(!filter.matches("ab"));
        assert!(!filter.matches(""));

        let filter = LabelFilter::from(&Matcher::new(not_re("a|b"), "host", "a|b"));
        assert_eq!(filter.matcher, PredicateMatch::NotEqual(list(&["a", "b"])));
        assert!(filter.matches(""));
        assert!(!filter.matches("a"));

        let filter = LabelFilter::from(&Matcher::new(re("api"), "job", "api"));
        assert_eq!(
            filter.matcher,
            PredicateMatch::Equal(PredicateValue::String("api".into()))
        );

        // Anything but plain literals keeps the regex, including an empty
        // alternative, which the list form would not match.
        for value in ["a.c", "a|", "server[0-9]+"] {
            let filter = LabelFilter::from(&Matcher::new(re(value), "host", value));
            assert_eq!(filter.op(), MatchOp::RegexEqual, "{value:?}");
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
