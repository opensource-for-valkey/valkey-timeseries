//! Shard-side PromQL aggregation push-down.
//!
//! Without this, evaluating `sum by (job) (http_requests_total)` in a cluster
//! ships every matching series to the coordinator and reduces there. This
//! command pushes the whole aggregation — the instant-vector parameters *and*
//! the operator — to each shard, so what crosses the wire is one partial state
//! (or one candidate sample) per group per shard rather than the raw input.
//!
//! It is the PromQL counterpart of the GROUPBY/REDUCE push-down in
//! `MRangeFanoutCommand`, and follows the same three rules:
//!
//! * The coordinator decides whether to push down; shards obey the request.
//! * Responses are self-describing: a shard that cannot apply the operator says
//!   so (`applied == false`) and returns its raw instant vector, which the
//!   coordinator aggregates itself. Mixed-version clusters degrade instead of
//!   returning wrong answers.
//! * The reduction the coordinator performs on the shards' output equals the
//!   single-node aggregation over the concatenated input; see
//!   [`crate::promql::exec::partial_aggregation`] for the per-operator argument.

use crate::common::Timestamp;
use crate::fanout::{
    ErrorKind, FanoutCommand, FanoutCommandResult, FanoutError, NodeInfo,
    get_cluster_command_timeout,
};
use crate::labels::filters::SeriesSelector;
use crate::promql::engine::PROMQL_CONFIG;
use crate::promql::engine::fanout::query_utils::local_instant_eval_samples;
use crate::promql::engine::fanout::type_conversions::proto_labels_to_eval_labels;
use crate::promql::engine::query_reader::{AggregationParam, AggregationRequest};
use crate::promql::exec::aggregations::{AggregationKind, PushdownStrategy, apply_aggregation};
use crate::promql::exec::partial_aggregation::{PartialGroups, merge_count_values};
use crate::promql::generated::{
    AggregationGroupPartial, AggregationKind as ProtoAggregationKind, AggregationQuery,
    AggregationQueryResponse, InstantQuery, InstantSample as ProtoInstantSample,
    SeriesSelector as ProtoSeriesSelector, series_selector::Matchers as ProtoMatchers,
};
use crate::promql::{EvalResult, EvalSample, ExprResult};
use promql_parser::label::Matchers;
use promql_parser::parser::LabelModifier;
use std::time::Duration;
use valkey_module::{Context, ValkeyError, ValkeyResult};

/// The instant-vector half of the request: what to select, and when.
#[derive(Debug, Clone)]
pub(in crate::promql) struct InstantVectorParams {
    pub matchers: Matchers,
    pub timestamp: Timestamp,
    pub lookback_delta: u64,
    pub max_series: u64,
    pub max_points_per_series: u64,
}

impl Default for InstantVectorParams {
    fn default() -> Self {
        let lookback_delta = {
            let guard = PROMQL_CONFIG.read().unwrap();
            guard.lookback_delta.as_millis() as u64
        };
        Self {
            matchers: Matchers {
                matchers: vec![],
                or_matchers: vec![],
            },
            timestamp: 0,
            lookback_delta,
            max_series: 0,
            max_points_per_series: 0,
        }
    }
}

pub(in crate::promql) struct AggregationFanoutCommand {
    vector: InstantVectorParams,
    aggregation: AggregationRequest,
    strategy: PushdownStrategy,
    timeout: Duration,
    /// [`PushdownStrategy::Reduce`]: the shards' partial states, merged per
    /// group as they arrive.
    partials: PartialGroups,
    /// [`PushdownStrategy::Select`]/[`PushdownStrategy::CountValues`]: the
    /// shards' locally selected samples or per-value counts.
    samples: Vec<EvalSample>,
    /// Raw instant-vector samples from a shard that did not apply the operator
    /// (`applied == false`), folded in by [`Self::into_result`].
    raw: Vec<EvalSample>,
    /// Latched when a peer rejected the operation outright — an older node with
    /// no aggregation push-down at all. The caller retries without push-down
    /// rather than failing the query.
    unsupported_peer: bool,
}

impl Default for AggregationFanoutCommand {
    fn default() -> Self {
        // Sum is an arbitrary but valid reduction: this instance only ever
        // stands in for a moved-out command (see `FanoutStateInner`), never
        // accumulates.
        let aggregation = AggregationRequest {
            kind: AggregationKind::Sum,
            modifier: None,
            param: None,
            eval_timestamp: 0,
        };
        Self {
            vector: InstantVectorParams::default(),
            strategy: PushdownStrategy::Reduce,
            partials: PartialGroups::new(aggregation.kind),
            aggregation,
            timeout: get_cluster_command_timeout(),
            samples: Vec::new(),
            raw: Vec::new(),
            unsupported_peer: false,
        }
    }
}

impl AggregationFanoutCommand {
    /// # Panics
    /// If `aggregation` has no push-down strategy. Callers must check
    /// [`AggregationKind::pushdown_strategy`] first — that check is also how
    /// they decide to push down at all.
    pub fn new(
        vector: InstantVectorParams,
        aggregation: AggregationRequest,
        timeout: Duration,
    ) -> Self {
        let strategy = aggregation
            .kind
            .pushdown_strategy()
            .expect("BUG: aggregation operator cannot be pushed down");
        let kind = aggregation.kind;
        Self {
            vector,
            aggregation,
            strategy,
            timeout,
            // Only consulted for the Reduce strategy; harmless otherwise.
            partials: PartialGroups::new(if kind.is_reduction() {
                kind
            } else {
                AggregationKind::Sum
            }),
            samples: Vec::new(),
            raw: Vec::new(),
            unsupported_peer: false,
        }
    }

    /// True when at least one peer could not handle the operation: the caller
    /// should re-run the query without push-down.
    pub fn peer_unsupported(&self) -> bool {
        self.unsupported_peer
    }

    /// Reduce the shards' output to the final instant vector.
    pub fn into_result(mut self) -> EvalResult<Vec<EvalSample>> {
        let AggregationRequest {
            kind,
            ref modifier,
            ref param,
            eval_timestamp: eval_time,
        } = self.aggregation;
        let raw = std::mem::take(&mut self.raw);

        match self.strategy {
            PushdownStrategy::Reduce => {
                // A shard that shipped raw samples is compensated by running
                // the shard-side accumulation here.
                if !raw.is_empty() {
                    self.partials.accumulate(modifier.as_ref(), raw);
                }
                Ok(self.partials.finalize(eval_time))
            }
            PushdownStrategy::Select => {
                // The selection is idempotent, so raw samples simply join the
                // candidate pool: re-selecting over the union is the answer.
                let mut candidates = std::mem::take(&mut self.samples);
                candidates.extend(raw);
                apply_aggregation(
                    kind,
                    modifier.as_ref(),
                    param.as_ref().map(AggregationParam::to_expr_result),
                    candidates,
                    eval_time,
                )
            }
            PushdownStrategy::CountValues => {
                let mut counts = std::mem::take(&mut self.samples);
                if !raw.is_empty() {
                    // Count the raw samples first, then merge those counts.
                    counts.extend(apply_aggregation(
                        kind,
                        modifier.as_ref(),
                        param.as_ref().map(AggregationParam::to_expr_result),
                        raw,
                        eval_time,
                    )?);
                }
                Ok(merge_count_values(counts))
            }
        }
    }

    fn accumulate_partials(&mut self, partials: Vec<AggregationGroupPartial>) {
        for partial in partials {
            let labels = proto_labels_to_eval_labels(partial.labels);
            self.partials
                .merge(labels, partial.state.unwrap_or_default().into());
        }
    }
}

impl FanoutCommand for AggregationFanoutCommand {
    type Request = AggregationQuery;
    type Response = AggregationQueryResponse;

    fn name() -> &'static str {
        "query-aggregation"
    }

    fn get_local_response(
        ctx: &Context,
        req: AggregationQuery,
    ) -> ValkeyResult<AggregationQueryResponse> {
        let Some(query) = req.query else {
            ctx.log_warning(
                "Received aggregation query with no instant-vector parameters, returning empty response",
            );
            return Ok(AggregationQueryResponse::default());
        };
        let Some(selector) = query.selector else {
            ctx.log_warning(
                "Received aggregation query with no selector, returning empty response",
            );
            return Ok(AggregationQueryResponse::default());
        };
        let series_selector: SeriesSelector = selector.try_into()?;
        let samples = local_instant_eval_samples(
            ctx,
            series_selector,
            query.timestamp,
            query.lookback_delta,
            query.max_series,
        )?;

        // An unrecognized operator means the coordinator is newer than this
        // node. Ship the raw instant vector and let it aggregate: correct, at
        // the cost of the transfer the push-down would have saved.
        let Some((kind, strategy)) = decode_kind(req.kind) else {
            ctx.log_warning(&format!(
                "Unsupported aggregation kind {} in pushed-down query; returning the raw instant vector",
                req.kind
            ));
            return Ok(raw_response(samples));
        };

        let modifier = req.grouping.map(LabelModifier::from);
        match strategy {
            PushdownStrategy::Reduce => {
                let mut groups = PartialGroups::new(kind);
                groups.accumulate(modifier.as_ref(), samples);
                Ok(AggregationQueryResponse {
                    partials: groups
                        .into_partials()
                        .map(|(labels, state)| AggregationGroupPartial {
                            labels: labels.iter().map(Into::into).collect(),
                            state: Some(state.into()),
                        })
                        .collect(),
                    samples: Vec::new(),
                    applied: true,
                })
            }
            PushdownStrategy::Select | PushdownStrategy::CountValues => {
                let param = request_param(req.scalar_param, req.label_param);
                let aggregated =
                    apply_aggregation(kind, modifier.as_ref(), param, samples, req.eval_timestamp)
                        .map_err(|e| ValkeyError::String(e.to_string()))?;
                Ok(AggregationQueryResponse {
                    partials: Vec::new(),
                    samples: aggregated.into_iter().map(Into::into).collect(),
                    applied: true,
                })
            }
        }
    }

    fn get_timeout(&self) -> Duration {
        self.timeout
    }

    fn generate_request(&self) -> AggregationQuery {
        let matchers: ProtoMatchers =
            ProtoMatchers::try_from(&self.vector.matchers).expect("invalid matchers");
        let query = InstantQuery {
            selector: Some(ProtoSeriesSelector {
                matchers: Some(matchers),
            }),
            timestamp: self.vector.timestamp,
            lookback_delta: self.vector.lookback_delta,
            max_series: self.vector.max_series,
            max_points_per_series: self.vector.max_points_per_series,
        };

        let (scalar_param, label_param) = match &self.aggregation.param {
            Some(AggregationParam::Scalar(value)) => (Some(*value), None),
            Some(AggregationParam::Label(label)) => (None, Some(label.clone())),
            None => (None, None),
        };

        AggregationQuery {
            query: Some(query),
            kind: ProtoAggregationKind::from(self.aggregation.kind) as i32,
            grouping: self.aggregation.modifier.as_ref().map(Into::into),
            scalar_param,
            label_param,
            eval_timestamp: self.aggregation.eval_timestamp,
        }
    }

    fn on_response(&mut self, resp: Self::Response, target: &NodeInfo) -> FanoutCommandResult {
        if !resp.applied {
            // Self-describing response from a peer that did not aggregate.
            self.raw
                .extend(resp.samples.into_iter().map(EvalSample::from));
            return Ok(());
        }

        match self.strategy {
            PushdownStrategy::Reduce => {
                // Corrupt-peer defense: a reduction response carries partials
                // only. Aggregating stray samples as if they were reduced
                // values would silently double-count.
                if !resp.samples.is_empty() {
                    return Err(FanoutError::custom(format!(
                        "TSDB: peer {} returned {} samples for a pushed-down {:?} reduction, which ships partial states",
                        target.socket_address,
                        resp.samples.len(),
                        self.aggregation.kind,
                    )));
                }
                self.accumulate_partials(resp.partials);
            }
            PushdownStrategy::Select | PushdownStrategy::CountValues => {
                if !resp.partials.is_empty() {
                    return Err(FanoutError::custom(format!(
                        "TSDB: peer {} returned {} partial states for a pushed-down {:?}, which ships samples",
                        target.socket_address,
                        resp.partials.len(),
                        self.aggregation.kind,
                    )));
                }
                self.samples
                    .extend(resp.samples.into_iter().map(EvalSample::from));
            }
        }
        Ok(())
    }

    fn on_error(&mut self, error: FanoutError, target: &NodeInfo) {
        // A peer that does not know this operation at all (rolling upgrade)
        // rejects the envelope rather than answering it. Latch that so the
        // caller can fall back to selecting the raw instant vector.
        if matches!(
            error.kind,
            ErrorKind::InvalidMessage
                | ErrorKind::UnknownMessageType
                | ErrorKind::UnsupportedFeatures
        ) {
            self.unsupported_peer = true;
        }
        crate::common::logging::log_warning(format!(
            "Fanout operation {}, failed for target {}: {error}",
            Self::name(),
            target.socket_address,
        ))
    }
}

/// A response carrying the unaggregated instant vector, for a shard that could
/// not apply the requested operator.
fn raw_response(samples: Vec<EvalSample>) -> AggregationQueryResponse {
    AggregationQueryResponse {
        partials: Vec::new(),
        samples: samples.into_iter().map(ProtoInstantSample::from).collect(),
        applied: false,
    }
}

/// Decode the operator and how to apply it, rejecting a value this node does not
/// know (proto3 decodes an unknown enum to its raw `i32`, which must not be
/// silently taken for the zero variant) as well as one that is known but never
/// pushed down. Both cases mean this node cannot honor the request.
fn decode_kind(kind: i32) -> Option<(AggregationKind, PushdownStrategy)> {
    let kind = AggregationKind::from(ProtoAggregationKind::try_from(kind).ok()?);
    Some((kind, kind.pushdown_strategy()?))
}

fn request_param(scalar: Option<f64>, label: Option<String>) -> Option<ExprResult> {
    match (scalar, label) {
        (Some(value), _) => Some(ExprResult::Scalar(value)),
        (None, Some(label)) => Some(ExprResult::String(label)),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::promql::exec::types::EvalLabels;
    use promql_parser::label::Labels as ModifierLabels;

    const EVAL_TS: Timestamp = 1_700_000_000_000;

    fn sample(labels: &[(&str, &str)], value: f64) -> EvalSample {
        EvalSample {
            labels: EvalLabels::from_pairs(labels),
            value,
            timestamp_ms: EVAL_TS,
            drop_name: false,
        }
    }

    fn node(port: u16) -> NodeInfo {
        NodeInfo::for_test(port)
    }

    fn params(kind: AggregationKind, modifier: Option<LabelModifier>) -> AggregationRequest {
        AggregationRequest {
            kind,
            modifier,
            param: None,
            eval_timestamp: EVAL_TS,
        }
    }

    fn command(aggregation: AggregationRequest) -> AggregationFanoutCommand {
        let vector = InstantVectorParams {
            timestamp: EVAL_TS,
            ..Default::default()
        };
        AggregationFanoutCommand::new(vector, aggregation, Duration::from_secs(1))
    }

    /// One shard's response, produced the way `get_local_response` produces it.
    fn shard_response(
        aggregation: &AggregationRequest,
        samples: Vec<EvalSample>,
    ) -> AggregationQueryResponse {
        let kind = aggregation.kind;
        let modifier = aggregation.modifier.as_ref();
        match kind.pushdown_strategy().unwrap() {
            PushdownStrategy::Reduce => {
                let mut groups = PartialGroups::new(kind);
                groups.accumulate(modifier, samples);
                AggregationQueryResponse {
                    partials: groups
                        .into_partials()
                        .map(|(labels, state)| AggregationGroupPartial {
                            labels: labels.iter().map(Into::into).collect(),
                            state: Some(state.into()),
                        })
                        .collect(),
                    samples: Vec::new(),
                    applied: true,
                }
            }
            PushdownStrategy::Select | PushdownStrategy::CountValues => {
                let param = aggregation
                    .param
                    .as_ref()
                    .map(AggregationParam::to_expr_result);
                let aggregated =
                    apply_aggregation(kind, modifier, param, samples, EVAL_TS).unwrap();
                AggregationQueryResponse {
                    partials: Vec::new(),
                    samples: aggregated.into_iter().map(Into::into).collect(),
                    applied: true,
                }
            }
        }
    }

    /// The full coordinator side: each shard aggregates its slice, the
    /// responses cross the wire (proto in, proto out), the coordinator reduces.
    fn fanout(aggregation: AggregationRequest, shards: &[Vec<EvalSample>]) -> Vec<(String, f64)> {
        let mut cmd = command(aggregation.clone());
        for (index, shard) in shards.iter().enumerate() {
            let response = shard_response(&aggregation, shard.clone());
            cmd.on_response(response, &node(7000 + index as u16))
                .expect("shard response accepted");
        }
        rendered(cmd.into_result().unwrap())
    }

    fn single_node(
        aggregation: &AggregationRequest,
        samples: Vec<EvalSample>,
    ) -> Vec<(String, f64)> {
        let param = aggregation
            .param
            .as_ref()
            .map(AggregationParam::to_expr_result);
        rendered(
            apply_aggregation(
                aggregation.kind,
                aggregation.modifier.as_ref(),
                param,
                samples,
                EVAL_TS,
            )
            .unwrap(),
        )
    }

    /// Results as sorted `(labels, value)` pairs so they can be compared
    /// irrespective of grouping/iteration order.
    fn rendered(samples: Vec<EvalSample>) -> Vec<(String, f64)> {
        let mut out: Vec<(String, f64)> = samples
            .into_iter()
            .map(|s| (s.labels.to_string(), s.value))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.total_cmp(&b.1)));
        out
    }

    fn test_shards() -> Vec<Vec<EvalSample>> {
        vec![
            vec![
                sample(&[("__name__", "m"), ("job", "a"), ("i", "1")], 1.0),
                sample(&[("__name__", "m"), ("job", "a"), ("i", "2")], 7.0),
                sample(&[("__name__", "m"), ("job", "b"), ("i", "3")], 3.0),
            ],
            vec![
                sample(&[("__name__", "m"), ("job", "a"), ("i", "4")], 5.0),
                sample(&[("__name__", "m"), ("job", "b"), ("i", "5")], 9.0),
            ],
            vec![sample(&[("__name__", "m"), ("job", "b"), ("i", "6")], 2.0)],
        ]
    }

    /// The push-down contract, end to end: for every operator with a strategy,
    /// the fanout result equals the single-node result over the same input.
    #[test]
    fn test_fanout_matches_single_node() {
        let shards = test_shards();
        let all: Vec<EvalSample> = shards.iter().flatten().cloned().collect();
        let by_job = LabelModifier::Include(ModifierLabels::new(vec!["job"]));

        let cases = [
            params(AggregationKind::Sum, None),
            params(AggregationKind::Sum, Some(by_job.clone())),
            params(AggregationKind::Avg, Some(by_job.clone())),
            params(AggregationKind::Min, Some(by_job.clone())),
            params(AggregationKind::Max, Some(by_job.clone())),
            params(AggregationKind::Count, Some(by_job.clone())),
            params(AggregationKind::Group, Some(by_job.clone())),
            params(AggregationKind::Stddev, Some(by_job.clone())),
            params(AggregationKind::Stdvar, Some(by_job.clone())),
            AggregationRequest {
                kind: AggregationKind::Topk,
                modifier: Some(by_job.clone()),
                param: Some(AggregationParam::Scalar(2.0)),
                eval_timestamp: EVAL_TS,
            },
            AggregationRequest {
                kind: AggregationKind::Bottomk,
                modifier: Some(by_job.clone()),
                param: Some(AggregationParam::Scalar(1.0)),
                eval_timestamp: EVAL_TS,
            },
            AggregationRequest {
                kind: AggregationKind::Topk,
                modifier: None,
                param: Some(AggregationParam::Scalar(3.0)),
                eval_timestamp: EVAL_TS,
            },
            AggregationRequest {
                kind: AggregationKind::Limitk,
                modifier: Some(by_job.clone()),
                param: Some(AggregationParam::Scalar(2.0)),
                eval_timestamp: EVAL_TS,
            },
            AggregationRequest {
                kind: AggregationKind::LimitRatio,
                modifier: Some(by_job.clone()),
                param: Some(AggregationParam::Scalar(0.5)),
                eval_timestamp: EVAL_TS,
            },
            AggregationRequest {
                kind: AggregationKind::CountValues,
                modifier: Some(by_job.clone()),
                param: Some(AggregationParam::Label("value".into())),
                eval_timestamp: EVAL_TS,
            },
        ];

        for aggregation in cases {
            let what = format!("{:?} by {:?}", aggregation.kind, aggregation.modifier);
            assert_eq!(
                single_node(&aggregation, all.clone()),
                fanout(aggregation.clone(), &shards),
                "{what}"
            );
        }
    }

    /// A peer that did not aggregate returns its raw instant vector; the
    /// coordinator must fold it in and still match the single-node result.
    #[test]
    fn test_unaggregated_peer_is_compensated() {
        let shards = test_shards();
        let all: Vec<EvalSample> = shards.iter().flatten().cloned().collect();
        let by_job = LabelModifier::Include(ModifierLabels::new(vec!["job"]));

        let cases = [
            params(AggregationKind::Sum, Some(by_job.clone())),
            params(AggregationKind::Stddev, Some(by_job.clone())),
            AggregationRequest {
                kind: AggregationKind::Topk,
                modifier: Some(by_job.clone()),
                param: Some(AggregationParam::Scalar(2.0)),
                eval_timestamp: EVAL_TS,
            },
            AggregationRequest {
                kind: AggregationKind::CountValues,
                modifier: Some(by_job.clone()),
                param: Some(AggregationParam::Label("value".into())),
                eval_timestamp: EVAL_TS,
            },
        ];

        for aggregation in cases {
            let what = format!("{:?}", aggregation.kind);
            let mut cmd = command(aggregation.clone());
            // Shard 0 speaks the protocol; shards 1 and 2 do not.
            cmd.on_response(shard_response(&aggregation, shards[0].clone()), &node(7000))
                .unwrap();
            for (index, shard) in shards.iter().enumerate().skip(1) {
                let raw = raw_response(shard.clone());
                assert!(!raw.applied);
                cmd.on_response(raw, &node(7000 + index as u16)).unwrap();
            }
            assert_eq!(
                single_node(&aggregation, all.clone()),
                rendered(cmd.into_result().unwrap()),
                "{what}"
            );
        }
    }

    /// The request carries the operator, its modifier and its parameter, plus
    /// the instant-vector parameters, and survives a proto round trip.
    #[test]
    fn test_request_round_trip() {
        use crate::fanout::serialization::{Deserialized, Serialized};

        let vector = InstantVectorParams {
            timestamp: EVAL_TS,
            lookback_delta: 300_000,
            max_series: 1_000,
            max_points_per_series: 11_000,
            ..Default::default()
        };
        let aggregation = AggregationRequest {
            kind: AggregationKind::Topk,
            modifier: Some(LabelModifier::Exclude(ModifierLabels::new(vec!["pod"]))),
            param: Some(AggregationParam::Scalar(5.0)),
            eval_timestamp: EVAL_TS,
        };
        let cmd = AggregationFanoutCommand::new(vector, aggregation, Duration::from_secs(1));

        let request = cmd.generate_request();
        let mut buf = Vec::new();
        request.serialize(&mut buf);
        let decoded = AggregationQuery::deserialize(&buf).unwrap();

        assert_eq!(decoded, request);
        assert_eq!(decoded.kind, ProtoAggregationKind::AggTopk as i32);
        assert_eq!(decoded.scalar_param, Some(5.0));
        assert_eq!(decoded.label_param, None);
        let grouping = decoded.grouping.clone().expect("grouping present");
        assert!(grouping.without);
        assert_eq!(grouping.labels, vec!["pod".to_string()]);
        assert_eq!(
            LabelModifier::from(grouping),
            LabelModifier::Exclude(ModifierLabels::new(vec!["pod"]))
        );

        let query = decoded.query.expect("instant-vector parameters present");
        assert_eq!(query.timestamp, EVAL_TS);
        assert_eq!(query.lookback_delta, 300_000);
        assert_eq!(query.max_series, 1_000);
        assert_eq!(query.max_points_per_series, 11_000);
        assert!(query.selector.is_some());
    }

    /// Only the decomposable operators are pushed down, and an operator this
    /// node does not know is never mistaken for the zero variant.
    #[test]
    fn test_decode_kind() {
        assert_eq!(
            decode_kind(0),
            Some((AggregationKind::Sum, PushdownStrategy::Reduce))
        );
        assert_eq!(
            decode_kind(ProtoAggregationKind::AggLimitRatio as i32),
            Some((AggregationKind::LimitRatio, PushdownStrategy::Select))
        );
        // Beyond the enum: a newer coordinator's operator.
        assert_eq!(decode_kind(99), None);
        assert_eq!(decode_kind(-1), None);
    }

    /// A response that carries the wrong payload shape for the request's
    /// strategy is rejected instead of being counted.
    #[test]
    fn test_mismatched_payload_is_rejected() {
        let aggregation = params(AggregationKind::Sum, None);
        let mut cmd = command(aggregation.clone());
        let stray = AggregationQueryResponse {
            partials: Vec::new(),
            samples: vec![sample(&[("job", "a")], 1.0).into()],
            applied: true,
        };
        assert!(cmd.on_response(stray, &node(7000)).is_err());

        let topk = AggregationRequest {
            kind: AggregationKind::Topk,
            modifier: None,
            param: Some(AggregationParam::Scalar(1.0)),
            eval_timestamp: EVAL_TS,
        };
        let mut cmd = command(topk);
        let stray = AggregationQueryResponse {
            partials: vec![AggregationGroupPartial::default()],
            samples: Vec::new(),
            applied: true,
        };
        assert!(cmd.on_response(stray, &node(7000)).is_err());
    }

    /// A peer that rejects the operation outright latches the fallback flag.
    #[test]
    fn test_unsupported_peer_latches_fallback() {
        let mut cmd = command(params(AggregationKind::Sum, None));
        assert!(!cmd.peer_unsupported());
        cmd.on_error(FanoutError::invalid_message(), &node(7000));
        assert!(cmd.peer_unsupported());

        // An ordinary failure is not a version problem.
        let mut cmd = command(params(AggregationKind::Sum, None));
        cmd.on_error(FanoutError::timeout(), &node(7000));
        assert!(!cmd.peer_unsupported());
    }
}
