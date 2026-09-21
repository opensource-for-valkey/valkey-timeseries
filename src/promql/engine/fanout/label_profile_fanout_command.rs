//! Cluster-wide label profile of a selector, for the coordinator's derived
//! filter push-down (`engine::derived_filters`).
//!
//! Each shard profiles the series it holds for the selector — how many
//! there are, and per label how many carry it and with which values — and
//! the coordinator merges the shards' answers. A series lives on exactly one
//! shard, so the counts add and "carried by every series" is exact after
//! the merge; the value sets union, and either side's overflow is the
//! whole's. No sample is read anywhere.
//!
//! A shard that matched more series than the request's cap says so rather
//! than answering, and the merged profile is then unavailable: the
//! coordinator leaves the selector as written, which is always safe.

use crate::fanout::{
    FanoutCommand, FanoutCommandResult, FanoutContext, FanoutError, NodeInfo,
    get_cluster_command_timeout, log_fanout_failure,
};
use crate::labels::filters::SeriesSelector;
use crate::promql::engine::fanout::query_utils::local_label_profile;
use crate::promql::engine::label_profile::{LabelProfile, LabelProfileBuilder, LabelValueProfile};
use crate::promql::generated::{
    LabelProfileQuery, LabelProfileResponse, LabelValueProfile as ProtoLabelValueProfile,
    SeriesSelector as ProtoSeriesSelector,
};
use promql_parser::label::Matchers;
use std::time::Duration;
use valkey_module::ValkeyResult;

pub struct LabelProfileFanoutCommand {
    matchers: Matchers,
    max_series: u64,
    timeout: Duration,
    merged: LabelProfileBuilder,
    /// A shard declined (more series than `max_series`): the merged profile
    /// is unavailable whatever the others said.
    unavailable: bool,
}

impl Default for LabelProfileFanoutCommand {
    fn default() -> Self {
        Self::new(Matchers::empty(), 0, get_cluster_command_timeout())
    }
}

impl LabelProfileFanoutCommand {
    pub fn new(matchers: Matchers, max_series: u64, timeout: Duration) -> Self {
        Self {
            matchers,
            max_series,
            timeout,
            merged: LabelProfileBuilder::new(),
            unavailable: false,
        }
    }

    /// The shards' profiles as one, or `None` when any shard declined or the
    /// total is past the cap.
    pub fn into_result(self) -> Option<LabelProfile> {
        if self.unavailable || (self.max_series > 0 && self.merged.series() > self.max_series) {
            return None;
        }
        Some(self.merged.finish())
    }
}

impl From<LabelProfile> for LabelProfileResponse {
    fn from(profile: LabelProfile) -> Self {
        LabelProfileResponse {
            series: profile.series,
            labels: profile
                .labels
                .into_iter()
                .map(|label| ProtoLabelValueProfile {
                    name: label.name,
                    carried_by: label.carried_by,
                    values: label.values,
                    overflow: label.overflow,
                })
                .collect(),
            overflow: false,
        }
    }
}

impl From<LabelProfileResponse> for LabelProfile {
    fn from(resp: LabelProfileResponse) -> Self {
        LabelProfile {
            series: resp.series,
            labels: resp
                .labels
                .into_iter()
                .map(|label| LabelValueProfile {
                    name: label.name,
                    carried_by: label.carried_by,
                    values: label.values,
                    overflow: label.overflow,
                })
                .collect(),
        }
    }
}

impl FanoutCommand for LabelProfileFanoutCommand {
    type Request = LabelProfileQuery;
    type Response = LabelProfileResponse;

    fn name() -> &'static str {
        "label-profile"
    }

    fn get_local_response(
        ctx: &FanoutContext,
        req: LabelProfileQuery,
    ) -> ValkeyResult<LabelProfileResponse> {
        let Some(selector) = req.selector else {
            ctx.log_warning(
                "Received label profile query with no selector, returning empty response",
            );
            return Ok(LabelProfileResponse::default());
        };
        let series_selector: SeriesSelector = (&selector).try_into()?;
        let ctx = ctx.lock()?;
        Ok(
            match local_label_profile(&ctx, series_selector, req.max_series as usize)? {
                Some(profile) => profile.into(),
                None => LabelProfileResponse {
                    overflow: true,
                    ..Default::default()
                },
            },
        )
    }

    fn get_timeout(&self) -> Duration {
        self.timeout
    }

    fn generate_request(&self) -> LabelProfileQuery {
        LabelProfileQuery {
            selector: Some(ProtoSeriesSelector::from(&self.matchers)),
            max_series: self.max_series,
        }
    }

    fn on_response(&mut self, resp: Self::Response, _target: &NodeInfo) -> FanoutCommandResult {
        if resp.overflow {
            self.unavailable = true;
        } else if !self.unavailable {
            // A shard whose value list is longer than it claims complete is
            // corrupt; the builder re-applies the cap regardless, so the
            // merge stays well-formed either way.
            self.merged.merge(LabelProfile::from(resp));
        }
        Ok(())
    }

    fn on_error(&mut self, error: FanoutError, target: &NodeInfo) {
        log_fanout_failure(Self::name(), target, &error);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::promql::engine::label_profile::MAX_PUSHDOWN_VALUES;

    fn node(port: u16) -> NodeInfo {
        NodeInfo::for_test(port)
    }

    fn shard(series: &[&[(&str, &str)]]) -> LabelProfileResponse {
        let mut builder = LabelProfileBuilder::new();
        for labels in series {
            builder.add_series(labels.iter().copied());
        }
        builder.finish().into()
    }

    fn command(max_series: u64) -> LabelProfileFanoutCommand {
        LabelProfileFanoutCommand::new(Matchers::empty(), max_series, Duration::from_secs(1))
    }

    #[test]
    fn shards_merge_by_adding_counts_and_uniting_values() {
        let mut cmd = command(0);
        cmd.on_response(
            shard(&[
                &[("region", "us"), ("host", "a")],
                &[("region", "us"), ("host", "b")],
            ]),
            &node(1),
        )
        .unwrap();
        cmd.on_response(
            shard(&[&[("region", "eu"), ("host", "c"), ("rack", "1")]]),
            &node(2),
        )
        .unwrap();
        cmd.on_response(shard(&[]), &node(3)).unwrap();

        let profile = cmd.into_result().unwrap();
        assert_eq!(profile.series, 3);
        let rendered: Vec<String> = profile
            .common_filters()
            .iter()
            .map(|m| m.to_string())
            .collect();
        // `rack` is not carried by every series once the shards are combined.
        assert_eq!(rendered, vec![r#"host=~"a|b|c""#, r#"region=~"eu|us""#]);
    }

    #[test]
    fn a_declining_shard_makes_the_profile_unavailable() {
        let mut cmd = command(10);
        cmd.on_response(shard(&[&[("region", "us")]]), &node(1))
            .unwrap();
        cmd.on_response(
            LabelProfileResponse {
                overflow: true,
                ..Default::default()
            },
            &node(2),
        )
        .unwrap();
        cmd.on_response(shard(&[&[("region", "eu")]]), &node(3))
            .unwrap();
        assert!(cmd.into_result().is_none());
    }

    #[test]
    fn the_cap_applies_to_the_total_across_shards() {
        let mut cmd = command(3);
        for port in 1..=2 {
            cmd.on_response(shard(&[&[("host", "a")], &[("host", "b")]]), &node(port))
                .unwrap();
        }
        assert!(cmd.into_result().is_none());
    }

    #[test]
    fn values_overflow_across_shards_as_they_do_within_one() {
        let mut cmd = command(0);
        for shard_no in 0..2 {
            let series: Vec<Vec<(&str, &str)>> = (0..MAX_PUSHDOWN_VALUES)
                .map(|i| {
                    vec![(
                        "host",
                        Box::leak(format!("s{shard_no}-h{i}").into_boxed_str()) as &str,
                    )]
                })
                .collect();
            let refs: Vec<&[(&str, &str)]> = series.iter().map(|s| s.as_slice()).collect();
            cmd.on_response(shard(&refs), &node(shard_no as u16))
                .unwrap();
        }
        let profile = cmd.into_result().unwrap();
        assert!(profile.labels[0].overflow);
        assert!(profile.common_filters().is_empty());
    }

    #[test]
    fn request_round_trip() {
        let matchers = Matchers::new(vec![promql_parser::label::Matcher::new(
            promql_parser::label::MatchOp::Equal,
            "region",
            "us",
        )]);
        let cmd = LabelProfileFanoutCommand::new(matchers, 7, Duration::from_secs(1));
        let req = cmd.generate_request();
        assert_eq!(req.max_series, 7);
        let selector: SeriesSelector = (&req.selector.unwrap()).try_into().unwrap();
        assert_eq!(selector.to_string(), r#"{region="us"}"#);
    }
}
