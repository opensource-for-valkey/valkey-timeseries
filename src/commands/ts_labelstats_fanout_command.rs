use super::fanout_codec::filters::{deserialize_matchers_list, serialize_matchers_list};
use super::fanout_codec::generated::{LabelValueCounts, StatsRequest, StatsResponse, ValueCount};
use crate::commands::DEFAULT_STATS_RESULTS_LIMIT;
use crate::commands::command_parser::LabelStatsOptions;
use crate::commands::ts_labelstats::reply_with_postings_stats;
use crate::commands::utils::get_multi_command_targets;
use crate::common::acl::check_metadata_permissions;
use crate::common::replies::ReplyContext;
use crate::fanout::{
    FanoutClientCommand, FanoutCommandResult, FanoutContext, FanoutTarget, NodeInfo,
};
use crate::series::index::{LabelCensus, PostingsStats, StatsBuilder, get_db_index};
use ahash::AHashMap;
use std::default::Default;
use valkey_module::{Context, Status, ValkeyResult};

/// The shards' label censuses summed: series counts by label name, then value. Every section of
/// the reply derives from these once all shards have answered.
#[derive(Default)]
struct StatsResults {
    series_count: u64,
    labels: AHashMap<String, AHashMap<String, u64>>,
}

pub struct LabelStatsFanoutCommand {
    options: LabelStatsOptions,
    state: StatsResults,
}

impl LabelStatsFanoutCommand {
    pub fn new(options: LabelStatsOptions) -> Self {
        let mut options = options;
        if options.limit == 0 {
            options.limit = DEFAULT_STATS_RESULTS_LIMIT;
        }

        Self {
            options,
            state: StatsResults::default(),
        }
    }
}

impl Default for LabelStatsFanoutCommand {
    fn default() -> Self {
        Self::new(LabelStatsOptions {
            limit: DEFAULT_STATS_RESULTS_LIMIT,
            ..Default::default()
        })
    }
}

impl FanoutClientCommand for LabelStatsFanoutCommand {
    type Request = StatsRequest;
    type Response = StatsResponse;

    fn name() -> &'static str {
        "label_stats"
    }

    fn get_local_response(ctx: &FanoutContext, req: StatsRequest) -> ValkeyResult<StatsResponse> {
        let matchers = deserialize_matchers_list(Some(req.filters))?;

        {
            let ctx = ctx.lock()?;
            // The coordinator checked its own node; each shard enforces its own ACL rules.
            check_metadata_permissions(&ctx)?;
        }
        // Only index data below: planning and batched counts need no GIL.
        //
        // The whole census is returned, untruncated: a shard's local top-K cannot establish the
        // global one (a value second everywhere can win overall), so the coordinator ranks after
        // summing. That makes the reply linear in the shard's distinct label-value pairs; a
        // high-cardinality label (a per-series `host` or `uid`) costs about one value per series.
        let census = get_db_index(ctx.db()).label_census_by_selectors(&matchers)?;
        Ok(census.into())
    }

    fn generate_request(&self) -> StatsRequest {
        StatsRequest {
            filters: serialize_matchers_list(&self.options.filters)
                .expect("serialize matchers list"),
        }
    }

    fn get_targets(&self, ctx: &Context) -> FanoutTarget {
        get_multi_command_targets(ctx, &self.options.tags)
    }

    fn on_response(&mut self, resp: Self::Response, _target: &NodeInfo) -> FanoutCommandResult {
        let state = &mut self.state;
        state.series_count = state.series_count.saturating_add(resp.series_count);
        for label in resp.labels {
            let values = state.labels.entry(label.name).or_default();
            for ValueCount { value, count } in label.values {
                let total = values.entry(value).or_default();
                *total = total.saturating_add(count);
            }
        }
        Ok(())
    }

    fn reply(&mut self, ctx: &ReplyContext) -> Status {
        let state = std::mem::take(&mut self.state);
        let stats = merged_stats(state, self.options.label.as_deref(), self.options.limit);
        reply_with_postings_stats(ctx, &stats);
        Status::Ok
    }
}

/// Ranks the summed censuses as a standalone read ranks its own index, so the two report the
/// same statistics for the same series.
fn merged_stats(state: StatsResults, focus_label: Option<&str>, limit: usize) -> PostingsStats {
    let mut stats = StatsBuilder::new(focus_label, limit);
    let mut pair = String::new();
    for (name, values) in &state.labels {
        for (value, &count) in values {
            pair.clear();
            pair.push_str(name);
            pair.push('=');
            pair.push_str(value);
            stats.add(name, value, &pair, count);
        }
    }
    stats.finish(state.series_count)
}

impl From<LabelCensus> for StatsResponse {
    fn from(census: LabelCensus) -> Self {
        StatsResponse {
            series_count: census.series_count,
            labels: census
                .labels
                .into_iter()
                .map(|label| LabelValueCounts {
                    name: label.name,
                    values: label
                        .values
                        .into_iter()
                        .map(|(value, count)| ValueCount { value, count })
                        .collect(),
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fanout::{NodeRole, SocketAddress};
    use crate::series::index::PostingStat;
    use std::net::{IpAddr, Ipv4Addr};

    fn target() -> NodeInfo {
        NodeInfo {
            id: Default::default(),
            shard_id: Default::default(),
            socket_address: SocketAddress {
                port: 0,
                primary_endpoint: IpAddr::V4(Ipv4Addr::LOCALHOST),
            },
            role: NodeRole::Primary,
            location: Default::default(),
            healthy: true,
        }
    }

    /// A shard's reply: `series_count` series, carrying `labels` as `(name, [(value, count)])`.
    fn census(series_count: u64, labels: &[(&str, &[(&str, u64)])]) -> StatsResponse {
        StatsResponse {
            series_count,
            labels: labels
                .iter()
                .map(|&(name, values)| LabelValueCounts {
                    name: name.to_string(),
                    values: values
                        .iter()
                        .map(|&(value, count)| ValueCount {
                            value: value.to_string(),
                            count,
                        })
                        .collect(),
                })
                .collect(),
        }
    }

    fn merge(label: Option<&str>, limit: usize, shards: &[StatsResponse]) -> PostingsStats {
        let mut command = LabelStatsFanoutCommand::new(LabelStatsOptions {
            limit,
            label: label.map(str::to_string),
            ..Default::default()
        });
        for shard in shards {
            command.on_response(shard.clone(), &target()).unwrap();
        }
        merged_stats(
            std::mem::take(&mut command.state),
            command.options.label.as_deref(),
            command.options.limit,
        )
    }

    fn ranked(stats: &[PostingStat]) -> Vec<(&str, u64)> {
        stats.iter().map(|s| (s.name.as_str(), s.count)).collect()
    }

    /// The winner is second on every shard, and ties at the cutoff resolve by name whatever the
    /// order responses arrive in.
    #[test]
    fn limit_applies_after_summing_shards() {
        let shards = [
            census(6, &[("host", &[("a", 3), ("common", 2), ("x", 1)])]),
            census(6, &[("host", &[("b", 3), ("common", 2), ("y", 1)])]),
        ];
        for reversed in [false, true] {
            let mut order = shards.to_vec();
            if reversed {
                order.reverse();
            }
            let stats = merge(Some("host"), 3, &order);
            assert_eq!(
                ranked(stats.series_count_by_focus_label_value.as_deref().unwrap()),
                [("common", 4), ("a", 3), ("b", 3)]
            );
            assert_eq!(
                ranked(&stats.series_count_by_label_value_pairs),
                [("host=common", 4), ("host=a", 3), ("host=b", 3)]
            );
        }
    }

    /// Every section derives from the summed pairs: a term on several shards counts once in the
    /// distinct totals, and a label name's count is the sum of its values'.
    #[test]
    fn every_section_derives_from_the_summed_pairs() {
        let shards = [
            census(
                3,
                &[
                    ("__name__", &[("cpu", 2), ("mem", 1)]),
                    ("host", &[("a", 2), ("shared", 1)]),
                ],
            ),
            census(
                2,
                &[
                    ("__name__", &[("cpu", 2)]),
                    ("host", &[("shared", 2)]),
                    ("zone", &[("z1", 1)]),
                ],
            ),
        ];
        let stats = merge(None, 10, &shards);
        assert_eq!(stats.series_count, 5);
        assert_eq!(stats.label_count, 3);
        assert_eq!(stats.total_label_value_pairs, 5);
        assert_eq!(
            ranked(&stats.series_count_by_metric_name),
            [("cpu", 4), ("mem", 1)]
        );
        assert_eq!(
            ranked(&stats.series_count_by_label_name),
            [("__name__", 5), ("host", 5), ("zone", 1)]
        );
        assert_eq!(
            ranked(&stats.series_count_by_label_value_pairs),
            [
                ("__name__=cpu", 4),
                ("host=shared", 3),
                ("host=a", 2),
                ("__name__=mem", 1),
                ("zone=z1", 1),
            ]
        );
        assert!(stats.series_count_by_focus_label_value.is_none());

        let metric_focus = merge(Some(""), 10, &shards);
        assert_eq!(
            metric_focus.series_count_by_focus_label_value.as_deref(),
            Some(metric_focus.series_count_by_metric_name.as_slice())
        );
        let absent = merge(Some("missing"), 10, &shards);
        assert_eq!(absent.series_count_by_focus_label_value, Some(vec![]));
    }

    /// A shard's census goes on the wire whole, in index order.
    #[test]
    fn a_census_converts_to_a_response_whole() {
        let local = LabelCensus {
            series_count: 2,
            labels: vec![crate::series::index::LabelValues {
                name: "host".to_string(),
                values: vec![("a".to_string(), 1), ("b".to_string(), 1)],
            }],
        };
        assert_eq!(
            StatsResponse::from(local),
            census(2, &[("host", &[("a", 1), ("b", 1)])])
        );
    }
}
