use super::fanout_codec::filters::{deserialize_matchers_list, serialize_matchers_list};
use super::fanout_codec::generated::{PostingStat as MPostingStat, StatsRequest, StatsResponse};
use crate::commands::DEFAULT_STATS_RESULTS_LIMIT;
use crate::commands::command_parser::LabelStatsOptions;
use crate::commands::ts_labelstats::reply_with_postings_stats;
use crate::common::threads::join;
use crate::fanout::{FanoutClientCommand, FanoutCommandResult, FanoutContext, NodeInfo};
use crate::series::index::{
    PostingStat, PostingsBitmap, PostingsStats, StatsMaxHeap, deserialize_bitmap,
    get_timeseries_index, serialize_bitmap,
};
use ahash::AHashMap;
use std::default::Default;
use std::ops::Deref;
use valkey_module::{Context, Status, ValkeyResult};

#[derive(Default)]
struct StatsResults {
    series_count: usize,
    series_count_by_metric_name: AHashMap<String, PostingStat>,
    series_count_by_label_name: AHashMap<String, PostingStat>,
    series_count_by_label_value_pairs: AHashMap<String, PostingStat>,
    series_count_by_focus_label_value: AHashMap<String, PostingStat>,
    labels_bitmap: PostingsBitmap,
    label_value_pairs_bitmap: PostingsBitmap,
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

    fn get_local_response(ctx: &Context, req: StatsRequest) -> ValkeyResult<StatsResponse> {
        let limit = req.limit as usize;
        let index_guard = get_timeseries_index(ctx);
        let index = index_guard.deref();
        let label = req.selected_label.as_deref().unwrap_or("");

        // Resolve the filter once: the counts and the label fingerprints have to be taken over
        // the same set of series, or the coordinator's distinct totals would cover series that
        // contributed nothing to the counts.
        let matchers = deserialize_matchers_list(Some(req.filters))?;
        let matching = index.matching_postings(&matchers)?;
        let matching = matching.as_ref();

        let (stats, (labels_bitmap, label_value_pairs_bitmap)) = join(
            || index.stats_restricted(matching, label, limit),
            || index.label_bitmaps_restricted(matching),
        );

        let mut response: StatsResponse = stats.into();
        response.labels_bitmap = serialize_bitmap(&labels_bitmap);
        response.label_value_pairs_bitmap = serialize_bitmap(&label_value_pairs_bitmap);

        Ok(response)
    }

    fn generate_request(&self) -> StatsRequest {
        StatsRequest {
            limit: self.options.limit as u32,
            selected_label: self.options.label.clone(),
            filters: serialize_matchers_list(&self.options.filters)
                .expect("serialize matchers list"),
        }
    }

    fn on_response(&mut self, resp: Self::Response, _target: &NodeInfo) -> FanoutCommandResult {
        // Handle the response from a remote target
        self.state.series_count += resp.series_count as usize;

        self.state.labels_bitmap |= deserialize_bitmap(&resp.labels_bitmap);
        self.state.label_value_pairs_bitmap |= deserialize_bitmap(&resp.label_value_pairs_bitmap);

        collate_stats_values(
            &mut self.state.series_count_by_metric_name,
            &resp.series_count_by_metric_name,
        );
        collate_stats_values(
            &mut self.state.series_count_by_label_name,
            &resp.series_count_by_label_name,
        );
        collate_stats_values(
            &mut self.state.series_count_by_label_value_pairs,
            &resp.series_count_by_label_value_pairs,
        );
        collate_stats_values(
            &mut self.state.series_count_by_focus_label_value,
            &resp.series_count_by_focus_label_value,
        );
        Ok(())
    }

    fn reply(&mut self, ctx: &FanoutContext) -> Status {
        let limit = self.options.limit;
        let state = std::mem::take(&mut self.state);

        // Calculate num_labels from the aggregated map to ensure uniqueness
        let label_count = state.labels_bitmap.cardinality() as usize;
        let total_label_value_pairs = state.label_value_pairs_bitmap.cardinality() as usize;
        let focused = if self.options.label.is_some() {
            Some(collect_map_values(
                state.series_count_by_focus_label_value,
                limit,
            ))
        } else {
            None
        };

        let result = PostingsStats {
            series_count: state.series_count as u64,
            label_count,
            total_label_value_pairs,
            series_count_by_metric_name: collect_map_values(
                state.series_count_by_metric_name,
                limit,
            ),
            series_count_by_label_name: collect_map_values(state.series_count_by_label_name, limit),
            series_count_by_label_value_pairs: collect_map_values(
                state.series_count_by_label_value_pairs,
                limit,
            ),
            series_count_by_focus_label_value: focused,
        };

        reply_with_postings_stats(ctx, &result);
        Status::Ok
    }
}

fn collect_map_values(map: AHashMap<String, PostingStat>, limit: usize) -> Vec<PostingStat> {
    let mut map = map;
    let mut heap = StatsMaxHeap::new(limit);
    for stat in map.drain().map(|(_, v)| v) {
        heap.push(stat);
    }
    heap.into_vec()
}

fn collate_stats_values(map: &mut AHashMap<String, PostingStat>, values: &[MPostingStat]) {
    for result in values.iter() {
        if let Some(stat) = map.get_mut(result.name.as_str()) {
            stat.count += result.count;
        } else {
            map.insert(
                result.name.clone(),
                PostingStat {
                    name: result.name.clone(),
                    count: result.count,
                },
            );
        }
    }
}
