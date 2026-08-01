use super::fanout_codec;
use super::fanout_codec::generated::{
    GroupPartialSeries, MultiRangeRequest, MultiRangeResponse, SeriesRangeResponse,
};
use crate::aggregators::MultiAggregateIterator;
use crate::aggregators::{PartialReducer, PartialState};
use crate::commands::utils::{MRangeReplyShape, reply_with_mrange_series_results};
use crate::common::{MultiSample, Sample};
use crate::fanout::{FanoutClientCommand, NodeInfo};
use crate::fanout::{FanoutCommandResult, FanoutContext};
use crate::iterators::{
    MultiSeriesRowIter, MultiSeriesSampleIter, RowReducer, create_sample_iterator_adapter,
};
use crate::series::chunks::{TimeSeriesChunk, UncompressedChunk};
use crate::series::mrange::{
    SampleLimit, build_mrange_grouped_labels, collect_rows, process_mrange_group_partials,
    process_mrange_query, sort_mrange_results,
};
use crate::series::request_types::{
    MRangeOptions, MRangeSeriesResult, RangeGroupingOptions, SeriesResultData,
};
use orx_parallel::ParIter;
use orx_parallel::ParIterResult;
use orx_parallel::{IntoParIter, IterIntoParIter};
use smallvec::SmallVec;
use std::collections::{BTreeMap, BTreeSet};
use valkey_module::{Context, Status, ValkeyError, ValkeyResult};

#[derive(Default)]
pub struct MRangeFanoutCommand {
    options: MRangeOptions,
    /// Accumulated per-series responses, each tagged with the responding
    /// shard's `applied_aggregation` echo: `true` means the payload is
    /// aggregated buckets, `false` means raw samples (e.g. a pre-push-down
    /// peer that ignored the request flag) which the coordinator must
    /// aggregate itself.
    series: Vec<(SeriesRangeResponse, bool)>,
    group_partials: Vec<GroupPartialSeries>,
    /// Shard-side aggregation push-down: shards return buckets instead of raw
    /// samples. Latched at construction so a concurrent `CONFIG SET` cannot
    /// desynchronize request generation and reply handling within one command.
    pushdown: bool,
    /// GROUPBY/REDUCE push-down: shards pre-reduce their local group members
    /// per bucket and return partial states. Only latched for decomposable
    /// reducers; others fall back to per-series bucket transport.
    pushdown_group: bool,
    /// COUNT push-down: shards apply COUNT as a head/tail pre-filter. The
    /// coordinator always re-applies COUNT, so this only bounds transfer and
    /// carries no version hazard.
    pushdown_count: bool,
}

impl MRangeFanoutCommand {
    pub fn new(options: MRangeOptions) -> Self {
        let config_enabled = crate::config::is_fanout_aggregation_pushdown_enabled();
        // Both push-downs support multi-aggregation: per-series buckets ship
        // as one chunk per aggregation column (SeriesRangeResponse.columns),
        // and group partials carry `column_count` states per bucket
        // (GroupPartialSeries), reduced column-wise with the same REDUCE type.
        let pushdown = config_enabled && options.range.aggregation.is_some();
        let pushdown_group = config_enabled
            && options
                .grouping
                .as_ref()
                .is_some_and(|g| PartialReducer::for_config(&g.aggregation).is_some());
        // COUNT can only be pre-applied shard-side when the shard streams the
        // same unit the coordinator counts: raw samples (no aggregation) or
        // pushed-down buckets. Under coordinator-side aggregation (multi),
        // truncating raw samples would corrupt partial buckets.
        let pushdown_count = config_enabled
            && options.range.count.is_some()
            && (options.range.aggregation.is_none() || pushdown);
        Self {
            options,
            series: Vec::with_capacity(8),
            group_partials: Vec::new(),
            pushdown,
            pushdown_group,
            pushdown_count,
        }
    }
}

impl FanoutClientCommand for MRangeFanoutCommand {
    type Request = MultiRangeRequest;
    type Response = MultiRangeResponse;

    fn name() -> &'static str {
        "mrange"
    }

    fn get_local_response(
        ctx: &Context,
        req: MultiRangeRequest,
    ) -> ValkeyResult<MultiRangeResponse> {
        let apply_aggregation = req.apply_aggregation;
        let apply_group_reduce = req.apply_group_reduce;
        let apply_count = req.apply_count;
        let mut options: MRangeOptions = req.try_into()?;
        // COUNT push-down: capture the requested direction and count before
        // they are reset below. The shard streams ascending, so a reverse
        // query keeps the tail (= first `count` in requested order); the
        // coordinator re-applies COUNT as the final authority either way.
        let limit = if apply_count {
            options.range.count.map(|n| {
                if options.is_reverse {
                    SampleLimit::Tail(n)
                } else {
                    SampleLimit::Head(n)
                }
            })
        } else {
            None
        };
        // These (along with grouping) will be handled after gathering all
        // results. Shards always aggregate/encode ascending; the coordinator
        // reverses bucket order and applies the authoritative COUNT.
        options.range.count = None;
        options.is_reverse = false;

        if apply_group_reduce && options.grouping.is_some() {
            // GROUPBY/REDUCE push-down: pre-reduce local group members per
            // bucket (per-series aggregation included when present) and ship
            // partial states instead of per-series samples.
            let partials = process_mrange_group_partials(ctx, options, limit)?;
            return Ok(MultiRangeResponse {
                series: Vec::new(),
                group_partials: partials.into_iter().map(Into::into).collect(),
                // Compatibility echo: group reduce implies the per-series
                // aggregation stage ran as part of the partial pipeline.
                applied_aggregation: true,
                applied_group_reduce: true,
                applied_count: apply_count,
                // No `series` in this branch, so nothing to intern.
                symbol_table_names: Vec::new(),
                symbol_table_values: Vec::new(),
            });
        }

        if !apply_aggregation {
            // Legacy flow: return raw samples; the coordinator aggregates.
            options.range.aggregation = None;
        }

        // Process the MRange query locally
        let series = process_mrange_query(ctx, options, true, limit)?;

        // Convert MRangeSeriesResult to SeriesResponse
        let mut serialized: Vec<SeriesRangeResponse> = series
            .into_iter()
            .map(|x| x.try_into())
            .collect::<Result<_, _>>()?;

        // Unconditional: the ref arrays are self-describing, so no request
        // opt-in or response echo is needed to make this decodable.
        let (symbol_table_names, symbol_table_values) =
            fanout_codec::symbol_table::intern_labels(&mut serialized);

        Ok(MultiRangeResponse {
            series: serialized,
            group_partials: Vec::new(),
            applied_aggregation: apply_aggregation,
            applied_group_reduce: false,
            applied_count: apply_count,
            symbol_table_names,
            symbol_table_values,
        })
    }

    fn generate_request(&self) -> MultiRangeRequest {
        let mut request = serialize_request(&self.options);
        request.apply_aggregation = self.pushdown;
        request.apply_group_reduce = self.pushdown_group;
        request.apply_count = self.pushdown_count;
        request
    }

    fn on_response(&mut self, resp: Self::Response, _target: &NodeInfo) -> FanoutCommandResult {
        self.ingest_response(resp).map_err(Into::into)
    }

    fn reply(&mut self, ctx: &FanoutContext) -> Status {
        self.options.range.latest = false;
        self.options.range.timestamp_filter = None;
        self.options.range.value_filter = None;

        let is_grouped = self.options.grouping.is_some();
        let series = std::mem::take(&mut self.series);
        let group_partials = std::mem::take(&mut self.group_partials);

        // Captured before process_responses, which clears aggregation options
        // under push-down (the RESP3 reply still reports the aggregator names).
        let shape = MRangeReplyShape::from_options(&self.options);

        match self.process_responses(series, group_partials) {
            Ok(mut series) => {
                sort_mrange_results(&mut series, is_grouped);
                let _ = reply_with_mrange_series_results(ctx, &series, &shape);
                Status::Ok
            }
            Err(e) => {
                let warning = format!("Error processing MRange responses: {e:?}");
                ctx.reply(Err(e));
                ctx.log_warning(&warning);
                Status::Err
            }
        }
    }
}

impl MRangeFanoutCommand {
    /// Resolve symbol-table label refs (if the shard used them) and fold the
    /// response into accumulated state. Split out from `on_response` so it can
    /// be driven directly in tests without constructing a `NodeInfo`.
    fn ingest_response(&mut self, mut resp: MultiRangeResponse) -> ValkeyResult<()> {
        // Resolve here, while the response still owns its symbol table —
        // downstream (self.series and everything built from it) never needs to
        // know interning happened. No flag to check: `resolve_labels` is driven
        // by the per-series ref arrays and is a no-op where there are none.
        fanout_codec::symbol_table::resolve_labels(
            &mut resp.series,
            &resp.symbol_table_names,
            &resp.symbol_table_values,
        )?;
        // Tag each series with the shard's applied_aggregation echo so the
        // reply path can compensate per response (compatibility handshake).
        let bucketed = resp.applied_aggregation;
        self.series
            .extend(resp.series.into_iter().map(|series| (series, bucketed)));
        self.group_partials.append(&mut resp.group_partials);
        Ok(())
    }

    /// Post-process the accumulated shard responses into the final reply
    /// series, compensating per response for shards that did not honor the
    /// push-down flags (compatibility handshake): raw series are aggregated
    /// coordinator-side, and under group-reduce push-down any per-series data
    /// is pre-reduced locally into partials before the cross-shard merge.
    fn process_responses(
        &mut self,
        series: Vec<(SeriesRangeResponse, bool)>,
        mut group_partials: Vec<GroupPartialSeries>,
    ) -> ValkeyResult<Vec<MRangeSeriesResult>> {
        let series = normalize_response_series(series, &self.options, self.pushdown)?;
        if self.pushdown && !is_multi_aggregation(&self.options) {
            // Single-aggregation push-down: every series is bucketed after
            // normalization, so clear aggregation to skip re-aggregation. The
            // remaining post-processing (GROUPBY reduce, reverse, COUNT)
            // operates on the buckets. Multi-aggregation keeps its options
            // (needed for the column count and the multi/data-variant branch);
            // its helpers detect already-bucketed rows by the `Rows` variant
            // and skip re-aggregation without clearing.
            self.options.range.aggregation = None;
        }

        if self.pushdown_group {
            // Shards honoring apply_group_reduce sent partials and no series;
            // any series present are tagged per-series data from a peer that
            // did not — pre-reduce them locally into equivalent partials.
            group_partials.extend(compensate_group_partials(series, &self.options)?);
            handle_group_partials(group_partials, &self.options)
        } else if self.options.grouping.is_some() {
            handle_grouping(series, &self.options)
        } else {
            let mut results = handle_basic(series, &self.options)?;
            // Authoritative EXCLUDEEMPTY pass. Shards already drop empty series
            // when they honor `exclude_empty`, but a peer that ignores the field
            // (or coordinator-side aggregation that empties a series) must not
            // change the reply. GROUPBY is rejected with EXCLUDEEMPTY at parse
            // time, so only this branch can see the flag.
            if self.options.exclude_empty {
                results.retain(|result| !result.data.is_empty());
            }
            Ok(results)
        }
    }
}

/// Normalize push-down heterogeneity at the wire boundary: under
/// single-aggregation push-down, a series tagged as raw (its shard did not
/// echo `applied_aggregation`) is aggregated coordinator-side so downstream
/// processing sees buckets for every series — raw and bucketed single-agg
/// payloads are otherwise indistinguishable. Multi-aggregation needs no eager
/// step: its handlers detect raw data by the `Chunk` variant and aggregate
/// lazily. Without push-down the tags are irrelevant and everything is raw.
fn normalize_response_series(
    series: Vec<(SeriesRangeResponse, bool)>,
    options: &MRangeOptions,
    pushdown: bool,
) -> ValkeyResult<Vec<SeriesRangeResponse>> {
    let needs_bucketing = pushdown && !is_multi_aggregation(options);
    if !needs_bucketing {
        return Ok(series.into_iter().map(|(response, _)| response).collect());
    }
    // Aggregate exactly as the shard would have: ascending, unbounded —
    // reversal and COUNT are applied downstream by the coordinator.
    let mut shard_range = options.range.clone();
    shard_range.count = None;
    series
        .into_par()
        .map(|(response, bucketed)| {
            if bucketed {
                return Ok(response);
            }
            let mut result = MRangeSeriesResult::try_from(response)?;
            let samples: Vec<Sample> = create_sample_iterator_adapter(
                result.data.sample_iter(),
                &shard_range,
                &None,
                false,
            )
            .collect();
            result.data = SeriesResultData::Chunk(TimeSeriesChunk::Uncompressed(
                UncompressedChunk::from_vec(samples),
            ));
            result.try_into()
        })
        .into_fallible_result()
        .collect()
}

/// Pre-reduce per-series data from shards that did not honor
/// `apply_group_reduce` into per-group partials, mirroring the shard-side
/// pipeline so `handle_group_partials` can merge them with real wire
/// partials. Input series must already be normalized (bucketed when the
/// query has AGGREGATION); with no AGGREGATION the reduce domain is raw
/// timestamps, matching standalone semantics.
fn compensate_group_partials(
    series: Vec<SeriesRangeResponse>,
    options: &MRangeOptions,
) -> ValkeyResult<Vec<GroupPartialSeries>> {
    use crate::aggregators::{PartialRowReducer, PartialSampleReducer};

    if series.is_empty() {
        return Ok(Vec::new());
    }
    let group_options = options
        .grouping
        .as_ref()
        .expect("Grouping options should be present");
    let Some(reducer) = PartialReducer::for_config(&group_options.aggregation) else {
        return Err(ValkeyError::String(format!(
            "TSDB: internal error: REDUCE {} does not support partial reduce",
            group_options.aggregation.aggregation_name()
        )));
    };

    let results = series
        .into_par()
        .map(MRangeSeriesResult::try_from)
        .into_fallible_result()
        .collect()?;
    let grouped = construct_group_map(results);

    Ok(grouped
        .into_iter()
        .map(|(label, data)| {
            let (bucket_timestamps, states, column_count) = if is_multi_aggregation(options) {
                let columns = options
                    .range
                    .aggregation
                    .as_ref()
                    .map(|a| a.aggregations.len())
                    .unwrap_or(1);
                let row_iters = data
                    .series
                    .iter()
                    .map(|s| series_rows_ascending(s, options))
                    .collect::<Vec<_>>();
                let merged = MultiSeriesRowIter::new(row_iters);
                let (timestamps, buckets): (Vec<i64>, Vec<_>) =
                    PartialRowReducer::new(merged, reducer.clone(), columns).unzip();
                let states = buckets.into_iter().flatten().collect::<Vec<_>>();
                (timestamps, states, columns)
            } else {
                let iters = data
                    .series
                    .iter()
                    .map(|s| s.data.sample_iter())
                    .collect::<Vec<_>>();
                let merged = MultiSeriesSampleIter::new(iters);
                let (timestamps, states): (Vec<i64>, Vec<PartialState>) =
                    PartialSampleReducer::new(merged, reducer.clone()).unzip();
                (timestamps, states, 1)
            };

            GroupPartialSeries {
                group_label_value: label,
                source_keys: data.keys.to_vec(),
                bucket_timestamps,
                states: states.into_iter().map(Into::into).collect(),
                column_count: column_count as u32,
            }
        })
        .collect())
}

fn handle_basic(
    series: Vec<SeriesRangeResponse>,
    options: &MRangeOptions,
) -> ValkeyResult<Vec<MRangeSeriesResult>> {
    series
        .into_par()
        .map(MRangeSeriesResult::try_from) // Explicit conversion
        .into_fallible_result()
        .map(|series| process_series_samples(series, options))
        .collect()
}

fn serialize_request(request: &MRangeOptions) -> MultiRangeRequest {
    // Note: request should have been validated on construction
    request
        .try_into()
        .expect("Failed to serialize MultiRangeRequest")
}

struct GroupData {
    keys: SmallVec<[String; 8]>,
    series: Vec<MRangeSeriesResult>,
}

type GroupMap = BTreeMap<String, GroupData>;

/// Apply GROUPBY/REDUCE to the series coming from remote nodes
fn handle_grouping(
    series: Vec<SeriesRangeResponse>,
    options: &MRangeOptions,
) -> ValkeyResult<Vec<MRangeSeriesResult>> {
    let group_options = options
        .grouping
        .as_ref()
        .expect("Grouping options should be present");
    let results = series
        .into_par()
        .map(MRangeSeriesResult::try_from)
        .into_fallible_result()
        .collect()?;
    let grouped_by_key = construct_group_map(results);

    Ok(grouped_by_key
        .into_iter()
        .iter_into_par()
        .map(|(label, data)| process_group(label, data, options, group_options))
        .collect())
}

/// Short, bounded rendering of a partial's source keys for error/log context:
/// enough to identify the owning shard without flooding the message when a
/// group has many members.
fn sources_preview(source_keys: &[String]) -> String {
    const MAX_SHOWN: usize = 3;
    let mut preview = source_keys
        .iter()
        .take(MAX_SHOWN)
        .cloned()
        .collect::<Vec<_>>()
        .join(",");
    if source_keys.len() > MAX_SHOWN {
        preview.push_str(&format!(",+{} more", source_keys.len() - MAX_SHOWN));
    }
    preview
}

/// Merge and finalize the per-(group, shard) partial states returned under
/// GROUPBY/REDUCE push-down: per group, merge states with equal bucket
/// timestamps across shards (column-wise for multi-aggregation), finalize
/// each bucket into a sample/row, then apply reversal and COUNT.
fn handle_group_partials(
    partials: Vec<GroupPartialSeries>,
    options: &MRangeOptions,
) -> ValkeyResult<Vec<MRangeSeriesResult>> {
    let group_options = options
        .grouping
        .as_ref()
        .expect("Grouping options should be present");
    let Some(reducer) = PartialReducer::for_config(&group_options.aggregation) else {
        return Err(ValkeyError::String(format!(
            "TSDB: internal error: REDUCE {} does not support partial reduce",
            group_options.aggregation.aggregation_name()
        )));
    };
    let kind = reducer.kind();
    // Multi-aggregation buckets carry one state per aggregation column, all
    // reduced with the same REDUCE kind; everything else is one column.
    let is_multi = is_multi_aggregation(options);
    let columns = if is_multi {
        options
            .range
            .aggregation
            .as_ref()
            .map(|a| a.aggregations.len())
            .unwrap_or(1)
    } else {
        1
    };

    type BucketStates = SmallVec<[PartialState; 4]>;
    type MergedGroup = (BTreeMap<i64, BucketStates>, BTreeSet<String>);
    let mut groups: BTreeMap<String, MergedGroup> = BTreeMap::new();
    for partial in partials {
        // Corrupt-peer defense: states must be row-major with exactly the
        // column count this query produces. The error carries the shape so an
        // incident log identifies the group and offending payload directly
        // (reply() logs it verbatim).
        if partial.column_count as usize != columns
            || partial.states.len() != partial.bucket_timestamps.len() * columns
        {
            return Err(ValkeyError::String(format!(
                "TSDB: invalid group partial states from peer: group '{}', column_count {} (expected {}), {} states for {} buckets, sources [{}]",
                partial.group_label_value,
                partial.column_count,
                columns,
                partial.states.len(),
                partial.bucket_timestamps.len(),
                sources_preview(&partial.source_keys),
            )));
        }
        let (buckets, sources) = groups.entry(partial.group_label_value).or_default();
        sources.extend(partial.source_keys);
        for (ts, row) in partial
            .bucket_timestamps
            .iter()
            .zip(partial.states.chunks_exact(columns))
        {
            // merge() copies `other` into a count == 0 state, so freshly
            // inserted default states merge as plain assignment.
            let merged = buckets
                .entry(*ts)
                .or_insert_with(|| (0..columns).map(|_| PartialState::default()).collect());
            for (into, state) in merged.iter_mut().zip(row.iter()) {
                PartialReducer::merge(kind, into, &state.into());
            }
        }
    }

    Ok(groups
        .into_iter()
        .map(|(label, (buckets, sources))| {
            let data = if is_multi {
                let rows = buckets.into_iter().map(|(timestamp, states)| MultiSample {
                    timestamp,
                    values: states
                        .iter()
                        .map(|state| PartialReducer::finalize(kind, state))
                        .collect(),
                });
                SeriesResultData::Rows(collect_rows(rows, options.is_reverse, options.range.count))
            } else {
                let mut samples: Vec<Sample> = buckets
                    .into_iter()
                    .map(|(timestamp, states)| Sample {
                        timestamp,
                        value: PartialReducer::finalize(kind, &states[0]),
                    })
                    .collect();
                if options.is_reverse {
                    samples.reverse();
                }
                if let Some(count) = options.range.count {
                    samples.truncate(count);
                }
                SeriesResultData::Chunk(TimeSeriesChunk::Uncompressed(UncompressedChunk::from_vec(
                    samples,
                )))
            };

            let sources: Vec<String> = sources.into_iter().collect(); // sorted via BTreeSet
            let labels = if options.with_labels {
                build_mrange_grouped_labels(
                    &group_options.group_label,
                    &label,
                    group_options.aggregation.aggregation_name(),
                    &sources,
                )
            } else {
                Vec::new()
            };

            MRangeSeriesResult {
                key: format!("{}={}", group_options.group_label, label),
                group_label_value: Some(label),
                labels,
                sources,
                data,
            }
        })
        .collect())
}

fn construct_group_map(series: Vec<MRangeSeriesResult>) -> BTreeMap<String, GroupData> {
    let mut grouped = BTreeMap::new();
    for meta in series {
        if let Some(label) = meta.group_label_value.clone() {
            let entry = grouped.entry(label).or_insert_with(|| GroupData {
                keys: SmallVec::new(),
                series: Vec::new(),
            });
            entry.keys.push(meta.key.clone());
            entry.series.push(meta);
        }
    }
    for data in grouped.values_mut() {
        data.keys.sort();
    }
    grouped
}

fn is_multi_aggregation(options: &MRangeOptions) -> bool {
    options
        .range
        .aggregation
        .as_ref()
        .is_some_and(|a| a.is_multi())
}

/// Coordinator-side multi-aggregation of one series' raw (already filtered,
/// ascending) samples into rows. Reverse/COUNT are NOT applied here; callers
/// decide per context.
fn aggregate_rows_ascending<'a>(
    samples: impl Iterator<Item = Sample> + 'a,
    options: &MRangeOptions,
) -> MultiAggregateIterator<impl Iterator<Item = Sample> + 'a> {
    let aggregation = options
        .range
        .aggregation
        .as_ref()
        .expect("multi-aggregation requires aggregation options");
    let (start_ts, end_ts) = options.range.get_timestamp_range();
    let aligned_timestamp = aggregation
        .alignment
        .get_aligned_timestamp(start_ts, end_ts);
    MultiAggregateIterator::new(samples, aggregation, aligned_timestamp)
}

/// Ascending multi-aggregation rows for one series, whichever way it arrived:
/// already-bucketed rows under aggregation push-down, or raw samples the
/// coordinator aggregates itself in the legacy/config-off path. Reverse/COUNT
/// are applied downstream.
fn series_rows_ascending<'a>(
    series: &'a MRangeSeriesResult,
    options: &'a MRangeOptions,
) -> Box<dyn Iterator<Item = MultiSample> + 'a> {
    match &series.data {
        SeriesResultData::Rows(rows) => Box::new(rows.iter().cloned()),
        SeriesResultData::Chunk(_) => {
            Box::new(aggregate_rows_ascending(series.data.sample_iter(), options))
        }
    }
}

fn process_group(
    label: String,
    data: GroupData,
    options: &MRangeOptions,
    group_options: &RangeGroupingOptions,
) -> MRangeSeriesResult {
    let result_data = if is_multi_aggregation(options) {
        // Column-wise reduce: per-series bucket rows, merged by bucket
        // timestamp, reduced independently per aggregation column. Under
        // aggregation push-down each series arrives already bucketed (Rows);
        // otherwise the coordinator aggregates the raw samples (Chunk) itself.
        let columns = options
            .range
            .aggregation
            .as_ref()
            .map(|a| a.aggregations.len())
            .unwrap_or(1);
        let row_iters = data
            .series
            .iter()
            .map(|s| series_rows_ascending(s, options))
            .collect::<Vec<_>>();
        let merged = MultiSeriesRowIter::new(row_iters);
        let reducer = RowReducer::new(
            merged,
            group_options.aggregation.create_aggregator(),
            columns,
        );
        SeriesResultData::Rows(collect_rows(
            reducer,
            options.is_reverse,
            options.range.count,
        ))
    } else {
        let samples = process_series_list(&data.series, options);
        SeriesResultData::Chunk(TimeSeriesChunk::Uncompressed(UncompressedChunk::from_vec(
            samples,
        )))
    };

    // Grouped replies report the group label, __reducer__ and __source__ only
    // under WITHLABELS; by default the label array is empty (matches the
    // standalone path, see group_series_by_label).
    let labels = if options.with_labels {
        build_mrange_grouped_labels(
            &group_options.group_label,
            &label,
            group_options.aggregation.aggregation_name(),
            &data.keys,
        )
    } else {
        Vec::new()
    };

    MRangeSeriesResult {
        key: format!("{}={}", group_options.group_label, label),
        group_label_value: Some(label),
        labels,
        sources: data.keys.to_vec(),
        data: result_data,
    }
}

fn process_series_samples(
    mut series: MRangeSeriesResult,
    options: &MRangeOptions,
) -> MRangeSeriesResult {
    if is_multi_aggregation(options) {
        let rows = collect_rows(
            series_rows_ascending(&series, options),
            options.is_reverse,
            options.range.count,
        );
        series.data = SeriesResultData::Rows(rows);
        return series;
    }
    let samples = process_series_list(std::slice::from_ref(&series), options);
    series.data = SeriesResultData::Chunk(TimeSeriesChunk::Uncompressed(
        UncompressedChunk::from_vec(samples),
    ));
    series
}

fn process_series_list(series: &[MRangeSeriesResult], options: &MRangeOptions) -> Vec<Sample> {
    let (reverse_iter, reverse_aggr) = validate_reverse(options);

    if reverse_iter {
        let mut samples: Vec<_> = series.iter().flat_map(|s| s.data.sample_iter()).collect();
        samples.sort_by_key(|b| std::cmp::Reverse(b.timestamp));
        create_sample_iterator_adapter(
            samples.into_iter(),
            &options.range,
            &options.grouping,
            reverse_aggr,
        )
        .collect()
    } else if series.len() == 1 {
        create_sample_iterator_adapter(
            series[0].data.sample_iter(),
            &options.range,
            &options.grouping,
            reverse_aggr,
        )
        .collect()
    } else {
        let iters = series
            .iter()
            .map(|s| s.data.sample_iter())
            .collect::<Vec<_>>();
        create_sample_iterator_adapter(
            MultiSeriesSampleIter::new(iters),
            &options.range,
            &options.grouping,
            reverse_aggr,
        )
        .collect()
    }
}

pub fn validate_reverse(options: &MRangeOptions) -> (bool, bool) {
    let has_aggregation = options.range.aggregation.is_some();

    // Aggregation requires ascending input order from base iterators
    // Without aggregation, base iterators can directly provide the requested order
    let should_reverse_iter = !has_aggregation && options.is_reverse;

    // Apply reversal after aggregation if needed
    let should_reverse_aggr = has_aggregation && options.is_reverse;

    (should_reverse_iter, should_reverse_aggr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregators::AggregationType;
    use crate::common::constants::SOURCE_KEY;
    use crate::series::chunks::UncompressedChunk;
    use crate::series::request_types::{
        AggregationOptions, AggregatorConfig, RangeGroupingOptions, RangeOptions,
    };

    fn samples(pairs: &[(i64, f64)]) -> Vec<Sample> {
        pairs
            .iter()
            .map(|&(timestamp, value)| Sample { timestamp, value })
            .collect()
    }

    fn series_result(key: &str, group: Option<&str>, data: Vec<Sample>) -> MRangeSeriesResult {
        MRangeSeriesResult {
            key: key.into(),
            group_label_value: group.map(String::from),
            labels: Vec::new(),
            sources: Vec::new(),
            data: SeriesResultData::Chunk(TimeSeriesChunk::Uncompressed(
                UncompressedChunk::from_vec(data),
            )),
        }
    }

    fn to_response(result: MRangeSeriesResult) -> SeriesRangeResponse {
        result.try_into().expect("serialize series result")
    }

    fn series_result_with_labels(
        key: &str,
        data: Vec<Sample>,
        labels: &[(&str, &str)],
    ) -> MRangeSeriesResult {
        MRangeSeriesResult {
            key: key.into(),
            group_label_value: None,
            labels: labels
                .iter()
                .map(|&(name, value)| crate::labels::Label {
                    name: name.into(),
                    value: value.into(),
                })
                .collect(),
            sources: Vec::new(),
            data: SeriesResultData::Chunk(TimeSeriesChunk::Uncompressed(
                UncompressedChunk::from_vec(data),
            )),
        }
    }

    fn label_pairs(series: &SeriesRangeResponse) -> Vec<(String, String)> {
        series
            .labels
            .iter()
            .map(|l| (l.name.clone(), l.value.clone()))
            .collect()
    }

    fn avg_aggregation(bucket_duration: u64) -> AggregationOptions {
        AggregationOptions {
            aggregations: smallvec::smallvec![
                AggregatorConfig::new(AggregationType::Avg, None).unwrap()
            ],
            bucket_duration,
            timestamp_output: Default::default(),
            alignment: Default::default(),
            report_empty: false,
        }
    }

    fn mrange_options(start: i64, end: i64) -> MRangeOptions {
        MRangeOptions {
            range: RangeOptions::with_range(start, end).unwrap(),
            ..Default::default()
        }
    }

    fn result_samples(result: &MRangeSeriesResult) -> Vec<Sample> {
        result.data.sample_iter().collect()
    }

    /// Simulate what a push-down shard returns: the per-series aggregation
    /// pipeline applied to the raw samples.
    fn shard_aggregate(raw: Vec<Sample>, options: &MRangeOptions) -> Vec<Sample> {
        create_sample_iterator_adapter(raw.into_iter(), &options.range, &None, false).collect()
    }

    /// Push-down equivalence: shard-side bucketing + coordinator post-processing
    /// with aggregation cleared must equal coordinator-side aggregation of the
    /// raw samples.
    #[test]
    fn test_pushdown_basic_equivalence() {
        let raw = samples(&[(0, 1.0), (10, 3.0), (110, 5.0), (120, 7.0), (250, 9.0)]);

        for (is_reverse, count) in [
            (false, None),
            (true, None),
            (true, Some(2)),
            (false, Some(1)),
        ] {
            let mut legacy_options = mrange_options(0, 1000);
            legacy_options.range.aggregation = Some(avg_aggregation(100));
            legacy_options.range.count = count;
            legacy_options.is_reverse = is_reverse;

            let legacy = handle_basic(
                vec![to_response(series_result("a", None, raw.clone()))],
                &legacy_options,
            )
            .unwrap();

            // Shard aggregates ascending with COUNT/reverse stripped, exactly
            // as get_local_response does under push-down.
            let mut shard_options = legacy_options.clone();
            shard_options.range.count = None;
            shard_options.is_reverse = false;
            let buckets = shard_aggregate(raw.clone(), &shard_options);

            // Coordinator with aggregation cleared (reply() under push-down)
            let mut coord_options = legacy_options.clone();
            coord_options.range.aggregation = None;
            let pushed = handle_basic(
                vec![to_response(series_result("a", None, buckets))],
                &coord_options,
            )
            .unwrap();

            assert_eq!(legacy.len(), 1);
            assert_eq!(pushed.len(), 1);
            assert_eq!(
                result_samples(&legacy[0]),
                result_samples(&pushed[0]),
                "reverse={is_reverse} count={count:?}"
            );
        }
    }

    /// Grouped push-down: the coordinator reduces per-series buckets column-wise
    /// per timestamp (the standalone "aggregate per series, then reduce" order).
    /// Also covers the GroupData.keys fix: group key and __source__ label.
    #[test]
    fn test_pushdown_grouped_reduce() {
        let make_responses = || {
            // Per-series buckets, as returned by push-down shards
            vec![
                to_response(series_result(
                    "a",
                    Some("us"),
                    samples(&[(0, 1.0), (100, 3.0)]),
                )),
                to_response(series_result(
                    "b",
                    Some("us"),
                    samples(&[(0, 5.0), (100, 1.0)]),
                )),
            ]
        };

        let mut options = mrange_options(0, 1000);
        options.with_labels = true;
        // aggregation already cleared by reply() under push-down
        options.grouping = Some(RangeGroupingOptions {
            aggregation: AggregatorConfig::new(AggregationType::Max, None).unwrap(),
            group_label: "region".into(),
        });

        let results = handle_grouping(make_responses(), &options).unwrap();

        assert_eq!(results.len(), 1);
        let group = &results[0];
        assert_eq!(group.key, "region=us");
        assert_eq!(group.group_label_value.as_deref(), Some("us"));
        assert_eq!(
            result_samples(group),
            samples(&[(0, 5.0), (100, 3.0)]),
            "max of per-series buckets per timestamp"
        );

        let source = group
            .labels
            .iter()
            .find(|l| l.name == SOURCE_KEY)
            .expect("__source__ label present");
        assert_eq!(source.value, "a,b");

        // Without WITHLABELS the label array must be empty
        options.with_labels = false;
        let results = handle_grouping(make_responses(), &options).unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].labels.is_empty());
        assert_eq!(results[0].key, "region=us");
    }

    /// The request flag mirrors the latched push-down decision.
    #[test]
    fn test_generate_request_flag() {
        let mut options = mrange_options(0, 1000);
        options.range.aggregation = Some(avg_aggregation(100));

        let mut command = MRangeFanoutCommand::new(options.clone());
        command.pushdown = true;
        assert!(command.generate_request().apply_aggregation);

        command.pushdown = false;
        assert!(!command.generate_request().apply_aggregation);

        // No aggregation in the query => never pushed down, regardless of config
        options.range.aggregation = None;
        let command = MRangeFanoutCommand::new(options);
        assert!(!command.pushdown);
        assert!(!command.generate_request().apply_aggregation);
    }

    /// Fallback matrix: group-reduce push-down is latched only for
    /// decomposable reducers; others fall back to legacy transport.
    #[test]
    fn test_group_reduce_latch_and_request_flag() {
        let grouping_with = |ty: AggregationType| {
            Some(RangeGroupingOptions {
                aggregation: AggregatorConfig::new(ty, None).unwrap(),
                group_label: "region".into(),
            })
        };

        // Decomposable reducer, no AGGREGATION: Phase 2 over raw timestamps
        let mut options = mrange_options(0, 1000);
        options.grouping = grouping_with(AggregationType::Avg);
        let command = MRangeFanoutCommand::new(options.clone());
        assert!(command.pushdown_group);
        assert!(command.generate_request().apply_group_reduce);

        // Decomposable reducer + AGGREGATION: both flags set
        options.range.aggregation = Some(avg_aggregation(100));
        let command = MRangeFanoutCommand::new(options.clone());
        assert!(command.pushdown_group);
        let request = command.generate_request();
        assert!(request.apply_group_reduce);
        assert!(request.apply_aggregation);

        // Non-decomposable reducer: falls back to per-series buckets (Phase 1)
        options.grouping = grouping_with(AggregationType::Increase);
        let command = MRangeFanoutCommand::new(options.clone());
        assert!(!command.pushdown_group);
        assert!(command.pushdown, "Phase 1 still applies");
        let request = command.generate_request();
        assert!(!request.apply_group_reduce);
        assert!(request.apply_aggregation);

        // No grouping: never group-pushed
        options.grouping = None;
        let command = MRangeFanoutCommand::new(options);
        assert!(!command.pushdown_group);
    }

    /// Coordinator merge/finalize of per-(group, shard) partial states:
    /// states with equal bucket timestamps merge across shards, sources are
    /// the sorted union, reversal and COUNT apply to the finalized buckets.
    #[test]
    fn test_handle_group_partials() {
        use crate::aggregators::PartialSampleReducer;

        let mut options = mrange_options(0, 1000);
        options.with_labels = true;
        options.grouping = Some(RangeGroupingOptions {
            aggregation: AggregatorConfig::new(AggregationType::Sum, None).unwrap(),
            group_label: "region".into(),
        });
        let reducer =
            PartialReducer::for_config(&options.grouping.as_ref().unwrap().aggregation).unwrap();

        // Simulate two shards computing partials over their local group members
        let shard_partial = |keys: &[&str], shard_samples: Vec<Sample>| {
            let (bucket_timestamps, states): (Vec<i64>, Vec<PartialState>) =
                PartialSampleReducer::new(shard_samples.into_iter(), reducer.clone()).unzip();
            GroupPartialSeries {
                group_label_value: "us".into(),
                source_keys: keys.iter().map(|s| s.to_string()).collect(),
                bucket_timestamps,
                states: states.into_iter().map(Into::into).collect(),
                column_count: 1,
            }
        };

        // Shard 1 holds series a; shard 2 holds series b and c (merged locally)
        let shard1 = shard_partial(&["a"], samples(&[(0, 1.0), (100, 3.0)]));
        let shard2 = shard_partial(&["b", "c"], samples(&[(0, 4.0), (0, 5.0), (200, 7.0)]));

        let results =
            handle_group_partials(vec![shard1.clone(), shard2.clone()], &options).unwrap();

        assert_eq!(results.len(), 1);
        let group = &results[0];
        assert_eq!(group.key, "region=us");
        assert_eq!(
            result_samples(group),
            samples(&[(0, 10.0), (100, 3.0), (200, 7.0)]),
            "sum merged across shards per bucket"
        );
        let source = group
            .labels
            .iter()
            .find(|l| l.name == SOURCE_KEY)
            .expect("__source__ label present");
        assert_eq!(source.value, "a,b,c");

        // Reverse + COUNT apply to the finalized buckets
        options.is_reverse = true;
        options.range.count = Some(2);
        options.with_labels = false;
        let results = handle_group_partials(vec![shard1, shard2], &options).unwrap();
        assert_eq!(
            result_samples(&results[0]),
            samples(&[(200, 7.0), (100, 3.0)])
        );
        assert!(results[0].labels.is_empty());
    }

    fn limit_for(is_reverse: bool, count: usize) -> SampleLimit {
        if is_reverse {
            SampleLimit::Tail(count)
        } else {
            SampleLimit::Head(count)
        }
    }

    /// COUNT push-down equivalence: shard-side head/tail truncation plus the
    /// coordinator's authoritative COUNT equals the untruncated flow, for raw
    /// and aggregated transport, both directions, count below/at/above the
    /// stream length.
    #[test]
    fn test_count_pushdown_equivalence() {
        let raw = samples(&[(0, 1.0), (10, 3.0), (110, 5.0), (120, 7.0), (250, 9.0)]);

        for aggregated in [false, true] {
            for is_reverse in [false, true] {
                for count in [1usize, 2, 10] {
                    let mut coord_options = mrange_options(0, 1000);
                    coord_options.range.count = Some(count);
                    coord_options.is_reverse = is_reverse;

                    // The stream a shard would ship without COUNT push-down
                    // (raw samples, or buckets under aggregation push-down)
                    let shard_stream = if aggregated {
                        let mut shard_options = mrange_options(0, 1000);
                        shard_options.range.aggregation = Some(avg_aggregation(100));
                        shard_aggregate(raw.clone(), &shard_options)
                    } else {
                        raw.clone()
                    };

                    let reference = handle_basic(
                        vec![to_response(series_result("a", None, shard_stream.clone()))],
                        &coord_options,
                    )
                    .unwrap();

                    // COUNT push-down: shard truncates head/tail before
                    // shipping; the coordinator re-applies COUNT unchanged.
                    let truncated: Vec<Sample> = limit_for(is_reverse, count)
                        .apply(shard_stream.into_iter())
                        .collect();
                    let pushed = handle_basic(
                        vec![to_response(series_result("a", None, truncated))],
                        &coord_options,
                    )
                    .unwrap();

                    assert_eq!(
                        result_samples(&reference[0]),
                        result_samples(&pushed[0]),
                        "aggregated={aggregated} reverse={is_reverse} count={count}"
                    );
                }
            }
        }
    }

    /// Grouped subset property: with timestamps staggered across shards (the
    /// merged k-th timestamp differs from either shard's k-th), per-shard
    /// truncation to COUNT rows must not change the per-group result. Covers
    /// both grouped transports: per-series buckets and group partials.
    #[test]
    fn test_count_pushdown_grouped_subset() {
        // Staggered: merged timestamps {0, 5, 10, 15, 20}
        let series_a = samples(&[(0, 1.0), (10, 2.0), (20, 3.0)]); // shard 1
        let series_b = samples(&[(5, 4.0), (10, 5.0), (15, 6.0)]); // shard 2

        let grouping = RangeGroupingOptions {
            aggregation: AggregatorConfig::new(AggregationType::Sum, None).unwrap(),
            group_label: "region".into(),
        };

        for is_reverse in [false, true] {
            for count in [1usize, 2, 3, 10] {
                let mut options = mrange_options(0, 1000);
                options.grouping = Some(grouping.clone());
                options.range.count = Some(count);
                options.is_reverse = is_reverse;
                let limit = limit_for(is_reverse, count);

                // --- Per-series transport (Phase 1 grouped / legacy) ---
                let full = |key: &str, s: &[Sample]| {
                    to_response(series_result(key, Some("us"), s.to_vec()))
                };
                let cut = |key: &str, s: &[Sample]| {
                    to_response(series_result(
                        key,
                        Some("us"),
                        limit.apply(s.iter().copied()).collect(),
                    ))
                };

                let reference =
                    handle_grouping(vec![full("a", &series_a), full("b", &series_b)], &options)
                        .unwrap();
                let pushed =
                    handle_grouping(vec![cut("a", &series_a), cut("b", &series_b)], &options)
                        .unwrap();
                assert_eq!(
                    result_samples(&reference[0]),
                    result_samples(&pushed[0]),
                    "per-series transport reverse={is_reverse} count={count}"
                );

                // --- Group-partials transport (Phase 2) ---
                let reducer = PartialReducer::for_config(&grouping.aggregation).unwrap();
                let partial = |keys: &[&str], s: &[Sample], limit: Option<SampleLimit>| {
                    let rows = crate::aggregators::PartialSampleReducer::new(
                        s.iter().copied(),
                        reducer.clone(),
                    );
                    let (bucket_timestamps, states): (Vec<i64>, Vec<PartialState>) = match limit {
                        Some(limit) => limit.apply(rows).unzip(),
                        None => rows.unzip(),
                    };
                    GroupPartialSeries {
                        group_label_value: "us".into(),
                        source_keys: keys.iter().map(|s| s.to_string()).collect(),
                        bucket_timestamps,
                        states: states.into_iter().map(Into::into).collect(),
                        column_count: 1,
                    }
                };

                let reference = handle_group_partials(
                    vec![
                        partial(&["a"], &series_a, None),
                        partial(&["b"], &series_b, None),
                    ],
                    &options,
                )
                .unwrap();
                let pushed = handle_group_partials(
                    vec![
                        partial(&["a"], &series_a, Some(limit)),
                        partial(&["b"], &series_b, Some(limit)),
                    ],
                    &options,
                )
                .unwrap();
                assert_eq!(
                    result_samples(&reference[0]),
                    result_samples(&pushed[0]),
                    "partials transport reverse={is_reverse} count={count}"
                );
            }
        }
    }

    /// EXCLUDEEMPTY rides on the request so shards can drop empty series before
    /// shipping, but the coordinator is the authority: a peer that ignores the
    /// field (here, both shards) must not be able to put an empty series back
    /// into the reply.
    #[test]
    fn test_exclude_empty_reapplied_at_coordinator() {
        let mut options = mrange_options(0, 500);
        options.exclude_empty = true;

        let mut command = MRangeFanoutCommand::new(options);
        let request = command.generate_request();
        assert!(request.exclude_empty, "shards are told to pre-filter");
        // The shard rebuilds its options from the request, so the flag has to
        // survive both directions of the codec for the pre-filter to happen.
        assert!(
            MRangeOptions::try_from(&request).unwrap().exclude_empty
                && MRangeOptions::try_from(request).unwrap().exclude_empty
        );

        let responses = vec![
            (
                to_response(series_result("s", None, samples(&[(100, 1.0), (400, 4.0)]))),
                false,
            ),
            (to_response(series_result("u", None, Vec::new())), false),
        ];
        let results = command.process_responses(responses, Vec::new()).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].key, "s");

        // Without the flag the empty series is reported, as before.
        let mut options = mrange_options(0, 500);
        options.exclude_empty = false;
        let mut command = MRangeFanoutCommand::new(options);
        assert!(!command.generate_request().exclude_empty);
        let responses = vec![
            (
                to_response(series_result("s", None, samples(&[(100, 1.0), (400, 4.0)]))),
                false,
            ),
            (to_response(series_result("u", None, Vec::new())), false),
        ];
        let mut results = command.process_responses(responses, Vec::new()).unwrap();
        sort_mrange_results(&mut results, false);
        assert_eq!(results.len(), 2);
        assert!(results[1].data.is_empty());
    }

    /// COUNT push-down latch: flag mirrors config && COUNT presence.
    #[test]
    fn test_count_pushdown_latch() {
        let mut options = mrange_options(0, 1000);
        assert!(!MRangeFanoutCommand::new(options.clone()).pushdown_count);
        assert!(
            !MRangeFanoutCommand::new(options.clone())
                .generate_request()
                .apply_count
        );

        options.range.count = Some(5);
        let command = MRangeFanoutCommand::new(options);
        assert!(command.pushdown_count);
        assert!(command.generate_request().apply_count);
    }

    fn multi_aggregation(bucket_duration: u64) -> AggregationOptions {
        AggregationOptions {
            aggregations: smallvec::smallvec![
                AggregationType::Avg.into(),
                AggregationType::Max.into(),
                AggregationType::Count.into(),
            ],
            bucket_duration,
            timestamp_output: Default::default(),
            alignment: Default::default(),
            report_empty: false,
        }
    }

    fn result_rows(result: &MRangeSeriesResult) -> Vec<crate::common::MultiSample> {
        match &result.data {
            SeriesResultData::Rows(rows) => rows.clone(),
            SeriesResultData::Chunk(_) => panic!("expected multi-aggregation rows"),
        }
    }

    /// Multi-aggregation enables all three push-downs: per-series buckets ship
    /// as per-column chunks, group partials carry `column_count` states per
    /// bucket, and COUNT rides on either. Non-decomposable reducers still fall
    /// back to per-series bucket transport, exactly as for single-aggregation.
    #[test]
    fn test_multi_aggregation_pushdown_flags() {
        let mut options = mrange_options(0, 1000);
        options.range.aggregation = Some(multi_aggregation(100));
        options.range.count = Some(5);
        options.grouping = Some(RangeGroupingOptions {
            aggregation: AggregatorConfig::new(AggregationType::Sum, None).unwrap(),
            group_label: "region".into(),
        });

        let command = MRangeFanoutCommand::new(options.clone());
        assert!(command.pushdown, "per-series aggregation push-down applies");
        assert!(
            command.pushdown_group,
            "group partial-state push-down applies column-wise"
        );
        assert!(command.pushdown_count, "COUNT push-down rides on pushdown");
        let request = command.generate_request();
        assert!(request.apply_aggregation);
        assert!(request.apply_group_reduce);
        assert!(request.apply_count);

        // non-decomposable reducer: multi falls back to per-series buckets
        options.grouping = Some(RangeGroupingOptions {
            aggregation: AggregatorConfig::new(AggregationType::Increase, None).unwrap(),
            group_label: "region".into(),
        });
        let command = MRangeFanoutCommand::new(options.clone());
        assert!(command.pushdown);
        assert!(!command.pushdown_group);
        assert!(!command.generate_request().apply_group_reduce);

        // single aggregation with the same shape behaves identically
        options.range.aggregation = Some(avg_aggregation(100));
        options.grouping = Some(RangeGroupingOptions {
            aggregation: AggregatorConfig::new(AggregationType::Sum, None).unwrap(),
            group_label: "region".into(),
        });
        let command = MRangeFanoutCommand::new(options);
        assert!(command.pushdown);
        assert!(command.pushdown_group);
        assert!(command.pushdown_count);
    }

    /// Coordinator multi-aggregation of shard raw samples: column i of the
    /// rows equals a single-aggregation run of that aggregator; reverse and
    /// COUNT apply to rows.
    #[test]
    fn test_multi_aggregation_basic() {
        let raw = samples(&[(0, 1.0), (10, 3.0), (110, 5.0), (120, 7.0), (250, 9.0)]);

        for (is_reverse, count) in [(false, None), (true, None), (true, Some(2))] {
            let mut options = mrange_options(0, 1000);
            options.range.aggregation = Some(multi_aggregation(100));
            options.range.count = count;
            options.is_reverse = is_reverse;

            let results = handle_basic(
                vec![to_response(series_result("a", None, raw.clone()))],
                &options,
            )
            .unwrap();
            assert_eq!(results.len(), 1);
            let rows = result_rows(&results[0]);

            // compare each column against the single-aggregation flow
            for (column, ty) in [
                AggregationType::Avg,
                AggregationType::Max,
                AggregationType::Count,
            ]
            .into_iter()
            .enumerate()
            {
                let mut single_options = options.clone();
                single_options.range.aggregation = Some(AggregationOptions {
                    aggregations: smallvec::smallvec![ty.into()],
                    ..multi_aggregation(100)
                });
                let single = handle_basic(
                    vec![to_response(series_result("a", None, raw.clone()))],
                    &single_options,
                )
                .unwrap();
                let singles = result_samples(&single[0]);

                assert_eq!(
                    rows.len(),
                    singles.len(),
                    "reverse={is_reverse} count={count:?}"
                );
                for (row, sample) in rows.iter().zip(singles.iter()) {
                    assert_eq!(row.timestamp, sample.timestamp);
                    assert_eq!(
                        row.values[column], sample.value,
                        "column {column} ({ty:?}) reverse={is_reverse} count={count:?}"
                    );
                }
            }
        }
    }

    /// Grouped multi-aggregation: column-wise reduce across the group's
    /// series. Hand-computed fixture: two series, avg,max buckets, REDUCE sum.
    #[test]
    fn test_multi_aggregation_grouped_column_reduce() {
        let mut options = mrange_options(0, 1000);
        options.with_labels = true;
        options.range.aggregation = Some(AggregationOptions {
            aggregations: smallvec::smallvec![
                AggregationType::Avg.into(),
                AggregationType::Max.into(),
            ],
            bucket_duration: 100,
            timestamp_output: Default::default(),
            alignment: Default::default(),
            report_empty: false,
        });
        options.grouping = Some(RangeGroupingOptions {
            aggregation: AggregatorConfig::new(AggregationType::Sum, None).unwrap(),
            group_label: "region".into(),
        });

        // series a buckets: [0,100): avg 2, max 3; [100,200): avg 5, max 5
        // series b buckets: [0,100): avg 10, max 12; [200,300): avg 7, max 7
        let responses = vec![
            to_response(series_result(
                "a",
                Some("us"),
                samples(&[(0, 1.0), (10, 3.0), (110, 5.0)]),
            )),
            to_response(series_result(
                "b",
                Some("us"),
                samples(&[(0, 8.0), (20, 12.0), (250, 7.0)]),
            )),
        ];

        let results = handle_grouping(responses, &options).unwrap();
        assert_eq!(results.len(), 1);
        let group = &results[0];
        assert_eq!(group.key, "region=us");
        let rows = result_rows(group);

        // column-wise sum across series per bucket timestamp:
        // ts 0:   avg: 2+10=12, max: 3+12=15
        // ts 100: only a       -> 5, 5
        // ts 200: only b       -> 7, 7
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].timestamp, 0);
        assert_eq!(rows[0].values.as_slice(), &[12.0, 15.0]);
        assert_eq!(rows[1].timestamp, 100);
        assert_eq!(rows[1].values.as_slice(), &[5.0, 5.0]);
        assert_eq!(rows[2].timestamp, 200);
        assert_eq!(rows[2].values.as_slice(), &[7.0, 7.0]);

        let source = group
            .labels
            .iter()
            .find(|l| l.name == SOURCE_KEY)
            .expect("__source__ label present");
        assert_eq!(source.value, "a,b");

        // MREVRANGE ordering + COUNT on rows
        options.is_reverse = true;
        options.range.count = Some(2);
        let responses = vec![
            to_response(series_result(
                "a",
                Some("us"),
                samples(&[(0, 1.0), (10, 3.0), (110, 5.0)]),
            )),
            to_response(series_result(
                "b",
                Some("us"),
                samples(&[(0, 8.0), (20, 12.0), (250, 7.0)]),
            )),
        ];
        let results = handle_grouping(responses, &options).unwrap();
        let rows = result_rows(&results[0]);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].timestamp, 200);
        assert_eq!(rows[1].timestamp, 100);
    }

    /// Simulate what a push-down shard returns for a multi-aggregation series:
    /// aggregate the raw samples ascending into bucket rows, then serialize
    /// them through the per-column transport (as `get_local_response` does).
    fn shard_multi_response(
        key: &str,
        group: Option<&str>,
        raw: &[Sample],
        options: &MRangeOptions,
    ) -> SeriesRangeResponse {
        let mut shard_options = options.clone();
        shard_options.range.count = None; // shard streams ascending, unbounded
        shard_options.is_reverse = false;
        let rows: Vec<crate::common::MultiSample> =
            aggregate_rows_ascending(raw.iter().copied(), &shard_options).collect();
        to_response(MRangeSeriesResult {
            key: key.into(),
            group_label_value: group.map(String::from),
            labels: Vec::new(),
            sources: Vec::new(),
            data: SeriesResultData::Rows(rows),
        })
    }

    /// Multi-aggregation push-down round trip (non-grouped): shard-aggregated
    /// bucket rows survive the per-column transport, and the coordinator
    /// post-processes them (reverse + COUNT) without re-aggregating. Result
    /// must equal coordinator-side aggregation of the raw samples.
    #[test]
    fn test_multi_aggregation_pushdown_roundtrip() {
        let raw = samples(&[(0, 1.0), (10, 3.0), (110, 5.0), (120, 7.0), (250, 9.0)]);

        for (is_reverse, count) in [
            (false, None),
            (true, None),
            (true, Some(2)),
            (false, Some(1)),
        ] {
            let mut options = mrange_options(0, 1000);
            options.range.aggregation = Some(multi_aggregation(100));
            options.range.count = count;
            options.is_reverse = is_reverse;

            let reference = handle_basic(
                vec![to_response(series_result("a", None, raw.clone()))],
                &options,
            )
            .unwrap();

            let pushed = handle_basic(
                vec![shard_multi_response("a", None, &raw, &options)],
                &options,
            )
            .unwrap();

            assert_eq!(
                result_rows(&reference[0]),
                result_rows(&pushed[0]),
                "reverse={is_reverse} count={count:?}"
            );
        }
    }

    /// Simulate a multi-aggregation shard's group-partials response for its
    /// local members of one group: per-series row pipeline (bucket
    /// aggregation, ascending), k-way merge, column-wise partial reduce —
    /// exactly the `process_mrange_group_partials` multi pipeline.
    fn shard_multi_group_partial(
        members: &[(&str, &[Sample])],
        options: &MRangeOptions,
    ) -> GroupPartialSeries {
        use crate::aggregators::PartialRowReducer;

        let mut shard_options = options.clone();
        shard_options.range.count = None; // shard streams ascending, unbounded
        shard_options.is_reverse = false;
        let columns = options
            .range
            .aggregation
            .as_ref()
            .unwrap()
            .aggregations
            .len();
        let reducer =
            PartialReducer::for_config(&options.grouping.as_ref().unwrap().aggregation).unwrap();

        let row_iters: Vec<_> = members
            .iter()
            .map(|(_, raw)| {
                let rows: Vec<MultiSample> =
                    aggregate_rows_ascending(raw.iter().copied(), &shard_options).collect();
                rows.into_iter()
            })
            .collect();
        let merged = MultiSeriesRowIter::new(row_iters);
        let (bucket_timestamps, buckets): (Vec<i64>, Vec<_>) =
            PartialRowReducer::new(merged, reducer, columns).unzip();

        GroupPartialSeries {
            group_label_value: "us".into(),
            source_keys: members.iter().map(|(key, _)| key.to_string()).collect(),
            bucket_timestamps,
            states: buckets.into_iter().flatten().map(Into::into).collect(),
            column_count: columns as u32,
        }
    }

    /// Multi-aggregation group-reduce push-down: shard-side column-wise
    /// partial states, merged and finalized at the coordinator, must equal
    /// the per-series bucket transport fallback (itself proven equal to the
    /// raw flow above). REDUCE avg makes the cross-shard merge nontrivial
    /// (count+sum accumulate across shards); one shard holds two series
    /// (pre-merged locally), the other holds one.
    #[test]
    fn test_multi_aggregation_group_partials_roundtrip() {
        let raw_a = samples(&[(0, 1.0), (10, 3.0), (110, 5.0)]);
        let raw_b = samples(&[(0, 8.0), (20, 12.0), (250, 7.0)]);
        let raw_c = samples(&[(0, 4.0), (110, 9.0), (250, 1.0)]);

        for (is_reverse, count) in [
            (false, None),
            (true, None),
            (true, Some(2)),
            (false, Some(1)),
        ] {
            let mut options = mrange_options(0, 1000);
            options.with_labels = true;
            options.range.aggregation = Some(multi_aggregation(100));
            options.range.count = count;
            options.is_reverse = is_reverse;
            options.grouping = Some(RangeGroupingOptions {
                aggregation: AggregatorConfig::new(AggregationType::Avg, None).unwrap(),
                group_label: "region".into(),
            });

            // Reference: per-series bucket transport with coordinator-side
            // column-wise reduce (the non-decomposable fallback path).
            let reference = handle_grouping(
                vec![
                    shard_multi_response("a", Some("us"), &raw_a, &options),
                    shard_multi_response("b", Some("us"), &raw_b, &options),
                    shard_multi_response("c", Some("us"), &raw_c, &options),
                ],
                &options,
            )
            .unwrap();

            // Phase 2: shard 1 holds a and b (pre-merged locally), shard 2
            // holds c; one partial series per (group, shard).
            let pushed = handle_group_partials(
                vec![
                    shard_multi_group_partial(&[("a", &raw_a), ("b", &raw_b)], &options),
                    shard_multi_group_partial(&[("c", &raw_c)], &options),
                ],
                &options,
            )
            .unwrap();

            assert_eq!(reference.len(), 1);
            assert_eq!(pushed.len(), 1);
            assert_eq!(reference[0].key, pushed[0].key);
            assert_eq!(
                result_rows(&reference[0]),
                result_rows(&pushed[0]),
                "reverse={is_reverse} count={count:?}"
            );
            let source = pushed[0]
                .labels
                .iter()
                .find(|l| l.name == SOURCE_KEY)
                .expect("__source__ label present");
            assert_eq!(source.value, "a,b,c");
        }
    }

    /// Corrupt-peer defense: partial series whose column_count or state count
    /// does not match the query's shape are rejected instead of misdecoded.
    #[test]
    fn test_group_partials_shape_validation() {
        let mut options = mrange_options(0, 1000);
        options.grouping = Some(RangeGroupingOptions {
            aggregation: AggregatorConfig::new(AggregationType::Sum, None).unwrap(),
            group_label: "region".into(),
        });

        let partial = |column_count: u32, states: usize| GroupPartialSeries {
            group_label_value: "us".into(),
            source_keys: vec!["a".into()],
            bucket_timestamps: vec![0, 100],
            states: (0..states)
                .map(|_| PartialState::default().into())
                .collect(),
            column_count,
        };

        // single-aggregation query expects one column, two states
        assert!(handle_group_partials(vec![partial(1, 2)], &options).is_ok());
        assert!(handle_group_partials(vec![partial(1, 3)], &options).is_err());

        // The rejection names the group and the offending shape for triage.
        let err = handle_group_partials(vec![partial(2, 4)], &options).unwrap_err();
        let msg = err.to_string();
        for needle in [
            "group 'us'",
            "column_count 2 (expected 1)",
            "4 states for 2 buckets",
            "sources [a]",
        ] {
            assert!(msg.contains(needle), "missing '{needle}' in: {msg}");
        }

        // multi query expects one state per column per bucket
        options.range.aggregation = Some(multi_aggregation(100)); // 3 columns
        assert!(handle_group_partials(vec![partial(3, 6)], &options).is_ok());
        assert!(handle_group_partials(vec![partial(1, 2)], &options).is_err());
        assert!(handle_group_partials(vec![partial(3, 5)], &options).is_err());
    }

    fn n_column_aggregation(bucket_duration: u64, n: usize) -> AggregationOptions {
        AggregationOptions {
            aggregations: (0..n)
                .map(|_| AggregatorConfig::new(AggregationType::Avg, None).unwrap())
                .collect(),
            bucket_duration,
            timestamp_output: Default::default(),
            alignment: Default::default(),
            report_empty: false,
        }
    }

    fn partial_with(
        bucket_count: usize,
        column_count: u32,
        state_count: usize,
    ) -> GroupPartialSeries {
        GroupPartialSeries {
            group_label_value: "us".into(),
            source_keys: vec!["a".into()],
            bucket_timestamps: (0..bucket_count).map(|i| i as i64 * 100).collect(),
            states: (0..state_count)
                .map(|_| PartialState::default().into())
                .collect(),
            column_count,
        }
    }

    /// Randomized fuzz over the corrupt-peer shape check: a peer that declares
    /// a `column_count` disagreeing with the query's own aggregation shape
    /// (single- or multi-aggregation, self-consistent state count for its own
    /// declared count) must always be rejected, never misdecoded.
    #[test]
    fn test_group_partials_column_count_mismatch_randomized() {
        use rand::{RngExt, SeedableRng};

        let mut rng = rand::rngs::StdRng::seed_from_u64(0xC011_7C0D);
        for _ in 0..200 {
            let expected_columns = rng.random_range(1..5);
            let mut options = mrange_options(0, 1000);
            options.grouping = Some(RangeGroupingOptions {
                aggregation: AggregatorConfig::new(AggregationType::Sum, None).unwrap(),
                group_label: "region".into(),
            });
            if expected_columns > 1 {
                options.range.aggregation = Some(n_column_aggregation(100, expected_columns));
            }

            let bucket_count = rng.random_range(1..5);
            // Declared column_count disagrees with what the query expects,
            // but the state count is internally consistent with what was
            // declared, isolating the column_count check itself.
            let bogus_column_count = loop {
                let c = rng.random_range(1..6);
                if c != expected_columns as u32 {
                    break c;
                }
            };
            let partial = partial_with(
                bucket_count,
                bogus_column_count,
                bucket_count * bogus_column_count as usize,
            );

            let result = handle_group_partials(vec![partial], &options);
            let err = match result {
                Ok(_) => panic!(
                    "expected rejection: expected_columns={expected_columns} bogus_column_count={bogus_column_count} bucket_count={bucket_count}"
                ),
                Err(e) => e.to_string(),
            };
            assert!(
                err.contains(&format!(
                    "column_count {bogus_column_count} (expected {expected_columns})"
                )),
                "expected_columns={expected_columns} bogus_column_count={bogus_column_count} bucket_count={bucket_count}: {err}"
            );
        }
    }

    /// Randomized fuzz over the corrupt-peer shape check: a peer that reports
    /// the correct `column_count` but appends extra trailing states beyond
    /// `buckets * columns` (e.g. a partial row from a truncated/duplicated
    /// send) must be rejected rather than silently ignored or misaligned.
    #[test]
    fn test_group_partials_extra_trailing_states_randomized() {
        use rand::{RngExt, SeedableRng};

        let mut rng = rand::rngs::StdRng::seed_from_u64(0x7EA1_1EAD);
        for _ in 0..200 {
            let expected_columns = rng.random_range(1..5);
            let mut options = mrange_options(0, 1000);
            options.grouping = Some(RangeGroupingOptions {
                aggregation: AggregatorConfig::new(AggregationType::Sum, None).unwrap(),
                group_label: "region".into(),
            });
            if expected_columns > 1 {
                options.range.aggregation = Some(n_column_aggregation(100, expected_columns));
            }

            let bucket_count = rng.random_range(1..5);
            let exact_state_count = bucket_count * expected_columns;
            let extra = rng.random_range(1..6);
            let partial = partial_with(
                bucket_count,
                expected_columns as u32,
                exact_state_count + extra,
            );

            let result = handle_group_partials(vec![partial], &options);
            let err = match result {
                Ok(_) => panic!(
                    "expected rejection: expected_columns={expected_columns} bucket_count={bucket_count} extra={extra}"
                ),
                Err(e) => e.to_string(),
            };
            assert!(
                err.contains(&format!(
                    "{} states for {bucket_count} buckets",
                    exact_state_count + extra
                )),
                "expected_columns={expected_columns} bucket_count={bucket_count} extra={extra}: {err}"
            );
        }
    }

    /// sources_preview stays bounded for large groups.
    #[test]
    fn test_sources_preview_bounded() {
        let keys: Vec<String> = (0..10).map(|i| format!("k{i}")).collect();
        assert_eq!(sources_preview(&keys[..2]), "k0,k1");
        assert_eq!(sources_preview(&keys[..3]), "k0,k1,k2");
        assert_eq!(sources_preview(&keys), "k0,k1,k2,+7 more");
        assert_eq!(sources_preview(&[]), "");
    }

    fn tagged(
        responses: Vec<SeriesRangeResponse>,
        bucketed: bool,
    ) -> Vec<(SeriesRangeResponse, bool)> {
        responses.into_iter().map(|r| (r, bucketed)).collect()
    }

    /// Compatibility handshake, non-grouped: a shard that does not echo
    /// `applied_aggregation` (pre-push-down peer) ships raw samples; the
    /// coordinator aggregates them itself, so a mixed batch equals the
    /// all-new result.
    #[test]
    fn test_mixed_version_basic_compensation() {
        let raw_a = samples(&[(0, 1.0), (10, 3.0), (110, 5.0), (250, 9.0)]);
        let raw_b = samples(&[(5, 2.0), (120, 4.0), (260, 6.0)]);

        for (is_reverse, count) in [(false, None), (true, None), (true, Some(2))] {
            let mut options = mrange_options(0, 1000);
            options.range.aggregation = Some(avg_aggregation(100));
            options.range.count = count;
            options.is_reverse = is_reverse;

            let bucket = |raw: &[Sample]| {
                let mut shard_options = options.clone();
                shard_options.range.count = None;
                shard_options.is_reverse = false;
                shard_aggregate(raw.to_vec(), &shard_options)
            };

            // All-new cluster: both shards echo and ship buckets.
            let mut command = MRangeFanoutCommand::new(options.clone());
            assert!(command.pushdown);
            let mut reference = command
                .process_responses(
                    vec![
                        (to_response(series_result("a", None, bucket(&raw_a))), true),
                        (to_response(series_result("b", None, bucket(&raw_b))), true),
                    ],
                    Vec::new(),
                )
                .unwrap();
            sort_mrange_results(&mut reference, false);

            // Mixed: shard b is a pre-push-down peer (raw samples, no echo).
            let mut command = MRangeFanoutCommand::new(options.clone());
            let mut mixed = command
                .process_responses(
                    vec![
                        (to_response(series_result("a", None, bucket(&raw_a))), true),
                        (to_response(series_result("b", None, raw_b.clone())), false),
                    ],
                    Vec::new(),
                )
                .unwrap();
            sort_mrange_results(&mut mixed, false);

            assert_eq!(reference.len(), mixed.len());
            for (r, m) in reference.iter().zip(mixed.iter()) {
                assert_eq!(r.key, m.key);
                assert_eq!(
                    result_samples(r),
                    result_samples(m),
                    "reverse={is_reverse} count={count:?}"
                );
            }
        }
    }

    /// Compatibility handshake, grouped: a pre-push-down peer returns tagged
    /// per-series raw data instead of group partials; the coordinator
    /// aggregates and pre-reduces it locally and merges the result with the
    /// wire partials of up-to-date shards.
    #[test]
    fn test_mixed_version_group_partials_compensation() {
        use crate::aggregators::PartialSampleReducer;

        let raw_a = samples(&[(0, 1.0), (10, 3.0), (110, 5.0)]);
        let raw_b = samples(&[(0, 8.0), (20, 12.0), (250, 7.0)]);

        for (is_reverse, count) in [(false, None), (true, Some(2))] {
            let mut options = mrange_options(0, 1000);
            options.with_labels = true;
            options.range.aggregation = Some(avg_aggregation(100));
            options.range.count = count;
            options.is_reverse = is_reverse;
            options.grouping = Some(RangeGroupingOptions {
                aggregation: AggregatorConfig::new(AggregationType::Sum, None).unwrap(),
                group_label: "region".into(),
            });

            let reducer =
                PartialReducer::for_config(&options.grouping.as_ref().unwrap().aggregation)
                    .unwrap();
            let partial_of = |key: &str, raw: &[Sample]| {
                let mut shard_options = options.clone();
                shard_options.range.count = None;
                shard_options.is_reverse = false;
                let buckets = shard_aggregate(raw.to_vec(), &shard_options);
                let (bucket_timestamps, states): (Vec<i64>, Vec<PartialState>) =
                    PartialSampleReducer::new(buckets.into_iter(), reducer.clone()).unzip();
                GroupPartialSeries {
                    group_label_value: "us".into(),
                    source_keys: vec![key.into()],
                    bucket_timestamps,
                    states: states.into_iter().map(Into::into).collect(),
                    column_count: 1,
                }
            };

            // All-new: both shards ship partials.
            let mut command = MRangeFanoutCommand::new(options.clone());
            assert!(command.pushdown_group);
            let reference = command
                .process_responses(
                    Vec::new(),
                    vec![partial_of("a", &raw_a), partial_of("b", &raw_b)],
                )
                .unwrap();

            // Mixed: shard holding b is pre-push-down (raw tagged series).
            let mut command = MRangeFanoutCommand::new(options.clone());
            let mixed = command
                .process_responses(
                    tagged(
                        vec![to_response(series_result("b", Some("us"), raw_b.clone()))],
                        false,
                    ),
                    vec![partial_of("a", &raw_a)],
                )
                .unwrap();

            assert_eq!(reference.len(), 1);
            assert_eq!(mixed.len(), 1);
            assert_eq!(reference[0].key, mixed[0].key);
            assert_eq!(
                result_samples(&reference[0]),
                result_samples(&mixed[0]),
                "reverse={is_reverse} count={count:?}"
            );
            let source = mixed[0]
                .labels
                .iter()
                .find(|l| l.name == SOURCE_KEY)
                .expect("__source__ label present");
            assert_eq!(source.value, "a,b");
        }
    }

    /// Compatibility handshake, grouped multi-aggregation: mixes wire
    /// partials with (a) fully-raw series from a pre-push-down peer and
    /// (b) bucketed rows from a peer that honored apply_aggregation but not
    /// apply_group_reduce. Both compensate to the all-partials result.
    #[test]
    fn test_mixed_version_multi_group_partials_compensation() {
        let raw_a = samples(&[(0, 1.0), (10, 3.0), (110, 5.0)]);
        let raw_b = samples(&[(0, 8.0), (20, 12.0), (250, 7.0)]);
        let raw_c = samples(&[(0, 4.0), (110, 9.0), (250, 1.0)]);

        let mut options = mrange_options(0, 1000);
        options.range.aggregation = Some(multi_aggregation(100));
        options.grouping = Some(RangeGroupingOptions {
            aggregation: AggregatorConfig::new(AggregationType::Avg, None).unwrap(),
            group_label: "region".into(),
        });

        let mut command = MRangeFanoutCommand::new(options.clone());
        assert!(command.pushdown_group, "multi latches group push-down");
        let reference = command
            .process_responses(
                Vec::new(),
                vec![
                    shard_multi_group_partial(&[("a", &raw_a), ("b", &raw_b)], &options),
                    shard_multi_group_partial(&[("c", &raw_c)], &options),
                ],
            )
            .unwrap();

        // (a) shard holding c is fully pre-push-down: raw samples, no echo.
        let mut command = MRangeFanoutCommand::new(options.clone());
        let mixed_raw = command
            .process_responses(
                tagged(
                    vec![to_response(series_result("c", Some("us"), raw_c.clone()))],
                    false,
                ),
                vec![shard_multi_group_partial(
                    &[("a", &raw_a), ("b", &raw_b)],
                    &options,
                )],
            )
            .unwrap();

        // (b) shard holding c honored apply_aggregation (bucket rows arrive
        // as the Rows variant) but not apply_group_reduce.
        let mut command = MRangeFanoutCommand::new(options.clone());
        let mixed_bucketed = command
            .process_responses(
                vec![(
                    shard_multi_response("c", Some("us"), &raw_c, &options),
                    true,
                )],
                vec![shard_multi_group_partial(
                    &[("a", &raw_a), ("b", &raw_b)],
                    &options,
                )],
            )
            .unwrap();

        assert_eq!(reference.len(), 1);
        assert_eq!(
            result_rows(&reference[0]),
            result_rows(&mixed_raw[0]),
            "raw peer compensated"
        );
        assert_eq!(
            result_rows(&reference[0]),
            result_rows(&mixed_bucketed[0]),
            "bucketed-only peer compensated"
        );
    }

    /// Multi-aggregation push-down round trip (grouped): per-series bucket rows
    /// arrive already aggregated; the coordinator merges and reduces
    /// column-wise without re-aggregating. Must equal the raw-sample flow where
    /// the coordinator aggregates each series itself.
    #[test]
    fn test_multi_aggregation_pushdown_roundtrip_grouped() {
        let raw_a = samples(&[(0, 1.0), (10, 3.0), (110, 5.0)]);
        let raw_b = samples(&[(0, 8.0), (20, 12.0), (250, 7.0)]);

        for (is_reverse, count) in [(false, None), (true, Some(2))] {
            let mut options = mrange_options(0, 1000);
            options.range.aggregation = Some(multi_aggregation(100));
            options.range.count = count;
            options.is_reverse = is_reverse;
            options.grouping = Some(RangeGroupingOptions {
                aggregation: AggregatorConfig::new(AggregationType::Sum, None).unwrap(),
                group_label: "region".into(),
            });

            let reference = handle_grouping(
                vec![
                    to_response(series_result("a", Some("us"), raw_a.clone())),
                    to_response(series_result("b", Some("us"), raw_b.clone())),
                ],
                &options,
            )
            .unwrap();

            let pushed = handle_grouping(
                vec![
                    shard_multi_response("a", Some("us"), &raw_a, &options),
                    shard_multi_response("b", Some("us"), &raw_b, &options),
                ],
                &options,
            )
            .unwrap();

            assert_eq!(reference.len(), pushed.len());
            assert_eq!(
                result_rows(&reference[0]),
                result_rows(&pushed[0]),
                "reverse={is_reverse} count={count:?}"
            );
        }
    }

    /// End-to-end through `ingest_response`: a shard that interned its labels
    /// (as `get_local_response` always does) must resolve back to exactly the
    /// labels it started with, with the shared `region` value collapsed to one
    /// dictionary entry.
    #[test]
    fn ingest_response_resolves_symbol_table_labels() {
        let mut wire = vec![
            to_response(series_result_with_labels(
                "a",
                samples(&[(0, 1.0)]),
                &[("region", "us-east-1"), ("env", "prod")],
            )),
            to_response(series_result_with_labels(
                "b",
                samples(&[(0, 2.0)]),
                &[("region", "us-east-1"), ("env", "staging")],
            )),
        ];
        let expected: Vec<Vec<(String, String)>> = wire.iter().map(label_pairs).collect();

        let (symbol_table_names, symbol_table_values) =
            fanout_codec::symbol_table::intern_labels(&mut wire);
        assert_eq!(symbol_table_names.len(), 2, "region, env");
        assert_eq!(symbol_table_values.len(), 3, "us-east-1, prod, staging");
        assert!(wire.iter().all(|s| s.labels.is_empty()), "interned away");

        let resp = MultiRangeResponse {
            series: wire,
            group_partials: Vec::new(),
            applied_aggregation: false,
            applied_group_reduce: false,
            applied_count: false,
            symbol_table_names,
            symbol_table_values,
        };

        let mut cmd = MRangeFanoutCommand::default();
        cmd.ingest_response(resp).expect("ingest_response");

        assert_eq!(cmd.series.len(), 2);
        for ((series, _bucketed), want) in cmd.series.iter().zip(&expected) {
            assert_eq!(&label_pairs(series), want, "series '{}'", series.key);
        }
    }

    /// Peer-controlled input: a symbol-table response whose ref arrays point
    /// past the end of the dictionary must be rejected, not indexed.
    #[test]
    fn ingest_response_rejects_out_of_range_symbol_table_ref() {
        let resp = MultiRangeResponse {
            series: vec![SeriesRangeResponse {
                key: "a".into(),
                group_label_value: String::new(),
                labels: Vec::new(),
                columns: Vec::new(),
                label_name_refs: vec![7],
                label_value_refs: vec![0],
            }],
            group_partials: Vec::new(),
            applied_aggregation: false,
            applied_group_reduce: false,
            applied_count: false,
            symbol_table_names: Vec::new(),
            symbol_table_values: vec!["us-east-1".into()],
        };

        let mut cmd = MRangeFanoutCommand::default();
        let err = cmd
            .ingest_response(resp)
            .expect_err("out-of-range ref must be rejected");
        assert!(err.to_string().contains("out of range"), "{err}");
        assert!(
            cmd.series.is_empty(),
            "a rejected response must not be partially ingested"
        );
    }

    /// `ingest_response` resolves unconditionally, with no flag to consult, so
    /// a response carrying `labels` directly and no refs has to survive the
    /// trip intact. Guards against the resolve step being made destructive.
    #[test]
    fn ingest_response_passes_through_uninterned_labels() {
        let wire = to_response(series_result_with_labels(
            "a",
            samples(&[(0, 1.0)]),
            &[("region", "us-east-1")],
        ));
        let expected = label_pairs(&wire);
        assert!(!expected.is_empty(), "premise: the series has labels");

        let resp = MultiRangeResponse {
            series: vec![wire],
            group_partials: Vec::new(),
            applied_aggregation: false,
            applied_group_reduce: false,
            applied_count: false,
            // No symbol table and no refs: labels carried directly.
            symbol_table_names: Vec::new(),
            symbol_table_values: Vec::new(),
        };

        let mut cmd = MRangeFanoutCommand::default();
        cmd.ingest_response(resp).expect("ingest_response");

        assert_eq!(cmd.series.len(), 1);
        assert_eq!(label_pairs(&cmd.series[0].0), expected);
    }

    /// A label-less series survives the unconditional round trip: it interns to
    /// empty refs, which is the same shape as an uninterned series, and must
    /// come back as label-less rather than erroring.
    #[test]
    fn ingest_response_handles_label_less_series() {
        let mut wire = vec![to_response(series_result("a", None, samples(&[(0, 1.0)])))];
        let (symbol_table_names, symbol_table_values) =
            fanout_codec::symbol_table::intern_labels(&mut wire);
        assert!(symbol_table_names.is_empty());
        assert!(symbol_table_values.is_empty());

        let resp = MultiRangeResponse {
            series: wire,
            group_partials: Vec::new(),
            applied_aggregation: false,
            applied_group_reduce: false,
            applied_count: false,
            symbol_table_names,
            symbol_table_values,
        };

        let mut cmd = MRangeFanoutCommand::default();
        cmd.ingest_response(resp).expect("ingest_response");

        assert_eq!(cmd.series.len(), 1);
        assert!(cmd.series[0].0.labels.is_empty());
    }
}
