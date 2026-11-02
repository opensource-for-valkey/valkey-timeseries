use crate::aggregators::{
    AggregationHandler, Aggregator, EmptyFillBounds, PartialReducer, PartialRowReducer,
    PartialSampleReducer, PartialState, bucket_start_for,
};
use crate::common::acl::check_metadata_permissions;
use crate::common::constants::{REDUCER_KEY, SOURCE_KEY};
use crate::common::context::key_for_display;
use crate::common::threads::{
    IntoParRayon, IterIntoParRayon, ParCollectionRayon, request_par_threads,
};
use crate::common::{MultiSample, Sample, Timestamp};
use crate::error_consts;
use crate::iterators::{
    KWayMerge, RowReducer, SampleReducer, TailIter, create_range_iterator,
    create_range_iterator_for_reply, create_range_iterator_from_base, create_range_tail_iterator,
    create_row_iterator, create_row_iterator_for_reply, create_row_tail_iterator,
    get_range_latest_sample, range_empty_fill_bounds,
};
use crate::labels::Label;
use crate::series::chunks::{TimeSeriesChunk, UncompressedChunk, samples_to_chunk_lossless};
use crate::series::index::series_by_selectors;
use crate::series::request_types::{
    MRangeOptions, MRangeSeriesResult, RangeGroupingOptions, RangeOptions, SeriesResultData,
};
use crate::series::{RangeSnapshot, SeriesGuard, TimeSeries};
use ahash::AHashMap;
use orx_parallel::Par;
use std::sync::atomic::{AtomicBool, Ordering};
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString};

struct MRangeSeriesMeta<'a> {
    series: &'a TimeSeries,
    /// The series key, verbatim. Binary, not `String` — see `MRangeSeriesResult::key`.
    source_key: Vec<u8>,
    latest: Option<Sample>,
    group_label_value: Option<String>,
}

/// Head/tail pre-filter applied shard-side under COUNT push-down
/// (`apply_count`). Tail is used for reverse queries: shards stream
/// ascending, so the last `n` items are the first `n` in requested order.
/// The coordinator always re-applies COUNT, so this only bounds transfer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SampleLimit {
    Head(usize),
    Tail(usize),
}

impl SampleLimit {
    pub(crate) fn apply<'a, I>(self, iter: I) -> Box<dyn Iterator<Item = I::Item> + 'a>
    where
        I: Iterator + 'a,
    {
        match self {
            SampleLimit::Head(n) => Box::new(iter.take(n)),
            SampleLimit::Tail(n) => Box::new(TailIter::new(iter, n)),
        }
    }
}

/// One (group, shard) partial series produced by GROUPBY/REDUCE push-down:
/// the shard's local members of the group, pre-reduced per bucket timestamp
/// into mergeable partial states.
pub(crate) struct GroupPartialsResult {
    pub group_label_value: String,
    pub source_keys: Vec<Vec<u8>>,
    /// Ascending; `states` holds `column_count` entries per timestamp.
    pub timestamps: Vec<Timestamp>,
    /// Row-major: bucket i, column j at `states[i * column_count + j]`.
    pub states: Vec<PartialState>,
    /// 1 unless the query is multi-aggregation (then one state per column).
    pub column_count: usize,
}

/// Shard-side handler for `apply_group_reduce`: per-series bucket aggregation
/// (when `AGGREGATION` is present), then a per-group k-way merge and partial
/// reduce. The caller must have stripped `count` and `is_reverse`.
pub(crate) fn process_mrange_group_partials(
    ctx: &Context,
    options: MRangeOptions,
    limit: Option<SampleLimit>,
    expired: &(dyn Fn() -> bool + Sync),
) -> ValkeyResult<Vec<GroupPartialsResult>> {
    check_metadata_permissions(ctx)?;

    if options.filters.is_empty() {
        return Err(ValkeyError::Str(error_consts::MISSING_FILTER));
    }

    let Some(grouping) = options.grouping.clone() else {
        return Err(ValkeyError::Str(
            "TSDB: internal error: group reduce requested without grouping",
        ));
    };

    let Some(reducer) = PartialReducer::for_config(&grouping.aggregation) else {
        // The coordinator only sets apply_group_reduce for decomposable
        // reducers; reject rather than silently produce wrong results.
        return Err(ValkeyError::String(format!(
            "TSDB: internal error: REDUCE {} does not support partial reduce",
            grouping.aggregation.aggregation_name()
        )));
    };

    let series_guards = series_by_selectors(ctx, &options.filters, None)?;

    let mut series_metas: Vec<MRangeSeriesMeta> = series_guards
        .iter()
        .map(|(guard, key)| MRangeSeriesMeta {
            series: guard,
            source_key: key.as_slice().to_vec(),
            group_label_value: None,
            latest: get_latest(&options.range, ctx, guard),
        })
        .collect();

    collect_group_label_values(&mut series_metas, &grouping);
    let grouped = group_series_by_label(series_metas, &grouping, false);

    // Multi-aggregation reduces column-wise: N states per bucket, one per
    // aggregation column, all with the same REDUCE type.
    let multi_columns = options
        .range
        .aggregation
        .as_ref()
        .filter(|a| a.is_multi())
        .map(|a| a.aggregations.len());

    let deadline = ReadDeadline::new(expired);
    let threads = request_par_threads(
        grouped.len(),
        grouped
            .values()
            .map(|g| estimated_work(&g.series, &options.range))
            .sum(),
    );
    let results = grouped
        .into_iter()
        .iter_into_par_rayon()
        .num_threads(threads)
        .map(|(label_value, group_data)| {
            if deadline.passed() {
                return GroupPartialsResult {
                    group_label_value: label_value,
                    source_keys: Vec::new(),
                    timestamps: Vec::new(),
                    states: Vec::new(),
                    column_count: 1,
                };
            }
            let mut source_keys: Vec<Vec<u8>> = group_data
                .series
                .iter()
                .map(|m| m.source_key.clone())
                .collect();
            source_keys.sort();

            // Per-series pipeline (aggregation included when present),
            // ascending; the group reducer runs once, across series, below.
            let (timestamps, states, column_count) = if let Some(columns) = multi_columns {
                let iterators = group_data
                    .series
                    .iter()
                    .map(|meta| {
                        create_row_iterator(meta.series, &options.range, meta.latest, false)
                    })
                    .collect::<Vec<_>>();

                let multi_iter = KWayMerge::new(iterators);
                let rows = PartialRowReducer::new(multi_iter, reducer.clone(), columns);
                let (timestamps, buckets): (Vec<Timestamp>, Vec<_>) = match limit {
                    Some(limit) => limit.apply(rows).unzip(),
                    None => rows.unzip(),
                };
                let states = buckets.into_iter().flatten().collect();
                (timestamps, states, columns)
            } else {
                let iterators = group_data
                    .series
                    .iter()
                    .map(|meta| {
                        create_range_iterator(meta.series, &options.range, meta.latest, false)
                    })
                    .collect::<Vec<_>>();

                let multi_iter = KWayMerge::new(iterators);
                let rows = PartialSampleReducer::new(multi_iter, reducer.clone());
                let (timestamps, states) = match limit {
                    Some(limit) => limit.apply(rows).unzip(),
                    None => rows.unzip(),
                };
                (timestamps, states, 1)
            };

            GroupPartialsResult {
                group_label_value: label_value,
                source_keys,
                timestamps,
                states,
                column_count,
            }
        })
        .collect();
    deadline.finish(results)
}

/// Who a node's TS.MRANGE results are for, which decides how they are shaped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MRangeScope {
    /// The client itself: grouped and sorted here, uncompressed.
    Client,
    /// A coordinator on another node: per-series results, compressed for the wire.
    RemoteShard,
    /// This node's own coordinator. Shaped like [`Self::RemoteShard`], but the results
    /// never leave the process, so compressing them would only cost an encode here
    /// and a decode in the coordinator.
    LocalShard,
}

impl MRangeScope {
    fn is_shard(self) -> bool {
        self != MRangeScope::Client
    }

    /// Whether per-series samples go through the wire encoding
    /// ([`samples_to_chunk_lossless`]).
    pub(crate) fn compress(self) -> bool {
        self == MRangeScope::RemoteShard
    }
}

/// `expired` says whether a shard's requester has stopped waiting: the decode, which runs under
/// the lock, stops between series once it has.
pub(crate) fn process_mrange_query(
    ctx: &Context,
    options: MRangeOptions,
    scope: MRangeScope,
    limit: Option<SampleLimit>,
    expired: &(dyn Fn() -> bool + Sync),
) -> ValkeyResult<Vec<MRangeSeriesResult>> {
    if options.filters.is_empty() {
        return Err(ValkeyError::Str(error_consts::MISSING_FILTER));
    }

    // ACL is enforced per matched key inside `series_by_selectors`: the query
    // fails closed if the filter matches any series the caller cannot read.
    // (An all-keys gate here would wrongly reject users who only have access to
    // the keys they actually match, and is inconsistent with TS.MGET.)
    let series_guards = series_by_selectors(ctx, &options.filters, None)?;

    let series_metas: Vec<MRangeSeriesMeta> = series_guards
        .iter()
        .map(|(guard, key)| MRangeSeriesMeta {
            series: guard,
            source_key: key.as_slice().to_vec(),
            group_label_value: None,
            latest: {
                // This is done upfront to enable parallel series processing below
                // (Context cannot be shared across threads)
                get_latest(&options.range, ctx, guard)
            },
        })
        .collect();

    process_mrange(series_metas, options, scope, limit, expired)
}

fn process_mrange(
    metas: Vec<MRangeSeriesMeta>,
    options: MRangeOptions,
    scope: MRangeScope,
    limit: Option<SampleLimit>,
    expired: &(dyn Fn() -> bool + Sync),
) -> ValkeyResult<Vec<MRangeSeriesResult>> {
    let is_clustered = scope.is_shard();
    let mut options = options;
    let mut metas = metas;

    if let Some(grouping) = &options.grouping {
        collect_group_label_values(&mut metas, grouping);
        // `is_clustered` here means that we are being called from a remote node. Since grouping
        // is cross-series, it can only be done when all results are available in the caller node.
        // However, we need to tag each series with the grouping label value (done above), so we can
        // group them by the grouping label when they are returned to the client.
        //
        // We remove grouping from the options here. The full request options are available in the done
        // handler, so we can use them to group the series.
        if is_clustered {
            options.grouping = None;
        }
    }
    let is_grouped = options.grouping.is_some();
    let exclude_empty = options.exclude_empty;

    if is_clustered {
        // Shard side: a series lives entirely on one shard, so its emptiness is
        // already decided here and dropping it now only saves transfer. The
        // coordinator re-applies EXCLUDEEMPTY regardless, so a peer that ignores
        // the request flag still yields the same reply.
        let deadline = ReadDeadline::new(expired);
        let items = handle_non_grouped(metas, options, scope.compress(), limit, &deadline);
        let mut items = deadline.finish(items)?;
        if exclude_empty {
            items.retain(|item| !item.data.is_empty());
        }
        return Ok(items);
    }

    let mut items = if is_grouped {
        handle_grouping(metas, options)?
    } else {
        handle_non_grouped(metas, options, false, None, &ReadDeadline::new(&|| false))
    };

    // EXCLUDEEMPTY is rejected together with GROUPBY at parse time, so this only
    // ever trims per-series results.
    if exclude_empty {
        items.retain(|item| !item.data.is_empty());
    }

    sort_mrange_results(&mut items, is_grouped);

    Ok(items)
}

/// The destination's still-open bucket, when `LATEST` asks for it.
///
/// Delegates to the single-key helper rather than re-deriving the rule. A local copy here
/// checked only that the bucket's timestamp fell inside the requested range, and so was
/// missing two conditions the shared version carries: the retention clamp, and the
/// `end_ts > last_timestamp()` guard that keeps an open bucket hidden unless the query
/// reaches past the last *stored* sample. Without them TS.MRANGE reported an open bucket
/// that TS.RANGE — and the reference — both omit, so the engine disagreed with itself
/// (found by the Tier C fuzzer). Keeping one implementation is what stops that recurring.
fn get_latest(options: &RangeOptions, ctx: &Context, series: &TimeSeries) -> Option<Sample> {
    get_range_latest_sample(ctx, series, options)
        .filter(|s| options.value_filter.is_none_or(|vf| vf.is_match(s.value)))
}

fn create_iter<'a>(
    series: &'a TimeSeries,
    options: &MRangeOptions,
    latest: Option<Sample>,
) -> Box<dyn Iterator<Item = Sample> + 'a> {
    create_range_iterator_for_reply(
        series,
        &options.range,
        latest,
        options.is_reverse,
        options.is_reverse || options.reply_reverse,
    )
}

/// Stops a shard's decode once its requester has given up. Decoding runs on a pool, so each
/// series or group asks before starting, what it returns is discarded, and [`Self::finish`]
/// turns the whole read into the error. A read that got to the end is answered however late:
/// only a read with work left is abandoned.
struct ReadDeadline<'a> {
    expired: &'a (dyn Fn() -> bool + Sync),
    abandoned: AtomicBool,
}

impl<'a> ReadDeadline<'a> {
    fn new(expired: &'a (dyn Fn() -> bool + Sync)) -> Self {
        Self {
            expired,
            abandoned: AtomicBool::new(false),
        }
    }

    fn passed(&self) -> bool {
        if self.abandoned.load(Ordering::Relaxed) {
            return true;
        }
        let passed = (self.expired)();
        if passed {
            self.abandoned.store(true, Ordering::Relaxed);
        }
        passed
    }

    fn finish<T>(&self, results: T) -> ValkeyResult<T> {
        if self.abandoned.load(Ordering::Relaxed) {
            return Err(ValkeyError::Str(error_consts::FANOUT_REQUESTER_GAVE_UP));
        }
        Ok(results)
    }
}

/// Rough number of samples `series` holds inside `[start, end]`, from its sample
/// count and timestamp span alone — no chunk is touched. Only used to decide
/// whether a request is worth dispatching to the pool, so being off by a factor
/// of a few does not matter.
fn estimated_samples_in_range(series: &TimeSeries, start: Timestamp, end: Timestamp) -> usize {
    let Some(last) = series.last_sample.map(|s| s.timestamp) else {
        return 0;
    };
    let first = series.first_timestamp;
    if end < first || start > last {
        return 0;
    }
    let span = (last - first).max(1) as f64;
    let overlap = (end.min(last) - start.max(first)).max(0) as f64;
    // A one-sample span still counts its sample.
    ((series.total_samples as f64 * (overlap / span)).ceil() as usize)
        .clamp(1, series.total_samples.max(1))
}

/// Sample-equivalents charged per series on top of its samples: iterator setup,
/// chunk lookup, result and label construction cost about as much as decoding
/// this many samples (a ten-series MRANGE over 100 samples each measured ~80 µs
/// per series), so a handful of series with short windows is still worth
/// spreading across the pool.
const PER_SERIES_WORK: usize = 1024;

/// Work estimate for a set of series over the request's range.
fn estimated_work(metas: &[MRangeSeriesMeta], options: &RangeOptions) -> usize {
    let (start, end) = options.get_timestamp_range();
    metas
        .iter()
        .map(|m| PER_SERIES_WORK + estimated_samples_in_range(m.series, start, end))
        .sum()
}

pub(crate) fn sort_mrange_results(results: &mut [MRangeSeriesResult], is_grouped: bool) {
    if is_grouped {
        results.sort_by(|a, b| a.group_label_value.cmp(&b.group_label_value));
    } else {
        results.sort_by(|a, b| a.key.cmp(&b.key));
    }
}

fn handle_non_grouped(
    metas: Vec<MRangeSeriesMeta>,
    options: MRangeOptions,
    compress: bool,
    limit: Option<SampleLimit>,
    deadline: &ReadDeadline<'_>,
) -> Vec<MRangeSeriesResult> {
    let is_multi = options
        .range
        .aggregation
        .as_ref()
        .is_some_and(|a| a.is_multi());

    let threads = request_par_threads(metas.len(), estimated_work(&metas, &options.range));
    metas
        .into_par_rayon()
        .num_threads(threads)
        .map(|meta| {
            if deadline.passed() {
                return MRangeSeriesResult::default();
            }
            // Multi-aggregation yields rows, which chunks cannot store. Under
            // aggregation push-down a shard produces these rows too; they ship
            // as per-column chunks (SeriesRangeResponse.columns), so the
            // clustered path is valid here.
            let data = if is_multi {
                let reply_reverse = options.is_reverse || options.reply_reverse;
                let iter = match limit {
                    Some(SampleLimit::Tail(count)) => create_row_tail_iterator(
                        meta.series,
                        &options.range,
                        meta.latest,
                        count,
                        reply_reverse,
                    ),
                    limit => {
                        let iter = create_row_iterator_for_reply(
                            meta.series,
                            &options.range,
                            meta.latest,
                            options.is_reverse,
                            reply_reverse,
                        );
                        match limit {
                            Some(limit) => limit.apply(iter),
                            None => iter,
                        }
                    }
                };
                SeriesResultData::Rows(iter.collect())
            } else {
                let iter = match limit {
                    Some(SampleLimit::Tail(count)) => create_range_tail_iterator(
                        meta.series,
                        &options.range,
                        meta.latest,
                        count,
                        options.is_reverse || options.reply_reverse,
                    ),
                    limit => {
                        let iter = create_iter(meta.series, &options, meta.latest);
                        match limit {
                            Some(limit) => limit.apply(iter),
                            None => iter,
                        }
                    }
                };
                let samples = iter.collect::<Vec<_>>();
                // Only a response that crosses the network is worth compressing —
                // and only once it holds enough samples to pay for the chunk
                // header (see `samples_to_chunk`).
                if compress {
                    SeriesResultData::Chunk(samples_to_chunk_lossless(samples))
                } else {
                    let chunk = UncompressedChunk::from_vec(samples);
                    SeriesResultData::Chunk(TimeSeriesChunk::Uncompressed(chunk))
                }
            };

            let labels = convert_labels(meta.series, options.with_labels, &options.selected_labels);

            MRangeSeriesResult {
                group_label_value: meta.group_label_value,
                key: meta.source_key,
                labels,
                sources: Vec::new(),
                data,
            }
        })
        .collect()
}

fn handle_grouping(
    metas: Vec<MRangeSeriesMeta>,
    options: MRangeOptions,
) -> ValkeyResult<Vec<MRangeSeriesResult>> {
    // Callers only reach this function after confirming `options.grouping.is_some()`
    // (see `process_mrange`); a `None` here would mean that invariant broke.
    let Some(grouping) = &options.grouping else {
        debug_assert!(false, "handle_grouping called without grouping options");
        return Err(ValkeyError::Str(error_consts::INTERNAL_ERROR));
    };

    let grouped_series_map = group_series_by_label(metas, grouping, options.with_labels);

    if grouped_series_map.is_empty() {
        return Ok(vec![]);
    }

    let mut options = options;
    let count = options.range.count;
    options.range.count = None;

    let total_work: usize = grouped_series_map
        .values()
        .map(|g| estimated_work(&g.series, &options.range))
        .sum();
    let threads = request_par_threads(grouped_series_map.len(), total_work);

    let is_multi = options
        .range
        .aggregation
        .as_ref()
        .is_some_and(|a| a.is_multi());

    // Aggregated single-column GROUPBY: decode and bucket every member series of every
    // group in parallel over *series* (not groups, so one big group still uses the pool),
    // then fold each group's rows into its bucket table in series order — the same order
    // the k-way merge yields, so the reduce is bit-identical to it. Groups the grid
    // cannot serve fall through to the merge below.
    let (dense, mut merged) =
        plan_dense_groups(grouped_series_map, &options, count, DENSE_STAGING_MAX_ROWS);

    let mut items: Vec<MRangeSeriesResult> = Vec::with_capacity(dense.len() + merged.len());
    if !dense.is_empty() {
        let grouping = options
            .grouping
            .as_ref()
            .expect("Grouping options should be present");
        let template = grouping.aggregation.create_aggregator();
        let series_refs: Vec<&MRangeSeriesMeta> =
            dense.iter().flat_map(|(_, g, _)| g.series.iter()).collect();
        let series_threads = request_par_threads(series_refs.len(), total_work);
        let mut rows: Vec<Vec<Sample>> = series_refs
            .par_rayon()
            .num_threads(series_threads)
            .map(|meta| series_bucket_rows(meta, &options.range))
            .collect();
        let mut rows = rows.drain(..);
        for (label_value, group_data, grid) in dense {
            let group_rows: Vec<Vec<Sample>> =
                rows.by_ref().take(group_data.series.len()).collect();
            match fold_group_rows(&grid, &template, &group_rows) {
                Some(samples) => {
                    let samples = collect_samples(samples.into_iter(), options.is_reverse, count);
                    items.push(grouped_result(label_value, group_data, grouping, samples));
                }
                None => merged.push((label_value, group_data)),
            }
        }
    }

    let merged_items = merged
        .into_iter()
        .iter_into_par_rayon()
        .num_threads(threads)
        .map(|(label_value, group_data)| {
            let grouping = options
                .grouping
                .as_ref()
                .expect("Grouping options should be present");
            let data = if is_multi {
                let rows = get_grouped_rows(&group_data.series, &options, grouping, count);
                SeriesResultData::Rows(rows)
            } else {
                let samples = get_grouped_samples(&group_data.series, &options, grouping, count);
                // Grouping only runs on the node answering the client
                // (`process_mrange` returns early when clustered), so these
                // samples never cross the network and compressing them would
                // only be undone by the reply serializer.
                SeriesResultData::Chunk(TimeSeriesChunk::Uncompressed(UncompressedChunk::from_vec(
                    samples,
                )))
            };
            let labels = group_data.labels;
            let key = format!("{}={}", grouping.group_label, label_value).into_bytes();
            MRangeSeriesResult {
                key,
                group_label_value: Some(label_value),
                labels,
                sources: group_data.source_keys,
                data,
            }
        })
        .collect::<Vec<_>>();
    items.extend(merged_items);

    Ok(items)
}

/// One reduced group as a result row, its samples as an uncompressed chunk (grouping only
/// runs on the node answering the client, so they never cross the network).
fn grouped_result(
    label_value: String,
    group_data: GroupedSeriesData,
    grouping: &RangeGroupingOptions,
    samples: Vec<Sample>,
) -> MRangeSeriesResult {
    MRangeSeriesResult {
        key: format!("{}={}", grouping.group_label, label_value).into_bytes(),
        group_label_value: Some(label_value),
        labels: group_data.labels,
        sources: group_data.source_keys,
        data: SeriesResultData::Chunk(TimeSeriesChunk::Uncompressed(UncompressedChunk::from_vec(
            samples,
        ))),
    }
}

/// The bucket grid an aggregated GROUPBY reduces on: every member series is bucketed with
/// the same `ALIGN` and bucket duration, so a row's timestamp identifies its bucket and the
/// group's reduce is a fold into a table indexed by bucket, not a merge of sorted streams.
struct BucketGrid {
    /// True start of the bucket the query window opens in; slot 0.
    origin: Timestamp,
    align: Timestamp,
    duration: u64,
    slots: usize,
}

/// More buckets than this and the table is not worth its memory against the merge
/// (a `-`/`+` window over a sparse series with a one-millisecond bucket is legal).
const DENSE_GROUP_MAX_SLOTS: usize = 1 << 20;

/// Bucket rows the dense path may stage at once across every group (64 MiB of
/// samples). The slot cap bounds one group's table, not series × buckets; groups
/// past this budget take the streaming merge instead.
const DENSE_STAGING_MAX_ROWS: usize = 4 << 20;

type DenseGroup<'a> = (String, GroupedSeriesData<'a>, BucketGrid);
type MergedGroup<'a> = (String, GroupedSeriesData<'a>);

/// Splits groups between the bucket table and the merge. A group takes the merge when
/// the grid cannot serve it, when an ascending `COUNT` asks for fewer buckets than the
/// grid spans (the merge stops after `count` buckets; the table decodes all of them),
/// or when staging its rows would exceed `budget` rows across the dense groups.
fn plan_dense_groups<'a>(
    groups: impl IntoIterator<Item = MergedGroup<'a>>,
    options: &MRangeOptions,
    count: Option<usize>,
    budget: usize,
) -> (Vec<DenseGroup<'a>>, Vec<MergedGroup<'a>>) {
    let is_multi = options
        .range
        .aggregation
        .as_ref()
        .is_some_and(|a| a.is_multi());
    let mut dense = Vec::new();
    let mut merged = Vec::new();
    let mut staged = 0usize;
    for (label_value, group_data) in groups {
        let grid = (!is_multi)
            .then(|| BucketGrid::for_query(&options.range, &group_data.series))
            .flatten()
            .filter(|grid| options.is_reverse || count.is_none_or(|c| c >= grid.slots))
            .filter(|grid| {
                let rows: usize = group_data
                    .series
                    .iter()
                    .map(|meta| estimated_bucket_rows(meta, &options.range, grid))
                    .sum();
                let fits = rows <= budget.saturating_sub(staged);
                if fits {
                    staged += rows;
                }
                fits
            });
        match grid {
            Some(grid) => dense.push((label_value, group_data, grid)),
            None => merged.push((label_value, group_data)),
        }
    }
    (dense, merged)
}

/// Upper bound on the rows [`series_bucket_rows`] yields for `meta`: the buckets its own
/// extent spans inside the window, and without `EMPTY` no more than one per sample.
fn estimated_bucket_rows(
    meta: &MRangeSeriesMeta,
    range: &RangeOptions,
    grid: &BucketGrid,
) -> usize {
    let series = meta.series;
    let (mut first, mut last) = if series.is_empty() {
        (Timestamp::MAX, Timestamp::MIN)
    } else {
        (series.first_timestamp, series.last_timestamp())
    };
    if let Some(latest) = meta.latest {
        first = first.min(latest.timestamp);
        last = last.max(latest.timestamp);
    }
    let (start, end) = range.get_timestamp_range();
    let (start, end) = (start.max(first), end.min(last));
    if start > end {
        return 0;
    }
    let buckets = ((end - start) as u64 / grid.duration) as usize + 2;
    let report_empty = range.aggregation.as_ref().is_some_and(|a| a.report_empty);
    let rows = if report_empty {
        buckets
    } else {
        buckets.min(series.total_samples + usize::from(meta.latest.is_some()))
    };
    rows.min(grid.slots)
}

impl BucketGrid {
    /// `None` when the query does not aggregate, when a bucket could start before 0 (the
    /// reported timestamp is clamped there, which folds distinct buckets onto one
    /// timestamp — the merge already treats those as one, but a table cannot address them),
    /// or when the window spans more buckets than the cap.
    fn for_query(range: &RangeOptions, members: &[MRangeSeriesMeta]) -> Option<Self> {
        let agg = range.aggregation.as_ref()?;
        let (start, end) = range.get_timestamp_range();
        // The grid is aligned on the query window, as every member's own iterator aligns it;
        // the table only needs to span the data, so an open `-`/`+` window is clipped to the
        // group's extent (a chained LATEST sample can lie past a series' last stored one).
        let align = agg.alignment.get_aligned_timestamp(start, end);
        let duration = agg.bucket_duration;
        if duration == 0 || members.is_empty() {
            return None;
        }
        let mut data_start = Timestamp::MAX;
        let mut data_end = Timestamp::MIN;
        for meta in members {
            if meta.series.is_empty() {
                continue;
            }
            data_start = data_start.min(meta.series.first_timestamp);
            data_end = data_end.max(meta.series.last_timestamp());
            if let Some(latest) = meta.latest {
                data_start = data_start.min(latest.timestamp);
                data_end = data_end.max(latest.timestamp);
            }
        }
        if data_start > data_end {
            return None;
        }
        let start = start.max(data_start);
        let end = end.min(data_end);
        if start > end {
            return None;
        }
        let origin = bucket_start_for(start, align, duration);
        if origin < 0 {
            return None;
        }
        let last = bucket_start_for(end, align, duration);
        // One slot past the last bucket: BUCKETTIMESTAMP `end` reports a bucket at its
        // upper edge, which `bucket_start_for` maps to the bucket after it.
        let slots = ((last - origin) as u64 / duration) as usize + 2;
        (slots <= DENSE_GROUP_MAX_SLOTS).then_some(Self {
            origin,
            align,
            duration,
            slots,
        })
    }

    /// Slot of a reported bucket timestamp, or `None` for one the grid cannot hold.
    #[inline]
    fn slot(&self, ts: Timestamp) -> Option<usize> {
        let start = bucket_start_for(ts, self.align, self.duration);
        if start < self.origin {
            return None;
        }
        let slot = ((start - self.origin) as u64 / self.duration) as usize;
        (slot < self.slots).then_some(slot)
    }

    /// The timestamp a slot reports: the first row that landed in it decides, so the
    /// table reproduces the merge's output exactly (including `BUCKETTIMESTAMP` mid/end
    /// and a clamped edge) rather than re-deriving it from the slot index.
    fn empty_table(&self, template: &Aggregator) -> DenseGroupTable {
        DenseGroupTable {
            slots: vec![None; self.slots],
            template: template.clone(),
        }
    }
}

/// One reduced bucket in the making: the reducer's state plus whether it accepted
/// anything — the same pair `SampleReducer` keeps per timestamp group.
#[derive(Clone)]
struct DenseSlot {
    timestamp: Timestamp,
    aggregator: Aggregator,
    has_samples: bool,
}

struct DenseGroupTable {
    slots: Vec<Option<DenseSlot>>,
    template: Aggregator,
}

impl DenseGroupTable {
    /// Folds one series' rows in. Rows arrive ascending per series and series are folded in
    /// group order, so each slot sees its contributions in exactly the order the k-way merge
    /// would have yielded them (timestamp, then series) — the reduce is bit-identical.
    /// Returns `false` for a row the grid cannot address; the caller then falls back.
    fn fold(&mut self, grid: &BucketGrid, rows: impl Iterator<Item = Sample>) -> bool {
        for row in rows {
            let Some(slot) = grid.slot(row.timestamp) else {
                return false;
            };
            let entry = self.slots[slot].get_or_insert_with(|| DenseSlot {
                timestamp: row.timestamp,
                aggregator: self.template.clone(),
                has_samples: false,
            });
            if entry.timestamp != row.timestamp {
                // Two reported timestamps in one bucket: not a grid the table can serve.
                return false;
            }
            if entry.aggregator.update(row.timestamp, row.value) {
                entry.has_samples = true;
            }
        }
        true
    }

    fn into_samples(self) -> Vec<Sample> {
        self.slots
            .into_iter()
            .flatten()
            .map(|mut slot| {
                let value = if slot.has_samples {
                    AggregationHandler::finalize(&mut slot.aggregator)
                } else {
                    slot.aggregator.empty_group_value()
                };
                Sample {
                    timestamp: slot.timestamp,
                    value,
                }
            })
            .collect()
    }
}

/// The bucket rows of one member series, ascending — what the merge would have pulled from
/// its iterator, materialised so the decode can run on any thread while the fold keeps
/// series order.
fn series_bucket_rows(meta: &MRangeSeriesMeta, range: &RangeOptions) -> Vec<Sample> {
    create_range_iterator(meta.series, range, meta.latest, false).collect()
}

/// Folds one group's per-series rows into a table; `None` when a row falls outside the grid
/// (the caller then runs the merge for that group).
fn fold_group_rows(
    grid: &BucketGrid,
    template: &Aggregator,
    rows_per_series: &[Vec<Sample>],
) -> Option<Vec<Sample>> {
    let mut table = grid.empty_table(template);
    for rows in rows_per_series {
        if !table.fold(grid, rows.iter().copied()) {
            return None;
        }
    }
    Some(table.into_samples())
}

/// The k-way merge reduce. `handle_grouping` has already tried the bucket table for
/// every group it could serve, so this never retries it.
fn get_grouped_samples(
    series_metas: &[MRangeSeriesMeta],
    options: &MRangeOptions,
    grouping_options: &RangeGroupingOptions,
    count: Option<usize>,
) -> Vec<Sample> {
    // This function gets the raw samples from all series in the group, then applies the grouping
    // reducer across the samples.
    // todo: choose approach based on data size and available memory?
    let is_reverse = options.is_reverse;

    // todo(perf): with sufficient memory, we could parallel load all samples into memory first,
    // and construct the KWayMerge from those. In low memory, we could use the code
    // below which iterates sequentially
    //
    // Per-series iterators must not apply the group reducer: reduction happens once,
    // across series, in the SampleReducer below.
    //
    // They also run ascending (`false`), like `get_grouped_rows` and the shard-side path:
    // the k-way merge below yields its sources' order, and `collect_samples` reverses the
    // *reduced* stream. Feeding it descending sources made the merge's output order depend
    // on it buffering every iterator, which stopped holding once the merge was corrected.
    let iterators = series_metas
        .iter()
        .map(|meta| create_range_iterator(meta.series, &options.range, meta.latest, false))
        .collect::<Vec<_>>();

    let multi_iter = KWayMerge::new(iterators);
    let aggregator = grouping_options.aggregation.create_aggregator();
    let reducer = SampleReducer::new(multi_iter, aggregator);

    collect_samples(reducer, is_reverse, count)
}

/// Multi-aggregation twin of `get_grouped_samples`: per-series row pipeline
/// (bucket aggregation, ascending), k-way merge by bucket timestamp, then a
/// column-wise reduce across the group's series.
fn get_grouped_rows(
    series_metas: &[MRangeSeriesMeta],
    options: &MRangeOptions,
    grouping_options: &RangeGroupingOptions,
    count: Option<usize>,
) -> Vec<MultiSample> {
    let columns = options
        .range
        .aggregation
        .as_ref()
        .map(|a| a.aggregations.len())
        .expect("multi-aggregation grouping requires aggregation options");

    // Per-series pipelines run ascending; reversal and COUNT apply to the
    // reduced rows below.
    let iterators = series_metas
        .iter()
        .map(|meta| create_row_iterator(meta.series, &options.range, meta.latest, false))
        .collect::<Vec<_>>();

    let multi_iter = KWayMerge::new(iterators);
    let aggregator = grouping_options.aggregation.create_aggregator();
    let reducer = RowReducer::new(multi_iter, aggregator, columns);

    collect_rows(reducer, options.is_reverse, count)
}

/// Apply reversal and COUNT to reduced rows. COUNT limits rows in the
/// requested order, so for reverse queries it applies after reversal
/// (returning the latest buckets).
pub(crate) fn collect_rows<I: Iterator<Item = MultiSample>>(
    iter: I,
    is_reverse: bool,
    count: Option<usize>,
) -> Vec<MultiSample> {
    if !is_reverse {
        return match count {
            Some(count) => iter.take(count).collect(),
            None => iter.collect(),
        };
    }
    // Reverse: only the last `count` rows can be returned, so hold no more than that.
    let mut rows: Vec<MultiSample> = match count {
        Some(count) => TailIter::new(iter, count).collect(),
        None => iter.collect(),
    };
    rows.reverse();
    rows
}

/// Apply reversal and COUNT to reduced samples. Like [`collect_rows`], COUNT limits samples in
/// the *requested* order, so a reverse query must reverse before truncating: taking from the
/// (ascending) iterator first would keep the oldest N and then merely reverse those, returning
/// the wrong window entirely rather than just the wrong order.
pub(crate) fn collect_samples<I: Iterator<Item = Sample>>(
    iter: I,
    is_reverse: bool,
    count: Option<usize>,
) -> Vec<Sample> {
    if !is_reverse {
        // Ascending: the requested order matches the iterator, so COUNT can stop it early.
        return match count {
            Some(count) => iter.take(count).collect(),
            None => iter.collect(),
        };
    }

    // Only the last `count` samples can be returned, so hold no more than that.
    let mut samples: Vec<Sample> = match count {
        Some(count) => TailIter::new(iter, count).collect(),
        None => iter.collect(),
    };
    samples.reverse();
    samples
}

fn convert_labels(
    series: &TimeSeries,
    with_labels: bool,
    selected_labels: &[String],
) -> Vec<Label> {
    if !with_labels && selected_labels.is_empty() {
        return Vec::new();
    }

    if selected_labels.is_empty() {
        return series.labels.iter().map(|l| l.into()).collect();
    }

    selected_labels
        .iter()
        .map(|name| {
            series
                .get_label(name)
                .map(|label| label.into())
                .unwrap_or_else(|| Label {
                    name: name.clone(),
                    value: String::new(),
                })
        })
        .collect()
}

pub(crate) fn build_mrange_grouped_labels(
    group_label_name: &str,
    group_label_value: &str,
    reducer_name_str: &str,
    source_identifiers: &[Vec<u8>],
) -> Vec<Label> {
    // The `__source__` label is a label *value*, and labels are UTF-8 strings throughout the
    // module, so a source key holding a non-UTF-8 byte cannot round-trip here. This is the one
    // place a key name is rendered lossily; the reply's own key field stays raw bytes.
    let sources = source_identifiers
        .iter()
        .map(|key| key_for_display(key))
        .collect::<Vec<_>>()
        .join(",");
    vec![
        Label {
            name: group_label_name.into(),
            value: group_label_value.to_string(),
        },
        Label {
            name: REDUCER_KEY.into(),
            value: reducer_name_str.into(),
        },
        Label {
            name: SOURCE_KEY.into(),
            value: sources,
        },
    ]
}

fn collect_group_label_values(metas: &mut Vec<MRangeSeriesMeta>, grouping: &RangeGroupingOptions) {
    for meta in metas.iter_mut() {
        meta.group_label_value = meta
            .series
            .label_value(&grouping.group_label)
            .map(|s| s.to_string());
    }
}

struct GroupedSeriesData<'a> {
    series: Vec<MRangeSeriesMeta<'a>>,
    labels: Vec<Label>,
    source_keys: Vec<Vec<u8>>,
}

fn group_series_by_label<'a>(
    metas: Vec<MRangeSeriesMeta<'a>>,
    grouping: &RangeGroupingOptions,
    with_labels: bool,
) -> AHashMap<String, GroupedSeriesData<'a>> {
    let mut grouped: AHashMap<String, GroupedSeriesData<'a>> = AHashMap::new();
    let group_by_label_name = &grouping.group_label;
    let reducer_name = grouping.aggregation.aggregation_name();

    for mut meta in metas.into_iter() {
        if let Some(label_value_str) = meta.group_label_value.take() {
            let entry = grouped
                .entry(label_value_str)
                .or_insert_with(|| GroupedSeriesData {
                    series: Vec::new(),
                    labels: Vec::new(),
                    source_keys: Vec::new(),
                });
            entry.source_keys.push(meta.source_key.clone());
            entry.series.push(meta);
        }
    }

    for (label_value_str, group_data) in grouped.iter_mut() {
        group_data.source_keys.sort();
        if with_labels {
            group_data.labels = build_mrange_grouped_labels(
                group_by_label_name,
                label_value_str,
                reducer_name,
                &group_data.source_keys,
            );
        }
    }

    grouped
}

// -------- deferred (off-main-thread) MRANGE --------

/// One matched series with everything the deferred decode needs, gathered on
/// the main thread while the series guard is held: the copied chunks, the
/// `LATEST` sample, the EMPTY fill bounds (both need the live series), the
/// reply key and labels.
pub struct SnapshotSeries {
    pub key: Vec<u8>,
    pub labels: Vec<Label>,
    pub snapshot: RangeSnapshot,
    pub latest: Option<Sample>,
    pub empty_fill: EmptyFillBounds,
    /// The GROUPBY label's value, collected for the coordinator on a shard.
    pub group_label_value: Option<String>,
}

/// Whether a snapshot can stand in for the series: multi-aggregation yields
/// rows, and `FILTER_BY_TS` needs the series-backed base reader.
fn snapshot_supports(range: &RangeOptions) -> bool {
    range.timestamp_filter.is_none() && !range.aggregation.as_ref().is_some_and(|a| a.is_multi())
}

/// Whether the non-clustered path can answer `options` from snapshots; grouping
/// needs the reducer pass as well.
pub(crate) fn is_snapshot_answerable(options: &MRangeOptions) -> bool {
    options.grouping.is_none() && snapshot_supports(&options.range)
}

/// Whether a shard can answer its share of a clustered TS.MRANGE from
/// snapshots. Grouping is fine here: a shard only tags each series with its
/// group, and the coordinator reduces.
pub(crate) fn is_shard_snapshot_answerable(options: &MRangeOptions) -> bool {
    snapshot_supports(&options.range)
}

/// Least estimated work (units of [`estimated_work`]) a TS.MRANGE needs before
/// it is deferred. Deferring adds a block/unblock round trip through the event
/// loop — ~100 µs measured when the server is otherwise idle, and lost in the
/// noise under load. Main-thread time runs at ~11 ns per unit (ten series × 100
/// samples ≈ 11k units ≈ 121 µs inline), so this is where deferring frees
/// about as much main-thread time as the round trip costs an idle client.
pub const DEFER_MIN_WORK: usize = 8192;

/// Like [`estimated_work`], but a non-aggregated COUNT caps each series'
/// samples: the decode stops there, deferred or not.
fn estimated_defer_work(guards: &[(SeriesGuard, ValkeyString)], options: &MRangeOptions) -> usize {
    let (start, end) = options.range.get_timestamp_range();
    let cap = match (&options.range.aggregation, options.range.count) {
        (None, Some(count)) => count,
        _ => usize::MAX,
    };
    guards
        .iter()
        .map(|(guard, _)| PER_SERIES_WORK + estimated_samples_in_range(guard, start, end).min(cap))
        .sum()
}

/// A non-clustered, non-grouped TS.MRANGE after its single key lookup: either
/// answered on the main thread, or snapshotted for decode off it.
pub(crate) enum LocalMRange<R> {
    Answered(Vec<MRangeSeriesResult>),
    Deferred(R, Vec<SnapshotSeries>),
}

/// Resolves and ACL-checks the series once, then defers when the request is
/// `contended` or carries at least [`DEFER_MIN_WORK`], and `reserve` yields a
/// worker slot —
/// copying out what the decode needs while the guards are held. Otherwise it
/// answers inline, exactly as [`process_mrange_query`] would. `options` must
/// satisfy [`is_snapshot_answerable`].
pub(crate) fn local_mrange_query<R>(
    ctx: &Context,
    options: &MRangeOptions,
    contended: bool,
    reserve: impl FnOnce() -> Option<R>,
) -> ValkeyResult<LocalMRange<R>> {
    if options.filters.is_empty() {
        return Err(ValkeyError::Str(error_consts::MISSING_FILTER));
    }
    let series_guards = series_by_selectors(ctx, &options.filters, None)?;
    let slot = if contended || estimated_defer_work(&series_guards, options) >= DEFER_MIN_WORK {
        reserve()
    } else {
        None
    };
    let Some(slot) = slot else {
        let metas = series_guards
            .iter()
            .map(|(guard, key)| MRangeSeriesMeta {
                series: guard,
                source_key: key.as_slice().to_vec(),
                group_label_value: None,
                latest: get_latest(&options.range, ctx, guard),
            })
            .collect();
        return process_mrange(metas, options.clone(), MRangeScope::Client, None, &|| false)
            .map(LocalMRange::Answered);
    };
    Ok(LocalMRange::Deferred(
        slot,
        snapshot_series(ctx, &series_guards, options),
    ))
}

/// Copies out of each guarded series what the decode needs; the caller holds
/// the guards, so this runs under the lock.
fn snapshot_series(
    ctx: &Context,
    guards: &[(SeriesGuard, ValkeyString)],
    options: &MRangeOptions,
) -> Vec<SnapshotSeries> {
    let (start, end) = options.range.get_timestamp_range();
    guards
        .iter()
        .map(|(guard, key)| SnapshotSeries {
            key: key.as_slice().to_vec(),
            labels: convert_labels(guard, options.with_labels, &options.selected_labels),
            snapshot: guard.snapshot_range(start, end),
            latest: get_latest(&options.range, ctx, guard),
            empty_fill: range_empty_fill_bounds(
                guard,
                &options.range,
                options.is_reverse,
                options.is_reverse || options.reply_reverse,
            ),
            group_label_value: options
                .grouping
                .as_ref()
                .and_then(|g| guard.label_value(&g.group_label))
                .map(|v| v.to_string()),
        })
        .collect()
}

/// A shard's share of a clustered TS.MRANGE, under the lock: resolve and
/// ACL-check the series, then snapshot them. [`decode_shard_snapshots`] does
/// the rest once the lock is released, so the shard's main thread waits only
/// for the copy, not the decode. `options` must satisfy
/// [`is_shard_snapshot_answerable`].
pub(crate) fn snapshot_shard_mrange(
    ctx: &Context,
    options: &MRangeOptions,
) -> ValkeyResult<Vec<SnapshotSeries>> {
    if options.filters.is_empty() {
        return Err(ValkeyError::Str(error_consts::MISSING_FILTER));
    }
    let series_guards = series_by_selectors(ctx, &options.filters, None)?;
    Ok(snapshot_series(ctx, &series_guards, options))
}

/// Decodes a shard's snapshots as `process_mrange` does for a clustered
/// request: per-series results with their group tags, wire-encoded when
/// `compress`, the COUNT push-down `limit` applied, EXCLUDEEMPTY applied, unsorted.
pub(crate) fn decode_shard_snapshots(
    series: Vec<SnapshotSeries>,
    options: &MRangeOptions,
    limit: Option<SampleLimit>,
    compress: bool,
    expired: &(dyn Fn() -> bool + Sync),
) -> ValkeyResult<Vec<MRangeSeriesResult>> {
    let work = series
        .iter()
        .map(|s| PER_SERIES_WORK + s.snapshot.estimated_samples())
        .sum();
    let threads = request_par_threads(series.len(), work);
    // The decode runs after the lock is released, so it is where a requester that
    // has given up stops this share, as `handle_non_grouped` does under the lock.
    let deadline = ReadDeadline::new(expired);
    let items: Vec<MRangeSeriesResult> = series
        .into_par_rayon()
        .num_threads(threads)
        .map(|s| {
            if deadline.passed() {
                return MRangeSeriesResult::default();
            }
            decode_snapshot(s, options, limit, compress)
        })
        .collect();
    let mut items = deadline.finish(items)?;
    if options.exclude_empty {
        items.retain(|item| !item.data.is_empty());
    }
    Ok(items)
}

/// One snapshot through the pipeline `handle_non_grouped` runs over a live
/// series. Grouping never applies here: a standalone deferred request has none,
/// and a shard leaves the reduce to the coordinator.
fn decode_snapshot(
    s: SnapshotSeries,
    options: &MRangeOptions,
    limit: Option<SampleLimit>,
    compress: bool,
) -> MRangeSeriesResult {
    let has_aggregation = options.range.aggregation.is_some();
    let should_reverse_iter = !has_aggregation && options.is_reverse;
    // The live path's base reader over the copied chunks: decoded lazily, so
    // COUNT stops the decode, descending for a non-aggregated reverse query, and
    // unboxed, so the aggregation inlines the decode as it does on the live path.
    let iter = create_range_iterator_from_base(
        s.snapshot.sample_iter(should_reverse_iter),
        &options.range,
        s.latest,
        options.is_reverse,
        s.empty_fill,
    );
    let samples: Vec<Sample> = match limit {
        Some(limit) => limit.apply(iter).collect(),
        None => iter.collect(),
    };
    let data = if compress {
        SeriesResultData::Chunk(samples_to_chunk_lossless(samples))
    } else {
        SeriesResultData::Chunk(TimeSeriesChunk::Uncompressed(UncompressedChunk::from_vec(
            samples,
        )))
    };
    MRangeSeriesResult {
        group_label_value: s.group_label_value,
        key: s.key,
        labels: s.labels,
        sources: Vec::new(),
        data,
    }
}

/// The off-thread half: decode every snapshot through the same pipeline
/// `handle_non_grouped` uses, apply EXCLUDEEMPTY, sort by key.
pub(crate) fn decode_snapshot_series(
    series: Vec<SnapshotSeries>,
    options: &MRangeOptions,
) -> Vec<MRangeSeriesResult> {
    let mut items: Vec<MRangeSeriesResult> = series
        .into_iter()
        .map(|s| decode_snapshot(s, options, None, false))
        .collect();
    if options.exclude_empty {
        items.retain(|item| !item.data.is_empty());
    }
    sort_mrange_results(&mut items, false);
    items
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregators::AggregationType;
    use crate::series::TimeSeries;
    use crate::series::request_types::{AggregationOptions, AggregatorConfig};

    fn make_series(samples: &[(i64, f64)]) -> TimeSeries {
        let mut series = TimeSeries::default();
        for &(ts, value) in samples {
            let _ = series.add(ts, value, None);
        }
        series
    }

    fn meta<'a>(series: &'a TimeSeries, key: &str, group: Option<&str>) -> MRangeSeriesMeta<'a> {
        MRangeSeriesMeta {
            series,
            source_key: key.into(),
            latest: None,
            group_label_value: group.map(String::from),
        }
    }

    fn multi_options(bucket_duration: u64) -> MRangeOptions {
        MRangeOptions {
            range: RangeOptions {
                date_range: crate::series::TimestampRange::from_timestamps(0, 1000).unwrap(),
                aggregation: Some(AggregationOptions {
                    aggregations: smallvec::smallvec![
                        AggregationType::Avg.into(),
                        AggregationType::Max.into(),
                    ],
                    bucket_duration,
                    timestamp_output: Default::default(),
                    alignment: Default::default(),
                    report_empty: false,
                }),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn keys_of(results: &[MRangeSeriesResult]) -> Vec<&str> {
        // Keys are bytes; every key these tests build is ASCII, so the unwrap doubles as an
        // assertion that nothing mangled them on the way through.
        let mut keys: Vec<&str> = results
            .iter()
            .map(|r| std::str::from_utf8(&r.key).expect("test keys are ASCII"))
            .collect();
        keys.sort();
        keys
    }

    /// EXCLUDEEMPTY drops exactly the series that report nothing, and it judges
    /// the *reported* payload rather than the stored series: a series whose only
    /// in-range sample is NaN still reports and is kept, while one emptied by
    /// FILTER_BY_VALUE is dropped just like one with no samples in the range.
    fn one_series() -> TimeSeries {
        let mut series = TimeSeries::new();
        series.add(100, 1.0, None);
        series
    }

    /// A shard's decode stops once its requester's deadline has passed, and says so instead of
    /// answering with what it skipped.
    #[test]
    fn a_shard_decode_past_its_deadline_is_abandoned() {
        let series = one_series();
        let options = MRangeOptions {
            range: RangeOptions::with_range(0, 500).unwrap(),
            ..Default::default()
        };

        let result = process_mrange(
            vec![meta(&series, "s", None)],
            options,
            MRangeScope::RemoteShard,
            None,
            &|| true,
        );

        match result {
            Err(ValkeyError::Str(msg)) => assert_eq!(msg, error_consts::FANOUT_REQUESTER_GAVE_UP),
            Err(other) => panic!("expected the requester-gave-up error, got {other:?}"),
            Ok(_) => panic!("expected the requester-gave-up error, got results"),
        }
    }

    #[test]
    fn a_shard_decode_within_its_deadline_is_answered() {
        let series = one_series();
        let options = MRangeOptions {
            range: RangeOptions::with_range(0, 500).unwrap(),
            ..Default::default()
        };

        let results = process_mrange(
            vec![meta(&series, "s", None)],
            options,
            MRangeScope::RemoteShard,
            None,
            &|| false,
        )
        .unwrap();

        assert_eq!(keys_of(&results), vec!["s"]);
    }

    /// Time running out after the last series is not abandonment: the read is done.
    #[test]
    fn a_finished_read_is_answered_however_late() {
        let deadline = ReadDeadline::new(&|| true);
        assert!(deadline.finish(()).is_ok(), "nothing was skipped");
        assert!(deadline.passed());
        assert!(deadline.finish(()).is_err());

        let none = ReadDeadline::new(&|| false);
        assert!(!none.passed());
        assert!(none.finish(()).is_ok());
    }

    #[test]
    fn test_exclude_empty_drops_series_with_no_reported_samples() {
        let in_range = make_series(&[(100, 100.0), (400, 400.0)]);
        let out_of_range = make_series(&[(2000, 2000.0)]);
        let nan_only = make_series(&[(150, f64::NAN)]);

        let metas = || {
            vec![
                meta(&in_range, "s", None),
                meta(&out_of_range, "u", None),
                meta(&nan_only, "n", None),
            ]
        };

        let mut options = MRangeOptions {
            range: RangeOptions::with_range(0, 500).unwrap(),
            ..Default::default()
        };

        // Default: every matched series is reported, empty payload included.
        let results = process_mrange(metas(), options.clone(), MRangeScope::Client, None, &|| {
            false
        })
        .unwrap();
        assert_eq!(keys_of(&results), vec!["n", "s", "u"]);

        options.exclude_empty = true;
        let results = process_mrange(metas(), options.clone(), MRangeScope::Client, None, &|| {
            false
        })
        .unwrap();
        assert_eq!(
            keys_of(&results),
            vec!["n", "s"],
            "only `u` reports nothing"
        );

        // Shard side (clustered): same decision, made before the payload ships.
        let results = process_mrange(
            metas(),
            options.clone(),
            MRangeScope::RemoteShard,
            None,
            &|| false,
        )
        .unwrap();
        assert_eq!(keys_of(&results), vec!["n", "s"]);

        // A value filter that removes every sample makes a series empty too.
        options.range.value_filter = Some(crate::series::ValueFilter::new(0.0, 200.0).unwrap());
        let results =
            process_mrange(metas(), options, MRangeScope::Client, None, &|| false).unwrap();
        assert_eq!(
            keys_of(&results),
            vec!["s"],
            "NaN fails the value filter, leaving `n` with nothing to report"
        );
    }

    /// Under AGGREGATION, emptiness is decided on the buckets: a series with no
    /// in-range samples produces none and is dropped, and MREVRANGE ordering
    /// does not change which series survive.
    #[test]
    fn test_exclude_empty_with_aggregation_and_reverse() {
        let in_range = make_series(&[(100, 100.0), (400, 400.0)]);
        let out_of_range = make_series(&[(2000, 2000.0)]);

        let mut options = multi_options(100);
        options.range.date_range = crate::series::TimestampRange::from_timestamps(0, 500).unwrap();
        options.exclude_empty = true;

        for is_reverse in [false, true] {
            options.is_reverse = is_reverse;
            let metas = vec![meta(&in_range, "s", None), meta(&out_of_range, "u", None)];
            let results =
                process_mrange(metas, options.clone(), MRangeScope::Client, None, &|| false)
                    .unwrap();
            assert_eq!(keys_of(&results), vec!["s"], "reverse={is_reverse}");
            assert_eq!(rows_of(&results[0]).len(), 2);
        }
    }

    fn rows_of(result: &MRangeSeriesResult) -> &[MultiSample] {
        match &result.data {
            SeriesResultData::Rows(rows) => rows,
            SeriesResultData::Chunk(_) => panic!("expected multi-aggregation rows"),
        }
    }

    /// Non-grouped local MRANGE with a multi-aggregation clause stores rows.
    #[test]
    fn test_local_non_grouped_multi() {
        let s1 = make_series(&[(0, 1.0), (10, 3.0), (110, 5.0)]);
        let s2 = make_series(&[(0, 8.0), (20, 12.0)]);

        let options = multi_options(100);
        let metas = vec![meta(&s1, "a", None), meta(&s2, "b", None)];

        let mut results =
            handle_non_grouped(metas, options, false, None, &ReadDeadline::new(&|| false));
        sort_mrange_results(&mut results, false);

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].key, b"a");
        let rows = rows_of(&results[0]);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].timestamp, 0);
        assert_eq!(rows[0].values.as_slice(), &[2.0, 3.0]); // avg, max of {1, 3}
        assert_eq!(rows[1].values.as_slice(), &[5.0, 5.0]);

        let rows = rows_of(&results[1]);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values.as_slice(), &[10.0, 12.0]);
    }

    /// Grouped local MRANGE: column-wise reduce across the group's series,
    /// with reverse ordering and COUNT applied to the reduced rows.
    #[test]
    fn test_local_grouped_multi_column_reduce() {
        let s1 = make_series(&[(0, 1.0), (10, 3.0), (110, 5.0)]);
        let s2 = make_series(&[(0, 8.0), (20, 12.0), (250, 7.0)]);

        let mut options = multi_options(100);
        options.grouping = Some(RangeGroupingOptions {
            aggregation: AggregatorConfig::new(AggregationType::Sum, None).unwrap(),
            group_label: "region".into(),
        });

        let metas = vec![meta(&s1, "a", Some("us")), meta(&s2, "b", Some("us"))];
        let results = handle_grouping(metas, options.clone()).unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].key, b"region=us");
        let rows = rows_of(&results[0]);
        // ts 0: avg 2+10=12, max 3+12=15; ts 100: a only; ts 200: b only
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].values.as_slice(), &[12.0, 15.0]);
        assert_eq!(rows[1].values.as_slice(), &[5.0, 5.0]);
        assert_eq!(rows[2].values.as_slice(), &[7.0, 7.0]);

        // reverse + COUNT operate on reduced rows (latest buckets first)
        options.is_reverse = true;
        options.range.count = Some(2);
        let metas = vec![meta(&s1, "a", Some("us")), meta(&s2, "b", Some("us"))];
        let results = handle_grouping(metas, options).unwrap();
        let rows = rows_of(&results[0]);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].timestamp, 200);
        assert_eq!(rows[1].timestamp, 100);
    }

    fn grouped_sum_options(bucket_duration: u64) -> MRangeOptions {
        MRangeOptions {
            range: RangeOptions {
                date_range: crate::series::TimestampRange::from_timestamps(0, 1000).unwrap(),
                aggregation: Some(AggregationOptions {
                    aggregations: smallvec::smallvec![AggregationType::Sum.into()],
                    bucket_duration,
                    timestamp_output: Default::default(),
                    alignment: Default::default(),
                    report_empty: false,
                }),
                ..Default::default()
            },
            grouping: Some(RangeGroupingOptions {
                aggregation: AggregatorConfig::new(AggregationType::Sum, None).unwrap(),
                group_label: "region".into(),
            }),
            ..Default::default()
        }
    }

    fn group<'a>(name: &str, members: Vec<MRangeSeriesMeta<'a>>) -> MergedGroup<'a> {
        let group = GroupedSeriesData {
            series: members,
            labels: Vec::new(),
            source_keys: Vec::new(),
        };
        (name.to_string(), group)
    }

    fn planned_names(planned: &[MergedGroup]) -> Vec<String> {
        planned.iter().map(|(name, _)| name.clone()).collect()
    }

    #[test]
    fn test_dense_plan_leaves_small_ascending_count_to_merge() {
        // 10 buckets of 100 ms over [0, 900]
        let s = make_series(&(0..10).map(|i| (i * 100, 1.0)).collect::<Vec<_>>());
        let options = grouped_sum_options(100);
        let plan = |options: &MRangeOptions, count| {
            let (dense, merged) = plan_dense_groups(
                [group("us", vec![meta(&s, "a", None)])],
                options,
                count,
                usize::MAX,
            );
            (dense.len(), merged.len())
        };

        assert_eq!(plan(&options, None), (1, 0));
        // Ascending COUNT 1 only needs the first bucket: stream it.
        assert_eq!(plan(&options, Some(1)), (0, 1));
        // A COUNT covering the whole grid gains nothing from streaming.
        assert_eq!(plan(&options, Some(1000)), (1, 0));
        // Reverse COUNT drains the merge anyway, so the table stays.
        let mut reverse = options.clone();
        reverse.is_reverse = true;
        assert_eq!(plan(&reverse, Some(1)), (1, 0));
    }

    #[test]
    fn test_dense_plan_respects_staging_budget() {
        // 10 samples, one per bucket: 10 staged rows per series.
        let samples: Vec<(i64, f64)> = (0..10).map(|i| (i * 100, 1.0)).collect();
        let (s1, s2, s3) = (
            make_series(&samples),
            make_series(&samples),
            make_series(&samples),
        );
        let options = grouped_sum_options(100);
        let groups = || {
            vec![
                group("a", vec![meta(&s1, "k1", None), meta(&s2, "k2", None)]),
                group("b", vec![meta(&s3, "k3", None)]),
            ]
        };

        let (dense, merged) = plan_dense_groups(groups(), &options, None, usize::MAX);
        assert_eq!((dense.len(), merged.len()), (2, 0));

        // Room for group "a" (20 rows) but not "a" + "b" (30).
        let (dense, merged) = plan_dense_groups(groups(), &options, None, 25);
        assert_eq!(
            dense.iter().map(|(n, _, _)| n.as_str()).collect::<Vec<_>>(),
            ["a"]
        );
        assert_eq!(planned_names(&merged), ["b"]);

        // A later, smaller group still fits after a larger one is turned away.
        let (dense, merged) = plan_dense_groups(groups(), &options, None, 15);
        assert_eq!(
            dense.iter().map(|(n, _, _)| n.as_str()).collect::<Vec<_>>(),
            ["b"]
        );
        assert_eq!(planned_names(&merged), ["a"]);
    }

    #[test]
    fn test_grouped_ascending_count_matches_full_prefix() {
        let s1 = make_series(&[(0, 1.0), (10, 3.0), (110, 5.0), (220, 2.0)]);
        let s2 = make_series(&[(0, 8.0), (120, 12.0), (250, 7.0)]);
        let metas = || vec![meta(&s1, "a", Some("us")), meta(&s2, "b", Some("us"))];
        let samples = |options: MRangeOptions| {
            let results = handle_grouping(metas(), options).unwrap();
            assert_eq!(results.len(), 1);
            match &results[0].data {
                SeriesResultData::Chunk(chunk) => {
                    crate::series::chunks::ChunkOps::iter(chunk).collect::<Vec<Sample>>()
                }
                SeriesResultData::Rows(_) => panic!("expected single-aggregation samples"),
            }
        };

        let full = samples(grouped_sum_options(100));
        assert_eq!(
            full.iter()
                .map(|s| (s.timestamp, s.value))
                .collect::<Vec<_>>(),
            [(0, 12.0), (100, 17.0), (200, 9.0)]
        );
        let mut counted = grouped_sum_options(100);
        counted.range.count = Some(2);
        assert_eq!(samples(counted), full[..2]);
    }

    fn samples_of(result: &MRangeSeriesResult) -> Vec<Sample> {
        match &result.data {
            SeriesResultData::Chunk(chunk) => {
                crate::series::chunks::ChunkOps::iter(chunk).collect()
            }
            SeriesResultData::Rows(_) => panic!("expected samples"),
        }
    }

    /// A series spread over many small chunks, so reverse decoding crosses chunk edges.
    fn multi_chunk_series(offset: f64) -> TimeSeries {
        let mut series = TimeSeries::new();
        series.chunk_size_bytes = 64;
        for i in 0..300i64 {
            let _ = series.add(i * 10, offset + (i % 17) as f64, None);
        }
        assert!(series.chunks.len() > 4, "fixture must span several chunks");
        series
    }

    /// Readings separated by NaN-only buckets and gaps (250 ms buckets). A NaN-only bucket
    /// right before a reading is where EMPTY's `last` answers differently for a forward
    /// reply (the value held so far) and a reverse one (the next bucket's value).
    fn nan_gap_series() -> TimeSeries {
        let mut series = TimeSeries::new();
        for (ts, value) in [
            (100, 1.0),
            (300, f64::NAN),
            (600, 5.0),
            (1300, f64::NAN),
            (1350, f64::NAN),
            (1600, 9.0),
            (2200, 3.0),
        ] {
            let _ = series.add(ts, value, None);
        }
        series
    }

    #[test]
    fn test_snapshot_sample_iter_matches_live_series() {
        use crate::series::SeriesSampleIterator;
        let series = multi_chunk_series(0.0);
        for (start, end) in [
            (0, 2990),
            (155, 2405),
            (1000, 1000),
            (3000, 4000),
            (500, 100),
        ] {
            let snapshot = series.snapshot_range(start, end);
            for is_reverse in [false, true] {
                let live: Vec<Sample> =
                    SeriesSampleIterator::new(&series, start, end, is_reverse).collect();
                assert_eq!(
                    snapshot.sample_iter(is_reverse).collect::<Vec<_>>(),
                    live,
                    "{start}..{end} reverse={is_reverse}"
                );
            }
        }
    }

    #[test]
    fn test_snapshot_decode_matches_live_path() {
        let (s1, s2) = (multi_chunk_series(0.0), multi_chunk_series(100.0));
        let aggregation = |report_empty| AggregationOptions {
            aggregations: smallvec::smallvec![AggregationType::Sum.into()],
            bucket_duration: 250,
            timestamp_output: Default::default(),
            alignment: Default::default(),
            report_empty,
        };
        for is_reverse in [false, true] {
            for count in [None, Some(1), Some(7), Some(1000)] {
                for agg in [None, Some(aggregation(false)), Some(aggregation(true))] {
                    let options = MRangeOptions {
                        range: RangeOptions {
                            date_range: crate::series::TimestampRange::from_timestamps(95, 2405)
                                .unwrap(),
                            count,
                            aggregation: agg.clone(),
                            ..Default::default()
                        },
                        is_reverse,
                        ..Default::default()
                    };
                    let snapshots = [(&s1, "a"), (&s2, "b")]
                        .into_iter()
                        .map(|(series, key)| {
                            let (start, end) = options.range.get_timestamp_range();
                            SnapshotSeries {
                                key: key.into(),
                                labels: Vec::new(),
                                snapshot: series.snapshot_range(start, end),
                                latest: None,
                                empty_fill: range_empty_fill_bounds(
                                    series,
                                    &options.range,
                                    options.is_reverse,
                                    options.is_reverse,
                                ),
                                group_label_value: None,
                            }
                        })
                        .collect();
                    let deferred = decode_snapshot_series(snapshots, &options);
                    let metas = vec![meta(&s1, "a", None), meta(&s2, "b", None)];
                    let mut live = handle_non_grouped(
                        metas,
                        options.clone(),
                        false,
                        None,
                        &ReadDeadline::new(&|| false),
                    );
                    sort_mrange_results(&mut live, false);

                    let case = format!("reverse={is_reverse} count={count:?} agg={agg:?}");
                    assert_eq!(keys_of(&deferred), keys_of(&live), "{case}");
                    for (d, l) in deferred.iter().zip(&live) {
                        assert_eq!(samples_of(d), samples_of(l), "{case}");
                    }
                }
            }
        }
    }

    /// A shard's snapshot decode gives the coordinator what the live clustered
    /// path gives it: same series, group tags, samples and EXCLUDEEMPTY, under
    /// each COUNT push-down limit.
    #[test]
    fn test_shard_snapshot_decode_matches_live_path() {
        let (s1, s2) = (multi_chunk_series(0.0), multi_chunk_series(100.0));
        let empty = TimeSeries::default();
        let gappy = nan_gap_series();
        // `last` under EMPTY carries values across gaps, and which way depends on the
        // reply's order, which a shard learns from `reply_reverse`.
        let aggregation = |aggregation: AggregationType| AggregationOptions {
            aggregations: smallvec::smallvec![aggregation.into()],
            bucket_duration: 250,
            timestamp_output: Default::default(),
            alignment: Default::default(),
            report_empty: true,
        };
        let aggs = [
            None,
            Some(aggregation(AggregationType::Max)),
            Some(aggregation(AggregationType::Last)),
        ];
        for limit in [None, Some(SampleLimit::Head(5)), Some(SampleLimit::Tail(5))] {
            for agg in &aggs {
                for exclude_empty in [false, true] {
                    for reply_reverse in [false, true] {
                        let options = MRangeOptions {
                            range: RangeOptions {
                                date_range: crate::series::TimestampRange::from_timestamps(
                                    95, 2405,
                                )
                                .unwrap(),
                                aggregation: agg.clone(),
                                ..Default::default()
                            },
                            exclude_empty,
                            reply_reverse,
                            ..Default::default()
                        };
                        let fixtures = [
                            (&s1, "a", "g1"),
                            (&s2, "b", "g2"),
                            (&empty, "c", "g1"),
                            (&gappy, "d", "g2"),
                        ];
                        let (start, end) = options.range.get_timestamp_range();
                        let snapshots = || {
                            fixtures
                                .iter()
                                .map(|(series, key, group)| SnapshotSeries {
                                    key: (*key).into(),
                                    labels: Vec::new(),
                                    snapshot: series.snapshot_range(start, end),
                                    latest: None,
                                    empty_fill: range_empty_fill_bounds(
                                        series,
                                        &options.range,
                                        options.is_reverse,
                                        options.is_reverse || options.reply_reverse,
                                    ),
                                    group_label_value: Some((*group).into()),
                                })
                                .collect()
                        };
                        let mut shard =
                            decode_shard_snapshots(snapshots(), &options, limit, true, &|| false)
                                .unwrap();
                        // The local share skips the wire encoding: same samples, never compressed.
                        let mut local =
                            decode_shard_snapshots(snapshots(), &options, limit, false, &|| false)
                                .unwrap();
                        assert!(local.iter().all(|r| matches!(
                            r.data,
                            SeriesResultData::Chunk(TimeSeriesChunk::Uncompressed(_))
                        )));
                        let metas = fixtures
                            .iter()
                            .map(|(series, key, group)| meta(series, key, Some(group)))
                            .collect();
                        let mut live = handle_non_grouped(
                            metas,
                            options.clone(),
                            true,
                            limit,
                            &ReadDeadline::new(&|| false),
                        );
                        if exclude_empty {
                            live.retain(|item| !item.data.is_empty());
                        }
                        sort_mrange_results(&mut shard, false);
                        sort_mrange_results(&mut local, false);
                        sort_mrange_results(&mut live, false);

                        let case = format!(
                            "limit={limit:?} agg={agg:?} exclude_empty={exclude_empty} \
                             reply_reverse={reply_reverse}"
                        );
                        assert_eq!(keys_of(&shard), keys_of(&live), "{case}");
                        assert_eq!(keys_of(&local), keys_of(&live), "{case}");
                        for ((d, u), l) in shard.iter().zip(&local).zip(&live) {
                            assert_eq!(d.group_label_value, l.group_label_value, "{case}");
                            assert_eq!(u.group_label_value, l.group_label_value, "{case}");
                            assert_eq!(samples_of(d), samples_of(l), "{case}");
                            assert_eq!(samples_of(u), samples_of(l), "{case}");
                        }
                    }
                }
            }
        }
    }

    /// A shard whose requester gave up stops its snapshot decode and answers with the
    /// error, as the live path does under the lock.
    #[test]
    fn test_shard_snapshot_decode_stops_once_expired() {
        let s1 = multi_chunk_series(0.0);
        let options = MRangeOptions {
            range: RangeOptions {
                date_range: crate::series::TimestampRange::from_timestamps(95, 2405).unwrap(),
                ..Default::default()
            },
            ..Default::default()
        };
        let (start, end) = options.range.get_timestamp_range();
        let snapshots = vec![SnapshotSeries {
            key: b"a".to_vec(),
            labels: Vec::new(),
            snapshot: s1.snapshot_range(start, end),
            latest: None,
            empty_fill: Default::default(),
            group_label_value: None,
        }];
        let result = decode_shard_snapshots(snapshots, &options, None, true, &|| true);
        assert!(result.is_err());
    }

    /// The coordinator's own share skips the wire encoding: the same per-series
    /// samples as a remote shard sends, never compressed.
    #[test]
    fn test_local_shard_scope_skips_wire_compression() {
        let (s1, s2) = (multi_chunk_series(0.0), multi_chunk_series(100.0));
        let options = MRangeOptions {
            range: RangeOptions {
                date_range: crate::series::TimestampRange::from_timestamps(95, 2405).unwrap(),
                ..Default::default()
            },
            ..Default::default()
        };
        let run = |scope| {
            let metas = vec![meta(&s1, "a", None), meta(&s2, "b", None)];
            let mut results =
                process_mrange(metas, options.clone(), scope, None, &|| false).unwrap();
            sort_mrange_results(&mut results, false);
            results
        };
        let (remote, local) = (run(MRangeScope::RemoteShard), run(MRangeScope::LocalShard));
        assert!(remote.iter().all(|r| matches!(
            r.data,
            SeriesResultData::Chunk(ref c) if !matches!(c, TimeSeriesChunk::Uncompressed(_))
        )));
        assert!(local.iter().all(|r| matches!(
            r.data,
            SeriesResultData::Chunk(TimeSeriesChunk::Uncompressed(_))
        )));
        assert_eq!(keys_of(&local), keys_of(&remote));
        for (l, r) in local.iter().zip(&remote) {
            assert_eq!(samples_of(l), samples_of(r));
        }
    }
}
