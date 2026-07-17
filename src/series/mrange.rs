use crate::aggregators::{PartialReducer, PartialRowReducer, PartialSampleReducer, PartialState};
use crate::common::constants::{REDUCER_KEY, SOURCE_KEY};
use crate::common::{MultiSample, Sample, Timestamp};
use crate::error_consts;
use crate::iterators::create_sample_iterator_adapter;
use crate::iterators::{
    MultiSeriesRowIter, MultiSeriesSampleIter, RowReducer, SampleReducer, TailIter,
    create_range_iterator, create_row_iterator,
};
use crate::labels::Label;
use crate::series::acl::check_metadata_permissions;
use crate::series::chunks::{TimeSeriesChunk, UncompressedChunk, samples_to_chunk_lossless};
use crate::series::index::series_by_selectors;
use crate::series::request_types::{
    MRangeOptions, MRangeSeriesResult, RangeGroupingOptions, RangeOptions, SeriesResultData,
};
use crate::series::{TimeSeries, get_latest_compaction_sample};
use ahash::AHashMap;
use orx_parallel::{IntoParIter, IterIntoParIter, ParIter};
use valkey_module::{Context, ValkeyError, ValkeyResult};

struct MRangeSeriesMeta<'a> {
    series: &'a TimeSeries,
    source_key: String,
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
    pub source_keys: Vec<String>,
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
            source_key: key.to_string(),
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

    Ok(grouped
        .into_iter()
        .iter_into_par()
        .map(|(label_value, group_data)| {
            let mut source_keys: Vec<String> = group_data
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

                let multi_iter = MultiSeriesRowIter::new(iterators);
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
                        create_range_iterator(
                            meta.series,
                            &options.range,
                            &None,
                            meta.latest,
                            false,
                        )
                    })
                    .collect::<Vec<_>>();

                let multi_iter = MultiSeriesSampleIter::new(iterators);
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
        .collect())
}

pub(crate) fn process_mrange_query(
    ctx: &Context,
    options: MRangeOptions,
    clustered: bool,
    limit: Option<SampleLimit>,
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
            source_key: key.to_string(),
            group_label_value: None,
            latest: {
                // This is done upfront to enable parallel series processing below
                // (Context cannot be shared across threads)
                get_latest(&options.range, ctx, guard)
            },
        })
        .collect();

    process_mrange(series_metas, options, clustered, limit)
}

fn process_mrange(
    metas: Vec<MRangeSeriesMeta>,
    options: MRangeOptions,
    is_clustered: bool,
    limit: Option<SampleLimit>,
) -> ValkeyResult<Vec<MRangeSeriesResult>> {
    let mut options = options;
    let mut metas = metas;

    if let Some(grouping) = &options.grouping {
        collect_group_label_values(&mut metas, grouping);
        // Is_clustered here means that we are being called from a remote node. Since grouping
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

    if is_clustered {
        return Ok(handle_non_grouped(metas, options, true, limit));
    }

    let mut items = if is_grouped {
        handle_grouping(metas, options)?
    } else {
        handle_non_grouped(metas, options, false, None)
    };

    sort_mrange_results(&mut items, is_grouped);

    Ok(items)
}

fn get_latest(options: &RangeOptions, ctx: &Context, series: &TimeSeries) -> Option<Sample> {
    if !options.latest || !series.is_compaction() {
        return None;
    }
    get_latest_compaction_sample(ctx, series).filter(|s| {
        let (start_ts, end_ts) = options.get_timestamp_range();
        let ts = s.timestamp;

        if ts < start_ts || ts > end_ts {
            return false;
        }

        if !options.value_filter.is_none_or(|vf| vf.is_match(s.value)) {
            return false;
        }

        if !options
            .timestamp_filter
            .as_ref()
            .is_none_or(|ts_vec| ts_vec.contains(&ts))
        {
            return false;
        }

        true
    })
}

fn create_iter<'a>(
    series: &'a TimeSeries,
    options: &MRangeOptions,
    latest: Option<Sample>,
) -> Box<dyn Iterator<Item = Sample> + 'a> {
    create_range_iterator(
        series,
        &options.range,
        &options.grouping,
        latest,
        options.is_reverse,
    )
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
    clustered: bool,
    limit: Option<SampleLimit>,
) -> Vec<MRangeSeriesResult> {
    let is_multi = options
        .range
        .aggregation
        .as_ref()
        .is_some_and(|a| a.is_multi());

    metas
        .into_par()
        .map(|meta| {
            // Multi-aggregation yields rows, which chunks cannot store. Under
            // aggregation push-down a shard produces these rows too; they ship
            // as per-column chunks (SeriesRangeResponse.columns), so the
            // clustered path is valid here.
            let data = if is_multi {
                let iter = create_row_iterator(
                    meta.series,
                    &options.range,
                    meta.latest,
                    options.is_reverse,
                );
                let iter = match limit {
                    Some(limit) => limit.apply(iter),
                    None => iter,
                };
                SeriesResultData::Rows(iter.collect())
            } else {
                let iter = create_iter(meta.series, &options, meta.latest);
                let iter = match limit {
                    Some(limit) => limit.apply(iter),
                    None => iter,
                };
                let samples = iter.collect::<Vec<_>>();
                // Only the clustered response crosses the network, so only it is
                // worth compressing — and only once it holds enough samples to
                // pay for the chunk header (see `samples_to_chunk`).
                if clustered {
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

    let items = grouped_series_map
        .into_iter()
        .iter_into_par()
        .map(|(label_value, group_data)| {
            let grouping = options
                .grouping
                .as_ref()
                .expect("Grouping options should be present");
            let is_multi = options
                .range
                .aggregation
                .as_ref()
                .is_some_and(|a| a.is_multi());
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
            let key = format!("{}={}", grouping.group_label, label_value);
            MRangeSeriesResult {
                key,
                group_label_value: Some(label_value),
                labels,
                sources: group_data.source_keys,
                data,
            }
        })
        .collect::<Vec<_>>();

    Ok(items)
}

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
    // and construct the MultiSeriesSampleIter from those. In low memory, we could use the code
    // below which iterates sequentially
    //
    // Per-series iterators must not apply the group reducer: reduction happens once,
    // across series, in the SampleReducer below.
    let iterators = series_metas
        .iter()
        .map(|meta| {
            create_range_iterator(
                meta.series,
                &options.range,
                &None,
                meta.latest,
                options.is_reverse,
            )
        })
        .collect::<Vec<_>>();

    let multi_iter = MultiSeriesSampleIter::new(iterators);
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

    let multi_iter = MultiSeriesRowIter::new(iterators);
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
    let mut rows: Vec<MultiSample> = iter.collect();
    if is_reverse {
        rows.reverse();
    }
    if let Some(count) = count {
        rows.truncate(count);
    }
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

    let mut samples: Vec<Sample> = iter.collect();
    samples.reverse();
    if let Some(count) = count {
        samples.truncate(count);
    }
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
    source_identifiers: &[String],
) -> Vec<Label> {
    let sources = source_identifiers.join(",");
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
    source_keys: Vec<String>,
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

pub fn create_mrange_iterator_adapter<'a>(
    base_iter: impl Iterator<Item = Sample> + 'a,
    options: &MRangeOptions,
) -> Box<dyn Iterator<Item = Sample> + 'a> {
    create_sample_iterator_adapter(
        base_iter,
        &options.range,
        &options.grouping,
        options.is_reverse,
    )
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

        let mut results = handle_non_grouped(metas, options, false, None);
        sort_mrange_results(&mut results, false);

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].key, "a");
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
        assert_eq!(results[0].key, "region=us");
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
}
