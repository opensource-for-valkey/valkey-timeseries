use super::fanout_codec::generated::{Label as FanoutLabel, Sample as FanoutSample};
use crate::commands::fanout_codec::MGetValue;
use crate::common::constants::{REDUCER_KEY, SOURCE_KEY};
use crate::common::replies::{
    IntoRawCtx, is_resp3_client, reply_label_ex, reply_with_array, reply_with_bulk_string,
    reply_with_labels, reply_with_labels_map, reply_with_map, reply_with_multi_samples,
    reply_with_sample_ex, reply_with_samples,
};
use crate::labels::Label;
use crate::series::request_types::{MRangeOptions, MRangeSeriesResult, SeriesResultData};
use valkey_module::{Context, Status, ValkeyResult, ValkeyValue, raw};

pub(super) fn reply_with_fanout_label<C: IntoRawCtx>(ctx: C, label: &FanoutLabel) {
    let raw_ctx = ctx.into_raw();
    if label.name.is_empty() {
        raw::reply_with_null(raw_ctx);
        return;
    }
    // A present name with an empty value means "label not on this series"
    // (SELECTED_LABELS): reply [name, nil], as reply_label does elsewhere.
    let value = (!label.value.is_empty()).then_some(label.value.as_str());
    reply_label_ex(raw_ctx, &label.name, value);
}

pub(super) fn reply_with_fanout_labels<C: IntoRawCtx>(ctx: C, v: &[FanoutLabel]) {
    let raw_ctx = ctx.into_raw();
    reply_with_array(raw_ctx, v.len());
    for label in v {
        reply_with_fanout_label(raw_ctx, label);
    }
}

pub fn reply_with_fanout_sample<C: IntoRawCtx>(ctx: C, sample: &Option<FanoutSample>) {
    let raw_ctx = ctx.into_raw();
    if let Some(s) = sample {
        reply_with_sample_ex(raw_ctx, s.timestamp, s.value);
    } else {
        reply_with_array(raw_ctx, 0);
    }
}

/// Reply-shape details for TS.MRANGE / TS.MREVRANGE that live in the request
/// options rather than in the per-series results: the per-series aggregator
/// names and the GROUPBY label/REDUCE name. RESP3 replies report these in
/// dedicated metadata maps. Capture the shape BEFORE the options are consumed
/// (or mutated by the fanout push-down machinery).
pub(super) struct MRangeReplyShape {
    aggregators: Vec<&'static str>,
    grouping: Option<GroupingReplyShape>,
}

struct GroupingReplyShape {
    reducer: &'static str,
}

impl MRangeReplyShape {
    pub(super) fn from_options(options: &MRangeOptions) -> Self {
        let aggregators = options
            .range
            .aggregation
            .as_ref()
            .map(|agg| {
                agg.aggregations
                    .iter()
                    .map(|config| config.aggregation_name())
                    .collect()
            })
            .unwrap_or_default();
        let grouping = options.grouping.as_ref().map(|g| GroupingReplyShape {
            reducer: g.aggregation.aggregation_name(),
        });
        Self {
            aggregators,
            grouping,
        }
    }
}

pub fn reply_with_mrange_series_result(ctx: &Context, series: &MRangeSeriesResult) {
    reply_with_array(ctx, 3);

    reply_with_bulk_string(ctx, &series.key);

    // series.labels has the same count as selected_labels
    reply_with_labels(ctx, &series.labels);

    reply_with_mrange_series_samples(ctx, series);
}

fn reply_with_mrange_series_samples(ctx: &Context, series: &MRangeSeriesResult) {
    match &series.data {
        SeriesResultData::Chunk(chunk) => reply_with_samples(ctx, chunk.iter()),
        SeriesResultData::Rows(rows) => reply_with_multi_samples(ctx, rows.iter()),
    }
}

/// A synthetic label injected into grouped WITHLABELS replies (RESP2 surface);
/// RESP3 reports the same information in the reducers/sources metadata maps
/// and keeps only the group label in the label map.
fn is_synthetic_group_label(label: &Label) -> bool {
    label.name == REDUCER_KEY || label.name == SOURCE_KEY
}

pub(super) fn reply_with_mrange_series_results(
    ctx: &Context,
    series_results: &[MRangeSeriesResult],
    shape: &MRangeReplyShape,
) -> ValkeyResult {
    if !is_resp3_client(ctx) {
        reply_with_array(ctx, series_results.len());
        for series in series_results {
            reply_with_mrange_series_result(ctx, series);
        }
        return Ok(ValkeyValue::NoReply);
    }

    // RESP3: a map keyed by series key (or group key). Non-grouped entries are
    // [labels-map, {aggregators: [...]}, samples]; grouped entries are
    // [labels-map, {reducers: [...]}, {sources: [...]}, samples].
    reply_with_map(ctx, series_results.len());
    for series in series_results {
        reply_with_bulk_string(ctx, &series.key);
        if let Some(grouping) = &shape.grouping {
            reply_with_array(ctx, 4);
            let labels: Vec<&Label> = series
                .labels
                .iter()
                .filter(|l| !is_synthetic_group_label(l))
                .collect();
            reply_with_labels_map(ctx, labels.into_iter());

            reply_with_map(ctx, 1);
            reply_with_bulk_string(ctx, "reducers");
            reply_with_array(ctx, 1);
            reply_with_bulk_string(ctx, grouping.reducer);

            reply_with_map(ctx, 1);
            reply_with_bulk_string(ctx, "sources");
            reply_with_array(ctx, series.sources.len());
            for source in &series.sources {
                reply_with_bulk_string(ctx, source);
            }
        } else {
            reply_with_array(ctx, 3);
            reply_with_labels_map(ctx, series.labels.iter());

            reply_with_map(ctx, 1);
            reply_with_bulk_string(ctx, "aggregators");
            reply_with_array(ctx, shape.aggregators.len());
            for name in &shape.aggregators {
                reply_with_bulk_string(ctx, name);
            }
        }
        reply_with_mrange_series_samples(ctx, series);
    }
    Ok(ValkeyValue::NoReply)
}

pub(super) fn reply_with_mget_values<C: IntoRawCtx>(ctx: C, values: &[MGetValue]) -> ValkeyResult {
    let raw_ctx = ctx.into_raw();
    if is_resp3_client(raw_ctx) {
        // RESP3: map keyed by series key; each value is [labels-map, sample].
        reply_with_map(raw_ctx, values.len());
        for value in values {
            reply_with_bulk_string(raw_ctx, value.key.as_str());
            reply_with_array(raw_ctx, 2);
            reply_with_fanout_labels_map(raw_ctx, &value.labels);
            reply_with_fanout_sample(raw_ctx, &value.sample);
        }
        return Ok(ValkeyValue::NoReply);
    }
    reply_with_array(raw_ctx, values.len());
    for value in values {
        reply_with_mget_value(raw_ctx, value);
    }
    Ok(ValkeyValue::NoReply)
}

fn reply_with_fanout_labels_map<C: IntoRawCtx>(ctx: C, labels: &[FanoutLabel]) {
    let raw_ctx = ctx.into_raw();
    // Nameless entries carry no renderable information in map form.
    let named = labels.iter().filter(|l| !l.name.is_empty());
    reply_with_map(raw_ctx, named.clone().count());
    for label in named {
        reply_with_bulk_string(raw_ctx, &label.name);
        if label.value.is_empty() {
            raw::reply_with_null(raw_ctx);
        } else {
            reply_with_bulk_string(raw_ctx, &label.value);
        }
    }
}

fn reply_with_mget_value<C: IntoRawCtx>(ctx: C, value: &MGetValue) -> Status {
    let raw_ctx = ctx.into_raw();
    reply_with_array(raw_ctx, 3);
    reply_with_bulk_string(raw_ctx, value.key.as_str());
    reply_with_fanout_labels(raw_ctx, &value.labels);
    reply_with_fanout_sample(raw_ctx, &value.sample);
    Status::Ok
}
