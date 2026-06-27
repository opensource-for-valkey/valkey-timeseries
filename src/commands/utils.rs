use super::fanout_codec::generated::{Label as FanoutLabel, Sample as FanoutSample};
use crate::commands::fanout_codec::MGetValue;
use crate::common::replies::{
    IntoRawCtx, ThreadSafeReplyContext, reply_label_ex, reply_with_array, reply_with_bulk_string,
    reply_with_double, reply_with_labels, reply_with_map, reply_with_multi_samples,
    reply_with_sample_ex, reply_with_samples, reply_with_str,
};
use crate::series::request_types::{MRangeSeriesResult, SeriesResultData};
use anofox_forecast::utils::AccuracyMetrics;
use valkey_module::{raw, Context, Status, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

pub(super) fn reply_with_fanout_label<C: IntoRawCtx>(ctx: C, label: &FanoutLabel) {
    let raw_ctx = ctx.into_raw();
    if label.name.is_empty() {
        raw::reply_with_null(raw_ctx);
        return;
    }
    reply_label_ex(raw_ctx, &label.name, Some(&label.value));
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

pub fn reply_with_mrange_series_result(ctx: &Context, series: &MRangeSeriesResult) {
    reply_with_array(ctx, 3);

    reply_with_bulk_string(ctx, &series.key);

    // series.labels has the same count as selected_labels
    reply_with_labels(ctx, &series.labels);

    match &series.data {
        SeriesResultData::Chunk(chunk) => reply_with_samples(ctx, chunk.iter()),
        SeriesResultData::Rows(rows) => reply_with_multi_samples(ctx, rows.iter()),
    }
}

pub(super) fn reply_with_mrange_series_results(
    ctx: &Context,
    series_results: &[MRangeSeriesResult],
) -> ValkeyResult {
    reply_with_array(ctx, series_results.len());
    for series in series_results {
        reply_with_mrange_series_result(ctx, series);
    }
    Ok(ValkeyValue::NoReply)
}

pub(super) fn reply_with_mget_values<C: IntoRawCtx>(ctx: C, values: &[MGetValue]) -> ValkeyResult {
    let raw_ctx = ctx.into_raw();
    reply_with_array(raw_ctx, values.len());
    for value in values {
        reply_with_mget_value(raw_ctx, value);
    }
    Ok(ValkeyValue::NoReply)
}

fn reply_with_mget_value<C: IntoRawCtx>(ctx: C, value: &MGetValue) -> Status {
    let raw_ctx = ctx.into_raw();
    reply_with_array(raw_ctx, 3);
    reply_with_bulk_string(raw_ctx, value.key.as_str());
    reply_with_fanout_labels(raw_ctx, &value.labels);
    reply_with_fanout_sample(raw_ctx, &value.sample);
    Status::Ok
}

pub fn reply_with_accuracy_metrics(ctx: &Context, metrics: &AccuracyMetrics) {
    reply_with_map(ctx, 7);

    reply_with_str(ctx, "mae");
    reply_with_double(ctx, metrics.mae);

    reply_with_str(ctx, "mse");
    reply_with_double(ctx, metrics.mse);

    reply_with_str(ctx, "rmse");
    reply_with_double(ctx, metrics.rmse);

    reply_with_str(ctx, "mape");
    if let Some(v) = metrics.mape {
        reply_with_double(ctx, v);
    } else {
        crate::common::replies::reply_with_null(ctx);
    }

    reply_with_str(ctx, "smape");
    reply_with_double(ctx, metrics.smape);

    reply_with_str(ctx, "mase");
    if let Some(v) = metrics.mase {
        reply_with_double(ctx, v);
    } else {
        crate::common::replies::reply_with_null(ctx);
    }

    reply_with_str(ctx, "r_squared");
    reply_with_double(ctx, metrics.r_squared);
}

pub(super) fn reply_with_double_array(ctx: &ThreadSafeReplyContext, values: &[f64]) {
    reply_with_array(ctx, values.len());
    for value in values {
        reply_with_double(ctx, *value);
    }
}

pub(super) fn get_store_key_pos(args: &[ValkeyString]) -> ValkeyResult<Option<usize>> {
    for (i, arg) in args.iter().enumerate() {
        if arg.eq_ignore_ascii_case(b"store") {
            if i + 1 >= args.len() {
                return Err(ValkeyError::Str("TSDB: Missing value for STORE argument"));
            }
            return Ok(Some(i + 1));
        }
    }
    Ok(None)
}