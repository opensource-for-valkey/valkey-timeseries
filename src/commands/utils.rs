use super::fanout_codec::generated::{Label as FanoutLabel, Sample as FanoutSample};
use crate::commands::fanout_codec::MGetValue;
use crate::common::context::ClientReplyContext;
use crate::common::replies::{
    IntoRawCtx, reply_label_ex, reply_with_array, reply_with_bulk_string, reply_with_labels,
    reply_with_sample_ex, reply_with_samples,
};
use crate::common::replies::reply_with_multi_samples;
use crate::common::{Sample, Timestamp};
use crate::labels::Label;
use crate::series::request_types::{MRangeSeriesResult, SeriesResultData};
use std::os::raw::c_long;
use valkey_module::{
    Context, Status, VALKEYMODULE_POSTPONED_ARRAY_LEN, ValkeyResult, ValkeyValue, raw,
};

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

impl ClientReplyContext {
    pub fn reply_with_label(&self, label: &str, value: &str) {
        let value = if value.is_empty() { None } else { Some(value) };
        self.reply_with_label_raw(label, value);
    }

    pub fn reply_with_labels(&self, labels: &[Label]) {
        self.reply_with_array(labels.len());
        for label in labels {
            self.reply_with_label_raw(&label.name, Some(&label.value));
        }
    }

    pub fn reply_with_label_raw(&self, label: &str, value: Option<&str>) {
        self.reply_with_array(2);
        self.reply_with_bulk_string(label);
        if let Some(value) = value {
            self.reply_with_bulk_string(value);
        } else {
            self.reply_with_null();
        }
    }

    pub fn reply_with_sample_raw(&self, timestamp: Timestamp, value: f64) -> Status {
        self.reply_with_array(2);
        self.reply_with_i64(timestamp);
        self.reply_with_f64(value)
    }

    #[inline]
    pub fn reply_with_sample(&self, sample: &Sample) -> Status {
        self.reply_with_sample_raw(sample.timestamp, sample.value)
    }

    pub fn reply_with_samples(&self, samples: impl Iterator<Item = Sample>) {
        raw::reply_with_array(self.ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN as c_long);

        let mut len = 0;
        for sample in samples {
            self.reply_with_sample(&sample);
            len += 1;
        }

        self.reply_with_array(len);
    }
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
