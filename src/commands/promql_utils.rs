use crate::common::context::ClientReplyContext;
use crate::common::{Sample, Timestamp};
use crate::labels::Label;
use crate::promql::engine::{ConcreteSeriesQuerier, SeriesQuerier};
use crate::promql::{EvalSample, EvalSamples, ExprResult, QueryValue};
use promql_parser::parser::value::ValueType;
use std::sync::Arc;
use valkey_module::{Context, Status};

pub(super) fn get_promql_querier(ctx: &Context) -> Arc<dyn SeriesQuerier> {
    let querier = ConcreteSeriesQuerier::create(ctx);
    Arc::new(querier)
}

pub(super) fn write_samples(ctx: &ClientReplyContext, samples: &[Sample]) -> Status {
    ctx.reply_with_array(samples.len());
    for sample in samples {
        ctx.reply_with_sample(sample);
    }
    Status::Ok
}

fn write_metric_hash(ctx: &ClientReplyContext, labels: &[Label]) -> Status {
    ctx.reply_with_map(labels.len());
    for label in labels {
        ctx.reply_with_string_key(&label.name);
        ctx.reply_with_bulk_string(&label.value);
    }
    Status::Ok
}

/// For an individual series returned from a range query, return the metric labels and corresponding samples.
/// Equivalent to the following JSON
/// ``` json
///     {
///         "metric" : {
///             "__name__" : "up",
///             "job" : "prometheus",
///             "instance" : "localhost:9090"
///         },
///         "values" : [
///             [ 1435781430.781, "1" ],
///             [ 1435781445.781, "1" ],
///             [ 1435781460.781, "1" ]
///         ]
///     }
///
/// ```
fn reply_with_range_sample(
    ctx: &ClientReplyContext,
    metric: &[Label],
    values: &[Sample],
) -> Status {
    ctx.reply_with_map(2);
    ctx.reply_with_string_key("metric");
    write_metric_hash(ctx, metric);
    ctx.reply_with_string_key("value");
    ctx.reply_with_array(values.len());
    for sample in values {
        ctx.reply_with_sample(sample);
    }
    Status::Ok
}

pub(super) fn reply_with_matrix(ctx: &ClientReplyContext, samples: &[EvalSamples]) -> Status {
    ctx.reply_with_array(samples.len());
    for sample in samples {
        reply_with_range_sample(ctx, sample.labels.as_ref(), &sample.values);
    }
    Status::Ok
}

/// For an individual series returned from an instant query, return the metric labels and value at the specified timestamp.
/// ``` json
/// {
///   "metric" : {
///      "__name__" : "up",
///      "job" : "prometheus",
///      "instance" : "localhost:9090"
///    },
///    "value": [ 1435781451.781, "1" ]
/// }
/// ```
pub fn reply_with_instant_sample(
    ctx: &ClientReplyContext,
    metric: &[Label],
    ts: Timestamp,
    value: f64,
) -> Status {
    ctx.reply_with_map(2);
    ctx.reply_with_string_key("metric");
    write_metric_hash(ctx, metric);
    ctx.reply_with_string_key("value");
    ctx.reply_with_sample(&Sample::new(ts, value));
    Status::Ok
}

pub(super) fn reply_with_instant_vector(ctx: &ClientReplyContext, sample: &[EvalSample]) -> Status {
    ctx.reply_with_array(sample.len());
    for s in sample {
        reply_with_instant_sample(ctx, s.labels.as_ref(), s.timestamp_ms, s.value);
    }
    Status::Ok
}

fn reply_with_value_type(ctx: &ClientReplyContext, value_type: ValueType) -> Status {
    match value_type {
        ValueType::Scalar => ctx.reply_with_string_key("scalar"),
        ValueType::String => ctx.reply_with_string_key("string"),
        ValueType::Matrix => ctx.reply_with_string_key("matrix"),
        ValueType::Vector => ctx.reply_with_string_key("vector"),
    }
}

fn reply_with_string_value(ctx: &ClientReplyContext, timestamp: Timestamp, value: &str) -> Status {
    ctx.reply_with_array(2);
    ctx.reply_with_i64(timestamp);
    ctx.reply_with_simple_string(value);
    Status::Ok
}

pub(super) fn reply_with_expr_result(
    ctx: &ClientReplyContext,
    result: ExprResult,
    eval_ts: Timestamp,
) -> Status {
    ctx.reply_with_map(2);
    ctx.reply_with_string_key("resultType");
    reply_with_value_type(ctx, result.value_type());
    ctx.reply_with_string_key("result");
    match result {
        ExprResult::InstantVector(samples) => reply_with_instant_vector(ctx, &samples),
        ExprResult::RangeVector(samples) => reply_with_matrix(ctx, &samples),
        ExprResult::Scalar(value) => ctx.reply_with_sample(&Sample::new(eval_ts, value)),
        ExprResult::String(value) => reply_with_string_value(ctx, eval_ts, &value),
    }
}

pub(super) fn reply_with_query_value(
    ctx: &ClientReplyContext,
    value: QueryValue,
    eval_ts: Timestamp,
) -> Status {
    ctx.reply_with_map(2);
    ctx.reply_with_string_key("resultType");
    let value_type = value.value_type().to_string();
    ctx.reply_with_bulk_string(&value_type);
    ctx.reply_with_string_key("result");

    match value {
        QueryValue::Vector(values) => {
            ctx.reply_with_array(values.len());
            for sample in values {
                reply_with_instant_sample(
                    ctx,
                    sample.labels.as_ref(),
                    sample.timestamp_ms,
                    sample.value,
                );
            }
        }
        QueryValue::Matrix(values) => {
            ctx.reply_with_array(values.len());
            for sample in values {
                reply_with_range_sample(ctx, sample.labels.as_ref(), &sample.samples);
            }
        }
        QueryValue::Scalar {
            timestamp_ms,
            value,
        } => {
            ctx.reply_with_sample(&Sample::new(timestamp_ms, value));
        }
        QueryValue::String(value) => {
            reply_with_string_value(ctx, eval_ts, &value);
        }
    }
    Status::Ok
}
