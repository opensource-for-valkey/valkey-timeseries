use crate::common::context::ClientReplyContext;
use crate::common::{Sample, Timestamp};
use crate::labels::Label;
use crate::promql::engine::{ConcreteSeriesQuerier, QueryReader};
use crate::promql::{EvalLabels, EvalSample, EvalSamples, ExprResult, QueryValue};
use promql_parser::parser::value::ValueType;
use std::sync::Arc;
use valkey_module::{Context, Status};

/// The query reader that serves one PromQL command, scoped to the shards owning
/// `hash_tags` (empty for an unscoped query). One reader serves every selector
/// in the expression, so the whole expression is evaluated over one shard set.
pub(super) fn get_promql_querier(ctx: &Context, hash_tags: Vec<String>) -> Arc<dyn QueryReader> {
    let querier = ConcreteSeriesQuerier::create_with_hash_tags(ctx, Arc::from(hash_tags));
    Arc::new(querier)
}

pub(super) fn write_samples(ctx: &ClientReplyContext, samples: &[Sample]) -> Status {
    ctx.reply_with_array(samples.len());
    for sample in samples {
        ctx.reply_with_sample(sample);
    }
    Status::Ok
}

/// A label set the reply writer can walk without caring how it is stored:
/// the evaluator's `EvalLabels` (possibly interned straight from storage) or
/// the output boundary's owned `[Label]`.
pub trait ReplyLabels {
    fn len(&self) -> usize;
    fn for_each_label(&self, f: impl FnMut(&str, &str));
}

impl ReplyLabels for EvalLabels {
    fn len(&self) -> usize {
        EvalLabels::len(self)
    }
    fn for_each_label(&self, mut f: impl FnMut(&str, &str)) {
        for label in self.iter() {
            f(label.name, label.value);
        }
    }
}

impl ReplyLabels for [Label] {
    fn len(&self) -> usize {
        <[Label]>::len(self)
    }
    fn for_each_label(&self, mut f: impl FnMut(&str, &str)) {
        for label in self {
            f(&label.name, &label.value);
        }
    }
}

fn write_metric_hash(ctx: &ClientReplyContext, labels: &(impl ReplyLabels + ?Sized)) -> Status {
    ctx.reply_with_map(labels.len());
    labels.for_each_label(|name, value| {
        ctx.reply_with_string_key(name);
        ctx.reply_with_bulk_string(value);
    });
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
    metric: &(impl ReplyLabels + ?Sized),
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
        reply_with_range_sample(ctx, &sample.labels, &sample.values);
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
    metric: &(impl ReplyLabels + ?Sized),
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
        reply_with_instant_sample(ctx, &s.labels, s.timestamp_ms, s.value);
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
