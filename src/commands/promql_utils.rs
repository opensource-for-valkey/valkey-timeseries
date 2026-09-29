use crate::common::replies::ReplyContext;
use crate::common::{Sample, Timestamp};
use crate::labels::Label;
use crate::promql::engine::{ConcreteSeriesQuerier, QueryReader};
use crate::promql::{EvalLabels, QueryValue};
use std::sync::Arc;
use valkey_module::{Context, Status};

/// The query reader that serves one PromQL command, scoped to the shards owning
/// `hash_tags` (empty for an unscoped query). One reader serves every selector
/// in the expression, so the whole expression is evaluated over one shard set.
pub(super) fn get_promql_querier(ctx: &Context, hash_tags: Vec<String>) -> Arc<dyn QueryReader> {
    let querier = ConcreteSeriesQuerier::create_with_hash_tags(ctx, Arc::from(hash_tags));
    Arc::new(querier)
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

fn write_metric_hash(ctx: &ReplyContext, labels: &(impl ReplyLabels + ?Sized)) -> Status {
    ctx.reply_with_map(labels.len());
    labels.for_each_label(|name, value| {
        ctx.reply_with_string(name);
        ctx.reply_with_string(value);
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
    ctx: &ReplyContext,
    metric: &(impl ReplyLabels + ?Sized),
    values: &[Sample],
) -> Status {
    ctx.reply_with_map(2);
    ctx.reply_with_string("metric");
    write_metric_hash(ctx, metric);
    ctx.reply_with_string("value");
    ctx.reply_with_array(values.len());
    for sample in values {
        ctx.reply_with_sample(sample);
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
    ctx: &ReplyContext,
    metric: &(impl ReplyLabels + ?Sized),
    ts: Timestamp,
    value: f64,
) -> Status {
    ctx.reply_with_map(2);
    ctx.reply_with_string("metric");
    write_metric_hash(ctx, metric);
    ctx.reply_with_string("value");
    ctx.reply_with_sample(&Sample::new(ts, value));
    Status::Ok
}

fn reply_with_string_value(ctx: &ReplyContext, timestamp: Timestamp, value: &str) -> Status {
    ctx.reply_with_array(2);
    ctx.reply_with_integer(timestamp);
    ctx.reply_with_simple_string(value);
    Status::Ok
}

pub(super) fn reply_with_query_value(
    ctx: &ReplyContext,
    value: QueryValue,
    eval_ts: Timestamp,
) -> Status {
    ctx.reply_with_map(2);
    ctx.reply_with_string("resultType");
    let value_type = value.value_type().to_string();
    ctx.reply_with_string(&value_type);
    ctx.reply_with_string("result");

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
