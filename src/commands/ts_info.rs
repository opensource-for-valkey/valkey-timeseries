use crate::common::replies::ReplyContext;
use crate::common::replies::is_resp3_client;
use crate::common::rounding::RoundingStrategy;
use crate::series::{TimeSeries, chunks::ChunkOps, get_timeseries, try_get_timeseries};
use std::collections::HashMap;
use valkey_module::redisvalue::ValkeyValueKey;
use valkey_module::{AclPermissions, Context, NextArg, ValkeyResult, ValkeyString, ValkeyValue};

acl_categories!(TS_INFO, "ts.info", "read fast timeseries");
#[valkey_module_macros::command({
    name: "ts.info",
    flags: [ReadOnly],
    summary: "Return information and statistics for a time series.",
    complexity: "O(1)",
    since: "1.0.0",
    arity: -2,
    key_spec: [{
        flags: [ReadOnly, Access],
        begin_search: Index({ index: 1 }),
        find_keys: Range({ last_key: 0, steps: 1, limit: 0 })
    }]
})]
pub fn ts_info_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1);
    let key = args.next_arg()?;

    let debugging = if let Ok(val) = args.next_str() {
        val.eq_ignore_ascii_case("debug")
    } else {
        false
    };

    args.done()?;
    let series = get_timeseries(ctx, &key, Some(AclPermissions::ACCESS))?;
    // The key is what TS.INFO DEBUG reports as `keySelfName`.
    let ValkeyValue::Map(mut fields) = get_ts_info(ctx, &series, debugging, &key) else {
        unreachable!("get_ts_info builds a map");
    };

    // Written field by field, in a fixed order. Returned as a `ValkeyValue::Map` — a
    // `HashMap` — the fields came back in a different order on every call.
    let reply = ReplyContext::new(ctx.ctx);
    reply.reply_with_map(fields.len());
    for name in INFO_FIELD_ORDER {
        let key = ValkeyValueKey::String(name.to_string());
        let Some(value) = fields.remove(&key) else {
            continue;
        };
        reply.reply_with_string(name);
        if *name == "Chunks" {
            reply_with_chunks_info(&reply, &series);
        } else {
            reply.reply(Ok(value));
        }
    }
    // Anything not in the list still has to go out: the map length above counted it.
    debug_assert!(
        fields.is_empty(),
        "TS.INFO field missing from INFO_FIELD_ORDER"
    );
    for (key, value) in fields {
        match key {
            ValkeyValueKey::String(name) => reply.reply_with_string(&name),
            other => reply.reply_with_string(&format!("{other:?}")),
        };
        reply.reply(Ok(value));
    }
    Ok(ValkeyValue::NoReply)
}

/// The order TS.INFO reports its fields in: the reference's, with this module's additions
/// (`encoding`, `metric`, `rounding`) beside their nearest relatives. DEBUG adds the last two.
const INFO_FIELD_ORDER: &[&str] = &[
    "totalSamples",
    "memoryUsage",
    "firstTimestamp",
    "lastTimestamp",
    "retentionTime",
    "chunkCount",
    "chunkSize",
    "chunkType",
    "encoding",
    "duplicatePolicy",
    "labels",
    "metric",
    "sourceKey",
    "rules",
    "ignoreMaxTimeDiff",
    "ignoreMaxValDiff",
    "rounding",
    "keySelfName",
    "Chunks",
];

/// `Chunks` for TS.INFO DEBUG: one map per chunk, its fields in a fixed order.
fn reply_with_chunks_info(reply: &ReplyContext, ts: &TimeSeries) {
    reply.reply_with_array(ts.chunks.len());
    for chunk in &ts.chunks {
        reply.reply_with_map(5);
        reply.reply_with_string("startTimestamp");
        reply.reply_with_integer(chunk.first_timestamp());
        reply.reply_with_string("endTimestamp");
        reply.reply_with_integer(chunk.last_timestamp());
        reply.reply_with_string("samples");
        reply.reply_with_integer(chunk.len() as i64);
        reply.reply_with_string("size");
        reply.reply_with_integer(chunk.size() as i64);
        // RTS replies bytesPerSample via ReplyWithDouble: native double on RESP3,
        // bulk string on RESP2 (compat finding #12).
        reply.reply_with_string("bytesPerSample");
        reply.reply(Ok(ValkeyValue::Float(chunk.bytes_per_sample() as f64)));
    }
}

fn get_ts_info(ctx: &Context, ts: &TimeSeries, debug: bool, key: &ValkeyString) -> ValkeyValue {
    // RESP3 clients receive `labels` and `rules` as native maps; RESP2 clients
    // receive the array-of-pairs / array-of-arrays forms. Everything else is
    // protocol-agnostic.
    let is_resp3 = is_resp3_client(ctx);
    let mut map: HashMap<ValkeyValueKey, ValkeyValue> = HashMap::with_capacity(ts.labels.len() + 1);
    let metric = ts.prometheus_metric_name();
    map.insert("metric".into(), metric.into());
    map.insert(
        "totalSamples".into(),
        ValkeyValue::Integer(ts.visible_total_samples() as i64),
    );
    map.insert(
        "memoryUsage".into(),
        ValkeyValue::Integer(ts.memory_usage() as i64),
    );
    map.insert(
        "firstTimestamp".into(),
        ValkeyValue::Integer(ts.visible_first_timestamp()),
    );
    // `reported_last_sample`, not `last_sample`: under `ts-compatibility-mode strict` a
    // compaction destination reports the last bucket closed by forward progress, and
    // TS.INFO must agree with the TS.GET/TS.MGET it gates (DIV-0023). Extended mode is
    // unaffected — there the two are the same sample.
    if let Some(last_sample) = ts.reported_last_sample() {
        map.insert(
            "lastTimestamp".into(),
            ValkeyValue::Integer(last_sample.timestamp),
        );
    } else {
        map.insert(
            "lastTimestamp".into(),
            ValkeyValue::Integer(ts.first_timestamp),
        );
    }
    map.insert(
        "retentionTime".into(),
        ValkeyValue::Integer(ts.retention.as_millis() as i64),
    );
    map.insert(
        "chunkCount".into(),
        ValkeyValue::Integer(ts.chunks.len() as i64),
    );
    map.insert(
        "chunkSize".into(),
        ValkeyValue::Integer(ts.chunk_size_bytes as i64),
    );

    if ts.chunk_encoding.is_compressed() {
        map.insert("chunkType".into(), "compressed".into());
    } else {
        map.insert("chunkType".into(), "uncompressed".into());
    }

    // add encoding
    map.insert("encoding".into(), ts.chunk_encoding.name().into());

    if let Some(policy) = ts.sample_duplicates.policy {
        map.insert("duplicatePolicy".into(), policy.as_str().into());
    } else {
        map.insert("duplicatePolicy".into(), ValkeyValue::Null);
    }

    map.insert("labels".into(), get_labels_info(ts, is_resp3));

    // Always present: nil when the series is not a compaction target
    // (RedisTimeSeries parity), or when its source no longer feeds it.
    let source_key = get_source_key(ctx, ts);
    map.insert(
        "sourceKey".into(),
        source_key.map_or(ValkeyValue::Null, ValkeyValue::from),
    );
    map.insert(
        ValkeyValueKey::String("rules".to_string()),
        get_rules_info(ctx, ts, is_resp3),
    );

    map.insert(
        "ignoreMaxTimeDiff".into(),
        ValkeyValue::Integer(ts.sample_duplicates.max_time_delta as i64),
    );
    map.insert(
        "ignoreMaxValDiff".into(),
        ValkeyValue::Float(ts.sample_duplicates.max_value_delta),
    );

    if let Some(rounding) = ts.rounding {
        let (name, digits) = match rounding {
            RoundingStrategy::SignificantDigits(d) => ("significantDigits", d),
            RoundingStrategy::DecimalDigits(d) => ("decimalDigits", d),
        };
        let result = ValkeyValue::Array(vec![
            ValkeyValue::from(name),
            ValkeyValue::Integer(digits.into()), // do we have negative digits?
        ]);
        map.insert("rounding".into(), result);
    }

    if debug {
        map.insert("keySelfName".into(), ValkeyValue::from(key));
        // yes, I know its title case, but that's what redis does. Written by
        // `reply_with_chunks_info`; only the key is needed here, for its place in the reply.
        map.insert("Chunks".into(), ValkeyValue::Null);
    }

    ValkeyValue::Map(map)
}

/// Series labels for TS.INFO.
///
/// RESP3: a map of `name -> value`. RESP2: an array of `[name, value]` pairs.
/// A label-less series yields an empty map / empty array respectively. Both
/// forms are empty (not nil) — see [`From<Label>`] for the RESP2 pair encoding.
fn get_labels_info(ts: &TimeSeries, is_resp3: bool) -> ValkeyValue {
    let mut labels = ts.labels.to_label_vec();
    labels.sort();

    if is_resp3 {
        let map: HashMap<ValkeyValueKey, ValkeyValue> = labels
            .into_iter()
            .map(|label| {
                let value = if label.value.is_empty() {
                    ValkeyValue::Null
                } else {
                    ValkeyValue::from(label.value)
                };
                (ValkeyValueKey::String(label.name), value)
            })
            .collect();
        return ValkeyValue::Map(map);
    }

    let labels_value = labels
        .into_iter()
        .map(|label| label.into())
        .collect::<Vec<ValkeyValue>>();
    ValkeyValue::from(labels_value)
}

/// Aggregator name as reported inside a TS.INFO `rules` entry: uppercase
/// (`AVG`, `STD.P`, …), matching RedisTimeSeries. Note this is TS.INFO-specific;
/// the aggregator/reducer names in TS.MRANGE metadata are lowercase and are
/// produced elsewhere.
fn rule_aggregator_name(rule: &crate::series::CompactionRule) -> String {
    rule.aggregator
        .aggregation_type()
        .to_string()
        .to_uppercase()
}

/// Compaction rules for TS.INFO.
///
/// RESP3: a map of `destKey -> [bucketDuration, aggregator, alignTimestamp]`.
/// RESP2: an array of `[destKey, bucketDuration, aggregator, alignTimestamp]`.
/// A rule whose destination no longer exists, or no longer names this series as
/// its source, is dropped from the reply (and logged), in both protocols.
fn get_rules_info(ctx: &Context, series: &TimeSeries, is_resp3: bool) -> ValkeyValue {
    let resolved = series
        .rules
        .iter()
        .filter_map(|rule| {
            let dest_key = rule.dest.to_key_string(ctx);
            let links_back = matches!(
                try_get_timeseries(ctx, &dest_key, None),
                Ok(Some(dest)) if dest
                    .src_series
                    .as_ref()
                    .is_some_and(|src| src.points_to(&series.key))
            );
            if !links_back {
                ctx.log_warning("Compaction rule has an invalid destination; omitting it");
                return None;
            }
            Some((dest_key.to_string_lossy(), rule))
        })
        .collect::<Vec<_>>();

    if is_resp3 {
        let rules_map: HashMap<ValkeyValueKey, ValkeyValue> = resolved
            .into_iter()
            .map(|(dest_key, x)| {
                (
                    ValkeyValueKey::String(dest_key.clone()),
                    ValkeyValue::Array(vec![
                        ValkeyValue::Integer(x.bucket_duration as i64),
                        ValkeyValue::SimpleString(rule_aggregator_name(x)),
                        ValkeyValue::Integer(x.align_timestamp),
                    ]),
                )
            })
            .collect();
        return ValkeyValue::Map(rules_map);
    }

    let rules_value = resolved
        .into_iter()
        .map(|(dest_key, x)| {
            ValkeyValue::Array(vec![
                ValkeyValue::BulkString(dest_key.clone()),
                ValkeyValue::Integer(x.bucket_duration as i64),
                ValkeyValue::SimpleString(rule_aggregator_name(x)),
                ValkeyValue::Integer(x.align_timestamp),
            ])
        })
        .collect::<Vec<_>>();
    ValkeyValue::Array(rules_value)
}

/// The key of the series that feeds `series`, if it still has a rule for it.
fn get_source_key(ctx: &Context, series: &TimeSeries) -> Option<String> {
    let source_key = series.src_series.as_ref()?.to_key_string(ctx);
    let feeds_series = matches!(
        try_get_timeseries(ctx, &source_key, None),
        Ok(Some(source)) if source
            .rules
            .iter()
            .any(|rule| rule.dest.points_to(&series.key))
    );
    if !feeds_series {
        ctx.log_warning("Compaction source series not found");
        return None;
    }
    Some(source_key.to_string_lossy())
}
