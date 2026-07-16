use crate::common::constants::META_KEY_LABEL;
use crate::common::rounding::RoundingStrategy;
use crate::series::index::get_timeseries_index;
use crate::series::{
    SeriesRef, TimeSeries,
    chunks::{ChunkOps, TimeSeriesChunk},
    get_timeseries,
};
use blart::AsBytes;
use smallvec::SmallVec;
use std::collections::HashMap;
use valkey_module::redisvalue::ValkeyValueKey;
use valkey_module::{AclPermissions, Context, NextArg, ValkeyResult, ValkeyString, ValkeyValue};

#[valkey_module_macros::command({
    name: "TS.INFO",
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
    let series = get_timeseries(ctx, &key, Some(AclPermissions::ACCESS), true)?;
    // must_exist was passed above. Therefore, unwrap is safe here
    let series = series.unwrap();
    Ok(get_ts_info(ctx, &series, debugging, None))
}

fn get_ts_info(
    ctx: &Context,
    ts: &TimeSeries,
    debug: bool,
    key: Option<&ValkeyString>,
) -> ValkeyValue {
    let mut map: HashMap<ValkeyValueKey, ValkeyValue> = HashMap::with_capacity(ts.labels.len() + 1);
    let metric = ts.prometheus_metric_name();
    map.insert("metric".into(), metric.into());
    map.insert(
        "totalSamples".into(),
        ValkeyValue::Integer(ts.total_samples as i64),
    );
    map.insert(
        "memoryUsage".into(),
        ValkeyValue::Integer(ts.memory_usage() as i64),
    );
    map.insert(
        "firstTimestamp".into(),
        ValkeyValue::Integer(ts.first_timestamp),
    );
    if let Some(last_sample) = ts.last_sample {
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

    if let Some(key) = key {
        map.insert(
            ValkeyValueKey::String(META_KEY_LABEL.into()),
            ValkeyValue::from(key),
        );
    }

    if ts.labels.is_empty() {
        map.insert("labels".into(), ValkeyValue::Null);
    } else {
        let mut labels = ts.labels.to_label_vec();
        labels.sort();

        let labels_value = labels
            .into_iter()
            .map(|label| label.into())
            .collect::<Vec<ValkeyValue>>();

        map.insert("labels".into(), ValkeyValue::from(labels_value));
    }

    // Always present: nil when the series is not a compaction target
    // (RedisTimeSeries parity), or when the source id cannot be resolved.
    let source_key = ts.src_series.and_then(|src_id| {
        let key = get_key_by_id(ctx, src_id);
        if key.is_none() {
            let msg = format!("Source series with id {src_id} not found");
            ctx.log_warning(&msg);
        }
        key
    });
    map.insert(
        "sourceKey".into(),
        source_key.map_or(ValkeyValue::Null, ValkeyValue::from),
    );
    map.insert(
        ValkeyValueKey::String("rules".to_string()),
        get_rules_info(ctx, ts),
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
        // yes, I know its title case, but that's what redis does
        map.insert("Chunks".into(), get_chunks_info(ts));
    }

    ValkeyValue::Map(map)
}

fn get_chunks_info(ts: &TimeSeries) -> ValkeyValue {
    let items = ts
        .chunks
        .iter()
        .map(get_one_chunk_info)
        .collect::<Vec<ValkeyValue>>();

    ValkeyValue::Array(items)
}

fn get_one_chunk_info(chunk: &TimeSeriesChunk) -> ValkeyValue {
    let mut map: HashMap<ValkeyValueKey, ValkeyValue> = HashMap::with_capacity(6);
    map.insert(
        "startTimestamp".into(),
        ValkeyValue::Integer(chunk.first_timestamp()),
    );
    map.insert(
        "endTimestamp".into(),
        ValkeyValue::Integer(chunk.last_timestamp()),
    );
    map.insert("samples".into(), ValkeyValue::Integer(chunk.len() as i64));
    map.insert("size".into(), ValkeyValue::Integer(chunk.size() as i64));
    map.insert(
        "bytesPerSample".into(),
        ValkeyValue::BulkString(chunk.bytes_per_sample().to_string()),
    );
    ValkeyValue::Map(map)
}

fn get_rules_info(ctx: &Context, series: &TimeSeries) -> ValkeyValue {
    if series.rules.is_empty() {
        return ValkeyValue::Array(vec![]);
    }

    let series_ids: SmallVec<[_; 16]> = series.rules.iter().map(|rule| rule.dest_id).collect();
    let keys_map = get_keys_by_id(ctx, &series_ids);

    let rules_value = series
        .rules
        .iter()
        .flat_map(|x| {
            let Some(dest_key) = keys_map.get(&x.dest_id) else {
                let msg = format!(
                    "Compaction rule has invalid destination id {}. Removing rule.",
                    x.dest_id
                );
                ctx.log_warning(&msg);
                return None;
            };

            Some(ValkeyValue::Array(vec![
                ValkeyValue::BulkString(dest_key.clone()),
                ValkeyValue::Integer(x.bucket_duration as i64),
                ValkeyValue::SimpleString(x.aggregator.aggregation_type().to_string()),
                ValkeyValue::Integer(x.align_timestamp),
            ]))
        })
        .collect::<Vec<_>>();

    ValkeyValue::Array(rules_value)
}

fn get_key_by_id(ctx: &Context, id: SeriesRef) -> Option<String> {
    let mut keys = get_keys_by_id(ctx, &[id]);
    keys.remove(&id)
}

fn get_keys_by_id(ctx: &Context, ids: &[SeriesRef]) -> HashMap<SeriesRef, String> {
    let index = get_timeseries_index(ctx);
    let mut state = ();
    index.with_postings(&mut state, |posting, _| {
        let mut map = HashMap::with_capacity(ids.len());
        for id in ids.iter().cloned() {
            if let Some(key) = posting.get_key_by_id(id) {
                let key_str = String::from_utf8_lossy(key.as_bytes()).to_string();
                map.insert(id, key_str);
            } else {
                let msg = format!("Series with id {id} not found");
                ctx.log_warning(&msg);
            }
        }
        map
    })
}
