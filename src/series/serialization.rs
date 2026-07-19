use crate::common::Sample;
use crate::common::rdb::*;
use crate::labels::MetricName;
use crate::series::chunks::{Chunk, ChunkEncoding, ChunkOps, TimeSeriesChunk};
use crate::series::compaction::CompactionRule;
use crate::series::series_data_type::TIMESERIES_TYPE_ENCODING_VERSION;
use crate::series::{SampleDuplicatePolicy, TimeSeries, TimeseriesId};
use valkey_module::{ValkeyError, ValkeyResult, raw};

pub fn rdb_save_series(series: &TimeSeries, rdb: *mut raw::RedisModuleIO) {
    raw::save_unsigned(rdb, series.id);
    series.labels.to_rdb(rdb);

    rdb_save_duration(rdb, &series.retention);

    rdb_save_optional_rounding(rdb, &series.rounding);
    series.sample_duplicates.rdb_save(rdb);

    // chunk related
    let tmp = series.chunk_encoding.name();
    raw::save_string(rdb, tmp);
    rdb_save_usize(rdb, series.chunk_size_bytes);
    rdb_save_usize(rdb, series.chunks.len());
    for chunk in series.chunks.iter() {
        chunk.save_rdb(rdb);
    }

    // rule related
    let src_id = series.src_series.unwrap_or_default();
    raw::save_unsigned(rdb, src_id);
    rdb_save_usize(rdb, series.rules.len());
    for rule in series.rules.iter() {
        rule.rdb_save(rdb);
    }
}

pub fn rdb_load_series(rdb: *mut raw::RedisModuleIO, enc_ver: i32) -> ValkeyResult<TimeSeries> {
    // Refuse foreign/unknown payload versions before touching the stream.
    // RedisTimeSeries registers the same `TSDB-TYPE` name with an
    // incompatible layout (encver 9 as of RTS 8.6); parsing it here would
    // silently misread data. Migration is via export/re-ingest, not RDB —
    // see COMPATIBILITY.md.
    if enc_ver != TIMESERIES_TYPE_ENCODING_VERSION {
        return Err(ValkeyError::String(format!(
            "TSDB: cannot load TSDB-TYPE RDB payload with encoding version {enc_ver} \
             (this module writes version {TIMESERIES_TYPE_ENCODING_VERSION}). \
             RedisTimeSeries RDB/DUMP payloads are incompatible and cannot be imported; \
             re-ingest via TS.RANGE export -> TS.MADD (see COMPATIBILITY.md)"
        )));
    }
    let id = raw::load_unsigned(rdb)? as TimeseriesId;
    let labels = MetricName::from_rdb(rdb)?;

    let retention = rdb_load_duration(rdb)?;

    let rounding = rdb_load_optional_rounding(rdb)?;
    let sample_duplicates = SampleDuplicatePolicy::rdb_load(rdb)?;

    // chunk related
    let chunk_encoding = ChunkEncoding::try_from(rdb_load_string(rdb)?)?;
    let chunk_size_bytes = rdb_load_usize(rdb)?;
    let chunks_len = rdb_load_len(rdb, MAX_RDB_COLLECTION_LEN)?;
    let mut chunks = Vec::with_capacity(chunks_len);
    let mut total_samples: usize = 0;
    let mut first_timestamp = 0;

    let mut last_sample: Option<Sample> = None;

    for _ in 0..chunks_len {
        let chunk = TimeSeriesChunk::load_rdb(rdb, enc_ver)?;
        total_samples += chunk.len();
        if first_timestamp == 0 {
            first_timestamp = chunk.first_timestamp();
        }
        last_sample = chunk.last_sample();
        chunks.push(chunk);
    }

    // rule related
    let src_id = raw::load_unsigned(rdb)? as TimeseriesId;
    let src_series = if src_id == 0 { None } else { Some(src_id) };

    let rules_len = rdb_load_len(rdb, MAX_RDB_COLLECTION_LEN)?;
    let mut rules = Vec::with_capacity(rules_len);
    for _ in 0..rules_len {
        let rule = CompactionRule::rdb_load(rdb)?;
        rules.push(rule);
    }
    rules.shrink_to_fit();

    let mut ts = TimeSeries {
        id,
        labels,
        retention,
        chunk_encoding,
        sample_duplicates,
        rounding,
        chunk_size_bytes,
        chunks,
        total_samples,
        first_timestamp,
        last_sample,
        _db: None,
        // Runtime-only strict-mode marker (DIV-0023); never serialized, so a
        // reloaded destination reports its true last sample until the next
        // forward bucket close.
        last_forward_close: None,
        src_series,
        rules,
    };

    // Assign `_db` from the IO context and count the series toward the current load window
    // (no-op for db-less payloads like RESTORE / TS._RESTORE strings).
    crate::series::index::persistence::observe_series_rdb_load(rdb, &mut ts);

    Ok(ts)
}
