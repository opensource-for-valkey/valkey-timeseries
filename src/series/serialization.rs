use crate::common::rdb::*;
use crate::common::{Sample, Timestamp};
use crate::labels::MetricName;
use crate::series::chunks::{
    Chunk, ChunkEncoding, ChunkOps, MAX_CHUNK_SIZE, MAX_LOADED_CHUNK_BYTES, TimeSeriesChunk,
    validate_chunk_size,
};
use crate::series::compaction::CompactionRule;
use crate::series::index::persistence::is_loading_active;
use crate::series::series_data_type::{
    MIN_TIMESERIES_TYPE_ENCODING_VERSION, TIMESERIES_TYPE_ENCODING_VERSION,
    is_supported_encoding_version,
};
use crate::series::{SampleDuplicatePolicy, SeriesLink, TimeSeries, TimeseriesId};
use valkey_module::{ValkeyError, ValkeyResult, raw};

/// Initial reservation caps for the chunk and rule vectors; past them they grow as elements load.
const MAX_PREALLOCATED_CHUNKS: usize = 1024;
const MAX_PREALLOCATED_RULES: usize = 16;

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
    SeriesLink::rdb_save_optional(series.src_series.as_ref(), rdb);
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
    if !is_supported_encoding_version(enc_ver) {
        let readable = if MIN_TIMESERIES_TYPE_ENCODING_VERSION == TIMESERIES_TYPE_ENCODING_VERSION {
            format!("version {TIMESERIES_TYPE_ENCODING_VERSION}")
        } else {
            format!(
                "versions {MIN_TIMESERIES_TYPE_ENCODING_VERSION} to \
                 {TIMESERIES_TYPE_ENCODING_VERSION}"
            )
        };
        return Err(ValkeyError::String(format!(
            "TSDB: cannot load TSDB-TYPE RDB payload with encoding version {enc_ver} \
             (this module reads {readable}). \
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
    // A compressed chunk's header (count, bounds, last value, encoder tail state) is only
    // checked against its stream by decoding it, which costs a decode and an encode of every
    // sample. That runs for `RESTORE` and a slot import's `TS._RESTORE`, which any client with
    // `RESTORE` can reach with a forged DUMP footer, but not while the server loads an RDB or
    // AOF it wrote itself: there the O(1) header checks in the chunk loaders stand alone.
    let verify_streams = !is_loading_active();
    // Reserve from the declared counts only up to a cap: each element is a read that fails on a
    // short payload, but an uncapped reservation runs first, and a 165-byte payload declaring
    // 16M chunks asked for ~1.8 GB (an abort wherever the allocator refuses it).
    let mut chunks = Vec::with_capacity(chunks_len.min(MAX_PREALLOCATED_CHUNKS));
    let mut total_samples: usize = 0;
    // `None` until the first non-empty chunk, not `0`: 0 is a valid timestamp, and using it
    // as the "unset" marker took chunk 1's first timestamp for a series whose data starts at
    // ts 0 — with no retention that value is the read floor, so everything in chunk 0 went
    // missing from ranges after a reload, RESTORE or slot migration.
    let mut first_timestamp: Option<Timestamp> = None;

    let mut last_sample: Option<Sample> = None;

    for _ in 0..chunks_len {
        let chunk = TimeSeriesChunk::load_rdb(rdb, enc_ver)?;
        if verify_streams {
            chunk.verify_stream().map_err(|e| {
                ValkeyError::String(format!(
                    "Invalid {} chunk: {e}",
                    chunk.get_encoding().name()
                ))
            })?;
        }
        total_samples = total_samples
            .checked_add(chunk.len())
            .ok_or(ValkeyError::Str("Invalid series: sample count overflows"))?;
        if first_timestamp.is_none() && !chunk.is_empty() {
            first_timestamp = Some(chunk.first_timestamp());
        }
        // An empty chunk has no last sample; it must not erase an earlier chunk's.
        if let Some(sample) = chunk.last_sample() {
            last_sample = Some(sample);
        }
        chunks.push(chunk);
    }
    let first_timestamp = first_timestamp.unwrap_or_default();

    // rule related
    let src_series = SeriesLink::rdb_load_optional(rdb)?;

    let rules_len = rdb_load_len(rdb, MAX_RDB_COLLECTION_LEN)?;
    let mut rules = Vec::with_capacity(rules_len.min(MAX_PREALLOCATED_RULES));
    for _ in 0..rules_len {
        let rule = CompactionRule::rdb_load(rdb)?;
        rules.push(rule);
    }
    rules.shrink_to_fit();

    let mut ts = TimeSeries {
        id,
        // Assigned from the IO context below, or by the caller for a payload without one.
        key: Box::default(),
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
        src_series,
        rules,
    };

    check_series_invariants(&ts)
        .map_err(|e| ValkeyError::String(format!("Invalid series: {e}")))?;

    // Assign `_db` from the IO context and count the series toward the current load window
    // (no-op for db-less payloads like RESTORE / TS._RESTORE strings).
    crate::series::index::persistence::observe_series_rdb_load(rdb, &mut ts);

    Ok(ts)
}

/// What the write path maintains for every series it stores, checked once a payload has been
/// assembled. Each chunk loader checks its own encoding; this covers what only the series sees.
///
/// Every rule is something no command can violate, never a tighter limit: a series load error
/// fails the whole RDB load (or `RESTORE`), so a rule stricter than the write path makes a
/// server that saved fine unable to restart.
///
/// - `chunk_size_bytes` passes `validate_chunk_size`, as `TS.CREATE`, `TS.ALTER` and the config do.
/// - Retention fits the `i64` milliseconds the commands parse.
/// - Every chunk has the series' encoding: every chunk is created from it, and `TS.ALTER`
///   cannot change it (it rejects `ENCODING`; RTS accepts the option and ignores it). A stray
///   encoding was what let an uncompressed chunk follow a compressed one, which a forged count
///   turned into an abort in the `FILTER_BY_TS` reverse path. A command that ever converts a
///   series in place must re-encode every chunk, or relax this rule.
/// - Every chunk's own budget is at most `MAX_CHUNK_SIZE` and its data at most
///   `MAX_LOADED_CHUNK_BYTES`. A chunk's `max_size` may still differ from the series' after
///   `TS.ALTER CHUNK_SIZE`, so it is not compared with `chunk_size_bytes`.
/// - Non-empty chunks hold non-negative timestamps, ascending with no overlap from one chunk to
///   the next. Empty chunks are allowed anywhere: a series keeps an empty tail chunk, and they
///   are harmless elsewhere.
pub(crate) fn check_series_invariants(ts: &TimeSeries) -> Result<(), String> {
    if validate_chunk_size(ts.chunk_size_bytes).is_err() {
        return Err(format!("chunk size {} out of range", ts.chunk_size_bytes));
    }
    if ts.retention.as_millis() > i64::MAX as u128 {
        return Err(format!(
            "retention {} ms out of range",
            ts.retention.as_millis()
        ));
    }
    let mut prev_last: Option<Timestamp> = None;
    for (i, chunk) in ts.chunks.iter().enumerate() {
        let encoding = chunk.get_encoding();
        if encoding != ts.chunk_encoding {
            return Err(format!(
                "chunk {i} is {encoding} in a {} series",
                ts.chunk_encoding
            ));
        }
        if chunk.max_size() > MAX_CHUNK_SIZE {
            return Err(format!(
                "chunk {i} budget {} out of range",
                chunk.max_size()
            ));
        }
        if chunk.size() > MAX_LOADED_CHUNK_BYTES {
            return Err(format!("chunk {i} holds {} bytes", chunk.size()));
        }
        if chunk.is_empty() {
            continue;
        }
        let first = chunk.first_timestamp();
        let last = chunk.last_timestamp();
        if first < 0 || first > last {
            return Err(format!("chunk {i} spans {first}..{last}"));
        }
        if let Some(prev) = prev_last
            && first <= prev
        {
            return Err(format!(
                "chunk {i} starts at {first}, not after the previous chunk's {prev}"
            ));
        }
        prev_last = Some(last);
    }
    Ok(())
}
