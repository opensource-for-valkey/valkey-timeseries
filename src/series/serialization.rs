use crate::common::rdb::*;
use crate::common::{Sample, Timestamp};
use crate::labels::MetricName;
use crate::series::chunks::{
    Chunk, ChunkEncoding, ChunkOps, MAX_CHUNK_SIZE, MAX_LOADED_CHUNK_BYTES, MIN_CHUNK_SIZE,
    TimeSeriesChunk, validate_chunk_size,
};
use crate::series::compaction::{CompactionRule, OpenBucketRepair};
use crate::series::index::persistence::is_loading_active;
use crate::series::seal_chunk;
use crate::series::series_data_type::{
    MIN_TIMESERIES_TYPE_ENCODING_VERSION, TIMESERIES_TYPE_ENCODING_VERSION,
    is_supported_encoding_version,
};
use crate::series::{SampleDuplicatePolicy, SeriesLink, TimeSeries, TimeseriesId};
use std::time::Duration;
use valkey_module::{ValkeyError, ValkeyResult, raw};

/// Initial reservation caps for the chunk and rule vectors; past them they grow as elements load.
const MAX_PREALLOCATED_CHUNKS: usize = 1024;
const MAX_PREALLOCATED_RULES: usize = 16;

/// Where a series payload came from, which decides how much of it is checked and what a failed
/// check does. Decided once per series, at the start of [`rdb_load_series`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LoadTrust {
    /// An RDB or AOF this server wrote, or a full sync from its primary: the write path made
    /// it and a whole-file checksum covers it.
    Own,
    /// `RESTORE` (and so `MIGRATE`'s receiving end) and a slot import's `TS._RESTORE`. Any
    /// client allowed to `RESTORE` can forge one: the DUMP footer is a checksum anyone can
    /// recompute, not a validation step.
    Foreign,
}

impl LoadTrust {
    fn name(self) -> &'static str {
        match self {
            LoadTrust::Own => "own",
            LoadTrust::Foreign => "foreign",
        }
    }

    /// The trust of the payload being loaded now: own inside the load window
    /// (`RdbStarted` / `AofStarted` / `ReplStarted` to `Ended` / `Failed`), foreign outside it.
    ///
    /// The window is enough because every load the module sees is synchronous, and a
    /// synchronous load refuses client commands with `-LOADING`. The one asynchronous load, a
    /// `repl-diskless-load swapdb` full sync, needs the module to declare
    /// `HANDLE_REPL_ASYNC_LOAD`, which it never does (`common::module_options`), so the server
    /// loads such a sync synchronously instead.
    ///
    /// Inside the window, a `TS._RESTORE` replayed from the AOF is own data. Outside it, so is
    /// nothing: a `RESTORE`, a slot import's `TS._RESTORE`, and the same commands replicated
    /// from a primary all count as foreign, the last because the primary checked them the same
    /// way.
    fn current() -> Self {
        if is_loading_active() {
            LoadTrust::Own
        } else {
            LoadTrust::Foreign
        }
    }
}

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
    // sample. That runs for a foreign payload, but not while the server loads an RDB or AOF it
    // wrote itself: there the O(1) header checks in the chunk loaders stand alone.
    //
    // Measured `RESTORE` cost with the pass (release build, server-side, honest series with
    // 1 MiB chunks; macOS, 2026-11-12):
    //
    //   Gorilla, constant (2 bits/sample, the densest)  7.5 ns/sample  31.6 ms per MiB
    //   Chimp, constant (4 bits/sample)                 8.5 ns/sample  17.9 ms per MiB
    //   Chimp, random values                           34.9 ns/sample   4.6 ms per MiB
    //   Uncompressed, random (no pass)                 40.6 ns/sample   2.9 ms per MiB
    //
    // So at worst about one full read of the restored key, linear in the bytes the sender had
    // to transmit. There is deliberately no budget: one that fell back to the header checks
    // past some size would be the bypass. A budget, if ever added, must refuse past it.
    let trust = LoadTrust::current();
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
        if trust == LoadTrust::Foreign {
            // The size limit first: it bounds what the decode below costs.
            chunk
                .check_write_limits()
                .and_then(|_| chunk.verify_stream())
                .map_err(|e| {
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

    let repairs = normalize_series(&mut ts, trust)
        .map_err(|e| ValkeyError::String(format!("Invalid series: {e}")))?;

    // Assign `_db` from the IO context and count the series toward the current load window
    // (no-op for db-less payloads like RESTORE / TS._RESTORE strings).
    crate::series::index::persistence::observe_series_rdb_load(rdb, &mut ts);

    if !repairs.is_empty() {
        crate::common::metrics::rdb::note_series_repairs(
            &ts.key,
            trust.name(),
            &repairs.repaired,
            &repairs.tolerated,
        );
    }

    Ok(ts)
}

/// Drops every empty chunk unless it is the series' only chunk, and reports whether any went.
///
/// An empty chunk's bounds read as zero, which breaks the ordering the chunk searches assume
/// once a series has more than 16 chunks (`binary_search_chunks_by_timestamp`): deletes and
/// bounded reads of existing samples found nothing. Commands never leave one anywhere but
/// alone: deletes, defrag and the retention trim all drop the chunks they empty, and
/// `get_last_chunk` creates a chunk on demand. So this changes no series the server wrote, and
/// the digest hashes samples, not chunks. Applied to every load, foreign or not.
pub(crate) fn drop_empty_chunks(ts: &mut TimeSeries) -> bool {
    if ts.chunks.len() < 2 || !ts.chunks.iter().any(|chunk| chunk.is_empty()) {
        return false;
    }
    ts.chunks.retain(|chunk| !chunk.is_empty());
    true
}

/// What [`normalize_series`] changed in a payload, or loaded although it breaks a rule. Each
/// entry names one rule, once per series.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Repairs {
    /// Rules the series broke and was brought back within.
    pub repaired: Vec<&'static str>,
    /// Rules the series still breaks, because no local repair exists. Only an own load
    /// tolerates one; a foreign load fails instead.
    pub tolerated: Vec<&'static str>,
}

impl Repairs {
    pub(crate) fn is_empty(&self) -> bool {
        self.repaired.is_empty() && self.tolerated.is_empty()
    }

    fn repair(&mut self, rule: &'static str) {
        if !self.repaired.contains(&rule) {
            self.repaired.push(rule);
        }
    }

    fn tolerate(&mut self, rule: &'static str) {
        if !self.tolerated.contains(&rule) {
            self.tolerated.push(rule);
        }
    }
}

pub(crate) const BUDGET_CLAMPED: &str = "chunk budget clamped";
pub(crate) const EMPTY_CHUNKS_DROPPED: &str = "empty chunks dropped";
pub(crate) const ENCODING_ADOPTED: &str = "series encoding taken from its chunks";
pub(crate) const STRAYS_REENCODED: &str = "chunks of another encoding re-encoded";
pub(crate) const OVERSIZED_SPLIT: &str = "oversized chunks split";
pub(crate) const CHUNK_SIZE_CLAMPED: &str = "chunk size clamped";
pub(crate) const RETENTION_CLAMPED: &str = "retention clamped";
pub(crate) const NEGATIVE_TIMESTAMPS: &str = "negative timestamps";
pub(crate) const CHUNKS_OUT_OF_ORDER: &str = "chunks out of order";
pub(crate) const UNDECODABLE_STRAYS_KEPT: &str = "undecodable chunks of another encoding kept";
pub(crate) const STALE_RULE_STATE_CLEARED: &str = "compaction state without an open bucket cleared";
pub(crate) const OFF_GRID_BUCKET_REBUILT: &str =
    "compaction bucket off its grid marked for rebuild";

/// Brings an assembled series within what the write path maintains, or refuses it.
///
/// Each chunk loader has already applied the rules no writer can break (the count bound, the
/// free-bit count, codec state, ordered bounds, the uncompressed order), and those fail any
/// load. The rules here mirror the write path instead, so a writer bug can break them: one did,
/// when a `TS.ADDBULK` timestamp past `i64::MAX` was stored negative and the server then
/// refused its own RDB. What a broken rule costs therefore depends on where the payload came
/// from ([`LoadTrust`]):
///
/// | Rule | Foreign | Own |
/// | --- | --- | --- |
/// | A chunk budget over `MAX_CHUNK_SIZE` | refused | clamped |
/// | Empty chunks beside others | dropped | dropped |
/// | The series' encoding differs from its chunks', which agree | adopted | adopted |
/// | Chunks of more than one encoding | refused | strays re-encoded to the series'; one that does not decode fully is kept as it is |
/// | Chunk data over `MAX_LOADED_CHUNK_BYTES` | refused | split, as the merge path splits |
/// | `chunk_size_bytes` outside `validate_chunk_size` | refused | clamped into range |
/// | Retention past `i64::MAX` ms | refused | clamped to `i64::MAX` ms |
/// | Negative timestamps | refused | loaded as they are |
/// | Chunks out of order or overlapping | refused | loaded as they are |
/// | A rule's aggregator state with no open bucket | cleared | cleared |
/// | A rule's open bucket off its grid | marked `dirty`, rebuilt by the next write | marked `dirty`, rebuilt by the next write |
///
/// Repairs are local and keep every sample. A rule with no such repair is tolerated rather
/// than enforced on an own load: refusing it would fail the whole RDB load, a worse outcome
/// than what the rule protects against. Before this branch those series loaded unchecked.
/// Every repair and tolerance is counted and logged (`note_series_repairs`).
///
/// Budgets come first because the split works from them: `needs_split` adds a quarter of a
/// budget to itself, which overflows for a forged `usize::MAX`.
pub(crate) fn normalize_series(ts: &mut TimeSeries, trust: LoadTrust) -> Result<Repairs, String> {
    let own = trust == LoadTrust::Own;
    // Fails a foreign load; lets an own load go on to repair or tolerate.
    let refuse = |err: String| if own { Ok(()) } else { Err(err) };
    let mut repairs = Repairs::default();

    for (i, chunk) in ts.chunks.iter_mut().enumerate() {
        if chunk.max_size() > MAX_CHUNK_SIZE {
            refuse(format!(
                "chunk {i} budget {} out of range",
                chunk.max_size()
            ))?;
            chunk.clamp_max_size(MAX_CHUNK_SIZE);
            repairs.repair(BUDGET_CLAMPED);
        }
    }

    if drop_empty_chunks(ts) {
        repairs.repair(EMPTY_CHUNKS_DROPPED);
    }

    if let Some(encoding) = ts.chunks.first().map(TimeSeriesChunk::get_encoding) {
        if ts
            .chunks
            .iter()
            .all(|chunk| chunk.get_encoding() == encoding)
        {
            // Only the hint new chunks are created from; nothing reads it otherwise.
            if encoding != ts.chunk_encoding {
                ts.chunk_encoding = encoding;
                repairs.repair(ENCODING_ADOPTED);
            }
        } else {
            // A stray encoding was what let an uncompressed chunk follow a compressed one,
            // which a forged count turned into an abort in the reverse `FILTER_BY_TS` path.
            refuse(format!(
                "chunks of more than one encoding in a {} series",
                ts.chunk_encoding
            ))?;
            let (reencoded, kept) = reencode_strays(ts)?;
            if reencoded > 0 {
                repairs.repair(STRAYS_REENCODED);
            }
            if kept > 0 {
                repairs.tolerate(UNDECODABLE_STRAYS_KEPT);
            }
        }
    }

    let oversized = ts
        .chunks
        .iter()
        .enumerate()
        .find(|(_, chunk)| chunk.size() > MAX_LOADED_CHUNK_BYTES)
        .map(|(i, chunk)| (i, chunk.size()));
    if let Some((i, bytes)) = oversized {
        refuse(format!("chunk {i} holds {bytes} bytes"))?;
        // Halves until no chunk is past 1.25x its budget (at most `MAX_CHUNK_SIZE`, clamped
        // above), so every chunk ends under the limit.
        ts.split_chunks_if_needed()
            .map_err(|e| format!("cannot split chunk {i}: {e}"))?;
        repairs.repair(OVERSIZED_SPLIT);
    }

    if validate_chunk_size(ts.chunk_size_bytes).is_err() {
        refuse(format!("chunk size {} out of range", ts.chunk_size_bytes))?;
        // Both ends of the range are multiples of 8, so rounding down stays inside it.
        ts.chunk_size_bytes = ts.chunk_size_bytes.clamp(MIN_CHUNK_SIZE, MAX_CHUNK_SIZE) / 8 * 8;
        repairs.repair(CHUNK_SIZE_CLAMPED);
    }

    if ts.retention.as_millis() > i64::MAX as u128 {
        refuse(format!(
            "retention {} ms out of range",
            ts.retention.as_millis()
        ))?;
        // The read floor subtracts retention from the last timestamp as an `i64`
        // (`get_min_timestamp`), so this keeps every sample. Clamping to the config's
        // ten-year maximum instead would expire older data on reload.
        ts.retention = Duration::from_millis(i64::MAX as u64);
        repairs.repair(RETENTION_CLAMPED);
    }

    let mut prev_last: Option<Timestamp> = None;
    for (i, chunk) in ts.chunks.iter().enumerate() {
        // Only a sole empty chunk is left.
        if chunk.is_empty() {
            continue;
        }
        let first = chunk.first_timestamp();
        let last = chunk.last_timestamp();
        if first > last {
            // The chunk loaders refuse these bounds; this keeps the order checks below sound.
            return Err(format!("chunk {i} spans {first}..{last}"));
        }
        if first < 0 {
            refuse(format!("chunk {i} starts at negative timestamp {first}"))?;
            repairs.tolerate(NEGATIVE_TIMESTAMPS);
        }
        if let Some(prev) = prev_last
            && first <= prev
        {
            refuse(format!(
                "chunk {i} starts at {first}, not after the previous chunk's {prev}"
            ))?;
            repairs.tolerate(CHUNKS_OUT_OF_ORDER);
        }
        prev_last = Some(prev_last.map_or(last, |prev| prev.max(last)));
    }

    // Rule state is repaired on every load: the repair is a no-op on anything the engine wrote,
    // and a refusal would protect nothing the repair does not.
    for rule in &mut ts.rules {
        match rule.repair_open_bucket() {
            Some(OpenBucketRepair::Cleared) => repairs.repair(STALE_RULE_STATE_CLEARED),
            Some(OpenBucketRepair::Rebuild) => repairs.repair(OFF_GRID_BUCKET_REBUILT),
            None => {}
        }
    }

    if !repairs.repaired.is_empty() {
        // Splits and re-encodes keep every sample, but the bookkeeping is cheap to redo and
        // then cannot disagree with the chunks.
        ts.update_first_last_timestamps();
        ts.recalculate_total_samples();
    }
    Ok(repairs)
}

/// Re-encodes every chunk whose encoding is not the series' own, keeping its budget.
/// Re-encodes every chunk not in the series' encoding, returning how many were re-encoded and
/// how many were kept as they are.
///
/// A stray whose stream stops decoding before its count is kept: `iter` ends at a decode
/// error, so re-encoding what it yields would store the readable prefix and lose the rest for
/// good. Kept, it reads as any own-load chunk with a damaged stream does (the decode error is
/// counted), and its bytes survive for recovery.
fn reencode_strays(ts: &mut TimeSeries) -> Result<(usize, usize), String> {
    let encoding = ts.chunk_encoding;
    let (mut reencoded, mut kept) = (0, 0);
    for (i, chunk) in ts.chunks.iter_mut().enumerate() {
        if chunk.get_encoding() == encoding {
            continue;
        }
        let samples: Vec<Sample> = chunk.iter().collect();
        if samples.len() != chunk.len() {
            kept += 1;
            continue;
        }
        let mut rebuilt = TimeSeriesChunk::new(encoding, chunk.max_size());
        rebuilt
            .set_data(&samples)
            .map_err(|e| format!("cannot re-encode chunk {i} as {encoding}: {e}"))?;
        seal_chunk(&mut rebuilt);
        *chunk = rebuilt;
        reencoded += 1;
    }
    Ok((reencoded, kept))
}

/// Whether a series is in the form a foreign load accepts unchanged: what the write path
/// stores, after [`normalize_series`]. For tests: every series a command sequence builds must
/// pass, or a reload would refuse or rewrite it.
#[cfg(test)]
pub(crate) fn check_series_invariants(ts: &TimeSeries) -> Result<(), String> {
    let mut loaded = ts.clone();
    let repairs = normalize_series(&mut loaded, LoadTrust::Foreign)?;
    if repairs.repaired.is_empty() {
        Ok(())
    } else {
        Err(format!("loads only after: {}", repairs.repaired.join(", ")))
    }
}
