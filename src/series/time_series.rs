use super::chunks::utils::{filter_samples_by_value, filter_timestamp_slice};
use super::{SampleAddResult, SampleDuplicatePolicy, TimeSeriesOptions, ValueFilter};
use crate::common::hash::IntMap;
use crate::common::rounding::RoundingStrategy;
use crate::common::time::current_time_millis;
use crate::common::{Sample, Timestamp};
use crate::config::DEFAULT_CHUNK_SIZE_BYTES;
use crate::error::{TsdbError, TsdbResult};
use crate::labels::{InternedLabel, MetricName};
use crate::series::DuplicatePolicy;
use crate::series::chunks::{Chunk, ChunkEncoding, ChunkOps, TimeSeriesChunk, validate_chunk_size};
use crate::series::compaction::CompactionRule;
use crate::series::digest::{
    calc_compaction_digest, calc_duplicate_policy_digest, calc_metric_name_digest,
    calc_rounding_digest,
};
use crate::series::index::next_timeseries_id;
use crate::series::sample_merge::merge_samples;
use crate::series::series_sample_iterator::SeriesSampleIterator;
use crate::{config, error_consts};
use get_size2::GetSize;
use orx_parallel::ParIterResult;
use orx_parallel::{IntoParIter, ParIter, Parallelizable, ParallelizableCollectionMut};
use smallvec::SmallVec;
use std::hash::Hash;
use std::mem::size_of;
use std::sync::atomic::AtomicBool;
use std::time::Duration;
use std::vec;
use valkey_module::digest::Digest;
use valkey_module::logging;
use valkey_module::{ValkeyError, ValkeyResult};

pub type TimeseriesId = u64;
pub type SeriesRef = u64;

/// Represents a time series consisting of chunks of samples, each with a timestamp and value.
#[derive(Clone, Debug, Hash, PartialEq, GetSize)]
pub struct TimeSeries {
    /// fixed, opaque internal id used in indexing
    pub id: SeriesRef,
    /// The label/value pairs
    pub labels: MetricName,
    /// Duration for which data is retained before automatic removal
    pub retention: Duration,
    /// Policy for handling duplicate samples
    pub sample_duplicates: SampleDuplicatePolicy,
    /// The chunk encoding algorithm used (Uncompressed, Gorilla, or chimp)
    pub chunk_encoding: ChunkEncoding,
    /// Optional strategy for rounding values (either by significant or decimal digits)
    pub rounding: Option<RoundingStrategy>,
    /// Target size for chunks in bytes
    pub chunk_size_bytes: usize,
    /// The time series chunks
    pub chunks: Vec<TimeSeriesChunk>,
    // meta
    /// Total number of samples in the time series
    pub total_samples: usize,
    /// The first timestamp in the time series
    pub first_timestamp: Timestamp,
    /// The last timestamp in the time series
    pub last_sample: Option<Sample>,
    pub src_series: Option<TimeseriesId>,
    pub rules: Vec<CompactionRule>,
    /// Internal bookkeeping for current db. Simplifies event handling related to indexing.
    /// This is not part of the time series data itself, nor is it stored to rdb.
    /// `None` means the series is currently unassigned to a local db.
    pub(crate) _db: Option<i32>,
    /// Strict-mode emulation state for DIV-0023, on a compaction *destination*: the
    /// sample published by the most recent bucket close driven by forward progress.
    ///
    /// Back-filling a bucket older than the open one materializes a downstream sample
    /// without refreshing this, which is exactly what RedisTimeSeries reports from
    /// `TS.GET`/`TS.MGET` — a cached destination last-sample that back-fill does not
    /// advance, so its reply disagrees with its own `TS.RANGE`. Only read when
    /// `ts-compatibility-mode` is `strict`; `extended` always reports the true last
    /// sample.
    ///
    /// Like `_db`, this is runtime-only and never stored to rdb: after a reload it is
    /// `None` and the gate falls back to the true last sample (documented in
    /// tests/compat/divergences.yml, DIV-0023).
    pub(crate) last_forward_close: Option<Sample>,
}

impl TimeSeries {
    /// Create a new empty time series.
    pub fn new() -> Self {
        TimeSeries::default()
    }

    pub fn with_options(options: TimeSeriesOptions) -> TsdbResult<Self> {
        let mut res = Self::new();
        if let Some(chunk_size) = options.chunk_size {
            validate_chunk_size(chunk_size)?;
            res.chunk_size_bytes = chunk_size;
        }

        res.chunk_encoding = options.chunk_encoding;
        res.retention = options.retention.unwrap_or_else(config::retention_period);
        res.rounding = options.rounding;
        res.sample_duplicates = options.sample_duplicate_policy.unwrap_or_default();

        // if !options.labels.iter().any(|x| x.name == METRIC_NAME_LABEL) {
        //     return Err(TsdbError::InvalidMetric(
        //         "ERR missing metric name".to_string(),
        //     ));
        // }

        res.labels = if let Some(labels) = options.labels {
            MetricName::new(&labels)
        } else {
            MetricName::default()
        };
        res.src_series = options.src_id;
        res.id = next_timeseries_id();

        Ok(res)
    }

    /// Utility to create a time series from an existing chunk. Useful for dealing with
    /// deserialized data from clustered commands.
    pub(crate) fn from_chunk(chunk: TimeSeriesChunk) -> TsdbResult<Self> {
        let mut ts = TimeSeries::with_options(TimeSeriesOptions {
            chunk_size: Some(chunk.max_size()),
            chunk_encoding: chunk.get_encoding(),
            ..Default::default()
        })?;
        ts.total_samples = chunk.len();
        ts.chunks.push(chunk);
        ts.update_first_last_timestamps();
        Ok(ts)
    }

    #[cfg(test)]
    pub(crate) fn from_chunks(chunks: Vec<TimeSeriesChunk>) -> TsdbResult<Self> {
        let mut ts = TimeSeries::with_options(TimeSeriesOptions {
            ..Default::default()
        })?;
        ts.total_samples = chunks.iter().map(|c| c.len()).sum();
        ts.chunks = chunks;
        ts.update_first_last_timestamps();
        Ok(ts)
    }

    pub fn len(&self) -> usize {
        self.total_samples
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_compressed(&self) -> bool {
        self.chunk_encoding != ChunkEncoding::Uncompressed
    }

    /// Get the full metric name of the time series, including labels in Prometheus format.
    /// For example,
    ///
    /// `http_requests_total{method="POST", status="500"}`
    ///
    /// Note that for our internal purposes, we store the metric name and labels separately and
    /// assume that the labels are sorted by name.
    pub fn prometheus_metric_name(&self) -> String {
        self.labels.to_string()
    }

    pub fn label_value(&self, name: &str) -> Option<&str> {
        self.labels.get_value(name)
    }

    pub fn get_label(&self, name: &str) -> Option<InternedLabel<'_>> {
        self.labels.get_tag(name)
    }

    #[inline]
    pub(crate) fn adjust_value(&self, value: f64) -> f64 {
        self.rounding.as_ref().map_or(value, |r| r.round(value))
    }

    #[inline]
    fn make_sample(&self, ts: Timestamp, value: f64) -> Sample {
        Sample {
            value: self.adjust_value(value),
            timestamp: ts,
        }
    }

    #[inline]
    fn record_appended_sample(&mut self, sample: Sample) {
        if self.total_samples == 0 {
            self.first_timestamp = sample.timestamp;
        }
        self.last_sample = Some(sample);
        self.total_samples += 1;
    }

    pub fn add(
        &mut self,
        ts: Timestamp,
        value: f64,
        dp_override: Option<DuplicatePolicy>,
    ) -> SampleAddResult {
        let result = self.add_deferring_retention(ts, value, dp_override);
        if result.is_ok() {
            self.apply_retention();
        }
        result
    }

    /// [`Self::add`] without the eager retention trim.
    ///
    /// Batch writers use this so that compaction can observe the series as it was
    /// *before* this batch's trim — see [`Self::apply_retention`].
    pub(super) fn add_deferring_retention(
        &mut self,
        ts: Timestamp,
        value: f64,
        dp_override: Option<DuplicatePolicy>,
    ) -> SampleAddResult {
        let sample = self.make_sample(ts, value);

        // Retention is decided before the duplicate policy, matching RedisTimeSeries: an item
        // below the window is `TooOld` even when a sample already sits at its timestamp.
        //
        // `upsert_sample` also tests this, but only on the `ts <= first_timestamp` branch —
        // a proxy that holds solely once the trim has run. Callers that defer the trim (the
        // TS.MADD fallback for in-batch duplicates) still hold the pre-trim samples, so
        // `first_timestamp` is stale, the branch is skipped, and the stale sample answers as a
        // DUPLICATE_POLICY violation instead. `is_older_than_retention` reads the floor from
        // the last timestamp rather than the first, so it is correct either way. For an
        // append (`ts > last_ts`) it can never fire.
        if self.is_older_than_retention(ts) {
            return SampleAddResult::TooOld;
        }

        if let Some(last) = self.last_sample {
            let last_ts = last.timestamp;

            // - ts < last_ts: upsert (no validation)
            // - ts >= last_ts: validate duplicates; if ignored -> return Ignored(last_ts)
            // - ts == last_ts: if not ignored, still go to upsert
            if ts >= last_ts && !self.validate_sample(&sample, &last, dp_override).is_ok() {
                return SampleAddResult::Ignored(last_ts);
            }
            if ts <= last_ts {
                self.upsert_sample(sample, dp_override)
            } else {
                self.add_sample_internal(sample)
            }
        } else {
            self.add_sample_internal(sample)
        }
    }

    /// Apply the retention window now.
    ///
    /// Retention is applied eagerly, matching RedisTimeSeries: a new max sample
    /// advances the window, so samples that just fell outside it are dropped now
    /// rather than waiting for the background trim task. This keeps
    /// total_samples / first_timestamp (TS.INFO) consistent with what a range
    /// query returns. `trim()` is a cheap no-op when nothing expired.
    ///
    /// Callers that write a batch and then compact must defer this until after
    /// compaction: RedisTimeSeries folds a sample into its downstream bucket at
    /// write time, so trimming the source first would drop that contribution from
    /// a bucket recalculation and diverge (see `sample_merge`).
    pub(super) fn apply_retention(&mut self) {
        if self.retention.is_zero() {
            return;
        }
        if let Err(e) = self.trim() {
            logging::log_warning(format!("TSDB: Error trimming time series: {e:?}"));
        }
    }

    pub(crate) fn validate_sample(
        &self,
        sample: &Sample,
        last_sample: &Sample,
        on_duplicate: Option<DuplicatePolicy>,
    ) -> SampleAddResult {
        if self
            .sample_duplicates
            .is_duplicate(sample, last_sample, on_duplicate)
        {
            SampleAddResult::Ignored(last_sample.timestamp)
        } else {
            SampleAddResult::Ok(*sample)
        }
    }

    pub(super) fn add_sample_internal(&mut self, sample: Sample) -> SampleAddResult {
        let chunk = self.get_last_chunk();
        if chunk.is_full() {
            return self.handle_full_chunk(sample);
        }
        match chunk.add_sample(&sample) {
            Ok(_) => {
                self.record_appended_sample(sample);
                SampleAddResult::Ok(sample)
            }
            Err(TsdbError::CapacityFull(_)) => self.handle_full_chunk(sample),
            Err(_) => SampleAddResult::Error(error_consts::CANNOT_ADD_SAMPLE),
        }
    }

    /// (Possibly) add a new chunk and append the given sample.
    fn add_chunk_with_sample(&mut self, sample: Sample) -> TsdbResult<()> {
        let mut chunk = self.create_chunk();
        chunk.add_sample(&sample)?;
        self.chunks.push(chunk);
        // trim chunks to keep memory usage in check
        self.chunks.shrink_to_fit();
        self.record_appended_sample(sample);

        Ok(())
    }

    pub(super) fn append_chunk(&mut self) {
        let new_chunk = self.create_chunk();
        self.chunks.push(new_chunk);
    }

    pub(super) fn create_chunk(&mut self) -> TimeSeriesChunk {
        TimeSeriesChunk::new(self.chunk_encoding, self.chunk_size_bytes)
    }

    fn handle_full_chunk(&mut self, sample: Sample) -> SampleAddResult {
        match self.add_chunk_with_sample(sample) {
            Ok(_) => SampleAddResult::Ok(sample),
            Err(TsdbError::DuplicateSample(_)) => SampleAddResult::Duplicate,
            Err(_) => SampleAddResult::Error(error_consts::CANNOT_ADD_SAMPLE),
        }
    }

    #[inline]
    fn get_last_chunk(&mut self) -> &mut TimeSeriesChunk {
        if self.chunks.is_empty() {
            self.append_chunk();
        }
        self.chunks.last_mut().unwrap()
    }

    fn upsert_sample(
        &mut self,
        sample: Sample,
        duplicate_policy_override: Option<DuplicatePolicy>,
    ) -> SampleAddResult {
        let duplicate_policy = self
            .sample_duplicates
            .resolve_policy(duplicate_policy_override);

        let chunks_len = self.chunks.len();
        debug_assert!(chunks_len > 0, "upsert called on empty series");

        // determine the target chunk index and whether it's the last chunk
        let target_idx = if sample.timestamp <= self.first_timestamp {
            if self.is_older_than_retention(sample.timestamp) {
                return SampleAddResult::TooOld;
            }
            0
        } else {
            let (pos, found) = get_chunk_index(&self.chunks, sample.timestamp);
            if found {
                pos
            } else {
                // The timestamp falls in a *gap* between chunks — chunks need not be
                // contiguous (a split leaves one). `get_chunk_index` reports a miss as
                // `chunks.len()`, which is not a usable index, so resolve it here: route the
                // sample into the chunk starting after the gap, the same rule the bulk path
                // uses (`group_samples_by_chunk`), which keeps chunks non-overlapping.
                // `upsert_sample` is only reached for `timestamp <= last_timestamp`, so a
                // chunk starting after the gap always exists; the last chunk is a defensive
                // fallback.
                self.chunks
                    .iter()
                    .position(|c| c.first_timestamp() > sample.timestamp)
                    .unwrap_or(chunks_len - 1)
            }
        };

        let is_last = target_idx + 1 == chunks_len;

        // obtain mutable reference to the chunk we will upsert into
        let chunk = match self.chunks.get_mut(target_idx) {
            Some(c) => c,
            None => {
                return SampleAddResult::Error("Internal error: no chunks available in upsert");
            }
        };

        // if chunk can accept the sample without splitting, do it directly
        if !chunk.should_split() {
            let old_size = chunk.len();
            let (size, res) = chunk.upsert(sample, duplicate_policy);
            if res.is_ok() {
                self.total_samples += size.saturating_sub(old_size);
                if is_last {
                    self.update_last_sample();
                }
                self.first_timestamp = sample.timestamp.min(self.first_timestamp);
            }
            return res;
        }

        // Otherwise split the chunk and upsert into whichever half now owns the sample's
        // timestamp. `split()` keeps the lower half in `chunk` and hands back the upper half,
        // so upserting blindly into the new chunk would miss an existing sample left behind in
        // the lower half: the duplicate policy would not fire and the timestamp would end up
        // stored in both chunks, breaking the ascending-timestamp invariant.
        match chunk.split() {
            Ok(mut new_chunk) => {
                // An empty upper half (splitting a single-sample chunk) owns nothing.
                let into_new =
                    new_chunk.len() > 0 && sample.timestamp >= new_chunk.first_timestamp();
                let (added, res) = if into_new {
                    let old_size = new_chunk.len();
                    let (size, res) = new_chunk.upsert(sample, duplicate_policy);
                    (size.saturating_sub(old_size), res)
                } else {
                    let old_size = chunk.len();
                    let (size, res) = chunk.upsert(sample, duplicate_policy);
                    (size.saturating_sub(old_size), res)
                };
                // `chunk`'s borrow ends here; `self` is usable again below.

                // Re-insert the upper half even when the upsert failed: `split()` already moved
                // those samples out of `chunk`, so dropping it here would lose them. The split
                // itself only redistributes samples, so it leaves `total_samples` unchanged.
                if new_chunk.len() > 0 {
                    let insert_at = self
                        .chunks
                        .partition_point(|c| c.first_timestamp() <= new_chunk.first_timestamp());
                    self.chunks.insert(insert_at, new_chunk);
                }

                if !res.is_ok() {
                    return res;
                }

                // best-effort trim; don't block ingestion on failure
                if let Err(e) = self.trim() {
                    logging::log_warning(format!("TSDB: Error trimming time series: {e:?}"));
                }

                self.total_samples += added;
                // The insert above shifts chunk positions, so recompute both ends rather than
                // relying on the pre-split `is_last`.
                self.update_first_last_timestamps();

                SampleAddResult::Ok(sample)
            }
            Err(_) => SampleAddResult::Error(error_consts::CHUNK_SPLIT),
        }
    }

    pub(crate) fn split_chunks_if_needed(&mut self) -> TsdbResult<()> {
        let errored: AtomicBool = AtomicBool::new(false);

        // todo: track error, but allow partials
        let mut new_chunks = if self.is_compressed() {
            self.chunks
                .par_mut()
                .filter(|c| c.is_full())
                .flat_map(|chunks| {
                    if let Ok(split_chunk) = chunks.split() {
                        Some(split_chunk)
                    } else {
                        errored.store(true, std::sync::atomic::Ordering::Relaxed);
                        None
                    }
                })
                .collect::<Vec<_>>()
        } else {
            let mut new_chunks = Vec::with_capacity(std::cmp::max(2, self.chunks.len() / 6));
            for c in self.chunks.iter_mut().filter(|c| c.is_full()) {
                if let Ok(split_chunk) = c.split() {
                    new_chunks.push(split_chunk);
                } else {
                    errored.store(true, std::sync::atomic::Ordering::Relaxed);
                }
            }
            new_chunks
        };

        if !new_chunks.is_empty() {
            // Each split leaves the source chunk holding the lower half (first_timestamp
            // unchanged) and yields the upper half as a new chunk, so `self.chunks` stays
            // sorted and `new_chunks` forms a second, non-overlapping sorted run. Merge the
            // two ordered runs in O(n) instead of re-sorting the whole vector (O(n log n)).
            //
            // `new_chunks` is produced in source order by the serial path; sort defensively
            // (k = number of full chunks, tiny) so the parallel path can't violate the
            // merge precondition.
            new_chunks.sort_by_key(|chunk| chunk.first_timestamp());

            let existing = std::mem::take(&mut self.chunks);
            let mut merged = Vec::with_capacity(existing.len() + new_chunks.len());

            let mut a = existing.into_iter().peekable();
            let mut b = new_chunks.into_iter().peekable();
            loop {
                match (a.peek(), b.peek()) {
                    (Some(x), Some(y)) => {
                        if x.first_timestamp() <= y.first_timestamp() {
                            merged.push(a.next().unwrap());
                        } else {
                            merged.push(b.next().unwrap());
                        }
                    }
                    (Some(_), None) => merged.push(a.next().unwrap()),
                    (None, Some(_)) => merged.push(b.next().unwrap()),
                    (None, None) => break,
                }
            }
            self.chunks = merged;
        }

        if errored.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(TsdbError::ChunkSplitError);
        }

        Ok(())
    }

    pub(super) fn recalculate_total_samples(&mut self) {
        self.total_samples = self.chunks.iter().map(|c| c.len()).sum();
    }

    /// Merges a collection of samples into the time series.
    ///
    /// This function efficiently groups samples by the chunks they would belong to
    /// and applies the appropriate duplicate policy when merging. If samples are split across
    /// multiple chunks, they are processed (mostly) in parallel to optimize performance.
    ///
    /// ## Note
    ///
    /// `samples` **must** be sorted by timestamp.
    ///
    /// ### Arguments
    ///
    /// * `samples` - A slice of samples to merge into the time series
    /// * `policy_override` - Optional override for the duplicate policy to use when merging
    ///
    /// ### Returns
    ///
    /// A result containing a vector of `SampleAddResult` with the outcome for each sample.
    ///
    pub fn merge_samples(
        &mut self,
        samples: &[Sample],
        policy_override: Option<DuplicatePolicy>,
    ) -> TsdbResult<Vec<SampleAddResult>> {
        let results = self.merge_samples_deferring_retention(samples, policy_override)?;
        // Eager retention trim, as in `add` — a batch can advance the window too.
        self.apply_retention();
        Ok(results)
    }

    /// [`Self::merge_samples`] without the eager retention trim, for callers that
    /// compact afterwards and must do so against the pre-trim series (see
    /// [`Self::apply_retention`]).
    pub(super) fn merge_samples_deferring_retention(
        &mut self,
        samples: &[Sample],
        policy_override: Option<DuplicatePolicy>,
    ) -> TsdbResult<Vec<SampleAddResult>> {
        if samples.is_empty() {
            return Ok(Vec::new());
        }
        merge_samples(self, samples, policy_override)
    }

    /// Get the time series between given start and end time (both inclusive).
    pub fn get_range(&self, start_time: Timestamp, end_time: Timestamp) -> Vec<Sample> {
        if !self.overlaps(start_time, end_time) {
            return Vec::new();
        }
        let Some(range) = self.get_chunk_index_bounds(start_time, end_time) else {
            return Vec::new();
        };
        let (start_index, end_index) = range;
        let chunks = &self.chunks[start_index..=end_index];
        let mut samples = get_range_parallel(chunks, start_time, end_time).unwrap_or_default();
        if chunks.len() > 1 {
            // If we have multiple chunks, we need to sort the samples by timestamp
            samples.sort_by_key(|s| s.timestamp);
        }
        samples
    }

    pub fn get_range_filtered(
        &self,
        start_timestamp: Timestamp,
        end_timestamp: Timestamp,
        timestamp_filter: Option<&[Timestamp]>,
        value_filter: Option<ValueFilter>,
    ) -> Vec<Sample> {
        debug_assert!(start_timestamp <= end_timestamp);

        // TODO: propagate errors
        let mut samples = if let Some(ts_filter) = timestamp_filter {
            let timestamps = filter_timestamp_slice(ts_filter, start_timestamp, end_timestamp);
            self.samples_by_timestamps(&timestamps).unwrap_or_default()
        } else {
            self.get_range(start_timestamp, end_timestamp)
        };

        if let Some(value_filter) = value_filter {
            filter_samples_by_value(&mut samples, &value_filter);
        }

        samples
    }

    pub fn get_sample(&self, start_time: Timestamp) -> ValkeyResult<Option<Sample>> {
        let (index, found) = get_chunk_index(&self.chunks, start_time);
        if found {
            let chunk = &self.chunks[index];
            let mut samples = chunk
                .get_range(start_time, start_time)
                .map_err(|_e| ValkeyError::Str(error_consts::ERROR_FETCHING_SAMPLE))?;
            Ok(samples.pop())
        } else {
            Ok(None)
        }
    }

    pub fn samples_by_timestamps(&self, timestamps: &[Timestamp]) -> TsdbResult<Vec<Sample>> {
        if self.is_empty() || timestamps.is_empty() {
            return Ok(vec![]);
        }

        struct ChunkMeta<'a> {
            chunk: &'a TimeSeriesChunk,
            timestamps: SmallVec<[Timestamp; 6]>,
        }

        let mut meta_map: IntMap<usize, ChunkMeta> = Default::default();

        for &ts in timestamps {
            let (index, found) = get_chunk_index(&self.chunks, ts);
            if found && index < self.chunks.len() {
                meta_map
                    .entry(index)
                    .or_insert_with(|| ChunkMeta {
                        chunk: &self.chunks[index],
                        timestamps: SmallVec::new(),
                    })
                    .timestamps
                    .push(ts);
            }
        }

        #[inline]
        fn meta_fetch(meta: &ChunkMeta) -> TsdbResult<Vec<Sample>> {
            meta.chunk.samples_by_timestamps(&meta.timestamps)
        }

        fn fetch_parallel(slice: &[ChunkMeta]) -> TsdbResult<Vec<Sample>> {
            match slice {
                [] => Ok(vec![]),
                [meta] => meta_fetch(meta),
                _ => slice
                    .par()
                    .map(|meta| meta_fetch(meta))
                    .into_fallible_result()
                    .flat_map(|r| r)
                    .collect(),
            }
        }

        let len = meta_map.len();
        if len == 0 {
            Ok(vec![])
        } else {
            let metas = meta_map.into_values().collect::<Vec<_>>();
            let mut samples = fetch_parallel(&metas)?;
            if len > 1 {
                // If we have multiple chunks, we need to sort the samples by timestamp
                samples.sort_by_key(|s| s.timestamp);
            }
            Ok(samples)
        }
    }

    pub fn iter(&self) -> SeriesSampleIterator<'_> {
        SeriesSampleIterator::new(self, self.first_timestamp, self.last_timestamp(), false)
    }

    pub fn range_iter(&self, start: Timestamp, end: Timestamp) -> SeriesSampleIterator<'_> {
        let start = start.max(self.get_min_timestamp());
        SeriesSampleIterator::new(self, start, end, false)
    }

    /// Iterate the samples physically stored in `[start, end]`, **without** clamping
    /// `start` to the retention window the way [`Self::range_iter`] does.
    ///
    /// Compaction recalculation needs this: a batch can both write an out-of-order
    /// sample and advance the retention window past it, and the trim runs only after
    /// compaction (see [`Self::apply_retention`]). Clamping there would aggregate the
    /// bucket without the sample the batch just accepted, publishing a downstream
    /// value computed from fewer samples than RedisTimeSeries uses. Query paths keep
    /// the clamp — an untrimmed but expired sample must stay invisible to reads.
    pub(super) fn stored_range_iter(
        &self,
        start: Timestamp,
        end: Timestamp,
    ) -> SeriesSampleIterator<'_> {
        SeriesSampleIterator::new(self, start, end, false)
    }

    pub fn overlaps(&self, start_ts: Timestamp, end_ts: Timestamp) -> bool {
        !self.is_empty() && self.last_timestamp() >= start_ts && self.first_timestamp <= end_ts
    }

    pub fn is_older_than_retention(&self, timestamp: Timestamp) -> bool {
        if self.retention.is_zero() {
            return false;
        }
        let min_ts = self.get_min_timestamp();
        timestamp < min_ts
    }

    /// Remove chunks that lie *entirely* outside the retention window, i.e. whose
    /// newest sample is older than `min_timestamp`. The boundary is strict: a
    /// sample at exactly `min_timestamp` is retained (a series with retention R
    /// keeps `timestamp >= lastTimestamp - R`), so a chunk whose last sample is
    /// `min_timestamp` is kept and trimmed partially by the caller.
    pub(super) fn remove_expired_chunks(&mut self, min_timestamp: Timestamp) -> usize {
        let mut deleted_count = 0;
        self.chunks.retain(|chunk| {
            let last_ts = chunk.last_timestamp();
            if last_ts < min_timestamp {
                deleted_count += chunk.len();
                false
            } else {
                true
            }
        });
        deleted_count
    }

    pub(super) fn trim(&mut self) -> TsdbResult<usize> {
        let min_timestamp = self.get_min_timestamp();
        if self.first_timestamp >= min_timestamp {
            return Ok(0);
        }

        let mut deleted_count = self.remove_expired_chunks(min_timestamp);

        // Handle the boundary chunk: drop only samples strictly older than the
        // retention window. `remove_range` is inclusive, so the upper bound is
        // `min_timestamp - 1` — the sample at `min_timestamp` itself is retained.
        if let Some(chunk) = self.chunks.first_mut()
            && chunk.first_timestamp() < min_timestamp
        {
            if let Ok(count) = chunk.remove_range(0, min_timestamp - 1) {
                deleted_count += count;
            } else {
                return Err(TsdbError::RemoveRangeError);
            }
        }

        self.total_samples -= deleted_count;

        if deleted_count > 0 {
            // Update first_timestamp and last_timestamp
            self.update_first_last_timestamps();
        }

        self.chunks.shrink_to_fit();

        Ok(deleted_count)
    }

    pub fn remove_range(&mut self, start_ts: Timestamp, end_ts: Timestamp) -> TsdbResult<usize> {
        debug_assert!(start_ts <= end_ts);

        let mut deleted_samples = 0;

        fn remove_internal(
            chunk: &mut TimeSeriesChunk,
            start_ts: Timestamp,
            end_ts: Timestamp,
        ) -> TsdbResult<usize> {
            // Should we delete the entire chunk?
            if chunk.is_contained_by_range(start_ts, end_ts) {
                let count = chunk.len();
                chunk.clear();
                Ok(count)
            } else {
                // handle partial deletion
                chunk.remove_range(start_ts, end_ts)
            }
        }

        let Some((start_index, end_index)) = self.get_chunk_index_bounds(start_ts, end_ts) else {
            return Ok(0);
        };

        let is_compressed = self.is_compressed();

        let mut chunks = self.chunks.as_mut_slice();
        chunks = &mut chunks[start_index..=end_index];

        deleted_samples = match (is_compressed, chunks) {
            (_, []) => 0,
            (false, many) => {
                for chunk in many.iter_mut() {
                    deleted_samples += remove_internal(chunk, start_ts, end_ts)?;
                }
                deleted_samples
            }
            (true, [one]) => remove_internal(one, start_ts, end_ts)?,
            (true, many) => many
                .into_par()
                .map(|chunk| remove_internal(chunk, start_ts, end_ts))
                .into_fallible_result()
                .sum()?,
        };

        // Remove empty chunks
        let saved_len = self.chunks.len();
        self.chunks.retain(|chunk| !chunk.is_empty());
        if self.chunks.len() < saved_len {
            self.chunks.shrink_to_fit();
        }

        // Update metadata
        self.total_samples = self.total_samples.saturating_sub(deleted_samples);
        self.update_first_last_timestamps();

        Ok(deleted_samples)
    }

    /// Checks if the time series has at least one sample in the given time range.
    ///
    /// ## Arguments
    ///
    /// * `start_time` - Start timestamp (inclusive)
    /// * `end_time` - End timestamp (inclusive)
    ///
    /// ## Returns
    ///
    /// `true` if at least one sample exists in the given range, `false` otherwise
    pub fn has_samples_in_range(&self, start_time: Timestamp, end_time: Timestamp) -> bool {
        // Check if the time series could possibly have samples in the range
        if !self.overlaps(start_time, end_time) {
            return false;
        }

        // Find the actual min timestamp accounting for retention
        let min_timestamp = self.get_min_timestamp().max(start_time);

        // Get chunk index bounds for the range
        let Some((start_index, end_index)) = self.get_chunk_index_bounds(min_timestamp, end_time)
        else {
            return false;
        };

        // Check if any chunk in the range has samples within the time range
        let chunks = &self.chunks[start_index..=end_index];

        // Do NOT use parallel iteration here. This method can be called from within
        // another parallel iterator (e.g., in `filter_series_by_date_range`),
        // and nested parallelism with Rayon can lead to deadlocks if the global
        // thread pool is exhausted. A sequential check is safer and the performance
        // impact is minimal for a single series's chunks.
        chunks
            .iter()
            .any(|c| c.has_samples_in_range(start_time, end_time))
    }

    pub fn increment_sample_value(
        &mut self,
        timestamp: Option<Timestamp>,
        delta: f64,
    ) -> ValkeyResult<SampleAddResult> {
        if delta.is_nan() {
            return Err(ValkeyError::Str(
                error_consts::CANNOT_INCREMENT_DECREMENT_NAN,
            ));
        }
        // if we have at least one sample, increment the last one
        let (timestamp, last_ts, value) = if let Some(sample) = self.last_sample {
            if sample.value.is_nan() {
                return Err(ValkeyError::Str(
                    error_consts::CANNOT_INCREMENT_DECREMENT_NAN,
                ));
            }
            let last_ts = sample.timestamp;
            let ts = timestamp.unwrap_or(last_ts);
            let value = sample.value + delta;
            (ts, last_ts, value)
        } else {
            let ts = timestamp.unwrap_or_else(current_time_millis);
            (ts, ts, delta)
        };

        if timestamp < last_ts {
            return Err(ValkeyError::Str(
                "TSDB: timestamp must be equal to or higher than the maximum existing timestamp",
            ));
        }

        // todo: should we add a flag to skip adjust_value()?
        Ok(self.add(timestamp, value, Some(DuplicatePolicy::KeepLast)))
    }

    pub(super) fn update_first_last_timestamps(&mut self) {
        if let Some(first_chunk) = self.chunks.first() {
            self.first_timestamp = first_chunk.first_timestamp();
        } else {
            self.first_timestamp = 0;
        }

        if let Some(last_chunk) = self.chunks.last() {
            self.last_sample = last_chunk.last_sample();
        } else {
            self.last_sample = None;
        }
    }

    pub fn data_size(&self) -> usize {
        self.chunks.iter().map(|x| x.size()).sum()
    }

    pub fn memory_usage(&self) -> usize {
        size_of::<Self>() + self.get_heap_size()
    }

    /// Returns the minimum timestamp of the time series, considering the retention period.
    pub(crate) fn get_min_timestamp(&self) -> Timestamp {
        if self.retention.is_zero() {
            self.first_timestamp
        } else {
            self.last_sample.map_or(0, |last| {
                last.timestamp
                    .saturating_sub(self.retention.as_millis() as i64)
                    .max(0)
            })
        }
    }

    pub(super) fn update_last_sample(&mut self) {
        if let Some(last_chunk) = self.chunks.last() {
            self.last_sample = last_chunk.last_sample();
        } else {
            self.last_sample = None;
        }
    }

    pub(crate) fn last_timestamp(&self) -> Timestamp {
        if let Some(last_sample) = self.last_sample {
            last_sample.timestamp
        } else {
            0
        }
    }

    pub fn chunk_containing_timestamp(&self, ts: Timestamp) -> Option<&TimeSeriesChunk> {
        let (index, found) = get_chunk_index(&self.chunks, ts);
        if found { self.chunks.get(index) } else { None }
    }

    /// Finds the start and end chunk indices (inclusive) for a date range.
    ///
    /// # Parameters
    ///
    /// * `start`: The lower bound of the range to search for.
    /// * `end`: The upper bound of the range to search for.
    ///
    /// # Returns
    ///
    /// Returns `Option<(usize, usize)>`:
    /// * `Some((start_idx, end_idx))` if valid indices are found within the range.
    /// * `None` if the series is empty, if all samples are less than `start`,
    ///   or if `start` and `end` are equal and greater than the sample at the found index.
    ///
    /// Used to get an inclusive bound for series chunks (all chunks containing samples in the range [start_index...=end_index])
    pub(crate) fn get_chunk_index_bounds(
        &self,
        start: Timestamp,
        end: Timestamp,
    ) -> Option<(usize, usize)> {
        if self.is_empty() {
            return None;
        }

        let len = self.chunks.len();

        let start_idx = find_start_chunk_index(&self.chunks, start);
        if start_idx >= len {
            return None;
        }

        let right = &self.chunks[start_idx..];
        let (idx, _found) = find_last_ge_index(right, end);
        let end_idx = start_idx + idx;

        // imagine this scenario:
        // chunk start timestamps = [10, 20, 30, 40]
        // start = 25, end = 25
        // we have a situation where start_index == end_index (2), yet samples[2] is greater than end,
        if start_idx == end_idx {
            // todo: get_unchecked
            if self.chunks[start_idx].first_timestamp() > end {
                return None;
            }
        }

        Some((start_idx, end_idx))
    }

    pub fn optimize(&mut self) {
        // todo: merge chunks if possible
        self.chunks.par_mut().for_each(|chunk| {
            let _ = chunk.optimize();
        });
    }

    #[cfg(test)]
    pub(super) fn update_state_from_chunks(&mut self) {
        self.update_first_last_timestamps();
        self.total_samples = self.chunks.iter().map(|x| x.len()).sum();
    }

    pub fn is_compaction(&self) -> bool {
        self.src_series.is_some()
    }

    /// The sample `TS.GET` / `TS.MGET` report when `LATEST` is not given.
    ///
    /// In `extended` mode (the default) that is simply the series' last sample. In
    /// `ts-compatibility-mode strict` a compaction destination instead reports the
    /// last bucket published by forward progress, matching RedisTimeSeries: it caches
    /// a destination last-sample that back-filling an older bucket does not refresh,
    /// so its reply disagrees with its own `TS.RANGE` (DIV-0023). `LATEST` is
    /// unaffected — both engines already agree there.
    ///
    /// Falls back to the true last sample when no forward close has been recorded:
    /// nothing published yet, or after a reload, since the marker is runtime-only.
    ///
    /// The marker is also only honored while the destination still holds that bucket.
    /// `TS.DEL` on the source (or retention) can drop it, and RedisTimeSeries then
    /// reports the destination as empty rather than naming a sample that is gone —
    /// so the stored sample is re-read and the marker ignored when it has vanished.
    pub fn reported_last_sample(&self) -> Option<Sample> {
        if crate::config::is_strict_rts_compat()
            && self.is_compaction()
            && !self.is_empty()
            && let Some(published) = self.last_forward_close
            && let Ok(Some(current)) = self.get_sample(published.timestamp)
        {
            return Some(current);
        }
        self.last_sample
    }

    pub(crate) fn debug_digest(&self, digest: &mut Digest) {
        // hash labels
        calc_metric_name_digest(&self.labels, digest);
        let retention_msecs = self.retention.as_millis() as i64;
        digest.add_long_long(retention_msecs);

        // Handle sample_duplicates
        calc_duplicate_policy_digest(&self.sample_duplicates, digest);

        digest.add_string_buffer(self.chunk_encoding.name().as_bytes());

        if let Some(rounding) = &self.rounding {
            calc_rounding_digest(rounding, digest);
        } else {
            digest.add_string_buffer(b"none");
        }
        digest.add_long_long(self.chunk_size_bytes as i64);

        digest.add_long_long(self.chunks.len() as i64);
        for chunk in self.chunks.iter() {
            chunk.debug_digest(digest);
        }

        digest.add_long_long(self.total_samples as i64);
        digest.add_long_long(self.first_timestamp);
        if let Some(sample) = &self.last_sample {
            digest.add_long_long(sample.timestamp);
            digest.add_string_buffer(sample.value.to_le_bytes().as_ref());
        } else {
            digest.add_long_long(-1); // indicate no last sample
        }

        let src_id = if let Some(id) = self.src_series {
            id as i64
        } else {
            -1 // use -1 to indicate no source series
        };
        digest.add_long_long(src_id);
        // add rules
        digest.add_long_long(self.rules.len() as i64);
        for rule in self.rules.iter() {
            calc_compaction_digest(rule, digest);
        }

        digest.end_sequence()
    }
}

impl Default for TimeSeries {
    fn default() -> Self {
        Self {
            id: 0,
            labels: Default::default(),
            retention: Default::default(),
            sample_duplicates: Default::default(),
            chunk_encoding: Default::default(),
            chunk_size_bytes: DEFAULT_CHUNK_SIZE_BYTES,
            chunks: vec![],
            total_samples: 0,
            first_timestamp: 0,
            rounding: None,
            last_sample: None,
            src_series: None,
            rules: vec![],
            _db: None,
            last_forward_close: None,
        }
    }
}

fn binary_search_chunks_by_timestamp(chunks: &[TimeSeriesChunk], ts: Timestamp) -> (usize, bool) {
    match chunks.binary_search_by(|probe| {
        if ts < probe.first_timestamp() {
            std::cmp::Ordering::Greater
        } else if ts > probe.last_timestamp() {
            std::cmp::Ordering::Less
        } else {
            std::cmp::Ordering::Equal
        }
    }) {
        Ok(pos) => (pos, true),
        Err(pos) => (pos, false),
    }
}

const LINEAR_SCAN_MAX: usize = 16;

/// Find the index of the first chunk in which the timestamp belongs. Assumes !chunks.is_empty()
pub(super) fn find_start_chunk_index(arr: &[TimeSeriesChunk], ts: Timestamp) -> usize {
    match arr {
        [] => 0,
        [first, ..] if ts <= first.first_timestamp() => 0,
        _ if arr.len() <= LINEAR_SCAN_MAX => arr
            .iter()
            .position(|x| ts >= x.first_timestamp())
            .unwrap_or(arr.len()),
        _ => {
            let (pos, _) = binary_search_chunks_by_timestamp(arr, ts);
            pos
        }
    }
}

/// Return the index of the chunk in which the timestamp belongs. Assumes !chunks.is_empty()
fn get_chunk_index(chunks: &[TimeSeriesChunk], timestamp: Timestamp) -> (usize, bool) {
    match chunks {
        [] => (0, false),
        _ if chunks.len() <= LINEAR_SCAN_MAX => chunks
            .iter()
            .enumerate()
            .find_map(|(i, chunk)| {
                if chunk.is_timestamp_in_range(timestamp) {
                    Some((i, true))
                } else {
                    None
                }
            })
            .unwrap_or((chunks.len(), false)),
        _ => binary_search_chunks_by_timestamp(chunks, timestamp),
    }
}

pub(super) fn find_last_ge_index(chunks: &[TimeSeriesChunk], ts: Timestamp) -> (usize, bool) {
    match chunks {
        [] => (0, false),
        _ if chunks.len() <= LINEAR_SCAN_MAX => chunks
            .iter()
            .rposition(|x| ts >= x.first_timestamp())
            .map_or((0, false), |idx| {
                let chunk = &chunks[idx];
                // `ts >= first_timestamp` here, so "not in range" means `ts` is past the
                // chunk's last timestamp. The chunk is still the last one whose first
                // timestamp is <= ts, so it must be included in any inclusive end bound.
                (idx, chunk.is_timestamp_in_range(ts))
            }),
        _ => binary_search_chunks_by_timestamp(chunks, ts),
    }
}

fn get_range_parallel(
    chunks: &[TimeSeriesChunk],
    start: Timestamp,
    end: Timestamp,
) -> TsdbResult<Vec<Sample>> {
    match chunks {
        [] => Ok(vec![]),
        [chunk] => chunk.get_range(start, end),
        _ => chunks
            .into_par()
            .map(|chunk| chunk.get_range(start, end))
            .into_fallible_result()
            .flat_map(|x| x)
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_one_entry() {
        let mut ts = TimeSeries::new();
        assert!(ts.add(100, 200.0, None).is_ok());

        assert_eq!(ts.get_last_chunk().len(), 1);
        let last_block = ts.get_last_chunk();
        let samples = last_block.get_range(0, 1000).unwrap();

        let data_point = samples.first().unwrap();
        assert_eq!(data_point.timestamp, 100);
        assert_eq!(data_point.value, 200.0);
        assert_eq!(ts.total_samples, 1);
        assert_eq!(ts.first_timestamp, 100);
        assert_eq!(ts.last_timestamp(), 100);
    }
}
