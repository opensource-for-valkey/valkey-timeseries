//! Compaction (downsampling) rules: a source series aggregates into destination series in fixed
//! time buckets.
//!
//! - this file: the rule type, its persistence, and the `TimeSeries` entry points;
//! - `engine`: one rule against one destination — bucket arithmetic, Context-free;
//! - `cascade`: every rule of a source, then each destination's own rules, with destination
//!   resolution and keyspace notifications;
//! - `links`: `LATEST`, relinking after `RENAME`, and rule-cycle checks.

use crate::aggregators::{AggregationHandler, Aggregator, calc_bucket_start};
use crate::common::rdb::{
    RdbSerializable, rdb_load_bool, rdb_load_timestamp, rdb_save_bool, rdb_save_timestamp,
};
use crate::common::{Sample, Timestamp};
use crate::error::TsdbResult;
use crate::error_consts;
use crate::series::{SeriesLink, TimeSeries};
use get_size2::GetSize;
use std::sync::atomic::AtomicU64;
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, raw};

mod cascade;
mod engine;
mod links;

pub use cascade::apply_compaction;
pub(crate) use cascade::fail_next_compaction;
pub use engine::CompactionOp;
#[cfg(feature = "test-utils")]
pub use engine::{bench_ingest_one, bench_rule};
pub use links::{MAX_COMPACTION_CHAIN_LENGTH, check_new_rule_links};
pub(crate) use links::{get_latest_compaction_sample, live_source_key, relink_renamed_series};

const TEMP_VEC_LEN: usize = 6;

#[derive(Debug, Clone, Hash, PartialEq)]
pub struct CompactionRule {
    /// The destination series (see [`SeriesLink`]).
    pub dest: SeriesLink,
    pub aggregator: Aggregator,
    pub bucket_duration: u64,
    pub align_timestamp: Timestamp,
    pub bucket_start: Option<Timestamp>,
    pub has_samples: bool,
    /// Set when the rule's last application returned an error, so its open-bucket state may not
    /// reflect every sample the source stored. The next `Ingest` rebuilds the open bucket from the
    /// source first (`rebuild_open_bucket_as_of`); a `RemoveRange` rebuilds it anyway. The rebuild
    /// covers the open bucket only: destination buckets the failed application lost or left stale
    /// (a bucket it should have closed, or one a failed removal should have recalculated) are not
    /// repaired — `ts_compaction_failures` is how an operator learns of them. Node-local:
    /// persisted, so a restart does not forget it, but not hashed into the digest, because a
    /// replica's application of the same write may well have succeeded.
    pub dirty: bool,
}

impl GetSize for CompactionRule {
    fn get_size(&self) -> usize {
        self.dest.get_size()
            + self.aggregator.get_size()
            + size_of::<u64>() // bucket_duration
            + size_of::<Timestamp>() // align_timestamp
            + size_of::<Option<Timestamp>>() // bucket_start
            + size_of::<bool>() // has_samples
            + size_of::<bool>() // dirty
    }
}

impl CompactionRule {
    pub(crate) fn calc_bucket_start(&self, ts: Timestamp) -> Timestamp {
        calc_bucket_start(ts, self.align_timestamp, self.bucket_duration)
    }

    /// Exclusive end of the bucket that starts at `bucket_start` (a [`Self::calc_bucket_start`]
    /// result).
    ///
    /// Not `bucket_start + bucket_duration`: with a non-zero align timestamp the first bucket's
    /// true start is negative and `calc_bucket_start` clamps it to 0, so that bucket is shorter
    /// than a full duration — `[0, align % duration)`. Adding a whole duration to it swept the
    /// next bucket's samples into its recomputation, double-counting them. The start of the
    /// bucket one duration on is the end in both cases.
    pub(crate) fn bucket_end(&self, bucket_start: Timestamp) -> Timestamp {
        self.calc_bucket_start(bucket_start.saturating_add_unsigned(self.bucket_duration))
    }

    pub(super) fn get_bucket_range(&self, ts: Timestamp) -> (Timestamp, Timestamp) {
        let start = self.calc_bucket_start(ts);
        (start, self.bucket_end(start))
    }

    pub(super) fn reset(&mut self) {
        AggregationHandler::reset(&mut self.aggregator);
        self.bucket_start = None;
        self.has_samples = false;
    }

    pub(super) fn update(&mut self, ts: Timestamp, value: f64) {
        if AggregationHandler::update(&mut self.aggregator, ts, value) {
            self.has_samples = true;
        }
    }
}

impl RdbSerializable for CompactionRule {
    fn rdb_save(&self, rdb: *mut raw::RedisModuleIO) {
        self.dest.rdb_save(rdb);
        self.aggregator.rdb_save(rdb);
        raw::save_unsigned(rdb, self.bucket_duration);
        rdb_save_timestamp(rdb, self.align_timestamp);
        rdb_save_timestamp(rdb, self.bucket_start.unwrap_or(-1));
        rdb_save_bool(rdb, self.has_samples);
        rdb_save_bool(rdb, self.dirty);
    }

    fn rdb_load(rdb: *mut raw::RedisModuleIO) -> ValkeyResult<Self> {
        let dest = SeriesLink::rdb_load(rdb)?;
        let mut aggregator = Aggregator::rdb_load(rdb)?;
        let bucket_duration = raw::load_unsigned(rdb)?;
        // `rate` persists its window in whole seconds; the rule's bucket duration is exact.
        if let Aggregator::Rate(r) = &mut aggregator {
            r.set_window_ms(bucket_duration);
        }
        let align_timestamp = rdb_load_timestamp(rdb)?;
        let start_ts = rdb_load_timestamp(rdb)?;
        let has_samples = rdb_load_bool(rdb)?;
        let dirty = rdb_load_bool(rdb)?;
        let bucket_start = if start_ts == -1 { None } else { Some(start_ts) };

        validate_restored_rule_parameters(bucket_duration, align_timestamp)?;

        Ok(CompactionRule {
            dest,
            aggregator,
            bucket_duration,
            align_timestamp,
            bucket_start,
            has_samples,
            dirty,
        })
    }
}

/// Rebuild `rule`'s open-bucket state from the source as it stood when its last timestamp was
/// `as_of`: the bucket containing `as_of`, aggregated over the stored samples in that bucket at or
/// below `as_of`. Anything above it is the current write's own append, already stored, which the
/// engine is about to stream in. `None` means the source was empty: no open bucket.
///
/// An approximation of the state a rule maintains by streaming — exact for every aggregator whose
/// bucket value depends only on the bucket's samples, but `rate`/`increase` and a retention window
/// shorter than a bucket can differ — so it runs only to recover a `dirty` rule.
pub(super) fn rebuild_open_bucket_as_of(
    parent: &TimeSeries,
    rule: &mut CompactionRule,
    as_of: Option<Timestamp>,
) {
    rule.reset();
    let Some(as_of) = as_of else {
        return;
    };
    let (start, end) = rule.get_bucket_range(as_of);
    rule.bucket_start = Some(start);
    for sample in parent.range_iter(start, end - 1) {
        if sample.timestamp > as_of {
            break;
        }
        rule.update(sample.timestamp, sample.value);
    }
}

/// Rule applications that returned an error (`INFO ts_compaction` → `ts_compaction_failures`).
/// Non-zero means a destination may be missing a bucket its source should have produced.
pub static COMPACTION_FAILURES: AtomicU64 = AtomicU64::new(0);
/// Open-bucket rebuilds of `dirty` rules (`ts_compaction_state_rebuilds`).
pub static COMPACTION_STATE_REBUILDS: AtomicU64 = AtomicU64::new(0);

/// Resync `series`' rules after `[start, end]` was removed from it (stored under `key`). The
/// removal is already committed, so a rule failure is logged rather than returned — see
/// [`TimeSeries::remove_range_with_compaction`]. The cascade has already marked the rule `dirty`
/// and counted the failure.
pub(crate) fn resync_rules_after_removal(
    ctx: &Context,
    key: &ValkeyString,
    series: &mut TimeSeries,
    start: Timestamp,
    end: Timestamp,
) {
    if let Err(err) = apply_compaction(ctx, series, CompactionOp::RemoveRange { start, end }) {
        ctx.log_warning(&format!(
            "TSDB: error running compaction after deleting from key '{key}': {err}"
        ));
    }
}

fn validate_restored_rule_parameters(
    bucket_duration: u64,
    align_timestamp: Timestamp,
) -> ValkeyResult<()> {
    if bucket_duration == 0 {
        return Err(ValkeyError::Str(error_consts::BUCKET_DURATION_TOO_SMALL));
    }

    // calc_bucket_start converts the duration to i64 for its modulo
    // arithmetic. Reject values that would become negative after that cast.
    if bucket_duration > i64::MAX as u64 {
        return Err(ValkeyError::String(format!(
            "TSDB: compaction rule bucket duration is too large: {bucket_duration}"
        )));
    }

    // TS.CREATERULE parses alignTimestamp as a non-negative timestamp. Keep
    // restored rules within the same domain rather than importing a value that
    // cannot be produced by the command path.
    if align_timestamp < 0 {
        return Err(ValkeyError::Str(error_consts::INVALID_ALIGNMENT_TIMESTAMP));
    }

    Ok(())
}

impl TimeSeries {
    pub fn add_compaction_rule(&mut self, rule: CompactionRule) {
        let mut rule = rule;
        if let Aggregator::Rate(r) = &mut rule.aggregator {
            r.set_window_ms(rule.bucket_duration);
        }
        self.rules.push(rule);
    }

    /// Removes the rule whose destination is the series stored under `dest_key`.
    pub fn remove_compaction_rule(&mut self, dest_key: &[u8]) -> Option<CompactionRule> {
        let index = self
            .rules
            .iter()
            .position(|rule| rule.dest.points_to(dest_key))?;
        Some(self.rules.remove(index))
    }

    /// Remove `[start_ts, end_ts]` from this series (`key`), then resync its rules.
    ///
    /// Only the source removal can fail the call. Once it is committed, a rule failure is logged
    /// and swallowed, as the write path does (`TS.ADD`/`TS.MADD`/`TS.ADDBULK`): an error here
    /// would skip the caller's replication and keyspace event, leaving the replica and the AOF
    /// with samples this node deleted. The failing rule is left `dirty` and counted in
    /// `ts_compaction_failures` by the cascade.
    pub fn remove_range_with_compaction(
        &mut self,
        ctx: &Context,
        key: &ValkeyString,
        start_ts: Timestamp,
        end_ts: Timestamp,
    ) -> TsdbResult<usize> {
        let deleted_count = self.remove_range(start_ts, end_ts)?;

        if deleted_count > 0 {
            resync_rules_after_removal(ctx, key, self, start_ts, end_ts);
        }

        Ok(deleted_count)
    }

    /// Drive this series' rules for one sample just stored at `sample.timestamp`, as a one-item
    /// batch. `prev_last` is the series' last timestamp from *before* the write: at or below it
    /// the write is a back-fill (or an in-place update) and the bucket is recalculated from the
    /// source, above it the sample streams into the open bucket.
    pub fn compact_sample(
        &mut self,
        ctx: &Context,
        sample: Sample,
        prev_last: Option<Timestamp>,
    ) -> TsdbResult<()> {
        // For an append the stored (rounded) value is the last sample; a back-fill only uses the
        // timestamp, since its bucket is re-read from the source.
        let stored = match self.last_sample {
            Some(last) if last.timestamp == sample.timestamp => last,
            _ => sample,
        };
        self.batch_compaction(ctx, &[stored], prev_last, &[stored.timestamp])
    }

    /// Propagate a batch of samples that were already merged into this series.
    ///
    /// `samples` must be sorted by timestamp ascending (unique timestamps) and `prev_last`
    /// must be this series' last timestamp from before the batch was merged. See
    /// [`CompactionOp::Ingest`].
    pub fn batch_compaction(
        &mut self,
        ctx: &Context,
        samples: &[Sample],
        prev_last: Option<Timestamp>,
        input_order: &[Timestamp],
    ) -> TsdbResult<()> {
        if self.rules.is_empty() || samples.is_empty() {
            return Ok(());
        }
        apply_compaction(
            ctx,
            self,
            CompactionOp::Ingest {
                samples,
                prev_last,
                input_order,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregators::AggregationType;

    fn rule(agg: AggregationType, bucket: u64) -> CompactionRule {
        CompactionRule {
            dest: SeriesLink::from("dst"),
            aggregator: agg.into(),
            bucket_duration: bucket,
            align_timestamp: 0,
            bucket_start: None,
            has_samples: false,
            dirty: false,
        }
    }

    fn series_with(samples: &[(Timestamp, f64)]) -> TimeSeries {
        let mut series = TimeSeries::new();
        for &(ts, value) in samples {
            assert!(series.add(ts, value, None).is_ok(), "fixture add {ts}");
        }
        series
    }

    #[test]
    fn rebuild_stops_at_as_of_so_the_pending_append_is_not_counted_twice() {
        // 1700 is the current write's own append, already stored; the append path streams it next.
        let series = series_with(&[(1000, 9.0), (1200, 3.0), (1700, 100.0)]);
        let mut rebuilt = rule(AggregationType::Sum, 1000);
        rebuild_open_bucket_as_of(&series, &mut rebuilt, Some(1200));
        assert_eq!(rebuilt.bucket_start, Some(1000));
        assert!(rebuilt.has_samples);
        assert_eq!(AggregationHandler::current(&rebuilt.aggregator), Some(12.0));
    }

    #[test]
    fn rebuild_for_an_empty_source_clears_the_bucket() {
        let series = series_with(&[(1000, 9.0)]);
        let mut rebuilt = rule(AggregationType::Sum, 1000);
        rebuilt.bucket_start = Some(1000);
        rebuilt.update(1000, 9.0);
        rebuild_open_bucket_as_of(&series, &mut rebuilt, None);
        assert_eq!(rebuilt.bucket_start, None);
        assert!(!rebuilt.has_samples);
    }

    #[test]
    fn restored_rule_rejects_zero_bucket_duration() {
        assert!(validate_restored_rule_parameters(0, 0).is_err());
    }

    #[test]
    fn restored_rule_rejects_duration_that_overflows_signed_arithmetic() {
        assert!(validate_restored_rule_parameters(i64::MAX as u64 + 1, 0).is_err());
    }

    #[test]
    fn restored_rule_rejects_negative_alignment() {
        assert!(validate_restored_rule_parameters(1, -1).is_err());
    }

    #[test]
    fn restored_rule_accepts_valid_parameters() {
        assert!(validate_restored_rule_parameters(1, Timestamp::MAX).is_ok());
    }
}
