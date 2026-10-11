//! What all PromQL work on this node may take together.
//!
//! Every other PromQL limit is per query: `ts-promql-max-samples-per-query` bounds what one
//! query loads, `ts-promql-max-response-series` what it returns. None of them sees the other
//! queries, so eight workers can each charge a full per-query budget, and a server already
//! near `maxmemory` with its data set admits a query as readily as an idle one. These do:
//!
//! - **Samples in flight** (`ts-promql-max-samples-in-flight`): every query's sample budget,
//!   and every shard read served for a peer, also charges one process-wide count, released when
//!   the budget is dropped. Past the limit the charge fails as the per-query one does. The
//!   count is the sum of the live budgets' running totals, so like them it is a charge, not a
//!   measure of what is held: it keeps counting samples a query has already dropped, and never
//!   sees what no budget charges (see [`super::sample_budget`]).
//! - **Memory** (`ts-promql-max-memory-percent`): the server's used memory as a share of
//!   `maxmemory`, sampled on the main thread each cron tick. Past the limit a query is refused
//!   at admission, and a running one stops at its next deadline check. With no `maxmemory`
//!   the ratio is 0 and this never refuses.
//! - **Reply size** (`ts-promql-max-reply-bytes`): estimated from the result before any of it
//!   is written, since the server buffers a normal client's reply whatever its size.
//!
//! All three refuse rather than wait: a query that waited mid-evaluation would hold whatever
//! it had already loaded while it did.

use crate::error_consts;
use crate::labels::Label;
use crate::promql::model::{QueryValue, RangeSample};
use crate::promql::{QueryError, QueryResult};
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

/// The state the process-wide limits are checked against: what live counted budgets have
/// charged, and the last sampled memory use.
///
/// One per node, [`ADMISSION`], which every query and shard read shares. A test that drives
/// the counts — charging past a limit, or faking memory pressure — makes its own instead:
/// unit tests run in parallel in one process, and doing it to [`ADMISSION`] refuses every
/// other test's queries meanwhile, since they run at the default limits.
#[derive(Debug)]
pub(crate) struct Admission {
    /// What live budgets that count toward the process-wide limit have charged in all.
    in_flight: AtomicUsize,
    /// The last sampled `used_memory / maxmemory`, as `f32` bits: 0 with no `maxmemory`.
    memory_ratio: AtomicU32,
}

/// This node's admission state.
pub(crate) static ADMISSION: Admission = Admission::new();

impl Admission {
    pub(crate) const fn new() -> Self {
        Self {
            in_flight: AtomicUsize::new(0),
            memory_ratio: AtomicU32::new(0),
        }
    }

    /// What live counted budgets have charged in all.
    pub(crate) fn samples_in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Relaxed)
    }

    /// Add `samples` to the process-wide count; fail once it passes `limit` (0: no limit).
    /// The samples are added either way: the caller releases what it added when it is done.
    pub(super) fn charge_in_flight(&self, samples: usize, limit: usize) -> QueryResult<()> {
        let total = self
            .in_flight
            .fetch_add(samples, Ordering::Relaxed)
            .saturating_add(samples);
        if limit > 0 && total > limit {
            return Err(QueryError::Overloaded(format!(
                "{}: running PromQL queries would have charged {total} > {limit} samples \
                 (ts-promql-max-samples-in-flight)",
                error_consts::PROMQL_OVERLOADED_ERROR
            )));
        }
        Ok(())
    }

    /// Give back samples an earlier [`Self::charge_in_flight`] added.
    pub(super) fn release_in_flight(&self, samples: usize) {
        self.in_flight.fetch_sub(samples, Ordering::Relaxed);
    }

    pub(in crate::promql) fn store_memory_ratio(&self, ratio: f32) {
        self.memory_ratio.store(ratio.to_bits(), Ordering::Relaxed);
    }

    /// Fail if the last sampled memory use is at or past `limit_percent` of `maxmemory`
    /// (0: no limit).
    pub(crate) fn check_memory(&self, limit_percent: u32) -> QueryResult<()> {
        if limit_percent == 0 {
            return Ok(());
        }
        let ratio = f32::from_bits(self.memory_ratio.load(Ordering::Relaxed));
        let percent = (ratio * 100.0) as u32;
        if percent >= limit_percent {
            return Err(QueryError::Overloaded(format!(
                "{}: memory use is {percent}% of maxmemory, at or past \
                 ts-promql-max-memory-percent ({limit_percent}%)",
                error_consts::PROMQL_OVERLOADED_ERROR
            )));
        }
        Ok(())
    }
}

/// What this node's live counted budgets have charged in all.
pub(crate) fn samples_in_flight() -> usize {
    ADMISSION.samples_in_flight()
}

/// Sample the server's memory use. Main thread only: the server computes the ratio from
/// state its own thread owns.
pub(crate) fn sample_memory() {
    let ratio = unsafe { valkey_module::raw::RedisModule_GetUsedMemoryRatio.unwrap()() };
    ADMISSION.store_memory_ratio(ratio);
}

/// Fail if this node's last sampled memory use is at or past `limit_percent` of `maxmemory`
/// (0: no limit).
pub(crate) fn check_memory(limit_percent: u32) -> QueryResult<()> {
    ADMISSION.check_memory(limit_percent)
}

// The reply's size, estimated from the RESP the reply writers emit. Generous rather than
// exact: a sample is an array of a timestamp and a formatted value, a label a name and a
// value with their length prefixes, a series a map of `metric` and `value`.
const SAMPLE_REPLY_BYTES: usize = 48;
const LABEL_REPLY_OVERHEAD: usize = 16;
const SERIES_REPLY_OVERHEAD: usize = 48;
const REPLY_OVERHEAD: usize = 64;

fn labels_reply_bytes(labels: &[Label]) -> usize {
    labels
        .iter()
        .map(|l| l.name.len() + l.value.len() + LABEL_REPLY_OVERHEAD)
        .sum()
}

fn series_reply_bytes(series: &RangeSample) -> usize {
    SERIES_REPLY_OVERHEAD
        + labels_reply_bytes(series.labels.as_ref())
        + series.samples.len() * SAMPLE_REPLY_BYTES
}

/// About how many bytes the reply to `value` takes.
pub(crate) fn estimate_reply_bytes(value: &QueryValue) -> usize {
    REPLY_OVERHEAD
        + match value {
            QueryValue::Vector(samples) => samples
                .iter()
                .map(|s| {
                    SERIES_REPLY_OVERHEAD
                        + labels_reply_bytes(s.labels.as_ref())
                        + SAMPLE_REPLY_BYTES
                })
                .sum(),
            QueryValue::Matrix(series) => series.iter().map(series_reply_bytes).sum(),
            QueryValue::Scalar { .. } => SAMPLE_REPLY_BYTES,
            QueryValue::String(s) => SAMPLE_REPLY_BYTES + s.len(),
        }
}

/// A floor on [`estimate_range_reply_bytes`] for a range result of `series`
/// series holding `points` samples in all, leaving out their labels: for
/// checking the reply's size while the result is still being built, without
/// ever refusing a result the final estimate would admit.
pub(crate) fn range_reply_bytes_floor(series: usize, points: usize) -> usize {
    REPLY_OVERHEAD
        .saturating_add(series.saturating_mul(SERIES_REPLY_OVERHEAD))
        .saturating_add(points.saturating_mul(SAMPLE_REPLY_BYTES))
}

/// About how many bytes the reply to a range query's `series` takes.
pub(crate) fn estimate_range_reply_bytes(series: &[RangeSample]) -> usize {
    REPLY_OVERHEAD + series.iter().map(series_reply_bytes).sum::<usize>()
}

/// Fail if a reply estimated at `bytes` is larger than `limit` (0: no limit).
pub(crate) fn check_reply_size(bytes: usize, limit: usize) -> QueryResult<()> {
    if limit > 0 && bytes > limit {
        return Err(QueryError::ReplyTooLarge { bytes, limit });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Sample;

    #[test]
    fn memory_is_refused_at_and_past_the_limit_only() {
        // Its own state: at 90% on `ADMISSION`, every concurrent test's query would be
        // refused, since they run at the default 90% limit.
        let admission = Admission::new();
        assert!(
            admission.check_memory(90).is_ok(),
            "no maxmemory reads as 0"
        );
        admission.store_memory_ratio(0.899);
        assert!(admission.check_memory(90).is_ok());
        admission.store_memory_ratio(0.9);
        let err = admission.check_memory(90).unwrap_err();
        assert!(matches!(err, QueryError::Overloaded(_)), "{err}");
        assert!(err.ends_query());
        assert!(err.to_string().contains("90% of maxmemory"), "{err}");
        assert!(admission.check_memory(0).is_ok(), "0 disables the check");
        assert!(
            ADMISSION.check_memory(90).is_ok(),
            "the node's own state is untouched"
        );
    }

    #[test]
    fn the_reply_estimate_grows_with_series_samples_and_labels() {
        let series = |labels: usize, samples: usize| {
            let names: Vec<String> = (0..labels).map(|i| format!("name{i}")).collect();
            let pairs: Vec<(&str, &str)> = names.iter().map(|n| (n.as_str(), "value")).collect();
            RangeSample {
                labels: crate::labels::Labels::from_pairs(&pairs),
                samples: (0..samples as i64).map(|t| Sample::new(t, 1.0)).collect(),
            }
        };
        let small = estimate_range_reply_bytes(&[series(2, 10)]);
        assert!(estimate_range_reply_bytes(&[series(2, 11)]) > small);
        assert!(estimate_range_reply_bytes(&[series(3, 10)]) > small);
        assert!(estimate_range_reply_bytes(&[series(2, 10), series(2, 10)]) > small);
        // A thousand series of a thousand points: tens of megabytes.
        let big: Vec<_> = (0..1000).map(|_| series(5, 1000)).collect();
        assert!(estimate_range_reply_bytes(&big) > 40_000_000);

        assert!(check_reply_size(small, 0).is_ok(), "0 is unlimited");
        assert!(check_reply_size(small, small).is_ok());
        let err = check_reply_size(small + 1, small).unwrap_err();
        assert!(
            err.to_string().contains("ts-promql-max-reply-bytes"),
            "{err}"
        );
    }
}
