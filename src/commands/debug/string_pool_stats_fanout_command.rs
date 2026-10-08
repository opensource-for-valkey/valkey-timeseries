use super::ts_debug::reply_with_string_pool_stats;
use crate::commands::fanout_codec::generated::{
    StringPoolBucket, StringPoolStatsRequest, StringPoolStatsResponse, StringPoolTopKEntry,
};
use crate::common::replies::ReplyContext;
use crate::common::string_interner::{BucketStats, InternedString, Stats, TopKEntry};
use crate::config::is_debug_mode_enabled;
use crate::error_consts;
use crate::fanout::{
    FanoutClientCommand, FanoutCommandResult, FanoutContext, FanoutTarget, NodeInfo,
};
use ahash::AHashMap;
use std::collections::BTreeMap;
use valkey_module::{Context, Status, ValkeyError, ValkeyResult};

/// String pool statistics summed over one or more nodes, in the shape `TS._DEBUG
/// STRINGPOOLSTATS` replies with.
///
/// Every node has its own pool, so the sums count a string once per node that holds it: `count`
/// is the number of (node, string) pairs and `allocated` is what the cluster spends, which is the
/// figure memory questions want. The `by_ref_count` buckets are keyed by each node's *local*
/// reference count.
///
/// The top-K lists merge each node's own top K by value, adding up reference counts and
/// allocations. By size that is exact: a string in the cluster-wide top K is in the top K of every
/// node that holds it. By reference count it is approximate — a string just below the cut on
/// every node can outrank one that made a single node's list, and a listed string's count omits
/// the nodes where it fell below the cut.
#[derive(Default)]
pub struct StringPoolSummary {
    pub total: BucketStats,
    pub by_ref_count: BTreeMap<usize, BucketStats>,
    pub by_size: BTreeMap<usize, BucketStats>,
    pub memory_saved_bytes: usize,
    pub holder_count: usize,
    pub holder_slot_bytes: usize,
    pub top_k_by_ref: Vec<StringPoolTopKEntry>,
    pub top_k_by_size: Vec<StringPoolTopKEntry>,
}

impl StringPoolSummary {
    /// The summary of this node's pool alone.
    pub fn local(top_k: usize) -> Self {
        let mut summary = Self::default();
        summary.merge(InternedString::get_stats_with_top_k(top_k).into());
        summary.finish_top_k(top_k);
        summary
    }

    pub fn merge(&mut self, resp: StringPoolStatsResponse) {
        if let Some(total) = &resp.total {
            add_bucket(&mut self.total, total);
        }
        for bucket in &resp.by_ref_count {
            add_bucket(
                self.by_ref_count.entry(bucket.key as usize).or_default(),
                bucket,
            );
        }
        for bucket in &resp.by_size {
            add_bucket(self.by_size.entry(bucket.key as usize).or_default(), bucket);
        }
        self.memory_saved_bytes += resp.memory_saved_bytes as usize;
        self.holder_count += resp.holder_count as usize;
        self.holder_slot_bytes += resp.holder_slot_bytes as usize;
        self.top_k_by_ref.extend(resp.top_k_by_ref);
        self.top_k_by_size.extend(resp.top_k_by_size);
    }

    /// Merges the collected per-node top-K entries by value, then keeps the `k` highest of each
    /// ranking. Ties break by value, so the order is stable across runs.
    pub fn finish_top_k(&mut self, k: usize) {
        self.top_k_by_ref = merge_top_k(std::mem::take(&mut self.top_k_by_ref));
        self.top_k_by_ref.sort_by(|a, b| {
            b.ref_count
                .cmp(&a.ref_count)
                .then_with(|| a.value.cmp(&b.value))
        });
        self.top_k_by_ref.truncate(k);

        self.top_k_by_size = merge_top_k(std::mem::take(&mut self.top_k_by_size));
        self.top_k_by_size
            .sort_by(|a, b| b.bytes.cmp(&a.bytes).then_with(|| a.value.cmp(&b.value)));
        self.top_k_by_size.truncate(k);
    }

    /// What the strings cost in total, holder slots included; see [`Stats::total_storage_bytes`].
    pub fn total_storage_bytes(&self) -> usize {
        self.total.allocated + self.holder_slot_bytes
    }
}

fn add_bucket(acc: &mut BucketStats, bucket: &StringPoolBucket) {
    acc.count += bucket.count as usize;
    acc.bytes += bucket.bytes as usize;
    acc.allocated += bucket.allocated as usize;
}

fn merge_top_k(entries: Vec<StringPoolTopKEntry>) -> Vec<StringPoolTopKEntry> {
    let mut merged: AHashMap<String, StringPoolTopKEntry> = AHashMap::with_capacity(entries.len());
    for entry in entries {
        match merged.get_mut(&entry.value) {
            Some(acc) => {
                acc.ref_count += entry.ref_count;
                acc.allocated += entry.allocated;
            }
            None => {
                merged.insert(entry.value.clone(), entry);
            }
        }
    }
    merged.into_values().collect()
}

fn to_bucket(key: usize, bucket: &BucketStats) -> StringPoolBucket {
    StringPoolBucket {
        key: key as u64,
        count: bucket.count as u64,
        bytes: bucket.bytes as u64,
        allocated: bucket.allocated as u64,
    }
}

fn to_top_k_entry(entry: &TopKEntry) -> StringPoolTopKEntry {
    StringPoolTopKEntry {
        value: entry.value.as_str().to_string(),
        ref_count: entry.ref_count as u64,
        bytes: entry.bytes as u64,
        allocated: entry.allocated as u64,
    }
}

impl From<Stats> for StringPoolStatsResponse {
    fn from(stats: Stats) -> Self {
        Self {
            total: Some(to_bucket(0, &stats.total_stats)),
            by_ref_count: stats
                .by_ref_stats
                .iter()
                .map(|(&key, bucket)| to_bucket(key, bucket))
                .collect(),
            by_size: stats
                .by_size_stats
                .iter()
                .map(|(&key, bucket)| to_bucket(key, bucket))
                .collect(),
            memory_saved_bytes: stats.memory_saved_bytes as u64,
            holder_count: stats.holder_count as u64,
            holder_slot_bytes: stats.holder_slot_bytes as u64,
            top_k_by_ref: stats.top_k_by_ref.iter().map(to_top_k_entry).collect(),
            top_k_by_size: stats.top_k_by_size.iter().map(to_top_k_entry).collect(),
        }
    }
}

/// `TS._DEBUG STRINGPOOLSTATS` across the cluster: one primary per shard reports its pool, and
/// the coordinator sums them into a [`StringPoolSummary`].
#[derive(Default)]
pub struct StringPoolStatsFanoutCommand {
    top_k: usize,
    summary: StringPoolSummary,
}

impl StringPoolStatsFanoutCommand {
    pub fn new(top_k: usize) -> Self {
        Self {
            top_k,
            summary: StringPoolSummary::default(),
        }
    }
}

impl FanoutClientCommand for StringPoolStatsFanoutCommand {
    type Request = StringPoolStatsRequest;
    type Response = StringPoolStatsResponse;

    fn name() -> &'static str {
        "cmd::string_pool_stats"
    }

    /// Reads the pool without the GIL: it is lock-free and holds no keys.
    fn get_local_response(
        _ctx: &FanoutContext,
        req: StringPoolStatsRequest,
    ) -> ValkeyResult<StringPoolStatsResponse> {
        // Each node gates its own internals, as it would for a direct TS._DEBUG call.
        if !is_debug_mode_enabled() {
            return Err(ValkeyError::Str(error_consts::DEBUG_MODE_DISABLED));
        }
        Ok(InternedString::get_stats_with_top_k(req.top_k as usize).into())
    }

    fn generate_request(&self) -> StringPoolStatsRequest {
        StringPoolStatsRequest {
            top_k: u32::try_from(self.top_k).unwrap_or(u32::MAX),
        }
    }

    /// Replicas hold a copy of their primary's labels, so including them would count each
    /// shard's pool twice.
    fn get_targets(&self, _ctx: &Context) -> FanoutTarget {
        FanoutTarget::Primary
    }

    fn on_response(&mut self, resp: Self::Response, _target: &NodeInfo) -> FanoutCommandResult {
        self.summary.merge(resp);
        Ok(())
    }

    fn reply(&mut self, ctx: &ReplyContext) -> Status {
        self.summary.finish_top_k(self.top_k);
        reply_with_string_pool_stats(ctx.context(), &self.summary, self.top_k > 0);
        Status::Ok
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(value: &str, ref_count: u64, bytes: u64) -> StringPoolTopKEntry {
        StringPoolTopKEntry {
            value: value.to_string(),
            ref_count,
            bytes,
            allocated: bytes + 16,
        }
    }

    fn bucket(key: u64, count: u64, bytes: u64, allocated: u64) -> StringPoolBucket {
        StringPoolBucket {
            key,
            count,
            bytes,
            allocated,
        }
    }

    fn node(
        by_ref: Vec<StringPoolTopKEntry>,
        by_size: Vec<StringPoolTopKEntry>,
    ) -> StringPoolStatsResponse {
        StringPoolStatsResponse {
            total: Some(bucket(0, 3, 30, 90)),
            by_ref_count: vec![bucket(1, 2, 20, 60), bucket(4, 1, 10, 30)],
            by_size: vec![bucket(10, 3, 30, 90)],
            memory_saved_bytes: 90,
            holder_count: 6,
            holder_slot_bytes: 48,
            top_k_by_ref: by_ref,
            top_k_by_size: by_size,
        }
    }

    #[test]
    fn merge_sums_buckets_by_key() {
        let mut summary = StringPoolSummary::default();
        summary.merge(node(vec![], vec![]));
        let mut other = node(vec![], vec![]);
        other.by_ref_count = vec![bucket(1, 1, 5, 20), bucket(2, 1, 5, 20)];
        summary.merge(other);
        summary.finish_top_k(0);

        assert_eq!(summary.total.count, 6);
        assert_eq!(summary.total.bytes, 60);
        assert_eq!(summary.total.allocated, 180);
        assert_eq!(summary.memory_saved_bytes, 180);
        assert_eq!(summary.holder_count, 12);
        assert_eq!(summary.holder_slot_bytes, 96);
        assert_eq!(summary.total_storage_bytes(), 276);

        let by_ref: Vec<_> = summary
            .by_ref_count
            .iter()
            .map(|(&k, b)| (k, b.count, b.bytes, b.allocated))
            .collect();
        assert_eq!(by_ref, vec![(1, 3, 25, 80), (2, 1, 5, 20), (4, 1, 10, 30)]);
        assert_eq!(summary.by_size.len(), 1);
        assert_eq!(summary.by_size[&10].count, 6);
        assert!(summary.top_k_by_ref.is_empty());
        assert!(summary.top_k_by_size.is_empty());
    }

    #[test]
    fn top_k_merges_the_same_string_across_nodes() {
        let mut summary = StringPoolSummary::default();
        summary.merge(node(
            vec![entry("job=api", 5, 7), entry("env=prod", 4, 8)],
            vec![entry("instance=host-1", 1, 15), entry("env=prod", 4, 8)],
        ));
        summary.merge(node(
            vec![entry("env=prod", 3, 8), entry("job=db", 2, 6)],
            vec![entry("instance=host-22", 1, 16), entry("env=prod", 3, 8)],
        ));
        summary.finish_top_k(2);

        let by_ref: Vec<_> = summary
            .top_k_by_ref
            .iter()
            .map(|e| (e.value.as_str(), e.ref_count, e.allocated))
            .collect();
        assert_eq!(by_ref, vec![("env=prod", 7, 48), ("job=api", 5, 23)]);

        let by_size: Vec<_> = summary
            .top_k_by_size
            .iter()
            .map(|e| (e.value.as_str(), e.bytes))
            .collect();
        assert_eq!(
            by_size,
            vec![("instance=host-22", 16), ("instance=host-1", 15)]
        );
    }

    #[test]
    fn top_k_ties_break_by_value() {
        let mut summary = StringPoolSummary::default();
        summary.merge(node(
            vec![entry("b", 3, 1), entry("a", 3, 1), entry("c", 3, 1)],
            vec![entry("y", 1, 4), entry("x", 1, 4)],
        ));
        summary.finish_top_k(2);

        let by_ref: Vec<_> = summary
            .top_k_by_ref
            .iter()
            .map(|e| e.value.as_str())
            .collect();
        assert_eq!(by_ref, vec!["a", "b"]);
        let by_size: Vec<_> = summary
            .top_k_by_size
            .iter()
            .map(|e| e.value.as_str())
            .collect();
        assert_eq!(by_size, vec!["x", "y"]);
    }
}
