use super::ts_debug::reply_with_index_memory;
use crate::commands::fanout_codec::generated::{IndexMemoryRequest, IndexMemoryResponse};
use crate::common::replies::ReplyContext;
use crate::config::is_debug_mode_enabled;
use crate::error_consts;
use crate::fanout::{
    FanoutClientCommand, FanoutCommandResult, FanoutContext, FanoutTarget, NodeInfo,
};
use crate::series::index::{IndexMemory, db_index_memory_usage, index_memory_usage};
use valkey_module::{Context, Status, ValkeyError, ValkeyResult};

impl From<IndexMemory> for IndexMemoryResponse {
    fn from(memory: IndexMemory) -> Self {
        Self {
            db_count: memory.db_count as u64,
            term_count: memory.term_count as u64,
            series_count: memory.series_count as u64,
            terms_bytes: memory.terms_bytes as u64,
            postings_bytes: memory.postings_bytes as u64,
            id_to_key_bytes: memory.id_to_key_bytes as u64,
            bookkeeping_bytes: memory.bookkeeping_bytes as u64,
        }
    }
}

impl From<IndexMemoryResponse> for IndexMemory {
    fn from(resp: IndexMemoryResponse) -> Self {
        Self {
            db_count: resp.db_count as usize,
            term_count: resp.term_count as usize,
            series_count: resp.series_count as usize,
            terms_bytes: resp.terms_bytes as usize,
            postings_bytes: resp.postings_bytes as usize,
            id_to_key_bytes: resp.id_to_key_bytes as usize,
            bookkeeping_bytes: resp.bookkeeping_bytes as usize,
        }
    }
}

/// This node's label index footprint: `db`'s alone, or every database's summed if `all_dbs`.
pub(super) fn local_index_memory(db: i32, all_dbs: bool) -> IndexMemory {
    if all_dbs {
        index_memory_usage()
    } else {
        db_index_memory_usage(db)
    }
}

/// `TS._DEBUG INDEXMEMORY` across the cluster: one node per shard reports its label index's
/// footprint, and the coordinator sums them. Each node measures the caller's database, which
/// travels in the request header, unless `all_dbs` asks for every database.
///
/// A replica builds its own index from the replication stream, so it holds the same terms and
/// series as its primary and either one answers for the shard. A replica is preferred: walking
/// the term dictionary holds each database's postings read lock for the length of the walk,
/// which stalls that node's index writers, and a replica takes no client writes. The figures
/// can trail the primary by the replication lag, and the stale-id tombstones each node sweeps
/// on its own schedule, so `bookkeeping_bytes` may differ slightly from the primary's.
///
/// Every field is a sum over shards, so `db_count` counts (shard, database) pairs.
#[derive(Default)]
pub struct IndexMemoryFanoutCommand {
    all_dbs: bool,
    memory: IndexMemory,
    nodes: usize,
}

impl IndexMemoryFanoutCommand {
    pub fn new(all_dbs: bool) -> Self {
        Self {
            all_dbs,
            ..Default::default()
        }
    }
}

impl FanoutClientCommand for IndexMemoryFanoutCommand {
    type Request = IndexMemoryRequest;
    type Response = IndexMemoryResponse;

    fn name() -> &'static str {
        "cmd::index_memory"
    }

    /// Walks the index without the GIL: it takes only the postings read locks, which rank
    /// below the GIL, and holds no keys.
    fn get_local_response(
        ctx: &FanoutContext,
        req: IndexMemoryRequest,
    ) -> ValkeyResult<IndexMemoryResponse> {
        // Each node gates its own internals, as it would for a direct TS._DEBUG call.
        if !is_debug_mode_enabled() {
            return Err(ValkeyError::Str(error_consts::DEBUG_MODE_DISABLED));
        }
        Ok(local_index_memory(ctx.db(), req.all_dbs).into())
    }

    fn generate_request(&self) -> IndexMemoryRequest {
        IndexMemoryRequest {
            all_dbs: self.all_dbs,
        }
    }

    fn get_targets(&self, _ctx: &Context) -> FanoutTarget {
        FanoutTarget::ReplicaPerShard
    }

    fn on_response(&mut self, resp: Self::Response, _target: &NodeInfo) -> FanoutCommandResult {
        self.memory.merge(resp.into());
        self.nodes += 1;
        Ok(())
    }

    fn reply(&mut self, ctx: &ReplyContext) -> Status {
        reply_with_index_memory(ctx.context(), &self.memory, self.nodes);
        Status::Ok
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(db_count: usize, terms_bytes: usize, postings_bytes: usize) -> IndexMemory {
        IndexMemory {
            db_count,
            term_count: 10,
            series_count: 4,
            terms_bytes,
            postings_bytes,
            id_to_key_bytes: 100,
            bookkeeping_bytes: 32,
        }
    }

    #[test]
    fn response_round_trips() {
        let memory = node(2, 300, 400);
        let resp: IndexMemoryResponse = memory.into();
        assert_eq!(IndexMemory::from(resp), memory);
    }

    #[test]
    fn responses_sum_field_by_field() {
        let mut memory = IndexMemory::default();
        for shard in [node(1, 300, 400), node(2, 50, 60)] {
            memory.merge(IndexMemoryResponse::from(shard).into());
        }

        assert_eq!(
            memory,
            IndexMemory {
                db_count: 3,
                term_count: 20,
                series_count: 8,
                terms_bytes: 350,
                postings_bytes: 460,
                id_to_key_bytes: 200,
                bookkeeping_bytes: 64,
            }
        );
        assert_eq!(memory.total_bytes(), 350 + 460 + 200 + 64);
    }
}
