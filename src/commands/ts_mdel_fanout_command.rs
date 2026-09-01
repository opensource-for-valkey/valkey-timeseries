use crate::commands::fanout_codec::filters::{deserialize_matchers_list, serialize_matchers_list};
use crate::commands::fanout_codec::{CountResponse, DateRange, MDelRequest};
use crate::common::context::is_replica;
use crate::error_consts;
use crate::fanout::{FanoutClientCommand, NodeInfo};
use crate::fanout::{FanoutCommandResult, FanoutContext};
use crate::fanout::{FanoutTargetMode, FanoutTargets, get_fanout_targets};
use crate::labels::filters::SeriesSelector;
use crate::series::{TimestampRange, delete_series_by_selectors};
use valkey_module::{Context, Status, ValkeyError, ValkeyResult, ValkeyValue};

#[derive(Default)]
pub struct MDelFanoutCommand {
    selectors: Vec<SeriesSelector>,
    date_range: Option<DateRange>,
    total_deleted: usize,
}

impl MDelFanoutCommand {
    pub fn new(selectors: Vec<SeriesSelector>, date_range: Option<TimestampRange>) -> Self {
        let date_range = date_range.map(|dr| {
            let (start, end) = dr.get_timestamps(None);
            DateRange { start, end }
        });
        MDelFanoutCommand {
            selectors,
            date_range,
            total_deleted: 0,
        }
    }
}

impl FanoutClientCommand for MDelFanoutCommand {
    type Request = MDelRequest;
    type Response = CountResponse;

    fn name() -> &'static str {
        "mdel"
    }

    /// Unlike every other fanout command in this module, `TS.MDEL` is a write. The trait default
    /// (`FanoutTargetMode::Random`) picks uniformly among each shard's primary *and* its replicas,
    /// which would delete keys directly on a replica — a write the replica's primary never made,
    /// and one the next full resync silently reverts. A write has exactly one correct target per
    /// shard.
    fn get_targets(&self, ctx: &Context) -> FanoutTargets {
        get_fanout_targets(ctx, FanoutTargetMode::Primary)
    }

    fn get_local_response(ctx: &Context, req: Self::Request) -> ValkeyResult<Self::Response> {
        // Defence in depth behind `get_targets`: the coordinator picked us from *its* cluster map,
        // and a failover between selection and delivery can leave that map naming a node that has
        // since been demoted. Fail this shard's slice loudly rather than diverge the replica.
        if is_replica(ctx) {
            return Err(ValkeyError::Str(error_consts::FANOUT_WRITE_ON_REPLICA));
        }

        let filters = deserialize_matchers_list(Some(req.filters))
            .map_err(|_e| ValkeyError::Str(error_consts::COMMAND_DESERIALIZATION_ERROR))?;

        let range = if let Some(date_range) = req.range {
            let ts = TimestampRange::from_timestamps(date_range.start, date_range.end)?;
            Some(ts)
        } else {
            None
        };

        // `delete_series_by_selectors` propagates its own effects (`DEL` / `TS.DEL`) to this
        // node's replicas. The command itself cannot be propagated from here: this runs in a
        // fanout RPC handler whose context has no client argv for `ReplicateVerbatim` to copy,
        // and a replica replaying `TS.MDEL` would fan the command out a second time.
        let deleted_count = delete_series_by_selectors(ctx, &filters, range)?;
        Ok(CountResponse {
            count: deleted_count as u64,
        })
    }

    fn generate_request(&self) -> Self::Request {
        let filters = serialize_matchers_list(&self.selectors)
            .expect("Failed to serialize selectors for MDelRequest");

        MDelRequest {
            range: self.date_range,
            filters,
        }
    }

    fn on_response(&mut self, resp: Self::Response, _target: &NodeInfo) -> FanoutCommandResult {
        self.total_deleted += resp.count as usize;
        Ok(())
    }

    fn reply(&mut self, ctx: &FanoutContext) -> Status {
        ctx.reply(Ok(ValkeyValue::Integer(self.total_deleted as i64)))
    }
}
