use super::fanout_codec::{KeyListResponse, MetaQueryRequest};
use super::fanout_codec::{deserialize_match_filter_options, serialize_match_filter_options};
use super::utils::get_multi_command_targets;
use crate::common::replies::{ReplyContext, reply_with_slice};
use crate::fanout::{FanoutClientCommand, FanoutTarget, NodeInfo};
use crate::fanout::{FanoutCommandResult, FanoutContext};
use crate::series::index::series_keys_by_selectors;
use crate::series::request_types::MatchFilterOptions;
use std::collections::BTreeSet;
use valkey_module::{Context, Status, ValkeyResult};

#[derive(Clone, Debug, Default)]
pub struct QueryIndexFanoutCommand {
    options: MatchFilterOptions,
    /// Optional hash tags that scope only the coordinator's fan-out.
    tags: Vec<String>,
    keys: BTreeSet<Vec<u8>>,
}

impl QueryIndexFanoutCommand {
    pub fn new(options: MatchFilterOptions, tags: Vec<String>) -> Self {
        Self {
            options,
            tags,
            keys: BTreeSet::new(),
        }
    }
}

impl FanoutClientCommand for QueryIndexFanoutCommand {
    type Request = MetaQueryRequest;
    type Response = KeyListResponse;

    fn name() -> &'static str {
        "index_query"
    }

    fn get_local_response(
        ctx: &FanoutContext,
        req: MetaQueryRequest,
    ) -> ValkeyResult<KeyListResponse> {
        let options = deserialize_match_filter_options(req.range, Some(req.filters))?;
        // The protobuf message owns each key separately.
        let keys = series_keys_by_selectors(ctx, &options.matchers, options.date_range)?;
        Ok(KeyListResponse {
            keys: keys.into_vecs(),
        })
    }

    fn generate_request(&self) -> MetaQueryRequest {
        let (range, filters) = serialize_match_filter_options(&self.options);
        MetaQueryRequest { range, filters }
    }

    fn get_targets(&self, ctx: &Context) -> FanoutTarget {
        get_multi_command_targets(ctx, &self.tags)
    }

    fn on_response(&mut self, resp: Self::Response, _target: &NodeInfo) -> FanoutCommandResult {
        self.keys.extend(resp.keys);
        Ok(())
    }

    fn reply(&mut self, ctx: &ReplyContext) -> Status {
        ctx.reply_with_array(self.keys.len());
        for key in self.keys.iter() {
            reply_with_slice(ctx, key);
        }
        Status::Ok
    }
}
