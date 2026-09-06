use super::fanout_codec::generated::{CountResponse, MetaQueryRequest};
use crate::commands::fanout_codec::{
    deserialize_match_filter_options, serialize_match_filter_options,
};
use crate::commands::utils::get_multi_command_targets;
use crate::fanout::{FanoutClientCommand, FanoutTarget, NodeInfo};
use crate::fanout::{FanoutCommandResult, FanoutContext};
use crate::series::index::count_matched_series;
use crate::series::request_types::MatchFilterOptions;
use valkey_module::{Context, Status, ValkeyResult};

#[derive(Default)]
pub struct CardFanoutCommand {
    options: MatchFilterOptions,
    /// Optional hash tags that scope only the coordinator's fan-out.
    tags: Vec<String>,
    result: usize,
}

impl CardFanoutCommand {
    pub fn new(options: MatchFilterOptions, tags: Vec<String>) -> Self {
        Self {
            options,
            tags,
            result: 0,
        }
    }
}

impl FanoutClientCommand for CardFanoutCommand {
    type Request = MetaQueryRequest;
    type Response = CountResponse;

    fn name() -> &'static str {
        "card"
    }

    fn get_local_response(ctx: &Context, req: MetaQueryRequest) -> ValkeyResult<CountResponse> {
        let options = deserialize_match_filter_options(req.range, Some(req.filters))?;
        let count = count_matched_series(ctx, options.date_range, &options.matchers)? as u64;
        Ok(CountResponse { count })
    }

    fn generate_request(&self) -> MetaQueryRequest {
        let (range, filters) = serialize_match_filter_options(&self.options);
        MetaQueryRequest { range, filters }
    }

    fn get_targets(&self, ctx: &Context) -> FanoutTarget {
        get_multi_command_targets(ctx, &self.tags)
    }

    fn on_response(&mut self, resp: Self::Response, _target: &NodeInfo) -> FanoutCommandResult {
        self.result += resp.count as usize;
        Ok(())
    }

    fn reply(&mut self, ctx: &FanoutContext) -> Status {
        ctx.reply_with_integer(self.result as i64)
    }
}
