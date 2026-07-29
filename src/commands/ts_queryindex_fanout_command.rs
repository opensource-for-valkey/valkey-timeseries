use super::fanout_codec::{MetaQueryRequest, StringListResponse};
use super::fanout_codec::{deserialize_match_filter_options, serialize_match_filter_options};
use crate::fanout::{FanoutClientCommand, NodeInfo};
use crate::fanout::{FanoutCommandResult, FanoutContext};
use crate::series::index::series_keys_by_selectors;
use crate::series::request_types::MatchFilterOptions;
use std::collections::BTreeSet;
use valkey_module::{Context, Status, ValkeyResult};

#[derive(Clone, Debug, Default)]
pub struct QueryIndexFanoutCommand {
    options: MatchFilterOptions,
    keys: BTreeSet<String>,
}

impl QueryIndexFanoutCommand {
    pub fn new(options: MatchFilterOptions) -> Self {
        Self {
            options,
            keys: BTreeSet::new(),
        }
    }
}

impl FanoutClientCommand for QueryIndexFanoutCommand {
    type Request = MetaQueryRequest;
    type Response = StringListResponse;

    fn name() -> &'static str {
        "index_query"
    }

    fn get_local_response(
        ctx: &Context,
        req: MetaQueryRequest,
    ) -> ValkeyResult<StringListResponse> {
        let options = deserialize_match_filter_options(req.range, Some(req.filters))?;
        let keys = series_keys_by_selectors(ctx, &options.matchers, options.date_range)?;
        let keys = keys.into_iter().map(|k| k.to_string()).collect::<Vec<_>>();
        Ok(StringListResponse { values: keys })
    }

    fn generate_request(&self) -> MetaQueryRequest {
        let (range, filters) = serialize_match_filter_options(&self.options);
        MetaQueryRequest { range, filters }
    }

    fn on_response(&mut self, resp: Self::Response, _target: &NodeInfo) -> FanoutCommandResult {
        for key in resp.values {
            self.keys.insert(key);
        }
        Ok(())
    }

    fn reply(&mut self, ctx: &FanoutContext) -> Status {
        ctx.reply_with_array(self.keys.len());
        for key in self.keys.iter() {
            ctx.reply_with_string(key);
        }
        Status::Ok
    }
}
