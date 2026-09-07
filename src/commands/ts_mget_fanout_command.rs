use super::fanout_codec::generated::{MGetValue, MultiGetRequest, MultiGetResponse};
use crate::commands::fanout_codec::filters::{deserialize_matchers_list, serialize_matchers_list};
use crate::commands::process_mget_request;
use crate::commands::utils::reply_with_mget_values;
use crate::common::logging::log_error;
use crate::common::replies::ReplyContext;
use crate::error_consts;
use crate::fanout::compute_hash_tag_fanout_target;
use crate::fanout::{FanoutClientCommand, FanoutTarget, NodeInfo};
use crate::fanout::{FanoutCommandResult, FanoutContext};
use crate::series::request_types::MGetRequest;
use valkey_module::{Context, Status, ValkeyError, ValkeyResult};

#[derive(Debug, Default)]
pub struct MGetFanoutCommand {
    options: MGetRequest,
    series: Vec<MGetValue>,
}

impl MGetFanoutCommand {
    pub fn new(options: MGetRequest) -> Self {
        Self {
            options,
            series: Vec::new(),
        }
    }
}

impl FanoutClientCommand for MGetFanoutCommand {
    type Request = MultiGetRequest;
    type Response = MultiGetResponse;

    fn name() -> &'static str {
        "mget"
    }

    fn get_targets(&self, ctx: &Context) -> FanoutTarget {
        compute_hash_tag_fanout_target(ctx, &self.options.tags)
    }

    fn get_local_response(
        ctx: &FanoutContext,
        req: MultiGetRequest,
    ) -> ValkeyResult<MultiGetResponse> {
        let filters = deserialize_matchers_list(Some(req.filters))
            .map_err(|_e| ValkeyError::Str(error_consts::COMMAND_DESERIALIZATION_ERROR))?;

        let mreq = MGetRequest {
            with_labels: req.with_labels,
            filters,
            selected_labels: req.selected_labels,
            latest: req.latest,
            tags: vec![],
        };

        // The per-series results carry the key as a `ValkeyString`, so convert
        // them before releasing the lock.
        let values: Vec<MGetValue> = {
            let ctx = ctx.lock()?;
            let results = process_mget_request(&ctx, mreq)?;
            results.into_iter().map(|resp| resp.into()).collect()
        };

        Ok(MultiGetResponse { values })
    }

    fn generate_request(&self) -> MultiGetRequest {
        let filters =
            serialize_matchers_list(&self.options.filters).expect("serialize matchers list");
        MultiGetRequest {
            with_labels: self.options.with_labels,
            filters,
            selected_labels: self.options.selected_labels.clone(),
            latest: self.options.latest,
        }
    }

    fn on_response(&mut self, resp: Self::Response, _target: &NodeInfo) -> FanoutCommandResult {
        self.series.extend(resp.values);
        Ok(())
    }

    fn reply(&mut self, ctx: &ReplyContext) -> Status {
        match reply_with_mget_values(ctx, &self.series) {
            Ok(_) => Status::Ok,
            Err(e) => {
                log_error(format!("Error processing MGET response: {}", e));
                Status::Err
            }
        }
    }
}
