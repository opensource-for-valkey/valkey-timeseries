use super::fanout_codec::filters::{deserialize_matchers_list, serialize_matchers_list};
use super::fanout_codec::{QueryLabelsRequest, QueryLabelsSubtype, StringListResponse};
use crate::commands::command_parser::QueryLabelsOptions;
use crate::fanout::{FanoutClientCommand, FanoutCommandResult, FanoutContext, NodeInfo};
use crate::series::index::query_labels_distinct;
use std::collections::BTreeSet;
use valkey_module::{Context, Status, ValkeyError, ValkeyResult};

/// Cluster-wide `TS.QUERYLABELS`.
///
/// Each shard computes the distinct names/values over its local (ACL-filtered) series and
/// ships them as a plain string list; the coordinator deduplicates the shard results into
/// one set and replies with an array (RESP2) or set (RESP3). Duplicates across shards are
/// inevitable (the same label/value pair appears on every shard that holds a matching
/// series), so dedup happens here, not on the shards.
#[derive(Default)]
pub struct QueryLabelsFanoutCommand {
    options: QueryLabelsOptions,
    values: BTreeSet<String>,
}

impl QueryLabelsFanoutCommand {
    pub fn new(options: QueryLabelsOptions) -> Self {
        Self {
            options,
            values: BTreeSet::new(),
        }
    }
}

impl FanoutClientCommand for QueryLabelsFanoutCommand {
    type Request = QueryLabelsRequest;
    type Response = StringListResponse;

    fn name() -> &'static str {
        "querylabels"
    }

    fn get_local_response(
        ctx: &Context,
        req: QueryLabelsRequest,
    ) -> ValkeyResult<StringListResponse> {
        let subtype = QueryLabelsSubtype::try_from(req.subtype).map_err(|_| invalid_subtype())?;
        if subtype == QueryLabelsSubtype::Unspecified {
            return Err(invalid_subtype());
        }
        let matchers = deserialize_matchers_list(Some(req.filters))?;
        let label = if req.label.is_empty() {
            None
        } else {
            Some(req.label)
        };

        let values = match subtype {
            QueryLabelsSubtype::Labels => query_labels_distinct(ctx, &matchers, None)?,
            QueryLabelsSubtype::Values => query_labels_distinct(ctx, &matchers, label.as_deref())?,
            QueryLabelsSubtype::Unspecified => return Err(invalid_subtype()),
        };

        Ok(StringListResponse {
            values: values.into_iter().collect(),
        })
    }

    fn generate_request(&self) -> QueryLabelsRequest {
        let subtype = if self.options.label.is_none() {
            QueryLabelsSubtype::Labels
        } else {
            QueryLabelsSubtype::Values
        };
        QueryLabelsRequest {
            subtype: subtype.into(),
            label: self.options.label.clone().unwrap_or_default(),
            filters: serialize_matchers_list(&self.options.matchers)
                .expect("serialize matchers list"),
        }
    }

    fn on_response(&mut self, resp: Self::Response, _target: &NodeInfo) -> FanoutCommandResult {
        for value in resp.values {
            self.values.insert(value);
        }
        Ok(())
    }

    fn reply(&mut self, ctx: &FanoutContext) -> Status {
        let values: Vec<String> = std::mem::take(&mut self.values).into_iter().collect();
        ctx.reply_with_set(values.len());
        for value in values.iter() {
            ctx.reply_with_string(value);
        }
        Status::Ok
    }
}

fn invalid_subtype() -> ValkeyError {
    ValkeyError::Str("TSDB: invalid query labels subtype on the wire — protocol error")
}
