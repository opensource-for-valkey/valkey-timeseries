use super::fanout_command::FanoutCommand;
use super::fanout_context::FanoutContext;
use super::serialization::{Deserialized, Serializable, Serialized};
use crate::fanout::{FanoutError, FanoutResult};
use ahash::RandomState;
use std::sync::LazyLock;
use valkey_module::{ValkeyError, ValkeyResult};

/// Type-erased function pointer for executing a fanout operation.
/// This allows us to store different fanout operations with different
/// Request/Response types in the same registry.
///
/// Called with the GIL *not* held; request decoding and response encoding run
/// outside it, and the operation takes it only for its keyspace/index work.
pub(super) type RequestHandlerCallback = fn(&FanoutContext, &[u8], &mut Vec<u8>) -> FanoutResult;

/// A registry for fanout operations that allows type-erased storage and retrieval
/// of [`FanoutCommand`] implementations.
pub struct FanoutOperationRegistry {
    operations: papaya::HashMap<&'static str, RequestHandlerCallback, RandomState>,
}

impl FanoutOperationRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            operations: papaya::HashMap::with_hasher(RandomState::new()),
        }
    }

    /// Register a fanout operation by name.
    ///
    /// # Type Parameters
    /// - `OP`: The operation type implementing FanoutOperation
    pub fn register<OP>(&self) -> ValkeyResult<()>
    where
        OP: FanoutCommand,
        OP::Request: Serializable,
        OP::Response: Serializable,
    {
        let name = OP::name();

        fn request_handler<OP>(
            ctx: &FanoutContext,
            req_buf: &[u8],
            dest: &mut Vec<u8>,
        ) -> FanoutResult
        where
            OP: FanoutCommand,
            OP::Request: Serializable,
            OP::Response: Serializable,
        {
            match OP::Request::deserialize(req_buf) {
                Ok(request) => {
                    if let Some(budget) = OP::time_budget(&request) {
                        // Counted from arrival: a request that waited out its budget in
                        // the queue is answered at once, unrun.
                        ctx.set_deadline(ctx.received() + budget);
                        if ctx.expired() {
                            return Err(FanoutError::timeout());
                        }
                    }
                    let response = OP::get_local_response(ctx, request)?;
                    response.serialize(dest);
                }
                Err(e) => {
                    let msg = format!("Failed to deserialize {} fanout request: {e}", OP::name());
                    return Err(FanoutError::serialization(&msg));
                }
            }
            Ok(())
        }

        let map = self.operations.pin();
        if map.contains_key(name) {
            return Err(ValkeyError::String(format!(
                "Operation '{name}' is already registered"
            )));
        }

        map.insert(name, request_handler::<OP>);

        Ok(())
    }

    #[inline]
    fn get_operation_by_name(&self, name: &str) -> Option<RequestHandlerCallback> {
        self.operations.pin().get(name).copied()
    }
}

static FANOUT_REGISTRY: LazyLock<FanoutOperationRegistry> =
    LazyLock::new(FanoutOperationRegistry::new);

/// Register a fanout operation.
///
/// # Type Parameters
/// - `OP`: The operation type implementing FanoutOperation
pub fn register_fanout_operation<OP>() -> ValkeyResult<()>
where
    OP: FanoutCommand,
    OP::Request: Serializable,
    OP::Response: Serializable,
{
    FANOUT_REGISTRY.register::<OP>()
}

pub(super) fn get_fanout_request_handler(name: &str) -> Option<RequestHandlerCallback> {
    FANOUT_REGISTRY.get_operation_by_name(name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fanout::{ErrorKind, FanoutCommandResult, NodeInfo};
    use crate::promql::generated::LabelProfileQuery;
    use std::time::{Duration, Instant};

    /// A command whose shard-side read must never run: it panics if it does.
    #[derive(Default)]
    struct NeverRuns;

    impl FanoutCommand for NeverRuns {
        type Request = LabelProfileQuery;
        type Response = LabelProfileQuery;

        fn name() -> &'static str {
            "test-never-runs"
        }

        fn get_local_response(
            _ctx: &FanoutContext,
            _req: LabelProfileQuery,
        ) -> ValkeyResult<LabelProfileQuery> {
            panic!("a request past its time budget was run");
        }

        fn time_budget(req: &LabelProfileQuery) -> Option<Duration> {
            (req.time_budget_ms > 0).then(|| Duration::from_millis(req.time_budget_ms))
        }

        fn generate_request(&self) -> LabelProfileQuery {
            LabelProfileQuery::default()
        }

        fn on_response(&mut self, _: LabelProfileQuery, _: &NodeInfo) -> FanoutCommandResult {
            Ok(())
        }
    }

    /// A request that waited in the lane's queue past its time budget is answered with a
    /// timeout and never run: its requester has already answered without it. The budget
    /// counts from arrival, not from when a worker picked the request up.
    #[test]
    fn a_request_that_outwaited_its_budget_in_the_queue_is_not_run() {
        let registry = FanoutOperationRegistry::new();
        registry.register::<NeverRuns>().unwrap();
        let handler = registry.get_operation_by_name(NeverRuns::name()).unwrap();

        let mut request = Vec::new();
        LabelProfileQuery {
            time_budget_ms: 5,
            ..Default::default()
        }
        .serialize(&mut request);
        let ctx =
            FanoutContext::new(None, 0).received_at(Instant::now() - Duration::from_millis(50));

        let err = handler(&ctx, &request, &mut Vec::new()).expect_err("refused unrun");
        assert_eq!(err.kind, ErrorKind::Timeout);
        assert!(ctx.expired(), "the deadline is recorded for the read too");
    }
}
