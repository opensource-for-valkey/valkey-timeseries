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

/// A registered operation, as a receiving node runs it.
#[derive(Clone, Copy)]
pub(super) struct RegisteredOperation {
    pub run: RequestHandlerCallback,
    /// [`FanoutCommand::cancellable`]: whether a request past its time budget may be
    /// dropped unrun, or stopped between read batches.
    pub cancellable: bool,
    /// [`FanoutCommand::response_features`]: `required_features` on this operation's replies.
    pub response_features: u64,
}

/// A registry for fanout operations that allows type-erased storage and retrieval
/// of [`FanoutCommand`] implementations.
pub struct FanoutOperationRegistry {
    operations: papaya::HashMap<&'static str, RegisteredOperation, RandomState>,
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

        map.insert(
            name,
            RegisteredOperation {
                run: request_handler::<OP>,
                cancellable: OP::cancellable(),
                response_features: OP::response_features(),
            },
        );

        Ok(())
    }

    #[inline]
    fn get_operation_by_name(&self, name: &str) -> Option<RegisteredOperation> {
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

pub(super) fn get_fanout_request_handler(name: &str) -> Option<RegisteredOperation> {
    FANOUT_REGISTRY.get_operation_by_name(name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::replies::ReplyContext;
    use crate::fanout::{FanoutClientCommand, FanoutCommandResult, NodeInfo};
    use valkey_module::Status;

    #[derive(Default)]
    struct DemandsFeatures;

    impl FanoutClientCommand for DemandsFeatures {
        type Request = ();
        type Response = ();

        fn name() -> &'static str {
            "demands_features"
        }

        fn response_features() -> u64 {
            0b101
        }

        fn get_local_response(_: &FanoutContext, _: ()) -> ValkeyResult<()> {
            Ok(())
        }

        fn generate_request(&self) {}

        fn on_response(&mut self, _: (), _: &NodeInfo) -> FanoutCommandResult {
            Ok(())
        }

        fn reply(&mut self, _: &ReplyContext) -> Status {
            Status::Ok
        }
    }

    /// The bits an operation demands of requesters reach its registration, which
    /// is what the receiving node puts on each reply's `required_features`.
    #[test]
    fn registration_records_the_response_features() {
        let registry = FanoutOperationRegistry::new();
        registry.register::<DemandsFeatures>().unwrap();
        let op = registry.get_operation_by_name("demands_features").unwrap();
        assert_eq!(op.response_features, 0b101);
    }
}
