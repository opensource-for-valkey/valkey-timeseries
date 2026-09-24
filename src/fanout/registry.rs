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
