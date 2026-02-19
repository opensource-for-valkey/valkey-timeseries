use std::os::raw::{c_int, c_void};
use valkey_module::{Context, ValkeyError, ValkeyString, raw};

// Callback types for handling blocked client operations
// Currently supports authentication reply callback for block_client_on_auth
#[derive(Debug)]
pub enum ReplyCallback<T> {
    Auth(fn(&Context, ValkeyString, ValkeyString, Option<&T>) -> Result<c_int, ValkeyError>),
}

#[derive(Debug)]
struct BlockedClientPrivateData<T: 'static> {
    reply_callback: Option<ReplyCallback<T>>,
    free_callback: Option<FreePrivateDataCallback<T>>,
    data: Option<Box<T>>,
}

// Callback type for freeing private data associated with a blocked client
type FreePrivateDataCallback<T> = fn(&Context, T);

pub struct BlockedClient<T: 'static = ()> {
    pub(crate) inner: *mut raw::RedisModuleBlockedClient,
    reply_callback: Option<ReplyCallback<T>>,
    free_callback: Option<FreePrivateDataCallback<T>>,
    data: Option<Box<T>>,
}

#[allow(dead_code)]
unsafe extern "C" fn free_callback_wrapper<T: 'static>(
    ctx: *mut raw::RedisModuleCtx,
    module_private_data: *mut c_void,
) {
    let context = Context::new(ctx);

    if module_private_data.is_null() {
        panic!("[free_callback_wrapper] Module private data is null; this should not happen!");
    }

    let user_private_data =
        unsafe { Box::from_raw(module_private_data as *mut BlockedClientPrivateData<T>) };

    // Execute free_callback only if both callback and data exist
    // Note: free_callback can exist without data - this is a valid state
    if let Some(free_cb) = user_private_data.free_callback
        && let Some(data) = user_private_data.data
    {
        free_cb(&context, *data);
    }
}

// We need to be able to send the inner pointer to another thread
unsafe impl<T> Send for BlockedClient<T> {}

impl<T> BlockedClient<T> {
    pub(crate) fn new(inner: *mut raw::RedisModuleBlockedClient) -> Self {
        Self {
            inner,
            reply_callback: None,
            free_callback: None,
            data: None,
        }
    }

    /// Sets private data for the blocked client.
    ///
    /// # Arguments
    /// * `data` - The private data to store
    ///
    /// # Returns
    /// * `Ok(())` - If the private data was successfully set
    /// * `Err(ValkeyError)` - If setting the private data failed (e.g., no free callback)
    pub fn set_blocked_private_data(&mut self, data: T) -> Result<(), ValkeyError> {
        if self.free_callback.is_none() {
            return Err(ValkeyError::Str(
                "Cannot set private data without a free callback - this would leak memory",
            ));
        }
        self.data = Some(Box::new(data));
        Ok(())
    }

    /// Aborts the blocked client operation
    ///
    /// # Returns
    /// * `Ok(())` - If the blocked client was successfully aborted
    /// * `Err(ValkeyError)` - If the abort operation failed
    pub fn abort(mut self) -> Result<(), ValkeyError> {
        unsafe {
            // Clear references to data and callbacks
            self.data = None;
            self.reply_callback = None;
            self.free_callback = None;

            if raw::RedisModule_AbortBlock.unwrap()(self.inner) == raw::REDISMODULE_OK as c_int {
                // Prevent the normal Drop from running
                self.inner = std::ptr::null_mut();
                Ok(())
            } else {
                Err(ValkeyError::Str("Failed to abort blocked client"))
            }
        }
    }
}

impl<T: 'static> Drop for BlockedClient<T> {
    fn drop(&mut self) {
        if !self.inner.is_null() {
            let callback_data_ptr = if self.free_callback.is_some() {
                Box::into_raw(Box::new(BlockedClientPrivateData {
                    reply_callback: None,
                    free_callback: self.free_callback.take(),
                    data: self.data.take(),
                })) as *mut c_void
            } else {
                std::ptr::null_mut()
            };

            unsafe {
                raw::RedisModule_UnblockClient.unwrap()(self.inner, callback_data_ptr);
            }
        }
    }
}

#[must_use]
pub fn create_blocked_client(ctx: &Context) -> BlockedClient {
    let blocked_client = unsafe {
        raw::RedisModule_BlockClient.unwrap()(
            ctx.ctx, // ctx
            None,    // reply_func
            None,    // timeout_func
            None, 0,
        )
    };

    BlockedClient::new(blocked_client)
}
