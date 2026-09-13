use crate::fanout::FanoutContext;
use crate::fanout::{FanoutClientCommand, FanoutResult};
use std::ffi::c_void;
use std::os::raw::c_int;
use valkey_module::{
    Context, Status, ValkeyError, ValkeyModule_BlockClient,
    ValkeyModule_BlockedClientMeasureTimeEnd, ValkeyModule_BlockedClientMeasureTimeStart,
    ValkeyModule_GetBlockedClientPrivateData, ValkeyModule_UnblockClient, ValkeyModuleCtx,
    ValkeyModuleString, raw,
};

const NO_TIMEOUT: i64 = 60000; // 60 seconds

#[repr(C)]
pub(super) struct BlockedClientPrivateData<OP>
where
    OP: FanoutClientCommand,
{
    op: OP,
    result: FanoutResult,
}

impl<OP> BlockedClientPrivateData<OP>
where
    OP: FanoutClientCommand,
{
    pub(super) fn new(operation: OP, result: FanoutResult) -> Self {
        Self {
            op: operation,
            result,
        }
    }
    fn reply(&mut self, ctx: &FanoutContext) -> Status {
        match self.result.as_ref() {
            Ok(_) => self.op.reply(ctx),
            Err(err) => {
                let _err: ValkeyError = err.into();
                // Forward the error using the fanout reply helpers
                ctx.reply(Err(_err))
            }
        }
    }
}

/// High-level wrapper for a blocked client.
pub(super) struct FanoutBlockedClient<T: FanoutClientCommand> {
    inner: *mut raw::ValkeyModuleBlockedClient,
    data: Option<Box<BlockedClientPrivateData<T>>>,
    time_measurement_ongoing: bool,
    is_blocked: bool,
}

// SAFETY: `inner` is a raw pointer to a `ValkeyModuleBlockedClient`, which is safe to hand off
// to another thread (that is the whole point of blocking a client: another thread eventually
// calls `ValkeyModule_UnblockClient` on it). The `T: Send` bound makes the rest of the struct's
// soundness argument self-contained here: `data: Option<Box<BlockedClientPrivateData<T>>>` owns
// a `T`, so sending `Self` across threads also sends that `T`. `FanoutClientCommand` already
// requires `Send` as a supertrait, but that bound lives in a different file — restating it here
// means this impl stays sound even if that supertrait bound is ever relaxed.
unsafe impl<T: FanoutClientCommand + Send> Send for FanoutBlockedClient<T> {}

impl<T> FanoutBlockedClient<T>
where
    T: FanoutClientCommand,
{
    pub fn new(ctx: &Context) -> Self {
        let bc_ptr = unsafe {
            ValkeyModule_BlockClient.unwrap()(
                ctx.ctx as *mut ValkeyModuleCtx,
                Some(reply_callback::<T>),
                None,
                // NOTE: We do not need a free callback because we handle freeing the data in the
                // reply callback. if FanoutOperation is made to not require non 'static, we need to
                // provide a free callback here to avoid memory leaks.
                None, // Some(free_callback::<T>),
                NO_TIMEOUT,
            )
        };

        let mut res = Self {
            inner: bc_ptr,
            time_measurement_ongoing: false,
            data: None,
            is_blocked: true,
        };

        res.measure_time_start();
        res
    }

    pub(super) fn set_private_data(&mut self, op: T, result: FanoutResult) {
        let private_data = Box::new(BlockedClientPrivateData::new(op, result));
        self.data = Some(private_data);
    }

    fn unblock(&mut self) {
        if !self.is_blocked {
            return;
        }
        self.is_blocked = false;

        // Ensure any ongoing measurement is ended.
        self.measure_time_end();

        // Take private_data for local use.
        let private_data_ptr = self.data.take().map_or(std::ptr::null_mut(), |boxed| {
            Box::into_raw(boxed) as *mut c_void
        });

        // Call out to the C API to actually unblock.
        unsafe {
            ValkeyModule_UnblockClient.unwrap()(self.inner, private_data_ptr);
        }
    }

    /// Start measuring time for a blocked client.
    pub fn measure_time_start(&mut self) {
        if self.time_measurement_ongoing {
            return;
        }
        unsafe { ValkeyModule_BlockedClientMeasureTimeStart.unwrap()(self.inner) };
        self.time_measurement_ongoing = true;
    }

    /// End measuring time for a blocked client.
    pub fn measure_time_end(&mut self) {
        if !self.time_measurement_ongoing {
            return;
        }
        unsafe { ValkeyModule_BlockedClientMeasureTimeEnd.unwrap()(self.inner) };
        self.time_measurement_ongoing = false;
    }
}

impl<T> Drop for FanoutBlockedClient<T>
where
    T: FanoutClientCommand,
{
    fn drop(&mut self) {
        self.unblock();
    }
}

fn take_data<T>(data: *mut c_void) -> T {
    // Cast the *mut c_void supplied by the Valkey API to a raw pointer of our custom type.
    let data = data.cast::<T>();

    // Take back ownership of the original boxed data, so we can unbox it safely.
    // If we don't do this, the data's memory will be leaked.
    let data = unsafe { Box::from_raw(data) };

    *data
}

extern "C" fn reply_callback<T: FanoutClientCommand>(
    ctx: *mut ValkeyModuleCtx,
    _argv: *mut *mut ValkeyModuleString,
    _argc: c_int,
) -> c_int {
    let op_ptr = unsafe { ValkeyModule_GetBlockedClientPrivateData.unwrap()(ctx) };
    let ctx = FanoutContext::new(ctx as *mut raw::RedisModuleCtx);
    if op_ptr.is_null() {
        // this means that there was an error in setting up RPC, so we should reply with an error.
        ctx.reply_error_string("No reply data") as c_int
    } else {
        // Cast to the correct type and then dereference once to get &mut ResponseContext<T>
        let mut response_ctx: BlockedClientPrivateData<T> = take_data(op_ptr);
        response_ctx.reply(&ctx) as c_int
    }
}
