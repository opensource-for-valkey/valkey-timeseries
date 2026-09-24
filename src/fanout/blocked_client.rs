use crate::common::replies::ReplyContext;
use crate::fanout::fanout_error::TIMEOUT_ERROR;
use crate::fanout::{FanoutClientCommand, FanoutResult};
use std::ffi::c_void;
use std::os::raw::c_int;
use std::time::Duration;
use valkey_module::{
    Context, Status, ValkeyError, ValkeyModule_BlockClient,
    ValkeyModule_BlockedClientMeasureTimeEnd, ValkeyModule_BlockedClientMeasureTimeStart,
    ValkeyModule_GetBlockedClientPrivateData, ValkeyModule_UnblockClient, ValkeyModuleCtx,
    ValkeyModuleString, raw,
};

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
    fn reply(&mut self, ctx: &ReplyContext) -> Status {
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
    /// Blocks the client for up to `timeout` — the fan-out's deadline. The RPC timer bounds the
    /// remote shards, but nothing else bounds the local share, so this is the deadline for the
    /// whole operation: when it passes the client gets the fan-out timeout error (see
    /// [`timeout_callback`]) instead of being released with no reply, as it was under a fixed
    /// 60 s block with no timeout callback.
    ///
    /// The server owns the private data from `UnblockClient` on: [`reply_callback`] only borrows
    /// it and [`free_callback`] drops it. A client that timed out is detached before the
    /// operation finishes, so its reply callback never runs; with the reply callback doing the
    /// freeing, as it used to, that result (an MRANGE's samples, say) leaked.
    pub fn new(ctx: &Context, timeout: Duration) -> Self {
        // 0 means "never" to the server.
        let timeout_ms = (timeout.as_millis() as i64).max(1);
        let bc_ptr = unsafe {
            ValkeyModule_BlockClient.unwrap()(
                ctx.ctx as *mut ValkeyModuleCtx,
                Some(reply_callback::<T>),
                Some(timeout_callback),
                Some(free_callback::<T>),
                timeout_ms,
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

extern "C" fn reply_callback<T: FanoutClientCommand>(
    ctx: *mut ValkeyModuleCtx,
    _argv: *mut *mut ValkeyModuleString,
    _argc: c_int,
) -> c_int {
    let op_ptr = unsafe { ValkeyModule_GetBlockedClientPrivateData.unwrap()(ctx) };
    let ctx = ReplyContext::new(ctx as *mut raw::RedisModuleCtx);
    if op_ptr.is_null() {
        // this means that there was an error in setting up RPC, so we should reply with an error.
        ctx.reply_error_string("No reply data") as c_int
    } else {
        // Borrowed: `free_callback` drops the data once this returns.
        // SAFETY: `unblock` stored a `Box<BlockedClientPrivateData<T>>` for this client, and the
        // server hands it back unchanged, on the main thread, before calling `free_callback`.
        let response_ctx = unsafe { &mut *op_ptr.cast::<BlockedClientPrivateData<T>>() };
        response_ctx.reply(&ctx) as c_int
    }
}

/// The fan-out deadline passed with the operation still running: reply with the same error
/// the RPC timeout path sends. The operation's eventual result is discarded by `free_callback`.
extern "C" fn timeout_callback(
    ctx: *mut ValkeyModuleCtx,
    _argv: *mut *mut ValkeyModuleString,
    _argc: c_int,
) -> c_int {
    let ctx = ReplyContext::new(ctx as *mut raw::RedisModuleCtx);
    ctx.reply_error_string(TIMEOUT_ERROR) as c_int
}

/// Drops the private data `unblock` handed to the server — after `reply_callback`, or in its
/// place when the client has timed out or disconnected.
extern "C" fn free_callback<T: FanoutClientCommand>(
    _ctx: *mut ValkeyModuleCtx,
    privdata: *mut c_void,
) {
    if privdata.is_null() {
        return;
    }
    // SAFETY: created by `Box::into_raw` in `unblock`, and freed only here.
    drop(unsafe { Box::from_raw(privdata.cast::<BlockedClientPrivateData<T>>()) });
}
