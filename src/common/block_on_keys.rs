//! Adapter around Valkey's native block-on-keys machinery.
//!
//! `valkey-module` wraps `RedisModule_BlockClient` (see
//! [`crate::common::replies::block_client`], used by `TS.OUTLIERS` to hand work to the thread
//! pool) but has no wrapper for `RedisModule_BlockClientOnKeysWithFlags`. This module supplies
//! the missing piece and confines the unsafe code to one place.
//!
//! The two blocking styles are deliberately *not* unified. A `BlockedClient` from the thread-pool
//! path unblocks when it drops; a block-on-keys handle must outlive the command that created it,
//! so it is never wrapped in a guard with a `Drop` impl.
//!
//! # Safety contract
//!
//! Callbacks cross the FFI boundary, so every entry point here catches panics: unwinding into C
//! is undefined behavior. Private data is a `Box` whose ownership passes to the server at block
//! time and comes back exactly once, through `free_privdata`.

use crate::common::replies::ReplyContext;
use std::os::raw::{c_int, c_longlong, c_void};
use std::panic::{AssertUnwindSafe, catch_unwind};
use valkey_module::{Context, ValkeyString, raw};

/// Status the server expects back from a block-on-keys reply callback: `Ok` means "I have replied,
/// unblock the client", `NotReady` means "leave the client blocked".
///
/// Deliberately not `Result`: neither variant is an error, and mapping them through `?` would
/// invite an accidental early return that leaves the client blocked forever.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadyStatus {
    /// The callback replied. Unblock the client.
    Replied,
    /// Nothing to reply with yet. Keep the client blocked.
    NotReady,
}

impl ReadyStatus {
    const fn as_c_int(self) -> c_int {
        match self {
            // The server treats OK as "served"; ERR as "still blocked".
            ReadyStatus::Replied => raw::REDISMODULE_OK as c_int,
            ReadyStatus::NotReady => raw::REDISMODULE_ERR as c_int,
        }
    }
}

/// Behavior a blocked `TS.READ` client needs from its callbacks, expressed without any FFI in the
/// signatures so the command module stays testable and unsafe-free.
///
/// `Self` is the private state handed to the server. It must be `'static`: the server owns it for
/// as long as the client stays blocked, which outlives the command call.
pub(crate) trait BlockedKeyHandler: Sized + 'static {
    /// Called when one of the blocked-on keys is signaled ready, or when it is deleted (because
    /// we block with `UNBLOCK_DELETED`).
    fn on_ready(&self, ctx: &ReplyContext) -> ReadyStatus;

    /// Called when the block times out, or when the client is released by `CLIENT UNBLOCK`.
    fn on_timeout(&self, ctx: &ReplyContext);
}

/// Load-time presence check for the symbols this module dereferences. Mirrors
/// [`crate::series::index::persistence::check_required_module_apis`]; both are called from
/// `preload` so an unsupported server refuses to load rather than failing on first use.
pub(crate) fn check_blocking_module_apis() -> Result<(), &'static str> {
    if unsafe { raw::RedisModule_BlockClientOnKeysWithFlags }.is_none() {
        return Err("RedisModule_BlockClientOnKeysWithFlags");
    }
    if unsafe { raw::RedisModule_GetBlockedClientPrivateData }.is_none() {
        return Err("RedisModule_GetBlockedClientPrivateData");
    }
    if unsafe { raw::RedisModule_SignalKeyAsReady }.is_none() {
        return Err("RedisModule_SignalKeyAsReady");
    }
    Ok(())
}

/// Mark `key` as having new data available, waking any client blocked on it.
///
/// This is an internal readiness signal, unrelated to client-visible keyspace notifications.
///
/// Only needed when an *existing* key's value grows in place. Installing a new value — including
/// through `RM_ModuleTypeSetValue`, which every series creation in this module goes through —
/// already signals via the server's `dbAdd`. See docs/plans/ts-read-implementation-plan.md §7.
pub(crate) fn signal_timeseries_ready(ctx: &Context, key: &ValkeyString) {
    // Absence is a load-time invariant (`check_blocking_module_apis`), but a missing signal must
    // never take the server down mid-write: the cost of skipping it is a reader that waits for
    // its timeout, which is survivable.
    let Some(signal) = (unsafe { raw::RedisModule_SignalKeyAsReady }) else {
        return;
    };
    unsafe { signal(ctx.ctx, key.inner) };
}

/// Block the current client on `key` until its handler reports readiness, `timeout_ms` elapses, or
/// the key is deleted.
///
/// `timeout_ms` of 0 means "no timeout"; the client then leaves only via a readiness signal, a
/// deletion, `CLIENT UNBLOCK`, or disconnect.
///
/// Returns `false` if the server refused to block the client, in which case `handler` has been
/// dropped and the caller still owes the client a reply.
///
/// # Panics
///
/// Does not panic. Callers must, however, have already ruled out a deny-blocking context:
/// blocking such a client trips a `serverAssert` inside the server and aborts the process. See
/// [`crate::common::context::is_blocking_denied`].
pub(crate) fn block_client_on_key<H: BlockedKeyHandler>(
    ctx: &Context,
    key: &ValkeyString,
    timeout_ms: i64,
    handler: H,
) -> bool {
    let Some(block_fn) = (unsafe { raw::RedisModule_BlockClientOnKeysWithFlags }) else {
        return false;
    };

    // Ownership moves to the server here and returns only through `free_privdata_callback`.
    let privdata = Box::into_raw(Box::new(handler)).cast::<c_void>();
    let mut keys = [key.inner];

    let blocked = unsafe {
        block_fn(
            ctx.ctx,
            Some(reply_callback::<H>),
            Some(timeout_callback::<H>),
            Some(free_privdata_callback::<H>),
            timeout_ms as c_longlong,
            keys.as_mut_ptr(),
            1,
            privdata,
            raw::REDISMODULE_BLOCK_UNBLOCK_DELETED as c_int,
        )
    };

    if blocked.is_null() {
        // The server did not take the private data, so we still own it.
        drop(unsafe { Box::from_raw(privdata.cast::<H>()) });
        return false;
    }
    true
}

/// Borrow the handler the server is holding for this blocked client.
///
/// Returns `None` if the pointer is absent or null — treated as a defect to be reported rather
/// than a reason to dereference.
unsafe fn handler_from_ctx<'a, H: BlockedKeyHandler>(
    ctx: *mut raw::RedisModuleCtx,
) -> Option<&'a H> {
    let getter = unsafe { raw::RedisModule_GetBlockedClientPrivateData }?;
    let privdata = unsafe { getter(ctx) };
    if privdata.is_null() {
        return None;
    }
    Some(unsafe { &*privdata.cast::<H>() })
}

unsafe extern "C" fn reply_callback<H: BlockedKeyHandler>(
    ctx: *mut raw::RedisModuleCtx,
    _argv: *mut *mut raw::RedisModuleString,
    _argc: c_int,
) -> c_int {
    let reply_ctx = ReplyContext::new(ctx);
    let Some(handler) = (unsafe { handler_from_ctx::<H>(ctx) }) else {
        reply_ctx.log_warning("TS.READ: blocked client has no private data; unblocking");
        reply_ctx.reply_error_string(crate::error_consts::INTERNAL_ERROR);
        return ReadyStatus::Replied.as_c_int();
    };

    // A panic here would unwind into C. Report it to the client instead: staying blocked forever
    // is a worse failure than an error reply.
    match catch_unwind(AssertUnwindSafe(|| handler.on_ready(&reply_ctx))) {
        Ok(status) => status.as_c_int(),
        Err(_) => {
            reply_ctx.log_warning("TS.READ: panic in readiness callback; unblocking with error");
            reply_ctx.reply_error_string(crate::error_consts::INTERNAL_ERROR);
            ReadyStatus::Replied.as_c_int()
        }
    }
}

unsafe extern "C" fn timeout_callback<H: BlockedKeyHandler>(
    ctx: *mut raw::RedisModuleCtx,
    _argv: *mut *mut raw::RedisModuleString,
    _argc: c_int,
) -> c_int {
    let reply_ctx = ReplyContext::new(ctx);
    let Some(handler) = (unsafe { handler_from_ctx::<H>(ctx) }) else {
        reply_ctx.log_warning("TS.READ: timed-out client has no private data");
        reply_ctx.reply_error_string(crate::error_consts::INTERNAL_ERROR);
        return raw::REDISMODULE_OK as c_int;
    };

    if catch_unwind(AssertUnwindSafe(|| handler.on_timeout(&reply_ctx))).is_err() {
        reply_ctx.log_warning("TS.READ: panic in timeout callback");
        reply_ctx.reply_error_string(crate::error_consts::INTERNAL_ERROR);
    }
    raw::REDISMODULE_OK as c_int
}

/// Reclaims the private data. Called exactly once per successful block, whatever ends it:
/// reply, timeout, `CLIENT UNBLOCK`, disconnect, or shutdown.
///
/// The context passed here may carry a NULL client if the client was already destroyed, so this
/// callback must not touch client, ACL, or protocol state — dropping the box is all it does.
unsafe extern "C" fn free_privdata_callback<H: BlockedKeyHandler>(
    _ctx: *mut raw::RedisModuleCtx,
    privdata: *mut c_void,
) {
    if privdata.is_null() {
        return;
    }
    let handler = unsafe { Box::from_raw(privdata.cast::<H>()) };
    // A Drop impl on H could panic; it must not unwind into C.
    let _ = catch_unwind(AssertUnwindSafe(move || drop(handler)));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ready_status_maps_to_server_codes() {
        // "Replied" must be the server's OK, or the client would stay blocked after we answered.
        assert_eq!(
            ReadyStatus::Replied.as_c_int(),
            raw::REDISMODULE_OK as c_int
        );
        assert_eq!(
            ReadyStatus::NotReady.as_c_int(),
            raw::REDISMODULE_ERR as c_int
        );
        assert_ne!(
            ReadyStatus::Replied.as_c_int(),
            ReadyStatus::NotReady.as_c_int()
        );
    }
}
