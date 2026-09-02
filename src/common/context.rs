use std::ffi::CString;
use std::os::raw::c_int;
use valkey_module::{
    Context, ContextFlags, RedisModule_GetSelectedDb, RedisModule_SelectDb, Status, ValkeyError,
    ValkeyModule_GetServerInfo, ValkeyModule_ServerInfoGetFieldSigned, ValkeyModuleCtx,
    ValkeyModuleServerInfoData, ValkeyResult, ValkeyString, raw,
};

use crate::fanout::{FANOUT_ACL_USER, fanout_acl_scope_active, is_clustered};

/// Build a `ValkeyString` from raw key bytes without going through `CString`.
///
/// `Context::create_string` funnels its argument through `CString::new(..).unwrap()`, so any
/// byte string holding an interior NUL panics. Valkey key names are binary-safe, so that is not
/// a corrupt-input-only concern: `TS.CREATE "a\0b"` is a legal key, and the same bytes come back
/// out of an RDB, a `RESTORE`, a rename, or the postings index. Worse, most of those paths run
/// inside a keyspace-notification or data-type callback, which is `extern "C"` and cannot unwind
/// — the panic becomes an `abort`, taking the whole server down with it.
///
/// Every conversion of keyspace-derived bytes into a `ValkeyString` must go through here.
/// `create_string` stays fine for module-authored literals and formatted numbers.
pub fn create_key_string(ctx: &Context, key: &[u8]) -> ValkeyString {
    ValkeyString::create_from_slice(ctx.ctx, key)
}

/// Render a binary key name for a log line or an error message.
///
/// Lossy on purpose, and only for human-readable diagnostics: a key holding a non-UTF-8 byte
/// still has to appear in a message somehow. Never build a client reply out of this — reply with
/// the raw bytes (`reply_with_slice`) so the caller gets back exactly the name it used.
pub fn key_for_display(key: &[u8]) -> std::borrow::Cow<'_, str> {
    String::from_utf8_lossy(key)
}

// Safety: RedisModule_GetSelectedDb is safe to call
pub fn get_current_db(ctx: &Context) -> i32 {
    unsafe { RedisModule_GetSelectedDb.unwrap()(ctx.ctx) }
}

pub fn set_current_db(ctx: &Context, db: i32) -> Status {
    // Safety: RedisModule_SelectDb is safe to call. It is a bug in the valkey_module
    // if the function is not available.
    unsafe {
        match RedisModule_SelectDb.unwrap()(ctx.ctx, db) {
            0 => Status::Ok,
            _ => Status::Err,
        }
    }
}

#[inline]
pub fn is_aof_client(client_id: u64) -> bool {
    client_id == u64::MAX
}

pub fn is_real_user_client(ctx: &Context) -> bool {
    let client_id = ctx.get_client_id();
    if client_id == 0 || is_aof_client(client_id) {
        return false;
    }
    if ctx.get_flags().contains(ContextFlags::REPLICATED) {
        return false;
    }
    true
}

/// Whether this server is a replica.
///
/// Server-level state, not client state: `RM_GetContextFlags` reads it from the server, so this
/// is valid from a detached/thread-safe context as well as from a command context. Used to keep
/// a cluster fanout write from being applied locally when the coordinator's cluster map is stale
/// enough to have addressed us as a primary (see `MDelFanoutCommand::get_local_response`).
#[inline]
pub fn is_replica(ctx: &Context) -> bool {
    ctx.get_flags().contains(ContextFlags::SLAVE)
}

/// Whether the current execution context forbids blocking the client — inside `MULTI`, a Lua
/// script, or a nested module call.
///
/// This must be consulted before every `RM_BlockClientOnKeys*` call. It is not a defensive check:
/// the server asserts `!deny_blocking || (islua || ismulti)` inside `moduleBlockClient` and aborts
/// the process when it fails.
#[inline]
pub fn is_blocking_denied(ctx: &Context) -> bool {
    ctx.get_flags().contains(ContextFlags::DENY_BLOCKING)
}

#[inline]
pub fn is_acl_enforced(ctx: &Context) -> bool {
    // Replicated (master-link) and AOF-applied commands must not trigger ACL checks:
    // the originating primary already enforced ACLs on the real client, and the
    // replication/AOF apply context has no current user. Enforcing here would call
    // the ACL API with a NULL username string, dereferencing a null pointer inside
    // `RedisModule_GetModuleUserFromUserName` and crashing the server. `is_real_user_client`
    // already excludes client_id == 0 (internal/module contexts), the AOF sentinel,
    // and the REPLICATED flag.
    is_real_user_client(ctx) || fanout_acl_scope_active()
}

pub fn get_acl_user(ctx: &Context) -> valkey_module::ValkeyString {
    if is_clustered(ctx) {
        let fanout_user = FANOUT_ACL_USER.with(|u| u.borrow().clone());
        if let Some(user) = fanout_user {
            return ctx.create_string(user.as_str());
        }
    }
    ctx.get_current_user()
}

pub fn get_server_info(ctx: &Context, section: &str) -> *mut ValkeyModuleServerInfoData {
    let info_fn = unsafe { ValkeyModule_GetServerInfo.unwrap() };
    let context = ctx.ctx as *mut ValkeyModuleCtx;
    let section_cstr = CString::new(section).expect("Failed to convert section to CString");
    unsafe { info_fn(context, section_cstr.as_ptr()) }
}

fn get_server_info_field_signed(
    info: *mut ValkeyModuleServerInfoData,
    field: &str,
) -> ValkeyResult<i64> {
    let get_signed_field_fn = unsafe {
        ValkeyModule_ServerInfoGetFieldSigned
            .expect("Failed to get ValkeyModule_ServerInfoGetFieldSigned")
    };
    let mut ignored: c_int = 0;
    unsafe {
        let field_value = CString::new(field).expect("Failed to convert field to CString");
        let res = get_signed_field_fn(info, field_value.as_ptr(), &mut ignored);
        if ignored != 0 {
            let msg = format!("Field '{field}' not found in server info");
            return Err(ValkeyError::String(msg));
        }
        Ok(res)
    }
}

pub fn register_server_event_handler(
    ctx: &Context,
    server_event: u64,
    inner_callback: raw::RedisModuleEventCallback,
) -> Result<(), ValkeyError> {
    let res = unsafe {
        raw::RedisModule_SubscribeToServerEvent.unwrap()(
            ctx.ctx,
            raw::RedisModuleEvent {
                id: server_event,
                dataver: 1,
            },
            inner_callback,
        )
    };
    if res != raw::REDISMODULE_OK as i32 {
        return Err(ValkeyError::Str("TSDB: failed subscribing to server event"));
    }

    Ok(())
}

pub fn get_available_memory(ctx: &Context) -> Option<i64> {
    // Fetch INFO MEMORY
    let info = get_server_info(ctx, "memory");

    let used_memory: i64 = get_server_info_field_signed(info, "used_memory").ok()?;
    let maxmemory: i64 = get_server_info_field_signed(info, "maxmemory").ok()?;

    // Compute available = maxmemory - used_memory (clamped to >= 0)
    if maxmemory > 0 {
        let diff = maxmemory - used_memory;
        Some(if diff > 0 { diff } else { 0 })
    } else {
        None
    }
}
