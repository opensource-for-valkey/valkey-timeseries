use crate::common::context::{get_acl_user, is_acl_enforced};
use crate::error_consts;
use std::ptr::NonNull;
use valkey_module::{AclPermissions, Context, ValkeyError, ValkeyResult, ValkeyString, raw};

/// A server user handle, as `RM_GetModuleUserFromUserName` returns it.
///
/// The handle borrows the server's `user` (the module API allocates only the
/// wrapper), so it is valid exactly as long as no ACL mutation can run: every
/// path that checks keys holds the module lock — a command on the main
/// thread, or a fan-out request handler under `MODULE_CONTEXT` — for the
/// whole of the check loop, which is why [`KeyAccess`] is built per request
/// and never stored.
struct ModuleUser(NonNull<raw::RedisModuleUser>);

impl ModuleUser {
    fn from_name(name: &ValkeyString) -> Option<Self> {
        // SAFETY: `name` is a live module string; the API returns null for an
        // unknown or disabled user rather than failing.
        let user = unsafe { raw::RedisModule_GetModuleUserFromUserName.unwrap()(name.inner) };
        NonNull::new(user).map(Self)
    }

    #[inline]
    fn allows(&self, key: &ValkeyString, permissions: &AclPermissions) -> bool {
        // SAFETY: the handle is live for the lifetime of `self` (see the type
        // docs) and `key` is a live module string.
        let status = unsafe {
            raw::RedisModule_ACLCheckKeyPermissions.unwrap()(
                self.0.as_ptr(),
                key.inner,
                permissions.bits(),
            )
        };
        raw::Status::from(status) == raw::Status::Ok
    }
}

impl Drop for ModuleUser {
    fn drop(&mut self) {
        // SAFETY: obtained from `RM_GetModuleUserFromUserName` and freed once.
        unsafe { raw::RedisModule_FreeModuleUser.unwrap()(self.0.as_ptr()) };
    }
}

enum Identity {
    /// Nothing to enforce for this context (replication or AOF apply, an
    /// internal context): every key passes.
    Unrestricted,
    User(ModuleUser),
    /// Enforced, but the user could not be resolved (deleted or disabled while
    /// the connection was open): every key is denied, as the name-based API
    /// would have denied it.
    Unknown,
}

/// The caller's ACL identity for one set of permissions, resolved once so a
/// loop over many keys costs one `RM_ACLCheckKeyPermissions` call per key.
///
/// Resolving by name — the only entry point valkey-module-rs offers — builds
/// a `ValkeyString` for the user name, looks the user up and allocates a
/// handle, then frees both; done per key that was six allocations to answer
/// a flag test, and 15 % of a 1000-series instant query.
pub struct KeyAccess {
    identity: Identity,
    permissions: AclPermissions,
}

impl KeyAccess {
    pub fn new(ctx: &Context, permissions: AclPermissions) -> Self {
        let identity = if !is_acl_enforced(ctx) {
            Identity::Unrestricted
        } else {
            match ModuleUser::from_name(&get_acl_user(ctx)) {
                Some(user) => Identity::User(user),
                None => Identity::Unknown,
            }
        };
        Self {
            identity,
            permissions,
        }
    }

    /// Whether the caller holds the permissions on `key`.
    #[inline]
    pub fn allows(&self, key: &ValkeyString) -> bool {
        match &self.identity {
            Identity::Unrestricted => true,
            Identity::User(user) => user.allows(key, &self.permissions),
            Identity::Unknown => false,
        }
    }

    /// [`Self::allows`] as the error the command contract names for the
    /// permission that was refused.
    #[inline]
    pub fn check(&self, key: &ValkeyString) -> ValkeyResult<()> {
        if self.allows(key) {
            return Ok(());
        }
        if self.permissions.contains(AclPermissions::DELETE) {
            return Err(ValkeyError::Str(error_consts::KEY_DELETE_PERMISSION_ERROR));
        }
        if self.permissions.contains(AclPermissions::UPDATE) {
            return Err(ValkeyError::Str(error_consts::KEY_WRITE_PERMISSION_ERROR));
        }
        Err(ValkeyError::Str(error_consts::PERMISSION_DENIED))
    }
}

pub fn clone_permissions(permissions: &AclPermissions) -> AclPermissions {
    let mut cloned = AclPermissions::empty();
    if permissions.contains(AclPermissions::ACCESS) {
        cloned |= AclPermissions::ACCESS;
    }
    if permissions.contains(AclPermissions::UPDATE) {
        cloned |= AclPermissions::UPDATE;
    }
    if permissions.contains(AclPermissions::DELETE) {
        cloned |= AclPermissions::DELETE;
    }
    cloned
}

pub fn has_all_keys_permissions(
    ctx: &Context,
    user: &ValkeyString,
    permissions: Option<AclPermissions>,
) -> bool {
    if !is_acl_enforced(ctx) {
        return true;
    }
    let all_keys = ctx.create_string("*");
    match &permissions {
        Some(perms) => ctx.acl_check_key_permission(user, &all_keys, perms).is_ok(),
        None => true,
    }
}

/// One key's check. A loop should build a [`KeyAccess`] once instead.
#[inline]
pub fn check_key_permissions(
    ctx: &Context,
    key: &ValkeyString,
    permissions: &AclPermissions,
) -> ValkeyResult<()> {
    KeyAccess::new(ctx, clone_permissions(permissions)).check(key)
}

pub fn check_metadata_permissions(ctx: &Context) -> ValkeyResult<()> {
    let perms = AclPermissions::ACCESS;
    let key = ctx.create_string("*");
    check_key_permissions(ctx, &key, &perms)
        .map_err(|_| ValkeyError::Str(error_consts::ALL_KEYS_READ_PERMISSION_ERROR))
}
