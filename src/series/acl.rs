use crate::common::context::is_acl_enforced;
use crate::error_consts;
use crate::fanout::acl::fanout_module_user;
use std::ptr::NonNull;
use std::rc::Rc;
use valkey_module::{AclPermissions, Context, ValkeyError, ValkeyResult, ValkeyString, raw};

/// A server user handle, as `RM_GetModuleUserFromUserName` returns it.
///
/// The handle borrows the server's `user` (the module API allocates only the
/// wrapper), so it is valid exactly as long as no ACL mutation can run: every
/// path that checks keys holds the module lock — a command on the main
/// thread, or a fan-out request handler under `MODULE_CONTEXT` — for the
/// whole of the check loop, which is why [`KeyAccess`] is built per request
/// and never stored.
///
/// A fan-out request handler resolves this once per GIL acquisition, in
/// `FanoutContext::lock`, and shares it (as an `Rc`) with every `KeyAccess`
/// built under that lock via [`fanout_module_user`] — never re-resolving the
/// name. The handle is dropped before the lock is released.
pub(crate) struct ModuleUser(NonNull<raw::RedisModuleUser>);

impl ModuleUser {
    pub(crate) fn from_name(name: &ValkeyString) -> Option<Self> {
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

/// The second all-keys probe: 33 bytes alternating `\x01` / `\xff`. Together with
/// `*` it admits only patterns that match everything in practice (`*`, `?*`,
/// `[\x01-\xff]*`, …): `~?`, `~[*]` or `~\*` match the one-character key `*` but
/// not this; a prefix or suffix pattern matches at most one of the two.
const ALL_KEYS_PROBE: [u8; 33] = {
    let mut probe = [0x01u8; 33];
    let mut i = 1;
    while i < 33 {
        probe[i] = 0xff;
        i += 2;
    }
    probe
};

impl ModuleUser {
    /// Whether the user's rules grant `permissions` on every key, so a loop
    /// over keys need not check each one.
    ///
    /// The module API exposes no "all keys" flag, so this asks the glob
    /// matcher about two keys no sane non-universal pattern matches both of
    /// (see [`ALL_KEYS_PROBE`]). A pattern crafted to pass both — a character
    /// class holding `*`, `\x01` and `\xff` followed by `*` — would be
    /// treated as universal; it also grants nearly everything.
    fn allows_all_keys(&self, ctx: &Context, permissions: &AclPermissions) -> bool {
        let star = ctx.create_string("*");
        self.allows(&star, permissions) && {
            let probe = ValkeyString::create_from_slice(ctx.ctx, &ALL_KEYS_PROBE);
            self.allows(&probe, permissions)
        }
    }
}

impl Drop for ModuleUser {
    fn drop(&mut self) {
        // SAFETY: obtained from `RM_GetModuleUserFromUserName` and freed once.
        unsafe { raw::RedisModule_FreeModuleUser.unwrap()(self.0.as_ptr()) };
    }
}

enum Identity {
    /// Nothing to check per key: no enforcement for this context (replication
    /// or AOF apply, an internal context), or the user's rules already grant
    /// the permission on every key.
    Unrestricted,
    User(Rc<ModuleUser>),
    /// Enforced, but the user could not be resolved (deleted or disabled while
    /// the connection was open): every key is denied, as the name-based API
    /// would have denied it.
    Unknown,
}

/// The caller's ACL identity for one set of permissions, resolved once so a
/// loop over many keys costs one `RM_ACLCheckKeyPermissions` call per key —
/// or nothing per key when the user may reach every key anyway (the `default`
/// user, and any deployment without key-scoped rules), which two probes
/// establish up front.
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
            // Reuse the handle a fan-out request handler already resolved (an `Rc`
            // clone, no FFI call) rather than resolving the name again here.
            let user = fanout_module_user()
                .or_else(|| ModuleUser::from_name(&ctx.get_current_user()).map(Rc::new));
            match user {
                Some(user) if user.allows_all_keys(ctx, &permissions) => Identity::Unrestricted,
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

    pub fn is_unrestricted(&self) -> bool {
        matches!(self.identity, Identity::Unrestricted)
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
    let Some(perms) = &permissions else {
        return true;
    };
    ModuleUser::from_name(user).is_some_and(|user| user.allows_all_keys(ctx, perms))
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
