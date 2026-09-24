use crate::common::context::{get_acl_user, is_acl_enforced};
use crate::error_consts;
use crate::fanout::acl::fanout_module_user;
use std::ptr::NonNull;
use std::rc::Rc;
use std::sync::{LazyLock, Mutex};
use valkey_module::{
    AclPermissions, CallOptionsBuilder, CallReply, CallResult, Context, ValkeyError, ValkeyResult,
    ValkeyString, raw,
};

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

impl ModuleUser {
    /// Whether the user's rules grant `permissions` on every key, so a loop
    /// over keys need not check each one.
    ///
    /// This is the server's own definition (`ACLSelectorHasUnrestrictedKeyAccess`,
    /// which `SORT … BY` uses): some one selector holds `allkeys`, or the
    /// literal pattern `*` with the needed permissions. A pattern that merely
    /// happens to match many keys (`?*`, `[a-z]*`) does not count; that user
    /// gets per-key checks instead. Probing sample keys against the glob
    /// matcher is not a substitute: any finite set of probes is matched by a
    /// pattern built for it.
    ///
    /// `user_name` must name this user; it is resolved only when the cached
    /// answer is missing.
    fn allows_all_keys(
        &self,
        ctx: &Context,
        user_name: impl FnOnce() -> ValkeyString,
        permissions: &AclPermissions,
    ) -> bool {
        let Some(description) = AclDescription::of(self) else {
            return false;
        };
        // Every all-keys grant renders as a token ending in `~*` (`~*`, `%R~*`,
        // `%W~*`), so its absence settles the answer without a server call. Its
        // presence proves nothing: the flat string is not parse-safe (`ACL SETUSER`
        // accepts spaces in a first-arg rule such as `+select|…`), so a positive
        // is confirmed from `ACL GETUSER`'s per-selector fields.
        if !description.as_slice().windows(2).any(|pair| pair == b"~*") {
            return false;
        }
        cached_all_keys_grants(ctx, description, user_name)
            .is_some_and(|grants| grants.cover(permissions))
    }
}

/// A counted reference to a user's rendered ACL rules, as
/// `RM_GetModuleUserACLString` returns it.
///
/// The server caches that string on the user and never edits it: a rule
/// change drops the user's reference and renders a new string on next use.
/// While this reference is held the old string cannot be freed, so its address
/// cannot be reused — the address identifies one exact rule set.
struct AclDescription(NonNull<raw::RedisModuleString>);

// SAFETY: the string is only created, read and released while the module lock
// is held (see [`ModuleUser`]), which serializes every touch of its refcount.
unsafe impl Send for AclDescription {}

impl AclDescription {
    fn of(user: &ModuleUser) -> Option<Self> {
        let get = unsafe { raw::RedisModule_GetModuleUserACLString }?;
        // SAFETY: the user handle is live (see `ModuleUser`); the call returns a
        // new reference that `Drop` releases.
        NonNull::new(unsafe { get(user.0.as_ptr()) }).map(Self)
    }

    fn as_slice(&self) -> &[u8] {
        ValkeyString::string_as_slice(self.0.as_ptr())
    }
}

impl Drop for AclDescription {
    fn drop(&mut self) {
        // SAFETY: owned reference, released once, under the module lock. The
        // string is not in any context's auto-memory pool, hence no context.
        unsafe { raw::RedisModule_FreeString.unwrap()(std::ptr::null_mut(), self.0.as_ptr()) };
    }
}

/// Which permission sets some one selector grants on every key: bit `1 << g`
/// is set for each selector whose all-keys grant `g` is non-empty, `g` being
/// [`READ_GRANT`] and/or [`WRITE_GRANT`]. A read+write request needs one
/// selector holding both, as the server's per-key check does.
#[derive(Clone, Copy)]
struct AllKeysGrants(u8);

const READ_GRANT: u8 = 1;
const WRITE_GRANT: u8 = 2;

impl AllKeysGrants {
    fn cover(self, permissions: &AclPermissions) -> bool {
        let mut need = 0;
        if permissions.contains(AclPermissions::ACCESS) {
            need |= READ_GRANT;
        }
        if permissions
            .intersects(AclPermissions::INSERT | AclPermissions::UPDATE | AclPermissions::DELETE)
        {
            need |= WRITE_GRANT;
        }
        (1..=3u8).any(|g| self.0 & (1 << g) != 0 && g & need == need)
    }

    /// Records one selector's `keys` field from `ACL GETUSER`: space-separated
    /// `~pattern` / `%R~pattern` / `%W~pattern` tokens (patterns cannot contain
    /// spaces), or `~*` for `allkeys`.
    fn add_selector(&mut self, keys: &[u8]) {
        let mut grant = 0;
        for token in keys.split(|&b| b == b' ') {
            let (perms, pattern) = match token.iter().position(|&b| b == b'~') {
                Some(tilde) => token.split_at(tilde),
                None => continue,
            };
            if pattern != b"~*" {
                continue;
            }
            grant |= match perms {
                b"" => READ_GRANT | WRITE_GRANT,
                [b'%', flags @ ..] => flags.iter().fold(0, |acc, flag| {
                    acc | match flag.to_ascii_uppercase() {
                        b'R' => READ_GRANT,
                        b'W' => WRITE_GRANT,
                        _ => 0,
                    }
                }),
                _ => 0,
            };
        }
        if grant != 0 {
            self.0 |= 1 << grant;
        }
    }
}

/// Answers for recently seen rule sets, keyed by [`AclDescription`] address.
/// Holding the description keeps the key valid (see its docs), so a hit is
/// exact and a rule change is always a miss. Only users with a `~*` token
/// reach it, so it stays small; the oldest entry goes first when full.
static ALL_KEYS_GRANTS: LazyLock<Mutex<Vec<(AclDescription, AllKeysGrants)>>> =
    LazyLock::new(|| Mutex::new(Vec::new()));

const ALL_KEYS_GRANTS_CAPACITY: usize = 32;

fn cached_all_keys_grants(
    ctx: &Context,
    description: AclDescription,
    user_name: impl FnOnce() -> ValkeyString,
) -> Option<AllKeysGrants> {
    let lock = || {
        ALL_KEYS_GRANTS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    };
    if let Some((_, grants)) = lock().iter().find(|(d, _)| d.0 == description.0) {
        return Some(*grants);
    }
    // Not under the cache lock: this runs a server command.
    let grants = query_all_keys_grants(ctx, &user_name())?;
    let mut cache = lock();
    if cache.len() >= ALL_KEYS_GRANTS_CAPACITY {
        cache.remove(0);
    }
    cache.push((description, grants));
    Some(grants)
}

/// Reads the user's selectors from `ACL GETUSER`, whose reply keeps each
/// selector's key patterns in a field of its own, unlike the flat rule string.
/// `None` when the reply is not the expected shape (or the user is gone).
fn query_all_keys_grants(ctx: &Context, user_name: &ValkeyString) -> Option<AllKeysGrants> {
    /// A field of a RESP2 map reply (a flat key/value array).
    fn field<'a>(map: &'a CallReply, name: &[u8]) -> Option<CallReply<'a>> {
        let CallReply::Array(pairs) = map else {
            return None;
        };
        (0..pairs.len())
            .step_by(2)
            .find_map(|i| match pairs.get(i)? {
                Ok(CallReply::String(key)) if key.as_bytes() == name => pairs.get(i + 1)?.ok(),
                _ => None,
            })
    }

    fn selector_keys(selector: &CallReply, grants: &mut AllKeysGrants) -> Option<()> {
        let CallReply::String(keys) = field(selector, b"keys")? else {
            return None;
        };
        grants.add_selector(keys.as_bytes());
        Some(())
    }

    let getuser = ctx.create_string("GETUSER");
    let options = CallOptionsBuilder::new().errors_as_replies().build();
    let reply: CallResult = ctx.call_ext("ACL", &options, &[&getuser, user_name]);
    let user = reply.ok()?;
    let mut grants = AllKeysGrants(0);
    // The root selector's fields sit at the top level.
    selector_keys(&user, &mut grants)?;
    let CallReply::Array(selectors) = field(&user, b"selectors")? else {
        return None;
    };
    for selector in selectors.iter() {
        selector_keys(&selector.ok()?, &mut grants)?;
    }
    Some(grants)
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
            let identify = |user: Rc<ModuleUser>, name: &dyn Fn() -> ValkeyString| {
                if user.allows_all_keys(ctx, name, &permissions) {
                    Identity::Unrestricted
                } else {
                    Identity::User(user)
                }
            };
            if let Some(user) = fanout_module_user() {
                identify(user, &|| get_acl_user(ctx))
            } else {
                let name = ctx.get_current_user();
                match ModuleUser::from_name(&name) {
                    Some(user) => identify(Rc::new(user), &|| name.clone()),
                    None => Identity::Unknown,
                }
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
    ModuleUser::from_name(user)
        .is_some_and(|handle| handle.allows_all_keys(ctx, || user.clone(), perms))
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

/// Gate for commands that report on the whole keyspace's metadata (label names and
/// values, label statistics) rather than on keys the caller names: the caller must be
/// able to read every key.
///
/// "Every key" means the rules say so ([`ModuleUser::allows_all_keys`]), not that some
/// sample keys pass: probing the literal key `*` admitted `~?`, `~[*]` and `~\*`, and a
/// later two-key probe admitted any pattern built to match both probes.
pub fn check_metadata_permissions(ctx: &Context) -> ValkeyResult<()> {
    if KeyAccess::new(ctx, AclPermissions::ACCESS).is_unrestricted() {
        Ok(())
    } else {
        Err(ValkeyError::Str(
            error_consts::ALL_KEYS_READ_PERMISSION_ERROR,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grants(selectors: &[&[u8]]) -> AllKeysGrants {
        let mut grants = AllKeysGrants(0);
        for keys in selectors {
            grants.add_selector(keys);
        }
        grants
    }

    const READ: AclPermissions = AclPermissions::ACCESS;
    const WRITE: AclPermissions = AclPermissions::UPDATE;

    #[test]
    fn only_the_literal_star_pattern_grants_every_key() {
        let all = grants(&[b"~*"]);
        assert!(all.cover(&READ) && all.cover(&WRITE) && all.cover(&(READ | WRITE)));

        for keys in [
            &b""[..],
            b"~abc*",
            b"~?",
            b"~[*]",
            b"~\\*",
            b"~?*",
            b"~**",
            b"~*)",
            b"~[*\x01]*",
            b"%R~abc* ~foo",
        ] {
            let g = grants(&[keys]);
            assert!(!g.cover(&READ) && !g.cover(&WRITE), "{keys:?}");
        }
    }

    #[test]
    fn scoped_star_grants_only_its_permission() {
        let read = grants(&[b"~abc* %R~*"]);
        assert!(read.cover(&READ));
        assert!(!read.cover(&WRITE));
        assert!(!read.cover(&(READ | WRITE)));

        let write = grants(&[b"%W~*"]);
        assert!(write.cover(&AclPermissions::DELETE));
        assert!(!write.cover(&READ));
    }

    #[test]
    fn read_write_needs_one_selector_holding_both() {
        // As the server's per-key check: a key passes only if one selector grants
        // every requested permission.
        let split = grants(&[b"%R~*", b"%W~*"]);
        assert!(split.cover(&READ) && split.cover(&WRITE));
        assert!(!split.cover(&(READ | WRITE)));

        let joined = grants(&[b"~abc*", b"%R~* %W~foo", b"~*"]);
        assert!(joined.cover(&(READ | WRITE)));
    }
}
