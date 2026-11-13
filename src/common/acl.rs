//! ACL checks, and the identity they run as.
//!
//! This is the one place that decides *whether* a key may be touched ([`is_acl_enforced`],
//! [`KeyAccess`], [`check_key_permissions`], [`check_metadata_permissions`]) and *as whom*. A
//! command on the main thread runs as its client's user. Work done on another thread, or for a
//! fan-out request from another node, has no such client: it captures the caller's name with
//! `caller_user_name` where the caller is known, and runs its checks inside a
//! `ThreadAclScope` for that name.

use crate::common::context::is_real_user_client;
use crate::common::logging::log_warning;
use crate::common::threads::{GilGuard, check_holds_gil};
use crate::error_consts;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::ops::Deref;
use std::ptr::{self, NonNull};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, Mutex, OnceLock};
use valkey_module::{
    AclPermissions, CallOptionsBuilder, CallReply, CallResult, Context, ValkeyError, ValkeyResult,
    ValkeyString, raw,
};

/// The ACL identity a [`ThreadAclScope`] attaches to the current thread.
///
/// `user` is the [`ModuleUser`] handle [`ThreadAclScope::enter_as`] resolves via
/// `RM_GetModuleUserFromUserName` and keeps for the life of the scope, so every
/// [`KeyAccess`] built under it reuses the handle instead of resolving the same name again.
/// It is `None` only for the test-only unresolved scope, which has no live server to
/// resolve against.
///
/// `name` is the string `user` was resolved from, kept for the life of the scope: the
/// thread-local holds this identity until the scope drops, whatever `KeyAccess` clones of
/// `user` exist. The handle does not depend on it (the server copies its own `user` pointer
/// at lookup); it is kept so `KeyAccess` can hand the name to `ACL GETUSER` without building
/// it again. Both are server objects, freed when the scope drops, so the scope must not
/// outlive the module lock it was entered under. `user` is declared first so it is released
/// first, the reverse of the order they were made in.
struct AclIdentity {
    user: Option<Rc<ModuleUser>>,
    name: ValkeyString,
}

thread_local! {
    static THREAD_ACL_USER: RefCell<Option<Rc<AclIdentity>>> = const { RefCell::new(None) };
}

/// Runs this thread's ACL checks as a given user until it drops.
///
/// For code holding the module lock on a context that is not the caller's own client: a
/// fan-out request handler on `MODULE_CONTEXT`, or a background job for a blocked client.
/// Whether checks apply at all is still decided by the context ([`is_acl_enforced`]); the
/// scope only says *which* user they check.
///
/// # Invariant
/// Thread-local, so it is not `Send`: any ACL-relevant operation must run on the thread
/// that entered it, and it must drop before the module lock it was entered under is
/// released.
///
/// Scopes do not nest. Entering one while another is live on the thread fails and leaves the
/// first untouched: a second would replace the first's identity, and dropping it would clear
/// the thread's identity while the first still lived, switching its later checks to the
/// context's own user without a sound.
#[must_use = "ACL checks run as this user only while the scope is live"]
pub(crate) struct ThreadAclScope {
    // An `Rc` marker: the identity it installs is an `Rc` in a thread-local.
    _not_send: PhantomData<Rc<()>>,
}

impl ThreadAclScope {
    /// Run this thread's checks as `user`, resolving the user's handle once, now. Call it
    /// under the module lock; threading rule R6 checks that (see `common::threads`), at entry
    /// and again when the scope drops.
    ///
    /// Fails when the user does not exist or is disabled, so a caller cannot silently run
    /// without enforcement, and when a scope is already live on this thread (see the type
    /// docs).
    pub(crate) fn enter_as(user: &str) -> ValkeyResult<Self> {
        // Before the lookup: it reads the server's user table, which the GIL guards.
        check_holds_gil();
        // Created with no context, so it belongs to the scope alone and is freed with it,
        // under the caller's lock, whatever context that lock came from. Built from the
        // bytes, so a name with a NUL in it is an unknown user, not a panic.
        let name = ValkeyString::create_from_slice(ptr::null_mut(), user.as_bytes());
        let module_user = ModuleUser::from_name(&name)
            .ok_or(ValkeyError::Str(error_consts::ACL_USER_UNAVAILABLE))?;
        Self::enter_with_identity(AclIdentity {
            user: Some(Rc::new(module_user)),
            name,
        })
    }

    /// Attach an identity with no user handle and no name, for tests that only exercise the
    /// thread-local scope: they have no live server to create a string or resolve a user.
    #[cfg(test)]
    fn enter_unresolved() -> ValkeyResult<Self> {
        check_holds_gil();
        Self::enter_with_identity(AclIdentity {
            user: None,
            // A null string: dropping one frees nothing, and nothing reads it.
            name: ValkeyString::from_redis_module_string(ptr::null_mut(), ptr::null_mut()),
        })
    }

    /// Install `identity` on this thread unless a scope already has one. On refusal no scope
    /// value exists, so nothing runs a `Drop` that could clear the live scope's identity;
    /// `identity` itself is released here, under the caller's lock.
    fn enter_with_identity(identity: AclIdentity) -> ValkeyResult<Self> {
        // `install` hands a refused identity back out of the thread-local access, so it is
        // dropped below, not inside it. See [`install`].
        match THREAD_ACL_USER.with(|cell| install(cell, identity)) {
            Ok(()) => Ok(Self {
                _not_send: PhantomData,
            }),
            Err(refused) => {
                drop(refused);
                // A broken invariant, not a client mistake: say so in the log once, as the
                // threading rules do, and fail the request closed.
                static REPORTED: AtomicBool = AtomicBool::new(false);
                if !REPORTED.swap(true, Ordering::Relaxed) {
                    log_warning(format!(
                        "ACL scope entered while another was live on this thread; refusing it\n{}",
                        std::backtrace::Backtrace::force_capture()
                    ));
                }
                Err(ValkeyError::Str(error_consts::ACL_SCOPE_ALREADY_ENTERED))
            }
        }
    }
}

impl Drop for ThreadAclScope {
    fn drop(&mut self) {
        // The identity's server objects are freed below; R6 says the GIL must still be held.
        check_holds_gil();
        // `clear` hands the identity back out of the thread-local access, so its server
        // objects are released here, still under the lock, but not inside it. See [`install`].
        let identity = THREAD_ACL_USER.with(clear);
        drop(identity);
    }
}

/// Put `value` in `cell` unless it is occupied, in which case `value` comes back.
///
/// The slot's two operations ([`install`], [`clear`]) never drop a value themselves: a
/// refused or displaced one is returned, so the caller drops it with `cell` not borrowed and
/// outside the closure it reached the thread-local through. That is not decoration:
/// [`AclIdentity`] owns server objects (`RM_FreeString`, `RM_FreeModuleUser`) whose release is
/// no business of this slot's bookkeeping, and a destructor that ever touched the slot again
/// would panic on a borrowed cell. Today none does; this keeps it from depending on that, or on
/// the order Rust happens to drop a closure's captures in.
fn install<T>(cell: &RefCell<Option<Rc<T>>>, value: T) -> Result<(), T> {
    let mut slot = cell.borrow_mut();
    if slot.is_some() {
        return Err(value);
    }
    *slot = Some(Rc::new(value));
    Ok(())
}

/// Empty `cell`, handing back what it held. See [`install`].
fn clear<T>(cell: &RefCell<Option<Rc<T>>>) -> Option<Rc<T>> {
    cell.borrow_mut().take()
}

/// The GIL, held together with a caller's ACL identity for one step of work done on its behalf.
///
/// Dereferences to [`Context`]. While it is alive, the caller's identity, when one was given,
/// is active on this thread, so key checks made through it run as that user; with none, they
/// run as the context's own user. On drop the identity goes first, then the GIL, so the
/// identity never outlives the lock (R6).
///
/// It does not select a database: that is its creator's job, done before the guard is built
/// (`FanoutContext::lock` selects the request's). Like [`GilGuard`], it has no client behind
/// it and must not be used to send replies.
pub struct AclContextGuard {
    // Declared first so the scope, and the user handle it resolved, drop under the lock.
    _acl: Option<ThreadAclScope>,
    guard: GilGuard,
}

impl AclContextGuard {
    /// Creates a new `AclContextGuard` from a `ThreadAclScope` and a `GilGuard`.
    pub(crate) fn new(acl: Option<ThreadAclScope>, guard: GilGuard) -> Self {
        Self { _acl: acl, guard }
    }

    pub(crate) fn for_user<S: AsRef<str>>(
        user_name: Option<S>,
        guard: GilGuard,
    ) -> ValkeyResult<Self> {
        let acl = match user_name {
            Some(name) => Some(ThreadAclScope::enter_as(name.as_ref())?),
            None => None,
        };
        Ok(Self::new(acl, guard))
    }
}

impl Deref for AclContextGuard {
    type Target = Context;

    fn deref(&self) -> &Context {
        &self.guard
    }
}

/// The identity the current thread's [`ThreadAclScope`] attached, if any.
fn thread_identity() -> Option<Rc<AclIdentity>> {
    THREAD_ACL_USER.with(|u| u.borrow().clone())
}

#[cfg(test)]
fn thread_acl_scope_active() -> bool {
    THREAD_ACL_USER.with(|u| u.borrow().is_some())
}

/// The ACL user name of `ctx`'s client, to carry to where the checks will run: another
/// thread, or another node.
///
/// `None` when the client has no user. That is the server's root client, which a module's
/// `RM_Call` without the `C` flag runs on and which no ACL rule applies to. The wire has no way
/// to say "root", so the receiving side then checks as its own context's user (the `default`
/// user on a fan-out shard), which may be stricter: such a call fails closed rather than open.
pub(crate) fn caller_user_name(ctx: &Context) -> Option<String> {
    let user = current_user_name(ctx)?.to_string();
    (!user.is_empty()).then_some(user)
}

/// Whether key checks apply to `ctx` at all.
///
/// They do not for replicated (master-link) or AOF-applied commands, whose primary already
/// checked the real client, nor for internal contexts ([`is_real_user_client`]). Nor do they
/// for a client with no user: the server's root client, which `RM_Call` without the `C` flag
/// runs on. Asking the ACL API about a user that client does not have would hand it a NULL
/// name, which it dereferences.
///
/// A live `ThreadAclScope` makes checks apply even on a context whose own client has no
/// user: the scope names the user to check as.
pub fn is_acl_enforced(ctx: &Context) -> bool {
    checked_as(ctx).is_some()
}

/// Whom a key check on a context runs as.
enum CheckedAs {
    /// The thread's [`ThreadAclScope`], with the handle it already resolved.
    Scope(Rc<AclIdentity>, Rc<ModuleUser>),
    /// The context's own client, by name.
    Client(ValkeyString),
}

/// Whom a key check on `ctx` runs as, or `None` when no check applies. The one statement of
/// the rule [`is_acl_enforced`] documents; [`KeyAccess::new`] applies it in the same pass.
fn checked_as(ctx: &Context) -> Option<CheckedAs> {
    if !is_real_user_client(ctx) {
        return None;
    }
    if let Some(identity) = thread_identity()
        && let Some(user) = identity.user.clone()
    {
        return Some(CheckedAs::Scope(identity, user));
    }
    current_user_name(ctx).map(CheckedAs::Client)
}

/// `ctx`'s client's user name, or `None` when the client has no user (the server's root
/// client) or there is no client. The module API returns NULL for both, and a NULL name must
/// never reach `RM_GetModuleUserFromUserName`.
fn current_user_name(ctx: &Context) -> Option<ValkeyString> {
    // SAFETY: `ctx` is a live context; the call returns a new string, or NULL rather than
    // failing.
    let name = unsafe { raw::RedisModule_GetCurrentUserName.unwrap()(ctx.ctx) };
    // Freed through the context that created it.
    (!name.is_null()).then(|| ValkeyString::from_redis_module_string(ctx.ctx, name))
}

/// A server user handle, as `RM_GetModuleUserFromUserName` returns it.
///
/// The handle borrows the server's `user` (the module API allocates only the
/// wrapper), so it is valid exactly as long as no ACL mutation can run: every
/// path that checks keys holds the module lock — a command on the main
/// thread, or a fan-out request handler under `MODULE_CONTEXT` — for the
/// whole of the check loop, which is why [`KeyAccess`] is built per request
/// and never stored.
///
/// Code running under a [`ThreadAclScope`] resolves this once, when the scope is
/// entered (for a fan-out request, once per GIL acquisition in
/// `FanoutContext::lock`), and shares it (as an `Rc`) with every `KeyAccess`
/// built under that scope — never re-resolving the name. The handle is dropped
/// with the scope, before the lock is released.
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
        let identify = |user: Rc<ModuleUser>, name: &dyn Fn() -> ValkeyString| {
            if user.allows_all_keys(ctx, name, &permissions) {
                Identity::Unrestricted
            } else {
                Identity::User(user)
            }
        };
        let identity = match checked_as(ctx) {
            None => Identity::Unrestricted,
            // Reuse the handle the scope already resolved (an `Rc` clone, no FFI call).
            Some(CheckedAs::Scope(identity, user)) => {
                identify(user, &|| identity.name.safe_clone(ctx))
            }
            Some(CheckedAs::Client(name)) => match ModuleUser::from_name(&name) {
                Some(user) => identify(Rc::new(user), &|| name.clone()),
                None => Identity::Unknown,
            },
        };
        Self {
            identity,
            permissions,
        }
    }

    pub fn unrestricted() -> Self {
        Self {
            identity: Identity::Unrestricted,
            permissions: AclPermissions::empty(),
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

impl Default for KeyAccess {
    fn default() -> Self {
        Self {
            identity: Identity::Unknown,
            permissions: AclPermissions::empty(),
        }
    }
}

pub fn clone_permissions(permissions: &AclPermissions) -> AclPermissions {
    let mut cloned = AclPermissions::empty();
    if permissions.contains(AclPermissions::ACCESS) {
        cloned |= AclPermissions::ACCESS;
    }
    if permissions.contains(AclPermissions::INSERT) {
        cloned |= AclPermissions::INSERT;
    }
    if permissions.contains(AclPermissions::UPDATE) {
        cloned |= AclPermissions::UPDATE;
    }
    if permissions.contains(AclPermissions::DELETE) {
        cloned |= AclPermissions::DELETE;
    }
    cloned
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
/// "Every key" means the rules say so (`ModuleUser::allows_all_keys`), not that some
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

/// The command [`check_db_access`] asks the server about. It must stay keyless, carry
/// a category the server restricts by database (`@read`), take no database argument,
/// and have no subcommands, so that the server's answer turns on the database alone
/// (a test in `commands` pins its categories).
pub(crate) const DB_PROBE_COMMAND: &str = "ts.queryindex";

/// `VALKEYMODULE_ACL_LOG_DB`: the denial reason `RM_ACLCheckPermissions` reports when the
/// user may not access the database. The bindings the crate ships predate it.
const ACL_LOG_DB: raw::RedisModuleACLLogEntryReason = 4;

type AclCheckPermissionsFn = unsafe extern "C" fn(
    user: *mut raw::RedisModuleUser,
    argv: *mut *mut raw::RedisModuleString,
    argc: std::os::raw::c_int,
    dbid: std::os::raw::c_int,
    denial_reason: *mut raw::RedisModuleACLLogEntryReason,
) -> std::os::raw::c_int;

/// `RM_ACLCheckPermissions` and the one-argument command line it is asked about.
struct DbProbe {
    check: AclCheckPermissionsFn,
    /// [`DB_PROBE_COMMAND`], created with no context and never freed. The server only
    /// reads it, and only under the module lock.
    argv0: NonNull<raw::RedisModuleString>,
}

// SAFETY: `argv0` is created once and then only read, under the module lock.
unsafe impl Send for DbProbe {}
unsafe impl Sync for DbProbe {}

/// The probe, or `None` on a server without `RM_ACLCheckPermissions`. Database ACL rules
/// (`db=`, `alldbs`, `resetdbs`) arrived with it, in Valkey 9.1, so a server without it has
/// no rule to check.
fn db_probe() -> Option<&'static DbProbe> {
    static PROBE: OnceLock<Option<DbProbe>> = OnceLock::new();
    PROBE
        .get_or_init(|| {
            let check = resolve_api::<AclCheckPermissionsFn>(c"ACLCheckPermissions")?;
            let argv0 =
                ValkeyString::create_from_slice(ptr::null_mut(), DB_PROBE_COMMAND.as_bytes());
            let raw_argv0 = NonNull::new(argv0.inner)?;
            // Kept for the life of the process.
            std::mem::forget(argv0);
            Some(DbProbe {
                check,
                argv0: raw_argv0,
            })
        })
        .as_ref()
}

/// Looks up a module API function the crate's bindings do not carry, under either of the
/// names the server registers it as. `F` must be that function's pointer type.
fn resolve_api<F: Copy>(name: &std::ffi::CStr) -> Option<F> {
    const { assert!(size_of::<F>() == size_of::<*mut std::ffi::c_void>()) };
    let name = name.to_bytes();
    // SAFETY: the getters are written once, at module load, before any command runs.
    let getters = unsafe { [raw::ValkeyModule_GetApi, raw::RedisModule_GetApi] };
    for (getter, prefix) in getters
        .into_iter()
        .zip([&b"ValkeyModule_"[..], b"RedisModule_"])
    {
        let Some(get) = getter else { continue };
        let full = std::ffi::CString::new([prefix, name].concat()).ok()?;
        let mut target: *mut std::ffi::c_void = ptr::null_mut();
        // SAFETY: `GetApi` writes the function's address to `target` or returns an error,
        // leaving it untouched.
        let status = unsafe { get(full.as_ptr(), (&raw mut target).cast()) };
        if status as isize == raw::REDISMODULE_OK && !target.is_null() {
            // SAFETY: `target` is the address of the function `F` names (see above).
            return Some(unsafe { std::mem::transmute_copy::<*mut std::ffi::c_void, F>(&target) });
        }
    }
    None
}

/// Whether the user [`checked_as`] names may access database `db` on this node.
///
/// The server checks a command's database against the client's selected one before it
/// dispatches, so a command on the main thread needs no help. This is for work that
/// selects a database on the caller's behalf: a fan-out share on another node, whose ACL
/// rules need not match the coordinator's, and a background job that re-takes the lock.
///
/// It asks the server whether the user may run [`DB_PROBE_COMMAND`] in `db`, and refuses
/// only a database denial. That isolates the database rule: each selector checks the
/// database before anything else, and the server reports a database denial only when every
/// selector refused on it (it is the lowest-ranked reason), so any other answer means some
/// selector grants `db`. Like the per-key checks, this is not tied to one selector: a user
/// granted `db` by one selector and the command or a key by another passes. That is accepted:
/// the module assumes a user's rules are the same on every node, so this is a safeguard
/// against an obvious mismatch, not a full re-check of the shard's rules.
pub(crate) fn check_db_access(ctx: &Context, db: i32) -> ValkeyResult<()> {
    // Before the probe: its first use creates a server string.
    check_holds_gil();
    let Some(probe) = db_probe() else {
        return Ok(());
    };
    match checked_as(ctx) {
        None => Ok(()),
        Some(CheckedAs::Scope(_, user)) => probe_db(probe, &user, db),
        Some(CheckedAs::Client(name)) => {
            let user = ModuleUser::from_name(&name)
                .ok_or(ValkeyError::Str(error_consts::ACL_USER_UNAVAILABLE))?;
            probe_db(probe, &user, db)
        }
    }
}

fn probe_db(probe: &DbProbe, user: &ModuleUser, db: i32) -> ValkeyResult<()> {
    // Not a reason the server reports: tells an invalid call from a denial.
    const UNSET: raw::RedisModuleACLLogEntryReason = raw::RedisModuleACLLogEntryReason::MAX;
    let mut argv = [probe.argv0.as_ptr()];
    let mut reason = UNSET;
    // SAFETY: the user handle and `argv` are live under the module lock, which the caller
    // holds; the server only reads them and writes `reason`.
    let status =
        unsafe { (probe.check)(user.0.as_ptr(), argv.as_mut_ptr(), 1, db, &raw mut reason) };
    if raw::Status::from(status) == raw::Status::Ok {
        return Ok(());
    }
    match reason {
        // `UNSET`: `db` out of range, or the probe command missing. Neither should happen
        // once the database was selected and the module loaded, so fail closed.
        ACL_LOG_DB | UNSET => Err(ValkeyError::Str(error_consts::DB_PERMISSION_ERROR)),
        // Some selector granted `db` and refused on something later (see above).
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::threads::GilToken;

    /// Marks this thread as holding the GIL: a stand-in for the module lock, which unit tests
    /// have no server to take. A scope is entered and dropped only under it (R6).
    fn fake_gil() -> GilToken {
        GilToken::take(|| ()).0
    }

    /// R6: a scope is entered, and dropped, only by a thread holding the GIL. A debug build
    /// panics on either; a release build logs the first, so this runs in debug builds only.
    #[cfg(debug_assertions)]
    #[test]
    fn a_scope_is_entered_and_dropped_only_under_the_gil() {
        use std::panic::{AssertUnwindSafe, catch_unwind};

        std::thread::spawn(|| {
            let entered = catch_unwind(|| ThreadAclScope::enter_unresolved().map(drop));
            assert!(entered.is_err(), "a scope was entered without the GIL");
            assert!(
                !thread_acl_scope_active(),
                "the refused entry installed an identity"
            );
        })
        .join()
        .unwrap();

        std::thread::spawn(|| {
            let gil = fake_gil();
            let scope = ThreadAclScope::enter_unresolved().expect("entered under the GIL");
            drop(gil);
            let dropped = catch_unwind(AssertUnwindSafe(|| drop(scope)));
            assert!(
                dropped.is_err(),
                "a scope was dropped after the GIL was released"
            );
        })
        .join()
        .unwrap();
    }

    #[test]
    fn a_nested_scope_is_rejected_and_the_outer_one_stays_in_force() {
        let _gil = fake_gil();
        let outer = ThreadAclScope::enter_unresolved().expect("no scope is live");
        let outer_identity = thread_identity().expect("the outer scope attached an identity");

        let Err(err) = ThreadAclScope::enter_unresolved() else {
            panic!("a second scope was accepted while the first was live");
        };
        assert!(
            matches!(err, ValkeyError::Str(msg) if msg == error_consts::ACL_SCOPE_ALREADY_ENTERED),
            "unexpected error: {err:?}"
        );
        let current = thread_identity().expect("the refusal cleared the outer identity");
        assert!(
            Rc::ptr_eq(&outer_identity, &current),
            "the refusal replaced the outer identity"
        );

        // The refused attempt created no scope, so there is nothing whose drop could
        // clear the live one: the outer scope still governs until it drops itself.
        assert!(thread_acl_scope_active());
        drop(outer);
        assert!(!thread_acl_scope_active());

        // A scope is enterable again once the first is gone.
        let again = ThreadAclScope::enter_unresolved().expect("the thread is free again");
        assert!(thread_acl_scope_active());
        drop(again);
        assert!(!thread_acl_scope_active());
    }

    /// A value the slot refuses or gives up is dropped by the caller, with the cell free: a
    /// destructor that re-enters the slot must find it unborrowed.
    #[test]
    fn a_refused_or_cleared_value_is_dropped_with_the_cell_free() {
        thread_local! {
            static CELL: RefCell<Option<Rc<Probe>>> = const { RefCell::new(None) };
            static FREE_AT_DROP: RefCell<Vec<bool>> = const { RefCell::new(Vec::new()) };
        }
        struct Probe;
        impl Drop for Probe {
            fn drop(&mut self) {
                let free = CELL.with(|cell| cell.try_borrow_mut().is_ok());
                FREE_AT_DROP.with(|seen| seen.borrow_mut().push(free));
            }
        }
        let dropped = || FREE_AT_DROP.with(|seen| seen.borrow().len());

        assert!(CELL.with(|cell| install(cell, Probe)).is_ok());

        let Err(refused) = CELL.with(|cell| install(cell, Probe)) else {
            panic!("a second value was installed over the first");
        };
        assert_eq!(
            dropped(),
            0,
            "the refusal dropped the value instead of returning it"
        );
        drop(refused);
        assert_eq!(dropped(), 1);

        let cleared = CELL.with(clear);
        assert_eq!(
            dropped(),
            1,
            "clearing dropped the value instead of returning it"
        );
        drop(cleared);
        assert_eq!(dropped(), 2);
        assert!(CELL.with(|cell| cell.borrow().is_none()));

        assert_eq!(
            FREE_AT_DROP.with(|seen| seen.borrow().clone()),
            [true, true],
            "a destructor found the cell borrowed"
        );
    }

    #[test]
    fn test_acl_scope_is_thread_local() {
        let _gil = fake_gil();
        assert!(!thread_acl_scope_active());
        let scope = ThreadAclScope::enter_unresolved().expect("no scope is live");
        assert!(thread_acl_scope_active());

        let child = std::thread::spawn(thread_acl_scope_active)
            .join()
            .expect("thread join failed");
        assert!(!child);

        drop(scope);
        assert!(!thread_acl_scope_active());
    }

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
