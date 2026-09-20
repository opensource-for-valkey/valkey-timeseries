use crate::series::acl::ModuleUser;
use std::cell::RefCell;
use std::rc::Rc;
use valkey_module::Context;

/// The ACL identity attached to the current thread's fan-out request.
///
/// `user` is the [`ModuleUser`] handle [`FanoutContext::lock`](crate::fanout::FanoutContext::lock) resolves via
/// `RM_GetModuleUserFromUserName` and keeps for the life of one GIL hold, so
/// [`crate::series::acl::KeyAccess::new`] can reuse it directly instead of
/// resolving the same name again for every `KeyAccess` built under that lock.
/// It is `None` only for [`FanoutAclScope::enter_with_user`]'s name-only test
/// path, which has no live server to resolve against.
pub(crate) struct FanoutIdentity {
    pub(crate) name: String,
    pub(crate) user: Option<Rc<ModuleUser>>,
}

thread_local! {
    pub(crate) static FANOUT_ACL_USER: RefCell<Option<Rc<FanoutIdentity>>> = const { RefCell::new(None) };
}

/// RAII marker for fanout ACL checks in detached contexts.
///
/// # Invariant
/// This marker is thread-local and only applies on the current thread. Any ACL-relevant
/// operation must execute on the same thread where this scope is entered.
pub struct FanoutAclScope;

impl FanoutAclScope {
    /// Attach a user name without a resolved [`ModuleUser`] handle. Exists for tests
    /// that only need to exercise the thread-local scope, not ACL resolution.
    /// Production code enters through [`FanoutContext::lock`](crate::fanout::FanoutContext::lock), which resolves
    /// the handle under the GIL and keeps it for that lock's lifetime.
    pub fn enter_with_user(user: &str) -> Self {
        Self::enter(FanoutIdentity {
            name: user.to_owned(),
            user: None,
        })
    }

    pub(super) fn enter(identity: FanoutIdentity) -> Self {
        FANOUT_ACL_USER.with(|u| {
            u.replace(Some(Rc::new(identity)));
        });
        Self
    }
}

impl Drop for FanoutAclScope {
    fn drop(&mut self) {
        FANOUT_ACL_USER.with(|u| {
            u.replace(None);
        });
    }
}

#[inline]
pub fn fanout_acl_scope_active() -> bool {
    FANOUT_ACL_USER.with(|u| u.borrow().as_ref().is_some_and(|id| !id.name.is_empty()))
}

pub(super) fn get_fanout_user(ctx: &Context) -> Option<String> {
    let user = ctx.get_current_user().to_string();
    if user.is_empty() {
        return None;
    }
    Some(user)
}

/// The [`ModuleUser`] handle [`FanoutContext::lock`](crate::fanout::FanoutContext::lock) already resolved for the
/// GIL hold in progress on this thread, if any. Cloning the result is an `Rc`
/// refcount bump, never a fresh `RM_GetModuleUserFromUserName` call.
pub(crate) fn fanout_module_user() -> Option<Rc<ModuleUser>> {
    FANOUT_ACL_USER.with(|u| u.borrow().as_ref().and_then(|id| id.user.clone()))
}

#[cfg(test)]
mod tests {
    use crate::fanout::{FanoutAclScope, fanout_acl_scope_active};
    use std::thread;

    #[test]
    fn test_fanout_acl_scope_is_thread_local() {
        assert!(!fanout_acl_scope_active());
        let _scope = FanoutAclScope::enter_with_user("test_user");
        assert!(fanout_acl_scope_active());

        let child = thread::spawn(fanout_acl_scope_active)
            .join()
            .expect("thread join failed");
        assert!(!child);
    }
}
