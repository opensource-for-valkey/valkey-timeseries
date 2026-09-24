use crate::common::context::set_current_db;
use crate::fanout::{FanoutAclScope, FanoutIdentity};
use crate::series::acl::ModuleUser;
use std::ops::Deref;
use std::rc::Rc;
use valkey_module::{
    Context, DetachedContext, DetachedContextGuard, MODULE_CONTEXT, Status, ValkeyError,
    ValkeyResult,
};

/// The GIL, held for one step of a shard-local fan-out request.
///
/// Dereferences to [`Context`]. While it is alive, the request's database is
/// selected, and its ACL identity is active on this thread; both are torn down
/// when it drops, before the GIL is released. Like [`DetachedContextGuard`],
/// it has no client behind it and must not be used to send replies.
pub struct FanoutContextGuard {
    // Declared first so the resolved `ModuleUser` handle is freed under the lock.
    _acl: Option<FanoutAclScope>,
    ctx: DetachedContextGuard,
}

impl Deref for FanoutContextGuard {
    type Target = Context;

    fn deref(&self) -> &Context {
        &self.ctx
    }
}

/// The shard-local side of one fan-out request: the ACL user it executes as,
/// the database it targets, and the module's detached context used to take
/// the GIL.
///
/// Built by the two fan-out entry points (the coordinator's own shard and the
/// cluster RPC receiver) and handed to `FanoutCommand::get_local_response`,
/// which runs on a worker thread with the GIL *not* held. [`FanoutContext::lock`]
/// is the only way that code should take it: the detached context is shared by
/// every worker, so the request's database has to be re-selected on each
/// acquisition, and the [`ModuleUser`] handle the ACL checks rely on is only
/// valid while the lock that resolved it is held.
///
/// # Invariant
/// The ACL identity a lock installs is thread-local, so a request must take
/// every GIL it needs on the thread that owns the `FanoutContext`.
pub struct FanoutContext {
    user: Option<String>,
    db: i32,
    ctx: &'static DetachedContext,
}

impl FanoutContext {
    /// `user` is the coordinator-side client's ACL name (`None`/empty = no
    /// enforcement), `db` the database the request runs against.
    pub fn new(user: Option<String>, db: i32) -> Self {
        let user = user.filter(|name| !name.is_empty());
        Self {
            user,
            db,
            ctx: &MODULE_CONTEXT,
        }
    }

    /// The ACL user the request executes as, if enforcement applies.
    #[cfg(test)]
    pub fn user(&self) -> Option<&str> {
        self.user.as_deref()
    }

    /// The database the request runs against.
    #[cfg(test)]
    pub fn db(&self) -> i32 {
        self.db
    }

    /// Take the GIL for a step of this request.
    ///
    /// Keep the guard's scope as small as the work allows — decode the request
    /// before locking and encode the response after releasing, so the server
    /// stays responsive while the shard-local reply is materialized.
    ///
    /// Fails when the request's database cannot be selected or its ACL user no
    /// longer exists, so a request cannot silently run against the wrong
    /// database or without enforcement.
    pub fn lock(&self) -> ValkeyResult<FanoutContextGuard> {
        let ctx = self.ctx.lock();

        if set_current_db(&ctx, self.db) == Status::Err {
            return Err(ValkeyError::String(format!(
                "failed to select database {}",
                self.db
            )));
        }

        let acl = match &self.user {
            Some(user) => {
                let user_name = ctx.create_string(user.as_str());
                let module_user = ModuleUser::from_name(&user_name).ok_or_else(|| {
                    ValkeyError::String(format!("ACL user '{user}' does not exist or is disabled"))
                })?;
                Some(FanoutAclScope::enter(FanoutIdentity {
                    name: user.clone(),
                    user: Some(Rc::new(module_user)),
                }))
            }
            None => None,
        };

        Ok(FanoutContextGuard { _acl: acl, ctx })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fanout_context_keeps_user_and_db() {
        let ctx = FanoutContext::new(Some("alice".to_owned()), 3);
        assert_eq!(ctx.user(), Some("alice"));
        assert_eq!(ctx.db(), 3);
    }

    #[test]
    fn test_fanout_context_collapses_empty_user() {
        let ctx = FanoutContext::new(Some(String::new()), 0);
        assert!(ctx.user().is_none());
        assert_eq!(ctx.db(), 0);
    }
}
