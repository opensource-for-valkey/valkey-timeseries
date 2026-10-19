use crate::common::acl::AclContextGuard;
use crate::common::context::set_current_db;
use crate::common::threads::{GilSource, LockGil};
use std::sync::OnceLock;
use std::time::Instant;
use valkey_module::{DetachedContext, MODULE_CONTEXT, Status, ValkeyError, ValkeyResult};

/// The shard-local side of one fan-out request: the ACL user it executes as,
/// the database it targets, and the module's detached context used to take
/// the GIL.
///
/// Built by the two fan-out entry points (the coordinator's own shard and the
/// cluster RPC receiver) and handed to `FanoutCommand::get_local_response`,
/// which runs on a worker thread with the GIL *not* held. The analysis lane
/// builds one too, for a standalone label search moved off the main thread: it
/// carries the calling client's ACL name and selected database the same way. [`FanoutContext::lock`]
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
    /// When the requester stops waiting, if it said ([`Self::set_deadline`]).
    deadline: OnceLock<Instant>,
}

impl FanoutContext {
    /// `user` is the requesting client's ACL name — the coordinator-side client
    /// for a fan-out share, the caller for a background label search — with
    /// `None`/empty meaning no enforcement; `db` the database the request runs
    /// against.
    pub fn new(user: Option<String>, db: i32) -> Self {
        let user = user.filter(|name| !name.is_empty());
        Self {
            user,
            db,
            ctx: &MODULE_CONTEXT,
            deadline: OnceLock::new(),
        }
    }

    /// Record when the requester stops waiting for this answer. Reads walking the
    /// keyspace for it stop between batches from then on ([`Self::expired`]). Only the
    /// first call counts.
    ///
    /// Nothing on this branch sets one yet: the PromQL fan-out does, from its
    /// request's time budget. Until then [`Self::expired`] is always false.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn set_deadline(&self, deadline: Instant) {
        let _ = self.deadline.set(deadline);
    }

    /// Whether the requester has stopped waiting: its answer would arrive after it gave up.
    pub fn expired(&self) -> bool {
        self.deadline
            .get()
            .is_some_and(|deadline| Instant::now() >= *deadline)
    }

    /// The ACL user the request executes as, if enforcement applies.
    #[cfg(test)]
    pub fn user(&self) -> Option<&str> {
        self.user.as_deref()
    }

    /// The database the request runs against.
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
    pub fn lock(&self) -> ValkeyResult<AclContextGuard> {
        let ctx = self.ctx.lock_gil();

        if set_current_db(&ctx, self.db) == Status::Err {
            return Err(ValkeyError::String(format!(
                "failed to select database {}",
                self.db
            )));
        }
        // Resolved afresh on every lock: a user deleted or disabled mid-request fails the
        // next step rather than finishing with rights it no longer has.
        AclContextGuard::for_user(self.user.as_deref(), ctx)
    }
}

/// A shard-local fan-out share: each lock re-selects the request's database and re-installs
/// its ACL identity. A background thread reads a given database through `MODULE_CONTEXT` the
/// same way: with the caller's user for a background label search, with none for internal
/// work.
impl GilSource for FanoutContext {
    type Guard<'s> = AclContextGuard;

    fn lock(&self) -> ValkeyResult<AclContextGuard> {
        FanoutContext::lock(self)
    }

    fn db(&self) -> i32 {
        FanoutContext::db(self)
    }

    fn expired(&self) -> bool {
        FanoutContext::expired(self)
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
    fn test_fanout_context_expires_only_with_a_deadline_in_the_past() {
        let ctx = FanoutContext::new(None, 0);
        assert!(!ctx.expired(), "no deadline: never expires");
        ctx.set_deadline(Instant::now() + std::time::Duration::from_secs(60));
        assert!(!ctx.expired());
        ctx.set_deadline(Instant::now());
        assert!(!ctx.expired(), "only the first deadline counts");

        let past = FanoutContext::new(None, 0);
        past.set_deadline(Instant::now());
        assert!(past.expired());
    }

    #[test]
    fn test_fanout_context_collapses_empty_user() {
        let ctx = FanoutContext::new(Some(String::new()), 0);
        assert!(ctx.user().is_none());
        assert_eq!(ctx.db(), 0);
    }
}
