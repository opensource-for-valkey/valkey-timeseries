//! What kind of thread the caller is. The threading rules (see the [module docs](super)) are
//! stated in terms of it: which threads may block, take the GIL, or keep nested work on
//! their own pool.

use std::cell::Cell;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ThreadRole {
    /// The server's main thread: command handlers, callbacks, the cron. Holds the GIL.
    Main,
    /// A plain thread the module started — an executor worker, a background task, a
    /// feature's processor thread. May block, and may take the GIL.
    Blocking,
    /// A worker of orx's shared pool. Never blocks, never takes the GIL.
    SharedPool,
    /// A worker of a pinned pool whose jobs may wait for a blocking thread's answer (R1's
    /// one exception). Never takes the GIL; its nested parallel work stays on its own pool,
    /// and no GIL holder may wait on it (R2).
    BlockingPool,
    /// A worker of a pinned pool whose jobs never block, isolated so that its work never
    /// depends on a pool that may be parked. Never takes the GIL; its nested parallel work
    /// stays on its own pool.
    IsolatedPool,
    /// A worker of a rayon pool that declared no role: rayon's global pool, which third-party
    /// crates enter on their own (`krcf` under `TS.OUTLIERS METHOD rcf`) and which is not
    /// orx's shared pool, or a pool built in a test. Never blocks, never takes the GIL.
    ForeignPool,
    /// Any other thread: a server I/O thread, a test harness thread.
    Other,
}

impl ThreadRole {
    /// Whether nested parallel work started on this thread stays on its own pool rather
    /// than moving to orx's shared pool.
    pub fn is_pinned(self) -> bool {
        matches!(self, Self::BlockingPool | Self::IsolatedPool)
    }
}

thread_local! {
    /// Set by the module on the threads it starts; `None` everywhere else.
    static ROLE: Cell<Option<ThreadRole>> = const { Cell::new(None) };
}

/// Declares the calling thread's role. Called once, first thing, by each thread the module
/// starts: a pool's `start_handler`, an executor worker, a background thread.
pub fn set_thread_role(role: ThreadRole) {
    ROLE.with(|r| r.set(Some(role)));
}

pub fn current_role() -> ThreadRole {
    if let Some(role) = ROLE.with(Cell::get) {
        return role;
    }
    if crate::is_main_thread() {
        return ThreadRole::Main;
    }
    if rayon_core::current_thread_index().is_some() {
        return if orx_parallel::Pool::global()
            .current_thread_index()
            .is_some()
        {
            ThreadRole::SharedPool
        } else {
            ThreadRole::ForeignPool
        };
    }
    ThreadRole::Other
}

/// Whether the caller is a worker of the pool its nested parallel work should stay on.
/// Cheaper than [`current_role`]: this is on the path of every parallel iteration.
#[inline]
pub(super) fn on_pinned_worker() -> bool {
    ROLE.with(Cell::get).is_some_and(ThreadRole::is_pinned)
        && rayon_core::current_thread_index().is_some()
}

/// Whether the caller is a worker of any rayon pool — itself part of some fan-out, so a
/// nested one costs more than it parallelizes.
#[inline]
pub fn on_pool_worker() -> bool {
    rayon_core::current_thread_index().is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roles_of_unmarked_threads() {
        assert_eq!(current_role(), ThreadRole::Other);
        let orx = orx_parallel::Pool::global().install(current_role);
        assert_eq!(orx, ThreadRole::SharedPool);
        let foreign = rayon_core::ThreadPoolBuilder::new()
            .num_threads(1)
            .build()
            .unwrap()
            .install(current_role);
        assert_eq!(foreign, ThreadRole::ForeignPool);
    }

    #[test]
    fn a_declared_role_wins() {
        let pool = rayon_core::ThreadPoolBuilder::new()
            .num_threads(1)
            .start_handler(|_| set_thread_role(ThreadRole::IsolatedPool))
            .build()
            .unwrap();
        assert_eq!(pool.install(current_role), ThreadRole::IsolatedPool);
        assert!(pool.install(on_pinned_worker));
        let role = std::thread::spawn(|| {
            set_thread_role(ThreadRole::Blocking);
            (current_role(), on_pinned_worker())
        })
        .join()
        .unwrap();
        assert_eq!(role, (ThreadRole::Blocking, false));
    }
}
