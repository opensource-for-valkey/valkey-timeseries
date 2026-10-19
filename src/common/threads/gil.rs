//! The one way to take the GIL (the module lock), and the checks that keep a thread from
//! waiting on work that needs the GIL it holds. The rules are in the [module docs](super).
//!
//! A broken rule is a deadlock waiting for the right load, so the checks run in release
//! builds too: each is a thread-local read next to a far costlier lock or wait. A debug
//! build (the unit tests) panics; a release build logs the first violation of each rule
//! with a backtrace and carries on.

use super::role::{ThreadRole, current_role, on_pool_worker};
use crate::common::context::get_current_db;
use crate::common::logging::log_warning;
use std::backtrace::Backtrace;
use std::cell::Cell;
use std::ffi::{CStr, c_int};
use std::marker::PhantomData;
use std::ops::Deref;
use std::panic::Location;
use std::sync::atomic::{AtomicBool, Ordering};
use valkey_module::{
    Context, DetachedContext, DetachedContextGuard, ValkeyLockIndicator, ValkeyResult, raw,
};

thread_local! {
    /// Whether this thread holds a [`GilToken`]. The main thread holds the GIL without one.
    static HOLDS_GIL: Cell<bool> = const { Cell::new(false) };
}

/// Marks the calling thread as holding the GIL for as long as it lives.
///
/// Every guard that takes the GIL owns one: [`GilGuard`] here, and any other guard over a
/// raw lock call (a blocked client's thread-safe context, say), which takes it with
/// [`GilToken::take`] so that it is checked exactly like [`LockGil::lock_gil`]. Not `Send`:
/// the mark is thread-local.
pub struct GilToken {
    _not_send: PhantomData<*const ()>,
}

impl GilToken {
    /// Checks that this thread may take the GIL (R1, and no re-entry, which never returns:
    /// the GIL is a plain mutex), then runs `lock`, which must take it, and marks the thread.
    ///
    /// Declare the token before the lock guard in a struct, so the mark is cleared just
    /// before the lock is released: slightly early, never late.
    #[track_caller]
    pub fn take<G>(lock: impl FnOnce() -> G) -> (Self, G) {
        if on_pool_worker() {
            violated(Rule::PoolWorkerTakesGil);
        } else if holds_gil() {
            violated(Rule::GilReentry);
        }
        let guard = lock();
        HOLDS_GIL.with(|h| h.set(true));
        (
            Self {
                _not_send: PhantomData,
            },
            guard,
        )
    }
}

impl Drop for GilToken {
    fn drop(&mut self) {
        HOLDS_GIL.with(|h| h.set(false));
    }
}

/// The GIL, taken with [`LockGil::lock_gil`]. Dereferences to [`Context`]; released on drop.
pub struct GilGuard {
    // Declared first: dropped, and the mark cleared, before `ctx` unlocks.
    _token: GilToken,
    ctx: DetachedContextGuard,
}

// SAFETY: a `GilGuard` exists only while the GIL is held, as a `DetachedContextGuard` does.
unsafe impl ValkeyLockIndicator for GilGuard {}

impl Deref for GilGuard {
    type Target = Context;

    fn deref(&self) -> &Context {
        &self.ctx
    }
}

/// Takes the GIL through a detached context — `MODULE_CONTEXT.lock_gil()` — checking first
/// that this thread may. Use it instead of `DetachedContext::lock`, which clippy rejects.
pub trait LockGil {
    fn lock_gil(&self) -> GilGuard;
}

impl LockGil for DetachedContext {
    #[track_caller]
    fn lock_gil(&self) -> GilGuard {
        let (token, ctx) = GilToken::take(|| self.lock());
        GilGuard { _token: token, ctx }
    }
}

/// Where a multi-step read gets the module lock from, one batch at a time.
pub trait GilSource {
    /// The lock for one batch; the series opened under it live no longer than it does.
    type Guard<'s>: Deref<Target = Context>
    where
        Self: 's;

    /// Take the lock for one batch.
    fn lock(&self) -> ValkeyResult<Self::Guard<'_>>;

    /// The database the read runs against.
    fn db(&self) -> i32;

    /// Whether the lock can be given back between batches. A caller that already holds it
    /// (the main thread) cannot, and is read in a single batch.
    fn can_release(&self) -> bool {
        true
    }

    /// Whether whoever asked for the read has stopped waiting for it, so the next batch
    /// would be read for nobody. Checked before each batch after the first.
    fn expired(&self) -> bool {
        false
    }
}

/// A caller already holding the lock: the read is one batch, as `series_by_selectors` is.
impl GilSource for Context {
    type Guard<'s> = &'s Context;

    fn lock(&self) -> ValkeyResult<&Context> {
        Ok(self)
    }

    fn db(&self) -> i32 {
        get_current_db(self)
    }

    fn can_release(&self) -> bool {
        false
    }
}

/// Whether the calling thread holds the GIL: the main thread, or a live [`GilToken`].
pub fn holds_gil() -> bool {
    HOLDS_GIL.with(Cell::get) || current_role() == ThreadRole::Main
}

/// R3: the caller is about to block until another thread answers. Only a blocking thread or
/// a blocking-pool worker may, and never while holding the GIL: whoever answers may need it.
#[track_caller]
pub fn check_may_block() {
    let role = current_role();
    if HOLDS_GIL.with(Cell::get)
        || matches!(
            role,
            ThreadRole::Main | ThreadRole::SharedPool | ThreadRole::IsolatedPool
        )
    {
        violated(Rule::BlockingWait);
    }
}

/// R6: the caller is about to do work only a GIL holder may do — read or free server state
/// the GIL protects, such as entering or dropping an ACL scope, whose user handle points into
/// the server's ACL table. For work nothing in the type system ties to a lock guard.
#[track_caller]
pub fn check_holds_gil() {
    if !holds_gil() {
        violated(Rule::GilRequired);
    }
}

/// What the server may do during [`yield_gil`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum YieldMode {
    /// Background work only: client commands wait until the caller finishes.
    Events,
    /// Client commands too, but only those flagged `allow-busy` (`AUTH`, `MULTI`, `SHUTDOWN`
    /// and a few more) run; every other command is refused with `-BUSY`. No `TS.*` command
    /// is `allow-busy`, and neither is anything that deletes or writes a key.
    Clients,
}

/// Lets the server process events while this thread holds the GIL through a long operation:
/// a wrapper over `RedisModule_Yield`. Call it periodically from the operation's loop.
///
/// `ctx` is the context the GIL is held through: the running command's context on the main
/// thread, or a thread-safe context while locked. `busy_reply` replaces the text after
/// `-BUSY` in the replies refused under [`YieldMode::Clients`] (`None` keeps the server's own
/// text); the server reads it only during the call.
///
/// The server yields only once `busy-reply-threshold` (default 5 s) has passed since `ctx` was
/// created, and then at most once per threshold; every other call returns after reading a
/// clock, so calling often is cheap. A context created long ago, such as the module's
/// detached context, yields on its first call. A call made during another yield returns at
/// once. While loading, the server processes events on its own schedule and refuses commands
/// with `-LOADING`.
///
/// What may change across the call depends on the thread:
/// - On the main thread, the server processes pending client I/O, and under
///   [`YieldMode::Clients`] runs the `allow-busy` commands above. No timer runs: the
///   module's cron and fan-out timeouts are time events, which a yield does not process.
/// - Off the main thread, the server may release the GIL, let the main thread run, and take
///   the GIL back before returning. Anything read under the GIL before the call — a series
///   pointer, a key handle, an index lookup — must be read again afterwards.
#[track_caller]
pub fn yield_gil(ctx: &Context, mode: YieldMode, busy_reply: Option<&CStr>) {
    check_holds_gil();
    let flags = match mode {
        YieldMode::Events => raw::REDISMODULE_YIELD_FLAG_NONE,
        YieldMode::Clients => raw::REDISMODULE_YIELD_FLAG_CLIENTS,
    };
    let busy_reply = busy_reply.map_or(std::ptr::null(), CStr::as_ptr);
    let yield_fn = unsafe { raw::RedisModule_Yield }.expect("RedisModule_Yield is not available");
    // SAFETY: `ctx` is live, and this thread holds the GIL (checked above), as the call
    // requires. `busy_reply` is null or a C string that outlives the call, after which the
    // server no longer reads it.
    unsafe { yield_fn(ctx.ctx, flags as c_int, busy_reply) }
}

/// R2: the caller is about to wait on a blocking pool, whose workers park on blocking
/// threads, which may need the GIL. A GIL holder waiting there can wait forever.
#[track_caller]
pub fn check_may_wait_on_blocking_pool() {
    if holds_gil() {
        violated(Rule::GilHolderWaitsOnBlockingPool);
    }
}

#[derive(Clone, Copy, Debug)]
enum Rule {
    PoolWorkerTakesGil,
    GilReentry,
    GilHolderWaitsOnBlockingPool,
    BlockingWait,
    GilRequired,
}

impl Rule {
    const COUNT: usize = 5;

    fn describe(self) -> &'static str {
        match self {
            Self::PoolWorkerTakesGil => {
                "R1: a pool worker took the GIL; a GIL holder waiting on the same pool can never \
                 get the worker back. Run the job on a BoundedExecutor or spawn_background"
            }
            Self::GilReentry => {
                "the GIL was taken by a thread that already holds it, which never returns"
            }
            Self::GilHolderWaitsOnBlockingPool => {
                "R2: a GIL holder waited on a blocking pool, whose jobs wait on threads that may \
                 need the GIL"
            }
            Self::BlockingWait => {
                "R3: a thread that must not block waited for another thread's answer; only \
                 blocking threads and blocking-pool workers may, and not while holding the GIL"
            }
            Self::GilRequired => {
                "R6: work that needs the GIL ran on a thread that does not hold it"
            }
        }
    }
}

static REPORTED: [AtomicBool; Rule::COUNT] = [const { AtomicBool::new(false) }; Rule::COUNT];

#[cold]
#[track_caller]
fn violated(rule: Rule) {
    let message = format!(
        "threading rule broken at {} on a {:?} thread: {}",
        Location::caller(),
        current_role(),
        rule.describe()
    );
    if cfg!(debug_assertions) {
        panic!("{message}");
    }
    if !REPORTED[rule as usize].swap(true, Ordering::Relaxed) {
        log_warning(format!("{message}\n{}", Backtrace::force_capture()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::threads::set_thread_role;

    fn panics(f: impl FnOnce() + std::panic::UnwindSafe) -> bool {
        std::panic::catch_unwind(f).is_err()
    }

    /// Marks this thread as holding the GIL without a server to lock.
    fn fake_gil() -> GilToken {
        GilToken::take(|| ()).0
    }

    #[test]
    fn a_token_marks_the_thread_until_dropped() {
        std::thread::spawn(|| {
            assert!(!holds_gil());
            let token = fake_gil();
            assert!(holds_gil());
            drop(token);
            assert!(!holds_gil());
        })
        .join()
        .unwrap();
    }

    #[test]
    fn taking_the_gil_twice_is_refused() {
        std::thread::spawn(|| {
            let _gil = fake_gil();
            assert!(panics(|| drop(fake_gil())));
        })
        .join()
        .unwrap();
    }

    #[test]
    fn pool_workers_may_not_take_the_gil() {
        let taken = orx_parallel::Pool::global().install(|| panics(|| drop(fake_gil())));
        assert!(taken);
    }

    #[test]
    fn blocking_threads_may_block_unless_they_hold_the_gil() {
        std::thread::spawn(|| {
            set_thread_role(ThreadRole::Blocking);
            check_may_block();
            let _gil = fake_gil();
            assert!(panics(check_may_block));
        })
        .join()
        .unwrap();
    }

    #[test]
    fn non_blocking_pools_may_not_block() {
        let shared = orx_parallel::Pool::global().install(|| panics(check_may_block));
        assert!(shared);
        let isolated = rayon_core::ThreadPoolBuilder::new()
            .num_threads(1)
            .start_handler(|_| set_thread_role(ThreadRole::IsolatedPool))
            .build()
            .unwrap();
        assert!(isolated.install(|| panics(check_may_block)));
        let blocking = rayon_core::ThreadPoolBuilder::new()
            .num_threads(1)
            .start_handler(|_| set_thread_role(ThreadRole::BlockingPool))
            .build()
            .unwrap();
        assert!(!blocking.install(|| panics(check_may_block)));
    }

    #[test]
    fn yielding_requires_the_gil() {
        std::thread::spawn(|| {
            // Refused before the server is called: the context is never read.
            let ctx = Context::new(std::ptr::null_mut());
            let payload = std::panic::catch_unwind(|| yield_gil(&ctx, YieldMode::Events, None))
                .expect_err("yielding without the GIL is a broken rule");
            let message = crate::common::threads::panic_message(payload.as_ref());
            assert!(message.contains("R6"), "{message}");
        })
        .join()
        .unwrap();
    }

    #[test]
    fn gil_holders_may_not_wait_on_a_blocking_pool() {
        std::thread::spawn(|| {
            check_may_wait_on_blocking_pool();
            let _gil = fake_gil();
            assert!(panics(check_may_wait_on_blocking_pool));
        })
        .join()
        .unwrap();
    }
}
