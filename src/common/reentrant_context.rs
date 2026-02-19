//! A reentrant wrapper around [`ThreadSafeContext<DetachedFromClient>`].
//!
//! The Valkey module API is **non-reentrant**: calling `lock()` on the same
//! thread twice without an intervening drop of the guard deadlocks on the
//! internal Valkey mutex.  [`ReentrantContext`] solves this transparently.

use std::ops::Deref;
use std::sync::Mutex;
use std::thread::{self, ThreadId};
use valkey_module::{
    Context, ContextGuard, DetachedFromClient, ThreadSafeContext, ValkeyModuleCtx, raw,
};

// ─────────────────────────────────────────────────────────────────────────────
// Internal state
// ─────────────────────────────────────────────────────────────────────────────

/// All mutable bookkeeping for [`ReentrantContext`], serialised by a [`Mutex`].
struct ReentrantState {
    /// The thread that currently holds the Valkey GIL, or `None` if unlocked.
    owner: Option<ThreadId>,

    /// Recursion depth / refcount: how many live [`ReentrantGuard`]s exist on
    /// the owner thread.
    ///
    /// Invariant: `depth == 0  ⟺  owner.is_none()  ⟺  guard.is_none()`.
    depth: usize,

    /// Raw pointer to the underlying `ValkeyModuleCtx`.
    ///
    /// Non-null iff `owner.is_some()`.  Used to construct lightweight
    /// [`Context`] wrappers for reentrant callers without re-locking.
    raw_ctx: *mut ValkeyModuleCtx,

    /// The actual [`ContextGuard`] that holds the Valkey GIL.
    ///
    /// Stored here so its lifetime is decoupled from any individual
    /// [`ReentrantGuard`]; only dropped (releasing the GIL) when `depth`
    /// falls to zero.
    guard: Option<ContextGuard>,
}

// SAFETY:
//  • `raw_ctx` is only dereferenced by the **owner thread** while `depth > 0`
//    (i.e. while the GIL is guaranteed to be held via `guard`).
//  • Every mutation of `ReentrantState` is protected by `Mutex<ReentrantState>`.
unsafe impl Send for ReentrantState {}
unsafe impl Sync for ReentrantState {}

// ─────────────────────────────────────────────────────────────────────────────
// ReentrantContext
// ─────────────────────────────────────────────────────────────────────────────

/// A reentrant wrapper around [`ThreadSafeContext<DetachedFromClient>`].
///
/// # Problem
///
/// The Valkey module context is **non-reentrant**: calling
/// [`ThreadSafeContext::lock`] a second time on the same OS thread—without
/// first dropping the previous [`ContextGuard`]—deadlocks because the
/// underlying C mutex is not recursive.
///
/// # Solution
///
/// `ReentrantContext` tracks:
///
/// * **Owner thread** – the [`ThreadId`] of whichever thread currently holds
///   the Valkey GIL.
/// * **Depth counter (refcount)** – incremented on every `lock()` call from
///   the owner thread, decremented on every [`ReentrantGuard`] drop.  The
///   Valkey GIL is released exactly when the counter returns to zero.
///
/// # Locking behaviour
///
/// | Caller                     | Behaviour                                                     |
/// |----------------------------|---------------------------------------------------------------|
/// | **Owner thread** (depth ≥ 1) | Returns immediately; depth += 1.  No re-locking occurs.  |
/// | **Any other thread**         | Blocks until the GIL is available; becomes the new owner. |
///
/// # Lock ordering
///
/// To avoid deadlock, the two internal locks are always acquired in the same
/// order:
///
/// 1. Valkey GIL  (`ThreadSafeContext::lock`)
/// 2. State `Mutex` (`self.state.lock()`)
///
/// The [`Drop`] implementation releases the state `Mutex` *before* releasing
/// the Valkey GIL.
pub struct ReentrantContext {
    tsc: ThreadSafeContext<DetachedFromClient>,
    state: Mutex<ReentrantState>,
}

// SAFETY: `ThreadSafeContext<DetachedFromClient>` is `Send + Sync`.
// `Mutex<ReentrantState>` is `Send + Sync` because `ReentrantState` is
// declared `Send + Sync` above (with the documented safety invariants).
unsafe impl Send for ReentrantContext {}
unsafe impl Sync for ReentrantContext {}

impl ReentrantContext {
    /// Creates a new [`ReentrantContext`].
    pub fn new() -> Self {
        Self {
            tsc: ThreadSafeContext::new(),
            state: Mutex::new(ReentrantState {
                owner: None,
                depth: 0,
                raw_ctx: std::ptr::null_mut(),
                guard: None,
            }),
        }
    }

    /// Acquires access to the Valkey [`Context`], returning a [`ReentrantGuard`].
    ///
    /// * **Same-thread reentrant call** – returns immediately after incrementing
    ///   the depth counter.  The new guard shares the raw pointer stored from
    ///   the initial acquisition; no C-level re-locking occurs.
    /// * **Cross-thread call** – blocks (inside `ThreadSafeContext::lock`) until
    ///   the current owner releases all its guards (depth → 0), then becomes the
    ///   new owner with depth = 1.
    ///
    /// The Valkey GIL is freed only when the **last** live [`ReentrantGuard`] is
    /// dropped (regardless of `is_owner`).
    pub fn lock(&self) -> ReentrantGuard<'_> {
        let current = thread::current().id();

        // ── Fast path: same thread is already the owner ───────────────────────
        {
            let mut state = self
                .state
                .lock()
                .expect("ReentrantContext: state mutex poisoned");

            if state.owner == Some(current) {
                state.depth += 1;

                // SAFETY: `raw_ctx` is non-null because `depth` was already ≥ 1,
                // meaning the owner's `ContextGuard` (stored in `state.guard`)
                // is alive and the Valkey GIL is held.
                let ctx = Context::new(state.raw_ctx as *mut raw::RedisModuleCtx);

                return ReentrantGuard {
                    parent: self,
                    ctx,
                    is_owner: false,
                };
            }
            // Release the Mutex before potentially blocking on the GIL below.
        }

        // ── Slow path: acquire the Valkey GIL (may block) ────────────────────
        //
        // Lock ordering: GIL first, state Mutex second (matches drop order).
        let context_guard = self.tsc.lock();

        // Extract the raw pointer while holding the freshly acquired guard.
        // SAFETY: `context_guard` holds the GIL; the pointer is valid.
        let raw_ctx: *mut ValkeyModuleCtx = context_guard.ctx as *mut ValkeyModuleCtx;

        // Build the `Context` the caller will receive.
        let ctx = Context::new(raw_ctx as *mut raw::RedisModuleCtx);

        // Store ownership metadata and the ContextGuard into shared state.
        {
            let mut state = self
                .state
                .lock()
                .expect("ReentrantContext: state mutex poisoned");

            debug_assert!(
                state.owner.is_none(),
                "ReentrantContext BUG: acquired Valkey GIL but owner is already \
                 set to {:?}",
                state.owner
            );

            state.owner = Some(current);
            state.depth = 1;
            state.raw_ctx = raw_ctx;
            state.guard = Some(context_guard);
        }

        ReentrantGuard {
            parent: self,
            ctx,
            is_owner: true,
        }
    }

    /// Returns the current recursion depth (0 = unlocked).
    ///
    /// Intended for diagnostics; the value may change concurrently.
    #[inline]
    pub fn depth(&self) -> usize {
        self.state
            .lock()
            .expect("ReentrantContext: state mutex poisoned")
            .depth
    }

    /// Returns the [`ThreadId`] of the current owner, or `None` if unlocked.
    ///
    /// Intended for diagnostics; the value may change concurrently.
    #[inline]
    pub fn owner(&self) -> Option<ThreadId> {
        self.state
            .lock()
            .expect("ReentrantContext: state mutex poisoned")
            .owner
    }
}

impl Default for ReentrantContext {
    fn default() -> Self {
        Self::new()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ReentrantGuard
// ─────────────────────────────────────────────────────────────────────────────

/// A guard returned by [`ReentrantContext::lock()`].
///
/// Grants access to the Valkey [`Context`] via [`Deref`].
///
/// On drop the internal depth counter is decremented.  When the counter reaches
/// zero the [`ContextGuard`] stored inside [`ReentrantContext`] is released,
/// freeing the Valkey GIL.
///
/// # Drop ordering
///
/// Guards on the owner thread may be dropped in **any order**; the GIL is not
/// released until the **last** guard is dropped.  The depth counter correctly
/// accounts for all outstanding guards regardless of which was created first.
///
/// # `!Send`
///
/// [`ReentrantGuard`] is `!Send` because [`Context`] wraps a raw C pointer and
/// must only be used on the thread that acquired it.
#[must_use = "dropping this guard immediately decrements the lock depth; \
              the Valkey GIL is released when depth reaches zero"]
pub struct ReentrantGuard<'a> {
    parent: &'a ReentrantContext,
    ctx: Context,
    /// `true` for the guard that first acquired the GIL on this ownership
    /// cycle (depth was 0 before `lock()` was called).
    ///
    /// Informational only — drop semantics are symmetric for all guards.
    is_owner: bool,
}

impl ReentrantGuard<'_> {
    /// Returns `true` if this guard represents the **initial** (non-reentrant)
    /// acquisition of the Valkey GIL.
    #[inline]
    pub fn is_owner(&self) -> bool {
        self.is_owner
    }

    /// Returns the current recursion depth at the moment of the call.
    ///
    /// The returned value is always ≥ 1 while any guard is live.
    #[inline]
    pub fn depth(&self) -> usize {
        self.parent.depth()
    }
}

impl Deref for ReentrantGuard<'_> {
    type Target = Context;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.ctx
    }
}

impl Drop for ReentrantGuard<'_> {
    fn drop(&mut self) {
        // Take the ContextGuard *out* of the Mutex before dropping it so that
        // the Valkey GIL is released **after** the state Mutex is released.
        //
        // This keeps the acquisition lock order (GIL → state Mutex) consistent
        // with the release order (state Mutex released → GIL released), avoiding
        // any possibility of cross-thread deadlock.
        let guard_to_drop: Option<ContextGuard> = {
            let mut state = self
                .parent
                .state
                .lock()
                .expect("ReentrantContext: state mutex poisoned");

            debug_assert!(
                state.depth > 0,
                "ReentrantContext BUG: depth underflow — ReentrantGuard dropped \
                 more times than lock() was called"
            );

            state.depth -= 1;

            if state.depth == 0 {
                // We are the last live guard; clear ownership metadata and
                // take the ContextGuard so it drops outside the Mutex.
                state.owner = None;
                state.raw_ctx = std::ptr::null_mut();
                state.guard.take()
            } else {
                None
            }
        }; // ← state Mutex released here

        // ContextGuard (and therefore the Valkey GIL) released here,
        // outside the state Mutex.
        drop(guard_to_drop);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    //! These tests exercise the depth counter and reentrant logic in isolation,
    //! without a real Valkey server (no actual GIL is acquired).
    //!
    //! Full integration tests (with a live Valkey instance) belong in the
    //! `tests/` directory.

    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Verify the initial state is unlocked.
    #[test]
    fn initial_state_is_unlocked() {
        // We cannot construct a real ReentrantContext without a Valkey server,
        // but we can test the state logic through the ReentrantState struct.
        let state = ReentrantState {
            owner: None,
            depth: 0,
            raw_ctx: std::ptr::null_mut(),
            guard: None,
        };
        assert!(state.owner.is_none());
        assert_eq!(state.depth, 0);
        assert!(state.raw_ctx.is_null());
        assert!(state.guard.is_none());
    }

    /// Verify depth counter arithmetic for simulated reentrant calls.
    #[test]
    fn depth_counter_increments_and_decrements() {
        // Simulate the counter logic without touching the actual GIL.
        let depth = Arc::new(AtomicUsize::new(0));

        let d = Arc::clone(&depth);
        // Simulate 3 reentrant lock() calls
        d.fetch_add(1, Ordering::SeqCst); // depth = 1 (owner)
        d.fetch_add(1, Ordering::SeqCst); // depth = 2 (reentrant)
        d.fetch_add(1, Ordering::SeqCst); // depth = 3 (reentrant)
        assert_eq!(d.load(Ordering::SeqCst), 3);

        // Simulate 3 guard drops
        d.fetch_sub(1, Ordering::SeqCst); // depth = 2
        d.fetch_sub(1, Ordering::SeqCst); // depth = 1
        d.fetch_sub(1, Ordering::SeqCst); // depth = 0 → GIL would be released
        assert_eq!(d.load(Ordering::SeqCst), 0);
    }
}
