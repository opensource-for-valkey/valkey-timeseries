//! Telling a blocked client's background work that the client has gone.
//!
//! A command that blocks its client and answers from another thread (`TS.QUERY`,
//! `TS.QUERYRANGE`) would otherwise run to completion after its client disconnected,
//! holding a worker for up to its whole `TIMEOUT` for a reply the server discards.
//!
//! The server reports the disconnect through a callback on the blocked-client handle
//! ([`BlockedClient::cancel_on_disconnect`](super::BlockedClient::cancel_on_disconnect)). The
//! callback runs on the main thread and only sets a flag here; the work reads it through a
//! [`CancelToken`] wherever it already checks its deadline, and stops there.
//!
//! The flag is keyed by the handle's address, which the server may reuse for a later blocked
//! client once this one is unblocked. So the callback only ever *sets* a registered flag,
//! never inserts one, and the registration is removed before the client is unblocked: an
//! address is registered for at most one live handle at a time.

use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, Ordering};

type Registry = papaya::HashMap<u64, AtomicBool, ahash::RandomState>;

static REGISTRY: LazyLock<Registry> = LazyLock::new(Registry::default);

/// What a background job checks to learn that its client disconnected. `Copy`, so it can
/// travel inside options that are passed by value; the default token is never cancelled.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CancelToken(u64);

impl CancelToken {
    /// Whether the client this token was issued for has disconnected.
    ///
    /// A lookup in a lock-free map, cheap enough for every deadline check. A token outliving
    /// its registration (work abandoned after its query ended) reads as not cancelled, or as
    /// a later registration at the same address: either way it only affects work whose
    /// result nobody is waiting for.
    pub fn is_cancelled(self) -> bool {
        self.0 != 0
            && REGISTRY
                .pin()
                .get(&self.0)
                .is_some_and(|flag| flag.load(Ordering::Acquire))
    }
}

/// A live registration for one blocked client; removed when dropped.
#[derive(Debug)]
pub(super) struct Registration(u64);

impl Registration {
    /// Register `key` (the blocked-client handle's address). The key must not be registered
    /// already, which holds as long as each registration is dropped before its handle is
    /// released to the server.
    pub(super) fn new(key: u64) -> Self {
        debug_assert_ne!(key, 0, "a null handle cannot be registered");
        REGISTRY.pin().insert(key, AtomicBool::new(false));
        Self(key)
    }

    pub(super) fn token(&self) -> CancelToken {
        CancelToken(self.0)
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        REGISTRY.pin().remove(&self.0);
    }
}

/// Record that the client registered under `key` disconnected. A key with no registration
/// (its work already finished) is ignored rather than inserted: see the module docs.
pub(super) fn mark_disconnected(key: u64) {
    if let Some(flag) = REGISTRY.pin().get(&key) {
        flag.store(true, Ordering::Release);
    }
}

/// A registered client for tests elsewhere in the crate: no server, no handle.
#[cfg(test)]
pub(crate) struct TestClient(Registration);

#[cfg(test)]
impl TestClient {
    pub(crate) fn new() -> Self {
        use std::sync::atomic::AtomicU64;
        // Odd, so never a real (aligned) handle address, and distinct per client.
        static NEXT: AtomicU64 = AtomicU64::new(0x7e57_0000_0001);
        Self(Registration::new(NEXT.fetch_add(2, Ordering::Relaxed)))
    }

    pub(crate) fn token(&self) -> CancelToken {
        self.0.token()
    }

    pub(crate) fn disconnect(&self) {
        mark_disconnected(self.0.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Keys well away from any real address, distinct per test so they can run in parallel.

    #[test]
    fn a_registered_client_is_cancelled_once_it_disconnects() {
        let registration = Registration::new(0xdead_0001);
        let token = registration.token();
        assert!(!token.is_cancelled());
        mark_disconnected(0xdead_0001);
        assert!(token.is_cancelled());
    }

    #[test]
    fn a_disconnect_after_the_work_finished_is_ignored() {
        let token = Registration::new(0xdead_0002).token();
        // The registration is gone: the callback must not resurrect it, or a later client
        // blocked at the same address would start out cancelled.
        mark_disconnected(0xdead_0002);
        assert!(!token.is_cancelled());
        let reused = Registration::new(0xdead_0002);
        assert!(!reused.token().is_cancelled());
    }

    #[test]
    fn the_default_token_is_never_cancelled() {
        mark_disconnected(0);
        assert!(!CancelToken::default().is_cancelled());
    }
}
