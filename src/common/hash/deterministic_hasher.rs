use ahash::{AHasher, RandomState};
use core::hash::{BuildHasher, Hasher};

/// A deterministic `ahash` hasher which can be used both directly as a
/// [`Hasher`] and as a [`BuildHasher`] for hash collections.
///
/// `RandomState` is retained as the builder so every hasher produced for a
/// collection starts with the same fixed seeds. The second field is the state
/// used when this type is passed directly to `Hash::hash` (for example when a
/// fingerprint is calculated).
pub struct DeterministicHasher(RandomState, AHasher);

impl Default for DeterministicHasher {
    fn default() -> Self {
        let state = RandomState::with_seeds(0, 0, 0, 0);
        let hasher = state.build_hasher();
        Self(state, hasher)
    }
}

impl BuildHasher for DeterministicHasher {
    type Hasher = AHasher;

    fn build_hasher(&self) -> Self::Hasher {
        self.0.build_hasher()
    }
}

impl Hasher for DeterministicHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.1.finish()
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        self.1.write(bytes);
    }
}

impl DeterministicHasher {
    pub fn new() -> Self {
        Self::default()
    }
}
