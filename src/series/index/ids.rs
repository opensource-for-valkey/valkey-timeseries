//! Timeseries ID generation.
//!
//! IDs are 64-bit values laid out as `[ epoch: 24 bits | counter: 40 bits ]`.
//!
//! The epoch is drawn at random once per process start; the counter is a
//! plain atomic increment. This gives:
//!
//! - **Cluster-wide uniqueness (probabilistic)**: two nodes collide only if
//!   they draw the same 24-bit epoch. Collisions are detected and remapped
//!   at the single point where two ID spaces merge — slot import (see
//!   `reindex.rs`) — so uniqueness only needs to be rare-failure, not
//!   absolute.
//! - **Dense postings bitmaps**: all IDs minted by one process share their
//!   high 24 bits and increment in the low bits, so roaring containers in
//!   the postings index fill completely before a new one opens — the same
//!   compression profile as a bare sequential counter.
//! - **Trivial "reset" semantics**: nothing is persisted, so there is no
//!   stale counter to detect after a crash, a copied RDB file, a VM clone,
//!   or a replica promotion. Every process start is a reset by construction.
//!
//! Epoch `0` is never drawn so no generated ID can collide with legacy
//! dense `AtomicU64`-era IDs (small integers), and no ID is ever `0`
//! (`id == 0` means "unassigned" elsewhere). Epoch `MAX` is also excluded
//! so a counter wrap — which carries into the epoch bits — cannot land on
//! epoch `0`. A wrap requires exhausting 2^40 (~1.1 trillion) IDs in one
//! process lifetime; the carried epoch is as uniformly random as a fresh
//! draw, so wrapping is harmless anyway.
#[allow(dead_code)]
use crate::series::TimeseriesId;
use rand::{RngExt, rng};
use std::sync::atomic::{AtomicU64, Ordering};

pub const EPOCH_BITS: u32 = 24;
pub const COUNTER_BITS: u32 = 40;

const EPOCH_MASK: u64 = (1 << EPOCH_BITS) - 1;
const COUNTER_MASK: u64 = (1 << COUNTER_BITS) - 1;

/// Generates unique timeseries IDs.
///
/// The packed `[epoch | counter]` state lives in a single atomic so ID
/// generation is one wait-free `fetch_add`; a counter wrap carries into the
/// epoch bits, so there is no epoch/counter tearing under contention.
pub struct IdGenerator {
    /// The last issued ID; `fetch_add(1)` issues the next one.
    state: AtomicU64,
}

// Module-level generator for the free-function API
static DEFAULT_GENERATOR: std::sync::LazyLock<IdGenerator> =
    std::sync::LazyLock::new(IdGenerator::new);

impl IdGenerator {
    pub fn new() -> Self {
        Self::with_epoch(mint_epoch())
    }

    /// Create a generator with a fixed epoch. The epoch is truncated to
    /// [EPOCH_BITS]; the counter starts at zero, so the first ID issued is
    /// `epoch << COUNTER_BITS | 1`.
    pub fn with_epoch(epoch: u64) -> Self {
        IdGenerator {
            state: AtomicU64::new((epoch & EPOCH_MASK) << COUNTER_BITS),
        }
    }

    #[cfg(test)]
    fn with_parts(epoch: u64, counter: u64) -> Self {
        IdGenerator {
            state: AtomicU64::new(
                ((epoch & EPOCH_MASK) << COUNTER_BITS) | (counter & COUNTER_MASK),
            ),
        }
    }

    pub fn next_id(&self) -> TimeseriesId {
        self.state.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub fn generate(&self) -> TimeseriesId {
        self.next_id()
    }

    pub fn epoch(&self) -> u32 {
        extract_epoch(self.state.load(Ordering::Relaxed))
    }

    pub fn extract_epoch(id: TimeseriesId) -> u32 {
        extract_epoch(id)
    }

    pub fn extract_counter(id: TimeseriesId) -> u64 {
        extract_counter(id)
    }
}

impl Default for IdGenerator {
    fn default() -> Self {
        Self::new()
    }
}

/// Generate a unique ID using the default generator
pub fn generate() -> TimeseriesId {
    DEFAULT_GENERATOR.next_id()
}

pub fn next_timeseries_id() -> TimeseriesId {
    generate()
}

pub fn extract_epoch(id: TimeseriesId) -> u32 {
    (id >> COUNTER_BITS) as u32
}

pub fn extract_counter(id: TimeseriesId) -> u64 {
    id & COUNTER_MASK
}

/// Draw the process-lifetime epoch. `0` is reserved (legacy dense IDs and
/// the unassigned sentinel); `MAX` is excluded so a counter-wrap carry
/// cannot produce epoch `0`.
fn mint_epoch() -> u64 {
    let mut rng = rng();
    loop {
        let epoch = rng.random::<u64>() & EPOCH_MASK;
        if epoch != 0 && epoch != EPOCH_MASK {
            return epoch;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_id_generation() {
        let generator = IdGenerator::with_epoch(42);
        let id1 = generator.next_id();
        let id2 = generator.next_id();

        assert_ne!(id1, id2, "IDs should be unique");
        assert!(id2 > id1, "IDs should be strictly increasing");
    }

    #[test]
    fn test_first_id_is_counter_one() {
        let generator = IdGenerator::with_epoch(7);
        let id = generator.next_id();

        assert_eq!(extract_epoch(id), 7);
        assert_eq!(extract_counter(id), 1);
        assert_ne!(id, 0, "no generated ID may be the unassigned sentinel");
    }

    #[test]
    fn test_epoch_truncation() {
        let generator = IdGenerator::with_epoch(u64::MAX);
        assert_eq!(generator.epoch() as u64, EPOCH_MASK);
    }

    #[test]
    fn test_extract_roundtrip() {
        let generator = IdGenerator::with_epoch(0x00AB_CDEF);
        let id = generator.next_id();

        assert_eq!(extract_epoch(id), 0x00AB_CDEF);
        assert_eq!(extract_counter(id), 1);
        assert_eq!(IdGenerator::extract_epoch(id), extract_epoch(id));
        assert_eq!(IdGenerator::extract_counter(id), extract_counter(id));
    }

    #[test]
    fn test_minted_epoch_bounds() {
        for _ in 0..64 {
            let epoch = mint_epoch();
            assert_ne!(epoch, 0, "epoch 0 is reserved for legacy IDs");
            assert_ne!(epoch, EPOCH_MASK, "epoch MAX would carry into 0");
            assert!(epoch <= EPOCH_MASK);
        }
    }

    #[test]
    fn test_default_generator_epoch() {
        let id = generate();
        let epoch = extract_epoch(id) as u64;

        assert_ne!(epoch, 0);
        assert_ne!(epoch, EPOCH_MASK);
    }

    #[test]
    fn test_counter_wrap_carries_into_epoch() {
        let generator = IdGenerator::with_parts(5, COUNTER_MASK - 1);

        let id1 = generator.next_id();
        assert_eq!(extract_epoch(id1), 5);
        assert_eq!(extract_counter(id1), COUNTER_MASK);

        let id2 = generator.next_id();
        assert_eq!(
            extract_epoch(id2),
            6,
            "counter wrap should carry into epoch"
        );
        assert_eq!(extract_counter(id2), 0);
        assert!(id2 > id1, "IDs remain strictly increasing across a wrap");
    }

    #[test]
    fn test_concurrent_generation() {
        use std::sync::Arc;
        use std::thread;

        let generator = Arc::new(IdGenerator::with_epoch(1));
        let mut handles = vec![];
        let mut ids = std::collections::HashSet::new();

        for _ in 0..10 {
            let gen_ = Arc::clone(&generator);
            handles.push(thread::spawn(move || {
                (0..100).map(|_| gen_.next_id()).collect::<Vec<_>>()
            }));
        }

        for handle in handles {
            let thread_ids = handle.join().unwrap();
            for id in thread_ids {
                assert!(!ids.contains(&id), "Duplicate ID found: {}", id);
                ids.insert(id);
            }
        }

        assert_eq!(ids.len(), 1000, "Should have 1000 unique IDs");
    }

    #[test]
    fn test_free_functions() {
        let id1 = generate();
        let id2 = next_timeseries_id();

        assert_ne!(id1, id2, "Generated IDs should be unique");
        assert_eq!(
            extract_epoch(id1),
            extract_epoch(id2),
            "default generator epoch is fixed for the process lifetime"
        );
        assert!(id2 > id1, "default generator IDs are strictly increasing");
    }
}
