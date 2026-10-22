//! `string_interning`: the pool of label strings every series shares. Both values are the
//! pool's own O(1) counts; `TS._DEBUG STRINGPOOLSTATS` has the distribution and the savings
//! figure, which cost a walk of the pool.

use crate::common::string_interner::InternedString;
use metered::Registry;
use metered::entry::gauge_value;

pub(super) fn register(registry: &mut Registry<'static>) {
    registry
        .register(
            gauge_value("strings")
                .read(|_: &()| InternedString::interned_count() as u64)
                .help("Distinct strings in the interning pool"),
        )
        .register(
            // The pool total, not a per-series share: the `memoryUsage` of the series holding
            // these labels already divides it amongst themselves, so adding it to the sum of
            // every key's `MEMORY USAGE` double-counts it.
            gauge_value("bytes")
                .read(|_: &()| InternedString::memory_used() as u64)
                .help("Heap held by the interning pool (already shared out across the MEMORY USAGE of the series that use it)")
                .unit("bytes"),
        );
}
