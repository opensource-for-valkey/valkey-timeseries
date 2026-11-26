//! [`StringFilter`](crate::series::index::StringFilter)s that wrap or combine other filters.
//!
//! Filters here expect the query given to an inner filter and the value passed to `accept` to
//! share the same case. To search case-insensitively, lowercase the query when building the
//! inner filter and wrap it in a [`CaseFoldingFilter`], so each value is lowercased once for the
//! whole chain.
//!
//! Ported from Prometheus `web/api/v1/search_filters.go`.

mod combining;
pub mod search_expr;
mod wrapping;

pub use combining::{ChainFilter, OrFilter, OrSearchesFilter};
pub use wrapping::{
    CaseFoldingFilter, MEMOIZING_FILTER_MAX_BYTES, MEMOIZING_FILTER_MAX_ENTRIES, MemoizingFilter,
};
