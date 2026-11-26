//! [`StringFilter`](crate::series::index::StringFilter)s for label search: term filters, filters
//! that wrap or combine other filters, and the builders that assemble them for a search.
//!
//! Filters here expect the query given to an inner filter and the value passed to `accept` to
//! share the same case. To search case-insensitively, lowercase the query when building the
//! inner filter and wrap it in a [`CaseFoldingFilter`], so each value is lowercased once for the
//! whole chain.
//!
//! Ported from Prometheus `web/api/v1/search_filters.go` and `web/api/v1/search.go`.

mod build;
mod combining;
pub(crate) mod label_filter;
pub mod search_expr;
mod term;
mod wrapping;

pub use build::{build_search_expr_filter, build_search_filter};
pub use combining::{ChainFilter, OrFilter, OrSearchesFilter};
pub use term::{ContainsFilter, JaroWinklerFilter, SubsequenceFilter, SubstringFilter};
pub use wrapping::{
    CaseFoldingFilter, MEMOIZING_FILTER_MAX_BYTES, MEMOIZING_FILTER_MAX_ENTRIES, MemoizingFilter,
};
