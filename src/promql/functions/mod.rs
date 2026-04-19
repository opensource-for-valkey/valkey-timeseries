mod date_functions;
mod deriv;
mod function_list;
mod functions_tests;
mod go_compat;
mod histogram;
mod holt_winters;
mod irate;
mod labels;
mod math_functions;
mod predict_linear;
mod range_vector_functions;
mod rate;
mod rollup_window;
mod sort;
mod special_functions;
mod types;
pub(crate) mod utils;

pub(crate) use function_list::*;
pub(crate) use types::*;

// Return the concrete `PromQLFunctionImpl` so callers can store the concrete
// implementation without relying on opaque `impl Trait` return types.
pub(in crate::promql) fn resolve_function(name: &str) -> Option<PromQLFunctionImpl> {
    PromQLFunctionImpl::try_from(name).ok()
}
