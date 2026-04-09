mod date_functions;
mod deriv;
mod function_list;
mod histogram;
mod holt_winters;
mod irate;
mod labels;
mod math_functions;
mod predict_linear;
mod range_vector_functions;
mod rate;
mod functions_tests;
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
    PromQLFunctionImpl::from_name(name).or_else(|| {
        // handle holt_winters as alias
        if name.len() == 12 && name.eq_ignore_ascii_case("holt_winters") {
            PromQLFunctionImpl::from_name("doubvle_exponential_smoothing")
        } else {
            None
        }
    })
}
