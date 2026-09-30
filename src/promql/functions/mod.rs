mod date_functions;
mod deriv;
mod function_list;
mod functions_tests;
mod go_compat;
mod histogram;
mod holt_winters;
mod idelta;
mod irate;
mod labels;
mod math_functions;
mod predict_linear;
mod range_vector_functions;
mod rate;
mod rollup_functions;
mod rollups;
mod sort;
mod special_functions;
mod state;
mod types;
pub(crate) mod utils;

pub(in crate::promql) use date_functions::{DateTimePart, datetime_from_seconds};
pub(crate) use function_list::*;
pub use rollup_functions::RollupKind;
pub(in crate::promql) use rollups::window_range;
pub(in crate::promql) use state::parse_unit;
pub(crate) use types::*;

// Return the concrete `PromQLFunctionImpl` so callers can store the concrete
// implementation without relying on opaque `impl Trait` return types.
pub(in crate::promql) fn resolve_function(name: &str) -> Option<PromQLFunctionImpl> {
    PromQLFunctionImpl::try_from(name).ok()
}

/// Declare this engine's functions that promql-parser doesn't know, so queries
/// using them parse and are arity/type-checked like the built-ins.
///
/// Idempotent and cheap after the first call. The parser's registry is
/// process-global and filled once, at module load, before any command can
/// parse a query; a clash with a future built-in fails the load. Tests never
/// load the module, so a test that parses one of these functions must call this
/// first (the generated promqltest tests do, via `build.rs`).
pub(crate) fn register_parser_functions() -> Result<(), String> {
    static REGISTERED: std::sync::OnceLock<Result<(), String>> = std::sync::OnceLock::new();
    REGISTERED
        .get_or_init(|| {
            promql_parser::parser::register_extra_functions(vec![
                state::duration_parser_function(),
                state::count_parser_function(),
            ])
        })
        .clone()
}
