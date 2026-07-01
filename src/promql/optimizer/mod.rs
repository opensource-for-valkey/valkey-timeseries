mod const_folding;
mod optimize;
pub mod pushdown;
#[cfg(test)]
mod pushdown_tests;
mod utils;

pub use optimize::optimize_expr;
