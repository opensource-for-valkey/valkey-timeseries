mod const_folding;
pub mod pushdown;
#[cfg(test)]
mod pushdown_tests;
mod optimize;
mod utils;

pub use optimize::optimize_expr;
