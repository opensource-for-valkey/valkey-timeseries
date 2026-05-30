mod const_evaluator;
pub mod pushdown;
#[cfg(test)]
mod pushdown_tests;
pub mod simplifier;
mod utils;

pub use simplifier::optimize_expr;
