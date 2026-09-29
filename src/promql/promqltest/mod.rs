mod assert;
mod dsl;
mod loader;
mod model;

mod evaluator;
pub mod openmetrics;
mod runner;

#[cfg(test)]
mod tests {
    include!(concat!(env!("OUT_DIR"), "/promql_tests_generated.rs"));
}
