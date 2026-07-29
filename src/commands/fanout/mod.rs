/// This module exists to perform conversions between local types and the
/// generated protobuf types.
mod chunks;
mod conversions;
pub(crate) mod filters;
#[cfg(test)]
mod proptest_roundtrip;

pub mod generated {
    include!(concat!(env!("OUT_DIR"), "/valkey_timeseries.fanout.v1.rs"));
}

pub use conversions::*;
pub use generated::*;
