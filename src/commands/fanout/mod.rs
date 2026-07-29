/// This module exists to perform conversions between local types and the
/// generated protobuf types.
mod chunks;
mod conversions;
pub(crate) mod filters;

pub mod generated {
    include!(concat!(env!("OUT_DIR"), "/valkey_timeseries.fanout.v1.rs"));
}

pub use conversions::*;
pub use generated::*;
