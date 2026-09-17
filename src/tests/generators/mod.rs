mod dataset;
mod generator;
mod labels;
mod mackey_glass;
mod rand;
mod workload;

use ::rand::prelude::StdRng;
use ::rand::{SeedableRng, rng};

pub use dataset::*;
pub use labels::*;
pub use rand::*;
pub use workload::*;

pub fn create_rng(seed: Option<u64>) -> StdRng {
    if let Some(seed) = seed {
        StdRng::seed_from_u64(seed)
    } else {
        let mut r = rng();
        StdRng::from_rng(&mut r)
    }
}
