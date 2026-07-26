//! Deterministic dataset matrix shared by unit tests, criterion benchmarks and
//! the `compression_report` tool.

use crate::common::Sample;
use crate::tests::generators::rand::{DataGenerator, ValueWorkload};
use crate::tests::generators::workload::TimestampModel;
use std::collections::HashMap;

/// Sample count used by the benchmark/report dataset matrix.
pub const DATASET_SAMPLES: usize = 64 * 1024;

/// Base seed for the dataset matrix; each dataset derives its own seed from it.
pub const DEFAULT_SEED: u64 = 0x7EA1_DA7A_5EED;

/// A value shape paired with a timestamp spacing model.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct DatasetKey {
    pub workload: ValueWorkload,
    pub timestamp_model: TimestampModel,
}

impl DatasetKey {
    pub const fn new(workload: ValueWorkload, timestamp_model: TimestampModel) -> Self {
        Self {
            workload,
            timestamp_model,
        }
    }

    pub fn id(self) -> String {
        format!("{}/{}", self.workload.id(), self.timestamp_model.id())
    }
}

/// The standard dataset matrix: every workload at a regular cadence, plus the
/// two "interesting" workloads under jittered and irregular spacing.
pub fn benchmark_dataset_keys() -> Vec<DatasetKey> {
    let mut keys = Vec::with_capacity(ValueWorkload::workloads().len() + 4);
    for workload in ValueWorkload::workloads() {
        keys.push(DatasetKey::new(*workload, TimestampModel::Regular));
    }
    for workload in [ValueWorkload::Drift, ValueWorkload::Noisy] {
        keys.push(DatasetKey::new(workload, TimestampModel::Jitter));
        keys.push(DatasetKey::new(workload, TimestampModel::Irregular));
    }
    keys
}

/// Seed for a dataset, derived from its key rather than its position in the
/// matrix, so adding, removing or reordering entries never reseeds the others
/// (and never invalidates unrelated rows of the compression baseline).
pub fn dataset_seed(key: DatasetKey) -> u64 {
    // FNV-1a over the key id. Spelled out rather than using `DefaultHasher`,
    // whose output is explicitly not stable across Rust releases — these seeds
    // have to reproduce byte-for-byte on any toolchain.
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

    let mut hash = FNV_OFFSET;
    for byte in key.id().as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    DEFAULT_SEED ^ hash
}

pub fn generate_dataset(key: DatasetKey, sample_count: usize, seed: u64) -> Vec<Sample> {
    DataGenerator::dataset(key.workload, key.timestamp_model, sample_count, seed)
}

/// Every dataset of the matrix, generated once and held for the duration of a
/// benchmark run.
pub struct DatasetRegistry {
    datasets: HashMap<DatasetKey, Vec<Sample>>,
}

impl DatasetRegistry {
    pub fn new() -> Self {
        Self::with_samples(DATASET_SAMPLES)
    }

    pub fn with_samples(sample_count: usize) -> Self {
        let mut datasets = HashMap::new();
        for key in benchmark_dataset_keys() {
            datasets.insert(key, generate_dataset(key, sample_count, dataset_seed(key)));
        }
        Self { datasets }
    }

    pub fn all_keys(&self) -> impl Iterator<Item = DatasetKey> + '_ {
        self.datasets.keys().copied()
    }

    pub fn dataset(&self, key: DatasetKey) -> &[Sample] {
        self.datasets
            .get(&key)
            .map(Vec::as_slice)
            .expect("dataset key missing")
    }
}

impl Default for DatasetRegistry {
    fn default() -> Self {
        Self::new()
    }
}
