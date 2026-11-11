//! Raw results as written to the run directory. Everything the report needs
//! is here, so `server_bench report` regenerates CSV/Markdown from disk
//! without touching a server.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::trace::Engine;

pub const RESULTS_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunResults {
    pub results_version: u32,
    pub run_id: String,
    pub cases: Vec<CaseResults>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaseResults {
    pub case_id: String,
    pub family: String,
    pub connections: u32,
    pub pipeline: u32,
    /// Closed-loop pipelined command latency is only meaningful because each
    /// reply is associated with the submission time of its batch; say so.
    pub latency_definition: String,
    pub trials: Vec<TrialPair>,
    pub memory: Vec<MemoryPair>,
}

/// One paired trial: both engines ran the same phase back to back, in the
/// recorded order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrialPair {
    pub trial: u32,
    pub order: [Engine; 2],
    pub subject: TrialResult,
    pub reference: TrialResult,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrialResult {
    pub engine: Engine,
    pub started_at: String,
    /// Wall time of the timed phase: first submission to last reply, across
    /// all connections.
    pub duration_s: f64,
    pub requests: u64,
    /// Samples written (write cases) or returned (range reads); 0 for GET.
    pub samples: u64,
    pub errors: u64,
    pub timeouts: u64,
    /// First few error texts, for the report.
    pub error_samples: Vec<String>,
    /// A trial that did not run to completion (timeout, transport failure,
    /// failed state check) gets no ratio.
    pub complete: bool,
    pub incomplete_reason: Option<String>,
    pub latency: LatencySummary,
    /// `INFO` deltas over the timed phase: net bytes, commands, CPU seconds.
    pub server_deltas: BTreeMap<String, f64>,
    /// Read-case only: how many complete passes over the cycle were made.
    pub cycles_completed: f64,
    /// Write-case only: the state check after the trial.
    pub state_verified: Option<bool>,
    pub under_calibrated: bool,
    /// Aggregate values that differed from the oracle within tolerance
    /// (order-dependent `sum`/`avg`); kept visible, never hidden.
    #[serde(default)]
    pub value_deviations: u64,
    #[serde(default)]
    pub max_rel_deviation: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LatencySummary {
    pub observations: u64,
    pub p50_us: f64,
    pub p95_us: f64,
    pub p99_us: f64,
    pub p999_us: f64,
    pub max_us: f64,
    pub mean_us: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryPair {
    pub trial: u32,
    pub order: [Engine; 2],
    pub subject: MemoryResult,
    pub reference: MemoryResult,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemorySnapshot {
    pub used_memory: u64,
    pub used_memory_rss: u64,
    pub allocator_allocated: Option<u64>,
    pub allocator_active: Option<u64>,
    pub mem_fragmentation_ratio: Option<f64>,
    pub dbsize: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryResult {
    pub engine: Engine,
    /// Fresh process, before any key existed (from preflight).
    pub empty_process: MemorySnapshot,
    /// Same process, immediately before this trial's setup.
    pub before: MemorySnapshot,
    /// After `TS.CREATE` of every series and a settle period.
    pub created_empty: MemorySnapshot,
    /// After preload and a settle period.
    pub loaded: MemorySnapshot,
    pub series: u64,
    pub samples: u64,
    /// `TS.INFO memoryUsage` summed over sampled keys.
    pub ts_info_memory_sampled: u64,
    /// `MEMORY USAGE` summed over the same keys.
    pub memory_usage_sampled: u64,
    pub sampled_keys: u64,
    pub settle_seconds: u64,
    /// False when the process had served earlier trials: RSS then includes
    /// allocator history, and only `used_memory` deltas are comparable.
    pub fresh_process: bool,
    pub complete: bool,
    pub incomplete_reason: Option<String>,
}

impl MemoryResult {
    /// Server-accounted bytes per retained sample, loaded minus created-empty.
    pub fn bytes_per_sample(&self) -> Option<f64> {
        if self.samples == 0 {
            return None;
        }
        Some(
            (self.loaded.used_memory as f64 - self.created_empty.used_memory as f64)
                / self.samples as f64,
        )
    }

    pub fn bytes_per_series_empty(&self) -> Option<f64> {
        if self.series == 0 {
            return None;
        }
        Some(
            (self.created_empty.used_memory as f64 - self.before.used_memory as f64)
                / self.series as f64,
        )
    }
}
