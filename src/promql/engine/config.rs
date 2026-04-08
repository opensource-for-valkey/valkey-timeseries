use crate::common::constants::MILLIS_PER_MIN;
use std::sync::{LazyLock, RwLock};
use std::time::Duration;

const DEFAULT_MAX_QUERY_LEN: usize = 16 * 1024;
const DEFAULT_MAX_UNIQUE_TIMESERIES: usize = 1000;
const DEFAULT_LATENCY_OFFSET: usize = 30 * 1000;
const DEFAULT_LOOKBACK_DELTA_MS: u64 = 5 * MILLIS_PER_MIN;
const DEFAULT_STEP: i64 = 5 * 60 * 1000;

pub static PROMQL_CONFIG: LazyLock<RwLock<PromqlConfig>> =
    LazyLock::new(|| RwLock::new(PromqlConfig::default()));

/// Global configuration options for request context
#[derive(Clone, Copy, Debug)]
pub struct PromqlConfig {
    /// should we log query stats?
    pub stats_enabled: bool,

    /// Whether to disable response caching. This may be useful during data back filling
    pub disable_cache: bool,

    /// Whether query tracing is enabled.
    pub trace_enabled: bool,

    /// The maximum query length in bytes
    pub max_query_len: usize,

    /// The maximum amount of memory a single query may consume. Queries requiring more memory are
    /// rejected. The total memory limit for concurrently executed queries can be estimated as
    /// `max_memory_per_query` multiplied by -provider.maxConcurrentQueries
    pub max_memory_per_query: usize,

    /// The maximum number of points per series that a subquery can generate.
    pub max_points_subquery_per_timeseries: usize,

    /// The maximum number of unique time series to be returned from instant or range queries
    /// This option allows limiting memory usage
    pub max_response_series: usize,

    /// Default lookback delta
    pub lookback_delta: Duration,

    /// Synonym to `-provider.lookback-delta` from Prometheus.
    /// It can be overridden on a per-query basis via max_lookback arg.
    /// See also the `max_staleness_interval` flag, which has the same meaning due to historical reasons
    pub max_lookback: Duration,

    /// Whether to fix lookback interval to `step` query arg value.
    /// If set to true, the query model becomes closer to the InfluxDB data model. If set to true,
    /// then `max_lookback` is ignored. Defaults to `false`
    pub set_lookback_to_step: bool,

    /// The maximum duration for query execution (default 30 secs)
    pub max_query_duration: Duration,

    /// Whether to optimize the query before execution
    pub optimize_queries: bool,
}

impl PromqlConfig {
    /// Create an execution config with the default setting
    pub fn new() -> Self {
        Default::default()
    }

    pub fn with_cache(mut self, caching: bool) -> Self {
        self.disable_cache = !caching;
        self
    }

    pub fn with_stats_enabled(mut self, stats_enabled: bool) -> Self {
        self.stats_enabled = stats_enabled;
        self
    }
}

impl Default for PromqlConfig {
    fn default() -> Self {
        PromqlConfig {
            stats_enabled: false,
            disable_cache: false,
            trace_enabled: false,
            lookback_delta: Duration::from_millis(DEFAULT_LOOKBACK_DELTA_MS),
            max_query_len: DEFAULT_MAX_QUERY_LEN,
            max_memory_per_query: 0,
            max_points_subquery_per_timeseries: 0,
            max_response_series: DEFAULT_MAX_UNIQUE_TIMESERIES,
            max_lookback: Duration::ZERO,
            set_lookback_to_step: false,
            max_query_duration: Duration::from_secs(30),
            optimize_queries: false,
        }
    }
}
