use crate::common::constants::MILLIS_PER_MIN;
use std::sync::{LazyLock, PoisonError, RwLock, RwLockReadGuard};
use std::time::Duration;

const DEFAULT_MAX_QUERY_LEN: usize = 16 * 1024;
const DEFAULT_MAX_UNIQUE_TIMESERIES: usize = 1000;
const DEFAULT_LOOKBACK_DELTA_MS: u64 = 5 * MILLIS_PER_MIN;

pub static PROMQL_CONFIG: LazyLock<RwLock<PromqlConfig>> =
    LazyLock::new(|| RwLock::new(PromqlConfig::default()));

/// Global configuration options for request context
#[derive(Clone, Copy, Debug)]
pub struct PromqlConfig {
    /// The maximum query length in bytes
    pub max_query_len: usize,

    /// The maximum number of points that a query can generate.
    pub max_points_per_timeseries: usize,

    /// The maximum number of samples one query may load across all its reads
    /// (Prometheus' `--query.max-samples`). 0 = unlimited.
    pub max_samples_per_query: usize,

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

    /// Whether a range query's binary operations narrow their selectors by
    /// what the series index knows about the other operand's series
    /// (`ts-promql-derived-filter-pushdown`).
    pub derived_filter_pushdown: bool,

    /// Whether to enable experimental functions. This may be useful for testing new functions
    /// before they are ready for production use.
    pub enable_experimental_functions: bool,
}

impl Default for PromqlConfig {
    fn default() -> Self {
        PromqlConfig {
            lookback_delta: Duration::from_millis(DEFAULT_LOOKBACK_DELTA_MS),
            max_query_len: DEFAULT_MAX_QUERY_LEN,
            max_points_per_timeseries: 0,
            max_samples_per_query: crate::config::PROMQL_MAX_SAMPLES_PER_QUERY_DEFAULT as usize,
            max_response_series: DEFAULT_MAX_UNIQUE_TIMESERIES,
            max_lookback: Duration::ZERO,
            set_lookback_to_step: false,
            max_query_duration: Duration::from_secs(30),
            optimize_queries: false,
            derived_filter_pushdown: true,
            // Off, as in Prometheus (`--enable-feature=promql-experimental-functions`).
            enable_experimental_functions: false,
        }
    }
}

/// The current PromQL settings.
///
/// A panic while the snapshot was being written would poison the lock. The snapshot is
/// plain values that the next `CONFIG SET` rewrites in full, so readers carry on past the
/// poison rather than turn one bug into every query failing, or into an abort on the main
/// thread, where the commands read it.
pub fn promql_config() -> RwLockReadGuard<'static, PromqlConfig> {
    PROMQL_CONFIG.read().unwrap_or_else(PoisonError::into_inner)
}

pub(crate) fn update_prom_config<F: FnMut(&mut PromqlConfig)>(mut f: F) {
    let mut promql_config = PROMQL_CONFIG
        .write()
        .unwrap_or_else(PoisonError::into_inner);
    f(&mut promql_config);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A panic while the snapshot is being written poisons the lock. Readers and
    /// writers carry on: `TS.QUERYRANGE` used to `expect` the lock on the main
    /// thread (a server abort), `TS.QUERY` to fail every later query, and a
    /// shard to read its sample budget as 0 (unlimited).
    #[test]
    fn a_poisoned_config_lock_is_still_readable() {
        let before = promql_config().max_samples_per_query;
        let poisoner = std::thread::spawn(|| {
            let _guard = PROMQL_CONFIG
                .write()
                .unwrap_or_else(PoisonError::into_inner);
            panic!("poison the PromQL config lock");
        });
        assert!(poisoner.join().is_err());
        assert!(PROMQL_CONFIG.is_poisoned());

        assert_eq!(promql_config().max_samples_per_query, before);
        update_prom_config(|config| config.max_samples_per_query = before);
        assert_eq!(promql_config().max_samples_per_query, before);
    }
}
