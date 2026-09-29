use crate::common::Sample;
use crate::labels::Label;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Temporality {
    Cumulative,
}

/// The metric type an OpenMetrics `# TYPE` line declares, as the loader maps it.
///
/// - **Gauge**: a value that can go up or down.
/// - **Sum**: an accumulating value; `monotonic` marks a counter.
/// - **Histogram**: a classic histogram's `_bucket`, `_sum` or `_count` series.
/// - **Summary**: a summary's quantile, `_sum` or `_count` series.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetricType {
    Gauge,
    Sum {
        monotonic: bool,
        temporality: Temporality,
    },
    Histogram {
        temporality: Temporality,
    },
    Summary,
}

/// A time series with its identifying labels and data points, as parsed from
/// OpenMetrics text for a `load` block.
///
/// A series is identified by its labels, which include the metric name stored
/// as `__name__`. `metric_type` and `unit` are family metadata from the
/// `# TYPE` and `# UNIT` lines.
#[derive(Debug, Clone)]
pub struct Series {
    /// Labels identifying this series, including `__name__` for the metric name.
    pub labels: Vec<Label>,

    // --- Metadata (last-write-wins) ---
    /// The type of metric (gauge or counter).
    pub metric_type: Option<MetricType>,

    /// Unit of measurement (e.g., "bytes", "seconds").
    pub unit: Option<String>,

    // --- Data ---
    /// One or more samples to write.
    pub samples: Vec<Sample>,
}
