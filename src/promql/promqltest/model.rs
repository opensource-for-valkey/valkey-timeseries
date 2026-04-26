use crate::common::Sample;
use crate::labels::Label;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Temporality {
    Cumulative,
    Delta,
    Unspecified,
}

/// The type of metric.
///
/// This enum represents the two fundamental metric types in time series data:
///
/// - **Gauge**: A value that can go up or down (e.g., temperature, memory usage)
/// - **Sum**: A monotonically increasing value (e.g., request count, bytes sent)
/// - **Histogram**: A value that can go up or down (e.g., temperature, memory usage)
/// - **ExponentialHistogram**: A value that can go up or down (e.g., temperature, memory usage)
/// - **Summary**: A value that can go up or down (e.g., temperature, memory usage)
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
    ExponentialHistogram {
        temporality: Temporality,
    },
    Summary,
}

impl MetricType {
    pub fn as_str(&self) -> &str {
        match self {
            MetricType::Gauge => "gauge",
            MetricType::Sum {
                monotonic: true, ..
            } => "counter",
            MetricType::Sum {
                monotonic: false, ..
            } => "gauge",
            MetricType::Histogram { .. } => "histogram",
            MetricType::ExponentialHistogram { .. } => "histogram",
            MetricType::Summary => "summary",
        }
    }
}

/// A time series with its identifying labels and data points.
///
/// A series represents a single stream of timestamped values.
///
/// # Identity and Metadata
///
/// A series is uniquely identified by its labels, which include the metric name
/// stored as `__name__`. The `metric_type`, `unit`, and `description` fields are
/// metadata with last-write-wins semantics.
///
/// # Example
///
/// ```
/// use crate::promql::promqltest::model::MetricType;
/// use crate::promql::promqltest::model::Series;
/// use crate::common::labels::Label;
/// use crate::common::Sample;
///
/// let series = Series::new(
///     "http_requests_total",
///     vec![Label::new("method", "GET")],
///     vec![Sample::new(1700000000000, 1.0)],
/// );
///
/// // Or use the builder:
/// let series = Series::builder("http_requests_total")
///     .label("method", "GET")
///     .sample(1700000000000, 1.0)
///     .build();
///
/// assert_eq!(series.name(), "http_requests_total");
/// ```
#[derive(Debug, Clone)]
pub struct Series {
    /// Labels identifying this series, including `__name__` for the metric name.
    pub labels: Vec<Label>,

    // --- Metadata (last-write-wins) ---
    /// The type of metric (gauge or counter).
    pub metric_type: Option<MetricType>,

    /// Unit of measurement (e.g., "bytes", "seconds").
    pub unit: Option<String>,

    /// Human-readable description of the metric.
    pub description: Option<String>,

    // --- Data ---
    /// One or more samples to write.
    pub samples: Vec<Sample>,
}

impl Series {
    /// Creates a new series with the given name, labels, and samples.
    ///
    /// The metric name is stored as a `__name__` label and prepended to the
    /// provided labels.
    ///
    /// # Panics
    ///
    /// Panics if `labels` contains a `__name__` label. The metric name should
    /// only be provided via the `name` parameter.
    pub fn new(name: impl Into<String>, labels: Vec<Label>, samples: Vec<Sample>) -> Self {
        assert!(
            !labels.iter().any(|l| l.name == "__name__"),
            "labels must not contain __name__; use the name parameter instead"
        );
        let mut all_labels = Vec::with_capacity(labels.len() + 1);
        all_labels.push(Label::metric_name(name));
        all_labels.extend(labels);
        Self {
            labels: all_labels,
            metric_type: None,
            unit: None,
            description: None,
            samples,
        }
    }

    /// Returns the metric name (value of the `__name__` label).
    ///
    /// # Panics
    ///
    /// Panics if the series was constructed without a `__name__` label.
    /// This should never happen when using the provided constructors.
    pub fn name(&self) -> &str {
        self.labels
            .iter()
            .find(|l| l.name == "__name__")
            .map(|l| l.value.as_str())
            .expect("Series must have a __name__ label")
    }

    /// Creates a builder for constructing a series.
    ///
    /// The builder provides a fluent API for creating series with
    /// labels, samples, and metadata fields.
    ///
    /// # Arguments
    ///
    /// * `name` - The metric name.
    pub fn builder(name: impl Into<String>) -> SeriesBuilder {
        SeriesBuilder::new(name)
    }
}

/// Builder for constructing [`Series`] instances.
///
/// Provides a fluent API for creating series with labels, samples,
/// and metadata fields.
#[derive(Debug, Clone)]
pub struct SeriesBuilder {
    labels: Vec<Label>,
    metric_type: Option<MetricType>,
    unit: Option<String>,
    description: Option<String>,
    samples: Vec<Sample>,
}

impl SeriesBuilder {
    fn new(name: impl Into<String>) -> Self {
        Self {
            labels: vec![Label::metric_name(name)],
            metric_type: None,
            unit: None,
            description: None,
            samples: Vec::new(),
        }
    }

    /// Adds a label to the series.
    ///
    /// # Panics
    ///
    /// Panics if `name` is `__name__`. The metric name should only be provided
    /// via [`Series::builder()`].
    pub fn label(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        let name = name.into();
        assert_ne!(
            name, "__name__",
            "cannot add __name__ label; use Series::builder(name) instead"
        );
        self.labels.push(Label::new(name, value.into()));
        self
    }

    /// Sets the metric type.
    pub fn metric_type(mut self, metric_type: MetricType) -> Self {
        self.metric_type = Some(metric_type);
        self
    }

    /// Sets the unit of measurement.
    pub fn unit(mut self, unit: impl Into<String>) -> Self {
        self.unit = Some(unit.into());
        self
    }

    /// Sets the description.
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Adds a sample with the given timestamp and value.
    pub fn sample(mut self, timestamp_ms: i64, value: f64) -> Self {
        self.samples.push(Sample::new(timestamp_ms, value));
        self
    }

    /// Adds a sample with the current timestamp.
    pub fn sample_now(mut self, value: f64) -> Self {
        self.samples.push(Sample::now(value));
        self
    }

    /// Builds the series.
    pub fn build(self) -> Series {
        Series {
            labels: self.labels,
            metric_type: self.metric_type,
            unit: self.unit,
            description: self.description,
            samples: self.samples,
        }
    }
}
