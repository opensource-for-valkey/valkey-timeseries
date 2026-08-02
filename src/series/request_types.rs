use crate::aggregators::{
    Aggregator, AllAggregator, AnyAggregator, CountIfAggregator, NoneAggregator, ShareAggregator,
    SumIfAggregator,
};
use crate::common::binop::ComparisonOperator;
use crate::common::hash::hash_f64;
use crate::common::{MultiSample, Sample, Timestamp};
use crate::labels::Label;
use crate::labels::filters::SeriesSelector;
use crate::series::chunks::{ChunkOps, TimeSeriesChunk};
use crate::series::{DateRange, TimestampRange, ValueFilter};
use get_size2::GetSize;
use smallvec::{SmallVec, smallvec};
use std::fmt::Display;
use std::hash::Hash;
use valkey_module::{RedisModuleIO, ValkeyError, ValkeyResult, ValkeyString};

pub use crate::aggregators::{AggregationType, BucketAlignment, BucketTimestamp};
use crate::common::rdb::{rdb_load_f64, rdb_load_u8, rdb_save_f64, rdb_save_u8};

#[derive(Debug, Copy, Clone, GetSize)]
pub struct ValueComparisonFilter {
    pub operator: ComparisonOperator,
    pub value: f64,
}

impl ValueComparisonFilter {
    pub(crate) fn save_to_rdb(&self, rdb: *mut RedisModuleIO) {
        let op = self.operator as u8;
        rdb_save_u8(rdb, op);
        rdb_save_f64(rdb, self.value);
    }

    pub fn load_from_rdb(rdb: *mut RedisModuleIO) -> ValkeyResult<Self> {
        let op_byte = rdb_load_u8(rdb)?;
        let operator: ComparisonOperator = op_byte.try_into()?;
        let value = rdb_load_f64(rdb)?;
        Ok(Self { operator, value })
    }
}

impl Hash for ValueComparisonFilter {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.operator.hash(state);
        hash_f64(self.value, state);
    }
}

impl PartialEq for ValueComparisonFilter {
    fn eq(&self, other: &Self) -> bool {
        self.operator == other.operator
            && (self.value.is_nan() && other.value.is_nan() || self.value == other.value)
    }
}

impl Default for ValueComparisonFilter {
    fn default() -> Self {
        Self {
            operator: ComparisonOperator::NotEqual,
            value: f64::NAN,
        }
    }
}

impl ValueComparisonFilter {
    pub fn compare(&self, sample_value: f64) -> bool {
        self.operator.compare(sample_value, self.value)
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq)]
pub struct AggregatorConfig {
    pub(crate) aggregation: AggregationType,
    pub(crate) value_filter: Option<ValueComparisonFilter>,
}

impl AggregatorConfig {
    pub fn new(
        aggregation: AggregationType,
        value_filter: Option<ValueComparisonFilter>,
    ) -> ValkeyResult<Self> {
        if aggregation.is_filtered() && value_filter.is_none() {
            return Err(ValkeyError::Str("TSDB: missing condition for aggregator"));
        } else if !(aggregation.is_filtered() || aggregation.has_filtered_variant())
            && value_filter.is_some()
        {
            return Err(ValkeyError::Str(
                "TSDB: aggregation type does not support a filter condition",
            ));
        }
        Ok(Self {
            aggregation,
            value_filter,
        })
    }

    pub fn aggregation_type(&self) -> AggregationType {
        self.aggregation
    }

    pub fn aggregation_name(&self) -> &'static str {
        self.aggregation.name()
    }

    pub fn filter(&self) -> Option<ValueComparisonFilter> {
        self.value_filter
    }

    pub fn create_aggregator(&self) -> Aggregator {
        let aggr_type = self.aggregation;
        if let Some(filter) = self.value_filter {
            match self.aggregation {
                AggregationType::All => {
                    Aggregator::All(AllAggregator::new(filter.operator, filter.value))
                }
                AggregationType::Any => {
                    Aggregator::Any(AnyAggregator::new(filter.operator, filter.value))
                }
                AggregationType::Count | AggregationType::CountIf => {
                    Aggregator::CountIf(CountIfAggregator::new(filter.operator, filter.value))
                }
                AggregationType::None => {
                    Aggregator::None(NoneAggregator::new(filter.operator, filter.value))
                }
                AggregationType::Share => {
                    Aggregator::Share(ShareAggregator::new(filter.operator, filter.value))
                }
                AggregationType::Sum | AggregationType::SumIf => {
                    Aggregator::SumIf(SumIfAggregator::new(filter.operator, filter.value))
                }
                _ => aggr_type.into(),
            }
        } else {
            aggr_type.into()
        }
    }
}

impl Default for AggregatorConfig {
    fn default() -> Self {
        Self {
            aggregation: AggregationType::Avg,
            value_filter: None,
        }
    }
}

// Allow easy conversion from AggregationType to AggregatorConfig without filter
impl From<AggregationType> for AggregatorConfig {
    fn from(aggregation: AggregationType) -> Self {
        Self {
            aggregation,
            value_filter: None,
        }
    }
}

/// Hard cap on the number of aggregators in one AGGREGATION clause.
pub const MAX_AGGREGATIONS: usize = 16;

#[derive(Debug, Clone, PartialEq)]
pub struct AggregationOptions {
    /// 1..=MAX_AGGREGATIONS entries; index = output column order.
    pub aggregations: SmallVec<[AggregatorConfig; 2]>,
    pub bucket_duration: u64,
    pub timestamp_output: BucketTimestamp,
    pub alignment: BucketAlignment,
    pub report_empty: bool,
}

/// A filter that can be either inclusive or exclusive over a date range.
/// Used primarily in metadata queries.
#[derive(Copy, Clone, Debug)]
pub enum MetaDateRangeFilter {
    Includes(DateRange),
    Excludes(DateRange),
}

impl Default for MetaDateRangeFilter {
    fn default() -> Self {
        MetaDateRangeFilter::Includes(DateRange::default())
    }
}

impl Display for MetaDateRangeFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetaDateRangeFilter::Includes(range) => {
                write!(f, "[{} .. {}]", range.start, range.end)
            }
            MetaDateRangeFilter::Excludes(range) => {
                write!(f, "NOT IN [{} .. {}]", range.start, range.end)
            }
        }
    }
}

impl MetaDateRangeFilter {
    pub fn new(start: Timestamp, end: Timestamp) -> ValkeyResult<Self> {
        Ok(MetaDateRangeFilter::Includes(DateRange { start, end }))
    }

    pub fn excludes(start: Timestamp, end: Timestamp) -> ValkeyResult<Self> {
        Ok(MetaDateRangeFilter::Excludes(DateRange { start, end }))
    }

    pub fn range(&self) -> (Timestamp, Timestamp) {
        match self {
            MetaDateRangeFilter::Includes(range) => (range.start, range.end),
            MetaDateRangeFilter::Excludes(range) => (range.start, range.end),
        }
    }

    pub fn is_exclude(&self) -> bool {
        matches!(self, MetaDateRangeFilter::Excludes(_))
    }
}

impl From<TimestampRange> for MetaDateRangeFilter {
    fn from(value: TimestampRange) -> Self {
        let (start, end) = value.get_timestamps(None);
        MetaDateRangeFilter::Includes(DateRange { start, end })
    }
}

impl Default for AggregationOptions {
    fn default() -> Self {
        Self {
            aggregations: smallvec![AggregatorConfig::default()],
            bucket_duration: 60_000, // default 1 minute
            timestamp_output: BucketTimestamp::Start,
            alignment: BucketAlignment::Default,
            report_empty: false,
        }
    }
}

impl AggregationOptions {
    /// The first (or only) aggregator of the clause.
    pub fn primary(&self) -> &AggregatorConfig {
        self.aggregations
            .first()
            .expect("AggregationOptions.aggregations must contain at least one entry")
    }

    pub fn is_multi(&self) -> bool {
        self.aggregations.len() > 1
    }

    pub fn create_aggregator(&self) -> Aggregator {
        self.primary().create_aggregator()
    }

    /// One stateful aggregator per list entry, in output column order.
    pub fn create_aggregators(&self) -> SmallVec<[Aggregator; 2]> {
        self.aggregations
            .iter()
            .map(|config| config.create_aggregator())
            .collect()
    }
}

#[derive(Default, Clone, Debug)]
pub struct MatchFilterOptions {
    pub date_range: Option<MetaDateRangeFilter>,
    pub matchers: Vec<SeriesSelector>,
    pub limit: Option<usize>,
}

impl From<Vec<SeriesSelector>> for MatchFilterOptions {
    fn from(matchers: Vec<SeriesSelector>) -> Self {
        Self {
            matchers,
            ..Default::default()
        }
    }
}

impl From<SeriesSelector> for MatchFilterOptions {
    fn from(matcher: SeriesSelector) -> Self {
        Self {
            matchers: vec![matcher],
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone)]
pub struct RangeGroupingOptions {
    pub aggregation: AggregatorConfig,
    pub group_label: String,
}

#[derive(Debug, Default, Clone)]
pub struct RangeOptions {
    pub date_range: TimestampRange,
    pub count: Option<usize>,
    pub latest: bool,
    pub aggregation: Option<AggregationOptions>,
    pub timestamp_filter: Option<Vec<Timestamp>>,
    pub value_filter: Option<ValueFilter>,
}

impl RangeOptions {
    pub fn get_timestamp_range(&self) -> (Timestamp, Timestamp) {
        self.date_range.get_timestamps(None)
    }

    pub fn with_range(start_ts: Timestamp, end_ts: Timestamp) -> ValkeyResult<Self> {
        Ok(Self {
            date_range: TimestampRange::from_timestamps(start_ts, end_ts)?,
            ..Default::default()
        })
    }
}

#[derive(Debug, Default, Clone)]
pub struct MRangeOptions {
    pub range: RangeOptions,
    pub filters: Vec<SeriesSelector>,
    pub with_labels: bool,
    pub selected_labels: Vec<String>,
    pub grouping: Option<RangeGroupingOptions>,
    pub is_reverse: bool,
    /// `EXCLUDEEMPTY`: drop matched series that report no samples/buckets for the
    /// query. Mutually exclusive with `grouping` (rejected at parse time).
    pub exclude_empty: bool,
}

/// TS.NRANGE / TS.NREVRANGE: a range over an explicit list of keys, pivoted so
/// that each reply row is one timestamp followed by one value per requested
/// column.
///
/// Key order is significant and duplicates are allowed — the reply has one
/// column block per `keys` entry, in the order given — so this holds the keys
/// verbatim rather than a set.
#[derive(Debug, Default, Clone)]
pub struct NRangeOptions {
    /// Shared range parameters. `range.aggregation` is unused: TS.NRANGE takes
    /// one aggregation clause per key, held in [`Self::aggregations`].
    pub range: RangeOptions,
    pub keys: Vec<ValkeyString>,
    /// Empty in raw mode; otherwise exactly one entry per key, in key order.
    /// The syntax has a single `bucketDuration`/`ALIGN`/`BUCKETTIMESTAMP`/`EMPTY`,
    /// so every entry carries the same bucket parameters and they differ only in
    /// their aggregator list.
    pub aggregations: Vec<AggregationOptions>,
    pub is_reverse: bool,
}

impl NRangeOptions {
    /// The aggregation clause requested for key `index`, or `None` in raw mode.
    pub fn aggregation_for(&self, index: usize) -> Option<&AggregationOptions> {
        self.aggregations.get(index)
    }

    /// Number of reply columns contributed by key `index`: one per aggregator
    /// under `AGGREGATION`, otherwise the single raw sample value.
    pub fn column_count(&self, index: usize) -> usize {
        self.aggregation_for(index)
            .map_or(1, |agg| agg.aggregations.len())
    }
}

/// Per-series MRANGE result data. `TimeSeriesChunk` can only store
/// `(ts, f64)` pairs, so multi-aggregation output uses a second
/// representation. Only the `Chunk` variant ever crosses the wire in fanout
/// responses; `Rows` exists purely on the coordinator/local reply path.
#[derive(Clone, Debug)]
pub(crate) enum SeriesResultData {
    Chunk(TimeSeriesChunk),
    Rows(Vec<MultiSample>),
}

impl Default for SeriesResultData {
    fn default() -> Self {
        SeriesResultData::Chunk(TimeSeriesChunk::default())
    }
}

impl From<TimeSeriesChunk> for SeriesResultData {
    fn from(chunk: TimeSeriesChunk) -> Self {
        SeriesResultData::Chunk(chunk)
    }
}

impl SeriesResultData {
    /// Whether this series reports nothing at all — the emptiness `EXCLUDEEMPTY`
    /// tests. It is the *reported* payload that counts, so a series whose samples
    /// were all removed by FILTER_BY_TS/FILTER_BY_VALUE, or whose only in-range
    /// samples produced no bucket, is empty; one reporting a NaN sample is not.
    pub(crate) fn is_empty(&self) -> bool {
        match self {
            SeriesResultData::Chunk(chunk) => chunk.is_empty(),
            SeriesResultData::Rows(rows) => rows.is_empty(),
        }
    }

    /// Iterate the raw samples of the `Chunk` variant. The coordinator ingest
    /// paths only ever hold chunks (rows never cross the wire); `Rows` yields
    /// nothing.
    pub(crate) fn sample_iter(&self) -> Box<dyn Iterator<Item = Sample> + '_> {
        match self {
            SeriesResultData::Chunk(chunk) => Box::new(chunk.iter()),
            SeriesResultData::Rows(_) => {
                unreachable!("sample_iter called on multi-aggregation rows")
            }
        }
    }
}

#[derive(Default, Clone, Debug)]
pub(crate) struct MRangeSeriesResult {
    pub key: String,
    pub group_label_value: Option<String>,
    pub labels: Vec<Label>,
    /// Source series keys backing a GROUPBY group (sorted). Empty for
    /// non-grouped results. RESP3 replies report these in the per-group
    /// `sources` metadata map regardless of WITHLABELS.
    pub sources: Vec<String>,
    pub data: SeriesResultData,
}

#[derive(Debug, Default, Clone)]
pub struct MGetRequest {
    pub with_labels: bool,
    pub filters: Vec<SeriesSelector>,
    pub selected_labels: Vec<String>,
    pub latest: bool,
}

pub struct MGetSeriesData {
    pub series_key: ValkeyString,
    pub labels: Vec<Option<Label>>,
    pub sample: Option<Sample>,
}

#[derive(Debug, Default, Clone)]
pub struct MDelRequest {
    pub range: Option<TimestampRange>,
    pub filters: Vec<SeriesSelector>,
}

#[derive(Debug, Default, Clone)]
pub struct MDelResponse {
    pub deleted_count: usize,
}

#[cfg(test)]
mod tests {}
