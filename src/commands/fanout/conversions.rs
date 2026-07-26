use super::filters::{deserialize_matchers_list, serialize_matchers_list};
use super::generated::{
    AggregationOptions as FanoutAggregationOptions, AggregationType as FanoutAggregationType,
    AggregatorConfig as FanoutAggregatorConfig, BucketAlignmentType, BucketTimestampType,
    ComparisonOperator as FanoutComparisonOperator, CompressionType as FanoutChunkEncoding,
    DateRange, GroupPartialSeries, GroupingOptions as FanoutGroupingOptions, Label as FanoutLabel,
    MetaDateRangeFilter as FanoutMetaDateRangeFilter, MultiRangeRequest,
    PostingStat as FanoutPostingStat, RangeRequest, ReducePartialState, Sample as FanoutSample,
    SeriesSelector as FanoutSeriesSelector, StatsResponse,
    ValueComparisonFilter as FanoutValueComparisonFilter, ValueRange as FanoutValueFilter,
};
use crate::aggregators::PartialState;
use crate::commands::fanout::MGetValue;
use crate::common::binop::ComparisonOperator;
use crate::labels::Label;
use crate::labels::filters::SeriesSelector;
use crate::series::chunks::ChunkEncoding;
use crate::series::mrange::GroupPartialsResult;
use crate::series::request_types::{
    AggregationOptions, AggregationType, AggregatorConfig, BucketAlignment, MAX_AGGREGATIONS,
    MGetSeriesData, MRangeOptions, MatchFilterOptions, MetaDateRangeFilter, RangeGroupingOptions,
    RangeOptions, ValueComparisonFilter,
};
use crate::series::{TimestampRange, ValueFilter};
use crate::{
    aggregators::BucketTimestamp,
    error_consts,
    series::index::{PostingStat, PostingsStats},
};
use smallvec::SmallVec;
use valkey_module::{ValkeyError, ValkeyResult, ValkeyValue};

impl From<ComparisonOperator> for FanoutComparisonOperator {
    fn from(value: ComparisonOperator) -> Self {
        match value {
            ComparisonOperator::Equal => FanoutComparisonOperator::Eq,
            ComparisonOperator::NotEqual => FanoutComparisonOperator::Neq,
            ComparisonOperator::GreaterThan => FanoutComparisonOperator::Gt,
            ComparisonOperator::GreaterThanOrEqual => FanoutComparisonOperator::Gte,
            ComparisonOperator::LessThan => FanoutComparisonOperator::Lt,
            ComparisonOperator::LessThanOrEqual => FanoutComparisonOperator::Lte,
        }
    }
}

impl From<FanoutComparisonOperator> for ComparisonOperator {
    fn from(value: FanoutComparisonOperator) -> Self {
        match value {
            FanoutComparisonOperator::Eq => ComparisonOperator::Equal,
            FanoutComparisonOperator::Neq => ComparisonOperator::NotEqual,
            FanoutComparisonOperator::Gt => ComparisonOperator::GreaterThan,
            FanoutComparisonOperator::Gte => ComparisonOperator::GreaterThanOrEqual,
            FanoutComparisonOperator::Lt => ComparisonOperator::LessThan,
            FanoutComparisonOperator::Lte => ComparisonOperator::LessThanOrEqual,
        }
    }
}

impl From<ChunkEncoding> for FanoutChunkEncoding {
    fn from(value: ChunkEncoding) -> Self {
        match value {
            ChunkEncoding::Uncompressed => FanoutChunkEncoding::Uncompressed,
            ChunkEncoding::Gorilla => FanoutChunkEncoding::Gorilla,
            ChunkEncoding::TsXor => FanoutChunkEncoding::Tsxor,
            ChunkEncoding::Xor2 => FanoutChunkEncoding::Xor2,
            ChunkEncoding::DeXor => FanoutChunkEncoding::Dexor,
            ChunkEncoding::Chimp => FanoutChunkEncoding::Chimp,
        }
    }
}

impl From<FanoutChunkEncoding> for ChunkEncoding {
    fn from(value: FanoutChunkEncoding) -> Self {
        match value {
            FanoutChunkEncoding::Uncompressed => ChunkEncoding::Uncompressed,
            FanoutChunkEncoding::Gorilla => ChunkEncoding::Gorilla,
            FanoutChunkEncoding::Tsxor => ChunkEncoding::TsXor,
            FanoutChunkEncoding::Xor2 => ChunkEncoding::Xor2,
            FanoutChunkEncoding::Dexor => ChunkEncoding::DeXor,
            FanoutChunkEncoding::Chimp => ChunkEncoding::Chimp,
        }
    }
}

impl From<TimestampRange> for DateRange {
    fn from(value: TimestampRange) -> Self {
        let (start, end) = value.get_timestamps(None);
        DateRange { start, end }
    }
}

impl From<DateRange> for TimestampRange {
    fn from(value: DateRange) -> Self {
        TimestampRange::from_timestamps(value.start, value.end)
            .expect("Invalid date range in decode_date_range")
    }
}

impl From<FanoutPostingStat> for PostingStat {
    fn from(value: FanoutPostingStat) -> Self {
        PostingStat {
            name: value.name,
            count: value.count,
        }
    }
}

impl From<PostingStat> for FanoutPostingStat {
    fn from(value: PostingStat) -> Self {
        FanoutPostingStat {
            name: value.name,
            count: value.count,
        }
    }
}

impl From<PostingsStats> for StatsResponse {
    fn from(value: PostingsStats) -> Self {
        StatsResponse {
            series_count_by_metric_name: value
                .series_count_by_metric_name
                .into_iter()
                .map(|s| s.into())
                .collect(),
            series_count_by_label_name: value
                .series_count_by_label_name
                .into_iter()
                .map(|s| s.into())
                .collect(),
            series_count_by_label_value_pairs: value
                .series_count_by_label_value_pairs
                .into_iter()
                .map(|s| s.into())
                .collect(),
            series_count_by_focus_label_value: value
                .series_count_by_focus_label_value
                .map(|v| v.into_iter().map(|s| s.into()).collect())
                .unwrap_or_default(),
            series_count: value.series_count,
            labels_bitmap: vec![],
            label_value_pairs_bitmap: vec![],
        }
    }
}

impl From<StatsResponse> for PostingsStats {
    fn from(value: StatsResponse) -> Self {
        let focused = if value.series_count_by_focus_label_value.is_empty() {
            None
        } else {
            Some(
                value
                    .series_count_by_focus_label_value
                    .into_iter()
                    .map(|s| s.into())
                    .collect(),
            )
        };
        PostingsStats {
            series_count_by_metric_name: value
                .series_count_by_metric_name
                .into_iter()
                .map(|s| s.into())
                .collect(),
            series_count_by_label_name: value
                .series_count_by_label_name
                .into_iter()
                .map(|s| s.into())
                .collect(),
            series_count_by_label_value_pairs: value
                .series_count_by_label_value_pairs
                .into_iter()
                .map(|s| s.into())
                .collect(),
            series_count_by_focus_label_value: focused,
            total_label_value_pairs: 0,
            label_count: 0,
            series_count: value.series_count,
        }
    }
}

impl From<BucketTimestamp> for BucketTimestampType {
    fn from(value: BucketTimestamp) -> Self {
        match value {
            BucketTimestamp::Start => BucketTimestampType::Start,
            BucketTimestamp::End => BucketTimestampType::End,
            BucketTimestamp::Mid => BucketTimestampType::Mid,
        }
    }
}

impl From<BucketTimestampType> for BucketTimestamp {
    fn from(value: BucketTimestampType) -> Self {
        match value {
            BucketTimestampType::Start => BucketTimestamp::Start,
            BucketTimestampType::End => BucketTimestamp::End,
            BucketTimestampType::Mid => BucketTimestamp::Mid,
        }
    }
}

impl From<BucketAlignmentType> for BucketAlignment {
    fn from(value: BucketAlignmentType) -> Self {
        match value {
            BucketAlignmentType::Default => BucketAlignment::Default,
            BucketAlignmentType::AlignStart => BucketAlignment::Start,
            BucketAlignmentType::AlignEnd => BucketAlignment::End,
            BucketAlignmentType::Timestamp => BucketAlignment::Timestamp(0),
        }
    }
}

impl From<AggregationType> for FanoutAggregationType {
    fn from(value: AggregationType) -> Self {
        match value {
            AggregationType::All => FanoutAggregationType::All,
            AggregationType::Any => FanoutAggregationType::Any,
            AggregationType::Avg => FanoutAggregationType::Avg,
            AggregationType::Count => FanoutAggregationType::Count,
            AggregationType::CountAll => FanoutAggregationType::CountAll,
            AggregationType::CountIf => FanoutAggregationType::CountIf,
            AggregationType::CountNan => FanoutAggregationType::CountNan,
            AggregationType::First => FanoutAggregationType::First,
            AggregationType::Increase => FanoutAggregationType::Increase,
            AggregationType::IRate => FanoutAggregationType::Irate,
            AggregationType::Last => FanoutAggregationType::Last,
            AggregationType::Min => FanoutAggregationType::Min,
            AggregationType::Max => FanoutAggregationType::Max,
            AggregationType::None => FanoutAggregationType::None,
            AggregationType::Sum => FanoutAggregationType::Sum,
            AggregationType::SumIf => FanoutAggregationType::SumIf,
            AggregationType::Range => FanoutAggregationType::Range,
            AggregationType::Rate => FanoutAggregationType::Rate,
            AggregationType::Share => FanoutAggregationType::ShareIf,
            AggregationType::StdP => FanoutAggregationType::StdP,
            AggregationType::StdS => FanoutAggregationType::StdS,
            AggregationType::VarP => FanoutAggregationType::VarP,
            AggregationType::VarS => FanoutAggregationType::VarS,
        }
    }
}

impl From<AggregatorConfig> for FanoutAggregationType {
    fn from(value: AggregatorConfig) -> Self {
        value.aggregation_type().into()
    }
}

impl From<FanoutAggregationType> for FanoutAggregatorConfig {
    fn from(value: FanoutAggregationType) -> Self {
        FanoutAggregatorConfig {
            aggregator_type: value as i32,
            value_filter: None,
        }
    }
}

impl From<FanoutAggregationType> for AggregationType {
    fn from(value: FanoutAggregationType) -> Self {
        match value {
            FanoutAggregationType::All => AggregationType::All,
            FanoutAggregationType::Any => AggregationType::Any,
            FanoutAggregationType::Avg => AggregationType::Avg,
            FanoutAggregationType::Count => AggregationType::Count,
            FanoutAggregationType::CountAll => AggregationType::CountAll,
            FanoutAggregationType::CountIf => AggregationType::CountIf,
            FanoutAggregationType::CountNan => AggregationType::CountNan,
            FanoutAggregationType::First => AggregationType::First,
            FanoutAggregationType::Increase => AggregationType::Increase,
            FanoutAggregationType::Irate => AggregationType::IRate,
            FanoutAggregationType::Last => AggregationType::Last,
            FanoutAggregationType::Max => AggregationType::Max,
            FanoutAggregationType::Min => AggregationType::Min,
            FanoutAggregationType::None => AggregationType::None,
            FanoutAggregationType::Range => AggregationType::Range,
            FanoutAggregationType::Rate => AggregationType::Rate,
            FanoutAggregationType::ShareIf => AggregationType::Share,
            FanoutAggregationType::Sum => AggregationType::Sum,
            FanoutAggregationType::SumIf => AggregationType::SumIf,
            FanoutAggregationType::StdP => AggregationType::StdP,
            FanoutAggregationType::StdS => AggregationType::StdS,
            FanoutAggregationType::VarP => AggregationType::VarP,
            FanoutAggregationType::VarS => AggregationType::VarS,
        }
    }
}

impl TryFrom<FanoutAggregatorConfig> for AggregatorConfig {
    type Error = ValkeyError;

    fn try_from(value: FanoutAggregatorConfig) -> Result<Self, Self::Error> {
        let aggr_type: FanoutAggregationType = value
            .aggregator_type
            .try_into()
            .map_err(|_| ValkeyError::Str(error_consts::UNKNOWN_AGGREGATION_TYPE))?;
        let aggregation_type: AggregationType = aggr_type.into();

        let filter = value.value_filter.map(|f| f.into());

        AggregatorConfig::new(aggregation_type, filter)
    }
}

impl TryFrom<&FanoutGroupingOptions> for RangeGroupingOptions {
    type Error = ValkeyError;

    fn try_from(value: &FanoutGroupingOptions) -> Result<RangeGroupingOptions, ValkeyError> {
        let aggregation: AggregatorConfig = value
            .aggregation
            .unwrap_or_default()
            .try_into()
            .map_err(|_| ValkeyError::Str(error_consts::UNKNOWN_AGGREGATION_TYPE))?; // todo: serialization error

        Ok(RangeGroupingOptions {
            aggregation,
            group_label: value.group_label.clone(),
        })
    }
}

impl TryFrom<FanoutGroupingOptions> for RangeGroupingOptions {
    type Error = ValkeyError;

    fn try_from(value: FanoutGroupingOptions) -> Result<RangeGroupingOptions, ValkeyError> {
        let aggregation_input = value.aggregation.unwrap_or_default();
        let aggregation = aggregation_input.try_into()?;

        Ok(RangeGroupingOptions {
            aggregation,
            group_label: value.group_label,
        })
    }
}

impl From<&RangeGroupingOptions> for FanoutGroupingOptions {
    fn from(value: &RangeGroupingOptions) -> Self {
        let aggregation: FanoutAggregatorConfig = value.aggregation.into();
        FanoutGroupingOptions {
            aggregation: Some(aggregation),
            group_label: value.group_label.clone(),
        }
    }
}

impl From<RangeGroupingOptions> for FanoutGroupingOptions {
    fn from(value: RangeGroupingOptions) -> Self {
        let aggregation: FanoutAggregatorConfig = value.aggregation.into();
        FanoutGroupingOptions {
            aggregation: Some(aggregation),
            group_label: value.group_label,
        }
    }
}

impl From<MGetSeriesData> for MGetValue {
    fn from(value: MGetSeriesData) -> Self {
        let labels = value
            .labels
            .into_iter()
            .map(|l| l.map_or_else(FanoutLabel::default, |l| l.into()))
            .collect();

        let sample = value.sample.map(|s| FanoutSample {
            timestamp: s.timestamp,
            value: s.value,
        });

        MGetValue {
            key: value.series_key.to_string_lossy(),
            labels,
            sample,
        }
    }
}

pub fn deserialize_match_filter_options(
    range: Option<FanoutMetaDateRangeFilter>,
    filters: Option<Vec<FanoutSeriesSelector>>,
) -> ValkeyResult<MatchFilterOptions> {
    let date_range: Option<MetaDateRangeFilter> = range.map(|r| r.into());
    let matchers: Vec<SeriesSelector> = deserialize_matchers_list(filters)?;
    Ok(MatchFilterOptions {
        date_range,
        matchers,
        limit: None,
    })
}

/// Serialize local [`MatchFilterOptions`] into the protobuf-compatible fields
/// needed by fanout request messages.  Returns `(range, filters)` suitable for
/// direct assignment into the generated protobuf request struct.
pub fn serialize_match_filter_options(
    options: &MatchFilterOptions,
) -> (Option<FanoutMetaDateRangeFilter>, Vec<FanoutSeriesSelector>) {
    let filters = serialize_matchers_list(&options.matchers).expect("serialize matchers list");
    let range = options.date_range.map(|r| r.into());
    (range, filters)
}

impl From<FanoutLabel> for Label {
    fn from(value: FanoutLabel) -> Self {
        let name = value.name.to_string();
        let value = value.value.to_string();
        Label { name, value }
    }
}

impl From<&Label> for FanoutLabel {
    fn from(value: &Label) -> Self {
        FanoutLabel {
            name: value.name.clone(),
            value: value.value.clone(),
        }
    }
}

impl From<Label> for FanoutLabel {
    fn from(value: Label) -> Self {
        FanoutLabel {
            name: value.name,
            value: value.value,
        }
    }
}

impl From<FanoutSample> for ValkeyValue {
    fn from(value: FanoutSample) -> Self {
        let row = vec![
            ValkeyValue::from(value.timestamp),
            ValkeyValue::from(value.value),
        ];
        ValkeyValue::from(row)
    }
}

impl From<ValueComparisonFilter> for FanoutValueComparisonFilter {
    fn from(value: ValueComparisonFilter) -> Self {
        let fanout_operator: FanoutComparisonOperator = value.operator.into();
        FanoutValueComparisonFilter {
            operator: fanout_operator.into(),
            value: value.value,
        }
    }
}

impl From<FanoutValueComparisonFilter> for ValueComparisonFilter {
    fn from(value: FanoutValueComparisonFilter) -> Self {
        let fanout_operator: FanoutComparisonOperator = value.operator.try_into().unwrap();
        let operator: ComparisonOperator = fanout_operator.into();
        ValueComparisonFilter {
            operator,
            value: value.value,
        }
    }
}

impl From<AggregatorConfig> for FanoutAggregatorConfig {
    fn from(value: AggregatorConfig) -> Self {
        let aggr_type: FanoutAggregationType = value.aggregation_type().into();
        FanoutAggregatorConfig {
            aggregator_type: aggr_type.into(),
            value_filter: value.filter().map(|filter| filter.into()),
        }
    }
}

impl From<&AggregationOptions> for FanoutAggregationOptions {
    fn from(value: &AggregationOptions) -> Self {
        // A single-aggregator query is simply a one-element list.
        let aggregators: Vec<FanoutAggregatorConfig> = value
            .aggregations
            .iter()
            .map(|config| (*config).into())
            .collect();
        let bucket_timestamp_type: BucketTimestampType = value.timestamp_output.into();

        let (bucket_alignment, alignment_timestamp) = match value.alignment {
            BucketAlignment::Default => (BucketAlignmentType::Default, 0),
            BucketAlignment::Start => (BucketAlignmentType::AlignStart, 0),
            BucketAlignment::End => (BucketAlignmentType::AlignEnd, 0),
            BucketAlignment::Timestamp(ts) => (BucketAlignmentType::Timestamp, ts),
        };

        FanoutAggregationOptions {
            aggregators,
            bucket_duration: value.bucket_duration as u32,
            bucket_timestamp_type: bucket_timestamp_type.into(),
            bucket_alignment: bucket_alignment.into(),
            alignment_timestamp,
            report_empty: value.report_empty,
        }
    }
}

impl From<AggregationOptions> for FanoutAggregationOptions {
    fn from(value: AggregationOptions) -> Self {
        (&value).into()
    }
}

impl TryFrom<FanoutAggregationOptions> for AggregationOptions {
    type Error = ValkeyError;

    fn try_from(value: FanoutAggregationOptions) -> Result<Self, Self::Error> {
        // Re-validate the list bounds and duplicates as a defense against
        // corrupt/malicious peers.
        if value.aggregators.is_empty() {
            return Err(ValkeyError::Str("TSDB: aggregation config is required"));
        }
        if value.aggregators.len() > MAX_AGGREGATIONS {
            return Err(ValkeyError::Str(error_consts::TOO_MANY_AGGREGATIONS));
        }
        let aggregations = value
            .aggregators
            .into_iter()
            .map(AggregatorConfig::try_from)
            .collect::<Result<SmallVec<[AggregatorConfig; 2]>, _>>()?;
        let mut seen: SmallVec<[AggregationType; 2]> = SmallVec::new();
        for config in aggregations.iter() {
            if seen.contains(&config.aggregation_type()) {
                return Err(ValkeyError::Str(error_consts::DUPLICATE_AGGREGATION));
            }
            seen.push(config.aggregation_type());
        }
        let bucket_duration = value.bucket_duration as u64;
        if bucket_duration == 0 {
            return Err(ValkeyError::Str("TSDB: bucket duration must be positive"));
        }
        let timestamp_output: BucketTimestampType = value
            .bucket_timestamp_type
            .try_into()
            .map_err(|_| ValkeyError::Str(error_consts::INVALID_BUCKET_TIMESTAMP_TYPE))?;
        let fanout_alignment: BucketAlignmentType = value
            .bucket_alignment
            .try_into()
            .map_err(|_| ValkeyError::Str(error_consts::INVALID_BUCKET_ALIGNMENT))?;

        let mut alignment: BucketAlignment = fanout_alignment.into();
        if matches!(alignment, BucketAlignment::Timestamp(_)) {
            let timestamp = value.alignment_timestamp;
            alignment = BucketAlignment::Timestamp(timestamp);
        }

        let report_empty = value.report_empty;

        Ok(AggregationOptions {
            aggregations,
            bucket_duration,
            timestamp_output: timestamp_output.into(),
            alignment,
            report_empty,
        })
    }
}

impl TryFrom<RangeRequest> for RangeOptions {
    type Error = ValkeyError;

    fn try_from(value: RangeRequest) -> Result<Self, Self::Error> {
        (&value).try_into()
    }
}

impl TryFrom<&RangeRequest> for RangeOptions {
    type Error = ValkeyError;

    fn try_from(value: &RangeRequest) -> Result<Self, Self::Error> {
        let date_range: TimestampRange = match value.range {
            Some(r) => r.into(),
            None => {
                return Err(ValkeyError::Str("TSDB: date range is required"));
            }
        };

        let count = if value.count == 0 {
            None
        } else {
            Some(value.count as usize)
        };

        let aggregation = if let Some(aggregation) = value.aggregation.clone() {
            let options = aggregation.try_into()?;
            Some(options)
        } else {
            None
        };

        let timestamp_filter = if value.timestamp_filter.is_empty() {
            None
        } else {
            Some(value.timestamp_filter.clone())
        };

        let value_filter: Option<ValueFilter> = value.value_filter.map(|filter| ValueFilter {
            min: filter.min,
            max: filter.max,
        });

        let latest = value.latest;

        Ok(RangeOptions {
            date_range,
            count,
            aggregation,
            timestamp_filter,
            value_filter,
            latest,
        })
    }
}

impl From<&RangeOptions> for RangeRequest {
    fn from(value: &RangeOptions) -> Self {
        let range: DateRange = value.date_range.into();

        let count = match value.count {
            Some(c) => c as u32,
            None => 0,
        };

        let aggregation = value
            .aggregation
            .as_ref()
            .map(FanoutAggregationOptions::from);

        let timestamp_filter = match value.timestamp_filter {
            Some(ref ts) => ts.clone(),
            None => vec![],
        };

        let value_filter: Option<FanoutValueFilter> =
            value.value_filter.map(|filter| FanoutValueFilter {
                min: filter.min,
                max: filter.max,
            });

        RangeRequest {
            range: Some(range),
            count,
            aggregation,
            timestamp_filter,
            value_filter,
            latest: value.latest,
        }
    }
}

impl TryFrom<&MultiRangeRequest> for MRangeOptions {
    type Error = ValkeyError;

    fn try_from(value: &MultiRangeRequest) -> Result<Self, Self::Error> {
        let range: RangeOptions = if let Some(r) = &value.range {
            r.try_into()?
        } else {
            return Err(ValkeyError::Str("TSDB: range is required"));
        };

        let mut filters: Vec<SeriesSelector> = Vec::with_capacity(value.filters.len());
        for filter in value.filters.iter() {
            filters.push(filter.try_into()?);
        }
        let with_labels = value.with_labels;

        let selected_labels = value.selected_labels.clone();

        let grouping: Option<RangeGroupingOptions> = match &value.grouping {
            Some(group) => Some(group.try_into()?),
            None => None,
        };

        let is_reverse = value.is_reverse;

        Ok(MRangeOptions {
            range,
            filters,
            with_labels,
            selected_labels,
            grouping,
            is_reverse,
        })
    }
}

impl TryFrom<MultiRangeRequest> for MRangeOptions {
    type Error = ValkeyError;

    fn try_from(value: MultiRangeRequest) -> Result<Self, Self::Error> {
        let range: RangeOptions = if let Some(r) = value.range {
            r.try_into()?
        } else {
            return Err(ValkeyError::Str("TSDB: range is required"));
        };
        let filters = deserialize_matchers_list(Some(value.filters))?;
        let with_labels = value.with_labels;

        let selected_labels = value.selected_labels;

        let grouping: Option<RangeGroupingOptions> = match value.grouping {
            Some(group) => Some(group.try_into()?),
            None => None,
        };

        let is_reverse = value.is_reverse;

        Ok(MRangeOptions {
            range,
            filters,
            with_labels,
            selected_labels,
            grouping,
            is_reverse,
        })
    }
}

impl TryFrom<&MRangeOptions> for MultiRangeRequest {
    type Error = ValkeyError;
    fn try_from(value: &MRangeOptions) -> Result<Self, Self::Error> {
        let range: RangeRequest = (&value.range).into();
        let filters: Vec<FanoutSeriesSelector> = serialize_matchers_list(&value.filters)?;
        let with_labels = value.with_labels;

        let selected_labels = value.selected_labels.clone();

        let grouping: Option<FanoutGroupingOptions> =
            value.grouping.as_ref().map(|group| group.into());

        Ok(MultiRangeRequest {
            range: Some(range),
            filters,
            with_labels,
            selected_labels,
            grouping,
            is_reverse: value.is_reverse,
            apply_aggregation: false,
            apply_group_reduce: false,
            apply_count: false,
        })
    }
}

impl TryFrom<MRangeOptions> for MultiRangeRequest {
    type Error = ValkeyError;
    fn try_from(value: MRangeOptions) -> Result<Self, Self::Error> {
        let range: RangeRequest = (&value.range).into();
        let filters: Vec<FanoutSeriesSelector> = serialize_matchers_list(&value.filters)?;
        let with_labels = value.with_labels;

        let selected_labels = value.selected_labels;

        let grouping: Option<FanoutGroupingOptions> = value.grouping.map(|group| group.into());

        Ok(MultiRangeRequest {
            range: Some(range),
            filters,
            with_labels,
            selected_labels,
            grouping,
            is_reverse: value.is_reverse,
            apply_aggregation: false,
            apply_group_reduce: false,
            apply_count: false,
        })
    }
}

impl From<&ReducePartialState> for PartialState {
    fn from(value: &ReducePartialState) -> Self {
        Self {
            count: value.count,
            acc1: value.acc1,
            acc2: value.acc2,
            acc1_c: value.acc1_compensation,
            ts: value.ts,
        }
    }
}

impl From<PartialState> for ReducePartialState {
    fn from(value: PartialState) -> Self {
        Self {
            count: value.count,
            acc1: value.acc1,
            acc2: value.acc2,
            acc1_compensation: value.acc1_c,
            ts: value.ts,
        }
    }
}

impl From<GroupPartialsResult> for GroupPartialSeries {
    fn from(value: GroupPartialsResult) -> Self {
        Self {
            group_label_value: value.group_label_value,
            source_keys: value.source_keys,
            bucket_timestamps: value.timestamps,
            states: value.states.into_iter().map(Into::into).collect(),
            column_count: value.column_count as u32,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregators::BucketAlignment;
    use crate::aggregators::BucketTimestamp;
    use crate::series::request_types::AggregationType;

    #[test]
    fn test_aggregation_options_to_fanout_full() {
        let options = AggregationOptions {
            aggregations: smallvec::smallvec![
                AggregatorConfig::new(
                    AggregationType::CountIf,
                    Some(ValueComparisonFilter {
                        operator: ComparisonOperator::GreaterThan,
                        value: 10.0,
                    }),
                )
                .unwrap()
            ],
            bucket_duration: 1000,
            timestamp_output: BucketTimestamp::Start,
            alignment: BucketAlignment::Timestamp(555),
            report_empty: true,
        };

        let fanout: FanoutAggregationOptions = options.into();

        assert_eq!(fanout.aggregators.len(), 1);
        let f_aggr = fanout.aggregators.into_iter().next().unwrap();
        assert_eq!(
            f_aggr.aggregator_type,
            FanoutAggregationType::CountIf as i32
        );
        let filter = f_aggr.value_filter.unwrap();
        assert_eq!(filter.operator, FanoutComparisonOperator::Gt as i32);
        assert_eq!(filter.value, 10.0);
        assert_eq!(fanout.bucket_duration, 1000);
        assert_eq!(
            fanout.bucket_timestamp_type,
            BucketTimestampType::Start as i32
        );
        assert_eq!(
            fanout.bucket_alignment,
            BucketAlignmentType::Timestamp as i32
        );
        assert_eq!(fanout.alignment_timestamp, 555);
        assert!(fanout.report_empty);
        assert_eq!(filter.operator, FanoutComparisonOperator::Gt as i32);
        assert_eq!(filter.value, 10.0);
    }

    #[test]
    fn test_aggregation_options_multi_round_trip() {
        // multi list survives the round trip in column order
        let options = AggregationOptions {
            aggregations: smallvec::smallvec![
                AggregationType::Avg.into(),
                AggregationType::Max.into(),
                AggregationType::Count.into(),
            ],
            bucket_duration: 500,
            timestamp_output: BucketTimestamp::Start,
            alignment: BucketAlignment::Default,
            report_empty: false,
        };

        let fanout: FanoutAggregationOptions = (&options).into();
        assert_eq!(fanout.aggregators.len(), 3);
        assert_eq!(
            fanout.aggregators[0].aggregator_type,
            FanoutAggregationType::Avg as i32
        );

        let back: AggregationOptions = fanout.try_into().unwrap();
        assert_eq!(back, options);
    }

    #[test]
    fn test_aggregation_options_single_decode() {
        // a single-aggregator query is a one-element list
        let fanout = FanoutAggregationOptions {
            aggregators: vec![FanoutAggregationType::Sum.into()],
            bucket_duration: 10,
            bucket_timestamp_type: BucketTimestampType::Start as i32,
            bucket_alignment: BucketAlignmentType::Default as i32,
            alignment_timestamp: 0,
            report_empty: false,
        };
        let options: AggregationOptions = fanout.try_into().unwrap();
        assert!(!options.is_multi());
        assert_eq!(options.primary().aggregation_type(), AggregationType::Sum);

        // empty list => error
        let fanout = FanoutAggregationOptions {
            aggregators: vec![],
            bucket_duration: 10,
            bucket_timestamp_type: BucketTimestampType::Start as i32,
            bucket_alignment: BucketAlignmentType::Default as i32,
            alignment_timestamp: 0,
            report_empty: false,
        };
        let result: Result<AggregationOptions, _> = fanout.try_into();
        assert!(result.is_err());
    }

    #[test]
    fn test_aggregation_options_wire_validation() {
        let make = |aggregators: Vec<FanoutAggregatorConfig>| FanoutAggregationOptions {
            aggregators,
            bucket_duration: 10,
            bucket_timestamp_type: BucketTimestampType::Start as i32,
            bucket_alignment: BucketAlignmentType::Default as i32,
            alignment_timestamp: 0,
            report_empty: false,
        };

        // duplicates from the wire are rejected
        let dup = make(vec![
            FanoutAggregationType::Avg.into(),
            FanoutAggregationType::Avg.into(),
        ]);
        let result: Result<AggregationOptions, _> = dup.try_into();
        assert!(result.is_err());

        // > MAX_AGGREGATIONS rejected
        let too_many = make(vec![
            FanoutAggregationType::Avg.into();
            MAX_AGGREGATIONS + 1
        ]);
        let result: Result<AggregationOptions, _> = too_many.try_into();
        assert!(result.is_err());
    }

    #[test]
    fn test_fanout_to_aggregation_options_alignments() {
        let alignments = vec![
            (BucketAlignmentType::Default, BucketAlignment::Default),
            (BucketAlignmentType::AlignStart, BucketAlignment::Start),
            (BucketAlignmentType::AlignEnd, BucketAlignment::End),
        ];

        for (fanout_type, expected) in alignments {
            let aggregator = FanoutAggregatorConfig {
                aggregator_type: FanoutAggregationType::Max as i32,
                value_filter: None,
            };
            let fanout = FanoutAggregationOptions {
                aggregators: vec![aggregator],
                bucket_duration: 10,
                bucket_timestamp_type: BucketTimestampType::End as i32,
                bucket_alignment: fanout_type as i32,
                alignment_timestamp: 0,
                report_empty: false,
            };

            let options: AggregationOptions = fanout.try_into().unwrap();
            assert_eq!(options.alignment, expected);
        }
    }

    #[test]
    fn test_fanout_to_aggregation_options_invalid_duration() {
        let aggregator = FanoutAggregatorConfig {
            aggregator_type: FanoutAggregationType::Count as i32,
            value_filter: None,
        };
        let fanout = FanoutAggregationOptions {
            aggregators: vec![aggregator],
            bucket_duration: 0, // Invalid duration
            bucket_timestamp_type: BucketTimestampType::Mid as i32,
            bucket_alignment: BucketAlignmentType::Default as i32,
            alignment_timestamp: 0,
            report_empty: false,
        };

        let result: Result<AggregationOptions, ValkeyError> = fanout.try_into();
        assert!(result.is_err());
        if let Err(ValkeyError::Str(s)) = result {
            assert!(s.contains("bucket duration must be positive"));
        }
    }

    #[test]
    fn test_range_request_to_range_options_full() {
        let request = RangeRequest {
            range: Some(DateRange {
                start: 1000,
                end: 2000,
            }),
            count: 10,
            aggregation: Some(FanoutAggregationOptions {
                aggregators: vec![FanoutAggregationType::Avg.into()],
                bucket_duration: 60,
                bucket_timestamp_type: BucketTimestampType::Mid.into(),
                bucket_alignment: BucketAlignmentType::AlignStart.into(),
                alignment_timestamp: 0,
                report_empty: true,
            }),
            timestamp_filter: vec![1050, 1100],
            value_filter: Some(FanoutValueFilter {
                min: 10.5,
                max: 20.5,
            }),
            latest: true,
        };

        let options: RangeOptions = (&request)
            .try_into()
            .expect("Should convert to RangeOptions");

        assert_eq!(options.date_range.get_timestamps(None), (1000, 2000));
        assert_eq!(options.count, Some(10));

        let agg = options.aggregation.unwrap();
        assert_eq!(agg.primary().aggregation_type(), AggregationType::Avg);
        assert_eq!(agg.bucket_duration, 60);
        assert_eq!(agg.timestamp_output, BucketTimestamp::Mid);
        assert_eq!(agg.alignment, BucketAlignment::Start);
        assert!(agg.report_empty);

        assert_eq!(options.timestamp_filter, Some(vec![1050, 1100]));
        let val_filter = options.value_filter.unwrap();
        assert_eq!(val_filter.min, 10.5);
        assert_eq!(val_filter.max, 20.5);
        assert!(options.latest);
    }

    #[test]
    fn test_range_options_to_range_request_minimal() {
        let options = RangeOptions {
            date_range: TimestampRange::from_timestamps(500, 1500).unwrap(),
            count: None,
            aggregation: None,
            timestamp_filter: None,
            value_filter: None,
            latest: false,
        };

        let request: RangeRequest = (&options).into();

        assert_eq!(request.range.unwrap().start, 500);
        assert_eq!(request.range.unwrap().end, 1500);
        assert_eq!(request.count, 0);
        assert!(request.aggregation.is_none());
        assert!(request.timestamp_filter.is_empty());
        assert!(request.value_filter.is_none());
        assert!(!request.latest);
    }

    #[test]
    fn test_range_request_missing_range_fails() {
        let request = RangeRequest {
            range: None,
            ..Default::default()
        };

        let result: Result<RangeOptions, ValkeyError> = (&request).try_into();
        assert!(result.is_err());
        if let Err(ValkeyError::Str(s)) = result {
            assert!(s.contains("date range is required"));
        }
    }

    #[test]
    fn test_round_trip_conversion() {
        let aggregation = AggregatorConfig::new(
            AggregationType::CountIf,
            Some(ValueComparisonFilter {
                operator: ComparisonOperator::LessThan,
                value: 50.0,
            }),
        )
        .unwrap();
        let original_options = RangeOptions {
            date_range: TimestampRange::from_timestamps(100, 200).unwrap(),
            count: Some(5),
            aggregation: Some(AggregationOptions {
                aggregations: smallvec::smallvec![aggregation],
                bucket_duration: 10,
                timestamp_output: BucketTimestamp::End,
                alignment: BucketAlignment::Timestamp(123),
                report_empty: false,
            }),
            timestamp_filter: None,
            value_filter: Some(ValueFilter { min: 1.0, max: 2.0 }),
            latest: false,
        };

        let request: RangeRequest = (&original_options).into();
        let back_to_options: RangeOptions = (&request).try_into().expect("Round trip failed");

        assert_eq!(
            back_to_options.date_range.get_timestamps(None),
            original_options.date_range.get_timestamps(None)
        );
        assert_eq!(back_to_options.count, original_options.count);
        assert_eq!(
            back_to_options.aggregation.unwrap().alignment,
            BucketAlignment::Timestamp(123)
        );
        assert_eq!(back_to_options.value_filter.unwrap().min, 1.0);
        assert_eq!(back_to_options.value_filter.unwrap().max, 2.0);
    }
}
