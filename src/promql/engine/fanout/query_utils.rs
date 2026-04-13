use crate::common::{Sample, Timestamp};
use crate::labels::MetricName;
use crate::labels::filters::SeriesSelector;
use crate::promql::generated::Label as PromLabel;
use crate::promql::generated::Sample as PromSample;
use crate::promql::generated::{
    InstantQueryResponse, InstantSample, RangeQueryResponse, RangeSample,
};
use crate::series::index::series_by_selectors;
use orx_parallel::IterIntoParIter;
use orx_parallel::ParIter;
use std::ops::Deref;
use valkey_module::{Context, ValkeyResult};

pub(super) fn handle_instant_query(
    ctx: &Context,
    selector: SeriesSelector,
    timestamp: Timestamp,
) -> ValkeyResult<InstantQueryResponse> {
    let series = series_by_selectors(ctx, &[selector], None)?;
    let samples = series
        .iter()
        .map(|(s, k)| {
            let series = s.deref();
            let key = k.to_string();
            (series, key)
        })
        .iter_into_par()
        .filter_map(|(s, key)| {
            match s.get_sample(timestamp) {
                Ok(Some(sample)) => {
                    let labels: Vec<crate::promql::generated::Label> = s
                        .labels
                        .iter()
                        .map(|label| crate::promql::generated::Label {
                            name: label.name.to_string(),
                            value: label.value.to_string(),
                        })
                        .collect();
                    Some(InstantSample {
                        labels,
                        value: sample.value,
                        timestamp: sample.timestamp,
                        key,
                    })
                }
                Ok(None) => None,
                Err(_) => None, // todo: log error
            }
        })
        .collect::<Vec<_>>();

    Ok(InstantQueryResponse { samples })
}

pub(super) fn handle_range_query(
    ctx: &Context,
    selector: SeriesSelector,
    start_time: i64,
    end_time: i64,
) -> ValkeyResult<RangeQueryResponse> {
    let series = series_by_selectors(ctx, &[selector], None)?;
    let ranges = series
        .iter()
        .map(|(s, _)| s.deref())
        .iter_into_par()
        .filter_map(|s| {
            let series_samples = s.get_range(start_time, end_time);
            if series_samples.is_empty() {
                return None;
            }
            let samples: Vec<PromSample> = series_samples.into_iter().map(Sample::into).collect();
            let labels = convert_labels(&s.labels);
            let range = RangeSample { labels, samples };
            Some(range)
        })
        .collect::<Vec<_>>();

    Ok(RangeQueryResponse { series: ranges })
}

fn convert_labels(mn: &MetricName) -> Vec<PromLabel> {
    mn.iter()
        .map(|label| PromLabel {
            name: label.name.to_string(),
            value: label.value.to_string(),
        })
        .collect()
}
