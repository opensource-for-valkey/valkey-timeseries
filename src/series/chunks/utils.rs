use crate::common::{Sample, Timestamp};
use crate::series::types::ValueFilter;
use smallvec::SmallVec;

#[inline]
pub(crate) fn filter_samples_by_value(samples: &mut Vec<Sample>, value_filter: &ValueFilter) {
    samples.retain(|s| s.value >= value_filter.min && s.value <= value_filter.max)
}

pub(crate) fn filter_timestamp_slice(
    ts_filter: &[Timestamp],
    start: Timestamp,
    end: Timestamp,
) -> SmallVec<[Timestamp; 32]> {
    let mut filtered: SmallVec<[Timestamp; 32]> = ts_filter
        .iter()
        .filter_map(|ts| {
            let ts = *ts;
            if ts >= start && ts <= end {
                Some(ts)
            } else {
                None
            }
        })
        .collect();

    filtered.sort();
    filtered.dedup();
    filtered
}
