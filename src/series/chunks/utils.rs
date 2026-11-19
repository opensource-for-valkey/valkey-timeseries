use crate::common::Timestamp;
use smallvec::SmallVec;

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
