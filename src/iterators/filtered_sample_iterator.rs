use crate::common::{Sample, Timestamp};
use crate::iterators::TimestampFilter;
use crate::series::ValueFilter;

pub struct FilteredSampleIterator<TIterator: Iterator<Item = Sample>> {
    inner: TIterator,
    value_filter: Option<ValueFilter>,
    ts_filter: Option<TimestampFilter>,
    first_ts: Timestamp,
    last_ts: Timestamp,
}

impl<TIterator: Iterator<Item = Sample>> FilteredSampleIterator<TIterator> {
    pub fn new(
        inner: TIterator,
        value_filter: Option<ValueFilter>,
        ts_filter: Option<&[Timestamp]>,
    ) -> Self {
        let (first_ts, last_ts) = if let Some(ts_filter) = &ts_filter
            && !ts_filter.is_empty()
        {
            (ts_filter[0], ts_filter[ts_filter.len() - 1])
        } else {
            (0, 0)
        };
        let ts_filter = ts_filter.map(TimestampFilter::new);
        Self {
            inner,
            value_filter,
            ts_filter,
            first_ts,
            last_ts,
        }
    }
}

impl<TIterator: Iterator<Item = Sample>> Iterator for FilteredSampleIterator<TIterator> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        for sample in self.inner.by_ref() {
            if let Some(ts_filter) = &self.ts_filter {
                let ts = sample.timestamp;
                if ts < self.first_ts {
                    continue;
                }
                if ts > self.last_ts {
                    return None;
                }
                if !ts_filter.matches(ts) {
                    continue;
                }
            }
            if let Some(value_filter) = &self.value_filter
                && !value_filter.is_match(sample.value)
            {
                continue;
            }
            return Some(sample);
        }
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}
