use crate::common::Sample;
use crate::iterators::SampleIter;
use crate::series::DuplicatePolicy;
use std::iter::Peekable;

/// Merges a chunk's stored samples (`left`) with incoming ones (`right`) in timestamp order,
/// resolving equal timestamps by the duplicate policy.
///
/// [`next_internal`](Self::next_internal) returns each surviving sample and whether the policy
/// refused the incoming one. On a refusal it emits the stored sample, with both inputs consumed,
/// because the rebuilt chunk still has to contain it. It is deliberately not an `Iterator`:
/// a caller has to see the flag, and one that dropped flagged samples would lose stored data.
pub(crate) struct SampleMergeIterator<'a> {
    left: Peekable<SampleIter<'a>>,
    right: Peekable<SampleIter<'a>>,
    duplicate_policy: DuplicatePolicy,
}

impl<'a> SampleMergeIterator<'a> {
    pub(crate) fn new(
        left: SampleIter<'a>,
        right: SampleIter<'a>,
        duplicate_policy: DuplicatePolicy,
    ) -> Self {
        SampleMergeIterator {
            left: left.peekable(),
            right: right.peekable(),
            duplicate_policy,
        }
    }

    pub(crate) fn next_internal(&mut self) -> Option<(Sample, bool)> {
        let mut blocked = false;

        let sample = match (self.left.peek(), self.right.peek()) {
            (Some(&left), Some(&right)) => {
                if left.timestamp == right.timestamp {
                    let ts = left.timestamp;
                    if let Ok(val) =
                        self.duplicate_policy
                            .duplicate_value(ts, left.value, right.value)
                    {
                        self.left.next();
                        self.right.next();
                        Some(Sample {
                            timestamp: ts,
                            value: val,
                        })
                    } else {
                        // BLOCK, or a NaN that MAX/MIN/SUM cannot combine. Both report
                        // `Duplicate` to the client on every write path, so one flag serves.
                        blocked = true;
                        self.right.next();
                        self.left.next()
                    }
                } else if left.timestamp < right.timestamp {
                    self.left.next()
                } else {
                    self.right.next()
                }
            }
            (Some(_), None) => self.left.next(),
            (None, Some(_)) => self.right.next(),
            (None, None) => None,
        };

        sample.map(|sample| (sample, blocked))
    }
}
