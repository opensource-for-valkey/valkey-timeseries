use crate::common::Sample;
use crate::series::DuplicatePolicy;
use std::iter::Peekable;

/// Merges a chunk's stored samples (`left`) with incoming ones (`right`) in timestamp order,
/// resolving equal timestamps by the duplicate policy.
///
/// [`next_internal`](Self::next_internal) returns each surviving sample, whether the policy
/// refused the incoming one, and whether the step consumed an incoming sample. On a refusal it
/// emits the stored sample, with both inputs consumed, because the rebuilt chunk still has to
/// contain it. It is deliberately not an `Iterator`: a caller has to see the flags, and one that
/// dropped flagged samples would lose stored data.
pub(crate) struct SampleMergeIterator<L: Iterator<Item = Sample>, R: Iterator<Item = Sample>> {
    left: Peekable<L>,
    right: Peekable<R>,
    duplicate_policy: DuplicatePolicy,
}

impl<L: Iterator<Item = Sample>, R: Iterator<Item = Sample>> SampleMergeIterator<L, R> {
    pub(crate) fn new(left: L, right: R, duplicate_policy: DuplicatePolicy) -> Self {
        SampleMergeIterator {
            left: left.peekable(),
            right: right.peekable(),
            duplicate_policy,
        }
    }

    /// The next sample to store, as `(sample, blocked, from_right)`: `blocked` when the policy
    /// refused the incoming sample at that timestamp, `from_right` when this step consumed an
    /// incoming sample (so it owes the caller a result for it).
    pub(crate) fn next_internal(&mut self) -> Option<(Sample, bool, bool)> {
        let mut blocked = false;
        let mut from_right = false;

        let sample = match (self.left.peek(), self.right.peek()) {
            (Some(&left), Some(&right)) => {
                if left.timestamp == right.timestamp {
                    let ts = left.timestamp;
                    from_right = true;
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
                    from_right = true;
                    self.right.next()
                }
            }
            (Some(_), None) => self.left.next(),
            (None, Some(_)) => {
                from_right = true;
                self.right.next()
            }
            (None, None) => None,
        };

        sample.map(|sample| (sample, blocked, from_right))
    }
}
