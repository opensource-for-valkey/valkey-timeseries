use super::{JoinAsOfIter, JoinkitExt};
use super::{JoinType, JoinValue};
use crate::common::Sample;
use itertools::{EitherOrBoth, Itertools};

/// Joins two timestamp-sorted sample streams.
///
/// Every positional join is one sorted merge (`merge_join_by`) filtered to the rows the join
/// type keeps. A series holds at most one sample per timestamp, so the merge pairs samples
/// one-to-one; no join needs to fan a timestamp out across several rows.
pub fn create_join_iter<L, R, IL, IR>(
    left: IL,
    right: IR,
    join_type: JoinType,
) -> Box<dyn Iterator<Item = JoinValue>>
where
    L: Iterator<Item = Sample> + 'static,
    R: Iterator<Item = Sample> + 'static,
    IL: IntoIterator<IntoIter = L, Item = Sample>,
    IR: IntoIterator<IntoIter = R, Item = Sample>,
{
    match join_type {
        JoinType::AsOf(ref options) => {
            let iter = JoinAsOfIter::new(
                left,
                right,
                options.strategy,
                options.tolerance,
                options.allow_exact_match,
            );
            Box::new(iter)
        }
        JoinType::Semi => {
            let iter = left
                .into_iter()
                .join_semi(right, |item| item.timestamp)
                .map(JoinValue::left);

            Box::new(iter)
        }
        JoinType::Left => Box::new(
            merge(left.into_iter(), right.into_iter()).filter_map(|row| match row {
                EitherOrBoth::Right(_) => None,
                row => Some(JoinValue(row)),
            }),
        ),
        JoinType::Right => Box::new(
            merge(left.into_iter(), right.into_iter()).filter_map(|row| match row {
                EitherOrBoth::Left(_) => None,
                row => Some(JoinValue(row)),
            }),
        ),
        JoinType::Anti => Box::new(
            merge(left.into_iter(), right.into_iter()).filter_map(|row| match row {
                EitherOrBoth::Left(l) => Some(JoinValue::left(l)),
                _ => None,
            }),
        ),
        JoinType::Inner => Box::new(
            merge(left.into_iter(), right.into_iter()).filter_map(|row| match row {
                EitherOrBoth::Both(l, r) => Some(JoinValue::both(l, r)),
                _ => None,
            }),
        ),
        JoinType::Full => Box::new(merge(left.into_iter(), right.into_iter()).map(JoinValue)),
    }
}

/// Full outer merge of two timestamp-sorted streams.
fn merge<L, R>(left: L, right: R) -> impl Iterator<Item = EitherOrBoth<Sample, Sample>>
where
    L: Iterator<Item = Sample>,
    R: Iterator<Item = Sample>,
{
    left.merge_join_by(right, compare_by_timestamp)
}

#[inline]
fn compare_by_timestamp(left: &Sample, right: &Sample) -> std::cmp::Ordering {
    left.timestamp.cmp(&right.timestamp)
}
