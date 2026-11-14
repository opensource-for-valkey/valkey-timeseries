use super::{JoinAsOfIter, JoinType, JoinValue};
use crate::common::Sample;
use itertools::{EitherOrBoth, Itertools};

/// Joins two timestamp-sorted sample streams.
///
/// Every exact-timestamp join is one sorted merge (`merge_join_by`) filtered to the rows the
/// join type keeps. A series holds at most one sample per timestamp, so the merge pairs
/// samples one-to-one, and SEMI is INNER keeping only the left side.
pub fn create_join_iter<L, R>(
    left: impl IntoIterator<Item = Sample, IntoIter = L>,
    right: impl IntoIterator<Item = Sample, IntoIter = R>,
    join_type: JoinType,
) -> Box<dyn Iterator<Item = JoinValue>>
where
    L: Iterator<Item = Sample> + 'static,
    R: Iterator<Item = Sample> + 'static,
{
    let keep: fn(JoinValue) -> Option<JoinValue> = match join_type {
        JoinType::AsOf(options) => {
            return Box::new(JoinAsOfIter::new(
                left,
                right,
                options.strategy,
                options.tolerance,
                options.allow_exact_match,
            ));
        }
        JoinType::Inner => |row| row.is_both().then_some(row),
        JoinType::Left => |row| row.has_left().then_some(row),
        JoinType::Right => |row| row.has_right().then_some(row),
        JoinType::Full => Some,
        JoinType::Semi => |row| match row {
            EitherOrBoth::Both(l, _) => Some(EitherOrBoth::Left(l)),
            _ => None,
        },
        JoinType::Anti => |row| row.is_left().then_some(row),
    };
    Box::new(
        left.into_iter()
            .merge_join_by(right, |l, r| l.timestamp.cmp(&r.timestamp))
            .filter_map(keep),
    )
}
