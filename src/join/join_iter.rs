use super::{JoinAsOfIter, JoinType, JoinValue};
use crate::common::Sample;
use itertools::{Either, EitherOrBoth, Itertools};

/// Joins two timestamp-sorted sample streams.
///
/// Every exact-timestamp join is one sorted merge (`merge_join_by`) filtered to the rows the
/// join type keeps. A series holds at most one sample per timestamp, so the merge pairs
/// samples one-to-one, and SEMI is INNER keeping only the left side.
///
/// Statically dispatched: a boxed iterator with a function-pointer filter measured 3.7–5.3×
/// slower per row than this shape (`benches/join.rs`, `join/merge`).
pub fn create_join_iter<L, R>(
    left: impl IntoIterator<Item = Sample, IntoIter = L>,
    right: impl IntoIterator<Item = Sample, IntoIter = R>,
    join_type: JoinType,
) -> impl Iterator<Item = JoinValue>
where
    L: Iterator<Item = Sample>,
    R: Iterator<Item = Sample>,
{
    match join_type {
        JoinType::AsOf(options) => Either::Right(JoinAsOfIter::new(
            left,
            right,
            options.strategy,
            options.tolerance,
            options.allow_exact_match,
        )),
        exact => Either::Left(
            left.into_iter()
                .merge_join_by(right, |l, r| l.timestamp.cmp(&r.timestamp))
                .filter_map(move |row| keep(exact, row)),
        ),
    }
}

/// The row an exact join of `join_type` makes of a merged pair, if it keeps one.
#[inline]
fn keep(join_type: JoinType, row: JoinValue) -> Option<JoinValue> {
    let kept = match join_type {
        JoinType::Inner => row.is_both(),
        JoinType::Left => row.has_left(),
        JoinType::Right => row.has_right(),
        JoinType::Full => true,
        JoinType::Anti => row.is_left(),
        JoinType::Semi => {
            return match row {
                EitherOrBoth::Both(l, _) => Some(EitherOrBoth::Left(l)),
                _ => None,
            };
        }
        JoinType::AsOf(_) => unreachable!("ASOF joins do not merge"),
    };
    kept.then_some(row)
}
