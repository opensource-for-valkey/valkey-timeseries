use super::{JoinAsOfIter, JoinType, JoinValue};
use crate::common::Sample;
use itertools::{Either, EitherOrBoth, Itertools};
use std::cell::Cell;
use std::rc::Rc;

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
        exact => {
            let (left, left_done) = Tracked::new(left.into_iter());
            let (right, right_done) = Tracked::new(right.into_iter());
            Either::Left(
                left.merge_join_by(right, |l, r| l.timestamp.cmp(&r.timestamp))
                    // Before the filter, which would otherwise pull the tail to find a row.
                    .take_while(move |_| !exhausted(exact, left_done.get(), right_done.get()))
                    .filter_map(move |row| keep(exact, row)),
            )
        }
    }
}

/// Whether an exact join of `join_type` can keep no further row, given which inputs have run
/// out. Once an input has, the merge yields only the other side's samples, unpaired: a join
/// that keeps only rows with a left sample (INNER, LEFT, SEMI, ANTI) is done when the left
/// input is, one that needs a right sample (INNER, SEMI, RIGHT) when the right input is.
///
/// Checked on each merged row: the merge reads both inputs before it yields a row, so the
/// row that reveals the end is already unpaired, and one this join drops.
#[inline]
fn exhausted(join_type: JoinType, left_done: bool, right_done: bool) -> bool {
    match join_type {
        JoinType::Inner | JoinType::Semi => left_done || right_done,
        JoinType::Left | JoinType::Anti => left_done,
        JoinType::Right => right_done,
        JoinType::Full => false,
        JoinType::AsOf(_) => unreachable!("ASOF joins do not merge"),
    }
}

/// An input that records when it returns `None`.
struct Tracked<I> {
    inner: I,
    done: Rc<Cell<bool>>,
}

impl<I> Tracked<I> {
    fn new(inner: I) -> (Self, Rc<Cell<bool>>) {
        let done = Rc::new(Cell::new(false));
        let tracked = Self {
            inner,
            done: Rc::clone(&done),
        };
        (tracked, done)
    }
}

impl<I: Iterator> Iterator for Tracked<I> {
    type Item = I::Item;

    #[inline]
    fn next(&mut self) -> Option<I::Item> {
        let next = self.inner.next();
        if next.is_none() {
            self.done.set(true);
        }
        next
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::join::JoinType;
    use std::cell::Cell;

    const EXACT: [JoinType; 6] = [
        JoinType::Inner,
        JoinType::Left,
        JoinType::Right,
        JoinType::Full,
        JoinType::Semi,
        JoinType::Anti,
    ];

    fn subset(mask: u32, value: f64) -> Vec<Sample> {
        (0..6)
            .filter(|i| mask & (1 << i) != 0)
            .map(|i| Sample::new(i * 10, value))
            .collect()
    }

    /// The early stop changes no answer: every pair of subsets of six timestamps, every exact
    /// join type, against the merge filtered to the end.
    #[test]
    fn stopping_early_matches_the_full_merge() {
        for join_type in EXACT {
            for left_mask in 0..64 {
                for right_mask in 0..64 {
                    let (left, right) = (subset(left_mask, 1.0), subset(right_mask, 2.0));
                    let want: Vec<_> = left
                        .iter()
                        .copied()
                        .merge_join_by(right.iter().copied(), |l, r| l.timestamp.cmp(&r.timestamp))
                        .filter_map(|row| keep(join_type, row))
                        .collect();
                    let got: Vec<_> =
                        create_join_iter(left.clone(), right.clone(), join_type).collect();
                    assert_eq!(got, want, "{join_type:?} left={left:?} right={right:?}");
                }
            }
        }
    }

    /// Once the side a join keeps rows from has run out, the other side's tail is not read.
    #[test]
    fn stops_reading_the_tail_once_no_row_can_be_kept() {
        for join_type in EXACT {
            let tail_pulled = Cell::new(0usize);
            let tail = (1..=1_000_000i64).map(|i| {
                tail_pulled.set(tail_pulled.get() + 1);
                Sample::new(i * 10, 0.0)
            });
            let one = [Sample::new(10, 1.0)];
            // RIGHT keeps rows with a right sample, so its long side is the left input.
            let rows = if join_type == JoinType::Right {
                create_join_iter(tail, one, join_type).count()
            } else {
                create_join_iter(one, tail, join_type).count()
            };
            if join_type == JoinType::Full {
                assert_eq!(tail_pulled.get(), 1_000_000, "FULL keeps every row");
                continue;
            }
            assert!(rows <= 1, "{join_type:?}");
            assert!(
                tail_pulled.get() <= 3,
                "{join_type:?} read {} tail samples",
                tail_pulled.get()
            );
        }
    }
}
