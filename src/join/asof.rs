//! `TS.JOIN ... ASOF`: match each left sample with one right sample near it in time.
//!
//! Both inputs come from a series, so their timestamps strictly increase. The matcher keeps a
//! single cursor into the right input: `cursor` is the first right sample at or after the
//! current left timestamp. Because left timestamps also increase, the cursor only moves
//! forward, and every candidate a strategy can choose sits next to it:
//!
//! - `before`: the sample just ahead of the cursor (strictly earlier than `left`),
//! - `exact`:  the sample at the cursor, when its timestamp equals `left`,
//! - `after`:  the first sample strictly later than `left`.
//!
//! Exact-match eligibility is decided per left sample from those three, so no earlier left
//! sample can leave behind a candidate that a later one is not allowed to use.
use crate::common::Sample;
use crate::join::JoinValue;
use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum AsOfJoinStrategy {
    /// The last right sample at or before the left sample.
    #[default]
    Backward,
    /// The first right sample at or after the left sample.
    Forward,
    /// The right sample nearest the left sample; when two are equally near, the later one.
    Nearest,
}

/// Picks the right sample `left` joins with, if any. `cursor` must be the first index whose
/// timestamp is not before the previous left sample's; it is advanced for `left`.
fn asof_match(
    right: &[Sample],
    cursor: &mut usize,
    left: &Sample,
    strategy: AsOfJoinStrategy,
    allow_exact_match: bool,
) -> Option<Sample> {
    let ts = left.timestamp;
    while right.get(*cursor).is_some_and(|r| r.timestamp < ts) {
        *cursor += 1;
    }
    let before = cursor.checked_sub(1).map(|i| right[i]);
    let exact = right.get(*cursor).filter(|r| r.timestamp == ts).copied();
    let after = right[*cursor..].iter().find(|r| r.timestamp > ts).copied();

    if allow_exact_match && exact.is_some() {
        return exact;
    }
    match strategy {
        AsOfJoinStrategy::Backward => before,
        AsOfJoinStrategy::Forward => after,
        AsOfJoinStrategy::Nearest => match (before, after) {
            (Some(b), Some(a)) => {
                // Ties go to the later sample.
                if ts.abs_diff(a.timestamp) <= ts.abs_diff(b.timestamp) {
                    Some(a)
                } else {
                    Some(b)
                }
            }
            (b, a) => b.or(a),
        },
    }
}

/// Streams ASOF rows: lazy over the left input, with the right input held in memory.
pub struct JoinAsOfIter<L> {
    left: L,
    right: Vec<Sample>,
    cursor: usize,
    strategy: AsOfJoinStrategy,
    /// Maximum distance in milliseconds; `None` for no limit.
    tolerance: Option<u64>,
    allow_exact_match: bool,
}

impl<L: Iterator<Item = Sample>> JoinAsOfIter<L> {
    pub fn new(
        left: impl IntoIterator<Item = Sample, IntoIter = L>,
        right: impl IntoIterator<Item = Sample>,
        strategy: AsOfJoinStrategy,
        tolerance: Option<Duration>,
        allow_exact_match: bool,
    ) -> Self {
        let right: Vec<Sample> = right.into_iter().collect();
        debug_assert!(
            right.is_sorted_by(|a, b| a.timestamp < b.timestamp),
            "ASOF right input must have strictly increasing timestamps"
        );
        Self {
            left: left.into_iter(),
            right,
            cursor: 0,
            strategy,
            tolerance: tolerance.map(|t| u64::try_from(t.as_millis()).unwrap_or(u64::MAX)),
            allow_exact_match,
        }
    }
}

impl<L: Iterator<Item = Sample>> Iterator for JoinAsOfIter<L> {
    type Item = JoinValue;

    fn next(&mut self) -> Option<Self::Item> {
        for left in self.left.by_ref() {
            let Some(right) = asof_match(
                &self.right,
                &mut self.cursor,
                &left,
                self.strategy,
                self.allow_exact_match,
            ) else {
                continue;
            };
            // The chosen candidate is the nearest one the strategy allows, so if it is out of
            // tolerance, every other candidate is too.
            if self
                .tolerance
                .is_none_or(|t| left.timestamp.abs_diff(right.timestamp) <= t)
            {
                return Some(JoinValue::Both(left, right));
            }
        }
        None
    }
}

#[cfg(test)]
mod test {
    use super::*;

    /// The pairs an ASOF join of `left` and `right` produces; `tolerance` in milliseconds.
    fn join_asof_samples(
        left: &[Sample],
        right: &[Sample],
        strategy: AsOfJoinStrategy,
        tolerance: Option<i64>,
        allow_eq: bool,
    ) -> Vec<(Sample, Sample)> {
        let tolerance = tolerance.map(|t| Duration::from_millis(t.unsigned_abs()));
        JoinAsOfIter::new(left.to_vec(), right.to_vec(), strategy, tolerance, allow_eq)
            .map(|row| match row {
                JoinValue::Both(l, r) => (l, r),
                other => panic!("ASOF produced a one-sided row: {other:?}"),
            })
            .collect()
    }

    /// The obviously-correct definition: look at every right sample.
    fn oracle(
        left: &[Sample],
        right: &[Sample],
        strategy: AsOfJoinStrategy,
        tolerance: Option<u64>,
        allow_eq: bool,
    ) -> Vec<(Sample, Sample)> {
        let mut out = vec![];
        for l in left {
            let eligible = right.iter().filter(|r| {
                let ok_side = match strategy {
                    AsOfJoinStrategy::Backward => r.timestamp <= l.timestamp,
                    AsOfJoinStrategy::Forward => r.timestamp >= l.timestamp,
                    AsOfJoinStrategy::Nearest => true,
                };
                ok_side && (allow_eq || r.timestamp != l.timestamp)
            });
            // Nearest by distance; ties (and Backward's "last") go to the later sample, while
            // Forward wants the earliest.
            let best = eligible.min_by(|a, b| {
                let da = l.timestamp.abs_diff(a.timestamp);
                let db = l.timestamp.abs_diff(b.timestamp);
                da.cmp(&db).then(b.timestamp.cmp(&a.timestamp))
            });
            if let Some(r) = best
                && tolerance.is_none_or(|t| l.timestamp.abs_diff(r.timestamp) <= t)
            {
                out.push((*l, *r));
            }
        }
        out
    }

    /// Every pair of subsets of `0..6` as left/right timestamps, every strategy, exact match
    /// on and off, and tolerances none/0/1/2. Several left samples per case, so a candidate
    /// one left sample selects is visible to the next.
    #[test]
    fn matches_exhaustive_oracle() {
        const N: u32 = 6;
        let subset = |mask: u32, value: f64| -> Vec<Sample> {
            (0..N)
                .filter(|i| mask & (1 << i) != 0)
                .map(|i| Sample::new(i as i64 * 10, value + i as f64))
                .collect()
        };
        let mut cases = 0;
        for left_mask in 0..(1u32 << N) {
            for right_mask in 0..(1u32 << N) {
                let left = subset(left_mask, 0.0);
                let right = subset(right_mask, 100.0);
                for strategy in [
                    AsOfJoinStrategy::Backward,
                    AsOfJoinStrategy::Forward,
                    AsOfJoinStrategy::Nearest,
                ] {
                    for allow_eq in [true, false] {
                        for tolerance in [None, Some(0), Some(10), Some(20)] {
                            let got = join_asof_samples(
                                &left,
                                &right,
                                strategy,
                                tolerance.map(|t| t as i64),
                                allow_eq,
                            );
                            let want = oracle(&left, &right, strategy, tolerance, allow_eq);
                            assert_eq!(
                                got, want,
                                "left={left:?} right={right:?} {strategy:?} allow_eq={allow_eq} tolerance={tolerance:?}"
                            );
                            cases += 1;
                        }
                    }
                }
            }
        }
        assert_eq!(cases, 64 * 64 * 3 * 2 * 4);
    }

    /// The case that used to fail: the first left sample takes the right sample at 10 as its
    /// nearest, and the second left sample, at 10 itself, must not reuse it.
    #[test]
    fn nearest_excluded_exact_match_is_not_reused() {
        let left = [Sample::new(9, 1.0), Sample::new(10, 2.0)];
        for (right, expected) in [
            (
                vec![Sample::new(10, 10.0), Sample::new(20, 20.0)],
                vec![(9, 10), (10, 20)],
            ),
            (vec![Sample::new(10, 10.0)], vec![(9, 10)]),
        ] {
            let got: Vec<_> =
                join_asof_samples(&left, &right, AsOfJoinStrategy::Nearest, None, false)
                    .iter()
                    .map(|(l, r)| (l.timestamp, r.timestamp))
                    .collect();
            assert_eq!(got, expected);
        }
    }

    #[test]
    fn consumes_left_lazily() {
        let mut pulled = 0;
        let left = (0..5).map(|i| {
            pulled += 1;
            Sample::new(i * 10, i as f64)
        });
        let right: Vec<Sample> = (0..5).map(|i| Sample::new(i * 10, 0.0)).collect();
        let first: Vec<_> = JoinAsOfIter::new(left, right, AsOfJoinStrategy::Backward, None, true)
            .take(1)
            .collect();
        assert_eq!(first.len(), 1);
        assert_eq!(pulled, 1);
    }

    #[test]
    fn test_join_asof_backward() {
        let left = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 20,
                value: 2.0,
            },
            Sample {
                timestamp: 30,
                value: 3.0,
            },
            Sample {
                timestamp: 40,
                value: 4.0,
            },
        ];

        let right = vec![
            Sample {
                timestamp: 5,
                value: 10.0,
            },
            Sample {
                timestamp: 15,
                value: 20.0,
            },
            Sample {
                timestamp: 25,
                value: 30.0,
            },
            Sample {
                timestamp: 35,
                value: 40.0,
            },
        ];

        let result = join_asof_samples(&left, &right, AsOfJoinStrategy::Backward, None, true);

        assert_eq!(result.len(), 4);
        assert_eq!(result[0].0.timestamp, 10);
        assert_eq!(result[0].1.timestamp, 5);
        assert_eq!(result[1].0.timestamp, 20);
        assert_eq!(result[1].1.timestamp, 15);
        assert_eq!(result[2].0.timestamp, 30);
        assert_eq!(result[2].1.timestamp, 25);
        assert_eq!(result[3].0.timestamp, 40);
        assert_eq!(result[3].1.timestamp, 35);
    }

    #[test]
    fn test_join_asof_reverse_with_tolerance() {
        let left = vec![
            Sample::new(1000, 0.),
            Sample::new(2000, 10.),
            Sample::new(3000, 20.),
            Sample::new(4000, 30.0),
            Sample::new(5000, 40.0),
            Sample::new(6000, 50.),
            Sample::new(7000, 60.),
            Sample::new(8000, 70.),
            Sample::new(9000, 80.),
            Sample::new(10000, 90.),
        ];

        let right = vec![
            Sample::new(1800, 100.0),
            Sample::new(2800, 200.0),
            Sample::new(3800, 300.0),
            Sample::new(4800, 400.0),
        ];

        let tolerance = Some(500);
        let result = join_asof_samples(&left, &right, AsOfJoinStrategy::Backward, tolerance, true);

        assert!(result.len() == 4);
        assert_eq!(result[0].0.timestamp, 2000);
        assert_eq!(result[0].1.timestamp, 1800);
        assert_eq!(result[1].0.timestamp, 3000);
        assert_eq!(result[1].1.timestamp, 2800);
        assert_eq!(result[2].0.timestamp, 4000);
        assert_eq!(result[2].1.timestamp, 3800);
        assert_eq!(result[3].0.timestamp, 5000);
        assert_eq!(result[3].1.timestamp, 4800);
    }

    #[test]
    fn test_join_asof_forward() {
        let left = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 20,
                value: 2.0,
            },
            Sample {
                timestamp: 30,
                value: 3.0,
            },
            Sample {
                timestamp: 40,
                value: 4.0,
            },
        ];

        let right = vec![
            Sample {
                timestamp: 15,
                value: 10.0,
            },
            Sample {
                timestamp: 25,
                value: 20.0,
            },
            Sample {
                timestamp: 35,
                value: 30.0,
            },
            Sample {
                timestamp: 45,
                value: 40.0,
            },
        ];

        let result = join_asof_samples(&left, &right, AsOfJoinStrategy::Forward, None, true);

        assert_eq!(result.len(), 4);
        assert_eq!(result[0].0.timestamp, 10);
        assert_eq!(result[0].1.timestamp, 15);
        assert_eq!(result[1].0.timestamp, 20);
        assert_eq!(result[1].1.timestamp, 25);
        assert_eq!(result[2].0.timestamp, 30);
        assert_eq!(result[2].1.timestamp, 35);
        assert_eq!(result[3].0.timestamp, 40);
        assert_eq!(result[3].1.timestamp, 45);
    }

    #[test]
    fn test_join_asof_nearest() {
        let left = vec![
            Sample {
                timestamp: 12,
                value: 1.0,
            },
            Sample {
                timestamp: 22,
                value: 2.0,
            },
            Sample {
                timestamp: 32,
                value: 3.0,
            },
            Sample {
                timestamp: 42,
                value: 4.0,
            },
        ];

        let right = vec![
            Sample {
                timestamp: 10,
                value: 10.0,
            },
            Sample {
                timestamp: 20,
                value: 20.0,
            },
            Sample {
                timestamp: 30,
                value: 30.0,
            },
            Sample {
                timestamp: 40,
                value: 40.0,
            },
        ];

        let result = join_asof_samples(&left, &right, AsOfJoinStrategy::Nearest, None, true);

        assert_eq!(result.len(), 4);
        assert_eq!(result[0].0.timestamp, 12);
        assert_eq!(result[0].1.timestamp, 10);
        assert_eq!(result[1].0.timestamp, 22);
        assert_eq!(result[1].1.timestamp, 20);
        assert_eq!(result[2].0.timestamp, 32);
        assert_eq!(result[2].1.timestamp, 30);
        assert_eq!(result[3].0.timestamp, 42);
        assert_eq!(result[3].1.timestamp, 40);
    }

    #[test]
    fn test_join_asof_with_tolerance() {
        let left = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 20,
                value: 2.0,
            },
            Sample {
                timestamp: 30,
                value: 3.0,
            },
            Sample {
                timestamp: 40,
                value: 4.0,
            },
        ];

        let right = vec![
            Sample {
                timestamp: 8,
                value: 10.0,
            },
            Sample {
                timestamp: 18,
                value: 20.0,
            },
            Sample {
                timestamp: 28,
                value: 30.0,
            },
            Sample {
                timestamp: 35,
                value: 40.0,
            },
        ];

        // With tolerance 3
        let result = join_asof_samples(&left, &right, AsOfJoinStrategy::Backward, Some(3), true);

        assert_eq!(result.len(), 3);
        assert_eq!(result[0].0.timestamp, 10);
        assert_eq!(result[0].1.timestamp, 8);
        assert_eq!(result[1].0.timestamp, 20);
        assert_eq!(result[1].1.timestamp, 18);
        assert_eq!(result[2].0.timestamp, 30);
        assert_eq!(result[2].1.timestamp, 28);
        // No match for timestamp 40 because nearest backward (35) is outside tolerance
    }

    #[test]
    fn test_join_asof_empty_inputs() {
        let left: Vec<Sample> = vec![];
        let right = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 20,
                value: 2.0,
            },
        ];

        let result = join_asof_samples(&left, &right, AsOfJoinStrategy::Backward, None, true);
        assert_eq!(result.len(), 0);

        let left = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 20,
                value: 2.0,
            },
        ];
        let right: Vec<Sample> = vec![];

        let result = join_asof_samples(&left, &right, AsOfJoinStrategy::Backward, None, true);
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_join_asof_exact_matches() {
        let left = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 20,
                value: 2.0,
            },
            Sample {
                timestamp: 30,
                value: 3.0,
            },
        ];

        let right = vec![
            Sample {
                timestamp: 10,
                value: 10.0,
            },
            Sample {
                timestamp: 20,
                value: 20.0,
            },
            Sample {
                timestamp: 30,
                value: 30.0,
            },
        ];

        // With allow_eq = true
        let result = join_asof_samples(&left, &right, AsOfJoinStrategy::Backward, None, true);

        assert_eq!(result.len(), 3);
        for res in result.iter() {
            assert_eq!(res.0.timestamp, res.1.timestamp);
        }

        // With allow_eq = false
        // Note that we specify a tolerance here otherwise the test would fail. This is because
        // otherwise the algorithm would simply scan backwards and therefore find a non-equal match.
        let result = join_asof_samples(&left, &right, AsOfJoinStrategy::Backward, Some(5), false);
        assert_eq!(result.len(), 0); // No matches because exact equality is not allowed
    }

    #[test]
    fn test_join_asof_nearest_equidistant() {
        let left = vec![
            Sample {
                timestamp: 15,
                value: 1.0,
            }, // Equidistant from 10 and 20
            Sample {
                timestamp: 25,
                value: 2.0,
            }, // Equidistant from 20 and 30
        ];

        let right = vec![
            Sample {
                timestamp: 10,
                value: 10.0,
            },
            Sample {
                timestamp: 20,
                value: 20.0,
            },
            Sample {
                timestamp: 30,
                value: 30.0,
            },
        ];

        // Nearest should prefer the later value when equidistant
        let result = join_asof_samples(&left, &right, AsOfJoinStrategy::Nearest, None, true);

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0.timestamp, 15);
        assert_eq!(result[0].1.timestamp, 20); // Should take later value when equidistant
        assert_eq!(result[1].0.timestamp, 25);
        assert_eq!(result[1].1.timestamp, 30);
    }
}
