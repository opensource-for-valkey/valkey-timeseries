use crate::common::Sample;
use std::cmp::Ordering;
use std::fmt::Display;
use valkey_module::ValkeyError;

// Copyright (c) 2020 Ritchie Vink
// Some portions Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
mod default;
mod join_asof_iter;

use crate::error_consts;
pub(crate) use default::*;
pub(crate) use join_asof_iter::*;

pub type IdxSize = usize;

#[inline]
fn ge_allow_eq<T: PartialOrd + Copy>(l: T, r: T, allow_eq: bool) -> bool {
    match l.partial_cmp(&r) {
        Some(Ordering::Equal) => allow_eq,
        Some(Ordering::Greater) => true,
        _ => false,
    }
}

#[inline]
fn lt_allow_eq<T: PartialOrd + Copy>(l: T, r: T, allow_eq: bool) -> bool {
    match l.partial_cmp(&r) {
        Some(Ordering::Equal) => allow_eq,
        Some(Ordering::Less) => true,
        _ => false,
    }
}

trait AsOfJoinState: Default {
    fn new(allow_eq: bool) -> Self;

    fn next<F: FnMut(IdxSize) -> Option<Sample>>(
        &mut self,
        left_val: &Sample,
        right: F,
        n_right: IdxSize,
    ) -> Option<IdxSize>;
}

#[derive(Default)]
struct AsOfJoinForwardState {
    scan_offset: IdxSize,
    allow_eq: bool,
}

impl AsOfJoinState for AsOfJoinForwardState {
    fn new(allow_eq: bool) -> Self {
        AsOfJoinForwardState {
            scan_offset: 0,
            allow_eq,
        }
    }

    #[inline]
    fn next<F: FnMut(IdxSize) -> Option<Sample>>(
        &mut self,
        left_val: &Sample,
        mut right: F,
        n_right: IdxSize,
    ) -> Option<IdxSize> {
        while self.scan_offset < n_right {
            if let Some(right_val) = right(self.scan_offset)
                && ge_allow_eq(right_val.timestamp, left_val.timestamp, self.allow_eq)
            {
                return Some(self.scan_offset);
            }
            self.scan_offset += 1;
        }
        None
    }
}

#[derive(Default)]
struct AsOfJoinBackwardState {
    // best_bound is the greatest right index <= left_val.
    best_bound: Option<IdxSize>,
    scan_offset: IdxSize,
    allow_eq: bool,
}

impl AsOfJoinState for AsOfJoinBackwardState {
    fn new(allow_eq: bool) -> Self {
        AsOfJoinBackwardState {
            best_bound: None,
            scan_offset: 0,
            allow_eq,
        }
    }

    #[inline]
    fn next<F: FnMut(IdxSize) -> Option<Sample>>(
        &mut self,
        left_val: &Sample,
        mut right: F,
        n_right: IdxSize,
    ) -> Option<IdxSize> {
        while self.scan_offset < n_right {
            if let Some(right_val) = right(self.scan_offset) {
                if lt_allow_eq(right_val.timestamp, left_val.timestamp, self.allow_eq) {
                    self.best_bound = Some(self.scan_offset);
                } else {
                    break;
                }
            }
            self.scan_offset += 1;
        }
        self.best_bound
    }
}

#[derive(Default)]
struct AsOfJoinNearestState {
    // best_bound is the nearest value to left_val, with ties broken towards the last element.
    best_bound: Option<IdxSize>,
    scan_offset: IdxSize,
    allow_eq: bool,
}

impl AsOfJoinState for AsOfJoinNearestState {
    #[inline]
    fn new(allow_eq: bool) -> Self {
        AsOfJoinNearestState {
            best_bound: None,
            scan_offset: 0,
            allow_eq,
        }
    }

    #[inline]
    fn next<F: FnMut(IdxSize) -> Option<Sample>>(
        &mut self,
        left_val: &Sample,
        mut right: F,
        n_right: IdxSize,
    ) -> Option<IdxSize> {
        // Skipping ahead to the first value greater than left_val. This is
        // cheaper than computing differences.
        while self.scan_offset < n_right {
            if let Some(scan_right_val) = right(self.scan_offset) {
                if lt_allow_eq(scan_right_val.timestamp, left_val.timestamp, self.allow_eq) {
                    self.best_bound = Some(self.scan_offset);
                } else if !self.allow_eq && scan_right_val.timestamp == left_val.timestamp {
                    // An exact match is excluded for this left sample, but it remains a
                    // candidate for every later one, so look past it without consuming it.
                    // Letting it into the distance comparison below picked it every time
                    // (a distance of zero always wins).
                    return self.nearest_past_exact(left_val, right, n_right);
                } else {
                    // Now we must compute a difference to see if scan_right_val
                    // is closer than our current best bound.
                    let scan_is_better = if let Some(best_idx) = self.best_bound {
                        debug_assert!(best_idx < n_right);
                        // SAFETY:
                        // - `best_bound` is only ever assigned from `self.scan_offset`
                        //   when `right(self.scan_offset)` has just returned `Some`,
                        //   so it only stores indices for which `right` previously
                        //   yielded a value.
                        // - `self.scan_offset` is monotonically increasing and the
                        //   outer loops ensure `self.scan_offset < n_right` whenever
                        //   `best_bound` is used, so `best_idx < n_right` holds.

                        // - We rely on `right` being consistent: for a given index that
                        //   previously returned `Some`, it will continue to do so.
                        //   If this invariant is violated, the `expect` below will
                        //   panic rather than causing undefined behavior.
                        let best_right_val = right(best_idx).expect(
                            "inconsistent `right` callback: previously returned Some for this index",
                        );

                        let left_ts = left_val.timestamp;

                        let best_diff = left_ts.abs_diff(best_right_val.timestamp);
                        let scan_diff = left_ts.abs_diff(scan_right_val.timestamp);

                        // Ties go to the later sample ("the last row ... nearest"). This used
                        // to borrow `allow_eq`, which is about exact timestamp matches, and so
                        // broke ties the other way whenever exact matches were disallowed.
                        scan_diff <= best_diff
                    } else {
                        true
                    };

                    if scan_is_better {
                        self.best_bound = Some(self.scan_offset);
                        self.scan_offset += 1;

                        // It is possible there are later elements equal to our
                        // scan, so keep going on.
                        while self.scan_offset < n_right {
                            if let Some(next_right_val) = right(self.scan_offset) {
                                if next_right_val == scan_right_val && self.allow_eq {
                                    self.best_bound = Some(self.scan_offset);
                                } else {
                                    break;
                                }
                            }

                            self.scan_offset += 1;
                        }
                    }

                    break;
                }
            }

            self.scan_offset += 1;
        }

        self.best_bound
    }
}

impl AsOfJoinNearestState {
    /// The nearest match for `left_val` when `right(self.scan_offset)` is an excluded exact
    /// match: the better of the current best bound (the last sample before it) and the first
    /// sample after the run of equal timestamps. Leaves the scan state untouched, so later left
    /// samples still see the equal-timestamp run.
    fn nearest_past_exact<F: FnMut(IdxSize) -> Option<Sample>>(
        &self,
        left_val: &Sample,
        mut right: F,
        n_right: IdxSize,
    ) -> Option<IdxSize> {
        let left_ts = left_val.timestamp;
        let mut ahead = self.scan_offset + 1;
        while ahead < n_right && right(ahead).is_some_and(|r| r.timestamp == left_ts) {
            ahead += 1;
        }
        let later = (ahead < n_right)
            .then(|| right(ahead).map(|r| (ahead, r.timestamp)))
            .flatten();
        let earlier = self
            .best_bound
            .and_then(|idx| right(idx).map(|r| (idx, r.timestamp)));
        match (earlier, later) {
            (Some((e_idx, e_ts)), Some((l_idx, l_ts))) => {
                // Ties go to the later sample, as in `next`.
                if left_ts.abs_diff(l_ts) <= left_ts.abs_diff(e_ts) {
                    Some(l_idx)
                } else {
                    Some(e_idx)
                }
            }
            (Some((idx, _)), None) | (None, Some((idx, _))) => Some(idx),
            (None, None) => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Hash)]
pub enum AsOfJoinStrategy {
    /// selects the last sample in the right series whose value is less than or equal to the left’s value
    #[default]
    Backward,
    /// selects the first sample in the right series whose value is greater than or equal to the left’s value.
    Forward,
    /// selects the sample in the right series whose value is nearest to the left's value.
    Nearest,
}

impl Display for AsOfJoinStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AsOfJoinStrategy::Backward => write!(f, "Backward"),
            AsOfJoinStrategy::Forward => write!(f, "Forward"),
            AsOfJoinStrategy::Nearest => write!(f, "Nearest"),
        }
    }
}

impl TryFrom<&str> for AsOfJoinStrategy {
    type Error = ValkeyError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let strategy = hashify::map_ignore_case!(
            value.as_bytes(),
            AsOfJoinStrategy,
            "forward" => AsOfJoinStrategy::Forward,
            "next" => AsOfJoinStrategy::Forward,
            "previous" => AsOfJoinStrategy::Backward,
            "backward" => AsOfJoinStrategy::Backward,
            "nearest" => AsOfJoinStrategy::Nearest,
        )
        .copied();

        match strategy {
            Some(strategy) => Ok(strategy),
            None => Err(ValkeyError::Str(error_consts::INVALID_ASOF_STRATEGY)),
        }
    }
}
