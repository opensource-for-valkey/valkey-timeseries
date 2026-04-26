use crate::common::hash::hash_f64;
use crate::common::rdb::{RdbSerializable, rdb_load_f64};
use get_size2::GetSize;
use std::borrow::Borrow;
use std::hash::Hash;
use std::ops::{Add, AddAssign};
use valkey_module::{RedisModuleIO, ValkeyResult, raw};

/// Kahan summation for improved numerical stability when summing floating-point numbers.
#[derive(Clone, Copy, Debug, Default, GetSize, PartialEq)]
pub struct KahanSum {
    sum: f64,
    compensation: f64,
}

impl KahanSum {
    pub fn new() -> Self {
        Self {
            sum: 0.0,
            compensation: 0.0,
        }
    }

    pub fn reset(&mut self) {
        self.sum = 0.0;
        self.compensation = 0.0;
    }

    pub fn incr(&mut self, inc: f64) {
        let (new_sum, new_compensation) = kahan_inc(inc, self.sum, self.compensation);
        self.sum = new_sum;
        self.compensation = new_compensation;
    }

    /// Reconstitutes a sum from its transported parts (see
    /// `PartialState::acc1`/`acc1_c` in the partial reducer). A plain f64 sum
    /// round-trips as `from_parts(sum, 0.0)`.
    pub(crate) fn from_parts(sum: f64, compensation: f64) -> Self {
        Self { sum, compensation }
    }

    /// Decomposes into `(sum, compensation)` for transport.
    pub(crate) fn into_parts(self) -> (f64, f64) {
        (self.sum, self.compensation)
    }

    /// The compensated sum. Neumaier's algorithm accumulates the running sum
    /// and the rounding error separately; the result is their sum.
    pub fn value(&self) -> f64 {
        self.sum + self.compensation
    }

    /// Returns the current error value
    pub fn err(&self) -> f64 {
        self.compensation
    }

    pub fn is_empty(&self) -> bool {
        self.sum == 0.0 && self.compensation == 0.0
    }
}

impl Add<f64> for KahanSum {
    type Output = KahanSum;

    fn add(self, rhs: f64) -> Self::Output {
        let mut result = self;
        result += rhs;
        result
    }
}

impl AddAssign<f64> for KahanSum {
    fn add_assign(&mut self, rhs: f64) {
        let (sum, c) = kahan_inc(rhs, self.sum, self.compensation);
        self.sum = sum;
        self.compensation = c;
    }
}

impl AddAssign<&KahanSum> for KahanSum {
    /// Merges two independently accumulated sums (e.g. shard partials):
    /// two-sums the running sums via `kahan_inc` — which folds the rounding
    /// error into this side's compensation — then adds the other side's
    /// compensation. Order-insensitive up to one ulp of the compensation term.
    fn add_assign(&mut self, rhs: &KahanSum) {
        let (sum, compensation) = kahan_inc(rhs.sum, self.sum, self.compensation);
        self.sum = sum;
        self.compensation = compensation + rhs.compensation;
    }
}

impl AddAssign<KahanSum> for KahanSum {
    fn add_assign(&mut self, rhs: KahanSum) {
        *self += &rhs;
    }
}

impl RdbSerializable for KahanSum {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        raw::save_double(rdb, self.sum);
        raw::save_double(rdb, self.compensation);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self> {
        let sum = rdb_load_f64(rdb)?;
        let compensation = rdb_load_f64(rdb)?;
        Ok(Self { sum, compensation })
    }
}

impl Hash for KahanSum {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        hash_f64(self.sum, state);
        hash_f64(self.compensation, state);
    }
}

#[derive(Clone, Copy, Debug, Default, GetSize, Hash, PartialEq)]
pub struct KahanAvg {
    count: u32,
    sum: KahanSum,
}

impl KahanAvg {
    pub fn new() -> Self {
        Self {
            count: 0,
            sum: KahanSum::new(),
        }
    }

    pub fn reset(&mut self) {
        self.count = 0;
        self.sum = KahanSum::new();
    }

    pub fn add(&mut self, inc: f64) {
        self.sum += inc;
        self.count = self.count.saturating_add(1);
    }

    pub fn value(&self) -> Option<f64> {
        if self.count == 0 {
            None
        } else {
            Some(self.sum.value() / (self.count as f64))
        }
    }
}
impl AddAssign<f64> for KahanAvg {
    fn add_assign(&mut self, rhs: f64) {
        self.add(rhs);
    }
}

impl RdbSerializable for KahanAvg {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        self.sum.rdb_save(rdb);
        raw::save_unsigned(rdb, self.count as u64);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self> {
        let sum = KahanSum::rdb_load(rdb)?;
        let count = raw::load_unsigned(rdb)? as u32;
        Ok(Self { count, sum })
    }
}

pub trait KahanSummator {
    /// Computes the Kahan sum of an iterator.
    /// # Example
    ///
    /// ```
    /// use crate::aggregators::kahan::*;
    /// let summands = [10000.0f32, 3.14159, 2.71828];
    /// let kahan_sum = summands.iter().kahan_sum();
    /// assert_eq!(10005.86f32, kahan_sum.sum());
    /// assert_eq!(0.0004813671f32, kahan_sum.err());
    /// ```
    fn kahan_sum(self) -> KahanSum;
}

impl<U, V> KahanSummator for U
where
    U: Iterator<Item = V>,
    V: Borrow<f64>,
{
    fn kahan_sum(self) -> KahanSum {
        self.fold(KahanSum::new(), |sum, item| sum + *item.borrow())
    }
}

/// Kahan summation increment with Neumaier improvement (1974)
///
/// Performs compensated summation to minimize floating-point rounding errors.
/// The Neumaier variant handles the case where the next term is larger than
/// the running sum, which the original Kahan algorithm (1965) did not address.
///
/// Returns (new_sum, new_compensation)
///
/// Copyright (c) 2024-present, OpenData project, and contributors
#[inline(never)]
// Important: do NOT inline.
// Compiler reordering of floating-point operations can cause precision loss.
// This was observed in Prometheus (issue #16714) and we lock the behavior
// to maintain IEEE-754 semantics exactly.
fn kahan_inc(inc: f64, sum: f64, c: f64) -> (f64, f64) {
    let t = sum + inc;

    let new_c = if t.is_infinite() {
        0.0
    } else if sum.abs() >= inc.abs() {
        // Neumaier improvement: swap roles when next term is larger
        c + ((sum - t) + inc)
    } else {
        c + ((inc - t) + sum)
    };

    (t, new_c)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_and_add_assign_f64() {
        let mut s = KahanSum::new();
        let s2 = s + 2.5f64;
        assert_eq!(s.value(), 0.0); // original unchanged
        assert!((s2.value() - 2.5).abs() < 1e-12);

        s += 1.5f64;
        assert!((s.value() - 1.5).abs() < 1e-12);

        s += 2.5f64;
        assert!((s.value() - 4.0).abs() < 1e-12);
    }

    #[test]
    fn test_add_assign_kahansum_combination() {
        let mut a = KahanSum::new();
        for _ in 0..1000 {
            a += 1e-6;
        }

        let mut b = KahanSum::new();
        b += 1000.0;

        let total_expected = a.value() + b.value();

        // combine by reference
        let mut combined = a;
        combined += &b;
        assert!((combined.value() - total_expected).abs() < 1e-9);

        // combine by value
        let mut combined2 = a;
        combined2 += b;
        assert!((combined2.value() - total_expected).abs() < 1e-9);
    }

    #[test]
    fn test_value_applies_compensation() {
        // Naive summation loses the 1.0s entirely (1e16 + 1.0 == 1e16), so
        // this only passes if value() folds the Neumaier compensation in.
        let mut s = KahanSum::new();
        for inc in [1e16, 1.0, 1.0, -1e16] {
            s += inc;
        }
        assert_eq!(s.value(), 2.0);
    }

    #[test]
    fn test_merge_catastrophic_cancellation() {
        // Split so each side carries a nonzero compensation and the running
        // sums cancel on merge; the naive merge of running sums yields 0.
        let mut a = KahanSum::new();
        a += 1e16;
        a += 1.0;
        let mut b = KahanSum::new();
        b += 1.0;
        b += -1e16;

        let mut merged = a;
        merged += &b;
        assert_eq!(merged.value(), 2.0);

        // merge order must not matter
        let mut merged2 = b;
        merged2 += &a;
        assert_eq!(merged2.value(), 2.0);
    }

    #[test]
    fn test_iterator_kahan_sum_trait() {
        let nums = [0.1f64, 0.2, 0.3, 0.4];
        let k = nums.iter().kahan_sum();
        let naive: f64 = nums.iter().copied().sum();
        assert!((k.value() - naive).abs() < 1e-12);
    }

    #[test]
    fn test_kahan_avg() {
        let mut avg = KahanAvg::new();
        assert_eq!(avg.value(), None);

        avg.add(1.0);
        avg.add(2.0);
        avg.add(3.0);

        let v = avg.value().unwrap();
        assert!((v - 2.0).abs() < 1e-12);
        // the check count didn't overflow and is correct
        assert_eq!(avg.count, 3);
    }
}
