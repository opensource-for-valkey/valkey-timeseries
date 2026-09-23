use super::{AsOfJoinStrategy, join_asof_samples};
use crate::common::Sample;
use crate::join::JoinValue;
use std::time::Duration;

pub struct JoinAsOfIter<L, R>
where
    L: Iterator<Item = Sample>,
    R: Iterator<Item = Sample>,
{
    is_init: bool,
    left: L,
    right: R,
    strategy: AsOfJoinStrategy,
    tolerance: Option<Duration>,
    allow_eq: bool,
    items: Vec<(Sample, Sample)>,
    idx: usize,
}

impl<L, R> JoinAsOfIter<L, R>
where
    L: Iterator<Item = Sample>,
    R: Iterator<Item = Sample>,
{
    pub fn new<IL, IR>(
        left: IL,
        right: IR,
        strategy: AsOfJoinStrategy,
        tolerance: Option<Duration>,
        allow_eq: bool,
    ) -> Self
    where
        IL: IntoIterator<IntoIter = L, Item = Sample>,
        IR: IntoIterator<IntoIter = R, Item = Sample>,
    {
        Self {
            is_init: false,
            left: left.into_iter(),
            right: right.into_iter(),
            strategy,
            tolerance,
            allow_eq,
            items: vec![],
            idx: 0,
        }
    }

    fn init(&mut self) {
        self.is_init = true;
        // Always passing `Some` made an omitted TOLERANCE a tolerance of 0 — exact matches
        // only — where the documented default is no limit.
        let tolerance = self.tolerance.map(|t| t.as_millis() as i64);
        let left: Vec<Sample> = self.left.by_ref().collect();
        let right: Vec<Sample> = self.right.by_ref().collect();
        self.items = join_asof_samples(&left, &right, self.strategy, tolerance, self.allow_eq);
    }
}

impl<L, R> Iterator for JoinAsOfIter<L, R>
where
    L: Iterator<Item = Sample>,
    R: Iterator<Item = Sample>,
{
    type Item = JoinValue;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.is_init {
            self.init();
        }
        match self.items.get(self.idx) {
            Some((left, right)) => {
                self.idx += 1;
                Some(JoinValue::both(*left, *right))
            }
            None => None,
        }
    }
}
