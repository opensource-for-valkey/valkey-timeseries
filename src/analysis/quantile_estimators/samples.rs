/// Represents a sample for quantile estimation.
/// Supports both weighted and unweighted samples.
pub struct Samples {
    pub values: Vec<f64>,
    pub sorted_weights: Option<Vec<f64>>, // None for unweighted samples
    pub total_weight: f64,
}

impl Samples {
    pub fn new(values: Vec<f64>) -> Self {
        Self::new_sorted_unweighted(values)
    }

    /// NaN values (missing readings) are dropped rather than sorted in: with
    /// `total_cmp`, NaN sorts as a real (if extreme) element, which both
    /// biases every quantile position — `n` counts an observation that carries
    /// no information — and can make the median itself NaN, silently
    /// disabling a MAD fit for the whole series. Sorted with
    /// [`f64::total_cmp`] rather than `partial_cmp().unwrap()`, which panicked
    /// on any NaN in the sample — reachable from `TS.OUTLIERS METHOD MAD` over
    /// a series with a missing reading.
    pub fn new_unweighted(values: Vec<f64>) -> Self {
        let mut values: Vec<f64> = values.into_iter().filter(|v| !v.is_nan()).collect();
        values.sort_by(f64::total_cmp);
        Self::new_sorted_unweighted(values)
    }

    pub fn new_sorted_unweighted(values: Vec<f64>) -> Self {
        let n = values.len() as f64;
        Samples {
            values,
            sorted_weights: None,
            total_weight: n,
        }
    }

    /// A NaN value is dropped along with its weight, for the same reason
    /// [`Self::new_unweighted`] drops one: it carries no information but would
    /// still bias `total_weight` and every quantile position derived from it.
    pub fn new_weighted(mut values: Vec<(f64, f64)>) -> Self {
        values.retain(|(v, _)| !v.is_nan());
        values.sort_by(|a, b| a.0.total_cmp(&b.0));
        let total_weight: f64 = values.iter().map(|(_, w)| *w).sum();
        let (sorted_values, sorted_weights): (Vec<f64>, Vec<f64>) = values.into_iter().unzip();
        Samples {
            values: sorted_values,
            sorted_weights: Some(sorted_weights),
            total_weight,
        }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn weighted_size(&self) -> f64 {
        self.total_weight
    }

    pub fn is_weighted(&self) -> bool {
        self.sorted_weights.is_some()
    }
}

impl From<Vec<f64>> for Samples {
    fn from(values: Vec<f64>) -> Self {
        Self::new_unweighted(values)
    }
}

impl From<&[f64]> for Samples {
    fn from(values: &[f64]) -> Self {
        Self::new_unweighted(values.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_unweighted_drops_nan_rather_than_sorting_it_in() {
        let sample = Samples::new_unweighted(vec![1.0, f64::NAN, 2.0, 3.0, f64::NAN]);

        assert_eq!(sample.values, vec![1.0, 2.0, 3.0]);
        assert_eq!(sample.len(), 3);
        assert_eq!(sample.weighted_size(), 3.0);
    }

    #[test]
    fn new_weighted_drops_nan_and_its_paired_weight() {
        let sample =
            Samples::new_weighted(vec![(1.0, 10.0), (f64::NAN, 99.0), (2.0, 20.0)]);

        assert_eq!(sample.values, vec![1.0, 2.0]);
        assert_eq!(sample.sorted_weights, Some(vec![10.0, 20.0]));
        assert_eq!(sample.weighted_size(), 30.0);
    }
}
