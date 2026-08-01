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

    /// Sorted with [`f64::total_cmp`] rather than `partial_cmp().unwrap()`,
    /// which panicked on any NaN in the sample — reachable from
    /// `TS.OUTLIERS METHOD MAD` over a series with a missing reading.
    pub fn new_unweighted(values: Vec<f64>) -> Self {
        let mut values = values;
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

    pub fn new_weighted(mut values: Vec<(f64, f64)>) -> Self {
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
