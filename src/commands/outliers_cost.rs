//! How much work a `TS.OUTLIERS` request is, before any sample is decoded.
//!
//! A rough cost in microseconds, used only to choose where the request runs. It is calculated
//! from the method, its options and the seasonality, because those move the work by orders of
//! magnitude for the same sample count.

use crate::analysis::outliers::{
    AnomalyDetectionMethodOptions, AnomalyOptions, RCF_DEFAULT_NUM_TREES, RCF_DEFAULT_SAMPLE_SIZE,
    effective_max_outliers,
};
use crate::analysis::seasonality::{MAX_SEASONALITY_PERIODS, Seasonality};

/// A request estimated above this runs on the analysis lane rather than the main thread.
pub(crate) const INLINE_COST_BUDGET_MICROS: u64 = 1_000;

/// The estimated cost of running `options` over `samples` samples: a linear base, plus what
/// the method and the seasonal decomposition add to it.
///
/// Calculated in `f64` so the largest options on the largest range saturate instead of
/// overflowing.
pub(crate) fn estimated_cost_micros(samples: usize, options: &AnomalyOptions) -> u64 {
    let n = samples as f64;
    let base = n / 5.0;
    let cost = base + method_cost(samples, options) + seasonality_cost(n, options);
    cost as u64
}

fn method_cost(samples: usize, options: &AnomalyOptions) -> f64 {
    let n = samples as f64;
    match &options.options {
        AnomalyDetectionMethodOptions::Esd(esd) => {
            let explicit = esd.as_ref().and_then(|esd| esd.max_outliers);
            let k = effective_max_outliers(samples, explicit) as f64;
            n * k * n.log2().max(1.0) / 500.0
        }
        AnomalyDetectionMethodOptions::Rcf(rcf) => {
            let trees = rcf.num_trees.unwrap_or(RCF_DEFAULT_NUM_TREES) as f64;
            let sample_size = rcf.sample_size.unwrap_or(RCF_DEFAULT_SAMPLE_SIZE) as f64;
            let shingle = rcf.shingle_size.unwrap_or(1) as f64;
            n * trees * shingle * sample_size.log2() / 10.0
        }
        _ => 0.0,
    }
}

fn seasonality_cost(n: f64, options: &AnomalyOptions) -> f64 {
    let mstl = |periods: usize| n * n * periods as f64 / 50.0;
    match &options.seasonality {
        None => 0.0,
        Some(Seasonality::Auto) => mstl(MAX_SEASONALITY_PERIODS),
        Some(Seasonality::Periods(periods)) => match periods.as_slice() {
            [period] => n * *period as f64 / 10.0,
            periods => mstl(periods.len()),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::outliers::{AnomalyDetectionMethodOptions, AnomalyOptions, RCFOptions};
    use crate::analysis::seasonality::Seasonality;

    fn esd_default() -> AnomalyOptions {
        AnomalyOptions {
            options: AnomalyDetectionMethodOptions::Esd(None),
            ..Default::default()
        }
    }

    fn rcf_default() -> AnomalyOptions {
        AnomalyOptions {
            options: AnomalyDetectionMethodOptions::Rcf(RCFOptions::default()),
            ..Default::default()
        }
    }

    #[test]
    fn cheap_methods_keep_the_five_thousand_sample_threshold() {
        let z = AnomalyOptions::default();
        assert!(estimated_cost_micros(5_000, &z) <= INLINE_COST_BUDGET_MICROS);
        assert!(estimated_cost_micros(6_000, &z) > INLINE_COST_BUDGET_MICROS);
    }

    #[test]
    fn two_period_seasonality_goes_to_the_lane_well_below_five_thousand() {
        let o = AnomalyOptions {
            seasonality: Some(Seasonality::Periods(vec![24, 168])),
            ..Default::default()
        };
        assert!(estimated_cost_micros(500, &o) > INLINE_COST_BUDGET_MICROS);
    }

    #[test]
    fn esd_and_rcf_leave_the_main_thread_early() {
        assert!(estimated_cost_micros(1_000, &esd_default()) > INLINE_COST_BUDGET_MICROS);
        assert!(estimated_cost_micros(200, &rcf_default()) > INLINE_COST_BUDGET_MICROS);
    }

    #[test]
    fn auto_seasonality_counts_as_the_most_periods_allowed() {
        let auto = AnomalyOptions {
            seasonality: Some(Seasonality::Auto),
            ..Default::default()
        };
        let four = AnomalyOptions {
            seasonality: Some(Seasonality::Periods(vec![2, 3, 5, 7])),
            ..Default::default()
        };
        assert_eq!(
            estimated_cost_micros(1_000, &auto),
            estimated_cost_micros(1_000, &four)
        );
    }

    #[test]
    fn one_period_costs_by_its_length() {
        let stl = |p| AnomalyOptions {
            seasonality: Some(Seasonality::Periods(vec![p])),
            ..Default::default()
        };
        // 1,000 / 5 + 1,000 * 24 / 10
        assert_eq!(estimated_cost_micros(1_000, &stl(24)), 2_600);
        assert_eq!(estimated_cost_micros(1_000, &stl(168)), 17_000);
    }

    #[test]
    fn empty_and_huge_ranges_do_not_overflow() {
        assert_eq!(estimated_cost_micros(0, &esd_default()), 0);
        assert_eq!(estimated_cost_micros(1, &AnomalyOptions::default()), 0);
        let auto_rcf = AnomalyOptions {
            seasonality: Some(Seasonality::Auto),
            ..rcf_default()
        };
        assert!(estimated_cost_micros(usize::MAX, &auto_rcf) > INLINE_COST_BUDGET_MICROS);
    }
}
