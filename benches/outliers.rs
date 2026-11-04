//! `TS.OUTLIERS` detector and decomposition cost.
//!
//! Each case runs the whole `detect_anomalies` pipeline over the same drifting seasonal series
//! at three lengths, so a size-dependent cliff shows up as a change of slope between them. ESD
//! is bounded by its outlier cap (default and `max_outliers = 10`); the STL and MSTL cases
//! measure seasonal adjustment followed by a z-score pass, and MSTL stays quadratic in the
//! series length by design.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use std::f64::consts::TAU;
use std::hint::black_box;
use std::time::Duration;
use valkey_timeseries::analysis::outliers::{
    AnomalyDetectionMethodOptions, AnomalyMADEstimator, AnomalyOptions, ESDOutlierOptions,
    MADAnomalyOptions, RCFOptions, detect_anomalies,
};
use valkey_timeseries::analysis::seasonality::Seasonality;

const SIZES: [usize; 3] = [1_000, 5_000, 20_000];

/// A 24-sample daily cycle on a fixed level, plus a deterministic 0..4 jitter.
fn drifting_series(n: usize) -> Vec<f64> {
    (0..n)
        .map(|i| 100.0 + 10.0 * (i as f64 * TAU / 24.0).sin() + ((i * 7919) % 41) as f64 / 10.0)
        .collect()
}

fn method(options: AnomalyDetectionMethodOptions) -> AnomalyOptions {
    AnomalyOptions {
        seasonality: None,
        options,
    }
}

fn seasonal(periods: &[usize]) -> AnomalyOptions {
    AnomalyOptions {
        seasonality: Some(Seasonality::Periods(periods.to_vec())),
        options: AnomalyDetectionMethodOptions::ZScore(None),
    }
}

fn bench_outliers(c: &mut Criterion) {
    let cases: Vec<(&str, AnomalyOptions)> = vec![
        (
            "zscore",
            method(AnomalyDetectionMethodOptions::ZScore(None)),
        ),
        (
            "mad_hd",
            method(AnomalyDetectionMethodOptions::Mad(MADAnomalyOptions {
                estimator: AnomalyMADEstimator::HarrellDavis,
                ..Default::default()
            })),
        ),
        (
            "esd_default",
            method(AnomalyDetectionMethodOptions::Esd(Some(
                ESDOutlierOptions::default(),
            ))),
        ),
        (
            "esd_max10",
            method(AnomalyDetectionMethodOptions::Esd(Some(
                ESDOutlierOptions {
                    max_outliers: Some(10),
                    ..Default::default()
                },
            ))),
        ),
        (
            "rcf_default",
            method(AnomalyDetectionMethodOptions::Rcf(RCFOptions::default())),
        ),
        ("stl_24", seasonal(&[24])),
        ("mstl_24_168", seasonal(&[24, 168])),
    ];

    let mut group = c.benchmark_group("outliers");
    for n in SIZES {
        let values = drifting_series(n);
        for (name, options) in &cases {
            group.bench_with_input(BenchmarkId::new(*name, n), &values, |b, values| {
                b.iter(|| detect_anomalies(black_box(values), options).unwrap())
            });
        }
    }
    group.finish();
}

criterion_group! {
    name = benches;
    // 10 samples: MSTL at 20k takes ~11 s per iteration, so the default 100 would run ~20 minutes.
    config = Criterion::default().measurement_time(Duration::from_secs(5)).sample_size(10);
    targets = bench_outliers
}
criterion_main!(benches);
