use crate::common::math::{kahan_avg, kahan_std_dev, kahan_sum, kahan_variance, quantile};
use crate::common::{Sample, Timestamp};
use crate::promql::functions::PromQLArg;
use crate::promql::functions::PromQLFunction;
use crate::promql::functions::deriv::rollup_deriv;
use crate::promql::functions::idelta::rollup_idelta;
use crate::promql::functions::irate::rollup_irate;
use crate::promql::functions::range_vector_functions::{rollup_changes, rollup_resets};
use crate::promql::functions::rate::{RateKind, extrapolated_rate_window};
use crate::promql::functions::rollups::{
    eval_rollups, eval_rollups_basic, rollup_series_over_grid, window_samples,
};
use crate::promql::functions::types::RollupWindow;
use crate::promql::functions::utils::{
    exact_arity_error, expect_exact_arg_count, expect_range_vector, expect_scalar,
};
use crate::promql::{EvalContext, EvalResult, EvalSample, EvaluationError, ExprResult};
// https://github.com/VictoriaMetrics/VictoriaMetrics/blob/master/app/vmselect/promql/rollup.go

/// A range-vector function that a shard can evaluate on its own.
///
/// A PromQL series lives entirely on one shard, so a rollup over that series'
/// own window needs no cross-shard merge algebra: the shard computes the final
/// value and the coordinator concatenates. That locality — not decomposability —
/// is the membership rule for this enum. A function belongs here when its output
/// depends on nothing but one series' samples inside the window, which is why
/// `absent_over_time` can never join: its answer depends on series being absent
/// across the whole cluster, something no single shard can observe.
///
/// The variants deliberately do not cover every eligible function yet; the list
/// grows once the conformance suite covers each addition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
// Variants are named for the PromQL functions they stand for, which is what
// makes `from_function_name` a transcription rather than a mapping to maintain.
#[allow(clippy::enum_variant_names)]
pub enum RollupKind {
    SumOverTime,
    CountOverTime,
    LastOverTime,
    AvgOverTime,
    MinOverTime,
    MaxOverTime,
    StddevOverTime,
    StdvarOverTime,
    MadOverTime,
    PresentOverTime,
    FirstOverTime,
    QuantileOverTime,
    TsOfFirstOverTime,
    TsOfLastOverTime,
    TsOfMinOverTime,
    TsOfMaxOverTime,
    Rate,
    Increase,
    Delta,
    IRate,
    IDelta,
    Deriv,
    Resets,
    Changes,
}

/// How a rollup reduces one window, mirroring the shapes the local evaluation
/// paths take.
enum RollupImpl {
    /// Reads the window through a [`RollupWindow`] view over de-interleaved
    /// value/timestamp arrays — [`eval_rollups`]' shape.
    Windowed(fn(&RollupWindow, Option<f64>) -> f64),
    /// Reads whole `Sample`s — [`eval_rollups_basic`]' shape.
    WholeWindow(fn(&[Sample]) -> f64),
    /// Reads whole `Sample`s and may decline to produce a value —
    /// [`crate::promql::functions::range_vector_functions::eval_range`]' shape.
    /// `irate` over a one-sample window has no answer, and declining is not the
    /// same as answering NaN.
    WholeWindowOptional(fn(&[Sample]) -> Option<f64>),
    /// Needs the window's own bounds as well as its samples: `rate` and friends
    /// extrapolate to the window edges, so where those edges are changes the
    /// answer even when the samples do not.
    WindowAware(fn(&[Sample], i64, Timestamp) -> Option<f64>),
}

impl RollupKind {
    /// The kind for a PromQL function name, or `None` when that function cannot
    /// be pushed down.
    ///
    /// Deliberately absent, and why:
    ///
    /// * `absent_over_time` — permanently. Its answer depends on a series being
    ///   absent across the *whole cluster*, which no single shard can observe.
    /// * `predict_linear` — it predicts relative to the query's evaluation
    ///   timestamp, which `@`/`offset` divorce from the window end. A shard is
    ///   told window ends only, so it cannot compute the right origin.
    /// * `double_exponential_smoothing` / `holt_winters` — takes two scalar
    ///   parameters and the request carries one.
    pub(in crate::promql) fn from_function_name(name: &str) -> Option<Self> {
        let kind = match name {
            "sum_over_time" => RollupKind::SumOverTime,
            "count_over_time" => RollupKind::CountOverTime,
            "last_over_time" => RollupKind::LastOverTime,
            "avg_over_time" => RollupKind::AvgOverTime,
            "min_over_time" => RollupKind::MinOverTime,
            "max_over_time" => RollupKind::MaxOverTime,
            "stddev_over_time" => RollupKind::StddevOverTime,
            "stdvar_over_time" => RollupKind::StdvarOverTime,
            "mad_over_time" => RollupKind::MadOverTime,
            "present_over_time" => RollupKind::PresentOverTime,
            "first_over_time" => RollupKind::FirstOverTime,
            "quantile_over_time" => RollupKind::QuantileOverTime,
            "ts_of_first_over_time" => RollupKind::TsOfFirstOverTime,
            "ts_of_last_over_time" => RollupKind::TsOfLastOverTime,
            "ts_of_min_over_time" => RollupKind::TsOfMinOverTime,
            "ts_of_max_over_time" => RollupKind::TsOfMaxOverTime,
            "rate" => RollupKind::Rate,
            "increase" => RollupKind::Increase,
            "delta" => RollupKind::Delta,
            "irate" => RollupKind::IRate,
            "idelta" => RollupKind::IDelta,
            "deriv" => RollupKind::Deriv,
            "resets" => RollupKind::Resets,
            "changes" => RollupKind::Changes,
            _ => return None,
        };
        Some(kind)
    }

    /// The PromQL function this kind stands for. Tests use it to build a query
    /// per kind, so the conformance suite covers whatever `all()` returns
    /// without a second list to keep in step.
    #[cfg(test)]
    pub(in crate::promql) fn function_name(self) -> &'static str {
        use RollupKind::*;
        match self {
            SumOverTime => "sum_over_time",
            CountOverTime => "count_over_time",
            LastOverTime => "last_over_time",
            AvgOverTime => "avg_over_time",
            MinOverTime => "min_over_time",
            MaxOverTime => "max_over_time",
            StddevOverTime => "stddev_over_time",
            StdvarOverTime => "stdvar_over_time",
            MadOverTime => "mad_over_time",
            PresentOverTime => "present_over_time",
            FirstOverTime => "first_over_time",
            QuantileOverTime => "quantile_over_time",
            TsOfFirstOverTime => "ts_of_first_over_time",
            TsOfLastOverTime => "ts_of_last_over_time",
            TsOfMinOverTime => "ts_of_min_over_time",
            TsOfMaxOverTime => "ts_of_max_over_time",
            Rate => "rate",
            Increase => "increase",
            Delta => "delta",
            IRate => "irate",
            IDelta => "idelta",
            Deriv => "deriv",
            Resets => "resets",
            Changes => "changes",
        }
    }

    /// Every kind, for exhaustive testing. A new variant that is not listed here
    /// escapes the conformance suite, so the compiler is made to complain: this
    /// is written as a match rather than a bare array.
    #[cfg(test)]
    pub(in crate::promql) fn all() -> Vec<RollupKind> {
        use RollupKind::*;
        let all = [
            SumOverTime,
            CountOverTime,
            LastOverTime,
            AvgOverTime,
            MinOverTime,
            MaxOverTime,
            StddevOverTime,
            StdvarOverTime,
            MadOverTime,
            PresentOverTime,
            FirstOverTime,
            QuantileOverTime,
            TsOfFirstOverTime,
            TsOfLastOverTime,
            TsOfMinOverTime,
            TsOfMaxOverTime,
            Rate,
            Increase,
            Delta,
            IRate,
            IDelta,
            Deriv,
            Resets,
            Changes,
        ];
        for kind in all {
            // Exhaustive: adding a variant without adding it above fails to
            // compile here.
            match kind {
                SumOverTime | CountOverTime | LastOverTime | AvgOverTime | MinOverTime
                | MaxOverTime | StddevOverTime | StdvarOverTime | MadOverTime | PresentOverTime
                | FirstOverTime | QuantileOverTime | TsOfFirstOverTime | TsOfLastOverTime
                | TsOfMinOverTime | TsOfMaxOverTime | Rate | Increase | Delta | IRate | IDelta
                | Deriv | Resets | Changes => {}
            }
        }
        all.to_vec()
    }

    /// The reduction this kind applies — the *same* function pointer the local
    /// evaluation path uses, so a pushed-down result cannot drift from a local
    /// one by implementation.
    fn implementation(self) -> RollupImpl {
        match self {
            RollupKind::SumOverTime => RollupImpl::Windowed(rollup_sum),
            RollupKind::CountOverTime => RollupImpl::WholeWindow(rollup_count),
            RollupKind::LastOverTime => RollupImpl::WholeWindow(rollup_last),
            RollupKind::AvgOverTime => RollupImpl::Windowed(rollup_avg),
            RollupKind::MinOverTime => RollupImpl::Windowed(rollup_min),
            RollupKind::MaxOverTime => RollupImpl::Windowed(rollup_max),
            RollupKind::StddevOverTime => RollupImpl::Windowed(rollup_stddev),
            RollupKind::StdvarOverTime => RollupImpl::Windowed(rollup_stdvar),
            RollupKind::MadOverTime => RollupImpl::Windowed(rollup_mad),
            RollupKind::PresentOverTime => RollupImpl::WholeWindow(rollup_present),
            RollupKind::FirstOverTime => RollupImpl::WholeWindow(rollup_first),
            RollupKind::QuantileOverTime => RollupImpl::Windowed(rollup_quantile),
            RollupKind::TsOfFirstOverTime => RollupImpl::WholeWindow(rollup_tfirst),
            RollupKind::TsOfLastOverTime => RollupImpl::WholeWindow(rollup_tlast),
            RollupKind::TsOfMinOverTime => RollupImpl::Windowed(rollup_ts_of_min),
            RollupKind::TsOfMaxOverTime => RollupImpl::Windowed(rollup_ts_of_max),
            RollupKind::Rate => RollupImpl::WindowAware(rate_window),
            RollupKind::Increase => RollupImpl::WindowAware(increase_window),
            RollupKind::Delta => RollupImpl::WindowAware(delta_window),
            RollupKind::IRate => RollupImpl::WholeWindowOptional(rollup_irate),
            RollupKind::IDelta => RollupImpl::WholeWindowOptional(rollup_idelta),
            RollupKind::Deriv => RollupImpl::WholeWindowOptional(rollup_deriv),
            RollupKind::Resets => RollupImpl::WholeWindowOptional(rollup_resets),
            RollupKind::Changes => RollupImpl::WholeWindowOptional(rollup_changes),
        }
    }

    /// Reduce `samples` over the windows ending at each of `window_ends`.
    ///
    /// The result is sparse: a window holding no samples contributes nothing, so
    /// the output is shorter than `window_ends` whenever the series has gaps.
    /// `samples` must be sorted ascending by timestamp.
    pub(in crate::promql) fn eval_windows(
        self,
        samples: &[Sample],
        window_ms: i64,
        lookback_ms: i64,
        step_ms: i64,
        window_ends: impl IntoIterator<Item = Timestamp>,
        param: Option<f64>,
    ) -> Vec<Sample> {
        match self.implementation() {
            RollupImpl::Windowed(f) => rollup_series_over_grid(
                samples,
                window_ms,
                lookback_ms,
                step_ms,
                window_ends,
                param,
                f,
            ),
            RollupImpl::WholeWindow(f) => {
                Self::over_windows(samples, window_ms, window_ends, |window, _| Some(f(window)))
            }
            RollupImpl::WholeWindowOptional(f) => {
                Self::over_windows(samples, window_ms, window_ends, |window, _| f(window))
            }
            RollupImpl::WindowAware(f) => {
                Self::over_windows(samples, window_ms, window_ends, |window, end| {
                    f(window, window_ms, end)
                })
            }
        }
    }

    /// Slice each window out of `samples` and reduce it with `f`, dropping the
    /// windows that hold no samples and the ones `f` declines to answer for.
    fn over_windows(
        samples: &[Sample],
        window_ms: i64,
        window_ends: impl IntoIterator<Item = Timestamp>,
        f: impl Fn(&[Sample], Timestamp) -> Option<f64>,
    ) -> Vec<Sample> {
        window_ends
            .into_iter()
            .filter_map(|window_end| {
                let window = window_samples(samples, window_end, window_ms)?;
                Some(Sample {
                    value: f(window, window_end)?,
                    timestamp: window_end,
                })
            })
            .collect()
    }
}

/// `rate` over one window, in the shape [`RollupImpl::WindowAware`] needs.
fn rate_window(samples: &[Sample], window_ms: i64, window_end: Timestamp) -> Option<f64> {
    extrapolated_rate_window(samples, window_ms, window_end, RateKind::Rate)
}

fn increase_window(samples: &[Sample], window_ms: i64, window_end: Timestamp) -> Option<f64> {
    extrapolated_rate_window(samples, window_ms, window_end, RateKind::Increase)
}

fn delta_window(samples: &[Sample], window_ms: i64, window_end: Timestamp) -> Option<f64> {
    extrapolated_rate_window(samples, window_ms, window_end, RateKind::Delta)
}

pub(super) fn exec_rollup_fn(
    name: &str,
    mut args: Vec<PromQLArg>,
    ctx: &EvalContext,
    optional_param: Option<f64>,
    f: fn(&RollupWindow, Option<f64>) -> f64,
) -> EvalResult<ExprResult> {
    expect_exact_arg_count(name, 1, args.len())?;
    let range = args.swap_remove(0).into_range_vector()?;
    Ok(ExprResult::InstantVector(eval_rollups(
        ctx,
        range,
        optional_param,
        f,
    )?))
}

pub(super) fn exec_basic_rollup_fn(
    name: &str,
    mut args: Vec<PromQLArg>,
    ctx: &EvalContext,
    f: fn(&[Sample]) -> f64,
) -> EvalResult<ExprResult> {
    expect_exact_arg_count(name, 1, args.len())?;
    let range = args.swap_remove(0).into_range_vector()?;
    Ok(ExprResult::InstantVector(eval_rollups_basic(ctx, range, f)))
}

macro_rules! make_rollup_function {
    ( $type_name: ident, $name: expr, $rf: expr) => {
        #[derive(Copy, Clone, Default)]
        pub(in crate::promql) struct $type_name;

        impl $type_name {
            pub fn new() -> Self {
                Self
            }
        }

        impl PromQLFunction for $type_name {
            fn apply(&self, _arg: PromQLArg, _ctx: &EvalContext) -> EvalResult<ExprResult> {
                Err(EvaluationError::ArgumentError(format!(
                    "invalid invocation of rollup function '{}'",
                    $name
                )))
            }

            fn apply_call(
                &self,
                args: Vec<PromQLArg>,
                ctx: &EvalContext,
            ) -> EvalResult<ExprResult> {
                exec_rollup_fn($name, args, ctx, None, $rf)
            }
        }
    };
}

macro_rules! basic_rollup_function {
    ( $type_name: ident, $name: expr, $rf: expr) => {
        #[derive(Copy, Clone, Default)]
        pub(in crate::promql) struct $type_name;

        impl $type_name {
            pub fn new() -> Self {
                Self
            }
        }

        impl PromQLFunction for $type_name {
            fn apply(&self, _arg: PromQLArg, _ctx: &EvalContext) -> EvalResult<ExprResult> {
                Err(EvaluationError::ArgumentError(format!(
                    "invalid invocation of rollup function '{}'",
                    $name
                )))
            }

            fn apply_call(
                &self,
                args: Vec<PromQLArg>,
                ctx: &EvalContext,
            ) -> EvalResult<ExprResult> {
                exec_basic_rollup_fn($name, args, ctx, $rf)
            }
        }
    };
}

make_rollup_function!(AvgOverTimeFunction, "avg_over_time", rollup_avg);
make_rollup_function!(MinOverTimeFunction, "min_over_time", rollup_min);
make_rollup_function!(MadOverTimeFunction, "mad_over_time", rollup_mad);
make_rollup_function!(MaxOverTimeFunction, "max_over_time", rollup_max);
make_rollup_function!(SumOverTimeFunction, "sum_over_time", rollup_sum);
make_rollup_function!(
    TsOfMinOverTimeFunction,
    "ts_of_min_over_time",
    rollup_ts_of_min
);
make_rollup_function!(
    TsOfMaxOverTimeFunction,
    "ts_of_max_over_time",
    rollup_ts_of_max
);
make_rollup_function!(StddevOverTimeFunction, "stddev_over_time", rollup_stddev);
make_rollup_function!(StdvarOverTimeFunction, "stdvar_over_time", rollup_stdvar);

basic_rollup_function!(PresentOverTimeFunction, "present_over_time", rollup_present);
basic_rollup_function!(CountOverTimeFunction, "count_over_time", rollup_count);
basic_rollup_function!(
    TsOfFirstOverTimeFunction,
    "ts_of_first_over_time",
    rollup_tfirst
);
basic_rollup_function!(
    TsOfLastOverTimeFunction,
    "ts_of_last_over_time",
    rollup_tlast
);
basic_rollup_function!(FirstOverTimeFunction, "first_over_time", rollup_first);
basic_rollup_function!(LastOverTimeFunction, "last_over_time", rollup_last);

fn rollup_avg(rfa: &RollupWindow, _param: Option<f64>) -> f64 {
    kahan_avg(rfa.values)
}

fn rollup_quantile(rfa: &RollupWindow, phi: Option<f64>) -> f64 {
    let mut values = rfa.values.to_vec();
    quantile(&mut values, phi.unwrap_or(0.5))
}

/// Min over time.
///
/// IMPORTANT:
/// We intentionally do NOT use `f64::min` or a fold with +inf.
///
/// Prometheus semantics:
/// - If the first value is NaN and later values are real numbers,
///   NaN is replaced by the first real number.
/// - If all values are NaN, the result must remain NaN.
///
/// A naive fold starting from +inf would incorrectly return +inf
/// for all-NaN input. This manual loop preserves exact PromQL behavior.
fn rollup_min(rfa: &RollupWindow, _param: Option<f64>) -> f64 {
    let mut min = rfa.values[0];

    for &cur in rfa.values.iter().skip(1) {
        if cur < min || min.is_nan() {
            min = cur;
        }
    }
    min
}

fn rollup_mad(rfa: &RollupWindow, _param: Option<f64>) -> f64 {
    let mut values = rfa.values.to_vec();

    let median = quantile(&mut values, 0.5);

    // reuse values vec for deviations to avoid extra allocation
    for value in values.iter_mut() {
        *value = (*value - median).abs();
    }

    quantile(&mut values, 0.5)
}

/// Max over time.
///
/// IMPORTANT:
/// We intentionally do NOT use `f64::max` or a fold with -inf.
///
/// Prometheus semantics:
/// - NaN is replaced by any subsequent real value.
/// - If all values are NaN, the result must remain NaN.
///
/// A naive fold starting from -inf would incorrectly return -inf
/// for all-NaN input. This manual loop guarantees semantic parity
/// with Prometheus.
fn rollup_max(rfa: &RollupWindow, _param: Option<f64>) -> f64 {
    let mut max = rfa.values[0];
    for &cur in rfa.values.iter().skip(1) {
        if cur > max || max.is_nan() {
            max = cur;
        }
    }
    max
}

fn rollup_ts_of_min(rfa: &RollupWindow, _param: Option<f64>) -> f64 {
    let values = rfa.values;
    let mut min_value = values[0];
    let mut min_timestamp = rfa.timestamps[0];
    for (v, ts) in rfa
        .values
        .iter()
        .copied()
        .zip(rfa.timestamps.iter().copied())
    {
        // Get the last timestamp for the minimum value as most users expect.
        if v <= min_value {
            min_value = v;
            min_timestamp = ts;
        }
    }
    min_timestamp as f64 / 1e3_f64
}

fn rollup_ts_of_max(rfa: &RollupWindow, _param: Option<f64>) -> f64 {
    let mut max_value = rfa.values[0];
    let mut max_timestamp = rfa.timestamps[0];

    for (v, ts) in rfa
        .values
        .iter()
        .copied()
        .zip(rfa.timestamps.iter().copied())
    {
        // Get the last timestamp for the maximum value as most users expect.
        if v >= max_value {
            max_value = v;
            max_timestamp = ts;
        }
    }

    max_timestamp as f64 / 1e3_f64
}

fn rollup_sum(rfa: &RollupWindow, _param: Option<f64>) -> f64 {
    kahan_sum(rfa.values)
}

fn rollup_stddev(rfa: &RollupWindow, _param: Option<f64>) -> f64 {
    kahan_std_dev(rfa.values)
}

fn rollup_stdvar(rfa: &RollupWindow, _param: Option<f64>) -> f64 {
    kahan_variance(rfa.values)
}

fn rollup_present(samples: &[Sample]) -> f64 {
    if !samples.is_empty() { 1.0 } else { 0.0 }
}

fn rollup_count(samples: &[Sample]) -> f64 {
    samples.len() as f64
}

fn rollup_tfirst(samples: &[Sample]) -> f64 {
    // Safety: the caller ensures !samples.is_empty()
    samples[0].timestamp as f64 / 1e3_f64
}

fn rollup_tlast(samples: &[Sample]) -> f64 {
    // Safety: the caller ensures !samples.is_empty()
    samples[samples.len() - 1].timestamp as f64 / 1e3_f64
}

fn rollup_first(samples: &[Sample]) -> f64 {
    // Safety: the caller ensures !samples.is_empty()
    samples[0].value
}

fn rollup_last(samples: &[Sample]) -> f64 {
    // Safety: the caller ensures !samples.is_empty()
    samples[samples.len() - 1].value
}

/// `absent_over_time(range-vector)`
///
/// Returns an empty vector if the range vector has any elements (i.e., at least
/// one series with at least one sample in the look-back window), or a
/// single-element instant vector with value `1` and no labels otherwise.
///
/// This matches Prometheus semantics: the function is used to detect when a
/// time series is absent from a given range.
#[derive(Copy, Clone)]
pub(in crate::promql) struct AbsentOverTimeFunction;

impl PromQLFunction for AbsentOverTimeFunction {
    fn apply(&self, arg: PromQLArg, ctx: &EvalContext) -> EvalResult<ExprResult> {
        let series = arg.into_range_vector()?;
        // todo: what labels should the output sample have?
        let has_samples = series.iter().any(|s| !s.values.is_empty());
        if has_samples {
            Ok(ExprResult::InstantVector(vec![]))
        } else {
            Ok(ExprResult::InstantVector(vec![EvalSample {
                timestamp_ms: ctx.evaluation_ts,
                value: 1.0,
                labels: Default::default(),
                drop_name: false,
            }]))
        }
    }
}

/// `quantile_over_time(scalar, range-vector)`
///
/// the φ-quantile (0 ≤ φ ≤ 1) of all float samples in the specified interval.
#[derive(Copy, Clone)]
pub(in crate::promql) struct QuantileOverTimeFunction;

impl PromQLFunction for QuantileOverTimeFunction {
    fn apply(&self, _arg: PromQLArg, _ctx: &EvalContext) -> EvalResult<ExprResult> {
        Err(exact_arity_error("quantile_over_time", 2, 0))
    }

    fn apply_call(&self, args: Vec<PromQLArg>, ctx: &EvalContext) -> EvalResult<ExprResult> {
        if args.len() != 2 {
            return Err(exact_arity_error("quantile_over_time", 2, args.len()));
        }
        let mut iter = args.into_iter();
        let phi_arg = iter.next().unwrap();
        let range_arg = iter.next().unwrap();

        let phi = expect_scalar(phi_arg, "quantiles_over_time", "phi")?;
        let range = expect_range_vector(range_arg, "quantiles_over_time")?;

        Ok(ExprResult::InstantVector(eval_rollups(
            ctx,
            range,
            Some(phi),
            rollup_quantile,
        )?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::promql::functions::rollups::window_samples;

    /// A kind's name must round-trip: with two dozen hand-written entries in
    /// `from_function_name`, a typo would silently make one function
    /// unpushable — or, worse, map it to the wrong reduction.
    #[test]
    fn every_kind_round_trips_through_its_function_name() {
        for kind in RollupKind::all() {
            assert_eq!(
                RollupKind::from_function_name(kind.function_name()),
                Some(kind),
                "{kind:?} does not round-trip via {:?}",
                kind.function_name()
            );
        }
    }

    /// `resolve_function` must know every pushable name, or the local fallback
    /// for that kind would fail to evaluate at all.
    #[test]
    fn every_kind_names_a_real_function() {
        for kind in RollupKind::all() {
            assert!(
                crate::promql::functions::resolve_function(kind.function_name()).is_some(),
                "{kind:?} names an unknown function {:?}",
                kind.function_name()
            );
        }
    }

    /// Seeing samples outside its window must not change a rollup's answer.
    ///
    /// The local path hands `eval_windows` exactly one window's samples, because
    /// that is all the matrix selector loaded. The pushed-down path hands it the
    /// union of the grid's windows and lets it slice, because loading each
    /// window separately is the transfer the push-down exists to avoid. A rollup
    /// that read the surrounding samples — [`RollupWindow`] offers them as
    /// `prev_value`, `real_prev_value` and `real_next_value` — would answer
    /// differently on the two paths for the same query.
    ///
    /// So every [`RollupKind`] must reduce a window identically with and without
    /// its neighbours in view. A function that cannot is not eligible for
    /// push-down, and this is where that is caught.
    #[test]
    fn every_kind_ignores_samples_outside_the_window() {
        let series: Vec<Sample> = (0..=30)
            .map(|i| Sample {
                timestamp: i * 10_000,
                value: i as f64,
            })
            .collect();
        let window_ms = 60_000;
        let ends: Vec<Timestamp> = (0..=300_000).step_by(30_000).collect();

        for kind in RollupKind::all() {
            let param = (kind == RollupKind::QuantileOverTime).then_some(0.9);

            // The whole series in view, sliced per window — the grid path.
            let with_context =
                kind.eval_windows(&series, window_ms, 0, 30_000, ends.iter().copied(), param);

            // One window at a time, nothing else in view — the local path.
            let isolated: Vec<Sample> = ends
                .iter()
                .filter_map(|&end| {
                    let window = window_samples(&series, end, window_ms)?;
                    kind.eval_windows(window, window_ms, 0, 30_000, [end], param)
                        .pop()
                })
                .collect();

            assert_eq!(
                with_context, isolated,
                "{kind:?} must not depend on samples outside its window"
            );
        }
    }
}
