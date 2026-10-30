use crate::common::Sample;
use crate::promql::functions::rollups::eval_series;
use crate::promql::functions::utils::{
    exact_arity_error, expect_exact_arg_count, expect_range_vector, expect_scalar,
};
use crate::promql::functions::{PromQLArg, PromQLFunction};
use crate::promql::{EvalContext, EvalResult, EvaluationError, ExprResult};

#[derive(Copy, Clone)]
pub(in crate::promql) struct DoubleExponentialSmoothingFunction;

impl PromQLFunction for DoubleExponentialSmoothingFunction {
    fn apply(&self, _arg: PromQLArg, _ctx: &EvalContext) -> EvalResult<ExprResult> {
        Err(exact_arity_error("double_exponential_smoothing", 3, 1))
    }

    fn apply_args(&self, args: Vec<PromQLArg>, ctx: &EvalContext) -> EvalResult<ExprResult> {
        eval_double_exponential_smoothing("double_exponential_smoothing", args, ctx)
    }
}

/// `holt_winters`, the name `double_exponential_smoothing` had before
/// Prometheus 3.0: the same evaluation, reporting errors under the name the
/// query used.
#[derive(Copy, Clone)]
pub(in crate::promql) struct HoltWintersFunction;

impl PromQLFunction for HoltWintersFunction {
    fn apply(&self, _arg: PromQLArg, _ctx: &EvalContext) -> EvalResult<ExprResult> {
        Err(exact_arity_error("holt_winters", 3, 1))
    }

    fn apply_args(&self, args: Vec<PromQLArg>, ctx: &EvalContext) -> EvalResult<ExprResult> {
        eval_double_exponential_smoothing("holt_winters", args, ctx)
    }
}

/// See https://en.wikipedia.org/wiki/Exponential_smoothing#Double_exponential_smoothing .
fn calculate_double_exponential_smoothing_value(
    samples: &[Sample],
    smoothing_factor: f64,
    trend_factor: f64,
) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }

    // Any non-finite sample makes the whole window NaN. The check rides along
    // in the recurrence loop rather than as a pass of its own: the loop is
    // latency-bound on the level/trend dependency chain, so an independent
    // compare per sample is free there, whereas a separate scan re-reads the
    // window. Window at [1d] step 10s is 8640 samples per step.
    if !samples[0].value.is_finite() {
        return Some(f64::NAN);
    }

    let mut level = samples[0].value;
    let mut trend = samples[1].value - level;

    for &Sample {
        timestamp: _,
        value,
    } in &samples[1..]
    {
        if !value.is_finite() {
            return Some(f64::NAN);
        }
        let previous_level = level;
        level = smoothing_factor * value + (1.0 - smoothing_factor) * (level + trend);
        trend = trend_factor * (level - previous_level) + (1.0 - trend_factor) * trend;
    }

    Some(level)
}

/// `function_name` is the name the query called — `double_exponential_smoothing`
/// or `holt_winters` — so an error is reported against it.
fn eval_double_exponential_smoothing(
    function_name: &str,
    args: Vec<PromQLArg>,
    ctx: &EvalContext,
) -> EvalResult<ExprResult> {
    expect_exact_arg_count(function_name, 3, args.len())?;
    let mut args = args.into_iter();
    let series = expect_range_vector(args.next().expect("checked arg count"), function_name)?;
    let smoothing_factor =
        expect_scalar(args.next().expect("checked arg count"), function_name, "sf")?;
    let trend_factor = expect_scalar(args.next().expect("checked arg count"), function_name, "tf")?;

    // Both bounds exclusive, as Prometheus checks them; a NaN factor fails too.
    if !(smoothing_factor > 0.0 && smoothing_factor < 1.0) {
        let msg = format!("invalid smoothing factor. Expected 0 < sf < 1, got {smoothing_factor}");
        return Err(EvaluationError::ArgumentError(msg));
    }

    if !(trend_factor > 0.0 && trend_factor < 1.0) {
        let msg = format!("invalid trend factor. Expected 0 < tf < 1, got {trend_factor}");
        return Err(EvaluationError::ArgumentError(msg));
    }

    Ok(ExprResult::InstantVector(eval_series(ctx, series, |s| {
        calculate_double_exponential_smoothing_value(&s.values, smoothing_factor, trend_factor)
    })?))
}

#[cfg(test)]
mod tests {
    use super::{calculate_double_exponential_smoothing_value, eval_double_exponential_smoothing};
    use crate::common::Sample;
    use crate::promql::EvalContext;
    use crate::promql::functions::PromQLArg;

    /// Prometheus checks both factors with exclusive bounds — `0 < sf < 1`,
    /// `0 < tf < 1` — and its error names the factor that failed. Ours must
    /// also name the function the user called, since two names share the
    /// implementation.
    #[test]
    fn factor_bounds_are_exclusive_and_errors_name_the_function() {
        let call = |name: &str, sf: f64, tf: f64| {
            eval_double_exponential_smoothing(
                name,
                vec![
                    PromQLArg::RangeVector(Vec::new()),
                    PromQLArg::Scalar(sf),
                    PromQLArg::Scalar(tf),
                ],
                &EvalContext::default(),
            )
        };
        for (sf, tf, factor) in [
            (0.0, 0.5, "smoothing factor"),
            (1.0, 0.5, "smoothing factor"),
            (0.5, 0.0, "trend factor"),
            (0.5, 1.0, "trend factor"),
        ] {
            let msg = call("holt_winters", sf, tf).unwrap_err().to_string();
            assert!(msg.contains(factor), "sf={sf} tf={tf}: {msg}");
        }
        assert!(call("holt_winters", 0.5, 0.5).is_ok());
        assert!(call("double_exponential_smoothing", 0.01, 0.1).is_ok());

        // A wrong argument type is reported against the name that was called.
        let msg = eval_double_exponential_smoothing(
            "holt_winters",
            vec![
                PromQLArg::Scalar(1.0),
                PromQLArg::Scalar(0.5),
                PromQLArg::Scalar(0.5),
            ],
            &EvalContext::default(),
        )
        .unwrap_err()
        .to_string();
        assert!(msg.contains("holt_winters"), "{msg}");
    }

    /// The two-pass shape the fused loop replaced: a non-finite scan, then
    /// the recurrence. Kept as the oracle so the fused loop is pinned
    /// bit-for-bit, including where in the window a non-finite value sits.
    fn reference(samples: &[Sample], sf: f64, tf: f64) -> Option<f64> {
        if samples.len() < 2 {
            return None;
        }
        if samples.iter().any(|x| !x.value.is_finite()) {
            return Some(f64::NAN);
        }
        let mut level = samples[0].value;
        let mut trend = samples[1].value - level;
        for s in &samples[1..] {
            let previous_level = level;
            level = sf * s.value + (1.0 - sf) * (level + trend);
            trend = tf * (level - previous_level) + (1.0 - tf) * trend;
        }
        Some(level)
    }

    fn window(values: &[f64]) -> Vec<Sample> {
        values
            .iter()
            .enumerate()
            .map(|(i, &v)| Sample::new(i as i64 * 10_000, v))
            .collect()
    }

    fn assert_same(samples: &[Sample], sf: f64, tf: f64) {
        let got = calculate_double_exponential_smoothing_value(samples, sf, tf);
        let want = reference(samples, sf, tf);
        assert_eq!(
            got.map(f64::to_bits),
            want.map(f64::to_bits),
            "sf={sf} tf={tf} samples={samples:?}"
        );
    }

    #[test]
    fn fused_loop_matches_two_pass_reference() {
        // Deterministic LCG so the fixture is reproducible without a dep.
        let mut state = 0x2545_F491_4F6C_DD1Du64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for len in [0usize, 1, 2, 3, 17, 500] {
            for &(sf, tf) in &[(0.3, 0.3), (0.01, 0.1), (1.0, 0.0), (0.0, 1.0)] {
                let mut values: Vec<f64> = (0..len)
                    .map(|_| ((next() % 20_000) as f64 - 10_000.0) / 7.0)
                    .collect();
                assert_same(&window(&values), sf, tf);
                if len >= 2 {
                    for &bad in &[f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
                        for pos in [0, 1, len / 2, len - 1] {
                            let keep = values[pos];
                            values[pos] = bad;
                            assert_same(&window(&values), sf, tf);
                            values[pos] = keep;
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn non_finite_anywhere_is_nan_and_short_windows_are_absent() {
        assert_eq!(
            calculate_double_exponential_smoothing_value(&[], 0.3, 0.3),
            None
        );
        assert_eq!(
            calculate_double_exponential_smoothing_value(&window(&[1.0]), 0.3, 0.3),
            None
        );
        for values in [
            [f64::NAN, 1.0, 2.0],
            [1.0, f64::INFINITY, 2.0],
            [1.0, 2.0, f64::NEG_INFINITY],
        ] {
            let got = calculate_double_exponential_smoothing_value(&window(&values), 0.3, 0.3);
            assert!(got.is_some_and(f64::is_nan), "{values:?} -> {got:?}");
        }
    }
}
