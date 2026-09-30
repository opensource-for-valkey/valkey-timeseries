//! `state_duration_over_time(v range-vector, state scalar [, unit string])`
//!
//! Flux's `stateDuration()` as a PromQL rollup: how long the window's final run
//! of samples equal to `state` has lasted, measured to the window's last sample.
//! A series whose last sample is not in `state` has no answer and is left out of
//! the result, as PromQL does for any function without an answer, where Flux
//! writes a `-1` sentinel. See
//! `docs/plans/state-duration-over-time-requirements.md` for the contract.
use crate::common::Sample;
use crate::parser::duration::parse_positive_duration_value;
use crate::promql::functions::rollups::eval_rollups_basic;
use crate::promql::functions::utils::{
    expect_min_arg_count, expect_range_vector, expect_scalar, expect_string, max_arity_error,
    min_arity_error,
};
use crate::promql::functions::{PromQLArg, PromQLFunction};
use crate::promql::{EvalContext, EvalResult, EvaluationError, ExprResult};
use promql_parser::parser::Function;
use promql_parser::parser::value::ValueType;

const NAME: &str = "state_duration_over_time";

/// Flux's default `unit: 1s`, which is also PromQL's unit for durations.
const DEFAULT_UNIT_MS: i64 = 1_000;

/// The parser-side declaration. The last argument (`unit`) is optional.
pub(super) fn parser_function() -> Function {
    Function::new(
        NAME,
        vec![ValueType::Matrix, ValueType::Scalar, ValueType::String],
        1,
        ValueType::Vector,
        true,
    )
}

#[derive(Copy, Clone, Default)]
pub(in crate::promql) struct StateDurationOverTimeFunction;

impl PromQLFunction for StateDurationOverTimeFunction {
    fn apply(&self, _arg: PromQLArg, _ctx: &EvalContext) -> EvalResult<ExprResult> {
        Err(min_arity_error(NAME, 2, 1))
    }

    fn apply_args(&self, args: Vec<PromQLArg>, ctx: &EvalContext) -> EvalResult<ExprResult> {
        expect_min_arg_count(NAME, 2, args.len())?;
        if args.len() > 3 {
            return Err(max_arity_error(NAME, 3, args.len()));
        }
        let mut iter = args.into_iter();
        let range = expect_range_vector(iter.next().unwrap(), NAME)?;
        let state = expect_scalar(iter.next().unwrap(), NAME, "state")?;
        if state.is_nan() {
            // No sample ever equals NaN, so every series would be dropped.
            return Err(EvaluationError::ArgumentError(format!(
                "{NAME}: state must not be NaN"
            )));
        }
        let unit_ms = match iter.next() {
            Some(arg) => parse_unit(&expect_string(arg, NAME, "unit")?)?,
            None => DEFAULT_UNIT_MS,
        } as f64;

        Ok(ExprResult::InstantVector(eval_rollups_basic(
            ctx,
            range,
            |window| rollup_state_duration(window, state).map(|ms| ms / unit_ms),
        )?))
    }
}

/// The length in milliseconds of the run of samples equal to `state` that ends
/// at the window's last sample, or `None` when the last sample is not in `state`
/// (or the window is empty).
///
/// Flux accumulates `0`, then `+Δt` for each consecutive match; those deltas
/// telescope to `t_last − t_run_start`, which is what this computes — exactly,
/// in integer milliseconds, however long the run. Unit-agnostic on purpose, so
/// a pushed-down path only has to carry `state`.
pub(in crate::promql) fn rollup_state_duration(values: &[Sample], state: f64) -> Option<f64> {
    let mut run_start = None;
    for sample in values {
        // IEEE `==`: a NaN sample never matches, and so breaks the run.
        if sample.value == state {
            run_start.get_or_insert(sample.timestamp);
        } else {
            run_start = None;
        }
    }
    let start = run_start?;
    let last = values.last()?;
    Some((last.timestamp - start) as f64)
}

/// Parse the `unit` argument with the module's duration parser.
///
/// A suffix is required: the shared parser reads a bare number as milliseconds,
/// where a PromQL reader would expect seconds. A suffix-less segment can only be
/// the last one (it consumes the rest of the input), so checking the final
/// character covers compound values like `"1h30"` too.
fn parse_unit(unit: &str) -> EvalResult<i64> {
    let invalid = |reason: &str| {
        EvaluationError::ArgumentError(format!("{NAME}: invalid unit {unit:?}: {reason}"))
    };
    if !unit.ends_with(|c: char| c.is_ascii_alphabetic()) {
        return Err(invalid(
            "a time unit suffix is required, e.g. \"1s\" or \"5m\"",
        ));
    }
    let ms = parse_positive_duration_value(unit).map_err(|e| invalid(&e.to_string()))?;
    if ms == 0 {
        return Err(invalid("must be greater than zero"));
    }
    Ok(ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn samples(points: &[(i64, f64)]) -> Vec<Sample> {
        points
            .iter()
            .map(|&(timestamp, value)| Sample { timestamp, value })
            .collect()
    }

    #[test]
    fn empty_window_has_no_answer() {
        assert_eq!(rollup_state_duration(&[], 1.0), None);
    }

    #[test]
    fn single_matching_sample_starts_a_run_at_zero() {
        assert_eq!(
            rollup_state_duration(&samples(&[(5_000, 1.0)]), 1.0),
            Some(0.0)
        );
    }

    #[test]
    fn single_non_matching_sample_has_no_answer() {
        assert_eq!(rollup_state_duration(&samples(&[(5_000, 0.0)]), 1.0), None);
    }

    /// The Flux docs' door example, closed = 1. Their table says 60 for the last
    /// row, but 17:43:16 → 17:44:27 is 71 s by their own rule; their `-1` for the
    /// open row is `None` here.
    #[test]
    fn door_example_accumulates_the_interval_not_the_count() {
        let rows = [
            (0, 1.0, Some(0.0)),
            (60_000, 1.0, Some(60_000.0)),
            (120_000, 1.0, Some(120_000.0)),
            (180_000, 0.0, None),
            (240_000, 1.0, Some(0.0)),
            (311_000, 1.0, Some(71_000.0)),
        ];
        let all = samples(&rows.map(|(t, v, _)| (t, v)));
        for (i, &(_, _, expected)) in rows.iter().enumerate() {
            assert_eq!(rollup_state_duration(&all[..=i], 1.0), expected, "row {i}");
        }
    }

    #[test]
    fn nan_sample_breaks_the_run() {
        let window = samples(&[(0, 1.0), (1_000, f64::NAN), (2_000, 1.0), (3_000, 1.0)]);
        assert_eq!(rollup_state_duration(&window, 1.0), Some(1_000.0));
        assert_eq!(rollup_state_duration(&window[..2], 1.0), None);
    }

    #[test]
    fn duplicate_timestamps_keep_the_run_alive() {
        let window = samples(&[(0, 1.0), (1_000, 1.0), (1_000, 1.0)]);
        assert_eq!(rollup_state_duration(&window, 1.0), Some(1_000.0));
    }

    /// A week of 1 s samples: the answer is an exact millisecond count, not a
    /// float sum of 604 800 deltas.
    #[test]
    fn long_run_is_exact() {
        let window: Vec<Sample> = (0..=604_800_i64)
            .map(|s| Sample {
                timestamp: 1_700_000_000_000 + s * 1_000 + 1,
                value: 3.0,
            })
            .collect();
        assert_eq!(rollup_state_duration(&window, 3.0), Some(604_800_000.0));
    }

    #[test]
    fn unit_accepts_suffixed_and_compound_durations() {
        assert_eq!(parse_unit("1s").unwrap(), 1_000);
        assert_eq!(parse_unit("500ms").unwrap(), 500);
        assert_eq!(parse_unit("1m").unwrap(), 60_000);
        assert_eq!(parse_unit("1h30m").unwrap(), 5_400_000);
        assert_eq!(parse_unit("1d").unwrap(), 86_400_000);
    }

    #[test]
    fn unit_rejects_zero_negative_bare_and_garbage() {
        for bad in ["0s", "0ms", "-1m", "60", "1h30", "abc", "", "nans"] {
            let err = parse_unit(bad).expect_err(bad).to_string();
            assert!(
                err.contains(NAME) && err.contains("invalid unit"),
                "{bad}: {err}"
            );
        }
    }
}
