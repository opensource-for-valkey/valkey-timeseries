use crate::promql::common::strings::compare_str_alphanumeric;
use crate::promql::functions::utils::{expect_instant_vector, min_arity_error};
use crate::promql::functions::{PromQLArg, PromQLFunction};
use crate::promql::{EvalResult, EvaluationError, ExprResult};
use std::cmp::Ordering;

/// `sort(v instant-vector)`
///
/// returns vector elements sorted by their float sample values, in ascending order
#[derive(Copy, Clone)]
pub(in crate::promql) struct SortFunction;

impl PromQLFunction for SortFunction {
    fn apply(&self, arg: PromQLArg, _eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        sort(arg, false)
    }
}

/// `sort_desc(v instant-vector)`
///
/// returns vector elements sorted descending by their float sample values
#[derive(Copy, Clone)]
pub(in crate::promql) struct SortDescFunction;

impl PromQLFunction for SortDescFunction {
    fn apply(&self, arg: PromQLArg, _eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        sort(arg, true)
    }
}

/// `sort_by_label(v instant-vector, label string, ...)`
///
/// returns vector elements sorted by the values of the given labels in ascending order
#[derive(Copy, Clone)]
pub(in crate::promql) struct SortByLabelFunction;

impl PromQLFunction for SortByLabelFunction {
    fn apply(&self, _arg: PromQLArg, _eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        Err(min_arity_error("sort_by_label", 2, 1))
    }

    fn apply_args(&self, args: Vec<PromQLArg>, _eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        sort_by_label(args, false)
    }
}

/// `sort_by_label_desc(v instant-vector, label string, ...)`
///
/// returns vector elements sorted by the values of the given labels in descending order
#[derive(Copy, Clone)]
pub(in crate::promql) struct SortByLabelDescFunction;

impl PromQLFunction for SortByLabelDescFunction {
    fn apply(&self, _arg: PromQLArg, _eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        Err(min_arity_error("sort_by_label_desc", 2, 1))
    }

    fn apply_args(&self, args: Vec<PromQLArg>, _eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        sort_by_label(args, false)
    }
}

fn sort(arg: PromQLArg, desc: bool) -> EvalResult<ExprResult> {
    let func_name = if desc { "sort_desc" } else { "sort_by" };
    let mut vector = expect_instant_vector(arg, func_name)?;
    vector.sort_by(|a, b| {
        let a = a.value;
        let b = b.value;
        // According to Prometheus semantic, NaNs should sort lower
        let ord = match (a.is_nan(), b.is_nan()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (false, false) => a.partial_cmp(&b).unwrap_or(Ordering::Equal),
        };

        if desc { ord.reverse() } else { ord }
    });
    Ok(ExprResult::InstantVector(vector))
}

fn sort_by_label(args: Vec<PromQLArg>, desc: bool) -> EvalResult<ExprResult> {
    let func_name = if desc {
        "sort_by_label_desc"
    } else {
        "sort_by_label"
    };

    let arg_count = args.len();
    if arg_count < 2 {
        return Err(min_arity_error(func_name, 2, args.len()));
    }
    let mut args_iter = args.into_iter();
    let vector_arg = args_iter.next().expect("checked arg count in sort");

    let mut vector = expect_instant_vector(vector_arg, func_name)?;
    let mut label_names = Vec::with_capacity(arg_count - 1);
    for arg in args_iter {
        let PromQLArg::String(label) = arg else {
            return Err(EvaluationError::ArgumentError(format!(
                "expected label name as argument to {func_name}, got {arg:?}"
            )));
        };
        label_names.push(label);
    }

    vector.sort_by(|a, b| {
        for label_name in &label_names {
            let av = a.labels.get(label_name);
            let bv = b.labels.get(label_name);
            if let (Some(av), Some(bv)) = (av, bv) {
                let ord = compare_str_alphanumeric(av, bv);
                if !ord.is_eq() {
                    return if desc { ord.reverse() } else { ord };
                }
                continue;
            } else if av.is_none() && bv.is_none() {
                continue;
            } else if av.is_some() {
                return if desc {
                    Ordering::Less
                } else {
                    Ordering::Greater
                };
            } else if bv.is_some() {
                return if desc {
                    Ordering::Greater
                } else {
                    Ordering::Less
                };
            }
        }

        // do full comparison. Labels is sorted, so by virtue of Label implementing Ord, the following will
        // properly compare the values in order of their names.
        let tie = a.labels.cmp(&b.labels);
        if desc { tie.reverse() } else { tie }
    });
    Ok(ExprResult::InstantVector(vector))
}
