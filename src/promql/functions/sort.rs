use crate::promql::common::strings::compare_str_alphanumeric;
use crate::promql::functions::utils::{expect_instant_vector, min_arity_error};
use crate::promql::functions::{PromQLArg, PromQLFunction};
use crate::promql::{EvalContext, EvalResult, EvaluationError, ExprResult};
use std::cmp::Ordering;

/// `sort(v instant-vector)`
///
/// returns vector elements sorted by their float sample values, in ascending order
#[derive(Copy, Clone)]
pub(in crate::promql) struct SortFunction;

impl PromQLFunction for SortFunction {
    fn apply(&self, arg: PromQLArg, _ctx: &EvalContext) -> EvalResult<ExprResult> {
        sort(arg, false)
    }
}

/// `sort_desc(v instant-vector)`
///
/// returns vector elements sorted descending by their float sample values
#[derive(Copy, Clone)]
pub(in crate::promql) struct SortDescFunction;

impl PromQLFunction for SortDescFunction {
    fn apply(&self, arg: PromQLArg, _ctx: &EvalContext) -> EvalResult<ExprResult> {
        sort(arg, true)
    }
}

/// `sort_by_label(v instant-vector, label string, ...)`
///
/// returns vector elements sorted by the values of the given labels in ascending order
#[derive(Copy, Clone)]
pub(in crate::promql) struct SortByLabelFunction;

impl PromQLFunction for SortByLabelFunction {
    fn apply(&self, _arg: PromQLArg, _ctx: &EvalContext) -> EvalResult<ExprResult> {
        Err(min_arity_error("sort_by_label", 2, 1))
    }

    fn apply_args(&self, args: Vec<PromQLArg>, _ctx: &EvalContext) -> EvalResult<ExprResult> {
        sort_by_label(args, false)
    }
}

/// `sort_by_label_desc(v instant-vector, label string, ...)`
///
/// returns vector elements sorted by the values of the given labels in descending order
#[derive(Copy, Clone)]
pub(in crate::promql) struct SortByLabelDescFunction;

impl PromQLFunction for SortByLabelDescFunction {
    fn apply(&self, _arg: PromQLArg, _ctx: &EvalContext) -> EvalResult<ExprResult> {
        Err(min_arity_error("sort_by_label_desc", 2, 1))
    }

    fn apply_args(&self, args: Vec<PromQLArg>, _ctx: &EvalContext) -> EvalResult<ExprResult> {
        sort_by_label(args, true)
    }
}

fn sort(arg: PromQLArg, desc: bool) -> EvalResult<ExprResult> {
    let func_name = if desc { "sort_desc" } else { "sort" };
    let mut vector = expect_instant_vector(arg, func_name)?;
    vector.sort_by(|a, b| {
        let a = a.value;
        let b = b.value;
        match (a.is_nan(), b.is_nan()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (false, false) => {
                let ord = a.partial_cmp(&b).unwrap_or(Ordering::Equal);
                if desc { ord.reverse() } else { ord }
            }
        }
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

#[cfg(test)]
mod tests {
    use super::{sort, sort_by_label};
    use crate::labels::{Label, Labels};
    use crate::promql::functions::PromQLArg;
    use crate::promql::{EvalSample, ExprResult};

    fn sample(value: f64, labels: &[(&str, &str)]) -> EvalSample {
        EvalSample {
            timestamp_ms: 0,
            value,
            labels: Labels::new(
                labels
                    .iter()
                    .map(|(name, value)| Label::new(*name, *value))
                    .collect(),
            )
            .into(),
            drop_name: false,
        }
    }

    fn values(result: ExprResult) -> Vec<f64> {
        let ExprResult::InstantVector(samples) = result else {
            panic!("expected instant vector");
        };
        samples.into_iter().map(|sample| sample.value).collect()
    }

    fn label_values(result: ExprResult, label: &str) -> Vec<String> {
        let ExprResult::InstantVector(samples) = result else {
            panic!("expected instant vector");
        };
        samples
            .into_iter()
            .map(|sample| sample.label_value(label).unwrap().to_string())
            .collect()
    }

    #[test]
    fn should_sort_nan_last_for_ascending_and_descending() {
        let input = vec![
            sample(f64::NAN, &[("id", "nan")]),
            sample(1.0, &[("id", "one")]),
            sample(2.0, &[("id", "two")]),
        ];

        let asc = values(sort(PromQLArg::InstantVector(input.clone()), false).unwrap());
        assert_eq!(asc[0], 1.0);
        assert_eq!(asc[1], 2.0);
        assert!(asc[2].is_nan());

        let desc = values(sort(PromQLArg::InstantVector(input), true).unwrap());
        assert_eq!(desc[0], 2.0);
        assert_eq!(desc[1], 1.0);
        assert!(desc[2].is_nan());
    }

    #[test]
    fn should_sort_by_label_descending() {
        let input = vec![
            sample(1.0, &[("instance", "1")]),
            sample(2.0, &[("instance", "2")]),
            sample(3.0, &[("instance", "10")]),
        ];

        let result = sort_by_label(
            vec![
                PromQLArg::InstantVector(input),
                PromQLArg::String("instance".to_string()),
            ],
            true,
        )
        .unwrap();

        assert_eq!(label_values(result, "instance"), vec!["10", "2", "1"]);
    }
}
