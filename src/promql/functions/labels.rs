use crate::promql::functions::types::{PromQLArg, PromQLFunction};
use crate::promql::functions::utils::{
    ensure_unique_labelsets, exact_arity_error, expect_exact_arg_count, expect_min_arg_count,
    expect_string, is_valid_label_name, min_arity_error,
};
use crate::promql::{EvalContext, EvalResult, EvaluationError, ExprResult};
use promql_parser::label::METRIC_NAME;
use regex::Regex;

#[derive(Copy, Clone)]
pub(in crate::promql) struct LabelReplaceFunction;

impl PromQLFunction for LabelReplaceFunction {
    fn apply(&self, _arg: PromQLArg, _eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        Err(exact_arity_error("label_replace", 5, 1))
    }

    fn apply_args(&self, args: Vec<PromQLArg>, _eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        // Expect exactly 5 evaluated arguments: instant vector, dst, replacement, src, regex
        expect_exact_arg_count("label_replace", 5, args.len())?;

        let mut args_iter = args.into_iter();
        let PromQLArg::InstantVector(mut samples) =
            args_iter.next().expect("validated args.len() == 5")
        else {
            return Err(EvaluationError::InternalError(
                "label_replace expects an instant vector as its first argument".to_string(),
            ));
        };

        let dst_label = args_iter
            .next()
            .expect("validated args.len() == 5")
            .into_string()?;
        let replacement = args_iter
            .next()
            .expect("validated args.len() == 5")
            .into_string()?;
        let src_label = args_iter
            .next()
            .expect("validated args.len() == 5")
            .into_string()?;
        let regex_src = args_iter
            .next()
            .expect("validated args.len() == 5")
            .into_string()?;

        if !is_valid_label_name(&dst_label) {
            return Err(EvaluationError::InternalError(format!(
                "invalid label name {:?}",
                dst_label
            )));
        }

        let regex = Regex::new(&format!("^(?s:{regex_src})$"))
            .map_err(|err| EvaluationError::InternalError(err.to_string()))?;

        for sample in &mut samples {
            let src_value = sample.labels.get(&src_label).unwrap_or_default();

            if let Some(captures) = regex.captures(src_value) {
                let mut replaced = String::new();
                captures.expand(&replacement, &mut replaced);

                if replaced.is_empty() {
                    sample.labels.remove(&dst_label);
                } else {
                    sample.labels.insert(&dst_label, replaced);
                }

                if dst_label == METRIC_NAME {
                    sample.drop_name = false;
                }
            }
        }

        ensure_unique_labelsets(&samples)?;
        Ok(ExprResult::InstantVector(samples))
    }

    fn apply_call(
        &self,
        evaluated_args: Vec<PromQLArg>,
        _ctx: &EvalContext,
    ) -> EvalResult<ExprResult> {
        expect_exact_arg_count("label_replace", 5, evaluated_args.len())?;

        let mut args_iter = evaluated_args.into_iter();
        let PromQLArg::InstantVector(mut samples) = args_iter
            .next()
            .expect("validated evaluated_args.len() == 5")
        else {
            return Err(EvaluationError::InternalError(
                "label_replace expects an instant vector as its first argument".to_string(),
            ));
        };

        let dst_label = expect_string(
            args_iter
                .next()
                .expect("validated evaluated_args.len() == 5"),
            "label_replace",
            "destination_label",
        )?;
        let replacement = expect_string(
            args_iter
                .next()
                .expect("validated evaluated_args.len() == 5"),
            "label_replace",
            "replacement",
        )?;
        let src_label = expect_string(
            args_iter
                .next()
                .expect("validated evaluated_args.len() == 5"),
            "label_replace",
            "source_label",
        )?;
        let regex_src = expect_string(
            args_iter
                .next()
                .expect("validated evaluated_args.len() == 5"),
            "label_replace",
            "regex",
        )?;

        if !is_valid_label_name(&dst_label) {
            return Err(EvaluationError::InternalError(format!(
                "invalid label name {:?}",
                dst_label
            )));
        }

        let regex = Regex::new(&format!("^(?s:{regex_src})$"))
            .map_err(|err| EvaluationError::InternalError(err.to_string()))?;

        for sample in &mut samples {
            let src_value = sample.labels.get(&src_label).unwrap_or_default();

            if let Some(captures) = regex.captures(src_value) {
                let mut replaced = String::new();
                captures.expand(&replacement, &mut replaced);

                if replaced.is_empty() {
                    sample.labels.remove(&dst_label);
                } else {
                    sample.labels.insert(&dst_label, replaced);
                }

                if dst_label == METRIC_NAME {
                    sample.drop_name = false;
                }
            }
        }

        ensure_unique_labelsets(&samples)?;
        Ok(ExprResult::InstantVector(samples))
    }
}

impl Default for LabelReplaceFunction {
    fn default() -> Self {
        LabelReplaceFunction
    }
}

/// `label_join(v instant-vector, dst_label string, separator string, src_label_1 string, src_label_2 string, ...)`
#[derive(Copy, Clone)]
pub(in crate::promql) struct LabelJoinFunction;

impl PromQLFunction for LabelJoinFunction {
    fn apply(&self, _arg: PromQLArg, _eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        Err(min_arity_error("label_join", 3, 1))
    }

    fn apply_call(
        &self,
        evaluated_args: Vec<PromQLArg>,
        _ctx: &EvalContext,
    ) -> EvalResult<ExprResult> {
        let actual_args = evaluated_args.len();
        expect_min_arg_count("label_join", 3, actual_args)?;

        let mut args_iter = evaluated_args.into_iter();
        let PromQLArg::InstantVector(mut samples) = args_iter
            .next()
            .expect("validated evaluated_args.len() == ctx.raw_args.len() >= 3")
        else {
            return Err(EvaluationError::InternalError(
                "label_join expects an instant vector as its first argument".to_string(),
            ));
        };
        let dst_arg = args_iter
            .next()
            .expect("validated evaluated_args.len() == 4");
        let separator_arg = args_iter
            .next()
            .expect("validated evaluated_args.len() == 3");

        let dst_label = expect_string(dst_arg, "label_join", "destination_label")?;
        let separator = expect_string(separator_arg, "label_join", "separator")?;
        let src_labels = args_iter
            .map(|arg| expect_string(arg, "label_join", "source_label"))
            .collect::<EvalResult<Vec<_>>>()?;

        if !is_valid_label_name(&dst_label) {
            return Err(EvaluationError::InternalError(format!(
                "invalid label name {:?}",
                dst_label
            )));
        }

        for sample in &mut samples {
            let mut joined = String::new();
            for (index, src_label) in src_labels.iter().enumerate() {
                if index > 0 {
                    joined.push_str(&separator);
                }
                if let Some(value) = sample.labels.get(src_label) {
                    joined.push_str(value);
                }
            }

            if joined.is_empty() {
                sample.labels.remove(&dst_label);
            } else {
                sample.labels.insert(&dst_label, joined);
            }

            if dst_label == METRIC_NAME {
                sample.drop_name = false;
            }
        }

        ensure_unique_labelsets(&samples)?;
        Ok(ExprResult::InstantVector(samples))
    }
}

impl Default for LabelJoinFunction {
    fn default() -> Self {
        LabelJoinFunction
    }
}
