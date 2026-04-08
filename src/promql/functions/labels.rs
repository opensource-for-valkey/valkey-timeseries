use crate::promql::functions::types::{FunctionCallContext, PromQLArg, PromQLFunction};
use crate::promql::functions::utils::{
    ensure_unique_labelsets, exact_arity_error, extract_string_arg, is_valid_label_name,
    min_arity_error,
};
use crate::promql::{EvalResult, EvaluationError, ExprResult};
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
        if args.len() != 5 {
            return Err(exact_arity_error("label_replace", 5, args.len()));
        }

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
        ctx: &FunctionCallContext<'_>,
    ) -> EvalResult<ExprResult> {
        if evaluated_args.len() != 5 || ctx.raw_args.len() != 5 {
            return Err(exact_arity_error("label_replace", 5, ctx.raw_args.len()));
        }

        let mut args_iter = evaluated_args.into_iter();
        let PromQLArg::InstantVector(mut samples) = args_iter
            .next()
            .expect("validated evaluated_args.len() == 5")
        else {
            return Err(EvaluationError::InternalError(
                "label_replace expects an instant vector as its first argument".to_string(),
            ));
        };

        let dst_label = extract_string_arg(&ctx.raw_args[1], "label_replace", 1)?;
        let replacement = extract_string_arg(&ctx.raw_args[2], "label_replace", 2)?;
        let src_label = extract_string_arg(&ctx.raw_args[3], "label_replace", 3)?;
        let regex_src = extract_string_arg(&ctx.raw_args[4], "label_replace", 4)?;

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

#[derive(Copy, Clone)]
pub(in crate::promql) struct LabelJoinFunction;

impl PromQLFunction for LabelJoinFunction {
    fn apply(&self, _arg: PromQLArg, _eval_timestamp_ms: i64) -> EvalResult<ExprResult> {
        Err(min_arity_error("label_join", 3, 1))
    }

    fn apply_call(
        &self,
        evaluated_args: Vec<PromQLArg>,
        ctx: &FunctionCallContext<'_>,
    ) -> EvalResult<ExprResult> {
        let actual_args = ctx.raw_args.len();
        if actual_args < 3 || evaluated_args.len() != actual_args {
            return Err(min_arity_error("label_join", 3, actual_args));
        }

        let mut args_iter = evaluated_args.into_iter();
        let PromQLArg::InstantVector(mut samples) = args_iter
            .next()
            .expect("validated evaluated_args.len() == ctx.raw_args.len() >= 3")
        else {
            return Err(EvaluationError::InternalError(
                "label_join expects an instant vector as its first argument".to_string(),
            ));
        };

        let dst_label = extract_string_arg(&ctx.raw_args[1], "label_join", 1)?;
        let separator = extract_string_arg(&ctx.raw_args[2], "label_join", 2)?;
        let src_labels = ctx.raw_args[3..]
            .iter()
            .enumerate()
            .map(|(index, arg)| extract_string_arg(arg, "label_join", index + 3))
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
