use super::ForecastTransformKind;
use super::SpecValue;
use super::transform_spec_parser::{TransformSpec, TransformSpecError, parse_transform_specs};
use crate::analysis::forecasting::DynTransform;
use anofox_forecast::transform::transforms::{
    BoxCoxTransform, DifferenceTransform, LogTransform, ScaleMethod, ScaleTransform,
    SeasonalDifferenceTransform, YeoJohnsonTransform,
};
use anofox_forecast::transform::{Pipeline, Transform};

pub fn build_transforms_from_specs(
    input: &str,
) -> Result<Vec<Box<dyn Transform>>, TransformSpecError> {
    parse_transform_specs(input)?
        .into_iter()
        .map(build_single_transform)
        .collect()
}

pub fn build_transform_pipeline(input: &str) -> Result<Pipeline, TransformSpecError> {
    let specs = parse_transform_specs(input)?;
    if specs.is_empty() {
        return Err(TransformSpecError::new("No transforms specified"));
    }
    let mut builder = Pipeline::builder();
    for spec in specs {
        let transform = build_single_transform(spec)?;
        builder = builder.transform(DynTransform::new(transform));
    }
    let pipeline = builder.build();
    Ok(pipeline)
}

pub fn build_single_transform(
    mut spec: TransformSpec,
) -> Result<Box<dyn Transform>, TransformSpecError> {
    match spec.transform_type {
        ForecastTransformKind::Difference => {
            spec.ensure_arity(1)?;
            let d = as_usize(&spec.positional_args[0], &spec.transform_name)?;
            Ok(Box::new(DifferenceTransform::new(d)))
        }
        ForecastTransformKind::SeasonalDifference => {
            spec.ensure_arity(1)?;
            let period = as_usize(&spec.positional_args[0], &spec.transform_name)?;
            Ok(Box::new(SeasonalDifferenceTransform::new(period)))
        }
        ForecastTransformKind::BoxCox => {
            let positional_lambda = match spec.positional_args.as_slice() {
                [] => None,
                [value] => Some(parse_lambda(value, &spec.transform_name)?),
                _ => {
                    return Err(TransformSpecError::new(format!(
                        "Transform {} expects at most one positional lambda argument",
                        spec.transform_name
                    )));
                }
            };

            let keyword_lambda = remove_kwarg(&mut spec, "lambda")
                .map(|value| parse_lambda(&value, &spec.transform_name))
                .transpose()?;

            if !spec.keyword_args.is_empty() {
                let unsupported = spec
                    .keyword_args
                    .iter()
                    .map(|(key, _)| key.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(TransformSpecError::new(format!(
                    "Unsupported keyword argument(s) for transform {}: {}",
                    spec.transform_name, unsupported
                )));
            }

            if positional_lambda.is_some() && keyword_lambda.is_some() {
                return Err(TransformSpecError::new(format!(
                    "Transform {} received lambda both positionally and as keyword",
                    spec.transform_name
                )));
            }

            let lambda = positional_lambda.or(keyword_lambda);
            Ok(match lambda {
                Some(lambda) => Box::new(BoxCoxTransform::with_lambda(lambda)),
                None => Box::new(BoxCoxTransform::auto()),
            })
        }
        ForecastTransformKind::YeoJohnson => {
            spec.ensure_arity(0)?;
            Ok(Box::new(YeoJohnsonTransform::auto()))
        }
        ForecastTransformKind::Scale => {
            spec.ensure_arity(1)?;
            let method = parse_scale_method(&spec.positional_args[0], &spec.transform_name)?;
            Ok(Box::new(ScaleTransform::new(method)))
        }
        ForecastTransformKind::Log => {
            spec.ensure_arity(0)?;
            Ok(Box::new(LogTransform::new()))
        }
    }
}

fn as_usize(value: &SpecValue, transform_name: &str) -> Result<usize, TransformSpecError> {
    value.as_usize().map_err(|_| {
        TransformSpecError::new(format!(
            "Transform {transform_name} expects a non-negative integer positional argument"
        ))
    })
}

fn parse_lambda(value: &SpecValue, transform_name: &str) -> Result<f64, TransformSpecError> {
    value.as_float().map_err(|_| {
        TransformSpecError::new(format!(
            "Transform {transform_name} expects lambda to be a numeric value"
        ))
    })
}

fn remove_kwarg(spec: &mut TransformSpec, key: &str) -> Option<SpecValue> {
    if let Some(pos) = spec.keyword_args.iter().position(|(k, _)| k == key) {
        Some(spec.keyword_args.remove(pos).1)
    } else {
        None
    }
}

fn parse_scale_method(
    value: &SpecValue,
    transform_name: &str,
) -> Result<ScaleMethod, TransformSpecError> {
    let ident = value.as_ident().ok_or_else(|| {
        TransformSpecError::new(format!(
            "Transform {transform_name} expects scale method as identifier or string"
        ))
    })?;

    if ident.eq_ignore_ascii_case("standardize") {
        Ok(ScaleMethod::Standardize)
    } else if ident.eq_ignore_ascii_case("normalize") {
        Ok(ScaleMethod::Normalize)
    } else if ident.eq_ignore_ascii_case("robustscale") || ident.eq_ignore_ascii_case("robust") {
        Ok(ScaleMethod::RobustScale)
    } else {
        Err(TransformSpecError::new(format!(
            "Unsupported Scale method '{ident}', expected one of Standardize, Normalize, RobustScale"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::build_transforms_from_specs;

    #[test]
    fn builds_supported_transforms_from_specs() {
        let transforms = build_transforms_from_specs(
            "Difference(1), SeasonalDifference(12), BoxCox, YeoJohnson, Scale(Standardize), Log",
        )
        .unwrap();

        assert_eq!(transforms.len(), 6);
    }

    #[test]
    fn supports_boxcox_lambda_positional_and_keyword() {
        let transforms = build_transforms_from_specs("BoxCox(0.25), BoxCox(lambda=0.5)").unwrap();
        assert_eq!(transforms.len(), 2);
    }

    #[test]
    fn rejects_boxcox_duplicate_lambda_sources() {
        let err = build_transforms_from_specs("BoxCox(0.25, lambda=0.5)").unwrap_err();
        assert!(
            err.to_string()
                .contains("received lambda both positionally and as keyword")
        );
    }
}
