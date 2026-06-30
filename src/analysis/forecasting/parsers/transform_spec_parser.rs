use super::spec_parser::parse_specs;
use super::{ForecastTransformKind, SpecError, SpecValue};

pub type TransformSpecError = SpecError;

#[derive(Debug, Clone)]
pub struct TransformSpec {
    pub transform_name: String,
    pub transform_type: ForecastTransformKind,
    pub positional_args: Vec<SpecValue>,
    pub keyword_args: Vec<(String, SpecValue)>,
}

impl TransformSpec {
    pub fn ensure_arity(&self, expected: usize) -> Result<(), TransformSpecError> {
        let actual = self.positional_args.len();
        if actual == expected {
            Ok(())
        } else {
            Err(TransformSpecError::new(format!(
                "{} expects {expected} positional args, got {actual}",
                self.transform_name,
            )))
        }
    }

}

pub fn parse_transform_specs(input: &str) -> Result<Vec<TransformSpec>, TransformSpecError> {
    parse_specs(input)?
        .into_iter()
        .map(|spec| {
            let transform_type = spec.name.parse().map_err(|_| {
                TransformSpecError::new(format!(
                    "Unsupported transform name {}",
                    spec.name
                ))
            })?;
            Ok(TransformSpec {
                transform_name: spec.name,
                transform_type,
                positional_args: spec.positional_args,
                keyword_args: spec.keyword_args,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::parse_transform_specs;

    #[test]
    fn parses_multiple_transforms() {
        let specs = parse_transform_specs("Difference(1), Scale(Standardize), Log").unwrap();
        assert_eq!(specs.len(), 3);
        assert_eq!(specs[0].transform_name, "Difference");
        assert_eq!(specs[1].transform_name, "Scale");
        assert_eq!(specs[2].transform_name, "Log");
    }

    #[test]
    fn rejects_unknown_transform_name() {
        let err = parse_transform_specs("Unknown(1)").unwrap_err();
        assert!(err.to_string().contains("Unsupported transform name"));
    }
}
