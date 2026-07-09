use super::ForecastModelKind;
use super::spec_parser::{KeywordArgs, SpecError, parse_specs};
use std::fmt::Display;

pub type ModelSpecError = SpecError;
pub use super::spec_parser::SpecValue;

#[derive(Debug, Clone)]
pub struct ModelSpec {
    pub model_name: String,
    pub model_type: ForecastModelKind,
    pub positional_args: Vec<SpecValue>,
    pub keyword_args: KeywordArgs,
}

impl ModelSpec {
    pub fn ensure_arity(&self, expected: usize) -> Result<(), ModelSpecError> {
        let actual = self.positional_args.len();
        if actual == expected {
            Ok(())
        } else {
            Err(ModelSpecError::new(format!(
                "{} expects {expected} positional args, got {actual}",
                self.model_name,
            )))
        }
    }

    pub fn get_kwarg(&self, key: &str) -> Option<&SpecValue> {
        self.keyword_args
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v)
    }

    pub(super) fn remove_kwarg(&mut self, key: &str) -> Option<SpecValue> {
        if let Some(pos) = self.keyword_args.iter().position(|(k, _)| k == key) {
            Some(self.keyword_args.remove(pos).1)
        } else {
            None
        }
    }

    pub fn get_kwarg_as_number(&self, key: &str) -> Result<Option<f64>, ModelSpecError> {
        if let Some(value) = self.get_kwarg(key) {
            match value {
                SpecValue::Number(n) => Ok(Some(*n)),
                _ => Err(ModelSpecError::new(format!(
                    "Expected keyword argument '{key}' for model {} to be a number",
                    self.model_name
                ))),
            }
        } else {
            Ok(None)
        }
    }

    pub fn get_kwarg_as_flag(&self, key: &str) -> Result<Option<bool>, ModelSpecError> {
        if let Some(value) = self.get_kwarg(key) {
            match value {
                SpecValue::Flag(n) => Ok(Some(*n)),
                _ => Err(ModelSpecError::new(format!(
                    "Expected keyword argument '{key}' for model {} to be a flag",
                    self.model_name
                ))),
            }
        } else {
            Ok(None)
        }
    }

    pub fn get_kwarg_as_ident(&self, key: &str) -> Option<&str> {
        self.get_kwarg(key).and_then(|v| match v {
            SpecValue::Ident(s) | SpecValue::String(s) => Some(s.as_str()),
            _ => None,
        })
    }

    pub fn get_usize_kwarg(&mut self, key: &str) -> Result<Option<usize>, ModelSpecError> {
        if let Some(arg) = self.remove_kwarg(key) {
            let value = arg.as_usize().map_err(|_| {
                ModelSpecError::new(format!("{} must be a non-negative integer", key))
            })?;
            return Ok(Some(value));
        }
        Ok(None)
    }

    pub fn expect_kwarg_as_float_list(
        &mut self,
        key: &str,
    ) -> Result<Option<Vec<f64>>, ModelSpecError> {
        match self.remove_kwarg(key) {
            Some(value) => {
                let value = value.as_float_list().map_err(|_| {
                    ModelSpecError::new(format!(
                        "Expected argument '{key}' for model {} to be a list of float values",
                        self.model_name
                    ))
                })?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    pub fn get_positionals_as_numbers(&self) -> Result<Vec<f64>, ModelSpecError> {
        self.positional_args
            .iter()
            .map(|arg| match arg {
                SpecValue::Number(n) => Ok(*n),
                _ => Err(ModelSpecError::new(format!(
                    "Expected all positional arguments for model {} to be numbers",
                    self.model_name
                ))),
            })
            .collect()
    }

    pub fn get_positionals_as_usize(&self) -> Result<Vec<usize>, ModelSpecError> {
        self.positional_args
            .iter()
            .map(as_usize)
            .collect::<Result<Vec<_>, _>>()
    }
}

impl ModelSpec {
    /// Returns the canonical display name matching `Forecaster::name()` output.
    pub fn canonical_model_name(&self) -> &str {
        use super::ForecastModelKind::*;
        match self.model_type {
            Adida => "ADIDA",
            Arima => "ARIMA",
            AutoArima => "AutoARIMA",
            Croston => "Croston",
            Garch => "GARCH",
            Holt => "Holt",
            Imapa => "IMAPA",
            Sarima => "SARIMA",
            SeasonalEs => "SeasonalES",
            SeasonalNaive => "SeasonalNaive",
            Ses => "SES",
            Sma => "SMA",
            Tbats => "TBATS",
            AutoTbats => "AutoTBATS",
            Theta => "Theta",
            Tsb => "TSB",
            Mstl => "MSTL",
            Mfles => "MFLES",
            Naive => "Naive",
            Ets => "ETS",
            AutoEts => "AutoETS",
            HoltWinters => "HoltWinters",
            RandomWalkWithDrift => "RandomWalkWithDrift",
        }
    }
}

impl Display for ModelSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}(", self.canonical_model_name())?;
        for (i, arg) in self.positional_args.iter().enumerate() {
            if i > 0 {
                write!(f, ",")?;
            }
            write!(f, "{}", arg)?;
        }
        for (i, (k, v)) in self.keyword_args.iter().enumerate() {
            if i == 0 && self.positional_args.is_empty() {
                write!(f, "{}={}", k, v)?;
            } else {
                write!(f, ",{}={}", k, v)?;
            }
        }
        write!(f, ")")?;
        Ok(())
    }
}

pub fn parse_model_specs(input: &str) -> Result<Vec<ModelSpec>, ModelSpecError> {
    parse_specs(input)?
        .into_iter()
        .map(|spec| {
            let model_type: ForecastModelKind = spec.name.parse().map_err(|_| {
                ModelSpecError::new(format!("Unsupported model name {}", spec.name))
            })?;
            Ok(ModelSpec {
                model_name: spec.name,
                model_type,
                positional_args: spec.positional_args,
                keyword_args: spec.keyword_args,
            })
        })
        .collect()
}

pub(super) fn get_usize_kwarg(
    spec: &mut ModelSpec,
    key: &str,
) -> Result<Option<usize>, ModelSpecError> {
    if let Some(arg) = spec.remove_kwarg(key) {
        return match arg {
            SpecValue::Number(n) if n >= 0.0 && n.fract() == 0.0 => Ok(Some(n as usize)),
            _ => Err(ModelSpecError::new(format!(
                "{} must be a non-negative integer",
                key
            ))),
        };
    }
    Ok(None)
}

pub(super) fn get_float_kwarg(
    spec: &mut ModelSpec,
    key: &str,
) -> Result<Option<f64>, ModelSpecError> {
    if let Some(arg) = spec.remove_kwarg(key) {
        match arg {
            SpecValue::Number(n) => Ok(Some(n)),
            _ => Err(ModelSpecError::new(format!(
                "Expected argument '{}' for model {} to be a float value",
                key, spec.model_name
            ))),
        }
    } else {
        Ok(None)
    }
}

pub(super) fn get_kwarg_as_flag(
    spec: &mut ModelSpec,
    key: &str,
) -> Result<Option<bool>, ModelSpecError> {
    if let Some(value) = spec.remove_kwarg(key) {
        match value {
            SpecValue::Flag(n) => Ok(Some(n)),
            _ => Err(ModelSpecError::new(format!(
                "Expected argument '{}' for model {} to be a flag",
                key, spec.model_name
            ))),
        }
    } else {
        Ok(None)
    }
}

pub(super) fn value_as_usize_list(value: &SpecValue) -> Result<Vec<usize>, ModelSpecError> {
    if let SpecValue::List(items) = value {
        let mut numbers = Vec::new();
        for item in items {
            if let SpecValue::Number(n) = item {
                if *n >= 1.0 && n.fract() == 0.0 {
                    numbers.push(*n as usize);
                } else {
                    return Err(ModelSpecError::new(format!(
                        "Expected all items in list to be positive integers, got {n}"
                    )));
                }
            } else {
                return Err(ModelSpecError::new(
                    "Expected all items in list to be numbers",
                ));
            }
        }
        Ok(numbers)
    } else {
        Err(ModelSpecError::new("Expected a list value"))
    }
}

fn as_usize(value: &SpecValue) -> Result<usize, ModelSpecError> {
    match value {
        SpecValue::Number(n) if *n >= 0.0 && n.fract() == 0.0 => Ok(*n as usize),
        _ => Err(ModelSpecError::new(
            "Canonical signatures require integer positional values",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::{SpecValue, parse_model_specs};

    #[test]
    fn parses_multiple_models_with_numbers_and_ident_values() {
        let specs = parse_model_specs("ARIMA(1, -2, 3e1), ETS(), AutoETS()").unwrap();
        assert_eq!(specs.len(), 3);
        assert_eq!(specs[0].model_name, "ARIMA");
        assert_eq!(specs[0].positional_args[0], SpecValue::Number(1.0));
        assert_eq!(specs[0].positional_args[1], SpecValue::Number(-2.0));
        assert_eq!(specs[0].positional_args[2], SpecValue::Number(30.0));
    }

    #[test]
    fn allows_trailing_comma_in_model_args() {
        let specs = parse_model_specs("ARIMA(1,2,3,)").unwrap();
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].positional_args.len(), 3);
    }

    #[test]
    fn rejects_duplicate_kwargs() {
        let err = parse_model_specs("ARIMA(p=1,p=2)").unwrap_err();
        assert!(err.to_string().contains("Duplicate keyword"));
    }

    #[test]
    fn rejects_positional_after_keyword() {
        let err = parse_model_specs("ARIMA(p=1,2)").unwrap_err();
        assert!(err.to_string().contains("Positional arguments"));
    }

    #[test]
    fn rejects_unknown_model_name() {
        let err = parse_model_specs("Unknown(1)").unwrap_err();
        assert!(err.to_string().contains("Unsupported model"));
    }

    #[test]
    fn parses_list_in_kwargs() {
        let specs = parse_model_specs("TBATS(periods=[12, 24, 7])").unwrap();
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].model_name, "TBATS");
        let (_, val) = &specs[0].keyword_args[0];
        match val {
            SpecValue::List(items) => {
                assert_eq!(items.len(), 3);
                assert_eq!(items[0], SpecValue::Number(12.0));
                assert_eq!(items[1], SpecValue::Number(24.0));
                assert_eq!(items[2], SpecValue::Number(7.0));
            }
            other => panic!("Expected list, got {:?}", other),
        }
    }

    #[test]
    fn parses_list_in_positional_args_with_trailing_comma() {
        let specs = parse_model_specs("TBATS([12, 24, 7,])").unwrap();
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].model_name, "TBATS");
        let val = &specs[0].positional_args[0];
        match val {
            SpecValue::List(items) => {
                assert_eq!(items.len(), 3);
                assert_eq!(items[0], SpecValue::Number(12.0));
                assert_eq!(items[1], SpecValue::Number(24.0));
                assert_eq!(items[2], SpecValue::Number(7.0));
            }
            other => panic!("Expected list, got {:?}", other),
        }
    }

    #[test]
    fn parses_bare_model_names_without_parentheses() {
        let specs = parse_model_specs("AutoARIMA, AutoETS, ETS").unwrap();
        assert_eq!(specs.len(), 3);
        assert_eq!(specs[0].model_name, "AutoARIMA");
        assert!(specs[0].positional_args.is_empty());
        assert!(specs[0].keyword_args.is_empty());

        assert_eq!(specs[1].model_name, "AutoETS");
        assert!(specs[1].positional_args.is_empty());
        assert!(specs[1].keyword_args.is_empty());

        assert_eq!(specs[2].model_name, "ETS");
        assert!(specs[2].positional_args.is_empty());
        assert!(specs[2].keyword_args.is_empty());
    }

    #[test]
    fn parses_bracketed_model_spec_list() {
        let specs = parse_model_specs("[ARIMA(1,2,3), ETS, AutoETS()]").unwrap();
        assert_eq!(specs.len(), 3);
        assert_eq!(specs[0].model_name, "ARIMA");
        assert_eq!(specs[1].model_name, "ETS");
        assert_eq!(specs[2].model_name, "AutoETS");
    }

    #[test]
    fn parses_empty_bracketed_model_spec_list() {
        let specs = parse_model_specs("[]").unwrap();
        assert!(specs.is_empty());
    }
}
