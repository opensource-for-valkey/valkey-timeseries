use super::model_spec_parser::{
    ModelSpec, ModelSpecError, SpecValue, get_float_kwarg, get_kwarg_as_flag, get_usize_kwarg,
    parse_model_specs, value_as_usize_list,
};
use super::utils::parse_decomposition_type;
use crate::analysis::forecasting::parsers::{
    ForecastModelKind, parse_seasonal_forecast_method, parse_trend_forecast_method,
};
use anofox_forecast::models::arima::{ARIMA, AutoARIMA, SARIMA};
use anofox_forecast::models::baseline::Naive;
use anofox_forecast::models::exponential::{AutoETS, ETS, ETSSpec};
use anofox_forecast::models::theta::{DecompositionType, Theta};
use anofox_forecast::models::{
    AutoTBATS, BoxedForecaster, MFLES, MSTLForecaster, SeasonalForecastMethod, TBATS,
    TrendForecastMethod,
};

pub fn build_models_from_specs(input: &str) -> Result<Vec<BoxedForecaster>, ModelSpecError> {
    parse_model_specs(input)?
        .into_iter()
        .map(build_single_model)
        .collect()
}

pub fn build_single_model(mut spec: ModelSpec) -> Result<BoxedForecaster, ModelSpecError> {
    match spec.model_type {
        ForecastModelKind::ARIMA => {
            spec.ensure_arity(3)?;
            let ints = spec.get_positionals_as_usize()?;
            Ok(Box::new(ARIMA::new(ints[0], ints[1], ints[2])))
        }
        ForecastModelKind::AutoARIMA => handle_auto_arima(&mut spec),
        ForecastModelKind::ETS => handle_ets(&mut spec),
        ForecastModelKind::AutoETS => handle_auto_ets(&mut spec), // Fallback (should have returned earlier)
        ForecastModelKind::SARIMA => {
            let ints = spec.get_positionals_as_usize()?;
            let forecaster = match spec.positional_args.len() {
                3 => SARIMA::new(ints[0], ints[1], ints[2], 0, 0, 0, 0),
                7 => SARIMA::new(
                    ints[0], ints[1], ints[2], ints[3], ints[4], ints[5], ints[6],
                ),
                _ => {
                    return Err(ModelSpecError::new(
                        "SARIMA requires either 3 positional args (p,d,q) or 7 positional args (p,d,q,P,D,Q,seasonal_period)",
                    ));
                }
            };
            Ok(Box::new(forecaster))
        }
        ForecastModelKind::TBATS => handle_tbats(&mut spec),
        ForecastModelKind::AutoTBATS => handle_auto_tbats(&mut spec),
        ForecastModelKind::Theta => handle_theta(&mut spec),
        ForecastModelKind::MSTL => handle_mstl(&mut spec),
        ForecastModelKind::MFLES => handle_mfles(&mut spec),
        ForecastModelKind::Naive => handle_naive(&mut spec),
    }
}

fn try_as_seasonal_period(arg: &SpecValue) -> Result<usize, ModelSpecError> {
    match arg {
        SpecValue::Number(n) if *n >= 1.0 && n.fract() == 0.0 => Ok(*n as usize),
        _ => Err(ModelSpecError::new(format!(
            "Expected a positive integer for seasonal period, but got {arg}"
        ))),
    }
}

pub fn get_seasonal_period(spec: &mut ModelSpec) -> Result<Option<usize>, ModelSpecError> {
    match get_seasonal_periods(spec)? {
        Some(mut periods) => {
            if periods.len() > 1 {
                return Err(ModelSpecError::new(format!(
                    "Expected 'seasonal_period' argument for model {} to be a single positive integer, but got a list of multiple periods",
                    spec.model_name
                )));
            }
            Ok(periods.pop())
        }
        None => Ok(None),
    }
}

pub fn get_seasonal_periods(spec: &mut ModelSpec) -> Result<Option<Vec<usize>>, ModelSpecError> {
    let Some(arg) = spec.remove_kwarg("seasonal_period") else {
        return Ok(None);
    };
    match arg {
        SpecValue::Number(n) if n >= 1.0 && n.fract() == 0.0 => Ok(Some(vec![n as usize])),
        SpecValue::List(items) => Ok(Some(value_as_usize_list(&SpecValue::List(items))?)),
        _ => Err(ModelSpecError::new(format!(
            "Expected 'seasonal_period' argument for model {} to be either a positive integer or a list of positive integers",
            spec.model_name
        ))),
    }
}

fn handle_auto_tbats(spec: &mut ModelSpec) -> Result<BoxedForecaster, ModelSpecError> {
    let ints = spec.get_positionals_as_usize()?;
    if ints.is_empty() {
        return Err(ModelSpecError::new(
            "AutoTBATS requires at least one seasonal period",
        ));
    }

    let mut forecaster = AutoTBATS::new(ints);

    if let Some(use_search) = get_kwarg_as_flag(spec, "use_boxcox_search")?
        && !use_search
    {
        forecaster = forecaster.without_box_cox_search();
    }

    if let Some(use_search) = get_kwarg_as_flag(spec, "use_damped_trend_search")?
        && !use_search
    {
        forecaster = forecaster.without_damped_search();
    }

    if let Some(use_search) = get_kwarg_as_flag(spec, "use_no_trend_search")?
        && !use_search
    {
        forecaster = forecaster.without_no_trend_search();
    }

    Ok(Box::new(forecaster))
}

fn handle_auto_arima(spec: &mut ModelSpec) -> Result<BoxedForecaster, ModelSpecError> {
    if !spec.positional_args.is_empty() {
        return Err(ModelSpecError::new(
            "AutoARIMA does not accept positional arguments",
        ));
    }

    let forecaster = if let Some(period) = get_seasonal_period(spec)? {
        AutoARIMA::seasonal(period)
    } else {
        AutoARIMA::new()
    };

    Ok(Box::new(forecaster))
}

fn handle_mfles(spec: &mut ModelSpec) -> Result<BoxedForecaster, ModelSpecError> {
    // MFLES requires at least one seasonal period, so if none provided, return error instead of defaulting to 12.
    // This is how the current library code functions internally currently
    let periods = if let Some(periods) = get_seasonal_periods(spec)? {
        periods
    } else {
        vec![12]
    };

    let mut forecaster = MFLES::new(periods);

    if let Some(max_rounds) = get_usize_kwarg(spec, "max_rounds")? {
        forecaster = forecaster.with_max_rounds(max_rounds as usize);
    }

    if let Some(seasonal_lr) = get_float_kwarg(spec, "seasonal_lr")? {
        forecaster = forecaster.with_seasonal_lr(seasonal_lr);
    }

    if let Some(trend_lr) = get_float_kwarg(spec, "trend_lr")? {
        forecaster = forecaster.with_trend_lr(trend_lr);
    }

    if let Some(robust) = get_kwarg_as_flag(spec, "robust")?
        && robust
    {
        forecaster = forecaster.robust();
    }

    if let Some(multiplicative) = get_kwarg_as_flag(spec, "multiplicative")? {
        forecaster = forecaster.multiplicative(multiplicative);
    }

    Ok(Box::new(forecaster))
}

fn handle_mstl(spec: &mut ModelSpec) -> Result<BoxedForecaster, ModelSpecError> {
    let ints = spec.get_positionals_as_usize()?;
    if ints.is_empty() {
        return Err(ModelSpecError::new(
            "MSTL requires at least one seasonal period",
        ));
    }

    let mut forecaster = MSTLForecaster::new(ints);

    if let Some(iterations) =
        crate::analysis::forecasting::parsers::model_spec_parser::get_usize_kwarg(
            spec,
            "iterations",
        )?
    {
        forecaster = forecaster.with_iterations(iterations);
    }

    if let Some(robust) = get_kwarg_as_flag(spec, "robust")?
        && robust
    {
        if robust {
            forecaster = forecaster.robust();
        }
    }

    if let Some(trend_method) = get_trend_forecast_method(spec)? {
        forecaster = forecaster.with_trend_method(trend_method);
    }

    if let Some(seasonal_method) = get_seasonal_forecast_method(spec)? {
        forecaster = forecaster.with_seasonal_method(seasonal_method);
    }

    Ok(Box::new(forecaster))
}

fn handle_naive(spec: &mut ModelSpec) -> Result<BoxedForecaster, ModelSpecError> {
    spec.ensure_arity(0)?;
    if !spec.keyword_args.is_empty() {
        return Err(ModelSpecError::new(
            "Naive forecaster does not accept keyword arguments",
        ));
    }

    let forecaster = Naive::new();

    Ok(Box::new(forecaster))
}
fn handle_tbats(spec: &mut ModelSpec) -> Result<BoxedForecaster, ModelSpecError> {
    let ints = spec.get_positionals_as_usize()?;
    if ints.is_empty() {
        return Err(ModelSpecError::new(
            "TBATS requires at least one seasonal period",
        ));
    }

    let mut forecaster = TBATS::new(ints);

    if let Some(box_cox) = spec.remove_kwarg("use_boxcox") {
        match box_cox {
            SpecValue::Flag(false) => forecaster = forecaster.with_box_cox(1.0),
            SpecValue::Flag(true) => forecaster = forecaster.with_box_cox(0.0),
            SpecValue::Number(lambda) => forecaster = forecaster.with_box_cox(lambda),
            _ => {
                return Err(ModelSpecError::new(format!(
                    "Expected 'use_boxcox' argument for model {} to be either a boolean flag or a numeric lambda value",
                    spec.model_name
                )));
            }
        }
    }

    if let Some(alpha) = get_float_kwarg(spec, "damped_trend")? {
        forecaster = forecaster.with_damped_trend(alpha);
    }

    Ok(Box::new(forecaster))
}

// Theta has no positional args, but we want to support keyword args for future extensibility
// (e.g., decomposition type), so handle it separately to provide clearer error messages.
fn handle_theta(spec: &mut ModelSpec) -> Result<BoxedForecaster, ModelSpecError> {
    let saved_spec = spec.clone();

    let forecaster = if let Some(period) = get_seasonal_period(spec)? {
        if let Some(decomposition) = get_decomposition_type_kwarg(spec)? {
            Theta::seasonal_with_decomposition(period, decomposition)
        } else if let Some(optimized) = get_kwarg_as_flag(spec, "optimized")? {
            if optimized {
                Theta::seasonal_optimized(period)
            } else {
                Theta::seasonal(period)
            }
        } else {
            Theta::seasonal(period)
        }
        // all other combos are invalid. raise error
    } else if let Some(theta) = get_float_kwarg(spec, "theta")? {
        Theta::with_theta(theta)
    } else if let Some(optimized) = get_kwarg_as_flag(spec, "optimized")? {
        if optimized {
            Theta::optimized()
        } else {
            Theta::new()
        }
    } else {
        return Err(ModelSpecError::new(format!(
            "Invalid Theta model specification: {}",
            saved_spec
        )));
    };

    if !spec.keyword_args.is_empty() {
        return Err(ModelSpecError::new(format!(
            "Invalid options for Theta forecaster: {}",
            saved_spec
        )));
    }

    Ok(Box::new(forecaster))
}

fn handle_auto_ets(spec: &mut ModelSpec) -> Result<BoxedForecaster, ModelSpecError> {
    if !spec.positional_args.is_empty() {
        return Err(ModelSpecError::new(
            "AutoETS does not accept positional arguments",
        ));
    }

    let forecaster = if let Some(period) = get_seasonal_period(spec)? {
        AutoETS::with_period(period)
    } else {
        AutoETS::new()
    };

    Ok(Box::new(forecaster))
}

// ETS/AutoETS have mixed positional args (notation string + optional integer),
// so handle them before the integer-only conversion below.
fn handle_ets(spec: &mut ModelSpec) -> Result<BoxedForecaster, ModelSpecError> {
    let args = &spec.positional_args;
    let forecaster = match args.as_slice() {
        [] => {
            // Default ETS() with no args is valid and defaults to ETS(ANN)
            ETS::default()
        }
        [SpecValue::Number(_)] => {
            let period = try_as_seasonal_period(&args[0])?;
            // ETS(12) style with just seasonal period, defaults to ANN notation
            ETS::new(ETSSpec::ann(), period)
        }
        [
            SpecValue::Ident(e),
            SpecValue::Ident(t),
            SpecValue::Number(s),
        ] => {
            // ETS(ANN) style with notation and seasonal period
            let notation = format!("{}{}{}", e, t, s);
            let ets_spec = ETSSpec::from_notation(&notation)
                .map_err(|e| ModelSpecError::new(e.to_string()))?;
            if let Some(period) = get_seasonal_period(spec)? {
                ETS::new(ets_spec, period)
            } else {
                ETS::new(ets_spec, 1)
            }
        }
        [
            SpecValue::Number(_p),
            SpecValue::Ident(e),
            SpecValue::Ident(t),
            SpecValue::Number(s),
        ] => {
            // ETS(ANN) style with notation and seasonal period
            let notation = format!("{}{}{}", e, t, s);
            let ets_spec = ETSSpec::from_notation(&notation)
                .map_err(|e| ModelSpecError::new(e.to_string()))?;
            let period = try_as_seasonal_period(&args[0])?;
            ETS::new(ets_spec, period)
        }
        _ => {
            return Err(ModelSpecError::new(
                "Invalid positional arguments for ETS. Expected either ETS(), ETS(ANN), or ETS(ANN, 12) where ANN is a 3-character notation string and 12 is a positive integer seasonal period.",
            ));
        }
    };

    Ok(Box::new(forecaster))
}

fn get_decomposition_type_kwarg(
    spec: &mut ModelSpec,
) -> Result<Option<DecompositionType>, ModelSpecError> {
    const KEY: &str = "decomposition_type";
    let msg = format!(
        "Expected argument '{KEY}' for model {} to be a decomposition type (additive or multiplicative)",
        spec.model_name
    );
    if let Some(arg) = spec.remove_kwarg(KEY) {
        match arg {
            SpecValue::Ident(s) | SpecValue::String(s) => match parse_decomposition_type(&s) {
                Some(t) => Ok(Some(t)),
                None => Err(ModelSpecError::new(msg)),
            },
            _ => Err(ModelSpecError::new(msg)),
        }
    } else {
        Ok(None)
    }
}

fn get_trend_forecast_method(
    spec: &mut ModelSpec,
) -> Result<Option<TrendForecastMethod>, ModelSpecError> {
    if let Some(arg) = spec.remove_kwarg("trend_forecast_method") {
        if let SpecValue::Ident(arg) = &arg {
            if let Some(method) = parse_trend_forecast_method(arg.as_str()) {
                return Ok(Some(method));
            };
        }
        Err(ModelSpecError::new(format!(
            "Invalid trend forecast method '{arg}' for model {}",
            spec.model_name
        )))
    } else {
        Ok(None)
    }
}

fn get_seasonal_forecast_method(
    spec: &mut ModelSpec,
) -> Result<Option<SeasonalForecastMethod>, ModelSpecError> {
    if let Some(arg) = spec.remove_kwarg("seasonal_forecast_method") {
        if let SpecValue::Ident(arg) | SpecValue::String(arg) = &arg {
            if let Some(method) = parse_seasonal_forecast_method(arg.as_str()) {
                return Ok(Some(method));
            };
        }
        Err(ModelSpecError::new(format!(
            "Invalid seasonal forecast method '{arg}' for model {}",
            spec.model_name
        )))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::build_models_from_specs;

    #[test]
    fn constructs_all_supported_models_from_canonical_specs() {
        let models = build_models_from_specs(
            "ARIMA(1,1,1), AutoARIMA(), SARIMA(1,1,1,1,1,1,12), TBATS(12), AutoTBATS(12), Theta(), MSTL(12), MFLES(12), ETS(), AutoETS()",
        )
            .unwrap();
        assert_eq!(models.len(), 10);
    }

    #[test]
    fn constructs_ets_from_notation() {
        // ETS with notation only (no seasonal period, defaults to 1)
        let models = build_models_from_specs("ETS(ANN)").unwrap();
        assert_eq!(models.len(), 1);

        // ETS with notation and seasonal period
        let models = build_models_from_specs("ETS(ANM, 12)").unwrap();
        assert_eq!(models.len(), 1);

        // ETS with additive trend and multiplicative seasonal
        let models = build_models_from_specs("ETS(AAM, 4)").unwrap();
        assert_eq!(models.len(), 1);

        // ETS with damped trend notation
        let models = build_models_from_specs("ETS(AAdN, 1)").unwrap();
        assert_eq!(models.len(), 1);
    }

    #[test]
    fn rejects_ets_invalid_notation() {
        assert!(build_models_from_specs("ETS(ZZZ)").is_err());
    }

    #[test]
    fn rejects_ets_too_many_args() {
        let result = build_models_from_specs("ETS(ANN, 12, 99)");
        assert!(result.is_err());
        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("at most 2 positional")
        );
    }
}
