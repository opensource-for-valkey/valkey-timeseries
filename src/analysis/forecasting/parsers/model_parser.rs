use super::model_spec_parser::{
    ModelSpec, ModelSpecError, SpecValue, get_float_kwarg, get_kwarg_as_flag, get_usize_kwarg,
    parse_model_specs, value_as_usize_list,
};
use super::utils::{parse_decomposition_type, parse_seasonal_type};
use crate::analysis::forecasting::parsers::{
    ForecastModelKind, parse_seasonal_forecast_method, parse_trend_forecast_method,
};
use anofox_forecast::models::arima::{ARIMA, AutoARIMA, SARIMA};
use anofox_forecast::models::baseline::{Naive, SeasonalNaive, SimpleMovingAverage};
use anofox_forecast::models::exponential::{
    AutoETS, ETS, ETSSpec, HoltLinearTrend, HoltWinters, SeasonalType,
    SimpleExponentialSmoothing,
};
use anofox_forecast::models::intermittent::{ADIDA, Croston, IMAPA, TSB};
use anofox_forecast::models::theta::{DecompositionType, Theta};
use anofox_forecast::models::{
    AutoTBATS, BoxedForecaster, GARCH, MFLES, MSTLForecaster, SeasonalForecastMethod, TBATS,
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
        ForecastModelKind::Arima => {
            spec.ensure_arity(3)?;
            let ints = spec.get_positionals_as_usize()?;
            Ok(Box::new(ARIMA::new(ints[0], ints[1], ints[2])))
        }
        ForecastModelKind::AutoArima => handle_auto_arima(&mut spec),
        ForecastModelKind::Ets => handle_ets(&mut spec),
        ForecastModelKind::AutoEts => handle_auto_ets(&mut spec), // Fallback (should have returned earlier)
        ForecastModelKind::Sarima => {
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
        ForecastModelKind::Tbats => handle_tbats(&mut spec),
        ForecastModelKind::AutoTbats => handle_auto_tbats(&mut spec),
        ForecastModelKind::Theta => handle_theta(&mut spec),
        ForecastModelKind::Mstl => handle_mstl(&mut spec),
        ForecastModelKind::Mfles => handle_mfles(&mut spec),
        ForecastModelKind::Naive => handle_naive(&mut spec),
        ForecastModelKind::SeasonalNaive => handle_seasonal_naive(&mut spec),
        ForecastModelKind::Sma => handle_sma(&mut spec),
        ForecastModelKind::Adida => handle_adida(&mut spec),
        ForecastModelKind::Croston => handle_croston(&mut spec),
        ForecastModelKind::Imapa => handle_imapa(&mut spec),
        ForecastModelKind::Tsb => handle_tsb(&mut spec),
        ForecastModelKind::Ses => handle_ses(&mut spec),
        ForecastModelKind::Garch => handle_garch(&mut spec),
        ForecastModelKind::Holt => handle_holt(&mut spec),
        ForecastModelKind::HoltWinters => handle_holt_winters(&mut spec),
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
    let Some(mut periods) = get_seasonal_periods(spec)? else {
        return Ok(None);
    };

    if periods.len() > 1 {
        return Err(ModelSpecError::new(format!(
            "Expected 'seasonal_period' argument for model {} to be a single positive integer, but got a list of multiple periods",
            spec.model_name
        )));
    }

    Ok(periods.pop())
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

    Ok(Box::new(if let Some(period) = get_seasonal_period(spec)? {
        AutoARIMA::seasonal(period)
    } else {
        AutoARIMA::new()
    }))
}

fn handle_mfles(spec: &mut ModelSpec) -> Result<BoxedForecaster, ModelSpecError> {
    // MFLES requires at least one seasonal period, so if none provided, return error instead of defaulting to 12.
    // This is how the current library code functions internally currently
    let periods = get_seasonal_periods(spec)?.unwrap_or_else(|| vec![12]);

    let mut forecaster = MFLES::new(periods);

    if let Some(max_rounds) = get_usize_kwarg(spec, "max_rounds")? {
        forecaster = forecaster.with_max_rounds(max_rounds);
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

    if let Some(iterations) = get_usize_kwarg(spec, "iterations")? {
        forecaster = forecaster.with_iterations(iterations);
    }

    if let Some(robust) = get_kwarg_as_flag(spec, "robust")?
        && robust
    {
        forecaster = forecaster.robust();
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

    Ok(Box::new(Naive::new()))
}

fn handle_seasonal_naive(
    spec: &mut ModelSpec,
) -> Result<BoxedForecaster, ModelSpecError> {
    let nums = spec.get_positionals_as_usize()?;

    let positional_period = nums.first().copied();
    let keyword_period = get_usize_kwarg(spec, "period")?;
    let period = positional_period.or(keyword_period).unwrap_or(12);

    if nums.len() > 1 {
        return Err(ModelSpecError::new(
            "SeasonalNaive accepts at most 1 positional argument (period)",
        ));
    }

    Ok(Box::new(SeasonalNaive::new(period)))
}

fn handle_sma(spec: &mut ModelSpec) -> Result<BoxedForecaster, ModelSpecError> {
    let nums = spec.get_positionals_as_usize()?;

    let positional_window = nums.first().copied();
    let keyword_window = get_usize_kwarg(spec, "window")?;
    let window = positional_window.or(keyword_window).unwrap_or(0);

    let mut forecaster = SimpleMovingAverage::new(window);

    if let Some(cp) = get_usize_kwarg(spec, "changepoint")? {
        forecaster = forecaster.with_changepoint(cp);
    }

    if nums.len() > 1 {
        return Err(ModelSpecError::new(
            "SMA accepts at most 1 positional argument (window)",
        ));
    }

    Ok(Box::new(forecaster))
}

fn handle_adida(spec: &mut ModelSpec) -> Result<BoxedForecaster, ModelSpecError> {
    spec.ensure_arity(0)?;

    let mut forecaster = ADIDA::new();

    if let Some(alpha) = get_float_kwarg(spec, "alpha")? {
        forecaster = forecaster.with_alpha(alpha);
    }

    if let Some(level) = get_usize_kwarg(spec, "aggregation_level")? {
        forecaster = forecaster.with_aggregation_level(level);
    }

    Ok(Box::new(forecaster))
}

fn handle_croston(spec: &mut ModelSpec) -> Result<BoxedForecaster, ModelSpecError> {
    spec.ensure_arity(0)?;

    let mut forecaster = Croston::new();

    if let Some(alpha) = get_float_kwarg(spec, "alpha")? {
        forecaster = forecaster.with_alpha(alpha);
    }

    let sba_optimized = get_kwarg_as_flag(spec, "sba_optimized")?.unwrap_or(false);
    let sba = get_kwarg_as_flag(spec, "sba")?.unwrap_or(false);
    let optimized = get_kwarg_as_flag(spec, "optimized")?.unwrap_or(false);

    if sba_optimized {
        forecaster = forecaster.sba_optimized();
    } else if sba {
        forecaster = forecaster.sba();
    } else if optimized {
        forecaster = forecaster.optimized();
    }

    Ok(Box::new(forecaster))
}

fn handle_imapa(spec: &mut ModelSpec) -> Result<BoxedForecaster, ModelSpecError> {
    spec.ensure_arity(0)?;

    let mut forecaster = IMAPA::new();

    if let Some(max_level) = get_usize_kwarg(spec, "max_aggregation")? {
        forecaster = forecaster.with_max_aggregation(max_level);
    }

    Ok(Box::new(forecaster))
}

fn handle_tsb(spec: &mut ModelSpec) -> Result<BoxedForecaster, ModelSpecError> {
    let nums = spec.get_positionals_as_numbers()?;

    let positional_alpha_d = nums.first().copied();
    let positional_alpha_p = nums.get(1).copied();
    let keyword_alpha_d = get_float_kwarg(spec, "alpha_d")?;
    let keyword_alpha_p = get_float_kwarg(spec, "alpha_p")?;

    let alpha_d = positional_alpha_d.or(keyword_alpha_d);
    let alpha_p = positional_alpha_p.or(keyword_alpha_p);

    let forecaster = match (alpha_d, alpha_p) {
        (Some(d), Some(p)) => TSB::new().with_params(d, p),
        (None, None) => TSB::new(),
        _ => {
            return Err(ModelSpecError::new(
                "TSB requires either both alpha_d and alpha_p, or neither for defaults",
            ));
        }
    };

    if nums.len() > 2 {
        return Err(ModelSpecError::new(
            "TSB accepts at most 2 positional arguments (alpha_d, alpha_p)",
        ));
    }

    Ok(Box::new(forecaster))
}

fn handle_ses(spec: &mut ModelSpec) -> Result<BoxedForecaster, ModelSpecError> {
    let nums = spec.get_positionals_as_numbers()?;

    // Alpha can come from a positional arg or the "alpha" keyword arg.
    let positional_alpha = nums.first().copied();
    let keyword_alpha = get_float_kwarg(spec, "alpha")?;
    let alpha = positional_alpha.or(keyword_alpha);

    let forecaster = match alpha {
        Some(a) => SimpleExponentialSmoothing::new(a),
        None => SimpleExponentialSmoothing::auto(),
    };

    if nums.len() > 1 {
        return Err(ModelSpecError::new(
            "SES accepts at most 1 positional argument (alpha)",
        ));
    }

    Ok(Box::new(forecaster))
}

fn handle_garch(spec: &mut ModelSpec) -> Result<BoxedForecaster, ModelSpecError> {
    let nums = spec.get_positionals_as_usize()?;

    // p and q can come from positional args or keyword args.
    let positional_p = nums.first().copied();
    let positional_q = nums.get(1).copied();
    let keyword_p = get_usize_kwarg(spec, "p")?;
    let keyword_q = get_usize_kwarg(spec, "q")?;

    let p = positional_p.or(keyword_p).unwrap_or(1);
    let q = positional_q.or(keyword_q).unwrap_or(1);

    let mut builder = GARCH::builder().p(p).q(q);

    if let Some(omega) = get_float_kwarg(spec, "omega")? {
        builder = builder.omega(omega);
    }

    if let Some(max_iterations) = get_usize_kwarg(spec, "max_iterations")? {
        builder = builder.max_iterations(max_iterations);
    }

    if let Some(tolerance) = get_float_kwarg(spec, "tolerance")? {
        builder = builder.tolerance(tolerance);
    }

    if nums.len() > 2 {
        return Err(ModelSpecError::new(
            "GARCH accepts at most 2 positional arguments (p, q)",
        ));
    }

    Ok(Box::new(builder.build()))
}

fn handle_holt(spec: &mut ModelSpec) -> Result<BoxedForecaster, ModelSpecError> {
    let nums = spec.get_positionals_as_numbers()?;

    // Alpha and beta can come from positional args or keyword args.
    let positional_alpha = nums.first().copied();
    let positional_beta = nums.get(1).copied();
    let positional_phi = nums.get(2).copied();
    let keyword_alpha = get_float_kwarg(spec, "alpha")?;
    let keyword_beta = get_float_kwarg(spec, "beta")?;
    let keyword_phi = get_float_kwarg(spec, "phi")?;
    let damped_flag = get_kwarg_as_flag(spec, "damped")?;

    let alpha = positional_alpha.or(keyword_alpha);
    let beta = positional_beta.or(keyword_beta);
    let phi = positional_phi.or(keyword_phi);

    // Determine if damping should be used: explicit phi, positional phi, or damped=true
    let use_damping = phi.is_some() || damped_flag.unwrap_or(false);

    let forecaster = match (alpha, beta, use_damping) {
        (Some(a), Some(b), true) => {
            let p = phi.unwrap_or(0.98);
            HoltLinearTrend::damped(a, b, p)
        }
        (Some(a), Some(b), false) => HoltLinearTrend::new(a, b),
        (None, None, true) => HoltLinearTrend::auto_damped(),
        (None, None, false) => HoltLinearTrend::auto(),
        _ => {
            return Err(ModelSpecError::new(
                "Holt requires either both alpha and beta (optionally with phi/damped), or neither for auto-optimization",
            ));
        }
    };

    if nums.len() > 3 {
        return Err(ModelSpecError::new(
            "Holt accepts at most 3 positional arguments (alpha, beta, phi)",
        ));
    }

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
        Theta::new()
    };

    if !spec.keyword_args.is_empty() {
        return Err(ModelSpecError::new(format!(
            "Invalid options for Theta forecaster: {}",
            saved_spec
        )));
    }

    Ok(Box::new(forecaster))
}

fn handle_holt_winters(spec: &mut ModelSpec) -> Result<BoxedForecaster, ModelSpecError> {
    let nums = spec.get_positionals_as_numbers()?;
    let seasonal_period = match nums.first() {
        Some(&n) if n >= 1.0 && n.fract() == 0.0 => n as usize,
        Some(_) => {
            return Err(ModelSpecError::new(
                "HoltWinters seasonal period must be a positive integer",
            ));
        }
        None => {
            // Fall back to keyword arg if no positional args
            match get_seasonal_period(spec)? {
                Some(period) => period,
                None => {
                    return Err(ModelSpecError::new(
                        "HoltWinters requires a seasonal period, e.g. HoltWinters(12) or HoltWinters(seasonal_period=12)",
                    ));
                }
            }
        }
    };

    let seasonal_type = if let Some(st) = get_seasonal_type_kwarg(spec)? {
        st
    } else {
        SeasonalType::Additive
    };

    // Get smoothing params from keyword args, with positional overrides
    let alpha = get_float_kwarg(spec, "alpha")?;
    let beta = get_float_kwarg(spec, "beta")?;
    let gamma = get_float_kwarg(spec, "gamma")?;

    let param_offset = if nums.is_empty() { 0 } else { 1 }; // skip seasonal period
    let alpha = if nums.len() > param_offset { Some(nums[param_offset]) } else { alpha };
    let beta = if nums.len() > param_offset + 1 { Some(nums[param_offset + 1]) } else { beta };
    let gamma = if nums.len() > param_offset + 2 { Some(nums[param_offset + 2]) } else { gamma };

    let forecaster = match (alpha, beta, gamma) {
        (Some(a), Some(b), Some(g)) => {
            HoltWinters::new(a, b, g, seasonal_period, seasonal_type)
        }
        (None, None, None) => {
            HoltWinters::auto(seasonal_period, seasonal_type)
        }
        _ => {
            return Err(ModelSpecError::new(
                "HoltWinters requires either all three smoothing parameters (alpha, beta, gamma) or none for auto-optimization",
            ));
        }
    };

    // Reject extraneous positional arguments beyond period + alpha, beta, gamma
    if nums.len() > 4 {
        return Err(ModelSpecError::new(
            "HoltWinters accepts at most 4 positional arguments: seasonal_period, alpha, beta, gamma",
        ));
    }

    Ok(Box::new(forecaster))
}

fn get_seasonal_type_kwarg(
    spec: &mut ModelSpec,
) -> Result<Option<SeasonalType>, ModelSpecError> {
    const KEY: &str = "seasonal_type";
    let msg = format!(
        "Expected argument '{KEY}' for model {} to be 'additive' or 'multiplicative'",
        spec.model_name
    );
    if let Some(arg) = spec.remove_kwarg(KEY) {
        match arg {
            SpecValue::Ident(s) | SpecValue::String(s) => match parse_seasonal_type(&s) {
                Some(t) => Ok(Some(t)),
                None => Err(ModelSpecError::new(msg)),
            },
            _ => Err(ModelSpecError::new(msg)),
        }
    } else {
        Ok(None)
    }
}

fn handle_auto_ets(spec: &mut ModelSpec) -> Result<BoxedForecaster, ModelSpecError> {
    if !spec.positional_args.is_empty() {
        return Err(ModelSpecError::new(
            "AutoETS does not accept positional arguments",
        ));
    }

    Ok(Box::new(if let Some(period) = get_seasonal_period(spec)? {
        AutoETS::with_period(period)
    } else {
        AutoETS::new()
    }))
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
        [SpecValue::Ident(notation)] => {
            // ETS(ANN) style with a 3-char notation string as a single identifier
            let ets_spec = ETSSpec::from_notation(notation)
                .map_err(|e| ModelSpecError::new(e.to_string()))?;
            if let Some(period) = get_seasonal_period(spec)? {
                ETS::new(ets_spec, period)
            } else {
                ETS::new(ets_spec, 1)
            }
        }
        [SpecValue::Ident(notation), SpecValue::Number(_period)] => {
            // ETS(ANN, 12) style with notation string and seasonal period
            let period = try_as_seasonal_period(&args[1])?;
            let ets_spec = ETSSpec::from_notation(notation)
                .map_err(|e| ModelSpecError::new(e.to_string()))?;
            ETS::new(ets_spec, period)
        }
        [SpecValue::Number(_p), SpecValue::Ident(notation)] => {
            // ETS(12, ANN) style with period first then notation
            let period = try_as_seasonal_period(&args[0])?;
            let ets_spec = ETSSpec::from_notation(notation)
                .map_err(|e| ModelSpecError::new(e.to_string()))?;
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
        if let SpecValue::Ident(arg) = &arg
            && let Some(method) = parse_trend_forecast_method(arg.as_str())
        {
            return Ok(Some(method));
        };
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
        if let SpecValue::Ident(arg) | SpecValue::String(arg) = &arg
            && let Some(method) = parse_seasonal_forecast_method(arg.as_str())
        {
            return Ok(Some(method));
        };
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
            "ADIDA(), ARIMA(1,1,1), AutoARIMA(), Croston(), GARCH(), Holt(), IMAPA(), SARIMA(1,1,1,1,1,1,12), SeasonalNaive(), SMA(), TBATS(12), AutoTBATS(12), Theta(), TSB(), MSTL(12), MFLES(12), ETS(), AutoETS(), HoltWinters(12), SES()",
        )
            .unwrap();
        assert_eq!(models.len(), 20);
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
                .contains("Invalid positional arguments")
        );
    }
}
