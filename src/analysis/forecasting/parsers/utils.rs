use anofox_forecast::models::exponential::SeasonalType;
use anofox_forecast::models::theta::DecompositionType;
use anofox_forecast::models::{SeasonalForecastMethod, TrendForecastMethod};

pub fn parse_trend_forecast_method(method: &str) -> Option<TrendForecastMethod> {
    match method {
        "linear" => Some(TrendForecastMethod::Linear),
        "AutoETS" => Some(TrendForecastMethod::AutoETS),
        "SES" => Some(TrendForecastMethod::SES),
        "Naive" => Some(TrendForecastMethod::Naive),
        _ => None,
    }
}

pub fn parse_decomposition_type(input: &str) -> Option<DecompositionType> {
    match input {
        "additive" => Some(DecompositionType::Additive),
        "multiplicative" => Some(DecompositionType::Multiplicative),
        _ => None,
    }
}

pub fn parse_seasonal_forecast_method(input: &str) -> Option<SeasonalForecastMethod> {
    match input {
        "Naive" => Some(SeasonalForecastMethod::Naive),
        "Average" => Some(SeasonalForecastMethod::Average),
        _ => None,
    }
}

pub fn parse_seasonal_type(input: &str) -> Option<SeasonalType> {
    match input {
        "additive" => Some(SeasonalType::Additive),
        "multiplicative" => Some(SeasonalType::Multiplicative),
        _ => None,
    }
}
