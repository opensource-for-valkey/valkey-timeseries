mod utils;
mod model_parser;
mod model_spec_parser;

pub use model_parser::*;
pub use model_spec_parser::parse_model_specs;
pub use utils::*;

#[derive(Debug, Clone, PartialEq, Eq, strum::EnumString, strum::Display)]
pub enum ForecastModelKind {
    ARIMA,
    AutoARIMA,
    SARIMA,
    TBATS,
    AutoTBATS,
    Theta,
    MSTL,
    MFLES,
    Naive,
    ETS,
    AutoETS,
}