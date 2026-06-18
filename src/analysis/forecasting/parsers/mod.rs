mod model_parser;
mod model_spec_parser;
mod utils;

pub use model_parser::*;
pub use utils::*;

#[derive(Debug, Clone, PartialEq, Eq, strum::EnumString, strum::Display)]
pub enum ForecastModelKind {
    Arima,
    AutoArima,
    Sarima,
    Tbats,
    AutoTbats,
    Theta,
    Mstl,
    Mfles,
    Naive,
    Ets,
    AutoEts,
}
