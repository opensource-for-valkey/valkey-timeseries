mod model_parser;
mod model_spec_parser;
mod utils;

pub use model_parser::*;
pub use utils::*;

#[derive(Debug, Clone, PartialEq, Eq, strum::EnumString, strum::Display)]
#[strum(ascii_case_insensitive)]
pub enum ForecastModelKind {
    Adida,
    Arima,
    AutoArima,
    Croston,
    Garch,
    Holt,
    Imapa,
    Sarima,
    SeasonalEs,
    SeasonalNaive,
    Ses,
    Sma,
    Tbats,
    AutoTbats,
    Theta,
    Tsb,
    Mstl,
    Mfles,
    Naive,
    Ets,
    AutoEts,
    HoltWinters,
}

impl ForecastModelKind {
    pub fn is_seasonal(&self) -> bool {
        matches!(
            self,
            Self::Croston
                | Self::SeasonalEs
                | Self::SeasonalNaive
                | Self::Tbats
                | Self::AutoTbats
                | Self::Theta
                | Self::Tsb
                | Self::Mstl
                | Self::Mfles
                | Self::Ets
                | Self::AutoEts
                | Self::HoltWinters
        )
    }

    pub fn is_multi_seasonal(&self) -> bool {
        matches!(
            self,
            Self::Tbats | Self::AutoTbats | Self::Mstl | Self::Mfles
        )
    }
}