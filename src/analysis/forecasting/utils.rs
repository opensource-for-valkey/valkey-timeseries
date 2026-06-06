use crate::common::Sample;
use crate::error::TsdbError;
use anofox_forecast::{ForecastError, core::TimeSeries as ForecastTimeSeries};
use chrono::{DateTime, Utc};

pub fn make_forecast_time_series(
    iter: impl Iterator<Item = Sample>,
) -> Result<ForecastTimeSeries, TsdbError> {
    let initial_capacity = iter.size_hint().0;
    let mut timestamps = Vec::with_capacity(initial_capacity);
    let mut values = Vec::with_capacity(initial_capacity);
    for sample in iter {
        let dt: DateTime<Utc> =
            DateTime::from_timestamp_millis(sample.timestamp).ok_or_else(|| {
                TsdbError::ForecastError(format!(
                    "Invalid timestamp {} in sample. ",
                    sample.timestamp
                ))
            })?;

        timestamps.push(dt);
        values.push(sample.value);
    }
    ForecastTimeSeries::univariate(timestamps, values).map_err(|e| e.into())
}

impl From<ForecastError> for TsdbError {
    fn from(err: ForecastError) -> Self {
        TsdbError::ForecastError(err.to_string())
    }
}

pub fn normalize_model_name(selected_model: &str) -> &str {
    match selected_model {
        "AutoARIMA (SARIMA)" => "SARIMA",
        "AutoARIMA" => "ARIMA",
        "AutoTheta" => "Theta",
        "AutoETS" => "ETS",
        other => other, // fallback to original name if it doesn't match known models
    }
}
