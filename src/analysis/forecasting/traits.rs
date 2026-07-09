use anofox_forecast::core::{Forecast, TimeSeries};
use anofox_forecast::models::{BoxedForecaster, FittedParams, Forecaster};
use anofox_forecast::transform::{InverseMode, Transform};
// for some reason, Forecaster is not implemented for Box<dyn Forecaster> in the anofox_forecast crate, so we need to re-export it here
use anofox_forecast::Result as ForecastResult;

pub struct DynForecaster(BoxedForecaster);

impl DynForecaster {
    pub fn new(forecaster: BoxedForecaster) -> Self {
        Self(forecaster)
    }

    pub fn into_inner(self) -> BoxedForecaster {
        self.0
    }
}

impl From<BoxedForecaster> for DynForecaster {
    fn from(forecaster: BoxedForecaster) -> Self {
        Self(forecaster)
    }
}

impl From<DynForecaster> for BoxedForecaster {
    fn from(forecaster: DynForecaster) -> Self {
        forecaster.0
    }
}

impl std::ops::Deref for DynForecaster {
    type Target = BoxedForecaster;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for DynForecaster {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Forecaster for DynForecaster {
    fn fit(&mut self, series: &TimeSeries) -> anofox_forecast::Result<()> {
        self.0.fit(series)
    }

    fn predict(&self, horizon: usize) -> anofox_forecast::Result<Forecast> {
        self.0.predict(horizon)
    }

    fn predict_with_intervals(
        &self,
        horizon: usize,
        level: f64,
    ) -> anofox_forecast::Result<Forecast> {
        self.0.predict_with_intervals(horizon, level)
    }

    fn fit_predict(
        &mut self,
        series: &TimeSeries,
        horizon: usize,
    ) -> anofox_forecast::Result<Forecast> {
        self.0.fit_predict(series, horizon)
    }

    fn fit_predict_with_intervals(
        &mut self,
        series: &TimeSeries,
        horizon: usize,
        level: f64,
    ) -> anofox_forecast::Result<Forecast> {
        self.0.fit_predict_with_intervals(series, horizon, level)
    }

    fn fitted_values(&self) -> Option<&[f64]> {
        self.0.fitted_values()
    }

    fn fitted_values_with_intervals(&self, level: f64) -> Option<Forecast> {
        self.0.fitted_values_with_intervals(level)
    }

    fn residuals(&self) -> Option<&[f64]> {
        self.0.residuals()
    }

    fn trend_component(&self) -> anofox_forecast::Result<&[f64]> {
        self.0.trend_component()
    }

    fn seasonal_component(&self) -> anofox_forecast::Result<&[f64]> {
        self.0.seasonal_component()
    }

    fn residual_component(&self) -> anofox_forecast::Result<Vec<f64>> {
        self.0.residual_component()
    }

    fn training_values(&self) -> anofox_forecast::Result<&[f64]> {
        self.0.training_values()
    }

    fn training_regressors(&self) -> Option<&std::collections::HashMap<String, Vec<f64>>> {
        self.0.training_regressors()
    }

    fn name(&self) -> &str {
        self.0.name()
    }

    fn is_fitted(&self) -> bool {
        self.0.is_fitted()
    }

    fn fitted_params(&self) -> Option<FittedParams> {
        self.0.fitted_params()
    }

    fn supports_exog(&self) -> bool {
        self.0.supports_exog()
    }

    fn has_exog(&self) -> bool {
        self.0.has_exog()
    }

    fn exog_names(&self) -> Option<&[String]> {
        self.0.exog_names()
    }

    fn exog_coefficients(&self) -> Option<&anofox_forecast::utils::OLSResult> {
        self.0.exog_coefficients()
    }

    fn predict_with_exog(
        &self,
        horizon: usize,
        future_regressors: &std::collections::HashMap<String, Vec<f64>>,
    ) -> anofox_forecast::Result<Forecast> {
        self.0.predict_with_exog(horizon, future_regressors)
    }

    fn predict_with_exog_intervals(
        &self,
        horizon: usize,
        future_regressors: &std::collections::HashMap<String, Vec<f64>>,
        level: f64,
    ) -> anofox_forecast::Result<Forecast> {
        self.0
            .predict_with_exog_intervals(horizon, future_regressors, level)
    }
}

#[derive(Debug)]
pub struct DynTransform(Box<dyn Transform>);

impl DynTransform {
    pub fn new(transform: Box<dyn Transform>) -> Self {
        Self(transform)
    }

    pub fn into_inner(self) -> Box<dyn Transform> {
        self.0
    }
}

impl From<Box<dyn Transform>> for DynTransform {
    fn from(transform: Box<dyn Transform>) -> Self {
        Self(transform)
    }
}

impl From<DynTransform> for Box<dyn Transform> {
    fn from(transform: DynTransform) -> Self {
        transform.0
    }
}

impl std::ops::Deref for DynTransform {
    type Target = Box<dyn Transform>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for DynTransform {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Transform for DynTransform {
    fn fit_transform(&mut self, values: &[f64]) -> ForecastResult<Vec<f64>> {
        self.0.fit_transform(values)
    }

    fn inverse(&self, values: &[f64], mode: InverseMode) -> ForecastResult<Vec<f64>> {
        self.0.inverse(values, mode)
    }

    fn offset(&self) -> usize {
        self.0.offset()
    }

    fn name(&self) -> &str {
        self.0.name()
    }

    fn clone_box(&self) -> Box<dyn Transform> {
        self.0.clone_box()
    }
}
