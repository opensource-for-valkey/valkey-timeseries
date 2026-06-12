# TS.AUTOFORECAST

Automatically select and fit the best forecasting model for a time series, returning predicted future values.

`TS.AUTOFORECAST` evaluates all enabled auto-forecasting model families (AutoARIMA, AutoETS, AutoTheta) and selects the
best model based on cross-validation error. The command returns the predicted values, model selection metadata, and
optionally prediction intervals and the ability to persist forecasts into a new or existing time series key.

## Syntax

```
TS.AUTOFORECAST key fromTimestamp toTimestamp
  HORIZON horizon
  [SEASONALITY period]
  [MODELS family1[,family2 ...]]
  [LEVEL confidence_level]
  [METRICS]
  [STORE destination]
```

[Examples](#examples)

## Required arguments

<details open>
<summary><code>key</code></summary>

Key name for the time series to forecast.
</details>

<details open>
<summary><code>fromTimestamp</code></summary>

Start timestamp for the range of data used to fit the model (inclusive).

Use `-` to denote the earliest timestamp in the series.
</details>

<details open>
<summary><code>toTimestamp</code></summary>

End timestamp for the range of data used to fit the model (inclusive).

Use `+` to denote the latest timestamp in the series.
</details>

<details open>
<summary><code>HORIZON horizon</code></summary>

Number of future data points to predict. Must be a positive integer.

</details>

## Optional arguments

<details open>
<summary><code>SEASONALITY period</code></summary>

The seasonal period of the data (number of observations per seasonal cycle). For example:

- `24` for hourly data with daily seasonality
- `7` for daily data with weekly seasonality

When provided, the forecasting models will account for seasonal patterns in the data.
</details>

<details open>
<summary><code>MODELS family1[,family2 ...]</code></summary>

Comma-separated list of model families to evaluate. Supported values (case-insensitive):

| Value | Aliases | Description |
|-------|---------|-------------|
| `ARIMA` | `AUTOARIMA` | Auto-selected ARIMA/SARIMA model |
| `ETS` | `AUTOETS` | Automatic exponential smoothing |
| `THETA` | `AUTOTHETA` | Theta method for forecasting |

If omitted, all three model families are enabled by default. At least one valid model must be specified.
</details>

<details open>
<summary><code>LEVEL confidence_level</code></summary>

Confidence level for prediction intervals, as a percentage between `0` and `100` (exclusive). For example, `LEVEL 95`
returns 95% prediction intervals.

When specified, the response includes:

- `level` — the confidence level
- `lower_interval` — array of lower bounds for each forecast point
- `upper_interval` — array of upper bounds for each forecast point

For each point `i`, `lower_interval[i] <= forecast[i] <= upper_interval[i]`.
</details>

<details open>
<summary><code>METRICS</code></summary>

When specified, the response includes a `metrics` map computed with
`anofox-forecast`'s `calculate_metrics` using in-sample observed values and
fitted values from the selected model.

Returned fields:

- `mae`
- `mse`
- `rmse`
- `mape` (may be `null` when actual contains zeros)
- `smape`
- `mase` (may be `null` when insufficient scaling history)
- `r_squared`

</details>

<details open>
<summary><code>STORE destination</code></summary>

Persist the forecast values into a time series key. The predicted values are stored as samples with timestamps continuing
from the last observed timestamp using the series' median sampling interval.

- If the destination key does not exist, a new time series is created.
- If the destination key already exists, the forecast samples are merged into it.
- When the source series has fewer than 2 timestamps or no positive time intervals, `STORE` is skipped with a warning.
</details>

## Return Value

The response is a flat key-value map (array of alternating keys and values) with the following fields:

| Field | Type | Always Present | Description |
|-------|------|----------------|-------------|
| `selected_model` | string | Yes | Name of the best model selected (`ARIMA`, `SARIMA`, `ETS`, or `Theta`) |
| `horizon` | string (int) | Yes | Number of forecast points |
| `forecast` | array of double | Yes | Predicted values in order |
| `level` | double | No | Confidence level (only when `LEVEL` is specified) |
| `lower_interval` | array of double | No | Lower prediction interval bounds |
| `upper_interval` | array of double | No | Upper prediction interval bounds |
| `metrics` | map | No | Accuracy metrics map (only when `METRICS` is specified) |

### Example Response

```
1) "selected_model"
2) "ARIMA"
3) "horizon"
4) "5"
5) "forecast"
6) 1) "105.32"
   2) "105.78"
   3) "106.24"
   4) "106.70"
   5) "107.16"
7) "level"
8) "95"
9) "lower_interval"
10) 1) "103.50"
    2) "103.12"
    3) "102.75"
    4) "102.38"
    5) "102.01"
11) "upper_interval"
12) 1) "107.14"
    2) "108.44"
    3) "109.73"
    4) "111.02"
    5) "112.31"
```

## Model Selection

`TS.AUTOFORECAST` evaluates each enabled model family by fitting multiple candidate models and comparing their
cross-validation performance. The model with the lowest error is selected to produce the final forecast.

- **AutoARIMA**: Automatically determines the optimal ARIMA (p,d,q) or SARIMA (P,D,Q,m) parameters.
- **AutoETS**: Automatically selects the best ETS (Error-Trend-Seasonality) model.
- **AutoTheta**: Fits the Theta method, which decomposes the series into short-term and long-term components.

The returned model name reflects the concrete model variant:

| Internal Name | Returned Name |
|---------------|---------------|
| `AutoARIMA` | `ARIMA` |
| `AutoARIMA (SARIMA)` | `SARIMA` |
| `AutoETS` | `ETS` |
| `AutoTheta` | `Theta` |

## Examples

### Basic Forecast

Predict the next 5 data points:

```
TS.AUTOFORECAST temperature:sensor1 - + HORIZON 5
```

### With Prediction Intervals

Predict 10 points with 95% confidence intervals:

```
TS.AUTOFORECAST temperature:sensor1 - + HORIZON 10 LEVEL 95
```

### With Accuracy Metrics

Include in-sample accuracy metrics for the selected model:

```
TS.AUTOFORECAST temperature:sensor1 - + HORIZON 10 METRICS
```

### With Specific Model

Use only the ARIMA model family:

```
TS.AUTOFORECAST temperature:sensor1 - + HORIZON 5 MODELS ARIMA
```

### With Seasonality

Specify hourly data with daily seasonality (period=24):

```
TS.AUTOFORECAST temperature:sensor1 - + HORIZON 24 SEASONALITY 24 MODELS ARIMA,ETS
```

### Store Forecast to a Key

Predict 5 points and persist them to a destination key:

```
TS.AUTOFORECAST temperature:sensor1 - + HORIZON 5 STORE temperature:sensor1:forecast
```

### Forecast on a Subset of Data

Use only the last 30 days of data:

```
TS.AUTOFORECAST temperature:sensor1 30d + HORIZON 7
```

## Error Responses

- `ERR wrong number of arguments` — Missing required arguments (minimum 5 arguments required).
- `TSDB: HORIZON is required` — The `HORIZON` argument was not provided.
- `TSDB: HORIZON must be greater than 0` — `HORIZON` value is zero or negative.
- `TSDB: the key does not exist` — The specified time series key was not found.
- `TSDB: LEVEL must be between 0 and 100` — The confidence level is out of range.
- `TSDB: unknown auto-forecast model` — An unrecognized model family was specified in `MODELS`.
- `TSDB: at least one valid model must be specified in MODELS` — The `MODELS` argument was empty.
- `TSDB: Missing value for MODELS` — The `MODELS` argument was given without a value.
- `TSDB: Missing value for STORE` — The `STORE` argument was given without a key name.
- `TSDB: Unknown argument` — An unrecognized optional argument was provided.
- `TSDB: Failed to prepare time series for forecasting` — Internal error converting series data.
- `TSDB: forecast error` — The forecasting model failed to fit or predict.

## Complexity

Depends on the number of enabled model families and the size of the input time series. Each model family performs a
hyperparameter search proportional to the number of candidate models evaluated.

Forecasting is executed on a background thread to avoid blocking the server.

## ACL Categories

`read write timeseries`
