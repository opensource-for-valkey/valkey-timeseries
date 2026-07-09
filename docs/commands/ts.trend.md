# TS.TREND

Fit trend components to a time series, with optional automatic model selection.

`TS.TREND` can operate in two modes:

- **Auto mode** (default): Fits multiple candidate trend models (Linear, Quadratic,
  Exponential, TheilSen, PiecewiseLinear) and selects the best one using an information
  criterion (AICc by default).
- **Specific model mode**: Fits a single specified trend model (Exponential, Logistic,
  Polynomial, or TheilSen).

The fitted trend values, optional predictions, model features, and optional accuracy metrics are returned.

## Syntax

```
TS.TREND key fromTimestamp toTimestamp
  [MODEL <Exponential|Logistic|Polynomial|TheilSen|Auto> [AICc|BIC|HOLDOUT]]
  [RECENCY <FULL|WINDOW n|FRACTION f|AUTO>]
  [PREDICT <horizon>]
  [FEATURES]
  [METRICS]
  [STORE <destination>]
```

[Examples](#examples)

## Required arguments

<details open>
<summary><code>key</code></summary>

Key name for the time series to analyze.
</details>

<details open>
<summary><code>fromTimestamp</code></summary>

Start timestamp for the range of data to analyze (inclusive).

Use `-` to denote the earliest timestamp in the series.
</details>

<details open>
<summary><code>toTimestamp</code></summary>

End timestamp for the range of data to analyze (inclusive).

Use `+` to denote the latest timestamp in the series.
</details>

## Optional arguments

<details open>
<summary><code>MODEL</code></summary>

Trend model to fit. If omitted, `Auto` is assumed.

* `Auto` (default) — Fit multiple candidate trend components and select the best one
  using an information criterion. Optionally followed by a criterion name:
  `AICc` (default), `BIC`, or `HOLDOUT`.
* `Exponential` — Fit an exponential trend: `y = exp(a + b*t)`.
* `Logistic` — Fit a logistic (S-curve) trend: `y = K / (1 + exp(-steepness * (t - midpoint)))`.
* `Polynomial` — Fit a quadratic polynomial trend (degree 2).
* `TheilSen` — Fit a robust linear trend using the Theil-Sen estimator (median of pairwise slopes).

When `MODEL` is `Auto`, the response includes `model`, `criterion`, and `scores`.
When a specific model is given, the response includes `model` but omits `criterion` and `scores`.

</details>

<details open>
<summary><code>RECENCY</code></summary>

Controls which portion of the data is used for fitting the trend. For forecasting,
the most recent trend is usually what matters.

* `FULL` — Use all data.
* `WINDOW n` — Use only the last `n` observations (minimum 4).
* `FRACTION f` (default: `0.3`) — Use the last fraction of data (e.g., `0.3` = last 30%).
* `AUTO` — Automatically detect the recency window via changepoint analysis (PELT).

The fitted values still cover the full series (the portion before the recency window
is filled by evaluating the fitted model at earlier indices — backwards extrapolation).

Examples:
- `RECENCY FULL` — fit on all data
- `RECENCY WINDOW 50` — fit on last 50 observations
- `RECENCY FRACTION 0.5` — fit on last 50% of data
- `RECENCY AUTO` — automatically detect recency window

</details>

<details open>
<summary><code>PREDICT</code></summary>

Number of steps ahead to predict the trend.

When specified, the response includes a `predicted_trend` array with `horizon` values.

Example: `PREDICT 10` predicts the trend for the next 10 observations.

</details>

<details open>
<summary><code>FEATURES</code></summary>

When specified, the response includes a `features` map with named attributes of the
fitted trend component. The exact features depend on the selected model.

</details>

<details open>
<summary><code>METRICS</code></summary>

When specified, the response includes a `accuracy_metrics` which measure the magnitude of the deviation over observed values vs. fitted trend values.

Returned metrics:

- `mae` (Mean Absolute Error): Average of absolute differences.
- `mse` (Mean Squared Error): Average of squared differences.
- `rmse` (Root Mean Squared Error): Square root of the mean squared error.
- `mape` (Mean Absolute Percentage Error): Average of absolute percentage differences (may be `null` when actual contains zeros).
- `smape` (Symmetric Mean Absolute Percentage Error): Average of symmetric absolute percentage differences.
- `mase` (Mean Absolute Scaled Error): Average of absolute errors scaled by the in-sample mean absolute error (may be `null` when insufficient data for scaling).
- `r_squared` (Coefficient of Determination): Proportion of variance explained by the model.

</details>

<details open>
<summary><code>STORE destination</code></summary>

Persist the fitted trend values into a time series key. The fitted values are stored with their
original timestamps from the input series.

- If the destination key does not exist, a new time series is created.
- If the destination key already exists, it is overwritten by default. Pass `MERGE` to merge
  the fitted (and optionally predicted) samples into the existing series instead.

When `STORE` is combined with `PREDICT`, the predicted trend values are appended after the fitted
values with timestamps continuing from the last observed timestamp using the series' median
sampling interval. If the series has fewer than two timestamps or no positive time intervals,
predicted values are skipped with a warning (fitted values are still stored).

</details>

## Return

`TS.TREND` returns a map (key-value pairs). The fields depend on whether `MODEL Auto`
(or default) or a specific model was used.

### Auto mode response

- `model` — Name of the selected trend model (e.g., "Linear", "Quadratic", "Exponential", "TheilSen", "PiecewiseLinear").
- `criterion` — The criterion used for selection: `AICc`, `BIC`, or `HOLDOUT`.
- `fitted_trend` — Array of in-sample fitted trend values (same length as the input data).
- `scores` — Array of `[name, score]` pairs for all candidate models, sorted from best to worst. Lower scores are better.
- `n_params` — Number of free parameters in the selected model.

### Specific model response

- `model` — Name of the trend model used (e.g., "Exponential", "Logistic", "Polynomial", "TheilSen").
- `fitted_trend` — Array of in-sample fitted trend values (same length as the input data).
- `n_params` — Number of free parameters in the fitted model.

### Optional response fields (both modes)

If `PREDICT` is specified:
- `predicted_trend` — Array of predicted trend values (length = `horizon`).

If `FEATURES` is specified:
- `features` — Map of named feature values for the fitted component.

If `METRICS` is specified:
- `metrics` — Map of forecast accuracy metrics (`mae`, `mse`, `rmse`, `mape`, `smape`, `mase`, `r_squared`) computed from observed vs fitted values.

### Example response (Auto mode)

```
1) "model"
2) "Linear"
3) "criterion"
4) "AICc"
5) "fitted_trend"
6) 1) (double) 20.1
   2) (double) 20.3
   3) (double) 20.5
   ...
7) "scores"
8) 1) 1) "Linear"
       2) (double) -45.2
    2) 1) "Quadratic"
       2) (double) -43.1
    3) 1) "TheilSen"
       2) (double) -41.8
    ...
9) "n_params"
10) (integer) 2
```

### Example response (specific model)

```
1) "model"
2) "Exponential"
3) "fitted_trend"
4) 1) (double) 20.1
   2) (double) 20.3
   3) (double) 20.5
   ...
5) "n_params"
6) (integer) 2
```

## Examples

Explore the following examples to learn how to get started.

## Select the best trend with AICc (default)

Get the best trend model for a time series using the default AICc criterion.

```
127.0.0.1:6379> TS.CREATE temperature
OK
127.0.0.1:6379> TS.ADD temperature 1000 20.1
(integer) 1000
127.0.0.1:6379> TS.ADD temperature 2000 20.3
(integer) 2000
127.0.0.1:6379> TS.ADD temperature 3000 20.5
(integer) 3000
127.0.0.1:6379> TS.ADD temperature 4000 20.8
(integer) 4000
127.0.0.1:6379> TS.ADD temperature 5000 21.0
(integer) 5000
127.0.0.1:6379> TS.TREND temperature - +
1) "model"
2) "Linear"
3) "criterion"
4) "AICc"
5) "fitted_trend"
6) 1) (double) 20.08
   2) (double) 20.31
   3) (double) 20.54
   4) (double) 20.77
   5) (double) 21.0
7) "scores"
8) 1) 1) "Linear"
       2) (double) -45.2
    2) 1) "Quadratic"
       2) (double) -43.1
    3) 1) "TheilSen"
       2) (double) -41.8
9) "n_params"
10) (integer) 2
```

## Select trend with BIC and predict ahead

Use BIC for selection (via MODEL Auto) and predict the next 5 trend values.

```
127.0.0.1:6379> TS.TREND temperature - + MODEL Auto BIC PREDICT 5
1) "model"
2) "Linear"
3) "criterion"
4) "BIC"
5) "fitted_trend"
6) 1) (double) 20.08
   2) (double) 20.31
   3) (double) 20.54
   4) (double) 20.77
   5) (double) 21.0
7) "scores"
8) 1) 1) "Linear"
       2) (double) -43.8
    2) 1) "Quadratic"
       2) (double) -39.5
    3) 1) "TheilSen"
       2) (double) -38.2
9) "predicted_trend"
10) 1) (double) 21.23
    2) (double) 21.46
    3) (double) 21.69
    4) (double) 21.92
    5) (double) 22.15
11) "n_params"
12) (integer) 2
```

## Use MODEL Auto with holdout criterion

Use auto model selection with the holdout criterion, specified inline after MODEL Auto.

```
127.0.0.1:6379> TS.TREND temperature - + MODEL Auto HOLDOUT
1) "model"
2) "Quadratic"
3) "criterion"
4) "HOLDOUT"
5) "fitted_trend"
6) 1) (double) 20.08
   2) (double) 20.31
   3) (double) 20.54
   4) (double) 20.77
   5) (double) 21.0
7) "scores"
8) 1) 1) "Quadratic"
       2) (double) 0.001
    2) 1) "Linear"
       2) (double) 0.002
9) "n_params"
10) (integer) 2
```

## Fit a specific exponential trend

Fit only an exponential trend model (no auto-selection).

```
127.0.0.1:6379> TS.TREND temperature - + MODEL Exponential
1) "model"
2) "Exponential"
3) "fitted_trend"
4) 1) (double) 20.08
   2) (double) 20.31
   3) (double) 20.54
   4) (double) 20.77
   5) (double) 21.0
5) "n_params"
6) (integer) 2
```

## Fit a specific Theil-Sen trend with prediction

Fit a robust Theil-Sen trend and predict ahead.

```
127.0.0.1:6379> TS.TREND temperature - + MODEL TheilSen PREDICT 5
1) "model"
2) "TheilSen"
3) "fitted_trend"
4) 1) (double) 20.08
   2) (double) 20.31
   3) (double) 20.54
   4) (double) 20.77
   5) (double) 21.0
5) "predicted_trend"
6) 1) (double) 21.23
   2) (double) 21.46
   3) (double) 21.69
   4) (double) 21.92
   5) (double) 22.15
7) "n_params"
8) (integer) 2
```

## Fit on recent data only

Use only the last window of observations for trend fitting.

```
127.0.0.1:6379> TS.TREND temperature - + RECENCY WINDOW 20
```

## Include feature details

Request additional features from the fitted trend component.

```
127.0.0.1:6379> TS.TREND temperature - + FEATURES
```

## Include fitted accuracy metrics

Request accuracy metrics computed from observed vs fitted values.

```
127.0.0.1:6379> TS.TREND temperature - + METRICS
```

## See also

- [`TS.AUTOFORECAST`](ts.autoforcast.md) — Automatic forecasting with model selection.
- [`TS.DECOMPOSE`](ts.decompose.md) — Decompose a series into trend, seasonal, and residual components.
- [`TS.PERIODS`](ts.periods.md) — Detect seasonal periods in a time series.
- [`TS.AUTOCORRELATION`](ts.autocorrelation.md) — Compute autocorrelation statistics.
