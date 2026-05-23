# TS.OUTLIERS

Detect outliers in a time series.

## Syntax

```
TS.OUTLIERS key fromTimestamp toTimestamp
  METHOD method [method-options]
  [OUTPUT <FULL | SIMPLE | CLEANED>]
  [DIRECTION <POSITIVE | NEGATIVE | BOTH>]
  [SEASONALITY period1 [period2 [period3 [period4]]]]
```

[Examples](#examples)

## Required arguments

<details open>
<summary><code>key</code></summary>

Key name for the time series.
</details>

<details open>
<summary><code>fromTimestamp</code></summary>

Start timestamp for the range query (inclusive).

Use `-` to denote the earliest timestamp in the series.
</details>

<details open>
<summary><code>toTimestamp</code></summary>

End timestamp for the range query (inclusive).

Use `+` to denote the latest timestamp in the series.
</details>

## Optional arguments

<details open>
<summary><code>OUTPUT</code></summary>

Controls the output format. One of:

* `SIMPLE` (default) - Returns only the detected outliers
* `FULL` - Returns all samples with scores, plus anomaly metadata
* `CLEANED` - Returns only normal samples (excluding outliers)

</details>

<details open>
<summary><code>DIRECTION</code></summary>

Filter anomalies by direction. One of:

* `BOTH` (default) - Detect outliers in both directions
* `POSITIVE` - Detect only positive outliers (spikes above normal)
* `NEGATIVE` - Detect only negative outliers (dips below normal)

</details>

<details open>
<summary><code>SEASONALITY</code></summary>

Adjust for seasonal patterns before outliers detection. If specified, the method will decompose the time series to
remove
seasonal components based on the provided periods and perform detection on the residuals.

```
SEASONALITY [AUTO | period1 ...]
```

* `AUTO` - Automatically detects the dominant seasonality period using periodograms. Suitable for data with unknown or
  complex seasonal patterns.
* For a single period: Uses STL (Seasonal-Trend decomposition using LOESS)
* For multiple periods: Uses MSTL (Multiple Seasonal-Trend decomposition using LOESS)
* Periods must be unique positive integers
* Requires at least `2 × max(period)` data points

**Example:** `SEASONALITY 24 168` adjusts for daily (24 hours) and weekly (168 hours) patterns in hourly data.
</details>

<details open>
<summary><code>METHOD</code></summary>

Specifies the outlier detection algorithm. **Required**. The following methods are supported:

#### CUSUM

Cumulative Sum statistical process control method.

```
METHOD CUSUM
```

Detects shifts in the mean by accumulating deviations from the target. No additional options.

---

#### EWMA

Exponentially Weighted Moving Average.

```
METHOD EWMA [ALPHA alpha]
```

**Options:**

* `ALPHA` - Smoothing factor (0 < alpha ≤ 1). Default: `0.3`. Higher values give more weight to recent observations.

---

#### IQR

Interquartile Range method.

```
METHOD IQR [THRESHOLD k]
```

**Options:**

* `THRESHOLD` - IQR multiplier for fence boundaries. Default: `1.5`. Values beyond `Q1 - k×IQR` or `Q3 + k×IQR` are
  outliers.

---

#### ZSCORE

Standard Z-score method.

```
METHOD ZSCORE [THRESHOLD threshold]
```

**Options:**

* `THRESHOLD` - Number of standard deviations from mean. Default: `3.0`. Values beyond `mean ± threshold × σ` are
  outliers.

---

#### MODIFIED-ZSCORE

Modified Z-score using Median Absolute Deviation (MAD).

```
METHOD MODIFIED-ZSCORE [THRESHOLD threshold]
```

**Options:**

* `THRESHOLD` - Modified Z-score threshold. Default: `3.5`. More robust to outliers than standard Z-score.

Uses `0.6745 × (value - median) / MAD` for scoring.

---

#### SMOOTHED-ZSCORE

Smoothed Z-score with adaptive filtering.

```
METHOD SMOOTHED-ZSCORE [THRESHOLD threshold] [LAG lag] [INFLUENCE influence]
```

**Options:**

* `THRESHOLD` - Z-score threshold. Default: `3.0`
* `LAG` - Window size for moving average. Default: `5`
* `INFLUENCE` - Weight of anomalies in calculations (0-1). Default: `0.5`. Lower values reduce the anomaly's impact on
  subsequent detection.

---

#### MAD

Median Absolute Deviation.

```
METHOD MAD [ESTIMATOR estimator] [THRESHOLD k]
```

**Options:**

* `ESTIMATOR` - MAD calculation method: `SIMPLE`, `HARRELL-DAVIS`, or `INVARIANT`. Default: `INVARIANT`
* `THRESHOLD` - MAD multiplier. Default: `3.0`

---

#### DOUBLE-MAD

Double MAD for asymmetric distributions.

```
METHOD DOUBLE-MAD [ESTIMATOR estimator] [THRESHOLD k]
```

**Options:**

* `ESTIMATOR` - MAD calculation method: `SIMPLE`, `HARRELL-DAVIS`, or `INVARIANT`. Default: `INVARIANT`
* `THRESHOLD` - MAD multiplier. Default: `3.0`

Separately analyzes values above and below the median for better handling of skewed data.

---

#### RCF

Random Cut Forest algorithm (AWS implementation).

```
METHOD RCF [NUM_TREES trees]
           [SAMPLE_SIZE size] 
           [THRESHOLD|CONTAMINATION threshold]
           [SHINGLE_SIZE shingle]
           [OUTPUT_AFTER warmup]
           [DECAY decay]
```

**Options:**

* `NUM_TREES` - Number of trees in forest. Default: `100`
* `SAMPLE_SIZE` - Sample size per tree. Default: `256`
* `THRESHOLD` - Anomaly score threshold in standard deviations. Default: `3.0`
* `CONTAMINATION` - The amount of contamination in the data set, i.e., the proportion of outliers in the data set.
* `SHINGLE_SIZE` - Sliding window size. Default: `1`. Use values > 1 for contextual anomalies.
* `OUTPUT_AFTER` - Warmup period (samples). Default: `32`
* `DECAY` - Time decay factor (0-1). Default: `0.1`. Controls how quickly old data is forgotten.

Either `THRESHOLD` or `CONTAMINATION` must be specified to determine the cutoff for anomaly scores. `THRESHOLD` sets a
fixed score
threshold, while `CONTAMINATION` determines the threshold based on the expected proportion of anomalies in the data.

</details>

## Return value

Returns anomaly information based on the `OUTPUT` format:

#### SIMPLE format

**Array reply:** Each anomaly as `[timestamp, value, signal, score]`

* `timestamp` - Sample timestamp (integer)
* `value` - Sample value (float)
* `signal` - Anomaly direction: `1` (positive) or `-1` (negative)
* `score` - Anomaly score (0.0-1.0, higher = stronger anomaly)

#### FULL format

**Map reply** with keys:

* `method` - Detection method name (string)
* `direction` - Detection direction: `positive`, `negative`, or `both` (string)
* `samples` - Array of sample tuples: `[[timestamp, value, score, signal], ...]`
    * `timestamp` - Sample timestamp (integer)
    * `value` - Original sample value (double)
    * `score` - Calculated anomaly score (0.0-1.0)
    * `signal` - Deviation direction (`-1`, `0`, `1`)
* `parameters` - A map of the parameters used for the detection method.
* `seasonality` - (optional) A map describing the seasonality parameters used.
* `method_info` - Algorithm-specific metadata (map, if available):
    * For IQR: `lower_fence`, `upper_fence`
    * For SPC methods (CUSUM, EWMA): `control_limits`, `center_line`

#### CLEANED format

**Array reply:** Cleaned samples only, as `[[timestamp, value], ...]`

## Complexity

O(N × M) where:

* N = number of samples in the range
* M = method complexity factor (varies by algorithm)

**Additional complexity for SEASONALITY:**

* O(N × P × I) where P = number of periods, I = decomposition iterations (default: 2)

## Examples

<details open>
<summary><b>Detect outliers using Modified Z-score</b></summary>

Detect outliers in temperature sensor data with the default threshold:

```valkey-cli
127.0.0.1:6379> TS.OUTLIERS temperature:sensor1 - + METHOD MODIFIED-ZSCORE
1) 1) (integer) 1609459200000
   2) "45.7"
   3) (integer) 1
   4) "0.87"
2) 1) (integer) 1609545600000
   2) "-5.2"
   3) (integer) -1
   4) "0.92"
```

</details>

<details open>
<summary><b>Detect with daily seasonality adjustment</b></summary>

Analyze sales data with 24-hour seasonality:

```valkey-cli
127.0.0.1:6379> TS.OUTLIERS sales:daily 1640000000000 1650000000000 SEASONALITY 24 METHOD ZSCORE THRESHOLD 3.0
1) 1) (integer) 1641234567000
   2) "1547.8"
   3) (integer) 1
   4) "0.93"
```

</details>

<details open>
<summary><b>Detect only positive spikes with full output</b></summary>

Get detailed analysis with metadata for API request spikes:

```valkey-cli
127.0.0.1:6379> TS.OUTLIERS requests:api - + OUTPUT FULL DIRECTION POSITIVE METHOD IQR THRESHOLD 1.5
1) "method"
2) "InterquartileRange"
3) "direction"
4) "positive"
5) "samples"
6) 1) 1) (integer) 1609459200000
      2) "125.4"
      3) "0.15"
      4) (integer) 0
   2) 1) (integer) 1609462800000
      2) "135.2"
      3) "0.18"
      4) (integer) 1
   ...
7) "parameters"
8) 1) "threshold"
    2) "1.5"
9) "method_info"
10) 1) "lower_fence"
    2) "-50.3"
    3) "upper_fence"
    4) "250.7"
```

</details>

<details open>
<summary><b>Multiple seasonality with trend adjustment</b></summary>

Handle both daily and weekly patterns in hourly traffic data:

```valkey-cli
127.0.0.1:6379> TS.OUTLIERS traffic:hourly - + SEASONALITY 24 168 METHOD MODIFIED-ZSCORE THRESHOLD 3.5
1) 1) (integer) 1641234567000
   2) "8547.2"
   3) (integer) 1
   4) "0.89"
```

</details>

<details open>
<summary><b>Get cleaned data (anomalies removed)</b></summary>

Extract normal operating data with anomalies removed:

```valkey-cli
127.0.0.1:6379> TS.OUTLIERS cpu:usage - + OUTPUT CLEANED DIRECTION BOTH METHOD EWMA ALPHA 0.3
1) 1) (integer) 1609459200000
   2) "45.2"
2) 1) (integer) 1609462800000
   2) "47.8"
...
```

</details>

<details open>
<summary><b>Random Cut Forest for contextual anomalies</b></summary>

Detect pattern-based anomalies using a sliding window:

```valkey-cli
127.0.0.1:6379> TS.OUTLIERS metrics:response_time - + METHOD RCF NUM_TREES 150 SHINGLE_SIZE 8 THRESHOLD 3.0
1) 1) (integer) 1641234567000
   2) "2547.3"
   3) (integer) 1
   4) "0.88"
```

</details>

## Notes

* **Minimum data requirements:**
    * At least 3 data points required for analysis
    * For `SEASONALITY`: requires at least `2 × max(period)` samples
* **Seasonality constraints:**
    * Maximum 4 periods allowed
    * Periods must be unique positive integers
    * Automatically sorted in ascending order
* **Score normalization:**
    * All anomaly scores normalized to [0.0, 1.0] range
    * Higher scores indicate stronger anomalies
* **Timestamp preservation:**
    * Output timestamps match original series timestamps
* **Performance tips:**
    * Use `DIRECTION` to filter results when only interested in one type of anomaly
    * Single seasonality periods are faster than multiple
    * RCF is more computationally expensive but better for complex patterns

## See also

TS.RANGE | TS.MRANGE