<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

# What are some promql functions added to the promql implementations of other popular prometheus-like databases

The most substantial “PromQL-plus” function set is **VictoriaMetrics’ MetricsQL**. Most other Prometheus-compatible backends—Thanos, Cortex, Grafana Mimir, and many managed vendors—generally aim to run upstream PromQL rather than invent a separate function surface. Their meaningful additions tend to be upstream experimental functions, tenancy/configuration controls, or API/query-engine features rather than new analytical functions.

## VictoriaMetrics / MetricsQL

MetricsQL is intentionally PromQL-compatible but adds a large library of functions that are useful candidates for a Valkey-native time-series query layer.


| Category | Added function examples | What it provides |
| :-- | :-- | :-- |
| Window statistics | `median_over_time`, `mode_over_time`, `geomean_over_time`, `range_over_time`, `distinct_over_time` | Statistics that PromQL lacks: median, mode, geometric mean, max–min range, and number of distinct sample values |
| Threshold/window predicates | `count_eq_over_time`, `count_ne_over_time`, `count_gt_over_time`, `count_le_over_time`; corresponding `share_*_over_time` functions | Count or proportion of samples satisfying a predicate over a window |
| Temporal shape | `increases_over_time`, `decreases_over_time`, `ascent_over_time`, `descent_over_time` | Number or total magnitude of directional changes; practical for gauges and event-like series |
| Point/time selection | `first_over_time`, `last_over_time`, `tfirst_over_time`, `tlast_over_time`, `lag`, `lifetime`, `duration_over_time` | First/last value or timestamp, age of latest sample, observed lifetime, and uptime/presence duration |
| Derivative/integral | `ideriv`, `deriv_fast`, `integrate` | Two-point derivative, cheaper endpoint derivative, and numerical integration of a gauge |
| Multi-output rollups | `rollup`, `rollup_delta`, `rollup_deriv`, `rollup_increase`, `rollup_rate`, `rollup_candlestick`, `aggr_over_time` | Return min/max/avg families, OHLC values, or several selected rollups in one scan |
| Quantiles/histograms | `quantiles_over_time`, `histogram_over_time` | Compute several window quantiles in one evaluation or synthesize a VM-style histogram from gauge samples |
| Robust anomaly detection | `mad_over_time`, `outlier_iqr_over_time`, `hoeffding_bound_lower`, `hoeffding_bound_upper` | Median absolute deviation, IQR-based outlier tests, and statistical bounds |
| Counter compatibility | `increase_prometheus`, `rate_prometheus`, `delta_prometheus`, `changes_prometheus`, `increase_pure` | Choose Prometheus-equivalent behavior or an alternate non-extrapolating/preceding-sample semantic explicitly |
| Label manipulation | `label_set`, `label_del`, `label_keep`, `label_map`, `label_move`, `label_copy`, `label_transform`, `label_uppercase`, `label_lowercase`, `label_match`, `label_mismatch`, `label_graphite_group` | Far richer in-query label engineering than Prometheus’ `label_replace` and `label_join` |
| Cross-series / aggregates | `limitk`, `limit_ratio`, `topk_avg`, `topk_max`, `topk_min`, `topk_median`, `bottomk_*`, `outliers_iqr`, `zscore`, `mad` | Stable selection, whole-range ranking, cross-series outlier detection, and standardized/robust dispersion metrics |

MetricsQL also adds useful non-function language features: `WITH` expressions, a `default` gap-fill binary operator, `if`/`ifnot` presence filters, `limit N` after aggregations, omitted lookbehind ranges for rollups, and `histogram_quantile(phi, buckets, boundsLabel)` to expose lower and upper estimated quantile bounds.[^1_1]

## Functions worth borrowing first

For an implementation focused on production observability rather than feature breadth, I would prioritize additions in this order:

1. **`last_over_time`, `first_over_time`, `present_over_time`, `lag`, `duration_over_time`, and `lifetime`**\
These cover extremely common operational questions: “is it reporting?”, “when did it last report?”, “what was the value at the start of the window?”, and “how long was it continuously observed?”
2. **`range_over_time`, `median_over_time`, `mad_over_time`, and `quantiles_over_time`**\
These are compact, high-value analytic primitives. `range_over_time(x[5m])` avoids spelling out `max_over_time(x[5m]) - min_over_time(x[5m])`; median/MAD are much more robust than average/stddev for bursty telemetry.
3. **`count_gt_over_time` and `share_gt_over_time` families**\
These make SLO-ish and saturation queries direct. For example:

```promql
share_gt_over_time(cpu_utilization_ratio[15m], 0.90)
```

expresses “what fraction of the last 15 minutes was CPU above 90%?” without needing a recording rule that converts each sample into a Boolean time series.
4. **`rollup` and `aggr_over_time`**\
These can amortize decode and scan cost in an in-memory compressed TSDB: read a range once and produce multiple results. For example:

```promql
aggr_over_time(
  ("min_over_time", "max_over_time", "avg_over_time"),
  temperature_celsius[1h]
)
```

This is attractive for a Valkey module because a single range iterator can maintain all requested aggregates.
5. **`outlier_iqr_over_time` and `zscore` / `mad`**\
These give dashboards and alert expressions lightweight anomaly detection without pushing users into a separate ML pipeline. `outlier_iqr_over_time` is particularly simple to explain: it reports a point only when it falls outside the window’s Tukey fences.[^1_1]

## Semantics to treat carefully

The names alone are not the compatibility risk; **evaluation semantics** are.

- VictoriaMetrics deliberately makes `rate`, `increase`, `delta`, and `changes` differ from Prometheus in several cases. In particular, MetricsQL uses the preceding raw sample for some window calculations and does not extrapolate `rate`/`increase` like Prometheus. It provides `*_prometheus` variants where users need Prometheus-style behavior.[^1_1]
- Therefore, if your objective is drop-in PromQL behavior, retain upstream `rate()` / `increase()` semantics exactly and introduce alternatives under explicit names such as `rate_no_extrapolation()` or `rate_precise()`. Do not silently repurpose standard function names.
- Some functions need nontrivial storage/query support:
    - Exact `median_over_time`, `mode_over_time`, `distinct_over_time`, and exact window quantiles need a materialized window or a selectable sketch/heap/order-statistics implementation.
    - `duration_over_time` depends on a clear missing-data model and a `max_interval` gap threshold.
    - `integrate` requires specifying interpolation assumptions and behavior across gaps.
    - Histogram-generating functions must define their bucket schema and cardinality bounds carefully.


## Other Prometheus-like systems

| System | What it adds | Practical implication |
| :-- | :-- | :-- |
| Grafana Mimir | Supports selectively enabling experimental PromQL functions per tenant or globally; the documented examples are `info`, `sort_by_label`, and `sort_by_label_desc` | This is primarily controlled adoption of upstream PromQL evolution, not a separate analytic extension language. The tenant-level allow-list is a useful model if custom functions could affect CPU, cardinality, or information disclosure. [^1_2] |
| Thanos | Primarily PromQL compatibility plus distributed query execution, deduplication, partial-response handling, downsampling, and global querying | The important innovation is query execution and data-source federation, not a distinctive function catalog. |
| Cortex | Primarily PromQL-compatible distributed, multi-tenant storage/querying | Like Mimir’s lineage, extensions are mostly tenancy, limits, and execution architecture rather than language functions. |
| GreptimeDB | Supports a largely standard PromQL subset alongside SQL and its native query facilities | It illustrates a common pattern: keep PromQL for compatibility and put richer relational/time-series analysis in SQL rather than expanding PromQL substantially. [^1_3] |
| ClickHouse’s PromQL layer | PromQL compatibility over ClickHouse storage, with SQL table functions such as `prometheusQuery` and `prometheusQueryRange` | This is another “PromQL as compatibility interface, SQL for extension” design rather than a custom-function-heavy dialect. [^1_4] |

## Suggested extension tiers

For a Valkey timeseries implementation, I would separate the API into three tiers:

- **Strict PromQL mode**: upstream Prometheus syntax and semantics only, especially for `rate`, staleness, vector matching, `NaN`, and label/metric-name behavior.
- **Extensions mode**: explicitly versioned functions such as:

```text
last_over_time
lag
duration_over_time
range_over_time
median_over_time
mad_over_time
count_gt_over_time
share_gt_over_time
quantiles_over_time
aggr_over_time
```

- **Experimental mode**: expensive or potentially cardinality-amplifying functions:

```text
distinct_over_time
count_values_over_time
histogram_over_time
outlier_iqr_over_time
rollup_candlestick
label_map
label_transform
```


That division keeps Prometheus dashboard portability intact while making advanced functions discoverable and operationally governable. Mimir’s feature-gated experimental-function model offers a useful precedent for enabling such capabilities per tenant rather than globally.[^1_2]

<span style="display:none">[^1_10][^1_11][^1_12][^1_13][^1_14][^1_15][^1_16][^1_17][^1_18][^1_19][^1_20][^1_21][^1_22][^1_23][^1_24][^1_25][^1_5][^1_6][^1_7][^1_8][^1_9]</span>

<div align="center">⁂</div>

[^1_1]: https://docs.victoriametrics.com/metricsql/

[^1_2]: https://grafana.com/docs/mimir/latest/configure/configure-experimental-promql-functions/

[^1_3]: https://docs.greptime.com/user-guide/query-data/promql/

[^1_4]: https://clickhouse.com/blog/introducing-promql

[^1_5]: https://help.aliyun.com/en/sls/promql-deep-dive

[^1_6]: https://docs.newrelic.com/docs/infrastructure/prometheus-integrations/view-query-data/supported-promql-features.md

[^1_7]: https://docs.newrelic.com/docs/infrastructure/prometheus-integrations/view-query-data/supported-promql-features/

[^1_8]: https://www.elastic.co/search-labs/blog/promql-elasticsearch-compute-engine

[^1_9]: https://promlabs.com/blog/2021/10/14/promql-vendor-compatibility-round-three/

[^1_10]: https://promlabs.com/blog/2020/11/26/an-update-on-promql-compatibility-across-vendors/

[^1_11]: https://www.tigerdata.com/learn/prometheus-alternatives

[^1_12]: https://tessl.io/registry/pantheon-ai/promql-toolkit/1.0.1/files/generator/references/promql_functions.md

[^1_13]: https://docs.victoriametrics.com/victoriametrics/metricsql/

[^1_14]: https://victoriametrics.com/blog/prometheus-monitoring-function-operator-modifier/

[^1_15]: https://docs.victoriametrics.com/victoriametrics-cloud/exploring-data/exploring-victoriametrics/

[^1_16]: https://docs.victoriametrics.com/victoriametrics/keyconcepts/

[^1_17]: https://victoriametrics.com/products/metricsql/

[^1_18]: https://docs.victoriametrics.com/victoriametrics/faq/

[^1_19]: https://grafana.com/docs/mimir/v3.2.x/configure/configure-experimental-promql-functions/

[^1_20]: https://grafana.com/oss/mimir/

[^1_21]: https://pkg.go.dev/github.com/VictoriaMetrics/metricsql

[^1_22]: https://docs.fly.io/monitoring/metrics/

[^1_23]: https://docs.groundcover.com/use-groundcover/remote-access-and-apis/api-examples/query-metrics

[^1_24]: https://readmex.com/en-US/grafana/mimir/page-609f515cf-de46-4ce6-9939-2a42eb9a19a5

[^1_25]: https://getbunker.net/documentation/guides/mimir


---

# Are there online sources of prometheus proposed new promql functions

Yes. The official Prometheus project exposes both **implemented experimental functions** and the more speculative **proposal/discussion stream** for functions that may never be accepted.

## Official implemented experiments

The best canonical source is Prometheus’s current [query-functions reference](https://prometheus.io/docs/prometheus/latest/querying/functions/). It lists functions gated by:

```text
--enable-feature=promql-experimental-functions
```

At present, that includes:


| Function | Purpose | Implementation implications |
| :-- | :-- | :-- |
| `mad_over_time(v range-vector)` | Median absolute deviation across samples in a range | Requires median/order-statistics work; a robust alternative to standard deviation |
| `ts_of_min_over_time(v range-vector)` | Timestamp of the minimum sample | Requires retaining both extremum value and its timestamp |
| `ts_of_max_over_time(v range-vector)` | Timestamp of the maximum sample | Same scan pattern as min, with defined tie behavior |
| `ts_of_last_over_time(v range-vector)` | Timestamp of the final sample | A low-cost companion to `last_over_time` |
| `sort_by_label(v, label...)` | Ascending natural sort using specified label values | Vector-level sort; no time-series range scan needed |
| `sort_by_label_desc(v, label...)` | Descending natural sort using label values | Same, reversed |
| `info(v, [data-label-selector])` | Enriches series with labels from matching “info” metrics | Requires special vector matching / identifying-label logic |
| `double_exponential_smoothing(v, sf, tf)` | Holt linear / double exponential smoothing | Stateful range traversal; formerly named `holt_winters` |

Prometheus explicitly says experimental functions may have their names, syntax, and semantics changed—or may be removed entirely—so they are strong signals of design direction but weak compatibility commitments.[^2_1][^2_2]

For a Valkey-based implementation, the low-risk candidates are `ts_of_min_over_time`, `ts_of_max_over_time`, `ts_of_last_over_time`, and label sorting. They are mostly direct extensions of an existing rollup iterator. `mad_over_time` is more valuable analytically, but substantially more expensive unless a window is already decoded/materialized or backed by an order-statistics strategy.

## Proposal and discussion sources

The main upstream venue is the Prometheus repository’s [PromQL issue search](https://github.com/prometheus/prometheus/issues?q=is%3Aissue+is%3Aopen+label%3Acomponent%2Fpromql). Look for issues labeled `component/promql`, often also labeled `kind/feature`. This is where maintainers debate syntax, semantics, query-engine cost, compatibility, native-histogram treatment, and whether a feature belongs in PromQL at all.

Concrete examples:


| Proposal | Status indicated by source | Motivation |
| :-- | :-- | :-- |
| [`histogram_observations(lower, upper, v)`](https://github.com/prometheus/prometheus/issues/16067) | Proposal | Estimate/count observations in an interval from a native histogram |
| [`histogram_quantiles(b, φ0, ...)`](https://github.com/prometheus/prometheus/issues/16464) | Open proposal | Return several quantiles from one classic or native histogram query, differentiated by a synthetic label |
| [`first_over_time(v)` and `ts_of_first_over_time(v)`](https://github.com/prometheus/prometheus/issues/16963) | Open proposal | Complete the symmetry with `last_over_time` and timestamp-returning extrema |
| [`default()`](https://github.com/prometheus/prometheus/issues/5806) | Closed proposal | Supply a fallback for absent or undefined data; this has historically been contentious because absence is fundamental to PromQL’s vector model |

The `histogram_quantiles` proposal is particularly relevant for a compressed engine: one range/instant scan over histogram data could produce multiple $\phi$ values, avoiding repeated `histogram_quantile()` evaluations and repeated vector matching. The design question is result shaping—its proposal uses a synthetic `__hist_quantile__` label—and whether the memory/cardinality cost is acceptable.[^2_3]

The `first_over_time` proposal is also a useful compatibility signal. It would pair naturally with `last_over_time`, while `ts_of_first_over_time` would complete the timestamp family alongside upstream experimental `ts_of_min_over_time`, `ts_of_max_over_time`, and `ts_of_last_over_time`.[^2_4]

## How to follow new proposals

Use these sources in descending order of authority:

1. **Prometheus function reference** for shipping semantics and feature-flag status.[^2_1]
2. **Prometheus feature-flags documentation** for stability classification and activation behavior. It specifically warns that experimental function interfaces can change or disappear.[^2_2]
3. **`prometheus/prometheus` issues labeled `component/promql`** for new designs, unresolved semantic questions, and rejected ideas.
4. **Prometheus pull requests touching `promql/functions.go`, `promql/parser`, `promql/engine.go`, and promql tests** for the actual near-term implementation signal.
5. **Release notes and migration documentation** for renamed, promoted, or removed functions. For example, `holt_winters` was renamed to `double_exponential_smoothing` and placed behind the experimental-functions flag.[^2_5]

## A useful implementation watch list

If you are selecting extensions for a PromQL-compatible Valkey TS module, I would watch upstream in four groups:

- **Symmetric range accessors:** `first_over_time`, `ts_of_first_over_time`, and timestamp-producing variants of existing rollups.
- **Robust statistics:** `mad_over_time`, plus potential future median/quantile-window work.
- **Native histogram analytics:** interval-observation and multiple-quantile operations, which are likely to matter more as native histograms become broadly deployed.
- **Metadata joins:** `info()`, where the upstream API still has deliberate limits and semantics likely to evolve.

A sensible compatibility policy is to keep an “upstream stable PromQL” mode, gate upstream experiments independently, and place Valkey-specific functions in a separate namespace or an explicit extension mode. That avoids later incompatibility if an upstream proposal lands with a different name or semantic contract.

<span style="display:none">[^2_10][^2_11][^2_12][^2_13][^2_14][^2_15][^2_6][^2_7][^2_8][^2_9]</span>

<div align="center">⁂</div>

[^2_1]: https://prometheus.io/docs/prometheus/latest/querying/functions/

[^2_2]: https://prometheus.io/docs/prometheus/latest/feature_flags/

[^2_3]: https://github.com/prometheus/prometheus/issues/16464

[^2_4]: https://github.com/prometheus/prometheus/issues/16963

[^2_5]: https://github.com/prometheus/prometheus/blob/main/docs/migration.md

[^2_6]: https://prometheus.io/blog/2025/12/16/introducing-info-function/

[^2_7]: https://prometheus.io/docs/prometheus/3.5/querying/functions/

[^2_8]: https://prometheus.io/docs/prometheus/latest/command-line/promtool/

[^2_9]: https://prometheus.io/docs/prometheus/3.15/command-line/promtool/

[^2_10]: https://prometheus.io/docs/prometheus/3.12/querying/functions/

[^2_11]: https://prometheus.io/docs/prometheus/3.5/command-line/promtool/

[^2_12]: https://prometheus.io/docs/prometheus/3.13/command-line/promtool/

[^2_13]: https://github.com/prometheus/prometheus/issues/16067

[^2_14]: https://github.com/prometheus/prometheus/issues/5806

[^2_15]: https://github.com/prometheus/prometheus/issues/16412


---

# Are there useful analytics functions in other timeseries database that would be useful to implement

Yes. Beyond PromQL extensions, other TSDBs contain several **high-value analytics primitives** that would fit a Valkey TimeSeries implementation well—especially if you expose them as `TS.RANGE` aggregation options, module commands, or an explicit PromQL-extension layer rather than trying to force every one into strict PromQL.

The strongest ideas are: time-weighted gauge analytics, counter summaries that compose across rollups, state-duration tracking, robust/approximate distribution functions, and temporal joins/alignment.

## Best candidates

| Capability | Representative systems | Example API shape | Why it is valuable |
| :-- | :-- | :-- | :-- |
| Time-weighted average and integral | Timescale Toolkit | `time_weight(method, ts, value)` → `average`, `integral` | Correct aggregation for irregularly sampled gauges |
| Counter summary / rollup state | Timescale Toolkit | `counter_agg(ts, value)` → `rate`, `delta`, `num_resets`, `slope` | Mergeable counter state across chunks and downsampling levels |
| Gauge summary / rollup state | Timescale Toolkit | `gauge_agg(ts, value)` → `average`, `integral`, `slope`, `delta` | Avoids incorrectly treating gauges like counters |
| State duration and run lengths | InfluxDB Flux, Timescale Toolkit | `state_duration(v[1h], predicate)` | “How long has CPU been high?” / outage-duration analysis |
| Gap filling and interpolation | TimescaleDB | `time_bucket_gapfill`, `locf`, `interpolate` | Regular dashboards and correct alignment of sparse data |
| Approximate percentiles and distributions | Timescale Toolkit | `uddsketch`, `percentile_agg`, `histogram` | Bounded-memory quantiles for high-cardinality data |
| Change-point / regime helpers | InfluxDB Flux, VictoriaMetrics | `state_tracking`, `changes`, `runs`, `duration` | Alerting and incident analysis of flapping or persistent conditions |
| Seasonal forecasting / smoothing | InfluxDB Flux | `holtWinters`, exponential smoothing | Baselines, trend estimates, and lightweight forecasts |
| Time-aware joins | QuestDB | `ASOF JOIN`, `LATEST ON`, `WINDOW JOIN` | Associate a measurement with the latest known metadata/config/state |
| Financial / OHLC rollups | QuestDB, VictoriaMetrics | `open`, `high`, `low`, `close`, `vwap` | Useful beyond finance: latency, sensor, and event-rate summaries |

Timescale’s Toolkit is unusually instructive because it models computations as aggregate states that can be **rolled up**. Its documented hyperfunctions include counter and gauge aggregates, time-weighted averages/integrals, state tracking, and percentile approximations.[^3_1][^3_2]

## Time-weighted gauge analytics

A normal average of samples is biased whenever sampling intervals differ:

```text
Samples: (0s, 0), (1s, 100), (60s, 100)

Sample mean            = (0 + 100 + 100) / 3 = 66.7
Time-weighted average  ≈ 98.3
```

For a gauge—CPU utilization, temperature, queue depth, memory in use—the second number is normally what an operator means by “average over the last minute.”

A useful API family would be:

```text
TS.TWA key fromTimestamp toTimestamp [METHOD LOCF|LINEAR]
TS.INTEGRAL key fromTimestamp toTimestamp [METHOD LOCF|LINEAR]
TS.AREA key fromTimestamp toTimestamp [METHOD LOCF|LINEAR]
```

Or PromQL extensions:

```promql
time_weighted_avg_over_time(queue_depth[15m])
time_weighted_avg_over_time(queue_depth[15m], "linear")
integral_over_time(power_watts[1h])
```

Two interpolation policies are sufficient initially:

- **LOCF / step-hold**: treat the most recent observed value as holding until the next observation. This is often right for configuration, state, inventory, queue depth, and sampled telemetry.
- **Linear**: interpolate between adjacent points. This is often right for physical sensor data and smoothly varying measurements.

Timescale documents `time_weight()` with both LOCF and linear methods, and exposes both average and integral operations, including rollup-friendly forms.[^3_3][^3_1]

### Implementation note

For a chunk with adjacent points $(t_i, v_i)$, LOCF integration is:

$$
\text{area} = \sum_i v_i (t_{i+1} - t_i)
$$

Linear integration uses trapezoids:

$$
\text{area} = \sum_i \frac{v_i + v_{i+1}}{2}(t_{i+1} - t_i)
$$

Then:

$$
\text{time-weighted average} =
\frac{\text{area}}{t_\text{end} - t_\text{start}}
$$

A mergeable internal summary can retain:

```text
first_timestamp, first_value
last_timestamp, last_value
area_within_chunk
observed_duration
method
```

The merge operation adds intra-chunk areas plus the boundary area between the prior last point and the next first point. That makes it especially compatible with compressed chunks, compaction, and downsampling.

## Counter and gauge summaries

PromQL’s `rate`, `increase`, `delta`, `deriv`, and `resets` are useful but generally re-scan raw samples at query time. Timescale’s `counter_agg()` and `gauge_agg()` model the source data as an intermediate aggregate that supports later accessor functions and rollups. Its counter API includes rate, delta, extrapolated/interpolated rate variants, reset count, number of changes, first/last values and timestamps, slope, and bounds-aware calculations.[^3_1]

A Valkey chunk-level summary could retain:

```text
count
first_ts, first_value
last_ts, last_value
min, max
sum
sum_of_squares
positive_delta_sum
negative_delta_sum
reset_count
change_count
time_delta
```

Then expose operations such as:

```text
TS.SUMMARY key from to TYPE COUNTER
TS.SUMMARY key from to TYPE GAUGE

TS.RATE key from to [COUNTER|GAUGE] [INTERPOLATE]
TS.RESETS key from to
TS.CHANGES key from to
TS.SLOPE key from to
TS.IRATE key from to [SIDE LEFT|RIGHT]
```

This supports both existing PromQL-like calculations and much faster evaluation over downsampled tiers.

The crucial point is to make the type explicit:

- A **counter** is monotonic except for resets; reset detection is semantic.
- A **gauge** is free to rise and fall; negative deltas are real signal rather than resets.

Timescale explicitly distinguishes `gauge_agg()` from `counter_agg()` for exactly this reason.[^3_4]

## State and persistence analytics

Influx Flux has useful state-oriented functions:

- `stateDuration()` returns the running duration for which a predicate remains true.
- `stateTracking()` can produce both a running consecutive-match count and duration.
- `stateCount()` tracks consecutive samples matching a condition.[^3_5][^3_6][^3_7]

These map extremely well to monitoring.

```promql
# How long has this series remained over the configured threshold?
state_duration_over_time(
  node_memory_utilization_ratio[2h],
  ">",
  0.90
)

# Is the service flapping?
state_runs_over_time(up[30m], 0)
```

A more composable form uses a Boolean/vector predicate:

```promql
state_duration_over_time(
  (node_memory_utilization_ratio > 0.90)[2h]
)
```

Potential functions:


| Function | Result |
| :-- | :-- |
| `state_duration_over_time(predicate-range)` | Duration of current or longest true run |
| `state_count_over_time(predicate-range)` | Samples in the current or longest true run |
| `state_changes_over_time(predicate-range)` | Number of false↔true transitions |
| `state_runs_over_time(predicate-range)` | Number of contiguous true regions |
| `time_above_over_time(v, threshold)` | Total elapsed time above a threshold |
| `share_above_over_time(v, threshold)` | Fraction of observed time above a threshold |

`time_above_over_time` is more meaningful than sample counting for irregular scrapes. Combined with time weighting, it answers operational questions such as:

> “What fraction of the prior 30 minutes was p99 latency above 500 ms?”

rather than merely:

> “What fraction of samples happened to exceed 500 ms?”

## Gap filling and interpolation

Prometheus deliberately treats missing samples as absent/stale rather than automatically filling them. That is correct for alert semantics, but dashboard and analytical queries frequently need controlled gap handling.

Useful explicit primitives:

```text
TS.ALIGN key from to STEP 1m METHOD NONE|LOCF|LINEAR
TS.GAPFILL key from to STEP 1m METHOD NULL|PREVIOUS|LINEAR|CONSTANT value
```

Potential query-language equivalents:

```promql
gapfill(avg_over_time(cpu_usage[1m]), 1m, "previous")
interpolate_over_time(temperature_celsius[15m], 1m, "linear")
```

Do **not** make this implicit in PromQL-compatible endpoints. Instead, make it explicit and preserve:

- the original timestamps,
- a way to distinguish synthetic from stored values,
- a maximum permissible fill interval,
- a documented behavior at query boundaries,
- `NULL`/absence as the default.

This is a natural fit for a `TS.RANGE` option because the server already emits bucketed output. It also makes Grafana-style graph rendering much more predictable without corrupting alert semantics.

## Distribution and quantile analytics

For many samples, an exact median or percentile requires retaining or sorting values. That is expensive for long windows, high-cardinality fanout, or distributed reduction. Timescale provides `uddsketch()` for approximate percentiles with a guaranteed relative error, which is a useful model for a bounded-memory aggregate state.[^3_8]

Worth adding:

```text
TS.PERCENTILE key from to 0.50 [METHOD EXACT|UDDSKETCH]
TS.HISTOGRAM key from to [BUCKETS ... | AUTO]
TS.QUANTILES key from to 0.50 0.90 0.95 0.99
TS.TOPVALUES key from to COUNT n
```

PromQL-style extensions could be:

```promql
quantiles_over_time(latency_ms[10m], 0.50, 0.90, 0.99)
approx_quantile_over_time(request_duration_seconds[1h], 0.99, "uddsketch")
```

Recommended semantics:

- Use **exact** quantiles only for bounded sample counts or a configurable query-memory budget.
- Use mergeable sketches for downsampled or distributed execution.
- Return error bounds / sketch parameters in a metadata-capable API when possible.
- Avoid exposing a silently approximate `quantile_over_time`; make approximation explicit in the name or query option.

For a compression-oriented engine, sketches are compelling because a per-chunk sketch can remain mergeable after compaction. However, they do trade away exact reproducibility and need a carefully versioned serialization format.

## Smoothing, forecast, and anomaly helpers

Flux includes `holtWinters()` and InfluxQL has technical-analysis functions based on Holt-Winters seasonal forecasting. Prometheus has moved toward the less misleading name `double_exponential_smoothing()` for Holt linear smoothing; it is experimental upstream.[^3_6][^3_9][^3_10]

A reasonable initial set:

```promql
double_exponential_smoothing(v[6h], 0.2, 0.1)
ema_over_time(v[30m], 0.2)
mad_over_time(v[1h])
zscore_over_time(v[1h])
```

The most operationally valuable outputs are usually **baseline residuals**, not multi-step forecasts:

```promql
(
  http_request_duration_seconds
  -
  double_exponential_smoothing(http_request_duration_seconds[6h], 0.2, 0.1)
)
/
mad_over_time(http_request_duration_seconds[6h])
```

That can serve as a robust anomaly score when paired with an explicit minimum-sample rule. Still, I would not make forecasting a first-wave storage-module feature: tuning, missing-data behavior, seasonal periods, query CPU, and alerting stability all need a fuller policy.

## Temporal joins and latest state

QuestDB’s SQL extensions are excellent inspiration even if SQL-style joins do not belong in PromQL. `ASOF JOIN` matches each left-side row to the latest right-side row at or before its timestamp; `LATEST ON` returns the newest row per partition; `SAMPLE BY` provides aligned time bucketing.[^3_11][^3_12][^3_13]

Examples:

```sql
SELECT trade.ts, trade.symbol, trade.quantity, quote.bid, quote.ask
FROM trades trade
ASOF JOIN quotes quote ON trade.symbol = quote.symbol;
```

Monitoring analogues include:

- attach the latest deployment SHA, feature-flag value, region, ownership label, or SLO target to each sample;
- align request latency with the latest autoscaling or configuration event;
- enrich a metric with dynamically changing metadata without copying that metadata into every raw sample.

For a module, a constrained primitive is safer than a general distributed join:

```text
TS.LATEST key
TS.MLATEST FILTER label_matchers...
TS.ASOF key timestamp
TS.MASOF timestamp FILTER label_matchers...
```

For cross-series work, define a narrow operation that requires a shared label key and has explicit fanout limits. In cluster mode, this should almost certainly live in a coordinator/client layer unless all relevant series are colocated by a hash tag.

## Recommended roadmap

For your likely goals—Valkey module production maturity, Prometheus interoperability, compressed storage, and cluster-aware operation—I would stage functions as follows.

### Tier 1: High value, tractable

- `first` / `last` / timestamp-of-extrema rollups
- `range`, `change_count`, `reset_count`, `uptime` / `observed_duration`
- `time_above`, `share_above`, and threshold count/share families
- LOCF time-weighted average and integral
- Explicit downsampling buckets with `min`, `max`, `sum`, `count`, `avg`, `first`, `last`
- Exact `median` and `mad` behind hard query-memory/sample limits


### Tier 2: Rollup-aware analytics

- Mergeable counter and gauge summaries
- Boundary-aware `rate`, `increase`, `delta`, and interpolated variants
- Linear time-weighted average/integral
- Gap-fill and resampling with strict maximum gap controls
- Multi-aggregate scans such as `aggr_over_time(min,max,avg,count)`


### Tier 3: Expensive or policy-heavy extensions

- Approximate quantiles using a mergeable sketch such as UddSketch
- State-machine/runs APIs
- Robust cross-series anomaly functions such as IQR outlier detection
- Seasonal smoothing, forecasting, and residual-based anomaly scoring
- Temporal joins or ASOF enrichment

The key architectural choice is to design intermediate aggregate states—counter, gauge, time-weighted, and sketch states—as **mergeable and serializable**. That gives you efficient chunk-level aggregation, background compaction, retention/downsampling tiers, cluster-side reduction, and predictable query cost rather than re-decoding every raw sample on each dashboard query.

<span style="display:none">[^3_14][^3_15][^3_16][^3_17][^3_18][^3_19][^3_20][^3_21][^3_22][^3_23][^3_24][^3_25][^3_26][^3_27][^3_28][^3_29][^3_30][^3_31][^3_32][^3_33][^3_34]</span>

<div align="center">⁂</div>

[^3_1]: https://docs.tigerdata.com/use-timescale/latest/hyperfunctions/about-hyperfunctions/

[^3_2]: https://docs.timescale.com/api/latest/hyperfunctions/state-tracking/state_agg/

[^3_3]: https://pig.pgsty.com/e/timescaledb_toolkit/

[^3_4]: https://docs.timescale.com/api/latest/hyperfunctions/counters-and-gauges/gauge_agg/

[^3_5]: https://docs.influxdata.com/flux/v0/stdlib/universe/statetracking/

[^3_6]: https://docs.influxdata.com/flux/v0/tags/transformations/

[^3_7]: https://docs.influxdata.com/flux/v0/stdlib/universe/stateduration/

[^3_8]: https://docs.tigerdata.com/api/latest/hyperfunctions/percentile-approximation/uddsketch/

[^3_9]: https://docs.influxdata.com/flux/v0/stdlib/universe/holtwinters/

[^3_10]: https://docs.influxdata.com/influxdb/v2/query-data/influxql/functions/technical-analysis/

[^3_11]: https://questdb.com/docs/query/sql/asof-join/

[^3_12]: https://questdb.com/docs/concepts/deep-dive/sql-extensions/

[^3_13]: https://questdb.com/docs/query/sql/sample-by/

[^3_14]: https://docs.influxdata.com/influxdb/cloud/query-data/flux/

[^3_15]: https://docs.influxdata.com/influxdb/v2/query-data/flux/

[^3_16]: https://questdb.com/docs/query/overview/

[^3_17]: https://questdb.com/docs/

[^3_18]: https://questdb.com/docs/query/sql/join/

[^3_19]: https://questdb.com/

[^3_20]: https://questdb.com/docs/concepts/designated-timestamp/

[^3_21]: https://github.com/questdb/questdb/

[^3_22]: https://questdb-questdb-51.mintlify.app/concepts/sql-extensions

[^3_23]: https://docs.maestrohub.com/data-integration/connect/sql/questdb

[^3_24]: https://questdb-questdb-51.mintlify.app/sql/overview

[^3_25]: https://docs.influxdata.com/kapacitor/v1/reference/nodes/query_flux_node/

[^3_26]: https://docs.influxdata.com/influxdb/cloud/query-data/flux/monitor-states/

[^3_27]: https://docs.influxdata.com/influxdb/v2/query-data/flux/monitor-states/

[^3_28]: https://docs.influxdata.com/flux/v0/stdlib/universe/

[^3_29]: https://docs.tigerdata.com/use-timescale/latest/hyperfunctions/

[^3_30]: https://www.perplexity.ai/search/89553dea-e8c4-498e-b760-06b7cac7bbf4

[^3_31]: https://www.perplexity.ai/search/28cf1261-fce5-40e4-986f-b71b687a65ae

[^3_32]: https://www.perplexity.ai/search/ae8eb97e-2204-4c4d-9b6d-06ab5a2f8b9c

[^3_33]: https://www.perplexity.ai/search/0756b405-7f92-43c6-9c6d-812215951434

[^3_34]: https://www.perplexity.ai/search/d2e2b21c-40f2-406c-bebc-7f53a5df8a5b


---

# \<q>time_above_over_time\</q> What would time_above_over_time and share_above_over_time look like (with examples)

`time_above_over_time` should measure **elapsed wall-clock time** that a time series is above a threshold within a range window. `share_above_over_time` should normalize that duration by the eligible window duration, yielding a fraction in $[0,1]$.

They are most useful for irregularly sampled gauges—CPU utilization, queue depth, latency, memory pressure—where “how long was the system unhealthy?” is not the same as “how many samples were unhealthy?”

VictoriaMetrics’ `share_gt_over_time` is related, but its documented semantics are a **fraction of raw samples** satisfying the predicate. A time-weighted variant would instead weight every sample by the duration it represents.[^4_1][^4_2]

## Proposed API

I would expose the generic predicate form eventually, but start with explicit threshold functions:

```promql
time_above_over_time(v range-vector, threshold scalar)
time_below_over_time(v range-vector, threshold scalar)
time_at_or_above_over_time(v range-vector, threshold scalar)
time_at_or_below_over_time(v range-vector, threshold scalar)

share_above_over_time(v range-vector, threshold scalar)
share_below_over_time(v range-vector, threshold scalar)
share_at_or_above_over_time(v range-vector, threshold scalar)
share_at_or_below_over_time(v range-vector, threshold scalar)
```

Example:

```promql
time_above_over_time(node_cpu_utilization_ratio[30m], 0.90)
share_above_over_time(node_cpu_utilization_ratio[30m], 0.90)
```

At an instant query evaluation time $T$, the range is:

$$
[W_s, W_e] = [T - 30\mathrm{m}, T]
$$

The per-series result is:

$$
\operatorname{time\_above}(v, \theta) =
\int_{W_s}^{W_e} \mathbf{1}[v(t) > \theta]\,dt
$$

$$
\operatorname{share\_above}(v, \theta) =
\frac{\operatorname{time\_above}(v, \theta)}
     {\operatorname{eligible\_duration}(v, W_s, W_e)}
$$

The implementation choice hiding inside `eligible_duration` is important: it determines whether a missing series means zero, unknown, or merely an excluded gap.

## Basic semantics

### Type and result

- Input: one range vector plus one scalar threshold.
- Output: an instant vector.
- Evaluation: independently for each input series.
- Metric-name behavior: remove `__name__`, as most PromQL transformations do.
- Labels: preserve the input series’ remaining labels.
- Output units:
    - `time_above_over_time`: seconds as a floating-point sample.
    - `share_above_over_time`: unitless fraction from 0 to 1.

For a series whose value never exceeds the threshold:

```promql
time_above_over_time(up[15m], 0)
```

returns `0`, assuming the series has valid coverage in that period.

For a series that remains continuously over the threshold for the entire eligible interval:

```promql
share_above_over_time(queue_depth[10m], 100)
```

returns `1`.

### Strict versus inclusive comparisons

Do not overload “above” to mean inclusive:

```text
time_above_over_time(v, x)          => v > x
time_at_or_above_over_time(v, x)    => v >= x
time_below_over_time(v, x)          => v < x
time_at_or_below_over_time(v, x)    => v <= x
```

This matters for common state metrics such as `up`, where the intended condition is often `up == 0`, and for zero-valued error-rate or saturation thresholds.

## Two plausible interpretations

There are two defensible APIs. I would support both only if their names make the distinction unmistakable.


| Semantics | Interpretation | Best for | Suggested name |
| :-- | :-- | :-- | :-- |
| Sample-weighted | Fraction/count of stored samples passing the comparison | Scrape-level quality, sample distribution, parity with MetricsQL | `share_gt_over_time`, `count_gt_over_time` |
| Time-weighted | Wall-clock duration/fraction for which the reconstructed signal passes | SLOs, sustained saturation, duration-based alerts, irregular samples | `time_above_over_time`, `share_time_above_over_time` |

MetricsQL’s `share_gt_over_time(m[d], threshold)` counts the proportion of raw samples greater than the threshold. That is useful, but a scrape schedule or burst of duplicate samples can change the result even if the real underlying system behavior did not.[^4_2]

For clarity, I would probably name the time-aware fraction:

```promql
share_time_above_over_time(v[window], threshold)
```

rather than the shorter `share_above_over_time`.

That name is longer, but it prevents a dangerous misunderstanding: sample fraction and time fraction often coincide only for perfectly regular scrapes with no gaps.

## Example: irregular scrapes

Suppose this is a CPU gauge over a 10-minute query window, with threshold $0.90$:


| Timestamp | CPU ratio | Interpretation under LOCF |
| :-- | --: | :-- |
| 00:00 | 0.20 | Below threshold |
| 00:01 | 0.95 | Above threshold |
| 00:02 | 0.92 | Above threshold |
| 00:09 | 0.10 | Below threshold |
| 00:10 | query end | End of window |

### Sample-weighted result

Three of four stored samples exceed 0.90:

```text
share_gt_over_time(cpu[10m], 0.90) = 3 / 4 = 0.75
```

That suggests CPU was high for 75% of the period.

### Time-weighted result with step-hold

With **last-observation-carried-forward** semantics:

```text
00:00–00:01  CPU = 0.20   below  => 1 minute
00:01–00:09  CPU > 0.90   above  => 8 minutes
00:09–00:10  CPU = 0.10   below  => 1 minute
```

Therefore:

```promql
time_above_over_time(cpu[10m], 0.90)
```

returns:

```text
480
```

because the duration is 8 minutes = 480 seconds.

```promql
share_time_above_over_time(cpu[10m], 0.90)
```

returns:

```text
0.8
```

because:

$$
\frac{480\mathrm{s}}{600\mathrm{s}} = 0.8
$$

In this example sample fraction says 75%, while time fraction says 80%. With a very sparse or bursty scrape pattern, the difference can be much larger.

## Crossing-aware linear interpolation

For physical gauges—temperature, battery level, pressure, a continuously varying queue estimate—it can be more accurate to estimate the crossing point between two samples.

Consider a threshold of 90:


| Timestamp | Value |
| :-- | --: |
| 00:00 | 80 |
| 00:10 | 100 |

With linear interpolation, the value crosses 90 halfway through:

```text
00:05
```

So:

```promql
time_above_over_time(sensor_value[10m], 90)
```

would return 300 seconds, and:

```promql
share_time_above_over_time(sensor_value[10m], 90)
```

would return 0.5.

For a pair of consecutive points $(t_0,v_0)$ and $(t_1,v_1)$, when the values straddle threshold $\theta$, estimate the crossing as:

$$
t_\times =
t_0 + (t_1-t_0)
\frac{\theta-v_0}{v_1-v_0}
$$

Then add the appropriate portion of $[t_0,t_1]$ to the above-threshold duration.

A clean extension syntax could be:

```promql
time_above_over_time(
  temperature_celsius[1h],
  80,
  "linear"
)

share_time_above_over_time(
  temperature_celsius[1h],
  80,
  "linear"
)
```

or, more consistently with PromQL’s limited function argument conventions:

```promql
time_above_over_time(temperature_celsius[1h], 80)
```

with explicit alternative functions:

```promql
time_above_linear_over_time(temperature_celsius[1h], 80)
share_time_above_linear_over_time(temperature_celsius[1h], 80)
```

I prefer distinct function names or a module-specific query option over a magic third string argument. PromQL’s type system and tooling are much friendlier to scalar/vector/range arguments than arbitrary string-enum arguments.

## Boundary and missing-data policy

This is the part that decides whether the function is safe for alerts.

### Window boundaries

A range vector normally contains samples observed inside the lookback window. To calculate time over the **entire** requested interval, the engine must optionally obtain boundary context:

- the last sample at or before $W_s$, to establish a value at the left boundary;
- the first sample at or after $W_e$, only for linear interpolation at the right boundary, if the API allows it;
- all samples inside the window.

For step-hold semantics, the left predecessor is essential. Without it, the interval from $W_s$ to the first in-window point is unknowable.

Example:

```text
Predecessor: 09:59:40 = 95
Window start: 10:00:00
First in-window sample: 10:04:00 = 95
Threshold: 90
```

The correct step-hold answer for 10:00–10:04 is four minutes above threshold. A range iterator that starts only at 10:00 and ignores the predecessor would undercount by four minutes.

This favors a storage API that can seek one point before the query start at little extra cost—particularly relevant if your chunks already maintain timestamp indexes or support reverse decoding.

### Staleness and maximum hold

Never carry the last observation forever. A source that stopped scraping must not quietly appear healthy or unhealthy for the full query range.

Expose a maximum interval, conceptually:

```text
max_hold = min(
  user-configured maximum,
  scrape-interval-derived staleness bound,
  series retention/query bound
)
```

One possible command-layer shape:

```text
TS.TIMEABOVE key from to THRESHOLD 0.9
  METHOD LOCF
  MAXGAP 2m
```

Or a PromQL extension:

```promql
time_above_over_time(cpu_usage[30m], 0.90, 2m)
```

I would avoid that third function argument for strict PromQL compatibility, but it is reasonable for a Valkey command or an extension-only grammar.

If the gap from one sample to the next exceeds `MAXGAP`, stop integrating at:

```text
previous_timestamp + MAXGAP
```

and classify the remainder as uncovered.

### Denominator choices

You need to decide whether `share` divides by wall-clock window length or known/covered duration.


| Denominator | Formula | Advantage | Risk |
| :-- | :-- | :-- | :-- |
| Full window | `above_duration / (end - start)` | Simple; pessimistic under missing data | Can make an absent metric look “fine” if missing periods count as not-above |
| Covered duration | `above_duration / covered_duration` | Measures behavior only while observed | Can hide data loss; 100% may be based on seconds of coverage |
| Strict full coverage | Return no sample / `NaN` unless coverage is sufficient | Safe for SLO and alert use | Needs clear coverage threshold and may surprise dashboard users |

I recommend returning a **coverage-aware tuple internally**, even if the public function returns only one scalar:

```text
above_duration
covered_duration
window_duration
sample_count
gap_count
max_gap
```

Then expose complementary functions:

```promql
time_above_over_time(v[30m], threshold)
observed_duration_over_time(v[30m])
coverage_over_time(v[30m])
share_time_above_over_time(v[30m], threshold)
```

with:

$$
\operatorname{coverage} =
\frac{\operatorname{covered\_duration}}
     {W_e-W_s}
$$

For alerting, require coverage explicitly:

```promql
(
  share_time_above_over_time(cpu_usage[30m], 0.90) > 0.80
)
and
(
  coverage_over_time(cpu_usage[30m]) >= 0.95
)
```

That means:

> Alert when CPU was above 90% for more than 80% of the observed time, but only if at least 95% of the half-hour had trustworthy coverage.

State-tracking systems make this distinction because duration calculations must account for transitions and liveness/observation gaps; Timescale’s state-tracking functions are designed specifically around durations spent in states and heartbeat/liveness analysis.[^4_3]

## Operational examples

### Sustained CPU saturation

```promql
share_time_above_over_time(
  node_cpu_utilization_ratio[30m],
  0.90
) > 0.80
```

Meaning: CPU utilization was greater than 90% for more than 80% of the last 30 minutes.

Add coverage:

```promql
(
  share_time_above_over_time(node_cpu_utilization_ratio[30m], 0.90) > 0.80
)
and
(
  coverage_over_time(node_cpu_utilization_ratio[30m]) >= 0.95
)
```


### Error-budget style availability

For binary `up`, use an inclusive or equality form:

```promql
time_eq_over_time(up[28d], 0)
```

or:

```promql
time_below_over_time(up[28d], 0.5)
```

Then derive availability:

```promql
1 -
(
  time_below_over_time(up[28d], 0.5)
  /
  observed_duration_over_time(up[28d])
)
```

For a true availability SLI, you would normally define expected coverage separately; otherwise monitoring failure can improve measured availability.

### Latency breach duration

```promql
time_above_over_time(
  histogram_quantile(
    0.99,
    sum by (le, service) (
      rate(http_request_duration_seconds_bucket[5m])
    )
  )[1h:1m],
  0.5
)
```

Conceptually: return the number of seconds during the last hour in which the one-minute p99 latency estimate exceeded 500 ms.

In standard PromQL, nested subquery/range behavior makes this syntactically feasible, but it can be expensive because the p99 expression is evaluated repeatedly. For a Valkey query engine, this is a reason to add evaluation limits, cache subquery steps, and strongly bound fanout.

### Queue depth saturation

```promql
share_time_above_over_time(
  worker_queue_depth[15m],
  10_000
)
```

This answers “what fraction of the last 15 minutes did the queue remain overloaded?”—a much better signal than the maximum, which may represent a one-second spike.

## Suggested implementation contract

For a first implementation, I would make this contract explicit:

```text
Function:
  time_above_over_time(range-vector, scalar) -> instant-vector
  share_time_above_over_time(range-vector, scalar) -> instant-vector

Comparison:
  Strict: value > threshold.

Output:
  Seconds for time_above.
  Ratio [0, 1] for share_time_above.

Interpolation:
  LOCF / zero-order hold only in v1.

Boundary context:
  Include the final sample at or before the range start.
  Clamp every interval to the requested range.

Staleness:
  Do not integrate across a gap longer than max_gap.
  Default max_gap should be explicit and conservative; do not inherit
  an undocumented scrape interval.

Missing data:
  Track covered duration separately.
  `share_time_above` divides by covered duration, returning NaN when
  there is no covered duration.

Observability:
  Provide observed_duration_over_time / coverage_over_time so alert
  authors can make data-completeness requirements explicit.
```

The key calculation is a single forward scan of decoded samples. For each adjacent pair, clip the segment to the query interval, reject or truncate it if it exceeds `MAXGAP`, then add either all or none of its duration under LOCF. That is $O(n)$ in decoded samples and requires $O(1)$ working state per series.

## Recommended names

For a PromQL-extension surface, my preferred minimal set is:

```promql
time_gt_over_time(v[window], threshold)
time_ge_over_time(v[window], threshold)
time_lt_over_time(v[window], threshold)
time_le_over_time(v[window], threshold)

share_time_gt_over_time(v[window], threshold)
share_time_ge_over_time(v[window], threshold)
share_time_lt_over_time(v[window], threshold)
share_time_le_over_time(v[window], threshold)

observed_duration_over_time(v[window])
coverage_over_time(v[window])
```

The operator-based names mirror MetricsQL’s `count_gt_over_time` and `share_gt_over_time`, while the inserted `time` makes the weighting model visible. MetricsQL already demonstrates that threshold-oriented count/share functions are useful; this proposal preserves that ergonomics while fixing the irregular-sampling ambiguity for duration-oriented analysis.[^4_1][^4_2]

<span style="display:none">[^4_10][^4_11][^4_12][^4_13][^4_14][^4_15][^4_16][^4_4][^4_5][^4_6][^4_7][^4_8][^4_9]</span>

<div align="center">⁂</div>

[^4_1]: https://victoriametrics.com/blog/victoriametrics-anomaly-detection-handbook-chapter-2/

[^4_2]: https://www.cheatgrid.com/devops/1645-victoriametrics-cheat-sheet

[^4_3]: https://www.tigerdata.com/docs/reference/toolkit/state-tracking

[^4_4]: https://docs.greptime.com/user-guide/query-data/promql/

[^4_5]: https://docs.victoriametrics.com/victoriametrics/

[^4_6]: https://docs.victoriametrics.com/

[^4_7]: https://docs.elest.io/books/timescaledb/page/detect-and-terminate-long-running-queries

[^4_8]: https://docs.elest.io/books/timescaledb/export/html

[^4_9]: https://newreleases.io/project/github/VictoriaMetrics/VictoriaMetrics/release/v1.40.1

[^4_10]: https://docs.victoriametrics.com/metricsql/

[^4_11]: https://docs.victoriametrics.com/victoriametrics/metricsql/

[^4_12]: https://docs.influxdata.com/influxdb/cloud/query-data/flux/monitor-states/

[^4_13]: https://victoriametrics.com/products/metricsql/

[^4_14]: https://docs.influxdata.com/influxdb/v2/query-data/flux/monitor-states/

[^4_15]: https://docs.influxdata.com/flux/v0/stdlib/universe/stateduration/

[^4_16]: https://docs.timescale.com/api/latest/hyperfunctions/state-tracking/state_agg/


---

# Sketch out a possible rust implementation of share_time_gt_over_time

Below is a possible implementation shape for:

```promql
share_time_gt_over_time(v[window], threshold)
```

using **LOCF / zero-order-hold** semantics, an explicit maximum gap, and a denominator of **observed (covered) duration**. It returns the fraction of covered wall-clock time during which `value > threshold`.

The core scan is linear in the number of decoded samples and uses constant working memory per series. It is designed to fit a chunked/compressed Valkey TimeSeries storage engine: request the predecessor sample before the range start, then iterate forward across the query window.

## Contract

For evaluation time $T$ and range $R$:

$$
W_s = T - R,\qquad W_e = T
$$

For each valid interval represented by a sample $(t_i, v_i)$, LOCF interprets its value as applying over:

$$
[t_i, t_{i+1})
$$

after clipping to the query window and to `max_gap`.

The function computes:

$$
\operatorname{share\_time\_gt}(v,\theta) =
\frac{
  \sum_i \Delta t_i \cdot \mathbf{1}[v_i > \theta]
}{
  \sum_i \Delta t_i
}
$$

where $\Delta t_i$ is an interval with known coverage.

Recommended externally visible behavior:


| Condition | Result |
| :-- | :-- |
| At least one valid covered interval | Ratio in $[0,1]$ |
| Samples exist, but all intervals are invalid/uncovered | `NaN` |
| No samples | No output series, following normal PromQL range-function behavior |
| `NaN` sample value | Do not count its interval as coverage |
| Gap exceeds `max_gap` | Count at most `max_gap` after the prior sample; remainder is uncovered |
| `+Inf` | Greater than every finite threshold |
| `-Inf` | Less than every finite threshold |
| Threshold `NaN` | Return `NaN`; no IEEE comparison against `NaN` is true |

The choice to divide by **covered duration** is deliberate. It prevents missing samples from silently becoming “below threshold.” Pair it with `coverage_over_time()` in alert rules where completeness matters.

## Core types

This version keeps timestamps as signed milliseconds, which is generally convenient at the storage/module boundary. Convert to seconds only when producing a PromQL sample or reporting an absolute duration.

```rust
use std::cmp::{max, min};

pub type TimestampMs = i64;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Sample {
    pub ts_ms: TimestampMs,
    pub value: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeRange {
    pub start_ms: TimestampMs,
    pub end_ms: TimestampMs,
}

impl TimeRange {
    pub fn new(start_ms: TimestampMs, end_ms: TimestampMs) -> Self {
        assert!(start_ms <= end_ms);
        Self { start_ms, end_ms }
    }

    pub fn duration_ms(self) -> u64 {
        self.end_ms.saturating_sub(self.start_ms) as u64
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoveragePolicy {
    /// Carry each finite sample forward, but never farther than `max_gap_ms`.
    ///
    /// If the next sample is farther away than max_gap, the remainder is
    /// uncovered rather than treated as the previous value.
    Locf {
        max_gap_ms: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TimeShare {
    pub matched_ms: u64,
    pub covered_ms: u64,
    pub window_ms: u64,
}

impl TimeShare {
    pub fn share(self) -> f64 {
        if self.covered_ms == 0 {
            f64::NAN
        } else {
            self.matched_ms as f64 / self.covered_ms as f64
        }
    }

    pub fn coverage(self) -> f64 {
        if self.window_ms == 0 {
            f64::NAN
        } else {
            self.covered_ms as f64 / self.window_ms as f64
        }
    }

    pub fn matched_seconds(self) -> f64 {
        self.matched_ms as f64 / 1_000.0
    }

    pub fn covered_seconds(self) -> f64 {
        self.covered_ms as f64 / 1_000.0
    }
}
```

`TimeShare` is intentionally more informative than the final PromQL result. The query function can emit only `share()`, while `coverage()` and `matched_seconds()` support:

```promql
share_time_gt_over_time(cpu_usage[30m], 0.90)
coverage_over_time(cpu_usage[30m])
time_gt_over_time(cpu_usage[30m], 0.90)
```

They all use exactly the same interval scanner.

## Core LOCF scanner

The scanner accepts samples in nondecreasing timestamp order. It expects the caller to include **one predecessor sample**—the final sample at or before the query start—when available.

```rust
pub fn share_time_gt_over_time<I>(
    samples: I,
    range: TimeRange,
    threshold: f64,
    policy: CoveragePolicy,
) -> TimeShare
where
    I: IntoIterator<Item = Sample>,
{
    let CoveragePolicy::Locf { max_gap_ms } = policy;

    let window_ms = range.duration_ms();
    if window_ms == 0 || threshold.is_nan() {
        return TimeShare {
            matched_ms: 0,
            covered_ms: 0,
            window_ms,
        };
    }

    let mut result = TimeShare {
        matched_ms: 0,
        covered_ms: 0,
        window_ms,
    };

    let mut previous: Option<Sample> = None;

    for current in samples {
        if let Some(prev) = previous {
            accumulate_locf_gt_interval(
                &mut result,
                prev,
                current.ts_ms,
                range,
                threshold,
                max_gap_ms,
            );
        }

        previous = Some(current);

        // No later interval can overlap the query range once this point is
        // at or beyond the right boundary. Its predecessor was already handled.
        if current.ts_ms >= range.end_ms {
            break;
        }
    }

    // If the final observed sample lies within the range, it can hold forward
    // until min(window_end, sample_ts + max_gap).
    if let Some(last) = previous {
        accumulate_locf_gt_interval(
            &mut result,
            last,
            range.end_ms,
            range,
            threshold,
            max_gap_ms,
        );
    }

    result
}
```

The important detail is that `accumulate_locf_gt_interval()` may be called twice for the final sample in a naïve formulation—once against the next sample, and once against the range end. The implementation below avoids double counting by clipping each interval so that it starts at `sample.ts_ms` and ends at either the next sample or `sample.ts_ms + max_gap`; however, to make the control flow unmistakable, I would use a version that separates “real next sample” from “tail” in production.

Here is the safer complete version:

```rust
pub fn share_time_gt_over_time<I>(
    samples: I,
    range: TimeRange,
    threshold: f64,
    policy: CoveragePolicy,
) -> TimeShare
where
    I: IntoIterator<Item = Sample>,
{
    let CoveragePolicy::Locf { max_gap_ms } = policy;

    let mut result = TimeShare {
        matched_ms: 0,
        covered_ms: 0,
        window_ms: range.duration_ms(),
    };

    if range.start_ms == range.end_ms || threshold.is_nan() {
        return result;
    }

    let mut iter = samples.into_iter();
    let Some(mut prev) = iter.next() else {
        return result;
    };

    for next in iter {
        if next.ts_ms < prev.ts_ms {
            // Production code should normally reject this in the iterator
            // construction layer. Ignore malformed backwards intervals here.
            prev = next;
            continue;
        }

        accumulate_locf_gt_interval(
            &mut result,
            prev,
            next.ts_ms,
            range,
            threshold,
            max_gap_ms,
        );

        if next.ts_ms >= range.end_ms {
            return result;
        }

        prev = next;
    }

    accumulate_locf_gt_interval(
        &mut result,
        prev,
        range.end_ms,
        range,
        threshold,
        max_gap_ms,
    );

    result
}

fn accumulate_locf_gt_interval(
    result: &mut TimeShare,
    sample: Sample,
    next_ts_ms: TimestampMs,
    range: TimeRange,
    threshold: f64,
    max_gap_ms: u64,
) {
    // `NaN` is unknown, not below the threshold. Therefore it contributes
    // neither matched time nor observed coverage.
    if sample.value.is_nan() {
        return;
    }

    // A duplicate timestamp contains no represented duration.
    if next_ts_ms <= sample.ts_ms {
        return;
    }

    let max_valid_end = sample
        .ts_ms
        .saturating_add(max_gap_ms.min(i64::MAX as u64) as i64);

    // LOCF is valid only through the earlier of the next point or max gap.
    let represented_end = min(next_ts_ms, max_valid_end);

    // Clip the represented segment [sample.ts_ms, represented_end) to
    // the requested query window [range.start_ms, range.end_ms).
    let clipped_start = max(sample.ts_ms, range.start_ms);
    let clipped_end = min(represented_end, range.end_ms);

    if clipped_end <= clipped_start {
        return;
    }

    let duration_ms = (clipped_end - clipped_start) as u64;
    result.covered_ms = result.covered_ms.saturating_add(duration_ms);

    if sample.value > threshold {
        result.matched_ms = result.matched_ms.saturating_add(duration_ms);
    }
}
```

This is the essential implementation. It defines each sample as owning time until the next sample, bounded by `max_gap_ms`, then clips the owned interval to the requested range.

## Query-engine integration

A range function cannot be implemented correctly with only samples inside `[start, end]`. The engine needs a predecessor point:

```rust
pub trait SeriesCursor {
    type Error;

    /// The greatest sample timestamp <= ts, if one exists.
    fn seek_le(&mut self, ts: TimestampMs) -> Result<Option<Sample>, Self::Error>;

    /// Iterates samples whose timestamps are strictly greater than `ts`.
    fn iter_gt<'a>(
        &'a mut self,
        ts: TimestampMs,
    ) -> Result<Box<dyn Iterator<Item = Result<Sample, Self::Error>> + 'a>, Self::Error>;
}
```

A storage-neutral adapter can then produce the required ordered sample stream:

```rust
pub fn evaluate_share_time_gt<C>(
    cursor: &mut C,
    range: TimeRange,
    threshold: f64,
    max_gap_ms: u64,
) -> Result<TimeShare, C::Error>
where
    C: SeriesCursor,
{
    let predecessor = cursor.seek_le(range.start_ms)?;
    let after_start = cursor.iter_gt(range.start_ms)?;

    let mut samples = Vec::new();

    if let Some(sample) = predecessor {
        samples.push(sample);
    }

    for sample in after_start {
        let sample = sample?;

        // Include one point at/after the right boundary. It closes the
        // preceding LOCF interval, then iteration can stop.
        let is_at_or_after_end = sample.ts_ms >= range.end_ms;
        samples.push(sample);

        if is_at_or_after_end {
            break;
        }
    }

    Ok(share_time_gt_over_time(
        samples,
        range,
        threshold,
        CoveragePolicy::Locf { max_gap_ms },
    ))
}
```

For a real compressed engine, avoid the temporary `Vec<Sample>`: make a chained iterator consisting of `predecessor.into_iter()` plus the chunk decoder iterator. The vector above only makes the sample-order requirement clear.

A chunk-aware path should use chunk metadata to skip fully irrelevant chunks:

```text
Chunk time range entirely before start:
  Only potentially useful if it contains the predecessor.

Chunk time range entirely after end:
  Stop after decoding its first sample, because that sample closes
  the preceding interval.

Chunk interval wholly inside range:
  Decode normally; later optimization can use summaries only when the
  entire chunk is known to be above or below threshold.
```


## Worked example

Given these samples:

```text
Window:    [00:00:00, 00:10:00)
Threshold: 0.90
Max gap:   5 minutes

00:00:00  0.20
00:01:00  0.95
00:02:00  0.92
00:09:00  0.10
```

The intervals are:

```text
[00:00, 00:01) = 0.20  => 60s covered, 0s matched
[00:01, 00:02) = 0.95  => 60s covered, 60s matched
[00:02, 00:07) = 0.92  => 300s covered, 300s matched
[00:07, 00:09)          => uncovered; max gap expires
[00:09, 00:10) = 0.10  => 60s covered, 0s matched
```

Therefore:

```text
matched duration = 360 seconds
covered duration = 480 seconds
window duration  = 600 seconds

share_time_gt_over_time = 360 / 480 = 0.75
coverage_over_time      = 480 / 600 = 0.80
```

Rust test:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn s(seconds: i64, value: f64) -> Sample {
        Sample {
            ts_ms: seconds * 1_000,
            value,
        }
    }

    #[test]
    fn locf_threshold_share_respects_max_gap() {
        let range = TimeRange::new(0, 600_000);

        let summary = share_time_gt_over_time(
            [
                s(0, 0.20),
                s(60, 0.95),
                s(120, 0.92),
                s(540, 0.10),
            ],
            range,
            0.90,
            CoveragePolicy::Locf {
                max_gap_ms: 300_000,
            },
        );

        assert_eq!(summary.matched_ms, 360_000);
        assert_eq!(summary.covered_ms, 480_000);
        assert_eq!(summary.window_ms, 600_000);
        assert!((summary.share() - 0.75).abs() < 1e-12);
        assert!((summary.coverage() - 0.80).abs() < 1e-12);
    }

    #[test]
    fn predecessor_covers_left_window_boundary() {
        let range = TimeRange::new(100_000, 200_000);

        let summary = share_time_gt_over_time(
            [
                s(90, 100.0),  // predecessor
                s(150, 100.0),
                s(200, 0.0),  // closes [150s, 200s)
            ],
            range,
            50.0,
            CoveragePolicy::Locf {
                max_gap_ms: 120_000,
            },
        );

        assert_eq!(summary.matched_ms, 100_000);
        assert_eq!(summary.covered_ms, 100_000);
        assert_eq!(summary.share(), 1.0);
    }

    #[test]
    fn no_covered_time_is_nan() {
        let range = TimeRange::new(0, 60_000);

        let summary = share_time_gt_over_time(
            [s(0, 1.0)],
            range,
            0.5,
            CoveragePolicy::Locf { max_gap_ms: 0 },
        );

        assert_eq!(summary.covered_ms, 0);
        assert!(summary.share().is_nan());
    }

    #[test]
    fn nan_value_is_unknown_not_below_threshold() {
        let range = TimeRange::new(0, 60_000);

        let summary = share_time_gt_over_time(
            [
                s(0, f64::NAN),
                s(30, 1.0),
                s(60, 1.0),
            ],
            range,
            0.5,
            CoveragePolicy::Locf {
                max_gap_ms: 60_000,
            },
        );

        assert_eq!(summary.matched_ms, 30_000);
        assert_eq!(summary.covered_ms, 30_000);
        assert_eq!(summary.share(), 1.0);
    }

    #[test]
    fn exact_threshold_is_not_greater_than() {
        let range = TimeRange::new(0, 60_000);

        let summary = share_time_gt_over_time(
            [s(0, 0.90), s(60, 0.90)],
            range,
            0.90,
            CoveragePolicy::Locf {
                max_gap_ms: 60_000,
            },
        );

        assert_eq!(summary.covered_ms, 60_000);
        assert_eq!(summary.matched_ms, 0);
        assert_eq!(summary.share(), 0.0);
    }
}
```


## Generic predicate implementation

You should not duplicate the scanner for `gt`, `ge`, `lt`, `le`, equality, range membership, or Boolean state tracking. Factor the comparator into a predicate:

```rust
pub fn time_share_over_time<I, P>(
    samples: I,
    range: TimeRange,
    policy: CoveragePolicy,
    predicate: P,
) -> TimeShare
where
    I: IntoIterator<Item = Sample>,
    P: Fn(f64) -> bool,
{
    let CoveragePolicy::Locf { max_gap_ms } = policy;

    let mut result = TimeShare {
        matched_ms: 0,
        covered_ms: 0,
        window_ms: range.duration_ms(),
    };

    let mut iter = samples.into_iter();
    let Some(mut prev) = iter.next() else {
        return result;
    };

    for next in iter {
        accumulate_locf_predicate_interval(
            &mut result,
            prev,
            next.ts_ms,
            range,
            max_gap_ms,
            &predicate,
        );

        if next.ts_ms >= range.end_ms {
            return result;
        }

        prev = next;
    }

    accumulate_locf_predicate_interval(
        &mut result,
        prev,
        range.end_ms,
        range,
        max_gap_ms,
        &predicate,
    );

    result
}

fn accumulate_locf_predicate_interval<P>(
    result: &mut TimeShare,
    sample: Sample,
    next_ts_ms: TimestampMs,
    range: TimeRange,
    max_gap_ms: u64,
    predicate: &P,
) where
    P: Fn(f64) -> bool,
{
    if sample.value.is_nan() || next_ts_ms <= sample.ts_ms {
        return;
    }

    let max_valid_end = sample
        .ts_ms
        .saturating_add(max_gap_ms.min(i64::MAX as u64) as i64);

    let represented_end = min(next_ts_ms, max_valid_end);
    let start = max(sample.ts_ms, range.start_ms);
    let end = min(represented_end, range.end_ms);

    if end <= start {
        return;
    }

    let elapsed_ms = (end - start) as u64;
    result.covered_ms = result.covered_ms.saturating_add(elapsed_ms);

    if predicate(sample.value) {
        result.matched_ms = result.matched_ms.saturating_add(elapsed_ms);
    }
}

pub fn share_time_gt_over_time<I>(
    samples: I,
    range: TimeRange,
    threshold: f64,
    policy: CoveragePolicy,
) -> TimeShare
where
    I: IntoIterator<Item = Sample>,
{
    if threshold.is_nan() {
        return TimeShare {
            matched_ms: 0,
            covered_ms: 0,
            window_ms: range.duration_ms(),
        };
    }

    time_share_over_time(samples, range, policy, |value| value > threshold)
}
```

The same primitive supports:

```rust
let gt = time_share_over_time(samples, range, policy, |x| x > threshold);
let ge = time_share_over_time(samples, range, policy, |x| x >= threshold);
let in_slo = time_share_over_time(samples, range, policy, |x| x <= target);
let unhealthy = time_share_over_time(samples, range, policy, |x| x == 0.0);
```

For a PromQL engine, the predicate should be represented as an enum rather than an allocation-heavy closure in the hot path:

```rust
#[derive(Debug, Clone, Copy)]
pub enum Comparison {
    Gt,
    Ge,
    Lt,
    Le,
    Eq,
    Ne,
}

impl Comparison {
    #[inline]
    pub fn matches(self, value: f64, threshold: f64) -> bool {
        match self {
            Self::Gt => value > threshold,
            Self::Ge => value >= threshold,
            Self::Lt => value < threshold,
            Self::Le => value <= threshold,
            Self::Eq => value == threshold,
            Self::Ne => value != threshold,
        }
    }
}
```


## Chunk-summary optimization

A naïve implementation decodes every point. That is correct, but may be unnecessarily expensive for long windows. A useful chunk summary is:

```rust
#[derive(Debug, Clone)]
pub struct ChunkSummary {
    pub first_ts_ms: TimestampMs,
    pub first_value: f64,
    pub last_ts_ms: TimestampMs,
    pub last_value: f64,
    pub min_value: f64,
    pub max_value: f64,
    pub sample_count: u32,
}
```

For a threshold $\theta$:

- `max_value <= θ`: no point inside the chunk is above the threshold.
- `min_value > θ`: every *sample value* is above threshold.
- Otherwise: decode the chunk.

But this alone is insufficient to add duration without decoding because you still need point-to-point timestamps, max-gap behavior, and boundaries. To make a chunk entirely reducible, you need a richer summary that represents duration-weighted threshold behavior, which is threshold-specific and therefore impractical for arbitrary thresholds.

Useful practical optimizations are:

1. **Skip decompression of chunks wholly before the predecessor or wholly after the first right-boundary sample.**
2. **Use min/max summaries to avoid value decoding only if timestamps are separately cheaply decodable.**
3. **Keep delta-of-delta timestamp streams separately decodable from value streams**, if your codec layout permits it.
4. **Use fixed-resolution rollups** for dashboard-scale duration queries:

```text
1-minute rollup:
  first, last, min, max, covered_duration,
  time_above per selected threshold family
```

5. **Precompute threshold-state rollups only for known policy/SLO thresholds**, such as 90% CPU, 500 ms p99 latency, or queue-depth capacity. Arbitrary user thresholds must generally scan source data.

## PromQL parser and evaluation shape

If you add it as an extension function, the signature conceptually is:

```text
share_time_gt_over_time(range-vector, scalar) → instant-vector
```

Example:

```promql
share_time_gt_over_time(
  node_cpu_utilization_ratio{cluster="prod"}[30m],
  0.90
)
```

At every instant evaluation timestamp, your evaluator:

1. Computes `[evaluation_time - range, evaluation_time]`.
2. Resolves the scalar threshold; reject or return no data if it is not a single scalar.
3. Selects matching time series.
4. Opens each series cursor with one predecessor sample.
5. Runs the LOCF scanner.
6. Emits one output sample per input series when `covered_ms > 0`; otherwise use your chosen extension policy—typically emit `NaN` or omit the series.
7. Removes `__name__` while retaining non-name labels.

For range queries, repeating a full scan for each evaluation step can be expensive. A later optimization is a rolling evaluator:

```text
At step k:
  retain the active interval contribution of each series,
  evict segments leaving the left side,
  insert segments entering the right side,
  update matched and covered durations incrementally.
```

However, correctness first: range boundaries, stale gaps, predecessor lookup, and inclusive/exclusive timestamp conventions must be proven with test fixtures before building an incremental path.

## Production decisions to lock down

Before exposing this function, document these choices precisely:

- **Interval convention:** use half-open intervals `[sample_ts, next_sample_ts)` to prevent double counting.
- **Boundary inclusion:** PromQL range selectors conventionally include samples in the configured lookback semantics; your duration integration must specify exactly whether the end boundary is included. For elapsed duration, it should normally make no numerical difference.
- **Predecessor lookup:** require the final raw sample at or before the window start.
- **Default `max_gap`:** preferably an explicit engine/module configuration, not an undocumented heuristic.
- **Duplicate timestamps:** resolve them at ingestion or define last-write-wins before iteration; never treat zero-length duplicate intervals as two observations.
- **Out-of-order timestamps:** normalize at ingestion or sort/merge before the duration evaluator.
- **`NaN` values:** treat as unknown coverage, not below threshold.
- **Stale markers:** if preserving Prometheus staleness markers, terminate coverage at the marker rather than integrating across it.
- **No coverage:** `NaN` is analytically honest; omitting the series may be more PromQL-like. Choose one consistently.
- **Coverage enforcement:** encourage alert rules to combine this with `coverage_over_time`, rather than making silent completeness assumptions.

A practical companion family would be:

```promql
time_gt_over_time(v[window], threshold)
share_time_gt_over_time(v[window], threshold)
observed_duration_over_time(v[window])
coverage_over_time(v[window])

time_ge_over_time(v[window], threshold)
time_lt_over_time(v[window], threshold)
time_le_over_time(v[window], threshold)
```

They can all share the exact scanner and internal `TimeShare` state, keeping semantics consistent while exposing both absolute duration and normalized SLO-friendly ratios.

<span style="display:none">[^5_1]</span>

<div align="center">⁂</div>

[^5_1]: https://docs.greptime.com/user-guide/query-data/promql/


---

# Sketch out a possible rust implementation of time_gt_over_time

> **Review note (2026-09-30):** this sketch assumes a predecessor sample before the window,
> which conflicts with the engine's `RollupKind` push-down contract. See
> [Review: implementing `time_gt_over_time` in this engine](#review-implementing-time_gt_over_time-in-this-engine)
> at the end of this document before implementing.

`time_gt_over_time(v[range], threshold)` is the absolute-duration sibling of `share_time_gt_over_time`: it returns the number of **covered seconds** for which the reconstructed series value was strictly greater than `threshold`.

For a first version, use **LOCF** (last-observation-carried-forward) semantics, a mandatory bounded `max_gap`, and a predecessor sample at or before the left query boundary. This gives a streaming $O(n)$, $O(1)$-state implementation that works naturally over decoded Valkey TS chunks.

## Contract

At evaluation timestamp $T$, for a range duration $R$:

$$
W_s = T - R,\qquad W_e = T
$$

For each sample $(t_i, v_i)$, assign its value to the half-open interval:

$$
[t_i, \min(t_{i+1}, t_i + G))
$$

where $G$ is `max_gap`. Clip that interval to the query window $[W_s, W_e)$.

Then:

$$
\operatorname{time\_gt\_over\_time}(v,\theta)
=
\sum_i
\left|
[t_i,\min(t_{i+1}, t_i+G))
\cap [W_s,W_e)
\right|
\cdot \mathbf{1}[v_i>\theta]
$$

Output is seconds as `f64`, preserving PromQL’s conventional floating-point sample representation.

Example:

```promql
time_gt_over_time(
  node_cpu_utilization_ratio{instance="api-01"}[30m],
  0.90
)
```

If CPU was known to be above 90% for 24 minutes, the output value is:

```text
1440
```


## Minimal types

```rust
use std::cmp::{max, min};

pub type TimestampMs = i64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeRange {
    pub start_ms: TimestampMs,
    pub end_ms: TimestampMs,
}

impl TimeRange {
    pub fn new(start_ms: TimestampMs, end_ms: TimestampMs) -> Self {
        assert!(start_ms <= end_ms);
        Self { start_ms, end_ms }
    }

    #[inline]
    pub fn duration_ms(self) -> u64 {
        self.end_ms.saturating_sub(self.start_ms) as u64
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocfPolicy {
    /// Do not carry one sample farther than this duration.
    ///
    /// A gap after this bound is treated as uncovered / unknown.
    pub max_gap_ms: u64,
}
```


## Direct implementation

This implementation takes an ordered sequence whose first element may be the predecessor sample—i.e. the most recent point at or before `range.start_ms`.

```rust
pub fn time_gt_over_time<I>(
    samples: I,
    range: TimeRange,
    threshold: f64,
    policy: LocfPolicy,
) -> f64
where
    I: IntoIterator<Item = Sample>,
{
    if range.start_ms >= range.end_ms || threshold.is_nan() {
        return f64::NAN;
    }

    let mut iter = samples.into_iter();
    let Some(mut previous) = iter.next() else {
        return f64::NAN;
    };

    let mut matched_ms: u64 = 0;
    let max_gap_ms = policy.max_gap_ms.min(i64::MAX as u64) as i64;

    for current in iter {
        if current.ts_ms < previous.ts_ms {
            // Prefer rejecting this at the storage/iterator layer.
            // Ignoring the malformed interval is conservative.
            previous = current;
            continue;
        }

        matched_ms = matched_ms.saturating_add(interval_gt_duration_ms(
            previous,
            current.ts_ms,
            range,
            threshold,
            max_gap_ms,
        ));

        // `current` closes the preceding interval. Nothing after it can
        // overlap the requested window once it is at or beyond range end.
        if current.ts_ms >= range.end_ms {
            return matched_ms as f64 / 1_000.0;
        }

        previous = current;
    }

    // The final sample can represent its value until query end, bounded by
    // max_gap. This is necessary when there is no sample at the right edge.
    matched_ms = matched_ms.saturating_add(interval_gt_duration_ms(
        previous,
        range.end_ms,
        range,
        threshold,
        max_gap_ms,
    ));

    matched_ms as f64 / 1_000.0
}
```

The helper applies all critical rules: finite values only, no backwards/zero-length intervals, `max_gap`, clipping, then threshold comparison.

```rust
#[inline]
fn interval_gt_duration_ms(
    sample: Sample,
    next_ts_ms: TimestampMs,
    range: TimeRange,
    threshold: f64,
    max_gap_ms: i64,
) -> u64 {
    // A NaN input is unknown; do not count its interval.
    if sample.value.is_nan() || next_ts_ms <= sample.ts_ms {
        return 0;
    }

    let valid_end_ms = sample.ts_ms.saturating_add(max_gap_ms);
    let represented_end_ms = min(next_ts_ms, valid_end_ms);

    let clipped_start_ms = max(sample.ts_ms, range.start_ms);
    let clipped_end_ms = min(represented_end_ms, range.end_ms);

    if clipped_end_ms <= clipped_start_ms || !(sample.value > threshold) {
        return 0;
    }

    (clipped_end_ms - clipped_start_ms) as u64
}
```


## Worked example

Input:

```text
Window:    [00:00, 00:10)
Threshold: 0.90
Max gap:   5 minutes

00:00  0.20
00:01  0.95
00:02  0.92
00:09  0.10
```

The LOCF intervals are:

```text
[00:00, 00:01)   0.20  below threshold     => 0 seconds
[00:01, 00:02)   0.95  above threshold     => 60 seconds
[00:02, 00:07)   0.92  above threshold     => 300 seconds
[00:07, 00:09)   unknown due to max gap    => 0 seconds
[00:09, 00:10)   0.10  below threshold     => 0 seconds
```

So:

```promql
time_gt_over_time(cpu_utilization_ratio[10m], 0.90)
```

returns:

```text
360
```

Rust test:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn s(seconds: i64, value: f64) -> Sample {
        Sample {
            ts_ms: seconds * 1_000,
            value,
        }
    }

    fn ten_minutes() -> TimeRange {
        TimeRange::new(0, 600_000)
    }

    #[test]
    fn computes_above_threshold_time_with_bounded_locf() {
        let result = time_gt_over_time(
            [
                s(0, 0.20),
                s(60, 0.95),
                s(120, 0.92),
                s(540, 0.10),
            ],
            ten_minutes(),
            0.90,
            LocfPolicy {
                max_gap_ms: 300_000,
            },
        );

        assert_eq!(result, 360.0);
    }

    #[test]
    fn predecessor_covers_window_left_boundary() {
        let result = time_gt_over_time(
            [
                s(90, 100.0), // predecessor, before window start
                s(150, 100.0),
                s(200, 0.0),  // closes [150, 200)
            ],
            TimeRange::new(100_000, 200_000),
            50.0,
            LocfPolicy {
                max_gap_ms: 120_000,
            },
        );

        assert_eq!(result, 100.0);
    }

    #[test]
    fn max_gap_prevents_unbounded_carry_forward() {
        let result = time_gt_over_time(
            [
                s(0, 1.0),
                s(600, 0.0),
            ],
            ten_minutes(),
            0.5,
            LocfPolicy {
                max_gap_ms: 60_000,
            },
        );

        // Only [00:00, 00:01) is known to be > 0.5.
        assert_eq!(result, 60.0);
    }

    #[test]
    fn equality_does_not_match_strict_gt() {
        let result = time_gt_over_time(
            [
                s(0, 0.9),
                s(60, 0.9),
            ],
            TimeRange::new(0, 60_000),
            0.9,
            LocfPolicy {
                max_gap_ms: 60_000,
            },
        );

        assert_eq!(result, 0.0);
    }

    #[test]
    fn nan_value_is_unknown_not_false() {
        let result = time_gt_over_time(
            [
                s(0, f64::NAN),
                s(30, 1.0),
                s(60, 1.0),
            ],
            TimeRange::new(0, 60_000),
            0.5,
            LocfPolicy {
                max_gap_ms: 60_000,
            },
        );

        // [0,30) is unknown; [30,60) is above threshold.
        assert_eq!(result, 30.0);
    }

    #[test]
    fn empty_input_returns_nan() {
        let result = time_gt_over_time(
            [],
            TimeRange::new(0, 60_000),
            0.5,
            LocfPolicy {
                max_gap_ms: 60_000,
            },
        );

        assert!(result.is_nan());
    }
}
```


## Reusable duration primitive

You will likely want the complete comparison family:

```text
time_gt_over_time(v[range], threshold)
time_ge_over_time(v[range], threshold)
time_lt_over_time(v[range], threshold)
time_le_over_time(v[range], threshold)
time_eq_over_time(v[range], threshold)
time_ne_over_time(v[range], threshold)
```

Avoid six scanners. Use a comparison enum:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Comparison {
    Gt,
    Ge,
    Lt,
    Le,
    Eq,
    Ne,
}

impl Comparison {
    #[inline]
    pub fn matches(self, value: f64, threshold: f64) -> bool {
        match self {
            Self::Gt => value > threshold,
            Self::Ge => value >= threshold,
            Self::Lt => value < threshold,
            Self::Le => value <= threshold,
            Self::Eq => value == threshold,
            Self::Ne => value != threshold,
        }
    }
}
```

Then generalize the function:

```rust
pub fn time_compare_over_time<I>(
    samples: I,
    range: TimeRange,
    threshold: f64,
    comparison: Comparison,
    policy: LocfPolicy,
) -> f64
where
    I: IntoIterator<Item = Sample>,
{
    if range.start_ms >= range.end_ms || threshold.is_nan() {
        return f64::NAN;
    }

    let mut iter = samples.into_iter();
    let Some(mut previous) = iter.next() else {
        return f64::NAN;
    };

    let mut matched_ms = 0_u64;
    let max_gap_ms = policy.max_gap_ms.min(i64::MAX as u64) as i64;

    for current in iter {
        matched_ms = matched_ms.saturating_add(interval_compare_duration_ms(
            previous,
            current.ts_ms,
            range,
            threshold,
            comparison,
            max_gap_ms,
        ));

        if current.ts_ms >= range.end_ms {
            return matched_ms as f64 / 1_000.0;
        }

        previous = current;
    }

    matched_ms = matched_ms.saturating_add(interval_compare_duration_ms(
        previous,
        range.end_ms,
        range,
        threshold,
        comparison,
        max_gap_ms,
    ));

    matched_ms as f64 / 1_000.0
}

#[inline]
fn interval_compare_duration_ms(
    sample: Sample,
    next_ts_ms: TimestampMs,
    range: TimeRange,
    threshold: f64,
    comparison: Comparison,
    max_gap_ms: i64,
) -> u64 {
    if sample.value.is_nan() || next_ts_ms <= sample.ts_ms {
        return 0;
    }

    let represented_end_ms = min(
        next_ts_ms,
        sample.ts_ms.saturating_add(max_gap_ms),
    );

    let start_ms = max(sample.ts_ms, range.start_ms);
    let end_ms = min(represented_end_ms, range.end_ms);

    if end_ms <= start_ms || !comparison.matches(sample.value, threshold) {
        return 0;
    }

    (end_ms - start_ms) as u64
}

pub fn time_gt_over_time<I>(
    samples: I,
    range: TimeRange,
    threshold: f64,
    policy: LocfPolicy,
) -> f64
where
    I: IntoIterator<Item = Sample>,
{
    time_compare_over_time(
        samples,
        range,
        threshold,
        Comparison::Gt,
        policy,
    )
}
```


## Storage cursor interface

The evaluator must retrieve one predecessor point. A range scan restricted only to samples inside the range cannot compute the interval from the left boundary to the first in-window sample.

```rust
pub trait SeriesCursor {
    type Error;

    /// Retrieves the latest sample whose timestamp is <= `ts_ms`.
    fn seek_le(&mut self, ts_ms: TimestampMs) -> Result<Option<Sample>, Self::Error>;

    /// Iterates samples with timestamps > `ts_ms` in ascending order.
    fn scan_gt<'a>(
        &'a mut self,
        ts_ms: TimestampMs,
    ) -> Result<Box<dyn Iterator<Item = Result<Sample, Self::Error>> + 'a>, Self::Error>;
}
```

At query execution:

```rust
pub fn eval_time_gt<C>(
    cursor: &mut C,
    range: TimeRange,
    threshold: f64,
    policy: LocfPolicy,
) -> Result<f64, C::Error>
where
    C: SeriesCursor,
{
    let mut samples = Vec::new();

    if let Some(predecessor) = cursor.seek_le(range.start_ms)? {
        samples.push(predecessor);
    }

    for item in cursor.scan_gt(range.start_ms)? {
        let sample = item?;
        let reached_end = sample.ts_ms >= range.end_ms;
        samples.push(sample);

        if reached_end {
            break;
        }
    }

    Ok(time_gt_over_time(samples, range, threshold, policy))
}
```

The production implementation should avoid materializing `Vec<Sample>` and feed a chained predecessor-plus-decoder iterator directly to `time_gt_over_time`. The important operational requirements are:

- obtain the last sample `<= range.start`;
- read samples after that point in timestamp order;
- stop after the first sample `>= range.end`, since it closes the preceding interval;
- retain `max_gap` as a query-policy parameter or module configuration rather than silently holding indefinitely.


## Optional linear variant

For continuously varying sensor values, LOCF is not always suitable. A linear threshold-crossing variant can estimate the crossing timestamp between two samples:

$$
t_\times =
t_0 + (t_1 - t_0)
\frac{\theta - v_0}{v_1-v_0}
$$

For example, `80 → 100` over 10 minutes with a threshold of `90` contributes 5 minutes above threshold, not 0 or 10 minutes.

I would implement that separately:

```promql
time_gt_linear_over_time(temperature_celsius[1h], 80)
```

rather than adding a string mode argument. It needs different handling for `NaN`, equal timestamps, `+/-Inf`, maximum gaps, and exact equality at crossings. Keep the LOCF function as the initial implementation: it has clear semantics for monitoring gauges and operational state.

## Companion functions

This function is most useful when paired with:

```promql
share_time_gt_over_time(v[window], threshold)
observed_duration_over_time(v[window])
coverage_over_time(v[window])
```

For example:

```promql
(
  time_gt_over_time(
    node_cpu_utilization_ratio{job="node"}[30m],
    0.90
  ) > 1_200
)
and
(
  coverage_over_time(
    node_cpu_utilization_ratio{job="node"}[30m]
  ) >= 0.95
)
```

That fires only if CPU was above 90% for more than 20 minutes of the past 30 minutes **and** at least 95% of the time window had valid coverage.



---

# Review: implementing `time_gt_over_time` in this engine

**Reviewed:** 2026-09-30, against the engine as of `b4dd222f4` (with `state_duration_over_time`
push-down in place). The resulting implementation plan is
[time-gt-over-time-plan.md](time-gt-over-time-plan.md).

The sections above are sound on the math, but they were written without this engine in view.
One assumption, the predecessor sample, conflicts with how rollups are evaluated here and needs a
decision before implementation. The rest are corrections and simplifications.

## 1. Blocker: the predecessor sample vs the `RollupKind` contract

The sketch requires "the last sample `<= range.start`" (`SeriesCursor::seek_le`). The engine
cannot supply that today without breaking push-down:

- **Local path:** the matrix selector loads exactly `(t − range, t]`
  ([`window_range`](../../src/promql/functions/rollups.rs)). No sample before the window is in view.
- **Grid push-down path:** loads the union of the grid's windows, so
  `RollupWindow::prev_value` is populated only for interior windows
  ([`RollupWindow`](../../src/promql/functions/types.rs)).
- **Contract:** every `RollupKind` must reduce a window identically with and without its
  neighbours in view. `every_kind_ignores_samples_outside_the_window`
  ([`rollup_functions.rs`](../../src/promql/functions/rollup_functions.rs)) enforces it. A
  function that reads the predecessor would answer differently depending on whether push-down ran.

Options:

1. **In-window-only semantics (recommended for v1).** Coverage starts at the first sample inside
   the window; `(W_s, t_first)` is *uncovered*, not "below threshold". This matches
   `state_duration_over_time` (`state_duration_ignores_runs_before_the_window`), keeps the function
   an ordinary pushable `RollupKind`, and passes the invariant test unchanged. The cost is a left
   edge undercounted by at most one scrape interval, which `share`/coverage report honestly.
2. **Predecessor semantics.** Both paths must load one extra sample: the local selector widened,
   and each shard fetching one point before the grid union's start. That needs a second rollup
   class that is allowed context, with its own invariant test. This is a design change, not a
   storage detail, and should get its own plan if wanted.

## 2. Semantic corrections

- **Window bounds.** The sections above use half-open `[W_s, W_e)`. The engine, like Prometheus 3,
  uses `(W_s, W_e]`. In the "irregular scrapes" example, the `00:00` sample is **not** in
  `cpu[10m]` evaluated at `00:10`. `time_gt` is unchanged (480 s), but a covered-duration
  denominator becomes 540 s, not 600 s.
- **The worked examples disagree.** The "irregular scrapes" example gives 480 s with unbounded
  LOCF; the Rust sketch gives 360 s for the same data with `max_gap = 5m`. Both follow from their
  assumptions, but the first relies on the unbounded carry-forward that "Staleness and maximum
  hold" forbids. Treat the bounded one as canonical.
- **Empty input.** The sketch returns `NaN`. The engine convention is no output sample for an empty
  window ([`rollups.rs`](../../src/promql/functions/rollups.rs)), and `state_duration_over_time`
  does the same (its decision D3). Apply that to zero covered duration too.
- **`NaN` threshold.** The sketch returns `NaN`. Since `x > NaN` is always false, `0` is the natural
  answer. Either is defensible; decide explicitly.
- **`NaN` samples.** For `time_gt_over_time` alone, "unknown" and "false" give the same result,
  since neither adds matched time. The distinction only matters for `share`/coverage. Treating
  `NaN` as unknown also ends coverage at a `STALE_NAN` marker
  ([`model.rs`](../../src/promql/model.rs)) for free.

## 3. Where `max_gap` lives

The contract calls `max_gap` mandatory, but the PromQL signature has two arguments and
`LocfPolicy` has no default. Proposal:

- **Optional third string argument:** `time_gt_over_time(x[30m], 0.9, "2m")`. The earlier
  objection to string arguments is outweighed by precedent: `state_duration_over_time`'s `unit`
  already does this, and `parse_unit` ([`state.rs`](../../src/promql/functions/state.rs)) provides
  the suffix-required rule, parse-once-per-call and the error path.
- **Default to the query's `lookback_delta`.** It is documented and configurable, so it is not the
  "undocumented scrape-interval heuristic" warned about above.
- **Carry it on the wire** as a `GridRollup` field, like `unit_ms`. A shard must reduce with exactly
  the coordinator's value; shards reading their own config would let the two paths drift.

## 4. Simplifications to the sketch

- **Drop the out-of-order branch.** Storage hands the reducer sorted, deduplicated slices, so it
  never runs. Duplicate timestamps become zero-length intervals and cost nothing.
- **Drop the scaffolding.** `SeriesCursor`, `eval_time_gt` and the local
  `Sample`/`TimeRange`/`LocfPolicy` types don't fit here. The real shape is a reducer over
  `&[Sample]` plus the window end, called through a `RollupImpl` arm, like `state_duration_window`.
  With in-window semantics, `max(sample.ts, range.start)` is always `sample.ts`.
- **Keep the integer arithmetic.** Summing `i64` milliseconds and dividing once is right and matches
  how `state_duration` stays exact. Use `Timestamp` throughout instead of the `u64`/`i64`
  conversions for `max_gap`.
- **Build the `Comparison` enum from day one**, even if only `gt` ships. The registry is keyed by
  function name, so expect one `RollupKind` (and one proto enum value) per operator, all sharing
  one scanner. `eq`/`ne` compare exactly, consistent with `changes()`/`resets()` (no tolerance).
- **Threshold as a push-down parameter:** follow the path `state` already takes through
  `RollupKind::check_params`. A threshold that is not constant across steps keeps the call local.

## 5. Scope and naming

- **Settle the name** on `time_gt_*` (per "Recommended names" above); `time_above_*` and
  `share_time_above_*` elsewhere in this document are superseded.
- **Ship coverage alongside it.** On its own, `time_gt_over_time` returns `0` both for "below
  threshold" and for "no coverage", and the alert examples above depend on `coverage_over_time`.
  Shipping `time_gt` alone invites unsafe alert rules.
- **Check the MetricsQL name first (unverified).** MetricsQL's
  `duration_over_time(m[d], max_interval)` is believed to mean "seconds the series was present,
  with a maximum-gap argument", which is essentially `observed_duration_over_time`. This
  document's summary table already lists `duration_over_time`. Confirm against the MetricsQL
  docs and reuse the name and argument convention if it matches.
- **Write a requirements doc** in the style of
  [`state-duration-over-time-requirements.md`](state-duration-over-time-requirements.md), with
  numbered decisions: window semantics (§1), `max_gap` default and wire field (§3), empty/`NaN`
  rules (§2) and the operator set (§4). The material above is research, not a contract to
  implement against.

**Bottom line:** `time_gt_over_time` is cheap (one pass, `O(1)` state) and fits the existing rollup
and push-down machinery closely, provided it uses in-window semantics. The predecessor-sample
design is the one part that does not fit this engine.
