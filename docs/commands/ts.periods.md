# TS.PERIODS

Detect seasonal periods in a time series using spectral analysis.

`TS.PERIODS` uses the SAZED ensemble to identify periodic patterns in the data. Each detected period is returned with metadata 
about its strength and reliability.

If the `DOMINANT` option is specified, only the single most significant period
is returned as an integer, or `nil` if no significant period is found.

## Syntax

```
TS.PERIODS key fromTimestamp toTimestamp [MIN_STRENGTH minStrength] [DOMINANT]
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
<summary><code>MIN_STRENGTH</code></summary>

Minimum seasonal differencing strength (0–1) for a period to be accepted.

* Default: `0.05`.
* Values > 0.6 indicate strong seasonality; < 0.3 is weak.
* Set to `0` to disable strength filtering entirely.

</details>

<details open>
<summary><code>DOMINANT</code></summary>

When specified, only the dominant (most significant) period is returned as an
integer value. Returns `nil` if no significant period is detected.

Without this option, all detected periods are returned as an array of arrays,
each containing: `[period, power, strength, acf, n_cycles]`.

* `period` — The detected period (integer number of observations per cycle).
* `power` — Spectral power at this period.
* `strength` — Seasonal differencing strength (0–1). Values > 0.6 indicate strong seasonality.
* `acf` — Autocorrelation at the detected lag. Positive values confirm a repeating pattern.
* `n_cycles` — Number of complete cycles of this period in the signal.

</details>

## Return

By default, `TS.PERIODS` returns an array where each element is itself an array of
5 elements representing a detected period:

```
[period, power, strength, acf, n_cycles]
```

When the `DOMINANT` option is used, returns an integer (the dominant period) or `nil`.

## Examples

### Detect all periods

```
127.0.0.1:6379> TS.CREATE ts:temperature
OK
127.0.0.1:6379> TS.ADD ts:temperature 1000 20.0
(integer) 1000
127.0.0.1:6379> TS.ADD ts:temperature 2000 22.0
(integer) 2000
127.0.0.1:6379> TS.ADD ts:temperature 3000 21.0
(integer) 3000
127.0.0.1:6379> TS.ADD ts:temperature 4000 23.0
(integer) 4000
127.0.0.1:6379> TS.ADD ts:temperature 5000 20.5
(integer) 5000
127.0.0.1:6379> TS.ADD ts:temperature 6000 22.5
(integer) 6000
127.0.0.1:6379> TS.PERIODS ts:temperature - +
1) 1) (integer) 2
   2) "0.5"
   3) "0.8"
   4) "0.7"
   5) (integer) 3
```

### Detect the dominant period only

```
127.0.0.1:6379> TS.PERIODS ts:temperature - + DOMINANT
(integer) 2
```
