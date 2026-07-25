# TS._DEBUG

Internal diagnostic and introspection tool for the valkey-timeseries module.

> **Note:** `TS._DEBUG` is intended for operators and developers. Its subcommands, output formats, and behavior may
> change between versions without notice. Do not rely on this command in production application logic.

## Enabling

`TS._DEBUG` is disabled by default. Every subcommand is rejected until the `debug-mode`
configuration parameter is enabled:

```
TS._DEBUG HELP
(error) TSDB: TS._DEBUG is disabled. Set the 'ts.debug-mode' configuration parameter to yes to enable it
```

Enable it at runtime:

```
CONFIG SET ts.debug-mode yes
```

or at startup, as a module load argument (unprefixed there — the `ts.` prefix applies only to
`CONFIG GET`/`CONFIG SET`):

```
loadmodule /path/to/libvalkey_timeseries.so debug-mode yes
```

The setting takes effect immediately and can be turned back off with `CONFIG SET ts.debug-mode no`.

## Syntax

````aiignore
TS._DEBUG <subcommand> [arguments]</subcommand>
````

## Subcommands

| Subcommand        | Description                                             |
|-------------------|---------------------------------------------------------|
| `HELP`            | Display available subcommands and brief descriptions    |
| `STRINGPOOLSTATS` | Report statistics for the global string interning pool  |
| `LIST_CONFIGS`    | List module configuration parameters and current values |

---

### TS._DEBUG HELP

Displays the available `TS._DEBUG` subcommands and their descriptions.

### Syntax

```bash
TS._DEBUG HELP
```

### Return Value

A flat array of alternating command name and description strings.

### Example

```
TS._DEBUG HELP
```

```
1) "TS._DEBUG SHOW_INFO"
2) "Show Info Variable Information"
3) "TS._DEBUG STRINGPOOLSTATS [TOPK]"
4) "Show String Interner Stats"
5) "TS._DEBUG LIST_CONFIGS [VERBOSE] [APP|DEV|HIDDEN]"
6) "List config names (default) or VERBOSE details, optionally filtered by visibility"
```

---

### TS._DEBUG STRINGPOOLSTATS

Returns memory usage and efficiency statistics for the global string interning pool. The pool deduplicates repeated
label names and values across all time series.

> **Note:** In cluster mode, this command reports statistics for the local node only.

### Syntax

```bash
TS._DEBUG STRINGPOOLSTATS [k]
```

### Arguments

| Argument | Type    | Required | Description                                                                             |
|----------|---------|----------|-----------------------------------------------------------------------------------------|
| `k`      | integer | No       | If provided and greater than `0`, include top-K entries ranked by ref count and by size |

### Return Value

Without `k` (or `k = 0`), returns an array of 4 elements:

| Index | Name            | Description                                                               |
|-------|-----------------|---------------------------------------------------------------------------|
| 0     | `GlobalStats`   | Aggregate statistics across all interned strings (see **BucketStats**)    |
| 1     | `ByRefcount`    | Array of `[refCount, BucketStats]` pairs, grouped by external ref count   |
| 2     | `BySize`        | Array of `[size, BucketStats]` pairs, grouped by string byte length       |
| 3     | `MemorySavings` | Estimated bytes and percentage saved by interning (see **MemorySavings**) |

When `k > 0`, two additional elements are appended:

| Index | Name         | Description                                                            |
|-------|--------------|------------------------------------------------------------------------|
| 4     | `TopKByRef`  | Top-K interned strings by external reference count (see **TopKEntry**) |
| 5     | `TopKBySize` | Top-K interned strings by allocated byte size (see **TopKEntry**)      |

#### BucketStats fields

Each `BucketStats` entry is a flat array of 12 alternating key/value fields:

| Field          | Type    | Description                                              |
|----------------|---------|----------------------------------------------------------|
| `count`        | integer | Number of distinct interned strings in this bucket       |
| `bytes`        | integer | Total logical byte length of strings in this bucket      |
| `avgSize`      | float   | Average logical byte length per string                   |
| `allocated`    | integer | Total allocated memory (including Arc overhead) in bytes |
| `avgAllocated` | float   | Average allocated memory per string                      |
| `utilization`  | integer | Ratio of used bytes to allocated bytes, as a percentage  |

#### MemorySavings fields

A flat array of 4 alternating key/value fields:

| Field              | Type    | Description                                                                  |
|--------------------|---------|------------------------------------------------------------------------------|
| `memorySavedBytes` | integer | Estimated bytes saved by sharing interned strings across multiple references |
| `memorySavedPct`   | float   | Percentage saved relative to the hypothetical cost without interning         |

#### TopKEntry fields

Each `TopKEntry` is a flat array of 8 alternating key/value fields:

| Field       | Type    | Description                                                        |
|-------------|---------|--------------------------------------------------------------------|
| `value`     | string  | The interned string value                                          |
| `refCount`  | integer | Number of external references (excluding the pool's own reference) |
| `bytes`     | integer | Logical byte length of the string                                  |
| `allocated` | integer | Total allocated memory for this string (Arc overhead + data)       |

### Examples

Basic statistics (no top-K):

```
TS._DEBUG STRINGPOOLSTATS
```

Statistics with top 10 strings by ref count and size:

```aiignore
TS._DEBUG STRINGPOOLSTATS 10
```

---

### TS._DEBUG LIST_CONFIGS

Lists the module's configuration parameters. In compact mode (default), returns only parameter names. In verbose mode,
returns detailed metadata and the current runtime value for each parameter.

### Syntax

```bash
TS._DEBUG LIST_CONFIGS
```

---

### TS._DEBUG LIST_CONFIGS

Lists the module's configuration parameters. In compact mode (default), returns only parameter names. In verbose mode,
returns detailed metadata and the current runtime value for each parameter.

### Syntax

```bash
TS._DEBUG LIST_CONFIGS [VERBOSE]
```

### Arguments

| Argument  | Required | Description                                                                  |
|-----------|----------|------------------------------------------------------------------------------|
| `VERBOSE` | No       | When present, includes metadata and current values for each config parameter |

### Return Value

**Without `VERBOSE`:** A flat array of configuration parameter name strings.

**With `VERBOSE`:** An array where each element is a flat array of 12 alternating key/value fields:

| Field     | Type   | Description                                                     |
|-----------|--------|-----------------------------------------------------------------|
| `name`    | string | Configuration parameter name                                    |
| `type`    | string | Value type: `integer`, `float`, `string`, `duration`, or `enum` |
| `default` | varies | Default value for the parameter                                 |
| `min`     | varies | Minimum allowed value, or `"none"` if unbounded                 |
| `max`     | varies | Maximum allowed value, or `"none"` if unbounded                 |
| `value`   | varies | Current runtime value                                           |

### Configuration Parameters

| Parameter                      | Type     | Default         | Description                                                                             |
|--------------------------------|----------|-----------------|-----------------------------------------------------------------------------------------|
| `ts-chunk-size-bytes`          | integer  | 4096            | Maximum memory per time series chunk, in bytes                                          |
| `ts-encoding`                  | enum     | `COMPRESSED`    | Default chunk encoding: `CHIMP`, `GORILLA` or `UNCOMPRESSED`                            |
| `ts-duplicate-policy`          | enum     | `BLOCK`         | Policy for handling duplicate timestamps: `BLOCK`, `FIRST`, `LAST`, `MIN`, `MAX`, `SUM` |
| `ts-retention-policy`          | duration | `0` (no expiry) | Default retention period (milliseconds)                                                 |
| `ts-compaction-policy`         | string   | `None`          | Default compaction rules applied to all new time series                                 |
| `ts-compatibility-mode`        | enum     | `EXTENDED`      | Which side of a value divergence from RedisTimeSeries to take: `EXTENDED` or `STRICT`   |
| `ts-decimal-digits`            | integer  | `none`          | Round sample values to N decimal places; `none` disables rounding                       |
| `ts-significant-digits`        | integer  | `none`          | Round sample values to N significant digits; `none` disables rounding                   |
| `ts-ignore-max-time-diff`      | duration | `0ms`           | Max time delta (ms) for which a duplicate sample is silently ignored                    |
| `ts-ignore-max-val-diff`     | float    | 0.0             | Max value delta for which a duplicate sample is silently ignored                        |
| `ts-num-threads`               | integer  | 8               | Number of worker threads for parallel query processing                                  |
| `ts-fanout-command-timeout`    | duration | —               | Timeout (ms) for fanout (cluster scatter/gather) commands                               |
| `ts-cluster-map-expiration-ms` | duration | —               | How long (ms) cluster slot-map entries are cached; `0` disables caching                 |
| `ts-fanout-aggregation-pushdown` | boolean | `yes`          | Shard-side aggregation/reduce push-down for clustered MRANGE. Not needed for rolling upgrades (version skew is handled automatically); disable only as an emergency/diagnostic revert to the coordinator-side path |

### Examples

List all config parameter names:

```valkey-cli
TS._DEBUG LIST_CONFIGS
```

```valkey-cli
1) "ts-chunk-size-bytes"
2) "ts-encoding"
3) "ts-duplicate-policy" ...
```

List configs with full metadata and current values:

```valkey-cli
TS._DEBUG LIST_CONFIGS VERBOSE
```
