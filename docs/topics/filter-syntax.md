# FILTER Argument Syntax

## Overview

The `FILTER` argument in `TS.MRANGE`, `TS.MREVRANGE`, `TS.QUERYINDEX`, `TS.QUERYLABELS` and `TS.MDEL` commands allows
you to select which time series to query based on their labels. You can provide one or more filter expressions to match
series with specific label values.

## Syntax

The filter syntax comes in 2 flavors:

- **Basic**: `region=us-east-1` or `sensor_type=(temperature,humidity)`
- **Prometheus style**: `metric_name{label1="value1",label2="value2",...}`

### Basic Filter Syntax (RedisTimeSeries compatible)

The basic filter syntax is a simplified format that focuses on label-value pairs without requiring metric names or
complex expressions. It supports the following formats:

Find series with a specific label value:

```aiignore
label=value 
```

Example: `region=us-east-1` matches series where the `region` label is exactly `us-east-1`.

Find series where a label exists but has a different value:

```aiignore
label!=value 
```

Example: `region!=us-east-1` matches series where the `region` label exists but is not `us-east-1`.

Find series where a label matches any value in a list:

```aiignore
label=(value1,value2,...) 
```

Example: `region=(us-east-1,us-west-2)` matches series where the `region` label is either `us-east-1` or `us-west-2`.

Find series where a label does not match any value in a list:

```aiignore
label!=(value1,value2,...) 
```

Example: `region!=(us-east-1,us-west-2)` matches series where the `region` label exists but is not `us-east-1` or
`us-west-2`.

Find series where a label is absent (`label=`) or present with any value (`label!=`).

#### How basic filters are read

A basic filter is read exactly as RedisTimeSeries reads it. The label is the text before the first operator and the
value is the text after it, **as written**:

- Nothing is trimmed: `host =a` names the label `host ` (with a trailing space), and `t=( humidity)` lists the value
  ` humidity`.
- Quotes are ordinary characters: `host="10.0.0.1"` matches a stored value that includes the quote marks. To match
  `10.0.0.1`, write `host=10.0.0.1`.
- Any other character is literal, including dots, spaces, `#`, `/`, `@`, non-ASCII text and the word `or`:
  `ip=10.0.0.1`, `city=Zürich`, `color=#ff0000` and `state=OR` all match those exact values.
- A value may not contain `=`, `,`, `(` or `)` outside the list form, and list items may not be empty or contain them.
  RedisTimeSeries gives such filters no consistent meaning (`e=x=y` is read as `e=x`), so they are rejected here. To
  match a value with those characters, use the Prometheus-style form, where values are quoted strings:
  `{e="x=y"}`.
- `or` does not combine basic filters: `a=1 or b=2` is one filter whose value is `1 or b=2` (rejected, because of the
  `=`). Write `{a="1" or b="2"}` to OR label matchers.

The extension operators `=~`, `!~`, `^=` and `^~` are also accepted in basic form. The regex operators take a quoted
pattern (`instance=~"host0[12]"`); `^=` and `^~` take raw values like `=`.

### Prometheus-style Filter Syntax

The Prometheus-style filter syntax allows for more complex queries using metric names and label matchers.
A series selector can take several forms:

**Metric Name Only**

```
metric_name
```

Matches all series with the given metric name.

***Example:***

```
temperature
```

**Metric Name with Labels**

```
metric_name{label1="value1",label2="value2",...}
```

Matches series with the specified metric name and label values.

***Example:***

```
temperature{city="NYC",sensor="indoor"}
```

As in Prometheus, a selector that names its metric before the braces must not name it again
inside them. `temperature{__name__="humidity"}`, `temperature{__name__=~"temp.*"}` and
`temperature{humidity}` are rejected with `metric name must not be set twice`, whatever the
operator, and so is the same thing in any `or` branch. To match on `__name__` with an operator,
leave out the outer name: `{__name__=~"temp.*", city="NYC"}`.

One trailing comma before `}` is allowed (`{city="NYC",}`), as in PromQL; an empty item
(`{,city="NYC"}`, `{a="1",,b="2"}`) is rejected.

**Label Matchers Only**

```
{label1="value1",label2="value2",...}
```

Matches any series with the specified label values, regardless of metric name.

***Example:***

```
{city="NYC",sensor="indoor", type="temp"}
```

### Label Matcher Operators

Each label matcher supports different comparison operators:

| Operator | Description                  | Example        |
|----------|------------------------------|----------------|
| `=`      | Exact match                  | `city="NYC"`   |
| `!=`     | Not equal                    | `city!="NYC"`  |
| `=~`     | Regular expression match     | `city=~"NY.*"` |
| `!~`     | Regular expression not match | `city!~"LA.*"` |

### Label Value Quoting

In the Prometheus-style form, label values are quoted using:

- **Double quotes**: `"value"`
- **Single quotes**: `'value'`
- **Backticks**: `` `value` ``

Escape sequences are supported within double- and single-quoted strings (e.g., `\"`, `\n`, `\\`); backtick strings
are raw. Basic filters (above) are never unquoted.

### Multiple Filters

You can specify multiple series selectors, separated by spaces. Series matching **any** of the selectors will be
included (logical OR).

**Example:**

```
FILTER temperature{city="NYC"} humidity{city="LA"}
```

This matches:

- All temperature series in NYC, OR
- All humidity series in LA

### Complete Examples

### Basic Range Query

```
TS.MRANGE 1609459200000 1609545600000 FILTER temperature{city="NYC"}
```

Returns all NYC temperature series between the specified timestamps.

### Multiple Label Matchers

```
TS.MRANGE - + FILTER temperature{city="NYC",sensor!="outdoor"}
```

Returns temperature series in NYC, excluding outdoor sensors, for the entire time range.

### Regular Expression Matching

```
TS.MRANGE - + FILTER {city=~"NY.*",sensor_type="temp"}
```

Returns series in cities starting with "NY" where sensor_type is "temp".

### Multiple Selectors

```
TS.MRANGE - + FILTER temperature{city="NYC"} temperature{city="LA"} humidity
```

Returns:

- Temperature series from NYC or LA, OR
- Any humidity series

### Aggregation

```
TS.MRANGE - + AGGREGATION avg 3600000 FILTER temperature{location=~"warehouse.*"}
```

Returns hourly averages for temperature series from warehouses.

## Notes

- Filter expressions are **case-sensitive** for label names and values.
- The `FILTER` keyword itself is **case-insensitive**.
- At least one filter expression is **required** for `TS.MRANGE` and `TS.MREVRANGE` commands.
- Series selectors follow Prometheus query language conventions for label matching.
- Label names must start with a letter, underscore, or colon, and can contain letters, numbers, underscores, colons, and
  dots.

## Related Commands

- `TS.MRANGE` - Query multiple time series with timestamps in ascending order
- `TS.MREVRANGE` - Query multiple time series with timestamps in descending order
- `TS.QUERYINDEX` - Query the label index without retrieving sample data

## See Also

- [TS.MRANGE Command Documentation](./commands/mrange.md)
