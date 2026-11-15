# FILTER Argument Syntax

## Overview

The `FILTER` argument of the multi-series commands (`TS.MRANGE`, `TS.MREVRANGE`, `TS.MGET`, `TS.MDEL`, `TS.QUERYLABELS`,
`TS.CARD`, the label-discovery commands, and others) selects series by their labels. `TS.QUERYINDEX` takes the same
selectors directly, without the `FILTER` keyword. Each selector is one command argument, and a command can take
several (see [Combining selectors](#combining-selectors)).

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

Example: `region!=us-east-1` matches series whose `region` label is not `us-east-1`, including series that have no
`region` label at all (as in RedisTimeSeries and Prometheus).

Find series where a label matches any value in a list:

```aiignore
label=(value1,value2,...) 
```

Example: `region=(us-east-1,us-west-2)` matches series where the `region` label is either `us-east-1` or `us-west-2`.

Find series where a label does not match any value in a list:

```aiignore
label!=(value1,value2,...) 
```

Example: `region!=(us-east-1,us-west-2)` matches series whose `region` label is neither `us-east-1` nor `us-west-2`,
including series without a `region` label.

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

| Operator | Description                                   | Example        |
|----------|-----------------------------------------------|----------------|
| `=`      | Exact match                                   | `city="NYC"`   |
| `!=`     | Not equal (also matches series without it)    | `city!="NYC"`  |
| `=~`     | Regular expression match (fully anchored)     | `city=~"NY.*"` |
| `!~`     | Regular expression not match                  | `city!~"LA.*"` |
| `^=`     | Starts with (extension)                       | `city^="New"`  |
| `^~`     | Does not start with (extension)               | `city^~"San"`  |

`=`, `!=`, `^=` and `^~` also take a value list: `{city=("NYC","LA")}` matches either value. A list needs at
least one value, separated by single commas: `()`, `(,"NYC")` and `("NYC",)` are rejected. An empty value means the
label is absent: `humidity{city=""}` matches humidity series without a `city` label, and `humidity{city!=""}` those
that have one. (On its own, `{city=""}` has no positive matcher and is rejected; see below.)

Matchers inside one pair of braces are ANDed. `or` between groups of matchers ORs them, and `or` between whole
selectors does the same: `{city="NYC" or city="LA"}` and `temperature{city="NYC"} or humidity` are single selectors.

### Regular Expression Syntax

Regular expressions follow Prometheus: [RE2 syntax](https://github.com/google/re2/wiki/Syntax),
as Go's `regexp` package reads it.

- **Fully anchored.** `city=~"NY"` matches only `NY`, not `NYC`; write `NY.*` for a prefix.
  Explicit `^` and `$` are allowed and change nothing.
- **`.` matches any character, newline included.**
- **`\d`, `\w`, `\s` and `\b` are ASCII**, as in Go: `\d` is `[0-9]`, `\w` is `[0-9A-Za-z_]`,
  `\s` is `[\t\n\f\r ]`. Use `\pN` or `\pL` to match Unicode digits or letters.
- **A `{` that does not start a repeat is a literal**: `abc{def}` matches the text `abc{def}`.
- **Repeat counts are limited to 1000**, including the product of nested counts: `(a{10}){100}`
  is accepted, `(a{10}){101}` is not.
- `\Q...\E` quotes literal text, and `[[:alpha:]]`-style POSIX classes are supported.

A pattern that is valid but compiles to more than 4 MiB is rejected with an error saying it is
too large. In practice this only happens with long repeats of large Unicode classes, such as
`\pL{1000}`.

Two kinds of pattern that Prometheus accepts are rejected:

- Patterns that only parse because Prometheus inserts the pattern into its own `^(?s:...)$`
  wrapper, such as an unbalanced `)(`.
- A repeat applied to a bare flag group, such as `(?i)*`.

### Label Value Quoting

In the Prometheus-style form, label values are quoted using:

- **Double quotes**: `"value"`
- **Single quotes**: `'value'`
- **Backticks**: `` `value` ``

Quoted strings follow PromQL's rules, which are Go's:

- In double- and single-quoted strings a backslash starts an escape: `\a \b \f \n \r \t \v \\`, the enclosing quote
  (`\"` in `"..."`, `\'` in `'...'`; the other quote needs no escape), `\xHH` or `\NNN` (a byte, in hex or three octal
  digits; bytes may combine into UTF-8, so `\xc3\xa9` is `é`), and `\uHHHH` / `\UHHHHHHHH` (a Unicode character). Any
  other escape is an error, as is a raw newline (write `\n`).
- Backtick strings are raw: no escapes, so `` `C:\` `` is `C:\`, and they cannot contain a backtick.

The same rules apply to `METRIC` label values and to the quoted pattern of `=~` / `!~` in a basic filter. Basic filter
values (above) are never unquoted.

### Combining selectors

Separate selector arguments are **ANDed**: a series is selected only if it matches every one of them, as in
RedisTimeSeries.

```
FILTER region=us-east-1 sensor_type=temperature
FILTER temperature{city="NYC"} sensor!=outdoor
```

The first selects series whose `region` is `us-east-1` **and** whose `sensor_type` is `temperature`. Two arguments
that name different metrics, such as `temperature{city="NYC"} humidity{city="LA"}`, select nothing.

At least one argument must contain a positive matcher (`label=value`, a list, a regex match, a metric name). A list of
only negative or absence matchers, such as `FILTER region!=eu` or `FILTER region=`, is rejected with
`TSDB: please provide at least one matcher`; it is accepted once another argument supplies the positive matcher
(`FILTER metric=cpu region!=eu`).

To select series that match **either** of two conditions, write one Prometheus-style selector with `or`. It is a
single argument, so quote it in a shell or `valkey-cli`:

```
FILTER 'temperature{city="NYC"} or humidity{city="LA"}'
```

This matches all temperature series in NYC, and all humidity series in LA. Basic filters cannot be combined with `or`
(see [How basic filters are read](#how-basic-filters-are-read)).

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
TS.MRANGE - + FILTER 'temperature{city=~"NYC|LA"} or humidity'
```

Returns temperature series from NYC or LA, and every humidity series. Written as three separate arguments
(`temperature{city="NYC"} temperature{city="LA"} humidity`), the same selectors would be ANDed and return nothing.

### Aggregation

```
TS.MRANGE - + AGGREGATION avg 3600000 FILTER temperature{location=~"warehouse.*"}
```

Returns hourly averages for temperature series from warehouses.

## Notes

- Filter expressions are **case-sensitive** for label names and values.
- The `FILTER` keyword itself is **case-insensitive**.
- At least one selector is **required**, and at least one positive matcher across all of them (see
  [Combining selectors](#combining-selectors)).
- Prometheus-style selectors follow Prometheus query language conventions for label matching.
- In the Prometheus-style form, an unquoted label name starts with a letter or underscore and may contain letters,
  digits, underscores, colons, dots and hyphens. Any other name can be quoted: `{"host name"="a"}`. In a basic filter
  the label name is the raw text before the operator.

## Related Commands

- `TS.MRANGE` - Query multiple time series with timestamps in ascending order
- `TS.MREVRANGE` - Query multiple time series with timestamps in descending order
- `TS.QUERYINDEX` - Query the label index without retrieving sample data

## See Also

- [TS.MRANGE Command Documentation](../commands/ts.mrange.md)
- [TS.QUERYINDEX Command Documentation](../commands/ts.queryindex.md)
