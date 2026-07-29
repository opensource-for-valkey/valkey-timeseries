# TS.MADD

Add multiple samples to multiple time series in a single command.

## Syntax

```
TS.MADD key timestamp value [key timestamp value ...]
```

## Description

The command accepts triplets of arguments: `key`, `timestamp`, and `value`. Each triplet adds a sample with the given
`timestamp` and `value` to the time series stored at `key`.

Unlike [`TS.ADD`](ts.add.md), `TS.MADD` does not create series: each `key` must already exist. A triplet naming a
missing key fails with a per-item error and creates nothing, leaving the rest of the batch unaffected.

## Required Arguments

- `key`: The name of the time series key
- `timestamp`: The timestamp of the sample (in milliseconds since epoch). Use `*` to use the server's current time
- `value`: The numeric value of the sample (float or integer)

At least one triplet must be provided, and the total number of arguments after the command name must be a multiple of 3.

## Return Value

An array of results, one for each sample added, in the order they were provided. Each result can be:

- A sample object (array with timestamp and value) representing the successfully added sample
- An error message string if the sample could not be added

## Errors

- `ERR wrong number of arguments` if the number of arguments is not a multiple of 3 or is less than 3
- `TSDB: invalid permissions` if the user lacks the required ACL permissions

Per-item errors are returned in place of that item's result and do not abort the rest of the batch:

- `TSDB: the key is not a TSDB key` if the key does not exist, or exists and holds another type
- `TSDB: invalid timestamp` if a timestamp cannot be parsed
- `TSDB: invalid value` if a value cannot be parsed as a number

## Examples

Add three samples to two different time series:

```
TS.MADD ts1 1000 10.5 ts2 2000 20.3 ts1 3000 15.2
```

Returns an array with three results, one for each sample addition.

Add samples using the current server time:

```
TS.MADD ts1 * 42.1 ts2 * 88.5
```

## See Also

- `TS.ADD` — Add a single sample to a time series
- `TS.ALTER` — Modify time series configuration