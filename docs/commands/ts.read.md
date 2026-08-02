# TS.READ

Read samples at or after a timestamp, optionally waiting for more to arrive.

`TS.READ` is the streaming counterpart to [`TS.RANGE`](./ts.range.md). Where `TS.RANGE` answers
"what is in this window", `TS.READ` answers "what is there from here on" — and, with `BLOCK`, will
wait for it. That makes it the building block for tailing a series: read, remember the last
timestamp you saw, and ask again from just past it.

## Syntax

```
TS.READ key timestamp [BLOCK milliseconds min_count] [MAX_COUNT max_count]
```

### Required Arguments

**key**: the time series key.

**timestamp**: the inclusive lower bound to read from. Either a non-negative millisecond
timestamp, or one of the sentinels below. A negative timestamp is rejected.

| Value | Meaning |
| --- | --- |
| *literal* | Start at exactly this timestamp, inclusive |
| `-` | Start at the earliest stored sample |
| `+` | Start at the newest stored sample, inclusive — returns exactly that one sample |
| `$` | Start just past the newest stored sample — returns nothing until new data arrives |

The sentinel is resolved **once**, when the command starts. A blocked client keeps the timestamp
it resolved at that moment; it does not re-resolve on each wakeup. This is what makes `$` mean
"only what arrives after I asked" rather than a cursor that forever outruns the data.

On a missing or empty series, all four forms return an empty array. With `BLOCK`, all four then
wait for the first sample to arrive — there is no stored data for `-`, `+`, or `$` to anchor to,
so a reader on a not-yet-created key is woken by the first write.

## Optional Arguments

**BLOCK milliseconds min_count**: wait until at least `min_count` qualifying samples exist.
`milliseconds` is the wait limit; `0` waits indefinitely. `min_count` must be positive, and may
not exceed `MAX_COUNT` when both are given.

If the threshold is already met the command returns immediately without waiting. Otherwise it
waits until one of:

- **the threshold is met** — returns the samples;
- **the timeout elapses** — returns whatever qualifies right now, even if that is fewer than
  `min_count` samples or none at all. A timeout is a normal reply, not an error;
- **the key is removed** — returns an empty array, successfully. This covers `DEL`, `UNLINK`,
  `FLUSHDB`/`FLUSHALL`, key expiry, and eviction.

**MAX_COUNT max_count**: return at most this many samples. Must be positive. When omitted, the
reply is unbounded — `TS.READ key -` with no cap reads the whole series.

Both clauses are optional, may appear in either order, and their keywords are case-insensitive.

## Return Value

A flat array of `[timestamp, value]` pairs in ascending timestamp order — the same shape as
[`TS.RANGE`](./ts.range.md). Under RESP2 values are bulk strings; under RESP3 they are doubles.
An empty result is an empty array under both protocols.

## Examples

Read from an inclusive cursor:

```
> TS.ADD temperature:room1 100 22.5
> TS.ADD temperature:room1 200 23.0
> TS.ADD temperature:room1 300 23.5
> TS.READ temperature:room1 200
1) 1) (integer) 200
   2) "23"
2) 1) (integer) 300
   2) "23.5"
```

Read just the newest sample, then only what comes after it:

```
> TS.READ temperature:room1 +
1) 1) (integer) 300
   2) "23.5"

> TS.READ temperature:room1 $
(empty array)
```

Wait up to 5 seconds for two new samples:

```
> TS.READ temperature:room1 $ BLOCK 5000 2
```

Take a page at a time:

```
> TS.READ temperature:room1 - MAX_COUNT 2
1) 1) (integer) 100
   2) "22.5"
2) 1) (integer) 200
   2) "23"
```

## Paging and tailing

To advance, resume from **one millisecond past the last timestamp you received**. The cursor is
inclusive, so re-using the last timestamp unchanged would return that sample a second time.

```
last = 0
loop:
    reply = TS.READ key <last == 0 ? "-" : last + 1> BLOCK 5000 1 MAX_COUNT 100
    if reply is empty: continue          # timed out; just ask again
    process(reply)
    last = reply[-1].timestamp
```

Two properties make this loop safe:

- **Reads do not consume.** Any number of clients can tail the same series; each gets every
  sample. `TS.READ` never removes or marks anything.
- **A timeout is not an error.** An empty reply means "nothing yet", so the loop simply
  re-issues.

Note that a sample written *out of order*, below a cursor you have already passed, will not be
returned by later calls — the cursor only moves forward. If backfill matters, re-query the window
with [`TS.RANGE`](./ts.range.md) rather than relying on the tail.

## Blocking inside MULTI, Lua, and other deny-blocking contexts

The order matters: the current data is evaluated **first**.

- If the threshold is already met, the command replies normally — including inside `MULTI` or
  `EVAL`, and including with `BLOCK 0`. It never needed to block, so nothing is denied.
- Only if the command would actually have to wait does it fail, with
  `TSDB: blocking TS.READ (with BLOCK) is not allowed inside MULTI, EVAL, or a deny-blocking
  context`.

Without `BLOCK`, `TS.READ` is an ordinary read and is always allowed.

## Cluster behavior

`TS.READ` is a single-key command in cluster mode; requests route or redirect to the shard owning
the key, and there is no fanout.

The blocking form follows the slot correctly too. If the slot migrates to another shard while a
client is waiting, the server releases that client with a `MOVED` naming the new owner, rather
than leaving it blocked on a key the node no longer serves. Handle it as you would a `MOVED` on
any other command — re-issue the read against the shard it points to. Cluster-aware clients do
this for you.

## Errors

| Condition | Error |
| --- | --- |
| Key holds a non-series value | `WRONGTYPE Operation against a key holding the wrong kind of value` |
| Negative or unparseable `timestamp` | `TSDB: invalid timestamp` |
| `MAX_COUNT` zero, negative, or non-integer | `TSDB: MAX_COUNT must be a positive integer` |
| `min_count` zero or negative | `TSDB: BLOCK min_count must be a positive integer` |
| Negative `milliseconds` | `TSDB: BLOCK milliseconds must be a non-negative integer` |
| `min_count` greater than `max_count` | `TSDB: BLOCK min_count must be <= MAX_COUNT` |
| Duplicate clause, missing value, or unknown token | `ERR wrong number of arguments for 'ts.read' command` |

`min_count > max_count` is rejected before the key is touched, so it errors even for a key that
does not exist.

Note that a missing key is **not** an error — it reads as an empty array, and with `BLOCK` it is a
valid thing to wait on.

## Complexity

O(log(n)+k), where n is the number of samples in the series and k is the number of returned
samples.

## See Also

- [TS.RANGE](./ts.range.md) – Query a bounded range of samples
- [TS.REVRANGE](./ts.revrange.md) – Query a range in descending order
- [TS.GET](./ts.get.md) – Get only the last sample
- [TS.ADD](./ts.add.md) – Add a sample to a time series
