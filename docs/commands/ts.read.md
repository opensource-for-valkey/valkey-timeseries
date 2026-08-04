# TS.READ

Read samples at or after a timestamp, optionally waiting for more to arrive.

`TS.READ` is the streaming counterpart to [`TS.RANGE`](./ts.range.md). Where `TS.RANGE` answers
"what is in this window", `TS.READ` answers "what is there from here on" — and, with `BLOCK`, will
wait for it. That makes it the building block for tailing a series: read, remember the last
timestamp you saw, and ask again from just past it.

## Syntax

```
TS.READ key timestamp [BLOCK milliseconds min_count] [MAX_COUNT max_count] [CONDITION op value]
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

The wait limit is either a bare number of milliseconds or a unit-suffixed duration — see
[Duration syntax](#duration-syntax) below. `BLOCK 5000 1`, `BLOCK 5s 1`, and `BLOCK 2h5m 1` all
name a wait limit; `BLOCK 0 1` and `BLOCK 0s 1` both wait indefinitely.

If the threshold is already met the command returns immediately without waiting. Otherwise it
waits until one of:

- **the threshold is met** — returns the samples;
- **the timeout elapses** — returns whatever qualifies right now, even if that is fewer than
  `min_count` samples or none at all. A timeout is a normal reply, not an error;
- **the key is removed** — returns an empty array, successfully. This covers `DEL`, `UNLINK`,
  `FLUSHDB`/`FLUSHALL`, key expiry, and eviction.

**MAX_COUNT max_count**: return at most this many samples. Must be positive. When omitted, the
reply is unbounded — `TS.READ key -` with no cap reads the whole series.

**CONDITION op value**: return only samples whose *value* satisfies the comparison. `op` is one of
`==`, `!=`, `>`, `>=`, `<`, `<=`, and `value` is a number.

All three clauses are optional, may appear in any order, and their keywords are case-insensitive.

### Duration syntax

The `BLOCK` wait limit accepts either spelling:

- **A bare number** — a count of milliseconds. `BLOCK 5000 1` waits up to five seconds.
- **A unit-suffixed duration** — a number followed by one of `ms`, `s`, `m`, `h`, `d`, `w`, or `y`
  (milliseconds, seconds, minutes, hours, days, weeks, years; a week is 7 days and a year 365).
  Segments may be chained and are summed, so `BLOCK 2h5m 1` waits up to two hours and five
  minutes. The number may be fractional — `1.5s` is 1500 ms — and the total is rounded to whole
  milliseconds.

A suffix is lowercase and joined to its number as a single argument, so `1S` and `3wk` are
rejected, as is any negative duration. An unparseable wait limit reports the same error as a
negative one: `TSDB: BLOCK milliseconds must be a non-negative integer`.

This is the same duration syntax `TS.CREATE` takes for `RETENTION` and `DEDUPE_INTERVAL`. The bare
millisecond form is the portable one — use it where the command also has to run against
RedisTimeSeries.

## CONDITION

> **Valkey TimeSeries extension.** RedisTimeSeries has no `CONDITION` clause and rejects it. It is
> available in both `ts-compatibility-mode` values.

The comparison reads **sample first**: `CONDITION > 500` keeps samples whose value is greater than
500. The two arguments are always separate tokens — `CONDITION > 500`, never `CONDITION >500` — and
the six operator spellings above are exact. `=` is not accepted as an alias for `==`.

The condition filters **every** `TS.READ`, not just a blocked one:

- Without `BLOCK`, the reply is the timestamp-eligible samples whose values also match.
- With `BLOCK`, `min_count` is the number of **matching** samples required to reply. A write that
  does not produce a match leaves the reader waiting.
- `MAX_COUNT` caps matching samples *after* filtering. It does not cap how many samples are
  examined to find them.
- On timeout, the reply is the matching partial result — an empty array when nothing matched.

A non-matching write does not extend the wait. Each wakeup re-evaluates the current data against
the condition and, if the threshold is still unmet, the original deadline stays in force: a wakeup
400 ms into `BLOCK 1000` leaves roughly 600 ms, not a fresh second.

Alert when a latency measurement exceeds 500 ms, waiting up to a minute:

```
> TS.READ api:latency:raw $ BLOCK 60000 1 CONDITION > 500
```

### NaN and infinity

A series can hold `nan`, `inf`, and `-inf`, and `value` accepts the same spellings a sample value
does. The six operators do not treat `nan` uniformly:

- `==` and `!=` have explicit NaN handling. `CONDITION == nan` selects exactly the NaN samples, and
  `CONDITION != nan` excludes them.
- `>`, `>=`, `<`, and `<=` are plain IEEE comparisons and are therefore **always false** against a
  NaN on either side. `CONDITION > nan` never matches anything, and a NaN sample never satisfies an
  ordering condition.

### In-place updates do not wake a conditioned reader

A blocked reader is woken when the series gains a sample. A write that rewrites an **existing**
timestamp in place — `TS.ADD` under a `DUPLICATE_POLICY`, or `TS.INCRBY`/`TS.DECRBY` at the last
timestamp — does not add one, so it does not wake anybody.

With a condition this is worth knowing, because such a write *can* change the matching set: a
sample rewritten from `100` to `600` newly satisfies `CONDITION > 500`, and the reader still stays
blocked. It sees the new value at its next wakeup from an appending write, or in its timeout
snapshot — never later than its deadline, because each evaluation re-reads current stored data.

Pair `CONDITION` with a finite `BLOCK` rather than `BLOCK 0` on a series maintained purely by
in-place updates.

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

Wait up to 5 seconds for two new samples — the wait limit may be written either way:

```
> TS.READ temperature:room1 $ BLOCK 5000 2
> TS.READ temperature:room1 $ BLOCK 5s 2
```

Take a page at a time:

```
> TS.READ temperature:room1 - MAX_COUNT 2
1) 1) (integer) 100
   2) "22.5"
2) 1) (integer) 200
   2) "23"
```

Alert when more than 5% of requests in a one-minute window take longer than 500 ms. A
[`TS.CREATERULE`](./ts.createrule.md) with `share(>500)` down-samples the raw latency values to
one fraction per minute; a worker then tails that compacted series, and `CONDITION` does the
threshold test on the server so the worker is woken only by a breach:

``` 
> TS.CREATE api:latency:raw
OK
> TS.CREATE api:latency:over_500ms:1m
OK
> TS.CREATERULE api:latency:raw api:latency:over_500ms:1m AGGREGATION share(>500) 1m 0
OK
```

Block until the condition is met. `share(>500)` returns a fraction from `0.0` to `1.0`, so `0.05` represents 5%. 
```
> TS.READ api:latency:over_500ms:1m $ BLOCK 60000 1 CONDITION > 0.05
```

Add some raw latency samples to see the alert in action:
```
> TS.ADD api:latency:raw 1000 450
OK
> TS.ADD api:latency:raw 2000 600
OK
> TS.ADD api:latency:raw 3000 700
OK
> TS.ADD api:latency:raw 4000 300
OK
> TS.ADD api:latency:raw 5000 800
OK
```

The worker is woken by the condition check on the server.


## Paging and tailing

To advance, resume from **one millisecond past the last timestamp you received**. The cursor is
inclusive, so re-using the last timestamp unchanged would return that sample a second time.

```python
# python
import valkey

client = valkey.Valkey(decode_responses=True)
last = None

while True:
    cursor = "-" if last is None else last + 1
    reply = client.execute_command(
        "TS.READ", "key", cursor, "BLOCK", 5000, 1, "MAX_COUNT", 100
    )
    if not reply:
        continue  # Timed out; just ask again.
    process(reply)
    last = reply[-1][0]
```

Two properties make this loop safe:

- **Reads do not consume.** Any number of clients can tail the same series; each gets every
  sample. `TS.READ` never removes or marks anything.
- **A timeout is not an error.** An empty reply means "nothing yet", so the loop simply
  re-issues.

Note that a sample written *out of order*, below a cursor you have already passed, will not be
returned by later calls — the cursor only moves forward. If backfill matters, re-query the window
with [`TS.RANGE`](./ts.range.md) rather than relying on the tail.

### Tailing with a CONDITION

The loop above advances its cursor from the reply, and with `CONDITION` that no longer works on its
own. An empty reply stops meaning "no new data" and starts meaning "nothing matched" — so a tail
whose condition rarely fires never moves its cursor and re-scans an ever-growing prefix on every
call. Two ways to avoid it:

- **Tail from `$`.** Each call only considers what arrives after it starts, which bounds the scan
  to the new tail regardless of how selective the condition is. This is the right default for
  alerting, where old non-matching data is of no interest.
- **Advance the cursor separately.** Track the frontier with an unconditioned read (or
  [`TS.GET`](./ts.get.md)) and pass that timestamp to the conditioned read, so the cursor keeps
  moving even through stretches where nothing matches.

A sparse condition over a `-` cursor on a large series is the shape to avoid: every unsatisfied
call reads the whole tail looking for matches that are not there.

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
| Negative `milliseconds`, or a wait limit that is neither a number of milliseconds nor a valid [duration](#duration-syntax) | `TSDB: BLOCK milliseconds must be a non-negative integer` |
| `min_count` greater than `max_count` | `TSDB: BLOCK min_count must be <= MAX_COUNT` |
| `CONDITION` operator outside the six spellings | `TSDB: invalid comparison operator` |
| `CONDITION` value not a number | `TSDB: CONDITION value must be a number` |
| Duplicate clause, missing value, or unknown token | `ERR wrong number of arguments for 'ts.read' command` |

`min_count > max_count` is rejected before the key is touched, so it errors even for a key that
does not exist.

Note that a missing key is **not** an error — it reads as an empty array, and with `BLOCK` it is a
valid thing to wait on.

## Complexity

O(log(n)+m), where n is the number of samples in the series and m is the number of samples
**examined**.

Without `CONDITION`, m is the number of returned samples: the read stops as soon as it has enough.

With `CONDITION`, m is the number of samples examined to find the matches, which is not the same
number. A satisfied read still stops as soon as it has `min_count` matches, but an unsatisfied one
examines every sample at or after `timestamp` — including on each wakeup while blocked. A sparse
condition over a `-` cursor on a large series is the expensive shape; see
[Tailing with a CONDITION](#tailing-with-a-condition).

## See Also

- [TS.RANGE](./ts.range.md) – Query a bounded range of samples
- [TS.REVRANGE](./ts.revrange.md) – Query a range in descending order
- [TS.GET](./ts.get.md) – Get only the last sample
- [TS.ADD](./ts.add.md) – Add a sample to a time series
