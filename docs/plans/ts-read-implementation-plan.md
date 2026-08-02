# TS.READ Implementation Plan

**Status:** Implemented 2026-08-02. Retained as the design record — the *why* behind the code,
and the evidence base (§6, §7) that pinned each behavior.
**Target:** Redis 8.10 `TS.READ` compatibility
**Reference documentation:** [Redis `TS.READ`](https://redis.io/docs/latest/commands/ts.read/) and
[Valkey module blocking operations](https://valkey.io/topics/modules-blocking-ops/)

## Outcome

Shipped as designed. The sections below are left in their original prescriptive voice; where the
work changed an answer, a dated note says so inline rather than silently rewriting the plan.

| Where it landed | |
| --- | --- |
| Command | [src/commands/ts_read.rs](../../src/commands/ts_read.rs) |
| Block-on-keys FFI adapter | [src/common/block_on_keys.rs](../../src/common/block_on_keys.rs) |
| Readiness signals | `ts_add`, `ts_madd`, `ts_incr_decr_by`, `series/bulk_add.rs`, `series/compaction.rs` |
| Standalone tests | [tests/test_ts_read.py](../../tests/test_ts_read.py) |
| Cluster tests | [tests/test_ts_read_cme.py](../../tests/test_ts_read_cme.py) |
| Differential tests | [tests/compat/test_compat_read.py](../../tests/compat/test_compat_read.py) |

**Three things the plan got wrong, all found by the work:**

1. **The cluster question answers itself.** §2 called slot migration "genuinely open" and guessed
   a blocked client would hang until timeout. The server already redirects it. See the resolution
   note in [§2](#registration-and-cluster-behavior) — it is the most load-bearing correction here,
   because it is the reason no cluster event handling was written.
2. **The `dont_cache` tip was expressible.** §1 doubted the macro could emit tips and offered a
   metadata divergence as the fallback. It can; no divergence was needed.
3. **The ASAN gate cannot run on macOS.** §5 lists it as required without qualification. It is
   Linux-only in practice — see the note in [§5](#5-validation-and-acceptance-criteria).

This is a clean-room plan. RedisTimeSeries source and test code must not be consulted, fetched,
vendored, copied, or reproduced from memory. Any behavior not specified by the public command
documentation must be established through black-box observation of the pinned `redis:8.10`
reference server and captured in locally authored compatibility tests.

Every behavioral row below is tagged with its provenance:

| Tag | Meaning |
| --- | --- |
| **[D]** | Stated by the public command documentation |
| **[P]** | Established by black-box probe of the pinned reference; see [§6](#6-reference-observations-2026-08-01) |
| **[S]** | Established by reading the Valkey server source we build against; see [§7](#7-server-mechanics-valkey-808-153-g4733aed65) |
| **[A]** | Assumption — not yet observed. Must be probed before the behavior is fixed in code. |

There are currently no **[A]** rows in §1. Anything added later that cannot be tagged **[D]**,
**[P]**, or **[S]** is not ready to implement.

## 1. Command contract

Register the following command:

```text
TS.READ key timestamp [BLOCK milliseconds min_count] [MAX_COUNT max_count]
```

Command metadata, captured verbatim from the reference **[P]**:

| Property | Value |
| --- | --- |
| Name | `ts.read` (lowercase, as registered) |
| Arity | `-3` |
| Flags | `readonly module` |
| ACL categories | `@read @timeseries` |
| Tips | `dont_cache` |
| Key specification | `begin_search` index `1`; `find_keys` range `lastkey 0, keystep 1, limit 0`; spec flags **`RO` only** |
| First/last/step | `1` / `1` / `1` |
| Since | `8.10.0` |
| Group / module | `module` / `timeseries` |
| Complexity | `O(log(n)+k) where n is the number of samples in the series and k is the number of returned samples` |
| Summary | `Read: return up to max_count samples with timestamp >= timestamp. With BLOCK, waits up to milliseconds ms until at least min_count qualifying samples exist` |

Two metadata details need an explicit decision rather than a silent choice:

- **`dont_cache` tip.** The reference sets it; no command in this module sets any tip, and the
  `#[valkey_module_macros::command]` path may not expose tips at all. Either find a registration
  path that emits it or register the difference as a known metadata divergence. Do not let the
  metadata test quietly assert an empty tip list as if it matched.
- **Key-spec flags.** The reference declares `RO` only, for `TS.READ` and for its other read
  commands alike. This module's read commands declare `[ReadOnly, Access]`
  (see [ts_get.rs](../../src/commands/ts_get.rs#L11)). Follow the module's existing convention —
  `ACCESS` is the semantically correct flag for a command that returns key data to the caller, and
  changing it would weaken ACL key-permission checking — and record the deliberate difference
  rather than treating it as a bug to fix later.

> **DECIDED 2026-08-02.**
>
> - **Tip: matched, no divergence.** The doubt above was unfounded — the macro *does* accept a
>   `tips` field, so `ts.read` is registered with `tips: "dont_cache"` and matches the reference
>   exactly. `TS.READ` is the only command in this module carrying a tip, and two tests hold that
>   line: one asserts it declares exactly `dont_cache`, the other that no *other* command declares
>   any tip, so the first cannot quietly become unrepresentative.
> - **Key spec: `RO` + `ACCESS`, deliberately different.** Followed the module convention as
>   directed. Recorded in [COMPATIBILITY.md](../../COMPATIBILITY.md) as the command's one
>   deliberate metadata difference, and asserted in `test_ts_command.py` so it stays intentional
>   instead of drifting.
>
> Both live in `TestTimeSeriesCommand` in
> [tests/test_ts_command.py](../../tests/test_ts_command.py).

`timestamp` is an inclusive lower-bound cursor, typed as a *string* argument in `COMMAND DOCS`
so that sentinels are accepted **[P]**. Accept only a non-negative 64-bit integer — a negative
literal is rejected with `TSDB: invalid timestamp` **[P]** — or one of these sentinels:

| Input | Resolution performed once, when the command begins | Provenance |
| --- | --- | --- |
| Literal | The literal millisecond timestamp, inclusive | **[P]** |
| `-` | The earliest stored timestamp | **[P]** |
| `+` | The latest stored timestamp, inclusive (returns exactly the last sample) | **[P]** |
| `$` | One past the latest stored timestamp (returns empty until new data arrives) | **[P]** |

On a missing or empty series all four forms return an empty array **[P]**. The resolved cursor
must remain stable while the client is blocked. `$` against a series whose latest timestamp is
`i64::MAX` returns an empty array — no error, no overflow, and the existing `i64::MAX` sample is
not included **[P]** — so represent it as an explicit "past the timestamp domain" cursor rather
than computing `latest + 1`.

### Reply shape

Identical to `TS.RANGE` **[P]**: a flat array of two-element `[timestamp, value]` pairs in
ascending timestamp order.

```text
RESP2                          RESP3
1) 1) (integer) 100            1) 1) (integer) 100
   2) "100.5"                     2) (double) 100.5
2) 1) (integer) 200            2) 1) (integer) 200
   2) "200.5"                     2) (double) 200.5
```

An empty result is an empty array under both protocols **[P]**. Use
[`reply_with_samples`](../../src/common/replies/raw_replies.rs#L133), which already produces exactly
this shape and already handles the RESP2/RESP3 value split; it accepts a raw
`*mut RedisModuleCtx`, so it is usable unchanged from an FFI callback.

`MAX_COUNT` caps the reply; if it is omitted, the reply is unbounded. A key of the wrong type
returns the standard `WRONGTYPE Operation against a key holding the wrong kind of value` — *not*
the `TSDB: the key is not a TSDB key` variant that `TS.ADD`/`TS.MADD` use **[P]**.

### Blocking

Without `BLOCK`, return immediately with the current result. With `BLOCK milliseconds min_count`
**[P]** for every item:

- return immediately if at least `min_count` qualifying samples already exist;
- otherwise wait until the threshold is met, the timeout elapses, or the key is removed;
- treat `BLOCK 0` as an indefinite wait;
- on timeout, return the current capped result even when it is empty or has fewer than
  `min_count` samples (observed: 1 sample returned against `min_count 3` at timeout);
- on key removal, return an empty array successfully;
- a client blocked on a *missing* key is woken when that key is created with qualifying data;
- do not consume samples — two readers blocked on one key both receive the sample that wakes them.

### Argument validation

All **[P]**, including the exact failure class:

| Input | Result |
| --- | --- |
| `BLOCK` and `MAX_COUNT` in either order | accepted |
| lowercase / mixed-case keywords | accepted |
| duplicate `BLOCK`, duplicate `MAX_COUNT`, missing value, stray token | `ERR wrong number of arguments for 'ts.read' command` |
| `MAX_COUNT 0`, negative, or non-integer | `TSDB: MAX_COUNT must be a positive integer` |
| `BLOCK <ms> 0` or negative `min_count` | `TSDB: BLOCK min_count must be a positive integer` |
| negative `milliseconds` | `TSDB: BLOCK milliseconds must be a non-negative integer` |
| `min_count > max_count` | `TSDB: BLOCK min_count must be <= MAX_COUNT` |

Note the failure *classes*: duplicated and malformed options resolve to an arity error rather than
a syntax error, and `min_count > max_count` is rejected up front, before any key access — a
missing key with `BLOCK 50 5 MAX_COUNT 2` still errors **[P]**. Exact error text is not a
compatibility requirement, but the same conditions must succeed or fail on both implementations,
and the arity-vs-`TSDB:` split is observable through `ERR`-prefix checks, so preserve it.

### Deny-blocking contexts

The rule is ordered, and the ordering is the whole point **[P]**:

1. Resolve the cursor and evaluate the current qualifying count.
2. If the threshold is already met, reply normally — **even inside `MULTI`, `EVAL`, or any other
   deny-blocking context**. `BLOCK 0` inside `MULTI` with sufficient data returns data.
3. Only if the threshold is *not* met and the context denies blocking, fail with an error. The
   reference replies `TSDB: blocking TS.READ (with BLOCK) is not allowed inside MULTI, EVAL, or a
   deny-blocking context`.

This is deliberately *unlike* `BLPOP`, which returns its non-blocking result inside `MULTI`. A
pre-flight "reject any `BLOCK` request in a deny-blocking context" check would diverge on step 2,
and a post-hoc check would crash the server — see [§7](#7-server-mechanics-valkey-808-153-g4733aed65).

## 2. Implementation design

### Parsing and retrieval

Add a dedicated `ts_read` command module containing:

- `ReadOptions`, holding the optional timeout, minimum count, and maximum count;
- a cursor enum that distinguishes an ordinary inclusive timestamp from "past `i64::MAX`";
- a strict parser for the command-specific timestamp vocabulary and optional arguments;
- one retrieval function shared by the initial command, readiness callback, and timeout callback.

New error strings belong in [error_consts.rs](../../src/error_consts.rs) with the module's other
`TSDB:` messages.

The retrieval function should open the series with read/access permissions and iterate from the
resolved cursor through `MAX_TIMESTAMP` ([constants.rs](../../src/common/constants.rs#L3) — the
module's spelling for `i64::MAX`). Return enough state for callers to distinguish a missing key,
wrong type, insufficient count, and a reply-ready snapshot. Readiness must always be re-evaluated
from current stored data because retention, out-of-order writes, or other clients may have changed
the qualifying set since the previous signal.

**Bound both scans separately.** These are different questions with different costs:

- *Readiness* only needs to know whether `min_count` qualifying samples exist. Stop counting at
  `min_count`; never scan the tail to answer it.
- *Reply* needs at most `max_count` samples. Apply the cap with an iterator limit rather than
  materializing beyond the reply limit.

This matters because the readiness callback runs once per blocked client per signal. An unbounded
readiness scan makes each write cost `O(blocked_readers × tail_length)` with the server's
execution lock held. Note also that `MAX_COUNT` is optional, so `TS.READ k -` with no cap is a
full-series read; that exposure is the same one `TS.RANGE` already carries, but the blocking form
can repeat it on every write, so the readiness short-circuit is not optional.

### Blocking adapter and ownership

Use Valkey's native block-on-keys mechanism rather than a worker thread. The `valkey-module` crate
exposes the required raw symbols but has no high-level wrapper for this form of blocking, so
isolate unsafe code in a small adapter around:

- `RedisModule_BlockClientOnKeysWithFlags`;
- `RedisModule_GetBlockedClientPrivateData`;
- `RedisModule_SignalKeyAsReady`.

This is a different mechanism from the module's existing blocking path — `TS.OUTLIERS` uses
[`block_client`](../../src/common/replies/thread_safe_reply_context.rs#L49) plus a thread-pool spawn
and a `ThreadSafeReplyContext`, whose `BlockedClient` unblocks on drop. The new adapter must not
reuse that ownership model: a block-on-keys handle stays registered after the command returns, so
it must not be tied to a guard that unblocks in `Drop`. Keep the two paths visibly separate.

Use crate `Context`, `ValkeyString`, key access, and reply helpers for all surrounding work;
[`ReplyContext::new`](../../src/common/replies/reply_context.rs#L21) takes a raw context pointer and
so is directly constructible inside the callbacks. Add the three raw symbols to the load-time
required-API check. That check currently lives in
[persistence.rs](../../src/series/index/persistence.rs#L315), which is the wrong home for blocking
symbols — hoist it to a neutral module, or add a second checker called from the same place in
[`preload`](../../src/lib.rs#L103).

The blocked-client private state owns the key bytes, resolved cursor, `min_count`, and optional
`max_count`. Allocate it as a `Box`, pass its pointer to Valkey, and recover it only by reference
inside reply/timeout callbacks. A free-private-data callback must reclaim it exactly once whether
the client succeeds, times out, disconnects, is externally unblocked, or the server aborts the
operation. Every FFI callback must check pointers and convert failures into replies/status codes;
no panic may cross the FFI boundary.

The protocol version does **not** need to be captured into the private state: the reply and
timeout callbacks receive a context carrying the real blocked client, so
[`is_resp3_client`](../../src/common/replies/raw_replies.rs#L13) reports the client's actual protocol
inside them **[S]**. The same holds for ACL identity — see
[§7](#7-server-mechanics-valkey-808-153-g4733aed65) for why, and for the one callback where it
does not hold.

Block with `REDISMODULE_BLOCK_UNBLOCK_DELETED`. The callbacks behave as follows:

1. The readiness callback reopens the key. It replies with an empty array and returns success if
   the key was removed, replies with an error and returns success for a wrong-type key, replies
   with samples and returns success once `min_count` is satisfied, and returns the module "not
   ready" status without replying otherwise.
2. The timeout callback reopens the key and always replies with the current capped snapshot,
   including a partial or empty result. A wrong-type key still produces the normal wrong-type
   error. Register it unconditionally, including for `BLOCK 0`: `CLIENT UNBLOCK` only works on a
   module client when a timeout callback is present **[S]**, and that is the only cleanup path an
   indefinite waiter has.
3. A request containing `BLOCK` must check the context's deny-blocking flag **after** evaluating
   the current qualifying count and **before** creating the raw blocked-client handle, per the
   ordering in [§1](#deny-blocking-contexts). The check is mandatory, not defensive: calling
   `BlockClientOnKeys` on a deny-blocking client outside Lua/`MULTI` trips a `serverAssert` and
   aborts the server **[S]**.

The initial lookup and registration happen in one Valkey command callback while the server's main
execution lock is held, so a writer cannot race between the insufficient-data check and registering
the key wait.

### Waking readers

Add a centralized `signal_timeseries_ready(ctx, key)` helper that calls
`RedisModule_SignalKeyAsReady`. This is an internal readiness signal, separate from client-visible
keyspace notifications.

**Key creation is already handled by the server.** `dbAdd` signals the new key as ready, and the
module object type maps to the module blocking type, so any path that installs a *new* key —
`RESTORE`, `COPY`, `RENAME`, `MOVE` — wakes a client blocked on that name with no module code at
all **[S]**, confirmed end-to-end against the reference **[P]**.

This extends further than it first appears. `RM_ModuleTypeSetValue` deletes the key and re-adds it
through `dbAdd`, and the `SETKEY_NO_SIGNAL` flag it passes suppresses only `signalModifiedKey`
(`WATCH`/CAS bookkeeping), not the readiness signal **[S]**. Every series this module installs goes
through that call — series creation, compaction-destination creation, and `TS._RESTORE` — so all of
them signal for free.

The timing is what makes this safe rather than merely lucky: ready keys are drained *after* `call()`
returns **[S]**, so a `TS.ADD` that auto-creates an empty series and then appends its sample has
committed the sample before any readiness callback runs, even though the signal fired while the
series was still empty. Do not add signal calls for key creation, and do not "optimize" this by
moving a signal earlier; verify the behavior with tests instead.

What the server does *not* do is signal when an existing key's value grows in place. Signal after
a committed mutation increases a series' stored sample count:

- `TS.ADD`;
- each affected key in `TS.MADD`;
- `TS.INCRBY` and `TS.DECRBY` when they append rather than only update;
- `TS.ADDBULK`;
- every direct or cascaded compaction destination that materializes a new sample into an
  already-existing destination series.

`TS._RESTORE` is deliberately absent from that list: it installs its payload through
[`set_value`](../../src/commands/ts_asm_restore.rs#L63), which is `RM_ModuleTypeSetValue`, which
deletes and re-adds through `dbAdd` — so it signals for free, as does every other series
installation in this module **[S]**.

Use final mutation results — [`SampleAddResult`](../../src/series/types.rs#L295) discriminates
`Ok` from `Duplicate`/`Ignored`/`TooOld` — or before/after sample counts, not merely parse success
or replication eligibility. Do not signal ignored writes, rejected writes, deletions of samples, or
value-only upserts: none can increase the qualifying count. It is harmless for a signal to wake
several clients because each callback independently rechecks its cursor and threshold.

Audit the non-command paths in
[server_events.rs](../../src/series/index/server_events.rs#L532) rather than assuming they are
covered. `loaded`, `restore`, `rename_to`, `copy_to`, and `move_to` all install a series under a
watched name; each is expected to be free via `dbAdd`, but each is also a place where this module
already runs code, and the *overwrite* variants (`RESTORE ... REPLACE`, `COPY ... REPLACE`,
`RENAME` onto an existing key) reach the creation path only because the core commands delete the
old value first. The reference wakes on all of them **[P]**. Confirm ours does too, and if any path
does not, that handler is the single choke point to fix — not a per-command hook.

Key removal is handled by the block-on-keys deletion flag. It must cover `DEL`, `UNLINK`,
`FLUSHDB`, `FLUSHALL`, expiry, and eviction without adding command-specific deletion hooks.
`SWAPDB` is likewise handled by the server, which rescans blocked keys in the swapped databases
and re-signals them **[S]**.

### Registration and cluster behavior

Register the command through the existing command attribute path, add its `read timeseries`
mapping to the central ACL table in [lib.rs](../../src/lib.rs#L119), and expose the module from
`src/commands/mod.rs`.

`TS.READ` remains a local, single-key command in cluster mode. Its key specification lets Valkey
route or redirect the request to the owning shard. Do not add a fanout operation, protobuf
message, configuration setting, persistence field, or replication record.

One cluster question is genuinely open: what happens to an *already blocked* client when its slot
migrates away. Valkey's blocked-key machinery has no slot-ownership hook, so the likely outcome is
that the client stays blocked until its timeout. Decide explicitly between accepting that
(documenting `BLOCK` as slot-migration-agnostic) and unblocking from the module's existing cluster
event handlers. Until that decision is made and tested, do not claim cluster correctness for the
blocking form in the acceptance criteria — the non-blocking form is unaffected either way.

> **RESOLVED 2026-08-02 — the premise above is wrong; the server already handles it.** Valkey's
> blocked-key machinery *does* have a slot-ownership hook, and it covers module clients.
> `clusterRedirectBlockedClientIfNeeded` (`cluster.c:1350`) handles `BLOCKED_MODULE` explicitly,
> gated on `moduleClientIsBlockedOnKeys(c)` — exactly the kind of block this design creates — and
> `handleClientsBlockedOnKeys` (`blocked.c:342`) runs it across every blocked client whenever the
> cluster configuration changes. A blocked `TS.READ` whose slot moves is therefore unblocked with
> a `MOVED` naming the new owner (or a cluster-down error for an unassigned slot).
>
> So the answer is neither of the two options offered: **rely on the server and add nothing.**
> Note this is a dividend of blocking *on keys* — a module that parked a worker thread on its own
> condvar would have gotten no redirect and would have needed the cluster event handlers this
> section contemplated. Verified by
> `tests/test_ts_read_cme.py::TestTsReadSlotMigration`, which found the behavior by failing
> against the assumption written here. Cluster correctness for the blocking form is claimable.

## 3. Verification

> **DONE 2026-08-02.** Everything below is covered, at the counts current as of writing: 20 Rust
> unit tests in `ts_read.rs`, 75 standalone cases in `tests/test_ts_read.py`, 2 cluster cases in
> `tests/test_ts_read_cme.py`, and 98 differential cases (RESP2 × RESP3) in
> `tests/compat/test_compat_read.py`. Two list items below asked for a decision rather than a
> test; both are resolved inline where they appear.

### Rust tests

Add unit coverage for:

- literal, `-`, `+`, and `$` parsing and one-time resolution;
- empty/missing series resolution and the `i64::MAX` `$` edge;
- either optional-argument order, case-insensitive keywords, missing values, stray tokens,
  duplicate keywords, invalid integers, zero/negative counts, and `min_count > max_count`,
  asserting the arity-vs-`TSDB:` failure split from [§1](#argument-validation);
- ascending retrieval, inclusive boundaries, out-of-order stored samples, unbounded replies, and
  `MAX_COUNT` truncation;
- the readiness short-circuit: deciding `min_count` must not consume the whole iterator;
- private-state cleanup and callback status decisions where they can be tested without a server.

### Standalone integration tests

Use independent client connections, the existing waiter utilities, and bounded socket/test
timeouts. Avoid fixed sleeps as synchronization. Every `BLOCK 0` test must have a guaranteed
write, deletion, or `CLIENT UNBLOCK` cleanup path.

Cover:

- immediate history reads, paging, missing/empty/wrong-type keys, and RESP2/RESP3 reply shape;
- `+` inclusive behavior and `$` excluding pre-existing data;
- stable sentinels while blocked, including a newly inserted out-of-order sample below the
  resolved cursor;
- immediate threshold satisfaction, threshold wakeup, empty and partial timeout replies, and
  indefinite blocking;
- multiple independent readers with different cursors, thresholds, and caps;
- wakeups from `TS.ADD`, `TS.MADD`, `TS.INCRBY`, `TS.DECRBY`, `TS.ADDBULK`, and direct/cascaded
  compaction output;
- wakeups from the non-command paths: `RESTORE`, `RESTORE ... REPLACE` over an existing series,
  `COPY`, `RENAME` onto the watched name, `TS._RESTORE`, and a `DEBUG RELOAD`;
- deletion by `DEL`, `UNLINK`, flush, expiry, and a deterministic maxmemory eviction setup;
- `SWAPDB` moving the watched key's database out from under a blocked reader;
- a reader blocked on a replica, woken by a write arriving over the replication link — the
  "signal on committed mutation" rule must hold on the apply path, where the context carries the
  `REPLICATED` flag;
- ACL revoked mid-block: the user's key pattern is removed while the client waits, then a write
  arrives. Decide and pin whether the wakeup errors or returns empty — **decided 2026-08-02: it
  errors.** The reference was not probed for this case, so this is defined behavior rather than an
  observed parity point; what matters either way is that the re-check runs against the *real*
  blocked client, so it neither hangs the reader nor faults on a missing user (see §7);
- disconnect and external-unblock cleanup, asserting `blocked_clients` in `INFO clients` returns
  to zero; `CLIENT UNBLOCK <id> TIMEOUT` must return `1` and deliver the snapshot;
- denied blocking in transactions, Lua, and nested calls — both the satisfied case (returns data)
  and the unsatisfied case (errors);
- command ACL category and key-pattern enforcement.

### Differential compatibility tests

Add `tests/compat/test_compat_read.py`, authored only from the public specification and black-box
observations. Existing synchronous `DiffClient` calls cannot issue the two blocking reads
sequentially, so start subject and reference reads concurrently on dedicated connections, drive
the same write or deletion against both, then compare normalized replies. Retain the normal
RESP2/RESP3 parametrization.

Compare reply values and shapes, but use generous lower/upper timing bounds rather than exact
elapsed-time equality. Include all parsing and key-state cases plus the blocking scenarios above.
On failure, ensure worker clients are closed or explicitly unblocked so the suite cannot hang or
leak state into the next test.

**Exclude `TS.READ` from the fuzzer.** [`_read_op`](../../tests/compat/fuzz_strategies.py#L400) draws
from the in-scope read surface and feeds a synchronous `DiffClient`; a drawn `BLOCK` would stall
the generated program and hang the soak rather than fail it. Either omit the command from the
strategy entirely or restrict it to the non-blocking form. Adding it to §2.1 of the test plan
without touching the strategy file is the failure mode to avoid.

### Metadata and cluster tests

Extend existing command tests to assert:

- arity, flags, legacy key range, key extraction, summary, complexity, and `since` metadata
  against the values captured in [§6](#6-reference-observations-2026-08-01);
- the `dont_cache` tip decision from [§1](#1-command-contract) — assert whatever was decided, and
  make the assertion say which it is;
- `@read` and `@timeseries` ACL membership;
- routing and wakeup through a cluster client for a normally owned key;
- the slot-move behavior chosen in [§2](#registration-and-cluster-behavior), once chosen.

> **AMENDED 2026-08-02 — the cluster list above is narrower in practice, on purpose.** The
> metadata and ACL bullets landed as written (in `test_ts_command.py`, `test_commands.py`, and
> `test_ts_acls.py`). The two *cluster* bullets did not, and the difference is deliberate.
>
> Each cluster test builds its own three-node cluster, so a case that proves nothing new costs a
> full spin-up. "Routing and wakeup for a normally owned key" turns out to be exactly that: the
> wakeup variant routes its write back to the very node the reader is parked on, making it the
> standalone test in cluster clothing. What replaced it is the sharper assertion the bullet was
> reaching for — **a non-owning primary must reply `MOVED` rather than read locally.** A cluster
> client reaching the right shard only proves the *client* worked it out; a `MOVED` proves the
> *server* extracted the key from the declared spec. Get the spec wrong and the non-owning node
> finds no key to route on, serves the read locally, and returns an empty array for a series that
> exists elsewhere — a wrong answer, and a silent one.
>
> So `tests/test_ts_read_cme.py` keeps two cases, each covering something a cluster is the only
> place to see: the key spec (via redirect) and the slot-migration behavior. Its module docstring
> records what was dropped and why.

## 4. Documentation and compatibility contract

Alongside the implementation:

- add `docs/commands/ts.read.md` with syntax, cursor guidance, blocking behavior, return shape,
  paging examples, and the recommendation to advance with `last_returned_timestamp + 1`;
- add `TS.READ` to [README.md](../../README.md), [docs/COMMANDS.md](../COMMANDS.md), and
  [docs/overview.md](../overview.md);
- add it to the compatible query-command list in [COMPATIBILITY.md](../../COMPATIBILITY.md) and make
  the corresponding change in [docs/topics/redistimeseries-migration.md](../topics/redistimeseries-migration.md);
- update the registered-command inventory in [AGENTS.md](../../AGENTS.md) ("Currently registered
  commands", under "Quick tips for code changes");
- add a suite-table row to [tests/compat/README.md](../../tests/compat/README.md);
- in [docs/plans/rts-compatibility-test-plan.md](rts-compatibility-test-plan.md), move it out of the
  §2.1 "not yet in scope" note *and* add its per-command row to the §6 matrix;
- add a dated completion note to the 2026-08-01 entry in
  [docs/plans/rts-reference-bumps.md](rts-reference-bumps.md#L68), which tracks the same follow-up;
  preserve the historical reference-bump entry itself.

[docs/commands/index.md](../commands/index.md) is deliberately **not** on this list. It is stale — it
sits inside an ```` ```aiignore ```` fence, omits `TS.QUERYINDEX`, `TS.QUERYLABELS`, and
`TS.REVRANGE`, and lists a `TS.STATS` that has no page. Adding one more entry to it would deepen
the inconsistency; either fix the file as separate work or leave it alone.

> **DONE 2026-08-02.** All eight items landed, and `docs/commands/index.md` was left alone as
> directed — it is still stale, still separate work.
>
> Two notes for anyone following the paths above. The planning docs have since moved into
> `docs/plans/`, so this file, the compatibility test plan, the bump log, and the first-run
> findings are now siblings; links here were rewritten to match. And the compatibility contract
> ended up recording **one** deliberate difference, not two — the key-spec flags. The
> slot-migration caveat drafted alongside it was withdrawn once the server turned out to redirect
> blocked clients correctly, so it was never a divergence to document.

## 5. Validation and acceptance criteria

Run focused checks first, followed by the repository gates:

```bash
cargo test --features enable-system-alloc ts_read
cargo test --doc --features enable-system-alloc
SERVER_VERSION=unstable TEST_PATTERN="test_ts_read" ./build.sh
RTS_COMPAT=1 SERVER_VERSION=unstable TEST_PATTERN="compat_read" ./build.sh
ASAN_BUILD=true SERVER_VERSION=unstable TEST_PATTERN="test_ts_read" ./build.sh
cargo fmt --check
cargo clippy --profile release --all-targets -- -D clippy::all
RUSTFLAGS="-D warnings" cargo build --all --all-targets --release
SERVER_VERSION=unstable ./build.sh
```

The ASAN pass is a **required** gate for this change, not an optional extra. The feature is
mostly raw FFI plus a manually managed `Box` that must be reclaimed exactly once across five
distinct lifecycle paths (reply, timeout, disconnect, external unblock, shutdown); a leak or
double free there is invisible to the ordinary suites.

> **RESULTS 2026-08-02 — every gate green except ASAN, which is Linux-only.**
>
> | Gate | Result |
> | --- | --- |
> | `cargo test ... ts_read` / `--doc` | 20 / 15 pass |
> | `fmt`, `clippy`, `RUSTFLAGS="-D warnings"` release | clean |
> | `TEST_PATTERN="test_ts_read"` | 77 pass |
> | Full standalone suite | 1038 pass, 0 errors |
> | `RTS_COMPAT=1 ... "compat_read"` | 98 pass; only the pre-registered DIV-0055 |
> | ASAN | **not run — see below** |
>
> **The ASAN line above needs qualifying, because the gate is easy to run vacuously.**
> `ASAN_BUILD=true` instruments nothing by itself; it only changes how pytest output is captured
> and greps it for `LeakSanitizer: detected memory leaks`. That line can only come from a
> `valkey-server` built with `SANITIZER=address` (CI's `asan-build` job does this and leaves the
> Rust module alone — the module allocates through the server's allocator, so instrumenting the
> server covers it). Against an ordinary binary the grep never matches and the phase reports
> success having checked nothing.
>
> It cannot be run on macOS at all: LeakSanitizer does not exist on Darwin, and an instrumented
> server wedges at startup on darwin/arm64. `build.sh` now refuses the combination outright rather
> than passing vacuously. **Run this gate on Linux — CI's `asan-build` job, or a container.**
>
> Note what this leaves unverified: the five-path `Box` reclamation argued for above is the one
> claim in this document with no local evidence behind it. The suites do show `blocked_clients`
> returning to zero across disconnect, timeout, and external unblock, but that is the server's
> client accounting, not heap accounting — it would not notice a leaked or double-freed
> `PendingRead`.

The feature is complete when:

- all documented immediate, blocking, timeout, deletion, and sentinel semantics pass under RESP2
  and RESP3;
- the compatibility suite reports no unregistered `TS.READ` divergence against the pinned
  reference, and the fuzzer's command strategy has been updated deliberately;
- every accepted sample-producing path can wake eligible readers, while ignored/upsert-only
  writes leave ineligible readers blocked;
- client disconnects, timeouts, external unblocks, and shutdown leave no blocked-client private
  data or server-side blocked clients behind, under ASAN;
- standalone single-key cluster operation works without adding fanout or persisted state, and the
  slot-migration behavior of a blocked client is decided, documented, and tested;
- command metadata, ACL categories, user documentation, and the compatibility contract all expose
  the new supported command consistently.

> **STATUS 2026-08-02 — six of six criteria met, with one qualifier.** The fourth criterion says
> "under ASAN"; locally that clause is unmet for the reason given above, and the *observable* half
> of it — no server-side blocked clients left behind — is covered by the standalone suite. The
> criterion should be treated as satisfied only once CI's Linux `asan-build` job has run this
> branch.
>
> The fifth criterion resolved more cheaply than expected: slot migration needed no module code,
> only a test proving the server handles it. Nothing about `TS.READ` is persisted, replicated, or
> fanned out, exactly as intended — no configuration, protobuf message, or RDB field was added.

## 6. Reference observations (2026-08-01)

Probed against the digest-pinned reference from
[docker-compose.compat.yml](../../docker-compose.compat.yml#L18):
`redis:8.10@sha256:c29e49ab2f85760a3827b53882e6dd9f5c6c3f0bb7d724e07bb31cbf275a5236`
(server 8.10.0, bundled `timeseries` 81000). Black-box only; no reference source was consulted.

Metadata is transcribed in [§1](#1-command-contract). Behavioral observations:

| # | Probe | Result |
| --- | --- | --- |
| 1 | `TS.READ nosuch 0` and each sentinel on a missing key | empty array |
| 2 | Each sentinel on an existing but empty series | empty array |
| 3 | `TS.READ str 0` on a string key | `WRONGTYPE Operation against a key holding the wrong kind of value` |
| 4 | Samples at 100/200/300, cursor `200` | returns 200 and 300 — inclusive |
| 5 | Cursor `+` | returns only the 300 sample — latest, inclusive |
| 6 | Cursor `$` | empty array |
| 7 | `$` with latest timestamp `9223372036854775807` | empty array; no error, no overflow, existing sample excluded |
| 8 | `$ BLOCK 100 1` at `i64::MAX` | blocks, returns empty at timeout |
| 9 | `TS.READ s -5` | `TSDB: invalid timestamp` |
| 10 | Reply shape, RESP2 / RESP3 | array of `[integer, bulk string]` / `[integer, double]` pairs — identical to `TS.RANGE` |
| 11 | `MAX_COUNT 2` | first 2 pairs |
| 12 | `MAX_COUNT` before `BLOCK`; lowercase keywords | accepted |
| 13 | duplicate `BLOCK`; duplicate `MAX_COUNT`; `BLOCK 50` with no `min_count`; trailing `BOGUS` | `ERR wrong number of arguments for 'ts.read' command` |
| 14 | `MAX_COUNT 0`; `MAX_COUNT abc` | `TSDB: MAX_COUNT must be a positive integer` |
| 15 | `BLOCK 50 0`; `BLOCK 50 -1` | `TSDB: BLOCK min_count must be a positive integer` |
| 16 | `BLOCK -1 1` | `TSDB: BLOCK milliseconds must be a non-negative integer` |
| 17 | `BLOCK 500 5 MAX_COUNT 1` | `TSDB: BLOCK min_count must be <= MAX_COUNT`, returned in ~4 ms — validated before key access |
| 18 | 1 sample, `BLOCK 600 3` | returned that 1 sample after ~674 ms — partial snapshot at timeout |
| 19 | `BLOCK 5000 2`, second sample written at ~300 ms | returned both samples at ~313 ms |
| 20 | `BLOCK 5000 9`, `DEL` at ~300 ms | empty array at ~308 ms, no error |
| 21 | `BLOCK 5000 1` on a **missing** key, `TS.ADD` at ~300 ms | returned the new sample at ~306 ms |
| 22 | Two readers, same key, one `TS.ADD` | both returned the sample at ~314 ms — no consumption |
| 23 | `MULTI`/`EVAL` with threshold met (incl. `BLOCK 0`) | returns data normally |
| 24 | `MULTI`/`EVAL` with threshold unmet | `TSDB: blocking TS.READ (with BLOCK) is not allowed inside MULTI, EVAL, or a deny-blocking context` |
| 25 | `CLIENT UNBLOCK <id> TIMEOUT` against a `BLOCK 0` reader | returns `1`; reader receives empty array; `blocked_clients` 1 → 0 |
| 26 | `RESTORE` into a watched missing key | wakes, ~331 ms |
| 27 | `COPY` into a watched missing key | wakes, ~308 ms |
| 28 | `RENAME` onto a watched missing key | wakes, ~309 ms |
| 29 | `RESTORE ... REPLACE` over a watched existing empty series | wakes, ~313 ms |
| 30 | Compaction destination receiving a materialized bucket | wakes, ~309 ms |

## 7. Server mechanics (Valkey 8.0.8-153-g4733aed65)

Read from the Valkey checkout that `build.sh` produces under `tests/build/valkey`. These settle
the FFI questions the design depends on; re-verify if the minimum supported server moves.

| Question | Answer | Evidence |
| --- | --- | --- |
| Does key creation wake module clients without a module signal? | Yes. `dbAddInternal` signals the new key as ready, and `OBJ_MODULE` maps to `BLOCKED_MODULE`. | `db.c:223`, `blocked.c:505` |
| Does in-place overwrite signal? | No. The `update_if_existing` branch returns before the signal; `RESTORE ... REPLACE` only wakes because the core command deletes the old value first, landing on the creation path. | `db.c:202-223` |
| Does installing a module value signal? | Yes. `VM_ModuleTypeSetValue` deletes the key then `setKey(..., SETKEY_DOESNT_EXIST)`, which routes to `dbAdd`. `SETKEY_NO_SIGNAL` suppresses `signalModifiedKey` only, not `signalKeyAsReady`. | `module.c:7545`, `db.c:417-435`, `server.h:3777` |
| When are ready keys drained? | After `call()` returns, so a command that creates a key and then fills it has committed the data before any readiness callback runs. | `server.c:4701` |
| Is `SWAPDB` handled? | Yes. `scanDatabaseForReadyKeys` re-signals keys with blocked clients in swapped databases. | `db.c:1709` |
| Whose client does the reply callback see? | The real blocked client — `ctx.client = bc->client`. | `module.c:8662` |
| …the timeout callback? | Same. | `module.c:9107` |
| …the free-private-data callback? | Same pointer, but it may be `NULL` if the client was already destroyed. The free callback must not touch client, ACL, or protocol state. | `module.c:280`, `module.c:8499` |
| Is `bc->client` ever a fake client? | Only under Lua/`MULTI`, where it is `NULL` by construction — and we refuse to block there. `reply_client`/`thread_safe_ctx_client` are the fakes, and neither is what the callbacks receive. | `module.c:279-302`, `module.c:8378` |
| Is `is_resp3_client` correct inside the callbacks? | Yes. `RESP3` is derived from `ctx->client->resp`, which is the real client's protocol. | `module.c:4135` |
| Are ACL/current-user lookups safe inside the callbacks? | Yes, for the same reason — the context carries the real client and therefore a real user. The null-user crash mode documented in [context.rs](../../src/common/context.rs#L44) does not apply here. | `module.c:4131`, `module.c:8662` |
| What happens if we block a deny-blocking client? | `serverAssert(!c->flag.deny_blocking \|\| (islua \|\| ismulti))` — the server aborts. The pre-check is mandatory. | `module.c:8361` |
| Does `CLIENT UNBLOCK` work on our blocked clients? | Only if a timeout callback is registered. | `module.c:9077` |

## Assumptions

- Observable behavior and error conditions target Redis 8.10 parity. Exact error wording remains
  outside this repository's compatibility contract, though the arity-vs-`TSDB:` failure split is
  itself observable and is treated as in-contract.
- Valkey 8.0 remains the minimum supported server; the required blocking APIs become explicit
  load-time requirements. The §7 mechanics were read from a 8.0.8-derived checkout and should be
  re-confirmed if that floor moves.
- This change adds no configuration, persistence migration, wire protocol, or replication-format
  change.
- All compatibility work continues to observe the repository's clean-room rule. §6 was produced
  by black-box probing of the pinned image only.

> **HELD 2026-08-02.** All four survived implementation.
>
> The load-time requirement is real code, not an intention: `check_blocking_module_apis` verifies
> `BlockClientOnKeysWithFlags`, `GetBlockedClientPrivateData`, and `SignalKeyAsReady` during
> `preload`, so an unsupported server refuses to load rather than failing on the first blocking
> read. The "no configuration, persistence, wire, or replication change" assumption held exactly —
> the only cross-cutting edits were readiness-signal calls at five existing mutation sites.
>
> The clean-room rule was observed throughout: no RedisTimeSeries source was consulted, and every
> **[P]** row traces to a probe of the pinned image. Worth keeping in view if this document is used
> as the template for the next command — §6 and §7 are what made the design decidable, and they
> were cheaper to produce than the debugging they replaced.
