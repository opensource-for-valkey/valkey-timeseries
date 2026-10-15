# Compaction under replication: analysis and remediation plan

Status: Approach 1 adopted and implemented (`docs/plans/2026-10-14-compaction-refactor.md`,
branch `compaction-refactor`): recompute on every node, DIV-0023 emulation dropped, inputs pinned,
dirty-rule recovery. Option A (§5) was adopted by the owner on 2026-10-14; the effects-replication
Option C was not taken, and its plan (`docs/plans/2026-10-14-compaction-effects-replication.md`)
is superseded and kept only as the record of that design. §3–§6 below are kept as the analysis
that led here; the notes marked "Resolved", "Adopted" and "Landed in" say what became of each item.
Reference behavior checked 2026-10-14 (§3.7): the reference recomputes on the replica and persists
its marker.
Written 2026-10-14 against `dos-defense` at `f4f44d482` (`src/series/compaction.rs` has since been
split into the `src/series/compaction/` module directory). Evidence in §3 comes from reading
`src/series/compaction.rs`, `sample_merge.rs`, `bulk_add.rs`, `serialization.rs`, `utils.rs`
and the write-command handlers, plus a primary/replica probe (§3.6) run against the built module.

This expands the one-line proposal item "compaction is recomputed on the replica rather than
replicated; needs equivalence tests beyond the `TS.INCRBY` and `TS.ADDBULK` paths"
(`proposal.md` §Replication, `proposal-complete.md` §3.5).

## 1. What "recomputed on the replica" means

A compaction rule runs **inside the source write**, synchronously, under the GIL. The primary
propagates only the source command (verbatim, or with `*` and auto-create options resolved).
Nothing it wrote to the destination is ever sent. The replica replays the source command and
re-runs the same engine against its own copies of the series.

The destination is therefore a function of five inputs on *each node independently*:

| Input | Where it lives | How the replica gets it |
|---|---|---|
| Source series' stored samples | `TimeSeries.chunks` | replication stream |
| Open-bucket state per rule: `bucket_start`, `has_samples`, the aggregator accumulator | `CompactionRule` on the source | RDB snapshot at full sync / ASM, then **derived** by replaying writes |
| Destination series' stored samples | dest `TimeSeries` | RDB snapshot, then **derived** |
| Strict-mode last-sample marker (`last_forward_close`, DIV-0023) | dest `TimeSeries`, **runtime only** | never — `rdb_load_series` sets it to `None` |
| Node configuration: `ts-compaction-policy`, `ts-compatibility-mode`, `ts-retention-policy` and the other auto-create defaults | per node | not part of the stream |

The engine (`apply_compaction` → `apply_op`) has four operations. Each is deterministic given
the inputs above, and each reads the source through a retention-clamped iterator
(`calculate_range` → `stored_range_iter(start.max(min_ts))`), so lazily-untrimmed expired
samples are excluded on every node regardless of whether the 10 s trim cron has run there:

* `AddNew` — O(1) stream into the open aggregator; a forward close publishes the finalized
  bucket to the destination (`add_dest_bucket`, `KeepLast`) and advances the marker.
* `Upsert` — recalculates the open bucket in place, or recalculates a closed bucket from the
  source and rewrites (or removes) its destination sample. Does **not** move the marker.
* `AddBatch` (`TS.MADD`, `TS.ADDBULK`, cascades) — classifies items as appends or back-fills by
  replaying the caller's input order against a running maximum seeded with `prev_last`, then
  streams the appends and recalculates each back-filled bucket once with the retention floor
  that would have been in force sequentially.
* `RemoveRange` (`TS.DEL`) — drops fully covered buckets, recalculates the two boundary
  buckets, then rebuilds the open bucket from whatever the source still holds
  (`resync_open_bucket_after_removal`), retracting any destination bucket the removal re-opened.

Cascades feed each level's `written` samples into the next level as an `AddBatch`; `RemoveRange`
cascades the same op. Rules on one source run in parallel for the batch and removal ops, each
into its own destination; there is no shared mutable state, so parallelism does not affect the
result.

## 2. Evaluation of the proposal statement

The statement is accurate but understates the problem in two ways.

1. **It frames the risk as "untested".** Two of the divergences below are structural, not
   hypothetical: one is reproduced (§3.2), one follows directly from the code (§3.1a). Tests
   would find them; they would not fix them.
2. **It does not say that the model has no reconciliation.** Recompute is correct *by
   invariant*: every node must evolve identical state from identical inputs forever. Any
   node-local deviation — a config difference, a transient failure, a version skew during a
   rolling upgrade — is permanent, silent, and only self-corrects where a later operation
   happens to rebuild from the source. The equivalence tests the proposal asks for are
   necessary under any model (§6 Phase 0), but under recompute they are the *only* guard.

## 3. Where primary and replica can diverge

Classified by cause. "Proven" = reproduced in §3.6; "structural" = follows from the code
without a reproduction; "suspected" = needs a probe before it is claimed.

What became of each class (all landed on branch `compaction-refactor`; task numbers refer to
`docs/plans/2026-10-14-compaction-refactor.md`): §3.1a policy destinations and §3.8 rule order →
Task 12; §3.1c auto-create defaults → Task 11 (which also replicates an explicit `TS.CREATE` with
the primary's resolved options); §3.1b `ts-compatibility-mode` → an operator rule in
`docs/overview.md` (it is read by the answering node, so it must match across the group); §3.2
the strict-mode marker → removed with the DIV-0023 emulation, Task 6; §3.3 version skew →
documented operational rule (Task 14) and a per-aggregator persistence test (Task 5); §3.4
failure paths → `dirty` rules rebuild their open bucket, plus failure counters (Task 13); §3.5
blocked-duplicate `TS.MADD` → the marker it concerned is gone (Task 6). §3.6–§3.10 are findings
recorded as history.

### 3.1 Inputs that are per-node configuration (structural)

**a. `ts-compaction-policy` destinations are created from each node's own config and are not
replicated.** `create_and_store_series` → `add_default_compactions` (`src/series/utils.rs`)
runs on every node that executes a `TS.CREATE` or an auto-creating write — including a replica
replaying one and an AOF replay. It opens the destination key, calls `create_series`, and
`set_value`s it directly; there is no `ctx.replicate` for the destination or the rule. So:

* A replica whose policy config differs from the primary's (a `CONFIG SET` applied to one node,
  different startup arguments, a rolling change) materializes different destination keys and
  rules. Nothing ever reconciles them.
* An AOF replay re-applies the policy *current at replay time*, not the one in force when the
  key was created.
* If the destination key already exists the code logs and `continue`s **without pushing the
  rule**, so the source silently ends up with fewer rules than the policy says — a second way
  for two nodes to disagree.

**b. `ts-compatibility-mode` is per node and gates the strict-mode last-sample.**
`reported_last_sample` consults `is_strict_rts_compat()` on the node answering the read. A
strict primary with an extended replica answers `TS.GET dest` differently by design. This is a
read-path concern, but it compounds §3.2.

**c. Auto-create defaults are re-resolved on the replica.** `TS.ADD`/`TS.MADD`/`TS.ADDBULK`
propagate the caller's arguments; a series auto-created without an explicit `RETENTION` takes
`ts-retention-policy` *on each node*. A retention mismatch changes the recalculation floor and
therefore destination values. `TS.INCRBY` already resolves its creation options before
propagating (`e57e0d107`); the other three do not. Not compaction-specific, but compaction is
where it becomes a numeric difference rather than a metadata one.

### 3.2 State that is never shipped — the strict-mode marker (proven)

`last_forward_close` records the bucket closed by the last *forward* close, so that in strict
mode `TS.GET`/`TS.MGET` without `LATEST` report what RedisTimeSeries reports after a back-fill
materializes an older bucket (DIV-0023). It is deliberately not serialized (`serialization.rs`:
"never serialized, so a reloaded destination reports its true last sample until the next
forward bucket close"). It also cannot be derived from stored data: in the DIV-0023 reproducer
the destination holds buckets 0 and 500 with the open bucket at 1000, and the marker is 0 —
"largest destination timestamp below the open bucket" would give 500.

Consequence (reproduced in §3.6, both nodes strict): after a full sync the primary answers
`TS.GET d:dst` → `[0, 0]` and the replica → `[500, 0]`, with identical `DEBUG DIGEST-VALUE`.
The same node disagrees with itself across `DEBUG RELOAD`. The divergence lasts until the next
forward close on that rule, which can be arbitrarily long for a slow series.

The reference does not have this problem: its cached last-sample survives both a full sync and
`DEBUG RELOAD` (§3.7), so in strict mode our reload/resync behavior is also a divergence from
RedisTimeSeries, not only between our own nodes.

### 3.3 State shipped by snapshot, then evolved by derivation — open-bucket rule state

`bucket_start`, `has_samples` and the aggregator accumulator are in the RDB (`CompactionRule`
and every `Aggregator` variant implement `RdbSerializable`, including the Kahan compensation
term and the Welford `mean/m2/count`). The probe shows the source digest — which hashes the
aggregator state and `bucket_start` — is identical after a full sync and after `DEBUG RELOAD`
for `avg`, `last`, `rate` and `std.s`, and that `TS.GET dest LATEST` agrees on both nodes.
**The comment in `test_ts_aofrewrite.py` ("bucket_start … may differ after restart") is stale**
and the test should assert the source digest instead of skipping it.

Residual risks, none reproduced:

* **Version skew.** If the primary and replica run different module versions and aggregator
  arithmetic changed between them (the project has already changed `sum`/`avg` to Neumaier and
  `std`/`var` to Welford — `COMPATIBILITY.md`), the two nodes compute different destination
  values from the same source and never converge. Under recompute every rolling upgrade is
  exposed to this; there is no policy today that an arithmetic change requires a full resync.
* **Persisted form narrower than in-memory form** for one of the 23 aggregator variants
  (`rate` re-derives its window from `bucket_duration` on load; others are direct). Only four
  variants were probed. Needs a per-variant round-trip property test (Phase 0).

### 3.4 Failure paths have no reconciliation (structural)

`apply_rules_internal` applies every rule, logs each failure, and returns the first error;
`process_series_with_compaction` then returns early — later cascade levels and the `ts.add:dest`
notifications for that write are skipped. Since `b13fdaf10` the source write is replicated
regardless (correct: the sample is stored). If the failure was node-local — an encode error,
OOM on one node, a destination that is stale on one node only — the destinations diverge and
nothing repairs them: no counter, no metric, no repair command. Errors here are rare internal
ones, but the point is categorical: recompute turns every transient local failure into a
permanent divergence.

### 3.5 Suspected — `TS.MADD` with a blocked duplicate (unverified)

`handle_replication` propagates only items whose result `is_ok()`. On the primary,
`add_group_sequentially` pushes a `Duplicate`-rejected timestamp into `added_order` because "a
repeated timestamp does not advance the running max, so it reads as a back-fill". If rejected
items are dropped from the propagated `TS.MADD`, the replica's `input_order` differs from the
primary's. Stored data is unaffected (the item was rejected on both), but
`trace_dest_cache_in_input_order` could place the marker differently. Needs a probe under
strict mode before it is counted.

### 3.6 Probe (2026-10-14)

Script: `scratchpad/probe.sh` (not committed). Two `valkey-server` processes from
`tests/build/binaries/unstable`, module from `target/release`, both in strict mode;
primary populated, replica attached with `REPLICAOF` and allowed to full-sync.

| Check | Primary | Replica after full sync | Same node after `DEBUG RELOAD` |
|---|---|---|---|
| `DEBUG DIGEST-VALUE` of a source mid-bucket (avg, last, rate, std.s) | — | identical | identical |
| `TS.GET dest LATEST` (open bucket) | `[0, 3.5]` etc. | identical | — |
| DIV-0023 reproducer, `TS.GET d:dst` strict | `[0, 0]` | **`[500, 0]`** | **`[500, 0]`** |
| `DEBUG DIGEST-VALUE d:dst` | — | identical | — |
| After next forward close (`TS.ADD d:src 1500 0`) | `[1000, 0]` | `[1000, 0]` | — |
| Bucket closed after reload (`avg` of 1, 2.5, 7) | `[0, 3.5]` | `[0, 3.5]` | — |

### 3.7 Reference behavior (2026-10-14)

Black-box, against the pinned `redis:8.10` image (bundled timeseries module 81000): a primary
and a `--replicaof` replica, MONITOR on the replica. Script: `scratchpad/rts_probe.sh`.

**The reference recomputes on the replica.** For a rule `avg 500` on `a:src → a:dst`, the
replica received exactly the source commands and nothing for the destination:

```
"ts.create" "a:src" / "ts.create" "a:dst" / "ts.createrule" "a:src" "a:dst" "AGGREGATION" "avg" "500"
"TS.ADD" "a:src" "0" "0"        "TS.ADD" "a:src" "1000" "0"        "TS.ADD" "a:src" "500" "7"
"TS.MADD" "a:src" "2000" "1" "a:src" "1500" "2"
"ts.del" "a:src" "900" "1100"
"ts.incrby" "a:src" "5" "TIMESTAMP" "1792008506783"
```

(`TS.ADD`/`TS.MADD` verbatim; `ts.del` and `ts.incrby` re-emitted with resolved bounds and a
resolved timestamp — the same split we make.) The destination, `TS.GET … LATEST`, and
`TS.INFO lastTimestamp` matched on both nodes afterwards, and a replica promoted mid-bucket
closed the bucket with the full sum (`1+2+4 = 7`): its open-bucket state exists because it
recomputed it.

**The reference persists its cached last-sample.** With the DIV-0023 reproducer, strict
`TS.GET m:dst` was `[0, 0]` on the primary, on the streamed replica, on the replica after a forced
full resync, and on the primary after `DEBUG RELOAD`. Ours was `[500, 0]` in the last two cases
(§3.6).

| Check | Reference primary | Reference replica (streamed) | Reference replica (full resync) | Reference primary after reload |
|---|---|---|---|---|
| Destination data after the stream | — | identical | identical | — |
| `TS.GET dst LATEST` | `[1000, 0]` | identical | identical | — |
| DIV-0023 `TS.GET m:dst` (strict) | `[0, 0]` | `[0, 0]` | `[0, 0]` | `[0, 0]` |
| Open bucket closed on the promoted replica | — | `[0, 7]` | — | — |
| `DEBUG DIGEST-VALUE` on a series | supported | — | — | — |

### 3.8 Found by the replica-oracle suite — policy rule order (proven, 2026-10-14)

`tests/test_ts_replication.py::test_replication_compaction_policy_rules` (an instance of §3.1a)
runs both nodes with the same `ts-compaction-policy` and auto-creates a source with `TS.ADD`.
Every destination holds identical samples, `TS.GET` and `TS.GET … LATEST` on both nodes, and
`rulesState` agrees once sorted — but the **source's `DEBUG DIGEST-VALUE` differs** on roughly
half the runs (3 of 6 in a manual loop with a two-rule policy). `TS.INFO key DEBUG` shows why:
the same two rules in opposite order (`[…_MAX_1, …_AVG_2]` on one node, `[…_AVG_2, …_MAX_1]` on
the other). `PolicyConfig::create_compaction_rules` (`src/series/compaction_policy.rs`) returns the
rules keyed by destination in a `std::collections::HashMap`, whose iteration order is seeded per
process, and `add_default_compactions` (`src/series/utils.rs`) iterates that map and pushes the
rules in its order. (`match_policy`'s `HashSet` is seeded per process too, but its order is lost
when the rules are re-keyed into the map.) Each node therefore builds its own order. Rule order
is observable (it is hashed into the digest and printed by `TS.INFO`) and decides the order in
which the rules of one source are applied.

Consequence for the suite: with two rules the failure is a coin flip, so the scenario uses six
(1/720 chance that two shuffles coincide) and is marked `xfail(strict=True)`. Whichever task makes
rule creation order deterministic (e.g. sort by `(bucket_duration, aggregation)` before pushing, or
replicate the rules instead of re-deriving them) must remove the marker.

**Resolved by refactor Task 12:** the primary replicates each policy destination as `TS.CREATE` +
`TS.CREATERULE` in its own rule order and a replica / AOF replay never applies the policy, so the
order is the primary's everywhere; the `xfail` marker is removed.

### 3.9 Rule created on existing data (reference does neither, 2026-10-14)

(The refactor plan's Task 8 names this "§3.8"; that number was already taken above.)

The batch-of-one switch (`docs/plans/2026-10-14-compaction-refactor.md`, Task 8) asked the
reference one question: a rule created on a source that already holds samples, whose first write
is a back-fill into the bucket holding the source's last sample — does the bucket open from that
one write (branch A, `sum` 8), or from what the source holds (branch B, `sum` 14)? Black-box,
pinned `redis:8.10` (timeseries 81000), via `redis-cli`. Every scenario starts from
`TS.CREATE src`, `TS.ADD src 0 1`, `TS.ADD src 1200 2`, `TS.ADD src 1500 4`, `TS.CREATE dst`,
`TS.CREATERULE src dst AGGREGATION <agg> 1000`.

The "Ours" columns and the variants below were measured on 2026-10-14, before refactor Task 9
routed `TS.ADD` and `TS.INCRBY` through the batch path; since then `TS.ADD` behaves as the
`TS.MADD` column (see "Current state" at the end of this section).

**Back-fill into the source's last bucket** (`tests/compat/test_compat_compaction.py::
test_backfill_as_first_write_after_createrule`; `TS.ADD` and one-item `TS.MADD` reply identically
on the reference):

| Step | Reference | Ours, `TS.ADD` | Ours, `TS.MADD` |
|---|---|---|---|
| write `src 1300 8`, then `TS.GET dst LATEST` | `[]` | `[1000, 8]` | `[1000, 14]` |
| `TS.GET dst` / `TS.RANGE dst - +` | `[]` / `[]` | `[]` / `[]` | `[]` / `[]` |
| `TS.ADD src 2000 16`, then `TS.GET dst LATEST` | `[2000, 30]` | `[2000, 16]` | `[2000, 16]` |
| `TS.RANGE dst - +` | `[]` | `[[1000, 8]]` | `[[1000, 14]]` |
| `TS.ADD src 3000 32`, then `TS.RANGE dst - +` | `[[2000, 30]]` | `[[1000, 8], [2000, 16]]` | `[[1000, 14], [2000, 16]]` |

**Back-fill into an older bucket** (`test_backfill_behind_the_open_bucket_as_first_write`, write
`src 300 8`; `TS.ADD` and `TS.MADD` identical on the reference):

| Step | Reference | Ours, `TS.ADD` | Ours, `TS.MADD` |
|---|---|---|---|
| `TS.GET dst LATEST` | `[0, 9]` | `[0, 8]` | `[0, 9]` |
| `TS.GET dst` / `TS.RANGE dst - +` | `[0, 9]` / `[[0, 9]]` | `[]` / `[]` | `[]` / `[]` |
| `TS.ADD src 2000 16`, `TS.ADD src 3000 32`, `TS.RANGE dst - +` | `[[0, 9], [2000, 16]]` | `[[0, 8], [2000, 16]]` | `[[0, 9], [2000, 16]]` |

**Variants** (all `TS.ADD`; `dst` after the listed writes, then `TS.ADD src 3000 32` or as noted):

| Variant | Reference | Ours |
|---|---|---|
| V1 `sum`: back-fill 1300=8, append 1600=64, `GET LATEST` | `[1000, 78]` | `[1000, 72]` |
| V1 cont.: 2000=16, 3000 → `RANGE` | `[[1000, 78], [2000, 16]]` | `[[1000, 72], [2000, 16]]` |
| V2 `count`: back-fill 1300, `GET LATEST`; 2000, 3000 → `RANGE` | `[]`; `[[2000, 4]]` | `[1000, 1]`; `[[1000, 1], [2000, 1]]` |
| V3 `sum`: overwrite 1500=8 (`ON_DUPLICATE LAST`); 2000, 3000 | `[]`; `[[2000, 26]]` | `[1000, 8]`; `[[1000, 8], [2000, 16]]` |
| V4 `sum`: plain append 1600=8 (control) | `[1000, 8]`; `[[1000, 8], [2000, 16]]` | identical |
| V5 `sum`: back-fill 1300=8, 1400=100; 2000, 3000 | `[]`; `[[2000, 130]]` | `[1000, 114]`; `[[1000, 114], [2000, 16]]` |
| V6 `sum`: back-fill 1300=8; append 5000=16, 6000 | `[5000, 30]`; `[[5000, 30]]` | `[5000, 16]`; `[[1000, 8], [5000, 16]]` |
| V7 `max`: back-fill 1300=8; 2000=1, 3000 | `[2000, 8]`; `[[2000, 8]]` | `[2000, 1]`; `[[1000, 8], [2000, 1]]` |
| V8 `sum`: back-fill 300=8, then 1300=100; 2000, 3000 | `[0, 9]`; `[[0, 9], [2000, 122]]` | `[0, 8]`; `[[0, 8], [1000, 106], [2000, 16]]` |

**Reading (black-box).** On a rule with no open bucket:

- A back-fill into an *older* bucket than the source's last one is recalculated from the source
  and published at once (`[0, 9]`), and leaves the rule without an open bucket — that is branch B's
  value, though our batch path *opens* bucket 0 instead of publishing it (visible only to a
  non-`LATEST` read before the next closing write).
- A back-fill into the source's *last* bucket recalculates that bucket from the source into the
  rule's running aggregate (V1: `78 = 2+8+4+64`; V2 count 3) but does not mark the bucket open
  (`TS.GET … LATEST` is `[]`). The next append that lands in the same bucket opens it and keeps
  the recalculated state — correct, and branch B's result (V1). The next append that lands in a
  *later* bucket opens that bucket **without resetting the aggregate**: bucket 1000 is never
  published and its contents are folded into the new bucket (`30 = 14+16`, `count` 4, `max` 8,
  V8 `122 = 2+4+100+16`). That is neither A nor B — the reference loses a bucket and corrupts the
  next one.

So branch C. `TS.ADD` and `TS.MADD` agree with each other on the reference in every case; ours
disagree with each other and with the reference. Undecided (owner): whether to keep the existing
behavior pending a ruling, emulate any part of the reference (the publish-at-once for an older
bucket is a plausible target; the leaked aggregate is not), or register a divergence. Note for the
xfail marks the brief prescribes for C: `test_backfill_behind_the_open_bucket_as_first_write
[TS.MADD]` **passes** today (the probes it issues don't see the open-vs-published difference), so
`xfail(strict=True)` on it would XPASS and fail.

**Ruling (2026-10-14):** keep the batch path's semantics (recalculate the bucket from the source;
branch B) — no engine change, so `TS.ADD` and `TS.MADD` agree once Task 9 routes `TS.ADD` through
the batch path; the reference's fold of the unpublished bucket into the next one is not reproduced
(both `test_backfill_as_first_write_after_createrule` cases are `xfail(strict=True)`). Residual:
our `TS.MADD` keeps an older back-filled bucket open until the next forward close, where the
reference publishes it at once — only an intermediate `TS.RANGE`/`TS.GET` (non-`LATEST`) before
that close shows the difference.

**Current state (after refactor Task 9, 2026-10-15).** `TS.ADD` and `TS.MADD` now take the same
batch path and agree with each other; the "Ours, `TS.ADD`" column and variants V1–V8 above describe
the retired single-sample path and are history. In
`tests/compat/test_compat_compaction.py`, `test_backfill_as_first_write_after_createrule` (the
back-fill into the source's last bucket) is `xfail(strict=True)` for both `TS.ADD` and `TS.MADD`,
citing this section — ours recalculates the bucket from the source, the reference folds it into the
next one — and `test_backfill_behind_the_open_bucket_as_first_write` passes for both. No
`divergences.yml` entry covers either case. **Open (owner):** keep the two strict xfails or
register a divergence for the fold; neither the tests nor `divergences.yml` were changed for it by
the refactor.

### 3.10 Found by the recomputation oracle — a NaN bucket never replaced a published one (fixed; diverges from the reference, DIV-0060, 2026-10-14)

`series::compaction::engine::tests::destination_matches_a_recomputation_from_the_source` (the
permanent oracle from the refactor's Task 8) shrank to: `irate`, bucket 500, writes
`[(3500, 0), (3799, 0), (4000, 0), (3500, 1)]` (duplicate policy `LAST`). The fourth write
back-fills the published bucket 3500, whose recalculation over `3500=1, 3799=0` is a counter reset
and finalizes to NaN; the destination kept the stale `0`. Cause: `add_dest_bucket` writes with
`DuplicatePolicy::KeepLast`, and `DuplicatePolicy::duplicate_value` keeps the old value whenever the
new one is NaN — for every non-`BLOCK` policy, `LAST` included — while `add` still reports the NaN
as written (so the cascade forwarded a NaN the destination itself never stored). Live repro on the
module: `TS.CREATERULE s d AGGREGATION irate 500`, `TS.ADD s 3500 0`, `3799 5`, `4000 0`
(`d` = `[3500, 16.72…]`), `TS.ADD s 3500 9 ON_DUPLICATE LAST` → `d` stayed `16.72…` while
`TS.RANGE s 3500 3999 AGGREGATION irate 500` says `nan`.

Reachable on the shared RTS surface — an earlier version of this section said otherwise, which
was wrong. Besides the extension aggregators that finalize to NaN over a counter reset (`irate`,
`rate`, `increase`), `std.p`/`std.s`/`var.p`/`var.s` finalize to NaN when their accumulation
overflows on values near `1e308` (found in review, black-box): rule `std.s 1000` (source
`DUPLICATE_POLICY LAST`); `TS.ADD s 0 1e308`, `TS.ADD s 1000 1` (publishes `[0, 0]`); back-fill
`TS.ADD s 100 1e308`, `200 -1e308`, `300 -1e308`. Both engines store the same source data and both
report `TS.RANGE s 0 999 AGGREGATION std.s 1000` = NaN, but the reference's `TS.RANGE d - +` stays
`[[0, 0]]` — stale, contradicting its own aggregation of the source — while ours is `[[0, nan]]`
(`[[0, 0]]` before the fix: the `LAST` duplicate rule kept the old value).

Fix: `add_dest_bucket` clears the bucket's existing destination entry before writing a NaN.
**Ruling (2026-10-14): keep the fix for every aggregator** — a destination bucket must equal a fresh
computation of its source bucket; we decline to reproduce the reference contradicting its own
`TS.RANGE`, the same policy as DIV-0023 and DIV-0024..DIV-0029. Registered as **DIV-0060**
(`behavior`, documentation-only, ungated; `COMPATIBILITY.md` "Divergences that qualify but are not
yet gated"), pinned per-engine by
`tests/compat/test_compat_compaction.py::TestNanBackfillOnDestination`. Regression test:
`backfill_turning_a_published_bucket_into_nan_replaces_it` (both ingest paths); proptest seed in
`proptest-regressions/series/compaction/engine.txt`.

## 4. Why it is built this way — and what that buys

The design has real merits that any remediation must keep:

* `TS.GET … LATEST` on a replica needs the open-bucket state, and recompute gives it for free.
* The stream carries one command per source write, no matter how many rules or cascade levels.
* No internal commands, no new wire format; a replica is a primary that does not accept writes.
* ASM (Valkey ≥ 9) already treats `TS._RESTORE` plus the forwarded writes as a replication feed,
  so cluster slot migration inherits exactly the same model and the same guarantees.
* It is the reference's model (§3.7): RedisTimeSeries also sends only the source command and
  recomputes on the replica, and persists the open-bucket state and the cached last-sample so a
  snapshot carries them. Matching it means every replica-visible behavior matches by
  construction of the same code paths.

What it does not buy is a guarantee. The replica's destination is right only while the invariant
"same code, same inputs, same order, no failures" holds on every node.

## 5. Remediation options

### Option A — keep recompute; pin the inputs; make it verifiable

1. Persist `last_forward_close` (optional field appended to the series RDB payload). Nothing
   has been released, so the format changes in place and the encoding version stays `1`.
   Fixes §3.2.
2. Gate `add_default_compactions` on `is_real_user_client(ctx)` and replicate the destination
   creations and rules it makes as explicit `TS.CREATE dest …` + `TS.CREATERULE` commands.
   Fix the "destination exists → rule dropped" branch. Fixes §3.1a.
3. Resolve auto-create defaults into the propagated `TS.ADD`/`TS.MADD`/`TS.ADDBULK`, as
   `TS.INCRBY` does. Fixes §3.1c.
4. Make the rule state a *self-validating cache*: store `applied_through: Option<Timestamp>`
   (the source's last timestamp when the rule state was last updated). Before applying any op,
   if it differs from the source's current last timestamp, rebuild the open bucket from the
   source (the `resync_open_bucket_after_removal` path) and count it. Heals §3.4 for the open
   bucket on the next write; makes a promoted replica safe under Option B.
5. The equivalence harness and observability of Phase 0 (§6).

Keeps: everything in §4. Costs: determinism stays an invariant to maintain across every future
engine or aggregator change; version skew (§3.3) and missed destination writes (§3.4) remain
unreconciled.

### Option B — primary computes, replica stores (effects replication)

The primary emits, after the source write and atomically with it (the server wraps a command's
`also_propagate` entries in `MULTI`/`EXEC`), one internal command per destination effect:

```
TS._COMPACT dest ADD <bucket_ts> <value> [FORWARD]
TS._COMPACT dest DEL <start> <end>
```

`FORWARD` marks a forward close so the replica sets the marker. The engine already records
`written` per rule; removals need the same bookkeeping. Cascaded levels emit their own effects.
On a node where `!is_real_user_client(ctx)` (replica link, AOF replay), source writes **skip
`apply_compaction`** and `TS._COMPACT` applies the effect as a raw `KeepLast` write / range
removal, emits `ts.add:dest`, and signals blocked `TS.READ` readers — the `TS.MDEL` and
`TS._RESTORE` precedents. A user's direct write to a destination on the primary is unchanged.

Open-bucket state on the replica is then frozen at the snapshot, so:

* `TS.GET … LATEST` on a replica computes the open bucket from the source's current samples
  when `applied_through != source.last_ts` (O(samples in the open bucket), read path only).
* On promotion, the first write to each source hits the same check (A.4) and rebuilds. No
  role-change hook is needed.

Keeps: `LATEST` on replicas (derived), ASM compatibility (the migration target is a feed
consumer), RTS-observable behavior on the primary. Gains: the replica holds exactly the
primary's numbers — §3.3 version skew and §3.4 local failures stop being replica problems;
determinism of the engine becomes a single-node property; compaction output is visible in
`MONITOR`/AOF. Costs: a new internal command (undocumented, guarded like `TS._RESTORE`); stream
traffic grows by at most the number of destination buckets touched per write (one per bucket
close per rule; ≤ 3 commands per rule per level for a `TS.DEL`); §3.1a–c and §3.2 still need
A.1–A.3 because they concern snapshots and creation, not effects.

### Option C — B plus A.1–A.4 (recommended at the time; not taken — see "Adopted" below)

A.1–A.4 are needed under either model (they fix snapshot and creation paths, and A.4 is what
makes B safe at promotion). B is the architectural change. Together they move the replica from
"correct while the invariant holds" to "correct by construction", at the cost of one internal
command and a bounded amount of extra stream traffic.

Recommendation, revised after §3.7: **A.1–A.4 are the fix** — they are what the reference does
(persist the state a snapshot needs) plus what it leaves to per-node configuration (policy
destinations, auto-create defaults), and they close every divergence this analysis found. With
them, Option A is the reference's architecture with an equivalence suite guarding it. Option C
is no longer a parity matter; its remaining case is version skew during a rolling upgrade
(aggregator arithmetic has changed twice in this codebase) and reconciliation of a node-local
compaction failure. Take C only if the project will not adopt the simpler rule "a replica never
runs a newer aggregator arithmetic than its primary without a full resync" and wants the
stronger guarantee at the cost of a mechanism the reference does not have in its stream and
AOF. Decide at the Phase 1 checkpoint, after Phase 0 and Phase 2 have landed — the work up to
there is identical under both.

**Adopted:** Option A, extended by the compaction refactor — `last_forward_close` removed (DIV-0023
ungated), one ingest operation, module split, `dirty` instead of `applied_through`. Option C was not
taken: the version-skew case is handled by the operational rule in `docs/overview.md` (upgrade the
primary and its replicas together and full-resync them when a release changes aggregation results),
and a node-local failure heals through the `dirty` rebuild instead of effects replication.

## 6. Plan

Phases are ordered so that each is independently landable. Phase 0 is no-regret under either
option and should start now; Phase 1 is the decision.

Outcome: Phases 0 and 2 landed on branch `compaction-refactor`; Phase 1 was decided for Option A;
Phase 3 was not taken. Each item below ends with a "Landed in" note naming the task of
`docs/plans/2026-10-14-compaction-refactor.md` that did it (or why nothing did).

### Phase 0 — observe, pin, and test (either option)

0.1 **Expose the hidden state.** `TS.INFO key DEBUG` (or `TS._DEBUG COMPACTION key`) reports,
per rule: `bucket_start`, `has_samples`, `applied_through`, an aggregator snapshot, and on a
destination the marker. Tests and operators need this to say *where* two nodes differ.
*Landed in Task 1* as `TS.INFO key DEBUG` → `rulesState`, one `[destKey, bucketStart, hasSamples,
current]` entry per rule (a fifth element, `dirty`, since Task 13); there is no `applied_through`
and no marker any more.

0.2 **Replica-oracle equivalence suite** in `tests/test_ts_replication.py`, each scenario
ending with `DEBUG DIGEST-VALUE` on every source and destination on both nodes plus the
strict-mode `TS.GET`/`TS.MGET` reply:
`TS.ADD`-fed rule (append, in-bucket upsert, back-fill into a closed bucket, two buckets back);
`TS.MADD` with out-of-order items, in-batch duplicates under each duplicate policy, and a
blocked duplicate (§3.5); `TS.DEL` partial, middle, and trailing-bucket removals with the
re-opened bucket; three-level chain and a diamond; `TS.CREATERULE` on a source with data;
`TS.DELETERULE` mid-bucket then re-create; direct destination writes erased by a source
`TS.DEL` (DIV-0021 model); retention on the source under a rule; policy-created rules; and a
full resync taken mid-bucket followed by the sequence above. Run with both nodes strict.
*Landed in Task 2* (`test_replication_compaction_*` in `tests/test_ts_replication.py`, with a real
`WAIT` barrier in the helpers).

0.3 **Restart and promotion variants** of 0.2: `DEBUG RELOAD` on the primary mid-sequence;
promote the replica, continue the sequence on it, re-attach the old primary and compare.
*Landed in Task 3.*

0.4 **Random-sequence oracle.** A Hypothesis strategy over `{ADD, MADD, ADDBULK, DEL,
CREATERULE, DELETERULE}` on a small key set (reuse `tests/compat/fuzz_strategies.py`), driving
the primary and comparing digests on the replica after catch-up, with periodic reload and
resync. Promote every shrunk failure into a deterministic test.
*Landed in Task 4* (`test_replication_compaction_random_sequences`).

0.5 **Per-aggregator persistence property test** (Rust): for all 23 `Aggregator` variants,
random update sequences → `rdb_save`/`rdb_load` → identical `Hash` and identical `finalize()`.
*Landed in Task 5* as an integration test, not a Rust property test:
`tests/test_compactions_add.py::test_rule_state_survives_reload` runs once per aggregator (all 23),
reloads the server mid-bucket and compares the rule's published value with a twin rule created
after the reload and fed the whole sequence.

0.6 **Fix the stale AOF-rewrite assertion**: compare the source digest too.
*Landed in Task 5* (`tests/test_ts_aofrewrite.py`).

0.7 **Observability**: a compaction-failure counter and a rule-state-rebuild counter in the
module `INFO` section; warnings name the source and destination keys.
*Landed in Task 13*: `INFO ts_compaction` → `ts_compaction_failures`, `ts_compaction_state_rebuilds`.

0.8 **Document** in `docs/` that `ts-compatibility-mode` and `ts-compaction-policy` must match
across nodes, pending A.2.
*Landed in Task 12* (`docs/overview.md` *Replication*), extended in Task 14 with the version-skew,
mixed-version, keyspace-event and failure-recovery notes.

### Phase 1 — decide A or C (owner sign-off)

Inputs: Phase 0 results (any further reproduced divergences), stream-traffic estimate for B on
a realistic rule fan-out (`tools/fleet_loader` fleet with the default policy), and the
rolling-upgrade policy the project wants.

*Decided:* Option A (owner, 2026-10-14). Option C was not taken (§5, "Adopted").

### Phase 2 — A.1–A.4 (either option)

2.1 Persist the marker (in-place format change, encoding version unchanged — pre-release);
extend 0.2's full-resync and reload scenarios to assert the strict-mode reply.
*Obsolete:* the marker was removed with the DIV-0023 emulation in Task 6, so there is nothing to
persist.
2.2 Replicate policy-created destinations and rules; gate policy application on
`is_real_user_client`; fix the dropped-rule branch; test with mismatched configs on the two
nodes.
*Landed in Task 12.*
2.3 Resolve auto-create defaults into propagated `TS.ADD`/`TS.MADD`/`TS.ADDBULK`.
*Landed in Task 11*, as a resolved `TS.CREATE` replicated ahead of the write (not resolved options
inside the write), and also for an explicit `TS.CREATE`.
2.4 `applied_through` and the rebuild-on-mismatch check. The rebuild must reproduce the
fresh-rule semantics pinned by `test_createrule_on_a_source_that_already_has_data` — define
what `applied_through` is at rule creation before implementing.
*Landed in Task 13*, as a persisted `dirty` flag set when a rule application fails, not an
`applied_through` comparison: the next write rebuilds the open bucket only for a dirty rule.

### Phase 3 — effects replication (Option C only)

*Not taken* (§5, "Adopted"); the items below are the record of the design.

3.1 `TS._COMPACT` internal command (positional registration next to `TS._RESTORE`; refuse real
user clients outside debug mode).
3.2 Record removals alongside `written`; emit effects in commit order after the source write;
verify the `MULTI`/`EXEC` wrapping on the replica with `MONITOR`.
3.3 Skip `apply_compaction` for replicated and AOF-applied source writes.
3.4 Derived `LATEST` on replicas and the promotion path via 2.4.
3.5 Re-run the whole of Phase 0 unchanged — the suite is the acceptance test. Extend the
compat harness only where replica-visible behavior changed (none expected on the primary).
3.6 ASM: migrate a source+destination pair mid-bucket under write load and compare digests on
the target (`tests/test_ts_asm.py`).

### Phase 4 — proposal updates

Record the outcome in `proposal.md` / `proposal-complete.md` §3.5 and the readiness criteria,
and add a `COMPATIBILITY.md` note if any replica-visible behavior differs from RedisTimeSeries.
*Landed in Task 14* (the proposals, this document and `docs/overview.md`); the `COMPATIBILITY.md`
entries are DIV-0023 (ungated, Task 6) and DIV-0060 (Task 8).

## 7. Out of scope here

The retention-trim cron (now covered by `test_replication_retention_trim_cron`), the
`*`-timestamp replica test, and `twa`. Cluster fan-out semantics are unaffected: compaction is
single-slot by construction.
