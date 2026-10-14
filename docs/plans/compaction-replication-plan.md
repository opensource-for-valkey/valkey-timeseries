# Compaction under replication: analysis and remediation plan

Status: analysis complete, remediation **not started**; a model decision (§5) is pending.
Implementation plan: `docs/plans/2026-10-14-compaction-effects-replication.md` (Parts 0–2 are
the committed fix; Part 3 is Option C, conditional on the checkpoint). Reference behavior
checked 2026-10-14 (§3.7): the reference recomputes on the replica and persists its marker.
Written 2026-10-14 against `dos-defense` at `f4f44d482`. Evidence in §3 comes from reading
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
the other). `CompactionPolicyConfig::match_policy` (`src/series/compaction_policy.rs`) collects the
matching policies into a `std::collections::HashSet`, whose iteration order is seeded per
process, and `add_default_compactions` (`src/series/utils.rs`) pushes the rules in that order.
Each node therefore builds its own order. Rule order is observable (it is hashed into the digest
and printed by `TS.INFO`) and decides the order in which the rules of one source are applied.

Consequence for the suite: with two rules the failure is a coin flip, so the scenario uses six
(1/720 chance that two shuffles coincide) and is marked `xfail(strict=True)`. Whichever task makes
rule creation order deterministic (e.g. sort by `(bucket_duration, aggregation)` before pushing, or
replicate the rules instead of re-deriving them) must remove the marker.

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

### Option C — B plus A.1–A.4 (recommended)

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

## 6. Plan

Phases are ordered so that each is independently landable. Phase 0 is no-regret under either
option and should start now; Phase 1 is the decision.

### Phase 0 — observe, pin, and test (either option)

0.1 **Expose the hidden state.** `TS.INFO key DEBUG` (or `TS._DEBUG COMPACTION key`) reports,
per rule: `bucket_start`, `has_samples`, `applied_through`, an aggregator snapshot, and on a
destination the marker. Tests and operators need this to say *where* two nodes differ.

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

0.3 **Restart and promotion variants** of 0.2: `DEBUG RELOAD` on the primary mid-sequence;
promote the replica, continue the sequence on it, re-attach the old primary and compare.

0.4 **Random-sequence oracle.** A Hypothesis strategy over `{ADD, MADD, ADDBULK, DEL,
CREATERULE, DELETERULE}` on a small key set (reuse `tests/compat/fuzz_strategies.py`), driving
the primary and comparing digests on the replica after catch-up, with periodic reload and
resync. Promote every shrunk failure into a deterministic test.

0.5 **Per-aggregator persistence property test** (Rust): for all 23 `Aggregator` variants,
random update sequences → `rdb_save`/`rdb_load` → identical `Hash` and identical `finalize()`.

0.6 **Fix the stale AOF-rewrite assertion**: compare the source digest too.

0.7 **Observability**: a compaction-failure counter and a rule-state-rebuild counter in the
module `INFO` section; warnings name the source and destination keys.

0.8 **Document** in `docs/` that `ts-compatibility-mode` and `ts-compaction-policy` must match
across nodes, pending A.2.

### Phase 1 — decide A or C (owner sign-off)

Inputs: Phase 0 results (any further reproduced divergences), stream-traffic estimate for B on
a realistic rule fan-out (`tools/fleet_loader` fleet with the default policy), and the
rolling-upgrade policy the project wants.

### Phase 2 — A.1–A.4 (either option)

2.1 Persist the marker (in-place format change, encoding version unchanged — pre-release);
extend 0.2's full-resync and reload scenarios to assert the strict-mode reply.
2.2 Replicate policy-created destinations and rules; gate policy application on
`is_real_user_client`; fix the dropped-rule branch; test with mismatched configs on the two
nodes.
2.3 Resolve auto-create defaults into propagated `TS.ADD`/`TS.MADD`/`TS.ADDBULK`.
2.4 `applied_through` and the rebuild-on-mismatch check. The rebuild must reproduce the
fresh-rule semantics pinned by `test_createrule_on_a_source_that_already_has_data` — define
what `applied_through` is at rule creation before implementing.

### Phase 3 — effects replication (Option C only)

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

## 7. Out of scope here

The retention-trim cron (now covered by `test_replication_retention_trim_cron`), the
`*`-timestamp replica test, and `twa`. Cluster fan-out semantics are unaffected: compaction is
single-slot by construction.
