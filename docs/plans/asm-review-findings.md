# Atomic slot migration review findings

**Date:** 2026-11-05  
**Status:** Findings recorded; fixes not implemented by this review.  
**Scope:** ASM event handling, delayed indexing, source cleanup, snapshot restore,
thread scheduling, and related tests.
**Second pass (index path, 2026-11-05):** [ASM-05](#asm-05-validate-the-asm-event-payload-before-interpreting-it)–[ASM-12](#asm-12-test-coverage-for-the-fixes-above)
extend this document; they cover the FFI event boundary, the cleanup scan, queue lifecycle
bookkeeping, and metrics/clarity details. Findings that restate an item above are folded in as
cross-references rather than separate entries.
**Third pass (server contract, 2026-11-06):** [re-examines every item above](#third-pass--server-contract-2026-11-06)
against the server's own ASM state machine and four new live probes; adds ASM-13–ASM-15 and a
verified-behaviour list. Nothing has been fixed between passes.

The review found one reproduced correctness bug, two lifecycle races identified by code
inspection, and a scheduling failure gap. The existing ASM and restore regression suites
passed. Passing those suites does not establish correctness for the interleavings below.

The review used this repository, the locally built Valkey server, and
[Valkey's public ASM documentation](https://valkey.io/topics/atomic-slot-migration/).
No RedisTimeSeries source or test code was consulted.

## Findings

| ID | Priority | Finding | Evidence |
| --- | --- | --- | --- |
| ASM-01 | P1 | Replayed creation and label changes bypass deferred indexing | Reproduced against a running cluster |
| ASM-02 | P1 | Old export cleanup can retire series imported by a later migration | Code inspection; interleaving not forced in a test |
| ASM-03 | P1 | An old drain can consume a later import's queued keys | Code inspection; interleaving not forced in a test |
| ASM-04 | P2 | Failed thread creation leaves a completed import without an indexing retry | Code inspection; spawn failure not injected |
| ASM-05 | P2 | ASM event payload is cast to a hand-mirrored struct with no null/version/range validation | Code inspection; payload from a drifted ABI would be reinterpreted |
| ASM-06 | P2 | Cleanup scan is O(all series) per export, counts slots element-wise per db, and may be unnecessary | Code inspection; needs the experiment below to settle |
| ASM-07 | P2 | An abort/completion that reports ranges different from its start leaks slots in `IMPORTING_SLOTS` forever | Code inspection; depends on server-reported ranges |
| ASM-08 | P3 | `flush` duplicates `TimeseriesIndex::mark_ids_as_stale`; the scan loop is harder than its invariant | Code inspection |
| ASM-09 | P3 | `post_migration_cleanup_ids_marked` counts ids queued for marking, not ids marked | Code inspection |
| ASM-10 | P3 | Global mutex taken per loaded/restored key on the RDB-load path | Code inspection |
| ASM-11 | P3 | Completion work has no coalescing or bound; each drain ends in a full index optimize | Code inspection; see also ASM-04 |
| ASM-12 | P2 | Unit coverage misses production conversion, queue ownership, and the cleanup loop | `asm.rs` test module inventory |

### ASM-01: Replayed writes bypass deferred indexing

Relevant code:

- [create_series](../../src/series/utils.rs) indexes newly created series immediately.
- [TS.ALTER update_series](../../src/commands/ts_alter.rs) removes and reindexes a series
  when its labels change.
- [TS._RESTORE](../../src/commands/ts_restore.rs) and the
  [restore event handler](../../src/series/index/server_events.rs) defer indexing while
  a key's slot is importing.

ASM replays mutations made after the source snapshot. A replayed `TS.CREATE`, creation
through `TS.ADD`, or `TS.ALTER ... LABELS` can therefore update the destination index
before the import completes. The creation and label-change paths bypass the deferral
used by restore. The cluster probe directly exercised `TS.CREATE` and `TS.ALTER`;
creation through `TS.ADD` shares `create_series` but was not separately probed.

While the source migration was held in `waiting-to-pause`, the destination reported
`DBSIZE=0`, yet its local index exposed both a newly created series and an existing
series whose labels had changed. Cluster-wide `TS.CARD` returned **2 instead of 1** for
each selector. `TS.MGET` returned the newly created series twice, including its sample.
Valkey's documented ASM behavior hides importing slots from reads until ownership moves.

The observed data after commit was intact. This probe demonstrated premature visibility
and duplicate results, not permanent data loss.

**Suggested fix:** Centralize import-aware index mutation. Creation and label updates
must preserve the destination's hidden state and queue the final key for publication
after its import completes. Audit other mutation paths, including copy and rename, for
the same invariant. Imported IDs must remain safe before their eventual local remap.

**Regression recipe:**

1. Start a three-primary cluster with one replica per primary and module debug mode enabled.
2. Choose a hash tag owned by source A and create an existing series in that slot with
   `LABELS review before`.
3. On A, issue `DEBUG SLOTMIGRATION PREVENT-PAUSE 1`, start migration to B with
   `CLUSTER MIGRATESLOTS SLOTSRANGE <slot> <slot> NODE <B-id>`, and wait for A's job to
   reach `waiting-to-pause`.
4. Confirm B's local `TS._DEBUG QUERYINDEX review=before` is empty.
5. On A, create a second series in the same slot with `LABELS review created`, add
   sample `(1000, 42)`, and alter the existing series to `LABELS review altered`.
6. Wait for replay. Inspect B's local index and run cluster-wide
   `TS.CARD FILTER review=created`, `TS.CARD FILTER review=altered`, and
   `TS.MGET FILTER review=created`.
7. Expect no matching destination index entries before commit, a count of one for each
   cluster selector, and one MGET row. The probe instead observed destination entries,
   counts of two, and two identical MGET rows.
8. Release `PREVENT-PAUSE`, wait for completion and index catch-up, then verify B's
   series data. Always release the debug setting and clean up any active migration.

The temporary probe lived at `/private/tmp/test_asm_review.py` during the review. It is
not a tracked regression test; the recipe above preserves the reproduction independently
of that temporary file. Promote it into the integration suite with the fix.

### ASM-02: Old export cleanup can retire newly returned series

[remove_non_owned_keys](../../src/series/index/asm.rs) scans the current database index
and marks IDs stale whenever their keys hash into the completed export's slot ranges.
Despite its name, it does not check current ownership. The background task retains slot
ranges but no migration generation or immutable set of exported IDs.

Possible interleaving:

1. A exports a slot to B and schedules source cleanup.
2. Cleanup is delayed or is still scanning a large database.
3. B exports the slot back to A, and A indexes the returned series under fresh IDs.
4. A's old cleanup reaches those IDs and retires them because their keys occupy the
   original export's slots.

The keyspace can then contain valid series that are absent from the index, with no
remaining import drain to repair them. This interleaving was identified by inspection,
not reproduced in the cluster probe.

**Suggested fix:** Bind retirement to the exported series IDs or to a slot generation
that is validated atomically with the index mutation. A separate ownership check before
mutation is insufficient if another transition can occur between the check and retirement.
Removing or replacing this scan also removes the exposure; see ASM-06 for the evidence that it
may be redundant with `unlink2` and for the experiment that settles it.

**Regression:** Hold an export cleanup pending, migrate its slot back, complete local
index publication, then release the old cleanup. Assert that the new IDs remain indexed.

### ASM-03: Old drains can take a later import's queued keys

In [asm.rs](../../src/series/index/asm.rs), `process_delayed_indexing` schedules work,
but `take_delayed_keys_in_slots` extracts keys only when that worker runs. The shared
queue is partitioned by database and slot, not by migration job. Although the server
event includes `job_name`, that identity is not retained by the handler or workers.

If an earlier import completes, its slot leaves the node, and a later import of the same
slot starts before the earlier worker drains its queue, the earlier worker can take
the later import's keys. It can then index those keys before the later import completes.
The comment that concurrent drains touch disjoint keys does not cover successive
migrations that reuse a slot while old work remains pending.

**Suggested fix:** Detach the completing import's queue in its event handler, while
the event and queued keys still have an unambiguous owner. Retain job identity or a slot
generation through processing, and validate each batch before publishing it: detaching
names alone does not prevent those names from referring to a later import's values.
Recording the ranges per job also fixes the removal side (ASM-07), where a mismatched abort
report leaks slots in `IMPORTING_SLOTS` permanently.

**Regression:** Suspend an import drain, export and re-import the slot, then resume the
old drain while the new import is active. Assert that the old worker neither takes nor
publishes the new import's keys and that the new import's completion publishes them once.

### ASM-04: Thread creation failure strands indexing

Each completion launches a thread through `spawn_task` in
[threads/mod.rs](../../src/common/threads/mod.rs). On spawn failure, the launcher logs
and increments a metric, then drops the job. The import has already completed and its
slots have been removed from `IMPORTING_SLOTS`; the queued keys have no scheduled retry.
They can remain unindexed indefinitely during normal operation.

**Suggested fix:** Use a persistent worker with retained pending work and bounded
concurrency. Submission or worker-start failure must leave the job eligible for retry.
Do not suppress a completion merely because another drain is already running.

**Regression:** Inject worker-start failure and verify that the import's queued work is
retained and retried successfully without requiring another migration or a restart.

## Second pass — index path (2026-11-05)

This pass covered the FFI event boundary, the source-side cleanup scan, the delayed-indexing
queue's lifecycle bookkeeping, and metrics/clarity details. It was a static review of this
repository plus the `range-set-blaze` 0.8 and `valkey-module` 0.1.15 crate sources in the cargo
registry; unlike the first pass it ran no cluster probe, so every item below is inspection-level
unless stated otherwise. Priorities: P1 as above, P2 = correctness/robustness risk or a
prerequisite for scaling, P3 = clarity and housekeeping.

### ASM-05: Validate the ASM event payload before interpreting it

The event payload is the only server struct in this subsystem that the `valkey-module` bindings
do not describe. `valkeymodule.h` in 0.1.15 exposes
`VALKEYMODULE_OPTIONS_HANDLE_ATOMIC_SLOT_MIGRATION` but neither the event id, the subevent ids,
nor `ValkeyModuleAtomicSlotMigrationInfo`, so `19`, `0..=5` and the `#[repr(C)]` layout in
[asm.rs](../../src/series/index/asm.rs) are all transcribed by hand. Yet `raise_event` (asm.rs:185)
does:

```rust
let info = unsafe { &*(data as *const ValkeyModuleAtomicSlotMigrationInfo) };
slot_migration_event_handler(event, info.convert_slot_ranges());
```

and `extend_slot_ranges` then walks `slot_ranges[0..num_slot_ranges]` as a raw pointer array. The
existing `res != REDISMODULE_OK` check in `subscribe_to_atomic_slot_migration_events` catches an
event id the server rejects outright, but not a **valid id for the wrong event** — in which case
another event's payload is reinterpreted as slot ranges, with `num_slot_ranges` driving the walk.
`num_slot_ranges` and each `c_int` range bound are likewise unvalidated: a negative or >65535
`c_int` silently wraps through `as u16`.

**Suggested fix:** reject `data.is_null()`; reject `version != 1` with a warning naming the
version, so an ABI bump is loud rather than fatal; reject `num_slot_ranges > 16384` and, per
range, `start > end` or either bound outside `0..16384` (skip that range, do not wrap). Return
`Option<RangeSetBlaze<u16>>` from the parser so the handler decides whether to raise the event; on
a rejected `ImportStarted`, fall back to *no* deferral rather than registering a slot set that a
later event may not be able to remove (see ASM-07).

**Validation:** unit-test against a synthetic struct — constructible in `#[cfg(test)]` because it
is module-local — with cases: valid, `version = 2`, null `slot_ranges`, `num_slot_ranges = u32::MAX`,
inverted range, boundary 0 and 16383.

### ASM-06: The cleanup scan is O(all series) per export, and may not be needed at all

[remove_non_owned_keys](../../src/series/index/asm.rs) (asm.rs:417) walks the entire `id_to_key`
BTreeMap of every db in 256-entry windows, computing CRC16 over **every** key name, and re-seeks
the map per window. Exporting one slot out of 16 384 therefore pays a full-fleet key scan; nothing
short-circuits on an empty slot set or an empty match. Two secondary costs sit in the same
function and are worth fixing regardless of the outcome below:

- `source_slots.iter().count()` in the `log_notice!` at asm.rs:434. `RangeSetBlaze::iter()` yields
  elements, so this is up to 16 384 steps **per db**, evaluated eagerly. `ranges_len()` — already
  used correctly at asm.rs:390 — is the right call.
- The read guard is reacquired per window, so a large fleet costs tens of thousands of
  postings-lock acquire/release pairs, interleaved with main-thread readers.

**Why it may be removable.** [unlink2](../../src/series/series_data_type.rs) is documented as the
sole owner of single-key index retirement, and the server's own unowned-key cleanup on the source
primary deletes through `dbGenericDelete`/`dbAsyncDelete`, both of which fire `unlink` first. That
is exactly the argument the doc comment at asm.rs:509-522 makes for *replicas*, without making it
for the primary. Combined with ASM-02 — where the same scan is unsound because it retires
*re-imported* series — the sweep looks like a net for one narrow case: an id that `unlink2` could
not retire because the index's id for that key had drifted (`remove_timeseries_for_key` returns
`false` and logs `"was not indexed under its key"`).

**Suggested fix, in order:** (1) land the cheap wins — `if source_slots.is_empty() { return 0; }`,
`ranges_len()`, and demote the per-db log to `log_debug`; (2) **experiment** rather than assume:
delete the sweep and run `test_source_index_cleared_after_migration` and
`test_source_replicas_consistent_after_migration`; if both stay green, removing it also closes
ASM-02, and the reason should be recorded where the next reader will look; (3) if it must stay as
a drift net, state the drift case in the doc comment and move the scan off the per-migration path
(periodic maintenance) rather than gating it behind another knob.

**Validation:** the cluster suite, plus the `TS._DEBUG QUERYINDEX`/`DBSIZE` assertions already in
those two tests.

### ASM-07: A completion or abort that reports different ranges leaks slots forever

`remove_importing_slots` (asm.rs:602) subtracts whatever ranges the abort/completion event
reports, and never checks them against what `ImportStarted` registered. If the server ever
reports a narrower range than its start, those slots remain in `IMPORTING_SLOTS` (asm.rs:583) for
the life of the process: every key in them is queued by
[is_key_in_slot_import](../../src/series/index/asm.rs) and never indexed, with no error and no
bound. The queue's `kept` set has the same shape. This is the removal-side counterpart of ASM-03:
the same identity the event already carries (`job_name`, unused at asm.rs:154) would make both
sides exact.

**Suggested fix:** record the ranges registered per job and remove *those* on completion/abort,
logging a warning when the event's ranges differ; or, as in ASM-03, key the whole structure by
`job_name`. Read `job_name` as a NUL-terminated `&[u8]`/`&CStr` (the field is `[c_char; 41]`).

**Validation:** unit test — `ImportStarted(S)` → `ImportAborted(S')` with `S' ⊂ S` → assert no slot
of `S` remains importable, and that a key in `S \ S'` is indexed immediately.

### ASM-08: `flush` duplicates an existing method; the scan loop is harder than its invariant

`flush` (asm.rs:420-429) does `get_db_index(db)` + `write_lock(&index.inner)` +
`postings.mark_ids_as_stale(batch)` — byte-for-byte what
[`TimeseriesIndex::mark_ids_as_stale`](../../src/series/index/timeseries_index.rs) already does
publicly. Separately, the outer loop (asm.rs:440-501) is more complex than its own invariant:
because both exit branches flush, `batch` is provably empty at the top of every iteration, and the
final iteration exists only to discover `processed == 0` after an extra read-lock acquisition and
range seek. `processed` exists solely to distinguish "window exhausted" from "window full".

**Suggested fix:** call `index.mark_ids_as_stale(batch)` and drop the closure; restructure the scan
as a loop over `range(cursor..).take(BATCH_SIZE)` that tracks the visited count from the iterator
itself (break on `None`, flush once per window, state the invariant in one comment line). Do this
**after** ASM-12's cleanup test exists.

### ASM-09: `post_migration_cleanup_ids_marked` counts queued ids, not marked ids

`deleted_count` (asm.rs:431) is incremented when an id is pushed into `batch`, before the write
lock is taken, and is fed to `POST_MIGRATION_CLEANUP_IDS_MARKED` at asm.rs:549. Between the
read-lock scan and the flush, a concurrent `unlink2` can retire the same id — harmless, since
`mark_ids_as_stale` is idempotent — so the counter can exceed the number of ids actually marked.

**Suggested fix:** have the flush path return the delta (`stale_ids.cardinality()` before/after, or
count `id_to_key.remove` hits) and feed that to the metric. Moot if ASM-06 removes the sweep.

### ASM-10: Global mutex taken per loaded/restored key

[is_key_in_slot_import](../../src/series/index/asm.rs) (asm.rs:588) is called for every
`loaded`/`restore` keyspace event and locks `Mutex<RangeSetBlaze<u16>>` unconditionally. All
callers are main-thread, so it is uncontended, but a 10M-key RDB load pays 10M lock/unlock pairs to
read a state that is empty in almost every run. (The CRC16 is correctly paid only when non-empty.)

**Suggested fix:** maintain an `AtomicUsize` job count alongside `IMPORTING_SLOTS` and check it
before locking: `if count.load(Relaxed) == 0 { return false; }`. Increment *before* inserting the
slot set and decrement *after* removing it, both under the mutex, so `count == 0` can never
coincide with a non-empty set; the fast path may then only err toward taking the lock.

### ASM-11: Completion work has no coalescing or bound

Both drains (asm.rs:397) and cleanup (asm.rs:534) spawn a fresh OS thread per completion with no
single-flight and no queue. The absence of a guard is deliberate and correct today (asm.rs:377-381),
and ASM-04 covers the failure mode. The remaining cost is that N overlapping completions produce N
threads that serialise on the GIL, and each drain that indexed anything ends with a **full**
`optimize_index_fully` pass per db (asm.rs:405) rather than an incremental one.

**Suggested fix:** only with a measurement — a long-lived drain worker (or pending-slot
accumulator) that coalesces work and runs one optimize pass per db per cycle. Much easier once
ASM-03/ASM-07 introduce job identity, since coalescing then merges jobs instead of slot sets.

### ASM-12: Test coverage for the fixes above

The unit tests in `asm.rs` cover `parse_slot_ranges` (asm.rs:64), which is `#[cfg(test)]` and
unreachable from production — the same gap noted under Clarity and coverage below, so this entry
records the concrete designs rather than restating it.

- **Payload conversion/validation (ASM-05):** synthetic `ValkeyModuleAtomicSlotMigrationInfoV1` —
  overlapping ranges, unsorted ranges, a single slot, boundaries, plus the rejection cases.
- **Cleanup loop (ASM-06/ASM-08):** build a `TimeSeriesIndex` for a test db with owned and exported
  hash tags interleaved so exported matches are sparser than `BATCH_SIZE` (exercising the
  window-advances-without-flushing path); assert `id_to_key` retains exactly the owned ids and the
  return value equals the exported count. `remove_non_owned_keys` takes no GIL, so this is cheap.
- **Drain ownership (ASM-03):** `ImportStarted(S)` → queue → `ImportCompleted(S)` without draining
  → `ImportStarted(S)` → queue a second key → drain the first job → assert the second key is still
  queued. Fails against today's slot-only filter.
- **Slot-set removal (ASM-07):** see that entry.
- Follow `timeseries_index_tests.rs`'s serialization conventions for any test touching
  `TIMESERIES_INDEX`.

### Verified-correct behaviour (do not "fix")

Checked deliberately during the second pass; changing any of these while addressing the findings
above would introduce bugs:

- **GIL before postings.** `index_timeseries_in_batch` takes `MODULE_CONTEXT.lock_gil()` first,
  then the postings write lock — the R5 order.
- **Open keys before locking.** It opens every key in the batch *before* taking the postings write
  lock, because opening a key runs lazy expiry, which reaps through `unlink2` and takes the same
  write lock on the same thread. Same reasoning as `get_series_by_id`.
- **Duplicate queueing is safe.** `index_loaded_series` calls `remove_timeseries_for_key` first, so
  a key queued twice gets its first id retired rather than leaking (one extra tombstone per
  duplicate; not worth optimising).
- **Abort ordering.** `remove_importing_slots` before `discard_delayed_keys_in_slots` — flag first,
  so no new key is queued between the two.
- **Per-slot tracking over a global flag.** The overlapping-import fix (asm.rs:578-584) is correct;
  ASM-07 refines its key, it does not undo it.
- **Registration gating.** `is_clustered` reads `ContextFlags::CLUSTER` (cluster-enabled config),
  not "a cluster is formed", so deciding once at load time is sound.
- **Shutdown handling.** `TaskRun::check_shutdown` at batch and db boundaries, with partial runs
  deliberately not counted as completions.

## Performance assessment

The batch implementation has useful properties: it opens keys before taking the postings
write lock, avoids lazy-expiry lock re-entry, and bounds indexing batches to 256 keys.
Blocking work runs outside the shared computation pool. Snapshot serialization also
avoids taking the module GIL in the forked child.

The main scaling concerns are structural; no throughput or latency benchmark was run:

- Every export cleanup scans the entire database index, including unrelated slots.
  Repeated small migrations therefore repeat work proportional to the whole database
  (ASM-06, which also questions whether the scan is needed at all).
- Each drain with indexed keys calls `optimize_index_fully` for its database. Concurrent
  or successive small imports can repeatedly optimize unrelated posting lists (ASM-11).
- Each completion partitions shared pending-key vectors and recalculates slots. Separate
  queues per import would reduce repeated scanning and clarify ownership (ASM-03, ASM-07).
- Thread creation per completion allows pending cleanup, drains, and optimization work
  to accumulate without a subsystem concurrency bound (ASM-04, ASM-11).

After correctness fixes, measure single-slot migrations in a large otherwise unchanged
database and bursts of independent migrations. Record query latency, maximum GIL hold
time, catch-up duration, queue memory, and worker count. Consider coalescing optimization
per database and retaining a bounded set of cleanup targets per migration.

Index publication is currently asynchronous after import completion. Existing integration
tests allow up to 15 seconds for catch-up. That is a test timeout, not an established
latency guarantee; the intended query-consistency contract during catch-up should be explicit.

## Clarity and coverage

- Define lifecycle invariants around migration identity, slot reuse, and the ownership of
  pending work. Historical explanations do not substitute for those invariants.
- Rename `remove_non_owned_keys` if it continues to select by recorded export ranges,
  or make its implementation actually validate ownership and migration generation.
- Four of the five ASM unit tests cover `parse_slot_ranges`, which is compiled only for
  tests and is not the production event-conversion path. Replace that coverage with
  tests of production conversion, queue ownership, drain scheduling, and cleanup.
- The compaction-test introduction in [test_ts_asm.py](../../tests/test_ts_asm.py) still
  describes ID-based links; the implementation links partners by key. Update the comment.

## Validation recorded during the review

The module was rebuilt with `cargo build --lib`. Integration tests used the debug module
and the local `unstable` Valkey binary, which reported version `255.255.255` and source
revision `6fc33cfa`.

| Check | Result |
| --- | --- |
| `cargo test --lib --features enable-system-alloc series::index::asm::tests` | 5 passed |
| `tests/test_ts_asm.py` | 10 passed |
| `tests/test_ts_restore_malformed.py` | 6 passed |
| New paused-import probe | Failed on expected cardinality 1 versus observed 2; duplicate MGET rows also observed |

The tests do not cover the forced interleavings proposed for ASM-02 and ASM-03 or injected
thread-start failure for ASM-04. Neither pass changed any implementation or tracked test.

## Consolidated fix order

Both passes agree on the invariants that matter (migration identity, slot reuse, ownership of
pending work); they differ only in which ones they reach from. Suggested sequence, smallest and
most independent first:

1. **ASM-05** — validate the event payload. Independent of everything else, and it closes the only
   memory-safety exposure in the subsystem.
2. **ASM-06, cheap half** — `is_empty` short-circuit, `ranges_len()`, demote the per-db log, and
   add ASM-12's cleanup-loop unit test. No behaviour change; the test is what makes the rest safe.
3. **ASM-01** — the only reproduced bug. Centralizing import-aware index mutation is also the hook
   the remaining lifecycle fixes want, so do it before ASM-03/ASM-07 rather than after.
4. **ASM-03 + ASM-07 (+ ASM-12 tests)** — one job-identity change covering both the queue and the
   slot registration, with the sequential-overlap and mismatched-abort unit tests. Promote ASM-01's
   reproduction recipe and ASM-02/ASM-03's interleavings into `tests/test_ts_asm.py` here.
5. **ASM-02 + ASM-06, experiment half** — decide whether the cleanup scan survives. Removing it
   closes ASM-02 and makes ASM-09 moot; keeping it requires the generation check ASM-02 asks for.
6. **ASM-08, ASM-10, ASM-04, ASM-11** — housekeeping, then bounded threading. ASM-11 and ASM-04
   belong to the same change and need the latency/queue/wait measurements listed under Performance
   assessment to justify them.


## Third pass — server contract (2026-11-06)

**Method.** The first two passes derived the server's behaviour from the public ASM topic page and
black-box observation. This pass read the server side directly — Valkey is BSD-3, so unlike
RedisTimeSeries its source is in scope — at the revision the local `unstable` binary was built from
(`tests/build/valkey`, commit `6fc33cf`, the same `6fc33cfa` the first pass tested against):
`cluster_migrateslots.c` (job state machine, event firing, import/abort cleanup),
`cluster_legacy.c` (dirty-slot deletion on ownership change), `db.c`/`server.c` (unlink hooks,
client classification) and `valkeymodule.h` (event payload). It then ran four new probes against a
three-primary/one-replica cluster on that binary and the release module built from this tree
(`target/release/libvalkey_timeseries.dylib`, newer than HEAD), plus the ASM unit tests. The probe
file is not tracked; its four scenarios are reproduced as recipes below.

Priorities are as in the second pass. Nothing in `src/series/index/asm.rs` has changed since the
first pass (last commit `d3ecba425`, 2026-11-02), so every status below is "still open" unless the
entry says otherwise.

### What the server actually does (facts the earlier passes assumed)

These settle several "depends on the server" caveats above.

1. **Events fire on replicas and on AOF replay, not only on the primary.** `IMPORT_STARTED` is
   raised from `CLUSTER SYNCSLOTS ESTABLISH`, which the target primary propagates to its replicas
   and to its AOF (`forceCommandPropagation(c, PROPAGATE_REPL | PROPAGATE_AOF)`); replicas create a
   *tracking-only* job and fire the same event. `IMPORT_COMPLETED`/`IMPORT_ABORTED` are raised from
   `finishSlotMigrationJob`, which replicas reach through the propagated `SYNCSLOTS FINISH`. So a
   target replica defers and drains exactly like its primary — which is why
   `test_target_replicas_index_consistent` passes without any replica-specific module code. The
   module doc comment's claim that "the event is only received on the source primary" is wrong for
   the import side (it is right for the export side: `EXPORT_*` fire only where the export job runs).
2. **The import client is a replicated client.** `isReplicatedClient(c)` is
   `c->flag.primary || (c->slot_migration_job && isImportSlotMigrationJob(...))`, and
   `RM_GetContextFlags` sets `CTX_FLAGS_REPLICATED` from it. `TS._RESTORE`'s `is_real_user_client`
   guard therefore admits the import stream in production — confirmed live by probe P4 (below),
   which is the first time the guard has been exercised with `ts.debug-mode` off. On replicas the
   feed is `flag.primary`; on AOF load it is `CLIENT_ID_AOF`. All three paths pass the guard.
3. **Source-side deletion runs `unlink2` for every exported key, before the module's cleanup
   thread can run.** On the ownership change, `clusterUpdateSlotsConfigWith` calls
   `clusterUpdateSlotExportsOnOwnershipChange()` (which finishes the export job and fires
   `EXPORT_COMPLETED`) and then, in the same main-thread call, `delKeysInSlot(slot, lazy, true,
   false)` for each dirty slot. `delKeysInSlot` goes through `dbAsyncDelete`/`dbSyncDelete` →
   `dbGenericDelete` → `moduleNotifyKeyUnlink`, i.e. the type's `unlink2`, synchronously per key,
   and additionally sends a module-only `del` keyspace notification. The module's
   `handle_post_migration_cleanup` thread needs the GIL, so by the time it scans, every exported
   key has already been retired through `unlink2`. This is direct evidence for ASM-06's "may be
   redundant" and removes the "replica vs primary" asymmetry the doc comment argues about: the
   primary is handled by the same hook as the replicas.
4. **Import abort deletes through the same path.** `SLOT_IMPORT_FINISHED_CLEANING_UP` runs
   `delKeysNotOwnedByMyself` (→ `delKeysInSlot` → `unlink2`) *before* `finishSlotMigrationJob`
   fires `IMPORT_ABORTED`. So by the time `discard_delayed_keys_in_slots` runs, the keys are gone
   and `unlink2` has already seen each of them (returning `false` from
   `remove_timeseries_for_key`, since they were never indexed). The abort path is sound; its only
   cost is one debug log line per never-indexed key.
5. **The payload struct matches the header at this revision.** `ValkeyModuleAtomicSlotMigrationInfoV1`
   is `{ uint64_t version; char job_name[41]; ValkeyModuleSlotRange *slot_ranges; uint32_t
   num_slot_ranges; }` with `int start, end` ranges, initialised with `version = 1`, ranges
   allocated per event and freed after the callback returns. The module's `#[repr(C)]` mirror is
   field-for-field identical. ASM-05 stands as a hardening item, not a current bug: the server
   validates `0 ≤ start ≤ end < 16384` on the way in, so today's values can't wrap `as u16`.
6. **RDB-restored import jobs fire no event.** `clusterRDBLoadSlotImport` (the RDB aux record a
   target writes for in-flight imports) calls `createSlotImportJob(NULL, …)` and does *not* call
   `fireModuleSlotMigrationEvent`; only the ESTABLISH command path does. See ASM-13.
7. **Series ids are `[24-bit random epoch | 40-bit counter]`** (`src/series/index/ids.rs`), minted
   per process. Two nodes share an id only if they drew the same epoch (≈1 in 16 M). This matters for
   ASM-01's `TS.ALTER` variant, below.

### Status of ASM-01 … ASM-12

| ID | Status after this pass | What changed |
| --- | --- | --- |
| ASM-01 | **Open, reproduced again (P1).** | Same numbers as the first pass: `TS.CARD` 2, two identical `TS.MGET` rows, target `DBSIZE` 0 while its local index already holds both series. One refinement: the replayed `TS.ALTER … LABELS` runs `remove_timeseries(series)` **by the series' foreign id** (the restored series keeps the source's id until the drain remaps it) and then re-indexes it under that foreign id. Thanks to fact 7 this is a warning (`Tried to remove non-existing series id …`) per replayed ALTER rather than an index corruption, and the drain's key-checked retire-and-reassign cleans it up. It would corrupt an unrelated local series only on an epoch collision; the fix for ASM-01 should route ALTER through the same key-checked path regardless. |
| ASM-02 | **Open; now firmly a drift-net question.** | Fact 3 shows the scan never retires anything `unlink2` hasn't already retired, except ids whose index entry drifted from their key. P3 (six back-and-forth migrations of one slot, 300 series) stayed consistent on every round, but P3 does not hold the old cleanup pending, so the forced interleaving remains unexercised. |
| ASM-03 | **Open; unexercised.** | P3's natural timing never left an old drain pending across a re-import (each round's drain finished within the ~0.2 s migration). The forced interleaving still needs the unit test from ASM-12. |
| ASM-04 | **Open.** | `spawn_task` only bumps `spawn_failures`; no retry. Unchanged. |
| ASM-05 | **Open; downgraded from "memory-safety exposure" to hardening.** | Fact 5: layout verified, server-validated bounds. Keep the null/version checks; they are cheap. |
| ASM-06 | **Open; evidence now favours removal.** | Fact 3 is the ordering argument the second pass asked for. The two cheap wins (`is_empty` short-circuit, `ranges_len()` instead of `iter().count()` at asm.rs:437 and :527) are still not done. |
| ASM-07 | **Open.** | The server reports the job's own `slot_ranges` list on every event for that job, so a start/end mismatch cannot come from the server today; the leak needs a module-side bug (e.g. ASM-05 rejecting one range). Still worth keying by `job_name` — fact 1 shows replicas see the same `job_name` as their primary, so it is a stable identity across the shard. |
| ASM-08, ASM-09, ASM-10, ASM-11 | **Open.** | Unchanged. ASM-09 becomes moot if ASM-06 removes the scan. ASM-11's "each drain ends in a full optimize" was visible in P3 as six `optimize_index_fully` passes for a 300-series slot — correct, just wasteful. |
| ASM-12 | **Open; one gap closed by P4, one added.** | P4 verifies the production (`debug-mode off`) path of `TS._RESTORE` for the first time — but only as a throw-away probe. The integration suite runs every ASM test under `ValkeyTimeSeriesClusterTestCaseDebugMode`, so CI never exercises the guard. Add one non-debug migration test (recipe P4). |

The stale compaction-test comment (ID-based links) flagged under *Clarity and coverage* is still at
`tests/test_ts_asm.py:638-642`.

### New findings

#### ASM-13 (P3): a node that restores an in-flight import from RDB indexes the importing keys

Fact 6: when a target primary or target replica restarts from an RDB written mid-import, or a target
replica full-syncs from its primary mid-import, the server recreates the import job from the RDB aux
record and hides the slots, but fires no `IMPORT_STARTED`. `IMPORTING_SLOTS` stays empty, so every
`loaded` event for a key in those slots goes straight to `index_series_by_key` (or the bulk build /
aux-preload path). Those series are then visible to this node's local `TS.MRANGE`/`TS.MGET`/
`TS.CARD` (and to cluster fan-out via this node) while the server still hides them from
`DBSIZE`/`SCAN` — the same phantom-read class as ASM-01, reachable without any write replay.

It self-heals: on `FINISH SUCCESS` the drain finds nothing queued; on `FINISH FAILED` the primary's
propagated `DEL`s (or its own `delKeysNotOwnedByMyself`) reach `unlink2`. A secondary effect on the
aux-preload path: the primary's saved postings index excludes the importing keys while
`LOADED_SERIES_STATS` counts them, so `reconcile_preloaded_indexes` sees `indexed != loaded` and runs
the full repair sweep after every such load.

**Suggested fix.** The module API has no way to ask the server which slots are importing, but
`CLUSTER GETSLOTMIGRATIONS` is callable from a context on `LoadingSubevent::Ended`: for every
in-progress `import` job, register its ranges in `IMPORTING_SLOTS` and move the ids already indexed
in those slots to the delayed queue (one `id_to_key` scan over the loaded db, the same loop
`remove_non_owned_keys` has today). Lower-cost alternative: document the gap. Either way, record it
under the ASM-01 fix so the "import-aware index mutation" has one owner.

**Regression.** Start an import with `PREVENT-PAUSE` held, `DEBUG RELOAD` (or restart) the target
primary, then assert its local `TS._DEBUG QUERYINDEX` for the slot's label is empty and that after
release the series appear exactly once.

#### ASM-14 (P3, unverified): AOF replay of an import that never finished

Fact 1 means an AOF written mid-import contains `SYNCSLOTS ESTABLISH`, the `TS._RESTORE`s, and —
only if the import finished before the crash — `SYNCSLOTS FINISH`. On replay the module queues the
keys under `IMPORTING_SLOTS`. If the `FINISH` is present, `IMPORT_COMPLETED` fires *during loading*
and the drain thread takes the GIL in the server's `processEventsWhileBlocked` windows
(`server.c` releases it there), which is legal but means the drain can run while the per-key AOF
indexing path is still active. If the `FINISH` is absent, the server leaves the job in
`SLOT_IMPORT_OCCURRING_ON_PRIMARY` and the module leaves the slots in `IMPORTING_SLOTS` and the keys
queued, with no event ever coming (the server only cancels such jobs on promotion or full sync).
Neither path was probed. Worth one AOF-replay test with and without the `FINISH` line before
deciding whether anything needs doing; the ASM-13 load-end reconciliation would cover the second
case for free.

#### ASM-15 (informational): the first fan-out after a handover fails with a retriable error

In every P3 round except the first, the first `TS.CARD`/`TS.MGET` issued after the migration
reached a terminal state failed with `A multi-shard command failed because the cluster topology has
changed`, then succeeded on retry. This is the fan-out's cluster-map refresh working as designed
(`src/fanout/fanout_error.rs:145`), not an index problem; it is recorded here because an operator
reading P2/P3 timings will see it, and because the compat/divergence docs should say clients must
retry it, if they don't already.

### Probe results

| Probe | Setup | Result |
| --- | --- | --- |
| P1 — ASM-01 recipe | 1 existing series, migration held at `waiting-to-pause`, replayed `TS.CREATE`+`TS.ADD`+`TS.ALTER` | Reproduced: target local index shows both series at `DBSIZE` 0; cluster `TS.CARD` 2 for both selectors; `TS.MGET` returns the created series twice. After commit: 1, 1, data intact. |
| P2 — visibility gap after handover | 2,000 series in one slot, `TS.CARD` sampled from an uninvolved node every ~0.3 ms through the migration and 3 s past completion | Migration took 64 ms. 24,927 samples; none below 2,000 after completion; first full count at +0.0 s. The async drain is below sampling resolution at this size. One sample 7 ms before completion returned the ASM-15 error. |
| P3 — slot reuse | 300 series with data, migrated A→B→A→B→A→B→A (six jobs, ~0.12–0.23 s each) | Every round: destination local index == key set, source local index empty, source `DBSIZE` 0, cluster `TS.CARD` 300, `TS.MGET` 300 rows. One ASM-15 error per round after the first. |
| P4 — production guard | Same cluster class with `ts.debug-mode` **off**, 50 series with data | `TS._DEBUG` rejected (debug really off); job state `success`; `TS.CARD` 50; `TS.MGET` 50 rows; every `TS.RANGE` on the target returns its sample. |

Recipes for P2–P4 are straightforward variants of the helpers in `tests/test_ts_asm.py`
(`_find_hash_tag_for_shard`, `_migrate_slot_range`, `_local_queryindex`); two gotchas for anyone
re-running them outside `build.sh`: `MODULE_PATH` must be absolute (the cluster tests `chdir`), and
keyless fan-out commands (`TS.CARD`, `TS.MGET FILTER …`) must go to a node client, not the
`ValkeyCluster` client, which cannot route them.

### Validation recorded during this pass

| Check | Result |
| --- | --- |
| `cargo test --lib --features enable-system-alloc series::index::asm::tests` | 5 passed (unchanged set) |
| P1–P4 above against `unstable` `6fc33cf` + release module from this tree | P1 reproduced ASM-01; P2, P3, P4 passed their assertions |
| `tests/test_ts_asm.py` | Not re-run this pass (no code change since the first pass's 10/10) |

### Revised fix order

The second pass's order stands, with three amendments:

1. Fold **ASM-13** into the ASM-01 change (step 3): both are "the index learned about an importing
   key through a path that doesn't consult `IMPORTING_SLOTS`", and the load-end reconciliation is
   the natural place to also absorb ASM-14's orphaned-queue case.
2. **ASM-06's experiment can be run with more confidence**: fact 3 means deleting
   `remove_non_owned_keys` cannot leave an exported key indexed unless `unlink2` already failed to
   retire it, so the only thing to measure is whether the drift net has ever caught anything —
   `post_migration_cleanup_ids_marked` is exactly that counter once the double-counting of ASM-09
   is fixed; if it stays at zero across the suite and a soak, remove the scan.
3. Add the **non-debug ASM test** (P4) to ASM-12 and land it first: it is independent of every
   fix and protects a path CI has never run.

## Implementation status (2026-11-06)

Landed on `fix/asm-bug-sweep`, one commit per step of the revised fix order (this document itself
is not committed):

| Commit | Closes | What |
| --- | --- | --- |
| `9a6055531` | ASM-05 | Payload validated (null, version, range count, bounds); nine unit tests |
| `691c299cf` | ASM-06 cheap half, ASM-08, ASM-09 | Scan simplified; `mark_ids_as_stale` returns the retired count |
| `eec888eb3` | ASM-01, ASM-10, ASM-12 (part) | Create/ADD-create/ALTER/RENAME/COPY defer during an import; atomic fast path; replayed-mutation regression test; first non-debug-mode ASM test |
| `e7c5831c5` | ASM-03, ASM-07 | Imports keyed by job (`ImportJob`); completion detaches the job's own queue and releases the slots it registered |
| `714677e26` | ASM-02, ASM-06 | The experiment passed (source tests green with the scan disabled); scan, `post_migration_cleanup` task and `tombstone_full_sweep` removed |
| `c59984213` | ASM-04 | `spawn_task` reports failure; unstarted drain work is kept, folded into the next drain and retried every 20 s |
| `b10bafdcb` | ASM-13 | Load end asks `CLUSTER GETSLOTMIGRATIONS`, registers unknown imports and moves their keys out of the index; regression test restarts the target's replica mid-import |

Not done: **ASM-11** (coalescing drains across overlapping completions — the doc asks for a
measurement first), and **ASM-14** stays unverified. The ASM-13 reconciliation registers an
AOF-restored import that never reached `FINISH` only if the AOF's `ESTABLISH` did not already
(it does, since `IMPORT_STARTED` fires for the AOF client); the stranded queue in that case is a
server-side job that never ends and needs no module change.

Validation at the end: `cargo test --lib` 1762 passed; `tests/test_ts_asm.py` (14 tests, two
classes) plus the restore, keyspace, AOF-rewrite, dump/restore, alter, create, debug-metrics,
index-persistence, RDB-load-limit, replication and background-task suites all green on `unstable`
`6fc33cf`.

## Proposal assessed (2026-11-06): read-side hiding instead of deferred indexing

**Proposal.** On ASM start, set a flag on `Postings` (cleared on ASM stop). Add a `Postings` method
that returns a key by id; while the flag is set, return `None` for a key whose slot the node does not
own. Imported keys would then be indexed immediately, and visibility decided at read time.

**Verdict.** The direction is sound and buys a lot (below), but not as specified. Three things break;
one nearby variant fixes all three. Not implemented.

### What breaks in the literal version

1. **`None` from the key lookup already means "stale", and stale means delete.** Every reader that
   resolves ids treats a missing key as index corruption and repairs it:
   `querier::resolve_series_keys` (querier.rs:107-109), `batched_querier::for_live_keys`
   (batched_querier.rs:805-806), `querier::open_resolved_series` (querier.rs:356) and
   `TimeSeriesIndex::keys_for_selectors` (timeseries_index.rs:353) all push the id into a stale list
   and then call `mark_ids_as_stale`, which removes the `id_to_key` entry and the `all_postings` bit
   (postings/stale.rs:148-156). A `get_key_by_id` that returned `None` for an importing key would
   permanently un-index every imported series any query touched during the import, and nothing
   re-indexes it afterwards. The lookup needs a third outcome (visible / hidden / missing), or the
   readers need a visibility check in front of the stale repair.

2. **"Owned by the node" must not come from the fan-out cluster map.** `ClusterMap.owned_slots` is
   parsed from `CLUSTER NODES` (importing markers are skipped, cluster_map.rs:1066-1067) with a
   750 ms TTL (`CLUSTER_MAP_EXPIRATION_MS`), and is refreshed only by `get_fanout_targets` or a
   topology-mismatch error. A purely local `TS.MRANGE` on the target never refreshes it, so after
   `ImportCompleted` the imported series would stay hidden until some fan-out happened to rebuild
   the map. `i_own_slot` is also `cfg(test)`. The authoritative oracle already exists: the `IMPORTS`
   registry in asm.rs, updated synchronously on the main thread by the event handler, with
   `IMPORT_ACTIVE` as its atomic fast path. "Owned" should mean "not in an in-flight import's slots",
   i.e. `is_key_in_slot_import`. The proposed flag on `Postings` is redundant with `IMPORT_ACTIVE`,
   and would have to be global rather than per-db in any case: slots are a property of keys, not
   databases.

3. **The id-to-key lookup is not on every read path.** Index-only reads never touch `id_to_key`:
   - `TS.CARD` with filters and no date range uses bitmap cardinality
     (`get_cardinality_by_selectors`, querier.rs:452-456); bare `TS.CARD` uses `id_to_key.len()`.
   - `TS.LABELNAMES`, `TS.LABELVALUES`, `TS.QUERYLABELS`, `TS.METRICNAMES` and `TS.LABELSTATS`
     read through the `Terms` view, which by design has no access to the directory
     (postings/terms.rs:1-11).
   - The Card, LabelStats, LabelSearch and QueryLabels fan-out shards use the same paths.

   The ASM-01 reproduction above is `TS.CARD` returning 2 instead of 1 — exactly the path a
   `get_key_by_id` gate leaves unfixed. The literal refactor would reopen the one bug this branch
   reproduced.

### What read-side hiding buys, done right

It removes the whole class of "a mutation path forgot to defer". Seven call sites intercept writes
today (`create_series`, `TS.ALTER`, `TS._RESTORE`, rename, copy, `with_series_for_indexing`,
`handle_key_restore`); with read-side hiding every path just indexes and visibility is decided in
one place. `reconcile_imports_after_load` only needs to register the job: the main-thread
`GETKEYSINSLOT` plus per-key open loop in `hide_keys_in_slots` disappears. The drain shrinks from
"open N keys under the GIL, index, `optimize_index_fully`" to flipping visibility on the main thread,
which also addresses ASM-11 and makes visibility synchronous with the server's ownership change
(better than today's async drain). Abort needs no module work: the server deletes the keys through
`unlink2` before firing the event (fact 4). Ids are already fine — `index_loaded_series` assigns a
fresh local id at restore time, which also removes the foreign-id `TS.ALTER` warning noted in the
third pass.

### Recommended shape: a hidden-id mask, not a per-key slot check

- Add `hidden_ids: Bitmap64` to `Postings` beside `stale_ids`, and mask it in `Terms` exactly as
  stale ids are masked. This covers every reader at once, counts and label dictionaries included,
  with no CRC16 per resolved key under the postings lock.
- Populate it in the single choke point `Postings::index_timeseries` when
  `is_key_in_slot_import(key)` holds. Replace `ImportJob::pending` (keys per db) with a per-db
  bitmap of the ids the job hides.
- On `ImportCompleted`, subtract the job's bitmaps from the mask on the main thread.
- For snapshot-restored imports (ASM-13), build the job's bitmap with one in-memory scan of
  `id_to_key` for keys hashing into the slots. No server calls, no key opens.
- Keep the atomic fast path: when no import is in flight the only cost is one `IMPORT_ACTIVE` load
  per indexed series.

### To verify before committing to it

- Whether `get_label_names` / `get_label_values` drop a term whose posting bitmap is empty after
  masking; otherwise a label only an importing series carries would still be listed.
- Whether `posting_by_labels` / `series_id_by_labels`, used by write paths for duplicate-label
  lookups, should see hidden series.
- Memory during an import is roughly neutral (hidden series occupy index memory instead of the
  queued-key list), but confirm on a large slot.

If the literal minimal version is wanted instead, it needs a tri-state lookup, the `IMPORTS`
registry as the oracle, and an explicit decision to accept the index-only reads above leaking
during a migration. Given the `TS.CARD` reproduction, that path is not recommended.

### Implementation notes (2026-11-06, `refactor/asm-read-side-hiding`)

The checks listed under "To verify before committing to it" were settled from the code:

- **Label dictionaries did leak.** `get_label_names` and `get_label_values` tested the raw bitmap
  for emptiness, so they now ask whether any id is visible.
- **Duplicate-label lookups treat a hidden series as absent.** This matches the previous deferral:
  a user cannot get `DUPLICATE_SERIES` over a series no read shows, and a replayed create the source
  already accepted is never rejected because of one.
- **Memory was not measured.** Hidden series occupy the index for the length of the import; the
  queued-key list they replace is gone. Confirm on a large slot.

Two statements above turned out wrong or incomplete:

- **"Mask it in `Terms` … covers every reader" was too strong.** `label_querier`'s name and value
  scans and `BatchIterator` walk `label_index` directly, and never masked stale ids either. They
  now use `Postings::visible_cardinality`, which also stops them counting tombstoned ids.
- **`SWAPDB` needs no handling.** The server rejects it in cluster mode.

Differences from the recommended shape:

- **Per-slot bitmaps inside `Postings`, not per-db bitmaps on `ImportJob`.** `Postings` does not know
  its db, a `FLUSHDB` then drops the bitmaps with the index, and a slot belongs to at most one import
  at a time. `ImportJob` keeps only its name and slots.
- **`bulk_index` is a second choke point** beside `index_timeseries`: the bulk load build does not go
  through it.
- **Hidden ids are persisted as ordinary ones.** Their keys are in the snapshot too, so the load
  digest agrees and the repair sweep that ASM-13 noted no longer fires. Load end hides them again,
  for known imports as well, because a preloaded index replaces the postings that were hiding them.
- **The post-import `optimize_index_fully` stays,** as the `asm_optimize` task: the periodic
  optimizer would take hours to reach a large index. ASM-11 is addressed by coalescing: one worker
  at a time, with imports that complete during a run folded into one more pass per db. Recording
  touched label keys was evaluated instead and rejected: on a 1M-series fleet it saves at most about
  6 ms per import, loses past roughly 10 % of slots, and a `blart` `TreeSet` was the largest
  candidate at 105-132 B/key, because its leaves hold the full key.

Bug found in review and fixed: `count()` excludes hidden series, and `TimeSeriesIndex::is_empty`
was `count() == 0`, so `trim_unused_dbs` dropped a db's index when it held only an import in progress,
and completion then published nothing. `is_empty` now checks the directory. The other `count()` callers
want the visible figure.

Abort publishes leftovers rather than retiring them: `finishSlotMigrationJob` clears the importing
state before firing the event, so any surviving key is already exposed by the server, as on a replica
whose full sync then fails its compatibility check. Completion and abort need only the job name, so a
bad slot range can no longer drop them. Load end also forgets registered imports the server no longer
lists, unless a `CLUSTER GETSLOTMIGRATIONS` entry could not be read.

Consequences to keep in mind:

- **Publishing is now synchronous with `IMPORT_COMPLETED`.** The source still indexes the slot until
  it learns of the ownership change, so a fan-out in that window can count the series on both nodes.
  The window existed before, but the drain's thread start used to delay the target's side of it.
  Not measured.
- **Breaking metric rename, pre-release:** the `asm_drain_*` task and `asm_drain_keys_indexed_total`
  become `asm_optimize_*` and `asm_keys_published_total`.
