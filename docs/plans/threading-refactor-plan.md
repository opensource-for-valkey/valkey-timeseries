# Threading model: clarity, reuse and deadlock avoidance (plan)

**Status:** Implemented 2026-09-28 on `promql` (uncommitted at the time of writing), with
both open decisions (§6) taken as recommended. Evaluated on `639efe8b6` ("Refactor thread
pooling to replace global pool with ModulePool"). No live deadlock was found. The problems
were that the safety rules existed only as prose, an unsized pool was left behind by that
commit, and several helpers duplicated each other.

Deviations from the text below:

- **Runtime checks run in release builds too.** `build.sh` runs the integration suite
  against a release build, so a `debug_assert!` would never see a live server. Each check
  is one thread-local read next to a far costlier lock or wait. A debug build panics; a
  release build logs `threading rule broken` once per rule, with a backtrace.
- **A GIL-held flag was added.** `LockGil::lock_gil` returns a `GilGuard` that marks the
  thread as holding the GIL. That lets the checks catch re-entry (a nested lock
  self-deadlocks: the module GIL is a plain mutex) as well as R2 and R3.
- **`std::thread::spawn` is not in `clippy.toml`.** Plain threads cannot cause these
  deadlocks, and many tests spawn them.
- **Executor reservations.** Phase 3 came before the OUTLIERS move, and `BoundedExecutor`
  gained `try_reserve()`. Both PromQL and OUTLIERS used to block the client first and then
  submit; on rejection the job, and the blocked client with it, was dropped, and nothing
  tested whether the error reply still reached the client. Now the client is blocked only
  once its queue slot is reserved.
- **TS.OUTLIERS runs inline for deny-blocking clients** (MULTI, scripts, `RM_Call` without
  K). It used to block them, which the server rejects or asserts on. This fix was not in
  the plan.
- **The selector processor's spawn failure is no longer a panic**
  (`SelectorBatchExecutor::sender` is an `Option`).
- **Phase 5.1 moved into Phase 2**, since `optimize_indices` needed a single-flight guard.
- **Phase 5.2 was narrowed** to deleting the dead `ClientThreadSafeContext::lock`, the one
  GIL path that bypassed the checks. The two blocked-client types turned out to sit on two
  separate reply-writer stacks (`common::replies::ReplyContext` and
  `common::context::ClientReplyContext`); merging those is reply formatting, not threading.
- **Not done:**
  - the TS.JOIN / TS.LABELSTATS A/B (both now wait on a pool of N threads, not C);
  - the thread-count comparison;
  - an ASAN pass;
  - an integration test for a busy OUTLIERS lane (the executor unit tests cover
    rejection);
  - `INFO` fields for the fanout and analysis lanes (their stats are available through
    `BoundedExecutor::stats`).

## 1. Inventory

N = `ts-num-threads`, C = CPU count. "GIL" means the module lock (`MODULE_CONTEXT.lock()`,
`FanoutContext::lock`, `ThreadSafeContextLock`).

| # | Execution context | Defined in | Size | Names | Blocks? | Takes the GIL? | Waited on under the GIL by |
|---|---|---|---|---|---|---|---|
| 1 | orx shared pool (`persistent-pool-rayon`) | `common/threads/mod.rs` (`init_thread_pool`) | N, capped at C | orx default | no | no | main-thread commands, fanout lanes, trim cron |
| 2 | rayon global pool | implicit (`rayon_core::spawn`/`join`) | **C, lazily built, unsized since `639efe8b6`** | unnamed | `optimize_indices` takes the postings write lock | no | TS.JOIN (main thread), LABELSTATS shard handler |
| 3 | `EVAL_POOL` (pinned) | `promql/engine/query_workers.rs` | N | `ts-promql-eval-*` | yes: waits on the selector executor | no | nobody (the reason it is safe) |
| 4 | `MATERIALIZE_POOL` (pinned) | `promql/engine/selector_batch_executor.rs` | N | `ts-promql-io-*` | no | no | the selector processor |
| 5 | PromQL query workers | `promql/engine/query_workers.rs` | `ts-promql-max-concurrent-queries` | `ts-promql-query-*` | yes | reply only | — |
| 6 | Selector processor | `promql/engine/selector_batch_executor.rs` | 1 | `ts-promql-selector` | yes | yes | — |
| 7 | Fanout lanes ×2 (`BoundedExecutor`) | `fanout/workers.rs` | 2 × clamp(N, 2, 8) | `ts-fanout-local-*`, `ts-fanout-request-*` | yes | yes | — |
| 8 | `spawn_background` one-shots | trim, stale-ids, index sweep, ASM delayed indexing, ASM cleanup | 1 each | `ts-series-trim`, … | yes | yes | — |

What already works and must be kept:

- Dedicated plain threads for anything that blocks while holding the GIL (rows 5–8).
- Pinned private pools for PromQL, so parked evaluation jobs can never starve a GIL holder
  (`parked_evaluations_never_starve_a_gil_holder_on_the_shared_pool` pins this).
- `RangeSnapshot` copy under the lock and decode after it.
- Panic isolation in every long-lived worker loop.

## 2. Findings

### 2.1 rayon's global pool survived `639efe8b6`, unsized and unowned

That commit stopped calling `build_global`, so rayon's global pool is now built lazily at
C threads and ignores `ts-num-threads`. It still serves four call sites through
`threads::spawn`/`threads::join`:

| Call site | Primitive | Caller context |
|---|---|---|
| `src/commands/ts_outliers.rs:170` | `spawn` | main thread; the job replies through a thread-safe context without locking |
| `src/series/tasks/optimize_indices.rs:31` | `spawn` | cron (main thread); the job takes the postings **write** lock (`timeseries_index.rs:643`) |
| `src/join/join_handler.rs:22` | `join` | main thread, **GIL held** |
| `src/commands/ts_labelstats_fanout_command.rs:87` | `join` | fanout executor thread, **under `ctx.lock()`** |

(`src/promql/exec/evaluator.rs:1623` also calls `join`, but it runs on `EVAL_POOL` and so
resolves to that pool.)

The two `join` sites wait on this pool while holding the GIL. That is safe today only
because none of the jobs that happen to be queued there take the GIL. Nothing enforces
it: a future `spawn` of a GIL-taking job would reintroduce the freeze described in
`spawn_background`'s docs. `optimize_indices` already breaks the broader rule that a pool
job must not block on a lock.

Checked and not a deadlock today: LABELSTATS computes `matching_postings` into an owned
bitmap and each `join` arm takes its own postings read lock. No guard is held across the
wait, so a queued optimize writer cannot wedge it.

### 2.2 TS.OUTLIERS has no back-pressure

`rayon_core::spawn` queues without a limit, and each job holds a blocked client. PromQL
(`ts-promql-max-queued-queries`) and fanout (`QUEUE_CAPACITY`) both reject work when
their queue is full.

### 2.3 The deadlock rules are prose, spread over seven doc comments

The rules live on `spawn_background`, `BoundedExecutor`, `pin_to_own_pool`/`ModulePool`,
`EVAL_POOL`, `SelectorBatchExecutor`, `wait_for_result` and `TimeSeries::get_range`.
Nothing checks them at runtime or in lint. The rule "enter parallel iteration through
`*_rayon`, never a bare `.par()`" is stated only on `ModulePool`. The earlier rayon
pool-parking freeze and the fanout-timer double free both came from breaking this kind
of implicit rule.

### 2.4 Duplication

- `QueryWorkers` (`query_workers.rs:47`) is `BoundedExecutor` (`common/threads/executor.rs`)
  plus queued/running/rejected stats and a queue limit that can change at runtime, with
  0 meaning unbounded. The worker loop, `catch_unwind` and spawn-failure handling are
  written twice.
- `ts_query.rs` and `ts_queryrange.rs` have the same submit body: create the blocked
  client, check the deadline, `run_evaluation`, reply.
- `TrimRunGuard` (`series_trim.rs:29`) and `StaleIdCleanupGuard` (`stale_ids.rs:65`) are
  the same single-flight guard over a static `AtomicBool`.
- Two `BlockedClient` types (`common/replies/thread_safe_reply_context.rs`,
  `common/context/blocked.rs`) and two thread-safe reply contexts
  (`ThreadSafeReplyContext`, `ClientThreadSafeContext`). OUTLIERS uses one pair and
  PromQL the other.
- Three `catch_unwind` worker loops: the executor, the query workers and the selector
  processor.

### 2.5 Dead code

- In `common/threads/mod.rs`: `spawn_with_context`, `spawn_scoped`, `join_scoped` (which
  carries a `// does this make sense?` comment), `run_on_main_thread` and
  `run_on_main_thread_with_context` with their callback wrappers. None has a caller
  outside the module, apart from its own doctest.
- `BoundedExecutor::try_spawn_in_context` has no callers.
- `ClientThreadSafeContext::lock` is `#[allow(dead_code)]`.
- The `rayon` crate in `Cargo.toml` is never imported; only `rayon-core` is used.

## 3. Target model

Three kinds of thread:

- **Main thread**: holds the GIL.
- **Blocking threads**: `BoundedExecutor` lanes, the selector processor and
  `spawn_background` one-shots. They may block and may take the GIL.
- **Compute pools**: shared (orx), materialize and eval.

Five rules:

- **R1.** A compute-pool job never takes the GIL and never blocks on anything except jobs
  of its own pool.
  - The one exception is EVAL, whose jobs wait on the selector executor.
- **R2.** A thread holding the GIL may wait only on a pool that follows R1 with no
  exception: shared or materialize, never EVAL.
- **R3.** Blocking waits (selector `recv`, fanout waits) happen only on blocking threads
  or EVAL workers.
- **R4.** Work nested inside a pinned pool (EVAL, materialize) stays in that pool.
- **R5.** No lock guard (GIL, postings, series) is held across a wait on a pool, except
  the GIL case R2 allows.

## 4. Work items

In order. Each phase builds and passes tests on its own.

### Phase 0: delete dead code (no risk)

1. Remove everything listed in §2.5, and their doctests.
2. Remove the `rayon` dependency once `cargo build --all-targets` confirms nothing
   uses it.

### Phase 1: enforce the rules before moving anything

Doing this first means any mistake in later phases trips an assertion.

1. Replace the `PINNED` bool in `common/threads/orx_pool.rs` with a thread-local
   `ThreadRole { Main, Blocking, SharedPool, EvalPool, MaterializePool }`.
   - Set it in each pool's `start_handler`, in the `BoundedExecutor` worker loop, in
     `spawn_background` and on the selector processor thread.
   - `ModulePool` routes by role.
2. Add a `threads::lock_gil()` wrapper that `debug_assert!`s R1. Route the eight
   `MODULE_CONTEXT.lock()` sites and `FanoutContext::lock` through it.
3. In `wait_for_result` (selector executor), `debug_assert!` that the caller is not a
   shared or materialize worker (R3).
4. Replace `rayon_core::current_thread_index().is_some()` in `TimeSeries::get_range`
   (`time_series.rs:670`) with a role query.
5. Add a `clippy.toml` with `disallowed-methods` for:
   - `DetachedContext::lock`
   - `rayon_core::{spawn, join, scope}`
   - `std::thread::spawn`
   - orx's bare `par`, `par_mut`, `into_par`, `iter_into_par`

   Allow them only inside `common/threads`. Still to check: whether clippy honours
   trait-method paths for orx's blanket traits. If it does not, a grep-based unit test
   gives the same guarantee.

### Phase 2: retire rayon's global pool

1. `threads::join`: route like `ModulePool::scope`. On a pinned worker use the current
   pool (the evaluator's behaviour is unchanged), otherwise use orx's shared pool.
   TS.JOIN and LABELSTATS then wait on a pool that obeys R2 by construction.
2. TS.OUTLIERS: move the background path to a new `BoundedExecutor` lane,
   `ts-analysis`, sized like the fanout lanes. When the queue is full, reply with a busy
   error.
   - This is a new error for TS.OUTLIERS, but the command is not part of the RTS
     surface, so there is no compatibility entry to add.
   - Its internal `*_rayon` work (RCF) then runs on the shared pool from a plain
     thread, which is allowed.
3. `optimize_indices`: move to `spawn_background`, since it takes a write lock (R1), and
   give it a single-flight guard (Phase 5). Today two overlapping runs can take the same
   cursor, and the second one restarts the db.
4. Delete `threads::spawn`.
5. Update the `ts-num-threads` docs in `config.rs` to say it sizes the shared, eval and
   materialize pools.

### Phase 3: consolidate executors

1. Extend `BoundedExecutor`:
   - Queue limit: `Capacity::Fixed(n)` or `Capacity::Dynamic(fn() -> usize)`, where 0
     means unbounded. Implement it with `QueryWorkers`' atomic-reservation scheme on an
     unbounded channel, which covers both cases (today's `sync_channel` can only do
     `Fixed`).
   - Add queued, running and rejected counters.
   - Log caught panics with `panic_message`.
2. Rebuild `QUERY_WORKERS` on it:
   `BoundedExecutor::new("ts-promql-query", max_concurrent_queries(), Capacity::Dynamic(max_queued_queries))`.
   Move its three tests into the executor tests. The fanout lanes get INFO stats for free.
3. Add `submit_evaluation(blocked_client, deadline, eval_fn, reply_fn)` in
   `query_workers.rs`. `ts_query.rs` and `ts_queryrange.rs` collapse to parse, submit,
   done.
4. `SelectorBatchExecutor::new`: keep the dedicated thread, because batching needs a
   single consumer. Replace the `.expect` on spawn, which runs inside a `LazyLock` and so
   poisons every later query, with a sender that is `None` when the thread fails to
   start, as `BoundedExecutor` already does.

### Phase 4: one home for pools

1. Add `common/threads/pools.rs` owning `EVAL_POOL` and `MATERIALIZE_POOL`, both built
   by one `build_pinned_pool(name, role)`. They stay lazy, so a node that never runs
   PromQL never starts them.
2. The PromQL modules import the pools instead of defining them. The deadlock rationale
   moves to the module docs, and each per-site comment shrinks to one line naming the
   rule it relies on.

### Phase 5: small unifications

1. Add a `SingleFlight` guard and `spawn_background_single(name, &FLAG, job)`, used by
   trim, stale-ids and optimize.
2. Merge the two blocked-client reply types (§2.4) into one. Drop the GIL `lock()` from
   the result: under R1 a reply context never needs the GIL.

### Phase 6: docs

1. Add a "Threading model" section (the §1 table and R1–R5) to the `common/threads`
   module docs.
2. Add one line to AGENTS.md under "Warnings / gotchas" pointing to it.

## 5. Verification

- For every phase: `cargo test --features enable-system-alloc`, including
  `parked_evaluations_never_starve_a_gil_holder_on_the_shared_pool`.
- New tests:
  - the role assertions fire in a debug build (Phase 1);
  - TS.JOIN and LABELSTATS finish while `EVAL_POOL` is full of parked jobs (Phase 2);
  - TS.OUTLIERS returns busy when its lane is full (Phase 2);
  - the query-worker tests pass against the unified executor (Phase 3).
- Full `./build.sh` in standalone and cluster, plus `ASAN_BUILD=true`.
- Performance: Phase 2 moves TS.JOIN and LABELSTATS onto a pool of N threads instead of
  C. Measure both with interleaved A/B runs; back-to-back single runs on a loaded laptop
  have been misleading before.
- Thread count: compare `ps -M <pid>` before and after. At N = 8, removing the global
  pool saves up to 8 threads and the `ts-analysis` lane adds 2.

## 6. Open decisions

1. **TS.OUTLIERS when its lane is full:** reply with a busy error (recommended; it
   matches PromQL and fanout), or block on the bounded queue.
2. **Enforcement:** use both the clippy `disallowed-methods` gate and the debug
   assertions (recommended), or assertions only, to avoid adding a `clippy.toml`.
