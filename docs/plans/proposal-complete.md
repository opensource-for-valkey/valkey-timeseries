# ValkeyTimeSeries release-readiness  proposal

## Proposal: Production Maturity Program for ValkeyTimeSeries

This proposal defines the engineering, validation, operational, and ecosystem work required to make ValkeyTimeSeries safe to deploy for production workloads. Production maturity will be measured against explicit correctness, compatibility, durability, performance, scalability, observability, security, documentation, and release-management criteria.

The proposal should request approval for:
* The target compatibility profile.
* The supported Valkey versions and deployment modes.
* The production workload and SLO definitions.
* The engineering workstreams and owners.
* The test infrastructure and benchmark environment.
* The release gates and rollout strategy.
* Any features explicitly deferred from the first production release.

### 1. Executive summary

ValkeyTimeSeries is a scalable Valkey module intended to be a drop-in RedisTimeSeries replacement, while also supporting extensions such as active expiration, Prometheus-style selectors, dependent compactions, anomaly detection, joins, and parallel query processing.

* **Target users and workloads.** Operators running RedisTimeSeries 8.10 workloads who want a drop-in replacement on Valkey, and metrics workloads that use the Prometheus-style selector, discovery, and analytics extensions. *Decision needed: name the initial adopters whose workloads Phase 0 will baseline.*
* **Proposed release scope.** The RedisTimeSeries 8.10 command surface (§3.1), with `twa` pending a Phase 1 decision, plus the shipped extensions listed in §3.1, on standalone, replica, and cluster deployments of Valkey 8.0.0 and later. Work on unmerged branches (`feat/forecasting`, PromQL, `comparative-benchmarks`) is out of scope unless merged before Milestone A closes.
* **Expected engineering investment.** *Decision needed: owners and estimates per phase are not yet assigned.* The critical path is the two open P0s (PANIC-1, DOS-1) and the end-to-end benchmark harness that Phase 0 depends on.
* **Decision requested.** Approval of the release base branch (§3), the compatibility profile, the release scope above, and the milestone sequence in §6.

The initial release is:
* A RedisTimeSeries-compatible core with staged Valkey-specific extensions.

### 2. Product contract and scope

| Area | Must define |
|---|---|
| Commands | Supported commands, subcommands, options, arity, return formats |
| Data model | Timestamp unit, value type, ordering, duplicate handling, missing values |
| Query semantics | Range inclusivity, aggregation behavior, grouping, filtering, sorting |
| Retention | When samples expire, whether expiration is active or lazy, boundary behavior |
| Compactions | Aggregators, bucket alignment, retention, rule dependencies, failure behavior |
| Labels | Allowed names and values, cardinality limits, indexing semantics |
| Persistence | RDB, AOF, restart compatibility, upgrade and downgrade policy |
| Replication | Determinism, propagation format, replica reads, failover behavior |
| Clustering | Single-key commands, multi-key commands, cross-slot behavior, resharding |
| Compatibility | RedisTimeSeries version or specification being targeted |
| Extensions | Prometheus selectors, joins, relative timestamps, metadata queries, and other additions |

RedisTimeSeries’ documented baseline includes raw timestamp/value samples, out-of-order insertion, duplicate policies, retention, labels, cross-series queries, aggregations, and compactions. Those areas should become explicit rows in the compatibility matrix rather than being treated as informal expectations.[github]
The scope section should also list non-goals. Proposed for the first release:
* No online format migration from an earlier experimental format, and no loading of RedisTimeSeries RDB payloads: both engines register `TSDB-TYPE`, and a foreign encoding version is refused by design (§3.4).
* No automatic global query coordinator unless one is deliberately designed and tested.
* No partial-success mode for cluster fan-out; a shard error fails the command (§3.5). Revisit after Milestone C.
* None of the commands that exist only on `feat/forecasting` (§3.9).

### 3. Current-state assessment

**Baseline.** The original assessment was made against commit `7d8e0aec`, and a first revision against `db36899e`, which it described as carrying `main`'s tip `6221baf5`. None of those three commits is an ancestor of any current branch, so neither earlier pass can be reproduced. **This revision (2026-10-14) re-verifies every claim against `unstable` @ `b13fdaf10` (2026-10-08).** `main` (tip `0ef86731e`, 2026-09-27) has diverged from it: `unstable` carries 22 commits that `main` lacks, and `main` carries 11 that `unstable` lacks by hash, of which 4 have no patch-equivalent on `unstable` — DUMP/RESTORE persistence of compaction rules (`219c723ca`), stale and duplicate compaction-rule handling (`0ef86731e`), batch index optimization (`9dcae48cf`), and a series-id indexing refactor (`2b864e121`). Choosing the release base branch is one of the decisions this proposal requests (§1); until it is made, every statement below describes `unstable`.

Between the earlier passes and this one, fixes closed most of the P0/P1 register in §3.8 — the reproduced whole-server aborts, the `TS.MDEL` replication loss, the defrag and index-deadlock defects, and the memory-accounting and lazyfree gaps — each with a regression test. §3.8 therefore marks every entry **fixed** or **open**. Code is cited by symbol and file rather than line number, because the line references of both earlier passes had drifted. The codebase is ~104k lines of Rust across 281 files, crate version `0.1.0`, with no release tags and no changelog.

Maturity is reported using four labels, applied per capability rather than per file:

| Label | Meaning |
|---|---|
| **Implemented** | Code exists and is covered by tests that would fail if it broke |
| **Partial** | Code exists and works for the common path; known gaps in edge cases, error handling, or scope |
| **Untested** | Code exists and may well be correct, but no test would notice if it were not |
| **Not supported** | Deliberately absent; an error or a documented non-goal |

The distinction between *Implemented* and *Untested* is the one this section exists to enforce. Several subsystems below are well-written and entirely unverified; the clearest demonstration is now historical — MDEL-1 hid a replication-loss defect behind a clean, replica-less suite until it was fixed and given a replica-bearing regression test (§3.8).

Two findings should be read before the rest of the section, because they bear on sequencing. First, the original assessment reproduced **five single-command whole-server aborts** against a running server, one of them reachable by a client holding a single command permission; all of the individual crashers are now fixed on `main` (§3.8), but the structural defect behind them — the absence of any FFI panic barrier (PANIC-1) — is **still open**, which keeps panic-safety work ahead of Milestone B rather than behind it. Second, §3.7 establishes that **no end-to-end latency harness exists on the base branch**: `tools/fleet_loader` drives a live server but reports only ingest rate, and the socket-driven `tools/server_bench` harness lives on the unmerged `comparative-benchmarks` branch. The performance and memory baselines Phase 0 depends on therefore cannot be produced from the base branch, which makes adopting that harness a prerequisite of Milestone A rather than an output of it.

#### 3.1 Current command coverage — **Implemented**

32 commands are registered: 30 user-facing via `#[valkey_module_macros::command]` annotations, plus `ts._debug` and `ts._restore` in the positional `commands:` table of `valkey_module!` in `src/lib.rs`. ACL categories are re-applied at load by `assign_command_acl_categories`, because the command-info path does not set them. Each handler declares its categories with an `acl_categories!` entry beside its annotation, the `every_annotated_command_declares_acl_categories` test pins the two registries to the same count, and an unknown command or category aborts the module load, so drift cannot ship silently.

| Group | Count | Commands |
|---|---:|---|
| RedisTimeSeries 8.10 surface | 21 | CREATE, ALTER, ADD, MADD, INCRBY, DECRBY, DEL, CREATERULE, DELETERULE, GET, MGET, RANGE, REVRANGE, MRANGE, MREVRANGE, NRANGE, NREVRANGE, READ, INFO, QUERYINDEX, QUERYLABELS |
| Extensions | 9 | ADDBULK, MDEL, CARD, LABELNAMES, LABELVALUES, METRICNAMES, LABELSTATS, JOIN, OUTLIERS |
| Internal / admin | 2 | `TS._DEBUG` (gated by `debug-mode`, default off), `TS._RESTORE` (AOF and slot-migration replay) |

Every command RedisTimeSeries 8.10 documents is implemented; none are missing. The single functional gap on the shared surface is the **`twa` (time-weighted average) aggregator**, which is *Not supported* — `AggregationType` has 23 variants and no `Twa` variant. It is rejected wherever RedisTimeSeries would accept it: `TS.CREATERULE ... AGGREGATION twa`, `AGGREGATION twa` in range queries, and `twa:...` compaction-policy entries.

Compatibility is targeted at RedisTimeSeries 8.10, pinned by image digest (`docker-compose.compat.yml`). `ts-compatibility-mode` (`strict` | `extended`, default `extended`) is narrower than its name suggests: it gates one behavior — repeated-option precedence (DIV-0014). Last-sample reporting on back-filled compaction destinations (DIV-0023) was the second gated behavior until 2026-10-14, when its emulation was dropped; it is now a permanent, ungated divergence. All other registered divergences are unconditional.

#### 3.2 Current data structures and indexes — **Partial**

A `TimeSeries` holds a flat `Vec<TimeSeriesChunk>` kept sorted and non-overlapping by first timestamp. Samples are 16-byte `(i64, f64)` pairs.

The label index is one `Postings` struct per database, combining:
* an adaptive radix tree mapping `"name=value"` keys to posting lists, which serves prefix scans and ordered ranges — there is no separate name→values or metric-name index; both are derived by prefix scan;
* a roaring bitmap of series ids per key;
* a btree mapping ids to keys that is the authoritative membership set — not the postings;
* a stale-id bitmap supporting lazy self-healing.

Databases are held in a **papaya** concurrent map (`TIMESERIES_INDEX` in `src/series/index/mod.rs`); series ids are minted process-globally as `[epoch:24 | counter:40]` and are not persisted. Cardinality is exact everywhere (roaring intersections), with no sketching. Query planning is modeled on Prometheus `PostingsForMatchers` (`postings_for_selectors` in `postings/planner.rs`), ordering by a static structural cost model and re-sorting by true bitmap cardinality before the intersection fold.

Two caveats matter for production:
* **Concurrency is still a per-db `std::sync::RwLock`, not the lock-free model the README's "concurrent lock-free reads" implies**, but the two worst lock-hold defects are gone: postings read locks are released before keys are opened or stale ids repaired, and `fetch_series_batch` was refactored to minimize locking (INDEX-1, fixed — see §3.8). The lock order is now stated: rule R5 in `src/common/threads/mod.rs` fixes it as GIL, then postings, and forbids holding the postings lock across a pool wait. R5 is enforced by review only, however, unlike R1–R3, which are checked at runtime, and there is still no lock-order test to keep it from regressing. The README's "concurrent lock-free reads" claim should be corrected to match.

Labels are stored per-series as refcounted `InternedString`s — one allocation per unique `"name=value"` pair across the keyspace, not one per series. The index term dictionary still keeps its own raw `Box<[u8]>` copy per term (`IndexKey` in `src/series/index/index_key.rs`), so each pair still appears twice in memory (once in the interner, once in the index), but no longer once per series. The interner is reference-counted and deallocates unused entries, and `INFO ts_memory` reports its live size.

#### 3.3 Compression formats and chunk behavior — **Implemented** (encoders) / **Partial** (chunk management)

Three encodings, dispatched through an `enum_dispatch` trait:

| Encoding | Disc. | Random access | Mutation cost | Notes |
|---|---:|---|---|---|
| Uncompressed | 1 | Yes (binary search, zero-copy slice) | O(n) memmove | 16 B/sample exactly |
| Gorilla | 2 | No (decode from bit 0) | Full re-encode | Fastest encode; **timestamps must be non-decreasing** |
| **Chimp (default)** | 3 | No | Full re-encode | ELF erasure over Chimp XOR; fastest decode; tolerates out-of-order |

Default chunk size is 4096 bytes, range [48 B, 1 MiB], multiple of 8. Note `ts-encoding compressed` resolves to **Chimp**, and the config's own description still reads "GORILLA or UNCOMPRESSED" — stale. Cluster wire encoding is a separate policy: uncompressed below 16 samples, Chimp at or above (`samples_to_chunk` and `WIRE_COMPRESSION_MIN_SAMPLES` in `chunks/serialization.rs`).

Measured ratios at 4 KiB Chimp: constant 0.50 B/sample (31.9x), discrete 2.17 (7.4x), counter 2.37 (6.8x), periodic 7.12 (2.25x), drift/irregular 8.17 (1.96x).

The chunk *manager* is weaker than the codecs, though its worst defect is fixed. `ChunkOps::size()` now reports bytes written for every encoding (`data_size()` on Gorilla and Chimp), so `utilization()` is a fill ratio against `max_size` rather than the memory ratio that read ≈2.0 for a full Chimp chunk. Two semantics remain unspecified: a merge can push a compressed chunk past `max_size` (`remaining_capacity()` saturates for that reason), and `estimate_remaining_sample_capacity()` relies on an amortized bytes-per-sample ratio that its own documentation calls unreliable for small chunks. Those figures drive `should_split()` and the per-chunk numbers in `TS.INFO ... DEBUG`.

#### 3.4 Persistence implementation — **Partial**

The module registers a native type `"TSDB-TYPE"` at **encoding version 1** (`src/series/series_data_type.rs`) with `rdb_save`/`rdb_load`, `aof_rewrite`, `free`, `unlink`, `copy`, `defrag`, `digest`, `mem_usage`, `free_effort`, and `aux_load`/`aux_save2`.

* **The compatibility window is a mechanism without a policy.** The loaders accept any encver in `MIN_TIMESERIES_TYPE_ENCODING_VERSION..=TIMESERIES_TYPE_ENCODING_VERSION`, and both bounds are currently 1. A narrow range is deliberate, because the module claims the same type name as RedisTimeSeries (encver 9) and must not misparse a foreign payload. What is missing is the policy: nothing states how far the minimum may trail the current version, the series body is positional and untagged with no per-field versioning, and no fixture exercises a version bump. There is no upgrade procedure, no downgrade policy, and no rolling-upgrade coexistence rule.
* **AOF rewrite emits a command, not a raw dump**: `TS._RESTORE key <payload>`, because the callback can run in a forked child with the module GIL inherited locked.
* **The label index is persisted separately and is properly versioned** — magic `TSIX` + version byte, with an independently versioned per-database body, written via `aux_save2` with `AUX_BEFORE_RDB`. It is fork-safe (`try_read`, writing nothing under contention) and **fails soft**: any parse failure discards the payload and rebuilds per key.
* Untrusted collection lengths are capped at 16 M entries (`MAX_RDB_COLLECTION_LEN` in `common/rdb.rs`).
* Protobuf (`prost`) is used **only** for the cluster fanout wire, never for persistence or replication.

The `DUMP`/`RESTORE` suite is now collected. The file was renamed to `tests/test_dump_restore.py` (commit `6e9246153`, 2026-10-06), so its 11 round-trip tests match `pytest.ini`'s `test_*.py` pattern and run in CI's integration job. All 11 pass against `unstable` @ `b13fdaf10` on Valkey `unstable` (run 2026-10-14). Malformed and truncated payload paths have their own end-to-end suites (`test_ts_restore_malformed.py`, `test_rdb_corrupt_load.py`).

#### 3.5 Replication and cluster behavior — **Partial** (standalone) / **Partially tested** (replica paths)

Replication is effected at 13 call sites. Non-deterministic input is handled unevenly:

| Path | Behavior | Status |
|---|---|---|
| `TS.ADD` / `TS.MADD` with `*` | Timestamp resolved and rewritten before propagation | Implemented |
| `TS.INCRBY` / `TS.DECRBY` without `TIMESTAMP` | Resolved timestamp and auto-create options rewritten before propagation | Implemented, replica-tested |
| `TS.ADDBULK` | Replicated verbatim; the replica replays the payload against its own copy of the series | Implemented, replica-tested |
| Compaction downstream writes | Recomputed on every node from the source write, as in the reference; rule state persisted with the source | Implemented, replica-tested (equivalence suite, Hypothesis oracle, reload, promotion); failed applications heal via `dirty` |
| Policy-created destinations / auto-create defaults | Replicated as explicit `TS.CREATE`/`TS.CREATERULE` with the primary's resolved options | Implemented, replica-tested |
| Retention / active trim | Not replicated and emits no notification; runs on every node every 10 s by design. The retention floor follows each series' last sample, not the clock, so every node removes only samples that reads already hide — stored layout may differ between runs, visible data cannot | Implemented, replica-tested |

`tests/test_ts_replication.py` now runs against a live replica (`REPLICAS_COUNT = 1`) and its additions cover `TS.MDEL` series- and range-deletion replication (the resolved effects reach the replica, MDEL-1) and `TS.INCRBY`/`TS.DECRBY` without `TIMESTAMP`: the propagated command carries the resolved timestamp and auto-create options (checked with `MONITOR` on the replica), and the replica's samples, retained window, and compaction destination samples match the primary's. `TS.ADDBULK` is now covered too: auto-creation options, unsorted payloads, duplicate-policy rejection and `ON_DUPLICATE SUM` (applied once on replay), compaction including recalculation of a closed bucket by an out-of-order batch, and the write-path retention trim all leave identical data on the replica. The active trim cron is covered as well: `test_replication_retention_trim_cron` leaves an expired prefix in a compressed head chunk, waits for each node's cron to drop it, and checks that `TS.RANGE`, `TS.INFO`, and `DEBUG DIGEST-VALUE` agree before and after; it fails if the cron is restricted to primaries without a replicated trim. Compaction recompute is covered by an equivalence suite in the same file that compares destination samples, `TS.GET … LATEST` and the open-bucket state (`TS.INFO key DEBUG` → `rulesState`) between a live primary and replica: `test_replication_compaction_*` (the `TS.ADD`, `TS.MADD`, `TS.DEL`, chain-and-diamond, rule-on-existing-data, `DELETERULE`-then-recreate, direct-destination-write and source-retention paths, full resync mid-bucket), `test_replication_compaction_reload_mid_sequence` and `test_replication_compaction_promotion`, and a Hypothesis oracle, `test_replication_compaction_random_sequences`, over random sequences of writes, deletes and rule changes. Policy-created destinations, their rule order and the auto-create and explicit-create defaults are pinned to the primary's configuration by `test_replication_policy_destinations_come_from_the_primary`, `test_replication_autocreate_*` and `test_replication_explicit_create_pins_primary_defaults` (AOF replay: `test_policy_destinations_survive_aof_replay`), and rule-state persistence by `test_rule_state_survives_reload` for all 23 aggregators. A rule whose application fails is marked `dirty` and rebuilds its open bucket on the next write (`test_compaction_failure_recovery.py`). The remaining untested propagation path is the `*` timestamp (exercised only incidentally by the `TS.DECRBY` test).

Clustering is a genuine scatter/gather over the Valkey cluster message bus, with 10 registered fanout operations (two of them back `TS._DEBUG`), a protobuf wire contract, and several well-built mechanisms: a hardened message envelope (range-checked db, pre-allocation caps, forward-compatible feature-bit gate), a cluster-map fingerprint handshake that aborts on topology skew, ACL identity propagation that **fails closed**, and real aggregation push-down for `TS.MRANGE` with per-shard capability echoes so mixed-version clusters degrade gracefully. Cross-slot behavior splits cleanly by command class: explicit-key commands declare key specs and get the engine's standard `CROSSSLOT` handling, while filter-based commands declare `key_spec: []` and scatter/gather instead.

Against that: there is **no partial-success mode** (any shard error or the 5 s timeout fails the whole command), per-shard error causes are replaced with a generic string and reach only the log, and a coordinator timeout stops the wait but not the shard's work, which keeps running under the GIL (DoS audit N2). Local fan-out work and peer requests also share one executor lane, so either can starve the other (N3). Atomic slot migration is implemented for Valkey ≥ 9.0.0 with import-delay and post-export cleanup; its option is now declared through a named module constant, `HANDLE_ATOMIC_SLOT_MIGRATION = 1 << 5` (`src/common/module_options.rs`, used from `src/series/index/asm.rs`), because the generated bindings still predate the server constant. `TS.MDEL` is no longer an exception to write-targeting (MDEL-1 — fixed; see §3.8).

#### 3.6 Background workers and expiration — **Partial**

One driver: the server `hz` cron (`__cron_event_handler` in `src/series/background_tasks.rs`), running four tasks — trim series (10 s), remove stale ids (20 s), optimize indices (60 s), trim unused dbs (300 s, inline on the main thread). Retention is applied **both** eagerly on the write path and actively by the trim cron.

* Task intervals are computed **once at init** from `INFO hz`; a later `CONFIG SET hz` is not observed and silently rescales every cadence. Unchanged.
* The three incremental tasks — trim, stale-id cleanup, and index optimize — are now each serialized by their own `Mutex` and run as cursored, size-bounded batches (`src/series/tasks/{series_trim,stale_ids,optimize_indices}.rs`), so a task no longer overlaps with itself under load and the batch cursor survives across cron ticks. There is still no queue and no backpressure — saturation simply means "more work next tick" — and there is no drain on shutdown.
* `deinitialize()` in `src/lib.rs` tears down nothing — no pool shutdown, no joins, no drain. Shutdown is a cooperative flag (`IS_SHUTTING_DOWN`) that long-running loops poll.
* **`ts-num-threads` now bounds query parallelism.** Since the threading consolidation, orx-parallel runs on a persistent rayon-core pool (the `persistent-pool-rayon` feature) that `init_thread_pool` sizes from `ts-num-threads`, capped at the available cores, and `threads::join` and the `*_rayon` adapters use the same pool. The thread kinds and the rules R1–R5 that keep them from deadlocking are documented in `src/common/threads/mod.rs`; R1–R3 are checked at runtime in release builds, and `clippy.toml` forbids the raw pool and GIL entry points. Two caveats remain: the setting also scales the `BoundedExecutor` lanes, and rayon's global pool, which `krcf` enters for `TS.OUTLIERS METHOD rcf`, is sized by rayon rather than by this setting.

Compaction is **synchronous on the write path**; there is no compaction worker. `topo_sort` is used only to reject cycles at rule-creation time, not to order execution. Downstream write failure is now handled consistently. Since `b13fdaf10`, `TS.ADD`, `TS.INCRBY`, and `TS.MADD` match `TS.ADDBULK`: the failure is logged, the command reports success, and the stored source sample is replicated so the replica re-runs the compaction on replay. Previously `TS.ADD` returned an error after committing the sample and skipped replication, so a client retrying under a `SUM` policy double-counted. A failed rule application is also counted (`INFO ts_compaction` → `ts_compaction_failures`) and marks the rule `dirty`, so the next write rebuilds its open bucket from the source (`ts_compaction_state_rebuilds`); a closed bucket lost to the failure is not repaired, which is why the counter exists.

#### 3.7 Existing unit, integration, fuzz, and benchmark coverage

Volume is high; distribution is uneven, and the gates are weak.

| Layer | Artifacts | Cases | In CI? |
|---|---|---:|---|
| Rust unit tests | ~150 `#[cfg(test)]` files | ≈1,535 `#[test]` fns | Yes |
| Property tests (proptest) | 3 files (codec, fanout codec, fanout message) | 18 | Yes |
| Rust doc tests | — | — | Yes |
| Python integration (single-node) | 66 files | ≈1,014 | Yes |
| Python integration (cluster) | 16 files | ≈176 | Yes |
| RTS differential compat suite | 23 files | 624 | RESP3 smoke only, **non-blocking** |
| Differential fuzzer (Hypothesis) | 1 target | — | **No** |
| Criterion benchmarks | 3 targets | 558 bench ids | **No** |
| End-to-end ingest load (`tools/fleet_loader`) | 1 tool | ingest rate only | **No** |
| End-to-end latency / concurrent throughput | **none on the base branch** (`tools/server_bench` is on `comparative-benchmarks`) | **0** | — |

Regression suites added since the original assessment (all in CI): corrupt and truncated RDB loads (`test_rdb_corrupt_load.py`), malformed `TS._RESTORE` payloads (`test_ts_restore_malformed.py`), cluster `TS.MDEL` replication against a live replica (`test_ts_mdel_replication_cme.py`), lazyfree (`test_ts_lazyfree.py`), memory reporting (`test_ts_memory_reporting.py`), flush-index consistency (`test_ts_flush_index.py`), index-lock reentrancy (`test_ts_index_lock_reentrancy.py`), and binary-safe key names (`test_binary_key_names.py`), plus Rust unit coverage for defrag, sealed-chunk capacity, and `free_effort` (in `defrag.rs`, `time_series_tests.rs`).

Strengths: the RedisTimeSeries differential harness is real and disciplined — every command is sent to both engines and normalized, "reference errors but subject succeeds" is a non-registrable hard failure, and 54 intentional divergences are registered with 24 fuzzer-shrunk reproducers promoted into a replayable corpus (up from 51 and 18 respectively at the original assessment). The property tests over the fanout codec are decode-*totality* tests running across an `extern "C"` boundary, and `proptest-regressions/` records three shrinks that all bottom out at a protobuf `_UNSPECIFIED` discriminant which "used to panic on every `TS.MRANGE` response decode" — i.e. a fixed, remotely-triggerable node abort.

Gaps that bear on release readiness:
* **No end-to-end latency benchmark on the base branch.** `benches/` and the report tools still time in-process methods. One live-server tool now exists: `tools/fleet_loader` (behind the `fleet-loader` feature, using the `redis` crate's cluster client) fills a standalone server or cluster with a realistic fleet and reports ingest rate. It does not measure latency distributions, concurrent read throughput, memory under sustained ingest, replication lag, or fan-out cost. A socket-driven harness, `tools/server_bench`, exists on the unmerged `comparative-benchmarks` branch. **Phase 0's latency and memory baselines cannot be produced from the base branch until that harness is merged or adopted.**
* **The base cluster harness runs `REPLICAS_COUNT = 0`**, and most of the CME suite still does — but four suites now set `REPLICAS_COUNT = 1` (`test_ts_read.py`, `test_ts_asm.py`, `test_ts_debug_cme.py`, and `test_ts_mdel_replication_cme.py`), so replica-dependent behaviour is exercised for those commands only.
* Only 7 of the 44 `src/commands/*.rs` files carry unit tests; every other handler is validated solely through a full server round-trip.
* `compat-smoke` is `continue-on-error: true`, and flipping it to blocking is itself named as an open exit criterion in CI. Unchanged.
* Absent entirely: ThreadSanitizer, Miri, coverage measurement, `cargo deny`, dependency pinning for the cloned test framework, aarch64 Linux, and any scheduled or nightly job. `cargo audit` now runs as a blocking CI job. The ASAN job sanitizes the Valkey C server build, not the Rust module.
* Untested at the unit level still: `common/rdb.rs`, `common/pool.rs`, `fanout/cluster_rpc.rs`, and all of `common/replies/` (`threads/batch_worker.rs` no longer exists after the threading consolidation). `defrag.rs` now has unit tests, and the RDB loaders gained end-to-end corrupt-input tests, so they leave this list.

#### 3.8 Known correctness bugs and operational risks — register with status

The entries below were identified by direct code audit, and the CRASH-1 family was **reproduced live** against `valkey-server unstable` with the module loaded. Between the original assessment and this revision, fixes landed on `main` that close all but two of them; the **Status** column records the current state, naming the change and the regression coverage where fixed. This list seeds the register in §5; it is not exhaustive.

| ID | Severity | Subsystem | Finding | Status (this revision) |
|---|---|---|---|---|
| PANIC-1 | **P0** | Whole module | **There is no FFI panic barrier on the main thread.** `catch_unwind` now guards blocked-client callbacks (`src/common/block_on_keys.rs`), every `BoundedExecutor` job, and every `spawn_background` thread (`src/common/threads/`), so a panic off the main thread is logged rather than fatal. No command handler is wrapped. Unwinding out of `ValkeyModuleCommandDispatcher` aborts the process. CRASH-1 and CRASH-2 were instances of this class. | **Open, narrowed.** The reproduced instances are fixed and off-thread work is guarded, but command dispatch and the RDB, aux, and defrag callbacks have no barrier, so a panic on those paths is still a whole-server abort. |
| DOS-1 | **P0** | Query | No cap on matched series, materialized samples, label-value scan, query range, or matchers; `RM_Yield` is never called; no query timeout or cancellation. A negative-only filter such as `FILTER host!=x` is now rejected at parse time (F1), but a bounded broad selector such as `__name__=~".+"` still opens every matching series on the main thread, and `TS.QUERYLABELS` without `FILTER` opens every key while skipping unreadable ones, so a narrowly-scoped ACL user pays the full cost (N1). `docs/topics/filter-dos-audit.md`, re-evaluated 2026-10-13: of nine original findings 2 are fixed, 2 narrowed, and 5 open, with four new findings N1–N4. | **Open.** F1 and F4 fixed, F2 and F6 narrowed; no series cap, sample budget, scan budget, yield, or deadline exists. |

Beyond the specific defects, the panic surface is the dominant structural risk. The individual crashers above — parser, `TS._RESTORE`, defrag, deadlocks, Gorilla split/iterator — are closed and regression-tested, but PANIC-1 remains: there is still no FFI barrier, so the ~170 production panic sites (93 `.unwrap()`, 46 `.expect(...)`, 8 `panic!`, 8 `unreachable!`, 14 `assert!` on an AST-aware count at the original assessment; a cruder line-based count reaches ~237) are still candidate whole-server aborts. The untrusted-bytes decoders are not among them. The RDB and fanout decoders (`series/serialization.rs`, `common/rdb.rs`, `chunks/serialization.rs`, `fanout/serialization.rs`, `index/postings/serialization.rs`) contain no `unwrap`, `expect`, `panic!`, or `unreachable!` outside tests. The two `unreachable!` sites earlier assessments attributed to them cannot be reached from input: in the Gorilla varbit decoder (`chunks/stream/varbit.rs`) the bucket reader stops after eight bits and returns 0–8, all of which `varbit_bucket_to_num_bits` maps; and in peer wire decoding (`src/commands/fanout_codec/filters.rs`) the contains and starts-with `unreachable!` arms follow a match that maps each op to exactly one matcher variant and rejects every other combination with an error. Rewriting them as errors is defensive hygiene, not a P0 item.

Diagnostics compound this. `[profile.release]` in `.cargo/config.toml` sets `strip = true`, so module frames in a production crash report resolve only to `RedisModule_OnUnload + <offset>`; reproductions are legible only because a panic message reaches the log before the abort. Release also sets no `overflow-checks`, which made DEFRAG-1 silent rather than loud — the arithmetic was fixed by removal, but overflow-checking remains absent as defense in depth. Both unchanged.

#### 3.9 Current documentation and packaging status — **Partial** (docs) / **Not supported** (packaging)

Documentation is unusually strong for internals and weak for operations. `COMPATIBILITY.md` (53 KB) is a genuine, carefully reasoned compatibility contract with explicit non-goals and a SemVer-based defect-correction policy; `AGENTS.md` (11 KB) documents architecture, conventions, and tooling; `docs/topics/` carries deep dives on encodings, filter syntax, label discovery, migration, and the DoS audit.

Against that:
* **The Extensions section of `COMPATIBILITY.md` advertises ten commands that do not exist** — `TS.FORECAST`, `TS.AUTOFORECAST`, `TS.DECOMPOSE`, `TS.TREND`, `TS.AUTOCORRELATION`, `TS.XCORR`, `TS.STATIONARITY`, `TS.PERIODS`, `TS.FEATURES`, `TS.FILLGAPS`. All are unmerged work on `feat/forecasting`. A compatibility contract that overstates the shipped surface is a defect in the contract.
* **`ts-emulate-release` does not exist.** The defect-correction section of `COMPATIBILITY.md` documents it as *the* mechanism by which compatibility defects get fixed in minor and patch releases without violating SemVer — the contract's entire remediation story. It is not a registered config; the identifier appears only in a doc comment on the compatibility-mode setting in `src/config.rs`. The contract promises a control that ships no implementation.
* Of 32 command pages, `ts.incrby.md` and `ts.decrby.md` are now complete pages, and literal `TODO`s remain only in `ts.card.md` and `ts.labelvalues.md`.
* The configuration reference is incomplete and partly wrong: 17 parameters exist, 14 appear in the only table, which lives inside the `TS._DEBUG` command page (`ts-index-build-max-memory`, `ts-index-persist`, and `debug-mode` are missing from it). `ts-num-threads` is documented as 8 against an actual 4, two other defaults are left blank, and the renamed `ts-chunk-size` persists in two documents.
* **Missing for a production release:** deployment guide, configuration reference, troubleshooting guide, upgrade/downgrade notes, operational runbooks, and a cluster operations guide.

Packaging is essentially absent: no `CHANGELOG`, no release tags, no release workflow, no container image, no example `valkey.conf`, no checksums or signed artifacts, no SBOM, and no multi-architecture builds. Distribution today is "clone and `cargo build --release`". There is also no `CONTRIBUTING`, `SECURITY.md`, or `NOTICE`. Version identity is unreconciled across three places: `Cargo.toml` 0.1.0, `pyproject.toml` 0.1.0, and `VK_TIMESERIES_VERSION = 1` (`src/lib.rs`). MSRV is now consistent at 1.96 in `Cargo.toml` and `README.md`. The minimum server version is Valkey 8.0.0, which **CI never exercises** (its matrix is `unstable` and `8.1`). The other packaging and versioning findings in this paragraph were re-verified unchanged.

One packaging issue is latent only because nothing ships yet. `.cargo/config.toml` applies `-C target-feature=+bmi1,+bmi2,+avx2` unconditionally to every x86-64 build, and there is **no runtime feature detection anywhere** in `src/`. A published binary would `SIGILL` on any pre-Haswell x86-64 — Intel before 2013, AMD before 2015, and any VM configured with a baseline CPU model (`qemu64`, `kvm64`, SandyBridge/IvyBridge, all common for live-migration compatibility) — with no diagnostic beyond "Illegal instruction." This must be resolved before the first artifact is published, not after.

A license review of the 303 crates then locked was performed manually for the original assessment, by registry inspection. The lockfile now holds 330 entries, so the review should be re-run and automated: `cargo audit` now runs in CI, but `cargo-deny` does not and **should be added**. **No copyleft dependency was found** — every crate offers a permissive Apache-2.0-compatible option: 209 MIT-or-Apache, 48 MIT, 17 Apache-2.0, 4 BSD-3-Clause. Two items need action rather than blocking: `valkey-module` is **BSD-3-Clause**, whose binary-redistribution attribution requirement becomes live the moment artifacts ship and is currently unmet because there is no `NOTICE` file; and `enquote` is **Unlicense**, which some corporate policies reject for lacking a patent grant. In-tree attribution for vendored code is correct (`src/join/asof/mod.rs`, `chunks/stream/varbit.rs`), and the clean-room boundary against RedisTimeSeries is explicitly enforced.

#### 3.10 Current client and ecosystem support — **Not supported**

There is no client library, Prometheus adapter, Grafana data source, Telegraf plugin, or exporter in this repository or referenced by it. The Python integration tests drive the module through raw command strings on a generic client. Every item in Phase 7 is greenfield.

#### 3.11 System diagram

```
                         Client (RESP2 / RESP3)
                                   |
                        Valkey command layer
              command info · key specs · ACL categories · cluster routing
                                   |
                     TS.* command handlers  (src/commands/)
              command_parser.rs · *_fanout_command.rs · fanout_codec/
                    |                                    |
        Query / aggregation                    Cluster fanout  (src/fanout/)
   src/parser · src/labels · src/iterators      cluster bus · protobuf envelope
   src/aggregators · src/join · src/analysis    cluster map · ACL identity scope
                    |                                    |
                    +------------------+-----------------+
                                       |
                Series metadata / label index  (src/series/index/)
        blart ART ("name=value") · croaring bitmaps · id_to_key · stale ids
                     per-db RwLock · papaya db map · string interner
                                       |
                 Chunk manager / codecs  (src/series/chunks/)
                  Uncompressed · Gorilla · Chimp (ELF-on-Chimp)
                                       |
        Persistence & lifecycle   (src/series/series_data_type.rs)
   RDB (encver 1) · index aux field (TSIX) · AOF via TS._RESTORE · digest
   replication (13 sites) · slot migration · keyspace events · free/unlink
                                       |
                 Background tasks  (src/series/background_tasks.rs)
        trim 10s · stale ids 20s · optimize indices 60s · unused dbs 300s
```

#### 3.12 Summary of maturity by subsystem

| Subsystem | Status | The gap that matters |
|---|---|---|
| Command surface (RTS 8.10) | Implemented | `twa` aggregator not supported |
| Extensions (JOIN, OUTLIERS, discovery) | Implemented | Documented as available but portability-limiting |
| Chunk encoders | Implemented | Property tests exclude NaN/Inf and out-of-order |
| Chunk management | Partial | GOR-1/MEM-1 capacity defects fixed (seal-chunk reclaim); `size()` now reports bytes written everywhere; over-`max_size` merges and the remaining-capacity estimate still unspecified |
| Label index | Partial | Deadlocks fixed and lock scope narrowed (INDEX-1); memory now reported via `INFO ts_memory`; lock order stated (R5) but enforced by review only |
| RDB persistence | Partial | Encver-range mechanism exists but both bounds are 1, and there is **no upgrade policy**; corrupt/truncated loads now error instead of aborting (CRASH-2/RDB-1 fixed) |
| Index persistence | Implemented | Versioned, fork-safe, degrades gracefully; aux loader now also guards foreign encver |
| AOF | Partial | Implemented; `DUMP`/`RESTORE` suite now collected (`test_dump_restore.py`) |
| Replication | Partial | MDEL-1 fixed; MDEL, INCRBY/DECRBY (which propagate the resolved timestamp), ADDBULK, compaction recompute, policy destinations and auto-create defaults replica-tested; retention-trim cron runs per node (floor follows the last sample; pinned by a test); `*`-timestamp propagation has no dedicated replica test |
| Cluster fanout | Partial | Strong envelope/ACL/push-down; no partial success; errors swallowed; timeout stops the wait, not the shard work |
| Atomic slot migration | Untested in production shape | Implemented for ≥9.0.0; option now a named constant; four CME suites run replicas but resharding/failover under load is not exercised |
| Retention & compaction | Partial | Synchronous; failure semantics consistent; failed applications are counted (`INFO ts_compaction`) and heal through `dirty` rules, but a closed bucket lost to a failure is not repaired |
| Background workers | Partial | Tasks serialized and cursor-batched; still no queue, backpressure, or shutdown drain; `hz` read once |
| Query safety / limits | **Not supported** | Selector-shape fixes F1/F4 landed; no caps, scan budget, yield, timeout, or cancellation; audit N1–N4 open |
| Input validation & panic safety | **Partial** | Reproduced aborts (CRASH-1/2, DEFRAG-1, INDEX-1, GOR-1) fixed with regression tests; PANIC-1 barrier absent on the main thread; RDB and fanout decoders have no reachable panic sites |
| Observability | **Partial** | `INFO ts_memory` added; still no operational metrics, slow-query log, or log level |
| ACL model | Implemented | Stricter than RTS by design; replica gating correct; minimum-privilege crashers (CRASH-1/CRASH-2) now fixed |
| Benchmarking | **Partial** | Microbenchmarks plus an ingest loader; the latency harness is on an unmerged branch |
| Documentation | Partial | Strong internals; no operational docs; `ts-emulate-release` promised but absent |
| Packaging & release | **Not supported** | No artifacts, changelog, or versioning; unconditional AVX2 would SIGILL on older CPUs |
| Client & ecosystem | **Not supported** | Nothing exists; compatibility claims asserted but never client-verified |

The assessment separates “implemented,” “partially implemented,” “untested,” and “not supported.” An untested feature should not be represented as production-ready merely because the code exists — MDEL-1 in §3.8 is the cautionary example: correct-looking code and a clean test suite hid a replication-loss defect that the (replica-less) suite could not observe. It is fixed now precisely because that structural gap — no replicas in the harness — was closed for the commands that needed it; the same reasoning now applies to every subsystem still marked Untested or Partial.

### 4. Production-readiness definition

Production readiness is a per-dimension property, and the maturity labels of §3 are the gate: a capability that is **Partial**, **Untested**, or **Not supported** is a defect in the corresponding dimension until it is closed. The acceptance gates at the end of §6 state the evidence each dimension must produce; the definitions below say what "ready" means in terms of the code that exists today. Each dimension names the current gap (from §3) that blocks it and the observable exit condition.

**1. Functional completeness.** The shipped surface is exactly the documented surface, and every command matches its reference contract under both RESP2 and RESP3. Today the surface overstates itself in both directions: the Extensions section of `COMPATIBILITY.md` advertises ten commands that do not exist (`TS.FORECAST`, `TS.AUTOFORECAST`, `TS.DECOMPOSE`, `TS.TREND`, `TS.AUTOCORRELATION`, `TS.XCORR`, `TS.STATIONARITY`, `TS.PERIODS`, `TS.FEATURES`, `TS.FILLGAPS`), while the one documented RTS 8.10 capability that is genuinely absent — the `twa` aggregator (`AggregationType` in `src/aggregators/mod.rs`) — is rejected wherever RTS accepts it. "Ready" means: the command table (30 user-facing commands plus `TS._DEBUG`/`TS._RESTORE`, re-covered by `assign_command_acl_categories` with no drift) is complete against the pinned 8.10 contract with no missing options or arities; the extension surface (`TS.JOIN`, `TS.OUTLIERS`, `TS.ADDBULK`, `TS.LABELSTATS`, `TS.METRICNAMES`, `TS.MDEL`, discovery commands) is explicitly scoped; every divergence in `divergences.yml` is intentional and test-enforced; `COMMAND`, `COMMAND INFO`, and `COMMAND DOCS` metadata (arity, ACL categories, key specs) match the module's real behavior rather than hand-written tables; and `ts-emulate-release` — the mechanism the defect-correction section of `COMPATIBILITY.md` names as the contract's entire remediation path — is either implemented as a registered config or removed from the contract. Exit evidence: Phase 1's command-level matrix filled from the differential suite with zero unregistered divergences, and no command page documenting a command the module does not ship.

**2. Data correctness.** The data model — 16-byte `(i64, f64)` samples, timestamp ordering, duplicate policies, retention boundaries, out-of-order insertion — is specified, and every policy has a reference-model or property test that fails if it regresses. Today the concrete gaps are test gaps more than behavior gaps: the codec property tests exclude NaN/Inf and out-of-order inputs (§3.3), which the wire path demonstrably carries (the fanout decode-totality tests bottom out at a previously remotely-triggerable abort, §3.7); the recompute-on-replica compaction model, formerly asserted by rule metadata alone, is now guarded by a primary/replica equivalence suite (§3.5); and MDEL-1 was exactly this kind of hidden defect — correct-looking code, a clean suite, and a replication-loss bug the replica-less harness could not observe — before it was fixed and replica-tested (§3.8). "Ready" means the Phase 2 data-model list is green: negative timestamps, timestamp overflow/underflow, every duplicate policy, out-of-order samples across chunk boundaries, empty series and empty ranges, NaN/±Inf/large finite values, aggregation of negative/sparse/mixed data, retention exactly at the boundary, partial-chunk and full-chunk deletion, repeated deletion of nonexistent keys, label replacement/removal/duplicates/empties, no-matching-series queries, and concurrent updates to indexed series — each with a test that would fail if the behavior changed. Where a divergence from RTS is deliberate, it is registered, not silent.

**3. Persistence and upgrade safety.** The persistence format is versioned, documented, and upgradeable, and no corrupt input can crash the server. Today the loaders accept an encoding-version range whose bounds are both 1, and the series body has no per-field versioning (§3.4). Nothing states how long an old version stays readable, and there is no upgrade procedure, no downgrade policy, and no documented coexistence rule for rolling upgrades. Corrupt or truncated payloads no longer abort the server: `HANDLE_IO_ERRORS` is declared and the loaders surface short reads as logged errors (CRASH-2/RDB-1 fixed, covered end-to-end by `test_rdb_corrupt_load.py` and `test_ts_restore_malformed.py`) — but there is still **no partial-recovery path**: a corrupt payload fails the whole load. `DUMP`/`RESTORE` round-trip coverage is now collected (`tests/test_dump_restore.py`, §3.4). "Ready" means: a versioned format with an explicit compatibility window and upgrade/downgrade procedure (Phase 3's "versioned persistence policy" is mandatory, never implicit); corrupt/truncated payloads fail the load with a documented error *and* a partial-recovery path (the panic is fixed; partial recovery is still outstanding); `TS._RESTORE` is gated or hardened so it cannot be reached with `+@timeseries` alone; the fork-safe, versioned `TSIX` index aux field keeps its soft-failure guarantee; and RDB/AOF restart, replay, SWAPDB, multi-db, and upgrade-from-prior-format tests are collected and green in CI.

**4. Replication and cluster behavior.** Replication is deterministic — the replica observes the same logical series regardless of timing — and cluster semantics (routing, partial failure, resharding, migration, failover) are specified and tested for every command class. Today `TS.ADD`/`TS.MADD` rewrite `*` timestamps and `TS.INCRBY`/`TS.DECRBY` propagate their resolved timestamp before replication; the `TS.INCRBY`/`TS.DECRBY` and `TS.ADDBULK` paths are replica-tested, but `*`-timestamp propagation is not; the retention-trim cron runs on every node by design — its floor is derived from the data, so it removes only samples reads already hide — and is replica-tested; and compaction destinations are recomputed on every node from the replicated source writes, as in the reference, rather than shipped; an equivalence suite with a Hypothesis oracle, reload and promotion variants verifies that recompute path, while the primary's resolved series options and its policy destinations are replicated as explicit `TS.CREATE`/`TS.CREATERULE` (§3.5). Recompute is exact only while both nodes run the same aggregation arithmetic, so the operational rule is: when a release changes aggregation results, upgrade the primary and its replicas together and full-resync the replicas (`docs/overview.md`, *Replication*). The replicated stream also changed in this release: a replica or AOF loader on an older build cannot parse the resolved `TS.CREATE` (`DECIMAL_DIGITS none`), and an old AOF tail replays auto-creates without policy rules, so the AOF should be rewritten after upgrading. `TS.MDEL` no longer has its write-targeting flaw: the fanout command targets `FanoutTargetMode::Primary` with a replica defense-in-depth check, and standalone deletion propagates resolved per-key `DEL`/`TS.DEL` effects (MDEL-1 fixed, covered by `test_ts_mdel_replication_cme.py` and the standalone replication suite). Most of the cluster harness still runs `REPLICAS_COUNT = 0`, but four suites now run against a live replica (§3.7). There is no partial-success mode: any shard error or the 5 s timeout fails the whole command, per-shard error causes are replaced with a generic string, and a timeout stops the coordinator's wait but not the shard's work (§3.5). "Ready" means: `*`-timestamp propagation has a test against a live replica (MDEL, `TS.INCRBY`/`TS.DECRBY`, `TS.ADDBULK`, compaction recompute, policy destinations, auto-create defaults and the retention-trim cron are now covered — keep them as regression gates); every command has a documented cluster class (single-key / hash-tagged / scatter-gather / coordinator / restricted, per Phase 4); partial-failure and timeout semantics are specified rather than "fail everything"; ACL identity propagation keeps its fail-closed behavior; and atomic slot migration (declared via the named `HANDLE_ATOMIC_SLOT_MIGRATION` module option for Valkey ≥ 9.0.0) is exercised in its production shape, including import-delay and post-export cleanup.

**5. Performance and scalability.** Performance is measured end-to-end, not inferred from in-process microbenchmarks, and targets are set from defined workloads before any optimization. Today the base branch has no end-to-end latency harness: the three criterion targets and the report tools time in-process methods, and `tools/fleet_loader` measures only ingest rate (§3.7). Phase 0's baseline (throughput, p50/95/99 latency, CPU, RSS, bytes per sample, index memory, replication lag, recovery time) cannot be produced until the `tools/server_bench` harness on `comparative-benchmarks` is merged or an equivalent is built. Query parallelism is now bounded: orx-parallel runs on the persistent pool that `ts-num-threads` sizes (§3.6), so what remains is to report that policy and measure it under load. With no caps on matched series, materialized samples, query range, or matchers, a single `TS.MRANGE - + FILTER __name__=~".+"` opens every series and blocks the event loop (DOS-1); the negative-only form is now rejected at parse time. "Ready" means: the Phase 0 workload matrix is benchmarked on fixed hardware with pinned versions and reproducible datasets; query parallelism is bounded and reported; and expensive queries are bounded, yielding (`RM_Yield`), or cancellable rather than monopolizing the event loop.

**6. Memory efficiency and resource bounds.** Memory accounting is correct in both directions — what is reported reflects what is held — allocations go through the Valkey module allocator, and resource use is bounded. What remains: compressed chunks can exceed `max_size` after a merge, and the remaining-capacity estimate is amortized (§3.3); each `"name=value"` term still exists twice (once in the interner, once in the index term dictionary, §3.2); and there are still no resource limits (result size, query range, matchers, compaction fan-out) to bound allocation. "Ready" means: `MEMORY USAGE`/`TS.INFO memoryUsage` reconcile with a tracked-allocator audit; the label index is accounted (done — keep as a gate); `utilization()` and remaining-capacity semantics are documented, including the over-`max_size` case; memory per sample, per series, and per label are measured in the Phase 0 baseline; and the Phase 6 limits (result size, query range, matchers, compaction fan-out) prevent unbounded allocation.

**7. Reliability and failure recovery.** No client-reachable input can crash the server, and shutdown, failover, and reload are clean. Today the structural risk is narrower but unchanged in kind: there is still no FFI panic barrier on the main thread. `catch_unwind` guards blocked-client callbacks, executor jobs, and background threads, but not command dispatch or the RDB, aux, and defrag callbacks, so a panic on those paths is still a whole-server abort (PANIC-1). The specific reproduced instances are gone: the single-command whole-server aborts (CRASH-1/CRASH-2), the defrag underflow and index misalignment (DEFRAG-1), the two index self-deadlocks (INDEX-1), and the Gorilla split/iterator defects (GOR-1) are all fixed with regression tests (§3.8). The RDB and fanout decoders have no reachable panic sites (§3.8). Background tasks are now serialized and cursor-batched but still have no shutdown drain, no backpressure, and `deinitialize()` tears down nothing (§3.6). Sanitizer coverage is weak: the ASAN job sanitizes the Valkey C server, not the Rust module; there is no TSan, Miri, coverage measurement, or scheduled/nightly job (§3.7). "Ready" means: a panic barrier at the FFI boundary with a logged, error-returning fallback; the §5 register closed to zero P0s (PANIC-1 and DOS-1 are the remaining ones); release overflow-checks enabled as defense in depth; lock-order tests that turn rule R5 from review-only into a check, and shutdown/cancellation tests; and sanitizer, fuzz, and soak runs (Phase 5) that complete without reproducible memory errors or leaks.

**8. Security and access control.** Access control is enforced at minimum privilege, untrusted input cannot crash or hang the server, and dependencies and licenses are audited. Today the ACL model is stricter than RTS by design and replica gating is correct; the two minimum-privilege whole-server crashers — CRASH-1 via only `-@all +ts.create` and CRASH-2 via `+@timeseries` — are fixed and regression-tested (§3.8). DOS-1 is still open: no query timeout, no caps, no `RM_Yield`, so a broad bounded filter blocks the event loop, and `TS.QUERYLABELS` without `FILTER` lets a narrowly-scoped ACL user walk every key (DoS audit N1). `cargo audit` now runs in CI, but there is no `cargo deny`, no dependency pinning for the cloned test framework, and no `SECURITY.md`. (The two `unreachable!` arms in peer wire decoding, `src/commands/fanout_codec/filters.rs`, were previously listed here; they are invariant-guarded and unreachable from a peer payload, §3.8.) The manual license review is clean of copyleft but flags `valkey-module` (BSD-3-Clause, attribution becomes live at the first shipped artifact) and `enquote` (Unlicense) (§3.9). "Ready" means: every P0 input path is fuzzed and sanitizer-clean with the reproduced crashers promoted into the corpus; the minimum-privilege matrix is tested (each command under its least grant); query resource limits are enforced; `cargo deny` and license checks join `cargo audit` in CI; and NOTICE/attribution obligations are met before any artifact ships.

**9. Observability and operability.** Operators can detect health, memory pressure, replication lag, compaction backlog, and query failure without source access. Today observability is thin but no longer empty: a module `INFO ts_memory` section reports label-index and interner memory, `INFO ts_compaction` counts failed rule applications and dirty-rule rebuilds, and the per-key accounting that made `MEMORY USAGE` unreliable is fixed (MEM-1, §3.8), but there is still no operational metrics surface, no slow-query log, and no configurable log level (§3.12). The config reference is incomplete and partly wrong — 17 parameters exist, 14 are in the table, `ts-num-threads` is documented as 8 against an actual 4 and two other defaults are blank, the `ts-chunk-size` rename persists in two documents, and the only table lives inside the `TS._DEBUG` page (§3.9). The config surface is what operators actually touch, so `ts.ts-compatibility-mode` (strict vs extended — narrower than its name: it gates DIV-0014; DIV-0023 is a permanent divergence since 2026-10-14) and `ts-num-threads` semantics need documentation that matches implementation. "Ready" means the Phase 6 metric set (series/sample/chunk counts, compressed vs uncompressed bytes, label-index and metadata bytes, compaction and expiration queue depth with errors, expired samples, query/ingest counts, errors, latency) is exported without unbounded per-series cardinality; structured logs with levels; slow-query logging; startup and recovery diagnostics; a config reference generated from the registered parameters; and runbooks for backup/restore, failover, and recovery.

**10. Documentation and ecosystem integration.** Documentation is correct, complete, and operational, and at least one representative client and observability integration are verified against the module. Today internals documentation is unusually strong but operations documentation is absent, and parts of the shipped docs are wrong: `COMPATIBILITY.md` overstates the surface (ten nonexistent commands), `ts-emulate-release` is documented but unimplemented, two command pages (`ts.card.md`, `ts.labelvalues.md`) still carry literal `TODO`s, and the README still claims "concurrent lock-free reads" although the label index uses a per-database `RwLock` (§3.2, §3.9). There is no deployment guide, troubleshooting guide, upgrade/downgrade notes, or cluster operations guide. The ecosystem is entirely greenfield — no client library, Prometheus adapter, Grafana data source, Telegraf plugin, or exporter exists or is referenced (§3.10) — so the "drop-in RedisTimeSeries replacement" claim is asserted but never client-verified. "Ready" means: command docs match the shipped surface; the config reference is generated from code; the operational docs of Phase 7 are published; and at least one client library plus a representative Prometheus/Grafana workflow pass smoke tests against the module (per the Acceptance gates table in §6).

**11. Release and support processes.** There is a repeatable, versioned release path with a changelog, and a documented support and rollback policy. Today there are no release tags, no changelog, and version identity is unreconciled across `Cargo.toml` (0.1.0), `pyproject.toml` (0.1.0), and `VK_TIMESERIES_VERSION = 1` (`src/lib.rs`) (§3.9). Distribution is "clone and `cargo build --release`" — no container image, no example `valkey.conf`, no checksums or signed artifacts, no SBOM, no multi-architecture builds, no release workflow. One landmine is latent only because nothing ships yet: `.cargo/config.toml` applies `-C target-feature=+bmi1,+bmi2,+avx2` unconditionally to every x86-64 build with no runtime feature detection, so the first published binary would `SIGILL` on any pre-Haswell CPU or a baseline VM model with no diagnostic beyond "Illegal instruction" (§3.9). CI never exercises the minimum supported server version — its matrix is `unstable` and `8.1`, not `8.0.0` (§3.9). "Ready" means: version identity is single-sourced; builds are reproducible and architecture-feature-gated; artifacts are signed and checksummed with an SBOM; the MSRV and minimum server version are enforced in CI; and the §6 Acceptance gates row for Release (reproducible artifacts, CI, changelog, versioning, rollback, support process) is evidenced with a completed Milestone F rollback rehearsal.

### 5. Gap analysis and risk register
Create one authoritative gap register with:
* Gap or risk.
* Affected command or subsystem.
* Severity: P0, P1, P2.
* User impact.
* Reproduction case.
* Proposed remediation.
* Test that prevents regression.
* Owner.
* Target milestone.
* Exit evidence.
Suggested severity definitions:
* P0: Data corruption, unrecoverable crash, silent data loss, security issue, or unusable core command.
* P1: Major compatibility gap, incorrect result, serious memory leak, broken persistence, cluster incompatibility, or unacceptable production performance.
* P2: Documentation gap, minor error-message mismatch, optimization, client convenience, or non-critical extension.

### 6. Concrete work plan
#### Phase 0: Establish baselines
Before changing implementation, define measurable targets.
Deliverables
* Command and behavior compatibility matrix.
* Supported Valkey version matrix.
* Deployment matrix: standalone, replica, cluster, TLS, AOF, RDB, multiple databases.
* Workload definitions.
* Initial performance baseline.
* Initial memory baseline.
* Release scope and non-goals.

Define workload profiles such as:
* High-rate sequential ingestion.
* Batched ingestion through TS.MADD.
* Batched ingestion through TS.MADD with out-of-order timestamps.
* High-rate ingestion with TS.ADDBULK.
* Out-of-order ingestion.
* Duplicate-heavy ingestion.
* Single-series range reads.
* Large fan-out multi-series reads.
* Label-index queries.
* Continuous compaction.
* Retention-heavy workloads.
* Mixed read/write workloads.
* Large-cardinality workloads.
* Restart and replica-recovery workloads.

Record at least:
* Throughput.
* p50, p95, p99, and maximum latency.
* CPU utilization.
* RSS and bytes per sample.
* Index memory per series and per label.
* Chunk count and average chunk size.
* Replication lag.
* AOF/RDB size.
* Restart and recovery time.
* Background compaction and expiration backlog.
Do not choose arbitrary acceptance numbers before this baseline. Establish targets from intended workloads, hardware, and comparable Valkey deployments.

**Current-codebase expansion.**  The existing `benches/` targets and `tools/compression_report.sh`, `latency_report.sh`, and `wire_report.sh` are useful codec microbenchmarks, but none opens a socket.  `tools/fleet_loader.sh` loads a live standalone server or cluster with a realistic fleet and is the natural dataset generator, and `tools/server_bench` on the `comparative-benchmarks` branch is a socket-driven latency harness.  Phase 0 should merge that harness rather than build a new one, and extend it to start standalone, replica, and cluster test targets and to write raw CSV/JSON results plus the server/module/client configuration.  It should reuse the fixed `DataGenerator` seeds where useful, but measure end-to-end `TS.ADD`, `TS.MADD`, range, filtered multi-range, and restart traffic rather than inferring those numbers from chunk methods.

The baseline matrix must include Valkey 8.0.0 (the declared minimum), 8.1, and unstable; CI presently covers 8.1 and unstable but not 8.0.0.  The deployment matrix must include a cluster with replicas: most of the CME harness still uses `REPLICAS_COUNT = 0` (four suites — TS.READ, ASM, `TS._DEBUG`, MDEL replication — now run with a live replica), and the replica-bearing topology still needs to extend to resharding, failover, and load.  For every run retain workload seed, series/label cardinality, pipeline depth, encoding, duplicate policy, retention/rules, server configuration, hardware/OS, and warm-up/steady-state/recovery intervals.  In addition to the listed measures, record bytes-in-use and capacity by chunk encoding, allocator-reconciled index/metadata memory, errors/timeouts/truncations, and separate synchronous compaction cost from cron-based expiration work.

The existing scatter/gather implementation and atomic slot-migration support are in scope. Seed the risk register with the §3.8 entries — fixed ones as regression gates to hold, open ones (PANIC-1, DOS-1, and the DoS audit's N1–N4) as work items — each with an owner, a deterministic regression test, and exit evidence.

#### Phase 1: Complete the gap analysis
Build a command-level matrix with columns such as:
Command
Syntax
Valid options
Invalid options
Return type
Error behavior
Read/write classification
ACL category
Key specification
Cluster behavior
Persistence behavior
Replication behavior
Complexity
Unit tests
Integration tests
Fuzz coverage
Documentation
This is particularly important for module commands. Valkey supports command metadata describing arity, complexity, arguments, ACL categories, key specifications, and COMMAND DOCS output. The module should expose this metadata accurately rather than relying only on hand-written argument validation.
Review each subsystem separately:
* Parser and argument validation.
* Timestamp parsing and relative-time handling.
* Duplicate and out-of-order insertion.
* Chunk splitting and merging.
* Compression and decompression.
* Retention and active expiration.
* Deletion.
* Aggregation.
* Label indexing.
* Compaction rules.
* Dependent compactions.
* Persistence.
* Replication.
* Cluster routing.
* Background workers.
* Memory accounting.
* Shutdown and reload behavior.

**Current-codebase expansion.**  Populate the matrix from the actual 30 annotated user commands plus `TS._DEBUG` and `TS._RESTORE`; include RESP2 and RESP3 replies, strict/extended compatibility mode, propagation behavior, and the matching divergence-registry entry.  Check the command annotations, `acl_categories!` declarations, and positional command table together so metadata cannot silently drift from handler behavior; the count-pinning test already covers the first two.  The `COMMAND`, `COMMAND INFO`, and `COMMAND DOCS` rows should be tested, not assumed from the macro annotations.

The subsystem reviews should produce invariants and adversarial tests, not only a feature inventory.  Priority examples: keep the adversarial empty-operand/config corpus that caught CRASH-1 as a regression gate; operator visibility when a downstream compaction write fails, now logged-and-succeed on every writer and counted in `INFO ts_compaction`; corrupted/empty codec and fanout inputs; cron cadence after `CONFIG SET hz`; worker saturation/backpressure/shutdown; and RDB/AOF loader behaviour in a live server (corrupt/truncated paths now have dedicated suites — hold them as gates).  Resolve contract drift in this phase: decide whether to implement `twa` or retain an explicit non-goal, remove or implement the ten nonexistent documented commands, and implement or remove the documented-but-unregistered `ts-emulate-release` setting.

#### Phase 2: Harden correctness and safety
Data-model correctness
Test and specify:
* Minimum and maximum timestamps.
* Negative timestamps.
* Timestamp overflow and underflow.
* Duplicate timestamps under every duplicate policy.
* Out-of-order samples across chunk boundaries.
* Empty series and empty query ranges.
* NaN, positive infinity, negative infinity, and very large finite values.
* Floating-point rounding behavior.
* Aggregation of negative, sparse, and mixed-value data.
* Retention boundaries exactly at the expiration timestamp.
* Deletion of complete chunks and partial ranges.
* Repeated deletion and deletion of nonexistent keys.
* Label replacement, removal, duplicate labels, and empty labels.
* Queries with no matching series.
* Concurrent updates to indexed series.

Memory and concurrency safety
For the Rust implementation:
* Audit every FFI boundary.
* Eliminate production-reachable panics from command paths.
* Verify Send and Sync assumptions for worker-thread data.
* Check lock ordering and shutdown behavior.
* Add bounded queues for compaction and expiration work.
* Define behavior when workers are saturated.
* Add cancellation and shutdown tests.
* Run AddressSanitizer, LeakSanitizer, ThreadSanitizer where feasible, and Miri for suitable Rust components.
* Use fuzzing against parsers, chunk encoders, decoders, persistence readers, and query filters.
* Add failpoints around chunk writes, index updates, compaction, RDB loading, and AOF replay.
Module memory should be allocated through the Valkey module allocation APIs where appropriate. Valkey documents that module-aware allocations are accounted for in memory reporting and eviction decisions, whereas ordinary malloc() allocations should generally be avoided.[valkey]

Query safety
Ensure that expensive queries cannot monopolize the Valkey event loop:
* Bound or expose query result limits.
* Define behavior for very large range queries.
* Avoid unbounded temporary allocations.
* Measure and expose query execution time.
* Yield during sufficiently long module operations where appropriate.
* Add cancellation or timeout behavior if supported by the design.

Valkey provides ValkeyModule_Yield specifically to allow long-running module commands to let the server process background work and, under controlled conditions, other clients.[valkey]

**Current-codebase expansion.**  The individual crash paths named in the original assessment — CRASH-1, CRASH-2/RDB-1, DEFRAG-1, INDEX-1, and the Gorilla split/iterator defects — are fixed on `main` with regression tests (§3.8).  What Phase 2 must still do: add the main-thread FFI panic barrier (PANIC-1; off-thread work is already guarded); keep the RDB and fanout decoders free of panic sites (the two `unreachable!` arms there are invariant-guarded, but returning errors instead is cheap hygiene); decide whether `TS._RESTORE` is internal/gated or safely available to an ACL user granted `@timeseries`; and keep each fixed crasher as a deterministic regression gate in CI.  Build the lock-order and Send/Sync review on the existing threading rules R1–R5 in `src/common/threads/mod.rs` rather than starting fresh.

The data-model corpus must include bit-exact NaN, infinities, `-0.0`, subnormals, timestamp extremes, duplicate timestamps, and out-of-order inputs.  This is required both for stored chunks and cluster wire payloads; grouped aggregation can emit NaN.  Promote every fuzzer shrink to a deterministic test (the live crash reproducers are already promoted and green).  Add release overflow checks unless a reviewed alternative proves all relevant arithmetic checked — still absent, though the defrag underflow itself was fixed by removing the arithmetic.

Resource work must define observable behavior for maximum matched series, matchers, materialized samples, query range, reply bytes, label count, and compaction fanout: reject, truncate with a signal, stream, or cursor.  Add safe `ValkeyModule_Yield` points and cancellation/deadline semantics where feasible so an all-series filtered `TS.MRANGE` cannot monopolize the event loop.  Follow the ordering in §7 of `docs/topics/filter-dos-audit.md`: first the cheap fan-out fixes (a shard checks the fan-out deadline inside its materialization and decode loops, N2, and local work stops sharing a lane with peer requests, N3); then a matched-series cap before keys are resolved, including `TS.QUERYLABELS`' no-`FILTER` branch (N1); then a label-value scan budget, the only fix for F3; then rejection counters and a slow-filter log; then a running sample budget and a standalone deadline.  `ts-num-threads` now bounds orx-parallel query execution; document and expose that policy, including the executor lanes it also scales and the rayon global pool it does not.  The memory-accounting work of the original phase is done on `main` — `MetricName` tracking, postings-index accounting, sealed-chunk capacity reclaim, and `free_effort` (§3.8 MEM-1/FREE-1) — so the remaining items are reconciling `MEMORY USAGE`/`TS.INFO` against a tracked-allocator audit and defining `utilization()`'s semantics.  Background tasks are now serialized and cursor-batched; still make saturation semantics explicit and test teardown rather than relying on the cooperative flag.

#### Phase 3: Validate persistence and lifecycle behavior
Test all combinations of:
* Empty and populated databases.
* Small and very large series.
* Compressed and uncompressed chunks.
* Active retention and partially expired data.
* Compaction rules and dependent rules.
* Multiple databases.
* SWAPDB.
* RDB save and load.
* AOF rewrite and replay.
* Restart during ingestion.
* Restart during compaction.
* Restart during active expiration.
* Corrupt or truncated persistence data.
* Upgrade from each supported prior format.
* Loading data produced by older module versions.
* Shutdown while background workers are active.
Define a versioned persistence policy:
* Format version.
* Compatibility window.
* Upgrade process.
* Downgrade behavior.
* Corruption response.
* Recovery tooling.
* Whether old and new module versions may coexist during rolling upgrades.
A production release should never leave the persistence format implicit.

**Current-codebase expansion.**  The native type accepts an encoding-version range whose bounds are both 1, while the series body is positional and untagged.  Before its first format change, specify a read/write compatibility window, a separate body-format policy if needed, upgrade fixtures, downgrade behavior, rolling-upgrade coexistence, and operator-visible recovery actions.  Preserve and test the independently versioned `TSIX` index auxiliary payload: its fail-soft rebuild behavior is valuable, but it must not conceal malformed primary persistence data.

Keep `tests/test_dump_restore.py`, now collected, green as a gate, and run the complete matrix against a real Valkey process.  If `main` is chosen as the base branch, or its four unique commits are ported, add a round-trip test for compaction rules surviving `DUMP`/`RESTORE`.  Malformed/truncated RDB/AOF and `TS._RESTORE` payloads now have dedicated end-to-end suites (`test_rdb_corrupt_load.py`, `test_ts_restore_malformed.py`), which since the `HANDLE_IO_ERRORS` change no longer terminate the server; extend that coverage to foreign encver and to restarts while writes, compaction, and expiration are active.  The acceptance criterion is not merely successful clean reload: corrupt input must produce the documented error/recovery behaviour without terminating the server, and every supported prior fixture must load in CI.

#### Phase 4: Validate replication and cluster behavior
Valkey Cluster shards keys across 16,384 hash slots, and multi-key operations require all participating keys to be in the same slot unless the implementation provides another routing mechanism.
For every command, document whether it is:
* Single-key and naturally cluster-routable.
* Multi-key but compatible with hash tags.
* Scatter/gather across shards.
* Coordinator-based.
* Restricted to one node.
* Explicitly unsupported in cluster mode.
This decision is especially important for:
* TS.MADD.
* TS.MGET.
* TS.MRANGE.
* TS.QUERYINDEX.
* Cross-series aggregations.
* Joins.
* Metadata queries.
* Compaction rules involving multiple keys.
A reasonable first-release policy may be:
* Support single-series operations on any shard.
* Support multi-key operations only when keys share a hash slot.
Test:
* MOVED and ASK redirections.
* Hash-tagged multi-key operations.
* Resharding while ingesting.
* Resharding while querying.
* Replica promotion.
* Primary failure during ingestion.
* Replica restart and catch-up.
* Module data during slot migration.
* AOF and RDB recovery after failover.
* Node replacement.
* Partial cluster failure.
* Cross-slot rejection.
* Cluster restart with an incomplete slot map.
* Atomic slot migration: no-op, single-key, multi-key, and cross-slot.
Also document the consistency model. Valkey Cluster uses asynchronous replication and does not guarantee strong consistency; acknowledged writes can theoretically be lost under certain failure scenarios. That limitation should be reflected in the TimeSeries durability documentation and test expectations.

**Current-codebase expansion.**  The work is to validate and harden the fanout implementation already present. The remaining propagation work is a dedicated replica test for the `*`-timestamp path, whose propagation is deterministic but unverified against a replica (`TS.INCRBY`/`TS.DECRBY`, `TS.ADDBULK`, compaction recompute, policy destinations, auto-create defaults and the retention-trim cron are now replica-tested; compaction is recomputed on every node and the cron runs on every node by design). Test each model by comparing logical primary/replica data after catch-up, not only rule metadata.

Cover `TS.MADD`, `TS.MGET`, `TS.MRANGE`/`TS.MREVRANGE`, discovery commands, joins, outliers, MDEL, and multi-key rules.  Specify ordering, timeout, cancellation, partial-failure, and per-shard-error semantics; current fanout fails the whole request after five seconds, hides shard causes from the client, and does not stop shard work after a coordinator timeout (DoS audit N2).

CI must run a replica-bearing cluster topology broadly and exercise MOVED/ASK, resharding under read/write load, promotion, catch-up, partial-node failure, node replacement, cross-slot rejection, and restart after failover.  Today four CME suites run with a replica (TS.READ, ASM, `TS._DEBUG`, MDEL replication); the rest of the topology work is outstanding.  Exercise the Valkey >=9 atomic migration path, including import-delay and post-export cleanup; the option is now a named module constant (`HANDLE_ATOMIC_SLOT_MIGRATION`), so replace the numeric-literal documentation once bindings expose the server constant.  Retain tests for topology-fingerprint mismatch and fail-closed ACL identity propagation.
#### Phase 5: Build the test pyramid
Unit and model tests
Implement:
* Parser tests.
* Timestamp and duration tests.
* Aggregator tests.
* Duplicate-policy tests.
* Chunk-boundary tests.
* Compression round-trip tests.
* Index consistency tests.
* Retention tests.
* Compaction tests.
* Persistence serialization tests.
* Reference-model tests.
For compression, use a simple uncompressed reference model as the oracle. Every codec should be tested for:
encode(samples) -> decode(bytes) == samples
Also test random access, partial decoding, append behavior, corrupted input, and transitions between compressed and mutable chunks.
Property and fuzz tests
Useful properties include:
* Encode/decode preserves samples.
* Insert followed by range query returns the expected ordered samples.
* Delete followed by range query removes exactly the requested interval.
* Applying retention repeatedly is equivalent to applying it once.
* Compaction results are independent of chunk boundaries.
* Index results match a full scan.
* Replaying persisted data produces the same logical series.
* Duplicate-policy behavior is deterministic.
* Equivalent query expressions produce equivalent results.
Integration tests
Run the module against actual Valkey processes and test:
* Command behavior.
* Error responses.
* RDB and AOF.
* Replication.
* Multiple databases.
* Eviction.
* Client reconnects.
* ACLs.
* COMMAND, COMMAND INFO, and COMMAND DOCS.
* Module loading and unloading where supported.
* Configuration changes.
* Upgrade and restart scenarios.
Valkey’s own test ecosystem separates unit, module, cluster, and Sentinel integration testing, which is a useful structure to mirror in the proposal.[github]
Load and soak tests
Create a repeatable benchmark harness that records:
* Operations per second.
* End-to-end latency distributions.
* CPU per operation.
* Memory per sample.
* Compression ratio.
* Label-index overhead.
* Compaction overhead.
* Retention overhead.
* Query fan-out cost.
* Replication lag.
* Recovery time.
* Error rate.
Run:
* Short saturation tests.
* Long-duration soak tests.
* Failure-injection tests.
* Mixed-workload tests.
* Memory-pressure tests.
* High-cardinality tests.
* Resharding and failover tests under load.
Use fixed hardware, pinned versions, reproducible datasets, and published workload generators. The benchmark report should include raw data and configuration, not only headline throughput.

**Current-codebase expansion.**  The repository already has ≈1,535 Rust unit tests, broad Python integration coverage (66 single-node files plus 16 cluster files, with new regression suites for corrupt RDB loads, malformed `TS._RESTORE`, MDEL replication, lazyfree, memory reporting, flush-index consistency, index-lock reentrancy, and binary-safe keys), a disciplined differential harness (54 divergences, 24 corpus reproducers), property tests for fanout decoding, and codec/report tooling.  Phase 5 should fill the structural gaps rather than duplicate those assets: only seven of 44 command-handler files have unit tests; most of the cluster suite still has no replicas; the compatibility smoke check is RESP3-only and `continue-on-error`; and the Hypothesis fuzzer is opt-in with no scheduled run.  Keep the clean-room boundary: derive compatibility behavior only from public documentation and black-box interaction with the pinned reference server.

Use the uncompressed model as a bit-exact codec oracle, including adversarial floats and timestamp extremes, and test corrupted input through the actual external boundary.  Add property tests for insert/range order, exact delete intervals, retention idempotence, compaction independence from chunk boundaries, full-scan index equivalence, persistence replay, duplicate determinism, and equivalent selectors.  Promote minimized differential-fuzzer failures into the existing corpus.  Make compatibility blocking only after the divergence registry has been reviewed; a reference error with subject success remains a hard failure.

Use the Phase 0 socket-driven harness for saturation, long soak, mixed read/write, memory-pressure, high-cardinality, failure injection, resharding, and failover tests.  Reports must retain raw measurements and configuration, distinguish client saturation from server/event-loop stalls, and report backlog/lag/error distributions as well as successful-request latency.
#### Phase 6: Add observability and operational controls
Expose enough information for operators to answer:
* How many time series exist?
* How many samples are stored?
* How much memory is used by samples, chunks, labels, and indexes?
* What is the compression ratio?
* How many samples have expired?
* How much compaction work is queued?
* Which aggregations are consuming CPU?
* How many queries are slow or truncated?
* How many malformed commands have been rejected?
* What is the persistence and recovery status?
Consider module-level information such as:
series_count
sample_count
chunk_count
compressed_bytes
uncompressed_bytes
label_index_bytes
series_metadata_bytes
compaction_queue_depth
compaction_errors
expiration_queue_depth
expired_samples
query_count
query_errors
query_latency
ingest_count
ingest_errors
Avoid exposing per-series or per-label metrics by default because those can create unbounded metric cardinality.
Add:
* Structured logs.
* Configurable log levels.
* Slow-query logging.
* Memory-pressure warnings.
* Startup and recovery diagnostics.
* Health-check behavior.
* Operational runbooks.
* Backup and restore procedures.
* Safe configuration defaults.
* Limits for maximum query range, result size, label count, and compaction fan-out.

**Current-codebase expansion.**  A module `INFO ts_memory` section now exists and reports label-index and interner memory (§3.8 MEM-1); extend this into a stable INFO surface with node/database scope and defined reset behaviour.  At minimum expose series, samples, chunks, logical uncompressed bytes, bytes-in-use and capacity by encoding, label-index bytes, metadata bytes, expired samples, background-work scheduled/running/backlog/error counts, ingest/query counts and errors, latency histograms, slow/truncated/rejected query counts, fanout/replication status, and persistence/recovery status.  Avoid per-series/per-label defaults, and make collection bounded so observability does not recreate the cardinality problem it reports.

The logs need structured fields and levels, not only extra messages: a slow-query record should include command class, elapsed time, selected/matched series, samples, response bytes, and safe identifiers; startup/recovery records should include format/config diagnostics; and memory/backlog warnings should identify the applicable configured limit.  Expose the actual query-parallelism policy: `ts-num-threads` sizes the shared pool that runs queries and scales the executor lanes, while rayon's global pool, used by `krcf`, is sized separately.  Add a failed-compaction counter, since that failure is now visible only in the log.  Generate the configuration reference from registered parameters or a checked source of truth, correcting the missing parameters, stale `ts-chunk-size` name, wrong defaults, and the nonexistent `ts-emulate-release` control.  Exercise backup/restore, corruption recovery, failover, slow-query, and overload runbooks before declaring this phase complete.

#### Phase 7: Integrate with the ecosystem
The ecosystem work should be a separate workstream, not a final documentation task.
Client libraries
Provide or validate support for:
* valkey-cli.
* Python.
* Go.
* Rust.
* Java.
* Node.js.
* Any client libraries specifically used by target customers.
Each client should have:
* Command wrappers.
* Typed response handling.
* Cluster behavior documented.
* Integration tests against the module.
* Examples for ingestion and querying.
* Version compatibility notes.

Observability tools
Validate representative workflows for:
* Prometheus remote read/write adapters.
* Grafana data sources.
* Telegraf.
* StatsD or Graphite bridges.
* OpenTelemetry exporters, if in scope.

RedisTimeSeries documents integrations with Prometheus, Grafana, Telegraf, StatsD, and Graphite; those are sensible compatibility targets, although each integration needs a separate Valkey-specific test matrix.[github]
For Prometheus compatibility, test:
* Metric-name mapping.
* Label encoding.
* Counter and gauge behavior.
* Staleness and missing samples.
* Out-of-order samples.
* Remote-read selectors.
* Remote-write batching.
* Query time ranges.
* High-cardinality behavior.
* Prometheus restart and retry behavior.

Packaging and deployment
Deliver:
* Versioned shared-module artifacts.
* Reproducible builds.
* Container images.
* Example valkey.conf.
* Architecture-specific build information.
* Checksums and signed release artifacts.
* Upgrade instructions.
* Compatibility matrix.
Because license compatibility is likely important for your project, include an explicit review of all bundled codecs, dependencies, generated code, and client libraries. Do not assume that compatibility testing against another module permits copying code or incorporating its implementation.

**Current-codebase expansion.**  The ecosystem is greenfield, so select initial customer-facing priorities rather than claiming every RedisTimeSeries-adjacent integration.  Start with `valkey-cli` and the client languages used by initial adopters; validate typed RESP2/RESP3 replies, pipelining, redirects, ACLs, error handling, and cluster behavior against live module targets.  Choose one Prometheus/Grafana workflow as a release gate and test metric/label mapping, counters/gauges, staleness, out-of-order writes, selectors/ranges, batching, retry/restart, and high cardinality.  Treat Telegraf, StatsD/Graphite, and OpenTelemetry as separately scoped integrations.

Packaging must reconcile `Cargo.toml`, `pyproject.toml`, and `VK_TIMESERIES_VERSION`, then add tags/changelog, reproducible shared-module and container builds, example configuration, supported-architecture policy, signed checksums, SBOM, and upgrade/rollback instructions.  Resolve the current unconditional `+bmi1,+bmi2,+avx2` target flags before publishing x86-64 artifacts: supply a baseline/runtime-dispatch design or make the hardware restriction an explicit and tested support boundary.  Add a `cargo deny`/license policy alongside the existing `cargo audit` job, cloned-framework dependency pinning, `SECURITY.md`, and `NOTICE` before distribution; account for the `valkey-module` BSD-3-Clause attribution and make an explicit policy decision on `enquote`'s Unlicense.

#### Acceptance gates
Use release gates that are objective and evidence-based.

| Gate | Required evidence |
| --- | --- |
| Functional | All in-scope commands pass positive, negative, boundary, and regression tests |
| Correctness | No known P0 correctness defects; model and property tests pass |
| Memory safety | Sanitizer, fuzz, and long-running tests show no reproducible memory errors or leaks |
| Persistence | RDB/AOF restart, replay, upgrade, and corruption tests pass |
| Replication | Replica catch-up, restart, promotion, and failover tests pass |
| Cluster | Routing, resharding, migration, failover, and cross-slot behavior are documented and tested |
| Performance | Workload-specific throughput and latency targets are met |
| Scalability | Series count, sample count, label cardinality, and chunk growth remain within defined limits |
| Reliability | Soak and chaos tests complete without unexplained degradation |
| Observability | Operators can detect health, memory pressure, lag, compaction backlog, and query failures |
| Security | ACL, malformed input, resource exhaustion, dependency, and license reviews are complete |
| Ecosystem | Supported clients and at least one representative observability integration pass smoke tests |
| Documentation | Command reference, deployment guide, compatibility matrix, troubleshooting guide, and upgrade notes are published |
| Release | Reproducible artifacts, CI, changelog, versioning, rollback, and support process are ready |

Do not permit a “production” label with unresolved P0 issues. For P1 issues, require either remediation or a formally approved exception that documents impact, workaround, owner, and removal milestone.

For this repository, the gates additionally require: corrupt-input tests that stay green in CI (now present — `test_rdb_corrupt_load.py`, `test_ts_restore_malformed.py`); a broadly replica-bearing cluster topology (only four suites run replicas today); end-to-end server measurements rather than codec-only reports; allocator-audit reconciliation of `MEMORY USAGE`/`TS.INFO` (per-key accounting is fixed; the audit remains); and an exercised rollback using the published artifact.  “No known P0” covers the still-open §3.8 P0s — PANIC-1 and DOS-1 — plus any fuzz/crash reproduction found during the program.  DOS-1's exit evidence is the DoS audit's §8 reproductions (F2, F3, N1, N2) re-run against the new limits, with `PING` p99 from a second client staying within target.
#### Suggested milestones
A practical sequence is:
1. Milestone A — Contract and baseline
    * Compatibility matrix.
    * Scope and non-goals.
    * Workload definitions.
    * Initial benchmark and memory baseline.
    * Risk register.
2. Milestone B — Correctness and persistence
    * Core command coverage.
    * Reference-model tests.
    * Fuzzing.
    * RDB/AOF.
    * Restart and upgrade behavior.
    * Memory and concurrency hardening.
3. Milestone C — Distributed readiness
    * Replication.
    * Failover.
    * Cluster routing.
    * Resharding.
    * Slot migration.
    * Explicit cross-slot semantics.
4. Milestone D — Performance and operability
    * Saturation benchmarks.
    * Soak tests.
    * Failure injection.
    * Memory limits.
    * Metrics, logs, diagnostics, and runbooks.
5. Milestone E — Ecosystem and release candidate
    * Client libraries.
    * Prometheus and Grafana validation.
    * Packaging.
    * Documentation.
    * Security and license review.
    * Release-candidate deployment.
6. Milestone F — Production rollout
    * Internal deployment.
    * Canary workload.
    * Limited external or community preview.
    * Rollback rehearsal.
    * General availability decision.

The milestones should be sequenced by risk rather than convenience.  Milestone A must choose the base branch, merge the benchmark harness, and deliver scope/matrix evidence; Milestone B must close the remaining client-reachable crash surface — the FFI panic barrier (PANIC-1) and query limits (DOS-1) — and pin the persistence format before new feature work (most originally-identified crashers are already fixed on `main` and are regression gates); Milestone C must extend replica coverage across the CME suite (four suites run replicas today) before calling fanout production-ready; Milestone D must demonstrate bounds, load behaviour, and operator visibility; Milestone E must produce the artifact and ecosystem evidence; and Milestone F is a canary and rollback decision, not the first time recovery is tested.
A concise final recommendation for the proposal is:
ValkeyTimeSeries should be promoted to production maturity only after its command semantics, persistence format, cluster behavior, resource limits, and compatibility profile are explicitly specified; after correctness, failure, load, soak, and ecosystem tests provide repeatable evidence; and after operators have the documentation, metrics, runbooks, and rollback mechanisms required to operate it safely.
