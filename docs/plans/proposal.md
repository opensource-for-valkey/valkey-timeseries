# ValkeyTimeSeries production-readiness proposal

The following outlines the production-maturity program for ValkeyTimeSeries, integrated with the Valkey Bundle release process. The program defines a supported compatibility profile, workload targets, release gates, owners, and a staged rollout. It must close the open P0 risks before any production claim.

The proposed first production release is a RedisTimeSeries 8.10-compatible core for supported commands, plus explicitly documented ValkeyTimeSeries extensions, distributed primarily through the Github releases and the Valkey Bundle. Compatibility must be defined by public RedisTimeSeries documentation and black-box testing of the pinned reference server; RedisTimeSeries source and tests are out of scope.

This assessment was verified against `unstable` @ `b13fdaf10` (2026-10-08) on 2026-10-14. `main` has diverged from `unstable`: it carries four commits with no equivalent on `unstable`, including DUMP/RESTORE persistence of compaction rules (`219c723ca`). Choosing the release base branch is the first decision this proposal requests.

## Product contract

| Area | Required decision |
|---|---|
| Compatibility | RedisTimeSeries 8.10 baseline, supported divergences, and strict versus extended mode |
| Commands | Supported syntax, options, replies, errors, ACLs, key specifications, and `COMMAND DOCS` metadata |
| Data model | Timestamp and value rules, duplicate policy, ordering, retention, and missing-value behavior |
| Queries | Range boundaries, aggregation, grouping, filtering, sorting, and resource limits |
| Durability | RDB/AOF compatibility window, upgrade policy, corruption response, and recovery procedure |
| Distributed operation | Replication, clustering, timeout, partial-failure, resharding, and failover behavior |
| Extensions | Valkey-specific commands and their portability limits |
| Operations | Supported Valkey versions, observability, and support policy |

Non-goals for the first release should be explicit.

## Current assessment

The codebase implements a broad command surface and several substantial extensions, but it is not yet production-ready. The remaining blockers are structural rather than isolated regressions.

### What is in place

- The RedisTimeSeries 8.10 command surface is implemented except for the `twa` aggregator. The module also provides extensions including `TS.JOIN`, `TS.OUTLIERS`, `TS.ADDBULK`, `TS.MDEL`, and label-discovery commands.
- Differential compatibility testing compares the module with a digest-pinned RedisTimeSeries 8.10 reference over RESP2 and RESP3. Intentional divergences are registered; a reference error with subject success remains a hard failure. In CI, the `compat-smoke` job runs RESP3 only and is non-blocking (`continue-on-error: true`).
- The storage engine supports Uncompressed, Gorilla, and Chimp (ELF on Chimp) codecs. Chimp is the default; cluster wire data uses uncompressed chunks below 16 samples and Chimp otherwise.
- Native RDB, AOF rewrite, index auxiliary persistence, clustering, fanout, ACL propagation, active expiration, and background maintenance exist.
- Atomic slot migration is implemented for Valkey ≥9.0.0; four cluster suites run replicas, but resharding and failover under load are not exercised. CI tests Valkey 8.1 and unstable, so atomic slot migration is exercised only on unstable.
- Query parallelism is bounded: work runs on the shared pool sized by `ts-num-threads`. The exception is `krcf` (behind `TS.OUTLIERS METHOD rcf`), which uses rayon's global pool.
- The repository has broad Rust and Python coverage, including command, codec/property, malformed-persistence, lazyfree, index-consistency, and differential compatibility tests over RESP2 and RESP3. A differential fuzzer and deterministic corpus complement those tests, but compatibility, replica, failover, and end-to-end workload coverage are not yet complete.

### Important gaps

| Area | Current state and test coverage | Production gap |
|---|---|---|
| Panic safety | **Partial.** Regression tests cover malformed persistence and selected crash-prone paths; `HANDLE_IO_ERRORS` is declared, and the RDB and fanout decoders have no `unwrap`, `expect`, or reachable `unreachable!` outside tests. Off-thread work (blocked-client callbacks, executor jobs, background threads) runs under `catch_unwind`. Systematic FFI-boundary and panic-injection coverage is absent. | No panic barrier on command dispatch or the RDB, aux, and defrag callbacks; a remaining reachable panic on those paths aborts the Valkey process. |
| Query safety | **Not supported.** Command and compatibility tests exercise query semantics, but no limit, timeout, cancellation, yielding, or exhaustion tests exist. | No limits for matched series, samples, label-value scans, range, or matchers; no yielding, timeout, or cancellation. A negative-only filter is now rejected at parse time, and regex compilation is size-bounded. |
| Persistence | **Partial.** Corrupt-load, save/restore, DUMP/RESTORE, AOF-rewrite, and index-persistence tests exist; compatibility-window, upgrade/downgrade, and real recovery/replay coverage is absent. | The loaders check a min..=max encoding-version range, but both ends are 1; there is no compatibility policy, upgrade path, or downgrade policy. |
| Replication | **Partial.** A live-replica suite covers create, add, delete, `TS.MDEL`, rules, `TS.INCRBY`/`TS.DECRBY` without `TIMESTAMP`, and `TS.ADDBULK` (auto-create options, duplicate policies, out-of-order recompaction), including the retention window and compaction destination samples, plus the retention-trim cron, compaction recompute (equivalence suite, Hypothesis oracle, reload and promotion), policy destinations, and auto-create defaults. | `*`-timestamp propagation has no dedicated replica test; aggregation-arithmetic changes across versions need a replica resync (documented). The retention-trim cron runs on every node by design: its floor follows each series' last sample, so nodes can differ only in stored samples, never visible ones. |
| Clustering | **Partial.** CME suites cover selected fanout commands with replicas; resharding, failover, promotion, node replacement, shard errors, and cancellation under load are untested. | Fanout fails as a whole on shard error or timeout; client-visible shard causes and cancellation semantics are undefined. A coordinator timeout stops the wait but not the shard's work. Replica coverage is narrow. |
| Performance | **Not supported** on the base branch. Codec, wire, and in-process benchmark reports exist, and `tools/fleet_loader` reports ingest rate against a live server; no latency harness or workload SLO gate exists. The socket-driven `tools/server_bench` harness is on the unmerged `comparative-benchmarks` branch. | Existing benchmarks measure in-process codecs, not a module-loaded server under client traffic. |
| Resource bounds | **Not supported.** No adversarial workload coverage establishes or verifies query, memory, worker, or compaction limits. | Query and compaction work can allocate or run without defined limits. |
| Shutdown | **Partial.** Background-task behavior has some functional coverage, but queue saturation, backpressure, cancellation, and shutdown-drain behavior are not tested. | Background tasks are cursor-batched and serialized and stop between batches once shutdown begins, but have no queue, backpressure policy, or shutdown drain. |
| Operations | **Partial.** `INFO ts_memory` and memory-reporting behavior are tested, but metrics, and logs have no operational exercise coverage. | `INFO ts_memory` exists, but there are no operational metrics, slow-query logging, structured logs, or complete runbooks. Metrics collection and `TS._DEBUG STATS` are on the unmerged `observability` branch. |
| Security tooling | **Partial.** CI runs `cargo audit` and an ASAN job, but the ASAN job instruments the Valkey server, not the Rust module. | No license check (`cargo-deny`), Rust-side sanitizer, or CI fuzzing. |
| Documentation | **Partial.** Command and compatibility behavior are exercised by tests, but documentation has no completeness or configuration-reference validation gate. | Contract and internals documentation are strong, but operations docs and configuration reference are incomplete or incorrect. |
| Bundle integration and release | **Not supported.** No Valkey Bundle build, installation, upgrade, or rollback test pipeline includes this module. | The module has no defined upstream release-integration contract: supported Valkey and Bundle versions, module loading/configuration, architecture policy, provenance inputs, upgrade/rollback behavior, and bundle smoke tests. Unconditional x86-64 AVX2/BMI flags need a support policy or runtime dispatch. |
| Ecosystem | **Not supported.** No live supported-client or Prometheus/Grafana interoperability smoke coverage exists. | No supported client or observability integration has been validated. |

### Open P0 risks

| Category | Risk | Required outcome |
|---|---|---|
| Panic | Command dispatch and the RDB, aux, and defrag callbacks have no `catch_unwind` barrier. Unwinding across the module FFI boundary aborts the server, so any of the remaining production `unwrap`/`expect`/`assert!` sites reached on those paths is a whole-server abort. The specific reproduced crashers are fixed and regression-tested. | Add an FFI boundary barrier, replace reachable panics with errors, and retain regression and fuzz tests. |
| DOS | A bounded but broad selector such as `TS.MRANGE - + FILTER __name__=~".+"` opens every matching series on the main thread and blocks the event loop. A regex matcher can scan every label value to return nothing, and a series cap does not bound that work. `TS.QUERYLABELS` without `FILTER` opens every key and skips unreadable ones, so even a narrowly scoped ACL user pays the full cost. In cluster mode a timeout returns an error but the shard keeps working under the GIL. See [filter-dos-audit.md](../topics/filter-dos-audit.md) (re-evaluated 2026-10-13). | Define and enforce work and result limits, including a label-value scan budget; add safe `ValkeyModule_Yield` points and deadline/cancellation behavior that stops shard work where feasible. |

### In-progress branches

Several gaps above already have implementations on unmerged branches. Each one must be rebased and merged, or explicitly retired, during Milestone A or B; until then, the gap stays open.

| Branch | Gap | State against `unstable` @ `b13fdaf10` |
|---|---|---|
| `observability` | Operations: metrics collection, `TS._DEBUG STATS` and `INFLIGHT` | 10 commits ahead, 0 behind; fast-forwardable |
| `panic-barrier` | Panic P0: a barrier around every command handler | 1 unmerged commit (`b2bdf231b`); 150 behind |
| `DOS-1` | DOS P0: query budgets, matched-series cap, label-value scan budget, cooperative yielding, cluster budget propagation, slow-filter log | 17 unmerged commits; 96 behind |
| `comparative-benchmarks` | Performance: the socket-driven `tools/server_bench` harness | 12 unmerged commits; 12 behind |
| `twa` | Functional: the `twa` aggregator | 27 unmerged commits from July; 247 behind |

## Release-readiness requirements

1. **Functional contract.** The documented command surface exactly matches shipped commands, metadata, and behavior. Decide whether `twa` is implemented, by reviving the `twa` branch, or an explicit non-goal.
2. **Data correctness.** Test timestamp extremes, duplicate policies, out-of-order writes across chunk boundaries, NaN/Infinity/`-0.0`, retention boundaries, deletion, labels, aggregation, and no-match queries. Use a simple uncompressed model as the codec oracle and make wire round trips bit-exact.
3. **Durability.** Define read/write format compatibility, rolling upgrades, downgrade behavior, corrupt-input handling, and recovery tooling before the first format change. Test RDB/AOF restart and replay against a real server.
4. **Distributed correctness.** Add a primary/replica test for `*` timestamps; compaction recompute, policy destinations, auto-create defaults, and the retention-trim cron are already covered by the replica suite. Define fanout ordering, timeout, cancellation, and partial-failure semantics. Exercise replica promotion, resharding, migration, failover, and restart under load.
5. **Performance and bounds.** Establish workload-specific throughput, p50/p95/p99 latency, CPU, RSS, bytes per sample, index memory, replication lag, and recovery-time baselines using a client-driven harness on fixed hardware. Bound query concurrency, result size, ranges, matchers, and compaction fanout.
6. **Reliability and security.** Close panic and DOS issues; add lock-order, cancellation, shutdown, and soak coverage. Run dependency, license, ACL, and malformed-input checks in CI, and make `compat-smoke` blocking across RESP2 and RESP3.
7. **Operability.** Publish bounded metrics for series, samples, chunks, memory, background work, errors, latency, fanout, replication, and recovery. Add structured logs, configurable levels, slow-query logging, a code-derived configuration reference, and recovery/failover runbooks.
8. **Release process and Valkey Bundle integration.** Treat Valkey Bundle as the primary distribution channel. Define the module-to-upstream release contract: supported Valkey and Bundle versions, module version mapping, build inputs, supported architectures and bases, `INFO modules` verification. Validate upgrades and rollback using versioned Bundle images; retain a standalone module as well.
9. **Ecosystem.** Validate at least the clients used by initial adopters plus one representative Prometheus/Grafana workflow, including RESP2/RESP3, pipelining, ACLs, redirects, retries, and cluster behavior.

## Delivery plan

### Milestone A — Contract and baseline

- Choose the release base branch and reconcile `main` with `unstable`.
- Approve compatibility profile, supported versions, extensions, and workloads.
- Merge or retire each [in-progress branch](#in-progress-branches); merge `observability` and the `comparative-benchmarks` harness first.
- Build a command matrix covering syntax, replies, errors, ACLs, key specs, persistence, replication, cluster behavior, tests, and documentation.
- Adopt `tools/server_bench` (or an equivalent) as the reproducible, client-driven harness for standalone, replica, and cluster targets. Record raw CSV/JSON results with workload seed, configuration, hardware, and warm-up/recovery intervals.
- Establish baselines on Valkey 8.0.0, 9.0.0, and unstable; include replica-bearing clusters. Align the CI matrix (today 8.1 and unstable) with the supported versions, including a 9.0 leg for atomic slot migration.

### Milestone B — Correctness, safety, and persistence

- Close all panic and DOS issues, starting from the `panic-barrier` and `DOS-1` branches; preserve all fixed crash reproducers as CI gates.
- Add adversarial model, property, fuzz, and failpoint tests. Promote minimized fuzzer findings to the deterministic corpus.
- Define and test persistence compatibility, RDB/AOF recovery, upgrade/downgrade, and corruption handling.
- Define memory and worker saturation semantics; reconcile `MEMORY USAGE` and `TS.INFO` with allocator tracking.

### Milestone C — Distributed readiness

- Verify deterministic replication and compaction behavior with live replicas.
- Define and test fanout failure, timeout, ordering, and cancellation behavior.
- Expand replica-bearing cluster tests to MOVED/ASK, resharding, promotion, failover, node replacement, cross-slot behavior, and atomic slot migration.

### Milestone D — Performance and operability

- Set and meet workload-specific resource and latency targets.
- Run saturation, mixed workload, high-cardinality, memory-pressure, soak, failure-injection, resharding, and failover tests.
- Deliver stable metrics, logs, slow-query diagnostics, configuration reference, and operational limits.

### Milestone E — Release candidate

- Correct command and operations documentation.
- Deliver validated client and observability integrations.
- Deliver a Valkey Bundle integration candidate: reproducible module build inputs, a versioned compatibility manifest, supported-architecture policy, module loading/configuration defaults, and Bundle-image smoke tests that verify `INFO modules` and core commands.
- Integrate the module into the upstream release pipeline. Supply the module-specific inputs for provenance, SBOMs, notices, security review, changelog entries, and upgrade documentation; the Valkey release process owns final image publication, checksums, and signatures.

### Milestone F — Rollout

- Make the general-availability decision from the release gates below.

## Release gates

| Gate | Required evidence |
|---|---|
| Functional | In-scope commands pass positive, negative, boundary, metadata, and regression tests. |
| Correctness | No open P0; model, property, and differential tests pass with reviewed divergences only. |
| Durability | RDB/AOF restart, replay, corruption, upgrade, and recovery tests pass. |
| Distributed | Replica catch-up, promotion, failover, routing, resharding, migration, and cross-slot tests pass. |
| Performance | End-to-end workload targets are met on recorded hardware and configurations. |
| Scalability | Resource limits hold under defined series, samples, labels, queries, and fanout workloads. |
| Reliability | Fuzz, sanitizer, chaos, and soak runs complete without reproducible leaks, crashes, or unexplained degradation. |
| Operations | Metrics, logs, limits, diagnostics, and runbooks have been exercised. |
| Security | ACL, malformed-input, dependency, license, and artifact reviews are complete. |
| Release and Bundle integration | The module is built reproducibly in the supported Valkey Bundle pipeline; versioned Bundle images pass module-load, `INFO modules`, core-command, upgrade, and rollback smoke tests on every supported architecture/base. Ownership and published evidence for provenance, notices, changelogs, signatures, and support are defined. |
| Ecosystem | Supported clients and one observability integration pass live smoke tests. |

P1 issues require remediation or a written exception with impact, workaround, owner, and removal milestone.

## Future releases

The program above scopes the first production release. Two substantial capability tracks already exist as in-progress branches and should be planned as follow-on releases once the first release has shipped. Both must go through the same functional, correctness, differential, and performance treatment before any release claim.

### PromQL support

The `promql` branch implements a native PromQL engine (`src/promql/`) exposed through `TS.QUERY` (instant) and `TS.QUERYRANGE` (range). It covers vector selectors, subqueries, rollup functions, binary operators with vector matching, aggregations, and a function library, along with query limits, per-query statistics, cluster fanout, and a dedicated engine benchmark. A follow-on release would make ValkeyTimeSeries a Prometheus-compatible query target — the natural path to the validated Prometheus/Grafana workflow required by the ecosystem gate.

### Predictive analytics and exploratory data analysis

The `feat/forecasting` branch adds predictive analytics and exploratory data analysis on top of the same statistical foundation as `TS.OUTLIERS`:

- **Forecasting.** `TS.FORECAST` fits user-specified models (ARIMA/SARIMA/AutoARIMA, ETS/AutoETS, exponential smoothing, Holt-Winters, Theta, naive and intermittent-demand models, RandomWalkWithDrift) with transform pipelines and model combination, returning predictions, prediction intervals, and accuracy metrics; `TS.AUTOFORECAST` selects the model automatically. Forecasts can be written back to a series with `STORE`.
- **Exploratory data analysis.** `TS.DECOMPOSE`, `TS.TREND`, `TS.PERIODS`, `TS.STATIONARITY`, `TS.AUTOCORRELATION`, `TS.FEATURES`, and `TS.STATS` for structure discovery and diagnostics; `TS.SANITIZE` and `TS.FILLGAPS` for data quality; and `TS.BACKTEST` and `TS.XCORR` for model validation and cross-series analysis. A Python EDA tutorial accompanies the commands.
