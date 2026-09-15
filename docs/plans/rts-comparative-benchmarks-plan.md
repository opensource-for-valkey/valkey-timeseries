# Comparative benchmarks against RedisTimeSeries

Status: implementation sequence steps 1 (reproducible foundation), 2 (first useful
comparison: ADD, MADD, GET, raw RANGE, memory, reports) and 3 (query breadth: aggregated
ranges, label queries, grouping, encoding/dataset sweep profiles and cross-run `compare`)
landed 2026-09-14/15 — `tools/server_bench.sh`, `tools/server_bench/`,
`tools/benchmark_dataset.rs`, `docker-compose.bench.yml`, and the
`tests/reference_server.sh` overrides. Steps 4–5 are not started. Only exploratory
(non-publishable) runs exist so far; see `tools/server_bench/README.md` for what runs
today and its known gaps.

## Objective and scope

Measure the application-visible throughput, latency, and memory cost of Valkey
TimeSeries versus the repository's pinned RedisTimeSeries reference. Results compare
the complete server/module stacks; they cannot isolate the module from its host server.
Performance parity remains a non-goal of [the compatibility contract](../../COMPATIBILITY.md).

Keep the existing Criterion benches and compression/latency/wire reports: they explain
internal costs and are useful when investigating a command-level regression. Add a
separate opt-in server benchmark suite. Do not time pytest, `DiffClient`, server startup,
dataset generation, or compatibility normalization as database operations.

Follow the [clean-room boundary](../../tests/compat/README.md): use only public command
documentation and black-box execution of the pinned binary image. Never consult or
import RedisTimeSeries source, tests, or benchmark code.

## Recommended design

Use a standalone Rust driver with a maintained RESP client and latency histogram
library, selected and pinned during implementation. Keep its dependencies in a separate
Cargo workspace under `tools/server_bench/`, with its own lockfile, so load-generator
dependencies do not enter the shipped module. Use Python only for untimed correctness
validation where reusing the existing normalization code is valuable.

Export deterministic datasets through a small root-crate binary using the existing
`DataGenerator`, `DatasetKey`, and `dataset_seed` implementation. This binary requires
`enable-system-alloc,test-utils`; build the live module separately with its normal
allocator (`cargo build --release`). The benchmark driver does not link the module.
Do not duplicate workload algorithms in another language.

| Proposed file | Responsibility |
| --- | --- |
| `tools/server_bench.sh` | Build orchestration, run directory, owned server lifecycle, cleanup |
| `tools/server_bench/Cargo.toml` and `src/` | CLI, scenario execution, RESP transport, histograms, reports |
| `tools/server_bench/scenarios/*.json` | Versioned, explicit workload profiles |
| `tools/server_bench/validate.py` | Untimed normalized comparison and fixture checks |
| `tools/server_bench/README.md` | Reproduction commands and metric definitions |
| `tools/benchmark_dataset.rs` | Export shared seeded fixtures with round-trip-safe float text |
| `docker-compose.bench.yml` | Isolated benchmark deployment and equal resource limits |
| `target/bench-reports/server/<run-id>/` | Manifest, fixtures/checksums, raw trials, histograms, CSV, Markdown |

Reuse reference provisioning and version validation from `tests/reference_server.sh`.
Its current Docker lifecycle is tied to `docker-compose.compat.yml`; first add narrowly
scoped optional compose-file/project overrides, retaining existing defaults and ownership
behavior for `build.sh` and `fuzz.sh`. The benchmark overlay must inherit the canonical
reference image pin rather than create a second independent pin. Extend lifecycle
checks to cover startup failure and signal cleanup before depending on the overrides.

## Comparable environments

- Publishable baseline: both servers in native-architecture containers on the same
  dedicated Linux host, using the same network path, CPU set, memory limit, and storage
  class. Run the engines sequentially on the same server CPUs, with the driver on
  separate CPUs. Pin the Valkey image by digest as well as the existing reference pin.
- Local macOS runs are useful for smoke checks. Mixed native/Docker runs or architecture
  emulation must be marked exploratory and excluded from the publishable summary.
- Start with one CPU per server; add an equal multi-core resource-budget profile later.
  Record module workers and server I/O threading. Do not assume equal defaults.
- Baseline settings: persistence off, no replicas, keyspace notifications off,
  no eviction, adequate memory headroom, no expiration/retention unless tested,
  and explicit duplicate policy. Read back and record effective settings.
- Set and verify `ts.ts-compatibility-mode strict` on the subject. Strict mode does not
  remove all intentional numerical divergences; validation still applies below.
- Default to RESP2 initially and add a separate RESP3 profile. Use the same client,
  protocol, connection count, pipeline depth, and input bytes for each paired case.
- Create series with explicit `CHUNK_SIZE 4096`. Compare subject default Chimp against
  reference `COMPRESSED` as the primary product comparison. Add subject Gorilla versus
  reference compressed, and uncompressed versus uncompressed as diagnostic profiles.
  Name actual settings; do not describe matching chunk budgets as identical layouts.
- Use fresh owned servers for memory trials and fresh datasets for every timed trial.
  External URLs are an advanced mode: use a unique key prefix, remove only owned keys,
  and never flush or reconfigure an externally managed server. Reject incompatible
  settings. Whole-process memory results require exclusive servers.

Record source commit and dirty state, module/binary hashes, image digests, resolved server
and module versions, dataset hash/seed, scenario version, build flags/toolchain, client
version, OS/kernel, CPU/architecture, affinity, RAM, container limits, network topology,
effective configuration, trial order, and timestamps. Redact URL credentials.

## Workload matrix

Start with curated profiles rather than the Cartesian product of every parameter.
The sizes below are proposed defaults; a dry run must print counts, estimated fixture
size, and the trial budget. Large cases are opt-in and preflight memory limits.

| Family | Cases | Primary measures |
| --- | --- | --- |
| Ingestion | Precreated series; ordered `TS.ADD`; pipelined ADD; `TS.MADD` with 16/128 samples per command | Successful commands/s, samples/s, latency |
| Point reads | `TS.GET` across uniform and hot-key distributions | Commands/s, p50/p95/p99 |
| Raw ranges | `TS.RANGE`/`TS.REVRANGE`; recent 100 points, middle 10%, full series | Queries/s, returned samples/s, latency, reply bytes |
| Aggregated ranges | `min`, `max`, `count`, `sum`, `avg`; explicitly aligned buckets yielding about 100/1,000 points | Queries/s, latency, input/output sample counts |
| Label queries | `TS.QUERYINDEX`, `TS.MGET`, bounded `TS.MRANGE`; matching 1%, 10%, 100% of series | Queries/s, latency, matched series and reply size |
| Grouped queries | MRANGE with time aggregation and `GROUPBY ... REDUCE`; 10/100 groups | Queries/s, latency, output cardinality |
| Memory | Empty series, loaded series, low/high-cardinality labels; three subject encodings | Dataset memory delta, RSS, bytes/retained sample |
| Updates (later) | Duplicate LAST/SUM and 1%/10% late inserts at explicit timestamp distances | Commands/s, latency, resulting sample counts |
| Retention (later) | Ordered ingestion through several retention windows | Samples/s, tail latency, steady-state memory |
| Compaction (later) | 0/1/3 rules per source; ingestion plus destination reads | Ingest penalty, memory, destination query latency |
| Mixed traffic (later) | 90% writes/10% recent reads and 50%/50%, explicitly by command count | Per-operation throughput and latency under offered load |

Use existing constant, drift, drift-quantized, noisy, and counter datasets at regular
timestamps first; add jitter and irregular spacing in the extended profile. Generate
distinct deterministic per-series values rather than cloning one series everywhere.
Label fixtures should independently control label count, label-value cardinality,
selectivity, and key/value byte lengths, with exact expected match counts.

Suggested shapes: one deep series with 1M samples; 1,000 series × 1,000 samples;
100,000 shallow series × 10 samples. These hold total samples constant while exposing
different indexing and per-series costs. Apply range tests to suitable shapes instead
of requesting unbounded multi-series output. Smoke uses 10 × 1,000 samples.

Sweep connections 1/8/32 and pipeline depth 1/16/64 in selected throughput cases.
Treat MADD batch size and pipeline depth as separate dimensions. Writers own disjoint
series and monotonic per-series timestamp streams unless the scenario explicitly tests
contention or disorder. Use explicit timestamps, never `*`. Maintain fixed encoded
value/timestamp lengths where practical. Query windows must contain equal logical data.

## Measurement and correctness rules

1. Preflight both servers, replay an untimed scenario, and validate replies and resulting
   state before reporting comparisons. Reuse `compat_normalize.py` outside timing, without
   inheriting pytest skips or treating registered divergences as benchmark passes.
   Unknown differences invalidate the pair. A known semantic difference is identified by
   divergence ID and excluded from equivalent-result ratios unless an explicit scenario
   oracle establishes equivalence. In particular, do not loosen tolerances to hide the
   variance/summation divergences. Check sample counts, timestamps, labels and bucket
   boundaries as well as values. Validate timed writes again after each trial.
2. Generate and encode requests outside timing; retain bounded per-worker buffers and
   drain/decode every reply. Validate all success/error responses, including individual
   MADD entries. No silent retries. Record timeouts, errors, and incomplete operations;
   an erroneous or incomplete pair gets no performance ratio.
3. Warm read cases for 5 seconds, then measure for 30 seconds, with five paired trials
   and alternating AB/BA order. Calibrate outside measurement to obtain adequate samples
   for faster cases. Write trials replay the same fixed-length trace against the same
   initial state; choose its length during calibration so the faster engine runs for
   at least 30 seconds. This prevents the faster engine accumulating a different dataset.
   Warm writes on disposable data and recreate the measured starting state afterward.
4. Time from request submission through full reply consumption using a monotonic clock.
   Report p50/p95/p99 and observation counts for unpipelined commands. A pipeline timed
   as one unit yields batch latency, never command p99 obtained by division. Only report
   per-command pipeline latency when transport instrumentation can associate each reply
   with its send timestamp. MADD command latency is also distinct from samples/s.
5. Initial throughput tests use bounded, closed-loop load. Label their percentiles as
   closed-loop latency; they do not describe latency at a fixed external arrival rate.
   Add open-loop scheduled arrivals for mixed-load/SLO tests, measuring from intended
   arrival time and recording queueing, missed arrivals, overload, and bounded backlogs.
6. Capture server/client CPU, wall time, bytes sent/received, and optional commandstats
   deltas. Investigate client saturation by increasing driver workers or moving to a
   separate load host. Record PING/small GET controls to characterize transport overhead;
   never subtract them from measured latency. Monitoring runs outside the hot loop or
   at the same documented low frequency on both engines.
7. Report each trial, median trial throughput, per-trial latency percentiles, spread,
   and a paired bootstrap confidence interval for ratios. Five trials provide only a
   rough uncertainty estimate; increase repetitions for publication when spread is high.
   Do not average percentiles or combine unrelated workloads into a headline score.
8. Define throughput ratio as subject/reference and latency/memory ratio as
   reference/subject, so values above 1 favor the subject. Include both absolute values
   and units, identify the selected memory metric, and omit undefined ratios.

For memory, measure an empty-process baseline, a created-but-empty-series state, and
the loaded state on fresh processes using identical persistent client connections.
Report absolute and delta `INFO MEMORY` allocated bytes and OS/container RSS, plus
sampled or complete per-series `TS.INFO memoryUsage` and `MEMORY USAGE` diagnostics.
Record allocator/fragmentation information and settle time. Use retained sample counts
as denominators; for compaction include both source and destination samples explicitly.
Server accounting and RSS capture different costs and must remain separate.

Do not compare `TS.INFO DEBUG` chunk `size` as a portable compression ratio: this repo
reports encoded payload there, while the reference's accounting need not share that
definition. Likewise, sums of per-key memory do not establish total index overhead.
Use a separate labels-on/labels-off experiment for observed indexing cost.

## Implementation sequence and acceptance criteria

1. **Reproducible foundation.** Add isolated lifecycle overrides/deployment, dataset
   export, versioned scenario parsing, preflight and manifests. Acceptance: the same
   seed exports identical bytes; both engines receive the same trace; wrong pins,
   settings, unsupported commands and startup failures fail explicitly; interrupts
   clean up only owned resources. Existing build/fuzz helper tests remain green.
2. **First useful comparison.** Implement ADD, MADD, GET, raw RANGE and memory, plus
   the Rust transport and report writer. Acceptance: smoke completes on both pinned
   stacks, verifies final state, records all units/counts, and regenerates Markdown
   from saved raw data without rerunning servers. Use two identical subject instances
   as a separate harness self-check; do not bypass pin checks in real comparisons.
3. **Query breadth.** Add aggregation, label selection, grouping, encoding and dataset
   sweeps. Acceptance: exact result cardinalities are checked, known numerical differences
   remain visible, and reports group by equivalent scenarios and resource budgets.
4. **Operational workloads.** Add updates, retention, compaction and open-loop mixed
   traffic. Acceptance: bounded datasets/backlogs, checked retention and compaction
   state, and distinct batch/command/arrival latency metrics.
5. **Automation and documentation.** Add a short manually triggered CI smoke job with
   uploaded artifacts. Run measurements on a dedicated performance host. Initially
   impose no wall-clock PR thresholds on shared CI. Establish repeatability before
   adding any dedicated-host regression gate; compare the subject with its own baseline
   as well as the pinned reference.

Meaningful harness tests cover trace determinism, reply/error counting, timeouts,
pipeline association, report arithmetic, invalid comparison suppression, and lifecycle
ownership. They do not assert that either implementation must be faster.

Cluster fanout, replication, RDB/AOF costs, TLS and WAN latency are subsequent projects.
Cluster comparison requires a separately validated reference topology that actually
supports the same cross-shard commands; single-node results cannot establish this.
Module-only commands such as `TS.ADDBULK`, `TS.JOIN`, and `TS.OUTLIERS` belong in separate
extension reports with explicit application-level baselines.

CLI:

```sh
tools/server_bench.sh --profile smoke --dry-run
tools/server_bench.sh --profile smoke
tools/server_bench.sh --profile core --trials 5 --read-duration 30s
tools/server_bench.sh --profile memory --encodings chimp,gorilla,uncompressed
```

## Public documentation used

Accessed 2026-09-05; use the pinned running server to resolve version differences.

- [Redis benchmarking guidance](https://redis.io/docs/latest/operate/oss_and_stack/management/optimization/benchmarks/):
  comparison methodology must account for client, concurrency, pipelining and deployment.
- [TS.CREATE](https://redis.io/docs/latest/commands/ts.create/): shared configuration
  surface; chunk size is an initial allocation budget, not an identical physical layout.
- [TS.INFO](https://redis.io/docs/latest/commands/ts.info/): per-series allocation and
  chunk diagnostics; retain their documented meanings in reports.
