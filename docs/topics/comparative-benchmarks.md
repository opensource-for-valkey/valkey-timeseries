# Comparative Benchmarks Against RedisTimeSeries

This guide explains how the repository measures Valkey TimeSeries (the **subject**)
against the pinned RedisTimeSeries reference server (the **reference**): what the
harness measures, how a run proceeds, how to run one, and how to read the results.

- Design and rules: [docs/plans/rts-comparative-benchmarks-plan.md](../plans/rts-comparative-benchmarks-plan.md)
- Driver reference (subcommands, scenario schema, run directory):
  [tools/server_bench/README.md](../../tools/server_bench/README.md)

---

## What it measures, and what it doesn't

The suite measures **application-visible** throughput, latency and memory of the whole
server + module stack, as seen by a client over RESP. It compares stacks: a result
cannot separate the module's cost from its host server's (Valkey 9.x vs Redis 8.10).

Performance parity is **not** part of the [compatibility contract](../../COMPATIBILITY.md).
These numbers point to where the work should go. They are not a pass/fail gate.

The suite sits beside the existing internal tools rather than replacing them:

| Tool | Answers |
| --- | --- |
| `cargo bench` (Criterion) | What one code path costs inside the crate |
| `tools/compression_report.sh`, `latency_report.sh`, `wire_report.sh` | Bytes per sample, command latency on our server, fan-out wire size |
| `tools/server_bench.sh` | How our server compares with the reference for the same client workload |

### Clean-room boundary

The reference is a **black box** reached only over the network. The harness, its
scenarios and its oracles come from public command documentation and from observing
the pinned `redis:8.10` image. RedisTimeSeries source, tests and benchmark code are
never consulted or ported (see [tests/compat/README.md](../../tests/compat/README.md)).

---

## Components

| Piece | Role |
| --- | --- |
| `tools/server_bench.sh` | Builds everything, starts and stops servers it owns, runs the driver |
| `tools/server_bench/` | Rust driver in its own Cargo workspace and lockfile, so its dependencies never reach the module |
| `tools/server_bench/scenarios/*.json` | Versioned workload profiles |
| `tools/benchmark_dataset.rs` | Exports deterministic fixtures with the crate's own `DataGenerator` |
| `docker-compose.bench.yml` | Both engines as containers under identical limits |
| `tests/reference_server.sh` | Shared reference lifecycle and version-pin validation (also used by `build.sh` and `fuzz.sh`) |
| `target/bench-reports/server/<run-id>/` | Manifest, traces, raw results, CSV, Markdown report |

The reference image pin lives in exactly one place, `docker-compose.compat.yml`.
The benchmark overlay `extends` it, so bumping the compat pin bumps the benchmark too.
The overlay runs under its own compose project (`valkey-ts-bench`, port 16479), so it
never touches the compat harness containers.

---

## How a run proceeds

```
build ─► export fixture ─► start servers ─► preflight ─► build traces ─► paired trials ─► report
```

### 1. Build

The wrapper builds three things:

- the module with `cargo build --release`, using the normal allocator;
- the fixture exporter, which needs `enable-system-alloc,test-utils`;
- the driver.

`--skip-build` reuses existing builds.

### 2. Export the fixture

`benchmark_dataset` writes `fixture.json`, `series.tsv` and `samples.tsv` into
`target/bench-reports/server/fixtures/<shape>/`. Identical arguments produce
byte-identical files on any toolchain:

- the dataset seed comes from `dataset_seed(DatasetKey)`;
- each series derives its own seed from that and its index;
- values are written as shortest round-trip float text.

Labels `l<i>` take the value `v<index % cardinality_i>`, so the exact number of series
matching each selector is known in advance. The driver re-hashes the files on every run
and refuses a fixture whose shape differs from the scenario's.

### 3. Start servers

The wrapper stops everything it starts, and nothing else. External servers
(`--subject-url`, `--reference-url`) are validated but never reconfigured or flushed.

### 4. Preflight

Before any timing, the driver checks both engines. It verifies:

- the reference's server and module versions match the pin;
- the subject's `ts.ts-compatibility-mode` is `strict`;
- every command the scenario uses is supported;
- the keyspace starts clean.

It then reads back the effective settings and records them. A failed preflight still
writes a manifest, with `status: preflight_failed: <reason>`.

### 5. Build traces

Each case is expanded into RESP frames **before** timing starts:

- engine-specific `TS.CREATE` setup;
- an untimed `TS.MADD` preload for read and memory cases;
- one workload stream per connection.

Each frame carries the reply it expects. Writers own disjoint series and send
monotonic per-series timestamps (explicit, never `*`). The workload bytes are the same
for both engines, and the manifest records a SHA-256 per stream to prove it.

### 6. Paired trials

Each case runs `trials` pairs in alternating AB/BA order. A trial does this on each engine:

1. Create the series with that engine's encoding, and preload when the case needs data.
2. Replay the workload over the case's connections, closed-loop, at the configured
   pipeline depth.
3. Validate every reply. Nothing is retried.
4. Delete only the fixture's keys.

Write cases replay the fixed trace once, then check the final state of every series
with `TS.INFO`. Read cases warm up untimed for `warmup_seconds`, then loop over a fixed
request cycle for `read_duration_seconds`.

### 7. Report

`results.json` holds the raw per-trial data and is rewritten after every case, so an
interrupted run can still be reported. `results.csv` and `report.md` are generated from
it, and `server_bench report --run-dir DIR` regenerates them without starting a server.

---

## Correctness comes before speed

A ratio is only meaningful when both engines did the same work. Each reply is checked
against an oracle computed from the fixture:

| Case | Check |
| --- | --- |
| `TS.ADD` / `TS.MADD` | Echoed timestamps; every per-entry MADD result |
| `TS.GET` | Timestamp and value |
| `TS.RANGE` / `REVRANGE` | Sample count and window |
| Aggregations | Bucket count and timestamps (`ALIGN start`, explicit bounds); `count`/`min`/`max` exact; `sum`/`avg` within 1e-9 relative |
| `TS.QUERYINDEX` | The exact expected key set |
| `TS.MGET` | The last sample of every matched series |
| `TS.MRANGE` | The exact per-series sample count in the window |
| `GROUPBY … REDUCE` | The expected groups, each with the expected bucket count |

If an error, timeout or incomplete reply occurs on either side, that pair is
**invalid** and gets no ratio. It is listed under "Invalid pairs".

Summation order differs between the engines, so `sum` and `avg` can disagree in the
last bits. Every value that differs at all is counted and listed under "Numerical
differences". Do **not** widen the tolerance to hide these differences.

---

## Comparable environments

The wrapper classifies each run by its deployment and records the kind in the manifest:

| `deployment.kind` | Setup | Use |
| --- | --- | --- |
| `self-check` | Two subject processes (`--self-check`) | Checks the harness itself; never a product comparison |
| `mixed` | Native subject process vs Docker reference (the default on macOS) | Smoke checks only. It mostly measures the Docker VM network hop |
| `containers-equal-limits` | Both engines in containers with equal CPU and memory limits (`--subject-docker`) | The only kind that can be published |

Every kind other than `containers-equal-limits` is flagged exploratory, both on stderr
and in the report banner. A publishable result also needs the conditions set out in
the plan:

- both containers run native-architecture on a dedicated Linux host;
- the subject image is pinned by digest (`BENCH_VALKEY_IMAGE=valkey/valkey:9.1@sha256:…`);
- the driver runs on CPUs separate from the servers.

Baseline server settings, applied to both engines: no persistence, no replicas,
keyspace notifications off, `noeviction`, one I/O thread by default, and
`CHUNK_SIZE 4096` with an explicit duplicate policy.

### Encodings

The primary product comparison is the subject's default **Chimp** against the
reference's `COMPRESSED`. Two diagnostic profiles add more pairs: Gorilla vs
`COMPRESSED` (`core-gorilla`) and uncompressed vs uncompressed (`core-uncompressed`).
The same chunk budget does not imply the same physical layout, so reports name the
actual settings.

---

## Running it

### Plan and harness check

```sh
tools/server_bench.sh --profile smoke --dry-run      # counts, fixture size, trial budget; starts nothing
tools/server_bench.sh --profile smoke --self-check   # two subject processes: harness check
tools/server_bench.sh --profile smoke --preflight-only
```

Always run `--dry-run` first on a new profile. It shows what the run will cost, and the
first cold build of the driver and exporter takes a few minutes.

### Exploratory comparison (local)

```sh
tools/server_bench.sh --profile smoke                       # subject process vs pinned reference container
tools/server_bench.sh --profile core --trials 5 --read-duration 30
```

### Equal-limits comparison

`--subject-docker` needs a **Linux** build of the module. On macOS, build it in a
container. bindgen needs libclang, so the bare image fails without it:

```sh
docker run --rm -v "$PWD":/work -v ~/.cargo/registry:/usr/local/cargo/registry \
  -w /work -e CARGO_TARGET_DIR=/work/target/linux rust:1.96-bookworm \
  bash -c 'apt-get update && apt-get install -y clang libclang-dev && cargo build --release'

MODULE_PATH="$PWD/target/linux/release/libvalkey_timeseries.so" \
BENCH_CPUS=4 BENCH_CPUSET=1-4 BENCH_MEM_LIMIT=1500m \
  tools/server_bench.sh --profile core --subject-docker --skip-build
```

Environment knobs for the compose overlay:

| Variable | Default | Meaning |
| --- | --- | --- |
| `BENCH_CPUS` | `1.0` | CPU quota per server |
| `BENCH_CPUSET` | unset | Pin servers to specific cores |
| `BENCH_MEM_LIMIT` | `4g` | Memory (and swap) limit per server |
| `BENCH_IO_THREADS` | `1` | `--io-threads` for both servers |
| `BENCH_VALKEY_IMAGE` | `valkey/valkey:9.1` | Subject image; pin by digest for publication |
| `BENCH_SUBJECT_PORT` | `16480` | Host port for the subject container |
| `COMPAT_REFERENCE_MODE` | `auto` | `docker` or `binary`, as for `build.sh compat` |

### A/B of two builds of our module

`--self-check` with `SELF_CHECK_MODULE_PATH` set makes the "reference" process load a
different module, so the run becomes a paired A/B of two builds. The manifest records
it as `deployment.reference_module`. This is the right tool for "did my branch make X
faster?", because both sides share the same host server.

```sh
SELF_CHECK_MODULE_PATH=/path/to/baseline/libvalkey_timeseries.dylib \
  tools/server_bench.sh --profile core --self-check --trials 5
```

### Comparing runs

```sh
server_bench compare --run-dir A --run-dir B ...
```

`compare` groups runs by deployment, limits, fixture shape and dataset, protocol and
trial budget. Only runs in the same group are comparable. Each run becomes a column
labelled by its encoding pair.

---

## Profiles

| Profile | Fixture | Covers |
| --- | --- | --- |
| `smoke` | 10 × 1,000 | One case per family; checks the pipeline end to end (results not publishable) |
| `core` | 1,000 × 1,000 | Ingest (ADD, MADD 16/128), point reads, raw ranges, memory |
| `agg` | core | Scan-only probe, count/min/sum/avg at 100 buckets, avg at 1,000, GROUPBY at 10/100 groups |
| `range` | core | Raw RANGE windows with RESP3 twins, `head100`, GET as a control |
| `query` | core | Aggregations, label queries at 1/10/100 % selectivity, 10/100 groups |
| `core-gorilla`, `core-uncompressed` | core | Encoding diagnostics |
| `core-counter`, `core-noisy`, `core-jitter` | core | Dataset (value and timestamp) variants |
| `core-shallow`, `core-deep` | 100,000 × 10; 1 × 1,000,000 | Shape variants that keep the total sample count fixed |
| `values-quantized`, `values-counter` | core | Only the cases that value shape affects (encode, scans, sum/max, memory), plus GET as a placement control |

You can write a custom scenario and run it with `--scenario FILE`. The schema rejects
unknown fields and unknown case kinds. The full schema is in the driver README.

---

## Reading the report

**Ratio direction.** Every ratio is written so that **> 1 favours the subject**:

- throughput ratio = subject / reference;
- latency and memory ratios = reference / subject.

**Ratios.** Each ratio compares per-engine medians over valid pairs, and the report
shows the min–max spread alongside. With at least three valid pairs it also gives a
95 % paired bootstrap interval. Five trials only give a rough estimate of uncertainty.

**Latency.** Latency is closed-loop, per command: from submission of the command's batch
to full consumption of its reply. With pipelining, each reply is still matched to its
own batch; the figure is not a batch time divided by depth. Closed-loop percentiles do
not describe latency at a fixed arrival rate.

**Server cost per command.** This comes from `INFO stats` and `INFO cpu` deltas over the
timed phase, divided by commands completed. **CPU per command is the most trustworthy
figure** when throughput is noisy, because it does not depend on how hard the clients
managed to load the server.

**Memory.** The memory cases snapshot `INFO memory` and `DBSIZE` at three points: empty,
after `TS.CREATE`, and after preload (settled). They also sample `TS.INFO memoryUsage`
and `MEMORY USAGE` on up to 100 keys. Bytes per sample uses server-accounted
`used_memory`. RSS is reported but only comparable when the report marks the trial as
a fresh process. Do not compare `TS.INFO DEBUG` chunk `size` across engines: the two
engines define it differently.

**⚠ rows.** A ⚠ marks a write trial in which the faster engine finished in less than
`min_write_seconds`. Treat that ratio as unconfirmed.

**What not to do.** Never average percentiles, combine unrelated workloads into a single
score, or subtract the PING/GET transport controls from measured latency.

---

## Pitfalls seen in practice

- **CPU placement on Docker Desktop.** Every virtio IRQ lands on CPU0 of the Docker VM.
  A server that ends up there pays for network softirqs and looks ~1.6× slower on every
  case, `TS.GET` included. Pin the servers away from it, for example
  `BENCH_CPUSET=1-7`. If the GET or PING CPU-per-command controls differ between
  engines, suspect placement before blaming code.
- **Docker VM memory.** The compose default of `4g` is not real on a Docker VM with
  4 GB in total. Set `BENCH_MEM_LIMIT` to what the VM can actually give both servers.
- **Local timezone cost.** Local subject processes run with `TZ=UTC0`. On macOS, an
  unset `TZ` makes libc re-read zoneinfo on every event-loop iteration. Set it yourself
  when profiling by hand.
- **RESP2 output bytes.** The subject sends RESP2 values as bulk strings and the
  reference as simple strings: +4 bytes per value, with identical values. Expect
  roughly +10 % output bytes on the subject.
- **macOS sleep.** Long runs freeze when the machine idle-sleeps. Start
  `caffeinate -i -s -m -w <pid>` alongside a multi-hour run.
- **Run directories are disposable.** `target/bench-reports/` is gitignored and is
  wiped along with `target/`. Copy out any run you want to keep for `compare`.
- **Stale builds.** Rebuild after switching branches. A stale module produces confusing
  preflight failures.

---

## Not covered yet

The current harness covers steps 1–3 of the plan: the foundation, ingest/read/memory,
and query breadth. Still missing:

- updates, retention, compaction and open-loop mixed traffic (step 4);
- automatic write-trace calibration and a fresh server process per memory trial;
- a manually triggered CI smoke job, dedicated-host runs and baseline tracking (step 5).

Cluster fan-out, replication, RDB/AOF, TLS, and module-only commands (`TS.ADDBULK`,
`TS.JOIN`, `TS.OUTLIERS`) are separate future projects. A cluster comparison would need
a reference topology that is validated separately.

---

## Testing the harness

```sh
(cd tools/server_bench && cargo test)                     # scenario, fixture, trace, manifest, redaction
python -m pytest tests/test_reference_server_lifecycle.py  # reference lifecycle and ownership; no Docker needed
```

These tests cover determinism, reply accounting, report arithmetic and lifecycle
ownership. They deliberately never assert that either engine is faster.
