# Server benchmark driver

Comparative, application-visible benchmarks of Valkey TimeSeries against the
repository's pinned RedisTimeSeries reference. Design and rules:
[docs/plans/rts-comparative-benchmarks-plan.md](../../docs/plans/rts-comparative-benchmarks-plan.md).
Clean-room boundary: [tests/compat/README.md](../../tests/compat/README.md) — the
reference is a black-box target reached only over the wire.

## Status

Implementation sequence steps 1–3 are in place:

| Piece | Where | State |
| --- | --- | --- |
| Reference lifecycle overrides (compose file/project), startup-failure ownership | `tests/reference_server.sh`, `tests/test_reference_server_lifecycle.py` | done |
| Benchmark deployment with equal limits, inheriting the reference image pin | `docker-compose.bench.yml` | done |
| Deterministic fixture export with checksums | `tools/benchmark_dataset.rs` | done |
| Versioned scenarios, fixture verification, trace building + hashing | `src/scenario.rs`, `src/fixture.rs`, `src/trace.rs` | done |
| Preflight (pins, settings, strict mode, command support, freshness) | `src/preflight.rs`, `src/engine.rs` | done |
| Run manifest | `src/manifest.rs` | done |
| Timed execution: ADD, MADD, GET, RANGE/REVRANGE, memory; reply validation; histograms | `src/executor.rs` | done |
| Raw results, CSV and Markdown reports, paired bootstrap CI | `src/results.rs`, `src/report.rs` | done |
| Orchestration | `tools/server_bench.sh` | done |
| Aggregated ranges, label queries (QUERYINDEX / MGET / bounded MRANGE), GROUPBY/REDUCE; exact cardinality checks; visible numerical deviations | `src/trace.rs` (oracles), `src/executor.rs` (checks) | done |
| Encoding and dataset sweeps: `core-gorilla`, `core-uncompressed`, `core-counter`, `core-noisy`, `core-jitter`, `core-shallow`, `core-deep` profiles; `server_bench compare` groups runs by equivalent scenario and budget | `scenarios/`, `src/report.rs` | done (profiles written, not yet run) |
| Updates, retention, compaction, open-loop mixed traffic | — | step 4, not started |
| Disposable-data warm-up for write trials; automatic write-trace calibration | — | not started: write trials replay the fixed trace once and are flagged ⚠ when shorter than `min_write_seconds` |
| Fresh server process per memory trial | — | not started: memory trials run on the servers the wrapper started; `fresh_process` is recorded and RSS is only comparable when true |

## Running

```sh
tools/server_bench.sh --profile smoke --dry-run      # plan only: counts, fixture size, trial budget
tools/server_bench.sh --profile smoke --self-check   # two subject processes; harness check only
tools/server_bench.sh --profile smoke                 # subject process vs pinned reference container
tools/server_bench.sh --profile smoke --preflight-only
```

The wrapper builds the module (`cargo build --release`, normal allocator), the
fixture exporter (`benchmark_dataset`, needs `enable-system-alloc,test-utils`) and
this driver (its own workspace, own lockfile), exports the scenario's fixture into
`target/bench-reports/server/fixtures/<shape>/`, starts the servers it is asked to
own, and invokes the driver. Everything it starts, it stops; external URLs
(`--subject-url`, `--reference-url`) are validated but never reconfigured or
flushed.

The reference comes from the shared `tests/reference_server.sh`, pointed at
`docker-compose.bench.yml` under the `valkey-ts-bench` compose project (port 16479 by
default), so it never collides with the compat harness containers. The reference
image pin lives only in `docker-compose.compat.yml`; the benchmark overlay
`extends` it. `COMPAT_REFERENCE_MODE=binary` and `COMPAT_REFERENCE_URL` work as
they do for `build.sh compat` and `fuzz.sh`.

Only a run with both engines as containers under equal limits on a dedicated Linux
host (`--subject-docker` with a Linux module build, `BENCH_CPUS`/`BENCH_CPUSET`/
`BENCH_MEM_LIMIT`) is publishable; every other deployment is flagged exploratory in
the manifest and on stderr.

## Driver subcommands

```
server_bench dry-run      --scenario F [--fixture DIR]
server_bench fixture-args --scenario F
server_bench preflight    --scenario F --fixture DIR --out DIR --subject URL --reference URL
                          (--reference-pin 8.10.0:81000 | --self-check)
                          [--subject-owned] [--reference-owned] [--module-path P] [--repo-root P]
                          [--trials N] [--read-duration-seconds S] [--note k=v]...
server_bench run          (same arguments) — preflight, then every case's paired trials
server_bench report       --run-dir DIR — regenerate results.csv / report.md from results.json
```

`--reference-pin` is supplied by the wrapper from `COMPAT_REFERENCE_VERSION` /
`COMPAT_REFERENCE_MODULE_VERSION` in `tests/reference_server.sh`, so the pin is
single-sourced. `--self-check` accepts a second subject build as the "reference";
the manifest records `comparison: self_check` and such runs are never a product
comparison.

## Scenarios

`scenarios/*.json`, `schema_version` 1. Unknown fields, unknown case kinds and
malformed selectors are rejected. A scenario names:

- `fixture`: series count, samples per series, workload and timestamp-model ids
  (passed to the exporter verbatim), label cardinalities, key prefix, interval;
- `series`: `chunk_size`, `duplicate_policy`, and the `ENCODING` sent to **each**
  engine (`subject`: chimp/gorilla/uncompressed, `reference`: compressed/uncompressed);
- `protocol` (resp2 default), `trials`, `warmup_seconds`, `read_duration_seconds`,
  `read_cycle_requests`;
- `cases`, each with `connections` and `pipeline`:
  - `add`, `madd {batch, samples_per_series}` — ingestion into precreated series; `samples_per_series` (default 1)
    is how many consecutive samples of one series a batch carries (1 = per-tick fan-in, `batch` = per-series buffering);
  - `get {distribution}`, `range {window, reverse}` — point and raw range reads;
  - `aggregate {window, aggregator, buckets, reverse}` — `ALIGN start AGGREGATION
    <min|max|count|sum|avg> <bucket>` with the bucket sized to yield about `buckets` points;
  - `queryindex {label}`, `mget {label}`, `mrange {label, window}` — label queries on
    `l<label>=<value>`; the value cycles deterministically, and selectivity follows the
    label's cardinality (100 → 1 %, 10 → 10 %, 1 → 100 %);
  - `groupby {label, group_label, window, aggregator, buckets, reducer}` — `TS.MRANGE ...
    GROUPBY l<group_label> REDUCE <reducer>`; output cardinality is the number of distinct
    group values among the matched series;
  - `memory`.

  Selectors are objects: `{"type": "uniform"}`, `{"type": "hot", "keys": N,
  "share_percent": P}`, `{"type": "recent", "points": N}`, `{"type": "head",
  "points": N}` (the first N samples: the same reply as `recent` from a read that
  starts at the series' first sample, so `recent − head` is what an engine spends
  decoding samples the window discards), `{"type": "middle", "percent": P}`,
  `{"type": "full"}`.

  A case may carry `"protocol": "resp3"` (or `"resp2"`) to override the scenario's
  protocol for its workload connections, so one run can hold RESP2/RESP3 twins of a
  case; both engines emit identical bytes under RESP3, which makes the RESP2 − RESP3
  spread the wire-format share of a gap.

Profiles: `smoke` (10 × 1,000, one case per family), `core` (1,000 × 1,000, ingest /
point / range / memory), `range` (the core fixture; raw RANGE windows with RESP3 twins,
`head100`, GET as a control — the profile behind
`docs/plans/range-performance-plan.md`), `query` (aggregations, label queries at
1/10/100 % selectivity, 10 and 100 groups), the encoding variants `core-gorilla` and `core-uncompressed`, and the
dataset/shape variants `core-counter`, `core-noisy`, `core-jitter`, `core-shallow`
(100,000 × 10) and `core-deep` (1 × 1,000,000). Run several and put them side by side with
`server_bench compare --run-dir A --run-dir B ...`, which groups runs by deployment,
limits, fixture shape/dataset, protocol and trial budget — only runs in one group are
comparable — and lists each run's ratios as a column labelled by its encoding pair.

## Fixtures

`benchmark_dataset` writes `fixture.json`, `series.tsv` and `samples.tsv`. The same
arguments export byte-identical files on any toolchain: the dataset seed comes
from `dataset_seed(DatasetKey)` in the module crate, each series derives its own
seed from that and its index (SplitMix64, spelled out in the manifest), and values
are the shortest round-trip float text, read back before the file is accepted.
Labels `l<i>` take value `v<index % cardinality_i>`; `expected_matches` in the
manifest gives the exact series count per value. The driver re-hashes every file
against the manifest on each run and refuses a fixture whose shape differs from
the scenario's.

## Traces

Every case is expanded into RESP frames before any timing: engine-specific
`TS.CREATE` setup (hashed per engine), an untimed `TS.MADD` preload for read and
memory cases, and per-connection workload streams. Writers own disjoint series
(round-robin) and emit timestamp-major, monotonic per-series streams; read cycles
draw keys from a case-seeded generator. Each frame carries its expected reply.
`traces.json` and the manifest record frame counts, bytes and SHA-256 per stream;
the workload digest is identical for both engines by construction and is recorded
per engine in the manifest.

## Measurement

Per case, `trials` paired trials run in alternating AB/BA order. Each trial, on
each engine: `TS.CREATE` every series (engine-specific encoding), preload the
fixture with `TS.MADD` for read and memory cases, open the case's connections,
then replay the workload closed-loop with the configured pipeline depth. Every
reply is validated against the fixture (`TS.ADD` echoes, per-entry `TS.MADD`
results, `TS.GET` timestamp and value, `TS.RANGE` count and window); nothing is
retried. Write trials replay the fixed trace once and are followed by a
`TS.INFO` state check on every series; read trials warm up untimed for
`warmup_seconds` and then loop over the fixed request cycle for
`read_duration_seconds`. Afterwards only the fixture's keys are deleted.

Every query reply is checked against an oracle computed from the fixture: aggregated
buckets must match in count and timestamps (`ALIGN start`, buckets from the window
start, empty buckets omitted); `count`, `min` and `max` values must match exactly;
`sum` and `avg` are compared with a relative tolerance of 1e-9, and every value that
differs at all is counted and listed under "Numerical differences" in the report —
the summation-order divergences stay visible and never become a pass/fail lever.
`TS.QUERYINDEX` must return exactly the expected key set, `TS.MGET` exactly the last
sample of every matched series, raw `TS.MRANGE` the exact per-series sample count
inside the window, and GROUPBY exactly the expected groups with the expected bucket
count each (reducer values are not compared: they depend on summation order across
series). `ALIGN start` requires explicit bounds on both products, so aggregated cases
never use the `-`/`+` shorthand.

Latency is measured per command from the submission of its batch to the full
consumption of its reply. With pipelining that is still a per-command figure —
each reply is matched to the batch it belongs to — and the report labels it
"closed-loop, pipelined depth N". These are closed-loop numbers; they do not
describe latency at a fixed arrival rate. `INFO stats`/`INFO cpu` deltas over
the timed phase (the warm-up excluded) give server-side bytes in/out, commands
and CPU seconds; the report's "Server cost per command" table divides them by
the commands completed, with reference/subject ratios. CPU per command is the
figure to trust when throughput trials are noisy — it does not depend on how
much of the server the clients managed to load.

Local subject processes run with `TZ=UTC0`: with `TZ` unset — or naming a
zoneinfo file such as `UTC` — macOS libc re-reads the file inside every
`localtime_r`, which the server calls once per event-loop iteration: half of all
main-thread samples in a profile. A POSIX string with no file behind it (`UTC0`)
is parsed once and cached. glibc caches either way, so containers are unaffected.
Set it when profiling by hand too.

Memory trials snapshot `INFO memory` and `DBSIZE` before setup, after
`TS.CREATE` (settled), and after preload (settled), plus `TS.INFO memoryUsage`
and `MEMORY USAGE` on up to 100 sampled keys. Bytes/sample uses server-accounted
`used_memory`; RSS is reported but only comparable across fresh processes.

The report gives per-engine medians over *valid* pairs with min–max spread,
ratios of those medians (throughput subject/reference, latency and memory
reference/subject, so > 1 favours the subject), and a 95 % paired bootstrap
interval when at least three valid pairs exist. An erroneous, timed-out or
incomplete trial invalidates its pair and is listed under "Invalid pairs".

## Run directory

`target/bench-reports/server/<run-id>/` holds `manifest.json` (source commit and
dirty state, module hash, host facts, effective server and module settings read
back from each engine, baseline `INFO memory`, deployment notes, redacted URLs,
scenario body with any overrides), `preflight.json`, `traces.json`,
`scenario.json`, `fixtures/fixture.json`, and after the trials `results.json`
(raw per-trial data), `results.csv` and `report.md`. A failed preflight still
leaves a manifest with `status: preflight_failed: <reason>`; `results.json` is
rewritten after every case, so an interrupted run can still be reported.

## Tests

```sh
(cd tools/server_bench && cargo test)                     # driver: scenario/fixture/trace/manifest/redaction
python -m pytest tests/test_reference_server_lifecycle.py  # helper lifecycle, no Docker needed
```
