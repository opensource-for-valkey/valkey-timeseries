# Chunk Encoder Benchmark Plan

**Status:** Plan only — not yet implemented.
**Goal:** Characterize the relative strengths and weaknesses of each chunk encoding
(`Uncompressed`, `Gorilla`, `Xor2`, `DeXor`, `Chimp`) across realistic data
distributions, and establish repeatable baselines so compression-ratio and latency
regressions are caught before merge.

---

## 1. What we are measuring

| Metric | Definition | How it is captured |
|---|---|---|
| Compression ratio | `(len() * 16) / data_size()` — raw `Sample` is 16 bytes (`i64` timestamp + `f64` value) | Deterministic report (not a timing benchmark) |
| Encode throughput | Samples/sec for (a) bulk `set_data`, (b) streaming `add_sample` | Criterion, `Throughput::Elements` |
| Decode throughput | Samples/sec for full-chunk iteration and `get_range` materialization | Criterion, `Throughput::Elements` |
| Memory allocations | Alloc count + bytes allocated per encode / per full decode; steady-state footprint via `GetSize` | Counting allocator (see §6) + `memory_usage()` |
| Query scan latency | Wall time for `range_iter`/`get_range` at several selectivities and positions | Criterion, time per query |

## 2. System under test

All five encodings are exercised through the public enum
`TimeSeriesChunk` (`src/series/chunks/timeseries_chunk.rs`), constructed with
`TimeSeriesChunk::new(encoding, chunk_size)`. This is deliberate:

- `Xor2Chunk` is `pub(crate)` (`src/series/chunks/mod.rs`), so an
  external bench crate cannot name it directly.
- The enum dispatch is the code path production queries actually take, so measuring
  through it includes the (small) match overhead uniformly for all encodings.

Relevant per-chunk API already available: `set_data(&[Sample])`, `add_sample`,
`iter()`, `range_iter(start, end)`, `get_range(start, end)`, `len()`, `size()`,
`data_size()` (per encoding), `bytes_per_sample()`, `memory_usage()` /
`get_size()` (via `GetSize` derive). Every encoding interleaves timestamps and
values in a single stream, so one ratio per chunk is the whole story.

**Out of scope:** `save_rdb`/`load_rdb` (requires a live module context),
cross-chunk series operations, and the label index. `serialize`/`deserialize`
round-trip *may* be added later as a phase-2 item since it is pure.

## 3. Workload generators

Create `benches/support/generators.rs` — a self-contained, seeded-RNG module.
The existing generators in `src/tests/generators/` are `#[cfg(test)]`-gated and
invisible to bench targets; rather than re-gate them with a feature that leaks
test code into release builds, copy the useful pieces (uniform, std-normal,
Mackey-Glass) into the bench support module and extend them. All generators take
an explicit `u64` seed (fixed constants checked into the bench source) so runs are
reproducible and baselines comparable across machines and commits.

### 3.1 Value distributions (required workloads)

| ID | Workload | Generator spec | What it stresses |
|---|---|---|---|
| `constant` | Constant values | `v = 42.0` for all samples; variant `constant_int` with `v = 1000.0` (integer-valued float) | Best case for XOR family (all-zero XOR) |
| `drift` | Slowly drifting | Random walk: `v += N(0, 0.01)`, start 100.0; bounded to ±5% of start | Small-mantissa XOR deltas; decimal-space deltas for DeXor |
| `periodic` | Periodic data | `v = A·sin(2πt/P) + N(0, A/100)`, A=50, P=1h of samples; plus a sawtooth variant | Predictable but continuously-changing values; neither constant nor random |
| `noisy` | High-cardinality noisy telemetry | `v = N(100, 25)` full-precision f64, effectively unique values | Worst case for XOR trailing/leading-zero tricks |
| `bursty` | Bursty / change-heavy | Regime-switching: long quiet segments (constant or slow drift, ~80% of samples) interrupted by bursts (200–500 samples of `N(µ·10, σ·20)`), regime switches from seeded RNG | Adaptive behavior; encoders that amortize over a whole chunk vs. per-sample |

Two supplementary distributions (cheap to add, high diagnostic value):

| ID | Workload | Why |
|---|---|---|
| `counter` | Monotonic counter with occasional resets (`v += Poisson(λ=10)`, reset to 0 every ~50k) | The single most common Prometheus-style shape; integer deltas |
| `discrete` | Values from a small set (e.g., 0.0/1.0 gauge, or {0, 0.25, 0.5, 1.0}) | Low-entropy non-constant; distinguishes dictionary-ish wins (a repeated-value window) from XOR wins |

#### Decimal-quantized variants

Real collection agents rarely emit full-precision f64: values arrive rounded to a
fixed number of decimals, which zeroes the low mantissa bits and changes what
every encoder has to carry. Each full-precision shape therefore has a `_q2`
counterpart — the same seeded values rounded to **2 decimal places**
(`QUANTIZED_DECIMALS` in `src/tests/generators/workload.rs`):

| ID | Base | Why |
|---|---|---|
| `drift_q2` | `drift` | Quantized drift often collapses to *no* change between samples; separates real drift cost from mantissa noise |
| `periodic_q2` | `periodic` | Predictable shape at realistic reporting precision |
| `noisy_q2` | `noisy` | The XOR worst case with ~46 mantissa bits removed; the clearest read on how much of `noisy`'s cost is precision, not entropy |
| `bursty_q2` | `bursty` | Regime switches without the mantissa noise inside each regime |

`constant`, `constant_int`, `counter` and `discrete` have no quantized variant:
their values are already exact at this precision.

### 3.2 Timestamp models

Timestamp compression is half the story (every encoding uses some flavour of
delta-of-delta). Cross the value distributions with:

| ID | Model |
|---|---|
| `ts_regular` | Fixed 1000 ms interval (scrape-style) — the default for every workload |
| `ts_jitter` | 1000 ms ± uniform(0, 50) ms — realistic agent jitter; breaks delta-of-delta zero-case |
| `ts_irregular` | Exponential inter-arrival, mean 1000 ms (event-style) |

To keep the matrix tractable, run **all value workloads × `ts_regular`**, and
only `drift` and `noisy` × {`ts_jitter`, `ts_irregular`}.

### 3.3 Sizes

- **Samples per dataset:** generate 64k samples per (workload, ts-model) once at
  bench startup, shared read-only across benchmarks.
- **Chunk sizes:** 1 KiB, 4 KiB (production default, `DEFAULT_CHUNK_SIZE_BYTES`),
  64 KiB. Chunk size changes how much history one chunk amortizes its headers
  over and how much work a single scan does. For fill-style benches the chunk is
  filled to `is_full()` from the shared dataset.

## 4. Cargo / harness changes

```toml
[dev-dependencies]
criterion = { version = "0.5", features = ["html_reports"] }
rand = "0.8"            # already an indirect dep; pin for generators
rand_distr = "0.4"      # normal / poisson / exp distributions

[[bench]]
name = "encode"
harness = false

[[bench]]
name = "decode"
harness = false

[[bench]]
name = "query_scan"
harness = false

[[bench]]
name = "allocations"
harness = false

[lib]
bench = false           # keep `cargo bench` from trying libtest benches on the lib
```

Directory layout:

```
benches/
  support/
    mod.rs          # shared: dataset registry, chunk builders, bench IDs
    generators.rs   # §3 generators, seeded
  encode.rs
  decode.rs
  query_scan.rs
  allocations.rs
tools/
  compression_report.rs   # §5 (a `[[bin]]` or example, not a criterion bench)
```

**Allocator caveat (must be validated first):** the `valkey_module!` macro in
`src/lib.rs` installs `ValkeyAlloc` as the global allocator in the rlib, so the
bench crate cannot install its own `#[global_allocator]`. `ValkeyAlloc` falls
back to the system allocator when not running inside the server (this is why
`cargo test` works today), and the `enable-system-alloc` feature forces system
alloc. **Step 0 of implementation:** confirm `cargo bench --features
enable-system-alloc` links and runs a hello-world criterion bench. If the module
init path drags in anything unusable outside the server, benches must run with
that feature always; bake it into the CI invocation.

Benchmark ID convention (stable names are what baselines diff against — treat
renames as breaking):

```
<group>/<encoding>/<workload>[/<ts_model>][/<chunk_size>]
e.g.  encode_bulk/gorilla/drift/ts_regular/4k
      scan/dexor/noisy/mid_10pct/64k
```

## 5. Compression ratio measurement

Compression ratio is deterministic — running it under criterion wastes time and
buries the numbers in timing reports. Instead:

- A standalone binary (`tools/compression_report.rs`, wired as `[[bin]]` or
  `cargo run --example compression_report`) that, for every
  (encoding × workload × ts-model × chunk size):
  1. fills a chunk via `set_data` until full,
  2. records `len()`, `data_size()`, `size()`, `bytes_per_sample()`,
     ratio = `len()*16 / data_size()`,
  3. emits both a human-readable Markdown table and a machine-readable
     CSV/JSON at `target/bench-reports/compression.{md,csv}`.
- **Regression gate:** check in a golden file
  (`benches/baselines/compression_baseline.csv`) of expected ratios. A small
  `#[test]`-style check (or CI step in the report binary with `--check`)
  fails if any cell regresses by more than a tolerance (suggest: ratio drop
  > 5% relative). Generators are seeded, so numbers are exactly reproducible —
  the tolerance only absorbs intentional algorithm tweaks.

Also record **fill capacity**: samples held by a 4 KiB chunk at `is_full()` per
encoding/workload. This is the operationally meaningful inverse of ratio (how
many chunks a series needs) and exercises the `estimate_remaining_sample_capacity`
logic indirectly.

## 6. Benchmark designs

### 6.1 `encode.rs` — encode throughput

Two shapes, both with `Throughput::Elements(n)` so criterion reports
samples/sec:

- **Bulk (`encode_bulk`)**: `iter_batched` — setup creates an empty
  `TimeSeriesChunk::new(enc, size)` + borrows the pre-generated `&[Sample]` slice
  sized to fill the chunk; routine calls `set_data(samples)`.
  `BatchSize::SmallInput`. This is the compaction/merge path.
- **Streaming (`encode_append`)**: setup creates empty chunk; routine loops
  `add_sample(&s)` over the slice until full or slice exhausted. This is the hot
  `TS.ADD` path. For any encoder that buffers and recompresses on append,
  expect and document a large gap between the two shapes — that gap is itself a
  key finding.

Matrix: 5 encodings × 7 workloads × `ts_regular` × 4 KiB, plus the §3.2 jitter
cross for `drift`/`noisy`, plus chunk-size sweep (1 KiB/4 KiB/64 KiB) for
`drift` and `noisy` only. ≈ 6 × (7 + 4 + 4) × 2 shapes ≈ 180 benchmark points —
acceptable at criterion defaults (~each point 5s warmup + 5s measure ⇒ budget
~25 min; trim with `--sample-size`/`measurement_time` config in a shared
`criterion_config()` helper, and provide `--quick` guidance in the README).

### 6.2 `decode.rs` — decode throughput

Setup (once per point): fill a chunk from the dataset. Routine:

- **`decode_full`**: drain `chunk.iter()`, folding `black_box` over
  `(sample.timestamp, sample.value)` to prevent dead-code elimination and to
  avoid measuring `Vec` growth.
- **`decode_materialize`**: `get_range(first_ts, last_ts)` returning
  `Vec<Sample>` — the shape the command layer actually uses; includes
  allocation cost on purpose.

Same matrix as 6.1.

### 6.3 `query_scan.rs` — query scan latency on common series shapes

Setup: fill one 4 KiB and one 64 KiB chunk per (encoding × workload). Measure
`range_iter(start, end)` drained with `black_box`, reporting **time per query**
(no throughput annotation — latency is the point). Range selectivity cases:

| Case | Range |
|---|---|
| `full` | `[first_ts, last_ts]` |
| `head_10pct` | first 10% of the time span |
| `mid_10pct` | 10% window centered mid-chunk — stresses seek-to-offset; every encoding must decode from the start of the chunk |
| `tail_10pct` | last 10% — common "recent data" dashboard query |
| `point` | single-timestamp hit mid-chunk `[ts, ts]` |
| `miss` | range before `first_timestamp` (early-out path) |

Workloads here can be trimmed to the shape-relevant set: `constant`, `drift`,
`noisy`, `bursty` (periodic/counter add little for scan behavior). ≈ 6 × 4 × 6 ×
2 sizes = 288 points; use reduced `measurement_time` (2 s) since per-iteration
cost is tiny.

Additionally one **filtered scan** case per encoding (`drift` workload only)
going through `TimeSeriesChunk::range_iter` + the value-filter iterator, to keep
an eye on the composed iterator overhead the command layer sees.

### 6.4 `allocations.rs` — memory allocations

Criterion can't count allocs natively and the lib owns the global allocator, so:

- **Preferred:** thread-local counting via a wrapper the lib exposes behind a
  new feature `bench-alloc` (e.g. `#[cfg(feature = "bench-alloc")]` swap
  `get_allocator!()` in `src/lib.rs` for a `CountingAllocator<System>` that
  exposes `alloc_stats() -> (count, bytes)` and `reset()`). The bench then uses
  `iter_custom` — or simpler, plain `#[test]`-style measurement outside
  criterion — to report allocs/op and bytes/op for: one `set_data` fill, one
  full decode, one `mid_10pct` scan.
- **Fallback** (if touching the allocator macro is unpalatable): report only
  steady-state footprint using the already-derived `GetSize`
  (`chunk.get_size()` / `memory_usage()`) per encoding/workload in the §5
  report, plus one-off `dhat`-under-`cargo test` profiling documented as a
  manual procedure. Decide at implementation time; the counting-allocator
  route is strongly preferred because alloc *count* is what predicts latency
  jitter under the server allocator.

Output as a table in `target/bench-reports/allocations.md`, with a golden-file
gate like §5 (tolerance: any increase in allocs/op > 10%).

## 7. Regression workflow

- **Local:** `cargo bench -- --save-baseline main` before a change,
  `cargo bench -- --baseline main` after; `critcmp` recommended in the README
  for side-by-side diffs.
- **CI (nightly or on-demand label, not per-PR — benches are noisy on shared
  runners):**
  1. run compression + allocation report binaries with `--check` against the
     golden files → hard fail on regression beyond tolerance;
  2. run criterion suite with `--save-baseline pr` vs. a cached `main`
     baseline; fail on > 15% throughput regression in any `encode_*`/`decode_*`
     group, > 20% on `scan` (looser: smaller absolute times, noisier). Use
     criterion's `--noise-threshold` / significance rather than raw means.
- Timing thresholds are advisory (warn) on shared runners until we have
  variance data; the deterministic gates (§5 compression, §6.4 allocations)
  are hard-fail from day one — they are the regressions this plan most wants to
  prevent and they are noise-free.

## 8. Analysis deliverable

After the first full run, produce `docs/chunk-encoding-comparison.md`:

- The §5 compression table and §6 throughput/latency tables.
- A **decision matrix**: rows = workload shapes, columns = encodings, cells =
  recommend / acceptable / avoid, with one-line justification (e.g. "Chimp:
  best ratio on decimal-quantized data and irregular timestamps, but loses to
  gorilla on constant series").
- Explicit callouts of asymmetries the single-number view hides:
  bulk-vs-streaming encode gap, mid-range seek cost, alloc count per append.
- This document is the artifact that answers "which encoding should be the
  default / recommended per use case", and should be refreshed whenever the
  golden baselines are intentionally changed.

## 9. Implementation phases

| Phase | Work | Depends on |
|---|---|---|
| 0 | Validate bench crate links & runs against the rlib (allocator caveat, §4); land Cargo scaffolding + empty bench targets | — |
| 1 | `benches/support/generators.rs` + dataset registry; unit-check generator determinism (same seed ⇒ same bytes) | 0 |
| 2 | `tools/compression_report.rs` + golden baseline + `--check` gate | 1 |
| 3 | `encode.rs`, `decode.rs` | 1 |
| 4 | `query_scan.rs` | 1 |
| 5 | `bench-alloc` feature + `allocations.rs` (or documented fallback) | 0 |
| 6 | CI wiring (§7), README for running/interpreting, first `docs/chunk-encoding-comparison.md` | 2–5 |

Phases 2–5 are independent of each other and can be parallelized or landed as
separate PRs.

## 10. Risks & open questions

- **Allocator linkage (§4)** — the one thing that can invalidate the whole
  approach; resolve in phase 0 before writing any benchmark code.
- **Dataset realism** — synthetic generators are proxies. If real telemetry
  extracts exist in `test-data/`, add a `replay` workload in a later phase; do
  not block the initial suite on it.
- **Criterion vs. divan** — divan is faster to run and has built-in alloc
  counting, but criterion is specified by this task and its baseline/HTML
  tooling fits the regression goal; revisit only if suite runtime becomes a
  problem.
- **`upsert_sample` / `merge_samples` with duplicates** — real write paths hit
  these; deferred to a phase-2 addition once the core matrix is stable.
