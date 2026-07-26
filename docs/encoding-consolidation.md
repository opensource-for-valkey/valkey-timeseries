# Chunk Encoding Consolidation — Measurement and Rationale

**Status:** `TsXor` has been removed (see §5.5); `Xor2` and `DeXor` are still present.
This document records what the measurements say, so that a future decision to trim the
encoding set further is made against data rather than intuition.

Every table below is the measurement as taken at commit `bf8bb9db`, when all six
encodings still existed. The tsxor rows are kept as-is — they are the evidence for the
removal, not a description of the current tree.

**Question asked:** if the six chunk encodings had to be reduced to a smaller set,
which should survive and why?

**Answer:** keep `Uncompressed`, `Gorilla` and `Chimp`; remove `TsXor`, `Xor2` and
`DeXor`. That cut gives up **2.0%** of the compression achievable with all five
compressed encodings, and removes roughly 6,200 lines. `DeXor` is the one genuinely
close call — see §5.4.

---

## 1. How the numbers were produced

All three report tools in `tools/` were run over the full matrix. They are documented
in `AGENTS.md` under Benchmarks; none of them is a criterion bench.

```sh
tools/compression_report.sh --by-workload ratio     # 168 rows: 6 encodings x 28 scenarios
tools/latency_report.sh --workloads all --ts-models all --samples 1000   # 216 rows
tools/wire_report.sh --workloads all --ts-models all                     # 6048 rows
```

Outputs land in `target/bench-reports/{compression,latency,wire}.{csv,md}`.

Measurements below were taken on an **Apple M2, 8 cores, rustc 1.92.0**, at commit
`bf8bb9db`, release profile with `--features enable-system-alloc,test-utils`.
Timings are wall-clock medians and are machine- and load-dependent: **compare rows
within one run, never absolute numbers across machines.** The size and ratio figures
are deterministic and do reproduce exactly.

### 1.1 Re-measured after the chimp repeat path (2026-07-26)

The original analysis found that chimp had no answer to gorilla on flat series (§5.3).
Chimp has since gained one: a value whose bit pattern matches its predecessor bypasses
the ELF layer and is written as Elf case `0` plus Chimp's `xor == 0` flag — 3 value
bits, where it previously fell through to the raw `10` marker for 4. Every compression figure
below is from a re-run against that change; the latency and wire tables are unchanged
(§3.3 is neutral to within ±1.4%, and §3.4's medians move by at most 0.6%).

What moved: chimp's geomean ratio 3.29 → **3.35**, its worst case against the best
encoding 0.40 → **0.50**, and `constant` 25.50 → **31.87**. What did not move: the
recommended keep-set, the 2.0% headline, or any conclusion in §5 — gorilla still wins
flat series by 2.0x, for the reason now given in §5.3.

---

## 2. The axes

Six axes were considered. They are not equally weighted, and §5 says which one
decided each call.

| # | Axis | Source | Why it matters |
|---|---|---|---|
| 1 | Correctness under adversarial payloads | `wire_report` gate | A gate, not a score — an encoding that cannot round-trip NaN is unusable on the fan-out path |
| 2 | Compression at chunk capacity | `compression.csv` | The user-visible reason to pick an `ENCODING` at all |
| 3 | Encode / decode / scan speed | `latency.csv` | Decode is paid on every read; encode once per sample |
| 4 | Fan-out wire payload across sample counts | `wire.csv` | `compression_report` fills chunks to capacity; the interesting threshold lives at small `n` |
| 5 | Memory footprint | `compression.csv` (`size`, `capacity_4k`) | This is a Valkey module; resident bytes per sample is the product |
| 6 | Maintenance surface and migration cost | `wc -l`, RDB/proto call sites | The reason to trim in the first place |

---

## 3. Results

### 3.1 Correctness (axis 1) — decides nothing

The wire report puts every encoding through adversarial payloads (NaN, ±inf, `-0.0`,
subnormals, timestamp extremes, duplicate timestamps) before measuring anything. This
matters because the grouped/aggregated fan-out path back-fills empty buckets with NaN.

**All six encodings passed: 0 non-lossless rows out of 6,048.**

No encoding is disqualified here, so the decision rests entirely on axes 2–6.

### 3.2 Compression at chunk capacity (axis 2)

28 scenarios = workload x timestamp model x chunk size (1k / 4k / 64k).
Ratio is `(len * 16) / data_size`; higher is better.

| encoding | geomean ratio | scenarios won | within 5% of best | worst case vs. best |
|---|---:|---:|---:|---:|
| chimp | **3.35** | **16/28** | **22/28** | **0.50** |
| dexor | 3.18 | 3/28 | 14/28 | 0.36 |
| gorilla | 3.03 | 7/28 | 13/28 | 0.27 |
| tsxor | 2.84 | 1/28 | 1/28 | 0.22 |
| xor2 | 2.02 | 1/28 | 3/28 | 0.09 |

"Worst case vs. best" is the floor: the lowest ratio this encoding achieves as a
fraction of the best encoding in that same scenario. Chimp has the shallowest floor,
meaning it is the safest single choice when the workload is unknown.

Every scenario's winner and its margin over the runner-up:

| scenario | winner | ratio | runner-up | ratio | margin |
|---|---|---:|---|---:|---:|
| constant/regular/4k | gorilla | 63.74 | dexor | 42.56 | **1.50x** |
| constant_int/regular/4k | gorilla | 63.74 | dexor | 42.55 | **1.50x** |
| periodic_q2/regular/4k | dexor | 7.08 | chimp | 5.91 | 1.20x |
| discrete/regular/4k | tsxor | 14.12 | gorilla | 12.27 | 1.15x |
| bursty_q2/regular/4k | chimp | 10.89 | tsxor | 9.47 | 1.15x |
| drift/jitter/4k | chimp | 2.27 | gorilla | 1.98 | 1.15x |
| counter/regular/4k | dexor | 10.16 | gorilla | 9.05 | 1.12x |
| drift/jitter/1k | chimp | 2.27 | gorilla | 2.04 | 1.11x |
| drift/irregular/4k | chimp | 1.96 | dexor | 1.76 | 1.11x |
| drift_q2/regular/4k | chimp | 10.87 | tsxor | 9.76 | 1.11x |
| noisy_q2/regular/4k | dexor | 4.83 | chimp | 4.37 | 1.11x |
| drift/jitter/64k | chimp | 2.27 | dexor | 2.05 | 1.11x |
| drift/irregular/64k | chimp | 1.97 | dexor | 1.82 | 1.09x |
| bursty/regular/4k | chimp | 2.66 | dexor | 2.57 | 1.04x |
| drift/irregular/1k | chimp | 1.97 | xor2 | 1.89 | 1.04x |
| noisy/irregular/1k | xor2 | 1.76 | chimp | 1.69 | 1.04x |
| noisy/jitter/1k | chimp | 1.87 | gorilla | 1.81 | 1.04x |
| noisy/irregular/64k | chimp | 1.68 | gorilla | 1.63 | 1.04x |
| noisy/jitter/64k | chimp | 1.89 | gorilla | 1.81 | 1.05x |
| noisy/regular/1k | gorilla | 2.22 | dexor | 2.16 | 1.03x |
| noisy/jitter/4k | chimp | 1.89 | gorilla | 1.82 | 1.03x |
| noisy/regular/4k | gorilla | 2.23 | dexor | 2.19 | 1.02x |
| noisy/irregular/4k | chimp | 1.67 | gorilla | 1.64 | 1.02x |
| drift/regular/64k | chimp | 2.67 | dexor | 2.61 | 1.02x |
| drift/regular/1k | gorilla | 2.68 | chimp | 2.65 | 1.01x |
| drift/regular/4k | gorilla | 2.68 | chimp | 2.66 | 1.01x |
| periodic/regular/4k | chimp | 2.25 | dexor | 2.23 | 1.01x |
| noisy/regular/64k | gorilla | 2.21 | dexor | 2.19 | 1.01x |

Note the shape of this table: outside `constant`, almost every margin is under 1.2x.
The encodings are far more alike than the count of them suggests.

### 3.3 Speed (axis 3)

`latency_report`, 1000 samples, medians across all workloads and timestamp models,
expressed as a multiple of `uncompressed`. Lower is better.

| encoding | encode bulk | encode append | decode iter | get_range | scan mid 10% |
|---|---:|---:|---:|---:|---:|
| uncompressed | 1.00x | 1.00x | 1.00x | 1.00x | 1.00x |
| xor2 | **85x** | **8.8x** | 11.4x | 92x | 28.8x |
| gorilla | 106x | 10.3x | 13.2x | 107x | 31.0x |
| chimp | 166x | 16.8x | **10.2x** | **83x** | **23.5x** |
| dexor | 184x | 18.1x | 17.6x | 142x | 40.2x |
| tsxor | **812x** | 78.2x | **9.1x** | 70x | 21.5x |

The headline: **chimp has the best decode of the practical encodings while gorilla has
the best compressed encode.** TsXor's 812x bulk encode is the outlier of the set.

### 3.4 Fan-out wire payload (axis 4)

`wire_report`, sweeping `n` from 1 to 8000. Payload bytes as a fraction of the raw
16-bytes-per-sample size, median over 42 workload shapes. Lower is better; values
above 1.00 mean the "compressed" payload is **larger** than the raw samples.

| n | gorilla | tsxor | xor2 | dexor | chimp |
|---:|---:|---:|---:|---:|---:|
| 1 | 1.91 | 3.04 | 2.87 | 2.22 | 2.74 |
| 3 | 1.18 | 2.09 | 1.62 | 1.26 | 1.33 |
| 8 | 0.75 | 1.52 | 0.93 | **0.68** | 0.72 |
| 12 | 0.66 | 1.38 | 0.79 | **0.56** | 0.60 |
| 16 | 0.61 | 1.32 | 0.71 | **0.50** | 0.52 |
| 30 | 0.55 | 1.21 | 0.60 | 0.41 | **0.41** |
| 64 | 0.50 | 1.09 | 0.52 | 0.36 | **0.34** |
| 128 | 0.48 | 1.00 | 0.49 | 0.37 | **0.31** |
| 256 | 0.47 | 0.64 | 0.56 | 0.35 | **0.29** |
| 1000 | 0.47 | 0.38 | 0.61 | 0.34 | **0.27** |
| 8000 | 0.49 | 0.30 | 0.65 | 0.35 | **0.27** |

This table is the clearest single view in the whole analysis. It shows why
`WIRE_COMPRESSION_MIN_SAMPLES = 16` exists (everything inflates at small `n`), that
**tsxor actively inflates payloads for all n < 128**, and that chimp is the asymptotic
winner from n=30 up.

Decode cost on the same path, as a multiple of uncompressed:

| n | gorilla | tsxor | xor2 | dexor | chimp |
|---:|---:|---:|---:|---:|---:|
| 16 | 2.80 | 3.81 | **1.90** | 3.40 | 2.61 |
| 64 | 3.58 | 3.79 | **1.88** | 4.67 | 3.00 |
| 256 | 3.92 | 3.23 | **2.71** | 4.98 | 3.02 |
| 1000 | 4.10 | **2.89** | 3.50 | 5.23 | 3.14 |
| 8000 | 4.09 | **2.79** | 3.66 | 5.39 | 3.17 |

Pareto-optimality on the joint `(wire_bytes, decode_us)` objective, across the 756
cells with n >= 16 (i.e. how often an encoding is not beaten on *both* axes at once):

| encoding | Pareto-optimal | smallest payload |
|---|---:|---:|
| chimp | **81%** | **50%** |
| xor2 | 62% | 4% |
| gorilla | 45% | 26% |
| tsxor | 22% | 1% |
| dexor | 19% | 19% |

Fraction of those cells where chimp beats the encoding on bytes **and** decode
simultaneously: tsxor 66%, dexor 62%, gorilla 48%, xor2 30%.

### 3.5 Memory footprint (axis 5)

From `compression.csv` at 4k chunk size, medians. `capacity_4k` is samples that fit in
a 4k chunk — higher is better. Note the `size` column is the `get_size()` heap
footprint (buffer *capacity*, which doubles) for gorilla/tsxor/dexor/chimp, so
`heap/data` is a fair comparison only within that group.

| encoding | data_size | heap size | heap/data | samples per 4k chunk |
|---|---:|---:|---:|---:|
| chimp | 4098 | 8368 | 2.04 | **900** |
| dexor | 4098 | 8368 | 2.04 | 831 |
| tsxor | 4098 | 9384 | **2.29** | 810 |
| gorilla | 4098 | 8368 | 2.04 | 686 |
| xor2 | 4099 | 8368 | 2.04 | **509** |
| uncompressed | 4096 | 4272 | 1.04 | 256 |

Chimp fits 31% more samples per chunk than gorilla and 77% more than xor2. TsXor
carries a visibly worse capacity-doubling profile than the rest.

### 3.6 Maintenance surface (axis 6)

Lines of Rust including tests, and test density:

| module | lines | of which tests | `#[test]` / `proptest!` |
|---|---:|---:|---:|
| dexor | **2766** | 696 | 41 |
| xor2 | **2319** | 744 | 19 |
| chimp | 1848 (+178 `elf64.rs`) | 447 | 31 |
| uncompressed | 1456 | 0 | 10 |
| gorilla | 1338 | 66 | 11 |
| tsxor | 1104 | 0 | 5 |
| `stream/` (shared) | 1332 | 0 | — |

`stream/` is used by chimp, gorilla, xor2, tsxor and dexor, so it survives any cut
that keeps chimp or gorilla. `elf64.rs` is chimp-exclusive.

The two largest modules, dexor and xor2, are also the two proposed for removal — 5,085
lines, or roughly 45% of the encoding tree.

---

## 4. The decisive calculation

Compression ratio per encoding is the wrong question when choosing a *set*. The right
one is: for each scenario, what is the best ratio still available after the cut? The
table below is the geomean of the per-scenario maximum over the surviving encodings.

| keep set | geomean | loss vs. all five |
|---|---:|---:|
| all five | 3.709 | — |
| chimp + gorilla + dexor | 3.685 | −0.6% |
| **chimp + gorilla** | **3.633** | **−2.0%** |
| chimp + gorilla + xor2 | 3.638 | −1.9% |
| chimp + dexor | 3.533 | −4.8% |
| chimp + xor2 | 3.351 | −9.6% |
| chimp alone | 3.346 | −9.8% |

Single-encoding removals from the full set:

| drop | geomean | loss |
|---|---:|---:|
| xor2 | 3.703 | −0.1% |
| tsxor | 3.690 | −0.5% |
| dexor | 3.657 | −1.4% |
| gorilla | 3.594 | −3.1% |
| chimp | 3.571 | −3.7% |

Two readings matter here. First, **chimp and gorilla are the only two encodings whose
removal costs more than 1.5%** — they are the load-bearing pair. Second, going from
chimp+gorilla down to chimp alone costs 7.8 percentage points, almost all of it from
the `constant` workload; that is the entire argument for keeping gorilla. That gap was
9.2 points before chimp gained its repeat path (§1.1) — the path narrowed the case for
gorilla without closing it.

---

## 5. Decisions

### 5.1 Keep `Uncompressed` — not a compression decision

It is the only correct encoding below the wire threshold. At n=1 every compressed
format produces a payload 1.9x–3.0x *larger* than the raw samples, and none of them
breaks even before n≈5 (see §3.4). It is the fallback in `samples_to_chunk_lossless`,
it is ~4x cheaper to append to than anything else, and `UNCOMPRESSED` is the
RedisTimeSeries-compatible name. Removing it is not on the table.

### 5.2 Keep `Chimp` — dominant on the axes that matter most

- Best geomean ratio (3.35) and best in 16/28 storage scenarios (§3.2).
- Within 5% of the best in 22/28 scenarios — no other encoding exceeds 14 (§3.2).
- Best decode of the practical encodings: 10.2x vs. gorilla's 13.2x (§3.3).
- Pareto-optimal in 81% of wire cells and smallest payload in 50% (§3.4).
- Fits the most samples per 4k chunk: 900 vs. gorilla's 686 (§3.5).
- Shallowest worst case: 0.50 of best, so it is the safest default when the workload
  shape is unknown.

The cost is a 166x bulk encode, and that trade is the right way round: encode happens
once per sample and, on the fan-out path, in parallel across shards; decode happens on
every read and, on that same path, serially in the coordinator. It is already the wire
encoding for exactly this reason (see `samples_to_chunk` in
`src/series/chunks/serialization.rs`).

### 5.3 Keep `Gorilla` — the one thing chimp still cannot absorb

Two independent reasons:

1. **Constant and near-constant series.** Gorilla reaches 63.74x on `constant` and
   `constant_int` against chimp's 31.87x — a 1.50x margin over the runner-up and by far
   the largest gap anywhere in the matrix (§3.2). Both encodings now have a
   repeated-value path; gorilla's is simply cheaper. A repeat costs gorilla 2 bits (one
   timestamp bit, one value control bit) against chimp's 4 (one timestamp bit, then Elf
   case `0` plus Chimp's `xor == 0` flag), because chimp has to carry an ELF case marker
   that gorilla has no equivalent of. Closing that last 2x needs run-length encoding —
   measured at ~5x on flat data, but it costs 6–11% on decode for every other workload,
   which is the wrong trade for an encoding chosen for its decode (§5.2). Flatlined
   gauges are not an exotic workload. Dropping gorilla costs 3.1%, the largest
   single-drop penalty after chimp.
2. **Compatibility and migration.** `parse_encoding` maps `"compressed"` to the default,
   which is `Gorilla`; `DEFAULT_CHUNK_ENCODING` is gorilla; it is the
   RedisTimeSeries-compatible name and the encoding in every existing RDB. Removing it
   is a migration event in a way that removing the other three is not.

It is also the cheapest compressed encoder (106x bulk, 10.3x append), which makes it
the right default for append-heavy ingestion even in scenarios where chimp reads better.

### 5.4 Remove `DeXor` — the close call, and the one to re-check

DeXor has the second-best geomean (3.18) and wins three scenarios outright:
`periodic_q2` 7.08 vs. chimp 5.91 (1.20x), `counter` 10.16 vs. gorilla 9.05 (1.12x),
`noisy_q2` 4.83 vs. chimp 4.37 (1.11x). Those are real wins, not noise.

Against that:

- **Worst decode in the entire set**: 17.6x bulk iteration, 5.3x on the wire path —
  slower than gorilla, tsxor *and* chimp at every sample count measured (§3.3, §3.4).
- **Largest module in the tree** at 2,766 lines (§3.6).
- Pareto-optimal in only 19% of wire cells; chimp beats it on bytes and decode
  simultaneously in 62% of them (§3.4).
- Within 5% of best in 14/28 scenarios against chimp's 22/28 (§3.2).
- Adding it back to chimp+gorilla recovers only **1.4 percentage points** (§4).

Verdict: remove. But the wins are concentrated on quantized (`_q2`) and counter shapes,
and they were measured against **synthetic generators**.

> **Re-check before cutting.** If the deployment is counter-dominated (Prometheus-style
> monotonic counters, quantized sensor feeds), re-run
> `tools/compression_report.sh --by-workload` against production-shaped data first.
> DeXor's case rests entirely on those shapes, and this analysis cannot see them.

### 5.5 Remove `TsXor` — dominated on every axis at once — **done**

- **812x bulk encode** — 4.9x worse than chimp, 7.7x worse than gorilla (§3.3).
- **Inflates wire payloads for all n < 128** (1.32x raw at n=16), which is precisely
  the range the fan-out path operates in (§3.4).
- Wins 1/28 storage scenarios (`discrete`, by 1.15x) and is within 5% of best in only
  1/28 — the worst coverage in the set (§3.2).
- Worst heap overhead: 2.29 heap/data vs. 2.04 for everything else (§3.5).

Its one real virtue is the cheapest decode at large n (2.79x vs. chimp's 3.17x). That
is a 12% edge, it only arrives past n≈128, and it costs a 4.9x encode penalty to buy.
Dropping tsxor costs 0.5%.

### 5.6 Remove `Xor2` — its premise does not survive the sweep

Xor2 is the worst compressor in the set by a wide margin: geomean 2.02 against chimp's
3.35, a worst case of 0.09 of best, and 509 samples per 4k chunk against chimp's 900 —
for 2,319 lines.

Its justification was "trades size for decode speed." The sweep refutes it. Xor2's
decode advantage exists only at n=16–128 (1.88x–1.90x vs. chimp's 2.61x–3.00x). By
n>=400 it is *slower* than chimp on the wire path (3.13x vs. 3.09x) and slower in the
1000-sample latency run (22.35 µs vs. 20.02 µs). So xor2 is faster **only in the window
where compression is worthless and `Uncompressed` is the correct answer anyway.**

Dropping xor2 costs 0.1% — the cheapest removal available.

---

## 6. Migration cost

Removing variants is not free. Three places carry the encoding identity, and each
needs deliberate handling.

> **How the `TsXor` removal handled these.** No `TsXor` implementation was ever
> deployed, so no migration path was built: discriminant `3` is simply retired (2),
> RDB files naming `tsxor` are not readable and are not expected to exist (2), and
> protobuf tag `2` is `reserved` (3). A future `Xor2`/`DeXor` cut cannot assume the
> same and still needs the handling below.

1. **`src/series/chunks/chunk.rs:27-35`** — the discriminants are load-bearing for RDB
   via `TryFrom<u8>`. Keep the numbering exactly as it is and make removed variants
   return `INVALID_CHUNK_ENCODING`. **Do not renumber `Chimp` to close the gap.**

2. **`src/series/serialization.rs:46`** — RDB loads the encoding **by name**, so an
   existing RDB containing `tsxor`, `xor2` or `dexor` series will fail to load. This
   needs a real migration path: either a read-only shim that loads the old format and
   re-encodes to chimp on load, or a documented major-version break. This is the
   largest single cost of the cut.

3. **`src/commands/fanout.response.proto:15-22`** — `CompressionType` is a protobuf
   enum. Reserve the removed tags (`reserved 2, 3, 4;`) rather than deleting them, so
   that a rolling cluster upgrade in which an old shard still emits `DEXOR` fails
   loudly at the conversion in `src/commands/fanout/conversions.rs:75-81` instead of
   silently mis-decoding.

Also touched: `tests/test_ts_encoding.py`, `tests/test_ts_create.py`,
`tests/test_ts_save_and_restore.py`, `src/tests/chunk_utils.rs`,
`src/series/chunks/proptest_roundtrip.rs`, the three `benches/*.rs`, and
`benches/baselines/compression_baseline.csv` (which must be regenerated with
`tools/compression_report.sh --save-baseline`).

---

## 7. Net effect

| | before | after |
|---|---:|---:|
| encodings | 6 | 3 |
| encoding-tree lines | ~11,300 | ~5,100 |
| achievable compression (geomean of per-scenario best) | 3.709 | 3.633 (−2.0%) |

`stream/` stays (chimp and gorilla both depend on it); only `elf64.rs` is
chimp-exclusive and it stays too.

The honest summary: five compressed encodings were carrying **2.0%** of compression
between them beyond what chimp and gorilla already provide, and the two encodings doing
the least work are the two largest modules in the tree.
