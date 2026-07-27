# Chunk Encoding Consolidation — Measurement and Rationale

**Status:** `Xor2` has been removed (§5.5) — chunk discriminants and protobuf tags were
renumbered contiguously rather than left as a gap, so RDBs and fan-out payloads written
before the cut are not readable. `DeXor` is still present; this document records what the
measurements say, so that a decision to trim it is made against data rather than
intuition.

**Question asked:** if the five chunk encodings had to be reduced to a smaller set,
which should survive and why?

**Answer:** keep `Uncompressed`, `Gorilla` and `Chimp`; remove `Xor2` and `DeXor`. That
cut gives up **1.5%** of the compression achievable with all four compressed encodings,
and removes roughly 5,100 lines. `DeXor` is the one genuinely close call — see §5.4.

---

## 1. How the numbers were produced

All three report tools in `tools/` were run over the full matrix. They are documented
in `AGENTS.md` under Benchmarks; none of them is a criterion bench.

```sh
tools/compression_report.sh --by-workload ratio     # 140 rows: 5 encodings x 28 scenarios
tools/latency_report.sh --workloads all --ts-models all --samples 1000   # 180 rows
tools/wire_report.sh --workloads all --ts-models all                     # 5040 rows
```

Outputs land in `target/bench-reports/{compression,latency,wire}.{csv,md}`.

Measurements below were taken on an **Apple M2, 8 cores, rustc 1.92.0**, on 2026-07-26,
release profile with `--features enable-system-alloc,test-utils`. Timings are wall-clock
medians and are machine- and load-dependent: **compare rows within one run, never
absolute numbers across machines.** The size and ratio figures are deterministic and do
reproduce exactly.

One encoder change since the original version of this analysis is already folded into
every table below, so no figure here needs mental adjustment for it: **the chimp repeat
path**. A value whose bit pattern matches its predecessor bypasses the ELF layer and is
written as Elf case `0` plus Chimp's `xor == 0` flag — 3 value bits, where it previously
fell through to the raw `10` marker for 4. This lifted chimp's geomean ratio to 3.35 and
`constant` to 31.87, and narrowed but did not close the case for gorilla (§5.3).

An earlier round of this analysis also cut one encoding outright, on the strength of the
same six axes. Every table below is a fresh run against the tree as it stands, so that
encoding appears in none of them.

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

**All five encodings passed: 0 non-lossless rows out of 5,040.**

No encoding is disqualified here, so the decision rests entirely on axes 2–6.

### 3.2 Compression at chunk capacity (axis 2)

28 scenarios = workload x timestamp model x chunk size (1k / 4k / 64k).
Ratio is `(len * 16) / data_size`; higher is better.

| encoding | geomean ratio | scenarios won | within 5% of best | worst case vs. best |
|---|---:|---:|---:|---:|
| chimp | **3.35** | **16/28** | **22/28** | **0.50** |
| dexor | 3.18 | 3/28 | 14/28 | 0.36 |
| gorilla | 3.03 | 8/28 | 14/28 | 0.27 |
| xor2 | 2.02 | 1/28 | 3/28 | 0.09 |

"Worst case vs. best" is the floor: the lowest ratio this encoding achieves as a
fraction of the best encoding in that same scenario. Chimp has the shallowest floor,
meaning it is the safest single choice when the workload is unknown.

Every scenario's winner and its margin over the runner-up:

| scenario | winner | ratio | runner-up | ratio | margin |
|---|---|---:|---|---:|---:|
| drift_q2/regular/4k | chimp | 10.87 | dexor | 3.89 | **2.79x** |
| bursty_q2/regular/4k | chimp | 10.89 | dexor | 4.91 | **2.22x** |
| constant/regular/4k | gorilla | 63.74 | dexor | 42.56 | **1.50x** |
| constant_int/regular/4k | gorilla | 63.74 | dexor | 42.55 | **1.50x** |
| discrete/regular/4k | gorilla | 12.27 | dexor | 9.06 | **1.35x** |
| periodic_q2/regular/4k | dexor | 7.08 | chimp | 5.91 | 1.20x |
| drift/jitter/4k | chimp | 2.27 | gorilla | 1.98 | 1.15x |
| counter/regular/4k | dexor | 10.16 | gorilla | 9.05 | 1.12x |
| drift/irregular/4k | chimp | 1.96 | dexor | 1.76 | 1.11x |
| drift/jitter/1k | chimp | 2.27 | gorilla | 2.04 | 1.11x |
| drift/jitter/64k | chimp | 2.27 | dexor | 2.05 | 1.11x |
| noisy_q2/regular/4k | dexor | 4.83 | chimp | 4.37 | 1.11x |
| drift/irregular/64k | chimp | 1.97 | dexor | 1.82 | 1.09x |
| noisy/jitter/64k | chimp | 1.89 | gorilla | 1.81 | 1.05x |
| drift/irregular/1k | chimp | 1.97 | xor2 | 1.89 | 1.04x |
| noisy/irregular/1k | xor2 | 1.76 | chimp | 1.69 | 1.04x |
| noisy/jitter/1k | chimp | 1.87 | gorilla | 1.81 | 1.04x |
| bursty/regular/4k | chimp | 2.66 | dexor | 2.57 | 1.04x |
| noisy/irregular/64k | chimp | 1.68 | gorilla | 1.63 | 1.04x |
| noisy/jitter/4k | chimp | 1.89 | gorilla | 1.82 | 1.03x |
| noisy/regular/1k | gorilla | 2.22 | dexor | 2.16 | 1.03x |
| drift/regular/64k | chimp | 2.67 | dexor | 2.61 | 1.02x |
| noisy/regular/4k | gorilla | 2.23 | dexor | 2.19 | 1.02x |
| noisy/irregular/4k | chimp | 1.67 | gorilla | 1.64 | 1.02x |
| drift/regular/1k | gorilla | 2.68 | chimp | 2.65 | 1.01x |
| noisy/regular/64k | gorilla | 2.21 | dexor | 2.19 | 1.01x |
| periodic/regular/4k | chimp | 2.25 | dexor | 2.23 | 1.01x |
| drift/regular/4k | gorilla | 2.68 | chimp | 2.66 | 1.01x |

Note the shape of this table: five scenarios are decided by a wide margin and the other
23 are all under 1.2x. Outside the quantized, constant and discrete shapes the encodings
are far more alike than the count of them suggests.

### 3.3 Speed (axis 3)

`latency_report`, 1000 samples, medians across all workloads and timestamp models,
expressed as a multiple of `uncompressed`. Lower is better.

| encoding | encode bulk | encode append | decode iter | get_range | scan mid 10% |
|---|---:|---:|---:|---:|---:|
| uncompressed | 1.00x | 1.00x | 1.00x | 1.00x | 1.00x |
| xor2 | **85x** | **8.9x** | 11.6x | 93x | 28.6x |
| gorilla | 106x | 11.5x | 13.6x | 108x | 31.6x |
| chimp | 173x | 17.7x | **10.4x** | **83x** | **23.5x** |
| dexor | 182x | 18.4x | 17.9x | 142x | 40.1x |

The headline: **chimp has the best decode of the practical encodings while gorilla has
the best compressed encode** among those with competitive ratios.

### 3.4 Fan-out wire payload (axis 4)

`wire_report`, sweeping `n` from 1 to 8000. Payload bytes as a fraction of the raw
16-bytes-per-sample size, median over 36 workload shapes. Lower is better; values
above 1.00 mean the "compressed" payload is **larger** than the raw samples.

| n | gorilla | xor2 | dexor | chimp |
|---:|---:|---:|---:|---:|
| 1 | **2.75** | 4.12 | 3.19 | 3.94 |
| 3 | **1.25** | 1.72 | 1.34 | 1.42 |
| 8 | 0.71 | 0.88 | **0.64** | 0.68 |
| 12 | 0.61 | 0.73 | **0.52** | 0.55 |
| 16 | 0.55 | 0.64 | **0.46** | 0.47 |
| 30 | 0.49 | 0.54 | **0.37** | **0.37** |
| 64 | 0.44 | 0.46 | 0.32 | **0.30** |
| 128 | 0.42 | 0.43 | 0.33 | **0.28** |
| 256 | 0.42 | 0.49 | 0.31 | **0.25** |
| 1000 | 0.41 | 0.53 | 0.30 | **0.24** |
| 8000 | 0.43 | 0.57 | 0.30 | **0.23** |

This table is the clearest single view in the whole analysis. It shows why
`WIRE_COMPRESSION_MIN_SAMPLES = 16` exists (everything inflates at small `n` — at n=1
every encoding produces a payload 2.7x–4.1x *larger* than the raw samples), that dexor
produces the smallest payload of any encoding across n=5–25 — including at the n=16
threshold itself — and that chimp takes over from n=30 and is the asymptotic winner.

Decode cost on the same path, as a multiple of uncompressed:

| n | gorilla | xor2 | dexor | chimp |
|---:|---:|---:|---:|---:|
| 16 | 3.25 | **2.49** | 4.24 | 3.25 |
| 64 | 3.58 | **1.96** | 4.50 | 3.00 |
| 256 | 3.95 | **2.77** | 4.93 | 3.01 |
| 1000 | 4.10 | 3.54 | 5.14 | **3.13** |
| 8000 | 4.11 | 3.67 | 5.35 | **3.17** |

Pareto-optimality on the joint `(wire_bytes, decode_us)` objective, across the 756
cells with n >= 16 (i.e. how often an encoding is not beaten on *both* axes at once):

| encoding | Pareto-optimal | smallest payload |
|---|---:|---:|
| chimp | **83%** | **51%** |
| xor2 | 67% | 4% |
| gorilla | 49% | 26% |
| dexor | 19% | 20% |

Read xor2's 67% against its 4%: it sits on the frontier almost entirely because it is
the fastest decoder at small `n`, not because it ever produces a small payload.

Fraction of those cells where chimp beats the encoding on bytes **and** decode
simultaneously: dexor 62%, gorilla 47%, xor2 30%.

### 3.5 Memory footprint (axis 5)

From `compression.csv` at 4k chunk size, medians. `capacity_4k` is samples that fit in
a 4k chunk — higher is better. Note the `size` column is the `get_size()` heap
footprint (buffer *capacity*, which doubles) for gorilla/dexor/chimp, so `heap/data` is
a fair comparison only within that group.

| encoding | data_size | heap size | heap/data | samples per 4k chunk |
|---|---:|---:|---:|---:|
| chimp | 4098 | 8368 | 2.04 | **900** |
| dexor | 4098 | 8368 | 2.04 | 831 |
| gorilla | 4098 | 8368 | 2.04 | 686 |
| xor2 | 4099 | 8368 | 2.04 | 509 |
| uncompressed | 4096 | 4272 | 1.04 | 256 |

Chimp fits 31% more samples per chunk than gorilla and 77% more than xor2.

### 3.6 Maintenance surface (axis 6)

Lines of Rust including tests. "Of which tests" counts from each file's first
`#[cfg(test)]` to EOF, so it is an upper bound on test code and is only comparable
between modules that follow the same layout.

| module | lines | of which tests | `#[test]` / `proptest!` |
|---|---:|---:|---:|
| dexor | **2766** | 286 | 41 |
| xor2 | **2319** | 764 | 19 |
| chimp | 2088 (+178 `elf64.rs`) | 340 | 39 |
| uncompressed | 1456 | 955 | 10 |
| gorilla | 1338 | 341 | 11 |
| `stream/` (shared) | 1332 | 303 | 17 |

`stream/` is used by chimp, gorilla, xor2 and dexor, so it survives any cut that keeps
chimp or gorilla. `elf64.rs` is chimp-exclusive.

The two largest modules, dexor and xor2, are also the two proposed for removal — 5,085
lines, or roughly 44% of the encoding tree.

---

## 4. The decisive calculation

Compression ratio per encoding is the wrong question when choosing a *set*. The right
one is: for each scenario, what is the best ratio still available after the cut? The
table below is the geomean of the per-scenario maximum over the surviving encodings.

| keep set | geomean | loss vs. all four |
|---|---:|---:|
| all four | 3.690 | — |
| chimp + gorilla + dexor | 3.685 | −0.1% |
| chimp + gorilla + xor2 | 3.638 | −1.4% |
| **chimp + gorilla** | **3.633** | **−1.5%** |
| chimp + dexor | 3.533 | −4.3% |
| chimp + xor2 | 3.351 | −9.2% |
| chimp alone | 3.346 | −9.3% |
| gorilla + dexor | 3.341 | −9.5% |

Single-encoding removals from the full set:

| drop | geomean | loss |
|---|---:|---:|
| xor2 | 3.685 | −0.1% |
| dexor | 3.638 | −1.4% |
| gorilla | 3.538 | −4.1% |
| chimp | 3.359 | −9.0% |

Two readings matter here. First, **chimp and gorilla are the only two encodings whose
removal costs more than 1.5%** — they are the load-bearing pair, and the gap between
them and the other two is now nearly threefold. Second, going from chimp+gorilla down to
chimp alone costs 7.8 percentage points, almost all of it from the `constant` workload;
that is the entire argument for keeping gorilla.

---

## 5. Decisions

### 5.1 Keep `Uncompressed` — not a compression decision

It is the only correct encoding below the wire threshold. At n=1 every compressed
format produces a payload 2.7x–4.1x *larger* than the raw samples, and none of them
breaks even before n≈5 (see §3.4). It is the fallback in `samples_to_chunk_lossless`,
it is ~4x cheaper to append to than anything else, and `UNCOMPRESSED` is the
RedisTimeSeries-compatible name. Removing it is not on the table.

### 5.2 Keep `Chimp` — dominant on the axes that matter most

- Best geomean ratio (3.35) and best in 16/28 storage scenarios (§3.2).
- Within 5% of the best in 22/28 scenarios — no other encoding exceeds 14 (§3.2).
- Best decode of the practical encodings: 10.4x vs. gorilla's 13.6x (§3.3).
- Pareto-optimal in 83% of wire cells and smallest payload in 51% (§3.4).
- Fits the most samples per 4k chunk: 900 vs. gorilla's 686 (§3.5).
- Shallowest worst case: 0.50 of best, so it is the safest default when the workload
  shape is unknown.

The cost is a 173x bulk encode, and that trade is the right way round: encode happens
once per sample and, on the fan-out path, in parallel across shards; decode happens on
every read and, on that same path, serially in the coordinator. It is already the wire
encoding for exactly this reason (see `samples_to_chunk` in
`src/series/chunks/serialization.rs`).

### 5.3 Keep `Gorilla` — the one thing chimp still cannot absorb

Two independent reasons:

1. **Flat and low-entropy series.** Gorilla reaches 63.74x on `constant` and
   `constant_int` against chimp's 31.87x — a 1.50x margin over the runner-up — and it
   also takes `discrete` at 12.27 (1.35x over dexor, with chimp further back). Both
   encodings now have a repeated-value path; gorilla's is simply cheaper. A repeat costs
   gorilla 2 bits (one timestamp bit, one value control bit) against chimp's 4 (one
   timestamp bit, then Elf case `0` plus Chimp's `xor == 0` flag), because chimp has to
   carry an ELF case marker that gorilla has no equivalent of. Closing that last 2x needs
   run-length encoding — measured at ~5x on flat data, but it costs 6–11% on decode for
   every other workload, which is the wrong trade for an encoding chosen for its decode
   (§5.2). Flatlined gauges and small-alphabet gauges are not exotic workloads. Dropping
   gorilla costs 4.1%, the largest single-drop penalty after chimp.
2. **Compatibility and migration.** `parse_encoding` maps `"compressed"` to the default,
   which is `Gorilla`; `DEFAULT_CHUNK_ENCODING` is gorilla; it is the
   RedisTimeSeries-compatible name and the encoding in every existing RDB. Removing it
   is a migration event in a way that removing the other two is not.

It is also the cheapest compressed encoder with a competitive ratio (106x bulk, 11.5x
append), which makes it the right default for append-heavy ingestion even in scenarios
where chimp reads better.

### 5.4 Remove `DeXor` — the close call, and the one to re-check

DeXor has the second-best geomean (3.18) and wins three scenarios outright:
`periodic_q2` 7.08 vs. chimp 5.91 (1.20x), `counter` 10.16 vs. gorilla 9.05 (1.12x),
`noisy_q2` 4.83 vs. chimp 4.37 (1.11x). Those are real wins, not noise. It also produces
the smallest wire payload of any encoding from n=5 to n=25 — the first stretch *above*
`WIRE_COMPRESSION_MIN_SAMPLES = 16`, not merely below it — before chimp takes over at
n=30.

Against that:

- **Worst decode in the entire set**: 17.9x bulk iteration, 5.35x on the wire path —
  slower than gorilla *and* chimp at every sample count measured (§3.3, §3.4).
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

### 5.5 Remove `Xor2` — its premise does not survive the sweep

Xor2 is the worst compressor in the set by a wide margin: geomean 2.02 against chimp's
3.35, a worst case of 0.09 of best, and 509 samples per 4k chunk against chimp's 900 —
for 2,319 lines.

Its justification was "trades size for decode speed." The sweep refutes it. Xor2's
decode advantage exists only up to n≈320 (1.86x–2.77x vs. chimp's 2.98x–3.09x); at
n=400 chimp overtakes it (3.03x vs. 3.13x) and the gap widens from there, reaching
3.17x vs. 3.67x at n=8000. It is also slower than chimp in the 1000-sample latency run
(11.6x vs. 10.4x decode iter). So xor2 is faster **only in the window where compression
is worthless and `Uncompressed` is the correct answer anyway.**

Dropping xor2 costs 0.1% — the cheapest removal available.

---

## 6. Migration cost

Removing variants is not free. Three places carry the encoding identity, and each
needs deliberate handling.

1. **`src/series/chunks/chunk.rs`** — the discriminants are load-bearing for RDB via
   `TryFrom<u8>`. Dropping a variant from the middle forces a choice: leave a gap (every
   surviving byte keeps its meaning, old RDBs stay readable for the encodings that
   remain) or renumber to stay contiguous (every RDB written before the cut is silently
   misread, because the bytes shift under it). The `Xor2` cut renumbered: the enum is
   contiguous over `1..=4`, and bytes written before the cut now decode to the wrong
   encoding. That trade needs an RDB version bump that rejects the old layout outright.

2. **`src/series/serialization.rs:46`** — RDB loads the encoding **by name**, so an
   existing RDB containing `xor2` or `dexor` series will fail to load. This needs a real
   migration path: either a read-only shim that loads the old format and re-encodes to
   chimp on load, or a documented major-version break. This is the largest single cost
   of the cut.

3. **`src/commands/fanout.response.proto`** — `CompressionType` is a protobuf enum,
   contiguous over `0..=3` after the `Xor2` cut. The removed tag was closed up rather
   than reserved, so a rolling cluster upgrade in which an old shard still emits the
   pre-cut tags mis-decodes silently instead of failing at the conversion in
   `src/commands/fanout/conversions.rs`. Removing `DeXor` too would shift `CHIMP` again;
   pair either cut with a wire-version guard.

> **Why the earlier cut is not a precedent.** The encoding removed in the previous round
> had no deployed implementations, so it took none of the handling above: the remaining
> discriminants and protobuf tags were closed up to stay contiguous, and no RDB shim was
> written. `Gorilla`-era RDBs and shipped `DeXor` series make that shortcut unavailable
> here — the `Xor2` renumber misreads every RDB and every in-flight fan-out response
> written before the cut, and a `DeXor` cut on the same terms would do it again.

Also touched: `tests/test_ts_encoding.py`, `tests/test_ts_create.py`,
`tests/test_ts_save_and_restore.py`, `src/tests/chunk_utils.rs`,
`src/series/chunks/proptest_roundtrip.rs`, the three `benches/*.rs`, and
`benches/baselines/compression_baseline.csv` (which must be regenerated with
`tools/compression_report.sh --save-baseline`).

---

## 7. Net effect

| | before | after |
|---|---:|---:|
| encodings | 5 | 3 |
| encoding-tree lines | ~11,500 | ~6,400 |
| achievable compression (geomean of per-scenario best) | 3.690 | 3.633 (−1.5%) |

`stream/` stays (chimp and gorilla both depend on it); only `elf64.rs` is
chimp-exclusive and it stays too.

The honest summary: four compressed encodings were carrying **1.5%** of compression
between them beyond what chimp and gorilla already provide, and the two encodings doing
the least work are the two largest modules in the tree.
