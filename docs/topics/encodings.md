# Chunk Encoding Guide

This document describes the three time-series chunk encodings available in
valkey-timeseries, their algorithms, performance characteristics, and a selection
rubric for choosing the right encoding based on your data.

---

## Overview

| Encoding | Discriminant | Origin | Compression | Best For |
|---|---|---|---|---|
| **Uncompressed** | 1 | — | None (16 B/sample) | High-throughput ingest, heavy backfill, very short series |
| **Gorilla** | 2 | Facebook Gorilla (2015) | XOR + delta-of-delta | Repetitive data (constants, flags, discrete sets, integer counters); fastest encode |
| **Chimp** | 3 (default) | Chimp + ELF| ELF erasure + Chimp XOR | Decimal-quantized data, unknown workloads; fastest decode |

The default encoding is **Chimp**, but it can be changed at runtime via the
`ts-encoding` Valkey config parameter (`CHIMP`, `GORILLA`, or `UNCOMPRESSED`).
The alias `COMPRESSED` maps to the current default (Chimp).

The default chunk size is **4,096 bytes** (configurable via `ts-chunk-size`,
range 64 B – 1 MiB, must be a multiple of 8).

---

## Encoding Details

### 1. Uncompressed

**Algorithm:** None. Each sample is stored as a raw 16-byte pair
(`i64` timestamp + `f64` value) in a flat `Vec<Sample>`.

**Key constants:**
- 16 bytes/sample always — constant, not amortized
- Capacity is `chunk_size / 16` samples (`max_elements`), so a 4 KiB chunk holds
  256 and a 64 KiB chunk holds 4,096. 
- Binary search for timestamp lookups

**Strengths:**
- Zero CPU overhead for encode and decode — the only encoding whose cost does
  not scale with value entropy
- O(log n) random access via binary search
- No compression artifacts or precision loss
- Predictable memory footprint, and the only encoding that reports an *exact*
  rather than amortized `bytes per sample`
- Mutations are a `Vec` insert. Both compressed encodings must decode and
  re-encode the entire chunk on an out-of-order upsert (see
  [Mutation cost](#mutation-cost)), so this is the only encoding where backfill
  is cheap

**Weaknesses:**
- 16× larger on disk than the typical compressed representation
- Fits 2–32× fewer samples per chunk than a compressed encoding, so a
  fixed chunk budget covers a much shorter time span

**When to use:**
- Data that is entirely random (cryptographic, high-entropy sensor noise) where
  no compressor finds a pattern
- Workloads where ingest throughput is the only bottleneck and storage is
  abundant
- Short-lived series where compression overhead isn't amortized
- Debugging or benchmarking — uncompressed serves as the baseline for all
  compression comparisons

---

### 2. Gorilla

**Algorithm:** Two-stage compression based on the Facebook Gorilla paper
(Pelkonen et al., 2015).

**Timestamp encoding — delta-of-delta:**
1. Sample 0: full 64-bit timestamp as varint
2. Sample 1: delta from previous as uvarint
3. Samples 2+: delta-of-delta through the Prometheus varbit buckets, sized for the deltas seen in
   practice. Regular intervals cost **1 bit** (`0`); everything else costs
   prefix + payload:

   | dod range | total bits |
   |---|---|
   | `0` | 1 |
   | ±3 | 7 |
   | ±31 | 12 |
   | ±255 | 17 |
   | ±2,047 | 22 |
   | ±131,071 | 30 |
   | ±16,777,215 | 39 |
   | ±3.6e16 | 64 |
   | anything else | 72 |

**Value encoding — XOR with leading/trailing zero optimization:**
1. Sample 0: raw 64-bit IEEE 754 value
2. Compute `xor = current ^ previous`
3. If `xor == 0` (unchanged): **1 bit** — the cheapest repeat of any encoding here
4. If the non-zero window fits within the previous leading/trailing zero bounds:
   2 control bits + the significant bits
5. Otherwise: 2 control bits + new leading zeros (5 bits) + significant count
   (6 bits) + the significant bits

A repeated value therefore costs **2 bits total** per sample (1 for the
delta-of-delta, 1 for the XOR), which is what produces the 64× ratio on
constant series.

**Key characteristics:**
- Sequential/causal: each point compresses using only the previous point

**Strengths:**
- The fastest compressed encoder — 1.67× faster bulk encode and 1.74× faster
  append than Chimp
- Unbeatable on repetition: constants (0.25 B/sample, 64×), discrete sets
  (12×) and counters (9×) are all Gorilla wins, by 1.34–2.00× over Chimp
- Delta-of-delta timestamps collapse regular intervals to 1 bit
- Mature, well-understood algorithm with predictable behavior

**Weaknesses:**
- ~18% slower to decode than Chimp, and ~21% slower to scan
- No special handling for decimal precision — floating-point noise in the low
  mantissa bits consumes bits. This is the single largest compression gap
  against Chimp (up to 3.08×) and it fires on most agent-reported telemetry
- Lower geometric-mean ratio (3.03 vs 3.35) and a deeper worst case: in its
  weakest scenario Gorilla achieves only 0.32 of the best available ratio,
  against Chimp's 0.50

**When to use:**
- Write-heavy ingest where encode cost dominates
- Series dominated by repetition: constants, flags, discrete/enumerated values,
  integer counters
- Values that are integral or full-entropy `f64` — anything where there is no
  fixed decimal precision for ELF to exploit
- Data with regular or near-regular, strictly ascending timestamps

---

### 3. Chimp (ELF-on-Chimp, Default)

**Algorithm:** A two-layer codec combining ELF (Erasing-based Lossless
Floating-point) erasure with Chimp XOR compression.

```
Sample (timestamp, f64 value)
          │
          ▼
┌──────────────────────┐
│   ELF Erasing Layer  │  ← Erases recoverable low mantissa bits
└──────────┬───────────┘
           │ (possibly erased u64 + case marker)
           ▼
┌──────────────────────┐
│   Chimp XOR Codec    │  ← Compresses the bit pattern
└──────────┬───────────┘
           │
           ▼
      BitStream
```

**Layer 1 — ELF Erasure:**

For each value, ELF determines whether low mantissa bits can be safely erased. For each 
value it computes the decimal significant-digit count, erases the low mantissa bits that 
carry no decimal information, and emits a 1- or 6-bit case marker so the decoder can reverse them.
Erasure only fires when it's provably reversible.

In practice, this means values like `3.14` (stored as `3.140000000000000124...`)
can erase most of the spurious low bits because the original precision is known
to be 2 decimal places. The erased bits were never real information — they were
IEEE 754 representation artifacts.

**Layer 2 — Chimp XOR Compression:**

Chimp compresses the (possibly erased) bit pattern using a 2-bit flag scheme:

| Flag | Meaning |
|---|---|
| `00` | XOR is zero (value unchanged) — 2 bits |
| `01` | New window with ≥6 trailing zeros — 2 + 3 + 6 + sig bits |
| `10` | Same leading zeros as last — 2 + sig bits |
| `11` | New leading zero count — 2 + 3 + sig bits |

Chimp uses **rounded** 3-bit leading-zero buckets (vs Gorilla's exact 5-bit
count) and a 2-bit fixed-width flag (vs Gorilla's variable-length control
bits). This makes Chimp decode ~18% faster but encode ~1.67× slower than
Gorilla.

**Repeated values** bypass ELF entirely and are written as ELF case `0` plus
Chimp's `xor == 0` flag — **3 value bits**, or 4 bits per sample once the
delta-of-delta bit is counted. That is double Gorilla's 2, and it is the whole
reason Gorilla wins the constant and discrete workloads.

Reserving that pairing for repeats is not free: a *distinct* value can erase
onto the bit pattern Chimp already holds, and the encoder must push those onto
the raw path so they cannot be read back as a repeat.

**Timestamp encoding:** Delta-of-delta with zigzag encoding and adaptive prefix
lengths — 1 bit for a regular interval, then 9/12/16-bit forms, then a 37- or
69-bit escape. Because the delta-of-delta is zigzagged and signed, **descending
timestamps round-trip**, where Gorilla's encoder refuses them.

**Key characteristics:**
- Two-layer design: ELF erasure runs first, Chimp compresses the result

**Strengths:**
- **Dramatically better on quantized data.** Values recorded to a fixed number
  of decimal places (e.g., `12.34`, `99.9`, `0.001`) compress 2–3× better than
  Gorilla because ELF erases the spurious low mantissa bits
- ~18% faster decode and ~21% faster scan than Gorilla
- 2-bit fixed-width control flags enable efficient branch prediction
- **The best choice when the workload is unknown.** Highest geometric-mean
  ratio (3.35 vs Gorilla's 3.03), most scenarios won (19/28), most scenarios
  within 5% of best (24/28), and the shallowest floor (0.50 of best in its
  worst scenario, against Gorilla's 0.32)
- Accepts every `f64` including NaN and ±inf: NaNs bypass ELF erasure and are
  stored as raw bit patterns, so payload bits such as a Prometheus stale marker
  survive the round trip unchanged

**Weaknesses:**
- ~1.67× slower bulk encode and ~1.74× slower append than Gorilla (ELF analysis
  on top of Chimp encoding)
- Repetition costs double: 4 bits per sample against Gorilla's 2, which loses
  the constant workload by 2.00× and discrete by 1.67×
- On unquantized floating-point data with random low mantissa bits, Chimp is a
  near-tie with Gorilla — ELF finds nothing to erase and the rounded
  leading-zero encoding is slightly less precise

**When to use:**
- **Data recorded with fixed decimal precision** — currency, percentages,
  sensor readings at known resolution. This is the highest-value rule in this
  document; see [Quantization matters](#quantization-matters)
- Any series whose shape you do not know in advance
- Metrics scraped from systems that round to 2–4 decimal digits

---

## Selection Rubric

### Rule 0: quantization dominates everything else

Before consulting any table below, answer one question: **are the values
reported to a fixed, small number of decimal places?** Percentages,
temperatures, currency, latencies in milliseconds — anything emitted by a
collection agent that rounds — all qualify.

If yes, choose **Chimp** and stop. It is worth 1.95–3.08× over Gorilla, which
is larger than every other effect in this document combined. No other single
property of the data comes close to mattering as much.

If no, continue.

### By Data Type

Winners below are the measured winner in `compression_baseline.csv` at 4 KiB
with regular timestamps, not estimates. "Margin" is over the runner-up.

| Data Pattern | Real-World Examples | Best Encoding | Margin | Why |
|---|---|---|---|---|
| **Quantized decimals** | Agent-reported CPU %, temperatures, latency at 2 decimal places | **Chimp** | **1.95–3.08×** | ELF erases the IEEE 754 artifacts Gorilla must carry |
| **Constant / rarely changing** | Config values, feature flags, idle gauges, thermostat setpoints | **Gorilla** | **2.00×** | 2 bits per repeated sample vs Chimp's 4; 64× compression |
| **Discrete / low-cardinality** | HTTP status codes, enum values, small integer sets | **Gorilla** | **1.67×** | Many XOR repeats; 12× compression |
| **Monotonic integer counters** | `http_requests_total`, `bytes_sent`, queue depths | **Gorilla** | **1.34×** | Integral values give ELF nothing to erase; its markers are pure overhead |
| **Periodic / seasonal** | Daily traffic patterns, hourly cron job metrics | **Chimp** | 1.18× | Sine/sawtooth values retain enough decimal structure for ELF |
| **Bursty / spikey** | Error rates, alert counts, intermittent metrics | **Chimp** | 1.08× | Quiet stretches erase well; bursts fall back to raw |
| **Smooth floating-point gauges** | Full-precision drift, computed averages | **either** | ~1.00× | Near-exact tie (2.66 vs 2.68); decide on encode/decode cost instead |
| **High-entropy / noisy** | Wide-spread sensor noise, ML model outputs | **either** | ~1.04× | Still compresses **2.15×** — do *not* reach for Uncompressed here |
| **Genuinely random bits** | Cryptographic values, hash outputs | **Uncompressed** | — | No pattern exists; avoid paying encode cost for nothing |

Note the shape of this table: outside the quantized, constant and discrete
shapes, the two compressed encodings are far more alike than having two of them
suggests. 24 of 28 measured scenarios put Chimp within 5% of the best available
ratio, and 16 of 28 do the same for Gorilla.

### By Operational Concern

| Concern | Recommendation |
|---|---|
| **Maximize ingest throughput** | **Uncompressed** — ~110× cheaper bulk encode than Gorilla, ~185× than Chimp |
| **Minimize storage, shape known** | Per the data-type table above |
| **Minimize storage, shape unknown** | **Chimp** — best geomean (3.35) and shallowest worst case (0.50 of best) |
| **Minimize decode / scan latency** | **Chimp** — ~18% faster decode, ~21% faster scan than Gorilla |
| **Minimize encode latency** | **Gorilla** — 1.67× faster bulk, 1.74× faster append than Chimp |
| **Predictable memory** | **Uncompressed** — exactly 16 B/sample, `chunk_size / 16` samples |
| **Best balance (default)** | **Chimp** — the shipped default; see [On the default](#on-the-default) |

### Timestamp regularity

Irregular arrival costs both compressed encodings roughly the same and **never
flips the winner**, so it is not a selection criterion — but it does change what
you should expect:

| Timestamp model | Gorilla | Chimp |
|---|---|---|
| Regular (fixed interval) | 2.68 | 2.65 |
| Jitter (±50 ms) | 1.98 | 2.27 |
| Irregular (exponential gaps) | 1.76 | 1.96 |

(`drift` workload, 4 KiB chunks.) Chimp degrades slightly more gracefully;
budget for roughly a 30% ratio loss going from regular to irregular arrival.

### Quantization Matters

The single biggest factor in choosing Gorilla vs Chimp is whether your values
have a known decimal precision. IEEE 754 `f64` representation introduces
spurious low bits that no XOR-based compressor can exploit — but ELF can erase
them.

**Example:** The value `12.34` is stored as `12.339999999999999857...` in IEEE
754. Gorilla sees random-looking low bits in the XOR delta and encodes them all
(~5–6 bytes). Chimp recognizes that the original had 2 decimal significant
digits, erases the spurious bits, and encodes only the meaningful ones (~1.5
bytes).

This effect is visible in the benchmark data for `_q2` (2-decimal quantized)
workloads:

| Workload | Gorilla (B/sample) | Chimp (B/sample) | Chimp Advantage |
|---|---|---|---|
| `drift_q2` | 4.29 | 1.47 | **2.9× better** |
| `noisy_q2` | 7.14 | 3.66 | **2.0× better** |
| `bursty_q2` | 4.14 | 1.47 | **2.8× better** |
| `periodic_q2` | 8.34 | 2.71 | **3.1× better** |

If your data source rounds to a known precision, **Chimp** is the clear winner.
If your data is raw `f64` with full mantissa entropy (e.g., ML model outputs,
scientific measurements), the two are a near-tie — Gorilla leads the `noisy`
workload by only 1.04× — so choose on encode/decode cost rather than size.

Note the asymmetry in risk: choosing Chimp for data that turns out to be
unquantized costs about 4%, while choosing Gorilla for data that turns out to be
quantized costs up to 208%.

---

## On the Default

The shipped default is **Chimp** (`DEFAULT_CHUNK_ENCODING` in
`src/config.rs`), and the `COMPRESSED` alias resolves to it.

The measurements support that choice for an unknown workload. Chimp wins 19 of
28 scenarios to Gorilla's 9, has the higher geometric-mean ratio (3.35 vs 3.03),
lands within 5% of the best available ratio more often (24/28 vs 16/28), and has
a shallower floor (0.50 vs 0.32) — the number that bounds how badly a blind
default can go wrong. It is also the cheaper reader on all three read paths.

The cost of that default is **encode speed**: Chimp is 1.67× slower on bulk
encode and 1.74× slower on append than Gorilla. If ingest is your binding
constraint, or your series are dominated by repetition (constants, flags,
discrete sets, integer counters), set `ts-encoding GORILLA` — Gorilla's wins are
real, but narrow in scope.

If you are choosing per-series with `TS.CREATE ... ENCODING`, keep the default
unless the data-type table above points elsewhere.

---

## Configuration

| Parameter | Valkey Config | Default |
|---|---|---|
| Encoding | `ts-encoding` | `CHIMP` |
| Chunk size | `ts-chunk-size` | `4096` (4 KiB) |
| Duplicate policy | `ts-duplicate-policy` | `BLOCK` |

Per-series override: `TS.CREATE` and `TS.ALTER` accept `ENCODING` and
`CHUNK_SIZE` arguments to override the global defaults for individual
time-series keys.

---

## Appendix: Measurements and Methodology

Every figure in this document comes from the report tools in `tools/`,
documented in `AGENTS.md` under Benchmarks. Neither is a criterion bench.

```sh
tools/compression_report.sh --check                                   #  84 rows
tools/latency_report.sh --workloads all --ts-models all --samples 1000 # 108 rows
```

Outputs land in `target/bench-reports/{compression,latency}.{csv,md}`.
The compression baseline is committed at
`benches/baselines/compression_baseline.csv`; `--check` fails the run on a
regression against it.

**Size and ratio figures are deterministic and reproduce exactly.** Timings are
wall-clock medians, machine- and load-dependent: **compare rows within one run,
never absolute numbers across machines.** The timings here were taken on an
Apple M2 in release profile with `--features enable-system-alloc,test-utils`.

### Compression summary

28 scenarios = workload × timestamp model × chunk size (1k / 4k / 64k).
Ratio is `(len * 16) / data_size`; higher is better.

| Encoding | Geomean ratio | Scenarios won | Within 5% of best | Worst case vs. best |
|---|---:|---:|---:|---:|
| **Chimp** | **3.35** | **19/28** | **24/28** | **0.50** |
| Gorilla | 3.03 | 9/28 | 16/28 | 0.32 |

"Worst case vs. best" is the floor: the lowest ratio the encoding achieves as a
fraction of the best encoding in that same scenario. It is the number that
matters when the workload is unknown, because it bounds how badly the choice can
go wrong.

### Latency summary

1,000 samples, medians across all workloads and timestamp models, expressed as a
multiple of Uncompressed. Lower is better.

| Encoding | Encode bulk | Encode append | Decode iter | `get_range` | Scan mid 10% |
|---|---:|---:|---:|---:|---:|
| Uncompressed | 1.00× | 1.00× | 1.00× | 1.00× | 1.00× |
| Gorilla | **111×** | **11.1×** | 13.8× | 117× | 82.5× |
| Chimp | 186× | 19.3× | **11.4×** | **95.7×** | **64.9×** |

Read this as: compression costs one to two orders of magnitude on encode against
a raw `Vec`, Gorilla is the cheaper encoder, and Chimp is the cheaper reader on
all three read paths.

### Workload detail (4 KiB chunks, regular timestamps)

Samples fitting in one chunk; higher is better.

| Workload | Uncompressed | Gorilla | Chimp | Winner |
|---|---:|---:|---:|---|
| `constant` / `constant_int` | 256 | **16318** | 8159 | Gorilla 2.00× |
| `discrete` | 256 | **3142** | 1887 | Gorilla 1.67× |
| `counter` | 256 | **2318** | 1730 | Gorilla 1.34× |
| `periodic_q2` | 256 | 491 | **1514** | Chimp 3.08× |
| `drift_q2` | 256 | 957 | **2782** | Chimp 2.91× |
| `bursty_q2` | 256 | 989 | **2788** | Chimp 2.82× |
| `noisy_q2` | 256 | 574 | **1118** | Chimp 1.95× |
| `periodic` | 256 | 488 | **576** | Chimp 1.18× |
| `bursty` | 256 | 629 | **682** | Chimp 1.08× |
| `noisy` | 256 | **572** | 552 | tie (1.04×) |
| `drift` | 256 | **686** | 682 | tie (1.01×) |

Workload shapes are defined in `src/tests/generators/workload.rs`. The `_q2`
variants are the same shapes rounded to 2 decimal places — "the precision most
collection agents report at".

---

## References

- **Gorilla:** Pelkonen, T., et al. "Gorilla: A Fast, Scalable, In-Memory Time
  Series Database." *PVLDB*, 2015.
- **Chimp:** Liakos, P., et al. "Chimp: Efficient Lossless Floating Point
  Compression for Time Series Databases." *PVLDB*, 2022.
- **ELF:** Li, R., et al. "ELF: Erasing-based Lossless Floating-Point
  Compression." *PVLDB*, 2023.
- Implementation credits in source: `src/series/chunks/gorilla/` (ported from
  SINTEF/rusty-chunkenc), `src/series/chunks/chimp/` (ported from
  gr.aueb.delorean.chimp + org.urbcomp.startdb.compress.elf).
