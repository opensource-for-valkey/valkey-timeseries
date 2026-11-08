# Gorilla encoder evaluation

2026-10-28. Branch: `perf/gorilla-codec-evaluation`.
Baseline: `106088e57`.

The runs used `3d2dc61c5`, a development-branch commit that is not in this repository.
`106088e57` has identical codec files, report tools and data generators, so it reproduces the
same measurements.

## Decision

Retain wider timestamp buckets, conservative XOR-window refreshes, an extended
leading-count representation, and a fused value-header decoder. The result is
primarily a compression improvement with modest aggregate read improvements;
it is **not a universal latency win**. Keep sparse seeking as an opt-in evaluator,
without adding a production cache or per-append bookkeeping.

New persistence writes use module encoding version **2**. This build reads
versions **1–2**. Older module builds reject new version 2 payloads. This applies
to every new RDB/DUMP payload and rewritten AOF, including series using other
encodings, because the version belongs to the module type. Upgrade readers
before writers; downgrade by exporting logical samples and recreating series.

**Later change:** the version bump was reverted before release. The module stays at
encoding version 1 and formats change in place until the first release, so neither
the version gate nor the version 1 read path described here applies.

## Changes retained

- **Timestamp buckets:** keep existing prefixes and payload widths, but widen
  symmetric limits to ±15, ±255, ±4,095, ±65,535, ±8,388,607 and ±2,147,483,647.
  Zero remains one bit. Exclude each payload's most-negative value so old
  readers' strict `>` sign-extension test remains compatible with these buckets.
- **Window refresh:** compare bit costs for compact previous windows (≤31 bits)
  and previous windows wider than the reader's 57-bit refill guarantee. Keep
  fitting ordinary 32–57-bit windows to avoid header churn on quantized noise.
  An extended window can still replace one to remove leading-zero padding.
  Equal-cost choices preserve the previous state.
- **Extended windows:** ordinary fresh headers stay 13 bits. The previously
  impossible combinations `leading=31, width>33` encode an extended leading count:
  width code N means leading N−1, and code zero means leading 63. Five further
  bits hold the nonzero significant width. This gives an 18-bit fresh header.
  It pays off beyond 36 leading zeros; reuse still costs two control bits plus
  the payload. Legacy and extended windows can coexist in a loaded/appended chunk.
- **Decoder:** read the ordinary leading/width fields as one 11-bit header and
  inline the helper. Preserve the existing per-bit timestamp-prefix reader.

## Measurements

Local **chunk microbenchmarks**, not server throughput or command-p99 results.
The binaries used the same release/LTO settings and
`enable-system-alloc,test-utils`; the production allocator build was validated
separately. Environment: macOS 15.3.1, arm64, Rust 1.96.0. CPU frequency and other
applications were uncontrolled. No timed run overlapped our builds or server tests.

Each round covers 12 repository workloads × 3 timestamp models, plus the same
counter increments shifted by 2^30, 2^40 and 2^50 under all three timestamp models:
**45 cases**. Seeds come from `dataset_seed`. Every encoding revision consumes
exactly the same samples. The chunk budget is deliberately large enough that
compression cannot change the number of samples timed.

Three complete rounds alternate baseline/candidate order. Each operation has
50 warmups, then 1,000 measurements for 1,000-sample cases or 300 measurements
for 10,000-sample cases. CSV values are medians of the three per-round medians.
Bulk/append timings include allocation; result destruction occurs after the
clock stops. Decode consumes every timestamp/value pair through `black_box`.
Range materialization includes allocation; middle/tail scans cover 10% of the
time span. Every case validates sample count and exact value bits after
serialization, reload and a further append before timing.

The table below is the geometric mean of paired candidate/baseline ratios for
the **36 zero-offset cases**; shifted counters are reported separately. Negative
values mean fewer bytes or less time. These are descriptive measurements, not
confidence intervals; small differences should be rechecked on deployment hardware.

| Samples | Payload bytes | Bulk time | Append time | Decode time | Full-range time | Middle scan | Tail scan |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1000 | -10.1% | +1.3% | +0.0% | -4.1% | -4.8% | -4.5% | -4.1% |
| 10000 | -11.1% | -2.8% | -2.8% | -5.8% | -6.5% | -5.7% | -5.7% |

Selected cases expose the space/CPU tradeoff:

| Samples | Case | Payload bytes, baseline → selected | Bytes | Bulk time | Decode time |
|---|---|---:|---:|---:|---:|
| 1,000 | Periodic, regular | 8,384 → 7,247 | -13.6% | -10.5% | +4.6% |
| 1,000 | Constant, jitter | 1,900 → 1,461 | -23.1% | -0.5% | +6.2% |
| 1,000 | Constant, irregular | 2,829 → 2,108 | -25.5% | -8.3% | +0.0% |
| 1,000 | Counter + 2^40 | 3,015 → 1,893 | -37.2% | +6.3% | -2.2% |
| 1,000 | Counter + 2^50 | 4,263 → 1,893 | -55.6% | +4.1% | -9.0% |
| 10,000 | Counter + 2^40 | 30,015 → 20,443 | -31.9% | +3.8% | +0.7% |
| 10,000 | Counter + 2^50 | 42,513 → 23,337 | -45.1% | +6.7% | -17.3% |

The largest bulk-encoding regression across all 45 cases is the regular
counter shifted by 2^30 at 10,000 samples: **23.4%**. The unshifted regular
counter rises **21.0%**, while its decode rises **3.2%**. At 1,000 samples, the
2^30 case rises 17.6%; discrete and noisy regular inputs have bulk regressions
of roughly 13%. The 1,000-sample noisy/irregular decode rises 9.8%. Do not adopt
this branch on an ingest-bound deployment based only on the aggregate table.
The shifted counters do not exercise a process restart/reset within these
sample counts.

### Rejected alternatives

The fully greedy refresh policy improved space further but made regular
quantized noise decode **40.5% slower** at 10,000 samples, for only about **2.4%**
less data. Requiring a byte of savings did not fix the slowdown. The selected
policy restores that case to effectively unchanged decode time and payload size.
It gives up some space savings on quantized drift and bursty full-precision data.

A 25-bit escape retained too much header overhead: it saved only about 2% near
2^40. The selected 18-bit escape saves 37.2% in the 1,000-sample case, without
adding a bit to every ordinary fresh window. A uniform six-bit leading field
was considered in the earlier size probe, but would charge every fresh window.

Unconditional timestamp-prefix peeking was mixed or slower in the initial
isolated probes, so it was not carried into the production decoder.

### Sparse checkpoint experiment

`gorilla_seek_report` fills actual 4 KiB and 64 KiB chunks, then evaluates
strides 64/256/1024 using 48-byte transient decoder snapshots. It validates exact
hits at checkpoint boundaries and gaps. Construction costs one full decode;
warm reads include binary search and decoder resumption. Index bytes include
the container and reserved snapshot capacity, in addition to the existing payload.
The 64 KiB results at stride 1024 are:

| Workload | Samples | Additional index/payload bytes | Build time | Warm last-100 read speedup |
|---|---:|---:|---:|---:|
| constant | 262,078 | 18.7% | 1374.6 µs | 264.3× |
| drift | 10,756 | 0.8% | 94.3 µs | 20.3× |
| periodic | 9,074 | 0.6% | 88.8 µs | 9.8× |
| counter | 37,623 | 2.7% | 311.1 µs | 51.9× |

The large warm-read gains warrant a separate cache design, but do not justify
an unconditional per-chunk index: a constant series pays roughly 19% additional
memory at this stride, and the first query must build the index. Production
adoption needs a memory budget, a build/reuse policy, mutation handling and
mixed read/write benchmarks. The prototype and seek-state APIs are gated by
`test-utils`; command handlers do not use them.

## Validation

- 1,730 unit tests passed; the strengthened legacy/extended mixed-stream test
  was also rerun explicitly after adding its final assertions.
- 12 doctests passed; 5 pre-existing examples were ignored.
- Clippy checked all targets with `-D clippy::all`.
- The normal production library built in release with `RUSTFLAGS="-D warnings"`;
  release report binaries were built separately with the required allocator feature.
- The 84-case compression regression check passed against the original baseline.
  The committed baseline was refreshed; non-Gorilla rows are unchanged.
- Golden version 1 encoder fixtures load, accept an extended window after their
  legacy prefix, and round-trip exactly. Every nonzero XOR window shape and
  truncated/invalid extended headers are covered.
- A live version 1 module produced an RDB and DUMP, loaded by the new module.
  Subsequent append, label queries, version 2 save/reload and clean rejection of
  the new DUMP by the old module passed. See [persistence results](data/gorilla-persistence.json).
- Final production-module compatibility and focused integration run:
  **3,021 passed, 2 skipped, 13 existing expected divergences**. The canonical
  lifecycle helper validated Redis 8.10.0 / TimeSeries 81000 before testing and
  stopped its owned reference container afterwards. An initial final attempt
  skipped the reference-dependent cases after Docker Desktop stopped; those
  skips were discarded, Docker was restarted, and the complete run succeeded.

## Reproduction and data

Build the current reports:

```sh
cargo build --release --features enable-system-alloc,test-utils \
  --bin gorilla_report --bin gorilla_seek_report --jobs 1
cp target/release/gorilla_report /tmp/gorilla-candidate
/tmp/gorilla-candidate --samples 1000 --iterations 1000 --out /tmp/candidate-1000.csv
/tmp/gorilla-candidate --samples 10000 --iterations 300 --out /tmp/candidate-10000.csv
target/release/gorilla_seek_report > /tmp/checkpoints.csv
```

For the original encoder, create a worktree at the baseline commit, copy
`tools/gorilla_report.rs` into its `examples/gorilla_report.rs`, and build that
example with the same release features. The report uses only APIs present in
the baseline. Keep the resulting baseline and candidate executables, alternate
their order across three rounds, and avoid concurrent compilation/testing.
`--jobs 1` avoids memory contention from multiple fat-LTO links on this host.
`--encoding chimp` runs the same measurements on the Chimp codec; the default is
`gorilla`, so the commands above are unchanged.

[1,000-sample medians](data/gorilla-1000.csv),
[10,000-sample medians](data/gorilla-10000.csv),
[checkpoint results](data/gorilla-checkpoints.csv), and
[environment](data/gorilla-environment.txt) are checked in. The `against` column
identifies each variant's paired baseline. `initial-baseline`/`extended` describe
the greedy 18-bit candidate; `margin-baseline`/`hysteresis` describe the rejected
byte-savings margin; `baseline`/`fast-window` are the final paired runs. Raw
per-round CSVs remain under `target/bench-reports/gorilla-evaluation/`.
