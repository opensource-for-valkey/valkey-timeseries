# RTS reference pin — bump log

Running record of every change to the pinned reference image in
[docker-compose.compat.yml](../docker-compose.compat.yml). The plan requires a
digest bump to be a reviewed change accompanied by a scan of what moved
(§11, "Reference drift"); this file is that record.

**Method.** Drift is characterized by *black-box probing* of the two images —
`CONFIG GET ts-*`, `TS.INFO` field sets, `COMMAND LIST FILTERBY MODULE
timeseries`, `DUMP` envelope bytes, plus targeted probes of every behavioral
claim in `tests/compat/divergences.yml` that names a version. The
RedisTimeSeries source repository is never consulted (AGENTS.md, plan §3
licensing boundary); running the shipped binary as a test target is what the
boundary permits.

**Two artifacts, one pin.** The digest-pinned image is canonical. A secondary
native-binary artifact (`redis-server` + `redistimeseries.so` from the Redis Ltd.
apt repository) is pinned in
[tests/reference_server.sh](../tests/reference_server.sh) as
`COMPAT_REFERENCE_VERSION` plus a per-distro/arch SHA256 table, and is bumped in
the *same* reviewed change as the digest. The two are only interchangeable once
an equivalence run is recorded below; until then
`COMPAT_REFERENCE_MODE=auto` always chooses the image.

---

## Native binary artifact (current pin: 8.10.0)

| | |
| --- | --- |
| source | `https://packages.redis.io/deb/pool/<dist>/r/re/redis-server_8.10.0-1rl2~<dist>1_<arch>.deb` |
| tuples | jammy, noble, bookworm × amd64, arm64 (SHA256s in `tests/reference_server.sh`) |
| payload | `usr/bin/redis-server`, `usr/lib/redis/modules/redistimeseries.so` |
| equivalence run | **not yet performed** — every tuple is a *candidate* |

**Equivalence run (required before `auto` may select binary mode).** For a given
distro/arch, against the image pinned above:

1. Fingerprint both and diff: `INFO server` (`redis_version`, `redis_build_id`,
   `arch_bits`), `MODULE LIST`, `INFO modules`, the `INFO` field inventory in
   `tests/compat/info-fields-8.10.yml`, the `CONFIG GET ts-*` surface, and the RDB
   version footer produced by `SAVE`.
2. Run the full compat suite against each with identical flags and compare
   `test-data/compat-report.json` plus the pass/skip/xfail-divergent counts. See
   the 2026-08-01 bump entry below for the current 8.10.0 image baseline.
3. Equivalence means identical divergence sets and identical counts. Any delta is
   a finding — a new registry entry, or grounds to leave binary mode off for this
   pin.

Record the result here and flip the tuple's status to `verified` in
`tests/reference_server.sh`, along with
`_COMPAT_REF_EQUIVALENCE_VERIFIED_PIN`.

---

## 2026-08-01 — redis:8.8.0 → redis:8.10.0

| | previous | current |
| --- | --- | --- |
| image | `redis:8.8` | `redis:8.10` |
| index digest | `sha256:234c902a…fa713c7b` | `sha256:c29e49ab…275a5236` |
| server | 8.8.0 | 8.10.0 |
| bundled `timeseries` module | 80800 | 81000 |
| RDB version (`SAVE` footer) | REDIS0014 | REDIS0015 |

### What changed

**1. Four new commands on the module's command surface.** `COMMAND LIST FILTERBY
MODULE timeseries` against 8.10 additionally lists `TS.NRANGE`, `TS.NREVRANGE`,
`TS.QUERYLABELS` and `TS.READ` — visible, non-hidden, `@read @timeseries`-flagged
commands, not internal cluster-fanout helpers (absent from 8.8's 17+2 command
list). `TS.QUERYLABELS` takes a `LABELS|VALUES` subtype and overlaps this
module's own `TS.LABELNAMES`/`TS.LABELVALUES` extensions. This module implements
none of the four. Left deliberately **out of the compared surface** for this
bump rather than silently pulled into §2.1 — see "Not yet in scope" in
[rts-compatibility-test-plan.md](rts-compatibility-test-plan.md), §2.1.
Writing dedicated compat coverage and deciding registry treatment for four
unimplemented commands is follow-up work, tracked here rather than blocking the
pin bump.

**2. New config `ts-topology-events` (default `yes`, immutable).** Present on
8.10, absent on 8.8. `CONFIG SET` is rejected with "can't set immutable config"
regardless of value; black-box probing found no observable command-level effect
under the default. No subject equivalent — registered as **DIV-0048**
(`unsupported`) and listed in `REFERENCE_ONLY_CONFIGS` in
`tests/compat/test_compat_config.py`, same treatment as `ts-libmr-protocol`
(DIV-0034) at the previous bump.

**3. RDB version footer moved REDIS0014 → REDIS0015.** No change to the module
encoding version (`encver` stayed 9 — decoded directly from a live `DUMP`
payload's module-id bits), so DIV-0010's persistence-interop expectations are
unaffected; only the server-level envelope version moved.

**4. `first`/`last` are now chronological on `TS.REVRANGE`, not scan-order — a
code fix, not a new divergence.** Through the 8.8 pin, RTS defined `first`/
`last` against the scan: under `TS.REVRANGE`, `first` reported each bucket's
*newest* sample (the mirror of `TS.RANGE`). This module matched that
deliberately (`AggregationOptions::for_scan_order`, wired into both the
single- and multi-aggregator row paths). On 8.10, RTS reports the
chronologically earliest/latest sample regardless of direction — confirmed
directly (`TS.REVRANGE k - + AGGREGATION first/last 1000` on a two-sample
bucket returns the same values as the equivalent `TS.RANGE` query) and via the
compat suite, which failed ~58 tests across `test_compat_range.py`,
`test_compat_multi_aggregation.py` and the `revrange_first_last_scan_order`
corpus reproducer. Fixed by removing the swap entirely
(`for_scan_order`/`AggregatorConfig::with_first_last_swapped` deleted from
`src/series/request_types.rs`; the three call sites in
`src/iterators/utils.rs` now use the requested aggregation unmodified) —
buckets are already aggregated chronologically forward and only the *finished
buckets* are reversed for output, so the natural (unswapped) result was
already the chronological one RTS 8.10 now expects.

The `last` EMPTY-bucket carry-forward direction (`CarryLastEmpty`) had to move
in lockstep: it was applied *after* `ReverseIter` (output order), which — now
that individual bucket values are chronological — carried from the wrong
neighbor on a reverse query (the chronologically *later* bucket instead of the
earlier one). Moved to run *before* `ReverseIter` in both
`finalize`/`finalize_row_iterator` (`src/iterators/utils.rs`), so the carry is
always chronological. Verified against the exact failing reproducer
(`test_empty_carry_forward_across_bucket_sizes[TS.REVRANGE]`): buckets `2000`
and `2500` (empty, between `1500`→`16` and `3000`→`32`) now carry `16` (the
chronologically preceding value) rather than `32`, matching the live
reference exactly.

Updated in the same change: the Rust unit test asserting the old scan-order
values (`test_reverse_iteration_with_last_aggregation_and_nan_values`), the
Python `test_revrange_all_aggregation_types[FIRST/LAST]` expectations (swapped
back to chronological), and the corpus fixture's stale description.
`docs/rts-compatibility-test-plan.md` §6 and `COMPATIBILITY.md` do not name
this behavior explicitly and needed no changes.

**5. `TS.CREATERULE ... AGGREGATION ""` (empty string) gets a more specific
reference error.** RTS 8.10 replies "TSDB: Empty aggregation type in list";
this module still replies the generic "TSDB: Unknown aggregation type" used
for every other unrecognized name. Every other bogus aggregator name (tested:
`median`, `average`, `p99`) is unaffected — both engines still agree. Pure
error-text divergence, registered as **DIV-0049**; not fixed (would mean
special-casing the empty string for a wording match only).

### What did *not* change

Verified identical across both images, so the frozen baselines carried over
unmodified:

- **`TS.INFO` field set** — all 14 fields, same order. `info-fields-8.8.yml` was
  renamed to `info-fields-8.10.yml` with its content untouched.
- **The 17-command shared surface** (§2.1) and its `TS.*` argument grammar —
  unchanged; the four additions are new surface, not a change to the existing 17.
- **`ts-*` config surface and defaults** (aside from the new `ts-topology-events`
  above) — all 8 compared names, defaults, and mutability unchanged;
  `ts-num-threads` remains mutable at runtime (the 8.6→8.8 change), `ts-encoding`
  remains a one-way latch, `ts-libmr-protocol` remains present with no subject
  equivalent.
- **The 13 aggregators** (including `twa`) — all accepted by `TS.CREATERULE`;
  `first`/`last`/`twa` remain rejected as `GROUPBY ... REDUCE` reducers.
- **`DUMP` envelope** — still `TSDB-TYPE` at encver 9.
- **Every version-named behavioral claim in the registry** was re-probed against
  8.10 and still holds: negative `TIMESTAMP` accepted by `TS.INCRBY`/`TS.DECRBY`
  (DIV-0032/DIV-0033), unknown trailing tokens silently ignored (DIV-0015),
  `ts-encoding` a one-way latch (DIV-0018), `ts-num-threads` defaulting to 3
  (DIV-0009), `*`/relative bounds rejected (DIV-0013), first-occurrence-wins on a
  duplicated option (DIV-0014), compaction-destination chaining refused
  (DIV-0017), repeated aggregator in a list accepted (DIV-0036), `GROUPBY` +
  multi-aggregator list rejected (DIV-0037), extension aggregators/inline
  conditions rejected (DIV-0038), floating-point cancellation to exactly `0` on
  `std.p`/`std.s`/`var.p`/`var.s` for large-magnitude, small-spread samples
  (DIV-0024..0029 — reproduced with the exact registry repro command). The
  `TS.INCRBY key <n> TIMESTAMP`-with-no-operand argument-vector overrun
  (DIV-0046) was **not** reproduced in 15 attempts on a disposable 8.10
  container; left registered rather than removed since a non-reproduction of a
  memory-layout-dependent bug is not proof it was fixed. Keyspace notifications
  (`ts.create`, `ts.add`) spot-checked and unchanged.

**Not re-validated:** replication semantics (test plan §7.5, the
`*`-timestamp-resolves-before-propagation finding) — that requires a primary +
replica pair and was out of scope for this pass. The dated
"re-confirmed against 8.8 on 2026-07-23" note in
[rts-compatibility-test-plan.md](rts-compatibility-test-plan.md) is left as-is
rather than reworded to imply re-confirmation that did not happen.
[rts-compat-first-run-findings.md](rts-compat-first-run-findings.md) keeps its
dated 8.6 references, per the existing convention.

### Fixture regenerated

`test-data/rts-8.8-timeseries.rdb` → `test-data/rts-8.10-timeseries.rdb`,
generated fresh against the pinned 8.10 image (header confirms `REDIS0015`).

### Native binary artifact

Bumped in the same change: `COMPAT_REFERENCE_VERSION` → `8.10.0`,
`COMPAT_REFERENCE_MODULE_VERSION` → `81000`, and the six jammy/noble/bookworm ×
amd64/arm64 SHA256 entries in `tests/reference_server.sh` replaced with the
`redis-server_8.10.0-1rl2~<dist>1_<arch>.deb` checksums from the
packages.redis.io `Packages` index (2026-08-01) — note the revision suffix
moved `1rl1` → `1rl2` (the index currently serves both for 8.10.0; `1rl2` is
the higher/latest revision, confirmed against the exact package the official
`redis:8.10` Dockerfile installs). One checksum spot-verified by direct
download + `sha256sum`. Still all `candidate` status — no equivalence run
performed.

### Verification

Full compat suite against the live 8.10 reference: **2548 passed, 6 skipped,
1 xfailed, 0 failed** (up from 8.8's 1368/6/1 — the suite grew between pins;
see `git log` on `tests/compat/` for the added coverage, unrelated to this
bump). Full Rust unit suite: **1287 passed, 0 failed**
(`cargo test --release --features enable-system-alloc --lib`).

First run against the freshly re-pinned reference (before the code fixes in
"What changed" #4/#5 above) surfaced **60 failures**: 58 from the
scan-order-vs-chronological `first`/`last` divergence and 2 from the new
`TS.CREATERULE` empty-aggregator error text. Both addressed above (one by
code fix, one by registry entry); the clean 0-failed run is after both
changes and a full rebuild.

Registered-divergence counts from the passing run: DIV-0001 (1380×,
TS.INFO reply-superset), DIV-0002 (134×, TS.RANGE float-format), DIV-0003
(91×, TS.REVRANGE float-format), DIV-0004 (5×, TS.GET float-format), DIV-0032
(2×, TS.INCRBY negative timestamp), DIV-0033 (2×, TS.DECRBY negative
timestamp), DIV-0049 (2×, TS.CREATERULE empty-aggregator text, new this
bump). Differential fuzzer (`COMPAT_FUZZ=1`) not re-run this pass — flagged
as follow-up, same as the command-surface and replication gaps noted above.

## 2026-07-23 — redis:8.6.4 → redis:8.8.0

| | previous | current |
| --- | --- | --- |
| image | `redis:8.6` | `redis:8.8` |
| index digest | `sha256:a051e4f4…cecad2b7` | `sha256:234c902a…fa713c7b` |
| server | 8.6.4 | 8.8.0 |
| bundled `timeseries` module | 80602 | 80800 |
| RDB version (`SAVE` footer) | REDIS0013 | REDIS0014 |

### What changed

**1. New config `ts-libmr-protocol` (default `INTERNAL`).** Present on 8.8,
absent on 8.6. Selects the wire protocol for libmr, the layer RedisTimeSeries
uses to fan multi-key queries across cluster shards. No subject equivalent —
registered as **DIV-0034** (`unsupported`) and listed in
`REFERENCE_ONLY_CONFIGS` in `tests/compat/test_compat_config.py`, which exempts
it from the "every RTS config name must exist on the subject" rule while still
asserting it remains present on the reference (so a stale entry surfaces).

**2. `ts-num-threads` became mutable.** On 8.6 `CONFIG SET ts-num-threads`
was rejected with `can't set immutable config`; on 8.8 it is accepted, the value
actually changes, and it is validated to `1..16`. This module still registers the
config `IMMUTABLE` (`src/config.rs`), so mutability is now per-engine — registered
as **DIV-0035** (`behavior`). Reproducing it means resizing a live rayon pool
shared by in-flight queries, which is a concurrency change and not forced by the
bump; the risk direction is safe (we reject what RTS accepts, so a `CONFIG SET`
carried over from RTS errors loudly rather than silently no-op'ing).

### What did *not* change

Verified identical across both images, so the frozen baselines carried over
unmodified:

- **`TS.INFO` field set** — all 14 fields, same order. `info-fields-8.6.yml` was
  renamed to `info-fields-8.8.yml` with its content untouched.
- **Command surface** — same 17 `ts.*` commands plus the two
  `timeseries.*` cluster commands.
- **`DUMP` envelope** — still `TSDB-TYPE` at encver 9, so the persistence
  interop expectations (DIV-0010) are unaffected.
- **Every version-named behavioral claim in the registry** was re-probed against
  8.8 and still holds: negative `TIMESTAMP` accepted by `TS.INCRBY`/`TS.DECRBY`
  (DIV-0032/DIV-0033), unknown trailing tokens silently ignored (DIV-0015),
  `ts-encoding` a one-way latch (DIV-0018), `ts-num-threads` defaulting to 3
  (DIV-0009), `twa` present (DIV-0014).

### Fixture regenerated

`test-data/rts-8.6-timeseries.rdb` → `test-data/rts-8.8-timeseries.rdb`. The
persistence test describes this file as the generated output of the *pinned*
reference, so it is regenerated on every bump rather than kept; it also moves
the asserted rejection onto the current RDB version (v14).

### Verification

Full compat suite against the live 8.8 reference: **1368 passed, 6 skipped,
1 xfailed** — the same totals as the 8.6 pin. Differential fuzzer (`COMPAT_FUZZ=1`,
subject in `ts-compatibility-mode strict`) clean, with only the known RESP2
float-format divergences (DIV-0002…DIV-0007) firing.

**Not re-validated:** [rts-compat-first-run-findings.md](rts-compat-first-run-findings.md)
is a dated record of the first run against the 8.6 pin and deliberately keeps its
8.6 references.
