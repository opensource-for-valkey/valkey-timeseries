# RTS compatibility harness — first differential run findings

**Date:** 2026-07-15
**Reference:** `redis:8.6@sha256:a051e4f48a5d0ceda6554974f3ad0f5369f4479197f36829332c1325cecad2b7`
**Subject:** valkey-server `unstable` + `libvalkey_timeseries` (branch `compatibility`)
**Result:** 27 passed / 21 failed of 48 smoke cases (24 scenarios × RESP2/RESP3)
**Update 2026-07-15:** findings #1–#9, #11, and #12 fixed, #13 registered as DIV-0002…DIV-0007 (see below); **suite green at 48 passed / 0 failed**. Every finding is now resolved as a module fix or a reviewed registry entry; the Phase 1 exit action (flipping the `compat-smoke` CI job to blocking, plan §8) is unblocked

This is the raw triage input the test plan's Phase 1 calls for
(docs/rts-compatibility-test-plan.md §10): each finding must be resolved as either a
module fix or a reviewed entry in `tests/compat/divergences.yml`. Reproducers are in
the failure output of `tests/compat/test_compat_smoke.py`; every finding below was
observed directly against the pinned reference image.

The frozen `TS.INFO` baseline (`tests/compat/info-fields-8.6.yml`) was validated
against the live reference: the RTS 8.6 field set matches exactly.

## Findings

| # | Layer | Surface | Observed difference | Suggested disposition |
|---|---|---|---|---|
| 1 | L1 | `TS.MRANGE`/`TS.MGET` RESP3 | RTS returns a native **map** keyed by series name, with per-series `{aggregators: []}` metadata (MRANGE); we return the RESP2 array shape on both protocols | **FIXED 2026-07-15** — RESP3 clients now get the map shapes (`[labels-map, {aggregators}, samples]`; grouped: `[labels-map, {reducers}, {sources}, samples]`; MGET: `[labels-map, sample]`), verified against the reference in standalone and cluster (fanout) modes. The fix also made MGET `SELECTED_LABELS` report a missing label as `[name, nil]` instead of a bare `nil` (RESP2), matching the reference |
| 2 | L2 | `TS.RANGE ... ALIGN n AGGREGATION ...` | Bucket assignment differs (e.g. `ALIGN 250`, bucket 1000: RTS buckets `[250,1250)` etc. with first bucket clamped to range start; ours groups differently) | **FIXED 2026-07-15** — the aggregation iterator clamped the bucket *start* to 0, shifting the first bucket's boundaries (with `ALIGN 250`/bucket 1000 the true first bucket `[-750,250)` became `[0,1000)`, swallowing samples from the next bucket). Bucket membership now uses the true (possibly negative) aligned start; only the *reported* timestamp clamps to 0, and the clamp is applied *before* the BUCKETTIMESTAMP adjustment (mid of `[-750,250)` reports `500`) — both probed against the live reference, which also confirmed the first bucket is clamped to 0, not to the range start (query from 500 still reports bucket `250`). Applies to RANGE/MRANGE/fanout/JOIN paths (shared iterator); the compaction bucket calc was intentionally left untouched. Reference-verified regression tests in `aggregate_iterator.rs` |
| 3 | L1 | `TS.MADD` per-item errors | RTS embeds real RESP **error** replies inside the array; we return the error text as a **bulk string** | **FIXED 2026-07-15** — the `SampleAddResult::Duplicate` reply arm emitted a simple string; it now emits a real RESP error like the other per-item failure arms (which already replied via `RedisModule_ReplyWithError`), so every failed TS.MADD item is an error-typed array element on RESP2 and RESP3. `test_madd_partial_failure` still fails, but now purely on the error **text** delta (`TSDB: duplicate sample` vs RTS's long "Error at upsert…" message) — that residual is #8, unchanged. Integration test updated to assert the error-typed element |
| 4 | L1 | `TS.INFO` `sourceKey` | RTS always emits `sourceKey` (nil when not a compaction); we omit it entirely for non-compaction series | **FIXED 2026-07-15** — `sourceKey` is now always present, nil for non-compactions (and nil as a defensive fallback if the source id cannot be resolved) |
| 5 | L1 | `TS.INFO` `labels` | RTS emits an empty **array** for a label-less series; we emit **nil** | **FIXED 2026-07-15** — a label-less series now emits an empty `labels` array (RESP2), matching the reference. The RESP3 map rendering of `labels` (empty `{}` and non-empty) remains #11 |
| 6 | L1 | `TS.INFO` `rules` aggregator | RTS reports the aggregator uppercase (`AVG`); we report lowercase (`avg`) | **FIXED 2026-07-15** — `TS.INFO` `rules` now reports the aggregator uppercase (`AVG`, `STD.P`, `TWA`, …), matching the reference for all supported aggregators. Scoped to TS.INFO only; the TS.MRANGE reducer/aggregator metadata stays lowercase (also matching the reference). RESP2/RESP3 regression assertions in `test_ts_info.py`; the `CompactionRule` test helper compares aggregators case-insensitively |
| 7 | L3 | WRONGTYPE condition | RTS: `TSDB: the key is not a TSDB key`; we: standard `WRONGTYPE ...` | **FIXED 2026-07-15** — probing the reference showed RTS is *not* uniform: only the auto-create write commands (`TS.ADD`, `TS.MADD`) reply `TSDB: the key is not a TSDB key`; GET/RANGE/INFO/ALTER/INCRBY/DEL/CREATERULE all keep the standard `WRONGTYPE` error. We already matched everywhere except `TS.ADD` (MADD's per-item path already mapped it), so the fix maps `WrongType` → `INVALID_TIMESERIES_KEY` at the TS.ADD key-open site only, leaving every other command on `WRONGTYPE`. Regression test in `test_ts_add.py`; `test_wrongtype` passes on both protocols |
| 8 | L3 | Duplicate-sample error text (`TS.ADD`/`TS.MADD`, BLOCK policy) | RTS: long "Error at upsert…" message; we: `TSDB: duplicate sample` | **FIXED 2026-07-15** — `DUPLICATE_SAMPLE` now carries RTS's exact text ("TSDB: Error at upsert, update is not supported when DUPLICATE_POLICY is set to BLOCK mode, or either current or new value is NaN and DUPLICATE_POLICY is MAX/MIN/SUM"), used by TS.ADD (top-level error) and TS.MADD (per-item error element). Probed against the reference: RTS emits this same text for every duplicate-blocked upsert variant (BLOCK policy, default-config BLOCK, and the NaN/MAX case). The TS.INCRBY older-timestamp path uses a different RTS error and was left untouched. `test_duplicate_block_policy` now passes on both protocols |
| 9 | L3 | Arity error text | RTS lowercases the command name (`'ts.queryindex'`); we echo uppercase (`'TS.QUERYINDEX'`) | **FIXED 2026-07-15** — commands are now registered in lowercase (`ts.create`, `ts.queryindex`, …) in `lib.rs`, so Valkey echoes the lowercase name in the arity error (and in `COMMAND INFO`/`DOCS`), matching the reference. Command dispatch is case-insensitive, so clients may still call `TS.CREATE`. This also resolves the `COMMAND INFO` name-casing divergence (§7.2 parity). Existing tests updated: arity-error string matches lowercased across the suite, and `COMMAND INFO` lookups made case-insensitive |
| 10 | L1 | `TS.INFO` extra fields | We emit `metric`, `encoding` (+ `rounding` when set) beyond the RTS set | Already registered as **DIV-0001** (`reply-superset`) |
| 11 | L1 | `TS.INFO` RESP3 shapes | RTS renders `labels` and `rules` as native RESP3 **maps** (`labels: {name: value}`, `rules: {}` when empty); we return the RESP2 array shapes on both protocols. Unmasked by the #4 fix — same class of gap as #1 | **FIXED 2026-07-15** — `TS.INFO` now branches on the client protocol: RESP3 emits `labels` as `{name: value}` and `rules` as `{destKey: [bucketDuration, aggregator, alignTimestamp]}` (both empty as `{}`); RESP2 shapes unchanged. Verified against the reference (empty and non-empty), with subject-only RESP2/RESP3 regression tests in `test_ts_info.py`. (The aggregator string is still lowercase — that residual `AVG` vs `avg` delta is #6, unchanged) |

| 12 | L1 | `TS.INFO DEBUG` `bytesPerSample` type | In the per-chunk `Chunks` list, RTS types `bytesPerSample` as a **double** (`4096.0`) in RESP3 and a bulk string (`b'4096'`) in RESP2; we emit a bulk string on both. Observed while probing #11; DEBUG-only, out of #11 scope | **FIXED 2026-07-15** — the per-chunk `bytesPerSample` is now replied as `ValkeyValue::Float` (`RedisModule_ReplyWithDouble`), which reproduces RTS's `ReplyWithDouble` typing exactly: native double on RESP3, numeric bulk string on RESP2. Byte *values* remain per-engine and uncompared (§6). RESP2/RESP3 type regression test in `test_ts_info.py` |
| 13 | L1 | Sample value text in RESP2 (`TS.RANGE` et al.) | RTS renders some doubles in scientific notation (`0.5` → `5E-1`) in RESP2 bulk strings; we render plain decimal (`0.5`). Identical parsed values — the harness tags it `float-format`. Unmasked in `test_madd_partial_failure[resp2]` once the #3/#8 error deltas were fixed (the scenario now reaches its `TS.RANGE` verification step); RESP3 is unaffected (native doubles) | **REGISTERED 2026-07-15 (DIV-0002…DIV-0007)** — reviewed decision: keep replying sample values via `ValkeyValue::Float`/`RedisModule_ReplyWithDouble` (already the case module-wide), so the RESP2 text is valkey-core's own fpconv formatting. RTS instead formats with its own method; matching it byte-for-byte would be impractical. Values are numerically identical and RESP3 compares parsed doubles, so this is registered as a `float-format` divergence for each sample-returning command (RANGE/REVRANGE/GET/MGET/MRANGE/MREVRANGE — one shared root cause) |

| 14 | L2 | `twa` aggregator missing | RTS 8.6 supports 13 compaction/range aggregators including `twa` (time-weighted average); our `AggregationType` has no `twa` variant, so `TS.CREATERULE ... AGGREGATION twa`, `AGGREGATION twa` in range queries, and `twa:...` compaction-policy entries are all rejected. Found 2026-07-16 during §7.1 config parity (the policy-grammar case `twa:1h:0` failed on "unknown aggregation type", not on the grammar) | **Open** — implement TWA (Phase 2 compaction deep-dive scope; needs time-weighted bucket semantics incl. the single-sample and bucket-edge cases from plan §6) or register `unsupported` (owner call). Tracked by a strict xfail in `test_compat_config.py::test_compaction_policy_twa` that flips when TWA lands |

## §7.1 config parity (2026-07-16)

Covered by `tests/compat/test_compat_config.py` (names, defaults, mutability,
value validation, COMPACTION_POLICY grammar): 23 passed, 1 xfail (#14 `twa`).
Resolution of the day-one suspects and everything else the diff surfaced:

- **Names**: `ts-chunk-size` → `ts-chunk-size-bytes`, `ts-ignore-max-value-diff`
  → `ts-ignore-max-val-diff` (renamed to the RTS names, no aliases kept). The
  module-name prefix valkey 8.0 forces on every module config
  (`ts.ts-chunk-size-bytes`) is registered as **DIV-0008** (`config-name`);
  the suffix — the part we control — matches RTS exactly.
- **Defaults**: `ts-encoding` now reports `compressed` (an existing alias of
  the default compressed encoding — behavior unchanged), matching the
  reference. `ts-num-threads` deliberately stays at 4 vs RTS's 3 — registered
  as **DIV-0009** (`behavior`) and asserted by the parity test. All other
  defaults already matched.
- **Validation**: the `ts-chunk-size-bytes` config bound was 64 but RTS (and
  our own `TS.CREATE`) accept 48 — lowered to 48. The COMPACTION_POLICY parser
  rejected two RTS-valid grammar forms: an alignTimestamp larger than the
  bucket duration (valid — bucket assignment is modular) and redis-style
  duration suffixes in the 4th field (it used a different parser than fields
  2–3). Both fixed, with RTS-grammar unit tests in `compaction_policy.rs`.

## §7.3 keyspace notifications (2026-07-16)

Covered by `tests/compat/test_compat_notifications.py`: both engines run with
`notify-keyspace-events KEA`; a canonical mutation script over the shared
write surface is executed on each and the per-command `(event, key)` sequences
are compared, plus an expiry-event case. Event names already matched
(`ts.create`, `ts.add`, `ts.add:dest`, `ts.incrby`/`ts.decrby`, `ts.alter`,
`ts.createrule:src`/`:dest`, `ts.del`, `ts.deleterule:src`/`:dest`). Three
behavioral deltas were found and fixed:

- **Auto-create emitted `ts.create`**: TS.ADD/TS.MADD/TS.INCRBY/TS.DECRBY on a
  missing key fired `ts.create` + the write event; the reference fires only
  the write event. The create helper's coupled replicate/notify flag was
  split, and the auto-creating call sites now suppress just the event (their
  replication is unchanged). `TS.ADDBULK` (extension) made consistent.
- **TS.DEL / upsert fired `ts.add:dest`**: compaction propagation notified
  destinations for every op; the reference emits `ts.add:dest` only on a
  bucket close (new data appended downstream) — a propagated `TS.DEL` emits
  only `ts.del`, and an upsert into a closed bucket recomputes the
  destination silently. The notification is now gated to the add-flavored
  compaction ops.
- **Latent replication bug (found by the missing `ts.add`)**: TS.ADD's
  upsert-with-rules branch early-returned after `upsert_compaction`, skipping
  `replicate_and_notify` entirely — an upsert into a series with compaction
  rules was never replicated (replica drift) and emitted no `ts.add`. The
  branch now falls through like every other successful add.

## §7.4 persistence interop (2026-07-16)

Owner decision recorded: **RTS→valkey RDB migration is not a roadmap item** —
the formats are incompatible and no conversion tooling is planned. Registered
as **DIV-0010** (`unsupported`); migration recipes (export/re-ingest, live
dual-write) documented in COMPATIBILITY.md. The defined-failure surface is
pinned by `tests/compat/test_compat_persistence.py`:

- **RESTORE of an RTS DUMP into us**: clean error, no key, server healthy.
  Today the *server's* RDB-version footer rejects Redis 8.6 payloads (v13 >
  valkey 8.0's max) before the module is reached — incidental protection.
- **The durable guard (new)**: `rdb_load_series` previously never checked
  `enc_ver` — an RTS payload admitted past the envelope (e.g. an RDB written
  by RTS on Redis 7, whose RDB version valkey accepts) would have been
  *parsed as ours*: the §11 silent-misparse data-integrity scenario. The
  loader (and the index `aux_load`) now reject any encver other than ours
  with a clear warning; RTS 8.6 writes encver 9 under the same `TSDB-TYPE`
  name (decoded from the payload's module-type id). Verified end-to-end with
  a crafted encver-9 payload (valid CRC): clean `Bad data format`, no key.
- **RTS RDB file at startup**: refused with `Can't handle RDB format version
  13` and a clean exit; fixture `test-data/rts-8.6-timeseries.rdb` (generated
  output of the pinned reference image) keeps this pinned.
- **Reverse (our DUMP into RTS 8.6)**: observed clean `Bad data format`
  rejection, no key, reference healthy — pinned as documentation (we don't
  control that direction).

## §7.5 replication & persistence self-consistency (2026-07-16)

Covered by `tests/compat/test_compat_replication.py`: primary→replica pairs on
*both* engines (a second pinned-image container replicating the harness
reference; a local valkey+module process replicating the session subject),
plus a cross-engine post-`DEBUG RELOAD` diff of `TS.INFO` + `TS.RANGE`.
Findings:

- **Plan premise corrected**: RTS 8.6 does *not* replicate auto-timestamp
  `TS.INCRBY` deterministically — it propagates the command verbatim and the
  replica stamps its own clock (30/30 divergent when probed). We behave the
  same (parity); explicit-`TIMESTAMP` increments and `*`-timestamp `TS.ADD`
  (resolved before propagation) replicate exactly on both engines. Pinned
  accordingly; the plan §7.5 text carries the correction.
- **Fixed: double propagation on auto-create (replica corruption)**: the
  create helper `alsoPropagate`d a verbatim copy of the auto-creating write
  *and* the command replicated itself, so replicas applied such writes twice
  — doubling `TS.INCRBY` values (observed `10` vs primary `5`) and, for
  `*` timestamps, materializing spurious samples. Auto-create sites no longer
  replicate from the create helper (`explicit_create` seam in
  `create_and_store_series`); only `TS.CREATE` itself replicates there.
- **Fixed: historical-upsert compaction recompute over-reach (L2)**: caught
  by the reload diff — upserting into a closed bucket recalculated it using
  the *current open* bucket's end boundary, folding every sample between the
  historical bucket and the open one into the recompute (`sum` bucket of
  `4+6+4.5` came out as `22.5` because the next bucket's `8` was included;
  reference: `14.5`). The recompute now uses the historical bucket's own
  span. Two integration tests in `test_compactions_add.py` had encoded the
  buggy values (`25.0`, `75.0`); both corrected to the reference-verified
  values (`70/3`, `45.0`).

Not yet exercised (Phase 2/3 scope): COMMAND INFO metadata (§7.2),
compaction deep-dive, fuzzing.

## Gate status

The `compat-smoke` CI job runs non-blocking (`continue-on-error`) until the findings
above are triaged and the suite is green with a reviewed registry; flipping it to
blocking is the Phase 1 exit action (plan §8).
