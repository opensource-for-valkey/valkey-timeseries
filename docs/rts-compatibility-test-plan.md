# RedisTimeSeries 8.6 Compatibility Test Plan

**Status:** Draft
**Reference implementation:** the official Redis release of RedisTimeSeries as shipped in Redis Open Source 8.6 (`redis:8.6` Docker image, pinned by digest)
**Owner:** TBD

valkey-timeseries advertises a *superset* of the RedisTimeSeries API. This plan defines what
"compatible" means, how we verify it continuously, and how we record the places where we
intentionally diverge. The existing pytest suite under `tests/` verifies our own behavior; nothing
today checks that behavior *against* RedisTimeSeries. This plan closes that gap.

---

## 1. Compatibility contract

"Consistent with RedisTimeSeries 8.6" is decomposed into five testable layers, from strictest to
loosest:

| Layer | Contract | Verification |
|---|---|---|
| **L1 — Wire replies** | Identical reply *shape and values* (after defined normalization) for the shared command surface, in both RESP2 and RESP3 | Differential harness (§4.1) |
| **L2 — Semantics** | Identical observable state transitions: same samples stored, same compactions produced, same keys created/deleted, same index results | Differential harness + state probes (TS.RANGE / TS.INFO after mutation) |
| **L3 — Errors** | Same *conditions* error; same error class/prefix (`TSDB:`, `ERR`, `WRONGTYPE`); message text matches unless registered as a divergence | Differential harness with error-policy comparator (§5.2) |
| **L4 — Operational surface** | Config parameters, keyspace notifications, ACL behavior, COMMAND INFO metadata, RDB/DUMP handling | Dedicated parity tests (§7) |
| **L5 — Ecosystem** | Real client libraries (valkey-py, redis-py, NRedisStack, Jedis) work unmodified against valkey-timeseries | Client conformance suites (§4.2) |

A compatibility claim in the README should reference the layer(s) we actually gate in CI.

## 2. Scope

### 2.1 In scope — the shared command surface

The 17 commands RedisTimeSeries 8.6 exposes:

`TS.CREATE`, `TS.ALTER`, `TS.ADD`, `TS.MADD`, `TS.INCRBY`, `TS.DECRBY`, `TS.DEL`,
`TS.CREATERULE`, `TS.DELETERULE`, `TS.RANGE`, `TS.REVRANGE`, `TS.MRANGE`, `TS.MREVRANGE`,
`TS.GET`, `TS.MGET`, `TS.INFO`, `TS.QUERYINDEX`

### 2.2 In scope — cross-cutting behavior

- Module configs (`ts-*`) — names, defaults, mutability, value ranges
- Duplicate policies, `ON_DUPLICATE`, `IGNORE` insertion filters
- Compaction rules: all 13 aggregators, `alignTimestamp`, bucket boundary semantics, upstream
  deletion/retention interaction
- Label filter language (`=`, `!=`, `=(a,b)`, `!=(a,b)`, presence/absence matchers)
- RESP2 vs RESP3 reply shapes
- Keyspace notifications, ACL categories, `COMMAND INFO`/`COMMAND DOCS` metadata
- Persistence interop: behavior when fed RedisTimeSeries-produced RDB/DUMP payloads (§7.4 — this
  is a *defined-failure* surface, not a data-migration promise)

### 2.3 Out of scope (extensions — non-interference checks only)

Our extensions (`TS.ADDBULK`, `TS.CARD`, `TS.MDEL`, `TS.JOIN`, `TS.XCORR`, `TS.STATS`,
`TS.LABELNAMES`/`TS.LABELVALUES`/`TS.LABELSTATS`/`TS.METRICNAMES`, the forecasting/analytics
family, `TS.SANITIZE`, `TS.FILLGAPS`, `TS._DEBUG`) are not compared against RedisTimeSeries.
They get one class of compat test: **non-interference** — using an extension on a series must not
change the behavior of any in-scope command on that series (e.g., `TS.MDEL` followed by
`TS.RANGE` behaves like the equivalent `TS.DEL` sequence would).

Also out of scope: performance parity, memory-usage parity (`TS.INFO memoryUsage` is compared for
*presence and type*, not value), and Redis Enterprise-specific behavior.

## 3. Reference environment

- **Reference:** the official Redis release — the `redis:8.6` Docker image (timeseries is bundled
  in Redis Open Source 8.x). Pin the exact digest in the compose file; bumping the digest is a
  reviewed change accompanied by a changelog scan of the corresponding RedisTimeSeries release
  notes.
- **Subject:** `valkey-server` (version matrix as used by `build.sh`) + `libvalkey_timeseries`
  built from the branch under test.
- Both run in Docker via a `docker-compose.compat.yml`, standalone mode, notifications enabled
  (`notify-keyspace-events KEA`), identical `maxmemory-policy noeviction`.
- **Licensing boundary:** Redis 8.x is RSALv2/SSPLv1/AGPLv3 tri-licensed — incompatible copyleft
  terms relative to this Apache-2.0 repo. Running the official binary image in CI as a black-box
  test target is fine; its *source and test suite* are off-limits. Nothing from the
  RedisTimeSeries repository may be vendored, copied, ported, or fetched at CI time. All
  compatibility tests in this plan are written clean-room, from public documentation and
  black-box observation of the reference server's behavior.

## 4. Test architecture

Three tiers, ordered by cost. Tier A is the workhorse; B–C catch what A's authors didn't think
of.

> **Why there is no "replay the upstream test suite" tier:** RedisTimeSeries' own functional
> suite lives in its source repository and carries the same incompatible copyleft licensing as
> the source (§3). Vendoring it, porting its test cases, or even fetching it at CI time is off
> the table. The coverage that suite would have provided is replaced by the clean-room matrix in
> §6 (derived from public command documentation) and the differential fuzzer (§4.3), which
> discovers undocumented edge-case behavior by observation rather than by reading upstream tests.

### 4.1 Tier A — Differential harness (golden + scripted scenarios)

A pytest plugin layered on the existing framework (`tests/valkey_timeseries_test_case.py`):

- New fixture pair `subject` / `reference` returning connected clients; a `DiffClient` wrapper sends
  each command to both, normalizes both replies (§5.1), and asserts equality — automatically, on
  every call. Scenario authors just write command sequences.
- Each test is parametrized over `resp2` / `resp3`.
- On mismatch, the failure report prints the command history (full reproducer), both raw replies,
  and the normalized diff.
- Mismatches matching an entry in the divergence registry (§5.3) are recorded as `XFAIL-DIVERGENT`
  in the conformance report rather than failing.
- Lives in `tests/compat/`, marked `@pytest.mark.rts_compat` so it is skippable when no
  reference server is available (developers without Docker).

Scenario files are organized per command (`tests/compat/test_compat_range.py`, …) and encode the
matrix in §6.

### 4.2 Tier B — Client-library conformance

Black-box proof that real clients can't tell the difference:

- **valkey-py / redis-py:** run the `commands/timeseries` test modules from the client repo
  against valkey-timeseries (both RESP versions).
- **NRedisStack (.NET)** and **Jedis (Java):** nightly-only, containerized, timeseries test
  classes only.

Client suites move fast; pin client versions and refresh quarterly. Unlike the RedisTimeSeries
repository, these client libraries are permissively licensed (MIT/BSD), so running their test
suites — or even porting individual cases — poses no licensing conflict.

### 4.3 Tier C — Property-based differential fuzzing

Hypothesis-based generator producing random but *valid-by-construction* command sequences over a
small key/label universe:

- Interleaved writers (`ADD`/`MADD`/`INCRBY`/`DECRBY`/`DEL`) with random `RETENTION`,
  `DUPLICATE_POLICY`, `IGNORE`, `CHUNK_SIZE`, `ENCODING`, plus compaction rules, then a barrage of
  readers (`RANGE`/`MRANGE`/`GET`/`MGET`/`INFO`/`QUERYINDEX`) with random option combinations.
- Timestamps drawn from adversarial distributions: clustered, out-of-order, bucket-boundary ±1,
  `0`, negative-rejection cases, far-future, duplicate.
- Values include `0.0`, `-0.0`, subnormals, huge magnitudes, high-precision decimals (float
  formatting is the classic divergence source), and for `INCRBY` paths, accumulation order.
- Shrinking yields a minimal reproducer which is then *checked into* Tier A as a golden test
  (regression corpus in `tests/compat/corpus/`).
- Runs nightly with a fixed time budget (e.g. 20 min) and a persisted seed corpus.

## 5. Comparison rules

### 5.1 Reply normalization

Applied to both sides before comparison:

1. **Floats:** parse bulk-string doubles and compare with relative tolerance 1e-12 *and* record
   the raw string. Exact-string mismatches with equal parsed values (e.g. `"1"` vs `"1.0"`) are
   reported as formatting deltas — failing by default for `TS.RANGE`-family sample values (clients
   parse these, but downstream text-diffing tools exist), because RTS's `%.17g`-style formatting
   is observable. If we choose tolerance here instead, that's a registry entry, not a silent pass.
2. **Maps:** RESP3 maps and RESP2 flattened name/value arrays (`TS.INFO`, `MRANGE` label sets)
   compare as unordered dicts.
3. **TS.INFO superset:** extra fields we emit are allowed and reported (not failed); *missing* or
   value-mismatched RTS 8.6 fields fail. RTS 8.6 baseline field list is frozen in
   `tests/compat/info-fields-8.6.yml`.
4. **MRANGE/MGET ordering:** RTS makes no cross-series order guarantee; sort by key before
   comparison. *Within* a series, sample order must match exactly.
5. **Nondeterministic values** (`memoryUsage`, `chunkSize`-dependent chunk counts under different
   internal defaults): compared by type/presence only, listed explicitly in the normalizer — every
   exemption is enumerated, none are pattern-based.

### 5.2 Error comparison policy

- Error **condition** must match: if the reference errors, we must error (and vice versa). A
  "reference errors, subject succeeds" mismatch is always a failure — that's an accepted-input
  superset, which silently breaks scripts ported back to Redis.
- Error **prefix** must match (`TSDB:` / `ERR` / `WRONGTYPE` — our `src/error_consts.rs` already
  follows the `TSDB:` convention).
- Full-text equality is *reported*; deltas fail unless registered. Rationale: client libraries and
  user runbooks match on error text more than we'd like.

### 5.3 Known-divergence registry

`tests/compat/divergences.yml` — the single source of truth the harness consults and the docs
build renders into a public "Compatibility" page:

```yaml
- id: DIV-0001
  command: TS.INFO
  kind: reply-superset          # reply-superset | error-text | behavior | config-name | unsupported
  description: >
    TS.INFO returns additional fields (…) beyond the RTS 8.6 set.
  rationale: extension metadata
  since: 0.x.0
```

Rules: every entry has an owner-reviewed rationale; `behavior`-kind entries additionally require a
sign-off in the PR that introduced them. CI fails if a divergence fires that has no entry, and
warns if an entry hasn't fired in 30 days (stale — maybe we fixed it; remove it).

## 6. Command test matrix (Tier A)

Dimensions applied to every in-scope command: **arg parsing** (each option valid/invalid/missing
value/duplicated/case-insensitivity), **key states** (missing key, WRONGTYPE, empty series),
**RESP2/RESP3**, **error paths**. Command-specific dimensions:

| Command | Specific dimensions to cover |
|---|---|
| `TS.CREATE` | RETENTION (0, >0, negative-reject); ENCODING COMPRESSED/UNCOMPRESSED; CHUNK_SIZE (min 48, max 1 MiB, multiple-of-8 rule, off-by-one rejects); DUPLICATE_POLICY ×6; IGNORE (values, requires-LAST interaction); LABELS (empty value, duplicate label reject, ordering in INFO); key-exists error |
| `TS.ALTER` | each mutable property changed/cleared; label replacement semantics (full replace, not merge); altering series with rules |
| `TS.ADD` | auto-create with per-config defaults; `*` timestamp; out-of-order insert; retention-window trimming trigger; ON_DUPLICATE override vs series policy vs `ts-duplicate-policy` config precedence; IGNORE filtering (time diff, value diff, both, only-applies-to-LAST rule); NaN/inf value rejects; timestamp 0 |
| `TS.MADD` | partial failure semantics (per-item errors in reply array, ordering); same-key repeated in one call; interaction with duplicate policy |
| `TS.INCRBY`/`TS.DECRBY` | auto-create; TIMESTAMP option incl. `*` and stale-timestamp error; accumulation on existing bucket; interaction with compaction rules |
| `TS.DEL` | boundary inclusivity; delete across chunk boundaries; delete on compacted source propagating to downstream (per RTS semantics); reply = deleted count |
| `TS.CREATERULE` | all 13 aggregators (`avg sum min max range count first last std.p std.s var.p var.s twa`); alignTimestamp; self-rule reject; rule-on-rule reject; dest-has-data behavior; source/dest missing errors |
| `TS.DELETERULE` | removes compaction; INFO reflects removal; nonexistent-rule error |
| `TS.RANGE`/`TS.REVRANGE` | `-`/`+` bounds; inclusive boundaries; COUNT; AGGREGATION × all aggregators × bucket boundary cases; ALIGN (`-`, `+`, `start`, `end`, explicit ts); BUCKETTIMESTAMP (`-`, `+`, `~`); EMPTY (gap buckets, value per aggregator: NaN vs 0 semantics); FILTER_BY_TS (dup ts, unsorted list); FILTER_BY_VALUE (min>max); LATEST on compacted target; combination ordering rules |
| `TS.MRANGE`/`TS.MREVRANGE` | full filter language matrix; WITHLABELS vs SELECTED_LABELS (missing label → nil); GROUPBY/REDUCE × all reducers, incl. empty groups and label-absent series; everything from RANGE applied per-series; reply nesting shape RESP2 vs RESP3 (this is where RESP3 differs most) |
| `TS.GET` | empty series reply shape; LATEST semantics on compaction target with open bucket |
| `TS.MGET` | filter matrix; WITHLABELS/SELECTED_LABELS; empty-series entries |
| `TS.INFO` | field-by-field vs frozen 8.6 baseline; DEBUG variant (chunk list: presence/shape, not byte counts); after ALTER/CREATERULE/DEL mutations |
| `TS.QUERYINDEX` | filter matrix; result ordering (normalize); no-match empty array; requires-non-empty-matcher error |

> **Phase 2 status (2026-07-16):** `TS.RANGE`/`TS.REVRANGE` (`tests/compat/test_compat_range.py`),
> `TS.MRANGE`/`TS.MREVRANGE` (`tests/compat/test_compat_mrange.py`), `TS.GET`/`TS.MGET`
> (`tests/compat/test_compat_get.py`), `TS.QUERYINDEX` (`tests/compat/test_compat_queryindex.py`),
> and the compaction deep-dive (`tests/compat/test_compat_compaction.py`) are landed and fully
> green — **the §6 read-path matrix is complete.** TS.QUERYINDEX needed no subject change: it
> mirrors the MGET filter behavior (including the DIV-0019/0020 Prometheus supersets) and its
> detailed parser diagnostics are a deliberate feature. The GET matrix found and fixed one subject
> bug: TS.GET reported
> valkey-module-rs's raw "Existing key has wrong Valkey type" on a WRONGTYPE key where RTS (and our
> own TS.RANGE/TS.INFO) report the standard WRONGTYPE — `with_timeseries` now maps it, and a missing
> key still reports KEY_NOT_FOUND. The MRANGE matrix found and fixed four subject bugs — REDUCE restricted to
> RTS's reducer set (`first`/`last` were wrongly accepted), SELECTED_LABELS with no labels now
> rejected, the ALIGN-`start`/`end`-needs-explicit-bound guard extended from RANGE to the MRANGE
> family, and a stray `REDUCE` without `GROUPBY` now rejected instead of silently ignored — and
> registered two Prometheus-superset FILTER divergences (DIV-0019 negative-only matcher, DIV-0020
> bare metric-name matcher) plus documented the arity-vs-TSDB error-message differences. The phase found and fixed one crash
> (`AGGREGATION <agg> 0` panicked the server; a zero-duration rule also persisted into the RDB and
> crashed the source's next write), one silent correctness bug (`TS.INCRBY`/`TS.DECRBY` never drove
> compaction rules), three accepted-input supersets, two over-strict rejections, and aligned ~10
> error texts.
>
> **Fixed since:** *out-of-order compaction upsert.* A late sample into an already-finalized
> bucket did not update the downstream value (RTS 100, ours 99 for `1 + 99` into a closed bucket),
> and overwriting a lone ts=0 sample deleted the downstream bucket outright. Root cause: the
> bucket-recalculation filter `null_ts_filter` excluded timestamp 0 (`ts != 0`). Now `true` —
> every stored sample counts. Verified byte-identical to RTS.
>
> **Fixed since (retention accounting):** *TS.INFO under retention.* `totalSamples` /
> `firstTimestamp` reported physically-buffered samples where RTS reports the retained window
> (`TS.RANGE` already agreed). Root cause: retention was applied lazily — only by the async
> background trim task and on chunk split — and `trim()` had an off-by-one boundary
> (removed `<= min`, dropping the sample at exactly `lastTimestamp - retention`, which RTS keeps).
> Now every write trims the series synchronously (`TimeSeries::add` / `merge_samples`), with the
> boundary aligned to the read path and RTS (keep `>= min`). Verified byte-identical, including a
> `DEBUG RELOAD` round-trip. The full compat suite is green.
>
> Divergences DIV-0012..DIV-0020 are registered but still need the §5.3 owner sign-off.
>
> **Registry hazard found while doing this (fixed):** an unscoped `behavior` entry matches every
> `value`/`shape`/`error-condition` delta on its command — nearly every real bug on that command.
> DIV-0013 silently absorbed finding (1) above until it was caught. `divergences.yml` entries now
> carry either a `details_regex` or `documentation_only: true`, and the loader enforces the
> distinction. Any future `behavior` entry needs the same scrutiny.
>
> **Tier C status (2026-07-16):** the property-based differential fuzzer (§4.3) is landed —
> `tests/compat/fuzz_strategies.py` (valid-by-construction command-sequence generators),
> `tests/compat/test_compat_fuzz.py` (opt-in via `COMPAT_FUZZ=1`), and a checked-in regression
> corpus (`tests/compat/corpus/`, replayed by `test_compat_corpus.py`). On its first runs against
> the reference it found and fixed two `TS.MADD` subject bugs, both now pinned in the corpus:
> (1) the batch retention gate used the pre-batch series max instead of an input-order running
> max, so an item below the floor a *preceding* item established was silently accepted-then-trimmed
> instead of reported `TooOld` — the gate is now input-order sensitive (mirroring sequential
> per-item `TS.ADD`), and the too-old error text was aligned to RTS ("Timestamp is older than
> retention"); (2) in-batch duplicate timestamps in one `TS.MADD` were rejected outright instead of
> folding per duplicate policy (`SUM` accumulates, `LAST` wins, `BLOCK` errors per duplicate),
> giving wrong stored values — such groups now fall back to sequential single-sample add, with
> compaction fed the distinct post-fold samples so rollups do not double-count. Verified
> byte-identical to RTS across all six duplicate policies and under an active compaction rule.

**Compaction deep-dive scenarios** (highest historical-divergence area, gets its own module):
bucket finalization timing (when does a sample land in the downstream series), out-of-order writes
into already-finalized buckets, `TS.DEL` on source ranges covered by finalized buckets, retention
expiring source data under an active rule, `twa` edge cases (single sample, bucket edges), and
restart persistence of partial-bucket state (`DEBUG RELOAD` on both engines mid-bucket, compare
downstream after next write).

## 7. Operational parity tests (Tier A modules)

### 7.1 Configuration
Enumerate `CONFIG GET ts-*` on both engines and diff: names, defaults, mutability
(`CONFIG SET` accept/reject), value validation. Known suspects to resolve on day one — our
`src/config.rs` registers `ts-chunk-size` and `ts-ignore-max-value-diff`; RTS 8.x uses
`ts-chunk-size-bytes` and `ts-ignore-max-val-diff`. Each name mismatch becomes either an alias we
add or a `config-name` divergence entry. Also verify `COMPACTION_POLICY`-style policy-string
parsing accepts the same grammar (`max:1M:1h;avg:2h:10d:...`).

### 7.2 Introspection & ACL
- `COMMAND INFO`/`COMMAND COUNT` for each shared command: arity, flags (write/readonly/denyoom),
  first/last/step keys, ACL categories. `COMMAND DOCS` compared loosely (presence).
- ACL: a user with `-@all +@read` / `+@write` / per-command grants behaves identically for shared
  commands (builds on existing `tests/test_ts_acls.py`).

### 7.3 Keyspace notifications
Subscribe `__keyevent@0__:*` on both engines, run a canonical mutation script, compare the event
name sequence (`ts.add`, `ts.incrby`, `ts.createrule`, `del`, expiry events, …).

### 7.4 Persistence interop (defined-failure surface)
We register the **same module type name** as RedisTimeSeries — `TSDB-TYPE`
(`src/series/series_data_type.rs:25`) — with our own payload format and `encver=1`, while RTS 8.6
uses higher encoding versions. Consequences must be pinned by tests, not discovered by users:

- `RESTORE` of an RTS-produced `DUMP` payload into valkey-timeseries: must fail **cleanly**
  (error, no crash, no partially-created key, server healthy after).
- Loading an RTS-produced RDB file: server must either refuse the file with a clear log message or
  skip/fail cleanly — never misparse. Fixture RDBs generated by running the reference server live
  in `test-data/` (generated output data, not upstream source — no licensing concern).
- The reverse direction (our DUMP into Redis 8.6): document observed behavior; we can't control
  it, but the compatibility page must state it.
- **Decision (owner, 2026-07-16): RTS→valkey RDB migration is NOT a roadmap item.** The data
  formats are incompatible and there is no plan for conversion tooling. Registered as
  **DIV-0010** (`unsupported`); the migration-path recipe (`TS.RANGE` export → `TS.MADD`
  import, or live dual-write) lives in COMPATIBILITY.md §"Persistence and on-disk format".
  The defined-failure surface above is pinned by `tests/compat/test_compat_persistence.py`,
  backed by a module-level encoding-version guard (`rdb_load_series` rejects any encver other
  than ours — RTS 8.6 writes encver 9 under the same `TSDB-TYPE` name).

### 7.5 Replication & persistence self-consistency (subject-only, reference-checked semantics)
Existing `tests/test_ts_replication.py` / `test_ts_aofrewrite.py` cover our own stack. Add
reference-diffed variants for semantics that RTS defines observably: effect-replication of `TS.ADD`
with `*` timestamp (replica must store the primary's timestamp), `TS.INCRBY` replication as
deterministic effect, and post-`DEBUG RELOAD` equivalence of `TS.INFO` + full `TS.RANGE` on both
engines independently (each engine must round-trip itself; the *diff* is on post-reload replies).

> **Observed correction (2026-07-16, `tests/compat/test_compat_replication.py`):** RTS 8.6 does
> *not* replicate auto-timestamp `TS.INCRBY` as a deterministic effect — it propagates the command
> verbatim and the replica stamps its own clock (30/30 divergent timestamps when probed). Only the
> explicit-`TIMESTAMP` form is deterministic. Both engines share the verbatim behavior, so the
> pinned semantics are: `*`-timestamp `TS.ADD` resolves before propagation (replica stores the
> primary's timestamp); explicit-`TIMESTAMP` `TS.INCRBY`/`TS.DECRBY` replicate exactly;
> auto-timestamp `TS.INCRBY` replicates the value but not the clock.

## 8. CI integration

- **PR gate (`compat-smoke`, ~5 min):** Tier A golden tests for commands touched by the diff plus
  a fixed smoke subset (CREATE/ADD/RANGE/MRANGE/INFO), RESP3 only, single reference version.
- **Nightly (`compat-full`):** all tiers, both RESP versions, full matrix, fuzzing budget,
  client suites. Publishes the **conformance report** artifact: per-command PASS / FAIL /
  DIVERGENT counts, new-vs-known divergence table, and the fuzzer corpus delta. Nightly failure
  pages the module owner (new divergence introduced upstream or by us).
- **Release gate:** `compat-full` green (with registry) is a release checklist item; the rendered
  compatibility page ships with the release notes.

## 9. Exit criteria

Bring-up is done when:

1. Tier A covers every row of the §6 matrix; zero unregistered mismatches.
2. Tier B: valkey-py and redis-py timeseries suites pass unmodified.
3. Tier C has run ≥ 14 consecutive nightly sessions with no new unregistered divergence.
4. §7.4 persistence behavior is pinned by tests and documented.
5. `divergences.yml` is reviewed and rendered into a public compatibility page.

## 10. Phasing

| Phase | Deliverable | Est. effort |
|---|---|---|
| 0 | Compose file, reference/subject fixtures, `DiffClient`, normalizer, registry format, CI smoke job | 1–1.5 wk |
| 1 | Tier A: write-path commands + INFO + config parity (§7.1) — expect this phase to *find* divergences; triage into fixes vs registry | 2 wk |
| 2 | Tier A: read-path matrix incl. compaction deep-dive; RESP2/RESP3 | 2 wk |
| 3 | §7.2–7.5 operational parity; persistence interop decision resolved | 1 wk |
| 4 | Tier B client suites; Tier C fuzzer + corpus loop; nightly report | 1.5 wk |

## 11. Risks & open questions

- **Reference drift:** Redis 8.6.x patch releases can change behavior; digest pinning + reviewed
  bumps mitigate. Track RTS release notes on bump.
- **Float formatting** is the most likely source of high-volume noise; the §5.1 policy decision
  (exact vs tolerance for sample values) should be made in Phase 0, deliberately, not under
  pressure of a red CI.
- **Cluster mode is a semantic superset, not a parity surface:** OSS RedisTimeSeries in cluster
  mode returns only local-shard data for multi-key queries; our fanout
  (`src/commands/fanout/`, `docs/fanout-compatibility-handshake.md`) answers cluster-wide. This is
  a headline `behavior` divergence entry with docs, not a test failure — but *single-shard*
  cluster replies (all keys on one slot via hash tags) should still be diffed against standalone
  RTS in Phase 2.
- **`TSDB-TYPE` name collision** (§7.4) is the highest-severity open decision: silent RDB
  misparse would be a data-integrity incident. Phase 0 should include a quick manual probe of both
  load directions to size the problem before the formal tests land.
- **Coverage gap from the clean-room constraint:** without replaying the upstream test suite
  (§4 — license-incompatible), edge cases that RTS's own tests encode but its documentation
  doesn't are only discoverable via the fuzzer and manual black-box probing of the reference
  server. Mitigate with a generous nightly fuzz budget and by promoting every *observed* behavior
  delta into a Tier A golden test. Contributors must not consult RedisTimeSeries source or test
  code when writing compat tests — document this rule in `tests/compat/README`.
