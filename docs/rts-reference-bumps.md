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

## Native binary artifact (current pin: 8.8.0)

| | |
| --- | --- |
| source | `https://packages.redis.io/deb/pool/<dist>/r/re/redis-server_8.8.0-1rl1~<dist>1_<arch>.deb` |
| tuples | jammy, noble, bookworm × amd64, arm64 (SHA256s in `tests/reference_server.sh`) |
| payload | `usr/bin/redis-server`, `usr/lib/redis/modules/redistimeseries.so` |
| equivalence run | **not yet performed** — every tuple is a *candidate* |

**Equivalence run (required before `auto` may select binary mode).** For a given
distro/arch, against the image pinned above:

1. Fingerprint both and diff: `INFO server` (`redis_version`, `redis_build_id`,
   `arch_bits`), `MODULE LIST`, `INFO modules`, the `INFO` field inventory in
   `tests/compat/info-fields-8.8.yml`, the `CONFIG GET ts-*` surface, and the RDB
   version footer produced by `SAVE`.
2. Run the full compat suite against each with identical flags and compare
   `test-data/compat-report.json` plus the pass/skip/xfail-divergent counts. The
   8.8.0 image baseline is 1368 passed / 6 skipped.
3. Equivalence means identical divergence sets and identical counts. Any delta is
   a finding — a new registry entry, or grounds to leave binary mode off for this
   pin.

Record the result here and flip the tuple's status to `verified` in
`tests/reference_server.sh`, along with
`_COMPAT_REF_EQUIVALENCE_VERIFIED_PIN`.

---

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
