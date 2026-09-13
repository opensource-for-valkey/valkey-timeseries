# AGENTS.md

Instructions for AI coding agents working in this repo. Keep this file concise — it's loaded into
every session, so prune anything an agent can discover by reading the code, and keep the "gotchas"
that would otherwise cost a wasted turn.

## IMPORTANT: clean-room rule

This repo is Apache-2.0. RedisTimeSeries — **including its test suite** — is RSALv2/SSPLv1/AGPLv3,
incompatible with that license.

**You MUST NOT consult, fetch, vendor, copy, or port RedisTimeSeries source or test code**, and must
not reproduce it from memory. Derive compatibility behavior only from public command documentation
and black-box observation of a running reference server. Running the pinned `redis:8.10` image as a
test target is fine; its source is off-limits. Details: [tests/compat/README.md](tests/compat/README.md).

## Common commands

Prerequisites: a Rust toolchain (edition 2024, MSRV 1.96) and nothing else — `protoc` is **not**
required (`build.rs` uses the pure-Rust [`protox`](https://docs.rs/protox) compiler, including for
regeneration via `VALKEY_TS_PROTO_REGEN=1`). If a `.proto` edit needs `buf lint` locally, install
[`buf`](https://buf.build); that's a separate, CI-only check from codegen drift.

```sh
# Build + lint (mirrors CI)
cargo fmt --check && cargo clippy --profile release --all-targets -- -D clippy::all \
  && RUSTFLAGS="-D warnings" cargo build --all --all-targets --release

# Local dev flow (recommended) — builds module + valkey-server, runs unit & integration tests
SERVER_VERSION=unstable ./build.sh
TEST_PATTERN="test_ts_add" SERVER_VERSION=unstable ./build.sh   # subset
RTS_COMPAT=1 SERVER_VERSION=unstable ./build.sh                 # + compat suite
ASAN_BUILD=true SERVER_VERSION=unstable ./build.sh              # ASAN pass
SERVER_VERSION=unstable ./build.sh --parallel=auto              # parallel integration tests (serial default)

# Unit / doc tests
cargo test --features enable-system-alloc
cargo test --doc --features enable-system-alloc

# Benchmarks & reports (--help on each script for flags)
cargo bench --features enable-system-alloc
tools/compression_report.sh [--check|--save-baseline]
tools/latency_report.sh
tools/wire_report.sh

# Compatibility fuzzer (needs Docker; strict mode required for a soak — see Warnings below)
./fuzz.sh --examples 20000 --duration 20m --stats
```

Key `./build.sh` env vars: `SERVER_VERSION` (required: `unstable`/`8.0`/`8.1`), `ASAN_BUILD`,
`TEST_PATTERN` (pytest `-k`), `PARALLEL_WORKERS`/`--parallel[=N]` (integration phase only — the
compat suite and ASAN job always run serially), `CLUSTER_MEET_TIMEOUT`, `RTS_COMPAT=1` (adds the
compat phase; mutually exclusive with `ASAN_BUILD`). `MODULE_PATH` is exported after build.

## Architecture

Valkey module (Rust crate) exposing `TS.*` commands via `valkey_module!` in `src/lib.rs`.

- `src/commands/` — one `ts_<name>.rs` file per command, exporting `ts_<name>_cmd`. Almost all are
  registered via `#[valkey_module_macros::command({...})]` on the handler, picked up by
  `register_commands`. Only `ts._debug` and `ts._restore` (internal/admin, undocumented) sit in the
  positional `commands:` table in `src/lib.rs` instead — see Conventions below for why.
- `src/series/` — storage, chunk encodings, compaction, background tasks, indexes, serialization.
  - `chunks/`: three encodings — **Chimp** (default), **Gorilla**, **Uncompressed**
    (`DEFAULT_CHUNK_ENCODING` in `src/config.rs`). Storage encoding is a user choice; cluster *wire*
    encoding is a separate, internal policy (see Conventions below).
  - Per-series ACL filtering: `acl.rs`.
- `src/fanout/` + `src/commands/*_fanout_command.rs` — cluster fanout over the protobuf contract in
  `proto/v1/`, registered via `register_fanout_operations` (8 ops: LabelStats, Card, LabelSearch,
  MDel, MGet, MRange, QueryIndex, QueryLabels).
  - `cluster_migrations.rs` — atomic slot migration (ASM, Valkey 9.0+) tracking. During an ASM the
    source node's forked `aof_rewrite` child can't take the module GIL, so it serializes via
    `rdb_save` and emits the internal `TS._RESTORE key <blob>` command instead of `DUMP`/normal
    commands; the destination replays it like a replication feed (`src/commands/ts_restore.rs`).
    Indexing is deferred mid-import (`src/series/index/server_events.rs`).
- Other command surfaces beyond RTS: `TS.JOIN` (`src/join/`), `TS.OUTLIERS` + statistical machinery
  (`src/analysis/` — ESD/CUSUM/EWMA/IQR/MAD/z-score/RCF), `TS.ADDBULK`, `TS.LABELSTATS`,
  `TS.METRICNAMES`, `TS.MDEL`, Prometheus-style selectors (`src/parser/`).
- Supporting: `src/aggregators/` (range-query aggregation), `src/common/` (encoding, logging, thread
  pools, RDB, interning), `src/labels/`, `src/iterators/`, `src/server_events.rs` (keyspace event →
  index sync for FLUSHDB/SWAPDB/RENAME/RESTORE/load).

## Conventions

- Commit messages follow Conventional Commits: `type(scope): summary` (e.g. `refactor(threads): ...`,
  `fix(series): ...`).
- **Command registration is two-part and both parts are enforced at compile/test time.** A handler
  gets `#[valkey_module_macros::command({...})]`, but that attribute sets no ACL categories — so each
  handler also needs an `acl_categories!(IDENT, "ts.name", "cats")` declaration immediately above it
  (`src/commands/mod.rs`). These feed a `linkme::distributed_slice` (`COMMAND_ACL_CATEGORIES`) that
  `assign_command_acl_categories` applies at load time, **aborting the module load** on an unknown
  name/category. A test (`every_annotated_command_declares_acl_categories`) pins the two counts to
  match, so a handler missing its declaration fails `cargo test`. `ts._debug`/`ts._restore` are the
  only exceptions — registered positionally in `valkey_module!`, which sets their categories directly.
- Wire encoding for cluster fan-out is decided in exactly one place — `samples_to_chunk[_lossless]`
  in `src/series/chunks/serialization.rs` (below `WIRE_COMPRESSION_MIN_SAMPLES`=16 samples:
  uncompressed; at/above: Chimp). Don't hand-roll encoding at a call site or add a third tier —
  both were tried and didn't survive measurement (see `tools/wire_report.sh`). `max_size` is
  advisory on this path (neither chunk type enforces it in `add_sample`, and fan-out never checks
  `is_full()`) — use `default()`.
- After editing a `.proto`: run `VALKEY_TS_PROTO_REGEN=1 cargo build` and commit the regenerated file
  under `proto/v1/generated/` — a normal build fails loudly if they disagree, so drift can't land
  silently.
- Behavior changes on the shared RTS surface: check against `tests/compat`, and if the difference is
  deliberate, record it in [COMPATIBILITY.md](COMPATIBILITY.md) and/or `tests/compat/divergences.yml`
  (behavior-kind entries need explicit PR sign-off).
- When adding/changing a command, update `docs/COMMANDS.md`, `docs/commands/`, `docs/overview.md`,
  and `README.md` (skip this for `TS._DEBUG`/`TS._RESTORE` — intentionally undocumented internals).

## Testing

- Unit/doc tests: `cargo test [--doc] --features enable-system-alloc`. Use `DataGenerator`
  (`crate::tests::generators`) for fixtures rather than hand-rolled loops.
- Integration: Python pytest under `tests/` (`test_ts_*.py`, `*_cme.py` = cluster-mode variants),
  driven by `./build.sh`.
- Compatibility harness (`tests/compat/`): diffs every reply against a pinned `redis:8.10` reference
  server, RESP2 + RESP3. Excluded from a plain `./build.sh`; opt in with `RTS_COMPAT=1` or
  `./build.sh compat`. Intentional mismatches go in `divergences.yml` as XFAIL-DIVERGENT — "reference
  errors, subject succeeds" always hard-fails and can't be registered away.
- Fuzzer (`tests/compat/test_compat_fuzz.py`, Hypothesis-driven): opt-in, not in the PR gate. Prefer
  `./fuzz.sh`; promote any shrunk failure into `tests/compat/corpus/<slug>.json` so it becomes a
  deterministic regression test (`test_compat_corpus.py`) in the same change as the fix.

## Warnings / gotchas

- **`enable-system-alloc` is mandatory** for anything linking the crate outside a live server (tests,
  doctests, benches, `tools/` binaries) — without it the binary SIGABRTs at startup
  (`Critical error: the Valkey Allocator isn't available`). `build.sh` passes it for you.
- **Rebuild after every pull/branch switch.** The module binary isn't tracked in git; a stale build
  causes opaque failures like empty `CONFIG GET` or `fuzz.sh` reporting the module isn't loaded.
- A `build.rs` failure saying the generated proto file "is missing/out of date" is **schema drift**,
  not a missing-toolchain problem — fix with `VALKEY_TS_PROTO_REGEN=1 cargo build`, not by installing
  `protoc`.
- **Never run the fuzzer against `extended` compat mode for a soak.** The subject defaults to
  `extended`, so gated divergences fail it as new bugs and Hypothesis stops in ~30s. `fuzz.sh` sets
  strict mode for you; driving pytest directly means doing it yourself
  (`CONFIG SET ts.ts-compatibility-mode strict`).
- `ASAN_BUILD` and compat mode (`RTS_COMPAT=1`) are mutually exclusive in `build.sh`.
- A `[[bin]]` target (e.g. `compression_report`) doesn't pull in dev-dependencies, so
  `cargo run --bin compression_report` needs `--features enable-system-alloc,test-utils` named
  explicitly, even though `cargo test`/`cargo bench` get `test-utils` automatically via the
  self dev-dependency.

## Where to look first

`build.sh`, `Cargo.toml`, `src/lib.rs`, `src/commands/*`, `src/series/*`, `tests/`,
[COMPATIBILITY.md](COMPATIBILITY.md), [tests/compat/README.md](tests/compat/README.md),
`docs/COMMANDS.md`, `docs/overview.md`.

This file favors discoverable, executable facts over domain rationale — `docs/` (including
in-progress `docs/plans/`) carries the deeper design and investigation notes.
