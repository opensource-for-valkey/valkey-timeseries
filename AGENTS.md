# AGENTS: Guidance for AI coding agents

Purpose

- Short, focused instructions to help an AI model become productive in this codebase quickly.

Quick start (commands you can run)

- Docker (containerized):
    - `make docker-build && make docker-up`  # build image, start standalone container
    - `make docker-up-cluster`               # start 3-node cluster for fanout testing
    - `make docker-test`                     # run integration tests against container
    - `make docker-down`                     # stop and remove containers
- Build + checks:
  `cargo fmt --check && cargo clippy --profile release --all-targets -- -D clippy::all && RUSTFLAGS="-D warnings" cargo build --all --all-targets --release`
- Local dev script (recommended):
    - `SERVER_VERSION=unstable ./build.sh`  # builds module, builds valkey-server, runs unit & integration tests
    - To run ASAN integration pass: `ASAN_BUILD=true SERVER_VERSION=unstable ./build.sh`
    - Run a subset of Python integration tests: `TEST_PATTERN="test_ts_add" SERVER_VERSION=unstable ./build.sh`
- Benchmarks: `cargo bench --features enable-system-alloc` (see Benchmarks below — the feature is mandatory).
- Compression report: `tools/compression_report.sh` (add `--check` to fail on regressions against a saved baseline).
- Latency report: `tools/latency_report.sh`. Wire-payload report: `tools/wire_report.sh` (see Benchmarks below).

Key ENV and behavior (from `./build.sh`)

- `SERVER_VERSION` (required): controls which valkey-server is cloned/built and stored at
  `tests/build/binaries/$SERVER_VERSION/valkey-server`. Valid values are `unstable`, `8.0`, and `8.1`; defaults to
  `unstable` if not set, which tracks the latest main branch.
- `ASAN_BUILD`: when set runs tests with LeakSanitizer checks and fails on leaks.
- `TEST_PATTERN`: passed to pytest `-k` to select tests.
- `MODULE_PATH` exported after build: `target/release/libvalkey_timeseries{.so,.dylib}` depending on OS.

Setup & Environment Notes

- Rust version: The project requires a minimum Rust version of `1.88`.
- Python tests: Integration tests use Python. Dependencies are managed in `pyproject.toml`, locked in `uv.lock`, and
  mirrored in `requirements.txt` (or installed via `uv sync`). The `build.sh` script handles this, but if running
  `pytest` manually, ensure packages are installed.
- Protobuf generation: `build.rs` compiles `src/commands/fanout.request.proto`, `src/commands/fanout.response.proto`,
  and `src/promql/types.proto`. If you change those files, rerun a build so the generated code under `OUT_DIR` is
  refreshed.
- Running manually: To manually start a server with the module loaded, run
  `valkey-server --loadmodule ./target/release/libvalkey_timeseries.so` (requires building the module first).

High-level architecture (big picture)

- This is a Valkey module (Rust crate) exposing TS.* commands to the Valkey server via the `valkey_module!` macro (
  `src/lib.rs`).
- Command implementations live in `src/commands/*` and are registered in `src/lib.rs` with a one-to-one mapping to
  Valkey commands. Example:
    - `["TS.ADD", commands::ts_add_cmd, "write deny-oom", 1, 1, 1, "write timeseries"]`
- Time-series core lives under `src/series` (storage, encoding, background tasks, indexes). Index/init helpers:
  `init_croaring_allocator()` and `init_background_tasks()` are invoked from `src/lib.rs`.
  - `src/series/chunks/` implements three encoding formats: **Chimp** (ELF-on-Chimp, default),
    **Gorilla**, **Uncompressed**. The default is controlled by `DEFAULT_CHUNK_ENCODING` in `src/config.rs`.
    Storage encoding is the user's choice; the encoding used for cluster *wire* payloads is a separate, internal policy —
    see "Wire encoding policy" under conventions below.
  - ACL filtering per series: `src/series/acl.rs`.
- PromQL lives under `src/promql/` (parser/engine/types, generated bindings, and `promqltest` fixtures); it is
  registered from `src/lib.rs` via `register_promql()` when clustered.
- Statistical/anomaly analysis helpers live under `src/analysis/`.
- Server keyspace event handling lives in `src/server_events.rs`; it keeps indexes in sync for `FLUSHDB`, `SWAPDB`,
  `RENAME`, `RESTORE`, and module load events.
- Cross-node fanout / clustering patterns: `src/fanout` and `src/commands/*_fanout_command.rs` use the protobuf wire
  contract in `proto/v1/` and explicit fanout registration (`register_fanout_operations`) to implement
  cluster-wide queries.

Project-specific conventions & patterns

- All Valkey commands are declared in the `valkey_module!` macro in `src/lib.rs`; change there to add/remove commands.
- Command files follow `ts_<command>.rs` naming and export `ts_<command>_cmd` functions (see `src/commands/mod.rs`).
- Fanout pattern: synchronous local implementation + `*_fanout_command.rs` files which marshal/unmarshal protobuf
  messages for cluster aggregation. Seven operations are currently registered (see `register_fanout_operations` in
  `src/commands/mod.rs`): `LabelStatsFanoutCommand`, `CardFanoutCommand`, `LabelSearchFanoutCommand`,
  `MDelFanoutCommand`, `MGetFanoutCommand`, `MRangeFanoutCommand`, `QueryIndexFanoutCommand`.
- Wire encoding policy: `samples_to_chunk` / `samples_to_chunk_lossless` in `src/series/chunks/serialization.rs` are the
  **single** decision point for how samples are encoded onto the cluster wire. Two tiers, keyed on sample count:
  below `WIRE_COMPRESSION_MIN_SAMPLES` (16) an `UncompressedChunk`, at or above it a `ChimpChunk`. Do not add a third
  tier or hand-roll an encoding at a call site — both were true before and neither survived measurement (see the wire
  report below): Chimp is smaller than Gorilla from ~12 samples up and cheaper to decode from ~16, so Gorilla has no
  window where it wins, and below ~12 every compressed encoding can produce a payload *larger* than the raw samples.
  Two things about this are easy to get wrong:
    - **Only data that actually crosses the network is compressed.** `handle_grouping` in `src/series/mrange.rs` runs
      solely on the node answering the client (`process_mrange` returns early when clustered), so it builds uncompressed
      chunks; compressing there would only be undone by the reply serializer. The clustered branch of
      `handle_non_grouped`, and `serialize_rows` in `src/commands/fanout_codec/chunks.rs`, are the paths that do compress.
    - **`max_size` is advisory on this path.** Neither `ChimpChunk` nor `GorillaChunk` enforces it in `add_sample` (the
      check is commented out in `gorilla_chunk.rs`), and the fan-out path never calls `is_full()`, so passing a
      `with_max_size(...)` budget there truncates nothing — it only widens the uvarint `max_size` occupies on the wire.
      Use `default()`.
  Chimp encoding is ~1.4x slower than Gorilla but decodes ~12% faster, which is the right way round for fan-out: shards
  encode their own slice in parallel while the coordinator decodes every shard's response serially.
- Initialization sequence (inside `initialize()` in `src/lib.rs`): `init_croaring_allocator` → `register_config` →
  `init_fanout` + `register_fanout_operations` (cluster only) → `register_server_events` → `init_thread_pool` →
  `init_background_tasks`.
- Minimum supported Valkey server version: `[8, 0, 0]` (enforced in `preload()` via
  `config::TIMESERIES_MIN_SUPPORTED_VERSION`).

- Allocator in tests: always pass `--features enable-system-alloc` when running anything that links the crate outside a
  live server (unit tests, doc tests, benches, `tools/` binaries). The `get_allocator!` macro in `src/lib.rs` is gated on
  `#[cfg(not(all(test, doctest)))]`, which does **not** fire for an ordinary `cargo test`, so without the feature the
  binary aborts at startup with `Critical error: the Valkey Allocator isn't available`. `build.sh` passes it for you.

Cargo features

- `default` = `min-valkey-compatibility-version-8-0` + `croaring/alloc`.
- `enable-system-alloc` — see the allocator note above; required for tests, benches and `tools/` binaries.
- `min-valkey-compatibility-version-8-0` — forwarded to `valkey-module`.
- `valkey_8_0` — controls compatibility with Valkey 8.0 (build with `--features valkey_8_0` or set
  `SERVER_VERSION=8.0` in `./build.sh`).
- `use-redismodule-api` — empty on purpose; the Redis module API is not supported.
- `test-utils` — compiles `src/tests/` (data generators, chunk helpers) into the library so benches and
  `tools/` binaries can use the same fixtures as unit tests. It is enabled automatically for dev targets by a
  **self dev-dependency** (`valkey-timeseries = { path = ".", features = ["test-utils"] }` in `[dev-dependencies]`),
  so `cargo test`, `cargo bench` and any `--all-targets` build get it without extra flags. A `[[bin]]` does not pull in
  dev-dependencies, so `cargo run --bin compression_report` must name the feature explicitly — cargo otherwise refuses
  with `target requires the features: test-utils`. Note that in an `--all-targets` build the feature is unified into the
  library build too, so the `.so`/`.dylib` produced by `build.sh` contains the (unreachable) fixture code; a plain
  `cargo build --release` does not.

Testing & debugging notes

- Unit tests: `cargo test --features enable-system-alloc`.
- Doc tests: `cargo test --doc --features enable-system-alloc`.
- Test fixtures: build sample data with `DataGenerator` (`src/tests/generators/`, imported as
  `crate::tests::generators::{DataGenerator, ValueWorkload, TimestampModel}`) rather than hand-rolling loops:
  `DataGenerator::builder().start(ts).samples(n).seed(s).algorithm(ValueWorkload::Drift).build().generate()`.
  `ValueWorkload` covers the four range-bounded random generators (`Uniform`, `StdNorm`, `MackeyGlass`, `Deriv`, which
  honour `.values(range)`) plus twelve absolute-valued shapes (`Constant`, `ConstantInt`, `Drift`, `Periodic`, `Noisy`,
  `Bursty`, `Counter`, `Discrete`, and the decimal-quantized variants `DriftQuantized`, `PeriodicQuantized`,
  `NoisyQuantized`, `BurstyQuantized` — same seeded values rounded to two decimals, see `quantized()`/`is_quantized()`;
  all of them ignore the range — see `is_workload()`). `TimestampModel` controls spacing
  (`Regular`, `Jitter`, `Irregular`). `DataGenerator::dataset(workload, model, samples, seed)` is the one-line form used
  by the benchmark matrix.
- Integration tests: Python pytest under `tests/` and rely on a built `valkey-server` and `tests/valkeytestframework`
  helper files (populated by `./build.sh`).
- To reproduce integration runs locally: run `SERVER_VERSION=unstable ./build.sh` — this will clone/build Valkey and
  copy the server binary to `tests/build/binaries/`.
- Leak detection: when `ASAN_BUILD` is set, the build script scans pytest output for LeakSanitizer output and fails if
  leaks are detected.
- PromQL benchmarks: `cargo bench --bench promql_engine --features enable-system-alloc` runs the Criterion suite in
  `benches/promql_engine.rs`; set `BENCH_PROFILE=ci` for the shorter CI-style profile.
- PromQL tests are generated by `build.rs` from `src/promql/promqltest/testdata/*.test` into
  `OUT_DIR/promql_tests_generated.rs`, so rerun a build after editing those fixtures.

Benchmarks

- Criterion benches live in `benches/` and are registered in `Cargo.toml` with `harness = false`: `encode`, `decode`,
  `query_scan`.
- **`--features enable-system-alloc` is required.** Bench and tool binaries link the crate's global allocator
  (`AlignedValkeyAlloc`), which needs a loaded Valkey runtime; without the feature every one of them aborts at startup
  with `Critical error: the Valkey Allocator isn't available` (SIGABRT). Same constraint as `cargo test`.
- Commands:
    - All benches: `cargo bench --features enable-system-alloc`
    - One target: `cargo bench --features enable-system-alloc --bench decode`
    - Filter by name: `cargo bench --features enable-system-alloc --bench decode -- gorilla`
    - Smoke run (executes each case once, no measurement — fast way to confirm benches still build and run):
      `cargo bench --features enable-system-alloc --bench decode -- --test`
- Groups: `encode_bulk` / `encode_append`, `decode_full` / `decode_materialize`, `scan` / `scan_filtered`. Bench ids are
  `encoding/workload/timestamp_model/chunk_size`.
- Shared fixtures live in the crate itself (`src/tests/`, exposed to dev targets by the `test-utils` feature) and are
  re-exported through `benches/support/mod.rs`, so benches, unit tests and the `tools/` report binaries all generate data
  through the same `DataGenerator`. `DatasetRegistry` builds 16 datasets of 64k samples from fixed seeds
  (the 12 `ValueWorkload` shapes at regular timestamps, plus drift/noisy at jitter and irregular timestamps), so results
  are comparable across runs, machines, and commits. The matrix is defined in `src/tests/generators/dataset.rs`
  (`benchmark_dataset_keys`, `DatasetKey`, `DATASET_SAMPLES`, `dataset_seed`); chunk sizes are 1k / 4k
  (`DEFAULT_CHUNK_SIZE_BYTES`) / 64k.
- `encode`, `decode` and `query_scan` all pass `-- --test`.
- Compression report (not a criterion bench): run it with `tools/compression_report.sh`, which wraps
  `cargo run --release --features "enable-system-alloc,test-utils" --bin compression_report` (both features must be
  named explicitly for a `[[bin]]`; see Cargo features above). It writes
  `target/bench-reports/compression.csv` and `.md` (84 rows: encoding × workload × timestamp model × chunk size —
  28 rows for each of the 3 encodings listed in `encodings()`).
  The `data_size`, `bytes_per_sample` and `ratio` columns come from `chunk_utils::encoded_size`, the bytes the encoder
  actually wrote. Do **not** switch them to `ChunkOps::size()`: gorilla and chimp report a `get_size()`
  heap footprint there (buffer *capacity*, which doubles), while uncompressed reports bytes in use, so a ratio
  built on it compares allocator slack instead of compression. The separate `size` column is the full heap footprint,
  including unused capacity.
  Script flags: `--check` fails if any compression ratio drops more than 5% below the baseline, `--save-baseline`
  records the run just made as the baseline, `--baseline <path>` overrides the default
  `benches/baselines/compression_baseline.csv`. `--check` exits 2 when that file is missing. Datasets are built from
  fixed seeds, so a re-run reproduces the baseline exactly; regenerate it with `--save-baseline` after any intentional
  encoder or dataset change, and review the diff rather than saving blind. `dataset_seed` hashes the dataset key
  (`workload/ts_model`) rather than its position in the matrix, so adding or reordering workloads leaves every other
  dataset — and its baseline row — byte-for-byte identical.
- `--by-workload [metric]` additionally writes a pivoted view —
  `target/bench-reports/compression_by_workload_<metric>.{csv,md}` — with one table per chunk size, one row per
  workload/timestamp model, and one column per encoding. `metric` is `ratio` (default), `bytes-per-sample`, or
  `capacity`; in the Markdown the best encoding per row is bold, with `uncompressed` excluded as the baseline. This is
  the layout to reach for when comparing encodings against each other; the flat `compression.csv` stays the source of
  truth for `--check`.
- Latency report (also not a criterion bench): `tools/latency_report.sh` wraps the `latency_report` binary
  (`tools/latency_report.rs`, same two required features). It answers "how fast" where the compression report answers
  "how small": for each encoding it times `set_data`, per-sample `add_sample`, a full `iter()` scan, `get_range` over the
  whole chunk, and a 10% mid-chunk `range_iter`, then writes `target/bench-reports/latency.{csv,md}` and prints the
  table. Every cell is the median of `--iterations` runs (default 200, after `--warmup` 20) of the *whole* operation, in
  µs. Parameterize with `--samples` (default 1000), `--encodings`/`--workloads`/`--ts-models` (comma lists or `all`),
  `--chunk-size` (default 1 MiB so nothing fills up), `--seed`, `--out-csv`/`--out-md`, `--quiet`. There is no baseline
  gate: these are wall-clock numbers, so compare rows within one run rather than across machines or commits. Note that
  `set_data` bulk-loads past the chunk budget while `add_sample` stops at `is_full()`, so a small `--chunk-size` makes
  the two encode columns cover different sample counts — the tool warns and records `append_len` in the CSV when that
  happens.
- Wire-payload report (also not a criterion bench): `tools/wire_report.sh` wraps the `wire_report` binary
  (`tools/wire_report.rs`, same two required features). It exists because neither of the other two answers the question
  the clustered fan-out path asks — `compression_report` fills chunks to capacity and `latency_report` uses a single
  fixed sample count, while the encoding threshold lives at small `n`. Each row replays the real round trip from
  `src/commands/fanout_codec/chunks.rs` (shard: `set_data` + `Chunk::serialize`; coordinator: `deserialize` +
  `iter().collect()`) and reports `wire_bytes` — the exact `SampleData::data` payload, not `encoded_size` — plus encode
  and decode medians, swept across `--sample-counts`. This is the tool to re-run when changing
  `WIRE_COMPRESSION_MIN_SAMPLES` or the wire encoding; it writes `target/bench-reports/wire.{csv,md}`.
  Two features carry the analysis:
    - **A correctness gate runs before any measurement.** Every encoding is put through adversarial payloads (NaN,
      infinities, `-0.0`, subnormals, `f64::MIN/MAX`, timestamp extremes, duplicate timestamps) and must round-trip
      bit-exactly. This is not academic: the grouped/aggregated path back-fills empty buckets with NaN, so an encoding
      that cannot carry one is unusable on the wire whatever it scores on size.
    - **`break_even` is a link speed in Gbit/s**, not a ratio: the bandwidth below which the bytes an encoding saves take
      longer to transmit than the extra CPU takes to spend. Compare it against the interconnect — an encoding pays off on
      any link *slower* than its figure, and `--link-gbps` (default 10) sets what the threshold summary is judged
      against. Worth knowing before optimizing this path: Chimp's whole-round-trip break-even is ~1.2–1.7 Gbps
      (~4–5 Gbps counting coordinator decode only), so on a 10–25 Gbps in-rack interconnect wire compression is a net
      latency *loss* at every sample count. It is a bandwidth-and-egress-cost measure, not a latency optimization; treat
      claims to the contrary as unmeasured.
  Other flags mirror the latency report: `--encodings`/`--workloads`/`--ts-models` (comma lists or `all`; `uncompressed`
  is always kept, since it is the baseline every delta is measured against), `--iterations`/`--warmup`, `--seed`,
  `--out-csv`/`--out-md`, `--quiet`. Wall-clock again, so no baseline gate — compare rows within one run.
- `build.sh` does not run benches or any of the three report tools; they are manual.

Where to look first (key files & directories)

- `src/lib.rs` — module entrypoint, command registration, lifecycle (preload/init/deinit).
- `src/commands/` — implementations and command parsing utilities (`command_parser.rs`).
- `src/series/` — core storage, encodings, indexes, background tasks.
- `src/promql/` — PromQL parser, execution engine, generated bindings, and test fixtures.
- `src/analysis/` — anomaly-detection and statistical analysis helpers.
- `src/server_events.rs` — keyspace event subscriptions and index maintenance.
- `src/fanout/` — cluster communication primitives.
- `src/aggregators/` — aggregation handlers for range queries.
- `src/common/` — shared utilities (encoding, logging, thread pool, RDB, string interning).
- `src/labels/` — label types and filter evaluation.
- `src/parser/` — filter syntax, metric name, timestamp, and duration parsers.
- `src/iterators/` — sample and row iterators.
- `src/join/` — ASOF join for TS.JOIN.
- `src/tests/` — shared test/bench support, compiled under `cfg(test)` or the `test-utils` feature:
    - `generators/rand.rs` — `DataGenerator` (bon builder) and `ValueWorkload`.
    - `generators/workload.rs` — the shape functions (drift/periodic/noisy/bursty/counter/discrete) and `TimestampModel`.
    - `generators/generator.rs`, `generators/mackey_glass.rs` — the range-bounded iterator generators.
    - `generators/dataset.rs` — `DatasetKey`, `DatasetRegistry`, and the benchmark dataset matrix.
    - `chunk_utils.rs` — `build_chunk`, `build_chunk_until_full`, `filled_prefix(_len)`, `encoded_size`, `CHUNK_SIZE_*`.
- `benches/` — criterion benchmarks; `benches/support/mod.rs` just re-exports `src/tests/` so benches, unit tests and the
  `tools/` report binaries share one implementation.
- `benches/promql_engine.rs` — Criterion benchmarks for PromQL evaluation and joins.
- `tools/` — the three report binaries, each with a `.sh` wrapper that builds and runs it with the right features:
    - `compression_report.rs` — encoding size/ratio matrix at chunk capacity, with baseline checking ("how small").
    - `latency_report.rs` — encode/decode/scan timings at a fixed sample count ("how fast").
    - `wire_report.rs` — serialized payload bytes and round-trip cost swept across sample counts, plus the correctness
      gate; the tool behind `WIRE_COMPRESSION_MIN_SAMPLES` ("is shipping this compressed worth it").
- `build.sh` — canonical developer flow for formatting, linting, building, and running tests.
- `Dockerfile` / `Dockerfile.source` — containerized builds (official Valkey base vs. full source).
- `docker-compose.yml` / `docker-compose.cluster.yml` — standalone and 3-node cluster testing.
- `scripts/docker-entrypoint.sh` — runtime configuration via env vars for Docker containers.
- `scripts/build-docker.sh` — helper for building Docker images with different versions/features.
- `Makefile` — unified interface wrapping Docker and host-native commands.
- `README.md`, `docs/COMMANDS.md`, and `docs/commands/` — human-facing command descriptions and examples.

Quick tips for code changes

- Add new commands: create `src/commands/ts_<name>.rs`, add function `ts_<name>_cmd`, then register in `valkey_module!`
  in `src/lib.rs`.
- Documentation: When adding or modifying commands, remember to update the human-facing docs in `docs/COMMANDS.md`,
  `docs/commands/`, `docs/overview.md`, and the supported list in `README.md`.
- When making cluster changes, search for `*_fanout_command.rs` to copy the fanout pattern and add protobuf messages in
  `proto/v1/`.
- Protobuf codegen is **checked in** at `proto/v1/generated/valkey_timeseries.fanout.v1.rs`. After editing any `.proto`, run
  `VALKEY_TS_PROTO_REGEN=1 cargo build` and commit the regenerated file; a normal build fails with instructions if the
  two disagree, so drift cannot land silently. Local↔wire conversions live beside it in `src/commands/fanout_codec/`
  (named `fanout_codec` rather than `fanout` so it does not collide with the `src/fanout/` transport layer).
- When changing `src/promql/types.proto` or `src/promql/promqltest/testdata/*.test`, rerun `cargo test` or `./build.sh`
  so `build.rs` regenerates the derived Rust files.
- When changing the Docker setup, rebuild with `make docker-build` (or `./scripts/build-docker.sh` for custom
  versions); the `Dockerfile.source` variant is for testing against unreleased Valkey versions.

Limitations of this document

- Focused on discoverable, executable patterns. It does not cover domain rationale beyond what is visible in
  source/docs.

If you need more context, inspect:

- `build.sh`, `Cargo.toml`, `src/lib.rs`, `src/commands/*`, `src/series/*`, and `tests/`.
