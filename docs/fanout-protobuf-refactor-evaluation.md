# Protobuf Refactoring Plan — Evaluation

This is a point-by-point technical evaluation of the protobuf refactoring proposal, written after inspecting every referenced file, the full `build.rs` / `src/commands/fanout/` / `src/fanout/` stack, and the CI workflow.

---

## Summary Verdict

The plan is **largely sound** and would improve the codebase materially. However, three substantive corrections are needed:

1. **Phase D is unsafe as currently phrased.** The `applied_*` flags in `MultiRangeResponse` are *request-echo fields*, not a generic capability-negotiation channel. A compact response layout (symbol table, single timestamp column, delta-encoded bucket timestamps) must use a **new request opt-in field** plus a **response acknowledgement** — the request field tells the shard "the coordinator can decode this format," and the response echo confirms "I honored it." Without the request opt-in, an old coordinator receiving a compact response would silently misinterpret it.

2. **`buf breaking` is incompatible with message deletion in the same commit.** Buf's `FILE_NO_DELETE` and `MESSAGE_NO_DELETE` rules treat deleting a public message as breaking by default. Establish the breaking baseline *after* the cleanup commits, or add explicit exemptions (`buf.yaml` `ignore_only`) for the known-removed messages in the deletion commit.

3. **The `.v1` package suffix does not version the wire protocol.** The package name is absent from the encoded bytes; dispatch is by the handler string (e.g. `"mrange"`) in the envelope header, not by message type. A future breaking `v2` needs a distinct handler or a version-dispatch path — a package rename alone is neither necessary nor sufficient for that. The `.v1` suffix is still useful for source organisation and `buf` configuration, but it should not be framed as wire-protocol versioning.

The rest — colocation, dead-schema deletion, structural merges, enum prefixing, `rerun-if-changed`, label-conversion dedup, `map_enum!`, round-trip proptests, and the `protox` swap — is **correct, well-motivated, and safe**.

---

## Detailed Findings

### 1. Colocation → `proto/valkey_timeseries/fanout/v1/`

**Verdict: Yes, with corrections.**

The current layout puts `.proto` files inside `src/commands/` while all consumers live in `src/commands/fanout/` and the transport layer in `src/fanout/`. A top-level `proto/` tree is the right move for the reasons stated: it treats the schema as an interface contract, enables `buf`, and stops Rust tooling from indexing `.proto` files.

**Correction — `.v1` suffix:** The package suffix does *not* appear on the wire (protobuf package names are purely a code-generation concern). A future breaking `v2` requires either a distinct handler name (e.g. `"mrange_v2"`) registered alongside `"mrange"`, or a version field in the envelope header — not just a package rename. The `.v1` suffix is still worth adopting now because it:
- Makes the `buf` module path unambiguous.
- Lets a `v2` package coexist in the source tree without renaming.
- Changes the generated Rust filename, which is a one-line edit in `mod.rs`.

**Correction — renaming `src/commands/fanout/`:** The proposal to rename it `fanout_codec/` or move it into `src/fanout/codec/` is sound — the collision between `src/commands/fanout/` and `src/fanout/` is a real navigation hazard. However, this is a Rust-wide rename touching import paths in every fanout command file. Do it after the schema move, not concurrently.

**Costs confirmed minimal:**
- `build.rs` needs new include paths (trivial).
- The crate is not published, so `Cargo.toml` `include` is irrelevant.
- Two path references in `AGENTS.md` need updating (lines referencing `.proto` locations).

**Additional finding — `docs/fanout-compatibility-handshake.md` does not exist yet.** The code references it in five places (`fanout_message.rs`, `cluster_rpc.rs`, `fanout_error.rs`, `config.rs`, `overview.md`) but the file is missing from `docs/`. This document should be written as part of Phase B since it captures the exact version-skew strategy the `applied_*` fields and `required_features` bits implement.

---

### 2. Dead Schema — Verified

**Verdict: Confirmed. All ten declarations are dead.**

A full-text search across `src/`, `tests/`, and `benches/` (all `.rs`, `.proto`, `.py` files) found no referent for any of:

| Declaration | Why dead |
|---|---|
| `MatchersConnector` | Enum; never imported or converted |
| `LabelNamesRequest` | Replaced by `LabelSearchRequest` |
| `LabelValuesRequest` | Replaced by `LabelSearchRequest` |
| `MetadataRequest` | Unused |
| `SearchQueryRequest` | Unused |
| `ClusterMessage` | Transport is hand-rolled in `fanout_message.rs` |
| `SeriesChunk` | Actual chunk transport uses `SampleData` |
| `ErrorResponse` | Errors use `FanoutError` serialisation |
| `LabelNamesResponse` | Replaced by `LabelSearchResponse` |
| `LabelValuesResponse` | Replaced by `LabelSearchResponse` |

That is 10 out of 29 messages (34%) and 2 out of 11 enums (18%) — a meaningful build-time and cognitive win.

**Correction — `reserved` does not apply to deleted messages.** The plan says "add `reserved` entries for every field the deletions above vacate." This is incorrect: `reserved` applies to fields *within* a retained message, preventing accidental reuse of field numbers. Deleting an entire message does not "vacate" field numbers that can be referenced by another message — protobuf field numbers are scoped to the enclosing message, not the file. The only `reserved` additions needed are:
- `reserved 1;` in `SeriesSelector` (field 1 was dropped in a prior edit; the oneof now starts at 2).
- `reserved 7;` in `LabelSearchRequest` (field 7 was deleted without a marker; fields jump from 6 to 8).

---

### 3. Structural Dedup — Verified Wire-Compatible

**Verdict: Correct. All four merges are byte-for-byte identical on the wire.**

| Proposed merge | Field tags | Wire types | Reason safe |
|---|---|---|---|
| `IndexQueryRequest` + `CardinalityRequest` → `MetaQueryRequest` | Both: `1: MetaDateRangeFilter`, `2: repeated SeriesSelector` | Both: LEN, LEN | Identical encoding |
| `MDelResponse` + `CardinalityResponse` → `CountResponse` | Both: `1: uint64` | Both: VARINT | Identical encoding |
| `IndexQueryResponse` + `LabelNamesResponse` + `LabelValuesResponse` → `StringListResponse` | Both: `1: repeated string` | Both: LEN | Identical encoding |

The critical enabling fact: dispatch is by handler name (e.g. `"queryindex"`, `"mdel"`, `"card"`) not by message type. Each `FanoutCommand` implementation defines `type Request` and `type Response` as associated types; changing those types to the merged names does not alter what bytes are sent or how they are decoded. The handler name in the envelope header selects the decoder.

**`serialize_match_filter_options` benefit confirmed.** The function at `conversions.rs:387` already treats `(Option<FanoutMetaDateRangeFilter>, Vec<FanoutSeriesSelector>)` as a unit. Returning a `MetaQueryRequest` instead of a tuple simplifies all call sites that destructure it.

**Enum-value prefixes:** The plan correctly notes that `prost-build` defaults `strip_enum_prefix: true`. This is confirmed in the installed `prost-build 0.14.4` source (`config.rs:1207`). Adding `AGGREGATION_TYPE_` / `MATCHER_OP_TYPE_` / etc. prefixes to proto enum values produces *zero* Rust-side diff — the generated variants remain `Max`, `Equal`, etc. — and zero wire diff (enum values are encoded as integers). The protection is purely against future collisions in the protobuf package namespace.

**Recommendation:** Add a compile-time assertion in `build.rs` that `strip_enum_prefix` is `true`, so a `prost-build` upgrade cannot silently change enum variant names.

---

### 4. Rust-Side Dedup — Verified

**Verdict: All findings confirmed.**

**Label conversion duplication:** `chunks.rs:192` (`convert_labels`) manually constructs `FanoutLabel { name, value }` from `Label`, and the inverse at `chunks.rs:218-226` manually destructures. Both are exact duplicates of the `From` impls at `conversions.rs:395-419`. Replacing with `.map(Into::into).collect()` is a pure simplification.

**Missing `From<Sample> for FanoutSample`:** `conversions.rs:359` constructs `FanoutSample { timestamp, value }` inline from a `Sample`. Adding a `From` impl is trivial and eliminates the last remaining inline construction.

**`AggregationType` maps:** The 23-arm × 2 `From` impls at `conversions.rs:208-281` are the single largest maintenance hazard in the file. A `map_enum!` declarative macro emitting both `From` impls from one variant table is the right approach. Alternative approaches considered and rejected:
- `#[repr(i32)]` + discriminants pinned to proto numbers: silently couples core types to wire numbering; a proto renumber would compile but break the wire.
- `strum` derive: already a dependency, but cannot express asymmetric mappings like `Share` ↔ `ShareIf`.

**Round-trip properties:** `proptest` is already a dev-dependency (`Cargo.toml:62`). Per-message-type round-trip tests (`local → proto → local`) are cheap to add and would have caught the `Share`/`ShareIf` naming asymmetry. Recommended strategy: one property per message type, with separate explicit test cases for known edge cases (invalid enum discriminants, NaN/infinity/subnormal float values, empty repeated fields).

---

### 5. Encoding Optimisations — Requires Gating

**Verdict: All are valid optimisations, but the implementation sequencing needs correction.**

#### 5.1 Multi-aggregation timestamps (highest payoff)

**Correct analysis.** `serialize_rows` at `chunks.rs:94` transposes rows into one `SampleData` column per aggregator, each carrying `(bucket_ts, value)` pairs. A 4-aggregator query ships the timestamp series 4×. The fix — one timestamp column + value-only columns — is conceptually clean.

**Correction — cannot use existing `applied_*` flags for this.** The `applied_aggregation` / `applied_group_reduce` / `applied_count` fields in `MultiRangeResponse` are *response echo fields*: the coordinator sets `apply_aggregation = true` in the request, and the shard echoes `applied_aggregation = true` in the response to confirm it honoured the flag. They are not a capability advertisement — they confirm what the request already asked for.

For a compact column layout:
1. Add `bool compact_columns = 10` (or similar) to `MultiRangeRequest`.
2. Add `bool applied_compact_columns = 6` to `MultiRangeResponse`.
3. The coordinator sets `compact_columns = true` in the request.
4. The shard, if it understands the flag, returns timestamp+value-only columns and sets `applied_compact_columns = true`.
5. The coordinator checks `applied_compact_columns` per shard response and falls back to the legacy layout for any shard that didn't set it (old-node or degraded path).

**The same pattern applies to symbol tables and delta-encoded bucket timestamps.** Each needs its own request opt-in + response echo. They can share a single `compact_format` bit if always deployed together, but independent flags give more flexibility and match the existing `apply_aggregation` / `apply_group_reduce` pattern.

**Additional finding — `SampleData.data` is `Vec<u8>`, not `bytes::Bytes`.** The plan's point 6 about `config.bytes(&["."])` is stale: `build.rs` does not call `.bytes()`, and `SampleData.data` is `Vec<u8>`. The `registry.rs` path passes `&[u8]` into the handler, so switching to `Bytes` would add an allocation (or at best a no-op) rather than save one. Drop this point.

#### 5.2 Symbol table for labels/keys

**Correct analysis.** This is the biggest single win for wide `WITHLABELS` queries. The implementation needs a `bool response_symbol_table = ...` in the relevant request messages and a corresponding echo.

#### 5.3 Delta-encoded bucket timestamps

**Correct analysis.** Epoch-ms varints for near-uniform bucket spacing are wasteful. `sint64` delta encoding cuts 6-byte timestamps to 1–2 bytes. Same gating pattern as above.

#### 5.4 `ReducePartialState` — leave as-is

**Confirmed.** Proto3 omits zero-valued scalar fields, so sparse reducers (many zero `acc2`/`acc1_compensation`) are already encoded efficiently. Columnarising would force encoding every element.

#### 5.5 `applied_*` bools → bitmask

**Correctly deferred.** Three bools cost ~6 bytes per response — not worth a wire change alone.

---

### 6. Build Tooling — Verified

#### `rerun-if-changed`

**Correct and urgent.** `build.rs` currently emits no `cargo:rerun-if-changed` directives. `prost-build` deliberately does not emit them. Cargo falls back to "rerun if any file in the package changed," which means `protoc` re-runs on every `.rs` edit. Adding:

```rust
println!("cargo:rerun-if-changed=proto/");
```

(or the current paths until the move) is a small, unambiguous build-time win.

#### `protox` swap

**Correct.** `protox` (a pure-Rust protobuf parser) feeds a `FileDescriptorSet` into `prost-build`, eliminating the `protoc` binary dependency. This removes:
- `sudo apt-get install -y protobuf-compiler` from CI (3 places in `ci.yml`).
- `brew install protobuf` from the macOS CI job.
- The local `protoc` requirement from every contributor's onboarding.

The `build.rs` change is ~10 lines. `protox` is already well-maintained and used in production by several Rust projects. The generated output is identical to `protoc` since `prost-build` does the code generation; only the parsing frontend changes.

**Recommendation:** Trial this as a standalone PR before the schema move so any subtle issues are isolated.

#### Checked-in generated code

**Reasonable but deferred.** Checking in the generated file (`src/.../generated.rs`) instead of using `include!` + `OUT_DIR` would improve IDE navigation in RustRover (which cannot index `OUT_DIR`). The cost is a CI drift check. Given the current team tooling, this is worth exploring after the schema stabilises (post-Phase C).

---

## Sequencing Corrections

The plan's suggested Phase A–E sequence needs two adjustments:

### Phase A (revised)

| Item | Risk |
|---|---|
| `rerun-if-changed` directives in `build.rs` | None |
| `protox` swap (separate PR, trial first) | Low |
| Enum-value prefixes | None (verify with compile test) |
| Add `reserved 1;` to `SeriesSelector`, `reserved 7;` to `LabelSearchRequest` | None |
| `optional` consistency (pick one style) | None |

### Phase B (revised)

| Item | Risk |
|---|---|
| Move to `proto/valkey_timeseries/fanout/v1/` | None |
| Split `common.proto` / `filters.proto` / `request.proto` / `response.proto` | None |
| Write `docs/fanout-compatibility-handshake.md` (referenced but missing) | None |
| Add `buf.yaml` + `buf lint` to CI | None |
| Update `AGENTS.md` path references | None |
| Rename `src/commands/fanout/` → `src/commands/fanout_codec/` (or fold into `src/fanout/codec/`) | Rust-wide rename |
| **After cleanup stabilises:** Add `buf breaking` to CI with baseline | Low |

### Phase C (revised)

| Item | Risk |
|---|---|
| Delete 10 dead declarations | None |
| Merge structurally-identical messages (`MetaQueryRequest`, `CountResponse`, `StringListResponse`) | None (bytes unchanged, dispatch by handler name) |
| `map_enum!` macro for `AggregationType` et al. | None (compile-time) |
| Replace label conversion duplication with `From` impls | None |
| Round-trip proptests per message type | None |

### Phase D (revised — must include gating)

| Item | Gating required |
|---|---|
| Symbol table for labels/keys | Request opt-in + response echo |
| Single timestamp column + value-only columns | Request opt-in + response echo (a new payload codec is also needed — `SampleData` currently bundles timestamps and values) |
| Delta-encoded bucket timestamps | Request opt-in + response echo |
| **Benchmark each independently before committing.** | — |

### Phase E

| Item | Notes |
|---|---|
| `v2` package with `_UNSPECIFIED` zero values, dropped `MatcherOpType`/oneof redundancy | Requires new handler names or version-dispatch in the envelope header, not just a package rename |

---

## Open Questions Not Addressed by the Plan

1. **MultiRangeRequest field 9 (`apply_count`)**: The proto comment says this "carries no version hazard." However, a pre-handshake shard that doesn't understand field 9 will ignore it (proto3 behaviour) and return all rows, which the coordinator then re-filters. Is this confirmed by integration tests against an older server build? The `applied_count` response flag is the safety net, but the plan should acknowledge this.

2. **`AggregationType.NONE = 13`**: This is distinct from `ALL = 0`. Is `NONE` ever sent over the wire? If not, it's another candidate for the dead-schema list.

3. **`StatsResponse` bitmap fields**: `labels_bitmap` and `label_value_pairs_bitmap` are always set to `vec![]` in `conversions.rs:180-181`. Are these wired but unused, or dead? If dead, they should be removed from the proto alongside the Phase C deletions.

4. **`CompressionType` enum**: Currently has `UNCOMPRESSED = 0`, `GORILLA = 1`, `CHIMP = 2`. The plan's point about zero-valued `_UNSPECIFIED` variants does not mention this enum, but `UNCOMPRESSED = 0` is a meaningful default — same issue as `AggregationType.ALL = 0`. Worth noting.

5. **Proto `ErrorKind` vs Rust `ErrorKind`**: The proto defines `ErrorKind` with values like `FAILED = 0` through `GENERIC = 9`. The Rust `ErrorKind` (`fanout_error.rs`) has different values (`InvalidMessage = 0`, `Custom = 255`). Are these intentionally different namespaces, or should they be aligned? The current design uses the Rust error serialisation (`FanoutError::serialize`/`deserialize`), not the proto `ErrorResponse` message — which is on the dead list.

---

## Implementation Notes

### `build.rs` after protox swap (sketch)

```rust
fn main() {
    let file_descriptors = protox::compile(
        &[
            "proto/valkey_timeseries/fanout/v1/common.proto",
            "proto/valkey_timeseries/fanout/v1/filters.proto",
            "proto/valkey_timeseries/fanout/v1/request.proto",
            "proto/valkey_timeseries/fanout/v1/response.proto",
        ],
        &["proto/"],
    )
    .unwrap();

    let mut config = prost_build::Config::new();
    // Verify strip_enum_prefix remains true
    assert!(
        config.strip_enum_prefix(),
        "strip_enum_prefix must be true; enum variant names would change"
    );
    config.compile_fds(file_descriptors).unwrap();

    println!("cargo:rerun-if-changed=proto/");
}
```

### `buf.yaml` (sketch)

```yaml
version: v2
modules:
  - path: proto/valkey_timeseries/fanout/v1
lint:
  use:
    - DEFAULT
breaking:
  use:
    - FILE
```

### `map_enum!` macro (sketch)

```rust
macro_rules! map_enum {
    ($local:ty <=> $fanout:ty { $($local_var:ident <=> $fanout_var:ident),+ $(,)? }) => {
        impl From<$local> for $fanout {
            fn from(value: $local) -> Self {
                match value {
                    $(<$local>::$local_var => <$fanout>::$fanout_var),+
                }
            }
        }
        impl From<$fanout> for $local {
            fn from(value: $fanout) -> Self {
                match value {
                    $(<$fanout>::$fanout_var => <$local>::$local_var),+
                }
            }
        }
    };
}

// Usage:
map_enum! { AggregationType <=> FanoutAggregationType {
    All <=> All,
    Any <=> Any,
    Avg <=> Avg,
    // ... 20 more
    VarP <=> VarP,
    VarS <=> VarS,
}}
```

This also handles the `Share` ↔ `ShareIf` / `IRate` ↔ `Irate` asymmetry — the macro makes it a single visible line rather than a buried mismatch in a 23-arm match.
