# Fanout compatibility handshake

In cluster mode a coordinator scatters work to every shard and merges what comes
back. During a rolling upgrade the nodes are not all running the same build, so
the coordinator can ask for something a peer has never heard of — a push-down it
does not implement, a function added since its release, an envelope format it
cannot parse.

The rule this document describes is that **version skew must degrade
performance, never correctness, and never require an operator to do anything.**
No node advertises a version, no cluster-wide barrier is negotiated, and no
config has to be flipped before or after an upgrade. Correctness comes from two
independent mechanisms, applied at different layers.

---

## 1. Two layers, two different failure modes

The distinction is whether a receiver that ignores something still produces a
correct answer.

| Layer | Mechanism | When a peer doesn't understand |
|---|---|---|
| Envelope (message header) | `required_features` bitmask | Reject the message explicitly |
| Payload (protobuf body) | Self-describing responses | Do less, and say so |

**Envelope changes are fail-fast** because ignoring them corrupts the read. If a
future release compresses payloads, a receiver that skips the compression bit
does not get a slightly worse answer — it gets garbage. So the header carries
`required_features`, and a node checks it before dispatching:

```
src/fanout/fanout_message.rs   SUPPORTED_MESSAGE_FEATURES, has_unsupported_features()
src/fanout/cluster_rpc.rs      the intake gate
src/fanout/fanout_error.rs     ErrorKind::UnsupportedFeatures = 10
```

No feature bits are defined yet (`SUPPORTED_MESSAGE_FEATURES == 0`). Allocate one
only for a change the receiver cannot safely ignore; everything else belongs in
the payload layer, where degradation is possible.

**Payload changes are self-describing** because ignoring them is safe: the peer
simply does less work, reports that it did less, and the coordinator makes up the
difference. That is the mechanism the rest of this document is about.

---

## 2. The load-bearing protobuf property

proto3 decodes an absent field to its zero value and drops unknown fields
silently. That is what makes an old shard able to parse a new coordinator's
request at all — and it dictates the single most important design rule:

> **`false` must mean "did nothing."**

Every handshake flag is a `bool` whose `false` value describes the *unoptimized*
behaviour. A shard that predates a push-down never sets the flag, proto3 decodes
the missing field as `false`, and the coordinator reads that as "this peer sent
me raw data" — which is exactly true. The compatibility comes for free from the
wire format; there is no version check anywhere.

A flag phrased the other way (`skipped_aggregation`, say) would invert this: the
old shard's silence would read as "I aggregated", and the coordinator would merge
raw samples as if they were partial states. Correctness would depend on every
peer being new — which is the property we are trying not to need.

The same reasoning covers enums. Enum fields decode to a raw `i32`, so an
unrecognized value is detectable rather than silently mapped onto a neighbour:

```rust
// src/promql/engine/fanout/grid_fanout_command.rs
let kind = ProtoRollupKind::try_from(rollup.kind)
    .map_err(|_| ...)
    .and_then(RollupKind::try_from)?;
```

`try_from` failing means the request names a function this node does not have.
The grid command refuses such a request outright (every node runs the same
build); a push-down that has to survive a rolling upgrade would instead answer
with its raw data and say so. **Enum lists therefore grow by appending only** —
renumbering an existing value would make an old shard confidently compute the
wrong function.

---

## 3. The handshakes in use

Three commands push work down today. Each response echoes which parts of the
request the shard actually honored.

### TS.MRANGE / TS.MREVRANGE

`MultiRangeResponse` in `src/commands/fanout.response.proto`:

| Flag | `true` means | `false` means |
|---|---|---|
| `applied_aggregation` | `series` holds aggregated buckets | `series` holds raw samples |
| `applied_group_reduce` | `group_partials` holds mergeable partial states | nothing was pre-reduced |
| `applied_count` | `COUNT` was used as a head/tail pre-filter | full result set |

The coordinator always re-applies `COUNT` as the final authority, so
`applied_count` is a transfer optimization rather than a semantic claim.

### TS.QUERY — PromQL aggregation

`AggregationQueryResponse`: one flag, `applied`. True and the response carries
the reduced result; false and it carries the raw instant vector for the
coordinator to aggregate. See `src/promql/engine/fanout/aggregation_fanout_command.rs`.

### TS.QUERY / TS.QUERYRANGE — PromQL grid queries

`GridQueryResponse` carries no flags: the pre-release codebase has no installed
base to stay compatible with, so the rollup handshake described in §4 was folded
into one *grid* request (`GridQuery`) that serves every range-query read of a
selector — stepped instant selection, rollups, and either fused with a reducing
aggregation. The response is self-describing by **which list a series lands
in**:

| List | Holds | Coordinator does |
|---|---|---|
| `series` | per series, a presence bitmap over the request's window ends plus one packed value per set bit (and, only when the query calls `timestamp()`, each pick's lag behind its window end) | index into its own window ends; concatenate |
| `partials` | one partial per `(group, step)` for a fused request | merge and finalize |
| `raw` | a series' raw span, when it is smaller than its grid output | run the same per-series stage, then the above |

A fused request answers in `partials`, an unfused one in `series`, and any
series may travel in `raw` under the size rule; `series` and `partials` never
both appear. The `series` form is columnar because both sides already hold the
window ends: addressing a point by index instead of by timestamp takes a
500-series × 60-step selection from 26 to 9.3 bytes per point (11 with the
lag column). A bitmap that reaches past the grid, or a value count that
disagrees with it, is rejected as a corrupt response. A response carrying per-series values for a fused request, or
partials for an unfused one, is rejected as corrupt rather than folded in twice.
A shard handed a rollup or aggregation it does not know refuses the request —
every node runs the same build. See
`src/promql/engine/fanout/grid_fanout_command.rs` and
`docs/plans/selector-pushdown-plan.md`.

### TS.QUERYRANGE — label profiles

`LabelProfileQuery` / `LabelProfileResponse` back the data-derived filter
push-down (`ts-promql-derived-filter-pushdown`): before a range query with a
binary operation is planned, the coordinator asks every shard which labels the
series of each operand's selector carry — per label, how many series and which
distinct values — and narrows the *other* operand's selectors by what every
series is known to satisfy. No sample is read for it. The response is
self-describing in the same sense as the grid's: a shard that matched more
series than the request's `max_series` answers `overflow` instead of a profile,
and the coordinator then leaves the selector as written, which is always safe.
Per-label value lists past the coordinator's cap are marked `overflow` too and
derive no filter. Counts add across shards (a series lives on one shard), so
"carried by every series" stays exact after the merge. See
`src/promql/engine/fanout/label_profile_fanout_command.rs`,
`src/promql/engine/derived_filters.rs` and
`docs/plans/derived-filter-pushdown-plan.md`.

---

## 4. Why the rollup handshake needed a second bit

The rollup push-down shipped with two flags before it was folded into the grid
query above. The reasoning is kept because it is the rule for the *next*
push-down that has to survive a rolling upgrade, and the wrong choice here is
silent.

`sum by (job) (rate(m[5m]))` asks a shard for two things: reduce each series'
windows, and fold the results into per-group partials. A shard that implements
the first but not the second is a completely ordinary state during a rolling
upgrade — and it does not know it is in that state. It sees `agg_kind` and
`agg_grouping` as unknown fields, drops them, applies the rollup it *did*
understand, and answers `applied = true`.

Had `applied` been widened to mean "did everything the request asked", that
answer would be a lie the coordinator has no way to detect. It would take
per-series rollup values for finished groups and return **ungrouped series** —
a wrong answer, not a slow one, from a node that behaved correctly.

The second bit removes the ambiguity: `aggregated == false` says the grouping
did not happen, whatever else did, and the coordinator groups the values itself.
Because the two bits are independent, a cluster mixing current, rollup-only, and
no-push-down peers is compensated **peer by peer** in the same query.

The general rule: **one flag per independently-skippable step.** If a request
asks for N things a peer might implement in any subset, the response needs N
bits. Reusing one bit for two steps is only safe when no build can ever
implement one without the other — and a build that predates the second step
always can.

---

## 5. Coordinator-decides fallback

Compensation happens per response, as each arrives, not as a whole-query
decision. `on_response` inspects the flags of that one peer and folds its
contribution into the right accumulator; `into_result` reduces and groups
whatever arrived un-reduced or un-grouped before merging everything.

The consequence is the one that matters operationally: **a lagging node costs
extra transfer and coordinator CPU for its own slice of the query, and nothing
more.** The other shards' work is unaffected, and the answer is identical either
way.

The remaining requirement is that the reduction be *the same code* on both
paths. Every pushable rollup is a named function shared by the local evaluator
and the shard-side reducer, so "the coordinator compensated" and "the shard
applied" cannot drift into two different answers for the same window.

---

## 6. What the toggles are not

`ts-fanout-aggregation-pushdown` (default `yes`), `ts-fanout-rollup-pushdown`
(default `yes`; it governs the whole grid push-down, stepped selectors included)
and `ts-promql-derived-filter-pushdown` (default `yes`; the label-profile round
before planning) are read **only by the coordinator**. Shards obey whatever the
request asks for.

They are not mixed-version safety knobs. Version skew is already correct by the
mechanism above, so a rolling upgrade needs no configuration change in either
direction. Their purpose is an emergency and diagnostic escape hatch: flipping
one off routes every affected query back through the coordinator-side path
without a module rollback — useful to mitigate a latent push-down bug, or to A/B
isolate whether a problem lives in push-down at all.

One consequence of "coordinator only": you cannot simulate an old peer by
setting the config differently on one node. Mixed-version behaviour is covered
by the round-trip tests beside each fanout command, not by the cluster
integration suites.

---

## 7. Adding a push-down

1. Add request fields as new proto3 field numbers. Never renumber or reuse.
2. Add enum values by appending. Decode unknown values through `try_from` and
   degrade; never map them onto a default.
3. Add **one response flag per independently-skippable step**, phrased so
   `false` describes the unoptimized behaviour.
4. Make the coordinator compensate per response, in `on_response` /
   `into_result`, not per query.
5. Share the computation kernel between the local path and the shard-side path
   so compensation cannot diverge from application.
6. Reject responses whose payload contradicts their own flags — a peer claiming
   `applied` while shipping raw data is corrupt, and double-counting it is worse
   than failing.
7. Cover the mixed-version matrix in round-trip tests: every combination of
   flags a peer might return, including the ones no current build produces.

Envelope-level changes are the exception to all of this: if a receiver cannot
produce a correct answer by ignoring the change, allocate a `required_features`
bit instead and let it fail fast.

---

## Related

- `src/fanout/` — envelope, transport, error kinds
- `src/commands/ts_mrange_fanout_command.rs` — MRANGE push-down
- `src/promql/engine/fanout/aggregation_fanout_command.rs` — PromQL aggregation
- `src/promql/engine/fanout/grid_fanout_command.rs` — PromQL grid queries: stepped selectors, rollups and fusion
- `src/promql/engine/fanout/label_profile_fanout_command.rs` — label profiles for the derived filter push-down
- `docs/plans/selector-pushdown-plan.md` — the grid push-down design in full
- `docs/plans/derived-filter-pushdown-plan.md` — the derived filter push-down design in full
- `docs/overview.md` — cluster mode and push-down from an operator's view

Adjacent but distinct: a cluster topology change between request and receipt is
caught by a cluster-map fingerprint check and fails the fanout fast
(`ErrorKind::ClusterMapMismatch`). That is a consistency guard, not a version
handshake — it protects against the shard set moving underneath a query rather
than against peers running different code.
