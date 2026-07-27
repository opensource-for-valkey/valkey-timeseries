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
// src/promql/engine/fanout/rollup_fanout_command.rs
let kind = RollupKind::from(ProtoRollupKind::try_from(req.kind).ok()?);
```

`try_from` failing means the coordinator is newer. The shard answers
`applied = false` and ships its raw windows. **Enum lists therefore grow by
appending only** — renumbering an existing value would make an old shard confidently
compute the wrong function.

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

### TS.QUERY / TS.QUERYRANGE — PromQL rollups

`RollupQueryResponse`: **two independent flags**, because there are two separable
optimizations in one request.

| `applied` | `aggregated` | Response carries | Coordinator does |
|---|---|---|---|
| `false` | `false` | raw windows in `raw` | reduce, then group |
| `true` | `false` | per-series values in `series` | group |
| `true` | `true` | per-`(group, step)` partials in `partials` | merge and finalize |

`applied` is read first, so the fourth combination (`applied = false`,
`aggregated = true`) resolves to the top row: the raw windows are taken and the
stray flag ignored, which is the safe reading. What *is* rejected is a response
whose payload contradicts its own flags — raw windows alongside `applied`,
per-series values alongside `aggregated`, or partials without `aggregated` —
because folding those in would double-count the series they belong to.

---

## 4. Why the rollup handshake needed a second bit

This is the part worth internalizing before adding a fourth push-down, because
the wrong choice here is silent.

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

`ts-fanout-aggregation-pushdown` (default `yes`) and `ts-fanout-rollup-pushdown`
(default `no`) are read **only by the coordinator**. Shards obey whatever the
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
- `src/promql/engine/fanout/rollup_fanout_command.rs` — PromQL rollups and fusion
- `docs/promql-rollup-pushdown-plan.md` — the rollup push-down design in full
- `docs/overview.md` — cluster mode and push-down from an operator's view

Adjacent but distinct: a cluster topology change between request and receipt is
caught by a cluster-map fingerprint check and fails the fanout fast
(`ErrorKind::ClusterMapMismatch`). That is a consistency guard, not a version
handshake — it protects against the shard set moving underneath a query rather
than against peers running different code.
