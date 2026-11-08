# Fanout review

Reviewed 2026-11-07, starting at `e6b077044`. Scope: all thirteen files under `src/fanout`,
with checks of command consumers, the shared byte pool, executor, ACL guard, protobuf types,
and module SDK logging. Implementation files were left unchanged; existing aggregator work
and review documents were left alone.

The strongest findings are an unbounded buffer-retention pattern and duplicate shard reads
under HASHTAG routing. Both occur without topology churn or malformed messages. The lifecycle
and topology findings below have narrower triggers, with their validation limits stated.

A second pass over the same files, using code inspection only, added FAN-10 … FAN-16 and the
notes marked "second pass" below. It re-read the code behind FAN-01, FAN-03 and FAN-07 and found
nothing to contradict them, and it contradicts none of the first nine. FAN-10 stands out among
the additions: the others need a code change to become live, while FAN-10 needs only a
transiently inconsistent `CLUSTER NODES` view and then degrades the whole node rather than one
reply.

A third pass (2026-11-07, at `10fc28282`; `src/fanout` is byte-identical to the review base)
re-read every file independently and then checked each earlier finding against the current code.
All sixteen still hold at the cited lines, and none needed a correction. It added FAN-17 and FAN-18
and the notes marked "third pass". FAN-17 is the most consequential finding in the document: after
any primary failover, every non-HASHTAG fanout fails on the node that took over, and a live
two-node failover reproduced it 10 out of 10 times.

| ID | Priority | Finding | Evidence |
|---|---|---|---|
| FAN-01 | P1 | Growable message buffers accumulate indefinitely in the fixed-size pool | Actual pool probe; dependency and call-site inspection |
| FAN-02 | P1 | Multiple tags on one shard can count that shard multiple times | Live primary/replica cluster |
| FAN-03 | P2 | An older refresh can overwrite a newer topology | Controlled concurrent refresh probe |
| FAN-04 | P2 | Overlapping slot ownership passes consistency checks | Actual parser/map-building probe |
| FAN-05 | P2 | Dropping an incomplete active fanout reports success | Actual state-machine probe |
| FAN-06 | P2 | A parsed NUL-containing handler panics in the intake logger | Parser and SDK logging probe; callback consequence by inspection |
| FAN-07 | P2 | Known failed replicas remain eligible targets | Live primary/replica cluster and parser probe |
| FAN-08 | P3 | Unused Local/ReplicasOnly modes violate their stated contracts | Map/target-selection probes |
| FAN-09 | P3 | Reordering replicas looks like a topology change | Actual topology-comparison probe |
| FAN-10 | P1 | An inconsistent map is rebuilt on every fanout, with no backoff | Refresh/backoff state-machine inspection |
| FAN-11 | P2 | The completion hook cannot run on timeout nor be overridden by client commands | Trait/blanket-impl call-graph inspection |
| FAN-12 | P3 | `Ord` and `PartialEq` disagree for NodeInfo and ShardInfo | Derive/impl inspection |
| FAN-13 | P3 | Target selection keeps dormant release-mode panic paths | Call-site and guard inspection |
| FAN-14 | P3 | `NodeId` string accessors disagree for an empty id; its UTF-8 invariant is debug-only | Accessor inspection |
| FAN-15 | P3 | Topology and expected-race logging runs at notice/warning on hot paths | Logging call-site inspection |
| FAN-16 | P3 | Two blocked-client paths can silently withhold or shorten the reply | Call-site inspection |
| FAN-17 | P1 | Slot-less masters — including a failed former primary — are fanout targets and enter the fingerprint | Live failover; parser probe |
| FAN-18 | P3 | HASHTAG routing silently drops tags whose slot has no owner | Target-selection inspection |

## Correctness and memory retention

**FAN-01 — The byte pool retains buffers that normal fanout allocation cannot reuse.**

[process_request_message](../../src/fanout/cluster_rpc.rs), line 630, starts response encoding
with a 1,024-byte pooled vector. [send_message_internal](../../src/fanout/cluster_rpc.rs),
lines 442–454, starts a second one and copies the encoded response into its envelope. Both
grow as needed and return to the static pool on drop. Request encoding uses the same pattern
with a 512-byte starting size.

The wrapper in [common/pool.rs](../../src/common/pool.rs), lines 5–14, clears a vector obtained
from `byte-pool` 0.2.4. That dependency's `Poolable for Vec<T>` defines its misleadingly named
`capacity()` as **the vector length**, and its allocator initializes a fixed-length vector.
The pool checks one queued buffer for an exact size match and retains every returned buffer,
without a count/byte cap. Clearing and serializing the vector changes that length: a response
body returned with length N no longer matches a subsequent request for length 1,024. Large
completed messages also enter a different queue from those small starting buffers. A small
response whose final length differs from the starting size has the same reuse problem.

Probe using the repository's actual `get_pooled_buffer`, in one sequential loop:

```rust
for _ in 0..16 {
    let mut buffer = get_pooled_buffer(1024);
    buffer.resize(8 * 1024 * 1024, 1);
    // Drop at the end of each iteration, before allocating the next buffer.
}
```

All sixteen large buffer addresses were distinct, leaving **128 MiB of vector capacity** in
the pool, with only one live caller at a time. The process's peak RSS was about 131 MiB,
versus about 1.8 MiB for zero iterations. These are local system-allocator figures, not a
server heap profile. The dependency's return path establishes that the buffers are retained;
the address/size probe demonstrates failure to reuse them across sequential requests.

Fix the pool integration around growable vectors: use actual allocation capacity with useful
size classes, bound total retained bytes, and discard oversized buffers. Preserve or benchmark
the pooling benefit for small steady workloads. Merely increasing the initial response size
moves the failure threshold. Add a sequential encode/drop/reuse test that verifies retained
memory reaches a bounded steady state.

**FAN-02 — Target selection deduplicates nodes after independently choosing them per slot.**

[HashTags/random_for_slots](../../src/fanout/cluster_map.rs), lines 580–588 and 639–651, remove
duplicate slots, then pick a node independently for every remaining slot. Two slots belonging
to the same shard can pick its primary and its replica. The `HashSet<NodeInfo>` retains both,
so both run the whole shard-local query. Incoming-response deduplication is correctly per
node, but that does not remove the duplicate *shard* contribution.

This reaches ordinary commands through
[get_multi_command_targets](../../src/commands/utils.rs), lines 207–216: READONLY clients may
use `HashTags`, and legacy-server routing also permits replicas. Primary-only hash-tag routing
does not have this particular problem because every slot in a shard selects the same primary.

Live reproduction: one primary owning all slots, one synchronized replica, one series labelled
`review=fanout`, and two distinct tags `review-a,review-b`:

```text
TS.CARD HASHTAG review-a,review-b FILTER review=fanout
```

| Client routing | Results in the first run |
|---|---|
| Primary, ordinary connection | 30 × `1` |
| Primary, READONLY connection | 46 × `1`, 54 × `2` |
| Replica, READONLY connection | 51 × `1`, 49 × `2` |

A second run reproduced the same intermittent double count. With two equiprobable nodes and
two slots, choosing both has probability 1/2. Cardinality is the directly reproduced error;
other consumers can duplicate series or double-weight grouped reductions by the same path.

Resolve requested slots to unique shard IDs **before** choosing one node per shard. Test
multiple distinct slots on one shard, repeated slots, multiple replicas, and mixed shards.

**FAN-03 — Concurrent refreshes can publish out of observation order.**

[refresh_cluster_map](../../src/fanout/mod.rs), lines 193–215, fetches/parses a snapshot and then
unconditionally publishes it when different from the current map. There is no refresh ordering
or single-flight publication rule. Parsing deliberately runs outside the GIL for worker callers,
so a slow old refresh can finish after a newer one and replace it. The comment at lines 151–152
that the worst concurrent outcome is an extra refresh is incorrect.

A controlled `ClusterNodesSource` probe held an older full-coverage snapshot while another
refresh published a changed slot layout, then released the older refresh. The final global map
had the old fingerprint. Both snapshots were individually consistent. This is a publication
race demonstration, not a live failover reproduction.

Serialize refresh ownership or order observations/publications so a refresh that lost its
generation cannot replace a newer observation. Holding a refresh mutex across a GIL acquisition
while a main-thread caller waits for that mutex would introduce a deadlock; use the repository's
threading rules when choosing single-flight behavior. Include stale-mark handling and failed
refreshes in the design so a failed leader does not consume the only invalidation.

**FAN-04 — Coverage checks do not detect conflicting owners.**

[build_slot_to_shard_map](../../src/fanout/cluster_map.rs), lines 951–959, inserts ranges into
`RangeMapBlaze`, replacing overlapping mappings. The later coverage check sees the normalized,
nonoverlapping result, after the evidence of conflicting ownership is gone.

A parser probe with primary A owning `0-16383` and primary B also claiming slot `1` returned
`is_consistent == true`. Both shards retain the overlapping ownership in `owned_slots`, while
the slot map chooses an owner according to insertion order. Thus the consistency flag and
fingerprint do not establish an unambiguous routing view.

Detect overlaps against the original ownership claims before insertion; reject or mark
conflicting snapshots unusable. There is a related guard gap: `get_fanout_targets` returns a
new map even when it remains inconsistent, and `cluster_fingerprint_matches` compares only
fingerprints. Agreeing on an incomplete/inconsistent view is not sufficient validation. The
overlap was reproduced with synthetic CLUSTER NODES text; no live overlapping-ownership or
partial-coverage command result was tested.

**FAN-05 — State destruction can turn a missing share into a successful partial reply.**

[FanoutStateInner::drop](../../src/fanout/fanout_command.rs), lines 395–403, calls completion for
any Active state. [on_completion](../../src/fanout/fanout_command.rs), lines 376–391, returns
success when it has no recorded error, even if `outstanding > 0`.

A direct probe created a two-share state, delivered one successful answer with value 7, and
dropped the state. Its completion callback received `(7, Ok(()))`, with the second answer
missing. One production path to losing an owner without an answer is a local handler panic:
the executor catches the panic outside the job, but `spawn_local_request` does not turn it into
`state.on_error`. If remote replies have finished/released their references, final state
destruction can follow. Deferred reclamation of the RPC entry affects when that happens.

The incomplete-state success was reproduced; a live panicking handler/client reply was not.
Make unfinished destruction fail closed, and convert worker panics into an explicit failed
share. Keep the Pending/setup-failure path distinct so it still discards an uninstalled callback.
Test local-only and mixed fanouts, panic before/after a remote answer, and exactly-once completion.

**FAN-06 — Parser totality does not extend to the callback's logging.**

[FanoutMessageHeader::try_from](../../src/fanout/fanout_message.rs), lines 71–82, accepts an
interior NUL in a handler name. For an unknown handler,
[on_request_received](../../src/fanout/cluster_rpc.rs), lines 754–762, interpolates that name
into a warning. The installed module SDK converts the log text with `CString::new(...).unwrap()`.
`unknown\0handler` therefore parses successfully but panics when this warning is logged.

The probe used the actual parser and SDK logger under `catch_unwind` and confirmed the panic.
In the real `extern "C"` intake callback an unwinding panic aborts the process. No malformed
cluster frame was injected into a live server; no ordinary-client trigger is established.

Validate internal handler identifiers and make logging of peer-supplied strings safe at the
C boundary. Error messages also cross that boundary, so rejecting NUL only in handler names
would not establish a general safe-logging contract. Preserve length-aware ACL identity
handling: the existing user-name test deliberately prevents truncating `alice\0evil` to `alice`.
Extend fuzzing from header parsing through intake/error handling with a recording transport/logger.

Third pass: the abort exposure is wider than the logger. None of the seven `extern "C"` functions
in `src/fanout` (four cluster-message receivers and three blocked-client callbacks) wraps its
body in `catch_unwind`. Neither does the RPC timer callback, which runs behind the SDK's C
trampoline; only the executor's job runner has a guard. On the coordinator,
`on_response_received` runs `deserialize` and the command's own `on_response` synchronously on
the main thread (see the performance section), and `reply_callback` runs the command's `reply`.
A panic in either piece of command-specific code (an index or arithmetic bug in an MRANGE merge,
say) therefore aborts the server, not one reply. A single guard per callback that converts a panic into a failed share (and a logged error)
would close the class, including FAN-06, and gives FAN-05's "convert worker panics" a matching
main-thread half.

**FAN-07 — Failed nodes are selected despite healthy alternatives.**

[parse_node_line](../../src/fanout/cluster_map.rs), lines 1024–1045, ignores the link-state value
and the `fail`/`fail?` flags. The resulting NodeInfo carries no health state, and random target
selection gives failed replicas the same eligibility as healthy ones.

In the isolated cluster, after stopping the replica and waiting until CLUSTER NODES marked it
`fail`, 20 single-tag READONLY CARD queries produced **7 correct replies and 13 generic fanout
errors**, despite the primary remaining healthy and owning all slots. The test used a 500 ms
fanout timeout. A synthetic map independently selected a `slave,fail ... disconnected` node on
roughly half of random draws and considered the map consistent.

Keep ownership and health distinct: retaining a failed owner's slots can be necessary to
describe the topology, but a known failed replica should not remain a candidate when a healthy
node can serve the shard. Define behavior for no healthy candidate and test failover transitions.
Adding automatic retry for writes would require a separate idempotency decision; this finding
and the healthy-alternative recommendation concern read routing.

Second pass: a related eligibility gap compounds this one.
[parse_node_address](../../src/fanout/cluster_map.rs), lines 1096–1135, turns an unparseable
port into `0` and still returns an address, so a malformed line leaves a target that can never
be dialled. That target is not filtered and does not mark the map inconsistent; it fails at send
time on every fanout that selects it, which is the same user-visible outcome as this finding's
failed replicas (a generic aggregate error instead of a healthy shard's answer).

Third pass: FAN-17 is the deterministic form of this finding. A failed node that owns no slots
forms its own shard, so it is selected on every draw rather than half of them. Both fixes need
the same `fail`-flag parsing; land them together.

**FAN-08 — Local and ReplicasOnly have misleading or broken dormant contracts.**

[get_local_shard](../../src/fanout/cluster_map.rs), lines 566–567, looks up the local *node* ID in
a set keyed by primary/shard ID. On a replica it returns None, although the builder correctly
marked its parent's shard local. The existing local-replica test verifies owned slots but does
not call this method. On a primary, `FanoutTarget::Local` then randomly picks from the shard,
so it can target a remote replica despite the variant's “only the local node” documentation.
`ReplicasOnly` documents one replica per shard but returns all replicas; a one-shard/two-replica
probe returned two targets.

No production callers of these two modes were found. Fix or remove them before reuse, and
make shard lookup and node lookup distinct in the API. These are dormant contract defects,
not demonstrated failures of the currently used default Random/Primary modes.

**FAN-09 — Replica order defeats unchanged-topology reuse.**

[same_topology](../../src/fanout/cluster_map.rs), lines 917–918, compares ShardInfo values whose
replicas are stored in a Vec populated in input order. Reordering two replica lines within one
unchanged shard makes `same_topology` false. The existing line-order test has only one replica
per shard, so it cannot expose this case.

Canonicalize replica order or compare membership independent of order. Otherwise a harmless
reordering rebuilds maps/caches and resets the adaptive refresh interval. The comparison failure
was reproduced; recurring line reorder and its latency effect were not measured on a server.

**FAN-10 — An inconsistent map is rebuilt on every fanout, with no backoff.**

[get_fanout_targets](../../src/fanout/mod.rs), lines 140–143, and
[get_or_refresh_cluster_map](../../src/fanout/mod.rs), lines 227–241, treat `!is_consistent` as
a refresh trigger. A rebuild that is itself inconsistent publishes that map and takes the
`Changed` branch of [refresh_cluster_map](../../src/fanout/mod.rs), lines 207–216, which calls
`reset_refresh_interval`. The next fanout therefore sees `!is_consistent` again and rebuilds
again: an inconsistent view is not a one-shot retry but a per-command `CLUSTER NODES` call,
parse and map build on the main thread, with the adaptive backoff never engaging — the
`Unchanged` branch that would grow it requires *both* maps to be consistent.

The trigger is ordinary. [parse_node_line](../../src/fanout/cluster_map.rs), lines 1017–1050,
returns `None` for any line without exactly one of `master`/`slave`, and a joining node is
advertised with the `handshake` flag, which drops that line and sets `is_consistent = false` for
the whole map. Socket-address collisions (common in single-host test clusters), a replica whose
master line is absent, and transient slot gaps do the same. So on a node that has just started a
join handshake, every fanout pays a full topology rebuild before it can reply, and because the
`Changed` branch also resets the interval, nothing damps it.

Treat inconsistency as a reason to keep the previous map and back off, as `Failed` already does,
or separate "the map is usable" from "the map is perfectly consistent" so an inconsistency does
not invalidate the published snapshot. Established by inspection of the refresh/backoff state
machine plus `CLUSTER NODES` flag semantics; not reproduced against a node in a `handshake`
state.

Related documentation drift: [mod.rs](../../src/fanout/mod.rs), lines 60–72, justifies the 5 s
cap by "the periodic refresh", but there is no periodic refresh. The only refresh sites are the
two lazy paths above and `TS._DEBUG REFRESH_CLUSTERMAP`; `series/background_tasks.rs` schedules
four tasks and none touches the map. The cap's stated rationale needs restating, or the periodic
task it assumes needs to exist.

**FAN-11 — The completion hook cannot run on timeout and cannot be overridden by client commands.**

[FanoutCommand::on_completion](../../src/fanout/fanout_command.rs), lines 99–100, is documented
as "Called once all responses have been received, or on timeout", but
[on_completion](../../src/fanout/fanout_command.rs), lines 365–392, calls
`self.operation.on_completion()` only in the clean-success branch — never for a timeout, an
abort, or a partially failed fanout. Any implementation written against the documented contract
would skip its finalization on exactly the paths that need it.

The blanket impl in [fanout_client_command.rs](../../src/fanout/fanout_client_command.rs),
lines 99–130, forwards only `name`, `get_local_response`, `cancellable`, `get_targets`,
`generate_request` and `on_response`. `get_timeout`, `fail_fast`, `on_error`, `on_completion`
and `generate_error_reply` are therefore unreachable for every client-facing command, so no
implementation can turn fail-fast on: the framework's call at
[fanout_command.rs](../../src/fanout/fanout_command.rs), line 349, always receives `false` and
the fail-fast machinery, including its `abort_error` preference, is dead. Either merge the two
traits (with the reply hook as the optional half) or forward the hooks; the split reads as
extensible but is not, and dead paths carry no test pressure.

**FAN-12 — `Ord` and `PartialEq` disagree for `NodeInfo` and `ShardInfo`.**

[NodeInfo](../../src/fanout/cluster_map.rs), lines 350–376, derives `PartialEq`/`Eq` over every
field but implements `Ord` over `id` alone; [ShardInfo](../../src/fanout/cluster_map.rs), lines
409–484, does the same. Two values with equal ids and different fields therefore compare
`Equal` while being `!=`, violating `Ord`'s documented consistency with `Eq`. Nothing misbehaves
today — ids are unique within each collection, and FAN-09's comparison relies on the derived
`PartialEq`, not on ordering — but `BTreeSet::get`/`contains` and the set equality check now mean
different things. Compare all fields in `Ord`, or document that id ordering is authoritative and
make the derived equality match it.

**FAN-13 — Target selection keeps dormant release-mode panic paths.**

[pick_target](../../src/fanout/cluster_map.rs), lines 428–440, guards its `replica_only` branch
with `debug_assert!` and then calls `rng.random_range(0..self.replicas.len())`, which panics on
an empty range in a release build. No caller passes `replica_only = true` — all four call sites
pass `false` — so the branch is dead code today. The complementary asymmetry is live:
[random_one_per_shard](../../src/fanout/cluster_map.rs), line 620, filters `!shard.is_empty()`,
but [random_one_from_slot](../../src/fanout/cluster_map.rs), line 647, does not, and the
primary-less branch of `pick_target` is an `assert!` (line 462). Both are unreachable only
because every shard in `self.shards` is created with a primary; nothing in the type says so.

The same class appears in map building: [build_slot_to_shard_map](../../src/fanout/cluster_map.rs),
lines 951–959, asserts that each parsed range's shard exists. That holds for the current builder,
but it is an assertion over text the node accepted from the cluster bus, and a panic there lands
in the worker that ran `ClusterMap::create`. Delete the dead branch, make the empty-shard paths
consistent (skip, don't assert), and report `is_consistent = false` (or return early) instead of
panicking while assembling a map from peer-influenced input.

**FAN-14 — `NodeId` string accessors disagree for an empty id, and its UTF-8 invariant is debug-only.**

[NodeId::as_str](../../src/fanout/cluster_map.rs), line 266, returns `""` for an empty id;
[AsRef<str>](../../src/fanout/cluster_map.rs), line 292, returns the 40-byte NUL buffer, and it
also backs `Borrow<str>` (line 328) and therefore `BTreeSet<ShardInfo>::get(&str)`. `as_bytes()`
(line 279) and `AsRef<[u8]>` (line 304) split the same way. Both string accessors then use
`from_utf8_unchecked` guarded only by a `debug_assert!`; the invariant that holds this up — ids
come only from a `&str` or from a pointer the C API guarantees holds hex — is invisible at these
call sites. Give the empty case one definition, and either accept a checked `&str` or
fixed-length hex slice at the boundary or move the invariant onto an `unsafe` constructor.

**FAN-15 — Topology and expected-race logging runs at notice/warning on hot paths.**

Every refresh logs `Refreshing cluster map...` ([mod.rs](../../src/fanout/mod.rs), line 193),
`Calling CLUSTER NODES...` ([cluster_map.rs](../../src/fanout/cluster_map.rs), line 720) and an
outcome notice (lines 204 and 213). That is three lines per cycle on every node, and the 5 s cap
admits a cycle forever, so roughly 50 k lines per node per day with no topology change at all;
under FAN-10 it becomes three lines per fanout command. `common::logging::log_notice` is
unconditional at the default `loglevel`. Likewise `FANOUT_IGNORED_UNKNOWN_REQUEST` and the
duplicate/foreign-sender warnings in [cluster_rpc.rs](../../src/fanout/cluster_rpc.rs), lines
113–155 and 851, fire for the *expected* steady state after a timeout-cancel race rather than
for a fault. Use `log_debug` for the refresh lifecycle, and rate-limit or demote the per-message
expected-race warnings; keep warnings for conditions an operator must act on.

Third pass: shard-side command errors are logged at warning on *both* ends, and an ordinary client
can trigger them. [process_request_message](../../src/fanout/cluster_rpc.rs), lines 662–668, logs
every non-timeout handler error before replying, and the coordinator's default
[FanoutCommand::on_error](../../src/fanout/fanout_command.rs), lines 89–97, logs it again per
target. A user who lacks read access to matched keys therefore writes one warning on every shard
plus one per shard on the coordinator for each command they issue: N+N lines per denied request,
under the client's control. Demote handler errors that represent a client-visible outcome
(permission, busy, swapped database) to debug or count them, and keep warnings for internal
failures.

**FAN-16 — Two blocked-client paths can silently withhold or shorten the reply.**

[FanoutBlockedClient::new](../../src/fanout/blocked_client.rs), line 80, computes
`(timeout.as_millis() as i64).max(1)`. `as_millis()` is a `u128`, so a duration above `i64::MAX`
milliseconds truncates to a negative value and the `.max(1)` then clamps it to **1 ms**. The
config bounds `ts-fanout-command-timeout` to 500–10 000 ms, which keeps this unreachable today,
but `get_timeout()` is an overridable trait method, so the cast should be
`i64::try_from(..).unwrap_or(i64::MAX)`.

The reply closures take the blocked-client mutex with `if let Ok(..)`
([fanout_client_command.rs](../../src/fanout/fanout_client_command.rs), lines 70 and 83), so a
poisoned mutex drops both the operation and its result without a reply, and the client waits out
its blocked-client timeout to be told the fanout timed out. Every other mutex in the module goes
through `common::sync::lock`, which ignores poisoning; do the same here.

**FAN-17 — Slot-less masters are shards: a failed former primary breaks every fanout after a
failover.**

[from_cluster_nodes](../../src/fanout/cluster_map.rs), lines 787–831, turns *every* line flagged
`master` with a parseable address into a `ShardInfo`, whether or not it owns slots. It also
ignores `fail` (FAN-07). After a failover, the old primary stays in `CLUSTER NODES` as
`master,fail … disconnected` with no slots, until it rejoins as a replica or every node runs
`CLUSTER FORGET`. It thus becomes a one-node shard of its own.
[random_one_per_shard](../../src/fanout/cluster_map.rs), lines 620–627, and
[primary_targets](../../src/fanout/cluster_map.rs), lines 674–686, iterate every shard, so
Random *and* Primary routing select it on every fanout, not on half the draws as FAN-07's
failed replica does. No healthy alternative exists, because the node is its own shard. The send
fails (or times out), the share counts as a `Custom` error, and the client gets the generic
aggregate error. The map remains `is_consistent`, because coverage is satisfied by the new
primary, so nothing triggers a rebuild that would help.

Live reproduction (local unstable server, release module built from HEAD): primary A owned
`0-16383`, B was its synchronized replica, and one series was labelled `review=fanout`. A was
`SIGKILL`ed and B ran `CLUSTER FAILOVER TAKEOVER`, after which B's `CLUSTER NODES` showed itself as
`myself,master … connected 0-16383` and A as `master,fail … disconnected`. Then, on B:

| Command (ordinary connection on B) | Result |
|---|---|
| `TS.CARD FILTER review=fanout` before failover (on A) | `1`, 3 out of 3 |
| `TS.CARD FILTER review=fanout` after failover | `ERR Internal error in fanout operation 'card'`, 10 out of 10 |
| `TS.MGET FILTER review=fanout` after failover | `ERR Internal error in fanout operation 'mget'` |

B's log shows `Failed to send fanout request … to node 127.0.0.1:7311` per command. The cluster
was healthy, and a single node held all the data. A parser probe on a three-line snapshot
(promoted replica, failed old primary, second primary) confirmed `is_consistent == true`, the
failed node in `primary_targets`, and its selection by `random_one_per_shard` in 100 of 100 draws.

Consequences beyond reads:

- `TS.MDEL` routes `Primary` ([ts_mdel_fanout_command.rs](../../src/commands/ts_mdel_fanout_command.rs),
  line 55) and is non-cancellable. Healthy shards delete their matches while the client is told
  the command failed. That is a partial write behind an error reply, the outcome the cancellable
  contract exists to avoid. This follows from the code path; it was not reproduced with more
  than one shard.
- [compute_cluster_fingerprint](../../src/fanout/cluster_map.rs), lines 963–970, hashes every
  shard's id, empty ones included. Two nodes whose views differ only by a slot-less master
  therefore disagree on the fingerprint even though slot ownership is identical. The probe
  confirmed this, comparing the snapshot with and without the failed node's line. This happens
  when one node has run `CLUSTER FORGET` and another has not, or while a newly met empty node is
  still gossiping. Every request between those nodes is then rejected as `ClusterMapMismatch`.
  The requester marks its map stale and refreshes, the views still legitimately differ, and the
  receiver forces a refresh of its own, so the mismatch repeats on every command.
  The mod.rs comment at lines 57–60 assumes only replica changes leave the fingerprint alone;
  slot-less masters are the case it misses. This was established by probe and inspection, not
  by a live forget/meet sequence.
- HASHTAG routing is unaffected (slot-based selection never reaches a slot-less shard), which
  makes the failure look query-dependent to an operator.

Build shards only from masters that own at least one slot, or keep slot-less masters out of
target sets and the fingerprint. Never drop a slot-*owning* master, even a failed one: a shard
whose only node is down must still fail the fanout rather than silently omit its data. Derive the
fingerprint from slot→owner ranges alone. Add the failover snapshot above as a parser test and a
`*_cme` failover case (kill a primary, promote its replica, expect correct replies).

**FAN-18 — HASHTAG routing silently drops tags whose slot has no owner.**

[random_one_from_slot](../../src/fanout/cluster_map.rs), lines 647–653, and the `HashTagsPrimary`
arm, lines 590–601, skip a slot for which `get_shard_by_slot` returns `None`. A query whose tags
span an owned slot and an unowned one therefore returns the owned shard's answer as the complete
result; only when *every* tag is unowned does `send_cluster_request` fail with `No cluster nodes
available`. An unowned slot requires a coverage gap, so the map is already inconsistent, but
FAN-10 means that map is still used rather than refused. The trigger is narrow (misconfiguration,
`CLUSTER DELSLOTS`, an incomplete slot assignment during setup), hence P3. The silent partial
reply is the defect. An unresolvable tag should fail the fanout, as an unreachable shard does.
Established by inspection; a probe could not exercise it because the coverage check's warning
needs the server logger.

## Performance and allocation patterns

**Coordinator decoding and reduction remain on the main thread.**
[on_response_received](../../src/fanout/cluster_rpc.rs), lines 864–888, synchronously invokes
the response handler, which [deserializes](../../src/fanout/fanout_command.rs), lines 168–180,
and calls `on_response`. Large protobuf graphs, string-table expansion, and command-specific
accumulation therefore run under the main-thread GIL. Final MRANGE normalization, fallback
aggregation, grouped reduction, and sorting also run from its reply callback. Moving shard
encoding off-thread is useful, but does not bound coordinator pauses. This is established by
the call graph; pause duration and an alternative implementation were not benchmarked.

**The response envelope adds a full payload copy while holding the GIL.**
After worker-side response serialization, `process_request_message` takes the GIL and
`send_message_internal` allocates/builds another vector, appending the entire body to it. The
Rust body and envelope coexist at roughly twice the encoded payload size, before accounting
for the decoded response or server transport buffers. Build the frame outside the GIL, or
encode the body directly after its header in one owned buffer. This should be assessed after
fixing FAN-01 so retention does not obscure transient-allocation measurements.

**Queue bounds limit jobs, not retained bytes.**
The lane caps queued jobs at 1,024 and uses 2–8 blocking workers. Each queued peer request owns
a copied payload, header strings, registration, cancellation flag, and job allocation; the
coordinator can separately retain results in its in-flight operation. Incoming requests copy
their payload and allocate a registration before `try_spawn` discovers a full queue. Reserve
first, then allocate/copy accepted work, and consider a byte budget in addition to job count.
The shared buffer pool's zero-initialization followed by clear/copy adds work on cache misses.

The client path also blocks before it attempts local-share admission, contrary to the repo's
reserve-before-blocking convention. Existing rejection paths do arrange an error reply, so
this is avoidable admission/lifecycle complexity rather than an established blocked-client leak.
Reserve any required local slot before blocking/sending, while retaining correct handling of
remote-only operations and remote shares already in flight.

**Early completion does not stop all unwanted work.**
Cancels are sent by `on_request_timeout`, not by FanoutState's early Busy/permission/topology
completion. Remaining remote work can run until the original deadline or answer normally, and
late successful payloads are deserialized before the Completed-state check discards them.
The local share receives a deadline but no shared “fanout completed” cancellation flag for a
read already running. Connect read cancellation to explicit completion, while preserving the
deliberately noncancellable write path. No cancellation-latency benchmark was run.

**Topology/target allocation costs have smaller opportunities.**

- HashTags removes duplicate slots with a linear `SmallVec::contains` for each tag. The parser
  has no tag-count cap. A slot bitmap or shard-ID set can make normalization linear and also
  supply FAN-02's required shard deduplication.
- Random target sets grow without reserving their known shard-count upper bound. Per-request
  `remote_targets` copies NodeInfo into a Vec solely for counting/iteration; measured NodeInfo
  is 104 bytes and NodeId 41 on this arm64 build. Iterate a shared snapshot where practical.
- Response tracking stores a second set of NodeIds and takes a mutex for every accepted
  answer. Current cluster receive callbacks are main-thread serialized; document future
  dispatch requirements before simplifying the atomics/locks.
- Every expired/stale refresh constructs a fresh map before discovering that topology is
  unchanged. Reusing published target caches is useful, but it does not avoid parsing,
  temporary maps, slot structures, and strings. Concurrent refresh coalescing addresses both
  that cost and FAN-03. Keep expensive parsing outside the GIL. FAN-10 is the case coalescing
  alone cannot fix: there the rebuild is the per-command steady state, and a backoff that
  engages on inconsistency is the larger win.
- `send_cluster_message` narrows `message_body.len()` with `as u32`. Enforce a checked wire-size
  limit before that cast and before committing large encodes. A >4 GiB message was not allocated
  or tested.

Third pass additions:

- **The receiver refreshes up to twice per request, uncoalesced.**
  [cluster_fingerprint_matches](../../src/fanout/cluster_rpc.rs), lines 537–554, first calls
  `get_or_refresh_cluster_map`, which may itself just have rebuilt the map. On a mismatch it
  forces a second `refresh_cluster_map` immediately, with nothing to tell that the first was
  already fresh. It runs on each of the lane's 2–8 workers independently. Under a persistent
  mismatch (FAN-17's forgotten slot-less master) or an inconsistent map (FAN-10), that becomes
  one or two `CLUSTER NODES` calls plus a full parse *per peer request*, on top of the
  coordinator's own per-command rebuild. Skip the forced refresh when the first call produced a
  new map, and route both sides through the coalesced refresh FAN-03 needs.
- **A finished answer is checked for cancellation but not for its deadline.**
  [process_request_message](../../src/fanout/cluster_rpc.rs), lines 632–654, drops an answer only
  when `is_cancelled()`. An answer that completes after the request's deadline is still sent: it
  takes the GIL, makes a full envelope copy, and goes on the wire, and the requester then logs it
  as an unknown request (FAN-15). The deadline counts from arrival, so it is never earlier than the
  requester's. This is the reasoning the `Err(Timeout)` arm already uses to stay silent.
  Use `expired()` on the success path too.
- **Mixed local/remote fanouts build the request twice.**
  [exec_command](../../src/fanout/fanout_command.rs), lines 134 and 161, calls
  `generate_request()` once for the wire and again for the local share. For MRANGE/MGET that
  means two filter and option trees, one of which is only serialized. Serialize first and give the
  local share the original. Separately, `FanoutStateInner::on_error` clones every `FanoutError` (a
  `String`) for the hook even though no implementation overrides it (FAN-11).

## Clarity, maintainability, and testability

Keep the useful existing guarantees: target/fingerprint pairing from one snapshot, per-node
duplicate/foreign-sender rejection, ACL and DB reinstallation on every shard lock, a bounded
worker lane, receiver deadlines/cancels, primary-only MDEL with a shard-side role check, and
blocked-client result freeing through the server's free callback. The timer code also explicitly
distinguishes stopping a pending timer from handling a firing one; preserve that distinction.

The main design improvement is to make completion an explicit owner with one outcome, a shared
deadline, and a cancellation decision. Today RPC remote-count state, command total-count state,
blocked-client timeout, and local/peer deadlines overlap. Destructor-driven completion conceals
missing replies, and the timeout durations start at different setup stages. Document which
parts count toward the overall timeout, including request construction and topology refresh.

Second pass: the asymmetry is concrete. The blocked-client timer starts in `exec` before
`exec_command` runs, while both the RPC timer and the local-share deadline are computed after
[get_fanout_targets](../../src/fanout/fanout_command.rs), line 130. A topology refresh under
FAN-10 therefore makes every share's deadline later than the client's, and
`fanout_duration_seconds` (which starts at the same later point) excludes selection and refresh
entirely, so the metric understates the pause a slow refresh causes.

The `exec_command` local-share ordering comment is narrower than it reads. Only the RPC
*setup-failure* return leaves the state `Pending`; synchronous send failures inside
`send_cluster_request` reach the RPC handler and can complete the fanout — invoking the blocked
client's reply path — before the local share is spawned
([fanout_command.rs](../../src/fanout/fanout_command.rs), lines 206–240). Behaviour is intact (a
completed cancellable share is skipped, a write still runs and its late result is ignored), but
the comment claims an invariant the code does not enforce, which is the kind of comment the next
change will trust.

The callback threading documentation needs correction. The closure in
`FanoutClientCommand::exec` says it runs on a background thread, but a last remote response,
send failure, or RPC timer completes it on the main thread; a last local response uses its
worker. `on_response`, `on_error`, and completion hooks need explicit “either thread” and GIL
contracts. They execute under the state mutex, so adding GIL-taking behavior there would also
require checking lock ordering against main-thread response delivery.

Other small cleanup opportunities: remove the unreachable timed-out branch after the Completed
check (`on_error(Timeout)` already completes); replace `res.err().unwrap()` after matching Err
with direct binding; correct the error-parser log that attributes malformed peer errors to this
node's ID; and review the safe `NodeId::from_raw` signature, whose memory-validity precondition
belongs on an unsafe function or a safe fixed-length slice interface.

Second pass additions to that list:

- `set_current_db` is called on the response and error receive paths
  ([cluster_rpc.rs](../../src/fanout/cluster_rpc.rs), lines 879 and 906) although neither path
  touches the keyspace — the shard side already selects its database per lock — so the call only
  mutates shared thread state on the main thread. `alloc_db_if_needed` (line 519) covers the
  intake side and duplicates the worker's `FanoutContext` selection; it should name whether it
  exists to allocate the database or to select it.
- Identity is duplicated where it feeds error classification. The MULTI/Lua refusal text is a
  constant in `fanout_client_command.rs`, line 12, and a literal in `cluster_rpc.rs`, line 364;
  `NO_CLUSTER_NODES_AVAILABLE` exists twice with different texts (`fanout_error.rs` and
  `error_consts.rs`) and both arms of `convert_from_string` are live; `SLOT_SIZE` (`utils.rs`)
  duplicates `NUM_SLOTS` (`cluster_map.rs`). `convert_from_string` is the fragile one: it infers
  a shard-side kind by comparing the handler's error text against strings owned by other modules,
  so changing a message at the producing site silently reclassifies the error as `Custom` — which
  is not cosmetic, because `KeyPermissions`, `Permissions`, `Busy` and `DatabaseSwapped` abort the
  fanout while `Custom` merely increments the aggregate error count. Carry the kind structurally
  where the error is raised, and keep text for the client message only.
  Third pass: the cost of falling into `Custom` is also that the text is lost. A `Custom` share
  error reaches the client only as `generate_error_reply()`, `Internal error in fanout operation
  '<name>'`, which no client command can override (FAN-11). The FAN-17 reproduction is a concrete
  example: an operator sees `Internal error` for what the log records as `Failed to send fanout
  request … to node 127.0.0.1:7311`. Any shard error with dynamic content (a database number, a
  limit value) is unclassifiable by equality and always ends up there. At least name the failing
  node, or the first share error's text, in the aggregate reply.
- The `ErrorKind` taxonomy is spread over six exhaustive matches — `as_str`, `metric_index`,
  `metric_name`, `TryFrom<u8>`, `ALL`, and the `convert_from_string` table — of which only the
  first two are pinned by tests. A single table-driven macro generating the enum, its wire code,
  metric name and text would make a new variant impossible to half-register.
- `cluster_map.rs` is 1 395 lines mixing the topology model, the parser, target selection and
  the lazy caches. The parser and `ParsedNodeLine` are pure and already have the module's best
  tests; extracting them removes roughly a third of the file without moving any logic.

The 86 unit tests concentrate on framing, error conversion, and map parsing. Preserve those
property tests, and add seams for a fake clock, transport, executor admission, and blocked-client
API so the following behavior can be tested without launching a server:

1. Successful completion only after every required share; worker unwind; setup rejection;
   timeout versus last reply; late and duplicate replies; exactly-once callback/free.
2. Multiple slots on one shard, multiple replicas, failed/disconnected candidates, overlapping
   claims, local replica lookup, and order-independent topology comparison. Also a slot-less
   failed master after failover (FAN-17), fingerprint equality with and without a slot-less
   master, and a HASHTAG set mixing owned and unowned slots (FAN-18). The map logger needs a
   no-server seam first: any snapshot with a coverage gap currently panics a unit test inside
   `log_warning`.
3. Controlled concurrent refresh publication and stale flags during failed refreshes.
4. Sequential buffer reuse and retained-byte bounds, plus rejection before payload allocation.
5. Parsing through actual intake/error handling, including strings that are valid protobuf
   but unsafe to pass unchanged to the logger.

Second pass: most of item 1 needs no new seam at all.
[FanoutState/FanoutStateInner](../../src/fanout/fanout_command.rs), lines 232–458, take only the
operation, the completion callback and a `&NodeInfo`, so a stub `FanoutCommand` with a `prost`
response type and an `mpsc` channel for the callback tests them today — no `Context`, no server.
That would pin the behaviours hardest to reason about and currently unverified: timeout zeroing
`outstanding` (lines 287–293), a response after `Completed` being ignored without touching the
`mem::take` placeholder (lines 325–338), `abort_error` winning over a concurrently recorded
`error_count` (lines 373–382), the FAN-05 drop-while-Active case (lines 395–405), and a rejected
local-share spawn dropping the callback without invoking it (lines 460–498). `fanout_command.rs`,
`fanout_client_command.rs`, `registry.rs`, `serialization.rs` and `workers.rs` have no tests;
`registry.rs` needs only a duplicate-registration case, `serialization.rs` one malformed buffer
asserting `COMMAND_DESERIALIZATION_ERROR`, and both `fail_fast` paths gain coverage from FAN-11
being either forwarded or deleted.

Existing cluster tests cover important ordinary routing, ACL, metrics, and timeout behavior.
Their HASHTAG fixtures largely choose tags on different primaries, so add a same-shard case
with replicas and READONLY. Include the known-failed-replica case without relying on random
draws for a permanent test: expose deterministic candidate selection or seed its RNG. No
existing test fails over a primary and then issues a fanout; FAN-17 needs one, and it is
deterministic.

Suggested order: fix FAN-17 first. It is the smallest change, and every deployment that
survives a failover hits it. Then fix FAN-01 and FAN-02; stop the per-command rebuild loop (FAN-10) and repair
publication/ownership validation and failed-node selection; settle the completion contract by
forwarding or deleting the unreachable hooks (FAN-11); make incomplete completion and callback
boundaries fail closed; then measure main-thread pauses, frame copying, queue bytes, cancellation,
and topology parsing before broader refactoring. The P3 additions (FAN-12 … FAN-16) are small and
mechanical; batch them with the first change that touches `cluster_map.rs` or `blocked_client.rs`
rather than scheduling them separately. FAN-18 belongs with FAN-02 (both are slot-to-shard
resolution in the HASHTAG path), and the callback panic guard belongs with FAN-05/FAN-06.

## Validation and limits

- `cargo test --offline --lib --features enable-system-alloc fanout::`: **86 passed**.
- `cargo build --offline --lib --features enable-system-alloc,test-utils`: passed; this rebuilt
  the debug module used by the live probes.
- Live probes used the local unstable Valkey binary with two isolated nodes, one full-slot
  primary and one synchronized replica, temporary directories, and RDB/AOF persistence disabled.
  They confirmed FAN-02 and FAN-07. Both processes were stopped after each run.
- The pool probe linked the actual repository library with system allocation. It measured
  distinct buffer addresses, actual vector capacities, and process peak RSS; it did not profile
  a live server or count all allocations.
- Component probes compiled temporary copies of production fanout code with server/transport
  entry points stubbed and logging suppressed where a live context was unavailable. They
  exercised actual map parsing/target selection, refresh publication, command state transitions,
  and header decoding. FAN-06 separately used the actual SDK logger under `catch_unwind`.
  Production algorithms for those probes were not changed. They are component reproductions,
  not full RPC/blocked-client integration tests.
- No full integration suite, compatibility/reference comparison, ASAN/Miri run, broad load test,
  comparative latency benchmark, or live migration/failover reproduction was completed. No
  RedisTimeSeries source or test code was consulted. The malformed-handler callback abort was
  inferred from the reproduced logger panic and its `extern "C"` call path, not triggered in
  a live server.
- Second pass: every fanout source file was re-read, together with the command consumers,
  `commands/utils.rs`, `common/threads/executor.rs`, `common/sync.rs`, `common/pool.rs`,
  `common/logging.rs` and `series/background_tasks.rs`. The fanout sources are identical to the
  review base (`git diff e6b077044..HEAD -- src/fanout src/commands/utils.rs` is empty; the one
  intervening commit touches `src/aggregators`).
  `cargo clippy --profile release --all-targets -- -D clippy::all` was clean. FAN-10 … FAN-16,
  the FAN-07 addendum and the second-pass notes are code-path arguments: no probe, cluster or
  live server was used for them, so their triggers are traced from the call graph rather than
  reproduced.
- Confirmed during that pass: `AtomicU64::try_update` returns the **previous** value, so the
  `v == 1` last-answer test in `finish_inflight_request` is correct; `refresh_cluster_map`,
  `mark_cluster_map_stale` and `TS._DEBUG REFRESH_CLUSTERMAP` are the only refresh entry points
  (no periodic task exists); `pick_target`'s `replica_only` argument is `false` at all four call
  sites; and no `FanoutCommand` implementation overrides `fail_fast`.
- Third pass, at `10fc28282`: `git diff e6b077044..HEAD -- src/fanout` is still empty. Every
  file was re-read before the document, and then each FAN-01 … FAN-16 citation was rechecked;
  line numbers and claims are current. `cargo test --offline --lib --features
  enable-system-alloc fanout::`: **86 passed**.
- FAN-17 live probe: the local `tests/build/binaries/unstable/valkey-server` and
  `target/release/libvalkey_timeseries.dylib` (built after HEAD), two cluster nodes with
  `cluster-node-timeout 2000`, persistence off, in scratch directories that were deleted
  afterwards. Steps: A takes all slots, B replicates A, create one series, `SIGKILL` A,
  `CLUSTER FAILOVER TAKEOVER` on B, then commands on B. Both processes were confirmed stopped.
  The FAN-17 parser probe ran as a temporary test module appended to `cluster_map.rs`. The file
  was restored byte-for-byte afterwards, and `git diff -- src/fanout` was empty again.
- Not run in the third pass: a multi-shard MDEL across a failover (FAN-17's partial-write
  consequence is a code-path argument), a live `CLUSTER FORGET`/`CLUSTER MEET` fingerprint
  mismatch, any probe of FAN-18 (blocked by the logger panic noted under testability), and any
  benchmark of the third-pass performance items.
