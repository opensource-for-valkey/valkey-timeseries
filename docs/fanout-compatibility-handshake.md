# Fan-out compatibility handshake

The module hasn't been released yet, so the fan-out protocol is changed in place. Nothing keeps
it readable by older builds, and every node in a cluster is expected to run the same build. Two
mechanisms are left for future changes.

## Payload: optional push-down, echoed back

A request asks a shard to do optional work. For example, `MultiRangeRequest` carries
`apply_aggregation`, `apply_group_reduce` and `apply_count` (`proto/v1/request.proto`). The shard
echoes the work it actually did in the response's `applied_*` fields
(`proto/v1/response.proto`), and the coordinator finishes whatever the shard didn't do for that
shard's slice.

## Envelope: `required_features`

The message header (`src/fanout/fanout_message.rs`) is a hand-written binary format, not
protobuf. When a request arrives, the receiver checks its `required_features` u16 against
`SUPPORTED_MESSAGE_FEATURES`. If the request needs a bit the receiver doesn't support, the
receiver rejects it with an `unsupported_features` error. It doesn't try to read it. No bits are
defined yet. A bit is for a change the receiver must not ignore, such as payload compression; a
field every node will understand goes straight into the header.

## Header fields

In wire order: marker, `version` (u16 LE), `request_id` (uvarint), `db` (signed varint),
`handler` and `user` (length-prefixed), `cluster_fingerprint` (u64 LE), `required_features`
(u16 LE), `time_budget_ms` (uvarint, 0 = none), then the payload.

### Time budget

The coordinator sends its fan-out timeout with each request. Replies and errors send 0. The
receiver measures the budget from when the request arrives, so the clocks on the two nodes don't
need to agree, and the receiver's deadline can't fall before the requester's.

- If a request is still waiting in the fan-out lane's queue when its budget runs out, the
  receiver drops it without running it. It also sends no reply, because the requester has
  already given up (`fanout_dropped_expired_total` in `TS._DEBUG METRICS`).
- If a read is past its budget, it stops between batches.
- Operations that write (`FanoutCommand::cancellable() == false`, i.e. `TS.MDEL`) never expire on
  a shard that received them.
