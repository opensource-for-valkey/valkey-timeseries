# Fan-out compatibility handshake

## Payload: optional push-down, echoed back

A request asks a shard to do optional work. For example, `MultiRangeRequest` carries
`apply_aggregation`, `apply_group_reduce` and `apply_count` (`proto/v1/request.proto`). The shard
echoes the work it actually did in the response's `applied_*` fields
(`proto/v1/response.proto`), and the coordinator finishes whatever the shard didn't do for that
shard's slice.

## Envelope: `required_features`

Every message starts with a marker and a length-prefixed `EnvelopeHeader`
(`proto/v1/envelope.proto`, parsed in `src/fanout/fanout_message.rs`), followed by the raw payload.
A receiver ignores header fields it doesn't know, so a new field's absent value must mean today's
behaviour.

A change the receiver must not ignore, such as payload compression, takes a bit in
`required_features` instead. When a request arrives, the receiver checks that field against
`SUPPORTED_MESSAGE_FEATURES`. If the request needs a bit the receiver doesn't support, the receiver
rejects it with an `unsupported_features` error. It doesn't try to read it. No bits are defined yet.

## Time budget

The coordinator sends its fan-out timeout with each request as `time_budget_ms`. Replies and
errors send 0, which means no limit, and so does a header without the field. The receiver measures
the budget from when the request arrives, so the clocks on the two nodes don't need to agree, and
the receiver's deadline can't fall before the requester's.

- If a request is still waiting in the fan-out lane's queue when its budget runs out, the
  receiver drops it without running it. It also sends no reply, because the requester has
  already given up (`fanout_dropped_expired_total` in `TS._DEBUG METRICS`).
- If a read is past its budget, it stops between batches and also sends no reply
  (`fanout_abandoned_expired_total`).
- Operations that write (`FanoutCommand::cancellable() == false`, i.e. `TS.MDEL`) never expire on
  a shard that received them.
