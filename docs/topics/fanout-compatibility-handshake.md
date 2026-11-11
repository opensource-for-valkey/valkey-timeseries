# Fan-out compatibility handshake

## Payload: push-down is binding

A shard does part of the work the coordinator would otherwise do. For `TS.MRANGE` it always
aggregates each series and pre-applies `COUNT`; `MultiRangeRequest.apply_group_reduce`
(`proto/v1/request.proto`) additionally asks it to answer a GROUPBY with group partials alone.
Nothing echoes the work back: the coordinator expects buckets whenever the query aggregates and
partials alone under `apply_group_reduce`, and rejects a share of any other shape.

No released version predates this, so no shard can answer otherwise. A push-down added after a
release will need a way to tell an older shard's answer apart, or a bit in `required_features`
(below) so an older shard refuses the request instead of answering it wrongly.

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
the receiver's deadline can't fall before the requester's. It can fall after it, by the transit
time and however long the receiver's main thread took to read the request.

To cover that gap, a coordinator whose timer fires sends a cancel message to the targets yet to
answer. It needs no clocks, and because it follows the request on the same link, a stalled receiver
reads the request and then the cancel. A cancelled request is treated as if its budget had run out.
A receiver that doesn't know the message type drops it, and its share ends at its budget instead.
A write is never cancelled, so a coordinator sends no cancel for one.

- If a request is cancelled or runs out of budget while waiting in the fan-out lane's queue, the
  receiver drops it without running it. It also sends no reply, because the requester has
  already given up (`fanout_dropped_expired_total` in `TS._DEBUG METRICS`).
- If a read is cancelled or past its budget, it stops part-way and also sends no reply
  (`fanout_abandoned_expired_total`). Most reads stop between batches. `TS.MRANGE` checks before
  taking the lock and once it has it. It then holds the lock only to select the series and copy
  their chunks, and checks between series or groups while it decodes them after releasing the
  lock. A query with `FILTER_BY_TS` reads the live series instead, so it holds the lock while it
  decodes, checking at the same points. Selecting the series can't be interrupted, and neither can
  one group's reduce.
- If a read finishes after its cancel arrives, the receiver discards the answer instead of sending
  it (`fanout_answers_discarded_total`).
- Operations that write (`FanoutCommand::cancellable() == false`, i.e. `TS.MDEL`) never expire on
  a shard that received them.
