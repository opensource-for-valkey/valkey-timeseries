# Fanout Compatibility Handshake

Fanout requests use protobuf payloads inside a small, hand-written envelope.
The envelope routes a request by handler name and carries a `required_features`
bitset. A receiver rejects a request that requires envelope features it does not
support, rather than interpreting its payload incorrectly.

The protobuf request and response messages are independently forward-compatible.
For an optional shard-side optimization, the coordinator sends an explicit
request flag and the shard echoes a corresponding `applied_*` response flag.
The coordinator evaluates each response independently:

1. A shard that echoes the flag supplied an optimized result.
2. A shard that does not echo it supplied the legacy form, which the coordinator
   processes locally before combining results.

This lets rolling upgrades mix versions without affecting query correctness. A
lagging shard costs additional transfer and coordinator work only for its own
slice of a request.

New response encodings must not reuse an `applied_*` field without a matching
request opt-in. The request field establishes that the coordinator can decode
the new representation; the response field confirms that the shard used it.
Retain the legacy fields and behaviour until every intended peer can process the
new form.