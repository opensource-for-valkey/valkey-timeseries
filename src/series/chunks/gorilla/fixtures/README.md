# Gorilla golden streams

These fixtures were generated from this repository's Gorilla encoder on 2026-11-12. They carry no
module encoding version: no release has shipped yet. They contain no RedisTimeSeries source or
test data.

Each input is 32 samples from `generate_dataset`, using the named workload,
`TimestampModel::Jitter`, and `dataset_seed` for that key. The first line is the hexadecimal output
of `GorillaEncoder::serialize`; subsequent lines contain `timestamp value_bits_hex`. Literal IEEE
754 bits keep the expected values independent of future changes to the random generators.

`drift_q2` exercises small XOR windows that the encoder reuses; `counter` exercises integers
whose XOR changes width; `noisy` exercises full-precision values that need a new window most of
the time. Every fixture uses jittered timestamps, so it carries non-zero delta-of-deltas.

The tests in `golden_tests.rs` re-encode the samples and compare the bytes, decode the bytes and
compare every sample bit for bit, then append, serialize again and compare.

A change to any of those outputs is a format change, not just a grammar change. A foreign load
accepts a compressed chunk only if the current encoder reproduces its stream exactly
(`XorChunk::verify_stream`), so chunks written before the change would fail `RESTORE`, `MIGRATE`
and slot import. Such a change needs:

- regenerated fixtures (`write_gorilla_fixtures`, `#[ignore]`d);
- once the module has been released, a new module encoding version, with `verify_stream` given
  that version (see its doc comment).
