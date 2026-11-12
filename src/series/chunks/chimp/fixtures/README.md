# Chimp golden streams

These fixtures were generated from this repository's encoder at
`9b0499227`, the
Chimp grammar as that commit left it. They carry no module encoding version: the
grammar changed in place before any release. They contain no RedisTimeSeries
source or test data.

Each input is 32 samples from `generate_dataset`, using the named workload,
`TimestampModel::Jitter`, and `dataset_seed` for that key. The first line is the
hexadecimal output of `ChimpCompressor::serialize`; subsequent lines contain
`timestamp value_bits_hex`. Literal IEEE 754 bits keep the expected values
independent of future changes to the random generators.

`drift_q2` exercises the decimal-place (`11` + alpha) header, the previous-alpha
hint and 3-bit repeats; `counter` exercises integers stored raw under reused
windows; `noisy` exercises full-precision values stored raw. Every fixture uses
jittered timestamps, so it carries non-zero delta-of-deltas.

The tests in `chimp_chunk_tests.rs` (`fixtures` module) re-encode the samples and
compare the bytes, decode the bytes and compare every sample bit for bit, then
append, serialize again and compare. A change to any of those outputs is a format
change: it needs regenerated fixtures (`write_chimp_fixtures`, `#[ignore]`d) and,
once the module has been released, a new module encoding version. That is because a foreign
load accepts a chunk only if the current encoder reproduces its stream exactly
(`XorChunk::verify_stream`, whose doc says what the version bump must change there).
