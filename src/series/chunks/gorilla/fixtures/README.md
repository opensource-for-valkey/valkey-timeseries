# Legacy Gorilla streams

These fixtures were generated from this repository's encoder at
`106088e57`, before the Gorilla changes.
They contain no RedisTimeSeries source or test data.

Each input is 32 samples from `generate_dataset`, using the named workload,
`TimestampModel::Jitter`, and `dataset_seed` for that key. The first line is the
hexadecimal output of `GorillaEncoder::serialize`; subsequent lines contain
`timestamp value_bits_hex`. Literal IEEE 754 bits keep the expected values
independent of future changes to the random generators.

The periodic case exercises full-width windows, the counter case exercises
trailing-zero windows, and the constant case preserves the unset-window state.
Tests load them, append, serialize again, and compare every sample bit for bit.
