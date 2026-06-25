# valkey-timeseries

**valkey-timeseries** (Apache-2.0) is a Rust-based module providing a TimeSeries data type for [Valkey](https:://valkey.io).
The goal of this module is to provide a simple, efficient, and easy-to-use time series data type for Valkey, as
well as provide a superset of the _RedisTimeSeries_ API.

## Features
- In-memory storage for time series data
- Configurable data retention period
- Configurable encoding
- Single sample and range queries
- Supports [Metadata](https://prometheus.io/docs/prometheus/latest/querying/api/#querying-metadata) like queries
- PromQL querying with the `TS.QUERY` and `TS.QUERYRANGE` commands, which
  support [instant and range queries](https://victoriametrics.com/blog/prometheus-monitoring-instant-range-query/)
  respectively.
- Basic compatibility with the [RedisTimeSeries](https://oss.redislabs.com/redistimeseries/) API.

## Scaling

**valkey-timeseries** offers two deployment modes: 
- **Standalone Mode**: Processing scales vertically with CPU cores
- **Cluster Mode**: Enables horizontal scaling across nodes for larger datasets

**Query Scaling Options:**
- For read-heavy workloads, you can direct queries to replicas if your application can tolerate some replication lag


## Performance

valkey-timeseries achieves high performance by storing series data in-memory and applying optimizations throughout the stack to efficiently use the host resources, such as:

- **Parallelism:** Low-overhead threading model that enables concurrent lock-free reads across series and chunks.
- **CPU Cache Efficiency:** Modern, cache-friendly algorithms for data and index storage.
- **Memory Efficiency:** Uses string interning for label-value pairs.
## Commands

Command names and option names are case-insensitive.

#### A note about keys
Since the general use-case for this module is querying across timeseries, it is a best practice to group related timeseries
using "hash tags" in the key. This allows for more efficient querying across related timeseries. For example, if your
metrics are generally grouped by environment, you could use a key like
`latency:api:{dev}` and `latency:frontend:{staging}`. If you are more likely to group by service, you could use
`latency:{api}:dev` and `latency:{frontend}:staging`.


https://tech.loveholidays.com/redis-cluster-multi-key-command-optimisation-with-hash-tags-8a2bd7ce12de

The following commands are supported

```aiignore
TS.ADD
TS.ADDBULK
TS.ALTER
TS.CARD
TS.CREATE
TS.CREATERULE
TS.DELETERULE
TS.DECRBY
TS.DEL
TS.GET
TS.INCRBY
TS.JOIN
TS.LABELNAMES
TS.LABELSTATS
TS.LABELVALUES
TS.MADD
TS.MDEL
TS.MGET
TS.MRANGE
TS.MREVRANGE
TS.OUTLIERS
TS.QUERY
TS.QUERYINDEX
TS.QUERYRANGE
TS.RANGE
TS.REVRANGE
TS._DEBUG
```


## Build instructions
```
curl https://sh.rustup.rs -sSf | sh
sudo yum install clang
git clone https://github.com/ccollie/valkey-timeseries.git
cd valkey-timeseries
cargo build --release
valkey-server --loadmodule ./target/release/libvalkey_timeseries.so
```
**Note**: This library requires a minimum rust version of `1.86`.

#### Running Unit tests

To run all unit tests, follow these steps:

    $ cargo test --features enable-system-alloc

#### Running Criterion benchmarks

The PromQL benchmark target is `promql_engine` and requires `enable-system-alloc`.

Default (local) profile uses longer warmup/measurement windows for more stable results:

```zsh
cargo bench --bench promql_engine --features enable-system-alloc
```

CI profile uses faster settings and flat sampling:

```zsh
BENCH_PROFILE=ci cargo bench --bench promql_engine --features enable-system-alloc
```

`BENCH_PROFILE` is read by `benches/promql_engine.rs`:

- unset (or any value other than `ci`): local/stable profile
- `ci`: fast profile (`SamplingMode::Flat`, shorter run time)

#### Local development script to build, run format checks, run unit / integration tests, and for cargo release:
```
# Builds the valkey-server (unstable) for integration testing.
SERVER_VERSION=unstable
./build.sh
# Build with asan, you may need to remove the old valkey binary if you have used ./build.sh before. You can do this by deleting the `.build` folder in the `tests` folder 
ASAN_BUILD=true
./build.sh
# Clean build artifacts
./build.sh clean
```

## Load the Module

### Docker (recommended for quick start)
The easiest way to run valkey-timeseries is with Docker:

```bash
# Build the image and start a standalone instance
make docker-build
make docker-up

# Or start a 3-node cluster for testing cluster features
make docker-up-cluster

# Connect and test
valkey-cli -h localhost TS.ADD test 1000 42
valkey-cli -h localhost TS.GET test
```

**Available Docker targets:**

| Command                    | Description                                            |
|----------------------------|--------------------------------------------------------|
| `make docker-build`        | Build production image (official `valkey/valkey` base) |
| `make docker-build-source` | Build from source (Valkey + module compiled together)  |
| `make docker-up`           | Start standalone container on port 6379                |
| `make docker-down`         | Stop and remove standalone container                   |
| `make docker-up-cluster`   | Start 3-node cluster (ports 6379-6381)                 |
| `make docker-down-cluster` | Stop and remove cluster                                |
| `make docker-shell`        | Open a shell in the running container                  |
| `make docker-logs`         | Follow container logs                                  |
| `make docker-test`         | Run integration tests against running container        |

**Custom builds:**

```bash
# Build for Valkey 8.0 with the valkey_8_0 feature flag
./scripts/build-docker.sh prod 8.0 valkey_8_0

# Build from Valkey's unstable/main branch
./scripts/build-docker.sh source unstable
```

**Runtime configuration via environment variables:**

| Variable | Default | Description |
|---|---|---|
| `VALKEY_PORT` | `6379` | Listen port |
| `VALKEY_CLUSTER_ENABLED` | (unset) | Set to `yes` to enable cluster mode |
| `VALKEY_LOGLEVEL` | `notice` | Log verbosity |
| `VALKEY_REQUIREPASS` | (unset) | Set a password |
| `VALKEY_SAVE` | (unset) | RDB save directive (e.g., `900 1`) |
| `VALKEY_APPENDONLY` | (unset) | Set to `yes` for AOF persistence |
| `VALKEY_EXTRA_ARGS` | (unset) | Extra args passed to valkey-server |

### Manual (host-native)
To test the module with a Valkey, you can load the module in the following ways:

#### Using valkey.conf:
```
1. Add the following to valkey.conf:
    loadmodule /path/to/libvalkey_timeseries.so
2. Start valkey-server:
    valkey-server /path/to/valkey.conf
```

#### Starting Valkey with the `--loadmodule` option:
```text
valkey-server --loadmodule /path/to/libvalkey_timeseries.so
```

#### Using the Valkey command `MODULE LOAD`:
```
1. Connect to a running Valkey instance using valkey-cli
2. Execute Valkey command:
    MODULE LOAD /path/to/libvalkey_timeseries.so
```
## License
valkey-timeseries is licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).