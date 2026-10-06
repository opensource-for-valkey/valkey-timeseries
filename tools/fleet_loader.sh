#!/usr/bin/env bash

# Builds and runs the `fleet_loader` binary, which fills a running server that
# has valkey-timeseries loaded with a realistic Kubernetes fleet: node_exporter,
# cAdvisor, kube-state-metrics, JVM / Go runtime and HTTP histogram series
# (src/tests/generators/labels.rs) with values scraped every interval over a
# window ending now (src/tests/generators/fleet_metrics.rs).
#
#   tools/fleet_loader.sh                            # small preset (~17k series), 1h at 15s, 127.0.0.1:6379
#   tools/fleet_loader.sh -p 7001                    # a cluster: series go to the primary owning their slot
#   tools/fleet_loader.sh --preset medium --flush    # ~120k series, FLUSHALL first
#   tools/fleet_loader.sh --duration 6h --interval 30s
#   tools/fleet_loader.sh --end 1767225600000        # a fixed window instead of one ending now
#   tools/fleet_loader.sh --clusters 2 --hosts 50 --namespaces 12 --pods 400 --routes 6
#   tools/fleet_loader.sh --encoding gorilla --chunk-size 8192 --retention 7d
#   tools/fleet_loader.sh --prefix b: --label fleet=b   # a second copy alongside the first
#   tools/fleet_loader.sh --emit /tmp/fleet.resp     # RESP to a file instead; then
#                                                    #   valkey-cli --pipe < /tmp/fleet.resp
#
# Keys are <prefix><metric>:ts:<n> (default prefix `k8s:`). The loader refuses to write
# over an earlier load unless --flush is given. The server rejects a series
# whose label set already exists under any key, so a second copy of the same
# fleet needs both another --prefix and a --label. Exits non-zero if any reply
# was an error.
#
# Run `tools/fleet_loader.sh --help` for every flag. The `fleet-loader` feature
# pulls in the generator (src/tests) and the `redis` client; `enable-system-alloc`
# is required by every target that links the crate's global allocator outside
# a running Valkey.

set -euo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$REPO_ROOT"

FEATURES="enable-system-alloc,fleet-loader"

if [ "${1:-}" = "--help" ]; then
    sed -n '3,30p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    echo
fi

cargo run --release --quiet --features "$FEATURES" --bin fleet_loader -- "$@"
