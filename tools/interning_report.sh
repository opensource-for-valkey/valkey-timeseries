#!/usr/bin/env bash

# Builds and runs the `interning_report` binary, which builds a realistic
# Kubernetes-fleet label set as MetricNames and reports what the string pool
# saves against the layouts interning replaced.
#
#   tools/interning_report.sh                        # medium preset (~120k series)
#   tools/interning_report.sh --preset large         # ~1M series; small is ~17k
#   tools/interning_report.sh --top 20               # longer top-K tables
#   tools/interning_report.sh --seed 42              # a different fleet of the same shape
#   tools/interning_report.sh --clusters 2 --hosts 50 --namespaces 12 --pods 400 --routes 6
#   tools/interning_report.sh --emit-commands /tmp/fleet.txt
#
# Overrides are checked against the generator's limits before anything is
# built: --hosts must be at least 1 whenever --clusters is, and --routes is
# capped at the 32 distinct routes a service can expose.
#
# --emit-commands writes one inline `TS.CREATE ... LABELS ...` per series, so
# the same fleet can be loaded into a running server and read back with
# `TS._DEBUG STRINGPOOLSTATS` / `INFO ts_memory`:
#
#   valkey-cli --pipe < /tmp/fleet.txt
#   valkey-cli CONFIG SET ts.debug-mode yes
#   valkey-cli TS._DEBUG STRINGPOOLSTATS 10
#
# The `test-utils` feature exposes src/tests (the fleet generator) to the
# binary; `enable-system-alloc` is required by every target that links the
# crate's global allocator outside a running Valkey.

set -euo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$REPO_ROOT"

FEATURES="enable-system-alloc,test-utils"

usage() {
    sed -n '3,26p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

REPORT_ARGS=()
while [ $# -gt 0 ]; do
    case "$1" in
        --preset|--seed|--top|--clusters|--hosts|--namespaces|--pods|--routes|--emit-commands)
            if [ $# -lt 2 ]; then
                echo "error: $1 requires a value" >&2
                exit 1
            fi
            REPORT_ARGS+=("$1" "$2")
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "error: unknown option '$1'" >&2
            usage >&2
            exit 1
            ;;
    esac
    shift
done

cargo run --release --quiet --features "$FEATURES" --bin interning_report -- "${REPORT_ARGS[@]+"${REPORT_ARGS[@]}"}"
