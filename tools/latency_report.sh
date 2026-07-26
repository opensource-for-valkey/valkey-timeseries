#!/usr/bin/env bash

# Builds and runs the `latency_report` binary, which times encode and decode
# paths for every chunk encoding at a fixed sample count and writes the matrix
# to target/bench-reports/.
#
#   tools/latency_report.sh                          # defaults: 1000 samples, drift + noisy
#   tools/latency_report.sh --samples 10000          # bigger working set
#   tools/latency_report.sh --encodings chimp,gorilla
#   tools/latency_report.sh --workloads all          # the 12 shape workloads
#   tools/latency_report.sh --workloads drift_q2,counter --ts-models all
#   tools/latency_report.sh --iterations 500 --warmup 50   # tighter medians
#   tools/latency_report.sh --chunk-size 4096        # measure at a real chunk budget
#   tools/latency_report.sh --seed 42                # override the per-dataset seed
#   tools/latency_report.sh --out-csv /tmp/lat.csv --out-md /tmp/lat.md
#   tools/latency_report.sh --quiet                  # files only, no stdout table
#
# Values for --encodings: uncompressed, gorilla, tsxor, xor2, dexor, chimp (or `all`).
# Values for --ts-models: regular, jitter, irregular (or `all`).
# Workload ids come from `ValueWorkload::id()` (drift, noisy, bursty, counter,
# discrete, constant, periodic, the *_q2 quantized variants, ...) or `all`.
#
# Measurements are medians of repeated runs of the whole operation; they are
# wall-clock and therefore machine- and load-dependent. Compare rows within one
# run, not absolute numbers across machines.
#
# The `test-utils` feature exposes src/tests (data generators, chunk helpers) to
# the binary; `enable-system-alloc` is required by every target that links the
# crate's global allocator outside a running Valkey.

set -euo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$REPO_ROOT"

FEATURES="enable-system-alloc,test-utils"
CSV="target/bench-reports/latency.csv"
MD="target/bench-reports/latency.md"

usage() {
    sed -n '3,29p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

REPORT_ARGS=()
while [ $# -gt 0 ]; do
    case "$1" in
        --samples|--chunk-size|--iterations|--warmup|--encodings|--workloads|--ts-models|--seed)
            if [ $# -lt 2 ]; then
                echo "error: $1 requires a value" >&2
                exit 1
            fi
            REPORT_ARGS+=("$1" "$2")
            shift
            ;;
        --out-csv)
            if [ $# -lt 2 ]; then
                echo "error: --out-csv requires a path" >&2
                exit 1
            fi
            CSV="$2"
            REPORT_ARGS+=("$1" "$2")
            shift
            ;;
        --out-md)
            if [ $# -lt 2 ]; then
                echo "error: --out-md requires a path" >&2
                exit 1
            fi
            MD="$2"
            REPORT_ARGS+=("$1" "$2")
            shift
            ;;
        --quiet)
            REPORT_ARGS+=("$1")
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

echo "Running latency report (release, features: $FEATURES)..."
cargo run --release --features "$FEATURES" --bin latency_report -- "${REPORT_ARGS[@]+"${REPORT_ARGS[@]}"}"

echo "Report: $REPO_ROOT/$CSV"
echo "        $REPO_ROOT/$MD"
