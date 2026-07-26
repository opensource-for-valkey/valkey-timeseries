#!/usr/bin/env bash

# Builds and runs the `wire_report` binary, which measures what each chunk
# encoding costs on the clustered fan-out path: serialized payload bytes plus
# shard-side encode and coordinator-side decode time, swept across sample
# counts. Writes the matrix to target/bench-reports/.
#
#   tools/wire_report.sh                             # defaults
#   tools/wire_report.sh --sample-counts 8,16,32,64  # custom sweep
#   tools/wire_report.sh --workloads all --ts-models all   # full shape matrix
#   tools/wire_report.sh --encodings gorilla,chimp   # uncompressed is always kept
#   tools/wire_report.sh --link-gbps 1               # judge against a slow link
#   tools/wire_report.sh --iterations 500 --warmup 50     # tighter medians
#   tools/wire_report.sh --seed 42                   # override the per-dataset seed
#   tools/wire_report.sh --out-csv /tmp/w.csv --out-md /tmp/w.md
#   tools/wire_report.sh --quiet                     # files only, no stdout table
#
# Values for --encodings: uncompressed, gorilla, tsxor, xor2, dexor, chimp (or `all`).
# Values for --ts-models: regular, jitter, irregular (or `all`).
# Workload ids come from `ValueWorkload::id()` (drift, noisy, counter, discrete,
# periodic, the *_q2 quantized variants, ...) or `all`.
#
# Before measuring anything, the run puts every encoding through a set of
# adversarial payloads (NaN, infinities, -0.0, subnormals, timestamp extremes,
# duplicate timestamps). The grouped/aggregated fan-out path back-fills empty
# buckets with NaN, so an encoding that cannot round-trip those bit-for-bit is
# unusable on the wire no matter how well it compresses. Failures print first.
#
# `break_even` is the link speed (Gbit/s) below which the bytes an encoding
# saves take longer to transmit than the extra CPU takes to spend. Compare it
# against the interconnect: an encoding pays off on any link slower than its
# break-even figure.
#
# Measurements are medians of repeated runs and are wall-clock, so they are
# machine- and load-dependent. Compare rows within one run, not absolute
# numbers across machines.
#
# The `test-utils` feature exposes src/tests (data generators, chunk helpers) to
# the binary; `enable-system-alloc` is required by every target that links the
# crate's global allocator outside a running Valkey.

set -euo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$REPO_ROOT"

FEATURES="enable-system-alloc,test-utils"
CSV="target/bench-reports/wire.csv"
MD="target/bench-reports/wire.md"

usage() {
    sed -n '3,40p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

REPORT_ARGS=()
while [ $# -gt 0 ]; do
    case "$1" in
        --sample-counts|--iterations|--warmup|--encodings|--workloads|--ts-models|--seed|--link-gbps)
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

echo "Running wire report (release, features: $FEATURES)..."
cargo run --release --features "$FEATURES" --bin wire_report -- "${REPORT_ARGS[@]+"${REPORT_ARGS[@]}"}"

echo "Report: $REPO_ROOT/$CSV"
echo "        $REPO_ROOT/$MD"
